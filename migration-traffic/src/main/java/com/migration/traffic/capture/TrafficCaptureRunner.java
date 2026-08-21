package com.migration.traffic.capture;

import com.migration.traffic.TrafficMetrics;
import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import com.migration.traffic.model.TrafficRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 流量复制任务的主循环：{@link TrafficSource} 取语句 → 归一成 {@link TrafficRecord} → 落盘。
 */
public class TrafficCaptureRunner {

    private static final Logger logger = LoggerFactory.getLogger(TrafficCaptureRunner.class);

    protected final Properties props;
    protected final String taskId;
    protected final File recordingDir;
    protected final AtomicBoolean stopped = new AtomicBoolean(false);
    /**
     * 收尾完成的信号。
     *
     * <p>存在的理由：JVM 收到 SIGTERM 后<b>只等 shutdown hook，不等 main 线程</b>。
     * hook 里只置一个 stop 标志就返回，JVM 随即退出，主循环被拦腰砍断——
     * 实测后果是双份的：最后一批已读到的语句还在缓冲区里<b>直接丢掉</b>，
     * 而且 {@code source.close()} 从未执行，<b>源库的 general_log 一直开着</b>。
     * 所以 hook 必须阻塞等这个闩。
     */
    private final CountDownLatch finished = new CountDownLatch(1);

    protected TrafficSource source;
    /** 源端引擎。捕获通道按它分派，录制头里也要写进去。 */
    protected TrafficEngine engine;
    protected TrafficWriter writer;
    protected SessionSchemaTracker schemaTracker;
    protected CaptureFilter filter;
    protected TrafficMetrics metrics;

    protected long seq;
    protected long t0Micros;
    protected long startedAtMs;
    protected long lastRateAt;
    protected long lastRateRecords;

    public TrafficCaptureRunner(Properties props) {
        this.props = props;
        this.taskId = props.getProperty("task.id", "unknown");
        this.recordingDir = new File("./files/" + taskId + "/traffic");
    }

    public void run() throws Exception {
        try {
            start();
            loop();
        } finally {
            try {
                shutdown();
            } finally {
                finished.countDown();
            }
        }
    }

    /** 等待收尾完成（封口 + 还原源库）。超时返回 false —— 调用方据此告警而不是假装干净退出。 */
    public boolean awaitFinished(long timeoutMs) {
        try {
            return finished.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected void start() throws Exception {
        filter = CaptureFilter.from(props);
        schemaTracker = new SessionSchemaTracker(
                (int) longProp("traffic.capture.max.tracked.sessions", 20_000L));
        metrics = new TrafficMetrics(taskId, "traffic_capture_liveness");

        engine = TrafficEngine.parse(props.getProperty("source.db.type",
                props.getProperty("traffic.engine", "mysql")));
        source = newSource(engine);
        source.open(props);
        t0Micros = source.t0Micros();
        startedAtMs = System.currentTimeMillis();
        // 连接池里的连接在捕获开始前就建好了，它们的 Connect/Init DB 行永远不会再出现
        schemaTracker.seed(source.snapshotSessionSchemas());

        RecordingManifest manifest = openManifest();

        writer = new TrafficWriter(recordingDir, manifest,
                longProp("traffic.capture.segment.max.records", 500_000L),
                longProp("traffic.capture.segment.max.bytes", 64L * 1024 * 1024),
                longProp("traffic.capture.flush.ms", 1000L));

        onStarted();
        logger.info("流量复制已启动: taskId={}, 录制目录={}", taskId, recordingDir.getAbsolutePath());
    }

    /**
     * 按引擎选捕获通道。三条通道之间没有任何共用代码——
     * MySQL 读 {@code mysql.general_log} 表、PG 读服务端日志文件、Oracle 读统一审计视图。
     */
    protected TrafficSource newSource(TrafficEngine eng) {
        switch (eng) {
            case MYSQL: return new GeneralLogTrafficSource();
            case POSTGRESQL: return new PgLogTrafficSource();
            case ORACLE: return new OracleAuditTrafficSource();
            default: throw new UnsupportedOperationException(
                    "该引擎的语句流捕获尚未接入: " + eng.wireName());
        }
    }

    /**
     * 建立本次运行要用的 manifest。默认是全新录制；
     * 子类可覆盖成"续录"（沿用原时间轴 + 记一条时间轴空洞）。
     */
    protected RecordingManifest openManifest() throws Exception {
        RecordingManifest manifest = TrafficWriter.newManifest(taskId,
                props.getProperty("task.name", taskId), source.fingerprint(),
                wallOf(t0Micros), t0Micros);
        manifest.engine = engine.wireName();
        manifest.captureBackend = source.captureBackend();
        applyFilterToManifest(manifest);
        return manifest;
    }

    protected void applyFilterToManifest(RecordingManifest manifest) {
        manifest.filter.databases.clear();
        manifest.filter.users.clear();
        manifest.filter.classes.clear();
        manifest.filter.databases.addAll(filter.databases());
        manifest.filter.users.addAll(filter.users());
        for (StatementClass c : filter.classes()) manifest.filter.classes.add(c.name());
        manifest.filter.sampleRate = filter.sampleRate();
        manifest.filter.enrich = Boolean.parseBoolean(props.getProperty("traffic.capture.enrich", "false"));
    }

    /** 子类挂钩：启动完成（落源库原始开关值等）。 */
    protected void onStarted() throws Exception {
    }

    /** 子类挂钩：每轮循环末尾（B2 用于体量护栏）。返回 true 表示应当停止。 */
    protected boolean onTick() throws Exception {
        return false;
    }

    protected void loop() throws Exception {
        long idleSleep = longProp("traffic.capture.idle.sleep.ms", 200L);
        while (!stopped.get()) {
            // 活性无条件先刷：源库空闲时也必须刷，否则看门狗会把闲着的任务当僵死杀掉
            metrics.liveness();
            List<TrafficSource.RawStatement> batch = source.poll();
            for (TrafficSource.RawStatement raw : batch) {
                if (stopped.get()) break;
                accept(raw);
            }
            if (!batch.isEmpty()) {
                writer.flush();
            }
            reportMetrics();
            if (onTick()) {
                break;
            }
            if (batch.isEmpty()) {
                Thread.sleep(idleSleep);
            }
        }
    }

    /** 把一条原始语句归一并写入。 */
    protected void accept(TrafficSource.RawStatement raw) throws Exception {
        String cmd = raw.commandType == null ? "" : raw.commandType;

        // 服务端预处理的模板行：参数未绑定，无法执行，整类丢弃。可回放的是它对应的 Execute 行。
        if ("Prepare".equalsIgnoreCase(cmd) || "Close stmt".equalsIgnoreCase(cmd)
                || "Reset stmt".equalsIgnoreCase(cmd) || "Long Data".equalsIgnoreCase(cmd)) {
            return;
        }

        if ("Connect".equalsIgnoreCase(cmd)) {
            schemaTracker.onConnect(raw.threadId, raw.argument);
            emit(raw, TrafficRecord.CMD_CONNECT, null, null, false);
            return;
        }
        if ("Quit".equalsIgnoreCase(cmd)) {
            emit(raw, TrafficRecord.CMD_QUIT, null, null, false);
            schemaTracker.onQuit(raw.threadId);
            return;
        }
        if ("Init DB".equalsIgnoreCase(cmd)) {
            schemaTracker.onInitDb(raw.threadId, raw.argument);
            // Init DB 是协议层操作，argument 是<b>裸库名</b>而不是 SQL。
            // 原样存下来的话，回放端把 "trf_b3" 当语句执行 → 1064 语法错。
            // 存成等价的可执行形态，录制文件才是"一串可以照着跑的语句"。
            String db = raw.argument == null ? "" : raw.argument.trim();
            String sql = db.isEmpty() ? null : "USE `" + db.replace("`", "``") + "`";
            emit(raw, TrafficRecord.CMD_INIT_DB, StatementClass.USE, sql, false);
            return;
        }

        boolean isExecute = "Execute".equalsIgnoreCase(cmd);
        if (!isExecute && !"Query".equalsIgnoreCase(cmd)) {
            return;     // Statistics / Ping / Debug / Field List 等：不是可回放的语句
        }

        String sql = raw.argument;
        if (sql == null || sql.isBlank()) {
            return;
        }
        // SQL 语法预处理的三行噪声（PREPARE 的文本被 MySQL 抹成 "..."、EXECUTE/DEALLOCATE
        // 引用的句柄在目标库不存在）。可回放的只有 Execute 命令类型那一行。
        //
        // <b>只对 MySQL 成立</b>：PG 与 Oracle 的 SQL 级 PREPARE/EXECUTE 在日志/审计里
        // 是完整原文，且 PREPARE 本身也会被录下来、回放时在同一条会话上先执行——
        // 照 MySQL 的规矩丢掉它们，等于把这类语句整批删掉。
        if (engine == TrafficEngine.MYSQL && !isExecute
                && StatementClassifier.isPreparedStatementNoise(sql)) {
            return;
        }

        schemaTracker.onQuery(raw.threadId, sql);
        StatementClass k = StatementClassifier.classify(engine, sql);
        emit(raw, isExecute ? TrafficRecord.CMD_EXECUTE : TrafficRecord.CMD_QUERY,
                k, sql, StatementClassifier.isRedacted(sql));
    }

    private void emit(TrafficSource.RawStatement raw, String cmd,
                      StatementClass k, String sql, boolean redacted) throws Exception {
        if (!filter.sessionSampled(raw.threadId)) {
            writer.manifest().stats.filtered++;
            return;
        }
        // 库名的来源按引擎不同：PG 的每行日志自带 dbname、Oracle 自带 schema，
        // 它们直接给在 raw.database 上；MySQL 没有，只能靠 SessionSchemaTracker 推导。
        String db = raw.database != null && !raw.database.isEmpty()
                ? raw.database : schemaTracker.schemaOf(raw.threadId);
        if (k != null && !filter.accept(k, db, raw.userHost)) {
            writer.manifest().stats.filtered++;
            return;
        }
        TrafficRecord r = new TrafficRecord();
        r.n = ++seq;
        r.t = raw.epochMicros - t0Micros;
        if (r.t < 0) r.t = 0;       // 开日志与取原点之间的极窄竞态，钳到 0 而不是记成负偏移
        r.s = raw.threadId;
        r.c = cmd;
        r.k = k;
        r.db = db;
        r.u = raw.userHost;
        r.q = sql;
        r.b = raw.binds;
        r.sn = raw.schema;
        r.rd = redacted || raw.redacted;
        // PG 的 SQLSTATE 与 Oracle 的 RETURN_CODE 都是日志/审计里自带的，
        // 不像 MySQL 那样要另开 performance_schema 富化通道
        if (raw.errorCode != 0 || raw.sqlState != null || raw.durationUs >= 0) {
            r.hasEnrich = true;
            r.errno = raw.errorCode;
            r.state = raw.sqlState;
            r.us = Math.max(0L, raw.durationUs);
        }
        writer.write(r);
    }

    protected void reportMetrics() {
        long now = System.currentTimeMillis();
        RecordingManifest.Stats st = writer.manifest().stats;
        metrics.set("traffic_capture_records", st.total);
        metrics.set("traffic_capture_bytes", writer.manifest().totalBytes());
        metrics.set("traffic_srclog_backlog", source.backlog());
        metrics.set("traffic_capture_sessions", schemaTracker.activeSessions());
        if (now - lastRateAt >= 5000L) {
            long delta = st.total - lastRateRecords;
            long rate = (now - lastRateAt) > 0 ? delta * 1000L / (now - lastRateAt) : 0L;
            metrics.set("traffic_capture_rate", rate);
            lastRateAt = now;
            lastRateRecords = st.total;
        }
    }

    protected void shutdown() {
        RecordingManifest m = writer != null ? writer.manifest() : null;
        if (m != null && schemaTracker != null) {
            m.stats.sessions = schemaTracker.activeSessions();
            m.stats.maxConcurrentSessions = schemaTracker.peakConcurrentSessions();
        }
        try {
            if (writer != null) {
                writer.seal(wallOf(nowMicros()), System.currentTimeMillis() - startedAtMs);
            }
        } catch (Exception e) {
            logger.error("录制封口失败", e);
        }
        // 顺序要紧：先封口再关 source。关 source 会还原源库开关，
        // 万一还原抛异常，录制文件至少已经是完整可用的。
        if (source != null) {
            source.close();
        }
        writeResultMarker();
        logger.info("流量复制已停止: 共 {} 条", seq);
    }

    /**
     * 收工标记。agent 靠它区分两种"进程退出了"：
     * 到量自动封口（任务应转 COMPLETED）与被人停掉/出错（不该转 COMPLETED）。
     * 光看退出码分不出来——两种都是 0。
     */
    protected void writeResultMarker() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("outcome", resultOutcome());
        p.setProperty("reason", resultReason() == null ? "" : resultReason());
        p.setProperty("records", String.valueOf(seq));
        p.setProperty("sealed", String.valueOf(writer != null && writer.manifest().sealed));
        try {
            com.migration.common.io.AtomicFileWriter.writeProperties(
                    new File(recordingDir, RESULT_MARKER), p, "流量复制收工标记");
        } catch (Exception e) {
            logger.warn("收工标记写出失败: {}", e.getMessage());
        }
    }

    /** 录制目录下的收工标记文件名。 */
    public static final String RESULT_MARKER = "capture_result.properties";

    protected String resultOutcome() {
        return "STOPPED";
    }

    protected String resultReason() {
        return null;
    }

    public void stop() {
        stopped.set(true);
    }

    protected long nowMicros() {
        return t0Micros + (System.currentTimeMillis() - startedAtMs) * 1000L;
    }

    protected static String wallOf(long epochMicros) {
        return ZonedDateTime.ofInstant(
                        Instant.ofEpochSecond(epochMicros / 1_000_000L, (epochMicros % 1_000_000L) * 1000L),
                        ZoneId.systemDefault())
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    protected long longProp(String key, long def) {
        try {
            String v = props.getProperty(key);
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
