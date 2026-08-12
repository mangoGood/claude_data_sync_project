package com.migration.capture;

import com.migration.common.AbstractCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class PostgresWalCapture extends AbstractCapture<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(PostgresWalCapture.class);

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';

    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private String startLsn;
    private String outputDir;
    private String taskId;
    private String slotName;
    private String publicationName;

    private Connection conn;
    private PGReplicationStream replicationStream;
    private BufferedWriter writer;
    private final AtomicLong eventCounter = new AtomicLong(0);
    private final AtomicLong fileCounter = new AtomicLong(0);
    private long maxEventsPerFile = 10000;
    private long currentFileEvents = 0;

    private volatile String currentLsn;
    private volatile long currentLsnNumeric;
    /** 本次启动是否从已落盘 LSN 续传（false 表示首次启动，用 checkpoint 的起始 LSN）。 */
    private boolean resumedFromPersisted;
    /** 复制槽保留状态的在线巡检：槽被删/被判 lost 时重连不会报错，只会静默丢数据，必须主动看。 */
    private boolean retentionCheckEnabled = true;
    private long retentionCheckIntervalMs = 60000;
    private volatile long lastRetentionCheckMs = 0;

    /**
     * WAL 流从什么时候开始不可用（0 表示正常）。
     *
     * <p>{@code capture_liveness} 是<b>进程级</b>心跳（AbstractCapture 里一个只看 running 的线程），
     * 复制线程死了或一直重连不上时它照刷不误，看门狗判健康 —— "进程活着、一个事件也没捕到"
     * 正是这条链路的盲区。所以这里额外盯住流本身，卡住够久就写 error_status。
     */
    private volatile long streamBrokenSinceMs = 0;
    private long streamDownReportMs = 300000;
    private volatile boolean streamDownReported = false;
    /** 自己写进 error_status 的那条中断告警原文，恢复时按原文比对再撤销。 */
    private volatile String streamDownStatusLine;

    /** 源库时钟与本机时钟的偏移；事件时间戳按它折算，否则延迟指标里混着两台机器的时钟差。 */
    private final com.migration.common.clock.SourceClockOffset sourceClock =
            new com.migration.common.clock.SourceClockOffset();
    /** 空闲心跳间隔：源库没有变更时，也要有一个**源端时钟**的时间基准供下游算延迟。 */
    private long idleHeartbeatMs = 5000;
    private volatile long lastIdleHeartbeatMs = 0;

    // 背压控制：extract 通过信号文件通知 capture 暂停/恢复
    private volatile boolean backpressurePaused = false;
    private String backpressureSignalPath;

    // 双向同步/环路防护（active-active 双活）：与 MySQL 侧同一套 origin 标记机制。
    // apply 端在每个应用事务里先写 __sync_origin 标记行，capture 端读到带标记的事务即判定
    // "复制而来"，跳过其业务数据事件、不回传，打断 A→B→A 回环。
    private boolean bidirectionalEnabled;
    private com.migration.common.bidi.BidiLoopGuard loopGuard;

    @Override
    protected void doInitialize() throws Exception {
        host = props.getProperty("source.db.host", "localhost");
        port = Integer.parseInt(props.getProperty("source.db.port", "5432"));
        database = props.getProperty("source.db.database", "postgres");
        user = props.getProperty("source.db.username", "postgres");
        password = props.getProperty("source.db.password", "");
        outputDir = props.getProperty("capture.output.dir", "binlog_output");
        // 位点来源优先级：已落盘 LSN > config.properties 的起始 LSN。后者是任务启动时写死一次的，
        // 崩溃重启若还认它，复制槽会被要求从任务最初的 LSN 重发，整段 WAL 重放（见 CapturePositionStore）。
        java.util.Properties persisted = com.migration.common.position.CapturePositionStore.preferPersisted(props)
                ? com.migration.common.position.CapturePositionStore.load(outputDir)
                : new java.util.Properties();
        startLsn = com.migration.common.position.CapturePositionStore.prefer(
                persisted, "wal.lsn", props.getProperty("capture.wal.lsn", ""), "WAL LSN");
        taskId = props.getProperty("task.id", "unknown");
        retentionCheckEnabled = Boolean.parseBoolean(
                props.getProperty("capture.position.health.enabled", "true"));
        retentionCheckIntervalMs = Long.parseLong(
                props.getProperty("capture.position.health.interval.ms", "60000"));
        streamDownReportMs = Long.parseLong(
                props.getProperty("capture.stream.down.report.ms", "300000"));
        idleHeartbeatMs = Long.parseLong(
                props.getProperty("capture.idle.heartbeat.ms", "5000"));
        maxEventsPerFile = Long.parseLong(props.getProperty("capture.max.events.per.file", "10000"));
        slotName = props.getProperty("capture.wal.slot.name", "migration_slot_" + taskId.replaceAll("[^a-z0-9_]", "_"));
        publicationName = props.getProperty("capture.wal.publication.name", "migration_pub_" + taskId.replaceAll("[^a-z0-9_]", "_"));

        backpressureSignalPath = "files/" + taskId + "/backpressure.signal";

        bidirectionalEnabled = com.migration.common.bidi.BidiConstants.isEnabled(props);
        loopGuard = new com.migration.common.bidi.BidiLoopGuard(bidirectionalEnabled);
        if (bidirectionalEnabled) {
            logger.info("PostgreSQL WAL Capture: 双向同步/环路防护已启用，将跳过带 {} 标记的复制事务",
                    com.migration.common.bidi.BidiConstants.MARKER_TABLE);
        }

        if (startLsn.isEmpty()) {
            startLsn = null;
        }
        resumedFromPersisted = !persisted.isEmpty();

        verifyResumePositionAvailable();

        logger.info("PostgreSQL WAL Capture initialized - host={}:{} database={} user={} outputDir={} taskId={} startLsn={} resumed={} slotName={}",
                host, port, database, user, outputDir, taskId, startLsn, resumedFromPersisted, slotName);
    }

    /**
     * 续传 LSN 是否还被复制槽保留着。
     *
     * <p>逻辑复制槽是 WAL 保留的唯一凭据：槽被删掉（或被下面 {@link #ensureReplicationSlot} 的
     * drop/recreate 路径重建过），{@code restart_lsn} 之前的 WAL 就已经被回收。此时拿旧 LSN
     * 发起 START_REPLICATION，服务端<b>不会报错</b>，而是从槽当前位置开始发——中间那段变更
     * 静默消失。这比报错危险得多，所以这里主动比对、发现即判失败。
     *
     * <p>只在"从已落盘位点续传"时检查：首次启动时槽还没建，startLsn 来自 checkpoint，属正常。
     */
    /**
     * 源库连接的 TLS 参数。
     *
     * <p>本类里连接分两类：普通查询连接、以及 {@code replication=database} 的<b>逻辑复制槽</b>连接。
     * 后者才是搬业务数据的那条，两类都必须带上——只加密前者的话，任务看着"配了 TLS"，
     * 真正的变更流仍然明文。
     *
     * <p>PG 的 {@code sslrootcert} 只吃 PEM，所以这里取的是 {@code pgUrlParams()}
     * （它用 PEM 形态的 CA 与 PKCS8 DER 私钥），不是 MySQL 那套 p12。
     */
    private String sslParams() {
        return com.migration.common.ssl.SslMaterial.from(props, "source").pgUrlParams();
    }

    private void verifyResumePositionAvailable() {
        if (!resumedFromPersisted || startLsn == null
                || !Boolean.parseBoolean(props.getProperty("capture.position.precheck.enabled", "true"))) {
            return;
        }
        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
        try (Connection checkConn = DriverManager.getConnection(url, user, password);
             Statement stmt = checkConn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT restart_lsn, restart_lsn > '" + startLsn + "'::pg_lsn AS lost "
                            + "FROM pg_replication_slots WHERE slot_name = '" + slotName + "'");
            if (!rs.next()) {
                failPositionUnavailable("复制槽 " + slotName + " 已不存在，续传 LSN " + startLsn
                        + " 之后的 WAL 未被保留");
                return;
            }
            if (rs.getBoolean("lost")) {
                failPositionUnavailable("复制槽 " + slotName + " 的 restart_lsn=" + rs.getString("restart_lsn")
                        + " 已越过续传 LSN " + startLsn + "，中间 WAL 已被回收");
            }
        } catch (CapturePositionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            logger.warn("续传 LSN 可用性预检跳过（查询失败）: {}", e.getMessage());
        }
    }

    /**
     * 源库没有变更时，往 {@code .cap} 写一条带**源端时钟**的心跳。
     *
     * <p>为什么需要它：extract 也有一条兜底心跳，但那条用的是**本机时钟**，
     * 下游拿它算出来的延迟恒等于 0 —— capture 卡死时反而显示"延迟极低"。
     * 这里的心跳时间戳取自源库（{@code now()}），并按测得的偏移折算到本机时钟域，
     * 因此空闲期的延迟数字是真的在量"心跳穿过 capture→extract→apply 要多久"。
     *
     * <p>口径要说清楚：它量的是**捕获端往下**这一段，不含"源库提交→capture 读到"
     * （那一段的落后量由复制槽的 WAL 字节差反映，见 {@link #checkRetentionQuietly}）。
     * 有真实变更时事件自带提交时刻，那才是全程延迟。
     */
    private void writeIdleHeartbeatIfNeeded() {
        if (idleHeartbeatMs <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastIdleHeartbeatMs < idleHeartbeatMs) {
            return;
        }
        lastIdleHeartbeatMs = now;
        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
        long sourceNowMs;
        try (Connection probe = DriverManager.getConnection(url, user, password);
             Statement stmt = probe.createStatement()) {
            long before = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint")) {
                if (!rs.next()) {
                    return;
                }
                sourceNowMs = rs.getLong(1);
            }
            sourceClock.observe(sourceNowMs, before, System.currentTimeMillis());
        } catch (Exception e) {
            logger.debug("空闲心跳取源库时间失败: {}", e.getMessage());
            return;
        }
        if (sourceClock.isSuspicious(5000)) {
            logger.warn("源库与本机时钟相差 {} ms，延迟指标已按该偏移折算；请检查两端 NTP",
                    sourceClock.offsetMs());
        }
        try {
            synchronized (this) {
                if (writer == null) {
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("SYNC_HEARTBEAT").append(FIELD_SEP);
                sb.append(currentLsn != null ? currentLsn : "0/0").append(FIELD_SEP);
                sb.append(currentLsnNumeric).append(FIELD_SEP);
                sb.append(sourceClock.toLocal(sourceNowMs)).append(FIELD_SEP);
                sb.append(0).append(FIELD_SEP);
                sb.append("source_clock");
                sb.append(RECORD_SEP);
                writer.write(sb.toString());
                writer.flush();
            }
        } catch (IOException e) {
            logger.debug("写空闲心跳失败: {}", e.getMessage());
        }
    }

    /** 流卡住超过阈值就上报，别让"进程活着但一个事件也捕不到"一直装作健康。 */
    private void reportStreamDownIfStuck() {
        if (streamDownReported || streamBrokenSinceMs == 0) {
            return;
        }
        if (System.currentTimeMillis() - streamBrokenSinceMs < streamDownReportMs) {
            return;
        }
        String detail = "WAL 复制流已中断超过 " + (streamDownReportMs / 1000)
                + " 秒且未能恢复，期间没有捕获到任何变更";
        logger.error("{}（进程仍在运行，活性文件不能反映这种状态）", detail);
        streamDownStatusLine = errorStatusLine("E3027", detail);
        streamDownReported = true;
        com.migration.common.io.AtomicFileWriter.writeStringQuietly(
                errorStatusFile(), streamDownStatusLine);
    }

    /**
     * 流恢复了就把自己写的那条中断告警撤掉，否则任务会一直挂着 FAILED。
     *
     * <p>只在文件内容<b>逐字等于</b>自己写的那条时才删：error_status 是 capture / extract /
     * increment 共用的一个文件，别人写了真错误的话不能被这里顺手抹掉。
     */
    private void clearStreamDownStatus() {
        if (!streamDownReported) {
            return;
        }
        streamDownReported = false;
        String mine = streamDownStatusLine;
        streamDownStatusLine = null;
        File file = errorStatusFile();
        try {
            if (mine != null && file.isFile()
                    && mine.equals(new String(java.nio.file.Files.readAllBytes(file.toPath()),
                    StandardCharsets.UTF_8))) {
                if (file.delete()) {
                    logger.info("WAL 复制流已恢复，撤销此前的中断告警");
                }
            }
        } catch (Exception e) {
            logger.debug("撤销中断告警失败: {}", e.getMessage());
        }
    }

    private void failPositionUnavailable(String detail) {
        logger.error("{}（本任务需重新初始化全量）", detail);
        writeCaptureErrorStatus("E3006", detail + "；需重新初始化全量同步");
        throw new CapturePositionUnavailableException(detail);
    }

    /** 起始位点已被源端回收，无法自动恢复。 */
    static class CapturePositionUnavailableException extends RuntimeException {
        CapturePositionUnavailableException(String message) {
            super(message);
        }
    }

    /** 写 {@code binlog_output/error_status}（格式同 increment 端），agent 轮询到即上报 FAILED。 */
    private void writeCaptureErrorStatus(String errorCode, String message) {
        com.migration.common.io.AtomicFileWriter.writeStringQuietly(
                errorStatusFile(), errorStatusLine(errorCode, message));
    }

    private File errorStatusFile() {
        File dir = new File(outputDir);
        if (!dir.exists()) dir.mkdirs();
        return new File(dir, "error_status");
    }

    private String errorStatusLine(String errorCode, String message) {
        return System.currentTimeMillis() + "|" + errorCode + "|-1|"
                + message.replace("|", "/") + "|capture\n";
    }

    @Override
    protected void doStart() throws Exception {
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        openNewOutputFile();

        Class.forName("org.postgresql.Driver");

        ensureReplicationSlot();
        ensurePublication();

        String url = String.format("jdbc:postgresql://%s:%d/%s?replication=database&stringtype=unspecified&" + sslParams(),
                host, port, database);
        Properties connProps = new Properties();
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        connProps.setProperty("preferQueryMode", "simple");
        connProps.setProperty("assumeMinServerVersion", "14");

        conn = DriverManager.getConnection(url, connProps);
        conn.setAutoCommit(false);

        logger.info("Connected to PostgreSQL for logical replication on database: {}", database);

        PGConnection pgConn = conn.unwrap(PGConnection.class);

        org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder builder = pgConn
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publicationName)
                .withStatusInterval(1000, java.util.concurrent.TimeUnit.MILLISECONDS);

        if (startLsn != null && !startLsn.isEmpty()) {
            try {
                LogSequenceNumber lsn = LogSequenceNumber.valueOf(startLsn);
                builder.withStartPosition(lsn);
                logger.info("Starting WAL replication from LSN: {}", startLsn);
            } catch (Exception e) {
                logger.warn("Failed to parse start LSN '{}', starting from current position: {}", startLsn, e.getMessage());
            }
        } else {
            logger.info("Starting WAL replication from current position");
        }

        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                replicationStream = builder.start();
                logger.info("WAL replication stream started on attempt {}", attempt);
                break;
            } catch (org.postgresql.util.PSQLException e) {
                if (e.getMessage() != null && e.getMessage().contains("is active for PID") && attempt < maxRetries) {
                    logger.warn("Replication slot is active (attempt {}/{}), will retry after cleanup: {}", 
                        attempt, maxRetries, e.getMessage());
                    try {
                        conn.close();
                    } catch (Exception ce) {
                        logger.warn("Error closing connection: {}", ce.getMessage());
                    }
                    Thread.sleep(2000);
                    ensureReplicationSlot();
                    String retryUrl = String.format("jdbc:postgresql://%s:%d/%s?replication=database&stringtype=unspecified&" + sslParams(),
                            host, port, database);
                    conn = DriverManager.getConnection(retryUrl, connProps);
                    conn.setAutoCommit(false);
                    PGConnection retryPgConn = conn.unwrap(PGConnection.class);
                    builder = retryPgConn
                            .getReplicationAPI()
                            .replicationStream()
                            .logical()
                            .withSlotName(slotName)
                            .withSlotOption("proto_version", 1)
                            .withSlotOption("publication_names", publicationName)
                            .withStatusInterval(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (startLsn != null && !startLsn.isEmpty()) {
                        try {
                            LogSequenceNumber lsn = LogSequenceNumber.valueOf(startLsn);
                            builder.withStartPosition(lsn);
                        } catch (Exception le) {
                            logger.warn("Failed to parse start LSN '{}': {}", startLsn, le.getMessage());
                        }
                    }
                } else {
                    throw e;
                }
            }
        }

        Thread replicateThread = new Thread(() -> {
            int consecutiveErrors = 0;
            long lastDataTime = System.currentTimeMillis();
            long lastLivenessCheck = System.currentTimeMillis();
            while (running) {
                try {
                    if (replicationStream == null) {
                        logger.warn("WAL replication stream is null, attempting to reconnect...");
                        reconnectReplication();
                        consecutiveErrors = 0;
                        lastDataTime = System.currentTimeMillis();
                    }

                    long now = System.currentTimeMillis();
                    if (now - lastLivenessCheck > 30000) {
                        lastLivenessCheck = now;
                        if (!isReplicationSlotActive()) {
                            logger.warn("Replication slot is not active in PG, forcing reconnect...");
                            replicationStream = null;
                            continue;
                        }
                    }

                    if (now - lastDataTime > 60000) {
                        logger.warn("No WAL data received for 60s, checking connection liveness...");
                        if (!isReplicationSlotActive()) {
                            logger.warn("Replication slot not active, forcing reconnect...");
                            replicationStream = null;
                            continue;
                        }
                        lastDataTime = now;
                    }

                    // 保留期巡检必须在这里、而不是只在收到消息之后做：它要防的正是
                    // "槽没了 → 收不到数据"，那时 processWalMessage 一次都不会被调用，
                    // 巡检也就永远不跑，告警在最需要它的时候必然缺席。方法自身按间隔节流。
                    checkRetentionQuietly();
                    writeIdleHeartbeatIfNeeded();

                    ByteBuffer msgBuffer = replicationStream.readPending();
                    if (msgBuffer != null) {
                        processWalMessage(msgBuffer);
                        consecutiveErrors = 0;
                        lastDataTime = System.currentTimeMillis();
                        streamBrokenSinceMs = 0;
                        clearStreamDownStatus();
                    } else {
                        Thread.sleep(10);
                    }
                } catch (CapturePositionUnavailableException e) {
                    // 位点已不可用（槽被删/被判 lost）：重试只会拿到一个从新位置开始的槽，
                    // 中间的变更再也拿不回来。停下来上报，等人重做全量
                    logger.error("WAL 位点不可用，停止捕获: {}", e.getMessage());
                    running = false;
                    break;
                } catch (Exception e) {
                    consecutiveErrors++;
                    if (streamBrokenSinceMs == 0) {
                        streamBrokenSinceMs = System.currentTimeMillis();
                    }
                    reportStreamDownIfStuck();
                    if (running) {
                        logger.error("Error in WAL replication stream (consecutive: {}): {}", consecutiveErrors, e.getMessage());
                        if (consecutiveErrors >= 5) {
                            logger.warn("Too many consecutive errors, forcing reconnect...");
                            try {
                                replicationStream = null;
                                reconnectReplication();
                                consecutiveErrors = 0;
                                lastDataTime = System.currentTimeMillis();
                            } catch (Exception re) {
                                logger.error("Failed to reconnect: {}", re.getMessage());
                            }
                        } else {
                            try {
                                Thread.sleep(1000 * consecutiveErrors);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }
            }
            try {
                if (replicationStream != null) replicationStream.close();
            } catch (Exception e) {
                logger.warn("Error closing replication stream: {}", e.getMessage());
            }
        }, "WAL-Replication-" + taskId);
        replicateThread.setDaemon(true);
        replicateThread.start();

        startBackpressureMonitor();

        logger.info("WAL replication stream started for slot: {}", slotName);
    }

    /**
     * 检查背压信号文件，更新 backpressurePaused 状态。
     * extract 进程在 THL 积压时写入 PAUSE 信号，积压解除后写入 RESUME。
     */
    private void checkBackpressureSignal() {
        if (backpressureSignalPath == null) return;
        File signalFile = new File(backpressureSignalPath);
        if (!signalFile.exists()) {
            backpressurePaused = false;
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(signalFile))) {
            String firstLine = reader.readLine();
            boolean shouldPause = firstLine != null && "PAUSE".equalsIgnoreCase(firstLine.trim());
            if (shouldPause != backpressurePaused) {
                backpressurePaused = shouldPause;
                if (shouldPause) {
                    logger.warn("收到背压暂停信号，暂停 WAL 事件处理");
                } else {
                    logger.info("收到背压恢复信号，恢复 WAL 事件处理");
                }
            }
        } catch (IOException e) {
            logger.debug("读取背压信号文件失败: {}", e.getMessage());
        }
    }

    /**
     * 启动后台线程定期检查背压信号，确保无事件时也能及时响应暂停/恢复。
     */
    private void startBackpressureMonitor() {
        Thread monitor = new Thread(() -> {
            while (running) {
                try {
                    checkBackpressureSignal();
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.debug("背压监控异常: {}", e.getMessage());
                }
            }
        }, "Backpressure-Monitor-" + taskId);
        monitor.setDaemon(true);
        monitor.start();
        logger.info("背压监控线程已启动, taskId={}", taskId);
    }

    private boolean isReplicationSlotActive() {
        try {
            String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
            try (Connection checkConn = DriverManager.getConnection(url, user, password)) {
                String connCatalog = checkConn.getCatalog();
                logger.info("Liveness check connected to: host={}:{} db={} connCatalog={}", host, port, database, connCatalog);
                try (Statement stmt = checkConn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT slot_name, active, active_pid, database FROM pg_replication_slots")) {
                    while (rs.next()) {
                        String sn = rs.getString("slot_name");
                        boolean act = rs.getBoolean("active");
                        int apid = rs.getInt("active_pid");
                        String db = rs.getString("database");
                        logger.info("  Found slot: name={} active={} activePid={} database={}", sn, act, apid, db);
                    }
                }
                try (Statement stmt = checkConn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT slot_name, active, active_pid FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                    if (rs.next()) {
                        boolean active = rs.getBoolean("active");
                        int activePid = rs.getInt("active_pid");
                        logger.info("Liveness check: slot '{}' active={}, activePid={}", slotName, active, activePid);
                        return active;
                    }
                    logger.warn("Liveness check: slot '{}' not found in pg_replication_slots", slotName);
                    return false;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to check replication slot liveness: {}", e.getMessage());
            return false;
        }
    }

    private synchronized void reconnectReplication() throws Exception {
        logger.info("Reconnecting WAL replication stream...");

        try {
            if (replicationStream != null) {
                try { replicationStream.close(); } catch (Exception e) { /* ignore */ }
            }
        } catch (Exception e) { /* ignore */ }

        try {
            if (conn != null && !conn.isClosed()) {
                try { conn.close(); } catch (Exception e) { /* ignore */ }
            }
        } catch (Exception e) { /* ignore */ }

        ensureReplicationSlot();

        String url = String.format("jdbc:postgresql://%s:%d/%s?replication=database&stringtype=unspecified&" + sslParams(),
                host, port, database);
        Properties connProps = new Properties();
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        connProps.setProperty("preferQueryMode", "simple");
        connProps.setProperty("assumeMinServerVersion", "14");

        conn = DriverManager.getConnection(url, connProps);
        conn.setAutoCommit(false);

        PGConnection pgConn = conn.unwrap(PGConnection.class);

        org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder builder = pgConn
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publicationName)
                .withStatusInterval(1000, java.util.concurrent.TimeUnit.MILLISECONDS);

        if (currentLsn != null && !currentLsn.isEmpty()) {
            try {
                LogSequenceNumber lsn = LogSequenceNumber.valueOf(currentLsn);
                builder.withStartPosition(lsn);
                logger.info("Reconnecting WAL replication from last known LSN: {}", currentLsn);
            } catch (Exception e) {
                logger.warn("Failed to parse current LSN '{}', starting from current position: {}", currentLsn, e.getMessage());
            }
        }

        replicationStream = builder.start();
        logger.info("WAL replication stream reconnected successfully from LSN: {}", currentLsn);
    }

    private void ensureReplicationSlot() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
        try (Connection checkConn = DriverManager.getConnection(url, user, password);
             Statement stmt = checkConn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                "SELECT slot_name, active, active_pid FROM pg_replication_slots WHERE slot_name = '" + slotName + "'");
            if (rs.next()) {
                boolean active = rs.getBoolean("active");
                int activePid = rs.getInt("active_pid");
                if (active && activePid > 0) {
                    // 崩溃重启后旧的 walsender 后端可能还没被服务端回收，槽显示 active。
                    // 只需踢掉那个后端等它释放——**绝不能顺手 drop 掉槽**：槽是 WAL 保留的唯一凭据，
                    // 删了再建，restart_lsn 之前的 WAL 立刻被回收，随后拿旧 LSN 续传时服务端不报错、
                    // 直接从新槽位置开始发，中间的变更静默消失。这是"位点在、数据没了"的典型丢数据路径。
                    logger.warn("Replication slot '{}' is active for PID {}, terminating backend (slot is kept to preserve WAL retention)",
                            slotName, activePid);
                    try {
                        stmt.execute("SELECT pg_terminate_backend(" + activePid + ")");
                    } catch (Exception e) {
                        logger.warn("Failed to terminate backend PID {}: {}", activePid, e.getMessage());
                    }
                    if (waitForSlotInactive(stmt)) {
                        logger.info("Replication slot '{}' released, resuming on the existing slot", slotName);
                    } else if (resumedFromPersisted) {
                        // 槽始终不释放且我们有续传位点：重建槽必然丢数据，宁可失败让人来处理。
                        failPositionUnavailable("复制槽 " + slotName + " 长时间未释放，无法在不丢数据的前提下续传");
                    } else {
                        logger.warn("Replication slot '{}' still active, recreating (no resume position to protect)", slotName);
                        try {
                            stmt.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
                            stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                            logger.info("Recreated replication slot '{}' with pgoutput plugin", slotName);
                        } catch (Exception e) {
                            logger.warn("Failed to drop/recreate slot '{}': {}", slotName, e.getMessage());
                        }
                    }
                } else {
                    logger.info("Replication slot '{}' already exists (inactive)", slotName);
                }
            } else if (hasResumePosition()) {
                // 槽是 WAL 保留的唯一凭据。到这一步说明它在运行中被删掉了（或被
                // max_slot_wal_keep_size 判成 lost 后清理掉）。这里若顺手建一个新槽，
                // 服务端不会报错，而是从新槽的位置开始发 —— 中间那段变更静默消失。
                // reconnectReplication() 每次重连都会走到这里，所以这条分支必须堵死。
                failPositionUnavailable("复制槽 " + slotName + " 已不存在（运行中被删除或被判 lost），"
                        + "已捕获到的位点 " + currentResumeLsn() + " 之后的 WAL 无法保证还在");
            } else {
                stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                logger.info("Created replication slot '{}' with pgoutput plugin", slotName);
            }
        }
    }

    /** 是否已经有"必须被保留住"的位点：续传位点，或本进程已经读到过的位点。 */
    private boolean hasResumePosition() {
        return resumedFromPersisted || currentLsn != null;
    }

    private String currentResumeLsn() {
        return currentLsn != null ? currentLsn : String.valueOf(startLsn);
    }

    /** 轮询等待槽被释放（被踢掉的 walsender 后端退出通常在数百毫秒内），最多 ~10s。 */
    private boolean waitForSlotInactive(Statement stmt) throws Exception {
        for (int i = 0; i < 20; i++) {
            Thread.sleep(500);
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT active FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                if (!rs.next() || !rs.getBoolean("active")) {
                    return true;
                }
            }
        }
        return false;
    }

    private void ensurePublication() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
        try (Connection checkConn = DriverManager.getConnection(url, user, password);
             Statement stmt = checkConn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                "SELECT pubname FROM pg_publication WHERE pubname = '" + publicationName + "'");
            if (rs.next()) {
                logger.info("Publication '{}' already exists", publicationName);
            } else {
                stmt.execute("CREATE PUBLICATION \"" + publicationName + "\" FOR ALL TABLES");
                logger.info("Created publication '{}' for all tables", publicationName);
            }
        }
    }

    private void processWalMessage(ByteBuffer msgBuffer) {
        if (!running) return;

        // 背压检查：如果 extract 发出暂停信号，则等待恢复
        if (backpressurePaused) {
            try {
                while (backpressurePaused && running) {
                    Thread.sleep(500);
                    checkBackpressureSignal();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        try {
            LogSequenceNumber receiveLsn = replicationStream.getLastReceiveLSN();
            currentLsn = receiveLsn.asString();
            currentLsnNumeric = receiveLsn.asLong();

            byte[] data = new byte[msgBuffer.remaining()];
            msgBuffer.get(data);

            String walData = new String(data, StandardCharsets.UTF_8);

            String parsedEvent = parsePgoutputMessage(data);
            String eventType;
            String eventDataStr;

            if (parsedEvent != null && !parsedEvent.isEmpty()) {
                eventType = parseWalEventType(parsedEvent);
                if ("WAL_EVENT".equals(eventType)) {
                    eventType = parseWalEventType(walData);
                }
                eventDataStr = parsedEvent;
            } else {
                eventType = parseWalEventType(walData);
                eventDataStr = walData.replace("\n", " ").replace("\r", " ");
            }

            // 事件的源端时间 = 本事务的提交时刻，再按测得的时钟偏移折算到本机时钟域，
            // 这样下游的 `本机 now − 事件时间戳` 就是真实的端到端延迟。
            // 拿不到提交时刻（不在事务中、或解析失败）才退回本机时钟。
            long timestamp = currentTxCommitMs > 0
                    ? sourceClock.toLocal(currentTxCommitMs) : System.currentTimeMillis();
            long xid = 0;

            // 双向同步/环路防护：前向单遍事务标记状态机。与 MySQL 侧一致，但 PG 的
            // extract 依赖 BEGIN/COMMIT 做事务分帧（不像 MySQL bidi 直接丢弃事务控制），
            // 故此处 BEGIN/COMMIT 照常写出，只跳过 __sync_origin 标记事件与被标记事务的业务 DML。
            if (bidirectionalEnabled) {
                if ("BEGIN".equals(eventType) || "COMMIT".equals(eventType)) {
                    loopGuard.onTransactionBoundary();
                } else {
                    boolean isDataEvent = "INSERT".equals(eventType)
                            || "UPDATE".equals(eventType) || "DELETE".equals(eventType);
                    if (isDataEvent) {
                        String tbl = extractPgTableName(eventDataStr);
                        // origin 标记行事件：置位并丢弃（标记表不外传），后续本事务数据将被跳过
                        if (com.migration.common.bidi.BidiConstants.MARKER_TABLE.equals(tbl)) {
                            loopGuard.onOriginMarker();
                            return;
                        }
                        // 当前事务带 origin 标记 → 对端复制而来的写入，跳过、不回传
                        if (loopGuard.shouldSkipReplicatedData()) {
                            return;
                        }
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append(eventType).append(FIELD_SEP);
            sb.append(currentLsn).append(FIELD_SEP);
            sb.append(currentLsnNumeric).append(FIELD_SEP);
            sb.append(timestamp).append(FIELD_SEP);
            sb.append(xid).append(FIELD_SEP);
            sb.append(eventDataStr);
            sb.append(RECORD_SEP);

            writer.write(sb.toString());
            writer.flush();

            replicationStream.setAppliedLSN(receiveLsn);
            replicationStream.setFlushedLSN(receiveLsn);

            long count = eventCounter.incrementAndGet();
            currentFileEvents++;

            if (currentFileEvents >= maxEventsPerFile) {
                rotateOutputFile();
            }

            if (count % 1000 == 0) {
                logger.info("Captured {} WAL events, current LSN: {}", count, currentLsn);
                savePosition();
            }
            checkRetentionQuietly();
        } catch (Exception e) {
            logger.error("Error processing WAL message: {}", e.getMessage(), e);
        }
    }

    private String parsePgoutputMessage(byte[] data) {
        if (data == null || data.length == 0) return "";

        try {
            int offset = 0;
            char msgType = (char) (data[offset] & 0xFF);
            offset++;

            switch (msgType) {
                case 'B':
                    return parseBeginMessage(data, offset);
                case 'C':
                    return parseCommitMessage(data, offset);
                case 'R':
                    return parseRelationMessage(data, offset);
                case 'I':
                    return parseInsertMessage(data, offset);
                case 'U':
                    return parseUpdateMessage(data, offset);
                case 'D':
                    return parseDeleteMessage(data, offset);
                case 'T':
                    return parseTruncateMessage(data, offset);
                case 'O':       // Origin：上游复制来源标记，本身不携带数据
                case 'Y':       // Type：自定义类型的定义，值仍以文本形态随行事件下发
                    return "";
                default:
                    // 黑名单式放行是丢数据的老路：不认识的消息类型至少要留下痕迹，
                    // 而不是当成"没这回事"。PG 每个大版本都在加消息类型
                    logger.warn("pgoutput 出现未处理的消息类型 '{}'（{}），该消息未被下发",
                            msgType, (int) msgType);
                    return "";
            }
        } catch (Exception e) {
            logger.debug("Error parsing pgoutput message: {}", e.getMessage());
            return "";
        }
    }

    /**
     * TRUNCATE('T') 消息。旧实现直接丢弃 —— 源端 {@code TRUNCATE} 在目标端不发生，
     * 之后源端重新灌入的数据靠 upsert 合进旧行，两端从此不一致且没有任何告警。
     *
     * <p>报文格式：关系数(4B)、选项位(1B，1=CASCADE、2=RESTART IDENTITY)、关系 OID 数组。
     * 下发成一条 DDL 语句交给既有的 DDL 通道（库名/表名映射、方言翻译都在那条路上）。
     * 一条语句 TRUNCATE 多张表是 PG 的语法，MySQL 目标端只支持单表 —— 那种情况下
     * 目标端会明确报错，而不是像改造前那样悄悄什么都不做。
     */
    private String parseTruncateMessage(byte[] data, int offset) {
        try {
            Cursor c = new Cursor(data, offset);
            int relationCount = c.int32();
            int options = c.int8();
            List<String> tables = new ArrayList<>(relationCount);
            String schema = null;
            for (int i = 0; i < relationCount; i++) {
                long relationId = c.int32() & 0xFFFFFFFFL;
                TableMeta meta = metaOf(relationId);
                if (schema == null) {
                    schema = meta.schema;
                }
                tables.add("\"" + meta.schema + "\".\"" + meta.table + "\"");
            }
            if (tables.isEmpty()) {
                return "";
            }
            StringBuilder sql = new StringBuilder("TRUNCATE TABLE ").append(String.join(", ", tables));
            if ((options & 2) != 0) {
                sql.append(" RESTART IDENTITY");
            }
            if ((options & 1) != 0) {
                sql.append(" CASCADE");
            }
            logger.info("捕获 TRUNCATE: {}", sql);
            return "TRUNCATE schema:" + schema + " sql:" + sql;
        } catch (Exception e) {
            logger.warn("解析 TRUNCATE 消息失败: {}", e.getMessage());
            return "";
        }
    }

    private String parseBeginMessage(byte[] data, int offset) {
        try {
            long lsn = readInt64BE(data, offset); offset += 8;
            long commitTime = readInt64BE(data, offset); offset += 8;
            long xid = readInt32BE(data, offset);
            // BEGIN 消息里的这个时间戳就是本事务的**提交时刻**（源库时钟）。
            // 事务内的行事件本身不带时间戳，提交时刻正是它们该有的源端时间。
            currentTxCommitMs = pgTimeToEpochMs(commitTime);
            return "BEGIN lsn=" + lsn + " transaction_id:" + xid;
        } catch (Exception e) {
            return "BEGIN";
        }
    }

    /**
     * PG 的时间戳是"2000-01-01 00:00:00 UTC 起的微秒数"，与 Unix 毫秒差 946684800000。
     */
    private static long pgTimeToEpochMs(long pgMicros) {
        return pgMicros / 1000L + 946684800000L;
    }

    /**
     * 本事务的提交时刻（源库时钟，毫秒）；不在事务中为 0。
     *
     * <p>旧实现给每个事件打的是"capture 读到这条消息的那一刻的本机时钟"，于是
     * 「源库提交 → walsender 投递 → capture 读到」这一整段完全不计入延迟 ——
     * capture 积压得再厉害，面板上的延迟也只反映下游。
     */
    private volatile long currentTxCommitMs = 0;

    private String parseCommitMessage(byte[] data, int offset) {
        try {
            int flags = readInt8(data, offset); offset++;
            long lsn = readInt64BE(data, offset); offset += 8;
            long endLsn = readInt64BE(data, offset); offset += 8;
            long commitTime = readInt64BE(data, offset); offset += 8;
            long xid = readInt32BE(data, offset);
            currentTxCommitMs = pgTimeToEpochMs(commitTime);
            return "COMMIT transaction_id:" + xid;
        } catch (Exception e) {
            return "COMMIT";
        }
    }

    /**
     * Relation('R') 消息：<b>权威</b>的表结构，与行值同处一个流、同一时刻，
     * 且 PG 在关系定义变化后会重发。
     *
     * <p>这是 PG 侧对付 schema 漂移的正解，地位等同 MySQL 的 {@code binlog_row_metadata=FULL}。
     * 旧实现只从这里取了库名表名就把列信息扔了，列名/类型改为回查
     * {@code information_schema} 并永久缓存 —— 源端 {@code DROP COLUMN} 之后，
     * wire 上的 tuple 少一列而缓存还是老的，被删列之后的每一列整体错位一格，
     * 写进目标库的是合法值、看不出异常。
     *
     * <p>报文格式（proto v1）：OID、namespace、relname、replica identity(1B)、列数(2B)，
     * 之后每列是 flags(1B，bit0=属于键)、列名、类型 OID(4B)、typmod(4B)。
     */
    private String parseRelationMessage(byte[] data, int offset) {
        try {
            Cursor c = new Cursor(data, offset);
            long relationId = c.int32();
            String schema = c.cstring();
            if (schema.isEmpty()) {
                schema = "public";
            }
            String table = c.cstring();
            c.int8();                       // replica identity
            int columnCount = c.int16();

            List<String> columns = new ArrayList<>(columnCount);
            List<String> types = new ArrayList<>(columnCount);
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                int flags = c.int8();
                String name = c.cstring();
                long typeOid = c.int32() & 0xFFFFFFFFL;
                c.int32();                  // 类型修饰符（长度/精度），值转换用不到
                columns.add(name);
                types.add(typeNameOf(typeOid));
                if ((flags & 1) != 0) {
                    keys.add(name);
                }
            }

            RelationSchema fresh = new RelationSchema(schema, table, columns, types, keys);
            RelationSchema prev = relationSchemas.put(relationId, fresh);
            if (prev != null && !prev.columns.equals(columns)) {
                logger.info("表 {}.{} 的列定义已变化（源端 DDL），按 Relation 消息更新: {} -> {}",
                        schema, table, prev.columns, columns);
            }
            // 回查源库那条老路径的缓存也一并作废，避免它在回退分支上继续给出旧结构
            String key = schema + "." + table;
            tableColumnsCache.remove(key);
            tableColumnTypesCache.remove(key);
            tablePrimaryKeysCache.remove(key);
            relationIdCache.put(relationId, new String[]{schema, table});

            logger.debug("Relation message: {}.{} (oid={}) columns={}", schema, table, relationId, columns);
            return "RELATION schema:" + schema + " table:" + table + " oid:" + relationId
                    + " columns:" + String.join(",", columns);
        } catch (Exception e) {
            logger.warn("解析 Relation 消息失败，该表将回退到查源库当前定义: {}", e.getMessage());
            return "";
        }
    }

    /** Relation 消息带来的表结构；每次收到新的 Relation 消息即整体替换（这就是失效机制）。 */
    private static final class RelationSchema {
        final String schema;
        final String table;
        final List<String> columns;
        final List<String> types;
        final List<String> keyColumns;

        RelationSchema(String schema, String table, List<String> columns,
                       List<String> types, List<String> keyColumns) {
            this.schema = schema;
            this.table = table;
            this.columns = columns;
            this.types = types;
            this.keyColumns = keyColumns;
        }
    }

    /** 按字节前进的读取游标：{@code String.length()} 是字符数，非 ASCII 的库表名/列名会按它错位。 */
    private static final class Cursor {
        private final byte[] data;
        private int pos;

        Cursor(byte[] data, int pos) {
            this.data = data;
            this.pos = pos;
        }

        int int8() {
            return data[pos++] & 0xFF;
        }

        int int16() {
            int v = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
            pos += 2;
            return v;
        }

        int int32() {
            int v = ((data[pos] & 0xFF) << 24) | ((data[pos + 1] & 0xFF) << 16)
                    | ((data[pos + 2] & 0xFF) << 8) | (data[pos + 3] & 0xFF);
            pos += 4;
            return v;
        }

        String cstring() {
            int end = pos;
            while (end < data.length && data[end] != 0) {
                end++;
            }
            String s = new String(data, pos, end - pos, StandardCharsets.UTF_8);
            pos = end + 1;
            return s;
        }
    }

    /**
     * 类型 OID → 类型名。
     *
     * <p>OID 与类型的对应关系是<b>不随表结构变化</b>的，所以这份缓存可以一直留着 ——
     * 与"表的列清单"那种必须失效的缓存是两回事。内置常见类型免去一次查询，
     * 其余（自定义类型/枚举/数组）回查 {@code format_type} 一次。
     *
     * <p>名字刻意与 {@code information_schema.columns.data_type} 的写法对齐
     * （"character varying" / "timestamp without time zone" …），下游按类型名做判断的地方不用改。
     */
    private String typeNameOf(long oid) {
        String builtin = BUILTIN_TYPE_NAMES.get(oid);
        if (builtin != null) {
            return builtin;
        }
        return resolvedTypeNames.computeIfAbsent(oid, id -> {
            String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
            try (Connection qConn = DriverManager.getConnection(url, user, password);
                 Statement stmt = qConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT format_type(" + id + ", NULL)")) {
                if (rs.next()) {
                    String name = rs.getString(1);
                    if (name != null && !name.isEmpty()) {
                        return name;
                    }
                }
            } catch (Exception e) {
                logger.warn("类型 OID {} 解析失败，按文本处理: {}", id, e.getMessage());
            }
            return "";
        });
    }

    private static final Map<Long, String> BUILTIN_TYPE_NAMES = new java.util.HashMap<>();

    static {
        BUILTIN_TYPE_NAMES.put(16L, "boolean");
        BUILTIN_TYPE_NAMES.put(17L, "bytea");
        BUILTIN_TYPE_NAMES.put(19L, "name");
        BUILTIN_TYPE_NAMES.put(20L, "bigint");
        BUILTIN_TYPE_NAMES.put(21L, "smallint");
        BUILTIN_TYPE_NAMES.put(23L, "integer");
        BUILTIN_TYPE_NAMES.put(25L, "text");
        BUILTIN_TYPE_NAMES.put(26L, "oid");
        BUILTIN_TYPE_NAMES.put(114L, "json");
        BUILTIN_TYPE_NAMES.put(142L, "xml");
        BUILTIN_TYPE_NAMES.put(650L, "cidr");
        BUILTIN_TYPE_NAMES.put(700L, "real");
        BUILTIN_TYPE_NAMES.put(701L, "double precision");
        BUILTIN_TYPE_NAMES.put(790L, "money");
        BUILTIN_TYPE_NAMES.put(829L, "macaddr");
        BUILTIN_TYPE_NAMES.put(869L, "inet");
        BUILTIN_TYPE_NAMES.put(1042L, "character");
        BUILTIN_TYPE_NAMES.put(1043L, "character varying");
        BUILTIN_TYPE_NAMES.put(1082L, "date");
        BUILTIN_TYPE_NAMES.put(1083L, "time without time zone");
        BUILTIN_TYPE_NAMES.put(1114L, "timestamp without time zone");
        BUILTIN_TYPE_NAMES.put(1184L, "timestamp with time zone");
        BUILTIN_TYPE_NAMES.put(1186L, "interval");
        BUILTIN_TYPE_NAMES.put(1266L, "time with time zone");
        BUILTIN_TYPE_NAMES.put(1560L, "bit");
        BUILTIN_TYPE_NAMES.put(1562L, "bit varying");
        BUILTIN_TYPE_NAMES.put(1700L, "numeric");
        BUILTIN_TYPE_NAMES.put(2950L, "uuid");
        BUILTIN_TYPE_NAMES.put(3802L, "jsonb");
    }

    private String parseInsertMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;
            char tupleType = (char) (data[offset] & 0xFF); offset++;

            TableMeta meta = metaOf(relationId);
            String schema = meta.schema;
            String table = meta.table;

            List<String> columnNames = meta.columns;
            List<String> columnTypes = meta.types;
            List<String> pkColumns = meta.keys;

            List<String> values = parseTupleData(data, offset, columnNames, columnTypes);

            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(schema).append(" table:").append(table);
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }
            appendColumnTypes(sb, columnTypes);
            sb.append(" new-tuple:{");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sb.append(",");
                String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                sb.append(colName).append(":").append(values.get(i) != null ? values.get(i) : com.migration.common.wire.CapTupleMarkers.NULL);
            }
            sb.append("}");

            return sb.toString();
        } catch (Exception e) {
            logger.error("Error parsing INSERT message: {}", e.getMessage());
            return "INSERT relation_id=" + (offset > 0 ? "unknown" : "unknown");
        }
    }

    private String parseUpdateMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;

            TableMeta meta = metaOf(relationId);
            String schema = meta.schema;
            String table = meta.table;

            List<String> columnNames = meta.columns;
            List<String> columnTypes = meta.types;
            List<String> pkColumns = meta.keys;

            List<String> oldValues = null;
            List<String> newValues = null;

            while (offset < data.length) {
                char tupleType = (char) (data[offset] & 0xFF); offset++;

                if (tupleType == 'K' || tupleType == 'O') {
                    oldValues = parseTupleData(data, offset, columnNames, columnTypes);
                    offset = advancePastTuple(data, offset);
                } else if (tupleType == 'N') {
                    newValues = parseTupleData(data, offset, columnNames, columnTypes);
                    offset = advancePastTuple(data, offset);
                } else {
                    break;
                }
            }

            if (newValues == null && oldValues != null) {
                newValues = oldValues;
                oldValues = null;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(schema).append(" table:").append(table);
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }
            appendColumnTypes(sb, columnTypes);

            if (oldValues != null) {
                sb.append(" old-tuple:{");
                for (int i = 0; i < oldValues.size(); i++) {
                    if (i > 0) sb.append(",");
                    String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                    sb.append(colName).append(":").append(oldValues.get(i) != null ? oldValues.get(i) : com.migration.common.wire.CapTupleMarkers.NULL);
                }
                sb.append("}");
            }

            if (newValues != null) {
                sb.append(" new-tuple:{");
                for (int i = 0; i < newValues.size(); i++) {
                    if (i > 0) sb.append(",");
                    String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                    sb.append(colName).append(":").append(newValues.get(i) != null ? newValues.get(i) : com.migration.common.wire.CapTupleMarkers.NULL);
                }
                sb.append("}");
            }

            return sb.toString();
        } catch (Exception e) {
            logger.error("Error parsing UPDATE message: {}", e.getMessage());
            return "UPDATE";
        }
    }

    private int advancePastTuple(byte[] data, int offset) {
        try {
            int numCols = readInt16BE(data, offset); offset += 2;
            for (int i = 0; i < numCols; i++) {
                if (offset >= data.length) break;
                char colFlag = (char) (data[offset] & 0xFF); offset++;
                if (colFlag == 't') {
                    int colLen = readInt32BE(data, offset); offset += 4;
                    offset += colLen;
                }
            }
        } catch (Exception e) {
            logger.debug("Error advancing past tuple: {}", e.getMessage());
        }
        return offset;
    }

    private String parseDeleteMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;
            char tupleType = (char) (data[offset] & 0xFF); offset++;

            TableMeta meta = metaOf(relationId);
            String schema = meta.schema;
            String table = meta.table;

            List<String> columnNames = meta.columns;
            List<String> columnTypes = meta.types;
            List<String> pkColumns = meta.keys;

            List<String> oldValues = parseTupleData(data, offset, columnNames, columnTypes);

            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(schema).append(" table:").append(table);
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }
            appendColumnTypes(sb, columnTypes);
            sb.append(" old-tuple:{");
            for (int i = 0; i < oldValues.size(); i++) {
                if (i > 0) sb.append(",");
                String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                sb.append(colName).append(":").append(oldValues.get(i) != null ? oldValues.get(i) : com.migration.common.wire.CapTupleMarkers.NULL);
            }
            sb.append("}");

            return sb.toString();
        } catch (Exception e) {
            logger.error("Error parsing DELETE message: {}", e.getMessage());
            return "DELETE";
        }
    }

    private List<String> parseTupleData(byte[] data, int offset, List<String> columnNames, List<String> columnTypes) {
        List<String> values = new ArrayList<>();
        try {
            int numCols = readInt16BE(data, offset); offset += 2;
            logger.debug("parseTupleData: numCols={}, dataLen={}, offset={}", numCols, data.length, offset);

            for (int i = 0; i < numCols; i++) {
                char colFlag = (char) (data[offset] & 0xFF); offset++;
                String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                logger.debug("  col[{}] name={} flag='{}' (0x{}) offset={}", i, colName, colFlag, Integer.toHexString(colFlag), offset);
                if (colFlag == 't') {
                    int colLen = readInt32BE(data, offset); offset += 4;
                    byte[] colData = new byte[colLen];
                    System.arraycopy(data, offset, colData, 0, colLen);
                    offset += colLen;

                    String colType = (i < columnTypes.size()) ? columnTypes.get(i) : "";
                    String value = formatColumnValue(colData, colType, colName);
                    logger.debug("  col[{}] value={}", i, value);
                    values.add(value);
                } else if (colFlag == 'n') {
                    logger.debug("  col[{}] NULL", i);
                    values.add(null);
                } else if (colFlag == 'u') {
                    // 行外存储（TOAST）里本次未被修改的值：PG **不发送**它。
                    // 绝不能与真 NULL('n') 合流——那样下游会生成 SET col=NULL，
                    // 把目标端已经正确的大字段抹掉，且全程无报错。用独立标记下发，
                    // 由 extract 把该列整个从 SET 列表里摘掉。
                    logger.debug("  col[{}] UNCHANGED_TOAST", i);
                    values.add(com.migration.common.wire.CapTupleMarkers.UNCHANGED);
                } else {
                    logger.debug("  col[{}] UNKNOWN_FLAG={}", i, (int)colFlag);
                    values.add(null);
                }
            }
        } catch (Exception e) {
            logger.debug("Error parsing tuple data: {}", e.getMessage());
        }
        return values;
    }

    private String formatColumnValue(byte[] colData, String colType, String colName) {
        String strValue = new String(colData, StandardCharsets.UTF_8);
        // 空串不能当成 NULL：wire 上空串是 't' + 长度 0，真 NULL 是 'n'，两者本来就分得开。
        // 旧实现在这里返回不带引号的 "NULL"，extract 只认 [null] 前缀，于是它作为普通字符串
        // 一路走到目标端，落成四个字符的 'NULL'（实测复现）。空串按类型正常渲染即可：
        // 文本类型走下面的加引号分支得到 ''，数值类型交由 extract 的空值判断兜底。
        String lowerType = colType != null ? colType.toLowerCase() : "";
        if ("boolean".equalsIgnoreCase(lowerType)) {
            return strValue.equals("t") ? "true" : "false";
        }
        if (lowerType.contains("integer") || lowerType.contains("bigint") ||
            lowerType.contains("smallint") || lowerType.contains("serial") ||
            lowerType.contains("bigserial") || lowerType.equals("int") ||
            lowerType.equals("int4") || lowerType.equals("int8") ||
            lowerType.equals("int2") || lowerType.equals("oid")) {
            return strValue;
        }
        if (lowerType.contains("numeric") || lowerType.contains("decimal") ||
            lowerType.contains("real") || lowerType.contains("double") ||
            lowerType.contains("float") || lowerType.equals("float4") ||
            lowerType.equals("float8") || lowerType.equals("money")) {
            return strValue;
        }
        return "'" + strValue.replace("'", "''") + "'";
    }

    /**
     * 行事件用到的表元数据。优先来自 Relation 消息（与行值同一时刻的权威信息），
     * 没收到过 Relation 消息时才退回查源库当前定义 —— 那条路径就是"用现在的结构解释过去的事件"，
     * 只作兜底。
     */
    private static final class TableMeta {
        final String schema;
        final String table;
        final List<String> columns;
        final List<String> types;
        final List<String> keys;

        TableMeta(String schema, String table, List<String> columns, List<String> types, List<String> keys) {
            this.schema = schema;
            this.table = table;
            this.columns = columns;
            this.types = types;
            this.keys = keys;
        }
    }

    private TableMeta metaOf(long relationId) {
        RelationSchema rel = relationSchemas.get(relationId);
        if (rel != null) {
            // Relation 消息里 flags 的 bit0 就是"该列属于复制标识键"，与 pg_index 查出来的一致，
            // 且是这一刻的定义 —— 不必再回查
            List<String> keys = !rel.keyColumns.isEmpty()
                    ? rel.keyColumns : fetchTablePrimaryKeys(rel.schema, rel.table);
            return new TableMeta(rel.schema, rel.table, rel.columns, rel.types, keys);
        }
        String[] schemaTable = resolveRelationId(relationId);
        String schema = schemaTable[0];
        String table = schemaTable[1];
        logger.warn("表 {}.{}(oid={}) 尚未收到 Relation 消息，回退查源库当前定义（结构若已变更会错位）",
                schema, table, relationId);
        return new TableMeta(schema, table,
                fetchTableColumns(schema, table),
                fetchTableColumnTypes(schema, table),
                fetchTablePrimaryKeys(schema, table));
    }

    /**
     * 把列类型随事件一起下发。
     *
     * <p>不下发的话 extract 只能自己回查源库当前定义 —— capture 这边按事件当时的结构解析出来的值，
     * 到那边又被当前结构重新解释一遍，漂移窗口原封不动地搬了过去。
     * 类型名里有空格（"character varying"），所以用花括号括起来，与 tuple 同一套取法。
     */
    private void appendColumnTypes(StringBuilder sb, List<String> columnTypes) {
        if (columnTypes == null || columnTypes.isEmpty()) {
            return;
        }
        sb.append(" column_types:{").append(String.join(",", columnTypes)).append("}");
    }

    private final Map<Long, RelationSchema> relationSchemas = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<Long, String> resolvedTypeNames = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<Long, String[]> relationIdCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumnsCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumnTypesCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, List<String>> tablePrimaryKeysCache = new java.util.concurrent.ConcurrentHashMap<>();

    private String[] resolveRelationId(long relationId) {
        return relationIdCache.computeIfAbsent(relationId, id -> {
            String[] result = new String[]{"public", "unknown_" + id};
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     Statement stmt = qConn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT n.nspname, c.relname FROM pg_class c " +
                             "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = " + id)) {
                    if (rs.next()) {
                        result[0] = rs.getString(1);
                        result[1] = rs.getString(2);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to resolve relation OID {}: {}", id, e.getMessage());
            }
            return result;
        });
    }

    private List<String> fetchTableColumns(String schema, String table) {
        String key = schema + "." + table;
        return tableColumnsCache.computeIfAbsent(key, k -> {
            List<String> columns = new ArrayList<>();
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     PreparedStatement stmt = qConn.prepareStatement(
                             "SELECT column_name FROM information_schema.columns " +
                             "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            columns.add(rs.getString(1));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch columns for {}.{}: {}", schema, table, e.getMessage());
            }
            return columns;
        });
    }

    private List<String> fetchTableColumnTypes(String schema, String table) {
        String key = schema + "." + table;
        return tableColumnTypesCache.computeIfAbsent(key, k -> {
            List<String> types = new ArrayList<>();
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     PreparedStatement stmt = qConn.prepareStatement(
                             "SELECT data_type FROM information_schema.columns " +
                             "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            types.add(rs.getString(1));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch column types for {}.{}: {}", schema, table, e.getMessage());
            }
            return types;
        });
    }

    private List<String> fetchTablePrimaryKeys(String schema, String table) {
        String key = schema + "." + table;
        return tablePrimaryKeysCache.computeIfAbsent(key, k -> {
            List<String> pkColumns = new ArrayList<>();
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     PreparedStatement stmt = qConn.prepareStatement(
                             "SELECT a.attname FROM pg_index i " +
                             "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                             "JOIN pg_class c ON c.oid = i.indrelid " +
                             "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                             "WHERE i.indisprimary AND n.nspname = ? AND c.relname = ? " +
                             "ORDER BY array_position(i.indkey, a.attnum)")) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            pkColumns.add(rs.getString(1));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch primary keys for {}.{}: {}", schema, table, e.getMessage());
            }
            return pkColumns;
        });
    }

    private int readInt8(byte[] data, int offset) {
        return data[offset] & 0xFF;
    }

    private int readInt16BE(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private int readInt32BE(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24) |
               ((data[offset + 1] & 0xFF) << 16) |
               ((data[offset + 2] & 0xFF) << 8) |
               (data[offset + 3] & 0xFF);
    }

    private long readInt64BE(byte[] data, int offset) {
        return ((long) (data[offset] & 0xFF) << 56) |
               ((long) (data[offset + 1] & 0xFF) << 48) |
               ((long) (data[offset + 2] & 0xFF) << 40) |
               ((long) (data[offset + 3] & 0xFF) << 32) |
               ((long) (data[offset + 4] & 0xFF) << 24) |
               ((long) (data[offset + 5] & 0xFF) << 16) |
               ((long) (data[offset + 6] & 0xFF) << 8) |
               ((long) (data[offset + 7] & 0xFF));
    }

    private String readCString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, StandardCharsets.UTF_8);
    }

    private String parseWalEventType(String walData) {
        if (walData == null || walData.isEmpty()) return "WAL_EVENT";
        if (walData.startsWith("TRUNCATE")) return "TRUNCATE";
        if (walData.startsWith("BEGIN")) return "BEGIN";
        if (walData.startsWith("COMMIT")) return "COMMIT";
        if (walData.contains("\"I\"")) return "INSERT";
        if (walData.contains("\"U\"")) return "UPDATE";
        if (walData.contains("\"D\"")) return "DELETE";
        if (walData.startsWith("B")) return "BEGIN";
        if (walData.startsWith("C")) return "COMMIT";
        if (walData.startsWith("I")) return "INSERT";
        if (walData.startsWith("U")) return "UPDATE";
        if (walData.startsWith("D")) return "DELETE";
        return "WAL_EVENT";
    }

    /** 从 pgoutput 解析结果里提取表名（格式 "schema:xxx table:yyy ..."），用于环路防护识别标记表。 */
    private String extractPgTableName(String eventDataStr) {
        if (eventDataStr == null) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("table:(\\S+)").matcher(eventDataStr);
        return m.find() ? m.group(1) : null;
    }

    private long parseLsnToLong(String lsn) {
        if (lsn == null || lsn.isEmpty()) return 0;
        String[] parts = lsn.split("/");
        if (parts.length == 2) {
            long segment = Long.parseLong(parts[0], 16);
            long offset = Long.parseLong(parts[1], 16);
            return (segment << 32) | offset;
        }
        return 0;
    }

    @Override
    protected void doStop() throws Exception {
        if (replicationStream != null) {
            try {
                replicationStream.close();
            } catch (Exception e) {
                logger.warn("Error closing replication stream: {}", e.getMessage());
            }
        }

        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing PostgreSQL connection: {}", e.getMessage());
            }
        }

        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (Exception e) {
                logger.warn("Error closing writer: {}", e.getMessage());
            }
        }

        savePosition();
        logger.info("PostgreSQL WAL capture stopped. Total events captured: {}", eventCounter.get());
    }

    private synchronized void openNewOutputFile() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        String fileName = String.format("binlog_%s_%04d.cap", timestamp, fileCounter.get());

        File outputFile = new File(outputDir, fileName);
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        currentFileEvents = 0;

        logger.info("Opened new WAL capture output file: {}", outputFile.getAbsolutePath());
    }

    private synchronized void rotateOutputFile() throws IOException {
        fileCounter.incrementAndGet();
        openNewOutputFile();
        logger.info("Rotated to new capture output file after {} events", maxEventsPerFile);
    }

    /**
     * 复制槽保留状态的在线巡检。
     *
     * <p>PG 这条链路的失效方式比 MySQL 更隐蔽：槽被人删掉、或 {@code max_slot_wal_keep_size}
     * 把槽判成 {@code lost} 之后，重新 START_REPLICATION <b>不会报错</b>，而是从槽当前位置开始发，
     * 中间那段变更静默消失。所以运行中就要盯住 {@code wal_status}，别等重启。
     *
     * <p>{@code wal_status} 是 PG13+ 才有的列；更老的版本查不到，如实记 UNKNOWN 而不是猜一个状态。
     */
    private void checkRetentionQuietly() {
        if (!retentionCheckEnabled) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastRetentionCheckMs < retentionCheckIntervalMs) {
            return;
        }
        lastRetentionCheckMs = now;

        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified&" + sslParams(), host, port, database);
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            String walStatus = null;
            long lagBytes = -1;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS lag "
                            + "FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                if (!rs.next()) {
                    com.migration.common.position.RetentionStatus.write(outputDir,
                            com.migration.common.position.RetentionStatus.State.LOST, 0,
                            "复制槽 " + slotName + " 已不存在");
                    logger.error("位点保留期告警：复制槽 {} 已不存在，重连将从新位置开始、中间变更会静默丢失",
                            slotName);
                    return;
                }
                walStatus = rs.getString("wal_status");
                lagBytes = rs.getLong("lag");
            } catch (SQLException noWalStatus) {
                // PG12 及更早没有 wal_status 列：退一步只取落后字节数
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS lag "
                                + "FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                    if (rs.next()) {
                        lagBytes = rs.getLong("lag");
                    }
                }
            }

            if ("lost".equalsIgnoreCase(walStatus)) {
                com.migration.common.position.RetentionStatus.write(outputDir,
                        com.migration.common.position.RetentionStatus.State.LOST, 0,
                        "复制槽 " + slotName + " wal_status=lost，所需 WAL 已被回收");
                logger.error("位点保留期告警：复制槽 {} 已被判为 lost，位点不可用，需重做全量", slotName);
            } else if ("unreserved".equalsIgnoreCase(walStatus)) {
                com.migration.common.position.RetentionStatus.write(outputDir,
                        com.migration.common.position.RetentionStatus.State.WARN, lagBytes,
                        "复制槽 " + slotName + " wal_status=unreserved，已超出保留配额，随时可能变成 lost");
                logger.warn("位点保留期告警：复制槽 {} wal_status=unreserved（落后 {} 字节），"
                        + "请尽快让消费追平或调大 max_slot_wal_keep_size", slotName, lagBytes);
            } else if (walStatus == null) {
                com.migration.common.position.RetentionStatus.write(outputDir,
                        com.migration.common.position.RetentionStatus.State.UNKNOWN, lagBytes,
                        "当前 PG 版本无 wal_status 列，仅记录落后字节数");
            } else {
                com.migration.common.position.RetentionStatus.write(outputDir,
                        com.migration.common.position.RetentionStatus.State.OK, lagBytes,
                        "复制槽 " + slotName + " wal_status=" + walStatus + "，落后 " + lagBytes + " 字节");
            }
        } catch (Exception e) {
            com.migration.common.position.RetentionStatus.write(outputDir,
                    com.migration.common.position.RetentionStatus.State.UNKNOWN, -1,
                    "巡检查询失败: " + e.getMessage());
            logger.debug("复制槽保留状态巡检跳过: {}", e.getMessage());
        }
    }

    /** 落盘 WAL 位点，原子写（tmp+fsync+rename），崩溃重启后据此续传而非从任务起始 LSN 重放。 */
    private void savePosition() {
        if (currentLsn == null) return;

        Properties posProps = new Properties();
        posProps.setProperty("wal.lsn", currentLsn);
        posProps.setProperty("wal.lsn.numeric", String.valueOf(currentLsnNumeric));
        posProps.setProperty("last.update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        com.migration.common.position.CapturePositionStore.save(
                outputDir, posProps, "WAL Capture position for task: " + taskId, taskId);
    }

    public String getCurrentLsn() {
        return currentLsn;
    }

    public long getCurrentLsnNumeric() {
        return currentLsnNumeric;
    }

    public long getEventCount() {
        return eventCounter.get();
    }
}
