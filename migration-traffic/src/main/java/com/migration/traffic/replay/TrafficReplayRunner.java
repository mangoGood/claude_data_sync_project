package com.migration.traffic.replay;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.ssl.SslMaterial;
import com.migration.traffic.TrafficMetrics;
import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.TrafficRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 流量回放主流程。
 *
 * <pre>
 *  录制文件 ──► Reader（按 n 顺序，一条不跳）
 *                 │
 *                 ▼
 *            Scheduler（绝对截止时刻 t0 + t/speed）
 *                 │  到点投递
 *                 ▼
 *   SessionRunner[s]  1 源会话 : 1 目标连接 : 1 线程（会话内串行保序）
 *                 │
 *                 ▼
 *             Reporter（指标 + 错误明细 + 报告）
 * </pre>
 */
public final class TrafficReplayRunner {

    private static final Logger logger = LoggerFactory.getLogger(TrafficReplayRunner.class);

    private final Properties props;
    private final String taskId;
    private final File recordingDir;
    private final File outDir;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final CountDownLatch finished = new CountDownLatch(1);

    private ReplayOptions options;
    private ReplayScheduler scheduler;
    private ReplayReporter reporter;
    private TrafficMetrics metrics;
    private DangerousStatementFilter dangerFilter;
    private RecordingReader reader;

    private String jdbcUrl;
    private String user;
    private String password;
    private SourceFingerprint fingerprint;

    /** 访问顺序的 LinkedHashMap：淘汰时天然从最久未用的开始找。 */
    private final LinkedHashMap<Long, SessionRunner> sessions = new LinkedHashMap<>(64, 0.75f, true);

    private long progressed;
    private String abortReason;

    public TrafficReplayRunner(Properties props) {
        this.props = props;
        this.taskId = props.getProperty("task.id", "unknown");
        this.recordingDir = new File(props.getProperty("traffic.replay.recording.dir",
                "./files/" + taskId + "/traffic_in"));
        this.outDir = new File("./files/" + taskId + "/traffic");
    }

    public void run() throws Exception {
        try {
            start();
            replay();
        } finally {
            try {
                shutdown();
            } finally {
                finished.countDown();
            }
        }
    }

    public void stop() {
        stopped.set(true);
    }

    public boolean awaitFinished(long timeoutMs) {
        try {
            return finished.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void start() throws Exception {
        options = ReplayOptions.from(props);
        metrics = new TrafficMetrics(taskId, "traffic_replay_liveness");
        dangerFilter = new DangerousStatementFilter(options.allowDangerous);
        if (!outDir.exists() && !outDir.mkdirs()) {
            logger.warn("回放输出目录创建失败: {}", outDir.getAbsolutePath());
        }
        reporter = new ReplayReporter(outDir, metrics);

        reader = new RecordingReader(recordingDir);
        RecordingManifest manifest = reader.manifest();
        fingerprint = manifest.source;
        logger.info("录制: 分段={}, 记录={}, 空洞={}, 源={} {}",
                manifest.segments.size(), manifest.totalRecords(), manifest.gaps.size(),
                fingerprint == null ? "?" : fingerprint.version,
                fingerprint == null ? "" : fingerprint.serverUuid);

        buildTargetUrl();
        guardAgainstSameInstance();

        scheduler = new ReplayScheduler(options, manifest);
    }

    private void buildTargetUrl() {
        String host = props.getProperty("target.db.host", "localhost");
        String port = props.getProperty("target.db.port", "3306");
        user = props.getProperty("target.db.username", "root");
        password = CredentialCipher.decrypt(props.getProperty("target.db.password", ""));
        jdbcUrl = String.format("jdbc:mysql://%s:%s/?%s"
                        + "&serverTimezone=UTC&characterEncoding=utf8&allowMultiQueries=false"
                        + "&allowPublicKeyRetrieval=true&connectTimeout=15000&socketTimeout=600000",
                host, port, SslMaterial.from(props, "target").mysqlUrlParams());
    }

    /**
     * 硬拦截：回放目标就是录制源库自己。
     *
     * <p>这不是洁癖。回放会在目标库执行 DML/DDL——把源库上刚发生过的一切<b>再做一遍</b>：
     * {@code UPDATE ... SET n=n+1} 变成二次累加，{@code INSERT} 变成重复插入，
     * {@code DROP TABLE} 是真的删。灾备任务早就有同类的"源目标隔离"检查，这里是它的对应物。
     */
    private void guardAgainstSameInstance() throws SQLException {
        if (fingerprint == null || fingerprint.serverUuid == null || fingerprint.serverUuid.isEmpty()) {
            logger.warn("录制里没有源库 server_uuid，无法判定目标是否就是源库本身");
            return;
        }
        String targetUuid;
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, password);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT @@server_uuid")) {
            targetUuid = rs.next() ? rs.getString(1) : null;
        }
        if (fingerprint.serverUuid.equals(targetUuid)) {
            String msg = "回放目标与录制源是同一个 MySQL 实例（server_uuid=" + targetUuid
                    + "）。回放会把源库上已经发生过的操作再做一遍——自增累加会翻倍、"
                    + "DROP 是真的删。已拒绝启动；确需如此请显式开启 traffic.replay.allow.same.instance";
            if (!options.allowSameInstance) {
                com.migration.traffic.TrafficErrorStatus.report(taskId, "E3124", msg);
                throw new SQLException(msg);
            }
            logger.error("！！{}（已被显式放行）", msg);
        }
    }

    private void replay() throws Exception {
        long total = reader.manifest().totalRecords();
        scheduler.start();
        long lastMetricsAt = 0;

        Iterator<TrafficRecord> it = reader.iterator();
        while (it.hasNext()) {
            if (stopped.get()) {
                logger.info("收到停止信号，回放中止于第 {} 条", progressed);
                break;
            }
            TrafficRecord r = it.next();
            progressed++;

            // 活性无条件刷：回放到一段长空洞时会静默很久，看门狗不能把它当僵死
            metrics.liveness();
            long now = System.currentTimeMillis();
            if (now - lastMetricsAt > 2000) {
                reporter.publishMetrics(progressed, total, scheduler);
                lastMetricsAt = now;
            }

            if (TrafficRecord.CMD_QUIT.equals(r.c)) {
                closeSession(r.s);
                continue;
            }
            if (TrafficRecord.CMD_CONNECT.equals(r.c)) {
                // Connect 本身不用回放，但正好拿它<b>预热连接</b>：录制里 Connect 总是先于
                // 该会话的第一条语句出现，此刻建连，等第一条语句到点时握手早已完成，
                // 不会把几十毫秒的建连时间算进它的时间轴偏差。
                // 只在会话数还宽裕时预热，免得为一堆短连接把配额占满。
                if (sessions.size() < options.maxSessions * 4 / 5) {
                    sessionFor(r);
                }
                continue;
            }
            if (!options.shouldExecute(r.k)) {
                reporter.record(ReplayOutcome.FILTERED, r, 0, r.t, null);
                continue;
            }
            if (r.q == null || r.q.isBlank()) {
                continue;
            }

            // 会话要在<b>等待之前</b>准备好：建连 + 会话环境对齐要几十毫秒，
            // 放到等待之后做，这几十毫秒就原样变成第一条语句的时间轴偏差。
            SessionRunner session = sessionFor(r);
            if (session == null) {
                reporter.record(ReplayOutcome.SESSION_EXHAUSTED, r, 0, r.t,
                        "并发会话已达上限 " + options.maxSessions + " 且无可淘汰会话");
                continue;
            }

            if (!scheduler.awaitTurn(r.t)) {
                reporter.record(ReplayOutcome.SKIPPED_LATE, r, 0, r.t,
                        "超过 " + options.lagSkipMs + "ms 未能按时执行，SKIP 档丢弃");
                continue;
            }

            session.submit(r, r.t, scheduler.deadlineNanos(r.t));

            if (shouldAbort()) {
                abortReason = String.format(java.util.Locale.ROOT,
                        "回放错误率 %.1f%% 超过阈值 %.1f%%，已停止以免继续在目标库上制造破坏",
                        reporter.errorRate() * 100, options.abortErrorRate * 100);
                logger.error(abortReason);
                com.migration.traffic.TrafficErrorStatus.report(taskId, "E3125", abortReason);
                break;
            }
        }
        drainAll();
        reporter.publishMetrics(progressed, total, scheduler);
    }

    private boolean shouldAbort() {
        return reporter.attempted() >= options.abortMinSamples
                && reporter.errorRate() > options.abortErrorRate;
    }

    /** 取（或建）该源会话对应的目标连接。到上限则尝试淘汰一条空闲且不在事务中的会话。 */
    private SessionRunner sessionFor(TrafficRecord r) {
        SessionRunner s = sessions.get(r.s);
        if (s != null) return s;
        if (sessions.size() >= options.maxSessions && !evictOne()) {
            return null;
        }
        s = new SessionRunner(r.s, jdbcUrl, user, password, fingerprint, options, reporter,
                dangerFilter, scheduler);
        sessions.put(r.s, s);
        return s;
    }

    /**
     * 淘汰一条会话。
     *
     * <p><b>事务中的会话绝不淘汰</b>：那会在目标库上留下一个悬挂事务（连接一断即回滚），
     * 该事务已执行的语句全部消失，而后续语句照常执行——回放结果会变得毫无意义且难以察觉。
     */
    private boolean evictOne() {
        Long victim = null;
        // accessOrder=true，迭代顺序即最久未用在前
        for (Map.Entry<Long, SessionRunner> e : sessions.entrySet()) {
            if (e.getValue().isEvictable()) {
                victim = e.getKey();
                break;
            }
        }
        if (victim == null) return false;
        SessionRunner s = sessions.remove(victim);
        s.close();
        logger.debug("淘汰空闲会话 s{}（并发会话已达上限）", victim);
        return true;
    }

    private void closeSession(long sourceSessionId) {
        SessionRunner s = sessions.remove(sourceSessionId);
        if (s != null) {
            s.drain(30_000);
            s.close();
        }
    }

    /** 收尾：等所有会话把已投递的语句跑完，再关连接。 */
    private void drainAll() {
        List<SessionRunner> all = new ArrayList<>(sessions.values());
        for (SessionRunner s : all) {
            s.drain(60_000);
        }
        for (SessionRunner s : all) {
            s.close();
        }
        sessions.clear();
    }

    private void shutdown() {
        try {
            drainAll();
        } catch (Exception e) {
            logger.warn("回放收尾异常: {}", e.getMessage());
        }
        String outcome = abortReason != null ? "FAILED" : (stopped.get() ? "STOPPED" : "COMPLETED");
        if (reporter != null) {
            reporter.writeSummary(outDir, scheduler, progressed, outcome, abortReason);
            reporter.close();
        }
        if (reader != null) {
            reader.close();
        }
        logger.info("流量回放结束: {} 条，结果={}，成功={}，回放错误={}，拦截={}，跳过={}，"
                        + "时间轴偏差 P50/P95/P99={}ms/{}ms/{}ms",
                progressed, outcome,
                reporter == null ? 0 : reporter.count(ReplayOutcome.OK),
                reporter == null ? 0 : reporter.count(ReplayOutcome.REPLAY_ERROR),
                reporter == null ? 0 : reporter.count(ReplayOutcome.BLOCKED),
                reporter == null ? 0 : reporter.count(ReplayOutcome.SKIPPED_LATE),
                scheduler == null ? 0 : scheduler.execPercentileLagMs(0.50),
                scheduler == null ? 0 : scheduler.execPercentileLagMs(0.95),
                scheduler == null ? 0 : scheduler.execPercentileLagMs(0.99));
        if (abortReason != null) {
            throw new IllegalStateException(abortReason);
        }
    }
}
