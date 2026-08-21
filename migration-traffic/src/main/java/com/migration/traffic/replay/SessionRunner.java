package com.migration.traffic.replay;

import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficRecord;
import com.migration.traffic.replay.dialect.TargetDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一条源会话 : 一条目标连接 : 一个执行线程。
 *
 * <p>这个 1:1:1 是回放正确性的地基：
 * <ul>
 *   <li>同一源会话内的语句必须<b>严格保序</b>——事务、临时表、会话变量、{@code USE}
 *       都只在同一条连接上才有意义；</li>
 *   <li>不同源会话之间<b>本来就没有顺序约束</b>，它们的先后完全由各自的到达时刻决定。
 *       所以跨会话顺序不由代码保证，而由时间轴保证——那正是源库当时的真实情形。</li>
 * </ul>
 */
final class SessionRunner {

    private static final Logger logger = LoggerFactory.getLogger(SessionRunner.class);

    private final long sourceSessionId;
    private final Properties props;
    private final TargetDialect dialect;
    /** 本会话连的库。PG 的连接终生绑定一个库，所以它在建连时就定死。 */
    private final String database;
    private final SourceFingerprint fingerprint;
    private final ReplayOptions options;
    private final ReplayReporter reporter;
    private final DangerousStatementFilter dangerFilter;
    private final ReplayScheduler scheduler;

    private final BlockingQueue<Job> queue;
    private final Thread worker;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private Connection conn;
    private String currentDb;
    /** 显式事务中（BEGIN/START TRANSACTION 之后、COMMIT/ROLLBACK 之前）。淘汰时绝不能动。 */
    private volatile boolean inTransaction;
    private volatile boolean busy;
    private volatile long lastActiveMs = System.currentTimeMillis();
    private volatile boolean failed;

    SessionRunner(long sourceSessionId, Properties props, TargetDialect dialect, String database,
                  SourceFingerprint fingerprint, ReplayOptions options,
                  ReplayReporter reporter, DangerousStatementFilter dangerFilter,
                  ReplayScheduler scheduler) {
        this.sourceSessionId = sourceSessionId;
        this.props = props;
        this.dialect = dialect;
        this.database = database;
        this.fingerprint = fingerprint;
        this.options = options;
        this.reporter = reporter;
        this.dangerFilter = dangerFilter;
        this.scheduler = scheduler;
        this.queue = new ArrayBlockingQueue<>(Math.max(16, options.sessionQueueSize));
        this.worker = new Thread(this::loop, "traffic-replay-s" + sourceSessionId);
        this.worker.setDaemon(true);
        this.worker.start();
    }

    /**
     * @param deadlineNanos 该语句<b>计划</b>执行的绝对时刻（nanoTime 坐标）。
     *                      执行线程据此算出真实的时间轴偏差——只在派发线程量偏差，
     *                      会有上万深的队列把它掩盖成 0。
     */
    private record Job(TrafficRecord record, long plannedMicros, long deadlineNanos) {
    }

    /**
     * 投递一条语句。队列满 = 目标库跟不上：
     * SKIP 档直接丢并计数，其余档位阻塞等待（保序优先于时效）。
     */
    boolean submit(TrafficRecord r, long plannedMicros, long deadlineNanos) throws InterruptedException {
        if (stopped.get() || failed) return false;
        Job job = new Job(r, plannedMicros, deadlineNanos);
        if (options.lagPolicy == ReplayOptions.LagPolicy.SKIP) {
            if (!queue.offer(job)) {
                reporter.record(ReplayOutcome.SKIPPED_LATE, r, 0, plannedMicros, "会话队列已满，SKIP 档丢弃");
                return true;
            }
            return true;
        }
        queue.put(job);
        return true;
    }

    private void loop() {
        // 提前建连：连接握手 + 会话环境对齐要几十毫秒，放到第一条语句执行时做，
        // 这几十毫秒会原样计进第一条语句的时间轴偏差。会话是在派发时创建的，
        // 此刻离第一条语句的计划时刻通常还有余量，正好用来把连接准备好。
        try {
            ensureConnection();
        } catch (SQLException e) {
            logger.warn("[s{}] 预建连接失败，将在首条语句执行时重试: {}", sourceSessionId, e.getMessage());
        }
        while (!stopped.get()) {
            Job job;
            try {
                job = queue.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (job == null) continue;
            busy = true;
            try {
                if (scheduler != null) {
                    scheduler.recordExecutionLag(System.nanoTime() - job.deadlineNanos());
                }
                execute(job.record(), job.plannedMicros());
            } catch (Exception e) {
                logger.debug("[s{}] 执行异常: {}", sourceSessionId, e.getMessage());
            } finally {
                busy = false;
                lastActiveMs = System.currentTimeMillis();
            }
        }
    }

    private void execute(TrafficRecord r, long plannedMicros) {
        // 被抹掉的口令谁也拿不到；把占位符当字面量执行会建出一个口令是占位符的账号
        if (r.rd) {
            reporter.record(ReplayOutcome.UNREPLAYABLE_REDACTED, r, 0, plannedMicros,
                    "语句里的口令已被抹除（源库写日志时抹的，或捕获侧脱敏），无法忠实回放");
            return;
        }
        String noBinds = dialect.unreplayableReason(r);
        if (noBinds != null) {
            reporter.record(ReplayOutcome.UNREPLAYABLE_NO_BINDS, r, 0, plannedMicros, noBinds);
            return;
        }
        String block = dangerFilter.blockReason(r.q);
        if (block != null) {
            reporter.record(ReplayOutcome.BLOCKED, r, 0, plannedMicros, block);
            return;
        }

        long begin = System.nanoTime();
        try {
            ensureConnection();
            alignSchema(r);
            if (TrafficRecord.CMD_INIT_DB.equals(r.c)) {
                // Init DB 是协议层的切库，alignSchema 已经做完了。这里必须提前返回：
                // 老录制里这条记录的 q 是<b>裸库名</b>，照原样执行就是一条语法错的 SQL。
                reporter.record(ReplayOutcome.OK, r, (System.nanoTime() - begin) / 1000L, plannedMicros, null);
                return;
            }
            long rows = runStatement(r);
            long elapsed = (System.nanoTime() - begin) / 1000L;
            trackTransaction(r);

            if (r.hasEnrich && r.errno != 0) {
                // 源库当时就报错，目标库却成功了 —— 也是一种不一致，但不算回放错误
                reporter.record(ReplayOutcome.OK, r, elapsed, plannedMicros, null);
                return;
            }
            if (options.compare == ReplayOptions.CompareMode.ROWCOUNT && r.hasEnrich) {
                long expected = r.k == StatementClass.SELECT ? r.rows : r.aff;
                if (expected != rows) {
                    reporter.record(ReplayOutcome.ROWCOUNT_MISMATCH, r, elapsed, plannedMicros,
                            "源端 " + expected + " 行，目标端 " + rows + " 行");
                    return;
                }
            }
            reporter.record(ReplayOutcome.OK, r, elapsed, plannedMicros, null);
        } catch (SQLException e) {
            long elapsed = (System.nanoTime() - begin) / 1000L;
            // 源库当时就报错的语句，在目标库也报错属于预期，不该算成回放发现的问题
            boolean expected = r.hasEnrich && r.errno != 0;
            reporter.record(expected ? ReplayOutcome.EXPECTED_ERROR : ReplayOutcome.REPLAY_ERROR,
                    r, elapsed, plannedMicros, "[" + e.getErrorCode() + "] " + e.getMessage());
            handleConnectionLoss(e);
        }
    }

    /**
     * 语句执行。
     *
     * <p>SELECT 必须把结果集读完才算真正执行了——只 {@code execute()} 不取结果，
     * 服务端的排序/临时表/网络传输都不会真的发生，回放出来的负载与源库不是一回事。
     */
    private long runStatement(TrafficRecord r) throws SQLException {
        if (dialect.needsPrepared(r)) {
            // PG / Oracle：参数没被替换进 SQL 文本，必须绑上去。
            // 没参数的语句一律走普通 Statement —— simple query 里可能是多条语句串在一起
            // （PG 实测 "SET …; SELECT …" 会被记成一行），PreparedStatement 送不出去。
            com.migration.traffic.replay.dialect.PreparedPlan plan = dialect.prepare(r.q, r.b);
            try (PreparedStatement ps = conn.prepareStatement(plan.sql())) {
                dialect.bind(ps, plan.binds());
                boolean hasResultSet = ps.execute();
                return hasResultSet ? drain(ps.getResultSet()) : Math.max(0, ps.getUpdateCount());
            }
        }
        try (Statement st = conn.createStatement()) {
            boolean hasResultSet = st.execute(r.q);
            if (!hasResultSet) {
                return Math.max(0, st.getUpdateCount());
            }
            return drain(st.getResultSet());
        }
    }

    private long drain(ResultSet rs) throws SQLException {
        long rows = 0;
        try (ResultSet r = rs) {
            while (r.next()) {
                rows++;
                if (options.maxFetchRows > 0 && rows >= options.maxFetchRows) break;
            }
        }
        return rows;
    }

    /**
     * 让目标连接的默认库与录制里这条语句当时的默认库一致。
     *
     * <p>不只依赖录制里的 {@code USE}/{@code Init DB} 记录：会话可能被淘汰后重建，
     * 也可能是连接池里"捕获开始前就存在"的会话（默认库来自 PROCESSLIST 播种，
     * 录制里根本没有对应的切库语句）。这里按记录自带的 db 自愈，且是幂等的。
     */
    private void alignSchema(TrafficRecord r) throws SQLException {
        // PG 的库由连接决定（连接终生绑定一个库），能切的只有 search_path
        String wanted = dialect.connectionBoundToDatabase() ? r.sn : r.db;
        if (wanted == null || wanted.isEmpty() || wanted.equals(currentDb)) return;
        String applied = dialect.switchSchema(conn, r.db, r.sn);
        currentDb = applied != null ? wanted : currentDb;
    }

    private void trackTransaction(TrafficRecord r) {
        switch (dialect.txEffect(r.q, r.k)) {
            case OPEN: inTransaction = true; break;
            case CLOSE: inTransaction = false; break;
            default: break;
        }
    }

    private void ensureConnection() throws SQLException {
        if (conn != null && !conn.isClosed()) return;
        conn = dialect.connect(props, database);
        currentDb = null;
        inTransaction = false;
        dialect.onConnect(conn, fingerprint);
    }

    private void handleConnectionLoss(SQLException e) {
        if (dialect.isConnectionLost(e)) {
            closeConnection();
        }
    }

    boolean isIdle() {
        return !busy && queue.isEmpty();
    }

    boolean isEvictable() {
        return isIdle() && !inTransaction;
    }

    long lastActiveMs() {
        return lastActiveMs;
    }

    long sourceSessionId() {
        return sourceSessionId;
    }

    /** 等这条会话把队列排空（回放收尾时用，保证不落下已投递的语句）。 */
    void drain(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && !isIdle()) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    void close() {
        stopped.set(true);
        worker.interrupt();
        closeConnection();
    }

    private void closeConnection() {
        try {
            if (conn != null && !conn.isClosed()) conn.close();
        } catch (SQLException ignored) {
            // 关闭失败无处可去
        }
        conn = null;
    }
}
