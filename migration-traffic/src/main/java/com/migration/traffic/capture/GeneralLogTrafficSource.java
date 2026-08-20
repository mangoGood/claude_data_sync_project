package com.migration.traffic.capture;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.ssl.SslMaterial;
import com.migration.traffic.model.SourceFingerprint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 基于 MySQL {@code general_log}（{@code log_output=TABLE}）的语句流捕获。
 *
 * <p>为什么是它：binlog 里没有 {@code SELECT}，{@code performance_schema} 的
 * {@code SQL_TEXT} 默认被截断在 1024 字节（截断后语法就不完整，是"抓到了但回放不了"）。
 * 只有 general_log 同时满足<b>纯 JDBC 可达</b>（agent 与源库可以不同机）、<b>不丢</b>、
 * <b>语句原文完整</b>三条。
 *
 * <p>四个实测得来的关键实现细节：
 * <ol>
 *   <li><b>{@code SET SESSION sql_log_off=1}</b>：采集连接自己的轮询 SQL 也会被写进 general_log，
 *       不关掉就是<b>自激</b>——每次轮询给下次轮询造出新数据，永远读不完。关掉后该连接彻底静默，
 *       只剩建连时的 3 行常数噪声（Connect / 驱动的 version_comment 探测 / SET 自身）。</li>
 *   <li><b>轮转只能 RENAME</b>：{@code DELETE FROM mysql.general_log} 报
 *       {@code ERROR 1556 You can't use locks with log tables}。实测 RENAME 在
 *       {@code general_log=ON} 状态下即可完成且<b>无缝</b>——MySQL 立刻改写新表，一条不丢。</li>
 *   <li><b>不加 ORDER BY</b>：CSV 引擎是平铺文件，全表扫描即写入顺序。这既省掉大表 filesort，
 *       又让同一微秒内的多条语句有<b>稳定的全序</b>（{@code ORDER BY event_time} 在时间戳相同时
 *       顺序未定义，而源库一微秒内跑几条语句是常态）。</li>
 *   <li><b>{@code argument} 按字节读</b>：日志表字符集是 utf8mb3 而该列是 mediumblob，
 *       原始字节完好；一旦让服务端 {@code CONVERT} 或让驱动按表字符集转 String，
 *       4 字节 UTF-8（emoji 等）就变成 {@code ???}。任务全绿，数据已经错了。</li>
 * </ol>
 */
public final class GeneralLogTrafficSource implements TrafficSource {

    private static final Logger logger = LoggerFactory.getLogger(GeneralLogTrafficSource.class);

    /** 源库上的互斥锁名：同一个源实例同时跑两个捕获任务会互相抢轮转，两边都丢数据。 */
    static final String SOURCE_LOCK_NAME = "synctask_traffic_capture";

    /**
     * 抢源库互斥锁的最长等待。
     *
     * <p>不是 0：会话锁随连接断开释放，而"上一个捕获任务刚停、连接还没被服务端回收"
     * 是正常的几秒窗口。等一等能让"停掉再重启"这种最普通的操作正常进行，
     * 又不会掩盖"真有另一个任务在跑"（那种情况下等多久都抢不到）。
     */
    private static final long LOCK_WAIT_MS = 20_000L;

    /** 轮转用的两张表。冻结表读干净即 DROP；下一张在 RENAME 时就位。 */
    static final String FROZEN_TABLE = "mysql.general_log_trf_read";
    static final String NEXT_TABLE = "mysql.general_log_trf_next";

    private Connection conn;
    private String jdbcUrl;
    private String user;
    private String password;

    private final SourceFingerprint fingerprint = new SourceFingerprint();

    /** 采集连接自己的 connection id（可能因重连变化，故是集合）。 */
    private final Set<Long> ownThreadIds = new LinkedHashSet<>();

    private String originalGeneralLog;
    private String originalLogOutput;
    private volatile boolean stateChanged;
    private volatile boolean closed;
    /** 源库开关是否<b>确认</b>已还原。还原失败时保持 false，上层据此保留兜底状态文件。 */
    private volatile boolean restored;

    private long rotateIntervalMs;
    private long rotateIntervalMinMs;
    private long rotateIntervalMaxMs;
    private long rotateRowsHighWater;
    private long lastRotateAt;
    private long lastBatchRows = -1;

    /** 录制原点：源库时钟的 epoch 微秒。所有偏移都相对它算。 */
    private long t0Micros;

    /** 用于上报错误码（写 error_status 文件给 agent 轮询）。 */
    private String taskId = "unknown";

    @Override
    public void open(Properties cfg) throws Exception {
        this.taskId = cfg.getProperty("task.id", "unknown");
        String host = cfg.getProperty("source.db.host", "localhost");
        String port = cfg.getProperty("source.db.port", "3306");
        this.user = cfg.getProperty("source.db.username", "root");
        this.password = CredentialCipher.decrypt(cfg.getProperty("source.db.password", ""));
        this.jdbcUrl = String.format("jdbc:mysql://%s:%s/?%s"
                        + "&serverTimezone=UTC&characterEncoding=utf8"
                        + "&allowPublicKeyRetrieval=true&connectTimeout=15000&socketTimeout=120000",
                host, port, SslMaterial.from(cfg, "source").mysqlUrlParams());

        this.rotateIntervalMs = longProp(cfg, "traffic.capture.rotate.interval.ms", 2000L);
        this.rotateIntervalMinMs = Math.max(200L, rotateIntervalMs / 8);
        this.rotateIntervalMaxMs = Math.max(rotateIntervalMs, rotateIntervalMs * 4);
        this.rotateRowsHighWater = longProp(cfg, "traffic.capture.rotate.rows", 200_000L);

        connect();
        acquireSourceLock();
        readFingerprint();
        // 原点必须在开启日志<b>之前</b>取：先取原点再开日志，最坏情况是漏掉极短一瞬的语句；
        // 反过来会让最早几条语句算出负偏移。
        this.t0Micros = readSourceNowMicros();
        enableGeneralLog(cfg);
        dropStaleRotationTables();
        this.lastRotateAt = System.currentTimeMillis();
        logger.info("general_log 捕获已开启: url={}, t0={}us, 轮转间隔={}ms", jdbcUrl, t0Micros, rotateIntervalMs);
    }

    /** 录制原点（源库 epoch 微秒）。 */
    public long t0Micros() {
        return t0Micros;
    }

    private void connect() throws SQLException {
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        try (Statement st = conn.createStatement()) {
            // 第一件事：让本连接从 general_log 里消失。晚一步就会开始自激。
            st.execute("SET SESSION sql_log_off = 1");
            try (ResultSet rs = st.executeQuery("SELECT CONNECTION_ID()")) {
                if (rs.next()) {
                    ownThreadIds.add(rs.getLong(1));
                }
            }
        }
    }

    /**
     * 源库级互斥。两个捕获任务同时轮转同一个 {@code mysql.general_log}，
     * 谁先 RENAME 谁就把对方还没读的语句整表拿走——两边都静默丢数据，且都显示正常。
     */
    private void acquireSourceLock() throws SQLException {
        acquireSourceLock(LOCK_WAIT_MS);
    }

    /**
     * 源库级互斥。两个捕获任务同时轮转同一个 {@code mysql.general_log}，
     * 谁先 RENAME 谁就把对方还没读的语句整表拿走——两边都静默丢数据，且都显示正常。
     *
     * <p>抢不到时要<b>等一会儿再试</b>，不能立刻判死：锁是会话级的，随连接断开释放，
     * 而"上一个捕获任务刚停、它的连接还没被服务端回收"是完全正常的几秒窗口。
     * 立刻失败会让"停掉再重启同一个任务"这种最普通的操作报错。
     */
    private void acquireSourceLock(long waitMs) throws SQLException {
        long deadline = System.currentTimeMillis() + Math.max(0, waitMs);
        SQLException last = null;
        while (true) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT GET_LOCK('" + SOURCE_LOCK_NAME + "', 1)")) {
                if (rs.next() && rs.getInt(1) == 1) {
                    return;
                }
                last = new SQLException("源库上已有另一个流量复制任务在运行（互斥锁 "
                        + SOURCE_LOCK_NAME + " 被占用）。同一个源实例同时捕获会让两边都丢数据，已拒绝启动");
            }
            if (System.currentTimeMillis() >= deadline) {
                throw last;
            }
            logger.info("源库互斥锁被占用（可能是上一个捕获任务的连接还没释放），继续等待…");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw last;
            }
        }
    }

    private void readFingerprint() throws SQLException {
        try (Statement st = conn.createStatement()) {
            fingerprint.serverUuid = scalar(st, "SELECT @@server_uuid");
            fingerprint.version = scalar(st, "SELECT VERSION()");
            fingerprint.serverId = longOf(scalar(st, "SELECT @@server_id"));
            fingerprint.sqlMode = scalar(st, "SELECT @@GLOBAL.sql_mode");
            fingerprint.timeZone = scalar(st, "SELECT @@GLOBAL.time_zone");
            fingerprint.charset = scalar(st, "SELECT @@GLOBAL.character_set_server");
            fingerprint.collation = scalar(st, "SELECT @@GLOBAL.collation_server");
            fingerprint.lowerCaseTableNames = (int) longOf(scalar(st, "SELECT @@GLOBAL.lower_case_table_names"));
        }
    }

    private long readSourceNowMicros() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT UNIX_TIMESTAMP(NOW(6))")) {
            rs.next();
            return toMicros(rs.getBigDecimal(1));
        }
    }

    /**
     * 开日志前把原值记下来。这两个值随后要写进元数据库（{@code traffic_task_config}），
     * 因为"还原"这件事必须在<b>本进程已经不存在</b>的情况下也做得到——
     * 只放内存里，agent 硬崩后就再没人知道该还原成什么，源库的 general_log 会一直开着写满磁盘。
     */
    private void enableGeneralLog(Properties cfg) throws SQLException {
        try (Statement st = conn.createStatement()) {
            originalGeneralLog = scalar(st, "SELECT @@GLOBAL.general_log");
            originalLogOutput = scalar(st, "SELECT @@GLOBAL.log_output");
            logger.info("源库 general_log 原值: general_log={}, log_output={}", originalGeneralLog, originalLogOutput);

            boolean alreadyTable = originalLogOutput != null
                    && originalLogOutput.toUpperCase(java.util.Locale.ROOT).contains("TABLE");
            try {
                if (!alreadyTable) {
                    st.execute("SET GLOBAL log_output = 'TABLE'");
                }
                if (!"1".equals(originalGeneralLog) && !"ON".equalsIgnoreCase(originalGeneralLog)) {
                    st.execute("SET GLOBAL general_log = 'ON'");
                }
            } catch (SQLException e) {
                com.migration.traffic.TrafficErrorStatus.report(taskId, "E3120",
                        "开启源库语句日志失败（需要 SUPER 或 SYSTEM_VARIABLES_ADMIN）: " + e.getMessage());
                throw e;
            }
            if ("1".equals(originalGeneralLog) || "ON".equalsIgnoreCase(originalGeneralLog)) {
                logger.warn("源库 general_log 本来就是开的——可能有别的系统也在读这张表，"
                        + "本任务的轮转会把它读不到的数据一起拿走");
            }
            stateChanged = true;
        }
    }

    /** 上一轮跑崩了可能留下冻结表；RENAME 的目标已存在会直接失败，先清掉。 */
    private void dropStaleRotationTables() {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + FROZEN_TABLE);
            st.execute("DROP TABLE IF EXISTS " + NEXT_TABLE);
        } catch (SQLException e) {
            logger.warn("清理残留轮转表失败（不影响启动，RENAME 时会再报）: {}", e.getMessage());
        }
    }

    @Override
    public List<RawStatement> poll() throws Exception {
        if (closed) return List.of();
        long now = System.currentTimeMillis();
        if (now - lastRotateAt < rotateIntervalMs) {
            return List.of();
        }
        lastRotateAt = now;
        ensureConnection();
        rotate();
        List<RawStatement> batch = drainFrozen();
        adaptInterval(batch.size());
        lastBatchRows = batch.size();
        return batch;
    }

    /**
     * 自适应轮转间隔：不靠 {@code SELECT COUNT(*)} 判断该不该轮转——
     * CSV 表没有索引，COUNT(*) 是全表扫描，在高 QPS 源库上比轮转本身还贵。
     * 改用"上一批读了多少行"反推：接近高水位就缩短间隔，清闲就放长。
     */
    private void adaptInterval(int rows) {
        if (rows > rotateRowsHighWater) {
            rotateIntervalMs = Math.max(rotateIntervalMinMs, rotateIntervalMs / 2);
        } else if (rows < rotateRowsHighWater / 8) {
            rotateIntervalMs = Math.min(rotateIntervalMaxMs, rotateIntervalMs + 500);
        }
    }

    private void rotate() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS " + NEXT_TABLE + " LIKE mysql.general_log");
            st.execute("RENAME TABLE mysql.general_log TO " + FROZEN_TABLE
                    + ", " + NEXT_TABLE + " TO mysql.general_log");
        } catch (SQLException e) {
            // 轮转做不了 = 源库的日志表会一路涨到把 datadir 写满。这不是"慢一点"，
            // 是会拖垮源库，必须立刻让人看见而不是埋在日志里。
            com.migration.traffic.TrafficErrorStatus.report(taskId, "E3121",
                    "源库语句日志轮转失败（需要 mysql 库上的 CREATE/DROP/ALTER 权限）: " + e.getMessage());
            throw e;
        }
    }

    private List<RawStatement> drainFrozen() throws SQLException {
        List<RawStatement> out = new ArrayList<>();
        // setFetchSize(MIN_VALUE) 是 MySQL 驱动的流式读开关：一次轮转可能有几十万行、
        // 每行带完整 SQL 文本，全量物化进内存会直接把子进程 OOM 掉。
        try (Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = st.executeQuery(
                    "SELECT UNIX_TIMESTAMP(event_time), user_host, thread_id, command_type, argument FROM "
                            + FROZEN_TABLE)) {
                while (rs.next()) {
                    long threadId = rs.getLong(3);
                    if (ownThreadIds.contains(threadId)) {
                        continue;   // 采集连接自身的 3 行常数噪声
                    }
                    RawStatement r = new RawStatement();
                    r.epochMicros = toMicros(rs.getBigDecimal(1));
                    r.userHost = SessionSchemaTracker.normalizeUserHost(rs.getString(2));
                    r.threadId = threadId;
                    r.commandType = rs.getString(4);
                    byte[] raw = rs.getBytes(5);
                    r.argument = raw == null ? "" : new String(raw, StandardCharsets.UTF_8);
                    out.add(r);
                }
            }
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + FROZEN_TABLE);
        }
        return out;
    }

    private void ensureConnection() throws SQLException {
        try {
            if (conn != null && conn.isValid(3)) return;
        } catch (SQLException ignored) {
            // 落到重连
        }
        logger.warn("采集连接已失效，重连中");
        closeQuietly(conn);
        connect();
        // 重连会掉 GET_LOCK（会话锁），必须重新抢；抢不到说明别的任务接手了，直接抛
        acquireSourceLock(LOCK_WAIT_MS);
    }

    @Override
    public SourceFingerprint fingerprint() {
        return fingerprint;
    }

    @Override
    public long backlog() {
        return lastBatchRows;
    }

    /**
     * 捕获开始时刻已存在连接的默认库快照（{@code connection id → db}）。
     *
     * <p>没有这一步，<b>连接池场景下整个录制的默认库都是未知的</b>：池里的连接在捕获开始前
     * 就建好了，它们的 {@code Connect}/{@code Init DB} 行永远不会再出现，
     * 于是所有非限定表名的语句（{@code SELECT * FROM t1}）在回放时都会撞上
     * "No database selected"，或者更糟——落到目标连接碰巧选中的另一个库上。
     * 而连接池恰恰是生产上的常态。
     */
    public java.util.Map<Long, String> snapshotSessionSchemas() {
        java.util.Map<Long, String> out = new java.util.LinkedHashMap<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT ID, DB FROM information_schema.PROCESSLIST WHERE DB IS NOT NULL")) {
            while (rs.next()) {
                out.put(rs.getLong(1), rs.getString(2));
            }
        } catch (SQLException e) {
            logger.warn("已存在会话的默认库快照失败（这些会话的非限定表名语句将缺少库信息）: {}", e.getMessage());
        }
        return out;
    }

    /** 源库开关是否确认已还原。false = 需要 agent 兜底。 */
    public boolean isRestored() {
        return restored;
    }

    /** 源库上要还原的两个值，供上层落库做兜底还原。 */
    public String originalGeneralLog() {
        return originalGeneralLog;
    }

    public String originalLogOutput() {
        return originalLogOutput;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (conn != null && !conn.isClosed() && stateChanged) {
                restoreSourceState(conn, originalGeneralLog, originalLogOutput);
                restored = true;
            } else if (!stateChanged) {
                restored = true;    // 没改过就无需还原
            }
        } catch (Exception e) {
            logger.error("还原源库 general_log 状态失败——源库可能仍在写语句日志，请人工确认", e);
        }
        try {
            if (conn != null && !conn.isClosed()) {
                try (Statement st = conn.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + FROZEN_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + NEXT_TABLE);
                    st.execute("SELECT RELEASE_LOCK('" + SOURCE_LOCK_NAME + "')");
                }
            }
        } catch (Exception e) {
            logger.warn("清理轮转表/释放互斥锁失败: {}", e.getMessage());
        }
        closeQuietly(conn);
    }

    /**
     * 把 {@code general_log}/{@code log_output} 还原成原值。
     *
     * <p>做成 static 是因为它有<b>第二个调用方</b>：agent 侧的兜底还原——
     * 捕获子进程被 {@code kill -9} 或 agent 整个硬崩时，本进程根本没机会执行 close()，
     * 那时要由别人拿着落库的原值连上源库来做这件事。
     */
    public static void restoreSourceState(Connection c, String generalLog, String logOutput) throws SQLException {
        try (Statement st = c.createStatement()) {
            boolean wasOn = "1".equals(generalLog) || "ON".equalsIgnoreCase(generalLog);
            st.execute("SET GLOBAL general_log = " + (wasOn ? "'ON'" : "'OFF'"));
            if (logOutput != null && !logOutput.isEmpty()) {
                st.execute("SET GLOBAL log_output = '" + logOutput.replace("'", "''") + "'");
            }
            logger.info("源库语句日志已还原: general_log={}, log_output={}", wasOn ? "ON" : "OFF", logOutput);
        }
    }

    // ==================== 小工具 ====================

    private static String scalar(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static long longOf(String s) {
        try {
            return s == null ? 0L : Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * {@code UNIX_TIMESTAMP(...)} 返回带 6 位小数的 DECIMAL —— <b>时区无关的真实 epoch</b>。
     * 不用 {@code getTimestamp()} 是因为那条路要经过驱动的时区换算，
     * 源库 time_zone、连接 serverTimezone、JVM 默认时区三者只要有一个不一致，
     * 算出来的偏移就整体平移几小时，而录制文件看上去完全正常。
     */
    static long toMicros(BigDecimal epochSeconds) {
        if (epochSeconds == null) return 0L;
        return epochSeconds.movePointRight(6).setScale(0, java.math.RoundingMode.HALF_UP).longValueExact();
    }

    private static void closeQuietly(Connection c) {
        try {
            if (c != null) c.close();
        } catch (SQLException ignored) {
            // 关闭失败无处可去
        }
    }

    private static long longProp(Properties p, String key, long def) {
        try {
            String v = p.getProperty(key);
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
