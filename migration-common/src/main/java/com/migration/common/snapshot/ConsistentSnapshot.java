package com.migration.common.snapshot;

import com.migration.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 全量一致性快照（P2-3）。
 *
 * <p>此前全量全程 autocommit、逐页 {@code WHERE pk > last LIMIT n}，各页看到的是各自时刻的库，
 * 目标端在全量期间处于"跨时间点拼接"状态。最终仍能收敛（增量位点取在全量之前 + 幂等重放），
 * 但没有"全量结束点 = 某个 GTID/LSN/SCN 的一致快照"这个语义，因此<b>做不了全量完成即校验</b>，
 * 只能等增量追平后再校验。这个类补上这个语义。
 *
 * <p>三种模式（{@code migration.full.snapshot.mode}）：
 * <ul>
 *   <li>{@code NONE}：完全保持旧行为，不记位点也不开快照事务。</li>
 *   <li>{@code GTID_ONLY}（默认）：只在搬运前<b>记一次位点</b>（MySQL GTID/binlog 坐标、
 *       PG LSN、Oracle SCN），不加锁、不改读取路径。零风险，代价是一次查询，
 *       换来"这次全量大致对应哪个位点"的可观测性与排障依据。</li>
 *   <li>{@code CONSISTENT}：真快照。各库手法不同——
 *       <ul>
 *         <li>MySQL 没有"导出快照给别的会话"的能力，只能 {@code FLUSH TABLES WITH READ LOCK}
 *             期间把<b>所有读连接</b>的 {@code START TRANSACTION WITH CONSISTENT SNAPSHOT}
 *             一起开出来再解锁（mydumper 的做法）。因此读连接必须<b>预建成池</b>并全程复用，
 *             不能像默认路径那样每页新建——新会话的快照点在解锁之后，就不是同一个快照了。</li>
 *         <li>PostgreSQL 有 {@code pg_export_snapshot()}，快照可被任意会话导入，
 *             于是每页新建连接的模型可以原样保留（借出时导入快照，归还时提交）。</li>
 *         <li>Oracle 用闪回查询 {@code AS OF SCN}，是<b>逐查询</b>生效的，同样不需要固定连接。</li>
 *         <li>TiDB 走 {@code AS OF TIMESTAMP TIDB_PARSE_TSO(tso)} 历史读，也是逐查询生效，
 *             且因为 TiKV 本身是 MVCC 的，<b>全程不加任何锁</b>——这是几种里代价最低的一条。
 *             代价在另一头：历史版本受 GC 生命期限制，GC 生命期短于全量耗时就会读失败，
 *             故建立时会检查 {@code tikv_gc_life_time}，太短则降级（见 {@link #beginTidb}）。</li>
 *       </ul></li>
 * </ul>
 *
 * <p>TiDB 之所以不能沿用 MySQL 分支：它虽然走 MySQL 协议（dbType 归一成 mysql），
 * 却不支持 {@code FLUSH TABLES WITH READ LOCK} 那套语义，靠 {@link DatabaseConfig#getFlavor()}
 * 区分。
 *
 * <p>位点会落到 {@code files/<taskId>/full_snapshot_position}，供"全量完成即校验"与排障使用。
 */
public class ConsistentSnapshot implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConsistentSnapshot.class);

    public enum Mode { NONE, GTID_ONLY, CONSISTENT }

    /**
     * TiDB 真快照要求的最小 GC 生命期。默认值只有 10 分钟，够不上一次正经全量；
     * 低于 1 小时一律降级为只记位点，免得跑到一半才因历史版本被回收而失败。
     */
    private static final long MIN_TIDB_GC_SECONDS = 3600;

    private final Mode mode;
    private final String dbType;
    /** 具体实现（tidb / mysql / ...）：TiDB 与 MySQL 的 dbType 相同但快照手法完全不同。 */
    private final String flavor;
    private final DatabaseConfig config;
    private final String taskId;

    private String position;
    /** MySQL/TiDB 的 binlog 坐标，供只能按 file:pos 起读的链路（如 MySQL→ES）直接对齐快照点。 */
    private String binlogFile;
    private long binlogPosition = -1;
    /** TiDB CONSISTENT 下的历史读 TSO。 */
    private long tidbTso = -1;
    /** PG 导出的快照 id；持有它的控制事务必须一直开着，否则快照失效。 */
    private String pgSnapshotId;
    private Connection controlConnection;
    /** MySQL CONSISTENT 下预建的快照读会话池；其它模式为 null。 */
    private BlockingQueue<Connection> readerPool;
    private final List<Connection> allReaders = new ArrayList<>();
    /** Oracle CONSISTENT 下的闪回 SCN。 */
    private long scn = -1;

    private ConsistentSnapshot(Mode mode, DatabaseConfig config, String taskId) {
        this.mode = mode;
        this.config = config;
        this.taskId = taskId;
        this.dbType = config.getDbType() == null ? "mysql" : config.getDbType().toLowerCase();
        String f = config.getFlavor();
        this.flavor = f == null ? this.dbType : f.toLowerCase();
    }

    private boolean isTidb() {
        return "tidb".equals(flavor);
    }

    /**
     * 开启快照。任何失败都<b>降级为 NONE</b> 并返回一个可用对象（读取路径与旧行为一致）——
     * 快照是增强项，不能因为源库缺权限（如没有 RELOAD/SELECT ANY DICTIONARY）就让整个全量起不来。
     *
     * @param readerCount CONSISTENT 模式下需要预建的并发读会话数（MySQL 用；其它库忽略）
     */
    public static ConsistentSnapshot begin(DatabaseConfig config, String modeName, int readerCount, String taskId) {
        Mode mode;
        try {
            mode = Mode.valueOf(modeName == null ? "GTID_ONLY" : modeName.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("未知的快照模式 {}，按 GTID_ONLY 处理", modeName);
            mode = Mode.GTID_ONLY;
        }
        ConsistentSnapshot snapshot = new ConsistentSnapshot(mode, config, taskId);
        if (mode == Mode.NONE) {
            return snapshot;
        }
        try {
            snapshot.doBegin(Math.max(1, readerCount));
            snapshot.persistPosition();
            logger.info("全量一致性快照已建立: mode={}, dbType={}, position={}", mode, snapshot.dbType, snapshot.position);
        } catch (Exception e) {
            logger.warn("建立全量一致性快照失败，降级为无快照全量（数据仍最终一致，但没有全量结束点语义）: {}",
                    e.getMessage());
            snapshot.abort();
            return new ConsistentSnapshot(Mode.NONE, config, taskId);
        }
        return snapshot;
    }

    private void doBegin(int readerCount) throws SQLException {
        if (isTidb()) {
            beginTidb();
            return;
        }
        switch (dbType) {
            case "postgresql" -> beginPostgres();
            case "oracle" -> beginOracle();
            default -> beginMysql(readerCount);
        }
    }

    // ==================== TiDB ====================

    /**
     * TiDB：取一个 TSO 作为历史读的时间点。{@code SHOW MASTER STATUS} 在 TiDB 上返回的
     * Position 就是 TSO，取它不需要任何锁。
     *
     * <p>{@code CONSISTENT} 额外要求历史版本在整个全量期间不被 GC 掉。默认 GC 生命期只有 10 分钟，
     * 远短于一次像样的全量，读到一半会报 "GC life time is shorter than transaction duration"。
     * 与其让全量跑到一半失败，不如在建立时就检查并<b>降级为只记位点</b>，同时把该调的参数打进日志
     * ——这与本类"快照是增强项，不能让全量起不来"的原则一致。
     */
    private void beginTidb() throws SQLException {
        controlConnection = newConnection();
        long tso = readTidbTso(controlConnection);
        position = "tso:" + tso;
        if (mode == Mode.GTID_ONLY) {
            return;
        }
        long gcSeconds = readTidbGcLifeTimeSeconds(controlConnection);
        if (gcSeconds >= 0 && gcSeconds < MIN_TIDB_GC_SECONDS) {
            logger.warn("TiDB GC 生命期仅 {}s，短于一次全量的常见耗时，历史读会在中途失效；"
                            + "本次降级为只记位点。要用真快照请先调大：SET GLOBAL tidb_gc_life_time='24h'",
                    gcSeconds);
            return;
        }
        if (tso <= 0) {
            logger.warn("未能取到 TiDB TSO，降级为只记位点");
            return;
        }
        tidbTso = tso;
    }

    /** TiDB 上 {@code SHOW MASTER STATUS} 的 Position 列即 TSO。 */
    private long readTidbTso(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW MASTER STATUS")) {
            if (rs.next()) {
                return rs.getLong(2);
            }
        } catch (SQLException e) {
            logger.debug("读取 TiDB TSO 失败: {}", e.getMessage());
        }
        return -1;
    }

    /** 读 {@code tikv_gc_life_time}（形如 "10m0s"/"24h"）；取不到返回 -1（按"未知"处理，不降级）。 */
    private long readTidbGcLifeTimeSeconds(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_life_time'")) {
            if (rs.next()) {
                return parseGoDuration(rs.getString(1));
            }
        } catch (SQLException e) {
            logger.debug("读取 tikv_gc_life_time 失败: {}", e.getMessage());
        }
        return -1;
    }

    /** 解析 Go 风格时长（"10m0s" / "24h" / "1h30m"）为秒；无法解析返回 -1。 */
    static long parseGoDuration(String value) {
        if (value == null || value.trim().isEmpty()) {
            return -1;
        }
        // ms 必须排在 m 之前，否则 "500ms" 会先匹配到 "500m"（30000 秒）
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(\\d+(?:\\.\\d+)?)\\s*(ms|h|m|s)")
                .matcher(value.trim().toLowerCase());
        long seconds = 0;
        boolean matched = false;
        while (m.find()) {
            matched = true;
            double n = Double.parseDouble(m.group(1));
            switch (m.group(2)) {
                case "h" -> seconds += (long) (n * 3600);
                case "m" -> seconds += (long) (n * 60);
                case "s" -> seconds += (long) n;
                default -> { /* ms：不足 1 秒，忽略 */ }
            }
        }
        return matched ? seconds : -1;
    }

    // ==================== MySQL ====================

    private void beginMysql(int readerCount) throws SQLException {
        controlConnection = newConnection();
        if (mode == Mode.GTID_ONLY) {
            position = readMysqlPosition(controlConnection);
            return;
        }
        boolean locked = false;
        try (Statement st = controlConnection.createStatement()) {
            st.execute("FLUSH TABLES WITH READ LOCK");
            locked = true;
            position = readMysqlPosition(controlConnection);
            readerPool = new ArrayBlockingQueue<>(readerCount);
            for (int i = 0; i < readerCount; i++) {
                Connection reader = newConnection();
                reader.setAutoCommit(false);
                try (Statement rs = reader.createStatement()) {
                    rs.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    // 必须在持锁期间开启：解锁之后再开的事务，快照点已经不是 position 那一刻了
                    rs.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                }
                allReaders.add(reader);
                readerPool.offer(reader);
            }
        } finally {
            if (locked) {
                try (Statement st = controlConnection.createStatement()) {
                    st.execute("UNLOCK TABLES");
                } catch (SQLException e) {
                    logger.warn("释放 FLUSH TABLES WITH READ LOCK 失败（源库将保持只读直到该连接断开）: {}", e.getMessage());
                }
            }
        }
    }

    private String readMysqlPosition(Connection conn) {
        // 无论最终报哪个位点，binlog 坐标都先取下来（按 file:pos 起读的链路要用）
        readMysqlBinlogCoordinates(conn);
        // GTID 优先：跨主从切换仍然可用；没开 GTID 时退回 file:pos
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT @@GLOBAL.gtid_executed")) {
            if (rs.next()) {
                String gtid = rs.getString(1);
                if (gtid != null && !gtid.trim().isEmpty()) {
                    return "gtid:" + gtid.replace("\n", "");
                }
            }
        } catch (SQLException e) {
            logger.debug("读取 gtid_executed 失败: {}", e.getMessage());
        }
        return binlogFile != null ? "binlog:" + binlogFile + ":" + binlogPosition : "unknown";
    }

    /**
     * 记录 binlog 坐标。GTID 之外<b>始终</b>要记 file:pos：MySQL→ES 这类链路的增量客户端只能按
     * file:pos 起读，拿不到坐标就只能在快照之外另取一次位点，那个位点与快照点之间的窗口就是
     * 会丢或会重的事件。
     */
    private void readMysqlBinlogCoordinates(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW MASTER STATUS")) {
            if (rs.next()) {
                binlogFile = rs.getString(1);
                binlogPosition = rs.getLong(2);
            }
        } catch (SQLException e) {
            logger.debug("读取 SHOW MASTER STATUS 失败: {}", e.getMessage());
        }
    }

    // ==================== PostgreSQL ====================

    private void beginPostgres() throws SQLException {
        controlConnection = newConnection();
        if (mode == Mode.GTID_ONLY) {
            position = "lsn:" + queryScalar(controlConnection, "SELECT pg_current_wal_lsn()::text");
            return;
        }
        controlConnection.setAutoCommit(false);
        try (Statement st = controlConnection.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            try (ResultSet rs = st.executeQuery("SELECT pg_export_snapshot()")) {
                if (rs.next()) {
                    pgSnapshotId = rs.getString(1);
                }
            }
        }
        position = "lsn:" + queryScalar(controlConnection, "SELECT pg_current_wal_lsn()::text")
                + ";snapshot:" + pgSnapshotId;
    }

    // ==================== Oracle ====================

    private void beginOracle() throws SQLException {
        controlConnection = newConnection();
        String value = queryScalar(controlConnection, "SELECT CURRENT_SCN FROM V$DATABASE");
        if (value != null) {
            scn = Long.parseLong(value.trim());
        }
        position = "scn:" + scn;
        if (mode == Mode.GTID_ONLY) {
            // 只记位点：不改查询，读到的仍是各页当时的库
            scn = -1;
        }
    }

    // ==================== 读连接 ====================

    /** 是否需要走快照读连接（NONE / 仅记位点时为 false，调用方按原路径每页新建连接）。 */
    public boolean providesReaders() {
        return mode == Mode.CONSISTENT && (readerPool != null || pgSnapshotId != null);
    }

    /**
     * 借出一个快照读连接。
     * MySQL 从预建池里取（不能新建：新会话拿不到同一个快照）；PG 现建现导入快照。
     */
    public Connection borrowReader() throws SQLException {
        if (readerPool != null) {
            try {
                Connection c = readerPool.poll(5, TimeUnit.MINUTES);
                if (c == null) {
                    throw new SQLException("等待快照读连接超时（池已耗尽，说明并发度大于预建连接数）");
                }
                return c;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("等待快照读连接被中断", e);
            }
        }
        Connection conn = newConnection();
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            st.execute("SET TRANSACTION SNAPSHOT '" + pgSnapshotId + "'");
        }
        return conn;
    }

    /** 归还读连接。池化的（MySQL）<b>不能提交</b>——提交即结束快照事务。 */
    public void releaseReader(Connection conn) {
        if (conn == null) {
            return;
        }
        if (readerPool != null) {
            readerPool.offer(conn);
            return;
        }
        try {
            conn.commit();
        } catch (SQLException ignore) {
            // 只读事务，提交失败无副作用
        }
        try {
            conn.close();
        } catch (SQLException ignore) { }
    }

    /**
     * 给表引用加上快照修饰。Oracle 闪回查询是逐查询生效的：
     * {@code SELECT ... FROM "T" AS OF SCN 123 WHERE ...}。其它库返回原样。
     */
    public String decorateTable(String quotedTable) {
        if (mode != Mode.CONSISTENT) {
            return quotedTable;
        }
        if ("oracle".equals(dbType) && scn > 0) {
            return quotedTable + " AS OF SCN " + scn;
        }
        if (isTidb() && tidbTso > 0) {
            // TiDB 历史读：TIDB_PARSE_TSO 把 TSO 还原成时间戳，逐查询生效、不加锁
            return quotedTable + " AS OF TIMESTAMP TIDB_PARSE_TSO(" + tidbTso + ")";
        }
        return quotedTable;
    }

    public Mode getMode() {
        return mode;
    }

    /** 本次全量对应的源端位点（GTID / binlog 坐标 / LSN / SCN / TSO）；未开启快照时为 null。 */
    public String getPosition() {
        return position;
    }

    /** 快照点的 binlog 文件名（MySQL 源且取到时）；否则 null。 */
    public String getBinlogFile() {
        return binlogFile;
    }

    /** 快照点的 binlog 偏移；未取到时为 -1。 */
    public long getBinlogPosition() {
        return binlogPosition;
    }

    /** 位点落盘，供"全量完成即校验"与排障读取。 */
    private void persistPosition() {
        SnapshotPosition.write(taskId, mode.name(), isTidb() ? "tidb" : dbType, position);
    }

    private Connection newConnection() throws SQLException {
        try {
            Class.forName(config.getJdbcDriverClass());
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC 驱动未找到: " + config.getJdbcDriverClass(), e);
        }
        return DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
    }

    private String queryScalar(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private void abort() {
        close();
    }

    /** 结束快照：提交/关闭所有快照事务与控制连接。全量搬完后调用。 */
    @Override
    public void close() {
        for (Connection reader : allReaders) {
            try { reader.commit(); } catch (SQLException ignore) { }
            try { reader.close(); } catch (SQLException ignore) { }
        }
        allReaders.clear();
        readerPool = null;
        if (controlConnection != null) {
            try { controlConnection.commit(); } catch (SQLException ignore) { }
            try { controlConnection.close(); } catch (SQLException ignore) { }
            controlConnection = null;
        }
    }
}
