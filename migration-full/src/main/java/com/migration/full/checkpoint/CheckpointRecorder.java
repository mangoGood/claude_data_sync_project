package com.migration.full.checkpoint;

import com.migration.db.DatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Checkpoint 记录器
 * 在 full migration 开始时记录源库的 binlog position 和 GTID
 */
public class CheckpointRecorder {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointRecorder.class);
    
    private static final String DB_URL_PREFIX = "jdbc:h2:file:";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";
    
    private String dbPath;
    private Connection connection;
    
    public CheckpointRecorder(String dbPath) {
        this.dbPath = dbPath;
        initDatabase();
    }
    
    /**
     * 初始化数据库
     */
    private void initDatabase() {
        try {
            String url = DB_URL_PREFIX + dbPath + ";AUTO_SERVER=TRUE";
            try {
                connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
            } catch (SQLException e) {
                if (e.getMessage() != null && e.getMessage().contains("already in use")) {
                    logger.warn("H2 database locked, cleaning lock files and retrying: {}", dbPath);
                    cleanH2LockFiles(dbPath);
                    connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                } else {
                    throw e;
                }
            }
            
            // 创建 checkpoint 表
            String createTableSql = "CREATE TABLE IF NOT EXISTS checkpoint (" +
                    "id INT PRIMARY KEY," +
                    "filename VARCHAR(255)," +
                    "position BIGINT," +
                    "gtid VARCHAR(255)," +
                    "timestamp BIGINT," +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")";
            
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createTableSql);
            }
            
            logger.info("Checkpoint 数据库初始化成功: {}", dbPath);
        } catch (SQLException e) {
            logger.error("初始化 Checkpoint 数据库失败", e);
            throw new RuntimeException("无法初始化 Checkpoint 数据库", e);
        }
    }

    private void cleanH2LockFiles(String dbPath) {
        String[] extensions = {".lock.db", ".mv.db", ".trace.db"};
        for (String ext : extensions) {
            java.io.File lockFile = new java.io.File(dbPath + ext);
            if (lockFile.exists()) {
                logger.info("Deleting H2 lock file: {}", lockFile.getAbsolutePath());
                lockFile.delete();
            }
        }
    }
    
    /**
     * 从源数据库获取当前的 binlog position 和 GTID
     */
    public BinlogPositionInfo getCurrentPosition(DatabaseConnection sourceConn) {
        String dbType = sourceConn.getConfig().getDbType();
        if ("oracle".equalsIgnoreCase(dbType)) {
            return getCurrentOracleScn(sourceConn);
        }
        if ("postgresql".equalsIgnoreCase(dbType)) {
            return getCurrentPostgresLsn(sourceConn);
        }
        return getCurrentMysqlPosition(sourceConn);
    }

    /**
     * 从 MySQL 源库获取当前 binlog 位点（SHOW MASTER STATUS）及 GTID。
     */
    private BinlogPositionInfo getCurrentMysqlPosition(DatabaseConnection sourceConn) {
        String filename = null;
        long position = -1;
        String gtid = null;

        try (Connection conn = sourceConn.getConnection()) {
            // 获取 binlog position
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
                if (rs.next()) {
                    filename = rs.getString("File");
                    position = rs.getLong("Position");
                    logger.info("当前 binlog 位置: {}:{}", filename, position);
                }
            }

            // 仅 gtid_mode=ON 才记录 GTID：OFF 时 gtid_executed 仍可能非空（历史遗留），
            // 据此让 capture 走 AUTO_POSITION 会被源库拒绝、增量拉取失败。以 gtid_mode 为准。
            boolean gtidOn = false;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT @@global.gtid_mode")) {
                if (rs.next()) {
                    String mode = rs.getString(1);
                    gtidOn = mode != null && mode.toUpperCase().startsWith("ON");
                }
            } catch (SQLException e) {
                logger.warn("查询 gtid_mode 失败，按未开启处理: {}", e.getMessage());
            }
            if (gtidOn) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT @@global.gtid_executed")) {
                    if (rs.next()) {
                        gtid = rs.getString(1);
                        logger.info("当前 GTID: {}", gtid);
                    }
                } catch (SQLException e) {
                    logger.warn("获取 GTID 失败: {}", e.getMessage());
                }
            } else {
                logger.info("源库 gtid_mode 非 ON，不记录 GTID，增量走 file+pos");
            }

        } catch (SQLException e) {
            logger.error("获取 binlog position 失败", e);
            throw new RuntimeException("无法获取 binlog position", e);
        }

        return new BinlogPositionInfo(filename, position, gtid, System.currentTimeMillis());
    }

    /**
     * 从 PostgreSQL 源库获取当前 WAL LSN。
     */
    private BinlogPositionInfo getCurrentPostgresLsn(DatabaseConnection sourceConn) {
        String lsn = null;
        long position = 0;

        try (Connection conn = sourceConn.getConnection();
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT pg_current_wal_lsn()")) {
                if (rs.next()) {
                    lsn = rs.getString(1);
                    logger.info("当前 PostgreSQL WAL LSN: {}", lsn);
                }
            }
            try (ResultSet rs = stmt.executeQuery("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")) {
                if (rs.next()) {
                    position = rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            logger.error("获取 PostgreSQL WAL LSN 失败", e);
            throw new RuntimeException("无法获取 PostgreSQL WAL LSN", e);
        }

        return new BinlogPositionInfo(lsn, position, null, System.currentTimeMillis());
    }

    /**
     * 从 Oracle 源库获取当前 SCN（System Change Number）。
     * SCN 作为增量捕获（LogMiner）的起始位点。
     */
    private BinlogPositionInfo getCurrentOracleScn(DatabaseConnection sourceConn) {
        String scn = null;
        long scnNumeric = 0;

        try (Connection conn = sourceConn.getConnection();
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
                if (rs.next()) {
                    scnNumeric = rs.getLong(1);
                    scn = String.valueOf(scnNumeric);
                    logger.info("当前 Oracle SCN: {}", scn);
                }
            }
            // 如果 CURRENT_SCN 不可用（权限不足），尝试 DBMS_FLASHBACK
            if (scn == null) {
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL")) {
                    if (rs.next()) {
                        scnNumeric = rs.getLong(1);
                        scn = String.valueOf(scnNumeric);
                        logger.info("当前 Oracle SCN (via DBMS_FLASHBACK): {}", scn);
                    }
                } catch (SQLException e) {
                    logger.warn("通过 DBMS_FLASHBACK 获取 SCN 失败: {}", e.getMessage());
                }
            }
        } catch (SQLException e) {
            logger.error("获取 Oracle SCN 失败", e);
            throw new RuntimeException("无法获取 Oracle SCN", e);
        }

        if (scn == null) {
            logger.warn("无法获取 Oracle SCN，将使用 null 作为起始位点");
        }

        // filename 字段复用为 SCN 字符串，position 字段为 SCN 数值
        return new BinlogPositionInfo(scn, scnNumeric, null, System.currentTimeMillis());
    }
    
    /**
     * 保存 checkpoint
     */
    public void saveCheckpoint(BinlogPositionInfo positionInfo) {
        if (positionInfo == null) {
            logger.warn("尝试保存空的位点信息");
            return;
        }
        
        String sql = "MERGE INTO checkpoint (id, filename, position, gtid, timestamp) " +
                "VALUES (1, ?, ?, ?, ?)";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, positionInfo.getFilename());
            stmt.setLong(2, positionInfo.getPosition());
            stmt.setString(3, positionInfo.getGtid());
            stmt.setLong(4, positionInfo.getTimestamp());
            
            stmt.executeUpdate();
            logger.info("保存 Checkpoint 成功: {}", positionInfo);
        } catch (SQLException e) {
            logger.error("保存 Checkpoint 失败", e);
            throw new RuntimeException("无法保存 Checkpoint", e);
        }
    }
    
    /**
     * 记录源数据库的快照位点
     */
    public void recordSnapshot(DatabaseConnection sourceConn) {
        logger.info("========================================");
        logger.info("开始记录源数据库快照位点");
        logger.info("========================================");
        
        BinlogPositionInfo positionInfo = getCurrentPosition(sourceConn);
        saveCheckpoint(positionInfo);
        
        logger.info("========================================");
        logger.info("源数据库快照位点记录完成");
        logger.info("========================================");
    }
    
    /**
     * 关闭数据库连接
     */
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Checkpoint 数据库连接已关闭");
            } catch (SQLException e) {
                logger.error("关闭 Checkpoint 数据库连接失败", e);
            }
        }
    }
    
    /**
     * Binlog 位点信息
     */
    public static class BinlogPositionInfo {
        private String filename;
        private long position;
        private String gtid;
        private long timestamp;
        
        public BinlogPositionInfo(String filename, long position, String gtid, long timestamp) {
            this.filename = filename;
            this.position = position;
            this.gtid = gtid;
            this.timestamp = timestamp;
        }
        
        public String getFilename() { return filename; }
        public long getPosition() { return position; }
        public String getGtid() { return gtid; }
        public long getTimestamp() { return timestamp; }
        
        @Override
        public String toString() {
            return String.format("BinlogPositionInfo{filename='%s', position=%d, gtid='%s', timestamp=%d}",
                    filename, position, gtid, timestamp);
        }
    }
}
