package com.migration.agent.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class CheckpointManager {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointManager.class);
    
    private static final String DB_URL_PREFIX = "jdbc:h2:file:";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";
    
    private String dbPath;
    private Connection connection;
    
    public CheckpointManager(String dbPath) {
        this.dbPath = dbPath;
        initDatabase();
    }
    
    private void initDatabase() {
        try {
            String url = DB_URL_PREFIX + dbPath + ";AUTO_SERVER=TRUE";
            try {
                connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
            } catch (SQLException e) {
                logger.warn("H2 database connection failed: {}, cleaning lock files and retrying", e.getMessage());
                cleanH2LockFiles(dbPath);
                try {
                    connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                } catch (SQLException e2) {
                    logger.warn("H2 database connection still failed after cleaning lock files, trying without AUTO_SERVER");
                    String urlNoAutoServer = DB_URL_PREFIX + dbPath;
                    cleanH2LockFiles(dbPath);
                    connection = DriverManager.getConnection(urlNoAutoServer, DB_USER, DB_PASSWORD);
                }
            }
            
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
    
    public BinlogPositionInfo loadCheckpoint() {
        String sql = "SELECT filename, position, gtid, timestamp FROM checkpoint WHERE id = 1";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                String filename = rs.getString("filename");
                long position = rs.getLong("position");
                String gtid = rs.getString("gtid");
                long timestamp = rs.getLong("timestamp");
                
                BinlogPositionInfo binlogPosition = new BinlogPositionInfo(filename, position, gtid, timestamp);
                logger.info("加载 Checkpoint 成功: {}", binlogPosition);
                return binlogPosition;
            }
        } catch (SQLException e) {
            logger.error("加载 Checkpoint 失败", e);
        }
        
        logger.info("未找到 Checkpoint 记录");
        return null;
    }
    
    public void saveCheckpoint(BinlogPositionInfo position) {
        if (position == null) {
            logger.warn("尝试保存空的位点信息");
            return;
        }
        
        String sql = "MERGE INTO checkpoint (id, filename, position, gtid, timestamp) " +
                "VALUES (1, ?, ?, ?, ?)";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, position.getFilename());
            stmt.setLong(2, position.getPosition());
            stmt.setString(3, position.getGtid());
            stmt.setLong(4, position.getTimestamp());
            
            stmt.executeUpdate();
            logger.info("保存 Checkpoint 成功: {}", position);
        } catch (SQLException e) {
            logger.error("保存 Checkpoint 失败", e);
            throw new RuntimeException("无法保存 Checkpoint", e);
        }
    }
    
    public BinlogPositionInfo getCurrentPositionFromSource(String sourceHost, int sourcePort, 
                                                           String sourceUser, String sourcePassword) {
        String filename = null;
        long position = -1;
        String gtid = null;
        
        String url = "jdbc:mysql://" + sourceHost + ":" + sourcePort + "?useSSL=false&serverTimezone=UTC&characterEncoding=utf8";
        
        try (Connection conn = DriverManager.getConnection(url, sourceUser, sourcePassword)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
                if (rs.next()) {
                    filename = rs.getString("File");
                    position = rs.getLong("Position");
                    logger.info("当前 binlog 位置: {}:{}", filename, position);
                }
            }
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT @@global.gtid_executed")) {
                if (rs.next()) {
                    gtid = rs.getString(1);
                    logger.info("当前 GTID: {}", gtid);
                }
            } catch (SQLException e) {
                logger.warn("获取 GTID 失败，可能未开启 GTID 模式: {}", e.getMessage());
            }
            
        } catch (SQLException e) {
            logger.error("获取 binlog position 失败", e);
            throw new RuntimeException("无法获取 binlog position", e);
        }
        
        return new BinlogPositionInfo(filename, position, gtid, System.currentTimeMillis());
    }

    public BinlogPositionInfo getCurrentPositionFromPostgres(String sourceHost, int sourcePort,
                                                              String sourceUser, String sourcePassword) {
        String lsn = null;
        long position = 0;

        String url = "jdbc:postgresql://" + sourceHost + ":" + sourcePort + "/postgres?stringtype=unspecified";

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.warn("PostgreSQL JDBC 驱动未找到，尝试使用默认驱动加载");
        }

        try (Connection conn = DriverManager.getConnection(url, sourceUser, sourcePassword)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT pg_current_wal_lsn()")) {
                if (rs.next()) {
                    lsn = rs.getString(1);
                    logger.info("当前 PostgreSQL WAL LSN: {}", lsn);
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT pg_current_wal_lsn()::pg_lsn::text")) {
                if (rs.next()) {
                    String lsnText = rs.getString(1);
                    try {
                        position = parseLsnToLong(lsnText);
                        logger.info("解析 WAL LSN 为数值: {}", position);
                    } catch (Exception e) {
                        logger.warn("解析 WAL LSN 数值失败，使用默认值 0: {}", e.getMessage());
                    }
                }
            } catch (SQLException e) {
                logger.warn("获取 WAL LSN 数值失败: {}", e.getMessage());
            }

        } catch (SQLException e) {
            logger.error("获取 PostgreSQL WAL LSN 失败", e);
            throw new RuntimeException("无法获取 PostgreSQL WAL LSN", e);
        }

        return new BinlogPositionInfo(lsn, position, null, System.currentTimeMillis());
    }

    /**
     * 从 Oracle 源库获取当前 SCN（System Change Number）作为位点。
     *
     * @param sourceHost     Oracle 主机
     * @param sourcePort     Oracle 端口
     * @param sourceDatabase Oracle 服务名或 SID
     * @param sourceUser     用户名
     * @param sourcePassword 密码
     * @return 包含 SCN 的位点信息（filename 字段存储 SCN 字符串）
     */
    public BinlogPositionInfo getCurrentPositionFromOracle(String sourceHost, int sourcePort,
                                                            String sourceDatabase,
                                                            String sourceUser, String sourcePassword) {
        String scn = null;
        long position = 0;

        String url = String.format("jdbc:oracle:thin:@%s:%d/%s", sourceHost, sourcePort, sourceDatabase);

        // 加载并显式注册 Oracle 驱动。
        // 在 fat jar (java -jar) 模式下，DriverManager 的 ServiceLoader 机制可能因
        // ContextClassLoader 问题无法自动发现驱动，因此显式注册更可靠。
        try {
            Class<?> driverClass = Class.forName("oracle.jdbc.OracleDriver");
            java.sql.Driver driver = (java.sql.Driver) driverClass.getDeclaredConstructor().newInstance();
            java.sql.DriverManager.registerDriver(driver);
        } catch (Exception e) {
            logger.warn("注册 Oracle 驱动失败: {}", e.getMessage());
        }

        try (Connection conn = DriverManager.getConnection(url, sourceUser, sourcePassword)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
                if (rs.next()) {
                    position = rs.getLong(1);
                    scn = String.valueOf(position);
                    logger.info("当前 Oracle SCN: {}", scn);
                }
            } catch (SQLException e) {
                logger.warn("获取 CURRENT_SCN 失败: {}", e.getMessage());
            }

            // 如果 CURRENT_SCN 不可用，尝试使用 DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER
            if (scn == null) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL")) {
                    if (rs.next()) {
                        position = rs.getLong(1);
                        scn = String.valueOf(position);
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

        return new BinlogPositionInfo(scn, position, null, System.currentTimeMillis());
    }

    private long parseLsnToLong(String lsn) {
        if (lsn == null || lsn.isEmpty()) {
            return 0;
        }
        String[] parts = lsn.split("/");
        if (parts.length == 2) {
            long segment = Long.parseLong(parts[0], 16);
            long offset = Long.parseLong(parts[1], 16);
            return (segment << 32) | offset;
        }
        return 0;
    }
    
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
