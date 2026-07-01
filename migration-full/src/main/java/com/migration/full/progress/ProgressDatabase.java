package com.migration.full.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * H2 数据库管理类，用于存储迁移进度
 */
public class ProgressDatabase {
    private static final Logger logger = LoggerFactory.getLogger(ProgressDatabase.class);
    
    private static final String DEFAULT_DB_URL = "jdbc:h2:./migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
    private Connection connection;
    private String dbUrl;

    public ProgressDatabase() {
        this(DEFAULT_DB_URL);
    }
    
    public ProgressDatabase(String dbPath) {
        this.dbUrl = "jdbc:h2:" + dbPath + ";MODE=MySQL;AUTO_SERVER=TRUE";
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            logger.error("H2 JDBC 驱动未找到", e);
            throw new RuntimeException("H2 JDBC 驱动未找到", e);
        }
    }

    /**
     * 初始化数据库连接和表结构
     */
    public void initialize() throws SQLException {
        connection = DriverManager.getConnection(dbUrl, "sa", "");
        createTables();
        logger.info("进度数据库初始化成功，URL: {}", dbUrl);
    }

    /**
     * 创建进度表
     */
    private void createTables() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS migration_progress (" +
                     "id INT AUTO_INCREMENT PRIMARY KEY, " +
                     "table_name VARCHAR(255) NOT NULL UNIQUE, " +
                     "total_rows BIGINT NOT NULL DEFAULT 0, " +
                     "migrated_rows BIGINT NOT NULL DEFAULT 0, " +
                     "last_migrated_id BIGINT DEFAULT NULL, " +
                     "status VARCHAR(50) NOT NULL DEFAULT 'PENDING', " +
                     "start_time TIMESTAMP NOT NULL, " +
                     "last_update_time TIMESTAMP NOT NULL, " +
                     "complete_time TIMESTAMP, " +
                     "error_message TEXT)";
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logger.debug("进度表创建成功");
        }
        
        String taskProgressSql = "CREATE TABLE IF NOT EXISTS task_progress (" +
                                 "task_id VARCHAR(36) PRIMARY KEY, " +
                                 "progress INT NOT NULL DEFAULT 0, " +
                                 "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)";
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(taskProgressSql);
            logger.debug("任务进度表创建成功");
        }
    }

    /**
     * 保存或更新迁移进度
     */
    public void saveProgress(MigrationProgress progress) throws SQLException {
        String sql = "MERGE INTO migration_progress (table_name, total_rows, migrated_rows, " +
                     "last_migrated_id, status, start_time, last_update_time, complete_time, error_message) " +
                     "KEY (table_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, progress.getTableName());
            pstmt.setLong(2, progress.getTotalRows());
            pstmt.setLong(3, progress.getMigratedRows());
            pstmt.setObject(4, progress.getLastMigratedId());
            pstmt.setString(5, progress.getStatus());
            pstmt.setTimestamp(6, Timestamp.valueOf(progress.getStartTime()));
            pstmt.setTimestamp(7, Timestamp.valueOf(progress.getLastUpdateTime()));
            pstmt.setTimestamp(8, progress.getCompleteTime() != null ? 
                              Timestamp.valueOf(progress.getCompleteTime()) : null);
            pstmt.setString(9, progress.getErrorMessage());
            
            pstmt.executeUpdate();
            logger.debug("保存进度: {}", progress);
        }
    }

    /**
     * 获取表的迁移进度
     */
    public MigrationProgress getProgress(String tableName) throws SQLException {
        String sql = "SELECT * FROM migration_progress WHERE table_name = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapRowToProgress(rs);
                }
            }
        }
        
        return null;
    }

    /**
     * 获取所有迁移进度
     */
    public List<MigrationProgress> getAllProgress() throws SQLException {
        List<MigrationProgress> progressList = new ArrayList<>();
        String sql = "SELECT * FROM migration_progress ORDER BY table_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                progressList.add(mapRowToProgress(rs));
            }
        }
        
        return progressList;
    }

    /**
     * 获取未完成的迁移进度
     */
    public List<MigrationProgress> getIncompleteProgress() throws SQLException {
        List<MigrationProgress> progressList = new ArrayList<>();
        String sql = "SELECT * FROM migration_progress WHERE status IN ('PENDING', 'IN_PROGRESS', 'FAILED') " +
                    "ORDER BY table_name";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                progressList.add(mapRowToProgress(rs));
            }
        }
        
        return progressList;
    }

    /**
     * 删除表的迁移进度
     */
    public void deleteProgress(String tableName) throws SQLException {
        String sql = "DELETE FROM migration_progress WHERE table_name = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            pstmt.executeUpdate();
            logger.debug("删除进度: {}", tableName);
        }
    }

    /**
     * 清空所有迁移进度
     */
    public void clearAllProgress() throws SQLException {
        String sql = "DELETE FROM migration_progress";
        
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            logger.info("清空所有迁移进度");
        }
    }

    /**
     * 检查是否有未完成的迁移
     */
    public boolean hasIncompleteProgress() throws SQLException {
        String sql = "SELECT COUNT(*) FROM migration_progress WHERE status IN ('PENDING', 'IN_PROGRESS', 'FAILED')";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        }
        
        return false;
    }

    /**
     * 将 ResultSet 映射为 MigrationProgress 对象
     */
    private MigrationProgress mapRowToProgress(ResultSet rs) throws SQLException {
        MigrationProgress progress = new MigrationProgress();
        progress.setTableName(rs.getString("table_name"));
        progress.setTotalRows(rs.getLong("total_rows"));
        progress.setMigratedRows(rs.getLong("migrated_rows"));
        
        long lastMigratedId = rs.getLong("last_migrated_id");
        if (!rs.wasNull()) {
            progress.setLastMigratedId(lastMigratedId);
        }
        
        progress.setStatus(rs.getString("status"));
        progress.setStartTime(rs.getTimestamp("start_time").toLocalDateTime());
        progress.setLastUpdateTime(rs.getTimestamp("last_update_time").toLocalDateTime());
        
        Timestamp completeTime = rs.getTimestamp("complete_time");
        if (completeTime != null) {
            progress.setCompleteTime(completeTime.toLocalDateTime());
        }
        
        progress.setErrorMessage(rs.getString("error_message"));
        
        return progress;
    }

    /**
     * 保存或更新任务进度
     */
    public void saveTaskProgress(String taskId, int progress) throws SQLException {
        String sql = "MERGE INTO task_progress (task_id, progress, updated_at) " +
                     "KEY (task_id) VALUES (?, ?, CURRENT_TIMESTAMP)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, taskId);
            pstmt.setInt(2, progress);
            pstmt.executeUpdate();
            logger.debug("保存任务进度: taskId={}, progress={}", taskId, progress);
        }
    }

    /**
     * 获取任务进度
     */
    public int getTaskProgress(String taskId) throws SQLException {
        String sql = "SELECT progress FROM task_progress WHERE task_id = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, taskId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("progress");
                }
            }
        }
        
        return 0;
    }

    /**
     * 关闭数据库连接
     */
    public void close() {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                    logger.info("进度数据库连接已关闭");
                }
            } catch (SQLException e) {
                logger.error("关闭进度数据库连接失败", e);
            }
        }
    }

    /**
     * 获取数据库连接
     */
    public Connection getConnection() {
        return connection;
    }
}
