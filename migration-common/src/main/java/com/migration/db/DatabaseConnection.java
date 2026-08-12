package com.migration.db;

import com.migration.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConnection {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnection.class);
    
    private DatabaseConfig config;
    private Connection connection;

    public DatabaseConnection(DatabaseConfig config) {
        this.config = config;
    }

    /**
     * 连接属性。口令与用户名之外，Oracle 的信任材料也只能走这里——它的 thin URL 不带查询串。
     * MySQL/PG 的加密参数已经拼在 URL 上，这里不会重复设置。
     */
    private java.util.Properties connectionProps() {
        java.util.Properties p = new java.util.Properties();
        if (config.getUsername() != null) {
            p.setProperty("user", config.getUsername());
        }
        if (config.getPassword() != null) {
            p.setProperty("password", config.getPassword());
        }
        if (config.isOracle()) {
            config.getSsl().applyOracleProperties(p);
        }
        return p;
    }

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                String driverClass = config.getJdbcDriverClass();
                Class.forName(driverClass);
                connection = DriverManager.getConnection(config.getJdbcUrl(), connectionProps());
                logger.info("成功连接到数据库: {} (type: {}, ssl: {})",
                            config.getDatabase(), config.getDbType(), config.getSslMode());
            } catch (ClassNotFoundException e) {
                logger.error("JDBC 驱动未找到: {}", config.getJdbcDriverClass(), e);
                throw new SQLException("JDBC 驱动未找到: " + config.getJdbcDriverClass(), e);
            } catch (SQLException e) {
                logger.error("连接数据库失败: {} (type: {})", config.getDatabase(), config.getDbType(), e);
                throw e;
            }
        }
        return connection;
    }

    public void close() {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                    logger.info("数据库连接已关闭: {}", config.getDatabase());
                }
            } catch (SQLException e) {
                logger.error("关闭数据库连接失败", e);
            }
        }
    }

    public void execute(String sql) throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute(sql);
        }
    }

    public boolean testConnection() {
        try {
            return getConnection() != null && !getConnection().isClosed();
        } catch (SQLException e) {
            logger.error("测试连接失败", e);
            return false;
        }
    }

    public void ensureDatabaseExists() {
        try {
            String driverClass = config.getJdbcDriverClass();
            Class.forName(driverClass);
            try (Connection conn = DriverManager.getConnection(config.getRootJdbcUrl(), connectionProps());
                 Statement stmt = conn.createStatement()) {
                stmt.execute(config.getCreateDatabaseSql());
                logger.info("确保目标数据库存在: {} (type: {})", config.getDatabase(), config.getDbType());
            }
        } catch (Exception e) {
            logger.warn("创建目标数据库失败（可能已存在）: {}", e.getMessage());
        }
    }
    
    public DatabaseConfig getConfig() {
        return config;
    }
}
