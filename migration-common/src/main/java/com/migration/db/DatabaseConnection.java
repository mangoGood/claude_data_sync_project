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

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                String driverClass = config.getJdbcDriverClass();
                Class.forName(driverClass);
                connection = DriverManager.getConnection(
                    config.getJdbcUrl(),
                    config.getUsername(),
                    config.getPassword()
                );
                logger.info("成功连接到数据库: {} (type: {})", config.getDatabase(), config.getDbType());
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
            try (Connection conn = DriverManager.getConnection(config.getRootJdbcUrl(), config.getUsername(), config.getPassword());
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
