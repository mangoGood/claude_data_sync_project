package com.migration.db;

import com.migration.config.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HikariCP connection pool manager for standalone migration modules.
 * Caches pools by JDBC URL to avoid recreating pools for the same database.
 */
public class ConnectionPoolManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolManager.class);

    private static final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    private static final int MAX_POOL_SIZE = 5;
    private static final int MIN_IDLE = 1;
    private static final long CONNECTION_TIMEOUT_MS = 30000;
    private static final long IDLE_TIMEOUT_MS = 600000;
    private static final long MAX_LIFETIME_MS = 1800000;
    private static final long LEAK_DETECTION_THRESHOLD_MS = 60000;

    /**
     * Get or create a connection pool for the given JDBC URL and credentials.
     */
    private static HikariDataSource getOrCreatePool(String url, String username, String password) {
        String key = url + "|" + username;
        return pools.computeIfAbsent(key, k -> {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(username);
            config.setPassword(password);
            config.setMaximumPoolSize(MAX_POOL_SIZE);
            config.setMinimumIdle(MIN_IDLE);
            config.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
            config.setIdleTimeout(IDLE_TIMEOUT_MS);
            config.setMaxLifetime(MAX_LIFETIME_MS);
            config.setLeakDetectionThreshold(LEAK_DETECTION_THRESHOLD_MS);
            config.setPoolName("migration-pool-" + k.hashCode());
            logger.info("Created HikariCP pool for: {}", url);
            return new HikariDataSource(config);
        });
    }

    /**
     * Get a connection from the pool for the given database.
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        HikariDataSource ds = getOrCreatePool(url, username, password);
        return ds.getConnection();
    }

    /**
     * Get a connection from the pool using a DatabaseConfig.
     */
    public static Connection getConnection(DatabaseConfig config) throws SQLException {
        return getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
    }

    /**
     * Close all pools and release resources. Should be called on shutdown.
     */
    public static void closeAll() {
        pools.forEach((key, ds) -> {
            try {
                ds.close();
                logger.info("Closed HikariCP pool for key: {}", key);
            } catch (Exception e) {
                logger.warn("Error closing pool: {}", key, e);
            }
        });
        pools.clear();
    }

    /**
     * Close a specific pool by URL and username.
     */
    public static void closePool(String url, String username) {
        String key = url + "|" + username;
        HikariDataSource ds = pools.remove(key);
        if (ds != null) {
            ds.close();
            logger.info("Closed HikariCP pool for: {}", url);
        }
    }
}
