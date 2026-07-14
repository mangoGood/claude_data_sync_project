package com.migration.db;

import com.migration.config.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
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

    /** 密码指纹（SHA-256 前 12 位十六进制）：让池 key 随密码变化，且不在内存 key 保留明文口令。 */
    private static String passwordFingerprint(String password) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest((password == null ? "" : password).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(12);
            for (int i = 0; i < 6; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return String.valueOf(password == null ? 0 : password.hashCode());
        }
    }

    /**
     * Get or create a connection pool for the given JDBC URL and credentials.
     */
    private static HikariDataSource getOrCreatePool(String url, String username, String password) {
        // key 含密码指纹：改密后不会命中旧池、复用旧凭证连接（与 backend DataSourcePoolManager 一致）。
        String key = url + "|" + username + "|" + passwordFingerprint(password);
        // 同 url+user 但密码不同的旧池：关闭并移除
        String stalePrefix = url + "|" + username + "|";
        Iterator<Map.Entry<String, HikariDataSource>> it = pools.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, HikariDataSource> e = it.next();
            if (e.getKey().startsWith(stalePrefix) && !e.getKey().equals(key)) {
                try {
                    e.getValue().close();
                    logger.info("Closed stale pool (credential changed) for: {}", url);
                } catch (Exception ex) {
                    logger.warn("Error closing stale pool: {}", ex.getMessage());
                }
                it.remove();
            }
        }
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
        String prefix = url + "|" + username + "|";
        pools.entrySet().removeIf(e -> {
            if (e.getKey().startsWith(prefix)) {
                try {
                    e.getValue().close();
                    logger.info("Closed HikariCP pool for: {}", url);
                } catch (Exception ex) {
                    logger.warn("Error closing pool: {}", ex.getMessage());
                }
                return true;
            }
            return false;
        });
    }
}
