package com.synctask.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HikariCP connection pool manager for user-specified databases.
 * Caches pools by JDBC URL to avoid recreating pools for the same database.
 */
@Component
public class DataSourcePoolManager {
    private static final Logger logger = LoggerFactory.getLogger(DataSourcePoolManager.class);

    private static final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    private static final int MAX_POOL_SIZE = 10;
    private static final int MIN_IDLE = 2;
    private static final long CONNECTION_TIMEOUT_MS = 30000;
    private static final long IDLE_TIMEOUT_MS = 600000;
    private static final long MAX_LIFETIME_MS = 1800000;
    private static final long LEAK_DETECTION_THRESHOLD_MS = 60000;

    /** 密码指纹（SHA-256 前 12 位十六进制）：让池 key 随密码变化，且不在内存 key 中保留明文口令。 */
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

    private static HikariDataSource getOrCreatePool(String url, String username, String password) {
        return getOrCreatePool(url, username, password, null);
    }

    private static HikariDataSource getOrCreatePool(String url, String username, String password,
                                                    java.util.Properties driverProps) {
        // key 必须包含密码指纹：否则改密码后仍命中旧池，复用旧凭证建立的连接，
        // 造成"错误密码也能连接成功"的假象。密码变化时旧池会被替换关闭。
        //
        // 驱动参数搬到 Properties 之后，key 也要带上它们的指纹：URL 不再区分
        // "同一个库、不同 sslMode/超时"这两种连接，只按 URL 建池会让后来者
        // 静默复用前一个参数集——包括**加密档位**，那是安全语义的静默降级。
        String propsFp = driverPropsFingerprint(driverProps);
        String key = url + "|" + username + "|" + passwordFingerprint(password)
                + (propsFp.isEmpty() ? "" : "|" + propsFp);
        // 同 url+user 但密码不同的旧池：关闭并移除（旧密码已失效，池里连接过期后只会报错）
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
            config.setPoolName("backend-pool-" + k.hashCode());
            if (driverProps != null) {
                // user/password 由 Hikari 自己那两个 setter 负责，重复放进
                // dataSourceProperties 只会让"口令到底从哪来"变得难查
                java.util.Properties forDriver = new java.util.Properties();
                for (String n : driverProps.stringPropertyNames()) {
                    if (!"user".equals(n) && !"password".equals(n)) {
                        forDriver.setProperty(n, driverProps.getProperty(n));
                    }
                }
                config.setDataSourceProperties(forDriver);
            }
            logger.info("Created HikariCP pool for: {}", url);
            return new HikariDataSource(config);
        });
    }

    public static Connection getConnection(String url, String username, String password) throws SQLException {
        HikariDataSource ds = getOrCreatePool(url, username, password);
        return ds.getConnection();
    }

    /**
     * 驱动参数走 {@link java.util.Properties} 的重载。
     *
     * <p>URL 里不再拼查询串（见 {@code JdbcConnections}），参数从这里进——
     * 这样即便有人把 {@code ?allowLoadLocalInfile=true} 混进库名，
     * 也只会得到一个"库不存在"，而不是一个被启用的危险驱动参数。
     */
    public static Connection getConnection(String url, String username, String password,
                                           java.util.Properties driverProps) throws SQLException {
        HikariDataSource ds = getOrCreatePool(url, username, password, driverProps);
        return ds.getConnection();
    }

    /** 驱动参数指纹：让池 key 随参数集变化。空/null 返回空串（与旧行为一致）。 */
    private static String driverPropsFingerprint(java.util.Properties props) {
        if (props == null || props.isEmpty()) {
            return "";
        }
        java.util.List<String> names = new java.util.ArrayList<>(props.stringPropertyNames());
        java.util.Collections.sort(names);
        StringBuilder sb = new StringBuilder();
        for (String n : names) {
            // 口令不入指纹：它已经由 passwordFingerprint 覆盖，不必在 key 里出现两次
            if ("password".equals(n)) {
                continue;
            }
            sb.append(n).append('=').append(props.getProperty(n)).append('&');
        }
        try {
            byte[] h = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                out.append(String.format("%02x", h[i]));
            }
            return out.toString();
        } catch (Exception e) {
            return String.valueOf(sb.toString().hashCode());
        }
    }

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
