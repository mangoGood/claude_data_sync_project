package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多目标库分发服务（fan-out）
 *
 * <p>将一个源库的变更同步到多个目标库，支持：
 * <ul>
 *   <li>并行分发：使用线程池并行写入多个目标库</li>
 *   <li>独立错误处理：单个目标库失败不影响其他目标库</li>
 *   <li>分发状态跟踪：记录每个目标库的同步状态和延迟</li>
 *   <li>事务一致性：每个目标库独立事务，保证各自原子性</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>
 * FanoutDispatcherService dispatcher = new FanoutDispatcherService(targetConnections);
 * dispatcher.initialize();
 * FanoutDispatcherService.DispatchResult result = dispatcher.dispatch(sqlStatements, tableName, opType);
 * </pre>
 */
public class FanoutDispatcherService {
    private static final Logger logger = LoggerFactory.getLogger(FanoutDispatcherService.class);

    private final List<TargetConfig> targetConfigs;
    private final List<TargetConnection> targetConnections = new ArrayList<>();
    private final ExecutorService executor;
    private final int parallelism;

    // 分发统计
    private final AtomicInteger totalDispatched = new AtomicInteger(0);
    private final AtomicInteger totalSuccess = new AtomicInteger(0);
    private final AtomicInteger totalFailure = new AtomicInteger(0);
    private final ConcurrentHashMap<String, AtomicInteger> perTargetSuccess = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> perTargetFailure = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> perTargetLastLatency = new ConcurrentHashMap<>();

    public FanoutDispatcherService(List<TargetConfig> targetConfigs) {
        this.targetConfigs = targetConfigs != null ? targetConfigs : Collections.emptyList();
        this.parallelism = Math.max(1, Math.min(this.targetConfigs.size(), 8));
        this.executor = Executors.newFixedThreadPool(this.parallelism, r -> {
            Thread t = new Thread(r, "fanout-dispatcher");
            t.setDaemon(true);
            return t;
        });
    }

    /** 初始化所有目标库连接 */
    public void initialize() throws SQLException {
        for (TargetConfig config : targetConfigs) {
            TargetConnection conn = new TargetConnection(config);
            conn.connect();
            targetConnections.add(conn);
            perTargetSuccess.put(config.getName(), new AtomicInteger(0));
            perTargetFailure.put(config.getName(), new AtomicInteger(0));
            logger.info("Fan-out 目标库已连接: {} -> {}:{}/{}", config.getName(), config.getHost(), config.getPort(), config.getDatabase());
        }
        logger.info("Fan-out 分发服务初始化完成，目标库数量: {}", targetConnections.size());
    }

    /**
     * 并行分发 SQL 到所有目标库
     *
     * @param sqlStatements SQL 语句列表
     * @param tableName     表名（用于统计）
     * @param opType        操作类型
     * @return 分发结果
     */
    public DispatchResult dispatch(List<String> sqlStatements, String tableName, String opType) {
        if (sqlStatements == null || sqlStatements.isEmpty()) {
            return DispatchResult.empty();
        }
        totalDispatched.incrementAndGet();

        List<Future<SingleTargetResult>> futures = new ArrayList<>();
        for (TargetConnection target : targetConnections) {
            futures.add(executor.submit(() -> dispatchToTarget(target, sqlStatements, tableName, opType)));
        }

        int successCount = 0;
        int failureCount = 0;
        List<String> failedTargets = new ArrayList<>();
        long maxLatency = 0;

        for (int i = 0; i < futures.size(); i++) {
            try {
                SingleTargetResult result = futures.get(i).get(30, TimeUnit.SECONDS);
                if (result.success) {
                    successCount++;
                    totalSuccess.incrementAndGet();
                    perTargetSuccess.get(result.targetName).incrementAndGet();
                    perTargetLastLatency.put(result.targetName, result.latencyMs);
                    if (result.latencyMs > maxLatency) maxLatency = result.latencyMs;
                } else {
                    failureCount++;
                    totalFailure.incrementAndGet();
                    perTargetFailure.get(result.targetName).incrementAndGet();
                    failedTargets.add(result.targetName + ": " + result.error);
                    logger.error("Fan-out 分发失败到目标库 {}: {}", result.targetName, result.error);
                }
            } catch (Exception e) {
                failureCount++;
                totalFailure.incrementAndGet();
                String targetName = targetConnections.get(i).config.getName();
                perTargetFailure.get(targetName).incrementAndGet();
                failedTargets.add(targetName + ": " + e.getMessage());
                logger.error("Fan-out 分发异常到目标库 {}", targetName, e);
            }
        }

        return new DispatchResult(successCount, failureCount, maxLatency, failedTargets);
    }

    private SingleTargetResult dispatchToTarget(TargetConnection target, List<String> sqlStatements,
                                                 String tableName, String opType) {
        long start = System.currentTimeMillis();
        String targetName = target.config.getName();
        try {
            Connection conn = target.getConnection();
            boolean origAutoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    for (String sql : sqlStatements) {
                        if (sql == null || sql.trim().isEmpty()) continue;
                        stmt.execute(sql.trim());
                    }
                }
                conn.commit();
                long latency = System.currentTimeMillis() - start;
                logger.debug("Fan-out 分发成功到 {} | table={} | op={} | latency={}ms", targetName, tableName, opType, latency);
                return new SingleTargetResult(true, targetName, null, latency);
            } catch (SQLException e) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                return new SingleTargetResult(false, targetName, e.getMessage(), System.currentTimeMillis() - start);
            } finally {
                try { conn.setAutoCommit(origAutoCommit); } catch (SQLException ignored) {}
            }
        } catch (SQLException e) {
            return new SingleTargetResult(false, targetName, "连接异常: " + e.getMessage(), System.currentTimeMillis() - start);
        }
    }

    /** 获取分发统计 */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("total_dispatched", totalDispatched.get());
        stats.put("total_success", totalSuccess.get());
        stats.put("total_failure", totalFailure.get());
        stats.put("target_count", targetConnections.size());
        stats.put("parallelism", parallelism);

        List<Map<String, Object>> targets = new ArrayList<>();
        for (TargetConfig config : targetConfigs) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("name", config.getName());
            t.put("host", config.getHost());
            t.put("port", config.getPort());
            t.put("database", config.getDatabase());
            t.put("success_count", perTargetSuccess.get(config.getName()).get());
            t.put("failure_count", perTargetFailure.get(config.getName()).get());
            t.put("avg_latency_ms", perTargetLastLatency.getOrDefault(config.getName(), 0L));
            targets.add(t);
        }
        stats.put("targets", targets);
        return stats;
    }

    /** 关闭所有连接 */
    public void shutdown() {
        logger.info("关闭 Fan-out 分发服务...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        for (TargetConnection conn : targetConnections) {
            try { conn.close(); } catch (Exception ignored) {}
        }
        logger.info("Fan-out 分发服务已关闭");
    }

    // ==================== 内部类 ====================

    public static class TargetConfig {
        private String name;
        private String host;
        private int port;
        private String database;
        private String username;
        private String password;

        public TargetConfig(String name, String host, int port, String database, String username, String password) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
        }

        public String getName() { return name; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public String getDatabase() { return database; }
        public String getUsername() { return username; }
        public String getPassword() { return password; }
    }

    private static class TargetConnection {
        final TargetConfig config;
        Connection connection;

        TargetConnection(TargetConfig config) {
            this.config = config;
        }

        void connect() throws SQLException {
            String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true",
                    config.getHost(), config.getPort(), config.getDatabase());
            connection = DriverManager.getConnection(url, config.getUsername(), config.getPassword());
        }

        Connection getConnection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                connect();
            }
            return connection;
        }

        void close() throws SQLException {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    private static class SingleTargetResult {
        final boolean success;
        final String targetName;
        final String error;
        final long latencyMs;

        SingleTargetResult(boolean success, String targetName, String error, long latencyMs) {
            this.success = success;
            this.targetName = targetName;
            this.error = error;
            this.latencyMs = latencyMs;
        }
    }

    /** 分发结果 */
    public static class DispatchResult {
        public final int successCount;
        public final int failureCount;
        public final long maxLatencyMs;
        public final List<String> failedTargets;

        public DispatchResult(int successCount, int failureCount, long maxLatencyMs, List<String> failedTargets) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.maxLatencyMs = maxLatencyMs;
            this.failedTargets = failedTargets;
        }

        static DispatchResult empty() {
            return new DispatchResult(0, 0, 0, Collections.emptyList());
        }

        public boolean isAllSuccess() {
            return failureCount == 0;
        }

        public boolean isPartialFailure() {
            return failureCount > 0 && successCount > 0;
        }

        public boolean isAllFailure() {
            return successCount == 0 && failureCount > 0;
        }
    }
}
