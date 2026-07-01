package com.migration.increment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConcurrentSqlExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentSqlExecutor.class);

    private final HikariDataSource dataSource;
    private final ExecutorService executorService;
    private final int threadPoolSize;
    private final AtomicInteger successCount;
    private final AtomicInteger failureCount;
    private final AtomicLong dmlCount;
    private final AtomicLong ddlCount;
    private final AtomicLong dclCount;
    private final long sqlTimeoutSeconds;

    public ConcurrentSqlExecutor(Properties props) {
        String url = props.getProperty("target.mysql.url");
        String user = props.getProperty("target.mysql.user");
        String password = props.getProperty("target.mysql.password");

        if (!url.contains("serverTimezone") && !url.contains("?")) {
            url = url + "?serverTimezone=UTC&useSSL=false";
        } else if (!url.contains("serverTimezone")) {
            url = url + "&serverTimezone=UTC";
        }

        this.threadPoolSize = Integer.parseInt(props.getProperty("concurrent.thread.pool.size", "8"));
        this.sqlTimeoutSeconds = Long.parseLong(props.getProperty("concurrent.sql.timeout.seconds", "30"));

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariConfig.setMaximumPoolSize(threadPoolSize + 2);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);
        hikariConfig.setPoolName("ConcurrentIncrementPool");
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.setAutoCommit(true);

        this.dataSource = new HikariDataSource(hikariConfig);
        this.executorService = Executors.newFixedThreadPool(threadPoolSize,
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "sql-executor-" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                });
        this.successCount = new AtomicInteger(0);
        this.failureCount = new AtomicInteger(0);
        this.dmlCount = new AtomicLong(0);
        this.ddlCount = new AtomicLong(0);
        this.dclCount = new AtomicLong(0);

        logger.info("ConcurrentSqlExecutor initialized - poolSize: {}, dbUrl: {}", threadPoolSize, url);
    }

    public ExecutionResult execute(DependencyGraph graph) {
        long startTime = System.currentTimeMillis();

        logger.info("Starting concurrent SQL execution...");
        logger.info("Total statements: {}, Conflict keys: {}, Avg concurrency: {}",
                graph.getStatementCount(), graph.getConflictKeyCount(),
                String.format("%.2f", graph.getAverageConcurrency()));

        graph.buildDependencies();
        List<List<SqlStatement>> layers = graph.topologicalSort();

        logger.info("Executing {} layers...", layers.size());

        for (int i = 0; i < layers.size(); i++) {
            List<SqlStatement> layer = layers.get(i);
            logger.info("Executing layer {}/{} with {} statements",
                    i + 1, layers.size(), layer.size());

            executeLayer(layer, i + 1, layers.size());
        }

        shutdown();

        long endTime = System.currentTimeMillis();
        ExecutionResult result = new ExecutionResult(
                successCount.get(),
                failureCount.get(),
                endTime - startTime,
                layers.size(),
                dmlCount.get(),
                ddlCount.get(),
                dclCount.get()
        );

        logger.info("Concurrent execution completed. {}", result);
        return result;
    }

    private void executeLayer(List<SqlStatement> layer, int layerNum, int totalLayers) {
        List<SqlStatement> dmlStmts = new ArrayList<>();
        List<SqlStatement> barrierStmts = new ArrayList<>();

        for (SqlStatement stmt : layer) {
            if (stmt.isBarrier()) {
                barrierStmts.add(stmt);
            } else if (stmt.getSql() != null && !stmt.getSql().isEmpty()) {
                dmlStmts.add(stmt);
            }
        }

        if (!dmlStmts.isEmpty()) {
            logger.info("  Layer {}/{}: Executing {} DML statements concurrently (pool size: {})",
                    layerNum, totalLayers, dmlStmts.size(), threadPoolSize);
            executeDmlConcurrently(dmlStmts);
        }

        if (!barrierStmts.isEmpty()) {
            List<SqlStatement> ddlStmts = new ArrayList<>();
            List<SqlStatement> dclStmts = new ArrayList<>();
            List<SqlStatement> commitStmts = new ArrayList<>();

            for (SqlStatement stmt : barrierStmts) {
                switch (stmt.getOperationType()) {
                    case DDL: ddlStmts.add(stmt); break;
                    case DCL: dclStmts.add(stmt); break;
                    case COMMIT: commitStmts.add(stmt); break;
                    default: commitStmts.add(stmt); break;
                }
            }

            if (!ddlStmts.isEmpty()) {
                logger.info("  Layer {}/{}: Executing {} DDL statements sequentially (barrier)",
                        layerNum, totalLayers, ddlStmts.size());
                executeBarriersSequentially(ddlStmts, "DDL");
                ddlCount.addAndGet(ddlStmts.size());
            }

            if (!dclStmts.isEmpty()) {
                logger.info("  Layer {}/{}: Executing {} DCL statements sequentially (barrier)",
                        layerNum, totalLayers, dclStmts.size());
                executeBarriersSequentially(dclStmts, "DCL");
                dclCount.addAndGet(dclStmts.size());
            }

            if (!commitStmts.isEmpty()) {
                for (SqlStatement stmt : commitStmts) {
                    try {
                        executeStatement(stmt);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                        logger.error("Failed to execute transaction boundary: {}", e.getMessage());
                    }
                }
            }
        }
    }

    private void executeDmlConcurrently(List<SqlStatement> dmlStmts) {
        List<Future<StatementResult>> futures = new ArrayList<>();
        for (SqlStatement stmt : dmlStmts) {
            Future<StatementResult> future = executorService.submit(() -> executeStatement(stmt));
            futures.add(future);
        }

        for (Future<StatementResult> future : futures) {
            try {
                StatementResult result = future.get(sqlTimeoutSeconds, TimeUnit.SECONDS);
                if (result.success) {
                    successCount.incrementAndGet();
                    dmlCount.incrementAndGet();
                } else {
                    failureCount.incrementAndGet();
                    logger.error("DML execution failed: {}", result.errorMessage);
                }
            } catch (TimeoutException e) {
                failureCount.incrementAndGet();
                logger.error("DML execution timeout after {}s", sqlTimeoutSeconds);
                future.cancel(true);
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.error("Error getting DML execution result", e);
            }
        }
    }

    private void executeBarriersSequentially(List<SqlStatement> barrierStmts, String type) {
        for (SqlStatement stmt : barrierStmts) {
            try {
                long startMs = System.currentTimeMillis();
                executeStatement(stmt);
                long elapsed = System.currentTimeMillis() - startMs;
                successCount.incrementAndGet();
                String sqlPreview = stmt.getSql() != null && stmt.getSql().length() > 120
                        ? stmt.getSql().substring(0, 120) + "..."
                        : stmt.getSql();
                logger.info("    ✓ [seqno={}] {} executed in {}ms: {}",
                        stmt.getSeqno(), type, elapsed, sqlPreview);
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.error("    ✗ [seqno={}] {} execution failed: {}",
                        stmt.getSeqno(), type, e.getMessage());
            }
        }
    }

    private StatementResult executeStatement(SqlStatement stmt) {
        Connection connection = null;
        Statement statement = null;

        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();

            if (stmt.getSql() == null || stmt.getSql().isEmpty()) {
                return new StatementResult(true, 0, null);
            }

            boolean isAtomicDdl = stmt.isBarrier() && stmt.getSql().trim().toUpperCase().startsWith("USE ");

            if (isAtomicDdl) {
                executeAtomicDdlInStatement(stmt, statement);
            } else {
                String[] sqlParts = stmt.getSql().split(";");
                for (String sqlPart : sqlParts) {
                    String trimmed = sqlPart.trim();
                    if (!trimmed.isEmpty()) {
                        logger.debug("[seqno={}] Executing SQL: {}", stmt.getSeqno(),
                                trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed);
                        statement.execute(trimmed);
                    }
                }
            }

            logger.debug("Successfully executed statement id={}, seqno={}", stmt.getId(), stmt.getSeqno());
            return new StatementResult(true, 0, null);

        } catch (SQLException e) {
            logger.error("Failed to execute statement id={}, seqno={}: {}", stmt.getId(), stmt.getSeqno(), e.getMessage());
            return new StatementResult(false, 0, e.getMessage());

        } finally {
            closeQuietly(statement);
            closeQuietly(connection);
        }
    }

    private void executeAtomicDdlInStatement(SqlStatement stmt, Statement statement) throws SQLException {
        String sql = stmt.getSql();
        String[] parts = sql.split(";");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                logger.debug("[seqno={}] Executing atomic DDL part: {}", stmt.getSeqno(), trimmed);
                statement.execute(trimmed);
            }
        }
        logger.debug("[seqno={}] Atomic DDL execution completed: USE + DDL", stmt.getSeqno());
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.warn("Failed to close resource", e);
            }
        }
    }

    public void shutdown() {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool closed");
        }
    }

    private static class StatementResult {
        final boolean success;
        final int affectedRows;
        final String errorMessage;

        StatementResult(boolean success, int affectedRows, String errorMessage) {
            this.success = success;
            this.affectedRows = affectedRows;
            this.errorMessage = errorMessage;
        }
    }

    public static class ExecutionResult {
        private final int successCount;
        private final int failureCount;
        private final long executionTimeMs;
        private final int layerCount;
        private final long dmlCount;
        private final long ddlCount;
        private final long dclCount;

        public ExecutionResult(int successCount, int failureCount,
                               long executionTimeMs, int layerCount,
                               long dmlCount, long ddlCount, long dclCount) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.executionTimeMs = executionTimeMs;
            this.layerCount = layerCount;
            this.dmlCount = dmlCount;
            this.ddlCount = ddlCount;
            this.dclCount = dclCount;
        }

        public int getSuccessCount() { return successCount; }
        public int getFailureCount() { return failureCount; }
        public long getExecutionTimeMs() { return executionTimeMs; }
        public int getLayerCount() { return layerCount; }
        public long getDmlCount() { return dmlCount; }
        public long getDdlCount() { return ddlCount; }
        public long getDclCount() { return dclCount; }

        @Override
        public String toString() {
            return String.format(
                    "ExecutionResult{success=%d, failure=%d, time=%dms, layers=%d, " +
                    "DML=%d(concurrent), DDL=%d(sequential), DCL=%d(sequential), throughput=%.2f sql/s}",
                    successCount, failureCount, executionTimeMs, layerCount,
                    dmlCount, ddlCount, dclCount,
                    executionTimeMs > 0 ? (double) (successCount + failureCount) * 1000 / executionTimeMs : 0
            );
        }
    }
}
