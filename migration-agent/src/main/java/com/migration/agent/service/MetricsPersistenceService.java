package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsPersistenceService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsPersistenceService.class);
    private static MetricsPersistenceService instance;

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final long flushIntervalMs;
    private final int batchSize;

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<MetricsDataPoint>> buffer = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<ProcessStatusDataPoint>> processBuffer = new ConcurrentHashMap<>();
    private volatile long lastFlushTime = 0;
    private volatile boolean initialized = false;

    static class MetricsDataPoint {
        final long ts;
        final long captureRate;
        final long e2eLatencyMs;
        final long rpoMs;
        final long rtoMs;
        final long captureQDepth;
        final long extractQDepth;
        final long applyQDepth;
        final long checkpointLag;
        final long captureEvents;
        final long appliedEvents;

        MetricsDataPoint(long ts, MetricsService.TaskMetrics metrics) {
            this.ts = ts;
            Map<String, Object> snapshot = metrics.toSnapshot();
            this.captureRate = toLong(snapshot.get("captureRate"));
            this.e2eLatencyMs = toLong(snapshot.get("e2eLatency"));
            this.rpoMs = toLong(snapshot.get("rpoMs"));
            this.rtoMs = toLong(snapshot.get("rtoMs"));
            this.captureQDepth = toLong(snapshot.get("captureQueueDepth"));
            this.extractQDepth = toLong(snapshot.get("extractQueueDepth"));
            this.applyQDepth = toLong(snapshot.get("applyQueueDepth"));
            this.checkpointLag = toLong(snapshot.get("checkpointLag"));
            this.captureEvents = toLong(snapshot.get("captureEventsTotal"));
            this.appliedEvents = toLong(snapshot.get("appliedEventsTotal"));
        }

        private static long toLong(Object val) {
            if (val instanceof Number) return ((Number) val).longValue();
            return 0;
        }
    }

    static class ProcessStatusDataPoint {
        final long ts;
        final String processName;
        final String state;
        final long pid;
        final int retryCount;
        final String cbState;

        ProcessStatusDataPoint(long ts, String processName, String state, long pid, int retryCount, String cbState) {
            this.ts = ts;
            this.processName = processName;
            this.state = state;
            this.pid = pid;
            this.retryCount = retryCount;
            this.cbState = cbState;
        }
    }

    private MetricsPersistenceService(String dbUrl, String dbUser, String dbPassword, long flushIntervalMs, int batchSize) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.flushIntervalMs = flushIntervalMs;
        this.batchSize = batchSize;
    }

    public static synchronized MetricsPersistenceService getInstance() {
        return instance;
    }

    public static synchronized void initialize(String dbUrl, String dbUser, String dbPassword, long flushIntervalMs, int batchSize) {
        if (instance == null) {
            instance = new MetricsPersistenceService(dbUrl, dbUser, dbPassword, flushIntervalMs, batchSize);
            instance.checkInitialized();
            instance.startRetentionCleanup();
        }
    }

    /**
     * 每日清理超过保留期的指标数据，防止时序表无限增长。
     * 落盘量估算：每任务 30s 一行、每行约 120B → ~350KB/天/任务（H2 含索引约 2 倍膨胀），
     * 默认保留 7 天 → 每任务稳态上限约 5MB。保留天数可用系统属性/环境变量
     * metrics.retention.days / METRICS_RETENTION_DAYS 调整。
     */
    private void startRetentionCleanup() {
        int retentionDays;
        try {
            retentionDays = Integer.parseInt(System.getProperty("metrics.retention.days",
                    System.getenv().getOrDefault("METRICS_RETENTION_DAYS", "7")));
        } catch (NumberFormatException e) {
            retentionDays = 7;
        }
        final int days = Math.max(1, retentionDays);
        java.util.concurrent.ScheduledExecutorService cleaner =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "metrics-retention-cleaner");
                    t.setDaemon(true);
                    return t;
                });
        cleaner.scheduleAtFixedRate(() -> {
            try {
                cleanupOldMetrics(days);
            } catch (Exception e) {
                logger.warn("Metrics retention cleanup failed: {}", e.getMessage());
            }
        }, 5, 24 * 60, java.util.concurrent.TimeUnit.MINUTES);
        logger.info("Metrics retention cleanup scheduled: keep {} days, run daily", days);
    }

    private void checkInitialized() {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            initialized = true;
            logger.info("MetricsPersistenceService initialized, flushInterval={}ms, batchSize={}", flushIntervalMs, batchSize);
        } catch (SQLException e) {
            initialized = false;
            logger.warn("MetricsPersistenceService DB connection failed, persistence disabled: {}", e.getMessage());
        }
    }

    public void recordMetrics(String taskId, MetricsService.TaskMetrics metrics) {
        if (!initialized) return;
        ConcurrentLinkedQueue<MetricsDataPoint> queue = buffer.computeIfAbsent(taskId, k -> new ConcurrentLinkedQueue<>());
        queue.offer(new MetricsDataPoint(System.currentTimeMillis(), metrics));
        tryFlush();
    }

    public void recordProcessStatus(String taskId, long ts, String processName, String state, long pid, int retryCount, String cbState) {
        if (!initialized) return;
        ConcurrentLinkedQueue<ProcessStatusDataPoint> queue = processBuffer.computeIfAbsent(taskId, k -> new ConcurrentLinkedQueue<>());
        queue.offer(new ProcessStatusDataPoint(ts, processName, state, pid, retryCount, cbState));
    }

    private void tryFlush() {
        long now = System.currentTimeMillis();
        if (now - lastFlushTime < flushIntervalMs) return;

        int totalMetrics = 0;
        for (Map.Entry<String, ConcurrentLinkedQueue<MetricsDataPoint>> entry : buffer.entrySet()) {
            totalMetrics += entry.getValue().size();
        }
        if (totalMetrics == 0) return;

        lastFlushTime = now;
        flushMetrics();
        flushProcessStatus();
    }

    private void flushMetrics() {
        for (Map.Entry<String, ConcurrentLinkedQueue<MetricsDataPoint>> entry : buffer.entrySet()) {
            String taskId = entry.getKey();
            ConcurrentLinkedQueue<MetricsDataPoint> queue = entry.getValue();

            List<MetricsDataPoint> batch = new ArrayList<>();
            while (!queue.isEmpty() && batch.size() < batchSize) {
                MetricsDataPoint dp = queue.poll();
                if (dp != null) batch.add(dp);
            }

            if (batch.isEmpty()) continue;

            String sql = "INSERT INTO task_metrics_ts (task_id, ts, capture_rate, e2e_latency_ms, rpo_ms, rto_ms, " +
                         "capture_q_depth, extract_q_depth, apply_q_depth, checkpoint_lag, capture_events, applied_events) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                for (MetricsDataPoint dp : batch) {
                    stmt.setString(1, taskId);
                    stmt.setLong(2, dp.ts);
                    stmt.setLong(3, dp.captureRate);
                    stmt.setLong(4, dp.e2eLatencyMs);
                    stmt.setLong(5, dp.rpoMs);
                    stmt.setLong(6, dp.rtoMs);
                    stmt.setLong(7, dp.captureQDepth);
                    stmt.setLong(8, dp.extractQDepth);
                    stmt.setLong(9, dp.applyQDepth);
                    stmt.setLong(10, dp.checkpointLag);
                    stmt.setLong(11, dp.captureEvents);
                    stmt.setLong(12, dp.appliedEvents);
                    stmt.addBatch();
                }
                stmt.executeBatch();
                logger.debug("Flushed {} metrics data points for task {}", batch.size(), taskId);
            } catch (SQLException e) {
                logger.warn("Failed to flush metrics for task {}: {}", taskId, e.getMessage());
            }
        }
    }

    private void flushProcessStatus() {
        for (Map.Entry<String, ConcurrentLinkedQueue<ProcessStatusDataPoint>> entry : processBuffer.entrySet()) {
            String taskId = entry.getKey();
            ConcurrentLinkedQueue<ProcessStatusDataPoint> queue = entry.getValue();

            List<ProcessStatusDataPoint> batch = new ArrayList<>();
            while (!queue.isEmpty() && batch.size() < batchSize) {
                ProcessStatusDataPoint dp = queue.poll();
                if (dp != null) batch.add(dp);
            }

            if (batch.isEmpty()) continue;

            String sql = "INSERT INTO task_process_status_ts (task_id, ts, process_name, state, pid, retry_count, cb_state) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                for (ProcessStatusDataPoint dp : batch) {
                    stmt.setString(1, taskId);
                    stmt.setLong(2, dp.ts);
                    stmt.setString(3, dp.processName);
                    stmt.setString(4, dp.state);
                    stmt.setLong(5, dp.pid);
                    stmt.setInt(6, dp.retryCount);
                    stmt.setString(7, dp.cbState);
                    stmt.addBatch();
                }
                stmt.executeBatch();
            } catch (SQLException e) {
                logger.warn("Failed to flush process status for task {}: {}", taskId, e.getMessage());
            }
        }
    }

    public List<Map<String, Object>> queryMetricsHistory(String taskId, long startTs, long endTs, long intervalMs) {
        if (!initialized) return Collections.emptyList();

        long effectiveInterval = intervalMs > 0 ? intervalMs : 30000;

        String sql;
        if (effectiveInterval <= 30000) {
            sql = "SELECT ts, capture_rate, e2e_latency_ms, rpo_ms, rto_ms, " +
                  "capture_q_depth, extract_q_depth, apply_q_depth, checkpoint_lag, " +
                  "capture_events, applied_events " +
                  "FROM task_metrics_ts WHERE task_id = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC";
        } else {
            long bucketMs = effectiveInterval;
            sql = "SELECT FLOOR(ts / " + bucketMs + ") * " + bucketMs + " AS ts_bucket, " +
                  "AVG(capture_rate) AS capture_rate, AVG(e2e_latency_ms) AS e2e_latency_ms, " +
                  "AVG(rpo_ms) AS rpo_ms, AVG(rto_ms) AS rto_ms, " +
                  "AVG(capture_q_depth) AS capture_q_depth, AVG(extract_q_depth) AS extract_q_depth, " +
                  "AVG(apply_q_depth) AS apply_q_depth, AVG(checkpoint_lag) AS checkpoint_lag, " +
                  "MAX(capture_events) AS capture_events, MAX(applied_events) AS applied_events " +
                  "FROM task_metrics_ts WHERE task_id = ? AND ts >= ? AND ts <= ? " +
                  "GROUP BY FLOOR(ts / " + bucketMs + ") ORDER BY ts_bucket ASC";
        }

        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, taskId);
            stmt.setLong(2, startTs);
            stmt.setLong(3, endTs);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("ts", rs.getLong(effectiveInterval <= 30000 ? "ts" : "ts_bucket"));
                    row.put("captureRate", rs.getLong("capture_rate"));
                    row.put("e2eLatency", rs.getLong("e2e_latency_ms"));
                    row.put("rpoMs", rs.getLong("rpo_ms"));
                    row.put("rtoMs", rs.getLong("rto_ms"));
                    row.put("captureQueueDepth", rs.getLong("capture_q_depth"));
                    row.put("extractQueueDepth", rs.getLong("extract_q_depth"));
                    row.put("applyQueueDepth", rs.getLong("apply_q_depth"));
                    row.put("checkpointLag", rs.getLong("checkpoint_lag"));
                    row.put("captureEventsTotal", rs.getLong("capture_events"));
                    row.put("appliedEventsTotal", rs.getLong("applied_events"));
                    result.add(row);
                }
            }
        } catch (SQLException e) {
            logger.warn("Failed to query metrics history for task {}: {}", taskId, e.getMessage());
        }
        return result;
    }

    public List<Map<String, Object>> queryProcessHistory(String taskId, long startTs, long endTs) {
        if (!initialized) return Collections.emptyList();

        String sql = "SELECT ts, process_name, state, pid, retry_count, cb_state " +
                     "FROM task_process_status_ts WHERE task_id = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC";

        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, taskId);
            stmt.setLong(2, startTs);
            stmt.setLong(3, endTs);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("ts", rs.getLong("ts"));
                    row.put("processName", rs.getString("process_name"));
                    row.put("state", rs.getString("state"));
                    row.put("pid", rs.getLong("pid"));
                    row.put("retryCount", rs.getInt("retry_count"));
                    row.put("circuitBreakerState", rs.getString("cb_state"));
                    result.add(row);
                }
            }
        } catch (SQLException e) {
            logger.warn("Failed to query process history for task {}: {}", taskId, e.getMessage());
        }
        return result;
    }

    public int cleanupOldMetrics(int retentionDays) {
        if (!initialized) return 0;
        long cutoffTs = System.currentTimeMillis() - (long) retentionDays * 24 * 3600 * 1000;

        int total = 0;
        String sql = "DELETE FROM task_metrics_ts WHERE ts < ?";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, cutoffTs);
            total += stmt.executeUpdate();
        } catch (SQLException e) {
            logger.warn("Failed to cleanup old metrics: {}", e.getMessage());
        }

        String sql2 = "DELETE FROM task_process_status_ts WHERE ts < ?";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(sql2)) {
            stmt.setLong(1, cutoffTs);
            total += stmt.executeUpdate();
        } catch (SQLException e) {
            logger.warn("Failed to cleanup old process status: {}", e.getMessage());
        }

        if (total > 0) {
            logger.info("Cleaned up {} old metrics records older than {} days", total, retentionDays);
        }
        return total;
    }

    public void shutdown() {
        flushMetrics();
        flushProcessStatus();
        logger.info("MetricsPersistenceService shutdown, all buffered data flushed");
    }
}
