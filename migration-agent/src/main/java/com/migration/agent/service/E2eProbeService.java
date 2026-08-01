package com.migration.agent.service;

import com.migration.agent.util.ConnectionStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 端到端探针（P2-4）。
 *
 * <p>现有的活性/延迟指标全是<b>各段自报</b>的：capture 说自己读到哪、increment 说自己应用到哪。
 * 这类指标的共同盲区是"段内自洽但整条链路不通"——比如 THL 一直在产出、位点一直在推进，
 * 目标库却因为某个过滤条件一行都没落。探针换一个视角：往源库写一个带 UUID 的标记行，
 * 计时它<b>出现在目标库</b>的时刻。这是唯一同时证明"链路真的通"和"端到端延迟是多少"的指标。
 *
 * <p>默认<b>关闭</b>（{@code probe.enabled}）：它会在用户的源库里建一张 {@code __sync_probe} 表
 * 并持续写入，这是对源库的副作用，必须由使用者显式同意。开启后 {@link #includeProbeTable}
 * 会把探针表并入任务的同步范围——不并进去的话标记行根本不会被捕获，探针会永远超时。
 *
 * <p>仅支持 MySQL/TiDB/PostgreSQL 源与目标；Oracle（没有 CREATE TABLE IF NOT EXISTS）
 * 与 Mongo/Redis/ES 这些非 SQL 目标直接跳过，不做半吊子适配。
 */
public class E2eProbeService implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(E2eProbeService.class);

    public static final String PROBE_TABLE = "__sync_probe";
    /** 探针行保留时长，超过即在源端删除（删除动作本身也会流过链路，顺带覆盖 DELETE 路径）。 */
    private static final long RETENTION_MS = 3600_000L;
    private static final long POLL_INTERVAL_MS = 500L;

    private final String taskId;
    private final String sourceConnectionString;
    private final String targetConnectionString;
    private final long intervalMs;
    private final long timeoutMs;
    private final MetricsService.TaskMetrics metrics;

    private volatile boolean running;
    private Thread worker;
    private long lastCleanupAt = 0;

    public E2eProbeService(String taskId, String sourceConnectionString, String targetConnectionString,
                           long intervalMs, long timeoutMs, MetricsService.TaskMetrics metrics) {
        this.taskId = taskId;
        this.sourceConnectionString = sourceConnectionString;
        this.targetConnectionString = targetConnectionString;
        this.intervalMs = Math.max(10_000L, intervalMs);
        this.timeoutMs = Math.max(1_000L, timeoutMs);
        this.metrics = metrics;
    }

    /** 该库类型能否跑探针。 */
    public static boolean supports(String dbType) {
        if (dbType == null) {
            return false;
        }
        String t = dbType.toLowerCase();
        return t.equals("mysql") || t.equals("tidb") || t.equals("postgresql");
    }

    /**
     * 把探针表并入任务的同步范围（表级同步下不并入就永远捕获不到标记行）。
     * 库级同步（value 不是表清单）无需处理——整库都在范围内。
     */
    public static void includeProbeTable(Map<String, Object> syncObjects) {
        if (syncObjects == null || syncObjects.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : syncObjects.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> tables = (List<Object>) value;
                if (!tables.contains(PROBE_TABLE)) {
                    tables.add(PROBE_TABLE);
                    logger.info("端到端探针已开启，探针表 {}.{} 并入同步范围", entry.getKey(), PROBE_TABLE);
                }
                return;   // 只需要一个库承载探针
            }
        }
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        ConnectionStringParser.ConnectionInfo src = parse(sourceConnectionString);
        ConnectionStringParser.ConnectionInfo tgt = parse(targetConnectionString);
        if (src == null || tgt == null || !supports(src.getType()) || !supports(tgt.getType())) {
            logger.info("[{}] 端到端探针不支持当前源/目标类型，跳过", taskId);
            return;
        }
        running = true;
        worker = new Thread(this::loop, "E2eProbe-" + taskId);
        worker.setDaemon(true);
        worker.start();
        logger.info("[{}] 端到端探针已启动，间隔 {}ms，超时 {}ms", taskId, intervalMs, timeoutMs);
    }

    private void loop() {
        ConnectionStringParser.ConnectionInfo src = parse(sourceConnectionString);
        ConnectionStringParser.ConnectionInfo tgt = parse(targetConnectionString);
        try {
            ensureTable(src);
            ensureTable(tgt);
        } catch (Exception e) {
            // 建表失败（权限不足等）就退出探针，不影响任务本体
            logger.warn("[{}] 端到端探针建表失败，探针停止: {}", taskId, e.getMessage());
            running = false;
            return;
        }
        while (running) {
            try {
                long latency = probeOnce(src, tgt);
                metrics.recordProbeLatency(latency);
                writeMetric(latency);
                if (latency < 0) {
                    logger.warn("[{}] 端到端探针超时（{}ms 内标记行未到达目标库）——链路可能整段不通", taskId, timeoutMs);
                }
                maybeCleanup(src);
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.debug("[{}] 端到端探针本轮失败: {}", taskId, e.getMessage());
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /** 写一行、等它出现在目标库。返回延迟毫秒；超时返回 -1。 */
    private long probeOnce(ConnectionStringParser.ConnectionInfo src, ConnectionStringParser.ConnectionInfo tgt)
            throws Exception {
        String probeId = UUID.randomUUID().toString();
        long sentAt = System.currentTimeMillis();
        try (Connection conn = connect(src);
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO " + PROBE_TABLE + " (probe_id, sent_at) VALUES (?, ?)")) {
            ps.setString(1, probeId);
            ps.setLong(2, sentAt);
            ps.executeUpdate();
        }

        long deadline = sentAt + timeoutMs;
        try (Connection conn = connect(tgt);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT sent_at FROM " + PROBE_TABLE + " WHERE probe_id = ?")) {
            while (System.currentTimeMillis() < deadline && running) {
                ps.setString(1, probeId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        // 两端时刻都由本进程的时钟测量，不受源/目标机器钟差影响
                        return System.currentTimeMillis() - sentAt;
                    }
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
        return -1;
    }

    /** 定期清理过期探针行，避免探针表无界增长。 */
    private void maybeCleanup(ConnectionStringParser.ConnectionInfo src) {
        long now = System.currentTimeMillis();
        if (now - lastCleanupAt < RETENTION_MS) {
            return;
        }
        lastCleanupAt = now;
        try (Connection conn = connect(src);
             PreparedStatement ps = conn.prepareStatement(
                     "DELETE FROM " + PROBE_TABLE + " WHERE sent_at < ?")) {
            ps.setLong(1, now - RETENTION_MS);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                logger.debug("[{}] 探针表清理 {} 行", taskId, deleted);
            }
        } catch (Exception e) {
            logger.debug("[{}] 探针表清理失败: {}", taskId, e.getMessage());
        }
    }

    private void ensureTable(ConnectionStringParser.ConnectionInfo info) throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS " + PROBE_TABLE
                + " (probe_id VARCHAR(64) NOT NULL PRIMARY KEY, sent_at BIGINT NOT NULL)";
        try (Connection conn = connect(info); Statement st = conn.createStatement()) {
            st.execute(ddl);
        }
    }

    private Connection connect(ConnectionStringParser.ConnectionInfo info) throws Exception {
        String url;
        if ("postgresql".equalsIgnoreCase(info.getType())) {
            url = "jdbc:postgresql://" + info.getHost() + ":" + info.getPort() + "/" + info.getDatabase();
        } else {
            url = "jdbc:mysql://" + info.getHost() + ":" + info.getPort() + "/" + info.getDatabase()
                    + "?useSSL=false&serverTimezone=UTC&connectTimeout=5000&socketTimeout=10000";
        }
        return DriverManager.getConnection(url, info.getUsername(), info.getPassword());
    }

    private ConnectionStringParser.ConnectionInfo parse(String connectionString) {
        try {
            return ConnectionStringParser.parse(connectionString);
        } catch (Exception e) {
            return null;
        }
    }

    private void writeMetric(long latencyMs) {
        try {
            File dir = new File("./files/" + taskId + "/binlog_output");
            if (!dir.exists() && !dir.mkdirs()) {
                return;
            }
            try (FileWriter w = new FileWriter(new File(dir, "probe_latency_ms"), false)) {
                w.write(String.valueOf(latencyMs));
            }
        } catch (Exception e) {
            logger.debug("[{}] 写探针指标失败: {}", taskId, e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        running = false;
        if (worker != null) {
            worker.interrupt();
            worker = null;
        }
    }
}
