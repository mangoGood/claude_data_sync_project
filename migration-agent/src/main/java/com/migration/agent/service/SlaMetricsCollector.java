package com.migration.agent.service;

import com.migration.agent.util.ConnectionStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * SLA 闭环指标采集（P2-4）。
 *
 * <p>把散在各处、只写进日志或文件的信号收敛成可告警的数字：绝对复制延迟、重放放大量、
 * 近 10 分钟重启次数、冲突数、死信数、磁盘占用。全部<b>只读</b>，且每一项都自己吞异常——
 * 指标采集把一个健康任务打挂是本末倒置（第 3 批就栽过一次）。
 *
 * <p>每个任务一个实例，由 {@code AbstractTaskExecutor} 持有，随任务生命周期存活。
 */
public class SlaMetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(SlaMetricsCollector.class);

    /** 源库时钟偏移的刷新间隔：偏移是"两台机器的钟差"，分钟级刷新足够，不必每次采集都去查源库。 */
    private static final long CLOCK_OFFSET_REFRESH_MS = 60_000L;

    private final String taskId;
    private final String sourceConnectionString;

    /** sourceNow − localNow，毫秒。用它把本地时钟换算成源库时钟，避免每次采集都打源库一次。 */
    private volatile long clockOffsetMs = 0;
    private volatile boolean clockOffsetKnown = false;
    private volatile long clockOffsetAt = 0;

    /** 行数缓存：(文件长度) → 行数，文件没变就不重数（死信/冲突文件可能很长）。 */
    private long conflictFileLen = -1;
    private long conflictLines = 0;
    private long deadletterFileLen = -1;
    private long deadletterLines = 0;

    public SlaMetricsCollector(String taskId, String sourceConnectionString) {
        this.taskId = taskId;
        this.sourceConnectionString = sourceConnectionString;
    }

    /**
     * 绝对口径的复制延迟：源库当前时刻 − 已应用事件的源端时刻。
     *
     * <p>为什么不能只用现有的 RPO：RPO 是"最新<b>捕获</b>事件时刻 − 已应用事件时刻"，
     * 源库空闲时分子分母都停在同一个值上，恒等于 0——链路整段卡死时它也是 0，
     * 这正是最需要告警的场景。用源库时钟当分子，卡多久涨多久。
     *
     * @param lastAppliedSourceTs 已应用事件的源端时间戳（毫秒），来自 rto_metric
     * @return 延迟毫秒；无法计算时返回 -1
     */
    public long replicationLagMs(Long lastAppliedSourceTs) {
        if (lastAppliedSourceTs == null || lastAppliedSourceTs <= 0) {
            return -1;
        }
        long sourceNow = sourceNowMs();
        if (sourceNow <= 0) {
            return -1;
        }
        return Math.max(0, sourceNow - lastAppliedSourceTs);
    }

    /** 源库"现在几点"。按偏移量缓存：偏移过期才真去查一次源库。 */
    private long sourceNowMs() {
        long now = System.currentTimeMillis();
        if (clockOffsetKnown && now - clockOffsetAt < CLOCK_OFFSET_REFRESH_MS) {
            return now + clockOffsetMs;
        }
        Long queried = querySourceClock();
        if (queried == null) {
            // 查不到就沿用上一次的偏移（源库短暂不可达时指标不该跳变）；从没查到过则放弃
            return clockOffsetKnown ? now + clockOffsetMs : -1;
        }
        clockOffsetMs = queried - System.currentTimeMillis();
        clockOffsetKnown = true;
        clockOffsetAt = now;
        return queried;
    }

    private Long querySourceClock() {
        ConnectionStringParser.ConnectionInfo info;
        try {
            info = ConnectionStringParser.parse(sourceConnectionString);
        } catch (Exception e) {
            return null;
        }
        if (info == null) {
            return null;
        }
        String type = info.getType() == null ? "mysql" : info.getType().toLowerCase();
        String url;
        String sql;
        switch (type) {
            case "postgresql" -> {
                url = "jdbc:postgresql://" + info.getHost() + ":" + info.getPort() + "/" + info.getDatabase();
                sql = "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint";
            }
            case "oracle" -> {
                url = "jdbc:oracle:thin:@" + info.getHost() + ":" + info.getPort() + "/" + info.getDatabase();
                sql = "SELECT (CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - DATE '1970-01-01') * 86400000 FROM dual";
            }
            case "mysql", "tidb" -> {
                url = "jdbc:mysql://" + info.getHost() + ":" + info.getPort() + "/" + info.getDatabase()
                        + "?useSSL=false&serverTimezone=UTC&connectTimeout=5000&socketTimeout=5000";
                sql = "SELECT ROUND(UNIX_TIMESTAMP(NOW(3)) * 1000)";
            }
            default -> {
                // Mongo/Redis/ES 等非 SQL 源没有统一的时钟查询，绝对延迟对它们不适用
                return null;
            }
        }
        try (Connection conn = DriverManager.getConnection(url, info.getUsername(), info.getPassword());
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
            logger.debug("[{}] 读取源库时钟失败: {}", taskId, e.getMessage());
        }
        return null;
    }

    /** capture 重启后重新读取的字节数（capture 侧写入该文件）。 */
    public long captureReplayBytes() {
        return readLongFile(new File("./files/" + taskId + "/binlog_output/capture_replay_bytes"));
    }

    /** 冲突累计条数（CDR 裁决记录，每行一条）。 */
    public long conflictCount() {
        File f = new File("./files/" + taskId + "/conflict.jsonl");
        long len = f.exists() ? f.length() : -1;
        if (len == conflictFileLen) {
            return conflictLines;
        }
        conflictLines = countLines(f);
        conflictFileLen = len;
        return conflictLines;
    }

    /** 死信累计条数（每行一条）。 */
    public long deadletterCount() {
        File f = new File("./files/" + taskId + "/deadletter.jsonl");
        long len = f.exists() ? f.length() : -1;
        if (len == deadletterFileLen) {
            return deadletterLines;
        }
        deadletterLines = countLines(f);
        deadletterFileLen = len;
        return deadletterLines;
    }

    /** 任务工作目录占用字节数。 */
    public long diskUsageBytes() {
        File dir = new File("./files/" + taskId);
        if (!dir.exists()) {
            return 0;
        }
        return TaskFilesJanitor.dirSizeBytes(dir);
    }

    private static long countLines(File f) {
        if (f == null || !f.exists()) {
            return 0;
        }
        long lines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
            while (reader.readLine() != null) {
                lines++;
            }
        } catch (Exception e) {
            return 0;
        }
        return lines;
    }

    private static long readLongFile(File f) {
        if (f == null || !f.exists()) {
            return 0;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
            String line = reader.readLine();
            return (line == null || line.trim().isEmpty()) ? 0 : Long.parseLong(line.trim());
        } catch (Exception e) {
            return 0;
        }
    }
}
