package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 断点续传可视化服务
 *
 * <p>聚合三类位点信息，展示同步进度链路：
 * <ol>
 *   <li>binlog 位点：capture 进程当前读取的 binlog 文件和位置</li>
 *   <li>THL seqno：extract 进程写入 THL 文件的最新序列号</li>
 *   <li>checkpoint：increment 进程已应用的 seqno 和对应 binlog 位点</li>
 * </ol>
 *
 * <p>三者关系：binlog → THL(seqno) → checkpoint(applied seqno)
 * 通过对比可直观看到各阶段积压和断点位置。
 */
public class CheckpointVisualizationService {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointVisualizationService.class);

    private static final String H2_CHECKPOINT_PATH_TEMPLATE = "./files/%s/checkpoint/increment_checkpoint";
    private static final String BINLOG_POSITION_FILE_TEMPLATE = "./files/%s/binlog_output/capture_position.properties";
    private static final String THL_LATEST_SEQNO_FILE_TEMPLATE = "./files/%s/thl_output/.extractor_seqno";
    private static final String RPO_METRIC_FILE_TEMPLATE = "./files/%s/binlog_output/rpo_metric";
    private static final String RTO_METRIC_FILE_TEMPLATE = "./files/%s/binlog_output/rto_metric";

    /**
     * 获取任务的断点续传可视化数据
     */
    public Map<String, Object> getCheckpointVisualization(String taskId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("timestamp", System.currentTimeMillis());

        // 1. binlog 位点（capture 当前位置）
        Map<String, Object> binlogPosition = readBinlogPosition(taskId);
        result.put("binlog", binlogPosition);

        // 2. THL seqno（extract 最新写入位置）
        Map<String, Object> thlSeqno = readThlLatestSeqno(taskId);
        result.put("thl", thlSeqno);

        // 3. checkpoint（increment 已应用位置）
        Map<String, Object> checkpoint = readCheckpoint(taskId);
        result.put("checkpoint", checkpoint);

        // 4. 计算各阶段差距
        Map<String, Object> gaps = calculateGaps(binlogPosition, thlSeqno, checkpoint);
        result.put("gaps", gaps);

        // 5. RPO/RTO 指标（扁平化到顶层，前端直接读 rpo_ms/rto_ms）
        Map<String, Object> metrics = readRpoRtoMetrics(taskId);
        result.put("rpo_ms", metrics.get("rpoMs"));
        result.put("rto_ms", metrics.get("rtoMs"));
        result.put("metrics", metrics);

        // 6. 链路状态判断
        String linkStatus = determineLinkStatus(binlogPosition, thlSeqno, checkpoint, gaps);
        result.put("linkStatus", linkStatus);

        logger.debug("Checkpoint visualization for task {}: status={}", taskId, linkStatus);
        return result;
    }

    /** 读取 capture 进程的当前 binlog 位点 */
    private Map<String, Object> readBinlogPosition(String taskId) {
        Map<String, Object> binlog = new LinkedHashMap<>();
        binlog.put("available", false);

        String path = String.format(BINLOG_POSITION_FILE_TEMPLATE, taskId);
        File file = new File(path);
        if (!file.exists()) {
            return binlog;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // properties 格式: binlog.file=xxx \n binlog.position=xxx
            java.util.Properties props = new java.util.Properties();
            props.load(reader);
            String fileVal = props.getProperty("binlog.file");
            String posVal = props.getProperty("binlog.position");
            if (fileVal != null && posVal != null && !fileVal.isEmpty() && !posVal.isEmpty()) {
                binlog.put("file", fileVal);
                binlog.put("position", Long.parseLong(posVal.trim()));
                binlog.put("available", true);
                binlog.put("raw", fileVal + ":" + posVal);
            }
        } catch (Exception e) {
            logger.warn("读取 binlog 位点文件失败: {}", e.getMessage());
        }
        return binlog;
    }

    /** 读取 extract 进程写入的最新 THL seqno */
    private Map<String, Object> readThlLatestSeqno(String taskId) {
        Map<String, Object> thl = new LinkedHashMap<>();
        thl.put("available", false);

        String path = String.format(THL_LATEST_SEQNO_FILE_TEMPLATE, taskId);
        File file = new File(path);
        if (!file.exists()) {
            return thl;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            if (line != null && !line.isEmpty()) {
                long seqno = Long.parseLong(line.trim());
                thl.put("seqno", seqno);
                thl.put("available", true);
            }
        } catch (Exception e) {
            logger.warn("读取 THL seqno 文件失败: {}", e.getMessage());
        }
        return thl;
    }

    /** 读取 increment 进程的 checkpoint（H2 数据库） */
    private Map<String, Object> readCheckpoint(String taskId) {
        Map<String, Object> checkpoint = new LinkedHashMap<>();
        checkpoint.put("available", false);

        String dbPath = String.format(H2_CHECKPOINT_PATH_TEMPLATE, taskId);
        File dbFile = new File(dbPath + ".mv.db");
        if (!dbFile.exists()) {
            // 尝试旧格式
            dbFile = new File(dbPath + ".h2.db");
            if (!dbFile.exists()) {
                return checkpoint;
            }
        }

        String url = "jdbc:h2:file:" + dbPath + ";AUTO_SERVER=TRUE;IFEXISTS=TRUE";
        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT seqno, binlog_file, binlog_position, event_id, updated_at FROM checkpoint WHERE id = 1")) {
                if (rs.next()) {
                    checkpoint.put("seqno", rs.getLong("seqno"));
                    checkpoint.put("binlog_file", rs.getString("binlog_file"));
                    checkpoint.put("binlog_position", rs.getLong("binlog_position"));
                    checkpoint.put("event_id", rs.getString("event_id"));
                    java.sql.Timestamp ts = rs.getTimestamp("updated_at");
                    checkpoint.put("updated_at", ts != null ? ts.toString() : "-");
                    checkpoint.put("available", true);
                }
            }
        } catch (SQLException e) {
            logger.warn("读取 checkpoint 数据库失败: {}", e.getMessage());
        }
        return checkpoint;
    }

    /** 计算各阶段差距 */
    private Map<String, Object> calculateGaps(Map<String, Object> binlog, Map<String, Object> thl, Map<String, Object> checkpoint) {
        Map<String, Object> gaps = new LinkedHashMap<>();

        // extract vs checkpoint：未应用的 THL 事件数（前端字段 pending_events）
        boolean thlAvailable = (boolean) thl.getOrDefault("available", false);
        boolean ckptAvailable = (boolean) checkpoint.getOrDefault("available", false);
        if (thlAvailable && ckptAvailable) {
            long thlSeqno = (long) thl.get("seqno");
            long ckptSeqno = (long) checkpoint.get("seqno");
            long pendingApply = thlSeqno - ckptSeqno;
            gaps.put("pending_events", pendingApply);
            gaps.put("pending_apply", pendingApply);
            gaps.put("pendingApplyStatus", pendingApply > 1000 ? "CRITICAL" : pendingApply > 100 ? "WARNING" : "OK");
        } else {
            gaps.put("pending_events", null);
            gaps.put("pending_apply", null);
            gaps.put("pendingApplyStatus", "UNKNOWN");
        }

        // binlog vs checkpoint：未同步的 binlog 位点差距（前端字段 binlog_gap）
        boolean binlogAvailable = (boolean) binlog.getOrDefault("available", false);
        if (binlogAvailable && ckptAvailable) {
            String captureFile = (String) binlog.get("file");
            long capturePos = (long) binlog.get("position");
            String ckptFile = (String) checkpoint.get("binlog_file");
            long ckptPos = (long) checkpoint.get("binlog_position");

            if (captureFile != null && captureFile.equals(ckptFile)) {
                long posGap = capturePos - ckptPos;
                gaps.put("binlog_gap", posGap);
                gaps.put("binlogPositionGap", posGap);
                gaps.put("binlogFileGap", 0);
                gaps.put("binlogGapStatus", posGap > 1_000_000 ? "WARNING" : "OK");
            } else if (captureFile != null && ckptFile != null) {
                // 不同文件，计算文件号差距
                int captureFileNum = extractBinlogFileNum(captureFile);
                int ckptFileNum = extractBinlogFileNum(ckptFile);
                int fileGap = captureFileNum - ckptFileNum;
                gaps.put("binlog_gap", fileGap);
                gaps.put("binlogFileGap", fileGap);
                gaps.put("binlogPositionGap", null);
                gaps.put("binlogGapStatus", fileGap > 5 ? "WARNING" : "OK");
            } else {
                gaps.put("binlog_gap", null);
            }
        } else {
            gaps.put("binlog_gap", null);
            gaps.put("binlogPositionGap", null);
            gaps.put("binlogGapStatus", "UNKNOWN");
        }

        return gaps;
    }

    private int extractBinlogFileNum(String fileName) {
        try {
            String numPart = fileName.replaceAll(".*\\.", "");
            return Integer.parseInt(numPart);
        } catch (Exception e) {
            return 0;
        }
    }

    /** 读取 RPO/RTO 指标文件 */
    private Map<String, Object> readRpoRtoMetrics(String taskId) {
        Map<String, Object> metrics = new LinkedHashMap<>();

        String rpoPath = String.format(RPO_METRIC_FILE_TEMPLATE, taskId);
        String rtoPath = String.format(RTO_METRIC_FILE_TEMPLATE, taskId);

        metrics.put("rpoMs", readMetricValue(rpoPath));
        metrics.put("rtoMs", readMetricValue(rtoPath));
        return metrics;
    }

    private Long readMetricValue(String path) {
        File file = new File(path);
        if (!file.exists()) return null;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            if (line != null && !line.isEmpty()) {
                String[] parts = line.split("\\|");
                if (parts.length >= 2) return Long.parseLong(parts[1].trim());
            }
        } catch (Exception e) {
            logger.debug("读取指标文件失败 {}: {}", path, e.getMessage());
        }
        return null;
    }

    /** 判断链路状态 */
    private String determineLinkStatus(Map<String, Object> binlog, Map<String, Object> thl,
                                        Map<String, Object> checkpoint, Map<String, Object> gaps) {
        boolean binlogOk = (boolean) binlog.getOrDefault("available", false);
        boolean thlOk = (boolean) thl.getOrDefault("available", false);
        boolean ckptOk = (boolean) checkpoint.getOrDefault("available", false);

        if (!binlogOk && !thlOk && !ckptOk) return "NO_DATA";
        if (!binlogOk || !thlOk || !ckptOk) return "PARTIAL";

        String pendingStatus = (String) gaps.getOrDefault("pendingApplyStatus", "UNKNOWN");
        String binlogStatus = (String) gaps.getOrDefault("binlogGapStatus", "UNKNOWN");

        if ("CRITICAL".equals(pendingStatus)) return "CRITICAL";
        if ("WARNING".equals(pendingStatus) || "WARNING".equals(binlogStatus)) return "WARNING";
        return "HEALTHY";
    }
}
