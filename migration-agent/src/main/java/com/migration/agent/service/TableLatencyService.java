package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表级同步延迟统计服务
 *
 * <p>按表维度记录同步延迟，生成热力图数据：
 * <ul>
 *   <li>每张表的最后应用时间、最后事件时间、延迟</li>
 *   <li>延迟分布统计（P50/P95/P99）</li>
 *   <li>热力图矩阵：表 × 时间窗口的延迟值</li>
 * </ul>
 *
 * <p>数据来源：increment 进程在执行 SQL 时，通过文件上报每表延迟。
 * 文件路径：./files/{taskId}/binlog_output/table_latency/{tableName}.tsv
 * 格式：appliedTs\teventTs\tlatencyMs\topType
 */
public class TableLatencyService {
    private static final Logger logger = LoggerFactory.getLogger(TableLatencyService.class);

    private static final String TABLE_LATENCY_DIR_TEMPLATE = "./files/%s/binlog_output/table_latency";
    private static final int MAX_HISTORY_POINTS = 60; // 保留最近60个数据点

    // 内存缓存：taskId -> tableName -> 延迟历史
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, List<LatencyRecord>>> latencyCache = new ConcurrentHashMap<>();

    /**
     * 记录表的同步延迟
     */
    public void recordLatency(String taskId, String tableName, long appliedTs, long eventTs, String opType) {
        if (tableName == null || tableName.isEmpty()) return;
        long latencyMs = appliedTs - eventTs;
        if (latencyMs < 0) latencyMs = 0;

        LatencyRecord record = new LatencyRecord(appliedTs, eventTs, latencyMs, opType);

        // 写入文件（持久化）
        writeLatencyToFile(taskId, tableName, record);

        // 更新内存缓存
        latencyCache.computeIfAbsent(taskId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tableName, k -> new ArrayList<>())
                .add(record);

        // 限制历史大小
        List<LatencyRecord> history = latencyCache.get(taskId).get(tableName);
        if (history.size() > MAX_HISTORY_POINTS) {
            synchronized (history) {
                while (history.size() > MAX_HISTORY_POINTS) {
                    history.remove(0);
                }
            }
        }
    }

    /**
     * 获取任务所有表的延迟热力图数据
     */
    public Map<String, Object> getTableLatencyHeatmap(String taskId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("timestamp", System.currentTimeMillis());

        // 先从文件加载（确保最新数据）
        loadFromFiles(taskId);

        ConcurrentHashMap<String, List<LatencyRecord>> tableMap = latencyCache.get(taskId);
        if (tableMap == null || tableMap.isEmpty()) {
            result.put("available", false);
            result.put("tables", Collections.emptyList());
            Map<String, Object> emptySummary = new LinkedHashMap<>();
            emptySummary.put("total_tables", 0);
            emptySummary.put("avg_latency_ms", 0);
            emptySummary.put("max_latency_ms", 0);
            emptySummary.put("bottleneck_table", "");
            result.put("summary", emptySummary);
            return result;
        }

        List<Map<String, Object>> tables = new ArrayList<>();
        long totalLatency = 0;
        long maxLatency = 0;
        String bottleneckTable = null;
        int tableCount = 0;

        for (Map.Entry<String, List<LatencyRecord>> entry : tableMap.entrySet()) {
            String tableName = entry.getKey();
            List<LatencyRecord> records = entry.getValue();
            if (records.isEmpty()) continue;

            tableCount++;
            Map<String, Object> tableData = buildTableLatencyData(tableName, records);
            tables.add(tableData);

            long avgLatency = ((Number) tableData.get("avg_ms")).longValue();
            totalLatency += avgLatency;
            if (avgLatency > maxLatency) {
                maxLatency = avgLatency;
                bottleneckTable = tableName;
            }
        }

        // 按平均延迟降序排序（瓶颈表在前）
        tables.sort((a, b) -> Long.compare(((Number) b.get("avg_ms")).longValue(), ((Number) a.get("avg_ms")).longValue()));

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total_tables", tableCount);
        summary.put("avg_latency_ms", tableCount > 0 ? totalLatency / tableCount : 0);
        summary.put("max_latency_ms", maxLatency);
        summary.put("bottleneck_table", bottleneckTable == null ? "" : bottleneckTable);

        result.put("available", true);
        result.put("tables", tables);
        result.put("summary", summary);
        return result;
    }

    private Map<String, Object> buildTableLatencyData(String tableName, List<LatencyRecord> records) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("table", tableName);

        LatencyRecord latest = records.get(records.size() - 1);
        data.put("latest_ms", latest.latencyMs);
        data.put("latest_op", latest.opType);
        data.put("latest_applied_ts", latest.appliedTs);

        // 统计
        long sum = 0, max = 0;
        List<Long> latencies = new ArrayList<>();
        for (LatencyRecord r : records) {
            sum += r.latencyMs;
            if (r.latencyMs > max) max = r.latencyMs;
            latencies.add(r.latencyMs);
        }
        long avg = records.isEmpty() ? 0 : sum / records.size();

        data.put("avg_ms", avg);
        data.put("max_ms", max);
        data.put("p50_ms", percentile(latencies, 50));
        data.put("p95_ms", percentile(latencies, 95));
        data.put("p99_ms", percentile(latencies, 99));
        data.put("sample_count", records.size());

        // 热力图数据点（最近20个）
        List<Map<String, Object>> heatmap = new ArrayList<>();
        int start = Math.max(0, records.size() - 20);
        for (int i = start; i < records.size(); i++) {
            LatencyRecord r = records.get(i);
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("ts", r.appliedTs);
            point.put("latency", r.latencyMs);
            point.put("op", r.opType);
            heatmap.add(point);
        }
        data.put("heatmap", heatmap);

        // 延迟等级（用于热力图着色）
        data.put("latency_level", classifyLatency(avg));
        return data;
    }

    private String classifyLatency(long avgMs) {
        if (avgMs < 100) return "EXCELLENT";
        if (avgMs < 500) return "GOOD";
        if (avgMs < 1000) return "NORMAL";
        if (avgMs < 5000) return "WARNING";
        return "CRITICAL";
    }

    private long percentile(List<Long> values, int p) {
        if (values.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        if (index < 0) index = 0;
        if (index >= sorted.size()) index = sorted.size() - 1;
        return sorted.get(index);
    }

    private void writeLatencyToFile(String taskId, String tableName, LatencyRecord record) {
        try {
            String dirPath = String.format(TABLE_LATENCY_DIR_TEMPLATE, taskId);
            File dir = new File(dirPath);
            if (!dir.exists()) dir.mkdirs();

            File file = new File(dir, sanitizeTableName(tableName) + ".tsv");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                writer.write(String.format("%d\t%d\t%d\t%s%n", record.appliedTs, record.eventTs, record.latencyMs, record.opType));
            }
        } catch (IOException e) {
            logger.debug("写入表延迟文件失败: {}", e.getMessage());
        }
    }

    private void loadFromFiles(String taskId) {
        String dirPath = String.format(TABLE_LATENCY_DIR_TEMPLATE, taskId);
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) return;

        File[] files = dir.listFiles((d, name) -> name.endsWith(".tsv"));
        if (files == null) return;

        for (File file : files) {
            String tableName = file.getName().replace(".tsv", "");
            List<LatencyRecord> records = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 4) {
                        try {
                            records.add(new LatencyRecord(
                                    Long.parseLong(parts[0]),
                                    Long.parseLong(parts[1]),
                                    Long.parseLong(parts[2]),
                                    parts[3]
                            ));
                        } catch (NumberFormatException ignored) {}
                    }
                }
            } catch (IOException e) {
                logger.debug("读取表延迟文件失败 {}: {}", file.getName(), e.getMessage());
            }

            if (!records.isEmpty()) {
                // 限制历史大小
                if (records.size() > MAX_HISTORY_POINTS) {
                    records = new ArrayList<>(records.subList(records.size() - MAX_HISTORY_POINTS, records.size()));
                }
                latencyCache.computeIfAbsent(taskId, k -> new ConcurrentHashMap<>())
                        .put(tableName, records);
            }
        }
    }

    private String sanitizeTableName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    /** 延迟记录 */
    private static class LatencyRecord {
        final long appliedTs;
        final long eventTs;
        final long latencyMs;
        final String opType;

        LatencyRecord(long appliedTs, long eventTs, long latencyMs, String opType) {
            this.appliedTs = appliedTs;
            this.eventTs = eventTs;
            this.latencyMs = latencyMs;
            this.opType = opType;
        }
    }
}
