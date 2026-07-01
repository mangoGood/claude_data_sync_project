package com.synctask.service;

import com.synctask.entity.SlowSqlRecord;
import com.synctask.repository.SlowSqlRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 慢SQL检测服务
 * 记录increment同步中执行时间超阈值的SQL
 */
@Service
public class SlowSqlService {
    private static final Logger logger = LoggerFactory.getLogger(SlowSqlService.class);

    /** 默认慢SQL阈值(毫秒) */
    private static final long DEFAULT_THRESHOLD_MS = 1000;

    @Autowired
    private SlowSqlRecordRepository slowSqlRepository;

    @Autowired
    private AlertRuleService alertRuleService;

    /**
     * 记录SQL执行，如果超过阈值则保存为慢SQL
     */
    @Transactional
    public void recordSqlExecution(String workflowId, Long userId, String sql,
                                   long executionTimeMs, String tableName, String sqlType) {
        if (executionTimeMs < DEFAULT_THRESHOLD_MS) return;

        SlowSqlRecord record = new SlowSqlRecord();
        record.setWorkflowId(workflowId);
        record.setUserId(userId);
        record.setSqlText(sql);
        record.setExecutionTimeMs(executionTimeMs);
        record.setTableName(tableName);
        record.setSqlType(sqlType);
        record.setThresholdMs(DEFAULT_THRESHOLD_MS);

        slowSqlRepository.save(record);

        logger.warn("慢SQL检测: workflowId={}, 耗时={}ms, table={}, sql={}",
                workflowId, executionTimeMs, tableName,
                sql.length() > 200 ? sql.substring(0, 200) + "..." : sql);
    }

    public Page<SlowSqlRecord> getSlowSqlRecords(Long userId, int page, int pageSize) {
        Pageable pageable = PageRequest.of(page - 1, pageSize);
        return slowSqlRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
    }

    public List<SlowSqlRecord> getSlowSqlByWorkflow(String workflowId) {
        return slowSqlRepository.findByWorkflowIdOrderByCreatedAtDesc(workflowId);
    }

    /**
     * 获取慢SQL统计
     */
    public java.util.Map<String, Object> getSlowSqlStats(Long userId) {
        List<SlowSqlRecord> records = slowSqlRepository
                .findByUserIdOrderByCreatedAtDesc(userId, PageRequest.of(0, 1000)).getContent();

        java.util.Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("total", records.size());

        if (records.isEmpty()) {
            stats.put("avgTimeMs", 0);
            stats.put("maxTimeMs", 0);
            stats.put("topTable", "-");
            return stats;
        }

        double avg = records.stream().mapToLong(SlowSqlRecord::getExecutionTimeMs).average().orElse(0);
        long max = records.stream().mapToLong(SlowSqlRecord::getExecutionTimeMs).max().orElse(0);

        // 按表分组统计
        java.util.Map<String, Long> tableCount = new java.util.HashMap<>();
        for (SlowSqlRecord r : records) {
            String tbl = r.getTableName() != null ? r.getTableName() : "unknown";
            tableCount.merge(tbl, 1L, Long::sum);
        }
        String topTable = tableCount.entrySet().stream()
                .max(java.util.Map.Entry.comparingByValue())
                .map(java.util.Map.Entry::getKey).orElse("-");

        stats.put("avgTimeMs", (long) avg);
        stats.put("maxTimeMs", max);
        stats.put("topTable", topTable);
        return stats;
    }
}
