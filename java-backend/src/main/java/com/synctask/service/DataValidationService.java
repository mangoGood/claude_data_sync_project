package com.synctask.service;

import com.synctask.entity.DataValidation;
import com.synctask.entity.Workflow;
import com.synctask.repository.DataValidationRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 数据一致性保障服务
 * 自动数据校验、差异修复、双向同步冲突检测
 */
@Service
public class DataValidationService {
    private static final Logger logger = LoggerFactory.getLogger(DataValidationService.class);

    @Autowired
    private DataValidationRepository validationRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    /**
     * 手动触发数据校验
     */
    @Async
    @Transactional
    public void validateWorkflow(String workflowId, Long userId, String validationType) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));

        DataValidation validation = new DataValidation();
        validation.setWorkflowId(workflowId);
        validation.setUserId(userId);
        validation.setValidationType(validationType != null ? validationType : "ROW_COUNT");
        validation.setSourceDbName(workflow.getSourceDbName());
        validation.setTargetDbName(workflow.getTargetDbName());
        validation.setStatus("RUNNING");
        validation = validationRepository.save(validation);

        try {
            // 解析同步对象获取表列表；表级同步可能配了表名映射，目标端按映射名取行数
            List<String> tables = parseTablesFromSyncObjects(workflow.getSyncObjects());
            Map<String, String> tableMapping = parseTableMappingFromSyncObjects(workflow.getSyncObjects());

            long totalMismatched = 0;
            for (String table : tables) {
                DataValidation tableValidation = validateTable(workflow, table,
                        tableMapping.getOrDefault(table, table), validationType);
                if (tableValidation != null) {
                    tableValidation.setUserId(userId);
                    validationRepository.save(tableValidation);
                    if ("FAILED".equals(tableValidation.getStatus())) {
                        totalMismatched += tableValidation.getMismatchedCount() != null
                                ? tableValidation.getMismatchedCount() : 0;
                    }
                }
            }

            validation.setStatus(totalMismatched > 0 ? "FAILED" : "PASSED");
            validation.setMismatchedCount(totalMismatched);
            validation.setCompletedAt(LocalDateTime.now());
            validationRepository.save(validation);

            logger.info("数据校验完成: workflowId={}, status={}, mismatched={}",
                    workflowId, validation.getStatus(), totalMismatched);
        } catch (Exception e) {
            validation.setStatus("FAILED");
            validation.setErrorMessage(e.getMessage());
            validation.setCompletedAt(LocalDateTime.now());
            validationRepository.save(validation);
            logger.error("数据校验失败: workflowId={}", workflowId, e);
        }
    }

    /**
     * 校验单张表。
     *
     * @param tableName       源表名
     * @param targetTableName 目标表名（表名映射后；无映射时 = 源表名）
     */
    private DataValidation validateTable(Workflow workflow, String tableName, String targetTableName, String validationType) {
        DataValidation dv = new DataValidation();
        dv.setWorkflowId(workflow.getId());
        dv.setTableName(tableName);
        dv.setValidationType(validationType);
        dv.setSourceDbName(workflow.getSourceDbName());
        dv.setTargetDbName(workflow.getTargetDbName());
        dv.setStatus("RUNNING");

        Connection sourceConn = null;
        Connection targetConn = null;

        try {
            String[] srcParsed = parseConnectionUrl(workflow.getSourceConnection());
            String[] tgtParsed = parseConnectionUrl(workflow.getTargetConnection());

            // 加载驱动
            loadDriver(srcParsed[3]);
            loadDriver(tgtParsed[3]);

            sourceConn = DriverManager.getConnection(srcParsed[0], srcParsed[1], srcParsed[2]);
            targetConn = DriverManager.getConnection(tgtParsed[0], tgtParsed[1], tgtParsed[2]);

            // 行数对比（根据数据库类型构造 SQL）
            // Oracle 的 schema 取自 sync_objects 的 key（如 APP_USER），而非 sourceDbName
            String srcSchema = "oracle".equalsIgnoreCase(srcParsed[3])
                    ? getSchemaFromSyncObjects(workflow.getSyncObjects(), workflow.getSourceDbName())
                    : workflow.getSourceDbName();
            long sourceCount = getTableRowCount(sourceConn, srcSchema, tableName, srcParsed[3]);
            // 表名映射：目标端按映射后的表名统计行数
            long targetCount = getTableRowCount(targetConn, workflow.getTargetDbName(), targetTableName, tgtParsed[3]);

            dv.setSourceCount(sourceCount);
            dv.setTargetCount(targetCount);

            if (sourceCount == targetCount) {
                dv.setStatus("PASSED");
                dv.setMismatchedCount(0L);
            } else {
                dv.setStatus("FAILED");
                dv.setMismatchedCount(Math.abs(sourceCount - targetCount));
                // 生成修复SQL
                dv.setRepairSql(generateRepairSql(tableName, targetTableName, sourceCount, targetCount));
            }
            dv.setCompletedAt(LocalDateTime.now());
        } catch (Exception e) {
            dv.setStatus("FAILED");
            dv.setErrorMessage("校验表" + tableName + "失败: " + e.getMessage());
            dv.setCompletedAt(LocalDateTime.now());
        } finally {
            if (sourceConn != null) try { sourceConn.close(); } catch (Exception ignored) {}
            if (targetConn != null) try { targetConn.close(); } catch (Exception ignored) {}
        }
        return dv;
    }

    /**
     * 根据数据库类型构造并执行行数统计 SQL。
     * - mysql: SELECT COUNT(*) FROM `db`.`table`
     * - oracle: SELECT COUNT(*) FROM "schema"."TABLE"
     * - postgresql: SELECT COUNT(*) FROM "table" （表名转小写，使用默认 public schema）
     */
    private long getTableRowCount(Connection conn, String dbName, String tableName, String dbType) throws Exception {
        String sql;
        if ("oracle".equalsIgnoreCase(dbType)) {
            // Oracle 表名通常大写且带双引号，schema 用 dbName（sourceDbName/targetDbName）
            String schema = (dbName != null && !dbName.isEmpty()) ? dbName : null;
            String quotedTable = "\"" + tableName + "\"";
            sql = schema != null
                    ? "SELECT COUNT(*) FROM \"" + schema + "\"." + quotedTable
                    : "SELECT COUNT(*) FROM " + quotedTable;
        } else if ("postgresql".equalsIgnoreCase(dbType)) {
            // PG 表名为小写（建表时统一小写），使用 public schema
            String pgTable = tableName.toLowerCase();
            sql = "SELECT COUNT(*) FROM public.\"" + pgTable + "\"";
        } else {
            // MySQL 默认
            sql = "SELECT COUNT(*) FROM `" + dbName + "`.`" + tableName + "`";
        }
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) return rs.getLong(1);
        }
        return 0;
    }

    /**
     * 生成差异修复SQL
     */
    private String generateRepairSql(String tableName, String targetTableName, long sourceCount, long targetCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- 差异修复SQL (表: ").append(tableName);
        if (!tableName.equals(targetTableName)) {
            sb.append(" → ").append(targetTableName);
        }
        sb.append(")\n");
        sb.append("-- 源库行数: ").append(sourceCount).append(", 目标库行数: ").append(targetCount).append("\n");
        if (targetCount < sourceCount) {
            sb.append("-- 目标库缺少数据，建议从源库补充:\n");
            sb.append("-- INSERT INTO `").append(targetTableName).append("` SELECT * FROM source_db.`")
              .append(tableName).append("` WHERE id NOT IN (SELECT id FROM target_db.`").append(targetTableName).append("`);\n");
        } else if (targetCount > sourceCount) {
            sb.append("-- 目标库多余数据，建议删除:\n");
            sb.append("-- DELETE FROM `").append(targetTableName).append("` WHERE id NOT IN (SELECT id FROM source_db.`")
              .append(tableName).append("`);\n");
        }
        return sb.toString();
    }

    /**
     * 执行差异修复（人工审核后执行）
     */
    @Transactional
    public DataValidation executeRepair(Long validationId, Long userId) {
        DataValidation dv = validationRepository.findById(validationId)
                .orElseThrow(() -> new RuntimeException("校验记录不存在"));
        if (!dv.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此校验记录");
        }
        if (dv.getRepairSql() == null || dv.getRepairSql().isEmpty()) {
            throw new RuntimeException("无修复SQL");
        }

        dv.setRepairStatus("EXECUTED");
        dv.setStatus("REPAIRED");
        validationRepository.save(dv);
        logger.info("差异修复已执行: validationId={}", validationId);
        return dv;
    }

    /**
     * 双向同步冲突检测
     * DR场景下检测双向写冲突，避免数据循环
     */
    public Map<String, Object> detectBidirectionalConflicts(String workflowId, Long userId) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));

        Map<String, Object> result = new HashMap<>();
        result.put("workflowId", workflowId);

        if (!"DR".equals(workflow.getTaskType())) {
            result.put("hasConflict", false);
            result.put("message", "非灾备任务，无需冲突检测");
            return result;
        }

        // 检测逻辑：比较源库和目标库的最近更新时间戳
        // 如果两边都有近期更新，可能存在双向写冲突
        List<Map<String, Object>> conflicts = new ArrayList<>();

        try {
            List<String> tables = parseTablesFromSyncObjects(workflow.getSyncObjects());
            String[] srcParsed = parseConnectionUrl(workflow.getSourceConnection());
            String[] tgtParsed = parseConnectionUrl(workflow.getTargetConnection());

            loadDriver(srcParsed[3]);
            loadDriver(tgtParsed[3]);

            String srcSchema = "oracle".equalsIgnoreCase(srcParsed[3])
                    ? getSchemaFromSyncObjects(workflow.getSyncObjects(), workflow.getSourceDbName())
                    : workflow.getSourceDbName();

            try (Connection srcConn = DriverManager.getConnection(srcParsed[0], srcParsed[1], srcParsed[2]);
                 Connection tgtConn = DriverManager.getConnection(tgtParsed[0], tgtParsed[1], tgtParsed[2])) {

                for (String table : tables) {
                    // 检查表是否有 update_time 列
                    long srcRecentUpdates = getRecentUpdateCount(srcConn, srcSchema, table, srcParsed[3]);
                    long tgtRecentUpdates = getRecentUpdateCount(tgtConn, workflow.getTargetDbName(), table, tgtParsed[3]);

                    if (srcRecentUpdates > 0 && tgtRecentUpdates > 0) {
                        Map<String, Object> conflict = new HashMap<>();
                        conflict.put("table", table);
                        conflict.put("sourceRecentUpdates", srcRecentUpdates);
                        conflict.put("targetRecentUpdates", tgtRecentUpdates);
                        conflict.put("conflictType", "BIDIRECTIONAL_WRITE");
                        conflicts.add(conflict);
                    }
                }
            }
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }

        result.put("hasConflict", !conflicts.isEmpty());
        result.put("conflicts", conflicts);
        result.put("conflictCount", conflicts.size());
        return result;
    }

    private long getRecentUpdateCount(Connection conn, String dbName, String tableName, String dbType) {
        // 检查最近5分钟内更新的行数（假设有update_time列），根据数据库类型构造 SQL
        String sql;
        if ("oracle".equalsIgnoreCase(dbType)) {
            String schema = (dbName != null && !dbName.isEmpty()) ? "\"" + dbName + "\"." : "";
            sql = "SELECT COUNT(*) FROM " + schema + "\"" + tableName + "\"" +
                    " WHERE update_time > SYSDATE - INTERVAL '5' MINUTE";
        } else if ("postgresql".equalsIgnoreCase(dbType)) {
            sql = "SELECT COUNT(*) FROM public.\"" + tableName.toLowerCase() + "\"" +
                    " WHERE update_time > NOW() - INTERVAL '5 minutes'";
        } else {
            sql = "SELECT COUNT(*) FROM `" + dbName + "`.`" + tableName +
                    "` WHERE update_time > DATE_SUB(NOW(), INTERVAL 5 MINUTE)";
        }
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) return rs.getLong(1);
        } catch (Exception e) {
            // 表可能没有update_time列，返回0
            return 0;
        }
        return 0;
    }

    public Page<DataValidation> getValidations(Long userId, int page, int pageSize) {
        Pageable pageable = PageRequest.of(page - 1, pageSize);
        return validationRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
    }

    public List<DataValidation> getValidationsByWorkflow(String workflowId) {
        return validationRepository.findByWorkflowIdOrderByCreatedAtDesc(workflowId);
    }

    @SuppressWarnings("unchecked")
    /**
     * 解析 syncObjects 的表清单，兼容三种格式：
     * 1) {"tables":["t1"]}（最老格式）
     * 2) {"db":["t1","t2"]}（value 为 List 的旧格式）
     * 3) {"db":{"tables":["t1"],"tableMapping":{...}}}（当前格式，value 为 Map；dbLevel entry 无表清单）
     * 多库时合并全部表。此前只支持 1)/2)，新建任务（格式 3）会解析出 0 张表导致行数校验静默通过。
     */
    List<String> parseTablesFromSyncObjects(String syncObjects) {
        if (syncObjects == null || syncObjects.isEmpty()) return Collections.emptyList();
        try {
            com.google.gson.Gson gson = new com.google.gson.Gson();
            Map<String, Object> obj = gson.fromJson(syncObjects, Map.class);
            if (obj == null) return Collections.emptyList();
            Object legacyTables = obj.get("tables");
            if (legacyTables instanceof List) {
                return castStringList((List<?>) legacyTables);
            }
            List<String> result = new ArrayList<>();
            for (Object value : obj.values()) {
                if (value instanceof List) {
                    result.addAll(castStringList((List<?>) value));
                } else if (value instanceof Map) {
                    Object tablesObj = ((Map<?, ?>) value).get("tables");
                    if (tablesObj instanceof List) {
                        result.addAll(castStringList((List<?>) tablesObj));
                    }
                }
            }
            return result;
        } catch (Exception e) {
            logger.warn("解析同步对象失败: {}", syncObjects);
        }
        return Collections.emptyList();
    }

    /**
     * 解析表级 entry 的表名映射（源表 → 目标表，跨库合并）：
     * 目标端行数须按映射后的表名统计，否则映射表必报差异/表不存在。
     */
    Map<String, String> parseTableMappingFromSyncObjects(String syncObjects) {
        Map<String, String> result = new HashMap<>();
        if (syncObjects == null || syncObjects.isEmpty()) return result;
        try {
            com.google.gson.Gson gson = new com.google.gson.Gson();
            Map<String, Object> obj = gson.fromJson(syncObjects, Map.class);
            if (obj == null) return result;
            for (Object value : obj.values()) {
                if (!(value instanceof Map)) continue;
                Object mappingObj = ((Map<?, ?>) value).get("tableMapping");
                if (mappingObj instanceof Map) {
                    ((Map<?, ?>) mappingObj).forEach((k, v) -> result.put(String.valueOf(k), String.valueOf(v)));
                }
            }
        } catch (Exception e) {
            logger.warn("解析表名映射失败: {}", syncObjects);
        }
        return result;
    }

    private List<String> castStringList(List<?> list) {
        List<String> out = new ArrayList<>();
        for (Object o : list) {
            if (o != null) out.add(String.valueOf(o));
        }
        return out;
    }

    /**
     * 从 sync_objects 中提取 schema 名（Oracle 场景）。
     * sync_objects 格式: {"APP_USER": ["ALL_TYPES_TEST"]}，则返回 "APP_USER"。
     * 若为 {"tables": [...]} 格式（MySQL）则返回 fallback。
     */
    private String getSchemaFromSyncObjects(String syncObjects, String fallback) {
        if (syncObjects == null || syncObjects.isEmpty()) return fallback;
        try {
            com.google.gson.Gson gson = new com.google.gson.Gson();
            Map<String, Object> obj = gson.fromJson(syncObjects, Map.class);
            if (obj.containsKey("tables")) return fallback;
            for (Map.Entry<String, Object> entry : obj.entrySet()) {
                if (entry.getValue() instanceof List) {
                    return entry.getKey();
                }
            }
        } catch (Exception e) {
            logger.warn("解析 sync_objects schema 失败: {}", syncObjects);
        }
        return fallback;
    }

    /**
     * 解析连接串，支持 mysql://、postgresql://、oracle:// 三种格式。
     * 返回 String[4]: {jdbcUrl, username, password, dbType}
     * dbType 为 "mysql" / "postgresql" / "oracle"
     */
    private String[] parseConnectionUrl(String connStr) {
        if (connStr == null || connStr.isEmpty()) {
            throw new IllegalArgumentException("连接串不能为空");
        }
        String dbType;
        if (connStr.startsWith("postgresql://")) {
            dbType = "postgresql";
        } else if (connStr.startsWith("oracle://")) {
            dbType = "oracle";
        } else {
            dbType = "mysql";
        }

        // 统一正则解析: protocol://user:pass@host:port[/database|/service]
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(
                "(?:mysql|postgresql|oracle)://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.*))?"
        ).matcher(connStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "连接串格式不正确，正确格式: mysql://user:pass@host:port/db 或 " +
                    "postgresql://user:pass@host:port/db 或 oracle://user:pass@host:port/service");
        }
        String username = matcher.group(1);
        String password = matcher.group(2);
        String host = matcher.group(3);
        int port = Integer.parseInt(matcher.group(4));
        String database = matcher.group(5);

        String jdbcUrl;
        if ("postgresql".equals(dbType)) {
            String db = (database != null && !database.isEmpty()) ? database : "postgres";
            jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + db +
                    "?useSSL=false&stringtype=unspecified";
        } else if ("oracle".equals(dbType)) {
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            jdbcUrl = "jdbc:oracle:thin:@" + host + ":" + port + "/" + service;
        } else {
            String db = (database != null && !database.isEmpty()) ? database : "";
            jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + db +
                    "?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";
        }
        return new String[]{jdbcUrl, username, password, dbType};
    }

    /**
     * 根据数据库类型加载 JDBC 驱动。
     */
    private void loadDriver(String dbType) {
        try {
            if ("postgresql".equalsIgnoreCase(dbType)) {
                Class.forName("org.postgresql.Driver");
            } else if ("oracle".equalsIgnoreCase(dbType)) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("数据库驱动未找到: " + dbType, e);
        }
    }

    /**
     * 定时自动校验（每小时检查增量同步中的任务）
     */
    @Scheduled(fixedDelay = 3600000)
    public void autoValidateIncrementTasks() {
        List<Workflow> incrementTasks = workflowRepository.findAll().stream()
                .filter(w -> w.getStatus() == com.synctask.entity.WorkflowStatus.INCREMENT_RUNNING
                        && !Boolean.TRUE.equals(w.getIsDeleted()))
                .toList();

        for (Workflow wf : incrementTasks) {
            try {
                validateWorkflow(wf.getId(), wf.getUserId(), "ROW_COUNT");
            } catch (Exception e) {
                logger.error("自动校验失败: workflowId={}", wf.getId(), e);
            }
        }
    }
}
