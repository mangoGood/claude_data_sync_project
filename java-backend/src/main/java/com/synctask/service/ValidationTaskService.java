package com.synctask.service;

import com.google.gson.Gson;
import com.synctask.dto.ContentCompareSession;
import com.synctask.entity.ValidationTask;
import com.synctask.entity.ValidationTaskLog;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.ValidationTaskLogRepository;
import com.synctask.repository.ValidationTaskRepository;
import com.synctask.repository.WorkflowRepository;
import com.synctask.util.DataSourcePoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ValidationTaskService {

    private static final Logger logger = LoggerFactory.getLogger(ValidationTaskService.class);

    @Autowired
    private ValidationTaskRepository validationTaskRepository;

    @Autowired
    private ValidationTaskLogRepository validationTaskLogRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private ContentCompareService contentCompareService;

    private final Gson gson = new Gson();

    private static final Pattern CONNECTION_PATTERN = Pattern.compile(
        "(mysql|postgresql)://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.*))?"
    );

    public static class ParsedConnection {
        public String type;
        public String username;
        public String password;
        public String host;
        public int port;
        public String database;

        public ParsedConnection(String type, String username, String password, String host, int port, String database) {
            this.type = type;
            this.username = username;
            this.password = password;
            this.host = host;
            this.port = port;
            this.database = database;
        }
    }

    public ParsedConnection parseConnection(String connectionStr) {
        if (connectionStr == null || connectionStr.isEmpty()) {
            throw new IllegalArgumentException("连接串不能为空");
        }

        Matcher matcher = CONNECTION_PATTERN.matcher(connectionStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("连接串格式不正确");
        }

        return new ParsedConnection(
            matcher.group(1),
            matcher.group(2),
            matcher.group(3),
            matcher.group(4),
            Integer.parseInt(matcher.group(5)),
            matcher.group(6)
        );
    }

    private String buildJdbcUrl(String type, String host, int port, String database) {
        String db = (database != null && !database.isEmpty()) ? database : "";
        if ("postgresql".equalsIgnoreCase(type)) {
            if (!db.isEmpty()) {
                return String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, db);
            }
            return String.format("jdbc:postgresql://%s:%d/?stringtype=unspecified", host, port);
        }
        if (!db.isEmpty()) {
            return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true", host, port, db);
        }
        return String.format("jdbc:mysql://%s:%d/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true", host, port);
    }

    public List<Workflow> getIncrementalWorkflows(Long userId) {
        List<Workflow> workflows = workflowRepository.findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(userId);
        return workflows.stream()
            .filter(w -> w.getStatus() == WorkflowStatus.INCREMENT_RUNNING
                || ("DR".equals(w.getTaskType()) && w.getStatus() == WorkflowStatus.FULL_COMPLETED))
            .toList();
    }

    public Page<ValidationTask> getValidationTasks(Long userId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return validationTaskRepository.findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(userId, pageable);
    }

    public ValidationTask getValidationTask(String id, Long userId) {
        return validationTaskRepository.findByIdAndUserIdAndIsDeletedFalse(id, userId)
            .orElseThrow(() -> new RuntimeException("校验任务不存在"));
    }

    @Transactional
    public ValidationTask createValidationTask(String workflowId, Long userId, String compareType) {
        Workflow workflow = workflowRepository.findByIdAndUserIdAndIsDeletedFalse(workflowId, userId)
            .orElseThrow(() -> new RuntimeException("任务不存在"));

        boolean isDrTask = "DR".equals(workflow.getTaskType());
        boolean isIncrementRunning = workflow.getStatus() == WorkflowStatus.INCREMENT_RUNNING;
        boolean isDrRunning = isDrTask && workflow.getStatus() == WorkflowStatus.FULL_COMPLETED;

        if (!isIncrementRunning && !isDrRunning) {
            throw new RuntimeException("只能为增量同步中或灾备中的任务创建对比任务");
        }

        List<ValidationTask> runningTasks = validationTaskRepository
            .findByWorkflowIdAndStatusInAndIsDeletedFalse(workflowId,
                Arrays.asList(ValidationTask.ValidationStatus.PENDING, ValidationTask.ValidationStatus.RUNNING));
        if (!runningTasks.isEmpty()) {
            throw new RuntimeException("该任务已有对比任务正在执行中，请等待完成后再创建");
        }

        if ("CONTENT".equals(compareType)) {
            String sourceType = workflow.getSourceConnection().startsWith("postgresql") ? "postgresql" : "mysql";
            String targetType = workflow.getTargetConnection().startsWith("postgresql") ? "postgresql" : "mysql";
            if (!sourceType.equalsIgnoreCase(targetType)) {
                throw new RuntimeException("内容对比仅支持源库和目标库为相同类型的数据库");
            }
        }

        ValidationTask task = new ValidationTask();
        String typeLabel = "CONTENT".equals(compareType) ? "内容对比" : "行数对比";
        task.setName(workflow.getName() + "-" + typeLabel + "-" + System.currentTimeMillis());
        task.setWorkflowId(workflowId);
        task.setWorkflowName(workflow.getName());
        task.setUserId(userId);
        task.setSourceConnection(workflow.getSourceConnection());
        task.setTargetConnection(workflow.getTargetConnection());
        task.setSyncObjects(workflow.getSyncObjects());
        task.setCompareType(compareType);
        task.setTaskType(workflow.getTaskType() != null ? workflow.getTaskType() : "SYNC");
        task.setStatus(ValidationTask.ValidationStatus.PENDING);

        validationTaskRepository.save(task);
        addLog(task.getId(), ValidationTaskLog.LogLevel.INFO, typeLabel + "任务已创建，等待执行");

        executeValidationAsync(task.getId());

        return task;
    }

    @Async
    @Transactional
    public void executeValidationAsync(String taskId) {
        ValidationTask task = validationTaskRepository.findById(taskId).orElse(null);
        if (task == null) {
            logger.error("对比任务不存在: {}", taskId);
            return;
        }

        try {
            task.setStatus(ValidationTask.ValidationStatus.RUNNING);
            task.setStartedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);

            if ("CONTENT".equals(task.getCompareType())) {
                executeContentCompare(task);
            } else {
                executeRowCountCompare(task);
            }
        } catch (Exception e) {
            logger.error("对比任务执行失败: {}", e.getMessage(), e);
            task.setStatus(ValidationTask.ValidationStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);
            addLog(taskId, ValidationTaskLog.LogLevel.ERROR, "对比任务执行失败: " + e.getMessage());
        }
    }

    private void executeContentCompare(ValidationTask task) {
        String taskId = task.getId();
        addLog(taskId, ValidationTaskLog.LogLevel.INFO, "开始执行数据内容对比");

        try {
            String sourceType = task.getSourceConnection().startsWith("postgresql") ? "postgresql" : "mysql";
            String targetType = task.getTargetConnection().startsWith("postgresql") ? "postgresql" : "mysql";

            Map<String, List<String>> syncObjectsMap = parseSyncObjectsSimple(task.getSyncObjects());

            boolean isDrTask = "DR".equals(task.getTaskType());
            if (isDrTask || syncObjectsMap.isEmpty()) {
                ParsedConnection sourceConn = parseConnection(task.getSourceConnection());
                boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceType);
                try (Connection sourceDb = DataSourcePoolManager.getConnection(
                        buildJdbcUrl(sourceConn.type, sourceConn.host, sourceConn.port, null),
                        sourceConn.username, sourceConn.password)) {
                    List<String> allDatabases = getAllDatabaseNames(sourceDb, sourceIsPg);
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        (isDrTask ? "灾备任务，对比源库和目标库的所有数据库: " : "sync_objects 为空，自动获取源库所有数据库: ") + allDatabases);
                    syncObjectsMap.clear();
                    for (String dbName : allDatabases) {
                        List<String> tableNames = getTableNames(sourceDb, dbName, sourceIsPg);
                        if (!tableNames.isEmpty()) {
                            syncObjectsMap.put(dbName, tableNames);
                        }
                    }
                }
            }

            ContentCompareSession session = contentCompareService.startCompare(
                task.getSourceConnection(), task.getTargetConnection(),
                sourceType, targetType, syncObjectsMap);

            addLog(taskId, ValidationTaskLog.LogLevel.INFO, "内容对比会话已创建: " + session.getSessionId());

            session = contentCompareService.runPhase1Checksum(session.getSessionId());

            int totalTables = session.getTables().size();
            int passedTables = 0;
            int failedTables = 0;
            long totalDiffs = 0;
            List<Map<String, Object>> tableResults = new ArrayList<>();

            for (int i = 0; i < session.getTables().size(); i++) {
                ContentCompareSession.TableCompareTask t = session.getTables().get(i);
                Map<String, Object> tr = new LinkedHashMap<>();
                tr.put("sourceTable", t.getSourceTable());
                tr.put("targetTable", t.getTargetTable());
                tr.put("sourceRowCount", t.getSourceRowCount());
                tr.put("targetRowCount", t.getTargetRowCount());
                tr.put("checksumMatch", t.getChecksumMatch());

                if (Boolean.TRUE.equals(t.getChecksumMatch())) {
                    passedTables++;
                    tr.put("status", "MATCH");
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "表 " + t.getSourceTable() + " 内容一致");
                } else if (t.getPrimaryKeyColumn() == null || t.getPrimaryKeyColumn().isEmpty()) {
                    failedTables++;
                    tr.put("status", "NO_PK");
                    addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                        "表 " + t.getSourceTable() + " 无主键，无法详细对比");
                } else {
                    ContentCompareSession.TableCompareTask diffResult =
                        contentCompareService.findDiffs(session.getSessionId(), i, 100);
                    int diffCount = diffResult.getDiffs().size();
                    if ("ERROR".equals(diffResult.getStatus())) {
                        failedTables++;
                        tr.put("status", "ERROR");
                        tr.put("diffCount", 0);
                        tr.put("diffs", Collections.emptyList());
                        addLog(taskId, ValidationTaskLog.LogLevel.ERROR,
                            "表 " + t.getSourceTable() + " 差异查找失败，可能是特殊数据类型导致查询异常");
                    } else if (diffCount == 0) {
                        passedTables++;
                        tr.put("status", "MATCH");
                        tr.put("diffCount", 0);
                        tr.put("diffs", Collections.emptyList());
                        addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                            "表 " + t.getSourceTable() + " 校验和不一致但逐行对比数据一致（CHECKSUM TABLE 对浮点/BIT/BLOB等类型可能产生误报）");
                    } else {
                        totalDiffs += diffCount;
                        failedTables++;
                        tr.put("status", "MISMATCH");
                        tr.put("diffCount", diffCount);
                        tr.put("diffs", diffResult.getDiffs());
                        addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                            "表 " + t.getSourceTable() + " 数据不一致，差异行数: " + diffCount);
                    }
                }
                tableResults.add(tr);
            }

            task.setTotalTables(totalTables);
            task.setPassedTables(passedTables);
            task.setFailedTables(failedTables);
            task.setMismatchedRows((long) totalDiffs);
            task.setCompareResult(gson.toJson(Map.of(
                "sessionId", session.getSessionId(),
                "tables", tableResults
            )));
            task.setStatus(ValidationTask.ValidationStatus.COMPLETED);
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);

            addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                String.format("内容对比完成: 共 %d 个表，一致 %d 个，不一致 %d 个，差异行数 %d",
                    totalTables, passedTables, failedTables, totalDiffs));

            contentCompareService.deleteSession(session.getSessionId());

        } catch (Exception e) {
            logger.error("内容对比执行失败: {}", e.getMessage(), e);
            task.setStatus(ValidationTask.ValidationStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);
            addLog(taskId, ValidationTaskLog.LogLevel.ERROR, "内容对比执行失败: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>> parseSyncObjectsSimple(String syncObjectsJson) {
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            return new HashMap<>();
        }
        try {
            Map<String, Object> raw = gson.fromJson(syncObjectsJson, Map.class);
            if (raw == null) return new HashMap<>();
            Map<String, List<String>> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                String dbName = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof List) {
                    result.put(dbName, (List<String>) value);
                } else if (value instanceof Map) {
                    Map<String, Object> inner = (Map<String, Object>) value;
                    Object tablesObj = inner.get("tables");
                    if (tablesObj instanceof List) {
                        result.put(dbName, (List<String>) tablesObj);
                    }
                }
            }
            return result;
        } catch (Exception e) {
            logger.warn("解析 sync_objects 失败: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    private void executeRowCountCompare(ValidationTask task) {
        String taskId = task.getId();
        addLog(taskId, ValidationTaskLog.LogLevel.INFO, "开始执行数据行数对比");

        ParsedConnection sourceConn = parseConnection(task.getSourceConnection());
        ParsedConnection targetConn = parseConnection(task.getTargetConnection());

        Map<String, Map<String, List<String>>> syncObjects = parseSyncObjects(task.getSyncObjects());

        int totalTables = 0;
        int passedTables = 0;
        int failedTables = 0;
        long totalRows = 0;
        long mismatchedRows = 0;
        List<Map<String, Object>> tableResults = new ArrayList<>();

        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn.type, sourceConn.host, sourceConn.port, sourceConn.database),
                sourceConn.username, sourceConn.password);
             Connection targetDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(targetConn.type, targetConn.host, targetConn.port, targetConn.database),
                targetConn.username, targetConn.password)) {

            boolean isDrTask = "DR".equals(task.getTaskType());
            if (isDrTask || syncObjects.isEmpty()) {
                boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceConn.type);
                List<String> allDatabases = getAllDatabaseNames(sourceDb, sourceIsPg);
                addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                    (isDrTask ? "灾备任务，对比源库和目标库的所有数据库: " : "sync_objects 为空，自动获取源库所有数据库: ") + allDatabases);
                syncObjects.clear();
                for (String dbName : allDatabases) {
                    List<String> tableNames = getTableNames(sourceDb, dbName, sourceIsPg);
                    if (!tableNames.isEmpty()) {
                        Map<String, List<String>> tableMap = new HashMap<>();
                        tableMap.put("tables", tableNames);
                        syncObjects.put(dbName, tableMap);
                    }
                }
            }

            for (Map.Entry<String, Map<String, List<String>>> dbEntry : syncObjects.entrySet()) {
                String sourceDbName = dbEntry.getKey();
                String targetDbName = targetConn.database != null ? targetConn.database : sourceDbName;
                List<String> tables = dbEntry.getValue().get("tables");

                if (tables == null || tables.isEmpty()) continue;

                for (String tableName : tables) {
                    totalTables++;
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO, 
                        "行数对比表: " + sourceDbName + "." + tableName);

                    try {
                        TableDiffResult diff = compareTableData(
                            sourceDb, targetDb, sourceDbName, targetDbName, tableName);

                        totalRows += diff.totalRows;
                        mismatchedRows += diff.mismatchedRows;

                        Map<String, Object> tr = new LinkedHashMap<>();
                        tr.put("sourceTable", tableName);
                        tr.put("targetTable", tableName);
                        tr.put("sourceRowCount", diff.sourceRowCount);
                        tr.put("targetRowCount", diff.targetRowCount);

                        if (diff.mismatchedRows == 0 && diff.error == null) {
                            passedTables++;
                            tr.put("status", "MATCH");
                            addLog(taskId, ValidationTaskLog.LogLevel.INFO, 
                                "表 " + sourceDbName + "." + tableName + " 行数对比通过，共 " + diff.totalRows + " 行");
                        } else {
                            failedTables++;
                            if (diff.error != null) {
                                tr.put("status", "ERROR");
                                addLog(taskId, ValidationTaskLog.LogLevel.ERROR, 
                                    "表 " + sourceDbName + "." + tableName + " 行数对比失败: " + diff.error);
                            } else {
                                tr.put("status", "MISMATCH");
                                tr.put("diffCount", diff.mismatchedRows);
                                addLog(taskId, ValidationTaskLog.LogLevel.WARNING, 
                                    "表 " + sourceDbName + "." + tableName + " 数据不一致，差异行数: " + diff.mismatchedRows);
                            }
                        }
                        tableResults.add(tr);
                    } catch (Exception e) {
                        failedTables++;
                        Map<String, Object> tr = new LinkedHashMap<>();
                        tr.put("sourceTable", tableName);
                        tr.put("targetTable", tableName);
                        tr.put("status", "ERROR");
                        tableResults.add(tr);
                        addLog(taskId, ValidationTaskLog.LogLevel.ERROR, 
                            "表 " + sourceDbName + "." + tableName + " 对比异常: " + e.getMessage());
                    }
                }
            }

            task.setTotalTables(totalTables);
            task.setPassedTables(passedTables);
            task.setFailedTables(failedTables);
            task.setTotalRows(totalRows);
            task.setMismatchedRows(mismatchedRows);
            task.setCompareResult(gson.toJson(Map.of("tables", tableResults)));
            task.setStatus(ValidationTask.ValidationStatus.COMPLETED);
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);

            addLog(taskId, ValidationTaskLog.LogLevel.INFO, 
                String.format("行数对比完成: 共 %d 个表，通过 %d 个，失败 %d 个，总行数 %d，差异行数 %d",
                    totalTables, passedTables, failedTables, totalRows, mismatchedRows));

        } catch (Exception e) {
            logger.error("行数对比执行失败: {}", e.getMessage(), e);
            task.setStatus(ValidationTask.ValidationStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);
            addLog(taskId, ValidationTaskLog.LogLevel.ERROR, "行数对比执行失败: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, List<String>>> parseSyncObjects(String syncObjectsJson) {
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            return new HashMap<>();
        }
        try {
            Map<String, Object> raw = gson.fromJson(syncObjectsJson, Map.class);
            if (raw == null) {
                return new HashMap<>();
            }
            Map<String, Map<String, List<String>>> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                String dbName = entry.getKey();
                Object value = entry.getValue();
                Map<String, List<String>> tableMap = new HashMap<>();
                if (value instanceof List) {
                    tableMap.put("tables", (List<String>) value);
                } else if (value instanceof Map) {
                    Map<String, Object> inner = (Map<String, Object>) value;
                    if (inner.containsKey("tables") && inner.get("tables") instanceof List) {
                        tableMap.put("tables", (List<String>) inner.get("tables"));
                    }
                }
                if (!tableMap.isEmpty()) {
                    result.put(dbName, tableMap);
                }
            }
            return result;
        } catch (Exception e) {
            logger.warn("Failed to parse sync_objects JSON: {}, error: {}", syncObjectsJson, e.getMessage());
            return new HashMap<>();
        }
    }

    private static class TableDiffResult {
        long totalRows;
        long mismatchedRows;
        long sourceRowCount;
        long targetRowCount;
        String error;
    }

    private TableDiffResult compareTableData(Connection sourceDb, Connection targetDb, 
            String sourceDbName, String targetDbName, String tableName) {
        TableDiffResult result = new TableDiffResult();

        try {
            boolean sourceIsPg = isPostgresqlConnection(sourceDb);
            boolean targetIsPg = isPostgresqlConnection(targetDb);

            long sourceRowCount = getRowCountSafe(sourceDb, sourceDbName, tableName, sourceIsPg);
            long targetRowCount = getRowCountSafe(targetDb, targetDbName, tableName, targetIsPg);

            if (sourceRowCount < 0 || targetRowCount < 0) {
                result.error = "获取行数失败";
                return result;
            }

            result.sourceRowCount = sourceRowCount;
            result.targetRowCount = targetRowCount;
            result.totalRows = sourceRowCount;

            if (sourceRowCount != targetRowCount) {
                result.mismatchedRows = Math.abs(sourceRowCount - targetRowCount);
                addLogForCurrentTask("行数对比: 源库 " + sourceDbName + "." + tableName + " 行数=" + sourceRowCount + ", 目标库 " + targetDbName + "." + tableName + " 行数=" + targetRowCount);
            } else {
                result.mismatchedRows = 0;
            }

        } catch (Exception e) {
            result.error = e.getMessage();
        }

        return result;
    }

    private void addLogForCurrentTask(String message) {
        try {
            logger.info(message);
        } catch (Exception e) {
            // ignore
        }
    }

    private boolean isPostgresqlConnection(Connection conn) throws SQLException {
        return conn.getMetaData().getDatabaseProductName().toLowerCase().contains("postgresql");
    }

    private long getRowCount(Connection conn, String dbName, String tableName, boolean isPg) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String sql = isPg
                ? "SELECT COUNT(*) FROM \"" + tableName + "\""
                : "SELECT COUNT(*) FROM `" + dbName + "`.`" + tableName + "`";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        return 0;
    }

    private long getRowCountSafe(Connection conn, String dbName, String tableName, boolean isPg) {
        try {
            return getRowCount(conn, dbName, tableName, isPg);
        } catch (SQLException e) {
            String msg = e.getMessage();
            if (msg != null && (msg.contains("doesn't exist") || msg.contains("does not exist")
                    || msg.contains("not found") || msg.contains("unknown table")
                    || msg.contains("no such table") || msg.contains("relation") && msg.contains("does not exist"))) {
                logger.info("表 {}.{} 在目标库不存在，视为0行: {}", dbName, tableName, msg);
                return 0;
            }
            logger.warn("获取表 {}.{} 行数失败: {}", dbName, tableName, msg);
            return -1;
        }
    }

    private static final Set<String> IGNORED_TABLES = Set.of("__sync_heartbeat");

    private List<String> getTableNames(Connection conn, String dbName, boolean isPg) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        String[] types = {"TABLE"};
        if (isPg) {
            try (ResultSet rs = metaData.getTables(dbName, "public", "%", types)) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (!IGNORED_TABLES.contains(tableName.toLowerCase())) {
                        tables.add(tableName);
                    }
                }
            }
        } else {
            try (ResultSet rs = metaData.getTables(dbName, null, "%", types)) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (!IGNORED_TABLES.contains(tableName.toLowerCase())) {
                        tables.add(tableName);
                    }
                }
            }
        }
        return tables;
    }

    private List<String> getAllDatabaseNames(Connection conn, boolean isPg) throws SQLException {
        List<String> databases = new ArrayList<>();
        if (isPg) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT schema_name FROM information_schema.schemata " +
                     "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') " +
                     "AND schema_name NOT LIKE 'pg_temp_%'")) {
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }
        } else {
            try (ResultSet rs = conn.getMetaData().getCatalogs()) {
                while (rs.next()) {
                    String db = rs.getString("TABLE_CAT");
                    if (db != null && !db.equalsIgnoreCase("information_schema")
                        && !db.equalsIgnoreCase("mysql")
                        && !db.equalsIgnoreCase("performance_schema")
                        && !db.equalsIgnoreCase("sys")) {
                        databases.add(db);
                    }
                }
            }
        }
        return databases;
    }

    @Transactional
    public void deleteValidationTask(String id, Long userId) {
        ValidationTask task = getValidationTask(id, userId);
        task.setIsDeleted(true);
        validationTaskRepository.save(task);
        addLog(id, ValidationTaskLog.LogLevel.INFO, "校验任务已删除");
    }

    private void addLog(String taskId, ValidationTaskLog.LogLevel level, String message) {
        ValidationTaskLog log = new ValidationTaskLog();
        log.setValidationTaskId(taskId);
        log.setLevel(level);
        log.setMessage(message);
        validationTaskLogRepository.save(log);
    }
}
