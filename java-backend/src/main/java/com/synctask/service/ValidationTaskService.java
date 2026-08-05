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
import java.util.stream.Collectors;

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
        "(mysql|postgresql|mongodb|elastic)://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.*))?"
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

    /**
     * 若连接串本身未带库名路径（.../host:port 后面没有 /db），补上显式指定的库名；
     * 已带库名路径或 dbName 为空时原样返回。用于避免下游把"连接串缺库名"误当作
     * "源库和目标库同名"处理（该假设仅在灾备镜像场景成立，普通同步场景源/目标库名可能不同）。
     */
    private String withDatabase(String connectionStr, String dbName) {
        if (connectionStr == null || dbName == null || dbName.isEmpty()) {
            return connectionStr;
        }
        Matcher matcher = CONNECTION_PATTERN.matcher(connectionStr);
        if (!matcher.matches()) {
            return connectionStr;
        }
        String existingDb = matcher.group(6);
        if (existingDb != null && !existingDb.isEmpty()) {
            return connectionStr; // 已显式指定库名，不覆盖
        }
        return connectionStr + "/" + dbName;
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

        // 列处理任务（列过滤/列名映射/附加列）：源表与目标表的行集/列集不再一一对应，
        // 行数对比与内容对比均无意义，直接拒绝创建
        if (hasColumnProcessing(workflow.getSyncObjects())) {
            throw new RuntimeException("该任务配置了列处理（列名过滤/列名映射/附加列），" +
                    "源端与目标端数据不再一一对应，无法进行内容对比和行数对比");
        }

        if ("CONTENT".equals(compareType)) {
            if (workflow.getTargetConnection().startsWith("elastic")) {
                throw new RuntimeException("Elasticsearch 任务暂不支持内容对比，请使用行数对比（按索引文档数）");
            }
            boolean srcMongo = workflow.getSourceConnection().startsWith("mongodb");
            boolean tgtMongo = workflow.getTargetConnection().startsWith("mongodb");
            if (srcMongo || tgtMongo) {
                // MongoDB 内容对比：按 _id 逐文档比对（类型互斥保证两端均为 mongodb）
                if (!(srcMongo && tgtMongo)) {
                    throw new RuntimeException("内容对比仅支持源库和目标库为相同类型的数据库");
                }
            } else {
                String sourceType = workflow.getSourceConnection().startsWith("postgresql") ? "postgresql" : "mysql";
                String targetType = workflow.getTargetConnection().startsWith("postgresql") ? "postgresql" : "mysql";
                if (!sourceType.equalsIgnoreCase(targetType)) {
                    throw new RuntimeException("内容对比仅支持源库和目标库为相同类型的数据库");
                }
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
        task.setSourceDbName(workflow.getSourceDbName());
        task.setTargetDbName(workflow.getTargetDbName());
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

        // MongoDB 内容对比走独立的文档比对路径（非 SQL checksum + JDBC）
        if (task.getSourceConnection().startsWith("mongodb")) {
            executeMongoContentCompare(task);
            return;
        }

        try {
            String sourceType = task.getSourceConnection().startsWith("postgresql") ? "postgresql" : "mysql";
            String targetType = task.getTargetConnection().startsWith("postgresql") ? "postgresql" : "mysql";

            Map<String, List<String>> syncObjectsMap = parseSyncObjectsSimple(task.getSyncObjects());

            boolean isDrTask = "DR".equals(task.getTaskType());
            String sourceConnStr = task.getSourceConnection();
            String targetConnStr = task.getTargetConnection();

            if (isDrTask || syncObjectsMap.isEmpty()) {
                // 灾备场景源/目标库名逐一对应镜像，ContentCompareService 用 sync_objects 的库名
                // 同时作为源/目标库名的兜底在这里是对的，连接串保持不带库名以便按库遍历
                ParsedConnection sourceConn = parseConnection(sourceConnStr);
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
            } else {
                // 非灾备同步：源库名和目标库名可能不同（如 prod -> prod_replica）。
                // ContentCompareService 在 mysql 连接串未带库路径时会用 sync_objects 的键（源库名）
                // 同时兜底源库和目标库连接的库名——源/目标库名不同时会把目标库错连到源库同名库上。
                // 这里显式把各自的库名拼进连接串，从根上避免这个误连接。
                sourceConnStr = withDatabase(sourceConnStr, task.getSourceDbName());
                targetConnStr = withDatabase(targetConnStr, task.getTargetDbName());

                // 库级同步对象（空表清单占位）：按库枚举全部表——对比范围严格等于任务同步的库
                boolean hasDbLevel = false;
                for (List<String> t : syncObjectsMap.values()) {
                    if (t.isEmpty()) {
                        hasDbLevel = true;
                        break;
                    }
                }
                if (hasDbLevel) {
                    ParsedConnection sc = parseConnection(task.getSourceConnection());
                    boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceType);
                    try (Connection sourceDb = DataSourcePoolManager.getConnection(
                            buildJdbcUrl(sc.type, sc.host, sc.port, null), sc.username, sc.password)) {
                        for (Map.Entry<String, List<String>> e : syncObjectsMap.entrySet()) {
                            if (e.getValue().isEmpty()) {
                                e.getValue().addAll(getTableNames(sourceDb, e.getKey(), sourceIsPg));
                                addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                                    "库级同步对象，内容对比范围为库 " + e.getKey() + " 的全部表（" + e.getValue().size() + " 个）");
                            }
                        }
                    }
                }
            }

            // 表名/库名映射（表级同步）：syncObjectsMap 已压平丢失映射，显式补传给对比服务。
            // 聚合路由配置同理：目标端读法（汇聚切片 / 拆分并集）全靠它解析
            String routeConfig = routeConfigOf(task);
            ContentCompareSession session = contentCompareService.startCompare(
                sourceConnStr, targetConnStr,
                sourceType, targetType, syncObjectsMap,
                parseTableMappings(task.getSyncObjects()),
                parseDbMappings(task.getSyncObjects()),
                routeConfig, routeNodeIdOf(routeConfig, parseConnection(task.getSourceConnection())));

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
                // 供后续"修复"使用：diffs 只带 primaryKeyValue/sourceData/targetData，
                // 缺主键列名和列类型（用于识别二进制列做 Base64 解码）就无法据此生成可执行的修复语句
                tr.put("sourceDb", t.getSourceDb());
                tr.put("targetDb", t.getTargetDb());
                tr.put("primaryKeyColumn", t.getPrimaryKeyColumn());
                // 修复时要重新解析路由落点，而 targetDb 在路由任务里已被改写成合并表/首片所在的库，
                // 当不了解析规则用的"默认目标库"
                tr.put("routeDefaultTargetDb", t.getRouteDefaultTargetDb());
                List<Map<String, String>> colMeta = new ArrayList<>();
                if (t.getColumns() != null) {
                    for (com.synctask.dto.ContentCompareSession.ColumnMeta cm : t.getColumns()) {
                        Map<String, String> m = new LinkedHashMap<>();
                        m.put("name", cm.getName());
                        m.put("type", cm.getType());
                        colMeta.add(m);
                    }
                }
                tr.put("columns", colMeta);

                if (Boolean.TRUE.equals(t.getChecksumMatch())) {
                    passedTables++;
                    tr.put("status", "MATCH");
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "表 " + t.getSourceTable() + " 内容一致");
                } else if ("UNSUPPORTED".equals(t.getStatus())) {
                    failedTables++;
                    tr.put("status", "UNSUPPORTED");
                    addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                        "表 " + t.getSourceTable() + " 的分片分布在多个目标实例上，内容对比只能连一个目标实例，已跳过");
                } else if (t.getPrimaryKeyColumn() == null || t.getPrimaryKeyColumn().isEmpty()) {
                    failedTables++;
                    tr.put("status", "NO_PK");
                    addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                        "表 " + t.getSourceTable() + " 无主键，无法详细对比");
                } else {
                    // 阶段1的结论要在 findDiffs 改写 status 之前留一份：拆分是"摘要一致也得逐行核"，
                    // 与"摘要不一致"是两种情况，日志不能混为一谈
                    String phase1Status = t.getStatus();
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
                            "NEEDS_ROW_SCAN".equals(phase1Status)
                                ? "表 " + t.getSourceTable() + " 摘要一致，逐行核对落点也没有错片行"
                                : "表 " + t.getSourceTable() + " 校验和不一致但逐行对比数据一致（CHECKSUM TABLE 对浮点/BIT/BLOB等类型可能产生误报）");
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

    /** 单次修复动作数上限：超出则整批跳过并提示手动处理，避免误配置/陈旧对比结果导致失控的大批量写入 */
    private static final int MAX_REPAIR_ACTIONS = 5000;

    /**
     * 一致性校验闭环：基于已完成的内容对比结果执行差异修复，随后复核确认收敛。
     * 校验(内容对比+逐行 diff) → 修复(按 diffType 生成 INSERT/UPDATE/DELETE，源库当前快照为准) → 复核(重新对比行数确认一致)。
     * 仅支持 CONTENT 类型对比（ROW_COUNT 只有聚合计数，不知道具体是哪些行，无法定位修复）。
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public Map<String, Object> repairValidationTask(String taskId, Long userId) {
        ValidationTask task = validationTaskRepository.findByIdAndUserIdAndIsDeletedFalse(taskId, userId)
            .orElseThrow(() -> new RuntimeException("校验任务不存在"));

        if (task.getStatus() != ValidationTask.ValidationStatus.COMPLETED) {
            throw new RuntimeException("只能修复已完成的对比任务");
        }
        if (!"CONTENT".equals(task.getCompareType())) {
            throw new RuntimeException("仅内容对比（CONTENT）任务支持自动修复：行数对比只有聚合计数，无法定位具体差异行，请重新创建内容对比任务");
        }
        if (task.getSourceConnection() != null && task.getSourceConnection().startsWith("mongodb")) {
            throw new RuntimeException("MongoDB 内容对比暂不支持一键修复，请人工核对差异文档");
        }
        if (task.getCompareResult() == null || task.getCompareResult().isEmpty()) {
            throw new RuntimeException("无对比结果数据");
        }

        Map<String, Object> parsed = gson.fromJson(task.getCompareResult(), Map.class);
        List<Map<String, Object>> tables = (List<Map<String, Object>>) parsed.get("tables");
        if (tables == null || tables.isEmpty()) {
            throw new RuntimeException("无表级对比结果");
        }

        ParsedConnection sourceConn = parseConnection(task.getSourceConnection());
        ParsedConnection targetConn = parseConnection(task.getTargetConnection());
        boolean isPg = "postgresql".equalsIgnoreCase(sourceConn.type);
        // 路由任务的修复要写到<b>路由后的落点</b>：汇聚要带上来源标识列，拆分要按分片键选片。
        // 照 tr 里的 targetTable 直接写，汇聚会漏掉标识列（写出一行谁也认领不了的数据），
        // 拆分会写到"第一片"上（下次对比又成了错片行）
        String routeConfig = routeConfigOf(task);
        String routeMode = RouteCompareSupport.modeOf(routeConfig);
        String routeNodeId = routeNodeIdOf(routeConfig, sourceConn);

        List<Map<String, Object>> tableSummaries = new ArrayList<>();
        int totalActions = 0;
        boolean anyFailure = false;
        boolean allVerified = true;

        for (Map<String, Object> tr : tables) {
            List<Map<String, Object>> diffs = (List<Map<String, Object>>) tr.get("diffs");
            if (diffs == null || diffs.isEmpty()) continue;

            String sourceTable = (String) tr.get("sourceTable");
            String targetTable = (String) tr.get("targetTable");
            String sourceDb = (String) tr.get("sourceDb");
            String targetDb = (String) tr.get("targetDb");
            String pkCol = (String) tr.get("primaryKeyColumn");
            List<Map<String, Object>> colMeta = (List<Map<String, Object>>) tr.get("columns");

            Map<String, Object> tableSummary = new LinkedHashMap<>();
            tableSummary.put("table", sourceTable);

            if (pkCol == null || pkCol.isEmpty() || colMeta == null || colMeta.isEmpty()) {
                tableSummary.put("status", "SKIPPED_NO_METADATA");
                tableSummaries.add(tableSummary);
                anyFailure = true;
                continue;
            }
            if (totalActions + diffs.size() > MAX_REPAIR_ACTIONS) {
                tableSummary.put("status", "SKIPPED_TOO_MANY_DIFFS");
                tableSummary.put("diffCount", diffs.size());
                tableSummaries.add(tableSummary);
                anyFailure = true;
                continue;
            }

            Set<String> binaryColumns = new HashSet<>();
            List<String> columnNames = new ArrayList<>();
            for (Map<String, Object> cm : colMeta) {
                String name = String.valueOf(cm.get("name"));
                String type = String.valueOf(cm.get("type")).toLowerCase();
                columnNames.add(name);
                if (type.contains("binary") || type.contains("blob") || type.contains("bytea") || "bit".equals(type)) {
                    binaryColumns.add(name);
                }
            }

            String routeDefaultDb = tr.get("routeDefaultTargetDb") instanceof String
                    ? (String) tr.get("routeDefaultTargetDb") : targetDb;
            RouteCompareSupport.MergeTarget mergeTarget = "MERGE".equals(routeMode)
                    ? RouteCompareSupport.mergeTargetOf(routeConfig, sourceDb, sourceTable, routeDefaultDb, routeNodeId)
                    : null;
            RouteCompareSupport.SplitTargets splitTargets = "SPLIT".equals(routeMode)
                    ? RouteCompareSupport.splitTargetsOf(routeConfig, sourceDb, sourceTable, routeDefaultDb)
                    : null;

            String targetQualified = mergeTarget != null
                ? qualifyFor(mergeTarget.database, mergeTarget.table, isPg)
                : (isPg ? quoteId(targetTable, true)
                        : quoteId(targetDb, false) + "." + quoteId(targetTable, false));
            Map<String, String> mergeTags = mergeTarget != null ? mergeTarget.tagFilters : Collections.emptyMap();

            int inserted = 0, updated = 0, deleted = 0, errors = 0;

            try (Connection targetConnDb = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(targetConn.type, targetConn.host, targetConn.port,
                        targetConn.database != null ? targetConn.database : targetDb),
                    targetConn.username, targetConn.password)) {

                for (Map<String, Object> diff : diffs) {
                    String diffType = (String) diff.get("diffType");
                    try {
                        if ("SOURCE_ONLY".equals(diffType) || "CONTENT_DIFF".equals(diffType)) {
                            String sourceDataJson = (String) diff.get("sourceData");
                            if (sourceDataJson == null) { errors++; continue; }
                            Map<String, Object> row = gson.fromJson(sourceDataJson, Map.class);
                            String writeTo = targetQualified;
                            if (splitTargets != null) {
                                // 分片键取自<b>源行</b>：这行本该去哪一片，由源端当前的值说了算
                                Integer shard = splitTargets.shardOf(shardKeyValue(row, splitTargets.shardKeyColumn));
                                if (shard == null) {
                                    errors++;
                                    logger.warn("修复表 {} 主键 {} 跳过：分片键算不出落点",
                                        sourceTable, diff.get("primaryKeyValue"));
                                    continue;
                                }
                                String[] read = splitTargets.shards.get(shard);
                                writeTo = qualifyFor(read[0], read[1], isPg);
                            }
                            executeUpsert(targetConnDb, writeTo, columnNames, pkCol, row, binaryColumns, isPg,
                                    mergeTags);
                            if ("SOURCE_ONLY".equals(diffType)) inserted++; else updated++;
                        } else if ("TARGET_ONLY".equals(diffType) || "WRONG_SHARD".equals(diffType)) {
                            // 错片行的修复是一次<b>搬迁</b>：先把它补到对的片上（差异带了源行才需要，
                            // 对的片上已有正确一份时不带），再删掉错片那份。顺序不能反——
                            // 先删后插中途失败还有源端可依据，先插后删失败会留下两份
                            Object pkValue = diff.get("primaryKeyValue");
                            if ("WRONG_SHARD".equals(diffType) && diff.get("sourceData") != null
                                    && splitTargets != null) {
                                Map<String, Object> row = gson.fromJson((String) diff.get("sourceData"), Map.class);
                                Integer shard = splitTargets.shardOf(
                                        shardKeyValue(row, splitTargets.shardKeyColumn));
                                if (shard != null) {
                                    String[] read = splitTargets.shards.get(shard);
                                    executeUpsert(targetConnDb, qualifyFor(read[0], read[1], isPg),
                                            columnNames, pkCol, row, binaryColumns, isPg, mergeTags);
                                    inserted++;
                                }
                            }
                            String deleteFrom = targetQualified;
                            if (diff.get("targetShard") instanceof String) {
                                String[] parts = ((String) diff.get("targetShard")).split("\\.", 2);
                                deleteFrom = parts.length == 2 ? qualifyFor(parts[0], parts[1], isPg)
                                        : qualifyFor(targetDb, parts[0], isPg);
                            }
                            StringBuilder sql = new StringBuilder("DELETE FROM ").append(deleteFrom)
                                    .append(" WHERE ").append(quoteId(pkCol, isPg)).append(" = ?");
                            for (String tag : mergeTags.keySet()) {
                                sql.append(" AND ").append(quoteId(tag, isPg)).append(" = ?");
                            }
                            try (PreparedStatement ps = targetConnDb.prepareStatement(sql.toString())) {
                                ps.setObject(1, pkValue);
                                int idx = 2;
                                for (String tagValue : mergeTags.values()) {
                                    ps.setString(idx++, tagValue);
                                }
                                ps.executeUpdate();
                            }
                            deleted++;
                        }
                    } catch (Exception e) {
                        errors++;
                        logger.warn("修复表 {} 主键 {} 失败: {}", sourceTable, diff.get("primaryKeyValue"), e.getMessage());
                    }
                }
            } catch (Exception e) {
                tableSummary.put("status", "ERROR");
                tableSummary.put("error", e.getMessage());
                tableSummaries.add(tableSummary);
                anyFailure = true;
                continue;
            }

            totalActions += diffs.size();

            // 修复后复核：重新对比行数确认收敛（闭环的最后一环，不做则修复是否生效全凭猜测）
            long newSourceCount = -1, newTargetCount = -1;
            boolean verified = false;
            try (Connection srcVerify = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(sourceConn.type, sourceConn.host, sourceConn.port,
                        sourceConn.database != null ? sourceConn.database : sourceDb),
                    sourceConn.username, sourceConn.password);
                 Connection tgtVerify = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(targetConn.type, targetConn.host, targetConn.port,
                        targetConn.database != null ? targetConn.database : targetDb),
                    targetConn.username, targetConn.password)) {
                newSourceCount = getRowCountSafe(srcVerify, sourceDb, sourceTable, isPg);
                // 复核也要按路由后的落点数，否则汇聚会拿"整张合并表"、拆分会拿"第一片"跟源表比，
                // 修得再对也永远显示没收敛
                if (mergeTarget != null) {
                    newTargetCount = getMergedRowCount(tgtVerify, mergeTarget, isPg);
                } else if (splitTargets != null) {
                    newTargetCount = 0;
                    for (String[] read : splitTargets.shards) {
                        long n = getRowCountSafe(tgtVerify, read[0], read[1], isPg);
                        if (n < 0) { newTargetCount = -1; break; }
                        newTargetCount += n;
                    }
                } else {
                    newTargetCount = getRowCountSafe(tgtVerify, targetDb, targetTable, isPg);
                }
                verified = newSourceCount >= 0 && newSourceCount == newTargetCount;
            } catch (Exception e) {
                logger.warn("修复后复核表 {} 失败: {}", sourceTable, e.getMessage());
            }

            tableSummary.put("status", errors == 0 ? "REPAIRED" : "PARTIAL");
            tableSummary.put("inserted", inserted);
            tableSummary.put("updated", updated);
            tableSummary.put("deleted", deleted);
            tableSummary.put("errors", errors);
            tableSummary.put("verifiedRowCountMatch", verified);
            tableSummary.put("sourceRowCountAfter", newSourceCount);
            tableSummary.put("targetRowCountAfter", newTargetCount);
            tableSummaries.add(tableSummary);

            if (errors > 0) anyFailure = true;
            if (!verified) allVerified = false;
        }

        String repairStatus = tableSummaries.isEmpty() ? "NONE" : (allVerified && !anyFailure ? "REPAIRED" : "PARTIAL");
        task.setRepairStatus(repairStatus);
        task.setRepairedAt(java.time.LocalDateTime.now());
        task.setRepairSummary(gson.toJson(Map.of("tables", tableSummaries)));
        validationTaskRepository.save(task);

        addLog(taskId, ValidationTaskLog.LogLevel.INFO,
            String.format("差异修复完成: 状态=%s, 共处理 %d 张表", repairStatus, tableSummaries.size()));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("repairStatus", repairStatus);
        result.put("tables", tableSummaries);
        return result;
    }

    /** 按源库快照 upsert 一行到目标（mysql: ON DUPLICATE KEY UPDATE；pg: ON CONFLICT DO UPDATE），二进制列先做 Base64 解码 */
    private void executeUpsert(Connection conn, String qualifiedTable, List<String> columnNames, String pkCol,
                                Map<String, Object> row, Set<String> binaryColumns, boolean isPg) throws SQLException {
        executeUpsert(conn, qualifiedTable, columnNames, pkCol, row, binaryColumns, isPg, Collections.emptyMap());
    }

    /**
     * @param extraColumns 固定值列（汇聚的来源标识列）：这些列不在源表里，但目标表的主键含它们，
     *                     不带上去就是插一行谁也认领不了的数据，而且下次对比照样是差异
     */
    private void executeUpsert(Connection conn, String qualifiedTable, List<String> columnNames, String pkCol,
                                Map<String, Object> row, Set<String> binaryColumns, boolean isPg,
                                Map<String, String> extraColumns) throws SQLException {
        List<String> nonPkColumns = columnNames.stream()
            .filter(c -> !c.equalsIgnoreCase(pkCol)).collect(Collectors.toList());

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(qualifiedTable).append(" (");
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) { sql.append(", "); placeholders.append(", "); }
            sql.append(quoteId(columnNames.get(i), isPg));
            placeholders.append("?");
        }
        for (String extra : extraColumns.keySet()) {
            sql.append(", ").append(quoteId(extra, isPg));
            placeholders.append(", ?");
        }
        sql.append(") VALUES (").append(placeholders).append(")");

        if (isPg) {
            // 汇聚目标表的主键是 源主键 + 来源标识列，冲突目标要写全，否则 ON CONFLICT 找不到约束
            StringBuilder conflictCols = new StringBuilder(quoteId(pkCol, true));
            for (String extra : extraColumns.keySet()) {
                conflictCols.append(", ").append(quoteId(extra, true));
            }
            sql.append(" ON CONFLICT (").append(conflictCols).append(") ");
            if (nonPkColumns.isEmpty()) {
                sql.append("DO NOTHING");
            } else {
                sql.append("DO UPDATE SET ");
                for (int i = 0; i < nonPkColumns.size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(quoteId(nonPkColumns.get(i), true)).append(" = EXCLUDED.").append(quoteId(nonPkColumns.get(i), true));
                }
            }
        } else {
            sql.append(" ON DUPLICATE KEY UPDATE ");
            if (nonPkColumns.isEmpty()) {
                sql.append(quoteId(pkCol, false)).append("=").append(quoteId(pkCol, false));
            } else {
                for (int i = 0; i < nonPkColumns.size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(quoteId(nonPkColumns.get(i), false)).append(" = VALUES(").append(quoteId(nonPkColumns.get(i), false)).append(")");
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < columnNames.size(); i++) {
                String col = columnNames.get(i);
                Object val = row.get(col);
                if (val != null && binaryColumns.contains(col)) {
                    val = Base64.getDecoder().decode(String.valueOf(val));
                }
                ps.setObject(i + 1, val);
            }
            int idx = columnNames.size() + 1;
            for (String value : extraColumns.values()) {
                ps.setString(idx++, value);
            }
            ps.executeUpdate();
        }
    }

    private String quoteId(String id, boolean isPg) {
        return isPg ? "\"" + id + "\"" : "`" + id + "`";
    }

    /** "库.表" 的方言限定名；PG 只用表名（与 getRowCount / 对比服务的口径保持一致）。 */
    private String qualifyFor(String db, String table, boolean isPg) {
        return isPg ? quoteId(table, true) : quoteId(db, false) + "." + quoteId(table, false);
    }

    /**
     * 从修复用的源行 JSON 里取分片键的值。列名大小写按目标端可能不同（MySQL 大小写不敏感），
     * 精确取不到就退回不区分大小写找一遍。
     */
    private Object shardKeyValue(Map<String, Object> row, String shardKeyColumn) {
        if (shardKeyColumn == null || shardKeyColumn.isEmpty()) {
            return null;
        }
        if (row.containsKey(shardKeyColumn)) {
            return row.get(shardKeyColumn);
        }
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey().equalsIgnoreCase(shardKeyColumn)) {
                return e.getValue();
            }
        }
        return null;
    }

    /**
     * 任务是否配置了列处理（syncObjects 任一库 entry 携带非空的
     * columnFilter/columnMapping/extraColumns）。列处理任务不支持行数/内容对比。
     */
    public boolean hasColumnProcessing(String syncObjectsJson) {
        // 实现落在 RouteConfigValidator：聚合路由的适用性校验也要判同一件事，两份判断迟早会漂
        return RouteConfigValidator.hasColumnProcessing(syncObjectsJson);
    }

    @SuppressWarnings("unchecked")
    /**
     * 解析 syncObjects 里表级 entry 的表名映射：db → {源表: 目标表}。
     * 压平表清单的解析方法（parseSyncObjects / parseSyncObjectsSimple）会丢掉 tableMapping，
     * 对比目标端取数前需要用它换算目标表名，否则映射表必报"目标表不存在/行数为 0"。
     */
    private Map<String, Map<String, String>> parseTableMappings(String syncObjectsJson) {
        Map<String, Map<String, String>> result = new HashMap<>();
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            return result;
        }
        try {
            Map<String, Object> raw = gson.fromJson(syncObjectsJson, Map.class);
            if (raw == null) return result;
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                if (!(entry.getValue() instanceof Map)) continue;
                Object mappingObj = ((Map<?, ?>) entry.getValue()).get("tableMapping");
                if (mappingObj instanceof Map) {
                    Map<String, String> m = new HashMap<>();
                    ((Map<?, ?>) mappingObj).forEach((k, v) -> m.put(String.valueOf(k), String.valueOf(v)));
                    if (!m.isEmpty()) {
                        result.put(entry.getKey(), m);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("解析 tableMapping 失败: {}", e.getMessage());
        }
        return result;
    }

    /**
     * 解析 syncObjects 里每库 entry 的库名映射（targetDb）：源库 → 目标库。
     * 压平表清单的解析方法会丢掉它；多库任务的对比目标端必须按源库逐一解析目标库，
     * 否则全部串到连接串里的单一库/首库上（与全量/增量链路 P0 串库同一性质）。
     * 优先级与 agent ConfigService 一致：entry.targetDb ＞ 全局 targetDbName ＞ 与源库同名。
     */
    private Map<String, String> parseDbMappings(String syncObjectsJson) {
        Map<String, String> result = new HashMap<>();
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            return result;
        }
        try {
            Map<String, Object> raw = gson.fromJson(syncObjectsJson, Map.class);
            if (raw == null) return result;
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                if (!(entry.getValue() instanceof Map)) continue;
                Object targetDbObj = ((Map<?, ?>) entry.getValue()).get("targetDb");
                if (targetDbObj instanceof String && !((String) targetDbObj).isEmpty()) {
                    result.put(entry.getKey(), (String) targetDbObj);
                }
            }
        } catch (Exception e) {
            logger.warn("解析库名映射失败: {}", e.getMessage());
        }
        return result;
    }

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
                    // 库级同步对象（{"db":{"dbLevel":true}}）：以空表清单占位，
                    // 对比执行时按库枚举全部表——范围严格等于任务同步的库，而非整个实例
                    if (Boolean.TRUE.equals(inner.get("dbLevel"))) {
                        result.put(dbName, new ArrayList<>());
                        continue;
                    }
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

        Map<String, Map<String, List<String>>> syncObjects = parseSyncObjects(task.getSyncObjects());
        boolean isDrTaskForConn = "DR".equals(task.getTaskType());

        String sourceConnStr = task.getSourceConnection();
        String targetConnStr = task.getTargetConnection();
        if (!isDrTaskForConn && !syncObjects.isEmpty()) {
            // 非灾备且已指定同步对象：源/目标库名可能不同（如 prod -> prod_replica），
            // 显式带上各自库名，避免连接串未含库名时目标连接落到错误的库（见下方 targetDbName 兜底）
            sourceConnStr = withDatabase(sourceConnStr, task.getSourceDbName());
            targetConnStr = withDatabase(targetConnStr, task.getTargetDbName());
        }

        ParsedConnection sourceConn = parseConnection(sourceConnStr);
        ParsedConnection targetConn = parseConnection(targetConnStr);

        if ("mongodb".equalsIgnoreCase(sourceConn.type)) {
            executeMongoRowCountCompare(task, sourceConn, targetConn);
            return;
        }

        if ("elastic".equalsIgnoreCase(targetConn.type)) {
            executeElasticRowCountCompare(task, sourceConn, targetConn);
            return;
        }

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
            } else {
                // 库级同步对象（空表清单占位）：按库枚举全部表——对比范围严格等于任务同步的库
                boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceConn.type);
                for (Map.Entry<String, Map<String, List<String>>> e : syncObjects.entrySet()) {
                    List<String> t = e.getValue().get("tables");
                    if (t == null || t.isEmpty()) {
                        List<String> all = getTableNames(sourceDb, e.getKey(), sourceIsPg);
                        e.getValue().put("tables", all);
                        addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                            "库级同步对象，行数对比范围为库 " + e.getKey() + " 的全部表（" + all.size() + " 个）");
                    }
                }
            }

            // 聚合路由（汇聚/拆分）：对比要按路由后的真实落点统计。
            // 来源实例标识与引擎侧同一套兜底规则：路由配置里写了就用它，否则源实例 host:port。
            String routeConfig = routeConfigOf(task);
            String routeNodeId = routeNodeIdOf(routeConfig, sourceConn);

            // 表名映射（表级同步）：目标端行数按映射后的表名统计
            Map<String, Map<String, String>> tableMappings = parseTableMappings(task.getSyncObjects());
            // 库名映射：多库任务目标库按源库逐一解析（entry.targetDb ＞ 连接串库名 ＞ 同名）
            Map<String, String> dbMappings = parseDbMappings(task.getSyncObjects());

            for (Map.Entry<String, Map<String, List<String>>> dbEntry : syncObjects.entrySet()) {
                String sourceDbName = dbEntry.getKey();
                String targetDbName = dbMappings.getOrDefault(sourceDbName,
                    targetConn.database != null ? targetConn.database : sourceDbName);
                List<String> tables = dbEntry.getValue().get("tables");

                if (tables == null || tables.isEmpty()) continue;

                Map<String, String> dbTableMapping = tableMappings.getOrDefault(sourceDbName, Collections.emptyMap());
                for (String tableName : tables) {
                    String targetTableName = dbTableMapping.getOrDefault(tableName, tableName);
                    totalTables++;
                    // 路由感知：汇聚的目标是合并表里属于本源表的那部分行，拆分的目标是全部分片之和。
                    // 不解析路由的话，对比会拿源表名去目标端找同名表——汇聚/拆分下那张表根本不存在，
                    // 结果是"目标端 0 行"，看着像数据全丢了。
                    RouteCompareSupport.MergeTarget mergeTarget = RouteCompareSupport.mergeTargetOf(
                            routeConfig, sourceDbName, tableName, targetDbName, routeNodeId);
                    RouteCompareSupport.SplitTargets splitTargets = RouteCompareSupport.splitTargetsOf(
                            routeConfig, sourceDbName, tableName, targetDbName);
                    if (mergeTarget != null) {
                        targetDbName = mergeTarget.database;
                        targetTableName = mergeTarget.table;
                    }
                    boolean renamed = !targetTableName.equals(tableName) || !targetDbName.equals(sourceDbName);
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "行数对比表: " + sourceDbName + "." + tableName
                            + (splitTargets != null
                                ? " → " + splitTargets.shards.size() + " 个分片之和"
                                : (renamed ? " → " + targetDbName + "." + targetTableName : ""))
                            + (mergeTarget != null && !mergeTarget.tagFilters.isEmpty()
                                ? "（按来源标识 " + mergeTarget.tagFilters + " 切片）" : ""));

                    try {
                        TableDiffResult diff = compareTableDataRouted(
                            sourceDb, targetDb, sourceDbName, targetDbName, tableName, targetTableName,
                            mergeTarget, splitTargets);

                        totalRows += diff.totalRows;
                        mismatchedRows += diff.mismatchedRows;

                        Map<String, Object> tr = new LinkedHashMap<>();
                        tr.put("sourceTable", tableName);
                        tr.put("targetTable", targetTableName);
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

    /**
     * MongoDB 行数对比：按集合文档数（countDocuments 精确计数）对比源/目标。
     * 结果 JSON 与 SQL 路径同构（tables[].sourceRowCount/targetRowCount/status），前端复用同一渲染。
     * 库级同步对象（空集合清单）按源库实时枚举集合，范围与任务同步范围一致。
     */
    private void executeMongoRowCountCompare(ValidationTask task, ParsedConnection sourceConn, ParsedConnection targetConn) {
        String taskId = task.getId();
        Map<String, Map<String, List<String>>> syncObjects = parseSyncObjects(task.getSyncObjects());

        int totalTables = 0;
        int passedTables = 0;
        int failedTables = 0;
        long totalRows = 0;
        long mismatchedRows = 0;
        List<Map<String, Object>> tableResults = new ArrayList<>();

        try (com.mongodb.client.MongoClient sourceClient = buildMongoClient(sourceConn);
             com.mongodb.client.MongoClient targetClient = buildMongoClient(targetConn)) {

            if (syncObjects.isEmpty()) {
                for (String dbName : sourceClient.listDatabaseNames()) {
                    if ("admin".equals(dbName) || "local".equals(dbName) || "config".equals(dbName)) {
                        continue;
                    }
                    Map<String, List<String>> tableMap = new HashMap<>();
                    tableMap.put("tables", new ArrayList<>());
                    syncObjects.put(dbName, tableMap);
                }
                addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                    "sync_objects 为空，自动获取源端所有数据库: " + syncObjects.keySet());
            }

            // 库级同步对象（空集合清单占位）：按库枚举全部集合
            for (Map.Entry<String, Map<String, List<String>>> e : syncObjects.entrySet()) {
                List<String> t = e.getValue().get("tables");
                if (t == null || t.isEmpty()) {
                    List<String> all = new ArrayList<>();
                    for (String name : sourceClient.getDatabase(e.getKey()).listCollectionNames()) {
                        if (!name.startsWith("system.")) {
                            all.add(name);
                        }
                    }
                    e.getValue().put("tables", all);
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "库级同步对象，行数对比范围为库 " + e.getKey() + " 的全部集合（" + all.size() + " 个）");
                }
            }

            for (Map.Entry<String, Map<String, List<String>>> dbEntry : syncObjects.entrySet()) {
                String sourceDbName = dbEntry.getKey();
                // Mongo 同步为按库镜像（MongoSyncMain 写目标同名库），多库任务的 targetDbName
                // 字段仅为展示兜底，不能作为对比目标库——始终对比目标端的同名库
                String targetDbName = sourceDbName;
                List<String> collections = dbEntry.getValue().get("tables");
                if (collections == null || collections.isEmpty()) continue;

                for (String collName : collections) {
                    totalTables++;
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "文档数对比集合: " + sourceDbName + "." + collName);

                    Map<String, Object> tr = new LinkedHashMap<>();
                    tr.put("sourceTable", collName);
                    tr.put("targetTable", collName);
                    try {
                        long srcCount = sourceClient.getDatabase(sourceDbName)
                                .getCollection(collName).countDocuments();
                        long tgtCount = targetClient.getDatabase(targetDbName)
                                .getCollection(collName).countDocuments();
                        tr.put("sourceRowCount", srcCount);
                        tr.put("targetRowCount", tgtCount);
                        totalRows += srcCount;

                        if (srcCount == tgtCount) {
                            passedTables++;
                            tr.put("status", "MATCH");
                            addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                                "集合 " + sourceDbName + "." + collName + " 文档数对比通过，共 " + srcCount + " 条");
                        } else {
                            failedTables++;
                            long diff = Math.abs(srcCount - tgtCount);
                            mismatchedRows += diff;
                            tr.put("status", "MISMATCH");
                            tr.put("diffCount", diff);
                            addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                                "集合 " + sourceDbName + "." + collName + " 文档数不一致，源 " + srcCount + " 条 / 目标 " + tgtCount + " 条");
                        }
                    } catch (Exception e) {
                        failedTables++;
                        tr.put("status", "ERROR");
                        addLog(taskId, ValidationTaskLog.LogLevel.ERROR,
                            "集合 " + sourceDbName + "." + collName + " 对比异常: " + e.getMessage());
                    }
                    tableResults.add(tr);
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
                String.format("文档数对比完成: 共 %d 个集合，通过 %d 个，失败 %d 个，总文档数 %d，差异数 %d",
                    totalTables, passedTables, failedTables, totalRows, mismatchedRows));

        } catch (Exception e) {
            logger.error("MongoDB 文档数对比执行失败: {}", e.getMessage(), e);
            task.setStatus(ValidationTask.ValidationStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);
            addLog(taskId, ValidationTaskLog.LogLevel.ERROR, "文档数对比执行失败: " + e.getMessage());
        }
    }

    /**
     * MySQL → Elasticsearch 行数对比：源表 COUNT(*) 对比目标索引 _count（先 _refresh 保证近实时可见）。
     * 索引名与 ElasticSyncMain 的映射规则一致：{库名}_{表名} 小写。
     * 结果 JSON 与 SQL 路径同构，前端复用同一渲染。
     */
    private void executeElasticRowCountCompare(ValidationTask task, ParsedConnection sourceConn, ParsedConnection targetConn) {
        String taskId = task.getId();
        Map<String, Map<String, List<String>>> syncObjects = parseSyncObjects(task.getSyncObjects());

        int totalTables = 0;
        int passedTables = 0;
        int failedTables = 0;
        long totalRows = 0;
        long mismatchedRows = 0;
        List<Map<String, Object>> tableResults = new ArrayList<>();

        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn.type, sourceConn.host, sourceConn.port, null),
                sourceConn.username, sourceConn.password)) {

            // 库级同步对象（空表清单占位）：按库枚举全部表——对比范围严格等于任务同步的库
            for (Map.Entry<String, Map<String, List<String>>> e : syncObjects.entrySet()) {
                List<String> t = e.getValue().get("tables");
                if (t == null || t.isEmpty()) {
                    List<String> all = getTableNames(sourceDb, e.getKey(), false);
                    e.getValue().put("tables", all);
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "库级同步对象，行数对比范围为库 " + e.getKey() + " 的全部表（" + all.size() + " 个）");
                }
            }

            for (Map.Entry<String, Map<String, List<String>>> dbEntry : syncObjects.entrySet()) {
                String dbName = dbEntry.getKey();
                List<String> tables = dbEntry.getValue().get("tables");
                if (tables == null || tables.isEmpty()) continue;

                for (String tableName : tables) {
                    totalTables++;
                    String indexName = (dbName + "_" + tableName).toLowerCase();
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                        "行数对比: " + dbName + "." + tableName + " -> 索引 " + indexName);

                    Map<String, Object> tr = new LinkedHashMap<>();
                    tr.put("sourceTable", tableName);
                    tr.put("targetTable", indexName);
                    try {
                        long srcCount;
                        try (java.sql.Statement stmt = sourceDb.createStatement();
                             java.sql.ResultSet rs = stmt.executeQuery(
                                 "SELECT COUNT(*) FROM `" + dbName + "`.`" + tableName + "`")) {
                            rs.next();
                            srcCount = rs.getLong(1);
                        }
                        long tgtCount = esCountDocs(targetConn, indexName);
                        tr.put("sourceRowCount", srcCount);
                        tr.put("targetRowCount", tgtCount);
                        totalRows += srcCount;

                        if (srcCount == tgtCount) {
                            passedTables++;
                            tr.put("status", "MATCH");
                            addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                                "表 " + dbName + "." + tableName + " 行数对比通过，共 " + srcCount + " 行");
                        } else {
                            failedTables++;
                            long diff = Math.abs(srcCount - tgtCount);
                            mismatchedRows += diff;
                            tr.put("status", "MISMATCH");
                            tr.put("diffCount", diff);
                            addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                                "表 " + dbName + "." + tableName + " 行数不一致，源 " + srcCount + " 行 / 目标索引 " + tgtCount + " 条");
                        }
                    } catch (Exception e) {
                        failedTables++;
                        tr.put("status", "ERROR");
                        addLog(taskId, ValidationTaskLog.LogLevel.ERROR,
                            "表 " + dbName + "." + tableName + " 对比异常: " + e.getMessage());
                    }
                    tableResults.add(tr);
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
                String.format("行数对比完成: 共 %d 个表，通过 %d 个，失败 %d 个，总行数 %d，差异数 %d",
                    totalTables, passedTables, failedTables, totalRows, mismatchedRows));

        } catch (Exception e) {
            logger.error("Elasticsearch 行数对比执行失败: {}", e.getMessage(), e);
            task.setStatus(ValidationTask.ValidationStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);
            addLog(taskId, ValidationTaskLog.LogLevel.ERROR, "行数对比执行失败: " + e.getMessage());
        }
    }

    /** _refresh 后 _count，保证 near-real-time 写入可见。 */
    private long esCountDocs(ParsedConnection conn, String index) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(15)).build();
        String base = "http://" + conn.host + ":" + conn.port;
        String auth = null;
        if (conn.username != null && !conn.username.isEmpty()) {
            auth = "Basic " + java.util.Base64.getEncoder().encodeToString(
                (conn.username + ":" + (conn.password == null ? "" : conn.password))
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        java.net.http.HttpRequest.Builder refreshReq = java.net.http.HttpRequest.newBuilder(
                java.net.URI.create(base + "/" + index + "/_refresh"))
            .timeout(java.time.Duration.ofSeconds(15))
            .POST(java.net.http.HttpRequest.BodyPublishers.noBody());
        if (auth != null) refreshReq.header("Authorization", auth);
        client.send(refreshReq.build(), java.net.http.HttpResponse.BodyHandlers.discarding());

        java.net.http.HttpRequest.Builder countReq = java.net.http.HttpRequest.newBuilder(
                java.net.URI.create(base + "/" + index + "/_count"))
            .timeout(java.time.Duration.ofSeconds(15)).GET();
        if (auth != null) countReq.header("Authorization", auth);
        java.net.http.HttpResponse<String> resp = client.send(countReq.build(),
            java.net.http.HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 404) {
            return 0; // 索引不存在按 0 条计
        }
        if (resp.statusCode() >= 300) {
            throw new RuntimeException("ES _count HTTP " + resp.statusCode());
        }
        return com.google.gson.JsonParser.parseString(resp.body()).getAsJsonObject().get("count").getAsLong();
    }

    /**
     * MongoDB 内容对比：按库/集合遍历，两端按 _id 逐文档比对（镜像同步，目标库名=源库名）。
     * 差异分三类，与 SQL 内容对比同构，供前端复用同一渲染：
     *   CONTENT_DIFF（字段值不同，附 diffFields）/ SOURCE_ONLY（仅源端有）/ TARGET_ONLY（仅目标端有）。
     * 每集合最多收集 MAX_DIFFS 条差异用于展示（与 SQL 路径 findDiffs 上限一致）。
     * MongoDB 无 SQL 意义的"修复"，故 diffs 仅用于展示、不含可执行修复所需的列类型元数据。
     */
    private void executeMongoContentCompare(ValidationTask task) {
        String taskId = task.getId();
        Map<String, Map<String, List<String>>> syncObjects = parseSyncObjects(task.getSyncObjects());
        ParsedConnection sourceConn = parseConnection(task.getSourceConnection());
        ParsedConnection targetConn = parseConnection(task.getTargetConnection());
        final int MAX_DIFFS = 100;

        int totalTables = 0;
        int passedTables = 0;
        int failedTables = 0;
        long totalDiffs = 0;
        List<Map<String, Object>> tableResults = new ArrayList<>();

        try (com.mongodb.client.MongoClient sourceClient = buildMongoClient(sourceConn);
             com.mongodb.client.MongoClient targetClient = buildMongoClient(targetConn)) {

            // 同步对象归一：空 → 源端全部业务库；库级空集合清单 → 枚举该库全部集合
            if (syncObjects.isEmpty()) {
                for (String dbName : sourceClient.listDatabaseNames()) {
                    if ("admin".equals(dbName) || "local".equals(dbName) || "config".equals(dbName)) continue;
                    Map<String, List<String>> tm = new HashMap<>();
                    tm.put("tables", new ArrayList<>());
                    syncObjects.put(dbName, tm);
                }
            }
            for (Map.Entry<String, Map<String, List<String>>> e : syncObjects.entrySet()) {
                List<String> t = e.getValue().get("tables");
                if (t == null || t.isEmpty()) {
                    List<String> all = new ArrayList<>();
                    for (String name : sourceClient.getDatabase(e.getKey()).listCollectionNames()) {
                        if (!name.startsWith("system.")) all.add(name);
                    }
                    e.getValue().put("tables", all);
                }
            }

            for (Map.Entry<String, Map<String, List<String>>> dbEntry : syncObjects.entrySet()) {
                String dbName = dbEntry.getKey();
                List<String> collections = dbEntry.getValue().get("tables");
                if (collections == null || collections.isEmpty()) continue;

                for (String collName : collections) {
                    totalTables++;
                    addLog(taskId, ValidationTaskLog.LogLevel.INFO, "内容对比集合: " + dbName + "." + collName);
                    com.mongodb.client.MongoCollection<org.bson.Document> srcColl =
                            sourceClient.getDatabase(dbName).getCollection(collName);
                    com.mongodb.client.MongoCollection<org.bson.Document> tgtColl =
                            targetClient.getDatabase(dbName).getCollection(collName);

                    Map<String, Object> tr = new LinkedHashMap<>();
                    tr.put("sourceTable", collName);
                    tr.put("targetTable", collName);
                    tr.put("sourceDb", dbName);
                    tr.put("targetDb", dbName);
                    tr.put("primaryKeyColumn", "_id");
                    List<Map<String, Object>> diffs = new ArrayList<>();
                    try {
                        long srcCount = srcColl.countDocuments();
                        long tgtCount = tgtColl.countDocuments();
                        tr.put("sourceRowCount", srcCount);
                        tr.put("targetRowCount", tgtCount);

                        // 源端全扫描：目标按 _id 点查（_id 天然有索引）比对；缺失=SOURCE_ONLY，值不同=CONTENT_DIFF
                        try (com.mongodb.client.MongoCursor<org.bson.Document> cur = srcColl.find().iterator()) {
                            while (cur.hasNext() && diffs.size() < MAX_DIFFS) {
                                org.bson.Document sdoc = cur.next();
                                Object id = sdoc.get("_id");
                                org.bson.Document tdoc = tgtColl.find(new org.bson.Document("_id", id)).first();
                                if (tdoc == null) {
                                    diffs.add(mongoDiff(id, "SOURCE_ONLY", null, sdoc, null));
                                } else {
                                    List<String> diffFields = mongoDiffFields(sdoc, tdoc);
                                    if (!diffFields.isEmpty()) {
                                        diffs.add(mongoDiff(id, "CONTENT_DIFF", diffFields, sdoc, tdoc));
                                    }
                                }
                            }
                        }
                        // 目标端反扫：找仅目标端存在的文档（TARGET_ONLY）
                        try (com.mongodb.client.MongoCursor<org.bson.Document> cur = tgtColl.find().iterator()) {
                            while (cur.hasNext() && diffs.size() < MAX_DIFFS) {
                                org.bson.Document tdoc = cur.next();
                                Object id = tdoc.get("_id");
                                if (srcColl.find(new org.bson.Document("_id", id)).first() == null) {
                                    diffs.add(mongoDiff(id, "TARGET_ONLY", null, null, tdoc));
                                }
                            }
                        }

                        boolean match = diffs.isEmpty() && srcCount == tgtCount;
                        tr.put("checksumMatch", match);
                        tr.put("diffCount", diffs.size());
                        tr.put("diffs", diffs);
                        if (match) {
                            passedTables++;
                            tr.put("status", "MATCH");
                            addLog(taskId, ValidationTaskLog.LogLevel.INFO, "集合 " + dbName + "." + collName + " 内容一致");
                        } else {
                            failedTables++;
                            totalDiffs += diffs.size();
                            tr.put("status", "MISMATCH");
                            addLog(taskId, ValidationTaskLog.LogLevel.WARNING,
                                "集合 " + dbName + "." + collName + " 内容不一致，差异文档数: " + diffs.size()
                                + (diffs.size() >= MAX_DIFFS ? "（已达展示上限）" : ""));
                        }
                    } catch (Exception ex) {
                        failedTables++;
                        tr.put("checksumMatch", false);
                        tr.put("status", "ERROR");
                        tr.put("diffCount", 0);
                        tr.put("diffs", Collections.emptyList());
                        addLog(taskId, ValidationTaskLog.LogLevel.ERROR,
                            "集合 " + dbName + "." + collName + " 内容对比异常: " + ex.getMessage());
                    }
                    tableResults.add(tr);
                }
            }

            task.setTotalTables(totalTables);
            task.setPassedTables(passedTables);
            task.setFailedTables(failedTables);
            task.setMismatchedRows(totalDiffs);
            task.setCompareResult(gson.toJson(Map.of("tables", tableResults)));
            task.setStatus(ValidationTask.ValidationStatus.COMPLETED);
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);

            addLog(taskId, ValidationTaskLog.LogLevel.INFO,
                String.format("内容对比完成: 共 %d 个集合，一致 %d 个，不一致 %d 个，差异文档数 %d",
                    totalTables, passedTables, failedTables, totalDiffs));

        } catch (Exception e) {
            logger.error("MongoDB 内容对比执行失败: {}", e.getMessage(), e);
            task.setStatus(ValidationTask.ValidationStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            task.setCompletedAt(java.time.LocalDateTime.now());
            validationTaskRepository.save(task);
            addLog(taskId, ValidationTaskLog.LogLevel.ERROR, "内容对比执行失败: " + e.getMessage());
        }
    }

    /** 组装一条与 SQL 内容对比同构的 Mongo 差异记录（sourceData/targetData 为文档 JSON）。 */
    private Map<String, Object> mongoDiff(Object id, String diffType, List<String> diffFields,
                                          org.bson.Document sdoc, org.bson.Document tdoc) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("primaryKeyValue", String.valueOf(id));
        d.put("diffType", diffType);
        if (diffFields != null) d.put("diffFields", diffFields);
        if (sdoc != null) d.put("sourceData", sdoc.toJson());
        if (tdoc != null) d.put("targetData", tdoc.toJson());
        return d;
    }

    /** 两文档差异字段（并集逐字段规范化比较）；_id 相同故跳过。 */
    private List<String> mongoDiffFields(org.bson.Document sdoc, org.bson.Document tdoc) {
        java.util.Set<String> keys = new java.util.LinkedHashSet<>();
        keys.addAll(sdoc.keySet());
        keys.addAll(tdoc.keySet());
        List<String> diff = new ArrayList<>();
        for (String k : keys) {
            if ("_id".equals(k)) continue;
            if (!mongoValEquals(sdoc.get(k), tdoc.get(k))) diff.add(k);
        }
        return diff;
    }

    /** 规范化的 BSON 值相等判断：嵌套文档/数组按其 JSON/字符串表示比较，数字按字符串比较。 */
    private boolean mongoValEquals(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return mongoCanon(a).equals(mongoCanon(b));
    }

    private String mongoCanon(Object v) {
        if (v == null) return "null";
        if (v instanceof org.bson.Document) return ((org.bson.Document) v).toJson();
        return String.valueOf(v);
    }

    /** 一次性 MongoClient（directConnection 只连指定节点），调用方负责 close。 */
    private com.mongodb.client.MongoClient buildMongoClient(ParsedConnection conn) {
        String userInfo = "";
        if (conn.username != null && !conn.username.isEmpty()) {
            String u = java.net.URLEncoder.encode(conn.username, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
            String p = java.net.URLEncoder.encode(conn.password == null ? "" : conn.password, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
            userInfo = u + ":" + p + "@";
        }
        String uri = String.format(
            "mongodb://%s%s:%d/?authSource=admin&directConnection=true"
                + "&connectTimeoutMS=15000&socketTimeoutMS=15000&serverSelectionTimeoutMS=15000",
            userInfo, conn.host, conn.port);
        return com.mongodb.client.MongoClients.create(uri);
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
                    // 库级同步对象：空表清单占位，执行时按库枚举全部表
                    if (Boolean.TRUE.equals(inner.get("dbLevel"))) {
                        tableMap.put("tables", new ArrayList<>());
                    } else if (inner.containsKey("tables") && inner.get("tables") instanceof List) {
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

    /** 对比任务所属同步任务的路由配置；取不到返回 null（按 1:1 对比）。 */
    private String routeConfigOf(ValidationTask task) {
        if (task.getWorkflowId() == null) {
            return null;
        }
        return workflowRepository.findById(task.getWorkflowId())
                .map(Workflow::getRouteConfig).orElse(null);
    }

    /**
     * 来源实例标识。必须与引擎写进 {@code _src_node} 列的值一致，否则汇聚对比的过滤条件
     * 一行都命中不了（看着像目标端 0 行）。规则与 {@code MigrationConfig#getRouteNodeId} 相同：
     * 路由配置里显式写了就用它（跨实例 leg），否则源实例 {@code host:port}。
     */
    @SuppressWarnings("unchecked")
    private String routeNodeIdOf(String routeConfig, ParsedConnection sourceConn) {
        if (routeConfig != null && !routeConfig.isEmpty()) {
            try {
                Map<String, Object> root = new com.google.gson.Gson().fromJson(routeConfig, Map.class);
                Object nodeId = root == null ? null : root.get("nodeId");
                if (nodeId != null && !String.valueOf(nodeId).trim().isEmpty()) {
                    return String.valueOf(nodeId).trim();
                }
            } catch (RuntimeException e) {
                logger.warn("解析路由配置的 nodeId 失败: {}", e.getMessage());
            }
        }
        return sourceConn == null ? "" : sourceConn.host + ":" + sourceConn.port;
    }

    /**
     * 路由感知的行数对比：
     * <ul>
     *   <li>汇聚：目标行数 = 合并表里 {@code _src_*} 等于本源表的那部分行；</li>
     *   <li>拆分：目标行数 = 全部分片表行数之和；</li>
     *   <li>未路由：与原来完全一致。</li>
     * </ul>
     */
    private TableDiffResult compareTableDataRouted(Connection sourceDb, Connection targetDb,
            String sourceDbName, String targetDbName, String tableName, String targetTableName,
            RouteCompareSupport.MergeTarget mergeTarget, RouteCompareSupport.SplitTargets splitTargets) {
        if (mergeTarget == null && splitTargets == null) {
            return compareTableData(sourceDb, targetDb, sourceDbName, targetDbName, tableName, targetTableName);
        }
        TableDiffResult result = new TableDiffResult();
        try {
            boolean sourceIsPg = isPostgresqlConnection(sourceDb);
            boolean targetIsPg = isPostgresqlConnection(targetDb);
            long sourceRowCount = getRowCountSafe(sourceDb, sourceDbName, tableName, sourceIsPg);
            long targetRowCount;
            if (splitTargets != null) {
                targetRowCount = 0;
                for (String[] shard : splitTargets.shards) {
                    long shardRows = getRowCountSafe(targetDb, shard[0], shard[1], targetIsPg);
                    if (shardRows < 0) {
                        result.error = "分片 " + shard[0] + "." + shard[1] + " 行数获取失败";
                        return result;
                    }
                    targetRowCount += shardRows;
                }
            } else {
                targetRowCount = getMergedRowCount(targetDb, mergeTarget, targetIsPg);
            }
            if (sourceRowCount < 0 || targetRowCount < 0) {
                result.error = "获取行数失败";
                return result;
            }
            result.sourceRowCount = sourceRowCount;
            result.targetRowCount = targetRowCount;
            result.totalRows = sourceRowCount;
            result.mismatchedRows = sourceRowCount == targetRowCount
                    ? 0 : Math.abs(sourceRowCount - targetRowCount);
        } catch (Exception e) {
            result.error = e.getMessage();
        }
        return result;
    }

    /** 汇聚表里属于某个源表的行数（按来源标识列过滤）。 */
    private long getMergedRowCount(Connection targetDb, RouteCompareSupport.MergeTarget target, boolean isPg) {
        String qualified = isPg ? quoteId(target.table, true)
                : quoteId(target.database, false) + "." + quoteId(target.table, false);
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(qualified);
        List<String> values = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, String> filter : target.tagFilters.entrySet()) {
            sql.append(i++ == 0 ? " WHERE " : " AND ")
               .append(quoteId(filter.getKey(), isPg)).append("=?");
            values.add(filter.getValue());
        }
        try (java.sql.PreparedStatement ps = targetDb.prepareStatement(sql.toString())) {
            for (int p = 0; p < values.size(); p++) {
                ps.setString(p + 1, values.get(p));
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            if (msg.contains("doesn't exist") || msg.contains("does not exist")) {
                logger.info("汇聚目标表 {}.{} 不存在，视为 0 行", target.database, target.table);
                return 0;
            }
            logger.warn("汇聚目标表 {}.{} 行数获取失败: {}", target.database, target.table, msg);
            return -1;
        }
    }

    private TableDiffResult compareTableData(Connection sourceDb, Connection targetDb,
            String sourceDbName, String targetDbName, String tableName, String targetTableName) {
        TableDiffResult result = new TableDiffResult();

        try {
            boolean sourceIsPg = isPostgresqlConnection(sourceDb);
            boolean targetIsPg = isPostgresqlConnection(targetDb);

            long sourceRowCount = getRowCountSafe(sourceDb, sourceDbName, tableName, sourceIsPg);
            // 表名映射（表级同步）：目标端按映射后的表名取行数
            long targetRowCount = getRowCountSafe(targetDb, targetDbName, targetTableName, targetIsPg);

            if (sourceRowCount < 0 || targetRowCount < 0) {
                result.error = "获取行数失败";
                return result;
            }

            result.sourceRowCount = sourceRowCount;
            result.targetRowCount = targetRowCount;
            result.totalRows = sourceRowCount;

            if (sourceRowCount != targetRowCount) {
                result.mismatchedRows = Math.abs(sourceRowCount - targetRowCount);
                addLogForCurrentTask("行数对比: 源库 " + sourceDbName + "." + tableName + " 行数=" + sourceRowCount + ", 目标库 " + targetDbName + "." + targetTableName + " 行数=" + targetRowCount);
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

    private static final Set<String> IGNORED_TABLES = Set.of("__sync_heartbeat", "__sync_origin");

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
