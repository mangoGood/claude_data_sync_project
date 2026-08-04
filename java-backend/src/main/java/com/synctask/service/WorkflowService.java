package com.synctask.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.synctask.dto.TaskCreatedMessage;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import jakarta.persistence.criteria.Predicate;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class WorkflowService {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);
    private static final Gson gson = new Gson();

    /** 任务名白名单：中英文、数字、空格、常见连接符。禁止 < > " ' & 等可用于 XSS 的字符（前端渲染已转义，此为纵深防御）。 */
    private static final java.util.regex.Pattern TASK_NAME_PATTERN =
            java.util.regex.Pattern.compile("^[\\u4e00-\\u9fa5A-Za-z0-9 _\\-.()（）\\[\\]]{1,100}$");

    private void validateTaskName(String name) {
        if (name == null || name.isEmpty()) {
            throw new RuntimeException("任务名称不能为空");
        }
        if (!TASK_NAME_PATTERN.matcher(name).matches()) {
            throw new RuntimeException("任务名称包含非法字符，仅允许中英文、数字、空格及 _-.()[] 等符号，长度不超过100");
        }
    }

    /**
     * 订阅任务的出口恒为 Kafka，与"目标库类型"无关——前端为了复用配置表单会把 targetType
     * 填成与 sourceType 相同的值。因此配对校验对订阅任务只看源类型，不看目标类型，
     * 否则 tidb 源的订阅会被"TiDB 不支持作为同步目标"误拦。
     */
    private static boolean isSubscribeTaskType(String taskType) {
        return "SUBSCRIBE".equals(taskType);
    }

    /**
     * 非 SQL 管线类型的配对约束：
     * <ul>
     *   <li>MongoDB 只支持 mongodb→mongodb（副本集到副本集），实时同步与灾备（单向/双向）均可，
     *       订阅任务则是 mongodb→Kafka；</li>
     *   <li>Elasticsearch 只能作为目标，且源必须是 MySQL（binlog 增量捕获），仅实时同步。</li>
     * </ul>
     */
    private void validateMongoTypePairing(String sourceType, String targetType, String taskType) {
        boolean srcMongo = "mongodb".equalsIgnoreCase(sourceType);
        boolean tgtMongo = "mongodb".equalsIgnoreCase(targetType);
        boolean srcEs = "elasticsearch".equalsIgnoreCase(sourceType);
        boolean tgtEs = "elasticsearch".equalsIgnoreCase(targetType);
        validateTidbTypePairing(sourceType, targetType, taskType);
        if (!srcMongo && !tgtMongo && !srcEs && !tgtEs) {
            return;
        }
        if (srcEs) {
            throw new RuntimeException("Elasticsearch 不支持作为同步源，仅支持 MySQL 到 Elasticsearch");
        }
        if (srcMongo && isSubscribeTaskType(taskType)) {
            // 订阅：mongodb → Kafka（Change Streams 直投），目标类型不参与校验
            return;
        }
        if (srcMongo != tgtMongo) {
            throw new RuntimeException("MongoDB 只能与 MongoDB 互相同步，不支持与其它数据库类型组合");
        }
        if (tgtEs && !"mysql".equalsIgnoreCase(sourceType)) {
            throw new RuntimeException("到 Elasticsearch 的同步目前仅支持 MySQL 源");
        }
        if (tgtEs && taskType != null && !"SYNC".equals(taskType)) {
            throw new RuntimeException("Elasticsearch 同步目前仅支持实时同步任务，不支持灾备/订阅");
        }
    }

    /**
     * TiDB 配对约束：TiDB 只能作为源。
     * <ul>
     *   <li>反向（MySQL→TiDB）需要在 TiDB 侧建表/写入的整套适配，尚未支持，故不能作目标；</li>
     *   <li>实时同步的目标只能是 MySQL；</li>
     *   <li>订阅任务出口是 Kafka（增量走 TiCDC changefeed），不受目标类型约束；</li>
     *   <li>灾备仍不支持：单向灾备的主备倒换会把 TiDB 变成写入目标，双向灾备的反向通道
     *       同样是 MySQL→TiDB，而 TiDB 作目标的建表/写入适配尚未支持。</li>
     * </ul>
     */
    private void validateTidbTypePairing(String sourceType, String targetType, String taskType) {
        boolean srcTidb = "tidb".equalsIgnoreCase(sourceType);
        boolean tgtTidb = "tidb".equalsIgnoreCase(targetType);
        if (srcTidb && isSubscribeTaskType(taskType)) {
            return;
        }
        if (tgtTidb) {
            throw new RuntimeException("TiDB 目前仅支持作为同步源，不支持作为同步目标");
        }
        if (!srcTidb) {
            return;
        }
        if (!"mysql".equalsIgnoreCase(targetType)) {
            throw new RuntimeException("TiDB 源目前仅支持同步到 MySQL");
        }
        if (taskType != null && !"SYNC".equals(taskType)) {
            throw new RuntimeException("TiDB 目前支持实时同步与数据订阅，不支持灾备任务");
        }
    }

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowLogRepository workflowLogRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private AgentClusterService agentClusterService;

    @Transactional
    public Workflow createWorkflow(String name, String sourceType, String targetType, Long userId, String taskType) {
        return createWorkflow(name, sourceType, targetType, userId, taskType, null, null);
    }

    @Transactional
    public Workflow createWorkflow(String name, String sourceType, String targetType, Long userId,
                                   String taskType, String drMode) {
        return createWorkflow(name, sourceType, targetType, userId, taskType, drMode, null);
    }

    /**
     * 一致性语义的类型默认值：订阅与灾备默认<b>事务一致</b>（下游/备库不能读到半个事务），
     * 普通同步默认<b>最终一致</b>（吞吐优先，源事务可被打散合并）。
     */
    public static String defaultConsistencyMode(String taskType) {
        if ("SUBSCRIBE".equals(taskType) || "DR".equals(taskType) || "DR_SHADOW".equals(taskType)) {
            return "TRANSACTIONAL";
        }
        return "EVENTUAL";
    }

    /**
     * 全量一致性快照的默认档位，按<b>源端</b>给——各家的真快照代价差得远：
     *
     * <ul>
     *   <li>MySQL 的真快照要 {@code RELOAD} 权限，且要在 {@code FLUSH TABLES WITH READ LOCK}
     *       期间把所有读会话开出来（期间源库只读）。这个代价不该默认替用户承担，故默认只记位点。</li>
     *   <li>TiDB（MVCC 历史读）、PostgreSQL（导出快照）、Oracle（闪回查询）、
     *       MongoDB（快照会话）、Redis（PSYNC 拿到的 RDB 本来就是快照）都<b>不需要全局锁</b>，
     *       默认就给真快照，让"全量结束点"这个语义默认可用。</li>
     * </ul>
     */
    public static String defaultSnapshotMode(String sourceType) {
        if (sourceType == null) {
            return "GTID_ONLY";
        }
        switch (sourceType.trim().toLowerCase()) {
            case "tidb":
            case "postgresql":
            case "oracle":
            case "mongodb":
            case "redis":
                return "CONSISTENT";
            default:
                return "GTID_ONLY";
        }
    }

    /** 归一化快照档位：空值走源端默认，非法值直接拒绝。 */
    private String resolveSnapshotMode(String requested, String sourceType) {
        if (requested == null || requested.trim().isEmpty()) {
            return defaultSnapshotMode(sourceType);
        }
        String v = requested.trim().toUpperCase();
        if (!"NONE".equals(v) && !"GTID_ONLY".equals(v) && !"CONSISTENT".equals(v)) {
            throw new RuntimeException("快照档位取值非法（应为 NONE / GTID_ONLY / CONSISTENT）: " + requested);
        }
        return v;
    }

    /** 归一化批量装载档位：空值走 AUTO，非法值直接拒绝。 */
    private String resolveBulkLoadMode(String requested) {
        if (requested == null || requested.trim().isEmpty()) {
            return "AUTO";
        }
        String v = requested.trim().toUpperCase();
        if (!"AUTO".equals(v) && !"BATCH".equals(v) && !"COPY".equals(v) && !"DIRECT_PATH".equals(v)) {
            throw new RuntimeException("批量装载档位取值非法（应为 AUTO / BATCH / COPY / DIRECT_PATH）: " + requested);
        }
        return v;
    }

    /** 归一化用户选择：空值走类型默认，非法值直接拒绝（免得静默落成默认值，用户以为选上了）。 */
    private String resolveConsistencyMode(String requested, String taskType) {
        if (requested == null || requested.trim().isEmpty()) {
            return defaultConsistencyMode(taskType);
        }
        String v = requested.trim().toUpperCase();
        if (!"TRANSACTIONAL".equals(v) && !"EVENTUAL".equals(v)) {
            throw new RuntimeException("一致性模式取值非法（应为 TRANSACTIONAL 或 EVENTUAL）: " + requested);
        }
        return v;
    }

    @Transactional
    public Workflow createWorkflow(String name, String sourceType, String targetType, Long userId,
                                   String taskType, String drMode, String consistencyMode) {
        String effectiveTaskType = taskType != null ? taskType : "SYNC";
        String effectiveName = name != null ? name.trim() : name;
        validateTaskName(effectiveName);
        if (workflowRepository.existsByUserIdAndTaskTypeAndNameAndIsDeletedFalse(userId, effectiveTaskType, effectiveName)) {
            throw new RuntimeException("已存在同名任务，请更换任务名称");
        }

        String effectiveSourceType = sourceType != null ? sourceType : "mysql";
        String effectiveTargetType = targetType != null ? targetType : "mysql";
        validateMongoTypePairing(effectiveSourceType, effectiveTargetType, effectiveTaskType);

        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID().toString());
        workflow.setName(effectiveName);
        workflow.setSourceType(effectiveSourceType);
        workflow.setTargetType(effectiveTargetType);
        workflow.setStatus(WorkflowStatus.CONFIGURING);
        workflow.setUserId(userId);
        workflow.setProgress(0);
        workflow.setIsBilling(false);
        workflow.setTaskType(effectiveTaskType);
        // 一致性语义只在这里写一次：创建后不可修改（updateConfig 显式拒绝改动）
        workflow.setConsistencyMode(resolveConsistencyMode(consistencyMode, effectiveTaskType));
        // 全量装载/快照档位：按源端给默认，任务启动前可在配置页修改
        workflow.setBulkLoadEnabled(true);
        workflow.setBulkLoadMode("AUTO");
        workflow.setSnapshotMode(defaultSnapshotMode(effectiveSourceType));

        if ("DR".equals(taskType)) {
            workflow.setMigrationMode("fullAndIncre");
            workflow.setDrStatus("DR_CONFIGURING");
            // 灾备方向：默认单向。双向（active-active 防回环）依赖捕获侧的 origin 标记跳过，
            // 目前 MySQL binlog、PostgreSQL WAL、MongoDB Change Streams 三条捕获链路均已实现，
            // 故支持 mysql↔mysql、pg↔pg 与 mongodb↔mongodb；其它类型（Oracle/ES）暂不支持双向。
            String effectiveDrMode = "BIDIRECTIONAL".equalsIgnoreCase(drMode) ? "BIDIRECTIONAL" : "UNIDIRECTIONAL";
            if ("BIDIRECTIONAL".equals(effectiveDrMode)) {
                String st = workflow.getSourceType();
                String tt = workflow.getTargetType();
                boolean bothMysql = "mysql".equalsIgnoreCase(st) && "mysql".equalsIgnoreCase(tt);
                boolean bothPg = "postgresql".equalsIgnoreCase(st) && "postgresql".equalsIgnoreCase(tt);
                boolean bothMongo = "mongodb".equalsIgnoreCase(st) && "mongodb".equalsIgnoreCase(tt);
                if (!bothMysql && !bothPg && !bothMongo) {
                    throw new RuntimeException("双向灾备目前仅支持 MySQL↔MySQL、PostgreSQL↔PostgreSQL 或 MongoDB↔MongoDB");
                }
            }
            workflow.setDrMode(effectiveDrMode);
        }

        Workflow savedWorkflow = workflowRepository.save(workflow);
        addLog(savedWorkflow.getId(), WorkflowLog.LogLevel.INFO, "任务创建成功，状态: 配置中");
        addLog(savedWorkflow.getId(), WorkflowLog.LogLevel.INFO,
                "一致性语义: " + ("TRANSACTIONAL".equals(savedWorkflow.getConsistencyMode())
                        ? "事务一致（目标提交顺序与源事务一致，增量串行应用）"
                        : "最终一致（按 表+主键 冲突矩阵并发应用，源事务可被打散合并）")
                        + "，创建后不可修改");
        return savedWorkflow;
    }

    @Transactional
    public Workflow updateConfig(String workflowId, Long userId, String sourceConnection, String targetConnection,
                                  String migrationMode, String syncObjects, String sourceDbName,
                                  String targetDbName, String sourceType, String targetType,
                                  String kafkaBootstrapServers, String kafkaTopicPrefix,
                                  String kafkaTopicStrategy, String subscribeFormat,
                                  Boolean fanoutEnabled, String targetConnections,
                                  Boolean syncAccount, Boolean syncAccountSuperPrivilege) {
        return updateConfig(workflowId, userId, sourceConnection, targetConnection, migrationMode, syncObjects,
                sourceDbName, targetDbName, sourceType, targetType, kafkaBootstrapServers, kafkaTopicPrefix,
                kafkaTopicStrategy, subscribeFormat, fanoutEnabled, targetConnections,
                syncAccount, syncAccountSuperPrivilege, null);
    }

    /**
     * 全量装载/快照档位。与一致性语义不同，这两项<b>任务启动前都可以改</b>——
     * 它们只影响全量怎么读、怎么写，不改变增量的投递语义，因此没有"前后半段语义不一致"的问题。
     * 启动后不可改由 {@link #updateConfig} 的 CONFIGURING 状态校验统一挡住。
     */
    public static class FullLoadOptions {
        private final Boolean bulkLoadEnabled;
        private final String bulkLoadMode;
        private final String snapshotMode;

        public FullLoadOptions(Boolean bulkLoadEnabled, String bulkLoadMode, String snapshotMode) {
            this.bulkLoadEnabled = bulkLoadEnabled;
            this.bulkLoadMode = bulkLoadMode;
            this.snapshotMode = snapshotMode;
        }

        boolean isEmpty() {
            return bulkLoadEnabled == null
                    && (bulkLoadMode == null || bulkLoadMode.trim().isEmpty())
                    && (snapshotMode == null || snapshotMode.trim().isEmpty());
        }
    }

    @Transactional
    public Workflow updateConfig(String workflowId, Long userId, String sourceConnection, String targetConnection,
                                  String migrationMode, String syncObjects, String sourceDbName,
                                  String targetDbName, String sourceType, String targetType,
                                  String kafkaBootstrapServers, String kafkaTopicPrefix,
                                  String kafkaTopicStrategy, String subscribeFormat,
                                  Boolean fanoutEnabled, String targetConnections,
                                  Boolean syncAccount, Boolean syncAccountSuperPrivilege,
                                  String consistencyMode) {
        return updateConfig(workflowId, userId, sourceConnection, targetConnection, migrationMode, syncObjects,
                sourceDbName, targetDbName, sourceType, targetType, kafkaBootstrapServers, kafkaTopicPrefix,
                kafkaTopicStrategy, subscribeFormat, fanoutEnabled, targetConnections,
                syncAccount, syncAccountSuperPrivilege, consistencyMode, null);
    }

    @Transactional
    public Workflow updateConfig(String workflowId, Long userId, String sourceConnection, String targetConnection,
                                  String migrationMode, String syncObjects, String sourceDbName,
                                  String targetDbName, String sourceType, String targetType,
                                  String kafkaBootstrapServers, String kafkaTopicPrefix,
                                  String kafkaTopicStrategy, String subscribeFormat,
                                  Boolean fanoutEnabled, String targetConnections,
                                  Boolean syncAccount, Boolean syncAccountSuperPrivilege,
                                  String consistencyMode, FullLoadOptions fullLoad) {
        Workflow workflow = getWorkflowById(workflowId, userId);

        if (workflow.getStatus() != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能修改配置中的任务，当前状态: " + workflow.getStatus().name());
        }

        // 一致性语义创建即定死：管线（串行事务投递 vs 冲突矩阵并发）与位点语义都按它编排，
        // 中途改会让同一条链路前后半段语义不一致。前端只读展示，这里再挡一道防绕过接口改。
        if (consistencyMode != null && !consistencyMode.trim().isEmpty()
                && !consistencyMode.trim().toUpperCase().equals(workflow.getConsistencyMode())) {
            throw new RuntimeException("一致性模式在任务创建时确定，创建后不可修改（当前: "
                    + workflow.getConsistencyMode() + "）");
        }

        String newSourceType = sourceType != null ? sourceType : workflow.getSourceType();
        String newTargetType = targetType != null ? targetType : workflow.getTargetType();
        validateMongoTypePairing(newSourceType, newTargetType, workflow.getTaskType());

        if (sourceConnection != null) workflow.setSourceConnection(sourceConnection);
        if (targetConnection != null) workflow.setTargetConnection(targetConnection);
        if (migrationMode != null) workflow.setMigrationMode(migrationMode);
        if (syncObjects != null) workflow.setSyncObjects(syncObjects);
        if (sourceDbName != null) workflow.setSourceDbName(sourceDbName);
        if (targetDbName != null) workflow.setTargetDbName(targetDbName);
        if (sourceType != null) workflow.setSourceType(sourceType);
        if (targetType != null) workflow.setTargetType(targetType);
        if (kafkaBootstrapServers != null) workflow.setKafkaBootstrapServers(kafkaBootstrapServers);
        if (kafkaTopicPrefix != null) workflow.setKafkaTopicPrefix(kafkaTopicPrefix);
        if (kafkaTopicStrategy != null) workflow.setKafkaTopicStrategy(kafkaTopicStrategy);
        if (subscribeFormat != null) workflow.setSubscribeFormat(subscribeFormat);
        if (syncAccount != null) workflow.setSyncAccount(syncAccount);
        if (syncAccountSuperPrivilege != null) workflow.setSyncAccountSuperPrivilege(syncAccountSuperPrivilege);
        if (fanoutEnabled != null) {
            workflow.setFanoutEnabled(fanoutEnabled);
            if (fanoutEnabled && targetConnections != null) {
                workflow.setTargetConnections(targetConnections);
                int count = countTargetConnections(targetConnections);
                workflow.setFanoutTargetCount(count);
            } else if (!fanoutEnabled) {
                workflow.setFanoutTargetCount(1);
            }
        }

        // 全量装载/快照档位：只在 CONFIGURING（未启动）状态放行，上面的状态校验已经挡过一次
        if (fullLoad != null && !fullLoad.isEmpty()) {
            if (fullLoad.bulkLoadEnabled != null) {
                workflow.setBulkLoadEnabled(fullLoad.bulkLoadEnabled);
            }
            if (fullLoad.bulkLoadMode != null && !fullLoad.bulkLoadMode.trim().isEmpty()) {
                workflow.setBulkLoadMode(resolveBulkLoadMode(fullLoad.bulkLoadMode));
            }
            if (fullLoad.snapshotMode != null && !fullLoad.snapshotMode.trim().isEmpty()) {
                workflow.setSnapshotMode(resolveSnapshotMode(fullLoad.snapshotMode, newSourceType));
            }
            addLog(workflowId, WorkflowLog.LogLevel.INFO, String.format(
                    "全量装载档位: %s（批量装载%s），快照档位: %s",
                    workflow.getBulkLoadMode(),
                    Boolean.FALSE.equals(workflow.getBulkLoadEnabled()) ? "已关闭" : "启用",
                    workflow.getSnapshotMode()));
        }

        addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务配置已更新");
        return workflowRepository.save(workflow);
    }

    /**
     * 保存聚合路由配置（分库分表汇聚/拆分）。只在 CONFIGURING 放行——路由决定数据写到哪张表，
     * 启动后再改会让前后两段数据落在不同地方。
     *
     * @param routeConfig 路由配置 JSON；空串/null = 清除路由，回到 1:1
     */
    @Transactional
    public Workflow updateRouteConfig(String workflowId, Long userId, String routeConfig) {
        Workflow workflow = getWorkflowById(workflowId, userId);
        if (workflow.getStatus() != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能修改配置中的任务的路由配置，当前状态: " + workflow.getStatus().name());
        }
        String normalized = RouteConfigValidator.validate(routeConfig);
        workflow.setRouteConfig(normalized);
        addLog(workflowId, WorkflowLog.LogLevel.INFO,
                normalized == null ? "聚合路由已清除（回到 1:1 同步）" : "聚合路由配置已更新");
        return workflowRepository.save(workflow);
    }

    /**
     * 跨实例汇聚：为 {@code routeConfig.legs} 里的每个额外源实例派生一个隐藏子任务（MERGE_LEG）。
     *
     * <p>每条 leg 是一条<b>完整独立</b>的采集管线（自己的 capture/位点/进度），只是目标端和
     * 路由规则与父任务相同，且带自己的 {@code nodeId} 写进来源标识列。父任务本身也是一条 leg
     * （用它自己的源连接），所以 N 个额外实例派生 N 个子任务。
     *
     * <p>沿用双向灾备 DR_SHADOW 那套"隐藏子任务"模式：列表里不展示，状态与进度由父任务聚合。
     *
     * @return 派生出的子任务
     */
    @Transactional
    public List<Workflow> createMergeLegs(Workflow parent) {
        List<Workflow> legs = new ArrayList<>();
        String routeConfig = parent.getRouteConfig();
        if (routeConfig == null || routeConfig.isEmpty() || parent.getMergeParentId() != null) {
            return legs;
        }
        Map<String, Object> root;
        try {
            root = new com.google.gson.Gson().fromJson(routeConfig, Map.class);
        } catch (RuntimeException e) {
            return legs;
        }
        if (root == null || !"MERGE".equalsIgnoreCase(String.valueOf(root.get("mode")))) {
            return legs;
        }
        Object legsObj = root.get("legs");
        if (!(legsObj instanceof List) || ((List<?>) legsObj).isEmpty()) {
            return legs;
        }
        // 已经派生过就不再重复（重复启动/重试）
        if (!workflowRepository.findByMergeParentId(parent.getId()).isEmpty()) {
            return legs;
        }

        com.google.gson.Gson gson = new com.google.gson.Gson();
        for (Object o : (List<?>) legsObj) {
            if (!(o instanceof Map)) {
                continue;
            }
            Map<?, ?> leg = (Map<?, ?>) o;
            String nodeId = String.valueOf(leg.get("nodeId"));
            Workflow child = new Workflow();
            child.setId(UUID.randomUUID().toString());
            child.setName(parent.getName() + "-" + nodeId);
            child.setTaskType("MERGE_LEG");
            child.setMergeParentId(parent.getId());
            child.setUserId(parent.getUserId());
            child.setStatus(WorkflowStatus.CONFIGURING);
            child.setProgress(0);
            child.setIsBilling(false);
            child.setMigrationMode(parent.getMigrationMode());
            // 源连接换成这条 leg 的实例；目标端、同步对象、路由规则与父任务一致
            child.setSourceConnection(legConnectionString(leg, parent.getSourceType()));
            child.setTargetConnection(parent.getTargetConnection());
            child.setSourceType(parent.getSourceType());
            child.setTargetType(parent.getTargetType());
            child.setSourceDbName(parent.getSourceDbName());
            child.setTargetDbName(parent.getTargetDbName());
            child.setConsistencyMode(parent.getConsistencyMode());
            child.setBulkLoadEnabled(parent.getBulkLoadEnabled());
            child.setBulkLoadMode(parent.getBulkLoadMode());
            child.setSnapshotMode(parent.getSnapshotMode());
            Object legSyncObjects = leg.get("syncObjects");
            child.setSyncObjects(legSyncObjects != null ? gson.toJson(legSyncObjects) : parent.getSyncObjects());
            // 子任务的路由配置不再带 legs（否则会递归派生），并钉上自己的 nodeId
            Map<String, Object> childRoute = new LinkedHashMap<>(root);
            childRoute.remove("legs");
            childRoute.put("nodeId", nodeId);
            child.setRouteConfig(gson.toJson(childRoute));
            workflowRepository.save(child);
            legs.add(child);
            addLog(parent.getId(), WorkflowLog.LogLevel.INFO,
                    "跨实例汇聚：已创建来源实例 " + nodeId + " 的采集通道（子任务 " + child.getId() + "）");
        }
        return legs;
    }

    /**
     * leg 的连接串。必须与其它任务同一格式（{@code mysql://user:pass@host:port/db}）——
     * agent 侧的 ConnectionStringParser 只认这个，给它 JSON 会直接 "Invalid connection string format"。
     */
    private static String legConnectionString(Map<?, ?> leg, String sourceType) {
        String scheme = sourceType == null || sourceType.trim().isEmpty()
                ? "mysql" : sourceType.trim().toLowerCase();
        if ("tidb".equals(scheme)) {
            scheme = "mysql";   // TiDB 讲 MySQL 协议，连接串同构
        }
        String user = str(leg.get("username"));
        String password = str(leg.get("password"));
        String host = str(leg.get("host"));
        String port = str(leg.get("port"));
        if (port.endsWith(".0")) {
            port = port.substring(0, port.length() - 2);   // gson 把整数解成 Double
        }
        String database = str(leg.get("database"));
        return scheme + "://" + user + ":" + password + "@" + host + ":" + port
                + (database.isEmpty() ? "" : "/" + database);
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o).trim();
    }

    /**
     * 跨实例汇聚的父任务聚合：进度取各 leg 的最小值（最慢那条决定整体可用性），
     * 状态取"最差"的那个（任一 leg 失败即父任务失败）。
     *
     * <p>取最小而不是平均：汇聚表要等所有来源都搬完才算完整，平均值会给出"80% 完成"
     * 这种让人误以为快好了的假象。
     */
    @Transactional
    public void aggregateMergeParent(String parentId) {
        Workflow parent = workflowRepository.findById(parentId).orElse(null);
        if (parent == null) {
            return;
        }
        List<Workflow> legs = workflowRepository.findByMergeParentId(parentId);
        if (legs.isEmpty()) {
            return;
        }
        int minProgress = 100;
        WorkflowStatus worst = null;
        for (Workflow leg : legs) {
            minProgress = Math.min(minProgress, leg.getProgress() == null ? 0 : leg.getProgress());
            worst = worseStatus(worst, leg.getStatus());
        }
        // 父任务自己也是一条采集通道，它的进度同样计入
        minProgress = Math.min(minProgress, parent.getProgress() == null ? 0 : parent.getProgress());
        parent.setProgress(minProgress);
        if (worst == WorkflowStatus.FAILED && parent.getStatus() != WorkflowStatus.FAILED) {
            parent.setStatus(WorkflowStatus.FAILED);
            parent.setErrorMessage("跨实例汇聚的某条来源通道失败，详见子任务");
            addLog(parentId, WorkflowLog.LogLevel.ERROR, "某条来源采集通道失败，父任务标记失败");
        }
        workflowRepository.save(parent);
    }

    /** 谁更"差"：FAILED 最差，其余保持先到的那个（父任务只关心有没有失败）。 */
    private static WorkflowStatus worseStatus(WorkflowStatus current, WorkflowStatus candidate) {
        if (candidate == WorkflowStatus.FAILED) {
            return WorkflowStatus.FAILED;
        }
        return current != null ? current : candidate;
    }

    @Transactional
    public Workflow launchWorkflow(String workflowId, Long userId) {
        Workflow workflow = getWorkflowById(workflowId, userId);
        
        if (workflow.getStatus() != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能启动配置中的任务，当前状态: " + workflow.getStatus().name());
        }
        
        if (workflow.getSourceConnection() == null || workflow.getSourceConnection().isEmpty()) {
            throw new RuntimeException("请先完成源库连接信息配置");
        }
        
        boolean isSubscribeTask = "SUBSCRIBE".equals(workflow.getTaskType());
        
        if (!isSubscribeTask) {
            if (workflow.getTargetConnection() == null || workflow.getTargetConnection().isEmpty()) {
                throw new RuntimeException("请先完成目标库连接信息配置");
            }
        }
        
        if (isSubscribeTask) {
            if (workflow.getKafkaBootstrapServers() == null || workflow.getKafkaBootstrapServers().isEmpty()) {
                throw new RuntimeException("请先配置Kafka连接地址");
            }
        }
        
        boolean isDrTask = "DR".equals(workflow.getTaskType());
        
        if (!isDrTask && !isSubscribeTask) {
            if (workflow.getSyncObjects() == null || workflow.getSyncObjects().isEmpty()) {
                throw new RuntimeException("请先选择同步对象");
            }
            if (workflow.getMigrationMode() == null || workflow.getMigrationMode().isEmpty()) {
                throw new RuntimeException("请先选择同步模式");
            }
        }
        
        if (isSubscribeTask) {
            if (workflow.getSyncObjects() == null || workflow.getSyncObjects().isEmpty()) {
                workflow.setSyncObjects("{\"_all\":true}");
            }
            workflow.setMigrationMode("subscribe");
        }

        // 双向灾备：创建隐藏的反向影子任务（B→A，仅增量）。此刻只建行不启动——
        // 若立即启动，反向全量会把尚未初始化的 B 反灌回 A；等正向进入增量同步
        // （KafkaConsumerService 监听到 INCREMENT_RUNNING）后再自动启动反向通道，
        // 反向 capture 从 B 的最新位点起步，天然跳过正向全量灌入 B 的存量数据。
        if (isDrTask && "BIDIRECTIONAL".equals(workflow.getDrMode()) && workflow.getDrPeerWorkflowId() == null) {
            Workflow shadow = new Workflow();
            shadow.setId(UUID.randomUUID().toString());
            shadow.setName(workflow.getName() + "-反向");
            shadow.setTaskType("DR_SHADOW");
            shadow.setDrMode("BIDIRECTIONAL");
            shadow.setDrPeerWorkflowId(workflow.getId());
            shadow.setUserId(workflow.getUserId());
            shadow.setStatus(WorkflowStatus.CONFIGURING);
            shadow.setProgress(0);
            shadow.setIsBilling(false);
            shadow.setMigrationMode("fullAndIncre");
            shadow.setSourceConnection(workflow.getTargetConnection());
            shadow.setTargetConnection(workflow.getSourceConnection());
            shadow.setSourceType(workflow.getTargetType());
            shadow.setTargetType(workflow.getSourceType());
            shadow.setSourceDbName(workflow.getTargetDbName());
            shadow.setTargetDbName(workflow.getSourceDbName());
            // 反向通道必须与正向同一套一致性语义：两个方向语义不同的双活，
            // 一边保事务、一边打散并发，冲突裁决的输入就不是同一个"事务视图"了
            shadow.setConsistencyMode(workflow.getConsistencyMode());
            // 装载/快照档位一并继承。影子通道是仅增量的（不跑全量），快照档位对它无实际作用，
            // 但保持两条通道配置一致，排障时不用怀疑"两边是不是设得不一样"
            shadow.setBulkLoadEnabled(workflow.getBulkLoadEnabled());
            shadow.setBulkLoadMode(workflow.getBulkLoadMode());
            shadow.setSnapshotMode(workflow.getSnapshotMode());
            // 反向通道镜像正向的同步对象集（灾备两端库名/表集一致）；为空则由 agent 在
            // 反向源库上自动发现——继承可避免把 B 实例上无关的库卷进反向同步
            shadow.setSyncObjects(workflow.getSyncObjects());
            workflowRepository.save(shadow);
            workflow.setDrPeerWorkflowId(shadow.getId());
            addLog(workflowId, WorkflowLog.LogLevel.INFO,
                    "双向灾备：已创建反向同步通道（影子任务 " + shadow.getId() + "），将在正向进入增量同步后自动启动");
        }

        // 跨实例汇聚：为每个额外源实例派生一条隐藏的采集通道（MERGE_LEG）
        List<Workflow> mergeLegs = createMergeLegs(workflow);

        workflow.setStatus(WorkflowStatus.PENDING);
        workflow.setIsBilling(true);
        // 集群化：先挑一台存活 agent 写进 agent_id + 租约，再投 Kafka（消息带 targetAgentId）。
        // 一台都没注册时 assign 返回 false，退回"广播 + 谁抢到算谁"的旧语义。
        if (agentClusterService.assign(workflow)) {
            addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务已指派给 agent: " + workflow.getAgentId());
        }
        workflowRepository.save(workflow);

        addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务启动中，状态: 启动中");

        try {
            kafkaProducerService.sendTaskCreatedMessage(workflow);
            addLog(workflowId, WorkflowLog.LogLevel.INFO, "任务消息已发送到 Kafka topic: sync-task-created，等待任务执行服务处理");
        } catch (Exception e) {
            addLog(workflowId, WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
        }

        // 各条 leg 与父任务同时启动：它们是彼此独立的采集管线，没有先后依赖
        // （与双向灾备的影子任务不同——那条要等正向进增量才能起，否则会把未初始化的数据反灌回去）
        for (Workflow leg : mergeLegs) {
            leg.setStatus(WorkflowStatus.PENDING);
            agentClusterService.assign(leg);
            workflowRepository.save(leg);
            try {
                kafkaProducerService.sendTaskCreatedMessage(leg);
            } catch (Exception e) {
                addLog(workflowId, WorkflowLog.LogLevel.WARNING,
                        "来源通道 " + leg.getName() + " 的 Kafka 消息发送失败: " + e.getMessage());
            }
        }

        return workflow;
    }

    /** 分页参数上限：防止 pageSize 传超大值一次拉全表打挂内存/DB。page 至少为 1。 */
    private static final int MAX_PAGE_SIZE = 200;

    private static int clampPageSize(int pageSize) {
        if (pageSize < 1) return 10;
        return Math.min(pageSize, MAX_PAGE_SIZE);
    }

    private static int clampPage(int page) {
        return Math.max(page, 1);
    }

    public Page<Workflow> getWorkflowsByUserId(Long userId, int page, int pageSize, String sortBy, String sortDirection) {
        String fieldName = mapSortField(sortBy);

        Sort.Direction direction = sortDirection.equalsIgnoreCase("ASC") ? Sort.Direction.ASC : Sort.Direction.DESC;
        Sort sort = Sort.by(direction, fieldName);
        Pageable pageable = PageRequest.of(clampPage(page) - 1, clampPageSize(pageSize), sort);
        return workflowRepository.findByUserId(userId, pageable);
    }

    public Page<Workflow> getWorkflowsByUserIdAndFilters(Long userId, String keyword, String status, String taskType, int page, int pageSize, String sortBy, String sortDirection) {
        return getWorkflowsByUserIdAndFilters(userId, keyword, status, taskType, null, null, page, pageSize, sortBy, sortDirection);
    }

    /**
     * 列表筛选：userId 必选，其余（keyword/status/taskType/sourceType/targetType）均可选独立组合。
     * 用 Specification 而非组合爆炸的 @Query 方法（此前 keyword×status×taskType 已有 8 个方法，
     * 再加 sourceType/targetType 两维会变成 32 个），按需拼接 WHERE 条件，新增筛选维度零额外方法。
     */
    public Page<Workflow> getWorkflowsByUserIdAndFilters(Long userId, String keyword, String status, String taskType,
                                                         String sourceType, String targetType,
                                                         int page, int pageSize, String sortBy, String sortDirection) {
        String fieldName = mapSortField(sortBy);
        Sort.Direction direction = sortDirection.equalsIgnoreCase("ASC") ? Sort.Direction.ASC : Sort.Direction.DESC;
        Sort sort = Sort.by(direction, fieldName);
        Pageable pageable = PageRequest.of(clampPage(page) - 1, clampPageSize(pageSize), sort);

        // 非法状态值给出明确报错而非 IllegalArgumentException 冒泡成 500
        WorkflowStatus parsedStatus = null;
        if (status != null && !status.trim().isEmpty()) {
            try {
                parsedStatus = WorkflowStatus.valueOf(status.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("无效的状态筛选值: " + status);
            }
        }
        final WorkflowStatus workflowStatus = parsedStatus;
        String trimmedKeyword = (keyword != null && !keyword.trim().isEmpty()) ? keyword.trim() : null;
        String trimmedTaskType = (taskType != null && !taskType.trim().isEmpty()) ? taskType : null;
        String trimmedSourceType = (sourceType != null && !sourceType.trim().isEmpty()) ? sourceType : null;
        String trimmedTargetType = (targetType != null && !targetType.trim().isEmpty()) ? targetType : null;

        Specification<Workflow> spec = (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            predicates.add(cb.equal(root.get("userId"), userId));
            predicates.add(cb.isFalse(root.get("isDeleted")));
            if (trimmedTaskType != null) {
                predicates.add(cb.equal(root.get("taskType"), trimmedTaskType));
            }
            if (workflowStatus != null) {
                predicates.add(cb.equal(root.get("status"), workflowStatus));
            }
            if (trimmedKeyword != null) {
                String pattern = "%" + trimmedKeyword.toLowerCase() + "%";
                predicates.add(cb.or(
                        cb.like(cb.lower(root.get("name")), pattern),
                        cb.like(cb.lower(root.get("id")), pattern)));
            }
            if (trimmedSourceType != null) {
                predicates.add(cb.equal(cb.lower(root.get("sourceType")), trimmedSourceType.toLowerCase()));
            }
            if (trimmedTargetType != null) {
                predicates.add(cb.equal(cb.lower(root.get("targetType")), trimmedTargetType.toLowerCase()));
            }
            return cb.and(predicates.toArray(new Predicate[0]));
        };

        return workflowRepository.findAll(spec, pageable);
    }
    
    public List<Workflow> getFailedWorkflowsByUserId(Long userId) {
        // DR_SHADOW（双向灾备反向通道）与 MERGE_LEG（跨实例汇聚的来源采集通道）都是隐藏子任务，
        // 不在任何列表中直接展示——它们的失败由各自的父任务聚合上报。
        // 过滤下推到 DB 查询，避免先全量取回再内存 filter。
        return workflowRepository.findByUserIdAndStatusExcludingTaskTypes(
                userId, WorkflowStatus.FAILED, List.of("DR_SHADOW", "MERGE_LEG"));
    }
    
    private String mapSortField(String sortBy) {
        switch (sortBy) {
            case "name":
                return "name";
            case "status":
                return "status";
            case "created_at":
                return "createdAt";
            case "is_billing":
                return "isBilling";
            default:
                return "createdAt";
        }
    }

    public Workflow getWorkflowById(String id, Long userId) {
        Workflow workflow = workflowRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        
        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权访问此任务");
        }
        
        return workflow;
    }

    public List<WorkflowLog> getWorkflowLogs(String workflowId, Long userId) {
        Workflow workflow = getWorkflowById(workflowId, userId);
        return workflowLogRepository.findByWorkflowIdOrderByCreatedAtDesc(workflow.getId());
    }

    /** 构造发给 agent 的任务控制消息（stop/resume/terminate 级联共用的字段装配）。 */
    private TaskCreatedMessage buildControlMessage(Workflow w, String messageType, String currentStatus) {
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(w.getId());
        message.setTaskName(w.getName());
        message.setUserId(w.getUserId());
        message.setSourceConnection(w.getSourceConnection());
        message.setTargetConnection(w.getTargetConnection());
        message.setMigrationMode(w.getMigrationMode());
        message.setCreatedAt(w.getCreatedAt());
        message.setMessageType(messageType);
        message.setCurrentStatus(currentStatus);
        message.setSourceType(w.getSourceType());
        message.setTargetType(w.getTargetType());
        message.setSourceDbName(w.getSourceDbName());
        message.setTargetDbName(w.getTargetDbName());
        message.setTaskType(w.getTaskType());
        message.setConsistencyMode(w.getConsistencyMode());
        message.setDrMode(w.getDrMode());
        message.setSyncObjects(parseSyncObjects(w.getSyncObjects()));
        // 控制消息（stop/resume/terminate/delete）同样定向：任务在哪台 agent 上跑，就只让那台处理
        message.setTargetAgentId(w.getAgentId());
        return message;
    }

    /**
     * 双向灾备：把主任务的控制操作级联到反向影子任务（用户不可见，必须跟随主任务生命周期，
     * 否则会留下无人管理的反向同步进程）。级联失败只记日志，不阻断主任务操作。
     */
    private void cascadeBidiShadow(Workflow primary, String action) {
        if (!"DR".equals(primary.getTaskType()) || !"BIDIRECTIONAL".equals(primary.getDrMode())
                || primary.getDrPeerWorkflowId() == null) {
            return;
        }
        Workflow shadow = workflowRepository.findById(primary.getDrPeerWorkflowId()).orElse(null);
        if (shadow == null) {
            return;
        }

        WorkflowStatus st = shadow.getStatus();
        boolean shadowActive = st == WorkflowStatus.PENDING || st == WorkflowStatus.RECEIVED
                || st == WorkflowStatus.STARTING || st == WorkflowStatus.INCREMENT_RUNNING
                || st == WorkflowStatus.FULL_MIGRATING || st == WorkflowStatus.FULL_COMPLETED;

        try {
            switch (action) {
                case "pause":
                    if (shadowActive) {
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "stop", st.name()));
                        shadow.setStatus(WorkflowStatus.PAUSED);
                        workflowRepository.save(shadow);
                    }
                    break;
                case "resume":
                    // 影子仍是 CONFIGURING（正向暂停/失败时还没进过增量）无需处理：
                    // 正向恢复后进入 INCREMENT_RUNNING 时会由状态消费者自动首次启动。
                    // PAUSED（正向暂停时一并停的）或 FAILED（反向通道也挂了）→ 从增量位点重新拉起。
                    if (st == WorkflowStatus.PAUSED || st == WorkflowStatus.FAILED) {
                        shadow.setStatus(WorkflowStatus.STARTING);
                        shadow.setIsBilling(true);
                        shadow.setErrorMessage(null);
                        shadow.setErrorCode(null);
                        shadow.setCompletedAt(null);
                        workflowRepository.save(shadow);
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "resume", "INCREMENT_RUNNING"));
                    }
                    break;
                case "stop":
                    if (shadowActive || st == WorkflowStatus.PAUSED) {
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "terminate", st.name()));
                    }
                    shadow.setStatus(WorkflowStatus.COMPLETED);
                    shadow.setCompletedAt(LocalDateTime.now());
                    shadow.setIsBilling(false);
                    workflowRepository.save(shadow);
                    break;
                case "delete":
                    if (shadowActive) {
                        kafkaProducerService.sendControlMessage(buildControlMessage(shadow, "terminate", st.name()));
                    }
                    shadow.setIsDeleted(true);
                    shadow.setIsBilling(false);
                    workflowRepository.save(shadow);
                    break;
                default:
                    return;
            }
            addLog(primary.getId(), WorkflowLog.LogLevel.INFO, "双向灾备：反向通道已级联执行 " + action);
        } catch (Exception e) {
            addLog(primary.getId(), WorkflowLog.LogLevel.WARNING,
                    "双向灾备：反向通道级联 " + action + " 失败: " + e.getMessage());
        }
    }

    /** 可暂停的运行中状态（与 cascadeBidiShadow 的 shadowActive 判定一致） */
    private static final java.util.Set<WorkflowStatus> PAUSABLE_STATUSES = java.util.Set.of(
            WorkflowStatus.PENDING, WorkflowStatus.RECEIVED, WorkflowStatus.STARTING,
            WorkflowStatus.FULL_MIGRATING, WorkflowStatus.FULL_COMPLETED, WorkflowStatus.INCREMENT_RUNNING);

    @Transactional
    public void pauseWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);

        // 状态守卫：只有运行中的任务才能暂停。此前无校验，配置中/已完成/失败的任务
        // 也会被强制改成 PAUSED 并发出无意义的停止消息，状态机随之错乱。
        if (!PAUSABLE_STATUSES.contains(workflow.getStatus())) {
            throw new RuntimeException("只能暂停运行中的任务，当前状态: " + workflow.getStatus().name());
        }

        String currentStatus = workflow.getStatus().name();

        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("stop");
        message.setCurrentStatus(currentStatus);
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());
        message.setConsistencyMode(workflow.getConsistencyMode());

        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已暂停，发送停止消息到 Kafka，当前状态: " + currentStatus);
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
        }

        workflow.setStatus(WorkflowStatus.PAUSED);
        workflowRepository.save(workflow);
        cascadeBidiShadow(workflow, "pause");
    }

    @Transactional
    public void resumeWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);

        // 状态守卫：只有已暂停的任务才能恢复（失败任务走 retry 接口）。
        if (workflow.getStatus() != WorkflowStatus.PAUSED) {
            throw new RuntimeException("只能恢复已暂停的任务，当前状态: " + workflow.getStatus().name());
        }

        String previousStatus = workflow.getStatus().name();
        workflow.setStatus(WorkflowStatus.STARTING);
        workflowRepository.save(workflow);
        
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("resume");
        message.setCurrentStatus(previousStatus);
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setTaskType(workflow.getTaskType());
        message.setConsistencyMode(workflow.getConsistencyMode());
        
        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已恢复，发送恢复消息到 Kafka，等待任务执行服务处理");
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 恢复消息发送失败: " + e.getMessage());
        }
        cascadeBidiShadow(workflow, "resume");
    }

    @Transactional
    public void stopWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);
        
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("terminate");
        message.setCurrentStatus(workflow.getStatus().name());
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());
        message.setConsistencyMode(workflow.getConsistencyMode());
        
        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "发送终止消息到 Kafka，结束所有相关进程");
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 终止消息发送失败: " + e.getMessage());
        }
        
        workflow.setStatus(WorkflowStatus.COMPLETED);
        workflow.setCompletedAt(LocalDateTime.now());
        workflow.setIsBilling(false);
        workflowRepository.save(workflow);
        addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已结束，状态: 已完成");
        cascadeBidiShadow(workflow, "stop");
    }

    @Transactional
    public void deleteWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);

        WorkflowStatus status = workflow.getStatus();
        if (status != WorkflowStatus.COMPLETED && status != WorkflowStatus.FAILED && status != WorkflowStatus.FULL_COMPLETED && status != WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能删除已完成、失败或配置中的任务，当前状态: " + status.name());
        }

        workflow.setIsDeleted(true);
        workflowRepository.save(workflow);
        addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务已删除（软删除）");
        cascadeBidiShadow(workflow, "delete");
    }

    @Transactional
    public void retryWorkflow(String id, Long userId) {
        Workflow workflow = getWorkflowById(id, userId);
        
        if (workflow.getStatus() != WorkflowStatus.FAILED) {
            throw new RuntimeException("只能重试失败的任务，当前状态: " + workflow.getStatus().name());
        }
        
        boolean incrementStarted = Boolean.TRUE.equals(workflow.getIncrementStarted());
        
        if (incrementStarted) {
            resumeIncrementWorkflow(workflow, null, null);
        } else {
            workflow.setStatus(WorkflowStatus.PENDING);
            workflow.setProgress(0);
            workflow.setIsBilling(true);
            workflow.setErrorMessage(null);
            workflow.setErrorCode(null);
            workflow.setCompletedAt(null);
            workflow.setTotalTables(null);
            workflow.setCompletedTables(null);
            workflow.setCurrentTable(null);
            workflow.setCurrentTableProgress(null);
            workflow.setCurrentTableRows(null);
            workflow.setCurrentTableTotalRows(null);
            workflowRepository.save(workflow);
            
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务重试中，状态重置为 PENDING");

            // 消息体由 sendTaskCreatedMessage(workflow) 内部构建（与首次启动同一条路径），
            // 此处不再手工拼装 TaskCreatedMessage——曾有一份拼装后从未发送的死代码，
            // 改字段只改到死代码上不会生效，故删除。
            try {
                kafkaProducerService.sendTaskCreatedMessage(workflow);
                addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务重试消息已发送到 Kafka，等待任务执行服务处理");
            } catch (Exception e) {
                addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
            }
        }

        // 双向灾备：主任务重试时把反向影子通道一并拉起，否则双向同步只剩单边。
        // resume 语义会处理影子当前处于 PAUSED 的情形；仍是 CONFIGURING（尚未进过增量）
        // 则等主任务进 INCREMENT_RUNNING 时由状态消费者自动首启（cascadeBidiShadow 内已判断）。
        cascadeBidiShadow(workflow, "resume");
    }

    /** 增量任务恢复：状态置 STARTING 并发 resume 控制消息（skip 参数非空时随消息下发人工裁决跳过清单）。 */
    private void resumeIncrementWorkflow(Workflow workflow, String skipSeqnos, String skipEventIds) {
        workflow.setStatus(WorkflowStatus.STARTING);
        workflow.setIsBilling(true);
        workflow.setErrorMessage(null);
        workflow.setErrorCode(null);
        workflow.setCompletedAt(null);
        workflowRepository.save(workflow);

        addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务恢复中，增量同步曾已启动，将从增量位点继续同步");

        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setSyncObjects(parseSyncObjects(workflow.getSyncObjects()));
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("resume");
        message.setCurrentStatus("INCREMENT_RUNNING");
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());
        message.setConsistencyMode(workflow.getConsistencyMode());
        if (skipSeqnos != null && !skipSeqnos.isEmpty()) {
            message.setSkipSeqnos(skipSeqnos);
        }
        if (skipEventIds != null && !skipEventIds.isEmpty()) {
            message.setSkipEventIds(skipEventIds);
        }

        try {
            kafkaProducerService.sendControlMessage(message);
            addLog(workflow.getId(), WorkflowLog.LogLevel.INFO, "任务恢复消息已发送到 Kafka（跳过全量同步，从增量位点继续）");
        } catch (Exception e) {
            addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING, "Kafka 消息发送失败: " + e.getMessage());
        }
    }

    private static final java.util.regex.Pattern FAILED_SEQNO_PATTERN =
            java.util.regex.Pattern.compile("seqno=(\\d+)");
    /** 错误信息里的稳定事件身份（binlog文件:位点）——resume 后 capture 重读 binlog 会重新分配 seqno，跳过必须按 eventId */
    private static final java.util.regex.Pattern FAILED_EVENT_ID_PATTERN =
            java.util.regex.Pattern.compile("eventId=([^\\]\\s]+)");

    /**
     * 人工裁决：跳过失败的增量事件并恢复任务。fail-stop 后运维确认该事件无法/无需修复
     * （如目标端已人工处理、脏事件不可重放）时使用；被跳过的事件由增量进程记入死信
     * （files/&lt;taskId&gt;/deadletter.jsonl），可经 GET /api/workflows/{id}/deadletter 查看。
     */
    @Transactional
    public long skipEventAndRetry(String id, Long userId, Long seqno) {
        Workflow workflow = getWorkflowById(id, userId);

        if (workflow.getStatus() != WorkflowStatus.FAILED) {
            throw new RuntimeException("只能对失败状态的任务跳过事件，当前状态: " + workflow.getStatus().name());
        }
        if (!Boolean.TRUE.equals(workflow.getIncrementStarted())) {
            throw new RuntimeException("该任务未进入过增量同步，无失败增量事件可跳过（全量失败请直接重试）");
        }

        String errorMessage = workflow.getErrorMessage();
        // 稳定身份优先：eventId（binlog文件:位点）跨重启不变；seqno 在 resume 重新提取后会变，
        // 只按 seqno 跳过会永不收敛（同一毒事件每次重启换一个 seqno 再次失败）
        String eventId = null;
        if (errorMessage != null) {
            java.util.regex.Matcher em = FAILED_EVENT_ID_PATTERN.matcher(errorMessage);
            if (em.find()) {
                eventId = em.group(1);
            }
        }
        if (seqno == null) {
            if (errorMessage != null) {
                java.util.regex.Matcher m = FAILED_SEQNO_PATTERN.matcher(errorMessage);
                if (m.find()) {
                    seqno = Long.parseLong(m.group(1));
                }
            }
            if (seqno == null && eventId == null) {
                throw new RuntimeException("无法从错误信息中定位失败事件（无 eventId/seqno），请在请求中显式指定 seqno");
            }
        }

        addLog(workflow.getId(), WorkflowLog.LogLevel.WARNING,
                "人工裁决：跳过失败事件 " + (eventId != null ? "eventId=" + eventId + " " : "")
                        + (seqno != null ? "seqno=" + seqno : "")
                        + " 并恢复任务，该事件将不被应用并记入死信记录");
        // 有 eventId 时只按 eventId 跳过：seqno 重启后会重新分配，附带下发可能误跳到无辜事件；
        // 仅当错误信息没有 eventId（老格式）时才退回 seqno
        resumeIncrementWorkflow(workflow, eventId == null && seqno != null ? String.valueOf(seqno) : null, eventId);
        cascadeBidiShadow(workflow, "resume");
        return seqno != null ? seqno : -1L;
    }

    /** 同步位点可视化：代理 agent 的 /api/checkpoint/{taskId}（先做属主校验）。 */
    public Map<String, Object> getCheckpointVisualization(String id, Long userId) {
        getWorkflowById(id, userId);
        return callAgentJson("/api/checkpoint/" + id, "查询同步位点失败（agent 不可达或未运行）", id);
    }

    // ==================== 实时监控指标：代理 agent 的只读监控接口 ====================
    //
    // 为什么必须由后端代理，而不是让页面直连 agent:8083：
    // agent 的监控端点在配置了 AGENT_API_TOKEN 时要求 Bearer <AGENT_API_TOKEN>，而这个 token 是
    // 服务端密钥，绝不能下发给浏览器。页面此前直连 agent（要么不带头、要么错带用户 JWT），
    // 于是所有监控接口一律 401——同步/订阅/灾备任务的指标卡片全是"--"。
    // 改由后端转发：用户侧用 JWT 鉴权 + 属主校验，服务端再拿 AGENT_API_TOKEN 去问 agent。

    /** 单任务实时指标：代理 agent 的 /api/metrics/{taskId}（先做属主校验）。 */
    public Map<String, Object> getTaskMetrics(String id, Long userId) {
        getWorkflowById(id, userId);
        return callAgentJson("/api/metrics/" + id, "查询任务实时指标失败（agent 不可达或未运行）", id);
    }

    /** 单任务历史指标：代理 agent 的 /api/metrics/{taskId}/history（先做属主校验）。 */
    public Map<String, Object> getTaskMetricsHistory(String id, Long userId, String query) {
        getWorkflowById(id, userId);
        String path = "/api/metrics/" + id + "/history" + (query != null && !query.isEmpty() ? "?" + query : "");
        return callAgentJson(path, "查询任务历史指标失败（agent 不可达或未运行）", id);
    }

    /** 表级延迟热力图：代理 agent 的 /api/table-latency/{taskId}（先做属主校验）。 */
    public Map<String, Object> getTableLatency(String id, Long userId) {
        getWorkflowById(id, userId);
        return callAgentJson("/api/table-latency/" + id, "查询表级延迟失败（agent 不可达或未运行）", id);
    }

    /** 分片路由指标：代理 agent 的 /api/route-metrics/{taskId}（先做属主校验）。 */
    public Map<String, Object> getRouteMetrics(String id, Long userId) {
        getWorkflowById(id, userId);
        return callAgentJson("/api/route-metrics/" + id, "查询分片路由指标失败（agent 不可达或未运行）", id);
    }

    /** 一对多分发状态：代理 agent 的 /api/fanout/{taskId}（先做属主校验）。 */
    public Map<String, Object> getFanoutStatus(String id, Long userId) {
        getWorkflowById(id, userId);
        return callAgentJson("/api/fanout/" + id, "查询分发状态失败（agent 不可达或未运行）", id);
    }

    /**
     * agent 上所有任务的实时指标，只保留当前用户自己的任务。
     *
     * <p>agent 不认识"用户"，返回的是本机全部任务；这里按属主过滤，避免把别人的任务指标
     * 透给当前登录用户。
     */
    public Map<String, Object> getAllTaskMetrics(Long userId) {
        Map<String, Object> raw = callAgentJson("/api/metrics", "查询实时指标失败（agent 不可达或未运行）");
        return Map.of("tasks", filterOwnedByUser(raw.get("tasks"), userId, "taskId"));
    }

    /** agent 运行态（活跃任务列表），同样按属主过滤。 */
    public Map<String, Object> getAgentStatus(Long userId) {
        Map<String, Object> raw = callAgentJson("/api/agent/status", "查询 agent 状态失败（agent 不可达或未运行）");
        Map<String, Object> out = new HashMap<>(raw);
        out.put("tasks", filterOwnedByUser(raw.get("tasks"), userId, "taskId"));
        return out;
    }

    /**
     * 排障压缩包：代理 agent 的 /api/diagnostics/{taskId}，返回原始 zip 字节。
     * 与上面几个 JSON 接口同因——页面直连 agent 会 401，必须由后端持 AGENT_API_TOKEN 转发。
     */
    public byte[] getDiagnosticsBundle(String id, Long userId) {
        getWorkflowById(id, userId);
        String agentBase = agentBaseUrlFor(id);
        String agentToken = System.getenv("AGENT_API_TOKEN");
        try {
            java.net.URL url = new java.net.URL(agentBase + "/api/diagnostics/" + id);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            if (agentToken != null && !agentToken.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + agentToken);
            }
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(60000);
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("agent 返回状态 " + conn.getResponseCode());
            }
            try (java.io.InputStream is = conn.getInputStream()) {
                return is.readAllBytes();
            }
        } catch (Exception e) {
            throw new RuntimeException("下载排障包失败（agent 不可达或未运行）: " + e.getMessage());
        }
    }

    /** 从 agent 返回的任务数组中筛出属于该用户的条目，并补上后端才知道的 taskType/name。 */
    private List<Map<String, Object>> filterOwnedByUser(Object tasksObj, Long userId, String idKey) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (!(tasksObj instanceof List<?> list)) {
            return result;
        }
        for (Object o : list) {
            if (!(o instanceof Map<?, ?> m)) continue;
            Object idVal = m.get(idKey);
            if (idVal == null) continue;
            Workflow w = workflowRepository.findById(idVal.toString()).orElse(null);
            if (w == null || !userId.equals(w.getUserId()) || Boolean.TRUE.equals(w.getIsDeleted())) {
                continue;
            }
            Map<String, Object> entry = new HashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                entry.put(String.valueOf(e.getKey()), e.getValue());
            }
            // agent 只知道 taskId，任务类型/名称由后端补齐，前端才能区分同步/订阅/灾备
            entry.put("taskType", w.getTaskType());
            entry.put("name", w.getName());
            entry.put("status", w.getStatus() != null ? w.getStatus().name() : null);
            entry.put("drStatus", w.getDrStatus());
            result.add(entry);
        }
        return result;
    }

    /**
     * 任务当前归属 agent 的基址。集群化后 agent 不止一台，硬编码 localhost:8083 会问错机器
     * （查到的指标是空的、位点是别人的）。查不到归属或该 agent 没注册时回退默认地址，
     * 单机部署行为不变。
     */
    private String agentBaseUrlFor(String taskId) {
        String fallback = System.getenv().getOrDefault("AGENT_BASE_URL", "http://localhost:8083");
        if (taskId == null) {
            return fallback;
        }
        try {
            Workflow w = workflowRepository.findById(taskId).orElse(null);
            if (w == null) {
                return fallback;
            }
            return agentClusterService.agentBaseUrl(w.getAgentId()).orElse(fallback);
        } catch (Exception e) {
            return fallback;
        }
    }

    /** GET agent HTTP 接口并解析 JSON（带可选 Bearer token）。 */
    private Map<String, Object> callAgentJson(String path, String errPrefix) {
        return callAgentJson(path, errPrefix, null);
    }

    /** taskId 非空时按任务归属路由到对应 agent。 */
    private Map<String, Object> callAgentJson(String path, String errPrefix, String taskId) {
        String agentBase = agentBaseUrlFor(taskId);
        String agentToken = System.getenv("AGENT_API_TOKEN");
        try {
            java.net.URL url = new java.net.URL(agentBase + path);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            if (agentToken != null && !agentToken.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + agentToken);
            }
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(10000);
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("agent 返回状态 " + conn.getResponseCode());
            }
            try (java.io.InputStream is = conn.getInputStream()) {
                String body = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
                return gson.fromJson(body, type);
            }
        } catch (Exception e) {
            throw new RuntimeException(errPrefix + ": " + e.getMessage());
        }
    }

    /** 死信记录查询：代理 agent 的 /api/agent/deadletter/{taskId}（先做属主校验）。 */
    public Map<String, Object> getDeadletterRecords(String id, Long userId) {
        getWorkflowById(id, userId);

        String agentBase = agentBaseUrlFor(id);
        String agentToken = System.getenv("AGENT_API_TOKEN");
        try {
            java.net.URL url = new java.net.URL(agentBase + "/api/agent/deadletter/" + id);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            if (agentToken != null && !agentToken.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + agentToken);
            }
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(10000);
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("agent 返回状态 " + conn.getResponseCode());
            }
            try (java.io.InputStream is = conn.getInputStream()) {
                String body = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
                return gson.fromJson(body, type);
            }
        } catch (Exception e) {
            throw new RuntimeException("查询死信记录失败（agent 不可达或未运行）: " + e.getMessage());
        }
    }

    /** 双向写写冲突记录：代理 agent 的 /api/agent/conflicts/{taskId}（与死信同格式，前端复用同一套展示）。 */
    public Map<String, Object> getConflictRecords(String id, Long userId) {
        getWorkflowById(id, userId);
        return callAgentJson("/api/agent/conflicts/" + id, "查询冲突记录失败（agent 不可达或未运行）", id);
    }

    private Map<String, Object> parseSyncObjects(String syncObjects) {
        if (syncObjects == null || syncObjects.isEmpty()) {
            return null;
        }
        try {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            return gson.fromJson(syncObjects, type);
        } catch (Exception e) {
            return null;
        }
    }

    private void addLog(String workflowId, WorkflowLog.LogLevel level, String message) {
        WorkflowLog log = new WorkflowLog();
        log.setWorkflowId(workflowId);
        log.setLevel(level);
        log.setMessage(message);
        workflowLogRepository.save(log);
    }

    /** 统计 targetConnections JSON 数组中的目标库数量（真解析 JSON，解析失败回退 1）。 */
    private int countTargetConnections(String targetConnectionsJson) {
        if (targetConnectionsJson == null || targetConnectionsJson.trim().isEmpty()) return 0;
        try {
            com.google.gson.JsonElement el = com.google.gson.JsonParser.parseString(targetConnectionsJson);
            if (el.isJsonArray()) {
                return Math.max(1, el.getAsJsonArray().size());
            }
            if (el.isJsonObject()) {
                return 1;
            }
        } catch (Exception e) {
            logger.warn("解析 targetConnections 失败，按 1 个目标计: {}", e.getMessage());
        }
        return 1;
    }

    @Transactional
    public Workflow failoverWorkflow(String workflowId, Long userId) {
        Workflow workflow = getWorkflowById(workflowId, userId);

        if (!"DR".equals(workflow.getTaskType())) {
            throw new RuntimeException("只有灾备任务才能执行主备倒换");
        }

        if ("BIDIRECTIONAL".equals(workflow.getDrMode())) {
            throw new RuntimeException("双向灾备两端均可读写、实时互同步，无需主备倒换");
        }

        if (workflow.getStatus() != WorkflowStatus.INCREMENT_RUNNING && workflow.getStatus() != WorkflowStatus.SWITCHING) {
            throw new RuntimeException("只有灾备中的任务才能执行主备倒换，当前状态: " + workflow.getStatus().name());
        }

        String originalSource = workflow.getSourceConnection();
        String originalTarget = workflow.getTargetConnection();
        String originalSourceType = workflow.getSourceType();
        String originalTargetType = workflow.getTargetType();
        String originalSourceDbName = workflow.getSourceDbName();
        String originalTargetDbName = workflow.getTargetDbName();

        workflow.setSourceConnection(originalTarget);
        workflow.setTargetConnection(originalSource);
        workflow.setSourceType(originalTargetType);
        workflow.setTargetType(originalSourceType);
        workflow.setSourceDbName(originalTargetDbName);
        workflow.setTargetDbName(originalSourceDbName);

        workflow.setStatus(WorkflowStatus.SWITCHING);
        workflow.setDrStatus("SWITCHING");
        workflow.setDrSwitchCount(workflow.getDrSwitchCount() != null ? workflow.getDrSwitchCount() + 1 : 1);
        workflow.setDrSwitchStartTime(java.time.LocalDateTime.now());
        workflowRepository.save(workflow);

        addLog(workflowId, WorkflowLog.LogLevel.INFO, "主备倒换开始，源库与目标库连接信息已交换，倒换次数: " + workflow.getDrSwitchCount());

        final TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setSyncObjects(parseSyncObjects(workflow.getSyncObjects()));
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setMessageType("failover");
        message.setCurrentStatus("INCREMENT_RUNNING");
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());
        message.setConsistencyMode(workflow.getConsistencyMode());

        final String logWorkflowId = workflowId;
        org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
            new org.springframework.transaction.support.TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    new Thread(() -> {
                        boolean httpSuccess = false;
                        try {
                            httpSuccess = callAgentFailoverApi(message);
                            if (httpSuccess) {
                                addLog(logWorkflowId, WorkflowLog.LogLevel.INFO, "主备倒换命令已通过Agent HTTP API直接发送成功");
                            }
                        } catch (Exception e) {
                            addLog(logWorkflowId, WorkflowLog.LogLevel.WARNING, "Agent HTTP API 调用失败: " + e.getMessage());
                        }

                        if (!httpSuccess) {
                            try {
                                kafkaProducerService.sendControlMessage(message);
                                addLog(logWorkflowId, WorkflowLog.LogLevel.INFO, "主备倒换命令已通过Kafka发送（HTTP API不可用时的降级方案）");
                            } catch (Exception e) {
                                addLog(logWorkflowId, WorkflowLog.LogLevel.WARNING, "Kafka 主备倒换消息发送失败: " + e.getMessage());
                            }
                        }
                    }, "FailoverNotifier-" + workflowId).start();
                }
            }
        );

        return workflow;
    }

    private boolean callAgentFailoverApi(TaskCreatedMessage message) {
        // agent 地址与鉴权 token 均可配（环境变量优先），不再写死 localhost；
        // agent 侧 failover 是敏感端点，配置了 AGENT_API_TOKEN 时必须带 Bearer token。
        String agentBase = agentBaseUrlFor(message.getTaskId());
        String agentToken = System.getenv("AGENT_API_TOKEN");
        String agentUrl = agentBase + "/api/agent/failover";
        try {
            java.net.URL url = new java.net.URL(agentUrl);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            if (agentToken != null && !agentToken.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + agentToken);
            }
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(30000);
            conn.setDoOutput(true);

            com.google.gson.Gson gson = new com.google.gson.GsonBuilder()
                .registerTypeAdapter(java.time.LocalDateTime.class, (com.google.gson.JsonSerializer<java.time.LocalDateTime>) (src, typeOfSrc, context) ->
                    context.serialize(src.format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
                .create();
            String jsonBody = gson.toJson(message);

            try (java.io.OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (java.io.InputStream is = conn.getInputStream()) {
                    String response = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                    logger.info("Agent failover API response: {}", response);
                }
                return true;
            } else {
                logger.warn("Agent failover API returned status: {}", responseCode);
                return false;
            }
        } catch (java.net.ConnectException e) {
            logger.warn("Agent HTTP API not available at {}: {}", agentUrl, e.getMessage());
            return false;
        } catch (Exception e) {
            logger.warn("Error calling Agent failover API: {}", e.getMessage());
            return false;
        }
    }
}
