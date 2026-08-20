package com.synctask.dto;

import java.time.LocalDateTime;
import java.util.Map;

public class TaskCreatedMessage {
    private String taskId;
    private String taskName;
    private Long userId;
    private String sourceConnection;
    private String targetConnection;
    private String migrationMode;
    private LocalDateTime createdAt;
    private String messageType;
    private String currentStatus;
    private Map<String, Object> syncObjects;
    private String sourceDbName;
    private String targetDbName;
    private String sourceType;
    private String targetType;
    private String taskType;
    /**
     * 一致性语义（TRANSACTIONAL / EVENTUAL），创建任务时选定、之后不可修改。
     * agent 据此编排增量投递：事务一致=串行按源事务提交；最终一致=按 表+主键 冲突矩阵并发。
     */
    private String consistencyMode;
    /** 全量批量装载开关（migration.full.bulk.enabled）。 */
    private Boolean bulkLoadEnabled;
    /** 批量装载档位 AUTO/BATCH/COPY/DIRECT_PATH（migration.full.bulk.mode）。 */
    private String bulkLoadMode;
    /** 全量一致性快照档位 NONE/GTID_ONLY/CONSISTENT（migration.full.snapshot.mode）。 */
    private String snapshotMode;
    private String drMode;
    private String kafkaBootstrapServers;
    private String kafkaTopicPrefix;
    private String kafkaTopicStrategy;
    private String subscribeFormat;
    /** 人工裁决要跳过的增量事件 seqno（逗号分隔），随 skip-event 的 resume 消息下发 */
    private String skipSeqnos;
    /** 人工裁决要跳过的增量事件 eventId（binlog文件:位点，逗号分隔）——跨重启稳定的首选身份 */
    private String skipEventIds;
    /** 账号同步（仅 mysql→mysql）：是否同步账号 */
    private Boolean syncAccount;
    /** 是否同步超级账号权限 */
    private Boolean syncAccountSuperPrivilege;
    /** 指派执行本任务的 agent（集群化）。为空=广播语义，任一 agent 都可接（兼容旧行为） */
    private String targetAgentId;
    /** 聚合路由配置 JSON（汇聚/拆分规则）；空 = 1:1 同步 */
    private String routeConfig;
    /** 本条管线的来源实例标识（跨实例汇聚的 leg 用，写进来源标识列） */
    private String routeNodeId;

    /**
     * 源/目标端的传输加密档位与证书 id。
     *
     * <p>下发的是<b>证书 id 而不是证书内容</b>：控制面消息会经 Kafka、可能落到 broker 磁盘上，
     * 私钥不该在那条路上流动。agent 拿到 id 后用它自己那条元数据库连接把证书取下来。
     */
    private String sourceSslMode;
    private String sourceSslCertId;
    private String targetSslMode;
    private String targetSslCertId;

    /**
     * 流量复制/回放的专属配置（JSON）。
     *
     * <p>用一个 JSON 串而不是把 20 个字段平铺进来：这些字段只对 6 种任务类型里的 2 种有意义，
     * 平铺会让每条任务派发消息都拖着一大片恒为 null 的字段，而且以后加一个档位就要改
     * DTO / TaskMessage / 序列化三处。
     */
    private String trafficConfig;

    public String getSourceSslMode() { return sourceSslMode; }
    public void setSourceSslMode(String sourceSslMode) { this.sourceSslMode = sourceSslMode; }
    public String getSourceSslCertId() { return sourceSslCertId; }
    public void setSourceSslCertId(String sourceSslCertId) { this.sourceSslCertId = sourceSslCertId; }
    public String getTargetSslMode() { return targetSslMode; }
    public void setTargetSslMode(String targetSslMode) { this.targetSslMode = targetSslMode; }
    public String getTargetSslCertId() { return targetSslCertId; }
    public void setTargetSslCertId(String targetSslCertId) { this.targetSslCertId = targetSslCertId; }

    /**
     * 把任务的加密配置搬进消息。
     *
     * <p><b>务必用这个方法，不要在各处手抄四行 setter。</b> 平台里有 6 处构造
     * {@code TaskCreatedMessage} 的地方（launch / resume / 灾备影子 / 倒换 / fan-out /
     * 跨实例汇聚 leg），手抄必然漏掉其中一两处——而漏掉的表现是那条腿<b>静默走明文</b>，
     * 任务全绿、没有任何报错。
     *
     * @param swap 源与目标对调（灾备影子任务 B→A、以及主备倒换后用）
     */
    public void applySsl(String srcMode, String srcCertId, String tgtMode, String tgtCertId, boolean swap) {
        if (swap) {
            this.sourceSslMode = tgtMode;
            this.sourceSslCertId = tgtCertId;
            this.targetSslMode = srcMode;
            this.targetSslCertId = srcCertId;
        } else {
            this.sourceSslMode = srcMode;
            this.sourceSslCertId = srcCertId;
            this.targetSslMode = tgtMode;
            this.targetSslCertId = tgtCertId;
        }
    }

    public String getTargetAgentId() {
        return targetAgentId;
    }

    public void setTargetAgentId(String targetAgentId) {
        this.targetAgentId = targetAgentId;
    }

    public TaskCreatedMessage() {
        this.messageType = "TASK_CREATED";
    }

    public String getConsistencyMode() {
        return consistencyMode;
    }

    public void setConsistencyMode(String consistencyMode) {
        this.consistencyMode = consistencyMode;
    }

    public Boolean getBulkLoadEnabled() {
        return bulkLoadEnabled;
    }

    public void setBulkLoadEnabled(Boolean bulkLoadEnabled) {
        this.bulkLoadEnabled = bulkLoadEnabled;
    }

    public String getBulkLoadMode() {
        return bulkLoadMode;
    }

    public void setBulkLoadMode(String bulkLoadMode) {
        this.bulkLoadMode = bulkLoadMode;
    }

    public String getSnapshotMode() {
        return snapshotMode;
    }

    public void setSnapshotMode(String snapshotMode) {
        this.snapshotMode = snapshotMode;
    }

    public String getDrMode() {
        return drMode;
    }

    public void setDrMode(String drMode) {
        this.drMode = drMode;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getSourceConnection() {
        return sourceConnection;
    }

    public void setSourceConnection(String sourceConnection) {
        this.sourceConnection = sourceConnection;
    }

    public String getTargetConnection() {
        return targetConnection;
    }

    public void setTargetConnection(String targetConnection) {
        this.targetConnection = targetConnection;
    }

    public String getMigrationMode() {
        return migrationMode;
    }

    public void setMigrationMode(String migrationMode) {
        this.migrationMode = migrationMode;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(String currentStatus) {
        this.currentStatus = currentStatus;
    }

    public Map<String, Object> getSyncObjects() {
        return syncObjects;
    }

    public void setSyncObjects(Map<String, Object> syncObjects) {
        this.syncObjects = syncObjects;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public String getTargetDbName() {
        return targetDbName;
    }

    public void setTargetDbName(String targetDbName) {
        this.targetDbName = targetDbName;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaTopicPrefix() {
        return kafkaTopicPrefix;
    }

    public void setKafkaTopicPrefix(String kafkaTopicPrefix) {
        this.kafkaTopicPrefix = kafkaTopicPrefix;
    }

    public String getKafkaTopicStrategy() {
        return kafkaTopicStrategy;
    }

    public void setKafkaTopicStrategy(String kafkaTopicStrategy) {
        this.kafkaTopicStrategy = kafkaTopicStrategy;
    }

    public String getSubscribeFormat() {
        return subscribeFormat;
    }

    public void setSubscribeFormat(String subscribeFormat) {
        this.subscribeFormat = subscribeFormat;
    }

    public String getSkipSeqnos() {
        return skipSeqnos;
    }

    public void setSkipSeqnos(String skipSeqnos) {
        this.skipSeqnos = skipSeqnos;
    }

    public String getSkipEventIds() {
        return skipEventIds;
    }

    public void setSkipEventIds(String skipEventIds) {
        this.skipEventIds = skipEventIds;
    }

    public Boolean getSyncAccount() {
        return syncAccount;
    }

    public void setSyncAccount(Boolean syncAccount) {
        this.syncAccount = syncAccount;
    }

    public Boolean getSyncAccountSuperPrivilege() {
        return syncAccountSuperPrivilege;
    }

    public void setSyncAccountSuperPrivilege(Boolean syncAccountSuperPrivilege) {
        this.syncAccountSuperPrivilege = syncAccountSuperPrivilege;
    }

    public String getRouteConfig() { return routeConfig; }
    public void setRouteConfig(String routeConfig) { this.routeConfig = routeConfig; }
    public String getRouteNodeId() { return routeNodeId; }
    public void setRouteNodeId(String routeNodeId) { this.routeNodeId = routeNodeId; }

    public String getTrafficConfig() {
        return trafficConfig;
    }

    public void setTrafficConfig(String trafficConfig) {
        this.trafficConfig = trafficConfig;
    }
}
