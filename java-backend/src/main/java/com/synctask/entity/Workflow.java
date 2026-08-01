package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "workflows")
public class Workflow {
    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false)
    private String name;

    // 连接串含数据库口令：落库前 AES-GCM 加密（DB 存 ENC: 密文），读取自动解密。列放宽为 TEXT 容纳密文。
    @Column(name = "source_connection", columnDefinition = "TEXT")
    @Convert(converter = com.synctask.security.EncryptedStringConverter.class)
    private String sourceConnection;

    @Column(name = "target_connection", columnDefinition = "TEXT")
    @Convert(converter = com.synctask.security.EncryptedStringConverter.class)
    private String targetConnection;

    @Enumerated(EnumType.STRING)
    private WorkflowStatus status = WorkflowStatus.CONFIGURING;

    private Integer progress = 0;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "error_code", length = 10)
    private String errorCode;

    @Column(name = "is_billing")
    private Boolean isBilling = false;

    @Column(name = "migration_mode")
    private String migrationMode = "full";

    @Column(name = "is_deleted")
    private Boolean isDeleted = false;

    @Column(name = "sync_objects", columnDefinition = "TEXT")
    private String syncObjects;

    @Column(name = "source_db_name")
    private String sourceDbName;

    @Column(name = "target_db_name")
    private String targetDbName;

    @Column(name = "source_type")
    private String sourceType = "mysql";

    @Column(name = "target_type")
    private String targetType = "mysql";

    @Column(name = "total_tables")
    private Integer totalTables;

    @Column(name = "completed_tables")
    private Integer completedTables;

    @Column(name = "current_table")
    private String currentTable;

    @Column(name = "current_table_progress")
    private Integer currentTableProgress;

    @Column(name = "current_table_rows")
    private Long currentTableRows;

    @Column(name = "current_table_total_rows")
    private Long currentTableTotalRows;

    @Column(name = "rpo_ms")
    private Long rpoMs;

    @Column(name = "rto_ms")
    private Long rtoMs;

    // ===== SLA 闭环指标（P2-4），由 agent 随状态消息上报 =====
    /** 绝对复制延迟：源库当前时刻 − 已应用事件的源端时刻。源库空闲时 rpoMs 恒为 0，这个不会。 */
    @Column(name = "replication_lag_ms")
    private Long replicationLagMs;

    @Column(name = "capture_replay_bytes")
    private Long captureReplayBytes;

    @Column(name = "restart_count_10m")
    private Integer restartCount10m;

    @Column(name = "conflict_count")
    private Long conflictCount;

    @Column(name = "deadletter_count")
    private Long deadletterCount;

    @Column(name = "disk_usage_bytes")
    private Long diskUsageBytes;

    @Column(name = "task_type", length = 20)
    private String taskType = "SYNC";

    @Column(name = "dr_status", length = 20)
    private String drStatus;

    @Column(name = "dr_mode", length = 20)
    private String drMode;

    @Column(name = "dr_peer_workflow_id", length = 36)
    private String drPeerWorkflowId;

    @Column(name = "dr_switch_count")
    private Integer drSwitchCount = 0;

    @Column(name = "dr_switch_start_time")
    private java.time.LocalDateTime drSwitchStartTime;

    @Column(name = "increment_started")
    private Boolean incrementStarted = false;

    @Column(name = "kafka_bootstrap_servers", length = 500)
    private String kafkaBootstrapServers;

    @Column(name = "kafka_topic_prefix", length = 100)
    private String kafkaTopicPrefix = "cdc";

    @Column(name = "kafka_topic_strategy", length = 20)
    private String kafkaTopicStrategy = "TABLE";

    @Column(name = "subscribe_format", length = 20)
    private String subscribeFormat = "DEBEZIUM_JSON";

    // fanout 多目标连接串（含口令）：同样落库前加密。
    @Column(name = "target_connections", columnDefinition = "TEXT")
    @Convert(converter = com.synctask.security.EncryptedStringConverter.class)
    private String targetConnections;

    @Column(name = "fanout_enabled")
    private Boolean fanoutEnabled = false;

    @Column(name = "fanout_target_count")
    private Integer fanoutTargetCount = 1;

    // 账号同步（仅 mysql→mysql）：是否同步账号，及是否同步超级账号权限。
    @Column(name = "sync_account")
    private Boolean syncAccount = false;

    @Column(name = "sync_account_super_privilege")
    private Boolean syncAccountSuperPrivilege = false;

    // 集群化（P1-1）：任务归属哪个 agent、租约到期时刻与代次。
    // agent_id 为 NULL 表示还没指派（或集群里一个 agent 都没注册，退回旧的广播语义）。
    @Column(name = "agent_id", length = 64)
    private String agentId;

    @Column(name = "lease_expire_at")
    private LocalDateTime leaseExpireAt;

    @Column(name = "lease_epoch")
    private Integer leaseEpoch = 0;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public WorkflowStatus getStatus() {
        return status;
    }

    public void setStatus(WorkflowStatus status) {
        this.status = status;
    }

    public Integer getProgress() {
        return progress;
    }

    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(LocalDateTime completedAt) {
        this.completedAt = completedAt;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public Boolean getIsBilling() {
        return isBilling;
    }

    public void setIsBilling(Boolean isBilling) {
        this.isBilling = isBilling;
    }

    public String getMigrationMode() {
        return migrationMode;
    }

    public void setMigrationMode(String migrationMode) {
        this.migrationMode = migrationMode;
    }

    public Boolean getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(Boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getSyncObjects() {
        return syncObjects;
    }

    public void setSyncObjects(String syncObjects) {
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

    public Integer getTotalTables() {
        return totalTables;
    }

    public void setTotalTables(Integer totalTables) {
        this.totalTables = totalTables;
    }

    public Integer getCompletedTables() {
        return completedTables;
    }

    public void setCompletedTables(Integer completedTables) {
        this.completedTables = completedTables;
    }

    public String getCurrentTable() {
        return currentTable;
    }

    public void setCurrentTable(String currentTable) {
        this.currentTable = currentTable;
    }

    public Integer getCurrentTableProgress() {
        return currentTableProgress;
    }

    public void setCurrentTableProgress(Integer currentTableProgress) {
        this.currentTableProgress = currentTableProgress;
    }

    public Long getCurrentTableRows() {
        return currentTableRows;
    }

    public void setCurrentTableRows(Long currentTableRows) {
        this.currentTableRows = currentTableRows;
    }

    public Long getCurrentTableTotalRows() {
        return currentTableTotalRows;
    }

    public void setCurrentTableTotalRows(Long currentTableTotalRows) {
        this.currentTableTotalRows = currentTableTotalRows;
    }

    public Long getRpoMs() {
        return rpoMs;
    }

    public void setRpoMs(Long rpoMs) {
        this.rpoMs = rpoMs;
    }

    public Long getRtoMs() {
        return rtoMs;
    }

    public void setRtoMs(Long rtoMs) {
        this.rtoMs = rtoMs;
    }

    public Long getReplicationLagMs() {
        return replicationLagMs;
    }

    public void setReplicationLagMs(Long replicationLagMs) {
        this.replicationLagMs = replicationLagMs;
    }

    public Long getCaptureReplayBytes() {
        return captureReplayBytes;
    }

    public void setCaptureReplayBytes(Long captureReplayBytes) {
        this.captureReplayBytes = captureReplayBytes;
    }

    public Integer getRestartCount10m() {
        return restartCount10m;
    }

    public void setRestartCount10m(Integer restartCount10m) {
        this.restartCount10m = restartCount10m;
    }

    public Long getConflictCount() {
        return conflictCount;
    }

    public void setConflictCount(Long conflictCount) {
        this.conflictCount = conflictCount;
    }

    public Long getDeadletterCount() {
        return deadletterCount;
    }

    public void setDeadletterCount(Long deadletterCount) {
        this.deadletterCount = deadletterCount;
    }

    public Long getDiskUsageBytes() {
        return diskUsageBytes;
    }

    public void setDiskUsageBytes(Long diskUsageBytes) {
        this.diskUsageBytes = diskUsageBytes;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getDrStatus() {
        return drStatus;
    }

    public void setDrStatus(String drStatus) {
        this.drStatus = drStatus;
    }

    public String getDrMode() {
        return drMode;
    }

    public void setDrMode(String drMode) {
        this.drMode = drMode;
    }

    public String getDrPeerWorkflowId() {
        return drPeerWorkflowId;
    }

    public void setDrPeerWorkflowId(String drPeerWorkflowId) {
        this.drPeerWorkflowId = drPeerWorkflowId;
    }

    public Integer getDrSwitchCount() {
        return drSwitchCount;
    }

    public void setDrSwitchCount(Integer drSwitchCount) {
        this.drSwitchCount = drSwitchCount;
    }

    public java.time.LocalDateTime getDrSwitchStartTime() {
        return drSwitchStartTime;
    }

    public void setDrSwitchStartTime(java.time.LocalDateTime drSwitchStartTime) {
        this.drSwitchStartTime = drSwitchStartTime;
    }

    public Boolean getIncrementStarted() {
        return incrementStarted;
    }

    public void setIncrementStarted(Boolean incrementStarted) {
        this.incrementStarted = incrementStarted;
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

    public String getTargetConnections() {
        return targetConnections;
    }

    public void setTargetConnections(String targetConnections) {
        this.targetConnections = targetConnections;
    }

    public Boolean getFanoutEnabled() {
        return fanoutEnabled;
    }

    public void setFanoutEnabled(Boolean fanoutEnabled) {
        this.fanoutEnabled = fanoutEnabled;
    }

    public Integer getFanoutTargetCount() {
        return fanoutTargetCount;
    }

    public void setFanoutTargetCount(Integer fanoutTargetCount) {
        this.fanoutTargetCount = fanoutTargetCount;
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

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public LocalDateTime getLeaseExpireAt() {
        return leaseExpireAt;
    }

    public void setLeaseExpireAt(LocalDateTime leaseExpireAt) {
        this.leaseExpireAt = leaseExpireAt;
    }

    public Integer getLeaseEpoch() {
        return leaseEpoch;
    }

    public void setLeaseEpoch(Integer leaseEpoch) {
        this.leaseEpoch = leaseEpoch;
    }
}
