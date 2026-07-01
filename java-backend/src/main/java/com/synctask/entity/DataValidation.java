package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 数据校验结果实体
 * 记录自动数据校验和差异修复
 */
@Entity
@Table(name = "data_validations", indexes = {
    @Index(name = "idx_validation_workflow_id", columnList = "workflow_id"),
    @Index(name = "idx_validation_user_id", columnList = "user_id"),
    @Index(name = "idx_validation_status", columnList = "status")
})
public class DataValidation {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_id", nullable = false, length = 36)
    private String workflowId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "table_name", length = 200)
    private String tableName;

    /** 校验类型: ROW_COUNT, CONTENT, CHECKSUM */
    @Column(name = "validation_type", length = 20)
    private String validationType = "ROW_COUNT";

    /** 源库名 */
    @Column(name = "source_db_name", length = 200)
    private String sourceDbName;

    /** 目标库名 */
    @Column(name = "target_db_name", length = 200)
    private String targetDbName;

    @Column(name = "source_count")
    private Long sourceCount;

    @Column(name = "target_count")
    private Long targetCount;

    @Column(name = "mismatched_count")
    private Long mismatchedCount;

    /** 状态: PENDING, RUNNING, PASSED, FAILED, REPAIRING, REPAIRED */
    @Column(name = "status", length = 20)
    private String status = "PENDING";

    @Column(name = "repair_sql", columnDefinition = "TEXT")
    private String repairSql;

    @Column(name = "repair_status", length = 20)
    private String repairStatus;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getValidationType() { return validationType; }
    public void setValidationType(String validationType) { this.validationType = validationType; }
    public String getSourceDbName() { return sourceDbName; }
    public void setSourceDbName(String sourceDbName) { this.sourceDbName = sourceDbName; }
    public String getTargetDbName() { return targetDbName; }
    public void setTargetDbName(String targetDbName) { this.targetDbName = targetDbName; }
    public Long getSourceCount() { return sourceCount; }
    public void setSourceCount(Long sourceCount) { this.sourceCount = sourceCount; }
    public Long getTargetCount() { return targetCount; }
    public void setTargetCount(Long targetCount) { this.targetCount = targetCount; }
    public Long getMismatchedCount() { return mismatchedCount; }
    public void setMismatchedCount(Long mismatchedCount) { this.mismatchedCount = mismatchedCount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getRepairSql() { return repairSql; }
    public void setRepairSql(String repairSql) { this.repairSql = repairSql; }
    public String getRepairStatus() { return repairStatus; }
    public void setRepairStatus(String repairStatus) { this.repairStatus = repairStatus; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }
}
