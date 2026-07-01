package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "validation_tasks")
public class ValidationTask {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(nullable = false)
    private String name;

    @Column(name = "workflow_id", nullable = false)
    private String workflowId;

    @Column(name = "workflow_name")
    private String workflowName;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "source_connection", columnDefinition = "TEXT")
    private String sourceConnection;

    @Column(name = "target_connection", columnDefinition = "TEXT")
    private String targetConnection;

    @Column(name = "sync_objects", columnDefinition = "TEXT")
    private String syncObjects;

    @Column(name = "compare_type")
    private String compareType = "ROW_COUNT";

    @Column(name = "task_type", length = 20)
    private String taskType = "SYNC";

    @Column(name = "compare_result", columnDefinition = "LONGTEXT")
    private String compareResult;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ValidationStatus status = ValidationStatus.PENDING;

    @Column(name = "total_tables")
    private Integer totalTables;

    @Column(name = "passed_tables")
    private Integer passedTables;

    @Column(name = "failed_tables")
    private Integer failedTables;

    @Column(name = "total_rows")
    private Long totalRows;

    @Column(name = "mismatched_rows")
    private Long mismatchedRows;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "is_deleted")
    private Boolean isDeleted = false;

    public enum ValidationStatus {
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED
    }

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }

    public String getWorkflowName() { return workflowName; }
    public void setWorkflowName(String workflowName) { this.workflowName = workflowName; }

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }

    public String getSourceConnection() { return sourceConnection; }
    public void setSourceConnection(String sourceConnection) { this.sourceConnection = sourceConnection; }

    public String getTargetConnection() { return targetConnection; }
    public void setTargetConnection(String targetConnection) { this.targetConnection = targetConnection; }

    public String getSyncObjects() { return syncObjects; }
    public void setSyncObjects(String syncObjects) { this.syncObjects = syncObjects; }

    public ValidationStatus getStatus() { return status; }
    public void setStatus(ValidationStatus status) { this.status = status; }

    public Integer getTotalTables() { return totalTables; }
    public void setTotalTables(Integer totalTables) { this.totalTables = totalTables; }

    public Integer getPassedTables() { return passedTables; }
    public void setPassedTables(Integer passedTables) { this.passedTables = passedTables; }

    public Integer getFailedTables() { return failedTables; }
    public void setFailedTables(Integer failedTables) { this.failedTables = failedTables; }

    public Long getTotalRows() { return totalRows; }
    public void setTotalRows(Long totalRows) { this.totalRows = totalRows; }

    public Long getMismatchedRows() { return mismatchedRows; }
    public void setMismatchedRows(Long mismatchedRows) { this.mismatchedRows = mismatchedRows; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getStartedAt() { return startedAt; }
    public void setStartedAt(LocalDateTime startedAt) { this.startedAt = startedAt; }

    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }

    public Boolean getIsDeleted() { return isDeleted; }
    public void setIsDeleted(Boolean isDeleted) { this.isDeleted = isDeleted; }

    public String getCompareType() { return compareType; }
    public void setCompareType(String compareType) { this.compareType = compareType; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getCompareResult() { return compareResult; }
    public void setCompareResult(String compareResult) { this.compareResult = compareResult; }
}
