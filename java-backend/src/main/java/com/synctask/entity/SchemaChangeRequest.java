package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * Schema 演进审批单。
 *
 * <p>DDL 策略的 MANUAL 档此前只是"停下来记一条日志"，没有后续——
 * 运维得自己去目标库手工执行，而平台不知道执行了没有。
 * 这张单把它升级成"停下来 + 建单 + 通过后由引擎继续应用"。
 */
@Entity
@Table(name = "schema_change_request")
public class SchemaChangeRequest {

    public enum Status {
        /** 等待审批 */
        PENDING,
        /** 已批准，等引擎应用 */
        APPROVED,
        /** 已驳回，引擎将跳过这条 DDL */
        REJECTED,
        /** 引擎已应用成功 */
        APPLIED,
        /** 引擎应用失败 */
        FAILED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_id", nullable = false, length = 36)
    private String workflowId;

    @Column(name = "db_name", length = 128)
    private String dbName;

    @Column(name = "table_name", length = 128)
    private String tableName;

    @Column(name = "ddl_type", length = 64)
    private String ddlType;

    @Column(name = "ddl_sql", nullable = false, columnDefinition = "TEXT")
    private String ddlSql;

    private Long seqno;

    @Column(name = "event_id", length = 256)
    private String eventId;

    /** 受影响列里最高的敏感级别，来自 data_classification。 */
    @Enumerated(EnumType.STRING)
    @Column(name = "max_level", length = 16)
    private DataClassification.Level maxLevel;

    /** 受影响的列（含下游，来自血缘）。审批人要看的正是这个。 */
    @Column(name = "affected_columns", length = 1024)
    private String affectedColumns;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Status status = Status.PENDING;

    @Column(name = "reviewer_id")
    private Long reviewerId;

    @Column(name = "review_comment", length = 1024)
    private String reviewComment;

    @Column(name = "reviewed_at")
    private LocalDateTime reviewedAt;

    @Column(name = "applied_at")
    private LocalDateTime appliedAt;

    @Column(name = "error_message", length = 1024)
    private String errorMessage;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String v) { this.workflowId = v; }
    public String getDbName() { return dbName; }
    public void setDbName(String v) { this.dbName = v; }
    public String getTableName() { return tableName; }
    public void setTableName(String v) { this.tableName = v; }
    public String getDdlType() { return ddlType; }
    public void setDdlType(String v) { this.ddlType = v; }
    public String getDdlSql() { return ddlSql; }
    public void setDdlSql(String v) { this.ddlSql = v; }
    public Long getSeqno() { return seqno; }
    public void setSeqno(Long v) { this.seqno = v; }
    public String getEventId() { return eventId; }
    public void setEventId(String v) { this.eventId = v; }
    public DataClassification.Level getMaxLevel() { return maxLevel; }
    public void setMaxLevel(DataClassification.Level v) { this.maxLevel = v; }
    public String getAffectedColumns() { return affectedColumns; }
    public void setAffectedColumns(String v) { this.affectedColumns = v; }
    public Status getStatus() { return status; }
    public void setStatus(Status v) { this.status = v; }
    public Long getReviewerId() { return reviewerId; }
    public void setReviewerId(Long v) { this.reviewerId = v; }
    public String getReviewComment() { return reviewComment; }
    public void setReviewComment(String v) { this.reviewComment = v; }
    public LocalDateTime getReviewedAt() { return reviewedAt; }
    public void setReviewedAt(LocalDateTime t) { this.reviewedAt = t; }
    public LocalDateTime getAppliedAt() { return appliedAt; }
    public void setAppliedAt(LocalDateTime t) { this.appliedAt = t; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String v) { this.errorMessage = v; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime t) { this.createdAt = t; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime t) { this.updatedAt = t; }
}
