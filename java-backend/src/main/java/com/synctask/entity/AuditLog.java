package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 审计日志实体
 *
 * <p>记录用户对系统执行的关键操作，支持审计追溯。
 * 覆盖操作类型：创建、启动、停止、删除、重试、故障切换、配置更新等。
 */
@Entity
@Table(name = "audit_logs", indexes = {
    @Index(name = "idx_audit_user_id", columnList = "user_id"),
    @Index(name = "idx_audit_workflow_id", columnList = "workflow_id"),
    @Index(name = "idx_audit_action", columnList = "action"),
    @Index(name = "idx_audit_created_at", columnList = "created_at")
})
public class AuditLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 操作用户ID */
    @Column(name = "user_id", nullable = false)
    private Long userId;

    /** 操作用户名（冗余存储，便于查询展示） */
    @Column(name = "username", length = 100)
    private String username;

    /** 操作类型 */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private Action action;

    /** 关联的工作流ID（可为空，例如登录操作） */
    @Column(name = "workflow_id", length = 36)
    private String workflowId;

    /** 工作流名称（冗余存储） */
    @Column(name = "workflow_name", length = 200)
    private String workflowName;

    /** 操作详情（JSON 格式存储请求参数等） */
    @Column(columnDefinition = "TEXT")
    private String details;

    /** 操作结果 */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Result result = Result.SUCCESS;

    /** 失败时的错误信息 */
    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    /** 客户端IP地址 */
    @Column(name = "client_ip", length = 50)
    private String clientIp;

    /** User-Agent */
    @Column(name = "user_agent", length = 500)
    private String userAgent;

    /** 操作时间 */
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }

    /** 操作类型枚举 */
    public enum Action {
        CREATE_TASK,
        UPDATE_CONFIG,
        LAUNCH_TASK,
        PAUSE_TASK,
        RESUME_TASK,
        STOP_TASK,
        DELETE_TASK,
        RETRY_TASK,
        FAILOVER_TASK,
        LOGIN,
        LOGOUT,
        CHANGE_PASSWORD,

        // ---- 2026-08-19 补：此前 96 个端点里 58 个零审计，而漏掉的恰是碰数据最深的几类 ----
        /** 证书上传/删除。带私钥，是整个平台最敏感的写操作。 */
        UPLOAD_CERTIFICATE,
        DELETE_CERTIFICATE,
        /** 连接凭证的新增与变更。 */
        UPDATE_CREDENTIAL,
        /** 元数据探查：会用连接凭证连上用户库读结构。 */
        BROWSE_METADATA,
        /** 数据校验 / 内容对比：逐行把两端业务数据读回来比。 */
        RUN_VALIDATION,
        /** 排障包下载：内含配置与日志。 */
        DOWNLOAD_DIAGNOSTICS,
        /** 死信查看与裁决：死信记录里带行数据。 */
        VIEW_DEADLETTER,
        SKIP_EVENT,
        /** 建号 / 改角色 / 停用账号。 */
        MANAGE_USER,

        // ---- V21 数据治理 ----
        /** 重建字段级血缘 */
        REBUILD_LINEAGE,
        /** 打标 / 改分级 / 改分级规则 */
        UPDATE_CLASSIFICATION,
        /** 批准 Schema 变更——批准即应用到目标库，必须留痕 */
        APPROVE_SCHEMA_CHANGE,
        /** 驳回 Schema 变更 */
        REJECT_SCHEMA_CHANGE
    }

    /** 操作结果枚举 */
    public enum Result {
        SUCCESS,
        FAILURE
    }

    // Getters and Setters

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
