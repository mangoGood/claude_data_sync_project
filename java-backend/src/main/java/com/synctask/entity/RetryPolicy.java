package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 重试策略配置实体
 * 按错误类型配置重试次数和间隔
 */
@Entity
@Table(name = "retry_policies", indexes = {
    @Index(name = "idx_retry_workflow_id", columnList = "workflow_id"),
    @Index(name = "idx_retry_user_id", columnList = "user_id")
})
public class RetryPolicy {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_id", length = 36)
    private String workflowId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "policy_name", length = 200)
    private String policyName;

    /** 错误类型: CONNECTION-连接错误, SQL-SQL错误, TIMEOUT-超时, ALL-所有错误 */
    @Column(name = "error_type", nullable = false, length = 20)
    private String errorType = "ALL";

    @Column(name = "max_retries", nullable = false)
    private Integer maxRetries = 3;

    /** 重试间隔(毫秒) */
    @Column(name = "retry_interval_ms", nullable = false)
    private Long retryIntervalMs = 5000L;

    /** 退避策略: FIXED-固定间隔, EXPONENTIAL-指数退避 */
    @Column(name = "backoff_strategy", length = 20)
    private String backoffStrategy = "EXPONENTIAL";

    @Column(name = "max_interval_ms")
    private Long maxIntervalMs = 60000L;

    @Column(name = "enabled")
    private Boolean enabled = true;

    @Column(name = "current_retry_count")
    private Integer currentRetryCount = 0;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getPolicyName() { return policyName; }
    public void setPolicyName(String policyName) { this.policyName = policyName; }
    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }
    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }
    public Long getRetryIntervalMs() { return retryIntervalMs; }
    public void setRetryIntervalMs(Long retryIntervalMs) { this.retryIntervalMs = retryIntervalMs; }
    public String getBackoffStrategy() { return backoffStrategy; }
    public void setBackoffStrategy(String backoffStrategy) { this.backoffStrategy = backoffStrategy; }
    public Long getMaxIntervalMs() { return maxIntervalMs; }
    public void setMaxIntervalMs(Long maxIntervalMs) { this.maxIntervalMs = maxIntervalMs; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean enabled) { this.enabled = enabled; }
    public Integer getCurrentRetryCount() { return currentRetryCount; }
    public void setCurrentRetryCount(Integer currentRetryCount) { this.currentRetryCount = currentRetryCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
