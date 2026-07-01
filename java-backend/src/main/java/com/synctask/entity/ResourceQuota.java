package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 资源配额实体
 * 按用户限制最大任务数、并发数、存储空间
 */
@Entity
@Table(name = "resource_quotas", indexes = {
    @Index(name = "idx_quota_user_id", columnList = "user_id")
})
public class ResourceQuota {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, unique = true)
    private Long userId;

    /** 最大任务数 */
    @Column(name = "max_tasks")
    private Integer maxTasks = 50;

    /** 最大并发运行任务数 */
    @Column(name = "max_concurrent_tasks")
    private Integer maxConcurrentTasks = 5;

    /** 最大存储空间(MB) */
    @Column(name = "max_storage_mb")
    private Long maxStorageMb = 10240L;

    /** API调用频率限制(次/分钟) */
    @Column(name = "api_rate_limit_per_min")
    private Integer apiRateLimitPerMin = 100;

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
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public Integer getMaxTasks() { return maxTasks; }
    public void setMaxTasks(Integer maxTasks) { this.maxTasks = maxTasks; }
    public Integer getMaxConcurrentTasks() { return maxConcurrentTasks; }
    public void setMaxConcurrentTasks(Integer maxConcurrentTasks) { this.maxConcurrentTasks = maxConcurrentTasks; }
    public Long getMaxStorageMb() { return maxStorageMb; }
    public void setMaxStorageMb(Long maxStorageMb) { this.maxStorageMb = maxStorageMb; }
    public Integer getApiRateLimitPerMin() { return apiRateLimitPerMin; }
    public void setApiRateLimitPerMin(Integer apiRateLimitPerMin) { this.apiRateLimitPerMin = apiRateLimitPerMin; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
