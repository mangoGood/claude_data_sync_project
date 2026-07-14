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

    /**
     * 增量同步限速(行/秒)：agent 侧 ContinuousIncrementMain 据此节流应用速率，避免增量应用过快
     * 反压到 capture/binlog 读取而打挂源库。null 或 <=0 表示不限速（保持现状默认行为）。
     */
    @Column(name = "max_increment_rows_per_sec")
    private Integer maxIncrementRowsPerSec;

    /**
     * 全量同步并发表数上限：作为 migration.full.parallelism 的封顶值（只降不升，不会绕过
     * 工程默认值），避免全量并行搬数时对源库产生过多并发连接/查询。null 或 <=0 表示不封顶。
     */
    @Column(name = "max_full_sync_concurrent_tables")
    private Integer maxFullSyncConcurrentTables;

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
    public Integer getMaxIncrementRowsPerSec() { return maxIncrementRowsPerSec; }
    public void setMaxIncrementRowsPerSec(Integer maxIncrementRowsPerSec) { this.maxIncrementRowsPerSec = maxIncrementRowsPerSec; }
    public Integer getMaxFullSyncConcurrentTables() { return maxFullSyncConcurrentTables; }
    public void setMaxFullSyncConcurrentTables(Integer maxFullSyncConcurrentTables) { this.maxFullSyncConcurrentTables = maxFullSyncConcurrentTables; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
