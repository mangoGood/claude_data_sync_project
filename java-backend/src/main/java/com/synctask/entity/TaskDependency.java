package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 任务依赖编排实体
 * 任务A完成后自动启动任务B
 */
@Entity
@Table(name = "task_dependencies", indexes = {
    @Index(name = "idx_dep_upstream", columnList = "upstream_workflow_id"),
    @Index(name = "idx_dep_downstream", columnList = "downstream_workflow_id"),
    @Index(name = "idx_dep_user_id", columnList = "user_id")
})
public class TaskDependency {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "upstream_workflow_id", nullable = false, length = 36)
    private String upstreamWorkflowId;

    @Column(name = "downstream_workflow_id", nullable = false, length = 36)
    private String downstreamWorkflowId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    /** 触发条件: ON_SUCCESS-上游成功, ON_COMPLETION-上游完成(无论成功失败), ON_FAILURE-上游失败 */
    @Column(name = "trigger_condition", length = 20)
    private String triggerCondition = "ON_SUCCESS";

    @Column(name = "enabled")
    private Boolean enabled = true;

    @Column(name = "last_triggered_at")
    private LocalDateTime lastTriggeredAt;

    @Column(name = "trigger_count")
    private Integer triggerCount = 0;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getUpstreamWorkflowId() { return upstreamWorkflowId; }
    public void setUpstreamWorkflowId(String upstreamWorkflowId) { this.upstreamWorkflowId = upstreamWorkflowId; }
    public String getDownstreamWorkflowId() { return downstreamWorkflowId; }
    public void setDownstreamWorkflowId(String downstreamWorkflowId) { this.downstreamWorkflowId = downstreamWorkflowId; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getTriggerCondition() { return triggerCondition; }
    public void setTriggerCondition(String triggerCondition) { this.triggerCondition = triggerCondition; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean enabled) { this.enabled = enabled; }
    public LocalDateTime getLastTriggeredAt() { return lastTriggeredAt; }
    public void setLastTriggeredAt(LocalDateTime lastTriggeredAt) { this.lastTriggeredAt = lastTriggeredAt; }
    public Integer getTriggerCount() { return triggerCount; }
    public void setTriggerCount(Integer triggerCount) { this.triggerCount = triggerCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}
