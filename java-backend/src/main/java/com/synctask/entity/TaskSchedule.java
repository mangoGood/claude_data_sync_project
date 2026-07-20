package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 定时调度配置实体
 * 支持Cron表达式调度全量同步任务
 */
@Entity
@Table(name = "task_schedules", indexes = {
    @Index(name = "idx_schedule_workflow_id", columnList = "workflow_id"),
    @Index(name = "idx_schedule_user_id", columnList = "user_id"),
    @Index(name = "idx_schedule_enabled", columnList = "enabled")
})
public class TaskSchedule {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_id", nullable = false, length = 36)
    private String workflowId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "cron_expression", nullable = false, length = 100)
    private String cronExpression;

    @Column(name = "schedule_name", length = 200)
    private String scheduleName;

    /** 调度类型: FULL_SYNC-全量同步, VALIDATION-数据校验 */
    @Column(name = "schedule_type", length = 20)
    private String scheduleType = "FULL_SYNC";

    /** 对比类型（仅 VALIDATION 调度用）: ROW_COUNT-行数对比, CONTENT-内容对比 */
    @Column(name = "compare_type", length = 20)
    private String compareType;

    @Column(name = "enabled")
    private Boolean enabled = true;

    @Column(name = "last_triggered_at")
    private LocalDateTime lastTriggeredAt;

    @Column(name = "next_trigger_at")
    private LocalDateTime nextTriggerAt;

    @Column(name = "trigger_count")
    private Integer triggerCount = 0;

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
    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }
    public String getScheduleName() { return scheduleName; }
    public void setScheduleName(String scheduleName) { this.scheduleName = scheduleName; }
    public String getScheduleType() { return scheduleType; }
    public void setScheduleType(String scheduleType) { this.scheduleType = scheduleType; }
    public String getCompareType() { return compareType; }
    public void setCompareType(String compareType) { this.compareType = compareType; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean enabled) { this.enabled = enabled; }
    public LocalDateTime getLastTriggeredAt() { return lastTriggeredAt; }
    public void setLastTriggeredAt(LocalDateTime lastTriggeredAt) { this.lastTriggeredAt = lastTriggeredAt; }
    public LocalDateTime getNextTriggerAt() { return nextTriggerAt; }
    public void setNextTriggerAt(LocalDateTime nextTriggerAt) { this.nextTriggerAt = nextTriggerAt; }
    public Integer getTriggerCount() { return triggerCount; }
    public void setTriggerCount(Integer triggerCount) { this.triggerCount = triggerCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
