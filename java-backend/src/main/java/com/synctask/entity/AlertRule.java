package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 告警规则实体
 * 支持RPO/RTO超阈值、进程异常、同步延迟等告警
 */
@Entity
@Table(name = "alert_rules", indexes = {
    @Index(name = "idx_alert_user_id", columnList = "user_id"),
    @Index(name = "idx_alert_workflow_id", columnList = "workflow_id"),
    @Index(name = "idx_alert_enabled", columnList = "enabled")
})
public class AlertRule {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "workflow_id", length = 36)
    private String workflowId;

    @Column(name = "rule_name", nullable = false, length = 200)
    private String ruleName;

    /** 指标类型: RPO_MS, RTO_MS, SYNC_LATENCY, PROCESS_DOWN, SYNC_FAILED, SLOW_SQL */
    @Column(name = "metric_type", nullable = false, length = 30)
    private String metricType;

    /** 比较操作符: GT, LT, EQ, GTE, LTE */
    @Column(name = "operator", nullable = false, length = 5)
    private String operator = "GT";

    @Column(name = "threshold")
    private Double threshold;

    /** 持续时间(秒)，超过该时长才告警 */
    @Column(name = "duration_seconds")
    private Integer durationSeconds = 0;

    /** 通知渠道: EMAIL, DINGTALK, WEBHOOK */
    @Column(name = "notify_channels", length = 100)
    private String notifyChannels = "WEBHOOK";

    @Column(name = "webhook_url", length = 500)
    private String webhookUrl;

    @Column(name = "email_recipients", length = 500)
    private String emailRecipients;

    @Column(name = "enabled")
    private Boolean enabled = true;

    @Column(name = "last_triggered_at")
    private LocalDateTime lastTriggeredAt;

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
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public String getRuleName() { return ruleName; }
    public void setRuleName(String ruleName) { this.ruleName = ruleName; }
    public String getMetricType() { return metricType; }
    public void setMetricType(String metricType) { this.metricType = metricType; }
    public String getOperator() { return operator; }
    public void setOperator(String operator) { this.operator = operator; }
    public Double getThreshold() { return threshold; }
    public void setThreshold(Double threshold) { this.threshold = threshold; }
    public Integer getDurationSeconds() { return durationSeconds; }
    public void setDurationSeconds(Integer durationSeconds) { this.durationSeconds = durationSeconds; }
    public String getNotifyChannels() { return notifyChannels; }
    public void setNotifyChannels(String notifyChannels) { this.notifyChannels = notifyChannels; }
    public String getWebhookUrl() { return webhookUrl; }
    public void setWebhookUrl(String webhookUrl) { this.webhookUrl = webhookUrl; }
    public String getEmailRecipients() { return emailRecipients; }
    public void setEmailRecipients(String emailRecipients) { this.emailRecipients = emailRecipients; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean enabled) { this.enabled = enabled; }
    public LocalDateTime getLastTriggeredAt() { return lastTriggeredAt; }
    public void setLastTriggeredAt(LocalDateTime lastTriggeredAt) { this.lastTriggeredAt = lastTriggeredAt; }
    public Integer getTriggerCount() { return triggerCount; }
    public void setTriggerCount(Integer triggerCount) { this.triggerCount = triggerCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
