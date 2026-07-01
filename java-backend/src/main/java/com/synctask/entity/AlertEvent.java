package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 告警事件记录
 */
@Entity
@Table(name = "alert_events", indexes = {
    @Index(name = "idx_alert_event_rule_id", columnList = "rule_id"),
    @Index(name = "idx_alert_event_workflow_id", columnList = "workflow_id"),
    @Index(name = "idx_alert_event_created_at", columnList = "created_at")
})
public class AlertEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "rule_id")
    private Long ruleId;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "workflow_id", length = 36)
    private String workflowId;

    @Column(name = "rule_name", length = 200)
    private String ruleName;

    @Column(name = "metric_type", length = 30)
    private String metricType;

    @Column(name = "metric_value")
    private Double metricValue;

    @Column(name = "threshold")
    private Double threshold;

    @Column(name = "message", columnDefinition = "TEXT")
    private String message;

    /** 状态: PENDING, NOTIFIED, RESOLVED */
    @Column(name = "status", length = 20)
    private String status = "NOTIFIED";

    @Column(name = "notify_channels", length = 100)
    private String notifyChannels;

    @Column(name = "notify_result", columnDefinition = "TEXT")
    private String notifyResult;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getRuleId() { return ruleId; }
    public void setRuleId(Long ruleId) { this.ruleId = ruleId; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public String getRuleName() { return ruleName; }
    public void setRuleName(String ruleName) { this.ruleName = ruleName; }
    public String getMetricType() { return metricType; }
    public void setMetricType(String metricType) { this.metricType = metricType; }
    public Double getMetricValue() { return metricValue; }
    public void setMetricValue(Double metricValue) { this.metricValue = metricValue; }
    public Double getThreshold() { return threshold; }
    public void setThreshold(Double threshold) { this.threshold = threshold; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getNotifyChannels() { return notifyChannels; }
    public void setNotifyChannels(String notifyChannels) { this.notifyChannels = notifyChannels; }
    public String getNotifyResult() { return notifyResult; }
    public void setNotifyResult(String notifyResult) { this.notifyResult = notifyResult; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getResolvedAt() { return resolvedAt; }
    public void setResolvedAt(LocalDateTime resolvedAt) { this.resolvedAt = resolvedAt; }
}
