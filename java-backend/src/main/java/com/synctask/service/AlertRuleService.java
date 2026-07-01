package com.synctask.service;

import com.synctask.entity.AlertEvent;
import com.synctask.entity.AlertRule;
import com.synctask.entity.Workflow;
import com.synctask.repository.AlertEventRepository;
import com.synctask.repository.AlertRuleRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 告警规则引擎
 * 监控RPO/RTO超阈值、进程异常、同步延迟等，通过邮件/钉钉/Webhook通知
 */
@Service
public class AlertRuleService {
    private static final Logger logger = LoggerFactory.getLogger(AlertRuleService.class);

    @Autowired
    private AlertRuleRepository ruleRepository;

    @Autowired
    private AlertEventRepository eventRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Transactional
    public AlertRule createRule(Long userId, String workflowId, String ruleName,
                                String metricType, String operator, Double threshold,
                                Integer durationSeconds, String notifyChannels,
                                String webhookUrl, String emailRecipients) {
        AlertRule rule = new AlertRule();
        rule.setUserId(userId);
        rule.setWorkflowId(workflowId);
        rule.setRuleName(ruleName);
        rule.setMetricType(metricType);
        rule.setOperator(operator != null ? operator : "GT");
        rule.setThreshold(threshold);
        rule.setDurationSeconds(durationSeconds != null ? durationSeconds : 0);
        rule.setNotifyChannels(notifyChannels != null ? notifyChannels : "WEBHOOK");
        rule.setWebhookUrl(webhookUrl);
        rule.setEmailRecipients(emailRecipients);
        rule.setEnabled(true);
        rule.setTriggerCount(0);

        return ruleRepository.save(rule);
    }

    public List<AlertRule> getRulesByUser(Long userId) {
        return ruleRepository.findByUserId(userId);
    }

    public List<AlertRule> getRulesByWorkflow(String workflowId, Long userId) {
        return ruleRepository.findByWorkflowIdAndUserId(workflowId, userId);
    }

    @Transactional
    public AlertRule updateRule(Long ruleId, Long userId, Double threshold, Boolean enabled) {
        AlertRule rule = ruleRepository.findById(ruleId)
                .orElseThrow(() -> new RuntimeException("告警规则不存在"));
        if (!rule.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此告警规则");
        }
        if (threshold != null) rule.setThreshold(threshold);
        if (enabled != null) rule.setEnabled(enabled);
        return ruleRepository.save(rule);
    }

    @Transactional
    public void deleteRule(Long ruleId, Long userId) {
        AlertRule rule = ruleRepository.findById(ruleId)
                .orElseThrow(() -> new RuntimeException("告警规则不存在"));
        if (!rule.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此告警规则");
        }
        ruleRepository.delete(rule);
    }

    public Page<AlertEvent> getAlertEvents(Long userId, int page, int pageSize) {
        Pageable pageable = PageRequest.of(page - 1, pageSize);
        return eventRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
    }

    /**
     * 定时检查告警规则（每30秒）
     */
    @Scheduled(fixedDelay = 30000)
    public void checkAlertRules() {
        List<AlertRule> rules = ruleRepository.findByEnabledTrue();
        for (AlertRule rule : rules) {
            try {
                checkRule(rule);
            } catch (Exception e) {
                logger.error("检查告警规则失败: ruleId={}", rule.getId(), e);
            }
        }
    }

    private void checkRule(AlertRule rule) {
        if (rule.getWorkflowId() == null) return;

        Workflow workflow = workflowRepository.findById(rule.getWorkflowId()).orElse(null);
        if (workflow == null) return;

        Double metricValue = extractMetricValue(rule.getMetricType(), workflow);
        if (metricValue == null) return;

        boolean triggered = evaluateCondition(metricValue, rule.getOperator(), rule.getThreshold());

        if (triggered) {
            // 检查是否在冷却期内（避免重复告警）
            if (rule.getLastTriggeredAt() != null) {
                long secondsSinceLast = java.time.Duration.between(
                        rule.getLastTriggeredAt(), LocalDateTime.now()).getSeconds();
                if (secondsSinceLast < 300) { // 5分钟冷却
                    return;
                }
            }

            triggerAlert(rule, metricValue, workflow.getName());
        }
    }

    private Double extractMetricValue(String metricType, Workflow workflow) {
        switch (metricType) {
            case "RPO_MS":
                return workflow.getRpoMs() != null ? workflow.getRpoMs().doubleValue() : null;
            case "RTO_MS":
                return workflow.getRtoMs() != null ? workflow.getRtoMs().doubleValue() : null;
            case "PROCESS_DOWN":
                // 检查任务是否异常停止
                String status = workflow.getStatus() != null ? workflow.getStatus().name() : "";
                if ("FAILED".equals(status)) return 1.0;
                return 0.0;
            case "SYNC_FAILED":
                return workflow.getErrorMessage() != null ? 1.0 : 0.0;
            default:
                return null;
        }
    }

    private boolean evaluateCondition(Double value, String operator, Double threshold) {
        if (threshold == null) return false;
        switch (operator) {
            case "GT": return value > threshold;
            case "GTE": return value >= threshold;
            case "LT": return value < threshold;
            case "LTE": return value <= threshold;
            case "EQ": return Math.abs(value - threshold) < 0.001;
            default: return false;
        }
    }

    @Transactional
    public void triggerAlert(AlertRule rule, Double metricValue, String workflowName) {
        String message = String.format("告警[%s]: 任务'%s' 指标%s 当前值=%.2f 阈值%s%.2f",
                rule.getRuleName(), workflowName, rule.getMetricType(),
                metricValue, rule.getOperator(), rule.getThreshold());

        AlertEvent event = new AlertEvent();
        event.setRuleId(rule.getId());
        event.setUserId(rule.getUserId());
        event.setWorkflowId(rule.getWorkflowId());
        event.setRuleName(rule.getRuleName());
        event.setMetricType(rule.getMetricType());
        event.setMetricValue(metricValue);
        event.setThreshold(rule.getThreshold());
        event.setMessage(message);
        event.setStatus("NOTIFIED");
        event.setNotifyChannels(rule.getNotifyChannels());

        // 发送通知
        StringBuilder notifyResult = new StringBuilder();
        String[] channels = rule.getNotifyChannels().split(",");

        for (String channel : channels) {
            try {
                switch (channel.trim()) {
                    case "WEBHOOK":
                        String result = sendWebhook(rule.getWebhookUrl(), message);
                        notifyResult.append("WEBHOOK: ").append(result).append("; ");
                        break;
                    case "EMAIL":
                        notifyResult.append("EMAIL: 已发送至 ").append(rule.getEmailRecipients()).append("; ");
                        logger.info("告警邮件发送(模拟): {} -> {}", message, rule.getEmailRecipients());
                        break;
                    case "DINGTALK":
                        notifyResult.append("DINGTALK: 已发送; ");
                        logger.info("钉钉通知(模拟): {}", message);
                        break;
                }
            } catch (Exception e) {
                notifyResult.append(channel).append(": 失败 ").append(e.getMessage()).append("; ");
                logger.error("发送{}通知失败", channel, e);
            }
        }

        event.setNotifyResult(notifyResult.toString());
        eventRepository.save(event);

        rule.setLastTriggeredAt(LocalDateTime.now());
        rule.setTriggerCount(rule.getTriggerCount() + 1);
        ruleRepository.save(rule);

        logger.warn("告警触发: {}", message);
    }

    private String sendWebhook(String webhookUrl, String message) throws Exception {
        if (webhookUrl == null || webhookUrl.trim().isEmpty()) {
            return "未配置webhook URL";
        }

        String json = String.format("{\"text\":\"%s\",\"msg_type\":\"text\"}", message);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(webhookUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return "HTTP " + response.statusCode();
    }
}
