package com.synctask.service;

import com.synctask.dto.TaskStatusUpdate;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Map;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowLogRepository workflowLogRepository;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private KafkaProducerService kafkaProducerService;
    
    private long serviceStartTime;

    @PostConstruct
    public void init() {
        serviceStartTime = System.currentTimeMillis();
        logger.info("KafkaConsumerService 初始化，启动时间戳: {}", serviceStartTime);
    }

    @KafkaListener(topics = "${spring.kafka.topics.task-status}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consumeTaskStatusMessage(Map<String, Object> messageMap) {
        logger.info("收到任务状态消息: {}", messageMap);

        try {
            Long messageTimestamp = getLongValue(messageMap, "timestamp");
            if (messageTimestamp != null && messageTimestamp < serviceStartTime) {
                logger.info("忽略旧消息: 消息时间戳 {} 早于服务启动时间 {}", messageTimestamp, serviceStartTime);
                return;
            }
            
            String taskId = getStringValue(messageMap, "taskId");
            String status = getStringValue(messageMap, "status");
            Integer progress = getIntegerValue(messageMap, "progress");
            String errorMessage = getStringValue(messageMap, "errorMessage");
            String message = getStringValue(messageMap, "message");
            String errorCode = getStringValue(messageMap, "errorCode");
            Boolean isBilling = getBooleanValue(messageMap, "isBilling");
            
            Integer totalTables = getIntegerValue(messageMap, "totalTables");
            Integer completedTables = getIntegerValue(messageMap, "completedTables");
            String currentTable = getStringValue(messageMap, "currentTable");
            Integer currentTableProgress = getIntegerValue(messageMap, "currentTableProgress");
            Long currentTableRows = getLongValue(messageMap, "currentTableRows");
            Long currentTableTotalRows = getLongValue(messageMap, "currentTableTotalRows");
            Long rpoMs = getLongValue(messageMap, "rpoMs");
            Long rtoMs = getLongValue(messageMap, "rtoMs");

            logger.info("解析消息: taskId={}, status={}, progress={}", taskId, status, progress);

            Workflow workflow = workflowRepository.findById(taskId).orElse(null);
            if (workflow == null) {
                logger.warn("任务不存在: taskId={}", taskId);
                return;
            }

            WorkflowStatus newStatus = parseWorkflowStatus(status);
            WorkflowStatus oldStatus = workflow.getStatus();
            
            if (oldStatus == WorkflowStatus.COMPLETED || oldStatus == WorkflowStatus.FAILED || oldStatus == WorkflowStatus.CONFIGURING) {
                logger.info("任务已处于终态/配置中 {}，忽略状态更新消息: newStatus={}", oldStatus, newStatus);
                return;
            }

            if (oldStatus == WorkflowStatus.SWITCHING && newStatus != WorkflowStatus.SWITCHING && newStatus != WorkflowStatus.INCREMENT_RUNNING && newStatus != WorkflowStatus.FAILED) {
                logger.info("任务正在倒换中 {}，忽略非增量同步状态更新: newStatus={}", oldStatus, newStatus);
                return;
            }

            if (oldStatus == WorkflowStatus.SWITCHING || newStatus == WorkflowStatus.SWITCHING) {
                logger.info("倒换相关状态变更 {} -> {}，重新从数据库读取最新连接信息", oldStatus, newStatus);
                workflow = workflowRepository.findById(taskId).orElse(workflow);
            }
            
            // newStatus 为 null：agent 的过程/通知类状态（CAPTURE_STARTED/RUNNING 等）或未知状态，
            // 不改变生命周期状态，但仍继续应用下面的进度、表信息、计费、RPO/RTO 等字段
            if (newStatus != null) {
                workflow.setStatus(newStatus);
            }

            if (progress != null) {
                workflow.setProgress(progress);
            }

            if (errorMessage != null && !errorMessage.isEmpty()) {
                workflow.setErrorMessage(errorMessage);
            } else if (message != null && !message.isEmpty() && newStatus == WorkflowStatus.FAILED) {
                workflow.setErrorMessage(message);
            }

            if (errorCode != null && !errorCode.isEmpty()) {
                workflow.setErrorCode(errorCode);
            } else if (newStatus == WorkflowStatus.FAILED && (errorCode == null || errorCode.isEmpty())) {
                workflow.setErrorCode("E9999");
            }

            if (isBilling != null) {
                workflow.setIsBilling(isBilling);
            }
            
            if (totalTables != null) {
                workflow.setTotalTables(totalTables);
            }
            if (completedTables != null) {
                workflow.setCompletedTables(completedTables);
            }
            if (currentTable != null) {
                workflow.setCurrentTable(currentTable);
            }
            if (currentTableProgress != null) {
                workflow.setCurrentTableProgress(currentTableProgress);
            }
            if (currentTableRows != null) {
                workflow.setCurrentTableRows(currentTableRows);
            }
            if (currentTableTotalRows != null) {
                workflow.setCurrentTableTotalRows(currentTableTotalRows);
            }
            if (rpoMs != null) {
                workflow.setRpoMs(rpoMs);
            }
            if (rtoMs != null) {
                workflow.setRtoMs(rtoMs);
            }

            String migrationMode = workflow.getMigrationMode();
            boolean isFullAndIncre = "fullAndIncre".equals(migrationMode);
            
            if (newStatus == WorkflowStatus.COMPLETED || newStatus == WorkflowStatus.FAILED) {
                workflow.setCompletedAt(LocalDateTime.now());
                workflow.setIsBilling(false);
            } else if (newStatus == WorkflowStatus.FULL_COMPLETED) {
                if (isFullAndIncre) {
                    // 全量+增量模式：全量完成后继续增量同步，保持计费
                    workflow.setIsBilling(true);
                } else {
                    // 仅全量模式：全量完成后任务结束，停止计费
                    workflow.setCompletedAt(LocalDateTime.now());
                    workflow.setIsBilling(false);
                }
            } else if (newStatus == WorkflowStatus.INCREMENT_RUNNING) {
                workflow.setIsBilling(true);
                workflow.setIncrementStarted(true);
            } else if (newStatus == WorkflowStatus.SUBSCRIBE_RUNNING) {
                workflow.setIsBilling(true);
            }

            if ("DR".equals(workflow.getTaskType())) {
                if (newStatus == WorkflowStatus.FULL_MIGRATING) {
                    workflow.setDrStatus("DR_INITIALIZING");
                } else if (newStatus == WorkflowStatus.INCREMENT_RUNNING) {
                    if (oldStatus == WorkflowStatus.SWITCHING && workflow.getDrSwitchStartTime() != null) {
                        long switchRtoMs = java.time.Duration.between(workflow.getDrSwitchStartTime(), java.time.LocalDateTime.now()).toMillis();
                        workflow.setRtoMs(switchRtoMs);
                        logger.info("主备倒换完成，RTO={}ms (从{}到{})", switchRtoMs, workflow.getDrSwitchStartTime(), java.time.LocalDateTime.now());
                    }
                    workflow.setDrStatus("DR_RUNNING");
                } else if (newStatus == WorkflowStatus.FAILED) {
                    workflow.setDrStatus("DR_ERROR");
                } else if (newStatus == WorkflowStatus.SWITCHING) {
                    workflow.setDrStatus("SWITCHING");
                }
            }

            if ("DR".equals(workflow.getTaskType()) && (oldStatus == WorkflowStatus.SWITCHING || newStatus == WorkflowStatus.SWITCHING)) {
                Workflow latestWorkflow = workflowRepository.findById(taskId).orElse(workflow);
                workflow.setSourceConnection(latestWorkflow.getSourceConnection());
                workflow.setTargetConnection(latestWorkflow.getTargetConnection());
                workflow.setSourceType(latestWorkflow.getSourceType());
                workflow.setTargetType(latestWorkflow.getTargetType());
                workflow.setSourceDbName(latestWorkflow.getSourceDbName());
                workflow.setTargetDbName(latestWorkflow.getTargetDbName());
                logger.info("倒换期间保护连接信息: source={}, target={}", latestWorkflow.getSourceConnection(), latestWorkflow.getTargetConnection());
            }

            workflowRepository.save(workflow);
            logger.info("任务状态已更新: taskId={}, status={}", workflow.getId(), workflow.getStatus());

            maybeLaunchBidiShadow(workflow, newStatus);
            maybePropagateShadowFailure(workflow, newStatus);

            String logMessage = buildStatusLogMessage(newStatus, oldStatus, progress, 
                workflow.getErrorMessage(), errorCode,
                totalTables, completedTables, currentTable, currentTableProgress);
            WorkflowLog.LogLevel logLevel = determineLogLevel(newStatus);
            
            addLog(workflow.getId(), logLevel, logMessage);

            TaskStatusUpdate update = new TaskStatusUpdate();
            update.setTaskId(workflow.getId());
            update.setStatus(workflow.getStatus().name());
            update.setProgress(workflow.getProgress());
            update.setUserId(workflow.getUserId());
            update.setMigrationMode(workflow.getMigrationMode());
            update.setTotalTables(workflow.getTotalTables());
            update.setCompletedTables(workflow.getCompletedTables());
            update.setCurrentTable(workflow.getCurrentTable());
            update.setCurrentTableProgress(workflow.getCurrentTableProgress());
            update.setCurrentTableRows(workflow.getCurrentTableRows());
            update.setCurrentTableTotalRows(workflow.getCurrentTableTotalRows());
            update.setErrorMessage(workflow.getErrorMessage());
            update.setErrorCode(workflow.getErrorCode());
            update.setRpoMs(workflow.getRpoMs());
            update.setRtoMs(workflow.getRtoMs());

            messagingTemplate.convertAndSend("/topic/task-status", update);
            messagingTemplate.convertAndSendToUser(
                workflow.getUserId().toString(), 
                "/queue/task-status", 
                update
            );
            logger.info("任务状态更新已推送到 WebSocket: taskId={}, userId={}", workflow.getId(), workflow.getUserId());

        } catch (Exception e) {
            logger.error("处理任务状态消息失败: {}", messageMap, e);
        }
    }

    private Long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.valueOf(String.valueOf(value));
    }

    /**
     * 将 agent 上报的状态串解析为工作流生命周期状态。
     * <ul>
     *   <li>能直接匹配 {@link WorkflowStatus} 常量的，返回对应值；</li>
     *   <li>agent 的过程/通知类状态做映射或忽略：{@code MIGRATION_STARTED → FULL_MIGRATING}，
     *       {@code CAPTURE_STARTED/EXTRACT_STARTED/RUNNING} 返回 null（不改变生命周期状态）；</li>
     *   <li>无法识别的状态记日志并返回 null，由调用方跳过状态变更但仍应用其余字段，
     *       避免整条消息因 {@code valueOf} 抛 {@link IllegalArgumentException} 被整体丢弃。</li>
     * </ul>
     */
    /**
     * 双向灾备编排：正向主任务进入增量同步（全量已完成）后，自动启动反向影子任务（B→A 仅增量）。
     *
     * <p>为什么等到 INCREMENT_RUNNING 才启动：反向 capture 无 checkpoint 时从 B 的"最新"binlog
     * 位点起步，此刻正向全量灌入 B 的存量写入已成为历史位点，天然不会被反向回传；正向增量
     * 后续写入 B 的事务带 origin 标记，由反向 capture 的环路防护跳过。
     *
     * <p>幂等：INCREMENT_RUNNING 会随周期性指标上报反复出现，仅当影子仍处于 CONFIGURING
     * （从未启动过）时才发射一次。
     */
    private void maybeLaunchBidiShadow(Workflow primary, WorkflowStatus newStatus) {
        if (newStatus != WorkflowStatus.INCREMENT_RUNNING) {
            return;
        }
        if (!"DR".equals(primary.getTaskType()) || !"BIDIRECTIONAL".equals(primary.getDrMode())
                || primary.getDrPeerWorkflowId() == null) {
            return;
        }
        try {
            Workflow shadow = workflowRepository.findById(primary.getDrPeerWorkflowId()).orElse(null);
            if (shadow == null || shadow.getStatus() != WorkflowStatus.CONFIGURING) {
                return;
            }
            shadow.setStatus(WorkflowStatus.PENDING);
            shadow.setIsBilling(true);
            workflowRepository.save(shadow);
            kafkaProducerService.sendTaskCreatedMessage(shadow);
            addLog(primary.getId(), WorkflowLog.LogLevel.INFO,
                    "双向灾备：正向已进入增量同步，反向同步通道（" + shadow.getId() + "）已自动启动");
            logger.info("双向灾备反向通道已启动: primary={}, shadow={}", primary.getId(), shadow.getId());
        } catch (Exception e) {
            logger.error("双向灾备反向通道启动失败: primary={}", primary.getId(), e);
            addLog(primary.getId(), WorkflowLog.LogLevel.WARNING,
                    "双向灾备：反向同步通道启动失败: " + e.getMessage());
        }
    }

    /** 反向影子任务失败时，把失败信息透出到用户可见的主任务上（不改变主任务状态，正向仍在同步）。 */
    private void maybePropagateShadowFailure(Workflow shadow, WorkflowStatus newStatus) {
        if (newStatus != WorkflowStatus.FAILED || !"DR_SHADOW".equals(shadow.getTaskType())
                || shadow.getDrPeerWorkflowId() == null) {
            return;
        }
        try {
            Workflow primary = workflowRepository.findById(shadow.getDrPeerWorkflowId()).orElse(null);
            if (primary == null) {
                return;
            }
            String reason = shadow.getErrorMessage() != null ? shadow.getErrorMessage() : "未知原因";
            primary.setErrorMessage("双向灾备反向通道（B→A）失败: " + reason);
            workflowRepository.save(primary);
            addLog(primary.getId(), WorkflowLog.LogLevel.ERROR,
                    "双向灾备：反向同步通道失败: " + reason + "（正向 A→B 同步不受影响，仍在运行）");
        } catch (Exception e) {
            logger.error("透传反向通道失败信息出错: shadow={}", shadow.getId(), e);
        }
    }

    private WorkflowStatus parseWorkflowStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        String s = status.trim().toUpperCase();
        switch (s) {
            case "MIGRATION_STARTED":
                return WorkflowStatus.FULL_MIGRATING;
            case "CAPTURE_STARTED":
            case "EXTRACT_STARTED":
            case "RUNNING":
                return null;
            default:
                try {
                    return WorkflowStatus.valueOf(s);
                } catch (IllegalArgumentException e) {
                    logger.warn("未知的任务状态 '{}'，跳过状态变更（仍应用进度等字段）", status);
                    return null;
                }
        }
    }

    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? String.valueOf(value) : null;
    }

    private Integer getIntegerValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Number) return ((Number) value).intValue();
        return Integer.valueOf(String.valueOf(value));
    }

    private Boolean getBooleanValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        return Boolean.valueOf(String.valueOf(value));
    }

    private String buildStatusLogMessage(WorkflowStatus newStatus, WorkflowStatus oldStatus, Integer progress, String errorMessage, String errorCode,
            Integer totalTables, Integer completedTables, String currentTable, Integer currentTableProgress) {
        switch (newStatus) {
            case CONFIGURING:
                return "任务配置中";
            case RECEIVED:
                return "Agent已接收任务，准备执行";
            case STARTING:
                return "任务启动中";
            case FULL_MIGRATING:
                StringBuilder msg = new StringBuilder("全量同步中");
                if (totalTables != null && completedTables != null) {
                    msg.append(String.format("，表进度: %d/%d", completedTables, totalTables));
                }
                if (currentTable != null) {
                    msg.append(String.format("，当前表: %s", currentTable));
                    if (currentTableProgress != null) {
                        msg.append(String.format(" (%d%%)", currentTableProgress));
                    }
                }
                return msg.toString();
            case FULL_COMPLETED:
                return "全量同步完成";
            case INCREMENT_RUNNING:
                return "增量同步中";
            case SUBSCRIBE_RUNNING:
                return "数据订阅中";
            case COMPLETED:
                return "任务执行完成";
            case FAILED:
                String errorDetail = errorMessage != null && !errorMessage.isEmpty() ? errorMessage : "未知错误";
                String errorCodeStr = errorCode != null && !errorCode.isEmpty() ? "[" + errorCode + "] " : "";
                return String.format("任务执行失败: %s%s", errorCodeStr, errorDetail);
            case PAUSED:
                return "任务已暂停";
            default:
                return String.format("任务状态更新为: %s", newStatus.name());
        }
    }

    private WorkflowLog.LogLevel determineLogLevel(WorkflowStatus status) {
        switch (status) {
            case FAILED:
                return WorkflowLog.LogLevel.ERROR;
            case PAUSED:
                return WorkflowLog.LogLevel.WARNING;
            default:
                return WorkflowLog.LogLevel.INFO;
        }
    }

    private void addLog(String workflowId, WorkflowLog.LogLevel level, String message) {
        WorkflowLog log = new WorkflowLog();
        log.setWorkflowId(workflowId);
        log.setLevel(level);
        log.setMessage(message);
        workflowLogRepository.save(log);
        logger.info("已添加任务日志: workflowId={}, level={}, message={}", workflowId, level, message);
    }
}
