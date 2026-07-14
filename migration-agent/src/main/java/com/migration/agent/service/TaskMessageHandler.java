package com.migration.agent.service;

import com.migration.agent.model.RecoveryTask;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStateInfo;
import com.migration.agent.model.TaskStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;

public class TaskMessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(TaskMessageHandler.class);

    private final ExecutorService taskExecutor;
    private final TaskProcessService taskProcessService;
    private final ConfigService configService;
    private final TaskStateService taskStateService;
    private final TaskRecoveryService taskRecoveryService;
    private final FailoverService failoverService;
    private final KafkaProducerService kafkaProducer;

    public TaskMessageHandler(ExecutorService taskExecutor, TaskProcessService taskProcessService,
                              ConfigService configService, TaskStateService taskStateService,
                              TaskRecoveryService taskRecoveryService, FailoverService failoverService,
                              KafkaProducerService kafkaProducer) {
        this.taskExecutor = taskExecutor;
        this.taskProcessService = taskProcessService;
        this.configService = configService;
        this.taskStateService = taskStateService;
        this.taskRecoveryService = taskRecoveryService;
        this.failoverService = failoverService;
        this.kafkaProducer = kafkaProducer;
    }

    public void handleTaskMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        String messageType = taskMessage.getMessageType();

        logger.info("Received task message: {} with messageType: {}", taskId, messageType);

        if ("stop".equals(messageType)) {
            taskExecutor.submit(() -> handleStopMessage(taskMessage));
        } else if ("terminate".equals(messageType)) {
            taskExecutor.submit(() -> handleTerminateMessage(taskMessage));
        } else if ("resume".equals(messageType)) {
            taskExecutor.submit(() -> handleResumeMessage(taskMessage));
        } else if ("delete".equals(messageType)) {
            taskExecutor.submit(() -> handleDeleteMessage(taskMessage));
        } else if ("failover".equals(messageType)) {
            taskExecutor.submit(() -> failoverService.handleFailoverFromKafka(taskMessage));
        } else {
            taskExecutor.submit(() -> processTask(taskMessage, taskId, taskMessage.getMigrationMode()));
        }
    }

    private void handleDeleteMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling delete message for task: {}", taskId);

        taskProcessService.removePausedTask(taskId);
        taskProcessService.stopTaskById(taskId);
        taskProcessService.stopMigrationAgentThread(taskId);

        logger.info("Task {} deleted, all processes stopped", taskId);
    }

    private void handleStopMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling stop message for task: {}", taskId);

        taskProcessService.addPausedTask(taskId);

        try {
            int progress = taskRecoveryService.getProgressFromDatabase(taskId);

            String currentStatus = taskMessage.getCurrentStatus();
            if (currentStatus == null || currentStatus.isEmpty()) {
                logger.warn("No currentStatus in message, falling back to MySQL query");
                RecoveryTask currentTask = taskRecoveryService.getRecoveryService().getTaskById(taskId);
                currentStatus = (currentTask != null) ? currentTask.getStatus() : "PAUSED";
            }

            logger.info("Task {} current status from message: {}", taskId, currentStatus);

            TaskStateInfo stateInfo = new TaskStateInfo(taskId);
            stateInfo.setTaskName(taskMessage.getTaskName());
            stateInfo.setUserId(taskMessage.getUserId());
            stateInfo.setMigrationMode(taskMessage.getMigrationMode());
            stateInfo.setSourceConnection(taskMessage.getSourceConnection());
            stateInfo.setTargetConnection(taskMessage.getTargetConnection());
            stateInfo.setSourceType(taskMessage.getSourceType());
            stateInfo.setTargetType(taskMessage.getTargetType());
            stateInfo.setCreatedAt(taskMessage.getCreatedAt());
            stateInfo.setStatus(currentStatus);
            stateInfo.setProgress(progress);

            taskStateService.saveTaskState(stateInfo);
            logger.info("Task state saved to H2 metadata database for task: {}, status: {}", taskId, currentStatus);

            taskProcessService.stopTaskById(taskId);
            taskProcessService.stopMigrationAgentThread(taskId);

            sendStatus(taskId, "PAUSED", "Task paused, state saved to H2", progress);

        } catch (Exception e) {
            logger.error("Error handling stop message for task: {}", taskId, e);
            taskProcessService.removePausedTask(taskId);
        }
    }

    private void handleTerminateMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling terminate message for task: {}", taskId);

        taskProcessService.removePausedTask(taskId);

        try {
            taskProcessService.stopTaskById(taskId);
            taskProcessService.stopMigrationAgentThread(taskId);

            logger.info("Task {} terminated, all processes stopped", taskId);

        } catch (Exception e) {
            logger.error("Error handling terminate message for task: {}", taskId, e);
        }
    }

    private void handleResumeMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling resume message for task: {}", taskId);

        taskProcessService.removePausedTask(taskId);

        try {
            TaskStateInfo stateInfo = taskStateService.getTaskState(taskId);

            logger.info("=== DEBUG: Task {} stateInfo from H2: {}", taskId, stateInfo != null ? "NOT NULL" : "NULL");

            if (stateInfo == null) {
                logger.warn("No saved state found in H2 for task: {}, using currentStatus from message: {}", taskId, taskMessage.getCurrentStatus());
                stateInfo = new TaskStateInfo(taskId);
                stateInfo.setMigrationMode(taskMessage.getMigrationMode());
                stateInfo.setSourceConnection(taskMessage.getSourceConnection());
                stateInfo.setTargetConnection(taskMessage.getTargetConnection());
                stateInfo.setSourceType(taskMessage.getSourceType());
                stateInfo.setTargetType(taskMessage.getTargetType());

                if (taskMessage.getCurrentStatus() != null && !taskMessage.getCurrentStatus().isEmpty()) {
                    stateInfo.setStatus(taskMessage.getCurrentStatus());
                    if ("FULL_COMPLETED".equals(taskMessage.getCurrentStatus()) || "INCREMENT_RUNNING".equals(taskMessage.getCurrentStatus())) {
                        stateInfo.setProgress(100);
                    }
                    logger.info("Using currentStatus from message as saved status: {}", taskMessage.getCurrentStatus());
                }
            } else {
                if (taskMessage.getSourceType() == null && stateInfo.getSourceType() != null) {
                    taskMessage.setSourceType(stateInfo.getSourceType());
                }
                if (taskMessage.getTargetType() == null && stateInfo.getTargetType() != null) {
                    taskMessage.setTargetType(stateInfo.getTargetType());
                }
            }

            logger.info("=== DEBUG: Task {} migrationMode from H2: {}", taskId, stateInfo.getMigrationMode());
            logger.info("=== DEBUG: Task {} status from H2: {}", taskId, stateInfo.getStatus());
            logger.info("=== DEBUG: Task {} progress from H2: {}", taskId, stateInfo.getProgress());

            configService.updateConfig(taskMessage);
            logger.info("Config updated for task: {}", taskId);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            String migrationMode = stateInfo.getMigrationMode();
            int progress = stateInfo.getProgress();
            String savedStatus = stateInfo.getStatus();

            logger.info("Resuming task: {} with mode: {}, progress: {}, status: {}",
                taskId, migrationMode, progress, savedStatus);

            if ("fullAndIncre".equals(migrationMode) || "subscribe".equals(migrationMode)) {
                boolean skipFullMigration = "FULL_COMPLETED".equals(savedStatus) ||
                                           "INCREMENT_RUNNING".equals(savedStatus) ||
                                           "SUBSCRIBE_RUNNING".equals(savedStatus);

                taskProcessService.startMigrationAgentThread(taskMessage, skipFullMigration);

                logger.info("MigrationAgentThread started for {} task: {}, skipFullMigration: {}", migrationMode, taskId, skipFullMigration);
            } else {
                if (progress < 100) {
                    taskProcessService.startMigrationForTask(taskId, taskMessage);
                    sendStatus(taskId, "STARTING", "Task resumed, starting migration", progress);
                } else {
                    sendStatus(taskId, "COMPLETED", "Task completed", progress);
                }
            }

        } catch (Exception e) {
            logger.error("Error handling resume message for task: {}", taskId, e);
            sendStatus(taskId, "FAILED", "Error resuming task: " + e.getMessage(), 0);
        }
    }

    private void processTask(TaskMessage taskMessage, String taskId, String migrationMode) {
        try {
            sendStatus(taskId, "RECEIVED", "Task received, preparing migration", 0);

            configService.updateConfig(taskMessage);
            logger.info("Config updated for task: {}", taskId);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            File configFile = new File("files/" + taskId + "/config.properties");
            if (configFile.exists()) {
                logger.info("Config file verified at: {}", configFile.getAbsolutePath());
            } else {
                logger.warn("Config file not found at: {}", configFile.getAbsolutePath());
            }

            if ("fullAndIncre".equals(migrationMode) || "subscribe".equals(migrationMode)) {
                taskProcessService.startMigrationAgentThread(taskMessage, false);
                logger.info("MigrationAgentThread started for {} task: {}", migrationMode, taskId);
            } else {
                logger.info("Full migration mode, skipping binlog process for task: {}", taskId);
                taskProcessService.startMigrationForTask(taskId, taskMessage);
            }

        } catch (Exception e) {
            logger.error("Error handling task message: {}", taskId, e);
            sendStatus(taskId, "FAILED", "Error: " + e.getMessage(), 0);
        }
    }

    private void sendStatus(String taskId, String status, String message, int progress) {
        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus(status);
        statusMessage.setMessage(message);
        statusMessage.setProgress(progress);

        kafkaProducer.sendStatus(statusMessage);
    }
}
