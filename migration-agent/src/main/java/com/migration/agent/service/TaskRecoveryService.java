package com.migration.agent.service;

import com.migration.agent.model.RecoveryTask;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class TaskRecoveryService {
    private static final Logger logger = LoggerFactory.getLogger(TaskRecoveryService.class);

    private final RecoveryService recoveryService;
    private final ConfigService configService;
    private final TaskProcessService taskProcessService;
    private final KafkaProducerService kafkaProducer;
    private final AgentConfig config;

    public TaskRecoveryService(RecoveryService recoveryService, ConfigService configService,
                               TaskProcessService taskProcessService, KafkaProducerService kafkaProducer) {
        this(recoveryService, configService, taskProcessService, kafkaProducer, new AgentConfig());
    }

    public TaskRecoveryService(RecoveryService recoveryService, ConfigService configService,
                               TaskProcessService taskProcessService, KafkaProducerService kafkaProducer,
                               AgentConfig config) {
        this.recoveryService = recoveryService;
        this.configService = configService;
        this.taskProcessService = taskProcessService;
        this.kafkaProducer = kafkaProducer;
        this.config = config;
    }

    public RecoveryService getRecoveryService() {
        return recoveryService;
    }

    public void recoverUnfinishedTasks() {
        logger.info("Starting to recover unfinished tasks...");

        try {
            List<RecoveryTask> unfinishedTasks = recoveryService.getUnfinishedTasks();

            if (unfinishedTasks.isEmpty()) {
                logger.info("No unfinished tasks found to recover");
                return;
            }

            for (RecoveryTask recoveryTask : unfinishedTasks) {
                try {
                    recoverTask(recoveryTask);
                } catch (Exception e) {
                    logger.error("Error recovering task: {}", recoveryTask.getTaskId(), e);
                    sendStatus(recoveryTask.getTaskId(), "FAILED",
                        "Failed to recover task: " + e.getMessage(), recoveryTask.getProgress());
                }
            }

            logger.info("Task recovery completed, recovered {} tasks", unfinishedTasks.size());

        } catch (Exception e) {
            logger.error("Error during task recovery", e);
        }
    }

    private void recoverTask(RecoveryTask recoveryTask) {
        String taskId = recoveryTask.getTaskId();
        String status = recoveryTask.getStatus();
        String migrationMode = recoveryTask.getMigrationMode();
        int progress = recoveryTask.getProgress();

        logger.info("Recovering task: id={}, status={}, mode={}, progress={}",
            taskId, status, migrationMode, progress);

        TaskMessage taskMessage = recoveryTask.toTaskMessage();

        try {
            configService.updateConfig(taskMessage);
            logger.info("Config updated for recovered task: {}", taskId);
        } catch (Exception e) {
            logger.error("Error updating config for task: {}", taskId, e);
            throw new RuntimeException("Failed to update config: " + e.getMessage(), e);
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if ("fullAndIncre".equals(migrationMode)) {
            recoverFullAndIncreTask(recoveryTask, taskMessage);
        } else {
            recoverFullOnlyTask(recoveryTask, taskMessage);
        }
    }

    private void recoverFullAndIncreTask(RecoveryTask recoveryTask, TaskMessage taskMessage) {
        String taskId = recoveryTask.getTaskId();
        String status = recoveryTask.getStatus();
        int progress = recoveryTask.getProgress();

        switch (status) {
            case "STARTING":
                logger.info("Task {} was in STARTING state, restarting from beginning", taskId);
                taskProcessService.startMigrationAgentThread(taskMessage, false);
                break;

            case "FULL_MIGRATING":
                logger.info("Task {} was in FULL_MIGRATING state (progress: {}%), resuming full migration",
                    taskId, progress);
                taskProcessService.startMigrationAgentThread(taskMessage, false);
                break;

            case "FULL_COMPLETED":
                logger.info("Task {} was in FULL_COMPLETED state, starting incremental sync", taskId);
                taskProcessService.startMigrationAgentThread(taskMessage, true);
                break;

            case "INCREMENT_RUNNING":
                logger.info("Task {} was in INCREMENT_RUNNING state, resuming incremental sync from checkpoint", taskId);
                taskProcessService.startMigrationAgentThread(taskMessage, true);
                break;

            case "SWITCHING":
                logger.info("Task {} was in SWITCHING state (failover in progress), resuming incremental sync with skipFullMigration", taskId);
                taskProcessService.startMigrationAgentThread(taskMessage, true);
                break;

            default:
                logger.warn("Unknown status {} for task {}, restarting from beginning", status, taskId);
                taskProcessService.startMigrationAgentThread(taskMessage, false);
        }
    }

    private void recoverFullOnlyTask(RecoveryTask recoveryTask, TaskMessage taskMessage) {
        String taskId = recoveryTask.getTaskId();
        String status = recoveryTask.getStatus();
        int progress = recoveryTask.getProgress();

        switch (status) {
            case "STARTING":
            case "FULL_MIGRATING":
                logger.info("Full-only task {} was in {} state (progress: {}%), resuming migration",
                    taskId, status, progress);
                try {
                    taskProcessService.startMigrationForTask(taskId, taskMessage);
                    sendStatus(taskId, "STARTING", "Task recovered, resuming migration", progress);
                } catch (Exception e) {
                    logger.error("Error resuming full migration for task: {}", taskId, e);
                    sendStatus(taskId, "FAILED", "Failed to resume migration: " + e.getMessage(), progress);
                }
                break;

            case "FULL_COMPLETED":
                logger.info("Full-only task {} was already completed", taskId);
                sendStatus(taskId, "COMPLETED", "Task already completed", 100);
                break;

            default:
                logger.warn("Unknown status {} for full-only task {}, treating as new task", status, taskId);
                try {
                    taskProcessService.startMigrationForTask(taskId, taskMessage);
                    sendStatus(taskId, "STARTING", "Task recovered, starting migration", 0);
                } catch (Exception e) {
                    logger.error("Error starting migration for task: {}", taskId, e);
                    sendStatus(taskId, "FAILED", "Failed to start migration: " + e.getMessage(), 0);
                }
        }
    }

    int getProgressFromDatabase(String taskId) {
        String progressDbUrl = "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
        try (Connection conn = DriverManager.getConnection(progressDbUrl, config.getH2MetadataUser(), config.getH2MetadataPassword());
             PreparedStatement stmt = conn.prepareStatement("SELECT progress FROM task_progress WHERE task_id = ?")) {

            stmt.setString(1, taskId);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getInt("progress");
            }
        } catch (Exception e) {
            logger.debug("Error getting progress from database for task: {}", taskId, e);
        }

        return 0;
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
