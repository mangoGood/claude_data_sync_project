package com.migration.agent.service;

import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStateInfo;
import com.migration.agent.model.TaskStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FailoverService {
    private static final Logger logger = LoggerFactory.getLogger(FailoverService.class);

    private final Set<String> failoverInProgress = ConcurrentHashMap.newKeySet();

    private final TaskProcessService taskProcessService;
    private final ConfigService configService;
    private final TaskStateService taskStateService;
    private final KafkaProducerService kafkaProducer;

    public FailoverService(TaskProcessService taskProcessService, ConfigService configService,
                           TaskStateService taskStateService, KafkaProducerService kafkaProducer) {
        this.taskProcessService = taskProcessService;
        this.configService = configService;
        this.taskStateService = taskStateService;
        this.kafkaProducer = kafkaProducer;
    }

    public boolean isFailoverInProgress(String taskId) {
        return failoverInProgress.contains(taskId);
    }

    public void handleFailoverFromKafka(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling failover message (Kafka) for task: {}", taskId);

        if (!failoverInProgress.add(taskId)) {
            logger.warn("Failover already in progress for task: {}, ignoring duplicate Kafka message", taskId);
            return;
        }

        try {
            sendStatus(taskId, "SWITCHING", "Failover in progress, stopping current processes", 100);

            taskProcessService.stopMigrationAgentThread(taskId);
            taskProcessService.stopTaskById(taskId);
            logger.info("All processes stopped for failover task: {}", taskId);

            try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.interrupted(); }
            logger.info("Waited 3s for processes to fully terminate for failover task: {}", taskId);

            performFailover(taskMessage, "SWITCHING");

        } catch (Exception e) {
            logger.error("Error handling failover message for task: {}", taskId, e);
            sendStatus(taskId, "FAILED", "Error during failover: " + e.getMessage(), 0);
        } finally {
            failoverInProgress.remove(taskId);
        }
    }

    public void handleFailoverDirect(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("=== DIRECT FAILOVER (HTTP API) for task: {} ===", taskId);

        if (!failoverInProgress.add(taskId)) {
            logger.warn("Failover already in progress for task: {}, ignoring duplicate request", taskId);
            return;
        }

        try {
            sendStatus(taskId, "SWITCHING", "Failover in progress, stopping current processes", 100);

            taskProcessService.stopMigrationAgentThread(taskId);
            taskProcessService.stopTaskById(taskId);
            logger.info("All processes stopped for failover task: {}", taskId);

            taskStateService.deleteTaskState(taskId);
            logger.info("Old task state from H2 deleted for failover task: {}", taskId);

            cleanFailoverFiles(taskId);

            configService.updateConfig(taskMessage);
            logger.info("Config updated for failover task: {} with swapped connections", taskId);

            clearBinlogPositionInConfig(taskId);

            TaskStateInfo stateInfo = buildTaskStateInfo(taskMessage, "INCREMENT_RUNNING", 100);
            taskStateService.saveTaskState(stateInfo);
            logger.info("Saved H2 state for failover task: {} with status INCREMENT_RUNNING", taskId);

            taskProcessService.startMigrationAgentThread(taskMessage, true);

            logger.info("Failover task {} restarted with skipFullMigration=true", taskId);
            sendStatus(taskId, "SWITCHING", "Failover processes starting, skipping full migration", 100);

            new Thread(() -> {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                failoverInProgress.remove(taskId);
                logger.info("Failover initialization period completed for task: {}", taskId);
            }, "FailoverInitGuard-" + taskId).start();

        } catch (Exception e) {
            logger.error("Error handling direct failover for task: {}", taskId, e);
            sendStatus(taskId, "FAILED", "Error during failover: " + e.getMessage(), 0);
            failoverInProgress.remove(taskId);
        }
    }

    private void performFailover(TaskMessage taskMessage, String targetStatus) throws Exception {
        String taskId = taskMessage.getTaskId();

        taskStateService.deleteTaskState(taskId);
        logger.info("Old task state from H2 deleted for failover task: {}", taskId);

        cleanFailoverFiles(taskId);

        configService.updateConfig(taskMessage);
        logger.info("Config updated for failover task: {} with swapped connections", taskId);

        clearBinlogPositionInConfig(taskId);

        TaskStateInfo stateInfo = buildTaskStateInfo(taskMessage, targetStatus, 100);
        taskStateService.saveTaskState(stateInfo);
        logger.info("Saved H2 state for failover task: {} with status {}", taskId, targetStatus);

        taskProcessService.startMigrationAgentThread(taskMessage, true);

        logger.info("Failover task {} restarted with skipFullMigration=true, capture/extractor/increment will start", taskId);
    }

    private void cleanFailoverFiles(String taskId) {
        deleteFileIfExists("files/" + taskId + "/checkpoint/checkpoint");
        deleteFileIfExists("files/" + taskId + "/checkpoint/seqno_checkpoint.json");

        cleanDirectory("files/" + taskId + "/thl_output");
        cleanDirectory("files/" + taskId + "/binlog_output");

        deleteFileIfExists("files/" + taskId + "/migration_progress.mv.db");
        deleteFileIfExists("files/" + taskId + "/migration_progress.trace.db");

        String[] checkpointDbFiles = {
            "files/" + taskId + "/checkpoint/checkpoint.mv.db",
            "files/" + taskId + "/checkpoint/checkpoint.trace.db",
            "files/" + taskId + "/checkpoint/checkpoint.lock.db",
            "files/" + taskId + "/checkpoint/increment_checkpoint.mv.db",
            "files/" + taskId + "/checkpoint/increment_checkpoint.trace.db",
            "files/" + taskId + "/checkpoint/increment_checkpoint.lock.db",
            "files/" + taskId + "/checkpoint/.increment_progress"
        };
        for (String dbFile : checkpointDbFiles) {
            deleteFileIfExists(dbFile);
        }
        logger.info("All checkpoint DB files deleted for failover task: {}", taskId);
    }

    private void clearBinlogPositionInConfig(String taskId) {
        File configFile = new File("files/" + taskId + "/config.properties");
        if (!configFile.exists()) {
            return;
        }

        try {
            Properties configProps = new Properties();
            try (InputStream cis = new FileInputStream(configFile)) {
                configProps.load(cis);
            }
            configProps.remove("capture.binlog.file");
            configProps.remove("capture.binlog.position");
            configProps.remove("checkpoint.binlog.file");
            configProps.remove("checkpoint.binlog.position");
            try (OutputStream cos = new FileOutputStream(configFile)) {
                configProps.store(cos, "Updated for failover - binlog position cleared");
            }
            logger.info("Cleared old binlog position in config for failover task: {}", taskId);
        } catch (Exception e) {
            logger.error("Error clearing binlog position in config for task: {}", taskId, e);
        }
    }

    private TaskStateInfo buildTaskStateInfo(TaskMessage taskMessage, String status, int progress) {
        TaskStateInfo stateInfo = new TaskStateInfo(taskMessage.getTaskId());
        stateInfo.setTaskName(taskMessage.getTaskName());
        stateInfo.setUserId(taskMessage.getUserId());
        stateInfo.setMigrationMode(taskMessage.getMigrationMode());
        stateInfo.setSourceConnection(taskMessage.getSourceConnection());
        stateInfo.setTargetConnection(taskMessage.getTargetConnection());
        stateInfo.setSourceType(taskMessage.getSourceType() != null ? taskMessage.getSourceType() : "mysql");
        stateInfo.setTargetType(taskMessage.getTargetType() != null ? taskMessage.getTargetType() : "mysql");
        stateInfo.setStatus(status);
        stateInfo.setProgress(progress);
        stateInfo.setCreatedAt(taskMessage.getCreatedAt() != null ? taskMessage.getCreatedAt() : java.time.LocalDateTime.now());
        return stateInfo;
    }

    private void deleteFileIfExists(String path) {
        File file = new File(path);
        if (file.exists()) {
            boolean deleted = file.delete();
            logger.info("Deleted file: {}, success: {}", file.getAbsolutePath(), deleted);
        }
    }

    private void cleanDirectory(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
        logger.info("Cleaned directory: {}", dirPath);
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
