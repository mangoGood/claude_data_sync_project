package com.migration.agent.service;

import com.migration.agent.manager.MigrationTaskManager;
import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.resilience.ProcessGuard;
import com.migration.agent.thread.MigrationAgentThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TaskProcessService {
    private static final Logger logger = LoggerFactory.getLogger(TaskProcessService.class);

    private final ConcurrentHashMap<String, ProcessGuard> captureGuards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProcessManager> extractManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProcessManager> incrementManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MigrationTaskManager> migrationTaskManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MigrationAgentThread> migrationAgentThreads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Thread> migrationAgentThreadWrappers = new ConcurrentHashMap<>();
    private final Set<String> pausedTasks = ConcurrentHashMap.newKeySet();

    private final KafkaProducerService kafkaProducer;
    private final TaskStateService taskStateService;
    private final AgentConfig config;

    public TaskProcessService(KafkaProducerService kafkaProducer, TaskStateService taskStateService, AgentConfig config) {
        this.kafkaProducer = kafkaProducer;
        this.taskStateService = taskStateService;
        this.config = config;
    }

    public void startMigrationAgentThread(TaskMessage taskMessage, boolean skipFullMigration) {
        String taskId = taskMessage.getTaskId();

        MigrationAgentThread agentThread = new MigrationAgentThread(taskMessage, kafkaProducer, taskStateService, skipFullMigration, config);
        migrationAgentThreads.put(taskId, agentThread);

        Thread threadWrapper = new Thread(agentThread, "MigrationAgentThread-" + taskId);
        threadWrapper.setDaemon(true);
        migrationAgentThreadWrappers.put(taskId, threadWrapper);
        threadWrapper.start();

        logger.info("MigrationAgentThread started for task: {}, skipFullMigration: {}", taskId, skipFullMigration);
    }

    public void stopMigrationAgentThread(String taskId) {
        MigrationAgentThread agentThread = migrationAgentThreads.remove(taskId);
        Thread threadWrapper = migrationAgentThreadWrappers.remove(taskId);

        if (agentThread != null) {
            try {
                agentThread.stopAndInterrupt(threadWrapper);
                logger.info("MigrationAgentThread stopped for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error stopping MigrationAgentThread for task: {}", taskId, e);
            }
        } else if (threadWrapper != null) {
            try {
                threadWrapper.interrupt();
                logger.info("MigrationAgentThread wrapper interrupted for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error interrupting thread wrapper for task: {}", taskId, e);
            }
        }
    }

    public void stopTaskById(String taskId) {
        if (migrationTaskManagers.containsKey(taskId)) {
            MigrationTaskManager manager = migrationTaskManagers.remove(taskId);
            try {
                manager.stop();
                logger.info("Migration task stopped for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error stopping migration task for task: {}", taskId, e);
            }
        }

        if (captureGuards.containsKey(taskId)) {
            ProcessGuard guard = captureGuards.remove(taskId);
            try {
                guard.stop();
                logger.info("Capture process stopped for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error stopping capture process for task: {}", taskId, e);
            }
        }

        if (extractManagers.containsKey(taskId)) {
            ProcessManager manager = extractManagers.remove(taskId);
            try {
                manager.stop();
                logger.info("Extract process stopped for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error stopping extract process for task: {}", taskId, e);
            }
        }

        if (incrementManagers.containsKey(taskId)) {
            ProcessManager manager = incrementManagers.remove(taskId);
            try {
                manager.stop();
                logger.info("Increment process stopped for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error stopping increment process for task: {}", taskId, e);
            }
        }
    }

    public void stopAll() {
        for (Map.Entry<String, MigrationAgentThread> entry : migrationAgentThreads.entrySet()) {
            try {
                logger.info("Stopping migration agent thread for task: {}", entry.getKey());
                entry.getValue().stop();
            } catch (Exception e) {
                logger.error("Error stopping migration agent thread", e);
            }
        }
        migrationAgentThreads.clear();

        for (Map.Entry<String, Thread> entry : migrationAgentThreadWrappers.entrySet()) {
            try {
                entry.getValue().interrupt();
            } catch (Exception e) {
                logger.error("Error interrupting thread wrapper", e);
            }
        }
        migrationAgentThreadWrappers.clear();

        for (Map.Entry<String, MigrationTaskManager> entry : migrationTaskManagers.entrySet()) {
            try {
                logger.info("Stopping migration task: {}", entry.getKey());
                entry.getValue().stop();
            } catch (Exception e) {
                logger.error("Error stopping migration task", e);
            }
        }
        migrationTaskManagers.clear();

        for (Map.Entry<String, ProcessGuard> entry : captureGuards.entrySet()) {
            try {
                logger.info("Stopping capture process for task: {}", entry.getKey());
                entry.getValue().stop();
            } catch (Exception e) {
                logger.error("Error stopping capture process", e);
            }
        }
        captureGuards.clear();

        for (Map.Entry<String, ProcessManager> entry : extractManagers.entrySet()) {
            try {
                logger.info("Stopping extract process for task: {}", entry.getKey());
                entry.getValue().stop();
            } catch (Exception e) {
                logger.error("Error stopping extract process", e);
            }
        }
        extractManagers.clear();

        for (Map.Entry<String, ProcessManager> entry : incrementManagers.entrySet()) {
            try {
                logger.info("Stopping increment process for task: {}", entry.getKey());
                entry.getValue().stop();
            } catch (Exception e) {
                logger.error("Error stopping increment process", e);
            }
        }
        incrementManagers.clear();

        pausedTasks.clear();
    }

    public void startCaptureForTask(String taskId) throws Exception {
        if (captureGuards.containsKey(taskId)) {
            logger.warn("Capture process already running for task: {}", taskId);
            return;
        }

        ProcessGuard captureGuard = new ProcessGuard("capture", taskId, config, kafkaProducer,
            () -> {
                ProcessManager pm = new ProcessManager(config.getCaptureJarPath(), "CaptureMain-" + taskId);
                pm.setTaskId(taskId);
                return pm;
            });

        boolean started = captureGuard.startAndGuard();
        if (!started) {
            throw new RuntimeException("Failed to start capture process for task: " + taskId);
        }

        captureGuards.put(taskId, captureGuard);
        sendStatus(taskId, "CAPTURE_STARTED", "Capture process started for task: " + taskId, 0);
        logger.info("Capture process started for task: {}", taskId);
    }

    public void startMigrationForTask(String taskId, TaskMessage taskMessage) throws Exception {
        if (migrationTaskManagers.containsKey(taskId)) {
            MigrationTaskManager existing = migrationTaskManagers.get(taskId);
            if (existing.isRunning()) {
                logger.warn("Migration task already running for task: {}", taskId);
                return;
            }
        }

        logger.info("Starting migration-full process for task: {}", taskId);

        int totalTables = calculateTotalTables(taskMessage.getSyncObjects());

        MigrationTaskManager migrationTaskManager = new MigrationTaskManager(
            config.getMigrationFullJarPath(), taskId, kafkaProducer,
            null, config.getH2MetadataUser(), config.getH2MetadataPassword(), totalTables
        );

        migrationTaskManagers.put(taskId, migrationTaskManager);

        migrationTaskManager.start();
        sendStatus(taskId, "MIGRATION_STARTED", "Full migration started for task: " + taskId, 0);

        logger.info("Migration task started for: {}", taskId);
    }

    private int calculateTotalTables(Map<String, Object> syncObjects) {
        if (syncObjects == null || syncObjects.isEmpty()) return 0;
        int count = 0;
        for (Map.Entry<String, Object> entry : syncObjects.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof java.util.List) {
                count += ((java.util.List<?>) value).size();
            } else if (value instanceof Map) {
                Map<?, ?> dbValue = (Map<?, ?>) value;
                Object tablesObj = dbValue.get("tables");
                if (tablesObj instanceof java.util.List) {
                    count += ((java.util.List<?>) tablesObj).size();
                }
            }
        }
        return count;
    }

    public void monitorCaptureProcesses() {
        for (Map.Entry<String, ProcessGuard> entry : captureGuards.entrySet()) {
            String taskId = entry.getKey();

            if (pausedTasks.contains(taskId)) {
                logger.debug("Skipping monitoring for paused task: {}", taskId);
                continue;
            }

            ProcessGuard guard = entry.getValue();
            if (!guard.isGuarding() && !guard.isRunning()) {
                logger.warn("Capture process for task {} is not running and not guarded, attempting restart", taskId);
                try {
                    guard.startAndGuard();
                } catch (Exception e) {
                    if (!pausedTasks.contains(taskId)) {
                        logger.error("Error restarting capture process for task: {}", taskId, e);
                    }
                }
            }
        }
    }

    public void addPausedTask(String taskId) {
        pausedTasks.add(taskId);
    }

    public void removePausedTask(String taskId) {
        pausedTasks.remove(taskId);
    }

    public boolean isTaskPaused(String taskId) {
        return pausedTasks.contains(taskId);
    }

    public boolean hasMigrationAgentThread(String taskId) {
        return migrationAgentThreads.containsKey(taskId);
    }

    public Map<String, Object> getAgentStatus() {
        Map<String, Object> status = new java.util.HashMap<>();
        status.put("activeTasks", migrationAgentThreads.size());
        status.put("pausedTasks", pausedTasks.size());
        status.put("captureProcesses", captureGuards.size());
        status.put("extractProcesses", extractManagers.size());
        status.put("incrementProcesses", incrementManagers.size());

        java.util.List<Map<String, String>> taskList = new java.util.ArrayList<>();
        for (Map.Entry<String, MigrationAgentThread> entry : migrationAgentThreads.entrySet()) {
            Map<String, String> taskInfo = new java.util.HashMap<>();
            taskInfo.put("taskId", entry.getKey());
            taskInfo.put("running", String.valueOf(entry.getValue().isRunning()));
            taskList.add(taskInfo);
        }
        status.put("tasks", taskList);

        return status;
    }

    private void sendStatus(String taskId, String status, String message, int progress) {
        if (pausedTasks.contains(taskId)) {
            logger.debug("Skipping status report for paused task: {}", taskId);
            return;
        }

        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus(status);
        statusMessage.setMessage(message);
        statusMessage.setProgress(progress);

        kafkaProducer.sendStatus(statusMessage);
    }
}
