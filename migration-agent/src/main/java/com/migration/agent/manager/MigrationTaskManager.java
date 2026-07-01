package com.migration.agent.manager;

import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MigrationTaskManager {
    private static final Logger logger = LoggerFactory.getLogger(MigrationTaskManager.class);
    
    private final String jarPath;
    private final String taskId;
    private final KafkaProducerService kafkaProducer;
    private final String metadataDbUrl;
    private final String metadataDbUser;
    private final String metadataDbPassword;
    
    private Process process;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService monitorExecutor;
    private Thread outputThread;
    
    public MigrationTaskManager(String jarPath, String taskId, 
                                 KafkaProducerService kafkaProducer,
                                 String metadataDbUrl, String metadataDbUser, String metadataDbPassword) {
        this.jarPath = jarPath;
        this.taskId = taskId;
        this.kafkaProducer = kafkaProducer;
        this.metadataDbUrl = "jdbc:h2:./files/" + taskId + "/metadata;MODE=MySQL;AUTO_SERVER=TRUE";
        this.metadataDbUser = metadataDbUser;
        this.metadataDbPassword = metadataDbPassword;
    }
    
    public void start() throws Exception {
        if (running.get()) {
            logger.warn("Migration task {} is already running", taskId);
            return;
        }
        
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new RuntimeException("Jar file not found: " + jarPath);
        }
        
        ProcessBuilder pb = new ProcessBuilder(
            "java", 
            "-Dtask.id=" + taskId,
            "-Dlogback.configurationFile=files/" + taskId + "/logback.xml",
            "-jar", jarPath,
            "--task-id", taskId
        );
        pb.redirectErrorStream(true);
        
        logger.info("Starting migration task {} with jar: {}", taskId, jarPath);
        process = pb.start();
        running.set(true);
        
        startOutputThread();
        startProgressMonitor();
        
        sendStatus("FULL_MIGRATING", "全量同步中", 0);
        logger.info("Migration task {} started with PID: {}", taskId, getPid());
    }
    
    private void startOutputThread() {
        outputThread = new Thread(() -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            Pattern progressPattern = Pattern.compile("Progress:\\s*(\\d+)%");
            
            try {
                while ((line = reader.readLine()) != null && running.get()) {
                    logger.info("[Migration-{}] {}", taskId, line);
                    
                    Matcher matcher = progressPattern.matcher(line);
                    if (matcher.find()) {
                        int progress = Integer.parseInt(matcher.group(1));
                        saveProgressToDatabase(progress);
                        sendStatus("FULL_MIGRATING", "全量同步中", progress);
                    }
                }
            } catch (Exception e) {
                logger.error("Error reading migration output for task {}", taskId, e);
            }
        });
        
        outputThread.start();
    }
    
    private void startProgressMonitor() {
        monitorExecutor = Executors.newSingleThreadScheduledExecutor();
        monitorExecutor.scheduleAtFixedRate(() -> {
            if (!process.isAlive()) {
                try {
                    int exitCode = process.exitValue();
                    running.set(false);
                    
                    if (exitCode == 0) {
                        logger.info("Migration task {} completed successfully", taskId);
                        saveProgressToDatabase(100);
                        sendStatus("FULL_COMPLETED", "全量同步完成", 100);
                    } else {
                        logger.error("Migration task {} failed with exit code: {}", taskId, exitCode);
                        sendStatus("FAILED", "Migration failed with exit code: " + exitCode, getProgressFromDatabase());
                    }
                    
                    stop();
                } catch (Exception e) {
                    logger.error("Error handling migration completion for task {}", taskId, e);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }
    
    private void saveProgressToDatabase(int progress) {
        String progressDbUrl = "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
        try (Connection conn = DriverManager.getConnection(progressDbUrl, metadataDbUser, metadataDbPassword)) {
            String sql = "MERGE INTO task_progress (task_id, progress, updated_at) " +
                        "KEY (task_id) VALUES (?, ?, CURRENT_TIMESTAMP)";
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, taskId);
                stmt.setInt(2, progress);
                stmt.executeUpdate();
                logger.debug("Saved progress {} for task {}", progress, taskId);
            }
        } catch (SQLException e) {
            logger.error("Error saving progress to database for task {}", taskId, e);
        }
    }
    
    private int getProgressFromDatabase() {
        String progressDbUrl = "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
        try (Connection conn = DriverManager.getConnection(progressDbUrl, metadataDbUser, metadataDbPassword)) {
            String sql = "SELECT progress FROM task_progress WHERE task_id = ?";
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, taskId);
                ResultSet rs = stmt.executeQuery();
                
                if (rs.next()) {
                    return rs.getInt("progress");
                }
            }
        } catch (SQLException e) {
            logger.error("Error getting progress from database for task {}", taskId, e);
        }
        
        return 0;
    }
    
    private void sendStatus(String status, String message, int progress) {
        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus(status);
        statusMessage.setMessage(message);
        statusMessage.setProgress(progress);
        
        kafkaProducer.sendStatus(statusMessage);
    }
    
    public void stop() {
        if (!running.get()) {
            return;
        }
        
        running.set(false);
        
        if (monitorExecutor != null) {
            monitorExecutor.shutdown();
            try {
                if (!monitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    monitorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                monitorExecutor.shutdownNow();
            }
        }
        
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                logger.error("Error stopping migration process for task {}", taskId, e);
            }
        }
        
        if (outputThread != null) {
            outputThread.interrupt();
        }
        
        logger.info("Migration task {} stopped", taskId);
    }
    
    public boolean isRunning() {
        return running.get() && process != null && process.isAlive();
    }
    
    public long getPid() {
        if (process != null) {
            try {
                return process.pid();
            } catch (UnsupportedOperationException e) {
                return -1;
            }
        }
        return -1;
    }
}