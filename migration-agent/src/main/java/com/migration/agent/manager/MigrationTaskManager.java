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
    
    /** 全量迁移进度轮询间隔，与 AbstractTaskExecutor 的 monitor.progress.interval.ms 默认值保持一致。 */
    private static final long PROGRESS_POLL_INTERVAL_SECONDS = 3;

    private final String jarPath;
    private final String taskId;
    private final KafkaProducerService kafkaProducer;
    private final String metadataDbUrl;
    private final String metadataDbUser;
    private final String metadataDbPassword;
    private final int totalTables;

    private Process process;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService monitorExecutor;
    private Thread outputThread;
    private volatile int completedTables = 0;

    public MigrationTaskManager(String jarPath, String taskId,
                                 KafkaProducerService kafkaProducer,
                                 String metadataDbUrl, String metadataDbUser, String metadataDbPassword,
                                 int totalTables) {
        this.jarPath = jarPath;
        this.taskId = taskId;
        this.kafkaProducer = kafkaProducer;
        this.metadataDbUrl = "jdbc:h2:./files/" + taskId + "/metadata;MODE=MySQL;AUTO_SERVER=TRUE";
        this.metadataDbUser = metadataDbUser;
        this.metadataDbPassword = metadataDbPassword;
        this.totalTables = totalTables;
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
            // H2 AUTO_SERVER 默认按 InetAddress.getLocalHost() 绑定监听地址；当主机名解析到
            // 局域网 IP（而非回环）时，agent 与本进程间纯本机的进度轮询连接会因局域网连通性
            // 抖动（WiFi/VPN/防火墙）而挂起数十秒才报 "Connection is broken"。强制绑定回环
            // 地址，使跨进程进度轮询不再依赖局域网连通性。
            "-Dh2.bindAddress=127.0.0.1",
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

            try {
                while ((line = reader.readLine()) != null && running.get()) {
                    logger.info("[Migration-{}] {}", taskId, line);
                }
            } catch (Exception e) {
                logger.error("Error reading migration output for task {}", taskId, e);
            }
        });

        outputThread.start();
    }

    /**
     * 定时轮询迁移进度并检测进程退出。进度来自 migration-full 实际写入的
     * migration_progress H2 表（与 AbstractTaskExecutor.startFullMigrationMonitor
     * 读取的是同一张表/同一份数据），不再依赖已废弃、从未被匹配过的 stdout
     * "Progress: NN%" 正则和从未被写入的 task_progress 表。
     */
    private void startProgressMonitor() {
        monitorExecutor = Executors.newSingleThreadScheduledExecutor();
        monitorExecutor.scheduleAtFixedRate(() -> {
            try {
                pollAndReportProgress();
            } catch (Exception e) {
                logger.debug("Error polling migration progress for task {}: {}", taskId, e.getMessage());
            }

            if (running.get() && !process.isAlive()) {
                try {
                    int exitCode = process.exitValue();

                    if (exitCode == 0) {
                        logger.info("Migration task {} completed successfully", taskId);
                        // 仅全量任务的"任务结束"即全量完成：库级同步在此把源库 trigger/event 复制到目标库
                        com.migration.agent.service.DbObjectsSyncService.syncTriggersAndEventsAtTaskEnd(taskId);
                        sendStatus("FULL_COMPLETED", "全量同步完成", 100,
                                totalTables, totalTables, null, 100, 0L, 0L);
                    } else {
                        logger.error("Migration task {} failed with exit code: {}", taskId, exitCode);
                        int lastProgress = totalTables > 0 ? (completedTables * 100) / totalTables : 0;
                        sendStatus("FAILED", "Migration failed with exit code: " + exitCode, lastProgress);
                    }

                    // 运行在 monitorExecutor 自己的线程里：不能调用会阻塞等待该
                    // executor 终止的 stop()（自等待死锁，直到 5s 超时才继续）。
                    // shutdown() 本身足以阻止该周期任务的下一次调度。
                    running.set(false);
                    monitorExecutor.shutdown();
                    if (outputThread != null) {
                        outputThread.interrupt();
                    }
                } catch (Exception e) {
                    logger.error("Error handling migration completion for task {}", taskId, e);
                }
            }
        }, PROGRESS_POLL_INTERVAL_SECONDS, PROGRESS_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /** 读取 migration_progress 表并上报真实的表级进度（完成表数/当前表行进度）。 */
    private void pollAndReportProgress() {
        String progressDbUrl = "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
        try (Connection conn = DriverManager.getConnection(progressDbUrl, metadataDbUser, metadataDbPassword)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM migration_progress WHERE status = 'COMPLETED'")) {
                if (rs.next()) {
                    completedTables = rs.getInt(1);
                }
            }

            String currentTable = null;
            long currentTableRows = 0;
            long currentTableTotalRows = 0;
            int currentTableProgress = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT table_name, total_rows, migrated_rows FROM migration_progress WHERE status = 'IN_PROGRESS' LIMIT 1")) {
                if (rs.next()) {
                    currentTable = rs.getString("table_name");
                    currentTableTotalRows = rs.getLong("total_rows");
                    currentTableRows = rs.getLong("migrated_rows");
                    if (currentTableTotalRows > 0) {
                        currentTableProgress = (int) ((currentTableRows * 100) / currentTableTotalRows);
                    }
                }
            }

            int overallProgress = totalTables > 0 ? (completedTables * 100) / totalTables : 0;
            sendStatus("FULL_MIGRATING", "全量同步中", overallProgress,
                    totalTables, completedTables, currentTable, currentTableProgress,
                    currentTableRows, currentTableTotalRows);
        } catch (SQLException e) {
            logger.debug("[{}] 读取迁移进度失败: {}", taskId, e.getMessage());
        }
    }

    private void sendStatus(String status, String message, int progress) {
        sendStatus(status, message, progress, null, null, null, null, null, null);
    }

    private void sendStatus(String status, String message, int progress,
                             Integer totalTablesArg, Integer completedTablesArg, String currentTable,
                             Integer currentTableProgress, Long currentTableRows, Long currentTableTotalRows) {
        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus(status);
        statusMessage.setMessage(message);
        statusMessage.setProgress(progress);
        statusMessage.setTotalTables(totalTablesArg);
        statusMessage.setCompletedTables(completedTablesArg);
        statusMessage.setCurrentTable(currentTable);
        statusMessage.setCurrentTableProgress(currentTableProgress);
        statusMessage.setCurrentTableRows(currentTableRows);
        statusMessage.setCurrentTableTotalRows(currentTableTotalRows);

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