package com.migration.agent.thread;

import com.migration.agent.checkpoint.CheckpointManager;
import com.migration.agent.checkpoint.CheckpointManager.BinlogPositionInfo;
import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStateInfo;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.resilience.ProcessGuard;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import com.migration.agent.service.MetricsPersistenceService;
import com.migration.agent.service.TaskStateService;
import com.migration.agent.util.SyncErrorCodeMapper;
import com.migration.common.MdcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任务执行器基类，封装所有任务类型共享的字段和方法。
 * 子类通过实现 {@link #doRun()} 方法定义具体执行流程。
 */
public abstract class AbstractTaskExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTaskExecutor.class);

    protected final TaskMessage taskMessage;
    protected final KafkaProducerService kafkaProducer;
    protected final TaskStateService taskStateService;
    protected final AgentConfig config;
    protected final String taskId;
    protected final AtomicBoolean stopped;
    protected final boolean skipFullMigration;
    protected final String migrationMode;
    protected final String sourceType;
    protected final String targetType;
    protected final String runningStatus;

    protected ProcessManager fullProcess;
    protected ProcessManager extractProcess;
    protected ProcessGuard captureGuard;
    protected ProcessGuard extractGuard;
    protected ProcessGuard incrementGuard;

    protected Thread fullMigrationMonitorThread;

    protected int totalTables = 0;
    protected volatile int completedTables = 0;
    protected volatile String lastSuccessfulStatus = null;

    protected long lastMetricsReportTime = 0;
    protected static final long METRICS_REPORT_INTERVAL_MS = 30000;

    public AbstractTaskExecutor(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                                TaskStateService taskStateService, boolean skipFullMigration, AgentConfig config) {
        this.taskMessage = taskMessage;
        this.kafkaProducer = kafkaProducer;
        this.taskStateService = taskStateService;
        this.config = config;
        this.taskId = taskMessage.getTaskId();
        this.stopped = new AtomicBoolean(false);
        this.skipFullMigration = skipFullMigration;
        this.migrationMode = taskMessage.getMigrationMode();
        this.sourceType = taskMessage.getSourceType() != null ? taskMessage.getSourceType() : "mysql";
        this.targetType = taskMessage.getTargetType() != null ? taskMessage.getTargetType() : "mysql";
        this.runningStatus = getRunningStatus();
        this.totalTables = calculateTotalTables(taskMessage.getSyncObjects());
    }

    /** 子类返回运行中状态名，如 INCREMENT_RUNNING / SUBSCRIBE_RUNNING */
    protected abstract String getRunningStatus();

    /** 子类实现具体执行流程 */
    protected abstract void doRun() throws Exception;

    /** 子类实现进程健康检查 */
    protected abstract boolean checkProcessHealth();

    /** 子类实现指标上报 */
    protected abstract void sendPeriodicMetricsUpdate(MetricsService.TaskMetrics taskMetrics);

    @Override
    public final void run() {
        String threadName = "TaskExecutor-" + taskId;
        Thread.currentThread().setName(threadName);
        MdcUtil.setTaskId(taskId);
        MdcUtil.setProcessName("agent");

        logger.info("[{}] 开始执行任务, taskType={}, skipFullMigration={}, sourceType={}",
                threadName, taskMessage.getTaskType(), skipFullMigration, sourceType);

        MetricsService.TaskMetrics taskMetrics = MetricsService.getInstance().getOrCreateTaskMetrics(taskId);

        try {
            doRun();

            // 进入持续监控循环（由子类控制是否进入）
            monitorLoop(taskMetrics);
        } catch (Exception e) {
            logger.error("[{}] 任务执行异常", threadName, e);
            saveTaskStateOnFailure();
            String errorCode = SyncErrorCodeMapper.mapExceptionToErrorCode(e, "agent");
            String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            sendFailedStatus(errorCode, "任务执行异常: " + errorMsg);
        } finally {
            stopAllProcesses();
            logger.info("[{}] 任务线程结束", threadName);
            MdcUtil.clear();
        }
    }

    /** 默认监控循环，子类可覆盖 */
    protected void monitorLoop(MetricsService.TaskMetrics taskMetrics) {
        String threadName = "TaskExecutor-" + taskId;

        while (!stopped.get()) {
            try {
                Thread.sleep(5000);

                if (!checkProcessHealth()) {
                    logger.error("[{}] 关键进程已停止且无法恢复，终止任务", threadName);
                    sendFailedStatus("E3003", "关键进程已停止且无法恢复，数据同步中断");
                    stopped.set(true);
                    break;
                }

                collectMetrics(taskMetrics);
                sendPeriodicMetricsUpdate(taskMetrics);
                persistMetrics(taskMetrics);

                if (checkIncrementErrorStatus()) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // ==================== 公共方法 ====================

    public void stop() {
        stopped.set(true);
        stopAllProcesses();
    }

    public void stopAndInterrupt(Thread thread) {
        stopped.set(true);
        stopAllProcesses();
        if (thread != null) {
            thread.interrupt();
        }
    }

    public boolean isRunning() {
        return !stopped.get();
    }

    public String getTaskId() {
        return taskId;
    }

    // ==================== Checkpoint 初始化 ====================

    protected boolean initCheckpoint() {
        String threadName = "TaskExecutor-" + taskId;
        logger.info("[{}] 初始化 checkpoint, sourceType={}", threadName, sourceType);

        if ("postgresql".equals(sourceType)) {
            return initPostgresCheckpoint(threadName);
        } else if ("oracle".equals(sourceType)) {
            return initOracleCheckpoint(threadName);
        } else {
            return initMysqlCheckpoint(threadName);
        }
    }

    protected boolean initMysqlCheckpoint(String threadName) {
        String checkpointDbPath = "./files/" + taskId + "/checkpoint/checkpoint";
        CheckpointManager checkpointManager = null;

        try {
            checkpointManager = new CheckpointManager(checkpointDbPath);
            BinlogPositionInfo existingCheckpoint = checkpointManager.loadCheckpoint();
            BinlogPositionInfo checkpointToUse = existingCheckpoint;

            if (existingCheckpoint != null) {
                logger.info("[{}] 发现已存在的 checkpoint: {}", threadName, existingCheckpoint);
            } else {
                logger.info("[{}] 未找到 checkpoint，从源数据库获取当前位点", threadName);
                String[] sourceCreds = getSourceCredentials(threadName);
                String sourceHost = sourceCreds[0];
                int sourcePort = Integer.parseInt(sourceCreds[1]);
                String sourceUser = sourceCreds[2];
                String sourcePassword = sourceCreds[3];

                if (sourceHost == null || sourceUser == null) {
                    logger.error("[{}] 源数据库配置为空", threadName);
                    sendStatus("FAILED", "源数据库配置为空", 0);
                    return false;
                }

                BinlogPositionInfo currentPosition = checkpointManager.getCurrentPositionFromSource(
                        sourceHost, sourcePort, sourceUser, sourcePassword);
                checkpointManager.saveCheckpoint(currentPosition);
                checkpointToUse = currentPosition;
                logger.info("[{}] 已记录当前位点作为 checkpoint: {}", threadName, currentPosition);
            }

            updateCheckpointConfig(checkpointToUse);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 初始化 checkpoint 失败", threadName, e);
            sendStatus("FAILED", "初始化 checkpoint 失败: " + e.getMessage(), 0);
            return false;
        } finally {
            if (checkpointManager != null) {
                checkpointManager.close();
            }
        }
    }

    protected boolean initPostgresCheckpoint(String threadName) {
        String checkpointDbPath = "./files/" + taskId + "/checkpoint/checkpoint";
        CheckpointManager checkpointManager = null;

        try {
            checkpointManager = new CheckpointManager(checkpointDbPath);
            BinlogPositionInfo existingCheckpoint = checkpointManager.loadCheckpoint();
            BinlogPositionInfo checkpointToUse = existingCheckpoint;

            if (existingCheckpoint != null) {
                logger.info("[{}] 发现已存在的 PostgreSQL checkpoint: {}", threadName, existingCheckpoint);
            } else {
                logger.info("[{}] 未找到 checkpoint，从 PostgreSQL 源数据库获取当前 WAL LSN", threadName);
                String[] sourceCreds = getSourceCredentials(threadName);
                String sourceHost = sourceCreds[0];
                int sourcePort = Integer.parseInt(sourceCreds[1]);
                String sourceUser = sourceCreds[2];
                String sourcePassword = sourceCreds[3];

                if (sourceHost == null || sourceUser == null) {
                    logger.error("[{}] 源数据库配置为空", threadName);
                    sendStatus("FAILED", "源数据库配置为空", 0);
                    return false;
                }

                BinlogPositionInfo currentPosition = checkpointManager.getCurrentPositionFromPostgres(
                        sourceHost, sourcePort, sourceUser, sourcePassword);
                checkpointManager.saveCheckpoint(currentPosition);
                checkpointToUse = currentPosition;
                logger.info("[{}] 已记录当前 PostgreSQL WAL LSN 作为 checkpoint: {}", threadName, currentPosition);
            }

            updateCheckpointConfig(checkpointToUse);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 初始化 PostgreSQL checkpoint 失败", threadName, e);
            sendStatus("FAILED", "初始化 PostgreSQL checkpoint 失败: " + e.getMessage(), 0);
            return false;
        } finally {
            if (checkpointManager != null) {
                checkpointManager.close();
            }
        }
    }

    protected boolean initOracleCheckpoint(String threadName) {
        String checkpointDbPath = "./files/" + taskId + "/checkpoint/checkpoint";
        CheckpointManager checkpointManager = null;

        try {
            checkpointManager = new CheckpointManager(checkpointDbPath);
            BinlogPositionInfo existingCheckpoint = checkpointManager.loadCheckpoint();
            BinlogPositionInfo checkpointToUse = existingCheckpoint;

            if (existingCheckpoint != null) {
                logger.info("[{}] 发现已存在的 Oracle checkpoint: {}", threadName, existingCheckpoint);
            } else {
                logger.info("[{}] 未找到 checkpoint，从 Oracle 源数据库获取当前 SCN", threadName);
                String[] sourceCreds = getSourceCredentials(threadName);
                String sourceHost = sourceCreds[0];
                int sourcePort = Integer.parseInt(sourceCreds[1]);
                String sourceUser = sourceCreds[2];
                String sourcePassword = sourceCreds[3];
                String sourceDatabase = getSourceDatabaseName(threadName);

                if (sourceHost == null || sourceUser == null) {
                    logger.error("[{}] 源数据库配置为空", threadName);
                    sendStatus("FAILED", "源数据库配置为空", 0);
                    return false;
                }

                BinlogPositionInfo currentPosition = checkpointManager.getCurrentPositionFromOracle(
                        sourceHost, sourcePort, sourceDatabase, sourceUser, sourcePassword);
                checkpointManager.saveCheckpoint(currentPosition);
                checkpointToUse = currentPosition;
                logger.info("[{}] 已记录当前 Oracle SCN 作为 checkpoint: {}", threadName, currentPosition);
            }

            updateCheckpointConfig(checkpointToUse);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 初始化 Oracle checkpoint 失败", threadName, e);
            sendStatus("FAILED", "初始化 Oracle checkpoint 失败: " + e.getMessage(), 0);
            return false;
        } finally {
            if (checkpointManager != null) {
                checkpointManager.close();
            }
        }
    }

    /**
     * 获取源数据库的名称（用于 Oracle 服务名/SID）。
     */
    protected String getSourceDatabaseName(String threadName) {
        TaskMessage.DatabaseConfig sourceConfig = taskMessage.getSource();
        if (sourceConfig != null && sourceConfig.getDatabase() != null && !sourceConfig.getDatabase().isEmpty()) {
            return sourceConfig.getDatabase();
        }
        if (taskMessage.getSourceDbName() != null && !taskMessage.getSourceDbName().isEmpty()) {
            return taskMessage.getSourceDbName();
        }
        // Oracle 默认服务名
        return "ORCL";
    }

    /** 返回 [host, port, user, password] */
    protected String[] getSourceCredentials(String threadName) {
        String sourceHost = null;
        String sourcePort;
        if ("postgresql".equals(sourceType)) {
            sourcePort = "5432";
        } else if ("oracle".equals(sourceType)) {
            sourcePort = "1521";
        } else {
            sourcePort = "3306";
        }
        String sourceUser = null;
        String sourcePassword = null;

        TaskMessage.DatabaseConfig sourceConfig = taskMessage.getSource();
        if (sourceConfig != null) {
            sourceHost = sourceConfig.getHost();
            sourcePort = String.valueOf(sourceConfig.getPort());
            sourceUser = sourceConfig.getUsername();
            sourcePassword = sourceConfig.getPassword();
        }

        if (sourceHost == null && taskMessage.getSourceConnection() != null) {
            try {
                com.migration.agent.util.ConnectionStringParser.ConnectionInfo sourceInfo =
                        com.migration.agent.util.ConnectionStringParser.parse(taskMessage.getSourceConnection());
                if (sourceInfo != null) {
                    sourceHost = sourceInfo.getHost();
                    sourcePort = String.valueOf(sourceInfo.getPort());
                    sourceUser = sourceInfo.getUsername();
                    sourcePassword = sourceInfo.getPassword();
                }
            } catch (Exception e) {
                logger.warn("[{}] 解析 sourceConnection 失败: {}", threadName, e.getMessage());
            }
        }

        if (sourceHost == null) {
            try {
                java.util.Properties props = new java.util.Properties();
                try (java.io.InputStream input = new java.io.FileInputStream("./files/" + taskId + "/config.properties")) {
                    props.load(input);
                }
                sourceHost = props.getProperty("source.db.host");
                sourcePort = props.getProperty("source.db.port", sourcePort);
                sourceUser = props.getProperty("source.db.username");
                sourcePassword = props.getProperty("source.db.password");
            } catch (Exception e) {
                logger.error("[{}] 读取配置文件失败: {}", threadName, e.getMessage());
            }
        }

        return new String[]{sourceHost, sourcePort, sourceUser, sourcePassword};
    }

    protected void updateCheckpointConfig(BinlogPositionInfo checkpoint) {
        if (checkpoint == null) return;
        if ("postgresql".equals(sourceType)) {
            updatePostgresCheckpointConfig(checkpoint);
        } else if ("oracle".equals(sourceType)) {
            updateOracleCheckpointConfig(checkpoint);
        } else {
            updateMysqlCheckpointConfig(checkpoint);
        }
    }

    protected void updateMysqlCheckpointConfig(BinlogPositionInfo checkpoint) {
        if (checkpoint.getFilename() == null || checkpoint.getFilename().isEmpty()) return;

        String threadName = "TaskExecutor-" + taskId;
        java.util.Properties props = new java.util.Properties();
        File configFile = new File("./files/" + taskId + "/config.properties");

        try {
            if (configFile.exists()) {
                try (java.io.InputStream input = new java.io.FileInputStream(configFile)) {
                    props.load(input);
                }
            }
            props.setProperty("checkpoint.binlog.file", checkpoint.getFilename());
            props.setProperty("checkpoint.binlog.position", String.valueOf(checkpoint.getPosition()));
            props.setProperty("capture.binlog.file", checkpoint.getFilename());
            props.setProperty("capture.binlog.position", String.valueOf(checkpoint.getPosition()));
            // GTID 集与 file+pos 同一时刻快照：capture 有 GTID 集时优先按 GTID 自动定位
            // （源端 HA 切换/binlog 文件名失效时 file+pos 作废而 GTID 仍有效）
            if (checkpoint.getGtid() != null && !checkpoint.getGtid().trim().isEmpty()) {
                String normalizedGtid = checkpoint.getGtid().replaceAll("\\s+", "");
                props.setProperty("checkpoint.gtid.set", normalizedGtid);
                props.setProperty("capture.gtid.set", normalizedGtid);
                logger.info("[{}] GTID 位点已写入配置文件: {}", threadName, normalizedGtid);
            }
            props.setProperty("extract.skip.before.checkpoint", "true");
            try (java.io.OutputStream output = new java.io.FileOutputStream(configFile)) {
                props.store(output, "Updated checkpoint for task: " + taskId);
            }
            logger.info("[{}] Checkpoint 位点已写入配置文件: {}:{}", threadName, checkpoint.getFilename(), checkpoint.getPosition());
        } catch (Exception e) {
            logger.error("[{}] 写入 checkpoint 配置失败", threadName, e);
        }
    }

    protected void updatePostgresCheckpointConfig(BinlogPositionInfo checkpoint) {
        String threadName = "TaskExecutor-" + taskId;
        java.util.Properties props = new java.util.Properties();
        File configFile = new File("./files/" + taskId + "/config.properties");

        try {
            if (configFile.exists()) {
                try (java.io.InputStream input = new java.io.FileInputStream(configFile)) {
                    props.load(input);
                }
            }
            if (checkpoint.getFilename() != null) {
                props.setProperty("checkpoint.wal.lsn", checkpoint.getFilename());
                props.setProperty("capture.wal.lsn", checkpoint.getFilename());
            }
            props.setProperty("checkpoint.wal.position", String.valueOf(checkpoint.getPosition()));
            props.setProperty("capture.wal.position", String.valueOf(checkpoint.getPosition()));
            props.setProperty("extract.skip.before.checkpoint", "true");
            try (java.io.OutputStream output = new java.io.FileOutputStream(configFile)) {
                props.store(output, "Updated PostgreSQL checkpoint for task: " + taskId);
            }
            logger.info("[{}] PostgreSQL Checkpoint 位点已写入配置文件: LSN={}", threadName, checkpoint.getFilename());
        } catch (Exception e) {
            logger.error("[{}] 写入 PostgreSQL checkpoint 配置失败", threadName, e);
        }
    }

    protected void updateOracleCheckpointConfig(BinlogPositionInfo checkpoint) {
        String threadName = "TaskExecutor-" + taskId;
        java.util.Properties props = new java.util.Properties();
        File configFile = new File("./files/" + taskId + "/config.properties");

        try {
            if (configFile.exists()) {
                try (java.io.InputStream input = new java.io.FileInputStream(configFile)) {
                    props.load(input);
                }
            }
            if (checkpoint.getFilename() != null) {
                props.setProperty("checkpoint.redo.scn", checkpoint.getFilename());
                props.setProperty("capture.redo.scn", checkpoint.getFilename());
            }
            props.setProperty("checkpoint.redo.position", String.valueOf(checkpoint.getPosition()));
            props.setProperty("capture.redo.position", String.valueOf(checkpoint.getPosition()));
            props.setProperty("extract.skip.before.checkpoint", "true");
            try (java.io.OutputStream output = new java.io.FileOutputStream(configFile)) {
                props.store(output, "Updated Oracle checkpoint for task: " + taskId);
            }
            logger.info("[{}] Oracle Checkpoint 位点已写入配置文件: SCN={}", threadName, checkpoint.getFilename());
        } catch (Exception e) {
            logger.error("[{}] 写入 Oracle checkpoint 配置失败", threadName, e);
        }
    }

    // ==================== 进程启动 ====================

    protected boolean startCaptureProcess() {
        String threadName = "TaskExecutor-" + taskId;
        String captureType;
        if ("postgresql".equals(sourceType)) {
            captureType = "WAL";
        } else if ("oracle".equals(sourceType)) {
            captureType = "REDO";
        } else {
            captureType = "binlog";
        }
        logger.info("[{}] 启动 capture 进程（拉取 {}）", threadName, captureType);

        try {
            captureGuard = new ProcessGuard("capture", taskId, config, kafkaProducer,
                    () -> {
                        ProcessManager pm = new ProcessManager(config.getCaptureJarPath(), "CaptureMain-" + taskId);
                        pm.setTaskId(taskId);
                        return pm;
                    }, runningStatus);

            boolean started = captureGuard.startAndGuard();
            if (!started) {
                logger.error("[{}] capture 进程启动失败", threadName);
                sendStatus("FAILED", "capture 进程启动失败", 0);
                return false;
            }
            logger.info("[{}] capture 进程启动成功", threadName);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 启动 capture 进程失败", threadName, e);
            sendStatus("FAILED", "启动 capture 进程失败: " + e.getMessage(), 0);
            return false;
        }
    }

    protected boolean startExtractProcess() {
        String threadName = "TaskExecutor-" + taskId;
        logger.info("[{}] 启动 extract 进程", threadName);

        try {
            extractGuard = new ProcessGuard("extract", taskId, config, kafkaProducer,
                    () -> {
                        ProcessManager pm = new ProcessManager(config.getExtractJarPath(), "ContinuousExtractMain-" + taskId);
                        pm.setTaskId(taskId);
                        return pm;
                    }, runningStatus);

            boolean started = extractGuard.startAndGuard();
            if (!started) {
                logger.error("[{}] extract 进程启动失败", threadName);
                return false;
            }
            logger.info("[{}] extract 进程启动成功", threadName);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 启动 extract 进程失败", threadName, e);
            return false;
        }
    }

    protected boolean startIncrementProcess() {
        String threadName = "TaskExecutor-" + taskId;
        logger.info("[{}] 启动增量同步进程", threadName);

        try {
            incrementGuard = new ProcessGuard("increment", taskId, config, kafkaProducer,
                    () -> {
                        ProcessManager pm = new ProcessManager(config.getIncrementJarPath(), "ContinuousIncrementMain-" + taskId);
                        pm.setTaskId(taskId);
                        return pm;
                    }, runningStatus);

            boolean started = incrementGuard.startAndGuard();
            if (!started) {
                logger.warn("[{}] 增量同步进程启动失败，ProcessGuard将负责重试", threadName);
                return false;
            }
            sendStatus(runningStatus, "增量同步中", 100);
            logger.info("[{}] 增量同步进程启动成功", threadName);
            return true;
        } catch (Exception e) {
            logger.warn("[{}] 启动增量同步进程失败: {}", threadName, e.getMessage());
            return false;
        }
    }

    protected boolean executeFullMigration() {
        String threadName = "TaskExecutor-" + taskId;
        logger.info("[{}] 开始执行全量迁移", threadName);

        try {
            fullProcess = new ProcessManager(config.getMigrationFullJarPath(), "MigrationFull-" + taskId);
            fullProcess.setTaskId(taskId);
            fullProcess.start();

            sendStatus("FULL_MIGRATING", "全量同步中", 0, 0, 0, null, 0, 0L, 0L);
            startFullMigrationMonitor();

            int exitCode = fullProcess.waitFor();

            if (fullMigrationMonitorThread != null) {
                fullMigrationMonitorThread.interrupt();
            }

            if (stopped.get()) {
                logger.info("[{}] 全量迁移被暂停", threadName);
                return false;
            }

            if (exitCode == 0) {
                logger.info("[{}] 全量迁移完成", threadName);
                sendStatus("FULL_COMPLETED", "全量同步完成", 100, totalTables, totalTables, null, 100, 0L, 0L);
                return true;
            } else {
                logger.error("[{}] 全量迁移失败，退出码: {}", threadName, exitCode);
                sendStatus("FAILED", "全量迁移失败，退出码: " + exitCode, 0);
                stopped.set(true);
                return false;
            }
        } catch (Exception e) {
            if (stopped.get()) {
                logger.info("[{}] 全量迁移被暂停（异常捕获）", threadName);
                return false;
            }
            logger.error("[{}] 全量迁移执行异常", threadName, e);
            sendStatus("FAILED", "全量迁移执行异常: " + e.getMessage(), 0);
            stopped.set(true);
            return false;
        }
    }

    protected void startFullMigrationMonitor() {
        fullMigrationMonitorThread = new Thread(() -> {
            String progressDbUrl = "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";

            while (!stopped.get() && fullProcess != null && fullProcess.isRunning()) {
                try {
                    Thread.sleep(config.getProgressMonitorIntervalMs());
                    if (stopped.get()) break;

                    try (Connection conn = DriverManager.getConnection(progressDbUrl, "sa", "")) {
                        String completedSql = "SELECT COUNT(*) FROM migration_progress WHERE status = 'COMPLETED'";
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(completedSql)) {
                            if (rs.next()) {
                                completedTables = rs.getInt(1);
                            }
                        }

                        String currentTableSql = "SELECT table_name, total_rows, migrated_rows, status FROM migration_progress WHERE status = 'IN_PROGRESS' LIMIT 1";
                        String currentTable = null;
                        long currentTableRows = 0;
                        long currentTableTotalRows = 0;
                        int currentTableProgress = 0;

                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(currentTableSql)) {
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
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("[{}] 全量迁移监控异常", taskId, e);
                }
            }
        }, "FullMigrationMonitor-" + taskId);
        fullMigrationMonitorThread.setDaemon(true);
        fullMigrationMonitorThread.start();
    }

    // ==================== 停止进程 ====================

    protected void stopAllProcesses() {
        String threadName = "TaskExecutor-" + taskId;
        logger.info("[{}] 停止所有进程", threadName);

        if (incrementGuard != null) {
            try { incrementGuard.stop(); logger.info("[{}] 增量同步进程已停止", threadName); }
            catch (Exception e) { logger.error("[{}] 停止增量同步进程失败", threadName, e); }
        }
        if (extractGuard != null) {
            try { extractGuard.stop(); logger.info("[{}] extract 进程已停止", threadName); }
            catch (Exception e) { logger.error("[{}] 停止 extract 进程失败", threadName, e); }
        }
        if (extractProcess != null) {
            try { extractProcess.stop(); } catch (Exception e) { logger.error("[{}] 停止 extract 进程失败", threadName, e); }
        }
        if (fullProcess != null) {
            try { fullProcess.stop(); logger.info("[{}] 全量迁移进程已停止", threadName); }
            catch (Exception e) { logger.error("[{}] 停止全量迁移进程失败", threadName, e); }
        }
        if (captureGuard != null) {
            try { captureGuard.stop(); logger.info("[{}] capture 进程已停止", threadName); }
            catch (Exception e) { logger.error("[{}] 停止 capture 进程失败", threadName, e); }
        }
        stopExtraProcesses(threadName);
    }

    /** 子类可覆盖以停止额外进程（如 subscribe） */
    protected void stopExtraProcesses(String threadName) {
    }

    // ==================== 状态上报 ====================

    protected void sendStatus(String status, String message, int progress) {
        sendStatus(status, message, progress, null, null, null, null, null, null);
    }

    protected void sendFailedStatus(String errorCode, String message) {
        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus("FAILED");
        statusMessage.setMessage(message);
        statusMessage.setProgress(0);
        statusMessage.setErrorCode(errorCode);
        logger.info("[{}] FAILED status with errorCode={}, message={}", taskId, errorCode, message);
        kafkaProducer.sendStatus(statusMessage);
    }

    protected void sendStatus(String status, String message, int progress,
                              Integer totalTables, Integer completedTables, String currentTable,
                              Integer currentTableProgress, Long currentTableRows, Long currentTableTotalRows) {
        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus(status);
        statusMessage.setMessage(message);
        statusMessage.setProgress(progress);
        statusMessage.setTotalTables(totalTables);
        statusMessage.setCompletedTables(completedTables);
        statusMessage.setCurrentTable(currentTable);
        statusMessage.setCurrentTableProgress(currentTableProgress);
        statusMessage.setCurrentTableRows(currentTableRows);
        statusMessage.setCurrentTableTotalRows(currentTableTotalRows);

        Long rpoMs = readMetricFile("./files/" + taskId + "/binlog_output/rpo_metric");
        Long rtoMs = readMetricFile("./files/" + taskId + "/binlog_output/rto_metric");
        Long calculatedRpo = calculateRpo();
        if (calculatedRpo != null) rpoMs = calculatedRpo;
        statusMessage.setRpoMs(rpoMs);
        statusMessage.setRtoMs(rtoMs);

        if ("FAILED".equals(status)) {
            String errorCode = SyncErrorCodeMapper.mapFailureToErrorCode(message);
            statusMessage.setErrorCode(errorCode);
            logger.info("[{}] FAILED status with errorCode={}, message={}", taskId, errorCode, message);
        }
        kafkaProducer.sendStatus(statusMessage);
    }

    // ==================== 指标收集 ====================

    protected void collectMetrics(MetricsService.TaskMetrics taskMetrics) {
        try {
            Long rpoMs = readMetricFile("./files/" + taskId + "/binlog_output/rpo_metric");
            Long rtoMs = readMetricFile("./files/" + taskId + "/binlog_output/rto_metric");

            if (rtoMs != null && rtoMs > 0) taskMetrics.recordE2eLatency(rtoMs);

            Long rpoValue = calculateRpo();
            if (rpoValue != null) {
                taskMetrics.recordCheckpointLag(rpoValue / 1000);
                taskMetrics.recordRpo(rpoValue);
            } else if (rpoMs != null && rpoMs > 0) {
                taskMetrics.recordRpo(rpoMs);
            }

            if (rtoMs != null && rtoMs > 0) taskMetrics.recordRto(rtoMs);

            taskMetrics.recordCaptureRate(readCaptureRateFromFile());
            readQueueDepthsFromFiles(taskMetrics);
        } catch (Exception e) {
            logger.debug("[{}] Error collecting metrics", taskId, e);
        }
    }

    protected void persistMetrics(MetricsService.TaskMetrics taskMetrics) {
        try {
            MetricsPersistenceService persistence = MetricsPersistenceService.getInstance();
            if (persistence != null) {
                persistence.recordMetrics(taskId, taskMetrics);
            }
        } catch (Exception e) {
            logger.debug("[{}] Error persisting metrics", taskId, e);
        }
    }

    protected boolean checkIncrementErrorStatus() {
        String errorFilePath = "./files/" + taskId + "/binlog_output/error_status";
        java.io.File errorFile = new java.io.File(errorFilePath);
        if (!errorFile.exists()) return false;

        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(errorFile))) {
            String line = reader.readLine();
            if (line != null) {
                String[] parts = line.split("\\|", 4);
                if (parts.length >= 4) {
                    String errorCode = parts[1];
                    String seqno = parts[2];
                    String message = parts[3];
                    logger.error("[{}] 检测到 increment 进程错误状态: errorCode={}, seqno={}, message={}",
                            taskId, errorCode, seqno, message);
                    sendFailedStatus(errorCode, "增量同步不可恢复错误 (seqno=" + seqno + "): " + message);
                    stopped.set(true);
                    errorFile.delete();
                    return true;
                }
            }
        } catch (java.io.IOException e) {
            logger.warn("[{}] 读取错误状态文件失败: {}", taskId, e.getMessage());
        }
        return false;
    }

    protected void saveTaskStateOnFailure() {
        if (taskStateService == null) {
            logger.warn("[{}] TaskStateService 未注入，无法保存任务状态到 H2", taskId);
            return;
        }
        try {
            String statusToSave = lastSuccessfulStatus != null ? lastSuccessfulStatus : "FAILED";
            TaskStateInfo stateInfo = new TaskStateInfo(taskId);
            stateInfo.setTaskName(taskMessage.getTaskName());
            stateInfo.setUserId(taskMessage.getUserId());
            stateInfo.setMigrationMode(migrationMode);
            stateInfo.setSourceConnection(taskMessage.getSourceConnection());
            stateInfo.setTargetConnection(taskMessage.getTargetConnection());
            stateInfo.setSourceType(sourceType);
            stateInfo.setTargetType(targetType);
            stateInfo.setStatus(statusToSave);
            stateInfo.setProgress(runningStatus.equals(statusToSave) || "FULL_COMPLETED".equals(statusToSave) ? 100 : 0);
            stateInfo.setCreatedAt(taskMessage.getCreatedAt() != null ? taskMessage.getCreatedAt() : java.time.LocalDateTime.now());
            taskStateService.saveTaskState(stateInfo);
            logger.info("[{}] 任务失败时状态已保存到 H2, statusToSave={}", taskId, statusToSave);
        } catch (Exception e) {
            logger.error("[{}] 保存任务状态到 H2 失败", taskId, e);
        }
    }

    // ==================== 工具方法 ====================

    protected Long readMetricFile(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) return null;
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    String[] parts = line.split("\\|");
                    if (parts.length >= 2) return Long.parseLong(parts[1].trim());
                }
            }
        } catch (Exception e) { }
        return null;
    }

    protected Long readMetricTimestamp(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) return null;
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    String[] parts = line.split("\\|");
                    if (parts.length >= 3) return Long.parseLong(parts[2].trim());
                }
            }
        } catch (Exception e) { }
        return null;
    }

    protected Long calculateRpo() {
        Long lastSourceEventTs = readMetricTimestamp("./files/" + taskId + "/binlog_output/rpo_metric");
        Long lastAppliedSourceTs = readMetricTimestamp("./files/" + taskId + "/binlog_output/rto_metric");
        if (lastSourceEventTs != null && lastAppliedSourceTs != null && lastSourceEventTs > 0 && lastAppliedSourceTs > 0) {
            long rpo = lastSourceEventTs - lastAppliedSourceTs;
            return rpo >= 0 ? rpo : 0L;
        }
        return null;
    }

    protected long readCaptureRateFromFile() {
        try {
            File rateFile = new File("./files/" + taskId + "/binlog_output/capture_rate");
            if (!rateFile.exists()) return 0;
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(rateFile))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) return Long.parseLong(line.trim());
            }
        } catch (Exception e) { }
        return 0;
    }

    protected void readQueueDepthsFromFiles(MetricsService.TaskMetrics taskMetrics) {
        readQueueDepthFromFile("capture", "./files/" + taskId + "/binlog_output/capture_queue_depth", taskMetrics);
        readQueueDepthFromFile("extract", "./files/" + taskId + "/binlog_output/extract_queue_depth", taskMetrics);
        readQueueDepthFromFile("apply", "./files/" + taskId + "/binlog_output/apply_queue_depth", taskMetrics);
    }

    protected void readQueueDepthFromFile(String stage, String filePath, MetricsService.TaskMetrics taskMetrics) {
        try {
            File file = new File(filePath);
            if (!file.exists()) return;
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    taskMetrics.recordQueueDepth(stage, Long.parseLong(line.trim()));
                }
            }
        } catch (Exception e) { }
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
}
