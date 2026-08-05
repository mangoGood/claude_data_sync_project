package com.migration.agent;

import com.migration.agent.manager.MigrationTaskManager;
import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.RecoveryTask;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStateInfo;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.AgentHttpServer;
import com.migration.agent.service.ConfigService;
import com.migration.agent.service.KafkaConsumerService;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.RecoveryService;
import com.migration.agent.service.TaskStateService;
import com.migration.agent.spring.AgentSpringConfig;
import com.migration.agent.thread.MigrationAgentThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class AgentMain {
    private static final Logger logger = LoggerFactory.getLogger(AgentMain.class);

    /** TiDB 源任务终结/删除时清理 TiCDC changefeed（暂停不清，恢复要靠它续传）。 */
    private static final com.migration.agent.service.TicdcChangefeedService TICDC_CHANGEFEED_SERVICE =
            new com.migration.agent.service.TicdcChangefeedService();
    
    // 默认值统一为本机地址（可被环境变量/agent.properties 覆盖）；不再硬编码内网 IP。
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CONSUMER_GROUP_ID = "migration-agent-group";
    private static final String METADATA_DB_USER = "sa";
    private static final String METADATA_DB_PASSWORD = "";

    private static final String MYSQL_DB_URL = System.getenv().getOrDefault("DB_URL", "jdbc:mysql://localhost:33306/sync_task_db?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true");
    private static final String MYSQL_DB_USER = System.getenv().getOrDefault("DB_USERNAME", "root");
    private static final String MYSQL_DB_PASSWORD = System.getenv().getOrDefault("DB_PASSWORD", "rootpassword");
    
    private static final String CAPTURE_JAR_PATH = "migration-capture/target/migration-capture-1.0.0.jar";
    private static final String MIGRATION_FULL_JAR_PATH = "migration-full/target/migration-full-1.0.0.jar";
    private static final String EXTRACT_JAR_PATH = "migration-extract/target/migration-extract-1.0.0.jar";
    private static final String INCREMENT_JAR_PATH = "migration-increment/target/migration-increment-1.0.0.jar";
    
    private static final long CAPTURE_MONITOR_INTERVAL = 30000;
    
    private KafkaConsumerService kafkaConsumer;
    private KafkaProducerService kafkaProducer;
    private ConfigService configService;
    private TaskStateService taskStateService;
    private RecoveryService recoveryService;
    private ScheduledExecutorService captureMonitorExecutor;
    private ExecutorService taskExecutor;
    private com.migration.agent.service.TaskFilesJanitor taskFilesJanitor;
    private com.migration.agent.service.AgentRegistryService agentRegistry;
    
    private final ConcurrentHashMap<String, ProcessManager> captureManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProcessManager> extractManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProcessManager> incrementManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MigrationTaskManager> migrationTaskManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MigrationAgentThread> migrationAgentThreads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Thread> migrationAgentThreadWrappers = new ConcurrentHashMap<>();
    private final Set<String> pausedTasks = ConcurrentHashMap.newKeySet();
    private final Set<String> failoverInProgress = ConcurrentHashMap.newKeySet();
    
    private AgentHttpServer httpServer;
    private ApplicationContext springContext;
    
    public static void main(String[] args) {
        com.migration.common.OracleNetCompat.apply();
        // 防御性设置：若 agent 的进度轮询恰好先于 migration-full 打开 H2 progress 库
        // （成为 AUTO_SERVER 的持有方），同样强制绑定回环地址，避免绑到局域网 IP。
        // 必须在任何 H2 Driver 类加载前设置（SysProperties 以 static final 读取一次）。
        System.setProperty("h2.bindAddress", "127.0.0.1");

        validateSecretsOrExit();

        AgentMain agent = new AgentMain();
        agent.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down agent...");
            agent.stop();
        }));
    }

    /**
     * 启动时安全配置校验：严格模式（SYNCTASK_STRICT_SECRETS=true）下敏感项缺失即退出，
     * 而非默默让敏感接口无鉴权运行 / 用内置默认主密钥解密。默认宽松（仅告警），不阻断本地起停。
     */
    private static void validateSecretsOrExit() {
        String flag = System.getenv("SYNCTASK_STRICT_SECRETS");
        if (flag == null || flag.isBlank()) flag = System.getProperty("synctask.strict.secrets", "");
        boolean strict = "true".equalsIgnoreCase(flag.trim()) || "1".equals(flag.trim());

        java.util.List<String> problems = new java.util.ArrayList<>();
        String apiToken = System.getenv("AGENT_API_TOKEN");
        if (apiToken == null || apiToken.isBlank()) {
            problems.add("AGENT_API_TOKEN 未配置——failover / start-increment 等敏感 HTTP 接口将无鉴权放行");
        }
        String masterKey = System.getenv("SYNCTASK_MASTER_KEY");
        if (masterKey == null || masterKey.isBlank()) masterKey = System.getProperty("synctask.master.key");
        if (masterKey == null || masterKey.isBlank()) {
            problems.add("SYNCTASK_MASTER_KEY 未配置——将退化为内置默认主密钥，与生产密文不兼容");
        }

        if (problems.isEmpty()) {
            logger.info("Agent 启动安全校验通过（strict={}）", strict);
            return;
        }
        String detail = String.join("\n  - ", problems);
        if (strict) {
            logger.error("Agent 启动安全校验失败（严格模式）：\n  - {}\n请注入上述密钥后重启。", detail);
            System.exit(1);
        } else {
            logger.warn("⚠ Agent 启动安全校验发现问题（宽松模式，仅告警；生产请设 SYNCTASK_STRICT_SECRETS=true）：\n  - {}", detail);
        }
    }
    
    public void start() {
        logger.info("Starting Migration Agent...");
        
        // 通过 Spring DI 容器初始化核心组件，便于测试和配置管理
        springContext = new AnnotationConfigApplicationContext(AgentSpringConfig.class);
        logger.info("Spring ApplicationContext initialized with DI beans");
        
        kafkaProducer = springContext.getBean(KafkaProducerService.class);
        configService = springContext.getBean(ConfigService.class);
        taskStateService = springContext.getBean(TaskStateService.class);
        AgentConfig agentConfig = springContext.getBean(AgentConfig.class);
        
        recoveryService = new RecoveryService(agentConfig.getMysqlDbUrl(), agentConfig.getMysqlDbUser(), agentConfig.getMysqlDbPassword());

        // 集群化：把本 agent 注册进元数据库并开始心跳/续租。必须在恢复任务之前——
        // 恢复要按"任务是否归属自己"过滤，没有身份就没法过滤。
        agentRegistry = new com.migration.agent.service.AgentRegistryService(
                agentConfig, () -> new java.util.HashSet<>(migrationAgentThreads.keySet()));
        agentRegistry.start();

        // 指标落盘（全局 H2 时序库）：监控页"任务启动以来"的历史曲线依赖它回填。
        // 此前该服务从未被初始化 → /api/metrics/{id}/history 一律 503，前端只能靠打开页面后的
        // 实时轮询累积数据，看起来像"从点开监控页的时间开始统计"。
        long metricsFlushMs = Long.parseLong(
                agentConfig.getRawProperty("metrics.persistence.flush.interval.ms", "30000"));
        int metricsBatchSize = Integer.parseInt(
                agentConfig.getRawProperty("metrics.persistence.batch.size", "100"));
        com.migration.agent.service.MetricsPersistenceService.initialize(
                "jdbc:h2:./files/agent_metrics;MODE=MySQL;AUTO_SERVER=TRUE",
                agentConfig.getH2MetadataUser(), agentConfig.getH2MetadataPassword(),
                metricsFlushMs, metricsBatchSize);

        // 位点中心持久化：位点不能只活在这台机器的磁盘上——V8 的故障转移假设"接管方从各自
        // checkpoint 续传"，而那个 checkpoint 在 files/<taskId>/ 里，换台机器就是空的，
        // 接管方于是去取"源库此刻的位点"，把崩溃到接管之间的变更整段跳过（不报错、不告警）。
        if (Boolean.parseBoolean(agentConfig.getRawProperty("checkpoint.central.enabled", "true"))) {
            com.migration.agent.checkpoint.CentralCheckpointStore checkpointStore =
                    com.migration.agent.checkpoint.CentralCheckpointStore.initialize(
                            agentConfig.getMysqlDbUrl(), agentConfig.getMysqlDbUser(),
                            agentConfig.getMysqlDbPassword());
            com.migration.agent.checkpoint.CheckpointHydrator.initialize(checkpointStore,
                    agentRegistry.getAgentId(),
                    Boolean.parseBoolean(agentConfig.getRawProperty("checkpoint.hydrate.fail.stop", "true")));
            com.migration.agent.checkpoint.CheckpointUploader.initialize(checkpointStore,
                    agentRegistry.getAgentId(),
                    Long.parseLong(agentConfig.getRawProperty("checkpoint.central.upload.interval.ms", "3000")),
                    Long.parseLong(agentConfig.getRawProperty("checkpoint.history.sample.interval.s", "300")) * 1000L,
                    () -> new java.util.HashSet<>(migrationAgentThreads.keySet()));
        } else {
            logger.info("位点中心持久化已关闭（checkpoint.central.enabled=false），回到本地位点行为");
        }

        kafkaConsumer = new KafkaConsumerService(KAFKA_BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID,
            this::handleTaskMessage);
        
        kafkaConsumer.start();
        
        httpServer = new AgentHttpServer(this);
        httpServer.start();
        
        taskExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("task-executor");
            return t;
        });
        
        captureMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("capture-monitor");
            return t;
        });
        captureMonitorExecutor.scheduleAtFixedRate(this::monitorCaptureProcesses,
            CAPTURE_MONITOR_INTERVAL, CAPTURE_MONITOR_INTERVAL, TimeUnit.MILLISECONDS);

        // 终态任务目录的定期清理（只清打过 .terminal 标记且过了保留期的，见 TaskFilesJanitor）
        taskFilesJanitor = new com.migration.agent.service.TaskFilesJanitor(
                agentConfig, () -> new java.util.HashSet<>(migrationAgentThreads.keySet()));
        captureMonitorExecutor.scheduleAtFixedRate(() -> {
            try {
                taskFilesJanitor.sweepOnce();
            } catch (Exception e) {
                logger.warn("清理终态任务目录出错: {}", e.getMessage());
            }
        }, 60_000, 3600_000, TimeUnit.MILLISECONDS);

        // 必须先收孤儿再恢复任务：上一任 agent 硬崩后遗留的子进程还在写目标库，
        // 直接恢复会让同一 taskId 起第二套进程造成双写；而任务实例锁又会被孤儿占着，
        // 导致新进程一直起不来直到熔断。顺序反了两头都不对。
        com.migration.agent.resilience.OrphanChildReaper.reap();

        recoverUnfinishedTasks();

        logger.info("Migration Agent started successfully, waiting for tasks...");
    }
    
    public void stop() {
        logger.info("Stopping all tasks...");
        
        for (Map.Entry<String, MigrationAgentThread> entry : migrationAgentThreads.entrySet()) {
            try {
                String taskId = entry.getKey();
                MigrationAgentThread thread = entry.getValue();
                logger.info("Stopping migration agent thread for task: {}", taskId);
                thread.stop();
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
                String taskId = entry.getKey();
                MigrationTaskManager manager = entry.getValue();
                logger.info("Stopping migration task: {}", taskId);
                manager.stop();
            } catch (Exception e) {
                logger.error("Error stopping migration task", e);
            }
        }
        migrationTaskManagers.clear();
        
        for (Map.Entry<String, ProcessManager> entry : captureManagers.entrySet()) {
            try {
                String taskId = entry.getKey();
                ProcessManager manager = entry.getValue();
                logger.info("Stopping capture process for task: {}", taskId);
                manager.stop();
            } catch (Exception e) {
                logger.error("Error stopping capture process", e);
            }
        }
        captureManagers.clear();

        for (Map.Entry<String, ProcessManager> entry : extractManagers.entrySet()) {
            try {
                String taskId = entry.getKey();
                ProcessManager manager = entry.getValue();
                logger.info("Stopping extract process for task: {}", taskId);
                manager.stop();
            } catch (Exception e) {
                logger.error("Error stopping extract process", e);
            }
        }
        extractManagers.clear();

        for (Map.Entry<String, ProcessManager> entry : incrementManagers.entrySet()) {
            try {
                String taskId = entry.getKey();
                ProcessManager manager = entry.getValue();
                logger.info("Stopping increment process for task: {}", taskId);
                manager.stop();
            } catch (Exception e) {
                logger.error("Error stopping increment process", e);
            }
        }
        incrementManagers.clear();
        
        pausedTasks.clear();
        
        if (captureMonitorExecutor != null) {
            captureMonitorExecutor.shutdown();
        }
        
        if (taskExecutor != null) {
            taskExecutor.shutdown();
        }
        
        if (kafkaConsumer != null) {
            kafkaConsumer.stop();
        }

        // 停机前把位点再上卷一次：优雅停机后任务马上会被改派，接管方拿到的位点越新，重放越少。
        // 必须排在 agentRegistry.stop() 之前——释放租约后后端可能立刻改派，那时再上卷
        // 就会撞上新主更高的 epoch 被 fencing 拒掉。
        com.migration.agent.checkpoint.CheckpointUploader uploader =
                com.migration.agent.checkpoint.CheckpointUploader.getInstance();
        if (uploader != null) {
            uploader.stop();
        }

        // 优雅停机时主动下线并释放租约，后端立刻能改派，不用干等 90s 心跳超时
        if (agentRegistry != null) {
            agentRegistry.stop();
        }

        if (httpServer != null) {
            httpServer.stop();
        }
        
        if (springContext instanceof AnnotationConfigApplicationContext) {
            ((AnnotationConfigApplicationContext) springContext).close();
            logger.info("Spring ApplicationContext closed");
        }
        
        logger.info("Agent stopped");
    }
    
    private void handleTaskMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        String messageType = taskMessage.getMessageType();

        logger.info("Received task message: {} with messageType: {}", taskId, messageType);

        // 定向下发：backend 已经挑好了执行的 agent，其它 agent 直接放行不处理。
        // 消息不带 targetAgentId（老后端 / 集群里没有注册过 agent）时退回广播语义，保持兼容。
        String target = taskMessage.getTargetAgentId();
        if (target != null && !target.isEmpty() && agentRegistry != null
                && !target.equals(agentRegistry.getAgentId())) {
            logger.info("任务 {} 指派给 agent {}，本 agent({}) 忽略", taskId, target, agentRegistry.getAgentId());
            return;
        }

        if ("stop".equals(messageType)) {
            taskExecutor.submit(() -> handleStopMessage(taskMessage));
        } else if ("terminate".equals(messageType)) {
            taskExecutor.submit(() -> handleTerminateMessage(taskMessage));
        } else if ("resume".equals(messageType)) {
            taskExecutor.submit(() -> handleResumeMessage(taskMessage));
        } else if ("delete".equals(messageType)) {
            taskExecutor.submit(() -> handleDeleteMessage(taskMessage));
        } else if ("failover".equals(messageType)) {
            taskExecutor.submit(() -> handleFailoverMessage(taskMessage));
        } else {
            taskExecutor.submit(() -> processTask(taskMessage, taskId, taskMessage.getMigrationMode()));
        }
    }
    
    private void handleDeleteMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling delete message for task: {}", taskId);
        
        pausedTasks.remove(taskId);
        
        stopTaskById(taskId);

        stopMigrationAgentThread(taskId);

        TICDC_CHANGEFEED_SERVICE.removeChangefeedIfTidb(taskId);

        // 任务已删除：目录打终态标记，保留期后由 TaskFilesJanitor 清掉（不立即删——
        // 子进程可能还在收尾写文件，日志/死信在保留期内还有排障价值）
        com.migration.agent.service.TaskFilesJanitor.markTerminal(taskId, "deleted");

        logger.info("Task {} deleted, all processes stopped", taskId);
    }
    
    private void handleStopMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling stop message for task: {}", taskId);
        
        pausedTasks.add(taskId);
        
        try {
            int progress = getProgressFromDatabase(taskId);
            
            String currentStatus = taskMessage.getCurrentStatus();
            if (currentStatus == null || currentStatus.isEmpty()) {
                logger.warn("No currentStatus in message, falling back to MySQL query");
                RecoveryTask currentTask = recoveryService.getTaskById(taskId);
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
            
            stopTaskById(taskId);
            
            stopMigrationAgentThread(taskId);
            
            sendStatus(taskId, "PAUSED", "Task paused, state saved to H2", progress);
            
        } catch (Exception e) {
            logger.error("Error handling stop message for task: {}", taskId, e);
            pausedTasks.remove(taskId);
        }
    }
    
    private void handleTerminateMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling terminate message for task: {}", taskId);
        
        pausedTasks.remove(taskId);
        
        try {
            stopTaskById(taskId);
            stopMigrationAgentThread(taskId);

            // TiDB 源：任务已终结，清掉 changefeed。留着它会一直持有源集群的 GC safepoint
            // （旧版本数据无法回收）并继续往 Kafka 投递，而已经没有人消费了。
            // 只在终态做——暂停(stop)必须保留，恢复正是靠 changefeed 自己的 checkpoint 续传。
            TICDC_CHANGEFEED_SERVICE.removeChangefeedIfTidb(taskId);

            logger.info("Task {} terminated, all processes stopped", taskId);

            // 库级同步：同步进程已全部停止（无双写风险），此刻把源库 trigger/event 复制到目标库
            com.migration.agent.service.DbObjectsSyncService.syncTriggersAndEventsAtTaskEnd(taskId);

            // 终结与删除同样是终态：打标，保留期后清理任务目录
            com.migration.agent.service.TaskFilesJanitor.markTerminal(taskId, "terminated");

        } catch (Exception e) {
            logger.error("Error handling terminate message for task: {}", taskId, e);
        }
    }
    
    private void handleResumeMessage(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("Handling resume message for task: {}", taskId);

        pausedTasks.remove(taskId);
        // 恢复/接管都要立刻认领，别等下一拍心跳：否则这 15s 窗口里租约还挂在老 agent 名下，
        // 巡检可能把同一个任务再改派给第三台
        if (agentRegistry != null) {
            agentRegistry.claimTask(taskId);
        }
        com.migration.agent.service.TaskFilesJanitor.clearTerminalMark(taskId);
        
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
            
            if (isSingleProcessEngine(taskMessage) || "fullAndIncre".equals(migrationMode) || "subscribe".equals(migrationMode)) {
                boolean skipFullMigration = "FULL_COMPLETED".equals(savedStatus) ||
                                           "INCREMENT_RUNNING".equals(savedStatus) ||
                                           "SUBSCRIBE_RUNNING".equals(savedStatus);

                MigrationAgentThread agentThread = new MigrationAgentThread(taskMessage, kafkaProducer, taskStateService, skipFullMigration);
                migrationAgentThreads.put(taskId, agentThread);
                
                Thread threadWrapper = new Thread(agentThread, "MigrationAgentThread-" + taskId);
                threadWrapper.setDaemon(true);
                migrationAgentThreadWrappers.put(taskId, threadWrapper);
                threadWrapper.start();
                
                logger.info("MigrationAgentThread started for {} task: {}, skipFullMigration: {}", migrationMode, taskId, skipFullMigration);
            } else {
                if (progress < 100) {
                    startMigrationForTask(taskId, taskMessage);
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

    private void handleFailoverMessage(TaskMessage taskMessage) {
        // Kafka 触发的灾备切换：切换后状态置 SWITCHING，完成即释放去重令牌。
        performFailover(taskMessage, "Kafka message", "SWITCHING", false);
    }

    /**
     * 灾备切换统一实现（Kafka 与 HTTP 直连两条入口共用，消除此前 ~90% 重复代码）。
     *
     * <p>差异由参数承载：{@code restartStatus}（重启后落库状态，Kafka=SWITCHING / HTTP=INCREMENT_RUNNING）、
     * {@code delayedGuardRelease}（去重令牌释放策略，Kafka=完成即释放 / HTTP=30s 初始化窗口后释放）。
     * 文件清理统一取两条历史路径的并集（删除不存在文件是无害 no-op），并在删文件前统一等待进程退出。
     */
    private void performFailover(TaskMessage taskMessage, String trigger, String restartStatus, boolean delayedGuardRelease) {
        String taskId = taskMessage.getTaskId();
        logger.info("=== FAILOVER ({}) for task: {} ===", trigger, taskId);

        if (!failoverInProgress.add(taskId)) {
            logger.warn("Failover already in progress for task: {}, ignoring duplicate {}", taskId, trigger);
            return;
        }

        boolean releaseScheduled = false;
        try {
            sendStatus(taskId, "SWITCHING", "Failover in progress, stopping current processes", 100);

            stopMigrationAgentThread(taskId);
            stopTaskById(taskId);
            logger.info("All processes stopped for failover task: {}", taskId);

            // 删文件前统一等待子进程完全退出，避免与仍在写文件的进程竞争
            try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }

            taskStateService.deleteTaskState(taskId);
            logger.info("Old task state from H2 deleted for failover task: {}", taskId);

            cleanupFailoverArtifacts(taskId);

            configService.updateConfig(taskMessage);
            logger.info("Config updated for failover task: {} with swapped connections", taskId);

            clearBinlogPositionInConfig(taskId);

            TaskStateInfo stateInfo = new TaskStateInfo(taskId);
            stateInfo.setTaskName(taskMessage.getTaskName());
            stateInfo.setUserId(taskMessage.getUserId());
            stateInfo.setMigrationMode(taskMessage.getMigrationMode());
            stateInfo.setSourceConnection(taskMessage.getSourceConnection());
            stateInfo.setTargetConnection(taskMessage.getTargetConnection());
            stateInfo.setSourceType(taskMessage.getSourceType() != null ? taskMessage.getSourceType() : "mysql");
            stateInfo.setTargetType(taskMessage.getTargetType() != null ? taskMessage.getTargetType() : "mysql");
            stateInfo.setStatus(restartStatus);
            stateInfo.setProgress(100);
            stateInfo.setCreatedAt(taskMessage.getCreatedAt() != null ? taskMessage.getCreatedAt() : java.time.LocalDateTime.now());
            taskStateService.saveTaskState(stateInfo);
            logger.info("Saved H2 state for failover task: {} with status {}", taskId, restartStatus);

            MigrationAgentThread agentThread = new MigrationAgentThread(taskMessage, kafkaProducer, taskStateService, true);
            migrationAgentThreads.put(taskId, agentThread);

            Thread threadWrapper = new Thread(agentThread, "MigrationAgentThread-Failover-" + taskId);
            threadWrapper.setDaemon(true);
            migrationAgentThreadWrappers.put(taskId, threadWrapper);
            threadWrapper.start();

            logger.info("Failover task {} restarted with skipFullMigration=true", taskId);
            sendStatus(taskId, "SWITCHING", "Failover processes starting, skipping full migration", 100);

            if (delayedGuardRelease) {
                // 保留 30s 初始化窗口，期间拒绝重复切换请求
                releaseScheduled = true;
                new Thread(() -> {
                    try { Thread.sleep(30000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    failoverInProgress.remove(taskId);
                    logger.info("Failover initialization period completed for task: {}", taskId);
                }, "FailoverInitGuard-" + taskId).start();
            }
        } catch (Exception e) {
            logger.error("Error handling failover ({}) for task: {}", trigger, taskId, e);
            sendStatus(taskId, "FAILED", "Error during failover: " + e.getMessage(), 0);
        } finally {
            if (!releaseScheduled) {
                failoverInProgress.remove(taskId);
            }
        }
    }

    /** 灾备切换的文件清理：两条历史路径删除项的并集（删不存在文件是无害 no-op）。 */
    private void cleanupFailoverArtifacts(String taskId) {
        java.io.File checkpointFile = new java.io.File("files/" + taskId + "/checkpoint/checkpoint");
        if (checkpointFile.exists()) {
            logger.info("Old checkpoint file deleted: {}, success: {}", checkpointFile.getAbsolutePath(), checkpointFile.delete());
        }

        java.io.File seqnoCheckpointFile = new java.io.File("files/" + taskId + "/checkpoint/seqno_checkpoint.json");
        if (seqnoCheckpointFile.exists()) {
            logger.info("Old seqno checkpoint file deleted: {}, success: {}", seqnoCheckpointFile.getAbsolutePath(), seqnoCheckpointFile.delete());
        }

        deleteDirContents(new java.io.File("files/" + taskId + "/thl_output"), "THL", taskId);
        deleteDirContents(new java.io.File("files/" + taskId + "/binlog_output"), "binlog", taskId);

        java.io.File progressDb = new java.io.File("files/" + taskId + "/migration_progress.mv.db");
        if (progressDb.exists()) {
            logger.info("Old migration progress DB deleted: {}, success: {}", progressDb.getAbsolutePath(), progressDb.delete());
        }
        java.io.File progressTrace = new java.io.File("files/" + taskId + "/migration_progress.trace.db");
        if (progressTrace.exists()) {
            progressTrace.delete();
        }

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
            java.io.File f = new java.io.File(dbFile);
            if (f.exists()) {
                logger.info("Deleted checkpoint file: {}, success: {}", f.getAbsolutePath(), f.delete());
            }
        }

        // 单进程引擎（Mongo）的位点同样是旧源专属：resume token 里编码的是旧副本集的时间戳与 UUID，
        // 拿去 resumeAfter 新源的 oplog 要么报 ChangeStreamHistoryLost、要么落到毫无关系的位置。
        // 进度文件一并清掉，否则 MongoSyncTask 会读到倒换前残留的 phase 误判任务状态。
        String[] singleProcessEngineFiles = {
            "files/" + taskId + "/checkpoint/mongo_resume_token.json",
            "files/" + taskId + "/mongo_progress.json"
        };
        for (String path : singleProcessEngineFiles) {
            java.io.File f = new java.io.File(path);
            if (f.exists()) {
                logger.info("Deleted single-process engine checkpoint: {}, success: {}", f.getAbsolutePath(), f.delete());
            }
        }

        // 统一位点与中心位点也必须一并作废。中心位点尤其不能留：本地清干净了，
        // 接管方一回灌就把刚清掉的旧源位点原样请回来——旧源的 GTID 拿到新源上，
        // 服务端会从新源 binlog 最开头整段重放，直接冲垮备库。
        clearCentralCheckpoints(taskId, "FAILOVER");
        logger.info("All checkpoint DB files deleted before config update for failover task: {}", taskId);
    }

    /**
     * 作废该任务的统一位点与中心位点。
     *
     * <p><b>倒换/重做全量时不能只清本地</b>：中心位点留着，接管方回灌就会把旧源的位点请回来。
     * 上卷缓存也要一并清掉，否则新位点会被"内容没变"的判断挡住而永远写不进中心库。
     */
    private void clearCentralCheckpoints(String taskId, String reason) {
        com.migration.agent.checkpoint.CheckpointCleaner.clear(taskId, reason);
    }

    private void deleteDirContents(java.io.File dir, String label, String taskId) {
        if (dir.exists()) {
            java.io.File[] files = dir.listFiles();
            if (files != null) {
                for (java.io.File f : files) {
                    f.delete();
                }
            }
            logger.info("Old {} files cleaned for failover task: {}", label, taskId);
        }
    }

    /**
     * 清除 config.properties 里的旧位点，使切换后从新源库的**当前**位点开始增量。
     *
     * <p>倒换后源库已换成原目标实例，旧源实例上的任何位点在新源上都无意义，必须全部清掉：
     * <ul>
     *   <li>MySQL：binlog file+pos 之外，<b>capture.gtid.set / checkpoint.gtid.set 尤其致命</b>——
     *       GTID 集里全是旧源 server_uuid 的事务，新源一条都不认识，
     *       服务端按"客户端缺这些事务"的语义从新源 binlog 的**最开头**重放整段历史
     *       （含历史 DDL 与全量导入），把新备库的数据冲垮；</li>
     *   <li>PostgreSQL：LSN 是实例内部编号，跨实例无意义（capture.wal.lsn 会让新槽从错误位置起步）；</li>
     *   <li>Oracle：SCN 同理。</li>
     * </ul>
     * 全部清空后 capture 走"从最新位点开始"，这正是主备倒换要的语义。
     * （{@code service/FailoverService} 里有一份等价实现，实际生效的是本方法，两处需保持一致。）
     */
    private void clearBinlogPositionInConfig(String taskId) {
        java.io.File configFile = new java.io.File("files/" + taskId + "/config.properties");
        if (!configFile.exists()) return;
        try {
            java.util.Properties configProps = new java.util.Properties();
            try (java.io.InputStream cis = new java.io.FileInputStream(configFile)) {
                configProps.load(cis);
            }
            for (String key : STALE_POSITION_KEYS_ON_FAILOVER) {
                configProps.remove(key);
            }
            // 倒换后新源就是原目标实例，再跑一次全量等于把备库整个灌回原主库。SQL 管线靠编排层的
            // skipFullMigration 跳过；单进程引擎（Mongo）在进程内自行决定全量与否，只能靠配置项传达。
            configProps.setProperty("migration.increment.only", "true");
            try (java.io.OutputStream cos = new java.io.FileOutputStream(configFile)) {
                configProps.store(cos, "Updated for failover - binlog position cleared");
            }
            logger.info("Cleared old capture position (binlog/gtid/lsn/scn) in config for failover task: {}", taskId);
        } catch (Exception e) {
            logger.warn("Failed to clear binlog position in config for failover task {}: {}", taskId, e.getMessage());
        }
    }

    /** 主备倒换后必须从 config.properties 清除的旧源位点键（三种源类型的并集，删不存在的键是 no-op）。 */
    public static final String[] STALE_POSITION_KEYS_ON_FAILOVER = {
        "capture.binlog.file", "capture.binlog.position",
        "checkpoint.binlog.file", "checkpoint.binlog.position",
        "capture.gtid.set", "checkpoint.gtid.set",
        "capture.wal.lsn", "capture.wal.position",
        "checkpoint.wal.lsn", "checkpoint.wal.position",
        "capture.redo.scn", "capture.redo.position",
        "checkpoint.redo.scn", "checkpoint.redo.position",
    };

    private void stopMigrationAgentThread(String taskId) {
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
    
    private void stopTaskById(String taskId) {
        if (migrationTaskManagers.containsKey(taskId)) {
            MigrationTaskManager manager = migrationTaskManagers.remove(taskId);
            try {
                manager.stop();
                logger.info("Migration task stopped for task: {}", taskId);
            } catch (Exception e) {
                logger.error("Error stopping migration task for task: {}", taskId, e);
            }
        }

        if (captureManagers.containsKey(taskId)) {
            ProcessManager manager = captureManagers.remove(taskId);
            try {
                manager.stop();
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
    
    /**
     * 单进程引擎（mongodb 源 / elasticsearch 目标 / redis 源）：全量与增量都委派给
     * {@link MigrationAgentThread} 内对应的 SyncTask（子进程自读 migration.mode 决定全量后
     * 是否进入增量），不走 SQL 侧 legacy MigrationTaskManager（migration-full）——否则仅全量
     * 模式会被误路由到 SQL 全量引擎，对 Redis/Mongo/ES 无意义且不产出数据。
     */
    private boolean isSingleProcessEngine(TaskMessage taskMessage) {
        return "mongodb".equalsIgnoreCase(taskMessage.getSourceType())
                || "elasticsearch".equalsIgnoreCase(taskMessage.getTargetType())
                || "redis".equalsIgnoreCase(taskMessage.getSourceType());
    }

    private void processTask(TaskMessage taskMessage, String taskId, String migrationMode) {
        try {
            sendStatus(taskId, "RECEIVED", "Task received, preparing migration", 0);

            // 同一 taskId 又跑起来了（重建/重启）：撤销终态标记，别让保留期到点删掉在跑的任务目录
            com.migration.agent.service.TaskFilesJanitor.clearTerminalMark(taskId);
            if (agentRegistry != null) {
                agentRegistry.claimTask(taskId);
            }

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
            
            if (isSingleProcessEngine(taskMessage) || "fullAndIncre".equals(migrationMode) || "subscribe".equals(migrationMode)) {
                // 双向灾备的反向影子通道（DR_SHADOW）只做增量：capture 从源库最新位点起步。
                // 绝不能跑全量——反向全量会把灾备库反灌回主库，覆盖/冲突主库存量数据。
                boolean skipFullMigration = "DR_SHADOW".equals(taskMessage.getTaskType());
                MigrationAgentThread agentThread = new MigrationAgentThread(taskMessage, kafkaProducer, taskStateService, skipFullMigration);
                migrationAgentThreads.put(taskId, agentThread);

                Thread threadWrapper = new Thread(agentThread, "MigrationAgentThread-" + taskId);
                threadWrapper.setDaemon(true);
                migrationAgentThreadWrappers.put(taskId, threadWrapper);
                threadWrapper.start();

                logger.info("MigrationAgentThread started for {} task: {} (skipFullMigration={})",
                        migrationMode, taskId, skipFullMigration);
            } else {
                logger.info("Full migration mode, skipping binlog process for task: {}", taskId);
                startMigrationForTask(taskId, taskMessage);
            }
            
        } catch (Exception e) {
            logger.error("Error handling task message: {}", taskId, e);
            sendStatus(taskId, "FAILED", "Error: " + e.getMessage(), 0);
        }
    }
    
    private void startCaptureForTask(String taskId, TaskMessage taskMessage) throws Exception {
        if (captureManagers.containsKey(taskId)) {
            logger.warn("Capture process already running for task: {}", taskId);
            return;
        }

        ProcessManager captureManager = new ProcessManager(CAPTURE_JAR_PATH, "CaptureMain-" + taskId);
        captureManager.setTaskId(taskId);
        captureManager.start();

        captureManagers.put(taskId, captureManager);
        sendStatus(taskId, "CAPTURE_STARTED", "Capture process started for task: " + taskId, 0);
        logger.info("Capture process started for task: {}", taskId);
    }
    
    private void startMigrationForTask(String taskId, TaskMessage taskMessage) throws Exception {
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
            MIGRATION_FULL_JAR_PATH, taskId, kafkaProducer,
            null, METADATA_DB_USER, METADATA_DB_PASSWORD, totalTables
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

    private void monitorCaptureProcesses() {
        for (Map.Entry<String, ProcessManager> entry : captureManagers.entrySet()) {
            String taskId = entry.getKey();

            if (pausedTasks.contains(taskId)) {
                logger.debug("Skipping monitoring for paused task: {}", taskId);
                continue;
            }

            ProcessManager captureManager = entry.getValue();

            try {
                captureManager.ensureRunning();
            } catch (Exception e) {
                if (!pausedTasks.contains(taskId)) {
                    logger.error("Error monitoring capture process for task: {}", taskId, e);
                }
            }
        }
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
    
    private int getProgressFromDatabase(String taskId) {
        String progressDbUrl = "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
        try (Connection conn = DriverManager.getConnection(progressDbUrl, METADATA_DB_USER, METADATA_DB_PASSWORD);
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
    
    private void recoverUnfinishedTasks() {
        logger.info("Starting to recover unfinished tasks...");
        
        try {
            List<RecoveryTask> unfinishedTasks = recoveryService.getUnfinishedTasks();
            
            if (unfinishedTasks.isEmpty()) {
                logger.info("No unfinished tasks found to recover");
                return;
            }
            
            int skipped = 0;
            for (RecoveryTask recoveryTask : unfinishedTasks) {
                try {
                    // 集群化后这道闸不能省：不加过滤的话，集群里<b>每台</b> agent 启动时都会把
                    // 所有未完成任务捞起来重跑一遍，等于人为制造双写。
                    // 归属别人且租约还有效的任务，交给它自己续跑。
                    if (agentRegistry != null && !agentRegistry.ownsTask(recoveryTask.getTaskId())) {
                        logger.info("任务 {} 归属其它 agent 且租约有效，跳过恢复", recoveryTask.getTaskId());
                        skipped++;
                        continue;
                    }
                    if (agentRegistry != null) {
                        agentRegistry.claimTask(recoveryTask.getTaskId());
                    }
                    recoverTask(recoveryTask);
                } catch (Exception e) {
                    logger.error("Error recovering task: {}", recoveryTask.getTaskId(), e);
                    sendStatus(recoveryTask.getTaskId(), "FAILED", 
                        "Failed to recover task: " + e.getMessage(), recoveryTask.getProgress());
                }
            }
            
            logger.info("Task recovery completed, recovered {} tasks（跳过 {} 个归属其它 agent 的）",
                    unfinishedTasks.size() - skipped, skipped);
            
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
        
        // 双向灾备反向影子通道：恢复时同样只做增量（capture 从 checkpoint/最新位点续传），
        // 绝不能落入全量恢复分支把灾备库反灌回主库
        if ("DR_SHADOW".equals(recoveryTask.getTaskType())) {
            logger.info("Task {} is a DR_SHADOW (bidirectional reverse channel), recovering as increment-only", taskId);
            startMigrationAgentThread(taskMessage, true);
            return;
        }

        if ("fullAndIncre".equals(migrationMode) || "subscribe".equals(migrationMode)) {
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
                startMigrationAgentThread(taskMessage, false);
                break;
                
            case "FULL_MIGRATING":
                logger.info("Task {} was in FULL_MIGRATING state (progress: {}%), resuming full migration", 
                    taskId, progress);
                startMigrationAgentThread(taskMessage, false);
                break;
                
            case "FULL_COMPLETED":
                logger.info("Task {} was in FULL_COMPLETED state, starting incremental sync", taskId);
                startMigrationAgentThread(taskMessage, true);
                break;
                
            case "INCREMENT_RUNNING":
                logger.info("Task {} was in INCREMENT_RUNNING state, resuming incremental sync from checkpoint", taskId);
                startMigrationAgentThread(taskMessage, true);
                break;

            case "SUBSCRIBE_RUNNING":
                logger.info("Task {} was in SUBSCRIBE_RUNNING state, resuming subscribe from checkpoint", taskId);
                startMigrationAgentThread(taskMessage, true);
                break;

            case "SWITCHING":
                logger.info("Task {} was in SWITCHING state (failover in progress), resuming incremental sync with skipFullMigration", taskId);
                startMigrationAgentThread(taskMessage, true);
                break;
                
            default:
                logger.warn("Unknown status {} for task {}, restarting from beginning", status, taskId);
                startMigrationAgentThread(taskMessage, false);
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
                    startMigrationForTask(taskId, taskMessage);
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
                    startMigrationForTask(taskId, taskMessage);
                    sendStatus(taskId, "STARTING", "Task recovered, starting migration", 0);
                } catch (Exception e) {
                    logger.error("Error starting migration for task: {}", taskId, e);
                    sendStatus(taskId, "FAILED", "Failed to start migration: " + e.getMessage(), 0);
                }
        }
    }
    
    private void startMigrationAgentThread(TaskMessage taskMessage, boolean skipFullMigration) {
        String taskId = taskMessage.getTaskId();
        
        MigrationAgentThread agentThread = new MigrationAgentThread(taskMessage, kafkaProducer, taskStateService, skipFullMigration);
        migrationAgentThreads.put(taskId, agentThread);
        
        Thread threadWrapper = new Thread(agentThread, "MigrationAgentThread-" + taskId);
        threadWrapper.setDaemon(true);
        migrationAgentThreadWrappers.put(taskId, threadWrapper);
        threadWrapper.start();
        
        logger.info("MigrationAgentThread started for recovered task: {}, skipFullMigration: {}", taskId, skipFullMigration);
    }

    public boolean isFailoverInProgress(String taskId) {
        return failoverInProgress.contains(taskId);
    }

    public void handleFailoverDirect(TaskMessage taskMessage) {
        // HTTP 直连触发的灾备切换：切换后状态置 INCREMENT_RUNNING，保留 30s 初始化窗口后释放去重令牌。
        performFailover(taskMessage, "HTTP API direct", "INCREMENT_RUNNING", true);
    }

    public void startIncrementDirect(TaskMessage taskMessage) {
        String taskId = taskMessage.getTaskId();
        logger.info("=== START INCREMENT DIRECT (HTTP API) for task: {} ===", taskId);

        if (migrationAgentThreads.containsKey(taskId)) {
            logger.warn("Task {} already has a running thread, stopping it first", taskId);
            stopMigrationAgentThread(taskId);
            stopTaskById(taskId);
        }

        try {
            configService.updateConfig(taskMessage);
            logger.info("Config updated for task: {}", taskId);

            TaskStateInfo stateInfo = new TaskStateInfo(taskId);
            stateInfo.setTaskName(taskMessage.getTaskName());
            stateInfo.setUserId(taskMessage.getUserId());
            stateInfo.setMigrationMode(taskMessage.getMigrationMode());
            stateInfo.setSourceConnection(taskMessage.getSourceConnection());
            stateInfo.setTargetConnection(taskMessage.getTargetConnection());
            stateInfo.setSourceType(taskMessage.getSourceType() != null ? taskMessage.getSourceType() : "mysql");
            stateInfo.setTargetType(taskMessage.getTargetType() != null ? taskMessage.getTargetType() : "mysql");
            stateInfo.setStatus("INCREMENT_RUNNING");
            stateInfo.setProgress(100);
            stateInfo.setCreatedAt(taskMessage.getCreatedAt() != null ? taskMessage.getCreatedAt() : java.time.LocalDateTime.now());
            taskStateService.saveTaskState(stateInfo);
            logger.info("Saved H2 state for task: {} with status INCREMENT_RUNNING", taskId);

            MigrationAgentThread agentThread = new MigrationAgentThread(taskMessage, kafkaProducer, taskStateService, true);
            migrationAgentThreads.put(taskId, agentThread);

            Thread threadWrapper = new Thread(agentThread, "MigrationAgentThread-Increment-" + taskId);
            threadWrapper.setDaemon(true);
            migrationAgentThreadWrappers.put(taskId, threadWrapper);
            threadWrapper.start();

            logger.info("Increment sync started for task: {} with skipFullMigration=true", taskId);

        } catch (Exception e) {
            logger.error("Error starting increment sync for task: {}", taskId, e);
            sendStatus(taskId, "FAILED", "Error starting increment sync: " + e.getMessage(), 0);
        }
    }

    public Map<String, Object> getAgentStatus() {
        Map<String, Object> status = new java.util.HashMap<>();
        status.put("activeTasks", migrationAgentThreads.size());
        status.put("pausedTasks", pausedTasks.size());
        status.put("captureProcesses", captureManagers.size());
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
}
