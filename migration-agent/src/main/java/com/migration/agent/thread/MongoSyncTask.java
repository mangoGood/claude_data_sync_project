package com.migration.agent.thread;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.resilience.ProcessGuard;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import com.migration.agent.service.TaskStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

/**
 * MongoDB 副本集 → 副本集 同步任务执行器。
 *
 * <p>与 SQL 任务的 capture/extract/increment 三进程管线不同，Mongo 同步是单个
 * migration-mongo 子进程（全量集合复制 + Change Streams 增量，见 MongoSyncMain）。
 * 本执行器复用既有任务编排（状态上报/ProcessGuard 守护/暂停终止），通过轮询子进程
 * 写出的 files/{taskId}/mongo_progress.json 把两阶段进度映射到统一的任务状态机：
 * FULL_MIGRATING(进度) → FULL_COMPLETED（仅全量即终态）/ INCREMENT_RUNNING（增量长驻）。
 */
public class MongoSyncTask extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(MongoSyncTask.class);
    private static final Gson gson = new Gson();

    private ProcessGuard mongoGuard;
    private String lastReportedPhase = "";

    public MongoSyncTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                         TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "INCREMENT_RUNNING";
    }

    @Override
    protected void doRun() throws Exception {
        String threadName = "MongoSyncTask-" + taskId;
        logger.info("[{}] 开始执行 Mongo 同步任务, mode={}", threadName, migrationMode);
        sendStatus("STARTING", "Mongo 同步任务启动中", 0);

        mongoGuard = new ProcessGuard("mongo", taskId, config, kafkaProducer,
                () -> {
                    ProcessManager pm = new ProcessManager(config.getMongoJarPath(), "MongoSyncMain-" + taskId);
                    pm.setTaskId(taskId);
                    return pm;
                }, getRunningStatus());

        if (!mongoGuard.startAndGuard()) {
            // 首次「就绪等待」窗口内进程夭折（启动即崩溃 / 早期被 kill）不再直接判死：此时 ProcessGuard
            // 守护线程已启动，会按熔断+重试策略自动重启恢复（与增量阶段崩溃走同一恢复路径）。真正无法
            // 启动时，guard 重试耗尽后 monitorLoop 的健康检查（!isGuarding() && !isRunning()）会上报
            // FAILED。此前直接 sendFailedStatus+stopped 会让守护线程因 stopped=true 立即退出、
            // 漏掉「启动窗口内崩溃」这一本可自愈的场景（全量拷贝极快时尤其容易命中）。
            logger.warn("[{}] mongo 首次启动就绪等待未通过，转入 ProcessGuard 自愈恢复", threadName);
        }

        // 仅增量（灾备反向影子通道 / 主备倒换后重启）压根不跑全量，别谎报"全量同步中"——
        // 那会让灾备详情页显示一个永远不会推进的全量进度。
        if (isIncrementOnly()) {
            sendStatus("INCREMENT_RUNNING", "Mongo 增量同步中（Change Streams）", 100);
            lastSuccessfulStatus = "INCREMENT_RUNNING";
        } else {
            sendStatus("FULL_MIGRATING", "Mongo 全量同步中", 0);
            lastSuccessfulStatus = "FULL_MIGRATING";
        }
        logger.info("[{}] mongo 同步进程已启动，进入进度监控 (incrementOnly={})", threadName, isIncrementOnly());
    }

    /**
     * 是否是"禁止全量"的通道。取自 config.properties 而不是 taskMessage：DR_SHADOW（ConfigService 写入）
     * 与主备倒换后重启（AgentMain 倒换时写入）两条路径都只在配置里留下这个标记。
     */
    private boolean isIncrementOnly() {
        java.util.Properties props = new java.util.Properties();
        File configFile = new File("files/" + taskId + "/config.properties");
        if (!configFile.exists()) {
            return false;
        }
        try (java.io.InputStream in = new java.io.FileInputStream(configFile)) {
            props.load(in);
        } catch (Exception e) {
            logger.debug("[{}] 读取 config.properties 失败: {}", taskId, e.getMessage());
            return false;
        }
        return Boolean.parseBoolean(props.getProperty("migration.increment.only", "false"));
    }

    /** 自定义监控循环：轮询 mongo_progress.json 映射任务状态，替代基类的 SQL 管线健康检查。 */
    @Override
    protected void monitorLoop(MetricsService.TaskMetrics taskMetrics) {
        String threadName = "MongoSyncTask-" + taskId;
        boolean fullOnly = !"fullAndIncre".equals(migrationMode);

        while (!stopped.get()) {
            try {
                Thread.sleep(5000);
                if (stopped.get()) {
                    break;
                }

                Map<String, Object> progress = readProgress();
                if (progress != null) {
                    String phase = String.valueOf(progress.getOrDefault("phase", ""));
                    int total = ((Number) progress.getOrDefault("totalCollections", 0)).intValue();
                    int done = ((Number) progress.getOrDefault("completedCollections", 0)).intValue();
                    String current = String.valueOf(progress.getOrDefault("currentCollection", ""));
                    long copied = ((Number) progress.getOrDefault("copiedDocs", 0)).longValue();

                    switch (phase) {
                        case "FULL": {
                            int pct = total > 0 ? (done * 100) / total : 0;
                            sendStatus("FULL_MIGRATING", "Mongo 全量同步中", pct,
                                    total, done, current.isEmpty() ? null : current, pct, copied, 0L);
                            break;
                        }
                        case "INCREMENT": {
                            if (!"INCREMENT".equals(lastReportedPhase)) {
                                lastSuccessfulStatus = "INCREMENT_RUNNING";
                                sendStatus("INCREMENT_RUNNING", "Mongo 增量同步中（Change Streams）", 100,
                                        total, total, null, 100, copied, 0L);
                            }
                            // 僵死看门狗：增量阶段 mongo 引擎靠 Change Streams 每 ~5s 的兜底 flush
                            // 刷新 mongo_progress.json；进程仍存活（isRunning=true）但进度文件长时间
                            // 不刷新 = 引擎冻结/死锁，上报失败（与 RedisSyncTask 同构）。
                            if (mongoGuard != null && mongoGuard.isRunning() && incrementProgressStalled()) {
                                logger.error("[{}] Mongo 增量引擎僵死：进程存活但进度文件长时间未刷新，判定失败", threadName);
                                sendFailedStatus("E3005", "Mongo 同步管线僵死：进程存活但长时间无进展（疑似死锁/阻塞/冻结）");
                                stopped.set(true);
                            }
                            break;
                        }
                        case "DONE": {
                            logger.info("[{}] Mongo 仅全量任务完成", threadName);
                            sendStatus("FULL_COMPLETED", "Mongo 全量同步完成", 100,
                                    total, total, null, 100, copied, 0L);
                            stopped.set(true);
                            break;
                        }
                        case "FAILED": {
                            String err = String.valueOf(progress.getOrDefault("error", "未知错误"));
                            logger.error("[{}] Mongo 同步进程报告失败: {}", threadName, err);
                            sendFailedStatus("E3002", "Mongo 同步失败: " + err);
                            stopped.set(true);
                            break;
                        }
                        default:
                            break;
                    }
                    lastReportedPhase = phase;
                }

                if (stopped.get()) {
                    break;
                }

                // 进程健康：guard 放弃守护且进程不在 → 任务失败
                if (mongoGuard != null && !mongoGuard.isGuarding() && !mongoGuard.isRunning()) {
                    // 仅全量模式下进程正常退出（exit 0 且 phase=DONE）已在上面处理；
                    // 走到这里说明进程消亡且未报告 DONE —— 视作异常
                    Map<String, Object> last = readProgress();
                    String lastPhase = last != null ? String.valueOf(last.getOrDefault("phase", "")) : "";
                    if ("DONE".equals(lastPhase)) {
                        sendStatus("FULL_COMPLETED", "Mongo 全量同步完成", 100);
                    } else {
                        logger.error("[{}] mongo 同步进程已停止且 ProcessGuard 已放弃守护", threadName);
                        sendFailedStatus("E3002", "mongo 同步进程异常退出且无法恢复");
                    }
                    stopped.set(true);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Mongo 监控循环异常", threadName, e);
            }
        }
    }

    @Override
    protected boolean checkProcessHealth() {
        return mongoGuard == null || mongoGuard.isGuarding() || mongoGuard.isRunning();
    }

    @Override
    protected void sendPeriodicMetricsUpdate(MetricsService.TaskMetrics taskMetrics) {
        // Mongo 同步不接 SQL 管线的 RPO/RTO 指标文件；状态与进度已由自定义 monitorLoop
        // 通过 mongo_progress.json 上报，这里无需周期性指标推送。
    }

    @Override
    protected void stopAllProcesses() {
        if (mongoGuard != null) {
            try {
                mongoGuard.stop();
            } catch (Exception e) {
                logger.warn("[{}] 停止 mongo 同步进程失败: {}", taskId, e.getMessage());
            }
        }
        super.stopAllProcesses();
    }

    /** mongo_progress.json 上次 mtime 及其推进时刻，用于增量阶段僵死判定。 */
    private long lastProgressMtime = 0L;
    private long lastProgressAdvanceTime = 0L;

    /** 进度文件超过 {@link AgentConfig#getStallThresholdMs()} 未刷新则判僵死（文件缺失时重置基线、不误判）。 */
    private boolean incrementProgressStalled() {
        File f = new File("files/" + taskId + "/mongo_progress.json");
        long now = System.currentTimeMillis();
        if (!f.exists()) {
            lastProgressAdvanceTime = 0L;
            return false;
        }
        long mtime = f.lastModified();
        if (lastProgressAdvanceTime == 0L || mtime != lastProgressMtime) {
            lastProgressMtime = mtime;
            lastProgressAdvanceTime = now;
            return false;
        }
        return (now - lastProgressAdvanceTime) >= config.getStallThresholdMs();
    }

    private Map<String, Object> readProgress() {
        try {
            File f = new File("files/" + taskId + "/mongo_progress.json");
            if (!f.exists()) {
                return null;
            }
            String json = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            return gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (Exception e) {
            logger.debug("[{}] 读取 mongo 进度文件失败: {}", taskId, e.getMessage());
            return null;
        }
    }
}
