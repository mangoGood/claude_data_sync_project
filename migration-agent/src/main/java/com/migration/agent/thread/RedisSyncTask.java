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
 * Redis → Redis 同步任务执行器。
 *
 * <p>与 SQL 任务的 capture/extract/increment 三进程管线不同，Redis 同步是单个 migration-redis
 * 子进程（PSYNC 全量 RDB + 复制命令流增量，见 RedisSyncMain）。本执行器复用既有任务编排
 * （状态上报/ProcessGuard 守护/暂停终止），通过轮询子进程写出的
 * files/{taskId}/redis_progress.json 把两阶段进度映射到统一的任务状态机：
 * FULL_MIGRATING(进度) → FULL_COMPLETED（仅全量即终态）/ INCREMENT_RUNNING（增量长驻）。
 *
 * <p>结构与 {@link MongoSyncTask} 一致（同为单进程引擎），仅进度字段口径不同（键数 vs 集合数）。
 */
public class RedisSyncTask extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RedisSyncTask.class);
    private static final Gson gson = new Gson();

    private ProcessGuard redisGuard;
    private String lastReportedPhase = "";

    public RedisSyncTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                         TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "INCREMENT_RUNNING";
    }

    @Override
    protected void doRun() throws Exception {
        String threadName = "RedisSyncTask-" + taskId;
        logger.info("[{}] 开始执行 Redis 同步任务, mode={}", threadName, migrationMode);
        sendStatus("STARTING", "Redis 同步任务启动中", 0);

        redisGuard = new ProcessGuard("redis", taskId, config, kafkaProducer,
                () -> {
                    ProcessManager pm = new ProcessManager(config.getRedisJarPath(), "RedisSyncMain-" + taskId);
                    pm.setTaskId(taskId);
                    return pm;
                }, getRunningStatus());

        if (!redisGuard.startAndGuard()) {
            // 仅全量的小数据可能在 ProcessGuard 的启动确认窗口（waitForStartup 每 5s 探一次
            // isRunning）内就完成 RESTORE 并正常退出（exit 0, phase=DONE）——这不是启动失败。
            // 读进度文件甄别：DONE=成功收尾；FAILED=引擎报错；否则才是真正的启动失败。
            Map<String, Object> p = readProgress();
            String ph = p != null ? String.valueOf(p.getOrDefault("phase", "")) : "";
            if ("DONE".equals(ph)) {
                long copied = ((Number) p.getOrDefault("copiedKeys", 0)).longValue();
                long total = ((Number) p.getOrDefault("totalKeys", 0)).longValue();
                logger.info("[{}] Redis 仅全量任务在启动确认窗口内已完成，copied={}", threadName, copied);
                sendStatus("FULL_COMPLETED", "Redis 全量同步完成", 100,
                        (int) total, (int) total, null, 100, copied, total);
            } else if ("FAILED".equals(ph)) {
                sendFailedStatus("E3002", "Redis 同步失败: " + p.getOrDefault("error", "未知错误"));
            } else {
                sendFailedStatus("E3001", "redis 同步进程启动失败");
            }
            stopped.set(true);
            return;
        }

        sendStatus("FULL_MIGRATING", "Redis 全量同步中", 0);
        lastSuccessfulStatus = "FULL_MIGRATING";
        logger.info("[{}] redis 同步进程已启动，进入进度监控", threadName);
    }

    /** 自定义监控循环：轮询 redis_progress.json 映射任务状态，替代基类的 SQL 管线健康检查。 */
    @Override
    protected void monitorLoop(MetricsService.TaskMetrics taskMetrics) {
        String threadName = "RedisSyncTask-" + taskId;

        while (!stopped.get()) {
            try {
                Thread.sleep(5000);
                if (stopped.get()) {
                    break;
                }

                Map<String, Object> progress = readProgress();
                if (progress != null) {
                    String phase = String.valueOf(progress.getOrDefault("phase", ""));
                    long total = ((Number) progress.getOrDefault("totalKeys", 0)).longValue();
                    long copied = ((Number) progress.getOrDefault("copiedKeys", 0)).longValue();
                    long currentDb = ((Number) progress.getOrDefault("currentDb", 0)).longValue();

                    switch (phase) {
                        case "FULL": {
                            int pct = total > 0 ? (int) Math.min(100, (copied * 100) / total) : 0;
                            sendStatus("FULL_MIGRATING", "Redis 全量同步中", pct,
                                    (int) total, (int) copied, "db" + currentDb, pct, copied, total);
                            break;
                        }
                        case "INCREMENT": {
                            if (!"INCREMENT".equals(lastReportedPhase)) {
                                lastSuccessfulStatus = "INCREMENT_RUNNING";
                                sendStatus("INCREMENT_RUNNING", "Redis 增量同步中（复制命令流）", 100,
                                        (int) total, (int) total, null, 100, copied, total);
                            }
                            // 僵死看门狗：增量阶段 redis 引擎会靠 PSYNC 心跳按时间兜底刷新进度文件，
                            // 进程仍存活（isRunning=true）但进度文件长时间不更新 = 引擎冻结/死锁，上报失败。
                            if (redisGuard != null && redisGuard.isRunning() && incrementProgressStalled()) {
                                logger.error("[{}] Redis 增量引擎僵死：进程存活但进度文件长时间未刷新，判定失败", threadName);
                                sendFailedStatus("E3005", "Redis 同步管线僵死：进程存活但长时间无进展（疑似死锁/阻塞/冻结）");
                                stopped.set(true);
                            }
                            break;
                        }
                        case "DONE": {
                            logger.info("[{}] Redis 仅全量任务完成", threadName);
                            sendStatus("FULL_COMPLETED", "Redis 全量同步完成", 100,
                                    (int) total, (int) total, null, 100, copied, total);
                            stopped.set(true);
                            break;
                        }
                        case "FAILED": {
                            String err = String.valueOf(progress.getOrDefault("error", "未知错误"));
                            logger.error("[{}] Redis 同步进程报告失败: {}", threadName, err);
                            sendFailedStatus("E3002", "Redis 同步失败: " + err);
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
                if (redisGuard != null && !redisGuard.isGuarding() && !redisGuard.isRunning()) {
                    Map<String, Object> last = readProgress();
                    String lastPhase = last != null ? String.valueOf(last.getOrDefault("phase", "")) : "";
                    if ("DONE".equals(lastPhase)) {
                        sendStatus("FULL_COMPLETED", "Redis 全量同步完成", 100);
                    } else {
                        logger.error("[{}] redis 同步进程已停止且 ProcessGuard 已放弃守护", threadName);
                        sendFailedStatus("E3002", "redis 同步进程异常退出且无法恢复");
                    }
                    stopped.set(true);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Redis 监控循环异常", threadName, e);
            }
        }
    }

    @Override
    protected boolean checkProcessHealth() {
        return redisGuard == null || redisGuard.isGuarding() || redisGuard.isRunning();
    }

    @Override
    protected void sendPeriodicMetricsUpdate(MetricsService.TaskMetrics taskMetrics) {
        // Redis 同步不接 SQL 管线的 RPO/RTO 指标文件；状态与进度已由自定义 monitorLoop
        // 通过 redis_progress.json 上报，这里无需周期性指标推送。
    }

    @Override
    protected void stopAllProcesses() {
        if (redisGuard != null) {
            try {
                redisGuard.stop();
            } catch (Exception e) {
                logger.warn("[{}] 停止 redis 同步进程失败: {}", taskId, e.getMessage());
            }
        }
        super.stopAllProcesses();
    }

    /** redis_progress.json 上次 mtime 及其推进时刻，用于增量阶段僵死判定。 */
    private long lastProgressMtime = 0L;
    private long lastProgressAdvanceTime = 0L;

    /** 进度文件超过 {@link AgentConfig#getStallThresholdMs()} 未刷新则判僵死（文件缺失时重置基线、不误判）。 */
    private boolean incrementProgressStalled() {
        File f = new File("files/" + taskId + "/redis_progress.json");
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
            File f = new File("files/" + taskId + "/redis_progress.json");
            if (!f.exists()) {
                return null;
            }
            String json = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            return gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (Exception e) {
            logger.debug("[{}] 读取 redis 进度文件失败: {}", taskId, e.getMessage());
            return null;
        }
    }
}
