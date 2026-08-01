package com.migration.agent.thread;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
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
 * MongoDB → Kafka 数据订阅任务执行器。
 *
 * <p>SQL 源的订阅是 capture + extract + subscribe 三进程（见 {@link SubscribeTask}）；MongoDB
 * 没有可落成 THL 的物理日志，变更出口就是 Change Streams，因此订阅由单个 migration-mongo
 * 子进程完成（MongoSubscribeMain 直接把变更投递到 Kafka）。
 *
 * <p>结构与 {@link MongoSyncTask} 同构（同为单进程引擎 + 进度文件轮询），差别只在阶段映射：
 * 订阅没有全量阶段，进程起来即 SUBSCRIBE_RUNNING 长驻。
 */
public class MongoSubscribeTask extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(MongoSubscribeTask.class);
    private static final Gson gson = new Gson();

    private ProcessGuard mongoGuard;

    public MongoSubscribeTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                              TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "SUBSCRIBE_RUNNING";
    }

    @Override
    protected void doRun() throws Exception {
        String threadName = "MongoSubscribeTask-" + taskId;
        logger.info("[{}] 开始执行 Mongo 订阅任务", threadName);
        sendStatus("STARTING", "Mongo 订阅任务启动中", 0);

        mongoGuard = new ProcessGuard("mongo-subscribe", taskId, config, kafkaProducer,
                () -> {
                    ProcessManager pm = new ProcessManager(config.getMongoJarPath(), "MongoSubscribeMain-" + taskId);
                    pm.setTaskId(taskId);
                    return pm;
                }, getRunningStatus());

        if (!mongoGuard.startAndGuard()) {
            // 与其它受守护引擎一致：启动就绪窗口内夭折不判死，交给 ProcessGuard 按退避重启自愈；
            // 真正起不来时 monitorLoop 的健康检查会上报 FAILED。
            logger.warn("[{}] mongo 订阅进程首次启动就绪等待未通过，转入 ProcessGuard 自愈恢复", threadName);
        }

        lastSuccessfulStatus = "SUBSCRIBE_RUNNING";
        sendStatus("SUBSCRIBE_RUNNING", "数据订阅中", 100);
        logger.info("[{}] mongo 订阅进程已启动，进入持续监控", threadName);
    }

    /** 自定义监控循环：轮询 mongo_progress.json 判定失败/僵死，替代基类的 SQL 管线健康检查。 */
    @Override
    protected void monitorLoop(MetricsService.TaskMetrics taskMetrics) {
        String threadName = "MongoSubscribeTask-" + taskId;

        while (!stopped.get()) {
            try {
                Thread.sleep(5000);
                if (stopped.get()) {
                    break;
                }

                Map<String, Object> progress = readProgress();
                if (progress != null && "FAILED".equals(String.valueOf(progress.getOrDefault("phase", "")))) {
                    String err = String.valueOf(progress.getOrDefault("error", "未知错误"));
                    logger.error("[{}] Mongo 订阅进程报告失败: {}", threadName, err);
                    sendFailedStatus("E3002", "Mongo 订阅失败: " + err);
                    stopped.set(true);
                    break;
                }

                // 僵死看门狗：进程仍存活（isRunning=true）但活性文件长时间不刷新 = 引擎冻结/死锁。
                // 看的是 subscribe_liveness（主循环每轮无条件刷），不是 subscribe_rto_ms
                // ——后者只在碰到带时间戳的事件时才写，空闲时段本就不更新，拿它判僵死会误杀。
                if (mongoGuard != null && mongoGuard.isRunning() && livenessStalled()) {
                    logger.error("[{}] Mongo 订阅引擎僵死：进程存活但长时间无活性心跳，判定失败", threadName);
                    sendFailedStatus("E3005", "Mongo 订阅管线僵死：进程存活但长时间无进展（疑似死锁/阻塞/冻结）");
                    stopped.set(true);
                    break;
                }

                if (mongoGuard != null && !mongoGuard.isGuarding() && !mongoGuard.isRunning()) {
                    logger.error("[{}] mongo 订阅进程已停止且 ProcessGuard 已放弃守护", threadName);
                    sendFailedStatus("E3002", "mongo 订阅进程异常退出且无法恢复");
                    stopped.set(true);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Mongo 订阅监控循环异常", threadName, e);
            }
        }
    }

    @Override
    protected boolean checkProcessHealth() {
        return mongoGuard == null || mongoGuard.isGuarding() || mongoGuard.isRunning();
    }

    @Override
    protected void sendPeriodicMetricsUpdate(MetricsService.TaskMetrics taskMetrics) {
        long now = System.currentTimeMillis();
        if (now - lastMetricsReportTime < METRICS_REPORT_INTERVAL_MS) return;
        lastMetricsReportTime = now;

        try {
            TaskStatusMessage statusMessage = new TaskStatusMessage();
            statusMessage.setTaskId(taskId);
            statusMessage.setStatus("SUBSCRIBE_RUNNING");
            statusMessage.setMessage("数据订阅中");
            statusMessage.setProgress(100);
            statusMessage.setRtoMs(readMetricFile("./files/" + taskId + "/metrics/subscribe_rto_ms"));
            attachSlaMetrics(statusMessage);

            kafkaProducer.sendStatus(statusMessage);
        } catch (Exception e) {
            logger.debug("[{}] Error sending mongo subscribe metrics update", taskId, e);
        }
    }

    @Override
    protected void stopAllProcesses() {
        if (mongoGuard != null) {
            try {
                mongoGuard.stop();
            } catch (Exception e) {
                logger.warn("[{}] 停止 mongo 订阅进程失败: {}", taskId, e.getMessage());
            }
        }
        super.stopAllProcesses();
    }

    private long lastLivenessMtime = 0L;
    private long lastLivenessAdvanceTime = 0L;

    /** 活性文件超过 {@link AgentConfig#getStallThresholdMs()} 未刷新则判僵死（文件缺失时重置基线）。 */
    private boolean livenessStalled() {
        File f = new File("files/" + taskId + "/binlog_output/subscribe_liveness");
        long now = System.currentTimeMillis();
        if (!f.exists()) {
            lastLivenessAdvanceTime = 0L;
            return false;
        }
        long mtime = f.lastModified();
        if (lastLivenessAdvanceTime == 0L || mtime != lastLivenessMtime) {
            lastLivenessMtime = mtime;
            lastLivenessAdvanceTime = now;
            return false;
        }
        return (now - lastLivenessAdvanceTime) >= config.getStallThresholdMs();
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
