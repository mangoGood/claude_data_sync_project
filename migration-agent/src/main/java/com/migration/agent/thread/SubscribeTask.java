package com.migration.agent.thread;

import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import com.migration.agent.service.TaskStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 订阅任务（数据订阅）。
 *
 * <p>执行流程：
 * <ol>
 *   <li>初始化 checkpoint</li>
 *   <li>启动 capture 进程</li>
 *   <li>启动 extract 进程</li>
 *   <li>启动 subscribe 进程</li>
 *   <li>进入持续监控模式</li>
 * </ol>
 *
 * <p>订阅任务不执行全量迁移，也不启动 increment 进程。
 */
public class SubscribeTask extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SubscribeTask.class);

    private ProcessManager subscribeProcess;

    public SubscribeTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                         TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "SUBSCRIBE_RUNNING";
    }

    @Override
    protected void doRun() throws Exception {
        String threadName = "SubscribeTask-" + taskId;
        logger.info("[{}] 开始执行订阅任务流程", threadName);
        sendStatus("STARTING", "订阅任务启动中", 0);

        if (!initCheckpoint()) {
            stopped.set(true);
            return;
        }

        if (!startCaptureProcess()) {
            stopped.set(true);
            return;
        }

        if (!startExtractProcess()) {
            stopped.set(true);
            return;
        }

        if (!startSubscribeProcess()) {
            logger.warn("[{}] subscribe进程启动失败，ProcessGuard将负责重试", threadName);
        }

        lastSuccessfulStatus = "SUBSCRIBE_RUNNING";
        sendStatus("SUBSCRIBE_RUNNING", "数据订阅中", 100);
        logger.info("[{}] 订阅任务启动完成，进入持续监控模式", threadName);
    }

    @Override
    protected boolean checkProcessHealth() {
        String threadName = "SubscribeTask-" + taskId;

        if (captureGuard != null && !captureGuard.isGuarding() && !captureGuard.isRunning()) {
            logger.error("[{}] capture 进程已停止且 ProcessGuard 已放弃守护", threadName);
            return false;
        }
        if (extractGuard != null && !extractGuard.isGuarding() && !extractGuard.isRunning()) {
            logger.error("[{}] extract 进程已停止且 ProcessGuard 已放弃守护", threadName);
            return false;
        }
        if (subscribeProcess != null && !subscribeProcess.isRunning()) {
            logger.warn("[{}] subscribe 进程已停止", threadName);
        }

        boolean captureAlive = captureGuard == null || captureGuard.isRunning();
        boolean extractAlive = extractGuard == null || extractGuard.isRunning();
        boolean subscribeAlive = subscribeProcess == null || subscribeProcess.isRunning();

        if (!captureAlive && !extractAlive && !subscribeAlive) {
            logger.error("[{}] 所有进程均不运行，数据流完全中断", threadName);
            return false;
        }
        return true;
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

            Long rtoMs = readMetricFile("./files/" + taskId + "/metrics/subscribe_rto_ms");
            statusMessage.setRtoMs(rtoMs);

            kafkaProducer.sendStatus(statusMessage);
            logger.debug("[{}] Subscribe metrics update: rtoMs={}", taskId, rtoMs);
        } catch (Exception e) {
            logger.debug("[{}] Error sending subscribe metrics update", taskId, e);
        }
    }

    private boolean startSubscribeProcess() {
        String threadName = "SubscribeTask-" + taskId;
        logger.info("[{}] 启动 subscribe 进程", threadName);

        try {
            subscribeProcess = new ProcessManager(config.getSubscribeJarPath(), "ContinuousSubscribeMain-" + taskId, taskId);
            subscribeProcess.start();
            logger.info("[{}] subscribe 进程已启动", threadName);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 启动 subscribe 进程失败", threadName, e);
            return false;
        }
    }

    @Override
    protected void stopExtraProcesses(String threadName) {
        if (subscribeProcess != null) {
            try {
                subscribeProcess.stop();
                logger.info("[{}] subscribe 进程已停止", threadName);
            } catch (Exception e) {
                logger.error("[{}] 停止 subscribe 进程失败", threadName, e);
            }
        }
    }
}
