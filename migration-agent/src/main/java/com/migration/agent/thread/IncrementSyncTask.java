package com.migration.agent.thread;

import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import com.migration.agent.service.TaskStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 仅增量同步任务（从增量阶段恢复）。
 *
 * <p>执行流程：
 * <ol>
 *   <li>上报 INCREMENT_RUNNING 状态</li>
 *   <li>启动 capture 进程</li>
 *   <li>启动 extract 进程</li>
 *   <li>启动 increment 进程</li>
 *   <li>进入持续监控模式</li>
 * </ol>
 */
public class IncrementSyncTask extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(IncrementSyncTask.class);

    public IncrementSyncTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                             TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, true, config);
    }

    @Override
    protected String getRunningStatus() {
        return "INCREMENT_RUNNING";
    }

    @Override
    protected void doRun() throws Exception {
        String threadName = "IncrementSyncTask-" + taskId;

        Thread.interrupted(); // 清除中断标志
        lastSuccessfulStatus = "INCREMENT_RUNNING";
        sendStatus("INCREMENT_RUNNING", "从增量同步阶段恢复", 100);

        if (!startCaptureProcess()) {
            logger.error("[{}] capture进程启动失败，增量同步无法继续", threadName);
            sendFailedStatus("E3001", "capture 进程启动失败，增量同步无法继续");
            stopped.set(true);
            return;
        }

        if (!startExtractProcess()) {
            logger.error("[{}] extract进程启动失败，增量同步无法继续", threadName);
            sendFailedStatus("E3002", "extract 进程启动失败，增量同步无法继续");
            stopped.set(true);
            return;
        }

        if (!startIncrementProcess()) {
            logger.warn("[{}] increment进程启动失败，ProcessGuard将负责重试", threadName);
        }

        logger.info("[{}] 增量同步任务恢复完成，进入持续监控模式", threadName);
    }

    @Override
    protected boolean checkProcessHealth() {
        String threadName = "IncrementSyncTask-" + taskId;

        if (captureGuard != null && !captureGuard.isGuarding() && !captureGuard.isRunning()) {
            logger.error("[{}] capture 进程已停止且 ProcessGuard 已放弃守护", threadName);
            return false;
        }
        if (extractGuard != null && !extractGuard.isGuarding() && !extractGuard.isRunning()) {
            logger.error("[{}] extract 进程已停止且 ProcessGuard 已放弃守护", threadName);
            return false;
        }
        if (incrementGuard != null && !incrementGuard.isGuarding() && !incrementGuard.isRunning()) {
            logger.error("[{}] increment 进程已停止且 ProcessGuard 已放弃守护", threadName);
            return false;
        }

        boolean captureAlive = captureGuard == null || captureGuard.isRunning();
        boolean extractAlive = extractGuard == null || extractGuard.isRunning();
        boolean incrementAlive = incrementGuard == null || incrementGuard.isRunning();

        if (!captureAlive && !extractAlive && !incrementAlive) {
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
            statusMessage.setStatus("INCREMENT_RUNNING");
            statusMessage.setMessage("增量同步中");
            statusMessage.setProgress(100);

            Long rpoMs = readMetricFile("./files/" + taskId + "/binlog_output/rpo_metric");
            Long rtoMs = readMetricFile("./files/" + taskId + "/binlog_output/rto_metric");
            Long calculatedRpo = calculateRpo();
            if (calculatedRpo != null) rpoMs = calculatedRpo;
            statusMessage.setRpoMs(rpoMs);
            statusMessage.setRtoMs(rtoMs);

            kafkaProducer.sendStatus(statusMessage);
            logger.debug("[{}] Periodic metrics update: rpoMs={}, rtoMs={}", taskId, rpoMs, rtoMs);
        } catch (Exception e) {
            logger.debug("[{}] Error sending periodic metrics update", taskId, e);
        }
    }
}
