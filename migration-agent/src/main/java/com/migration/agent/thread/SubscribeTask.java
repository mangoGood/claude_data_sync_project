package com.migration.agent.thread;

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

    /**
     * subscribe 子进程同样交给 ProcessGuard 守护。
     *
     * <p>此前它是裸的 ProcessManager：进程一崩就再也没人拉起，而 capture/extract 还活着，
     * {@link #checkProcessHealth()} 于是继续返回 true，任务一直显示"数据订阅中"，
     * 实际上一条数据都不再进 Kafka，也永远不会上报失败。
     */
    private ProcessGuard subscribeGuard;

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
        // subscribe 是订阅链路唯一的出口：它不工作 = Kafka 收不到任何数据。
        // 因此和 capture/extract 一样，守护放弃即判任务失败，不能只打条 warn 就当无事发生。
        if (subscribeGuard != null && !subscribeGuard.isGuarding() && !subscribeGuard.isRunning()) {
            logger.error("[{}] subscribe 进程已停止且 ProcessGuard 已放弃守护", threadName);
            return false;
        }
        return true;
    }

    /**
     * 订阅链路的活性文件：capture / extract / subscribe 各看自己直接刷的那个文件。
     *
     * <ul>
     *   <li>{@code capture_liveness} —— capture 基类活性线程每 ~3s 改写（源无关）；</li>
     *   <li>{@code capture_queue_depth} —— extract 每轮扫描（~1s）无条件改写；</li>
     *   <li>{@code subscribe_liveness} —— subscribe 主循环每轮 + 处理大文件时每 ~2s 改写。</li>
     * </ul>
     *
     * <p>不能只看 {@code subscribe_rto_ms}：它只在碰到带源时间戳的事件时才写，空闲时段本就不更新，
     * 拿它判僵死会误杀；而且上游 capture 冻结后 subscribe 还会把积压排空、rto 继续推进，
     * 会把上游冻结掩盖掉。
     */
    @Override
    protected java.util.List<String> stallLivenessFiles() {
        String dir = "./files/" + taskId + "/binlog_output/";
        return java.util.Arrays.asList(
                dir + "capture_liveness", dir + "capture_queue_depth", dir + "subscribe_liveness");
    }

    /** subscribe 进程正在被 ProcessGuard 重启时，只跳过它自己那个活性文件（capture/extract 照常判定）。 */
    @Override
    protected boolean livenessOwnerRestarting(String path) {
        if (path.endsWith("subscribe_liveness")) {
            return subscribeGuard != null && !subscribeGuard.isRunning();
        }
        return super.livenessOwnerRestarting(path);
    }

    /** subscribe_liveness 缺失只有在 subscribe 进程确实活着时才算僵死信号（启动即冻结也能检出）。 */
    @Override
    protected boolean livenessFileExpected(String path) {
        if (path.endsWith("subscribe_liveness")) {
            return subscribeGuard != null && subscribeGuard.isRunning();
        }
        return super.livenessFileExpected(path);
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

            attachSlaMetrics(statusMessage);

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
            subscribeGuard = new ProcessGuard("subscribe", taskId, config, kafkaProducer,
                    () -> new ProcessManager(config.getSubscribeJarPath(),
                            "ContinuousSubscribeMain-" + taskId, taskId),
                    getRunningStatus());

            boolean started = subscribeGuard.startAndGuard();
            if (!started) {
                // 同 capture/extract：启动窗口内没就绪不等于起不来，守护线程会继续按退避重启，
                // 此处不能把任务判死（判死会 stopped=true，守护线程随即退出、进程永不再拉起）。
                logger.warn("[{}] subscribe 进程未在启动窗口内就绪，交由 ProcessGuard 继续重启", threadName);
                return subscribeGuard.isGuarding();
            }
            logger.info("[{}] subscribe 进程启动成功", threadName);
            return true;
        } catch (Exception e) {
            logger.error("[{}] 启动 subscribe 进程失败", threadName, e);
            return false;
        }
    }

    @Override
    protected void stopExtraProcesses(String threadName) {
        if (subscribeGuard != null) {
            try {
                subscribeGuard.stop();
                logger.info("[{}] subscribe 进程已停止", threadName);
            } catch (Exception e) {
                logger.error("[{}] 停止 subscribe 进程失败", threadName, e);
            }
        }
    }
}
