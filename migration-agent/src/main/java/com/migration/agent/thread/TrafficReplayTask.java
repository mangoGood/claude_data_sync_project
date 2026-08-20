package com.migration.agent.thread;

import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.resilience.ProcessGuard;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import com.migration.agent.service.TaskStateService;
import com.migration.agent.service.TrafficRecordingFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 流量回放任务：单个 {@code migration-traffic --mode replay} 子进程。
 *
 * <p>启动前要先把<b>录制文件备齐</b>：集群里录制躺在产出它的那台 agent 上，
 * 回放任务可能被指派到另一台。见 {@link TrafficRecordingFetcher}。
 */
public class TrafficReplayTask extends AbstractTaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(TrafficReplayTask.class);

    private ProcessGuard replayProcessGuard;

    public TrafficReplayTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                             TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "TRAFFIC_REPLAYING";
    }

    /** 回放的进度是"录制读到第几条"，由子进程自己记，不走位点体系。 */
    @Override
    protected boolean usesCheckpoint() {
        return false;
    }

    @Override
    protected void doRun() throws Exception {
        logger.info("[{}] 开始执行流量回放任务", taskId);
        sendStatus("STARTING", "流量回放启动中", 0);

        Properties cfg = loadTaskConfig();
        try {
            new TrafficRecordingFetcher(config).ensureAvailable(taskId, cfg);
        } catch (Exception e) {
            stopped.set(true);
            sendFailedStatus("E3123", "录制文件不可用: " + e.getMessage());
            return;
        }

        replayProcessGuard = new ProcessGuard("traffic-replay", taskId, config, kafkaProducer,
                () -> {
                    ProcessManager pm = new ProcessManager(config.getTrafficJarPath(),
                            "TrafficReplay-" + taskId, taskId);
                    pm.setMainArgs(new String[]{"--mode", "replay",
                            "--config", "files/" + taskId + "/config.properties"});
                    return pm;
                },
                getRunningStatus());

        if (!replayProcessGuard.startAndGuard() && !replayProcessGuard.isGuarding()) {
            stopped.set(true);
            sendFailedStatus("E3125", "流量回放进程启动失败");
            return;
        }

        lastSuccessfulStatus = getRunningStatus();
        sendStatus(getRunningStatus(), "流量回放中", 0);
        logger.info("[{}] 流量回放已启动，进入持续监控", taskId);
    }

    /**
     * 回放进程退出<b>不等于</b>失败：回放本来就是有终点的。
     *
     * <p>靠子进程写的报告区分：{@code outcome=COMPLETED} 是跑完了，
     * {@code FAILED} 是错误率熔断，{@code STOPPED} 是被人停的。
     * 只看进程还在不在，会把每一次正常跑完都判成"关键进程已停止"。
     */
    @Override
    protected boolean checkProcessHealth() {
        Properties report = readReport();
        if (report != null) {
            String outcome = report.getProperty("outcome", "");
            if ("COMPLETED".equals(outcome)) {
                finish("COMPLETED", "流量回放完成：共 " + report.getProperty("total", "0") + " 条，"
                        + "回放错误 " + report.getProperty("count.REPLAY_ERROR", "0") + " 条");
                return true;
            }
            if ("FAILED".equals(outcome)) {
                finishFailed(report.getProperty("reason", "回放错误率超过阈值"));
                return true;
            }
        }
        if (replayProcessGuard != null
                && !replayProcessGuard.isGuarding() && !replayProcessGuard.isRunning()) {
            logger.error("[{}] 流量回放进程已停止且 ProcessGuard 已放弃守护", taskId);
            return false;
        }
        return true;
    }

    private void finish(String status, String message) {
        stopped.set(true);
        if (replayProcessGuard != null) replayProcessGuard.stop();
        sendStatus(status, message, 100);
    }

    private void finishFailed(String reason) {
        stopped.set(true);
        if (replayProcessGuard != null) replayProcessGuard.stop();
        sendFailedStatus("E3125", reason);
    }

    /** 回放进程主循环无条件刷新的活性文件（空洞期间也刷，否则长空洞会被当成僵死）。 */
    @Override
    protected List<String> stallLivenessFiles() {
        return Arrays.asList("./files/" + taskId + "/metrics/traffic_replay_liveness");
    }

    @Override
    protected boolean livenessOwnerRestarting(String path) {
        return replayProcessGuard != null && !replayProcessGuard.isRunning();
    }

    @Override
    protected boolean livenessFileExpected(String path) {
        return replayProcessGuard != null && replayProcessGuard.isRunning();
    }

    @Override
    protected void sendPeriodicMetricsUpdate(MetricsService.TaskMetrics taskMetrics) {
        long now = System.currentTimeMillis();
        if (now - lastMetricsReportTime < METRICS_REPORT_INTERVAL_MS) return;
        lastMetricsReportTime = now;
        try {
            TaskStatusMessage msg = new TaskStatusMessage();
            msg.setTaskId(taskId);
            msg.setStatus(getRunningStatus());
            Long done = readMetricFile("./files/" + taskId + "/metrics/traffic_replay_progress");
            Long total = readMetricFile("./files/" + taskId + "/metrics/traffic_replay_total");
            Long lagMs = readMetricFile("./files/" + taskId + "/metrics/traffic_replay_lag_ms");
            int progress = (total != null && total > 0 && done != null)
                    ? (int) Math.min(99, done * 100 / total) : 0;
            msg.setProgress(progress);
            msg.setMessage("流量回放中" + (done != null && total != null ? "（" + done + "/" + total + "）" : ""));
            // 回放的"落后多少"用 rtoMs 这一栏承载：语义一致（目标端相对计划的滞后）
            if (lagMs != null) {
                msg.setRtoMs(lagMs);
            }
            attachSlaMetrics(msg);
            kafkaProducer.sendStatus(msg);
        } catch (Exception e) {
            logger.debug("[{}] 流量回放指标上报失败", taskId, e);
        }
    }

    /** 同捕获侧：基类的 stopAllProcesses 不认识这个 guard，不覆盖就停不掉回放子进程。 */
    @Override
    protected void stopExtraProcesses(String threadName) {
        if (replayProcessGuard != null) {
            try {
                replayProcessGuard.stop();
                logger.info("[{}] 流量回放进程已停止", threadName);
            } catch (Exception e) {
                logger.error("[{}] 停止流量回放进程失败", threadName, e);
            }
        }
    }

    private Properties loadTaskConfig() {
        Properties p = new Properties();
        File f = new File("./files/" + taskId + "/config.properties");
        if (!f.isFile()) return p;
        try (java.io.InputStream in = new java.io.FileInputStream(f)) {
            p.load(in);
        } catch (Exception e) {
            logger.warn("[{}] 读取任务配置失败: {}", taskId, e.getMessage());
        }
        return p;
    }

    private Properties readReport() {
        File f = new File("./files/" + taskId + "/traffic/replay_report.properties");
        if (!f.isFile()) return null;
        Properties p = new Properties();
        try (java.io.InputStream in = new java.io.FileInputStream(f)) {
            p.load(in);
            return p;
        } catch (Exception e) {
            return null;
        }
    }
}
