package com.migration.agent.thread;

import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.resilience.ProcessGuard;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import com.migration.agent.service.TaskStateService;
import com.migration.agent.service.TrafficSourceGuardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 流量复制任务：单个 {@code migration-traffic --mode capture} 子进程。
 *
 * <p>比其它执行器多做一件事——<b>源库开关的兜底还原</b>。捕获期间源库的
 * {@code general_log} 是开着的，子进程被 {@code kill -9}、宿主断电、agent 硬崩时
 * 它没机会自己关掉，而不关掉的后果是源库每条语句都继续写日志表，直到把源库磁盘写满。
 * 所以这里在任务终结的每一条路径上都补一次还原。
 */
public class TrafficCaptureTask extends AbstractTaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(TrafficCaptureTask.class);

    private ProcessGuard captureProcessGuard;
    private final TrafficSourceGuardService sourceGuard = new TrafficSourceGuardService();

    public TrafficCaptureTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                              TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "TRAFFIC_CAPTURING";
    }

    /** 语句流没有位点可续，位点体系对它只有副作用。 */
    @Override
    protected boolean usesCheckpoint() {
        return false;
    }

    @Override
    protected void doRun() throws Exception {
        logger.info("[{}] 开始执行流量复制任务", taskId);
        sendStatus("STARTING", "流量复制启动中", 0);

        captureProcessGuard = new ProcessGuard("traffic-capture", taskId, config, kafkaProducer,
                () -> {
                    ProcessManager pm = new ProcessManager(config.getTrafficJarPath(),
                            "TrafficCapture-" + taskId, taskId);
                    pm.setMainArgs(new String[]{"--mode", "capture",
                            "--config", "files/" + taskId + "/config.properties"});
                    return pm;
                },
                getRunningStatus());

        if (!captureProcessGuard.startAndGuard()) {
            // 与其它引擎一致：启动窗口内没就绪不等于起不来，守护线程会继续按退避重启。
            // 此处判死会让 stopped=true、守护线程随即退出，进程永不再被拉起。
            logger.warn("[{}] 流量复制进程未在启动窗口内就绪，交由 ProcessGuard 继续重启", taskId);
            if (!captureProcessGuard.isGuarding()) {
                stopped.set(true);
                sendFailedStatus("E3120", "流量复制进程启动失败");
                return;
            }
        }

        lastSuccessfulStatus = getRunningStatus();
        sendStatus(getRunningStatus(), "流量复制中", 100);
        logger.info("[{}] 流量复制已启动，进入持续监控", taskId);
    }

    @Override
    protected boolean checkProcessHealth() {
        if (captureProcessGuard != null
                && !captureProcessGuard.isGuarding() && !captureProcessGuard.isRunning()) {
            // 子进程彻底起不来了：源库开关还开着，必须在判失败之前先还原
            logger.error("[{}] 流量复制进程已停止且 ProcessGuard 已放弃守护", taskId);
            sourceGuard.restoreIfPending(taskId);
            return false;
        }
        // 到量自动封口是"正常收工"，不是进程异常
        if (captureProcessGuard != null && !captureProcessGuard.isRunning() && isCompletedByLimit()) {
            return true;
        }
        return true;
    }

    /**
     * 活性文件只看 {@code traffic_capture_liveness}。
     *
     * <p>不能看 {@code traffic_capture_records}：源库空闲时一条语句都不会录到，
     * 那个文件自然不动，拿它判活性会把闲着的任务当僵死杀掉。
     * liveness 由捕获主循环<b>无条件</b>每轮刷新，与有没有抓到语句无关。
     */
    @Override
    protected List<String> stallLivenessFiles() {
        return Arrays.asList("./files/" + taskId + "/metrics/traffic_capture_liveness");
    }

    @Override
    protected boolean livenessOwnerRestarting(String path) {
        return captureProcessGuard != null && !captureProcessGuard.isRunning();
    }

    @Override
    protected boolean livenessFileExpected(String path) {
        return captureProcessGuard != null && captureProcessGuard.isRunning();
    }

    @Override
    protected void sendPeriodicMetricsUpdate(MetricsService.TaskMetrics taskMetrics) {
        long now = System.currentTimeMillis();
        if (now - lastMetricsReportTime < METRICS_REPORT_INTERVAL_MS) return;
        lastMetricsReportTime = now;

        // 子进程已到量自封口：任务应转 COMPLETED（不是失败），并把录制信息定格
        if (isCompletedByLimit() && (captureProcessGuard == null || !captureProcessGuard.isRunning())) {
            stopped.set(true);
            if (captureProcessGuard != null) captureProcessGuard.stop();
            sourceGuard.restoreIfPending(taskId);
            sendStatus("COMPLETED", "流量复制完成：" + resultReason(), 100);
            return;
        }

        try {
            TaskStatusMessage msg = new TaskStatusMessage();
            msg.setTaskId(taskId);
            msg.setStatus(getRunningStatus());
            msg.setMessage("流量复制中");
            msg.setProgress(100);
            Long records = readMetricFile("./files/" + taskId + "/metrics/traffic_capture_records");
            Long bytes = readMetricFile("./files/" + taskId + "/metrics/traffic_capture_bytes");
            if (bytes != null) {
                msg.setCaptureReplayBytes(bytes);
            }
            attachSlaMetrics(msg);
            kafkaProducer.sendStatus(msg);
            logger.debug("[{}] 流量复制指标: 记录={}, 字节={}", taskId, records, bytes);
        } catch (Exception e) {
            logger.debug("[{}] 流量复制指标上报失败", taskId, e);
        }
    }

    /** 子进程写的收工标记：{@code outcome=COMPLETED} 表示到量自封口。 */
    private boolean isCompletedByLimit() {
        Properties p = readResultMarker();
        return p != null && "COMPLETED".equals(p.getProperty("outcome"));
    }

    private String resultReason() {
        Properties p = readResultMarker();
        String reason = p == null ? null : p.getProperty("reason");
        return reason == null || reason.isEmpty() ? "已达录制上限" : reason;
    }

    private Properties readResultMarker() {
        File f = new File("./files/" + taskId + "/traffic/capture_result.properties");
        if (!f.isFile()) return null;
        Properties p = new Properties();
        try (java.io.InputStream in = new java.io.FileInputStream(f)) {
            p.load(in);
            return p;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 停掉捕获子进程，然后确认源库开关已还原。
     *
     * <p>必须覆盖这个钩子：基类的 {@code stopAllProcesses()} 只认识
     * capture/extract/increment/full 那几个固定字段，不覆盖它的话，
     * <b>捕获子进程根本不会被停</b>——它会变成孤儿继续跑，源库的 general_log
     * 也就一直开着。实测过一次：任务显示"已结束"，源库日志表还在涨。
     *
     * <p>顺序也是有讲究的：<b>先停进程、再兜底还原</b>。子进程正常退出会自己还原并
     * 删掉状态文件，此时兜底是个空动作；只有它被 SIGKILL（10s 优雅期没退完）时，
     * 状态文件才会留下来，兜底这一步才真正干活。反过来先还原的话，
     * 还在跑的子进程随后可能又把日志打开。
     */
    @Override
    protected void stopExtraProcesses(String threadName) {
        if (captureProcessGuard != null) {
            try {
                captureProcessGuard.stop();
                logger.info("[{}] 流量复制进程已停止", threadName);
            } catch (Exception e) {
                logger.error("[{}] 停止流量复制进程失败", threadName, e);
            }
        }
        if (!sourceGuard.restoreIfPending(taskId)) {
            // 源库开关没能还原 = 它会继续把每条语句写进日志表直到磁盘满。
            // 这不能只留一行日志：必须让任务显式失败，人才会来看。
            sendFailedStatus("E3126",
                    "任务已结束但未能确认还原源库的 general_log/log_output，源库会持续写入语句日志，请人工确认");
        }
    }
}
