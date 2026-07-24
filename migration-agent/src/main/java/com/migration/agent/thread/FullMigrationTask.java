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
 * 全量迁移任务（含可选增量同步）。
 *
 * <p>执行流程：
 * <ol>
 *   <li>初始化 checkpoint</li>
 *   <li>启动 capture 进程</li>
 *   <li>执行全量迁移</li>
 *   <li>若 migrationMode=fullAndIncre，继续启动 extract + increment 进入持续增量同步</li>
 *   <li>仅 full 模式下完成即结束</li>
 * </ol>
 */
public class FullMigrationTask extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(FullMigrationTask.class);

    public FullMigrationTask(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                             TaskStateService taskStateService, AgentConfig config) {
        super(taskMessage, kafkaProducer, taskStateService, false, config);
    }

    @Override
    protected String getRunningStatus() {
        return "INCREMENT_RUNNING";
    }

    @Override
    protected void doRun() throws Exception {
        String threadName = "FullMigrationTask-" + taskId;

        sendStatus("STARTING", "任务启动中", 0, totalTables, 0, null, 0, 0L, 0L);

        if (!initCheckpoint()) {
            stopped.set(true);
            return;
        }

        // TiDB 纯全量任务不起 capture：TiDB 的增量出口是 TiCDC changefeed，而 changefeed 一旦建立
        // 就会持有源集群的 GC safepoint 并持续向 Kafka 投递——纯全量任务用不到它，建了只会白白
        // 拖住源库 GC、堆积 Kafka 数据。其余源类型的 capture 是纯读取，保持原有行为不变。
        boolean skipCapture = "tidb".equals(sourceType) && !"fullAndIncre".equals(migrationMode);
        if (skipCapture) {
            logger.info("[{}] TiDB 仅全量任务，跳过 capture（增量才需要 TiCDC changefeed）", threadName);
        } else if (!startCaptureProcess()) {
            sendFailedStatus("E3001", "capture 进程启动失败");
            stopped.set(true);
            return;
        }

        if (!executeFullMigration()) {
            return;
        }

        boolean isFullOnly = !"fullAndIncre".equals(migrationMode);
        if (isFullOnly) {
            logger.info("[{}] 仅全量同步任务完成，状态: FULL_COMPLETED", threadName);
            stopAllProcesses();
            sendStatus("FULL_COMPLETED", "全量同步完成", 100, totalTables, totalTables, null, 100, 0L, 0L);
            stopped.set(true);
            return;
        }

        lastSuccessfulStatus = "FULL_COMPLETED";

        if (!startExtractProcess()) {
            sendFailedStatus("E3002", "extract 进程启动失败，增量同步无法继续");
            stopped.set(true);
            return;
        }

        if (!startIncrementProcess()) {
            logger.warn("[{}] increment进程启动失败，ProcessGuard将负责重试", threadName);
        }

        lastSuccessfulStatus = "INCREMENT_RUNNING";
        logger.info("[{}] 增量同步任务启动完成，进入持续监控模式", threadName);
    }

    /**
     * 僵死看门狗的活性文件：进入增量阶段后 capture/extract/increment 各自的活性文件才出现，
     * 全量阶段它们尚不存在，checkPipelineStalled 会因文件缺失而跳过、不误判。
     */
    @Override
    protected java.util.List<String> stallLivenessFiles() {
        return incrementLivenessFiles();
    }

    /** 三个守护进程都 RUNNING 时才做僵死检查；有进程正在被 ProcessGuard 重启则跳过（交崩溃恢复处理）。 */
    @Override
    protected boolean guardsHealthyForStallCheck() {
        return (captureGuard == null || captureGuard.isRunning())
                && (extractGuard == null || extractGuard.isRunning())
                && (incrementGuard == null || incrementGuard.isRunning());
    }

    @Override
    protected boolean checkProcessHealth() {
        String threadName = "FullMigrationTask-" + taskId;

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
