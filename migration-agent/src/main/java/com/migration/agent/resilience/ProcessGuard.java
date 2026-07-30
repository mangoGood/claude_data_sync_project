package com.migration.agent.resilience;

import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ProcessGuard {
    private static final Logger logger = LoggerFactory.getLogger(ProcessGuard.class);

    /** 长睡眠切片：让 stop() 能在几秒内收敛，而不是被一次 30 分钟的 sleep 卡住。 */
    private static final long SLEEP_CHUNK_MS = 5000;

    private final String processName;
    private final String taskId;
    private final AgentConfig config;
    private final KafkaProducerService kafkaProducer;
    private final String runningStatus;

    private final AtomicReference<ProcessManager> processRef = new AtomicReference<>();
    private final AtomicBoolean guarding = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final RetryPolicy retryPolicy;
    private final CircuitBreaker circuitBreaker;
    private final ProcessFactory processFactory;

    /** 短期重试预算耗尽后进入的“长期重连”轮次；重启成功即清零。 */
    private final AtomicInteger longTermAttempts = new AtomicInteger(0);
    private final long reconnectIntervalMs;
    private final int reconnectMaxAttempts;
    /** 是否已就本轮不可用上报过 RECONNECTING（避免每次睡眠切片都刷一条状态）。 */
    private final AtomicBoolean reconnectingReported = new AtomicBoolean(false);

    /** crash-loop 检测：滑动窗口内的重启时刻。 */
    private final Deque<Long> restartTimestamps = new ArrayDeque<>();
    private final long crashLoopWindowMs;
    private final int crashLoopThreshold;
    /** 本轮 crash-loop 是否已上报（窗口内重启次数回落到阈值以下才复位，避免每次重启都刷一条）。 */
    private final AtomicBoolean crashLoopReported = new AtomicBoolean(false);

    private Thread guardThread;

    @FunctionalInterface
    public interface ProcessFactory {
        ProcessManager create() throws Exception;
    }

    public ProcessGuard(String processName, String taskId, AgentConfig config,
                        KafkaProducerService kafkaProducer, ProcessFactory processFactory) {
        this(processName, taskId, config, kafkaProducer, processFactory, "INCREMENT_RUNNING");
    }

    public ProcessGuard(String processName, String taskId, AgentConfig config,
                        KafkaProducerService kafkaProducer, ProcessFactory processFactory, String runningStatus) {
        this.processName = processName;
        this.taskId = taskId;
        this.config = config;
        this.kafkaProducer = kafkaProducer;
        this.processFactory = processFactory;
        this.runningStatus = runningStatus != null ? runningStatus : "INCREMENT_RUNNING";

        this.retryPolicy = RetryPolicy.builder()
            .maxRetries(config.getRetryMaxAttempts())
            .initialDelayMs(config.getRetryInitialDelayMs())
            .multiplier(config.getRetryMultiplier())
            .maxDelayMs(config.getRetryMaxDelayMs())
            .onRetry(ctx -> logger.warn("[{}] Retry attempt {}/{} for process {}, delay={}ms",
                taskId, ctx.getAttempt(), ctx.getMaxRetries(), processName, ctx.getDelayMs()))
            .onExhausted(ctx -> {
                logger.error("[{}] All {} retry attempts exhausted for process {}", taskId, ctx.getMaxRetries(), processName);
                sendAlert("RETRY_EXHAUSTED", processName + " retry exhausted after " + ctx.getMaxRetries() + " attempts");
            })
            .build();

        this.circuitBreaker = CircuitBreaker.builder()
            .failureThreshold(config.getCircuitBreakerFailureThreshold())
            .openTimeoutMs(config.getCircuitBreakerOpenTimeoutMs())
            .openTimeoutMultiplier(config.getCircuitBreakerOpenTimeoutMultiplier())
            .maxOpenTimeoutMs(config.getCircuitBreakerMaxOpenTimeoutMs())
            .onStateChange(newState -> {
                logger.warn("[{}] CircuitBreaker for {} transitioned to {}", taskId, processName, newState);
                if (newState == CircuitBreaker.State.OPEN) {
                    sendAlert("CIRCUIT_OPEN", processName + " circuit breaker OPEN - consecutive failures detected, retries paused");
                } else if (newState == CircuitBreaker.State.CLOSED) {
                    logger.info("[{}] CircuitBreaker for {} recovered to CLOSED", taskId, processName);
                }
            })
            .build();

        this.reconnectIntervalMs = config.getReconnectIntervalMs();
        this.reconnectMaxAttempts = config.getReconnectMaxAttempts();
        this.crashLoopWindowMs = config.getCrashLoopWindowMs();
        this.crashLoopThreshold = config.getCrashLoopThreshold();
    }

    public boolean startAndGuard() {
        if (stopped.get()) {
            logger.warn("[{}] ProcessGuard for {} is stopped, refusing to start", taskId, processName);
            return false;
        }

        boolean started = false;
        try {
            ProcessManager process = processFactory.create();
            processRef.set(process);
            process.start();

            if (!waitForStartup(process)) {
                logger.error("[{}] {} process failed to start, guard thread will retry", taskId, processName);
                circuitBreaker.recordFailure();
            } else {
                logger.info("[{}] {} process started successfully", taskId, processName);
                circuitBreaker.recordSuccess();
                retryPolicy.reset();
                reportProcessStatus("RUNNING");
                started = true;
            }

        } catch (Exception e) {
            logger.error("[{}] Failed to start {} process, guard thread will retry", taskId, processName, e);
            circuitBreaker.recordFailure();
        }

        startGuardThread();
        return started;
    }

    private boolean waitForStartup(ProcessManager process) {
        for (int i = 0; i < 6; i++) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (stopped.get()) {
                    return false;
                }
            }

            if (process.isRunning()) {
                logger.info("[{}] {} process ready after {}s", taskId, processName, (i + 1) * 5);
                return true;
            }
            logger.info("[{}] {} not ready, waiting... ({}s elapsed)", taskId, processName, (i + 1) * 5);
        }
        return false;
    }

    private void startGuardThread() {
        if (guarding.getAndSet(true)) {
            return;
        }

        guardThread = new Thread(() -> {
            logger.info("[{}] Guard thread started for {}", taskId, processName);

            while (guarding.get() && !stopped.get()) {
                try {
                    long monitorInterval = getMonitorInterval();
                    Thread.sleep(monitorInterval);

                    if (!guarding.get() || stopped.get()) {
                        break;
                    }

                    ProcessManager process = processRef.get();
                    if (process == null) {
                        continue;
                    }

                    if (!process.isRunning()) {
                        if (stopped.get()) {
                            logger.info("[{}] {} stopped intentionally", taskId, processName);
                            break;
                        }

                        logger.warn("[{}] {} process crashed, attempting recovery...", taskId, processName);
                        // crash-loop 按"崩溃次数"计，而不是"重启成功次数"：中间夹一次启动失败
                        // （比如上一进程还没退干净、端口/锁没释放）会让成功次数少记，反而把真正的
                        // 反复崩溃漏报。崩溃是客观事实，先记下来，等重启成功时一并上报。
                        reportProcessStatus("STOPPED");
                        boolean recovered = attemptRecovery();

                        if (!recovered) {
                            logger.error("[{}] {} process recovery failed, stopping guard", taskId, processName);
                            guarding.set(false);
                            break;
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("[{}] Guard thread error for {}", taskId, processName, e);
                }
            }

            guarding.set(false);
            logger.info("[{}] Guard thread stopped for {}", taskId, processName);
        }, "ProcessGuard-" + processName + "-" + taskId);

        guardThread.setDaemon(true);
        guardThread.start();
    }

    /**
     * 崩溃恢复：短期指数退避重试 → 预算耗尽后转「长期重连」，直到成功、被停止或长期重连次数用尽。
     *
     * <p>与旧实现的两点关键差别：
     * <ul>
     *   <li>熔断打开<b>不再直接放弃</b>。旧实现里 OPEN ⇒ 返回 false ⇒ 守护线程退出 ⇒ 进程永不再被拉起，
     *       目标库一次超过 ~2.5 分钟的维护窗口就能把任务打死。现在等到 OPEN 到期由熔断器放行探测。</li>
     *   <li>失败路径<b>由递归改为循环</b>。长期重连是无限轮次的，递归会把栈打爆。</li>
     * </ul>
     */
    private boolean attemptRecovery() {
        while (!stopped.get()) {
            if (!awaitNextAttempt()) {
                return false;
            }
            if (stopped.get()) {
                return false;
            }
            if (!circuitBreaker.allowRequest()) {
                // 熔断窗口还没到期（长期重连间隔短于熔断退避时会走到这里）：继续等，不消耗重试预算
                long remaining = Math.max(1000, circuitBreaker.getOpenRemainingMs());
                logger.info("[{}] {} 熔断未到期，再等 {}ms", taskId, processName, remaining);
                if (!sleepInterruptibly(remaining)) {
                    return false;
                }
                continue;
            }
            if (restartOnce()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 本轮重启前的等待。短期预算内走指数退避；耗尽后进入长期重连——
     * 间隔取 {@code max(reconnect.interval.ms, 熔断剩余时长)}，于是熔断的指数退避（60s→…→30min）
     * 自然成了长期重连的节奏上界，与方案里的「5~30 分钟固定间隔」一致。
     *
     * @return false 表示不该再试了（已停止 / 被中断 / 长期重连次数用尽，后者已上报 FAILED）
     */
    private boolean awaitNextAttempt() {
        if (retryPolicy.shouldRetry()) {
            if (!retryPolicy.recordAttempt()) {
                return false;
            }
            long delay = Math.max(0, retryPolicy.getCurrentDelayMs());
            logger.info("[{}] {} 短期重试 {}/{}，{}ms 后重启",
                    taskId, processName, retryPolicy.getAttemptCount(), retryPolicy.getMaxRetries(), delay);
            return sleepInterruptibly(delay);
        }

        int round = longTermAttempts.incrementAndGet();
        if (reconnectMaxAttempts >= 0 && round > reconnectMaxAttempts) {
            logger.error("[{}] {} 长期重连 {} 次仍未恢复，判定失败", taskId, processName, reconnectMaxAttempts);
            sendStatus("FAILED", processName + " 进程异常退出：短期重试与长期重连（"
                    + reconnectMaxAttempts + " 次）均未恢复，需人工介入");
            return false;
        }

        long wait = Math.max(reconnectIntervalMs, circuitBreaker.getOpenRemainingMs());
        reportReconnecting(round, wait);
        return sleepInterruptibly(wait);
    }

    /** 起一次新进程并等它就绪；失败只记熔断，由调用方决定是否再来一轮。 */
    private boolean restartOnce() {
        // crash-loop 按"窗口内的重启次数"计。不能只数"守护循环发现的崩溃"：进程若死在
        // 启动就绪窗口里（waitForStartup 期间被杀/自己退出），那一次根本不会走崩溃分支，
        // 于是最典型的 crash-loop（起来几秒就死）反而数不到——实测连杀 3 次只记到 1 次。
        int attempts = recordRestartAttempt();
        if (attempts >= crashLoopThreshold && crashLoopReported.compareAndSet(false, true)) {
            reportCrashLoop(attempts);
        }
        try {
            ProcessManager oldProcess = processRef.get();
            if (oldProcess != null) {
                try {
                    oldProcess.stop();
                } catch (Exception e) {
                    logger.warn("[{}] Error stopping old {} process", taskId, processName, e);
                }
            }

            logger.info("[{}] Restarting {} process (short-term {}/{}, long-term {})...",
                taskId, processName, retryPolicy.getAttemptCount(), retryPolicy.getMaxRetries(),
                longTermAttempts.get());

            ProcessManager newProcess = processFactory.create();
            processRef.set(newProcess);
            newProcess.start();

            if (!waitForStartup(newProcess)) {
                logger.error("[{}] {} process restart failed", taskId, processName);
                circuitBreaker.recordFailure();
                return false;
            }

            logger.info("[{}] {} process restarted successfully", taskId, processName);
            circuitBreaker.recordSuccess();
            retryPolicy.reset();
            longTermAttempts.set(0);
            reconnectingReported.set(false);
            reportProcessStatus("RUNNING");
            reportRestartSucceeded();
            return true;

        } catch (Exception e) {
            logger.error("[{}] Failed to restart {} process", taskId, processName, e);
            circuitBreaker.recordFailure();
            return false;
        }
    }

    /**
     * 重启成功后的状态上报。窗口内重启次数超阈值时报 <b>E3007</b>（反复崩溃）而不是
     * 一句"已自动重启恢复"——旧实现让永久 crash-loop 在监控上完全不可见：每次崩溃都上报
     * "进程已自动重启恢复 + INCREMENT_RUNNING"，看板上跟健康任务毫无区别。
     */
    private void reportRestartSucceeded() {
        int inWindow = restartCountInWindow();
        if (inWindow >= crashLoopThreshold) {
            sendStatus(runningStatus, crashLoopMessage(inWindow) + "（本次已重启恢复）", "E3007");
        } else {
            crashLoopReported.set(false);
            sendStatus(runningStatus, processName + " 进程已自动重启恢复");
        }
    }

    /** crash-loop 上报：日志 + 告警 + 带 E3007 的状态消息（任务不判失败——进程还能起来）。 */
    private void reportCrashLoop(int restarts) {
        String msg = crashLoopMessage(restarts);
        logger.error("[{}] {}", taskId, msg);
        sendAlert("CRASH_LOOP", msg);
        sendStatus(runningStatus, msg, "E3007");
    }

    private String crashLoopMessage(int restarts) {
        return String.format("%s 进程在 %d 分钟内已重启 %d 次，疑似反复崩溃（crash-loop），请检查子进程日志",
                processName, crashLoopWindowMs / 60000, restarts);
    }

    /** 记录一次重启尝试并返回滑动窗口内的重启次数。 */
    private int recordRestartAttempt() {
        long now = System.currentTimeMillis();
        synchronized (restartTimestamps) {
            restartTimestamps.addLast(now);
            pruneCrashWindow(now);
            return restartTimestamps.size();
        }
    }

    /** 滑动窗口内的重启次数（只读）。 */
    int restartCountInWindow() {
        synchronized (restartTimestamps) {
            pruneCrashWindow(System.currentTimeMillis());
            return restartTimestamps.size();
        }
    }

    private void pruneCrashWindow(long now) {
        while (!restartTimestamps.isEmpty() && now - restartTimestamps.peekFirst() > crashLoopWindowMs) {
            restartTimestamps.pollFirst();
        }
    }

    /** 长期重连期间把任务状态置为 RECONNECTING（可自愈，不算失败），同一轮不可用只报一次。 */
    private void reportReconnecting(int round, long waitMs) {
        String limit = reconnectMaxAttempts >= 0 ? String.valueOf(reconnectMaxAttempts) : "∞";
        String msg = String.format("%s 进程短期重试已耗尽，进入长期重连（第 %d/%s 次，%ds 后重试）",
                processName, round, limit, waitMs / 1000);
        logger.warn("[{}] {}", taskId, msg);
        if (reconnectingReported.compareAndSet(false, true)) {
            sendStatus("RECONNECTING", msg);
        }
    }

    /** 分片睡眠，便于 stop() 快速收敛；返回 false 表示被停止或被中断。 */
    private boolean sleepInterruptibly(long totalMs) {
        long remaining = totalMs;
        while (remaining > 0) {
            if (stopped.get()) {
                return false;
            }
            long chunk = Math.min(SLEEP_CHUNK_MS, remaining);
            try {
                Thread.sleep(chunk);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remaining -= chunk;
        }
        return !stopped.get();
    }

    private long getMonitorInterval() {
        switch (processName.toLowerCase()) {
            case "capture":
                return config.getCaptureMonitorIntervalMs();
            case "extract":
                return config.getExtractMonitorIntervalMs();
            case "increment":
                return config.getIncrementMonitorIntervalMs();
            default:
                return config.getCaptureMonitorIntervalMs();
        }
    }

    public void stop() {
        stopped.set(true);
        guarding.set(false);

        reportProcessStatus("STOPPED");

        ProcessManager process = processRef.get();
        if (process != null) {
            try {
                process.stop();
            } catch (Exception e) {
                logger.warn("[{}] Error stopping {} process", taskId, processName, e);
            }
        }

        if (guardThread != null) {
            guardThread.interrupt();
        }

        logger.info("[{}] ProcessGuard stopped for {}", taskId, processName);
    }

    public boolean isRunning() {
        ProcessManager process = processRef.get();
        return process != null && process.isRunning();
    }

    public boolean isGuarding() {
        return guarding.get() && !stopped.get();
    }

    public ProcessManager getProcess() {
        return processRef.get();
    }

    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }

    public int getRetryAttemptCount() {
        return retryPolicy.getAttemptCount();
    }

    private void sendStatus(String status, String message) {
        sendStatus(status, message, null);
    }

    private void sendStatus(String status, String message, String errorCode) {
        if (kafkaProducer == null) return;
        TaskStatusMessage statusMessage = new TaskStatusMessage();
        statusMessage.setTaskId(taskId);
        statusMessage.setStatus(status);
        statusMessage.setMessage(message);
        if ("FAILED".equals(status)) {
            statusMessage.setProgress(0);
        } else {
            statusMessage.setProgress(100);
        }
        if (errorCode != null) {
            statusMessage.setErrorCode(errorCode);
        }
        kafkaProducer.sendStatus(statusMessage);
    }

    private void sendAlert(String alertType, String message) {
        logger.warn("[{}] ALERT [{}]: {}", taskId, alertType, message);
    }

    private void reportProcessStatus(String state) {
        try {
            MetricsService.TaskMetrics metrics = MetricsService.getInstance().getOrCreateTaskMetrics(taskId);
            ProcessManager process = processRef.get();
            long pid = process != null ? process.getPid() : -1;
            String uptime = "";
            metrics.updateProcessStatus(
                processName,
                state,
                pid,
                uptime,
                retryPolicy.getAttemptCount(),
                circuitBreaker.getState().name()
            );
        } catch (Exception e) {
            logger.debug("[{}] Failed to report process status metrics", taskId, e);
        }
    }
}
