package com.migration.agent.resilience;

import com.migration.agent.manager.ProcessManager;
import com.migration.agent.model.TaskStatusMessage;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ProcessGuard {
    private static final Logger logger = LoggerFactory.getLogger(ProcessGuard.class);

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
            .onStateChange(newState -> {
                logger.warn("[{}] CircuitBreaker for {} transitioned to {}", taskId, processName, newState);
                if (newState == CircuitBreaker.State.OPEN) {
                    sendAlert("CIRCUIT_OPEN", processName + " circuit breaker OPEN - consecutive failures detected, retries paused");
                } else if (newState == CircuitBreaker.State.CLOSED) {
                    logger.info("[{}] CircuitBreaker for {} recovered to CLOSED", taskId, processName);
                }
            })
            .build();
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

    private boolean attemptRecovery() {
        if (!circuitBreaker.allowRequest()) {
            logger.warn("[{}] CircuitBreaker is OPEN for {}, retries paused", taskId, processName);
            sendAlert("CIRCUIT_BLOCKED", processName + " is blocked by circuit breaker, retries paused");
            return false;
        }

        if (!retryPolicy.shouldRetry()) {
            logger.error("[{}] Retry exhausted for {}, giving up", taskId, processName);
            sendStatus("FAILED", processName + " 进程异常退出，重试已耗尽");
            return false;
        }

        if (!retryPolicy.recordAttempt()) {
            sendStatus("FAILED", processName + " 进程异常退出，重试已耗尽");
            return false;
        }

        try {
            retryPolicy.sleepBeforeRetry();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        if (stopped.get()) {
            logger.info("[{}] Task stopped during retry wait for {}", taskId, processName);
            return false;
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

            logger.info("[{}] Restarting {} process (attempt {}/{})...",
                taskId, processName, retryPolicy.getAttemptCount(), retryPolicy.getMaxRetries());

            ProcessManager newProcess = processFactory.create();
            processRef.set(newProcess);
            newProcess.start();

            if (!waitForStartup(newProcess)) {
                logger.error("[{}] {} process restart failed", taskId, processName);
                circuitBreaker.recordFailure();
                return attemptRecovery();
            }

            logger.info("[{}] {} process restarted successfully", taskId, processName);
            circuitBreaker.recordSuccess();
            retryPolicy.reset();
            reportProcessStatus("RUNNING");
            sendStatus(runningStatus, processName + " 进程已自动重启恢复");
            return true;

        } catch (Exception e) {
            logger.error("[{}] Failed to restart {} process", taskId, processName, e);
            circuitBreaker.recordFailure();
            return attemptRecovery();
        }
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
