package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
    private static MetricsService instance;

    private final ConcurrentHashMap<String, TaskMetrics> taskMetricsMap = new ConcurrentHashMap<>();
    private volatile MicrometerBridge micrometerBridge;

    private MetricsService() {
        try {
            micrometerBridge = (MicrometerBridge) Class.forName("com.migration.agent.service.MicrometerBridge")
                .getConstructor().newInstance();
            logger.info("MetricsService initialized with PrometheusMeterRegistry");
        } catch (Throwable e) {
            micrometerBridge = null;
            logger.warn("Micrometer not available, Prometheus metrics export disabled. Error: {}", e.getMessage());
        }
    }

    public static synchronized MetricsService getInstance() {
        if (instance == null) {
            instance = new MetricsService();
        }
        return instance;
    }

    public boolean isMicrometerAvailable() {
        return micrometerBridge != null;
    }

    public String scrapePrometheus() {
        if (micrometerBridge == null) {
            return "# Micrometer not available - metrics collection disabled\n";
        }
        return micrometerBridge.scrape();
    }

    public TaskMetrics getOrCreateTaskMetrics(String taskId) {
        return taskMetricsMap.computeIfAbsent(taskId, this::createTaskMetrics);
    }

    private TaskMetrics createTaskMetrics(String taskId) {
        logger.info("Creating metrics for task: {} (micrometerAvailable={})", taskId, micrometerBridge != null);
        TaskMetrics metrics = new TaskMetrics(taskId);
        if (micrometerBridge != null) {
            micrometerBridge.registerTaskGauges(taskId, metrics);
        }
        return metrics;
    }

    public void removeTaskMetrics(String taskId) {
        TaskMetrics removed = taskMetricsMap.remove(taskId);
        if (removed != null) {
            if (micrometerBridge != null) {
                micrometerBridge.unregisterTaskGauges(taskId);
            }
            logger.info("Removed metrics for task: {}", taskId);
        }
    }

    public Map<String, Object> getTaskMetricsSnapshot(String taskId) {
        TaskMetrics metrics = taskMetricsMap.get(taskId);
        if (metrics == null) {
            return Collections.emptyMap();
        }
        return metrics.toSnapshot();
    }

    public List<Map<String, Object>> getAllTaskProcessStatus() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, TaskMetrics> entry : taskMetricsMap.entrySet()) {
            Map<String, Object> snapshot = entry.getValue().toSnapshot();
            snapshot.put("taskId", entry.getKey());
            result.add(snapshot);
        }
        return result;
    }

    public static class TaskMetrics {
        private final String taskId;

        private final AtomicLong captureRate = new AtomicLong(0);
        private final AtomicLong e2eLatencyMs = new AtomicLong(0);
        private final AtomicLong captureQueueDepth = new AtomicLong(0);
        private final AtomicLong extractQueueDepth = new AtomicLong(0);
        private final AtomicLong applyQueueDepth = new AtomicLong(0);
        private final AtomicLong checkpointLagSec = new AtomicLong(0);
        private final AtomicLong rpoMs = new AtomicLong(0);
        private final AtomicLong rtoMs = new AtomicLong(0);

        private final AtomicLong captureEventsTotal = new AtomicLong(0);
        private final AtomicLong appliedEventsTotal = new AtomicLong(0);

        private final ConcurrentHashMap<String, ProcessStatus> processStatuses = new ConcurrentHashMap<>();

        public TaskMetrics(String taskId) {
            this.taskId = taskId;
        }

        public AtomicLong captureRateAtomic() { return captureRate; }
        public AtomicLong e2eLatencyAtomic() { return e2eLatencyMs; }
        public AtomicLong captureQueueDepthAtomic() { return captureQueueDepth; }
        public AtomicLong extractQueueDepthAtomic() { return extractQueueDepth; }
        public AtomicLong applyQueueDepthAtomic() { return applyQueueDepth; }
        public AtomicLong checkpointLagAtomic() { return checkpointLagSec; }

        public void recordCaptureRate(long ratePerSec) {
            captureRate.set(ratePerSec);
        }

        public void recordE2eLatency(long latencyMs) {
            e2eLatencyMs.set(latencyMs);
        }

        public void recordQueueDepth(String stage, long depth) {
            switch (stage.toLowerCase()) {
                case "capture" -> captureQueueDepth.set(depth);
                case "extract" -> extractQueueDepth.set(depth);
                case "apply" -> applyQueueDepth.set(depth);
            }
        }

        public void recordCheckpointLag(long lagSec) {
            checkpointLagSec.set(lagSec);
        }

        public void recordRpo(long ms) {
            rpoMs.set(ms);
        }

        public void recordRto(long ms) {
            rtoMs.set(ms);
        }

        public void incrementEventsCaptured(long count) {
            captureEventsTotal.addAndGet(count);
        }

        public void incrementEventsApplied(long count) {
            appliedEventsTotal.addAndGet(count);
        }

        public void updateProcessStatus(String processName, String state, long pid, String uptime,
                                         int retryCount, String circuitBreakerState) {
            processStatuses.put(processName, new ProcessStatus(processName, state, pid, uptime, retryCount, circuitBreakerState));
        }

        public void removeProcessStatus(String processName) {
            processStatuses.remove(processName);
        }

        public Map<String, Object> toSnapshot() {
            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("captureRate", captureRate.get());
            snapshot.put("e2eLatency", e2eLatencyMs.get());
            snapshot.put("queueDepth", captureQueueDepth.get() + extractQueueDepth.get() + applyQueueDepth.get());
            snapshot.put("captureQueueDepth", captureQueueDepth.get());
            snapshot.put("extractQueueDepth", extractQueueDepth.get());
            snapshot.put("applyQueueDepth", applyQueueDepth.get());
            snapshot.put("checkpointLag", checkpointLagSec.get());
            snapshot.put("rpoMs", rpoMs.get());
            snapshot.put("rtoMs", rtoMs.get());
            snapshot.put("captureEventsTotal", captureEventsTotal.get());
            snapshot.put("appliedEventsTotal", appliedEventsTotal.get());

            List<Map<String, Object>> processes = new ArrayList<>();
            for (ProcessStatus ps : processStatuses.values()) {
                Map<String, Object> p = new LinkedHashMap<>();
                p.put("name", ps.name);
                p.put("state", ps.state);
                p.put("pid", ps.pid);
                p.put("uptime", ps.uptime);
                p.put("retryCount", ps.retryCount);
                p.put("circuitBreakerState", ps.circuitBreakerState);
                processes.add(p);
            }
            snapshot.put("processes", processes);

            return snapshot;
        }
    }

    private static class ProcessStatus {
        final String name;
        final String state;
        final long pid;
        final String uptime;
        final int retryCount;
        final String circuitBreakerState;

        ProcessStatus(String name, String state, long pid, String uptime, int retryCount, String circuitBreakerState) {
            this.name = name;
            this.state = state;
            this.pid = pid;
            this.uptime = uptime;
            this.retryCount = retryCount;
            this.circuitBreakerState = circuitBreakerState;
        }
    }
}
