package com.migration.agent.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MicrometerBridge {
    private static final Logger logger = LoggerFactory.getLogger(MicrometerBridge.class);

    private final PrometheusMeterRegistry meterRegistry;
    private final List<String> registeredMeters = new ArrayList<>();

    public MicrometerBridge() {
        this.meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        logger.info("MicrometerBridge initialized with PrometheusMeterRegistry");
    }

    public PrometheusMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public String scrape() {
        return meterRegistry.scrape();
    }

    public void registerTaskGauges(String taskId, MetricsService.TaskMetrics metrics) {
        Gauge.builder("migration.capture.rate", metrics.captureRateAtomic(), v -> v.get())
            .tag("task", taskId)
            .description("Event capture rate in events/sec")
            .register(meterRegistry);
        registeredMeters.add("migration.capture.rate:" + taskId);

        Gauge.builder("migration.e2e.latency", metrics.e2eLatencyAtomic(), v -> v.get())
            .tag("task", taskId)
            .description("End-to-end latency in milliseconds")
            .register(meterRegistry);
        registeredMeters.add("migration.e2e.latency:" + taskId);

        Gauge.builder("migration.queue.depth.capture", metrics.captureQueueDepthAtomic(), v -> v.get())
            .tag("task", taskId)
            .description("Capture stage queue depth")
            .register(meterRegistry);
        registeredMeters.add("migration.queue.depth.capture:" + taskId);

        Gauge.builder("migration.queue.depth.extract", metrics.extractQueueDepthAtomic(), v -> v.get())
            .tag("task", taskId)
            .description("Extract stage queue depth")
            .register(meterRegistry);
        registeredMeters.add("migration.queue.depth.extract:" + taskId);

        Gauge.builder("migration.queue.depth.apply", metrics.applyQueueDepthAtomic(), v -> v.get())
            .tag("task", taskId)
            .description("Apply stage queue depth")
            .register(meterRegistry);
        registeredMeters.add("migration.queue.depth.apply:" + taskId);

        Gauge.builder("migration.checkpoint.lag", metrics.checkpointLagAtomic(), v -> v.get())
            .tag("task", taskId)
            .description("Checkpoint lag in seconds")
            .register(meterRegistry);
        registeredMeters.add("migration.checkpoint.lag:" + taskId);

        logger.debug("Registered Micrometer gauges for task: {}", taskId);
    }

    public void unregisterTaskGauges(String taskId) {
        meterRegistry.find("migration.capture.rate").tags("task", taskId).meters()
            .forEach(meterRegistry::remove);
        meterRegistry.find("migration.e2e.latency").tags("task", taskId).meters()
            .forEach(meterRegistry::remove);
        meterRegistry.find("migration.queue.depth.capture").tags("task", taskId).meters()
            .forEach(meterRegistry::remove);
        meterRegistry.find("migration.queue.depth.extract").tags("task", taskId).meters()
            .forEach(meterRegistry::remove);
        meterRegistry.find("migration.queue.depth.apply").tags("task", taskId).meters()
            .forEach(meterRegistry::remove);
        meterRegistry.find("migration.checkpoint.lag").tags("task", taskId).meters()
            .forEach(meterRegistry::remove);
        registeredMeters.removeIf(m -> m.endsWith(":" + taskId));
        logger.debug("Unregistered Micrometer gauges for task: {}", taskId);
    }
}
