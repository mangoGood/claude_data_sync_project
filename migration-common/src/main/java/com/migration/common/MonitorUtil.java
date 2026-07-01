package com.migration.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MonitorUtil {
    private static final Logger logger = LoggerFactory.getLogger(MonitorUtil.class);

    private static final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private static final Map<String, Long> timestamps = new ConcurrentHashMap<>();
    private static final Map<String, Object> metrics = new ConcurrentHashMap<>();

    public static void incrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
    }

    public static void incrementCounter(String name, long value) {
        counters.computeIfAbsent(name, k -> new AtomicLong(0)).addAndGet(value);
    }

    public static long getCounter(String name) {
        AtomicLong counter = counters.get(name);
        return counter != null ? counter.get() : 0;
    }

    public static void recordStartTime(String operation) {
        timestamps.put(operation + "_start", System.currentTimeMillis());
    }

    public static long recordEndTime(String operation) {
        Long startTime = timestamps.get(operation + "_start");
        if (startTime == null) {
            return -1;
        }

        long duration = System.currentTimeMillis() - startTime;
        metrics.put(operation + "_duration", duration);

        return duration;
    }

    public static void setMetric(String name, Object value) {
        metrics.put(name, value);
    }

    public static Object getMetric(String name) {
        return metrics.get(name);
    }

    public static void logStatistics() {
        logger.info("=== Monitoring Statistics ===");

        if (!counters.isEmpty()) {
            logger.info("Counters:");
            counters.forEach((name, counter) ->
                logger.info("  {}: {}", name, counter.get()));
        }

        if (!metrics.isEmpty()) {
            logger.info("Metrics:");
            metrics.forEach((name, value) ->
                logger.info("  {}: {}", name, value));
        }

        logger.info("=============================");
    }

    public static void reset() {
        counters.clear();
        timestamps.clear();
        metrics.clear();
        logger.info("Monitoring statistics reset");
    }

    public static Map<String, Long> getAllCounters() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((name, counter) -> result.put(name, counter.get()));
        return result;
    }

    public static Map<String, Object> getAllMetrics() {
        return new ConcurrentHashMap<>(metrics);
    }

    public static double calculateThroughput(String operation, long count) {
        Long startTime = timestamps.get(operation + "_start");
        if (startTime == null || count == 0) {
            return 0.0;
        }

        long duration = System.currentTimeMillis() - startTime;
        if (duration == 0) {
            return 0.0;
        }

        return (count * 1000.0) / duration;
    }

    public static void logThroughput(String operation, long count) {
        double throughput = calculateThroughput(operation, count);
        logger.info("Operation '{}' throughput: {:.2f} events/second", operation, throughput);
    }
}
