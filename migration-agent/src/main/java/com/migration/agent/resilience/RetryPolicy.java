package com.migration.agent.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class RetryPolicy {
    private static final Logger logger = LoggerFactory.getLogger(RetryPolicy.class);

    private final int maxRetries;
    private final long initialDelayMs;
    private final double multiplier;
    private final long maxDelayMs;
    private final AtomicInteger attemptCount = new AtomicInteger(0);
    private final Consumer<RetryContext> onRetryCallback;
    private final Consumer<RetryContext> onExhaustedCallback;

    public static class Builder {
        private int maxRetries = 3;
        private long initialDelayMs = 5000;
        private double multiplier = 2.0;
        private long maxDelayMs = 300000;
        private Consumer<RetryContext> onRetryCallback;
        private Consumer<RetryContext> onExhaustedCallback;

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder initialDelayMs(long initialDelayMs) {
            this.initialDelayMs = initialDelayMs;
            return this;
        }

        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        public Builder maxDelayMs(long maxDelayMs) {
            this.maxDelayMs = maxDelayMs;
            return this;
        }

        public Builder onRetry(Consumer<RetryContext> callback) {
            this.onRetryCallback = callback;
            return this;
        }

        public Builder onExhausted(Consumer<RetryContext> callback) {
            this.onExhaustedCallback = callback;
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private RetryPolicy(Builder builder) {
        this.maxRetries = builder.maxRetries;
        this.initialDelayMs = builder.initialDelayMs;
        this.multiplier = builder.multiplier;
        this.maxDelayMs = builder.maxDelayMs;
        this.onRetryCallback = builder.onRetryCallback;
        this.onExhaustedCallback = builder.onExhaustedCallback;
    }

    public long getNextDelayMs() {
        int attempt = attemptCount.get();
        if (attempt >= maxRetries) {
            return -1;
        }
        long delay = (long) (initialDelayMs * Math.pow(multiplier, attempt));
        return Math.min(delay, maxDelayMs);
    }

    public boolean shouldRetry() {
        return attemptCount.get() < maxRetries;
    }

    public boolean recordAttempt() {
        int current = attemptCount.incrementAndGet();
        if (current <= maxRetries) {
            long delay = getNextDelayMsForAttempt(current - 1);
            logger.info("Retry attempt {}/{}, next delay: {}ms", current, maxRetries, delay);

            if (onRetryCallback != null) {
                onRetryCallback.accept(new RetryContext(current, maxRetries, delay));
            }
            return true;
        }

        logger.warn("Retry exhausted after {} attempts", maxRetries);
        if (onExhaustedCallback != null) {
            onExhaustedCallback.accept(new RetryContext(current, maxRetries, -1));
        }
        return false;
    }

    public void sleepBeforeRetry() throws InterruptedException {
        long delay = getNextDelayMsForAttempt(attemptCount.get() - 1);
        if (delay > 0) {
            logger.info("Sleeping {}ms before retry (exponential backoff)...", delay);
            TimeUnit.MILLISECONDS.sleep(delay);
        }
    }

    private long getNextDelayMsForAttempt(int attempt) {
        long delay = (long) (initialDelayMs * Math.pow(multiplier, attempt));
        return Math.min(delay, maxDelayMs);
    }

    public void reset() {
        attemptCount.set(0);
    }

    public int getAttemptCount() {
        return attemptCount.get();
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public static class RetryContext {
        private final int attempt;
        private final int maxRetries;
        private final long delayMs;

        public RetryContext(int attempt, int maxRetries, long delayMs) {
            this.attempt = attempt;
            this.maxRetries = maxRetries;
            this.delayMs = delayMs;
        }

        public int getAttempt() {
            return attempt;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public long getDelayMs() {
            return delayMs;
        }
    }
}
