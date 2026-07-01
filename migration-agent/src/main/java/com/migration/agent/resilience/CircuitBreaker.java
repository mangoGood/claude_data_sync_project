package com.migration.agent.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class CircuitBreaker {
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED,
        OPEN
    }

    private final int failureThreshold;
    private final Consumer<State> onStateChangeCallback;

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);

    public static class Builder {
        private int failureThreshold = 5;
        private Consumer<State> onStateChangeCallback;

        public Builder failureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
            return this;
        }

        public Builder onStateChange(Consumer<State> callback) {
            this.onStateChangeCallback = callback;
            return this;
        }

        public CircuitBreaker build() {
            return new CircuitBreaker(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private CircuitBreaker(Builder builder) {
        this.failureThreshold = builder.failureThreshold;
        this.onStateChangeCallback = builder.onStateChangeCallback;
    }

    public boolean allowRequest() {
        return state.get() == State.CLOSED;
    }

    public void recordSuccess() {
        if (state.get() == State.CLOSED) {
            failureCount.set(0);
        }
    }

    public void recordFailure() {
        int count = failureCount.incrementAndGet();
        if (count >= failureThreshold && state.compareAndSet(State.CLOSED, State.OPEN)) {
            notifyStateChange(State.OPEN);
            logger.warn("CircuitBreaker transitioned from CLOSED to OPEN (consecutive failures: {})", count);
        }
    }

    public State getState() {
        return state.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    public void reset() {
        State oldState = state.getAndSet(State.CLOSED);
        failureCount.set(0);
        if (oldState != State.CLOSED) {
            notifyStateChange(State.CLOSED);
            logger.info("CircuitBreaker manually reset to CLOSED");
        }
    }

    private void notifyStateChange(State newState) {
        if (onStateChangeCallback != null) {
            try {
                onStateChangeCallback.accept(newState);
            } catch (Exception e) {
                logger.error("Error in CircuitBreaker state change callback", e);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("CircuitBreaker{state=%s, failures=%d/%d}",
            state.get(), failureCount.get(), failureThreshold);
    }
}
