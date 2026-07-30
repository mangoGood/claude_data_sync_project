package com.migration.agent.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * 三态熔断器：CLOSED → OPEN → HALF_OPEN → (CLOSED | OPEN)。
 *
 * <p>为什么必须有 HALF_OPEN：旧实现只有 CLOSED/OPEN，且 {@link #reset()} 全仓无人调用——
 * 连续失败打开后 {@link #allowRequest()} 恒为 false，守护线程直接退出，进程<b>再也不会被拉起</b>。
 * 目标库一次超过重试总时长（默认 ~2.5 分钟）的计划内维护窗口就能把任务永久打死。
 *
 * <p>OPEN 到期后放行<b>一次</b>探测（HALF_OPEN）：探测成功回 CLOSED；探测失败回 OPEN，
 * 并把打开时长按 {@code openTimeoutMultiplier} 指数延长到 {@code maxOpenTimeoutMs} 封顶——
 * 既让"目标库修好了就自己恢复"成立，又不会对一个长期不可用的目标库高频重试。
 */
public class CircuitBreaker {
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED,
        OPEN,
        /** OPEN 到期后的探测态：只放行一次尝试，用它的成败决定回 CLOSED 还是重新 OPEN。 */
        HALF_OPEN
    }

    private final int failureThreshold;
    private final long openTimeoutMs;
    private final double openTimeoutMultiplier;
    private final long maxOpenTimeoutMs;
    private final Consumer<State> onStateChangeCallback;
    private final LongSupplier clock;

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    /** 当前这次 OPEN 的持续时长（每次探测失败都乘 multiplier，封顶 maxOpenTimeoutMs）。 */
    private final AtomicLong currentOpenTimeoutMs = new AtomicLong(0);
    /** OPEN 到期时刻（毫秒时间戳）；非 OPEN 态无意义。 */
    private final AtomicLong openUntilMs = new AtomicLong(0);

    public static class Builder {
        private int failureThreshold = 5;
        private long openTimeoutMs = 60000;
        private double openTimeoutMultiplier = 2.0;
        private long maxOpenTimeoutMs = 1800000;
        private Consumer<State> onStateChangeCallback;
        private LongSupplier clock = System::currentTimeMillis;

        public Builder failureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
            return this;
        }

        /** OPEN 后多久放行第一次探测，默认 60s。 */
        public Builder openTimeoutMs(long openTimeoutMs) {
            this.openTimeoutMs = openTimeoutMs;
            return this;
        }

        /** 探测失败后打开时长的放大倍数，默认 2.0。 */
        public Builder openTimeoutMultiplier(double openTimeoutMultiplier) {
            this.openTimeoutMultiplier = openTimeoutMultiplier;
            return this;
        }

        /** 打开时长上限，默认 30 分钟。 */
        public Builder maxOpenTimeoutMs(long maxOpenTimeoutMs) {
            this.maxOpenTimeoutMs = maxOpenTimeoutMs;
            return this;
        }

        public Builder onStateChange(Consumer<State> callback) {
            this.onStateChangeCallback = callback;
            return this;
        }

        /** 仅供测试注入虚拟时钟。 */
        public Builder clock(LongSupplier clock) {
            this.clock = clock;
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
        this.openTimeoutMs = Math.max(1000, builder.openTimeoutMs);
        this.openTimeoutMultiplier = Math.max(1.0, builder.openTimeoutMultiplier);
        this.maxOpenTimeoutMs = Math.max(this.openTimeoutMs, builder.maxOpenTimeoutMs);
        this.onStateChangeCallback = builder.onStateChangeCallback;
        this.clock = builder.clock != null ? builder.clock : System::currentTimeMillis;
    }

    /**
     * 是否放行本次尝试。CLOSED 恒放行；OPEN 在到期时**自动转 HALF_OPEN 并放行一次**；
     * HALF_OPEN 期间（探测已在飞行中）不再放行第二次。
     */
    public boolean allowRequest() {
        State current = state.get();
        if (current == State.CLOSED) {
            return true;
        }
        if (current == State.OPEN && clock.getAsLong() >= openUntilMs.get()
                && state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
            notifyStateChange(State.HALF_OPEN);
            logger.info("CircuitBreaker OPEN 已到期，转 HALF_OPEN 放行一次探测");
            return true;
        }
        return false;
    }

    /** OPEN 态下距离下次放行探测还剩多久；非 OPEN 态返回 0。 */
    public long getOpenRemainingMs() {
        if (state.get() != State.OPEN) {
            return 0;
        }
        return Math.max(0, openUntilMs.get() - clock.getAsLong());
    }

    public void recordSuccess() {
        State current = state.get();
        if (current == State.HALF_OPEN && state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            failureCount.set(0);
            currentOpenTimeoutMs.set(0);
            notifyStateChange(State.CLOSED);
            logger.info("CircuitBreaker 探测成功，HALF_OPEN → CLOSED");
            return;
        }
        if (current == State.CLOSED) {
            failureCount.set(0);
        }
    }

    public void recordFailure() {
        State current = state.get();
        if (current == State.HALF_OPEN && state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            long next = nextOpenTimeout();
            openUntilMs.set(clock.getAsLong() + next);
            notifyStateChange(State.OPEN);
            logger.warn("CircuitBreaker 探测失败，HALF_OPEN → OPEN，下次探测 {}ms 后", next);
            return;
        }
        int count = failureCount.incrementAndGet();
        if (count >= failureThreshold && state.compareAndSet(State.CLOSED, State.OPEN)) {
            long next = nextOpenTimeout();
            openUntilMs.set(clock.getAsLong() + next);
            notifyStateChange(State.OPEN);
            logger.warn("CircuitBreaker CLOSED → OPEN（连续失败 {} 次），{}ms 后放行探测", count, next);
        }
    }

    /** 打开时长：首次取 openTimeoutMs，之后每次乘 multiplier，封顶 maxOpenTimeoutMs。 */
    private long nextOpenTimeout() {
        long prev = currentOpenTimeoutMs.get();
        long next = prev <= 0 ? openTimeoutMs
                : Math.min(maxOpenTimeoutMs, (long) (prev * openTimeoutMultiplier));
        currentOpenTimeoutMs.set(next);
        return next;
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
        currentOpenTimeoutMs.set(0);
        openUntilMs.set(0);
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
        return String.format("CircuitBreaker{state=%s, failures=%d/%d, openRemaining=%dms}",
            state.get(), failureCount.get(), failureThreshold, getOpenRemainingMs());
    }
}
