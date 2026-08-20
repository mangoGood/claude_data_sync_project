package com.migration.traffic.replay;

import com.migration.traffic.model.RecordingManifest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * 时间轴调度：把录制里的相对偏移换算成本次回放的<b>绝对截止时刻</b>，并等到那一刻。
 *
 * <p>两个要点：
 * <ol>
 *   <li><b>绝对截止时刻，不做累加睡眠。</b> {@code sleep(间隔)} 逐条累加会把每次的调度误差
 *       攒成秒级漂移——录制里 1000 条间隔 10ms 的语句，累加睡眠跑完可能比原来长十几秒。
 *       这里每条都用 {@code t0 + t/speed} 现算，误差不累积。</li>
 *   <li><b>{@code Thread.sleep} 精度不够。</b> 它在 Linux 上的实际粒度是 1~15ms，
 *       而录制里语句间隔常常就是几毫秒。距截止 >2ms 用 {@code parkNanos}，
 *       最后 2ms 自旋。</li>
 * </ol>
 */
public final class ReplayScheduler {

    /** 进入自旋的阈值：再近就别 park 了，park 的唤醒抖动本身就有 1ms 量级。 */
    private static final long SPIN_THRESHOLD_NANOS = 2_000_000L;

    private final double speed;
    private final ReplayOptions.LagPolicy lagPolicy;
    private final long lagSkipNanos;
    private final List<long[]> compressedGaps = new ArrayList<>();

    private long t0Nanos;
    /** STRETCH 档累计的时间轴平移量（纳秒）。 */
    private long shiftNanos;

    private long maxLagNanos;
    private long lagSumNanos;
    private long lagSamples;
    private final long[] lagHistogramNanos;

    /**
     * <b>执行</b>侧的偏差统计（会话线程真正开始跑这条语句的时刻 − 计划时刻）。
     *
     * <p>与投递偏差分开统计，是因为两者可以差出好几个数量级：会话队列有上万的深度，
     * 派发线程可以准点把语句全丢进队列（投递偏差 ≈ 0），而目标库还在慢吞吞地执行，
     * 实际落后几分钟。只报投递偏差 = 报了一个"永远很好看"的数字，
     * 而回放到底像不像恰恰要看执行侧。
     */
    private final java.util.concurrent.atomic.AtomicLong execMaxNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong execSumNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong execSamples = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong execLastNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLongArray execHistogram =
            new java.util.concurrent.atomic.AtomicLongArray(4096);

    public ReplayScheduler(ReplayOptions options, RecordingManifest manifest) {
        this.speed = options.speed;
        this.lagPolicy = options.lagPolicy;
        this.lagSkipNanos = options.lagSkipMs * 1_000_000L;
        this.lagHistogramNanos = new long[4096];
        if (options.gapPolicy == ReplayOptions.GapPolicy.COMPRESS && manifest.gaps != null) {
            for (RecordingManifest.Gap g : manifest.gaps) {
                compressedGaps.add(new long[]{g.fromT, g.durationUs()});
            }
            compressedGaps.sort((a, b) -> Long.compare(a[0], b[0]));
        }
    }

    /** 开始计时。之后所有截止时刻都相对它算。 */
    public void start() {
        t0Nanos = System.nanoTime();
    }

    /** 某条记录（录制偏移 tUs）在本次回放里的截止时刻（nanoTime 坐标）。 */
    public long deadlineNanos(long tUs) {
        long effectiveUs = tUs - compressedBefore(tUs);
        return t0Nanos + shiftNanos + (long) (effectiveUs * 1000L / speed);
    }

    /**
     * 等到该条记录的时刻。
     *
     * @return true = 该执行；false = 已过期太久且策略为 SKIP，调用方应丢弃
     */
    public boolean awaitTurn(long tUs) throws InterruptedException {
        long deadline = deadlineNanos(tUs);
        long now = System.nanoTime();
        long lag = now - deadline;

        if (lag > 0) {
            recordLag(lag);
            if (lagPolicy == ReplayOptions.LagPolicy.SKIP && lag > lagSkipNanos) {
                return false;
            }
            if (lagPolicy == ReplayOptions.LagPolicy.STRETCH) {
                // 把落后的这一段整体加进平移量，后续语句的相对间隔因此保持正确
                shiftNanos += lag;
            }
            return true;    // 已经晚了，立刻执行
        }

        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            if (remaining > SPIN_THRESHOLD_NANOS) {
                LockSupport.parkNanos(remaining - SPIN_THRESHOLD_NANOS);
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("回放调度被中断");
                }
            } else {
                Thread.onSpinWait();
            }
        }
        recordLag(0);
        return true;
    }

    /** 落在 tUs 之前的空洞总时长（COMPRESS 档才非零）。 */
    private long compressedBefore(long tUs) {
        long sum = 0;
        for (long[] g : compressedGaps) {
            if (g[0] < tUs) sum += g[1];
            else break;
        }
        return sum;
    }

    private void recordLag(long lagNanos) {
        if (lagNanos < 0) lagNanos = 0;
        lagSamples++;
        lagSumNanos += lagNanos;
        if (lagNanos > maxLagNanos) maxLagNanos = lagNanos;
        // 直方图按毫秒分桶，超出上限归到最后一桶
        int bucket = (int) Math.min(lagHistogramNanos.length - 1, lagNanos / 1_000_000L);
        lagHistogramNanos[bucket]++;
    }

    public long maxLagMicros() {
        return maxLagNanos / 1000L;
    }

    public long avgLagMicros() {
        return lagSamples == 0 ? 0 : (lagSumNanos / lagSamples) / 1000L;
    }

    /** 时间轴偏差分位数（毫秒）。这是"回放到底像不像"的答案，必须一等公民对待。 */
    public long percentileLagMs(double q) {
        if (lagSamples == 0) return 0;
        long target = (long) Math.ceil(lagSamples * q);
        long acc = 0;
        for (int i = 0; i < lagHistogramNanos.length; i++) {
            acc += lagHistogramNanos[i];
            if (acc >= target) return i;
        }
        return lagHistogramNanos.length - 1;
    }

    public long samples() {
        return lagSamples;
    }

    /** 会话线程真正开始执行时调用，nanos = 实际时刻 − 计划截止时刻（负数按 0 计）。 */
    public void recordExecutionLag(long nanos) {
        if (nanos < 0) nanos = 0;
        execSamples.incrementAndGet();
        execSumNanos.addAndGet(nanos);
        execLastNanos.set(nanos);
        execMaxNanos.accumulateAndGet(nanos, Math::max);
        int bucket = (int) Math.min(execHistogram.length() - 1, nanos / 1_000_000L);
        execHistogram.incrementAndGet(bucket);
    }

    public long execMaxLagMicros() {
        return execMaxNanos.get() / 1000L;
    }

    public long execAvgLagMicros() {
        long n = execSamples.get();
        return n == 0 ? 0 : (execSumNanos.get() / n) / 1000L;
    }

    /** 最近一条语句的执行偏差（毫秒）：UI 上"当前落后多少"就是它。 */
    public long execLastLagMs() {
        return execLastNanos.get() / 1_000_000L;
    }

    public long execPercentileLagMs(double q) {
        long total = execSamples.get();
        if (total == 0) return 0;
        long target = (long) Math.ceil(total * q);
        long acc = 0;
        for (int i = 0; i < execHistogram.length(); i++) {
            acc += execHistogram.get(i);
            if (acc >= target) return i;
        }
        return execHistogram.length() - 1;
    }

    public long execSamples() {
        return execSamples.get();
    }

    /** 本次回放已进行的时长（毫秒）。 */
    public long elapsedMs() {
        return (System.nanoTime() - t0Nanos) / 1_000_000L;
    }
}
