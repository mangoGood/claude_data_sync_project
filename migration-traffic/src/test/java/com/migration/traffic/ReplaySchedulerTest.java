package com.migration.traffic;

import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.replay.ReplayOptions;
import com.migration.traffic.replay.ReplayScheduler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("回放时间轴调度")
class ReplaySchedulerTest {

    private static RecordingManifest manifestWithGap(long fromT, long toT) {
        RecordingManifest m = new RecordingManifest();
        if (toT > fromT) {
            m.gaps.add(new RecordingManifest.Gap(fromT, toT, "CAPTURE_RESUMED"));
        }
        return m;
    }

    @Test
    @DisplayName("截止时刻按绝对偏移算，不累加 —— 累加睡眠会把误差攒成秒级漂移")
    void deadlinesAreAbsolute() {
        ReplayOptions o = new ReplayOptions();
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        long d0 = s.deadlineNanos(0);
        long d1 = s.deadlineNanos(5_000_000L);      // 录制里 +5s
        long d2 = s.deadlineNanos(10_000_000L);     // 录制里 +10s
        assertEquals(5_000_000_000L, d1 - d0, 1_000_000L, "间隔必须是 5s");
        assertEquals(5_000_000_000L, d2 - d1, 1_000_000L);
    }

    @Test
    @DisplayName("倍速：2 倍速时间隔减半")
    void speedScalesIntervals() {
        ReplayOptions o = new ReplayOptions();
        o.speed = 2.0;
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        long d0 = s.deadlineNanos(0);
        long d1 = s.deadlineNanos(10_000_000L);
        assertEquals(5_000_000_000L, d1 - d0, 1_000_000L);
    }

    @Test
    @DisplayName("空洞 PRESERVE：空洞时长原样保留在时间轴上")
    void gapPreserved() {
        ReplayOptions o = new ReplayOptions();
        o.gapPolicy = ReplayOptions.GapPolicy.PRESERVE;
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(1_000_000L, 31_000_000L));
        s.start();
        long before = s.deadlineNanos(1_000_000L);
        long after = s.deadlineNanos(31_000_000L);
        assertEquals(30_000_000_000L, after - before, 1_000_000L, "30s 空洞应原样等待");
    }

    @Test
    @DisplayName("空洞 COMPRESS：空洞被压成 0，后续语句整体提前")
    void gapCompressed() {
        ReplayOptions o = new ReplayOptions();
        o.gapPolicy = ReplayOptions.GapPolicy.COMPRESS;
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(1_000_000L, 31_000_000L));
        s.start();
        long before = s.deadlineNanos(1_000_000L);
        long after = s.deadlineNanos(31_000_000L);
        assertEquals(0L, after - before, 1_000_000L, "空洞被压掉后两者应几乎同时");
    }

    @Test
    @DisplayName("SKIP 档：过期太久的语句被丢弃")
    void skipPolicyDropsStale() throws InterruptedException {
        ReplayOptions o = new ReplayOptions();
        o.lagPolicy = ReplayOptions.LagPolicy.SKIP;
        o.lagSkipMs = 50;
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        Thread.sleep(200);
        assertFalse(s.awaitTurn(0), "计划时刻已过去 200ms，超过 50ms 容忍度，应丢弃");
    }

    @Test
    @DisplayName("WAIT 档：再晚也不丢，只把落后量记下来")
    void waitPolicyNeverDrops() throws InterruptedException {
        ReplayOptions o = new ReplayOptions();
        o.lagPolicy = ReplayOptions.LagPolicy.WAIT;
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        Thread.sleep(120);
        assertTrue(s.awaitTurn(0), "WAIT 档不该丢语句");
        assertTrue(s.maxLagMicros() >= 100_000L, "落后量应被记录，实际 " + s.maxLagMicros() + "us");
    }

    @Test
    @DisplayName("STRETCH 档：落后后把时间轴整体顺延，后续间隔保持正确")
    void stretchShiftsTimeline() throws InterruptedException {
        ReplayOptions o = new ReplayOptions();
        o.lagPolicy = ReplayOptions.LagPolicy.STRETCH;
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        long plainDeadline = s.deadlineNanos(1_000_000L);
        Thread.sleep(120);
        s.awaitTurn(0);                       // 已落后 ~120ms → 平移量增加
        long shifted = s.deadlineNanos(1_000_000L);
        assertTrue(shifted > plainDeadline, "STRETCH 应把后续截止时刻顺延");
    }

    @Test
    @DisplayName("执行侧偏差与派发侧分开统计 —— 派发侧有队列兜着，永远好看")
    void executionLagTrackedSeparately() {
        ReplayOptions o = new ReplayOptions();
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        s.recordExecutionLag(800_000_000L);   // 执行落后 800ms
        s.recordExecutionLag(1_200_000_000L);
        assertEquals(0, s.maxLagMicros(), "没走 awaitTurn，派发偏差应为 0");
        assertEquals(1_200_000L, s.execMaxLagMicros());
        assertEquals(1200, s.execLastLagMs());
        assertTrue(s.execPercentileLagMs(0.99) >= 1200);
        assertEquals(2, s.execSamples());
    }

    @Test
    @DisplayName("按时到达时不该记出落后量")
    void onTimeHasNoLag() throws InterruptedException {
        ReplayOptions o = new ReplayOptions();
        ReplayScheduler s = new ReplayScheduler(o, manifestWithGap(0, 0));
        s.start();
        assertTrue(s.awaitTurn(30_000L));     // 30ms 之后
        assertTrue(s.maxLagMicros() < 20_000L, "准点执行的偏差应很小，实际 " + s.maxLagMicros() + "us");
    }
}
