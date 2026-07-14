package com.migration.common.ratelimit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RowRateLimiter} 单元测试：验证"配额落到执行层"的核心契约——
 * 未超配额不阻塞，超配额阻塞到窗口结束，unlimited(<=0)完全不限速。
 */
@DisplayName("RowRateLimiter 增量限速")
class RowRateLimiterTest {

    @Test
    @DisplayName("unlimited(<=0)：acquire 立即返回，不阻塞")
    void unlimitedNeverBlocks() throws InterruptedException {
        RowRateLimiter limiter = new RowRateLimiter(0);
        assertTrue(limiter.isUnlimited());
        long t0 = System.currentTimeMillis();
        limiter.acquire(1_000_000); // 超大批量也应立即返回
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed < 50, "unlimited 不应阻塞，实测 " + elapsed + "ms");
    }

    @Test
    @DisplayName("窗口内未超配额：不阻塞")
    void underQuotaDoesNotBlock() throws InterruptedException {
        RowRateLimiter limiter = new RowRateLimiter(1000);
        assertFalse(limiter.isUnlimited());
        long t0 = System.currentTimeMillis();
        limiter.acquire(500);
        limiter.acquire(400);
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed < 100, "未超配额不应阻塞，实测 " + elapsed + "ms");
    }

    @Test
    @DisplayName("单次调用超过配额：阻塞到窗口结束（约 1s 量级）")
    void overQuotaBlocksUntilWindowEnd() throws InterruptedException {
        RowRateLimiter limiter = new RowRateLimiter(100);
        long t0 = System.currentTimeMillis();
        limiter.acquire(150); // 超过 100/s 配额
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed >= 800, "超配额应阻塞到接近窗口结束(~1s)，实测 " + elapsed + "ms");
        assertTrue(elapsed < 1500, "阻塞时间不应明显超过一个窗口，实测 " + elapsed + "ms");
    }

    @Test
    @DisplayName("跨窗口：第二次调用在新窗口重新计数，不与上一窗口的超额累加")
    void newWindowResetsCount() throws InterruptedException {
        RowRateLimiter limiter = new RowRateLimiter(100);
        limiter.acquire(150); // 触发一次窗口阻塞，窗口随之重置
        long t0 = System.currentTimeMillis();
        limiter.acquire(50); // 新窗口内未超配额
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed < 100, "新窗口内未超配额不应阻塞，实测 " + elapsed + "ms");
    }

    @Test
    @DisplayName("非正行数（0/负数）：不计入配额，不阻塞")
    void nonPositiveRowsIgnored() throws InterruptedException {
        RowRateLimiter limiter = new RowRateLimiter(10);
        long t0 = System.currentTimeMillis();
        limiter.acquire(0);
        limiter.acquire(-5);
        long elapsed = System.currentTimeMillis() - t0;
        assertTrue(elapsed < 50);
    }

    @Test
    @DisplayName("并发：一个线程超配额阻塞时，不应持锁阻塞另一线程进入 acquire")
    void sleepDoesNotHoldLockForOtherThreads() throws InterruptedException {
        // A 线程超配额将阻塞约 1s；B 线程随后调用 acquire。若 sleep 在锁内，
        // B 会被卡到 A 睡醒（~1s）；修复后 B 应能立即进入临界区并快速返回。
        RowRateLimiter limiter = new RowRateLimiter(100);
        Thread a = new Thread(() -> {
            try { limiter.acquire(150); } catch (InterruptedException ignored) {}
        });
        a.start();
        Thread.sleep(100); // 确保 A 已进入阻塞等待

        long t0 = System.currentTimeMillis();
        // B 计入的是 A 打开的下一窗口，量小不超配额，应立即返回
        limiter.acquire(1);
        long bElapsed = System.currentTimeMillis() - t0;
        assertTrue(bElapsed < 300, "B 不应被 A 的锁内 sleep 阻塞，实测 " + bElapsed + "ms");

        a.join();
    }
}
