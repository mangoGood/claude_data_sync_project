package com.migration.common.watch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DirectoryChangeWatcher} 单元测试：验证"事件驱动握手"的核心契约——
 * 有文件创建/追加时立即唤醒（而非等满兜底超时），无事件时按兜底超时返回。
 *
 * <p>断言用宽松上界（远小于旧的 3000ms 轮询间隔，但给 CI/慢机器留余量）：
 * Linux(inotify) 下实际唤醒 ~ms；macOS(轮询式 WatchService) 下由兜底超时兜底。
 */
class DirectoryChangeWatcherTest {

    @Test
    void awaitReturnsPromptlyOnFileCreate(@TempDir Path dir) throws Exception {
        try (DirectoryChangeWatcher watcher = new DirectoryChangeWatcher(dir.toString())) {
            long fallbackMs = 5000; // 兜底很长：若靠兜底返回，会明显超过下面的断言上界
            CompletableFuture<Long> elapsed = CompletableFuture.supplyAsync(() -> {
                long t0 = System.currentTimeMillis();
                try {
                    watcher.awaitChange(fallbackMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return System.currentTimeMillis() - t0;
            });

            Thread.sleep(150); // 确保 await 已进入等待
            Files.writeString(dir.resolve("binlog_0001.cap"), "hello");

            long ms;
            try {
                ms = elapsed.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError("awaitChange 未在文件创建后及时返回（疑似只靠兜底超时）", e);
            }
            // 唤醒应远快于 5000ms 兜底；给足余量取 2500ms（仍 < 旧 3000ms 轮询）
            assertTrue(ms < 2500, "文件创建后应尽快唤醒，实测 " + ms + "ms");
        }
    }

    @Test
    void awaitReturnsPromptlyOnFileModify(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("binlog_0001.thl");
        Files.writeString(file, "seed");

        try (DirectoryChangeWatcher watcher = new DirectoryChangeWatcher(dir.toString())) {
            CompletableFuture<Long> elapsed = CompletableFuture.supplyAsync(() -> {
                long t0 = System.currentTimeMillis();
                try {
                    watcher.awaitChange(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return System.currentTimeMillis() - t0;
            });

            Thread.sleep(150);
            // 追加写：extract/capture 会向"当前最新文件"追加，消费端必须被 MODIFY 唤醒
            Files.writeString(file, "seed+more", java.nio.file.StandardOpenOption.APPEND);

            long ms = elapsed.get(3, TimeUnit.SECONDS);
            assertTrue(ms < 2500, "文件追加写后应尽快唤醒，实测 " + ms + "ms");
        }
    }

    @Test
    void awaitReturnsFalseOnTimeoutWhenIdle(@TempDir Path dir) throws Exception {
        try (DirectoryChangeWatcher watcher = new DirectoryChangeWatcher(dir.toString())) {
            long t0 = System.currentTimeMillis();
            boolean changed = watcher.awaitChange(300);
            long ms = System.currentTimeMillis() - t0;

            assertFalse(changed, "空闲时应返回 false（兜底超时）");
            assertTrue(ms >= 250, "应等待约兜底超时才返回，实测 " + ms + "ms");
        }
    }

    @Test
    void afterCloseDoesNotBusyLoop(@TempDir Path dir) throws IOException, InterruptedException {
        DirectoryChangeWatcher watcher = new DirectoryChangeWatcher(dir.toString());
        watcher.close();
        long t0 = System.currentTimeMillis();
        boolean changed = watcher.awaitChange(200);
        long ms = System.currentTimeMillis() - t0;
        assertFalse(changed);
        // 关闭后应退化为 sleep(超时)，而不是立即返回导致调用方忙循环
        assertTrue(ms >= 150, "关闭后应按超时休眠避免忙循环，实测 " + ms + "ms");
    }
}
