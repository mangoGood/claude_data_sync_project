package com.migration.common.proc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 任务级单实例互斥的核心不变量：同一 taskId+role 第二次取锁必须失败，
 * 且锁文件里留下持锁者 PID（收孤儿时要用）。
 *
 * <p>注意锁文件路径是相对 {@code files/<taskId>/} 的，测试用独一无二的 taskId 并自行清理。
 */
@DisplayName("TaskInstanceLock 任务级单实例互斥")
class TaskInstanceLockTest {

    private final String taskId = "unit-test-lock-" + System.nanoTime();

    @AfterEach
    void cleanup() {
        File dir = new File("files/" + taskId);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) f.delete();
        }
        dir.delete();
    }

    @Test
    @DisplayName("第一次取锁成功，第二次被挡住")
    void secondAcquireIsRejected() {
        TaskInstanceLock first = TaskInstanceLock.tryAcquire(taskId, "capture");
        assertNotNull(first, "首次取锁应成功");
        try {
            assertNull(TaskInstanceLock.tryAcquire(taskId, "capture"),
                    "同一 taskId+role 的第二个实例必须被挡住，否则就是双写");
        } finally {
            first.close();
        }
    }

    @Test
    @DisplayName("不同 role 之间互不干扰")
    void differentRolesDoNotBlockEachOther() {
        TaskInstanceLock capture = TaskInstanceLock.tryAcquire(taskId, "capture");
        TaskInstanceLock extract = TaskInstanceLock.tryAcquire(taskId, "extract");
        try {
            assertNotNull(capture);
            assertNotNull(extract, "capture 与 extract 是两把锁，不该互斥");
        } finally {
            if (capture != null) capture.close();
            if (extract != null) extract.close();
        }
    }

    @Test
    @DisplayName("释放后可以重新取到（进程崩溃后由内核释放，等价于此）")
    void lockIsReusableAfterRelease() {
        TaskInstanceLock first = TaskInstanceLock.tryAcquire(taskId, "increment");
        assertNotNull(first);
        first.close();

        TaskInstanceLock second = TaskInstanceLock.tryAcquire(taskId, "increment");
        assertNotNull(second, "锁释放后应能重新取得");
        second.close();
    }

    @Test
    @DisplayName("锁文件记录持锁 PID 与启动时刻，供 agent 收孤儿使用")
    void lockFileRecordsHolderPid() throws Exception {
        TaskInstanceLock lock = TaskInstanceLock.tryAcquire(taskId, "subscribe");
        assertNotNull(lock);
        try {
            File f = TaskInstanceLock.lockFileFor(taskId, "subscribe");
            assertTrue(f.exists());
            String content = new String(Files.readAllBytes(f.toPath()), "UTF-8");
            assertTrue(content.contains("subscribe"), "锁文件应记录 role: " + content);

            long[] holder = TaskInstanceLock.readHolderPid(f);
            assertNotNull(holder);
            assertEquals(ProcessHandle.current().pid(), holder[0]);
        } finally {
            lock.close();
        }
    }
}
