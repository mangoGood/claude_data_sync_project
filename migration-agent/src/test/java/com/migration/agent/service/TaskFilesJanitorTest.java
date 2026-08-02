package com.migration.agent.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 终态任务目录清理。
 *
 * <p>这里最重要的两条是"<b>不该删的绝不能删</b>"：
 * 没打终态标记的目录（PAUSED/FAILED，位点和 checkpoint 都在里面）不动，
 * 正在跑的任务目录不动。删错的代价是恢复变成从头重来或直接丢数据。
 */
@DisplayName("任务目录终态清理：只清打过标且过保留期的")
class TaskFilesJanitorTest {

    private final File root = new File("files");
    private String prefix;

    @BeforeEach
    void setUp() {
        prefix = "janitor-test-" + System.nanoTime() + "-";
        root.mkdirs();
    }

    @AfterEach
    void tearDown() throws Exception {
        File[] dirs = root.listFiles(f -> f.isDirectory() && f.getName().startsWith("janitor-test-"));
        if (dirs == null) return;
        for (File dir : dirs) {
            try (java.util.stream.Stream<java.nio.file.Path> walk = Files.walk(dir.toPath())) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private File taskDir(String suffix) throws Exception {
        File dir = new File(root, prefix + suffix);
        new File(dir, "binlog_output").mkdirs();
        Files.write(new File(dir, "config.properties").toPath(), "a=b".getBytes(StandardCharsets.UTF_8));
        return dir;
    }

    /** 直接写标记文件，把"打标时刻"挪到过去，免得测试真等 72 小时。 */
    private void markAt(File dir, long ts) throws Exception {
        Files.write(new File(dir, ".terminal").toPath(),
                (ts + "|deleted\n").getBytes(StandardCharsets.UTF_8));
    }

    private TaskFilesJanitor janitor(Set<String> active) {
        return new TaskFilesJanitor(new AgentConfig(), () -> active);
    }

    @Test
    @DisplayName("过了保留期的终态目录被清理")
    void sweepsExpiredTerminalDir() throws Exception {
        File dir = taskDir("expired");
        markAt(dir, System.currentTimeMillis() - 100L * 3600_000L);   // 100 小时前，超过默认 72h

        assertEquals(1, janitor(Collections.emptySet()).sweepOnce());
        assertFalse(dir.exists(), "过期的终态目录应被整棵删掉");
    }

    @Test
    @DisplayName("保留期内的终态目录留着（排障还要看日志和死信）")
    void keepsRecentTerminalDir() throws Exception {
        File dir = taskDir("recent");
        markAt(dir, System.currentTimeMillis() - 3600_000L);          // 1 小时前

        assertEquals(0, janitor(Collections.emptySet()).sweepOnce());
        assertTrue(dir.exists());
    }

    @Test
    @DisplayName("没打终态标记的目录一律不动（PAUSED/FAILED 的位点在里面）")
    void neverTouchesUnmarkedDir() throws Exception {
        File dir = taskDir("unmarked");
        dir.setLastModified(System.currentTimeMillis() - 1000L * 3600_000L);

        assertEquals(0, janitor(Collections.emptySet()).sweepOnce());
        assertTrue(dir.exists());
        assertTrue(new File(dir, "config.properties").exists());
    }

    @Test
    @DisplayName("正在运行的任务目录不动，哪怕标记已过期")
    void skipsActiveTask() throws Exception {
        File dir = taskDir("active");
        markAt(dir, System.currentTimeMillis() - 100L * 3600_000L);

        assertEquals(0, janitor(Set.of(dir.getName())).sweepOnce());
        assertTrue(dir.exists());
    }

    @Test
    @DisplayName("任务重新拉起会撤销终态标记")
    void clearMarkOnRestart() throws Exception {
        File dir = taskDir("restarted");
        TaskFilesJanitor.markTerminal(dir.getName(), "deleted");
        assertTrue(new File(dir, ".terminal").isFile());

        TaskFilesJanitor.clearTerminalMark(dir.getName());
        assertFalse(new File(dir, ".terminal").exists());

        markAt(dir, System.currentTimeMillis() - 100L * 3600_000L);
        TaskFilesJanitor.clearTerminalMark(dir.getName());
        assertEquals(0, janitor(Collections.emptySet()).sweepOnce(), "标记撤销后不该再被清理");
        assertTrue(dir.exists());
    }
}
