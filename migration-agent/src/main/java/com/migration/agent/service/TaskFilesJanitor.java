package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * 任务目录的终态清理。
 *
 * <p>{@code files/<taskId>/} 里装着 THL、cap、日志、H2、位点——一个跑几天的任务能到 GB 级。
 * 任务删除/终结后这些文件<b>从来没有人清</b>：本机实测已累积 12GB / 204 个目录，全是历史残留。
 *
 * <p>为什么是"打标 + 延迟清理"而不是删除时立刻清：
 * <ul>
 *   <li>删除消息到达时子进程可能还在写文件，立刻删会和它们打架；</li>
 *   <li>刚出问题的任务，日志和死信往往正是排障要看的东西，直接删掉等于毁证据。</li>
 * </ul>
 * 所以终结时只写一个 {@code .terminal} 标记，过了保留期（{@code task.files.retention.hours}，
 * 默认 72 小时）再由巡检删除；期间任务若被重新拉起，标记会被自动清掉。
 *
 * <p><b>只清打过标的目录</b>——PAUSED/FAILED 的任务目录绝不动：位点和 checkpoint 都在里面，
 * 删掉就等于把"恢复"变成"从头重来或直接丢数据"。
 */
public class TaskFilesJanitor {
    private static final Logger logger = LoggerFactory.getLogger(TaskFilesJanitor.class);

    private static final String FILES_DIR = "files";
    private static final String TERMINAL_MARKER = ".terminal";

    private final AgentConfig config;
    /** 当前仍在跑的任务（正在运行的目录一律不清）。 */
    private final Supplier<Set<String>> activeTasks;

    public TaskFilesJanitor(AgentConfig config, Supplier<Set<String>> activeTasks) {
        this.config = config;
        this.activeTasks = activeTasks;
    }

    /** 任务进入终态（删除/终结）时打标，记录时刻与原因。 */
    public static void markTerminal(String taskId, String reason) {
        if (taskId == null || taskId.isEmpty()) return;
        File dir = new File(FILES_DIR, taskId);
        if (!dir.isDirectory()) return;
        try {
            Files.write(new File(dir, TERMINAL_MARKER).toPath(),
                    (System.currentTimeMillis() + "|" + (reason == null ? "" : reason) + System.lineSeparator())
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            logger.info("[{}] 任务目录已标记为终态（{}），保留期后清理", taskId, reason);
        } catch (IOException e) {
            logger.warn("[{}] 写终态标记失败: {}", taskId, e.getMessage());
        }
    }

    /** 任务被重新拉起时清掉终态标记，避免保留期到点把活任务的目录删掉。 */
    public static void clearTerminalMark(String taskId) {
        if (taskId == null || taskId.isEmpty()) return;
        File marker = new File(new File(FILES_DIR, taskId), TERMINAL_MARKER);
        if (marker.exists() && marker.delete()) {
            logger.info("[{}] 任务重新启动，终态标记已清除", taskId);
        }
    }

    /** 巡检一轮，返回被删除的任务目录数。 */
    public int sweepOnce() {
        int retentionHours = config.getTaskFilesRetentionHours();
        if (retentionHours <= 0) {
            return 0;
        }
        File root = new File(FILES_DIR);
        File[] dirs = root.listFiles(File::isDirectory);
        if (dirs == null) {
            return 0;
        }
        Set<String> active = activeTasks != null ? activeTasks.get() : java.util.Collections.emptySet();
        long retentionMs = retentionHours * 3600_000L;
        long now = System.currentTimeMillis();
        int removed = 0;

        for (File dir : dirs) {
            String taskId = dir.getName();
            if (active.contains(taskId)) {
                continue;
            }
            File marker = new File(dir, TERMINAL_MARKER);
            if (!marker.isFile()) {
                continue;
            }
            long markedAt = readMarkedAt(marker);
            if (now - markedAt < retentionMs) {
                continue;
            }
            long sizeMb = dirSizeBytes(dir) / (1024 * 1024);
            if (deleteRecursively(dir)) {
                removed++;
                logger.info("[{}] 终态任务目录已清理（标记于 {}h 前，释放 ~{}MB）",
                        taskId, (now - markedAt) / 3600_000L, sizeMb);
            }
        }
        return removed;
    }

    private long readMarkedAt(File marker) {
        try {
            String first = Files.readAllLines(marker.toPath()).stream().findFirst().orElse("");
            int bar = first.indexOf('|');
            return Long.parseLong((bar > 0 ? first.substring(0, bar) : first).trim());
        } catch (Exception e) {
            // 标记文件损坏：退回文件自身的 mtime，仍然只在超过保留期后才清
            return marker.lastModified();
        }
    }

    /**
     * 目录总字节数。
     *
     * <p><b>一定要吞掉所有异常</b>：任务目录里的 THL/cap/队列深度文件是边写边删的，
     * {@code Files.walk} 惰性遍历时随时可能撞上刚消失的文件，抛的还是
     * {@link java.io.UncheckedIOException}（RuntimeException，catch IOException 接不住）。
     * 这是个纯粹的巡检动作，让它把一个健康任务判失败是本末倒置——实测第一版就因此
     * 在 {@code extract_queue_depth} 被重写的瞬间把任务打成 FAILED(E9999)。
     */
    public static long dirSizeBytes(File dir) {
        Path path = dir.toPath();
        try (Stream<Path> walk = Files.walk(path)) {
            return walk.filter(p -> {
                try {
                    return Files.isRegularFile(p);
                } catch (Exception e) {
                    return false;
                }
            }).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (Exception e) {
                    return 0L;
                }
            }).sum();
        } catch (Exception e) {
            logger.debug("统计目录大小失败 {}: {}", dir, e.getMessage());
            return 0L;
        }
    }

    private boolean deleteRecursively(File dir) {
        try (Stream<Path> walk = Files.walk(dir.toPath())) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    logger.debug("删除失败 {}: {}", p, e.getMessage());
                }
            });
        } catch (IOException e) {
            logger.warn("清理任务目录失败 {}: {}", dir, e.getMessage());
            return false;
        }
        return !dir.exists();
    }
}
