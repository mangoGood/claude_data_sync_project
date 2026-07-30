package com.migration.agent.resilience;

import com.migration.common.proc.TaskInstanceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * agent 启动时先收孤儿：清掉上一任 agent 留下的、还活着的子进程。
 *
 * <p><b>为什么必须在恢复任务之前做</b>：agent 被 SIGKILL 后子进程会被 init 收养并继续运行
 * （继续拉 binlog、继续写目标库）。新 agent 起来走恢复流程，为同一 taskId 再拉一套子进程 ——
 * 于是同一批变更被应用两遍。{@link TaskInstanceLock} 会挡住第二套，但那样任务就永远起不来了
 * （锁被孤儿占着，ProcessGuard 反复重试直到熔断）。所以顺序只能是：<b>先收孤儿，再恢复任务</b>。
 *
 * <p><b>怎么找到孤儿</b>：不用 {@code pgrep} 解析命令行（macOS 上 {@code ProcessHandle.info().arguments()}
 * 拿不到，各平台行为不一），而是读每个子进程自己写下的锁文件 {@code files/<taskId>/.<role>.lock}，
 * 里面记着 {@code pid|启动时刻}。启动时刻用来排除 PID 复用——只有 PID 存在<b>且</b>启动时刻吻合，
 * 才认定是我们要找的那个进程，避免误杀一个恰好复用了该 PID 的无关进程。
 */
public final class OrphanChildReaper {

    private static final Logger logger = LoggerFactory.getLogger(OrphanChildReaper.class);

    /** 关掉收孤儿（同机多 agent 共享 files/ 的调试场景）。 */
    public static final String ENABLED_KEY = "agent.orphan.reap.enabled";

    private static final long GRACEFUL_WAIT_MS = 5000;

    private OrphanChildReaper() {
    }

    /** @return 被清理掉的孤儿进程数 */
    public static int reap() {
        if (!Boolean.parseBoolean(System.getProperty(ENABLED_KEY,
                System.getenv().getOrDefault("AGENT_ORPHAN_REAP_ENABLED", "true")))) {
            logger.warn("孤儿子进程回收已关闭（{}=false）", ENABLED_KEY);
            return 0;
        }

        File filesDir = new File("files");
        File[] taskDirs = filesDir.listFiles(File::isDirectory);
        if (taskDirs == null || taskDirs.length == 0) {
            return 0;
        }

        long selfPid = ProcessHandle.current().pid();
        List<ProcessHandle> orphans = new ArrayList<>();
        List<String> descriptions = new ArrayList<>();

        for (File taskDir : taskDirs) {
            File[] locks = taskDir.listFiles((d, name) -> name.startsWith(".") && name.endsWith(".lock"));
            if (locks == null) continue;
            for (File lockFile : locks) {
                long[] holder = TaskInstanceLock.readHolderPid(lockFile);
                if (holder == null || holder[0] == selfPid) continue;
                Optional<ProcessHandle> h = ProcessHandle.of(holder[0]);
                if (!h.isPresent() || !h.get().isAlive()) continue;
                if (!startTimeMatches(h.get(), holder[1])) {
                    // PID 复用：该 PID 上现在跑的是别的程序，绝不能碰
                    logger.debug("锁文件 {} 记录的 pid={} 启动时刻不匹配，判定为 PID 复用，跳过",
                            lockFile.getPath(), holder[0]);
                    continue;
                }
                if (!looksLikeJava(h.get())) {
                    // 第二道保险：我们的子进程一定是 JVM。启动时刻拿不到的平台上，
                    // 这条能挡住"PID 恰好被一个非 Java 程序复用"的误杀。
                    logger.warn("锁文件 {} 记录的 pid={} 当前不是 Java 进程，跳过以免误杀",
                            lockFile.getPath(), holder[0]);
                    continue;
                }
                orphans.add(h.get());
                descriptions.add(taskDir.getName() + "/" + lockFile.getName() + " pid=" + holder[0]);
            }
        }

        if (orphans.isEmpty()) {
            logger.info("未发现上一任 agent 遗留的子进程");
            return 0;
        }

        logger.warn("发现 {} 个上一任 agent 遗留的子进程（agent 硬崩后未随之退出），"
                + "在恢复任务前先清理，避免同一任务双写: {}", orphans.size(), descriptions);

        for (ProcessHandle h : orphans) {
            try {
                h.destroy();
            } catch (Exception e) {
                logger.warn("终止孤儿进程 pid={} 失败: {}", h.pid(), e.getMessage());
            }
        }
        long deadline = System.currentTimeMillis() + GRACEFUL_WAIT_MS;
        for (ProcessHandle h : orphans) {
            while (h.isAlive() && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (h.isAlive()) {
                logger.warn("孤儿进程 pid={} 未在 {}ms 内退出，强制终止", h.pid(), GRACEFUL_WAIT_MS);
                h.destroyForcibly();
            }
        }
        logger.info("孤儿子进程清理完成，共 {} 个", orphans.size());
        return orphans.size();
    }

    private static boolean looksLikeJava(ProcessHandle h) {
        String cmd = h.info().command().orElse("");
        if (cmd.isEmpty()) {
            return true; // 平台拿不到命令名，靠 PID+启动时刻判断
        }
        String base = cmd.substring(cmd.lastIndexOf('/') + 1);
        return base.equals("java") || base.startsWith("java");
    }

    private static boolean startTimeMatches(ProcessHandle h, long recordedStartMs) {
        if (recordedStartMs <= 0) {
            return true; // 平台拿不到启动时刻，退化成只比 PID
        }
        long actual = h.info().startInstant().map(java.time.Instant::toEpochMilli).orElse(0L);
        return actual <= 0 || Math.abs(actual - recordedStartMs) <= 2000;
    }
}
