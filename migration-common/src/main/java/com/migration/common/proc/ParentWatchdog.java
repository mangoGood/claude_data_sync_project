package com.migration.common.proc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 子进程随父进程（agent）一起死。
 *
 * <p>Java 的 {@code ProcessBuilder} 起出来的子进程在父进程被 SIGKILL 后会被 init 收养并<b>继续运行</b>，
 * 于是 agent 硬崩留下一堆还在拉 binlog、还在写目标库的孤儿进程。仓库里靠
 * {@code restart_agent.sh} 的 {@code pkill} 人肉兜底，但那只覆盖"通过脚本重启"这一条路径——
 * agent 自己 OOM、被运维 kill -9、容器被驱逐时都不过脚本。
 *
 * <p>本看门狗在每个子进程内起一个守护线程，每 {@value #CHECK_INTERVAL_MS}ms 检查父 PID 是否还在，
 * 父没了就自杀。与 {@link TaskInstanceLock} 是互补的两道闸：看门狗让孤儿在秒级自行消失，
 * 文件锁保证万一没消失也绝不会有第二套进程写同一个目标。
 */
public final class ParentWatchdog {

    private static final Logger logger = LoggerFactory.getLogger(ParentWatchdog.class);

    /** agent 通过 -D 传入自身 PID。 */
    public static final String PID_KEY = "agent.watchdog.pid";
    /** agent 自身的启动时刻（epoch ms），用于排除 PID 复用。 */
    public static final String START_KEY = "agent.watchdog.start";

    private static final long CHECK_INTERVAL_MS = 5000;
    /** 父进程已死 → 优雅退出的最长等待，超时硬退（避免 shutdown hook 卡住让孤儿续命）。 */
    private static final long GRACEFUL_EXIT_TIMEOUT_MS = 15000;
    private static final int EXIT_CODE_PARENT_GONE = 17;

    private ParentWatchdog() {
    }

    /** 未配置 {@value #PID_KEY} 时静默 no-op（手工/测试直接跑 jar 的场景）。 */
    public static void start() {
        String pidStr = System.getProperty(PID_KEY, "");
        if (pidStr.isEmpty()) {
            return;
        }
        final long parentPid;
        try {
            parentPid = Long.parseLong(pidStr.trim());
        } catch (NumberFormatException e) {
            logger.warn("忽略非法的 {}={}", PID_KEY, pidStr);
            return;
        }
        final long parentStartMs = parseLong(System.getProperty(START_KEY, ""), 0L);

        if (!parentAlive(parentPid, parentStartMs)) {
            logger.error("父进程 pid={} 在本进程启动时已不存在，直接退出", parentPid);
            System.exit(EXIT_CODE_PARENT_GONE);
        }

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(CHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (!parentAlive(parentPid, parentStartMs)) {
                    logger.error("父进程（agent, pid={}）已消失，本子进程退出以免成为孤儿继续写目标库", parentPid);
                    hardExitAfter(GRACEFUL_EXIT_TIMEOUT_MS);
                    System.exit(EXIT_CODE_PARENT_GONE);
                    return;
                }
            }
        }, "parent-watchdog");
        t.setDaemon(true);
        t.start();
        logger.info("父进程看门狗已启动，监视 agent pid={}", parentPid);
    }

    /**
     * PID 存在且启动时刻吻合才算活着。只比 PID 会在 PID 复用时把"父其实已死"误判成活着，
     * 反过来更危险：那正是孤儿继续跑的情形。
     */
    private static boolean parentAlive(long pid, long expectedStartMs) {
        java.util.Optional<ProcessHandle> h = ProcessHandle.of(pid);
        if (!h.isPresent() || !h.get().isAlive()) {
            return false;
        }
        if (expectedStartMs <= 0) {
            return true;
        }
        long actual = h.get().info().startInstant().map(java.time.Instant::toEpochMilli).orElse(0L);
        if (actual <= 0) {
            return true; // 平台拿不到启动时刻，退化成只比 PID
        }
        return Math.abs(actual - expectedStartMs) <= 2000;
    }

    /** shutdown hook 卡死时的兜底：到点直接 halt。 */
    private static void hardExitAfter(long timeoutMs) {
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            Runtime.getRuntime().halt(EXIT_CODE_PARENT_GONE);
        }, "parent-watchdog-hard-exit");
        t.setDaemon(true);
        t.start();
    }

    private static long parseLong(String s, long dft) {
        try {
            return s == null || s.trim().isEmpty() ? dft : Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return dft;
        }
    }
}
