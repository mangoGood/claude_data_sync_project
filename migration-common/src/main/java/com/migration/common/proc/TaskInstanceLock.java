package com.migration.common.proc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;

/**
 * 任务级单实例互斥：{@code files/<taskId>/.<role>.lock} 上的 {@link FileChannel#tryLock()}。
 *
 * <p><b>解决什么</b>：agent 被 SIGKILL 时，它 fork 出来的 capture/extract/increment/subscribe
 * 子进程<b>不会</b>随之退出（{@code restart_agent.sh} 里那句 {@code pkill -f 'migration-…/target'}
 * 就是在人肉兜这个底）。agent 重新拉起后走恢复流程，为同一个 taskId 再起一套子进程——
 * 于是同一份 binlog 被两个 capture 同时拉、同一批 THL 被两个 increment 同时应用：
 * 目标库双写，非幂等语句（自增列、无主键表、{@code UPDATE ... SET x = x + 1}）直接算错。
 *
 * <p><b>为什么用文件锁而不是 PID 文件</b>：文件锁由内核在进程退出时<b>无条件</b>释放，
 * 无论进程是正常退出、崩溃还是被 SIGKILL——PID 文件做不到这一点，残留的 PID 文件反而会
 * 把正常启动挡在门外。锁文件内容（PID + 启动时刻）只作为诊断信息与
 * {@code OrphanChildReaper} 的收孤儿依据，互斥性完全由锁本身保证。
 *
 * <p>拿不到锁的进程<b>立即退出</b>（退出码 {@value #EXIT_CODE_LOCK_HELD}）并打印持锁 PID，
 * 而不是继续跑——这条路径上"少跑一个进程"永远优于"多跑一个进程"。
 */
public final class TaskInstanceLock implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TaskInstanceLock.class);

    /** 同一 taskId+role 已有实例在跑时的退出码。 */
    public static final int EXIT_CODE_LOCK_HELD = 9;

    /** 关掉互斥（仅用于单机多实例的压测场景，生产不要动）。 */
    public static final String ENABLED_KEY = "task.instance.lock.enabled";

    /** 持有引用防止 FileLock 被 GC 回收；同一 JVM 内一个 role 只会取一次。 */
    private static volatile TaskInstanceLock held;

    private final File lockFile;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final FileLock lock;

    private TaskInstanceLock(File lockFile, RandomAccessFile raf, FileChannel channel, FileLock lock) {
        this.lockFile = lockFile;
        this.raf = raf;
        this.channel = channel;
        this.lock = lock;
    }

    public static File lockFileFor(String taskId, String role) {
        return new File("files/" + taskId, "." + role + ".lock");
    }

    /**
     * 取锁，取不到就退出进程。所有子进程 main() 的第一件事都应该是这个。
     *
     * @param taskId 任务 ID
     * @param role   进程角色：capture / extract / increment / subscribe / full / mongo / redis / elastic
     */
    public static void acquireOrExit(String taskId, String role) {
        if (!Boolean.parseBoolean(System.getProperty(ENABLED_KEY, "true"))) {
            logger.warn("任务级单实例互斥已被 -D{}=false 关闭，role={} taskId={}", ENABLED_KEY, role, taskId);
            return;
        }
        if (taskId == null || taskId.isEmpty() || "unknown".equals(taskId)) {
            logger.warn("taskId 未知，跳过单实例互斥（role={}）", role);
            return;
        }
        TaskInstanceLock acquired = tryAcquire(taskId, role);
        if (acquired == null) {
            String holder = readHolder(lockFileFor(taskId, role));
            logger.error("同一任务的 {} 实例已在运行（{}），本进程退出以避免双写。"
                            + "若确认持锁进程已死，删除 {} 后重试。",
                    role, holder, lockFileFor(taskId, role).getAbsolutePath());
            System.exit(EXIT_CODE_LOCK_HELD);
        }
        held = acquired;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            TaskInstanceLock l = held;
            if (l != null) {
                l.close();
            }
        }, "task-instance-lock-release"));
        logger.info("已取得任务实例锁: {} (pid={})", acquired.lockFile.getPath(), ProcessHandle.current().pid());
    }

    /** 取锁；已被别的进程持有返回 null。 */
    public static TaskInstanceLock tryAcquire(String taskId, String role) {
        File f = lockFileFor(taskId, role);
        File dir = f.getParentFile();
        if (dir != null && !dir.exists()) {
            dir.mkdirs();
        }
        RandomAccessFile raf = null;
        FileChannel channel = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            channel = raf.getChannel();
            FileLock lock = channel.tryLock();
            if (lock == null) {
                closeQuietly(channel, raf);
                return null;
            }
            // 锁到手后写入诊断信息：PID + 进程启动时刻（收孤儿时用启动时刻排除 PID 复用的误杀）
            ProcessHandle self = ProcessHandle.current();
            long startMs = self.info().startInstant().map(java.time.Instant::toEpochMilli).orElse(0L);
            byte[] payload = (self.pid() + "|" + startMs + "|" + role + "|" + taskId + "\n")
                    .getBytes(StandardCharsets.UTF_8);
            channel.truncate(0);
            channel.write(java.nio.ByteBuffer.wrap(payload), 0);
            channel.force(true);
            return new TaskInstanceLock(f, raf, channel, lock);
        } catch (java.nio.channels.OverlappingFileLockException e) {
            // 同一 JVM 内重复取同一把锁——按"已持有"处理
            closeQuietly(channel, raf);
            return null;
        } catch (Exception e) {
            logger.warn("取任务实例锁 {} 失败，放行以免误伤正常启动: {}", f, e.getMessage());
            closeQuietly(channel, raf);
            // 拿不到锁文件本身（磁盘只读等）不该阻断任务，返回一个"空锁"表示放行
            return new TaskInstanceLock(f, null, null, null);
        }
    }

    /** 读锁文件里的持有者描述，仅用于日志。 */
    private static String readHolder(File f) {
        try {
            byte[] b = java.nio.file.Files.readAllBytes(f.toPath());
            String s = new String(b, StandardCharsets.UTF_8).trim();
            return s.isEmpty() ? "持锁 PID 未知" : "持锁 pid|startMs|role|taskId = " + s;
        } catch (Exception e) {
            return "持锁 PID 未知（" + e.getMessage() + "）";
        }
    }

    /** 解析锁文件，返回 {pid, startMs}；不可用返回 null。 */
    public static long[] readHolderPid(File f) {
        try {
            String s = new String(java.nio.file.Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8).trim();
            if (s.isEmpty()) return null;
            String[] parts = s.split("\\|");
            if (parts.length < 2) return null;
            return new long[]{Long.parseLong(parts[0].trim()), Long.parseLong(parts[1].trim())};
        } catch (Exception e) {
            return null;
        }
    }

    private static void closeQuietly(FileChannel channel, RandomAccessFile raf) {
        try { if (channel != null) channel.close(); } catch (Exception ignored) { }
        try { if (raf != null) raf.close(); } catch (Exception ignored) { }
    }

    @Override
    public void close() {
        try { if (lock != null && lock.isValid()) lock.release(); } catch (Exception ignored) { }
        closeQuietly(channel, raf);
        // 锁文件本身留着：内容是最后一任持有者的 PID，排障有用；下次启动会 truncate 重写。
    }
}
