package com.migration.common.watch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

/**
 * 目录变更监听器：用 {@link WatchService} 把管道各级从"定时轮询"改为"事件驱动握手"。
 *
 * <p>数据管道跨进程通过文件系统交接（capture 写 .cap → extract 读 .cap 写 .thl → increment 读 .thl），
 * 无法用进程内队列直连；{@code WatchService} 是同机跨进程的正确原语。
 *
 * <p>{@link #awaitChange(long)} 阻塞到目标目录出现 CREATE/MODIFY 事件即返回（Linux 走 inotify，
 * 唤醒延迟 ~ms），或到 {@code timeoutMs} 兜底超时返回。相比固定 {@code sleep(3000)} 轮询：
 * <ul>
 *   <li>RPO：有数据时立即唤醒，秒级 → 百毫秒级（Linux 亚毫秒）；</li>
 *   <li>CPU：空闲时 inotify 上真正阻塞、不做无谓的整目录 listFiles+排序，兜底超时仅作安全网。</li>
 * </ul>
 *
 * <p>兜底超时仍保留的原因：(1) 心跳/背压等周期性逻辑需要在空闲时也照常触发；
 * (2) macOS 的 JDK WatchService 是轮询实现（非 inotify），兜底超时为其提供延迟上界；
 * (3) 任何平台下事件丢失（OVERFLOW）时不至于永久阻塞。
 *
 * <p>注意：必须同时监听 ENTRY_CREATE 与 ENTRY_MODIFY —— extract/capture 会向"当前最新文件"
 * 追加写入（而非每批新建文件），消费端要能被追加写唤醒。
 */
public class DirectoryChangeWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DirectoryChangeWatcher.class);

    private final Path dir;
    private final WatchService watchService;
    private volatile boolean closed = false;

    public DirectoryChangeWatcher(String dirPath) throws IOException {
        this.dir = Paths.get(dirPath);
        Files.createDirectories(dir);
        this.watchService = FileSystems.getDefault().newWatchService();
        this.dir.register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY);
        logger.info("目录变更监听已注册: {}", dir);
    }

    /**
     * 阻塞直到监听目录出现变更事件，或到达兜底超时。
     *
     * @param timeoutMs 兜底超时（毫秒）；<=0 视为无限等待
     * @return true=观察到变更（应立即重新扫描）；false=兜底超时（按周期逻辑重新扫描）
     */
    public boolean awaitChange(long timeoutMs) throws InterruptedException {
        if (closed) {
            // 已关闭：退化为纯 sleep，避免调用方进入忙循环
            if (timeoutMs > 0) Thread.sleep(timeoutMs);
            return false;
        }
        WatchKey key;
        try {
            key = (timeoutMs > 0)
                    ? watchService.poll(timeoutMs, TimeUnit.MILLISECONDS)
                    : watchService.take();
        } catch (java.nio.file.ClosedWatchServiceException e) {
            if (timeoutMs > 0) Thread.sleep(timeoutMs);
            return false;
        }
        if (key == null) {
            return false; // 兜底超时
        }

        // 合并抖动：把已就绪的后续事件一次性排空，一次唤醒对应一次扫描，避免"每个文件一次扫描"
        boolean changed = false;
        do {
            for (var event : key.pollEvents()) {
                if (event.kind() != StandardWatchEventKinds.OVERFLOW) {
                    changed = true;
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                // 目录不可再监听（被删除等）：本次仍返回 changed，让调用方扫描一次
                logger.warn("监听目录已失效: {}", dir);
                break;
            }
            key = watchService.poll(); // 非阻塞排空已合并的连续事件
        } while (key != null);

        return changed;
    }

    @Override
    public void close() {
        closed = true;
        try {
            watchService.close();
        } catch (IOException e) {
            logger.debug("关闭 WatchService 出错: {}", e.getMessage());
        }
    }
}
