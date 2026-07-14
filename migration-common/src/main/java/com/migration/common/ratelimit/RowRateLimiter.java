package com.migration.common.ratelimit;

/**
 * 简单定长窗口限速器：把"每秒处理行数"钳制在配额以内，避免增量应用过快反压到
 * capture/binlog 读取（进而打挂源库），或全量搬数过快压垮源库连接。
 *
 * <p>算法：以 1 秒为窗口，累计窗口内已计入的行数；一旦超过配额，阻塞到窗口结束
 * （而非平滑令牌桶）——实现简单、行为可预测，足以满足"别把源库打挂"这一诉求，
 * 不追求瞬时速率完全平滑。
 *
 * <p>{@code maxRowsPerSecond <= 0} 表示不限速（默认行为，向后兼容）。线程安全：
 * {@link #acquire(long)} 仅在锁内更新窗口计数、锁外阻塞等待，允许多线程共享同一限速器
 * （如全量并行迁移的多个 worker）而不会因一个线程等待而阻塞其余线程拿锁。
 */
public class RowRateLimiter {

    private final long maxRowsPerSecond;
    private long windowStartMs;
    private long rowsInWindow;

    public RowRateLimiter(long maxRowsPerSecond) {
        this.maxRowsPerSecond = maxRowsPerSecond;
        this.windowStartMs = System.currentTimeMillis();
    }

    public boolean isUnlimited() {
        return maxRowsPerSecond <= 0;
    }

    /**
     * 记入 {@code rows} 行；若本秒窗口内累计已超过配额，阻塞到窗口结束再放行。
     * 单次超大批量（rows 本身就 > 配额）不会死等一整秒以上——按"这一窗口用完就放行"处理，
     * 下一窗口从零重新计数，避免大批量事件被无限拖长。
     */
    public void acquire(long rows) throws InterruptedException {
        if (maxRowsPerSecond <= 0 || rows <= 0) {
            return;
        }
        long sleepMs;
        // 只在锁内更新窗口状态并算出应睡时长；实际 Thread.sleep 放到锁外，
        // 否则一个线程睡整秒会持锁阻塞共享同一限速器的其他 worker（全量并行/多表增量），
        // 使全局吞吐被压到远低于配额。
        synchronized (this) {
            long now = System.currentTimeMillis();
            if (now - windowStartMs >= 1000) {
                windowStartMs = now;
                rowsInWindow = 0;
            }
            rowsInWindow += rows;
            if (rowsInWindow > maxRowsPerSecond) {
                sleepMs = 1000 - (now - windowStartMs);
                // 预置下一窗口起点为本线程的预计醒来时刻，睡醒后从零计数；
                // 期间其他线程进来会计入这个"下一窗口"，行为与原语义一致。
                windowStartMs = now + Math.max(sleepMs, 0);
                rowsInWindow = 0;
            } else {
                sleepMs = 0;
            }
        }
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
    }
}
