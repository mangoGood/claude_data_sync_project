package com.migration.common.bulk;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量装载通道的运行计数，各引擎共用一套口径，便于横向比较吞吐与失败率。
 *
 * <p>{@code batchFailures} 与 {@code replayedRows} 是排障的关键：批级失败率高说明目标端在批上
 * 持续报错（主键冲突/超包大小/超时），此时吞吐会悄悄退化成逐条写——只看行速率是看不出来的。
 */
public final class BulkLoadStats {
    private final AtomicLong rows = new AtomicLong();
    private final AtomicLong failedRows = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong batchFailures = new AtomicLong();
    private final AtomicLong replayedRows = new AtomicLong();
    private final long startedAt = System.currentTimeMillis();

    public void recordBatch(long ok, long fail, long batchBytes) {
        rows.addAndGet(ok);
        failedRows.addAndGet(fail);
        bytes.addAndGet(Math.max(0L, batchBytes));
        batches.incrementAndGet();
    }

    /** 批级失败并降级为逐条重放时调用（replayed = 本批重放的条数）。 */
    public void recordBatchFailure(long replayed) {
        batchFailures.incrementAndGet();
        replayedRows.addAndGet(Math.max(0L, replayed));
    }

    public long getRows() {
        return rows.get();
    }

    public long getFailedRows() {
        return failedRows.get();
    }

    public long getBytes() {
        return bytes.get();
    }

    public long getBatches() {
        return batches.get();
    }

    public long getBatchFailures() {
        return batchFailures.get();
    }

    public long getReplayedRows() {
        return replayedRows.get();
    }

    /** 行/秒（含攒批与网络往返，即端到端装载速率）。 */
    public double rowsPerSecond() {
        long elapsed = Math.max(1L, System.currentTimeMillis() - startedAt);
        return rows.get() * 1000.0 / elapsed;
    }

    /** 日志用的一行摘要。 */
    public String summary() {
        return String.format("装载 %d 行（失败 %d），%d 批（批失败 %d，重放 %d 行），%.0f 行/秒",
                rows.get(), failedRows.get(), batches.get(), batchFailures.get(),
                replayedRows.get(), rowsPerSecond());
    }
}
