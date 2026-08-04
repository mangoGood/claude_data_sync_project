package com.migration.elastic;

import com.migration.common.bulk.BulkLoadChannel;
import com.migration.common.bulk.BulkLoadOptions;
import com.migration.common.bulk.BulkLoadStats;

import java.util.ArrayList;
import java.util.List;

/**
 * Elasticsearch 全量的 _bulk 装载通道。
 *
 * <p>补掉原先"固定 1000 条一批"的两个短板：
 * <ul>
 *   <li><b>字节阈值</b>：ES 官方建议单个 bulk 请求控制在 5–15MB。只按条数攒批时，宽表
 *       （长文本/JSON 列）一批就能到几百 MB，要么触发 {@code http.max_content_length} 直接 413，
 *       要么把协调节点的堆顶穿。</li>
 *   <li><b>背压重试</b>：见 {@link EsClient#bulkWithRetry}。</li>
 * </ul>
 */
final class EsBulkChannel implements BulkLoadChannel<String[]> {

    /** 单批条数默认值（沿用原实现的 1000，行为不变）。 */
    static final int DEFAULT_BATCH_ROWS = 1000;
    /** 单批字节默认值：落在 ES 建议的 5–15MB 区间中段。 */
    static final long DEFAULT_BATCH_BYTES = 8L * 1024 * 1024;
    /** 429/503 的重试次数：指数退避到 5s 上限，6 次约覆盖 30s 的瞬时拥塞。 */
    private static final int MAX_RETRIES = 6;

    private final EsClient es;
    private final int batchRows;
    private final long batchBytes;
    private final BulkLoadStats stats = new BulkLoadStats();
    private final List<String[]> buffered = new ArrayList<>();
    private long bufferedBytes;

    EsBulkChannel(EsClient es, BulkLoadOptions options) {
        this.es = es;
        this.batchRows = options.rows(DEFAULT_BATCH_ROWS);
        this.batchBytes = options.bytes(DEFAULT_BATCH_BYTES);
    }

    @Override
    public void add(String[] op) {
        buffered.add(op);
        bufferedBytes += op[0].length() + (op[1] != null ? op[1].length() : 0) + 2L;
    }

    @Override
    public boolean isFull() {
        return buffered.size() >= batchRows || bufferedBytes >= batchBytes;
    }

    @Override
    public boolean isEmpty() {
        return buffered.isEmpty();
    }

    BulkLoadStats stats() {
        return stats;
    }

    @Override
    public long[] flush() throws Exception {
        if (buffered.isEmpty()) {
            return new long[]{0, 0};
        }
        long flushedBytes = bufferedBytes;
        try {
            long[] r = es.bulkWithRetry(buffered, MAX_RETRIES);
            stats.recordBatch(r[0], r[1], flushedBytes);
            return r;
        } finally {
            buffered.clear();
            bufferedBytes = 0;
        }
    }

    @Override
    public void close() {
        buffered.clear();
        bufferedBytes = 0;
    }
}
