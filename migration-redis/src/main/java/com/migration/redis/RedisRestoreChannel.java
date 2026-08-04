package com.migration.redis;

import com.migration.common.bulk.BulkLoadChannel;
import com.migration.common.bulk.BulkLoadOptions;
import com.migration.common.bulk.BulkLoadStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.RestoreParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis 全量（RDB → RESTORE）的批量装载通道。
 *
 * <p>此前全量是<b>逐键一个 RESTORE 往返</b>：每个键都要等一次 RTT，键多的库里时间几乎全花在
 * 网络等待上，与键的大小无关。pipeline 把一批 RESTORE 一次性写出、再一次性收结果，
 * 往返次数从 N 降到 N/批大小——这是 Redis 侧最直接的装载加速手段，也是各家迁移工具的标配。
 *
 * <p><b>失败按键计，不整批丢</b>：{@code syncAndReturnAll} 会把每条命令各自的结果（含错误）
 * 原样返回，因此批里个别键失败只计这几个键，其余照常落库。只有连接级异常才降级为逐键重放，
 * 让"是这个键的问题还是链路的问题"能分辨出来。
 *
 * <p>逻辑库切换由调用方保证：{@link RedisSyncMain} 在 db 变化时先 flush 再 SELECT，
 * 因为 pipeline 与 SELECT 共用同一条连接，缓冲里的键必须落在它自己那个库里。
 */
final class RedisRestoreChannel implements BulkLoadChannel<RedisRestoreChannel.Entry> {
    private static final Logger logger = LoggerFactory.getLogger(RedisRestoreChannel.class);

    /** 单批键数默认值：Redis 命令小、RTT 是瓶颈，512 已能把往返摊薄两个数量级。 */
    static final int DEFAULT_BATCH_KEYS = 512;
    /** 单批字节默认值：大 value（大 hash/zset 的 dump）下按字节收口，避免一次写出几百 MB。 */
    static final long DEFAULT_BATCH_BYTES = 8L * 1024 * 1024;

    /** 一个待装载的键：dump 字节 + TTL 语义。 */
    static final class Entry {
        final byte[] key;
        final byte[] dump;
        final long ttl;
        final boolean absTtl;

        Entry(byte[] key, byte[] dump, long ttl, boolean absTtl) {
            this.key = key;
            this.dump = dump;
            this.ttl = ttl;
            this.absTtl = absTtl;
        }

        long sizeBytes() {
            return (key != null ? key.length : 0) + (dump != null ? dump.length : 0) + 16L;
        }
    }

    private final Jedis jedis;
    private final int batchKeys;
    private final long batchBytes;
    private final BulkLoadStats stats = new BulkLoadStats();
    private final List<Entry> buffered = new ArrayList<>();
    private long bufferedBytes;

    RedisRestoreChannel(Jedis jedis, BulkLoadOptions options) {
        this.jedis = jedis;
        this.batchKeys = options.rows(DEFAULT_BATCH_KEYS);
        this.batchBytes = options.bytes(DEFAULT_BATCH_BYTES);
    }

    @Override
    public void add(Entry entry) {
        buffered.add(entry);
        bufferedBytes += entry.sizeBytes();
    }

    @Override
    public boolean isFull() {
        return buffered.size() >= batchKeys || bufferedBytes >= batchBytes;
    }

    @Override
    public boolean isEmpty() {
        return buffered.isEmpty();
    }

    BulkLoadStats stats() {
        return stats;
    }

    @Override
    public long[] flush() {
        if (buffered.isEmpty()) {
            return new long[]{0, 0};
        }
        long flushedBytes = bufferedBytes;
        long ok = 0;
        long fail = 0;
        try {
            Pipeline pipeline = jedis.pipelined();
            for (Entry e : buffered) {
                pipeline.restore(e.key, e.ttl, e.dump, params(e));
            }
            List<Object> results = pipeline.syncAndReturnAll();
            for (Object r : results) {
                if (r instanceof Exception) {
                    fail++;
                    logger.error("RESTORE 失败: {}", ((Exception) r).getMessage());
                } else {
                    ok++;
                }
            }
            // 结果数与提交数对不上（理论上不该发生）：按提交数补齐，别让进度塌陷
            if (results.size() < buffered.size() && fail == 0) {
                ok = buffered.size();
            }
        } catch (JedisConnectionException e) {
            logger.warn("pipeline 批量 RESTORE 遇连接异常（{} 个键），降级为逐键重放: {}",
                    buffered.size(), e.getMessage());
            stats.recordBatchFailure(buffered.size());
            long[] replayed = replayKeyByKey();
            ok = replayed[0];
            fail = replayed[1];
        }
        stats.recordBatch(ok, fail, flushedBytes);
        buffered.clear();
        bufferedBytes = 0;
        return new long[]{ok, fail};
    }

    /** 逐键重放。连接级异常直接上抛（链路已断，重放没有意义，交给进程守护重启重跑全量）。 */
    private long[] replayKeyByKey() {
        long ok = 0;
        long fail = 0;
        for (Entry e : buffered) {
            try {
                jedis.restore(e.key, e.ttl, e.dump, params(e));
                ok++;
            } catch (JedisConnectionException ce) {
                throw ce;
            } catch (RuntimeException re) {
                fail++;
                logger.error("逐键 RESTORE 失败: {}", re.getMessage());
            }
        }
        return new long[]{ok, fail};
    }

    private RestoreParams params(Entry e) {
        RestoreParams params = RestoreParams.restoreParams().replace();
        if (e.absTtl) {
            // ABSTTL：直接用绝对过期时间（毫秒），避免与源库的时钟差
            params.absTtl();
        }
        return params;
    }

    @Override
    public void close() {
        buffered.clear();
        bufferedBytes = 0;
    }
}
