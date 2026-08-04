package com.migration.common.bulk;

import com.migration.common.bulk.PgBinaryCopyEncoder.PgType;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL 目标端的 {@code COPY ... FROM STDIN WITH (FORMAT binary)} 装载通道
 * （档位 {@link BulkLoadOptions.Mode#COPY}）。
 *
 * <p>相对语句重写（多值 INSERT）再进一档的原因：COPY 走的是独立的批量装载协议，服务端跳过
 * SQL 解析与计划、按元组直接入堆，是 PostgreSQL 侧公认最快的入库路径。选<b>二进制</b>格式而非
 * 文本格式，是为了不让值退回字符串渲染——理由见 {@link PgBinaryCopyEncoder}。
 *
 * <p><b>失败即回退，不即失败</b>：COPY 是全批事务性的，一行冲突整批不落地；编码器也可能遇到
 * 不认识的 Java 值类型。这两种情况都不让任务失败，而是把整批交给 {@link JdbcBatchChannel}
 * 用 INSERT 重放（它自己再按行降级）。因此 COPY 通道的收益是纯性能，正确性边界仍由 INSERT 路径兜底。
 * 断点续传/重跑场景下目标端已有行会让每批都冲突回退，此时吞吐等同 BATCH 档——这是预期行为。
 */
public class JdbcCopyChannel implements JdbcBulkChannel {
    private static final Logger logger = LoggerFactory.getLogger(JdbcCopyChannel.class);

    private final String copySql;
    private final String insertSql;
    private final String tableName;
    private final PgType[] types;
    private final int batchRows;
    private final long batchBytes;
    private final BulkLoadStats stats = new BulkLoadStats();
    private final List<Object[]> buffered = new ArrayList<>();
    private long bufferedBytes;

    private Connection conn;
    private JdbcBatchChannel fallback;

    JdbcCopyChannel(Connection conn, String copySql, String insertSql, String tableName,
                    PgType[] types, int batchRows, long batchBytes) {
        this.conn = conn;
        this.copySql = copySql;
        this.insertSql = insertSql;
        this.tableName = tableName;
        this.types = types;
        this.batchRows = Math.max(1, batchRows);
        this.batchBytes = Math.max(1L, batchBytes);
    }

    @Override
    public void add(Object[] row) {
        buffered.add(row);
        bufferedBytes += JdbcBatchChannel.estimateRowBytes(row);
    }

    @Override
    public boolean isFull() {
        return buffered.size() >= batchRows || bufferedBytes >= batchBytes;
    }

    @Override
    public boolean isEmpty() {
        return buffered.isEmpty();
    }

    @Override
    public BulkLoadOptions.Mode mode() {
        return BulkLoadOptions.Mode.COPY;
    }

    @Override
    public BulkLoadStats stats() {
        return stats;
    }

    @Override
    public void rebind(Connection newConn) throws SQLException {
        this.conn = newConn;
        if (fallback != null) {
            // 回退通道的缓冲由本类持有，重建时不需要它重放——直接换一个干净的
            fallback.close();
            fallback = null;
        }
    }

    @Override
    public long[] flush() throws SQLException {
        if (buffered.isEmpty()) {
            return new long[]{0, 0};
        }
        long flushedBytes = bufferedBytes;
        byte[] payload;
        try {
            payload = serialize();
        } catch (Exception e) {
            logger.warn("表 {} 二进制 COPY 编码失败（{} 行），整批回退到 INSERT: {}",
                    tableName, buffered.size(), e.getMessage());
            return replayViaInsert(flushedBytes);
        }
        try {
            PGConnection pg = conn.unwrap(PGConnection.class);
            long copied = pg.getCopyAPI().copyIn(copySql, new ByteArrayInputStream(payload));
            long ok = copied > 0 ? copied : buffered.size();
            stats.recordBatch(ok, 0, flushedBytes);
            clearBuffer();
            return new long[]{ok, 0};
        } catch (Exception e) {
            logger.warn("表 {} COPY 装载失败（{} 行），整批回退到 INSERT: {}",
                    tableName, buffered.size(), e.getMessage());
            return replayViaInsert(flushedBytes);
        }
    }

    /**
     * COPY 失败后的兜底：整批交给 INSERT 通道。它内部还会在批失败时按行重放，
     * 因此主键冲突这类"部分行已存在"的场景仍然只跳过冲突行。
     */
    private long[] replayViaInsert(long flushedBytes) throws SQLException {
        stats.recordBatchFailure(buffered.size());
        if (fallback == null || conn.isClosed()) {
            if (fallback != null) {
                fallback.close();
            }
            fallback = new JdbcBatchChannel(conn, insertSql, tableName, batchRows);
        }
        for (Object[] row : buffered) {
            fallback.add(row);
        }
        long[] r = fallback.flush();
        stats.recordBatch(r[0], r[1], flushedBytes);
        clearBuffer();
        return r;
    }

    private byte[] serialize() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(1024, (int) Math.min(bufferedBytes, Integer.MAX_VALUE)));
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.write(PgBinaryCopyEncoder.header());
            for (Object[] row : buffered) {
                PgBinaryCopyEncoder.encodeRow(dos, row, types);
            }
            dos.write(PgBinaryCopyEncoder.trailer());
        }
        return out.toByteArray();
    }

    private void clearBuffer() {
        buffered.clear();
        bufferedBytes = 0;
    }

    @Override
    public void close() {
        if (fallback != null) {
            fallback.close();
            fallback = null;
        }
    }
}
