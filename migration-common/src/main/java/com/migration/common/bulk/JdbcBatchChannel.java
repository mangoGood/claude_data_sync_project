package com.migration.common.bulk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 全量写侧的批量装载通道（语句重写档 {@code BATCH} 与 Oracle 直接路径档 {@code DIRECT_PATH}）。
 *
 * <p>装载走的仍然是 {@code PreparedStatement.addBatch/executeBatch}，提速来自驱动层的
 * <b>批量语句重写</b>：MySQL {@code rewriteBatchedStatements=true} 与 PostgreSQL
 * {@code reWriteBatchedInserts=true} 会把 N 条单行 INSERT 合并成一条多值 INSERT，
 * 往返次数从 N 降到 1。Oracle 的 {@code executeBatch} 本身就是数组绑定，额外的提速来自
 * {@code /*+ APPEND_VALUES *&#47;} 直接路径（见 {@link JdbcBulkChannels}）。
 *
 * <p><b>为什么 MySQL 不用 LOAD DATA LOCAL INFILE</b>：那是<b>文本</b>协议，值要先渲染成字符串再由
 * 服务端解析，等于绕开 PreparedStatement 的类型绑定——本项目增量链路正是因为文本管道踩过 5 类
 * 值保真缺陷（时间精度、二进制、布尔、NULL、枚举），后来统一收敛到类型化绑定。何况 LOAD DATA
 * 还需要服务端 {@code local_infile=ON} 这种部署侧开关。PostgreSQL 那边之所以能再上一档，是因为
 * {@code COPY} 有<b>二进制</b>格式（见 {@link JdbcCopyChannel}），类型绑定不丢。
 *
 * <p>这个类同时补掉了原先直写 PreparedStatement 的两个坑：
 * <ul>
 *   <li><b>批失败即整批丢</b>：语句被重写后，一行主键冲突会让<b>整条多值 INSERT</b> 失败。
 *       原逻辑只是 warn 一句就继续下一页，等于静默丢掉一整批。这里失败后按行重放，
 *       只跳过真正冲突的那几行。</li>
 *   <li><b>目标连接重建即丢缓冲</b>：原逻辑重连后只是重新 prepare，已 addBatch 的行随旧
 *       statement 一起消失，计数却照常推进。这里保留行缓冲，重连后重放。</li>
 * </ul>
 */
public class JdbcBatchChannel implements JdbcBulkChannel {
    private static final Logger logger = LoggerFactory.getLogger(JdbcBatchChannel.class);

    /**
     * 单批字节上限的默认值。只按行数攒批时，宽行（LOB/长文本）会把重写后的多值 INSERT 顶过
     * {@code max_allowed_packet}，报错还与"批多大"无关。8MB 远低于 MySQL 默认的 64MB 服务端上限，
     * 留足了驱动侧的封装开销。
     */
    public static final long DEFAULT_BATCH_BYTES = 8L * 1024 * 1024;

    private final String batchSql;
    /** 逐行重放用的 SQL。Oracle 直接路径档下与 {@link #batchSql} 不同：重放必须去掉 APPEND_VALUES 提示。 */
    private final String replaySql;
    private final String tableName;
    private final int batchRows;
    private final long batchBytes;
    private final BulkLoadOptions.Mode mode;
    private final BulkLoadStats stats = new BulkLoadStats();
    /** 已 addBatch、尚未成功提交的行；用于批失败按行重放与连接重建后重放。 */
    private final List<Object[]> buffered = new ArrayList<>();
    private long bufferedBytes;

    private Connection conn;
    private PreparedStatement stmt;
    private PreparedStatement singleRowStmt;

    public JdbcBatchChannel(Connection conn, String insertSql, String tableName, int batchRows) throws SQLException {
        this(conn, insertSql, insertSql, tableName, batchRows, DEFAULT_BATCH_BYTES, BulkLoadOptions.Mode.BATCH);
    }

    JdbcBatchChannel(Connection conn, String batchSql, String replaySql, String tableName,
                     int batchRows, long batchBytes, BulkLoadOptions.Mode mode) throws SQLException {
        this.conn = conn;
        this.batchSql = batchSql;
        this.replaySql = replaySql;
        this.tableName = tableName;
        this.batchRows = Math.max(1, batchRows);
        this.batchBytes = Math.max(1L, batchBytes);
        this.mode = mode;
        this.stmt = conn.prepareStatement(batchSql);
    }

    @Override
    public void add(Object[] row) throws SQLException {
        bind(stmt, row);
        stmt.addBatch();
        buffered.add(row);
        bufferedBytes += estimateRowBytes(row);
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
        return mode;
    }

    @Override
    public BulkLoadStats stats() {
        return stats;
    }

    /**
     * 目标连接已断开时重建写通道：在新连接上重新 prepare，并把尚未落库的缓冲行重新 addBatch。
     * 不重放的话这些行会静默消失（原实现的行为）。
     */
    @Override
    public void rebind(Connection newConn) throws SQLException {
        closeQuietly(stmt);
        closeQuietly(singleRowStmt);
        singleRowStmt = null;
        this.conn = newConn;
        this.stmt = newConn.prepareStatement(batchSql);
        for (Object[] row : buffered) {
            bind(stmt, row);
            stmt.addBatch();
        }
    }

    /**
     * 提交缓冲的整批。返回 {成功行数, 失败行数}。
     * 批失败（重写后一行冲突即整批失败）时降级为按行重放，只跳过真正冲突的行。
     */
    @Override
    public long[] flush() throws SQLException {
        if (buffered.isEmpty()) {
            return new long[]{0, 0};
        }
        long flushedBytes = bufferedBytes;
        try {
            int[] results = stmt.executeBatch();
            long[] counted = countBatchResults(results, buffered.size());
            stats.recordBatch(counted[0], counted[1], flushedBytes);
            clearBuffer();
            return counted;
        } catch (SQLException e) {
            logger.warn("表 {} 批量写入失败（{} 行），降级为逐行重放: {}", tableName, buffered.size(), e.getMessage());
            stats.recordBatchFailure(buffered.size());
            try {
                stmt.clearBatch();
            } catch (SQLException ignore) {
                // 部分驱动在批失败后 clearBatch 也会抛，忽略即可——下面按行重放不依赖它
            }
            long[] replayed = replayRowByRow();
            stats.recordBatch(replayed[0], replayed[1], flushedBytes);
            clearBuffer();
            return replayed;
        }
    }

    private void clearBuffer() {
        buffered.clear();
        bufferedBytes = 0;
    }

    /** 按行重放缓冲：主键冲突跳过（不计失败），其余异常计失败。 */
    private long[] replayRowByRow() throws SQLException {
        long success = 0;
        long fail = 0;
        if (singleRowStmt == null || singleRowStmt.isClosed()) {
            singleRowStmt = conn.prepareStatement(replaySql);
        }
        for (Object[] row : buffered) {
            try {
                bind(singleRowStmt, row);
                singleRowStmt.executeUpdate();
                success++;
            } catch (SQLException e) {
                if (isDuplicateKeyError(e)) {
                    // 计成功而不是跳过：批失败前驱动通常已经把不冲突的行写进去了，重放时它们必然冲突。
                    // 若按"跳过"计，一批里只要有一行冲突，整批就都不计数——进度条会平白少掉一整批。
                    // 冲突意味着这一行在目标端已经存在，对全量的语义（行已就位）就是成功。
                    success++;
                    logger.debug("表 {} 逐行重放遇主键冲突（目标端已有该行）", tableName);
                } else {
                    fail++;
                    logger.error("表 {} 逐行重放写入失败: {}", tableName, e.getMessage());
                }
            }
        }
        return new long[]{success, fail};
    }

    /**
     * 统计批结果码。<b>{@link Statement#SUCCESS_NO_INFO}（-2）必须算成功</b>：
     * 批量语句一旦被驱动重写成多值 INSERT，返回的就是 SUCCESS_NO_INFO 而非逐行影响数。
     * 若沿用"负数即失败"的老口径，开启重写后每一次全量都会把<b>全部行报成失败</b>。
     * 只有 {@link Statement#EXECUTE_FAILED}（-3）才是真失败。
     */
    public static long[] countBatchResults(int[] results, int submitted) {
        long success = 0;
        long fail = 0;
        for (int r : results) {
            if (r == Statement.EXECUTE_FAILED) {
                fail++;
            } else {
                success++;
            }
        }
        // 驱动重写后可能只返回一个汇总结果码，按提交行数补齐，避免进度统计塌成 1
        if (results.length < submitted && fail == 0) {
            success = submitted;
        }
        return new long[]{success, fail};
    }

    private void bind(PreparedStatement ps, Object[] row) throws SQLException {
        for (int i = 0; i < row.length; i++) {
            ps.setObject(i + 1, row[i]);
        }
    }

    /**
     * 估算一行在网络上的字节数。只求量级正确（用于字节阈值攒批），不追求精确：
     * 字符串按 UTF-8 最坏的 3 字节/字符估，宁可批小一点也不要撞包大小上限。
     */
    static long estimateRowBytes(Object[] row) {
        long size = 0;
        for (Object v : row) {
            if (v == null) {
                size += 1;
            } else if (v instanceof byte[]) {
                size += ((byte[]) v).length;
            } else if (v instanceof CharSequence) {
                size += ((CharSequence) v).length() * 3L;
            } else if (v instanceof Number || v instanceof Boolean) {
                size += 8;
            } else {
                size += 24;
            }
        }
        return size;
    }

    public static boolean isDuplicateKeyError(SQLException e) {
        int errorCode = e.getErrorCode();
        String sqlState = e.getSQLState();
        if (errorCode == 1062 || "23000".equals(sqlState) || "23505".equals(sqlState)) {
            return true;
        }
        String message = e.getMessage();
        return message != null && (message.contains("Duplicate entry")
                || message.contains("duplicate key value")
                || message.contains("PRIMARY") || message.contains("UNIQUE"));
    }

    @Override
    public void close() {
        closeQuietly(stmt);
        closeQuietly(singleRowStmt);
    }

    private static void closeQuietly(PreparedStatement ps) {
        if (ps != null) {
            try { ps.close(); } catch (SQLException ignore) { }
        }
    }
}
