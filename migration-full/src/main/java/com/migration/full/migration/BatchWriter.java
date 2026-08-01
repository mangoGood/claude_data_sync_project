package com.migration.full.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 全量写侧的批量装载通道（P2-2）。
 *
 * <p>装载走的仍然是 {@code PreparedStatement.addBatch/executeBatch}，真正的提速来自
 * 驱动层的<b>批量语句重写</b>：MySQL {@code rewriteBatchedStatements=true} 与 PostgreSQL
 * {@code reWriteBatchedInserts=true} 会把 N 条单行 INSERT 合并成一条多值 INSERT，
 * 往返次数从 N 降到 1（Oracle 驱动的 executeBatch 本身就是数组绑定，无需额外开关）。
 *
 * <p><b>为什么不用 LOAD DATA LOCAL INFILE / COPY FROM STDIN</b>：那两条通道都是<b>文本</b>协议，
 * 值要先被渲染成字符串再由服务端解析，等于绕开 PreparedStatement 的类型绑定——本项目
 * 增量链路正是因为文本管道踩过 5 类值保真缺陷（时间精度、二进制、布尔、NULL、枚举），
 * 后来统一收敛到类型化绑定。为一档吞吐把全量重新退回文本管道不划算，何况 LOAD DATA 还需要
 * 服务端 {@code local_infile=ON} 这种部署侧开关。语句重写通道保留了类型绑定，零协议风险。
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
class BatchWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BatchWriter.class);

    private final String insertSql;
    private final String tableName;
    private final int batchRows;
    /** 已 addBatch、尚未成功提交的行；用于批失败按行重放与连接重建后重放。 */
    private final List<Object[]> buffered = new ArrayList<>();

    private Connection conn;
    private PreparedStatement stmt;
    private PreparedStatement singleRowStmt;

    BatchWriter(Connection conn, String insertSql, String tableName, int batchRows) throws SQLException {
        this.conn = conn;
        this.insertSql = insertSql;
        this.tableName = tableName;
        this.batchRows = Math.max(1, batchRows);
        this.stmt = conn.prepareStatement(insertSql);
    }

    void add(Object[] row) throws SQLException {
        bind(stmt, row);
        stmt.addBatch();
        buffered.add(row);
    }

    boolean isFull() {
        return buffered.size() >= batchRows;
    }

    boolean isEmpty() {
        return buffered.isEmpty();
    }

    /**
     * 目标连接已断开时重建写通道：在新连接上重新 prepare，并把尚未落库的缓冲行重新 addBatch。
     * 不重放的话这些行会静默消失（原实现的行为）。
     */
    void rebind(Connection newConn) throws SQLException {
        closeQuietly(stmt);
        closeQuietly(singleRowStmt);
        singleRowStmt = null;
        this.conn = newConn;
        this.stmt = newConn.prepareStatement(insertSql);
        for (Object[] row : buffered) {
            bind(stmt, row);
            stmt.addBatch();
        }
    }

    /**
     * 提交缓冲的整批。返回 {成功行数, 失败行数}。
     * 批失败（重写后一行冲突即整批失败）时降级为按行重放，只跳过真正冲突的行。
     */
    long[] flush() throws SQLException {
        if (buffered.isEmpty()) {
            return new long[]{0, 0};
        }
        try {
            int[] results = stmt.executeBatch();
            long[] counted = countBatchResults(results, buffered.size());
            buffered.clear();
            return counted;
        } catch (SQLException e) {
            logger.warn("表 {} 批量写入失败（{} 行），降级为逐行重放: {}", tableName, buffered.size(), e.getMessage());
            try {
                stmt.clearBatch();
            } catch (SQLException ignore) {
                // 部分驱动在批失败后 clearBatch 也会抛，忽略即可——下面按行重放不依赖它
            }
            long[] replayed = replayRowByRow();
            buffered.clear();
            return replayed;
        }
    }

    /** 按行重放缓冲：主键冲突跳过（不计失败），其余异常计失败。 */
    private long[] replayRowByRow() throws SQLException {
        long success = 0;
        long fail = 0;
        if (singleRowStmt == null || singleRowStmt.isClosed()) {
            singleRowStmt = conn.prepareStatement(insertSql);
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
    static long[] countBatchResults(int[] results, int submitted) {
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

    static boolean isDuplicateKeyError(SQLException e) {
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
