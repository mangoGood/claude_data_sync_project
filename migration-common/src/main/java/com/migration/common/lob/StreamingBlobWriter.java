package com.migration.common.lob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 把一个大字段流式写进目标行，客户端内存与字段大小无关。
 *
 * <p>两条路径，按"单条语句装不装得下"自动选：
 *
 * <p><b>A · 单次流式</b>（{@code len + 余量 ≤ max_allowed_packet}）：一条
 * {@code UPDATE t SET col = ?}，参数用 {@code setBinaryStream}。开了
 * {@code useServerPrepStmts=true} 时驱动走 COM_STMT_SEND_LONG_DATA，
 * 按 {@code blobSendChunkSize}（默认 1MB）把参数分片推给服务端，客户端只需一个小缓冲。
 *
 * <p><b>B · 分块追加</b>（一条语句放不下时）：{@code UPDATE t SET col = CONCAT(col, ?)} 逐块追加。
 * 1GB 的值加上其余列会超出单条语句的预算，所以题设场景走的是这条路。
 * 每块仍是流式发送，块大小不占堆，因此可以开到 256MB——1GB 只要 4 次追加。
 *
 * <p><b>B 路径不能突破单值上限</b>（实测踩过）：{@code max_allowed_packet} 同时限制
 * {@code CONCAT()} 的<b>结果</b>大小，把块切小一点毫无用处。它买到的只是
 * {@code (packet − 余量, packet]} 这个窄带——而 1GB 的字段配 1GB 的
 * {@code max_allowed_packet} 恰好落在这个窄带里。真正的硬上限是
 * {@code max_allowed_packet} 本身（MySQL 最大 1GB），预检必须据此拦截。
 *
 * <p><b>幂等与续传</b>：写之前先读目标端 {@code OCTET_LENGTH}，
 * 已经等长就直接跳过，写了一半就从那个偏移接着写。这一点对 1GB 的值是刚需——
 * 崩在 700MB 处如果只能从 0 重来，重试本身就会拖垮整个任务。
 */
public final class StreamingBlobWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(StreamingBlobWriter.class);

    /** 目标端已存在的值长度：-1 = 该列为 NULL，-2 = 行不存在。 */
    public static final long TARGET_NULL = -1;
    public static final long TARGET_ROW_MISSING = -2;

    public enum Path {
        /** 一条语句写完。 */
        SINGLE,
        /** 分块追加。 */
        APPEND
    }

    /** 每写完一块回调一次，供上层落块级断点。 */
    public interface BlockProgress {
        void onBlock(long writtenBytes, long totalBytes) throws SQLException;
    }

    public static final class Result {
        public final Path path;
        public final long bytesWritten;
        public final int statements;
        public final boolean skipped;

        Result(Path path, long bytesWritten, int statements, boolean skipped) {
            this.path = path;
            this.bytesWritten = bytesWritten;
            this.statements = statements;
            this.skipped = skipped;
        }

        @Override
        public String toString() {
            return "Result{path=" + path + ", bytes=" + bytesWritten + ", stmts=" + statements
                    + ", skipped=" + skipped + "}";
        }
    }

    private final Connection conn;
    private final LobSqlDialect dialect;
    private final LobWriteOptions options;
    private final String qualifiedTable;
    private final String column;
    private final String pkColumn;
    private final Object pkValue;
    private final long packetLimitBytes;
    private final String desc;

    private PreparedStatement setStmt;
    private PreparedStatement appendStmt;
    private BlockProgress progress;

    public StreamingBlobWriter(Connection conn, LobSqlDialect dialect, LobWriteOptions options,
                               String qualifiedTable, String column, String pkColumn, Object pkValue,
                               long packetLimitBytes) {
        this.conn = conn;
        this.dialect = dialect;
        this.options = options;
        this.qualifiedTable = qualifiedTable;
        this.column = column;
        this.pkColumn = pkColumn;
        this.pkValue = pkValue;
        this.packetLimitBytes = packetLimitBytes;
        this.desc = qualifiedTable + "." + column + "#" + pkColumn + "=" + pkValue;
    }

    public StreamingBlobWriter onBlock(BlockProgress progress) {
        this.progress = progress;
        return this;
    }

    /** 读目标端一条语句能带多少字节。读不到就按 MySQL 的保守默认 64MB 处理。 */
    public static long probePacketLimit(Connection conn, LobSqlDialect dialect) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(dialect.packetLimitSql())) {
            if (rs.next()) {
                long v = rs.getLong(1);
                if (v > 0) {
                    return v;
                }
            }
        } catch (SQLException e) {
            logger.warn("读取目标端 max_allowed_packet 失败，按 64MB 保守处理: {}", e.getMessage());
        }
        return 64L * 1024 * 1024;
    }

    /**
     * 选路：一条语句放得下就走单次流式，否则分块追加。
     *
     * <p>余量是必须的——语句里除了这个大字段还有主键、SQL 文本和协议头，
     * 卡着上限选 SINGLE 会在服务端报 packet too large，而那个错误跟"批多大"无关、极难归因。
     */
    public static Path choosePath(long lobBytes, long packetLimitBytes, long safetyMarginBytes) {
        long budget = packetLimitBytes - safetyMarginBytes;
        return (budget > 0 && lobBytes <= budget) ? Path.SINGLE : Path.APPEND;
    }

    /** 目标端当前值的字节长度（{@link #TARGET_NULL} / {@link #TARGET_ROW_MISSING}）。 */
    public long probeTargetLength() throws SQLException {
        return JdbcLobChunkSource.byteLength(conn, dialect, qualifiedTable, column, pkColumn, pkValue);
    }

    public Result write(LobChunkSource source) throws SQLException, IOException {
        return write(source, probeTargetLength());
    }

    /**
     * 已知目标端当前长度时的写入（省掉一次探测）。
     *
     * <p>这个重载不是微优化：目标端 {@code OCTET_LENGTH} 在大值上会让服务端把整个 LOB 读一遍，
     * 1GB 的行探一次就是一次完整的磁盘读。而刚刚 INSERT 出来的新行长度必然是 NULL，
     * 调用方（全量瘦行插入）能直接给出这个事实，没必要再问一次数据库。
     *
     * @param already 目标端当前字节数；{@link #TARGET_NULL} 表示该列为 NULL
     */
    public Result write(LobChunkSource source, long already) throws SQLException, IOException {
        long total = source.length();
        if (already == TARGET_ROW_MISSING) {
            throw new SQLException("目标行不存在，无法写入大字段: " + desc
                    + "（大字段搬运必须在瘦行 INSERT 之后）");
        }
        if (already == total && total > 0) {
            logger.debug("目标端已是完整长度，跳过大字段搬运: {} ({} bytes)", desc, total);
            return new Result(Path.SINGLE, 0, 0, true);
        }

        // 单值不可能超过 max_allowed_packet —— 分块追加也救不了：
        // 该参数同时限制 CONCAT() 的<b>结果</b>大小，不只是语句/参数大小，
        // 所以"块切小一点"完全无效。撞上时 MySQL 只会甩一句
        // "Result of concat() was larger than max_allowed_packet - truncated"，
        // 既没说是哪张表哪一列，也没说该调到多大。这里换成可以直接照做的报错。
        if (total > packetLimitBytes) {
            throw new SQLException("大字段 " + desc + " 长度 " + total
                    + " 字节，超过目标端 max_allowed_packet=" + packetLimitBytes
                    + "。该参数同时限制单值上限与 CONCAT 结果上限，分块追加无法绕过。"
                    + "请把目标端 max_allowed_packet 调到 >= " + total
                    + "（MySQL 上限 1073741824，即 1GB；超过 1GB 的单值无法通过 SQL 协议写入）");
        }

        if (total == 0) {
            // 空值：一条语句写个空串，不必走流式
            try (PreparedStatement ps = conn.prepareStatement(
                    dialect.setSql(qualifiedTable, column, pkColumn))) {
                ps.setBytes(1, new byte[0]);
                ps.setObject(2, pkValue);
                ps.executeUpdate();
            }
            commitIfManual();
            return new Result(Path.SINGLE, 0, 1, false);
        }

        Path path = choosePath(total, packetLimitBytes, options.packetSafetyMarginBytes());
        if (path == Path.SINGLE) {
            return writeSingle(source, total);
        }
        return writeAppend(source, total, already);
    }

    private Result writeSingle(LobChunkSource source, long total) throws SQLException, IOException {
        // SINGLE 是一条语句整值覆盖，目标端原先是 NULL 还是半截值都无所谓，天然幂等
        try (SourceChunkedBlobInputStream in =
                     new SourceChunkedBlobInputStream(source, options.sourceChunkBytes())) {
            PreparedStatement ps = setStatement();
            ps.setBinaryStream(1, in, total);
            ps.setObject(2, pkValue);
            ps.executeUpdate();
        }
        commitIfManual();
        verifyLength(total);
        if (progress != null) {
            progress.onBlock(total, total);
        }
        return new Result(Path.SINGLE, total, 1, false);
    }

    private Result writeAppend(LobChunkSource source, long total, long already)
            throws SQLException, IOException {
        long offset = 0;
        boolean overwriteFirstBlock = true;
        if (already > total) {
            // 目标比源还长：上一轮写坏了或源被改小。追加语义没法"缩回去"，只能从头重写。
            logger.warn("目标端大字段长度 {} 大于源端 {}，整值重写: {}", already, total, desc);
        } else if (already > 0) {
            offset = already;
            overwriteFirstBlock = false;   // 续写：第一块也得用 CONCAT，不能覆盖已写好的部分
        }

        int statements = 0;
        try (SourceChunkedBlobInputStream in =
                     new SourceChunkedBlobInputStream(source, options.sourceChunkBytes())) {
            if (offset > 0) {
                // 只推进偏移量，不会真的去源端拉这些块
                long skipped = in.skip(offset);
                if (skipped != offset) {
                    throw new IOException("续传定位失败: " + desc + " 期望跳过 " + offset
                            + " 实际 " + skipped);
                }
                logger.info("大字段续传: {} 从 {}/{} 字节继续", desc, offset, total);
            }
            while (offset < total) {
                long block = Math.min(options.appendBlockBytes(), total - offset);
                boolean useSet = overwriteFirstBlock && offset == 0;
                PreparedStatement ps = useSet ? setStatement() : appendStatement();
                BoundedInputStream bounded = new BoundedInputStream(in, block);
                ps.setBinaryStream(1, bounded, block);
                ps.setObject(2, pkValue);
                ps.executeUpdate();
                if (bounded.remaining() != 0) {
                    // 驱动没把声明的字节读完 → 下一块的起点就对不上，必须当场炸掉。
                    // 让它跑下去只会写出一个长度对不上、内容错位的值。
                    throw new IOException("驱动未读满声明的块长度: " + desc + " 剩余 "
                            + bounded.remaining() + "/" + block);
                }
                offset += block;
                statements++;
                // 每块一个自治事务：1GB 分 4 块，undo/redo 不会累积成一个巨型事务
                commitIfManual();
                if (progress != null) {
                    progress.onBlock(offset, total);
                }
            }
        }
        verifyLength(total);
        return new Result(Path.APPEND, total - Math.max(0, Math.min(already, total)), statements, false);
    }

    private void verifyLength(long expected) throws SQLException {
        long actual = probeTargetLength();
        if (actual != expected) {
            throw new SQLException("大字段写入后长度不符: " + desc + " 期望 " + expected + " 实际 " + actual);
        }
    }

    private void commitIfManual() throws SQLException {
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
    }

    private PreparedStatement setStatement() throws SQLException {
        if (setStmt == null) {
            setStmt = conn.prepareStatement(dialect.setSql(qualifiedTable, column, pkColumn));
        }
        return setStmt;
    }

    private PreparedStatement appendStatement() throws SQLException {
        if (appendStmt == null) {
            appendStmt = conn.prepareStatement(dialect.appendSql(qualifiedTable, column, pkColumn));
        }
        return appendStmt;
    }

    @Override
    public void close() {
        closeQuietly(setStmt);
        closeQuietly(appendStmt);
        setStmt = null;
        appendStmt = null;
    }

    private static void closeQuietly(PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException ignored) {
                // 关闭失败不该盖掉真正的业务异常
            }
        }
    }
}
