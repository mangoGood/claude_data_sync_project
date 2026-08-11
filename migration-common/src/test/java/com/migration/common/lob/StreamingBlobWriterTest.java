package com.migration.common.lob;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 大字段流式写入器（两条写路径）。
 *
 * <p>用 H2 内存库跑：这里要验的是<b>写入器自己的编排</b>——选路、块切分、续传定位、幂等、
 * 长度校验——这些逻辑与具体数据库无关。真库上的行为（COM_STMT_SEND_LONG_DATA 是否
 * 真的分片推、1GB 是否真的不进堆）由集测覆盖，单测不连库。
 *
 * <p>H2 的二进制拼接是 {@code ||} 而不是 MySQL 的 {@code CONCAT}，所以这里注入一个
 * H2 方言；写入器本身不认识任何一种 SQL 文本，这也正是把 SQL 抽成方言接口的原因。
 */
@DisplayName("大字段流式写入器")
class StreamingBlobWriterTest {

    /** H2 方言：语义与 MySQL 版逐条对应，只是拼接与函数名不同。 */
    private static final class H2LobDialect implements LobSqlDialect {
        @Override
        public String substringSql(String t, String c, String pk, boolean textColumn) {
            return "SELECT SUBSTRING(" + c + ", CAST(? AS INT), CAST(? AS INT)) FROM " + t + " WHERE " + pk + " = ?";
        }

        @Override
        public String byteLengthSql(String t, String c, String pk) {
            return "SELECT COALESCE(OCTET_LENGTH(" + c + "), -1) FROM " + t + " WHERE " + pk + " = ?";
        }

        @Override
        public String setSql(String t, String c, String pk) {
            return "UPDATE " + t + " SET " + c + " = ? WHERE " + pk + " = ?";
        }

        @Override
        public String appendSql(String t, String c, String pk) {
            // CAST 是必须的：H2 对未定型的 ? 走字符拼接，会把二进制参数当字符串转换而报错
            return "UPDATE " + t + " SET " + c + " = COALESCE(" + c + ", X'') || CAST(? AS VARBINARY(20000000)) "
                    + "WHERE " + pk + " = ?";
        }

        @Override
        public String packetLimitSql() {
            return "SELECT 16777216";
        }
    }

    /** 内存里的假源，内容由种子决定，不依赖数据库。 */
    private static final class MemSource implements LobChunkSource {
        private final byte[] data;
        int fetches;

        MemSource(byte[] data) {
            this.data = data;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public byte[] fetch(long offset, int len) {
            fetches++;
            int n = (int) Math.min(len, data.length - offset);
            byte[] out = new byte[Math.max(0, n)];
            System.arraycopy(data, (int) offset, out, 0, out.length);
            return out;
        }

        @Override
        public String describe() {
            return "mem";
        }
    }

    private static final H2LobDialect DIALECT = new H2LobDialect();
    private static final String TABLE = "t_lob";

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:lob_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, payload VARBINARY(20000000))");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    private void insertThinRow(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + TABLE + " (id, payload) VALUES (?, NULL)")) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
    }

    private byte[] readBack(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT payload FROM " + TABLE + " WHERE id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBytes(1) : null;
            }
        }
    }

    private static byte[] payload(int length, int seed) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) ((i * 31 + seed) % 251);
        }
        return b;
    }

    private static String md5(byte[] b) {
        try {
            StringBuilder sb = new StringBuilder();
            for (byte x : MessageDigest.getInstance("MD5").digest(b)) {
                sb.append(String.format("%02x", x));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private StreamingBlobWriter writer(int id, LobWriteOptions opts, long packetLimit) {
        return new StreamingBlobWriter(conn, DIALECT, opts, TABLE, "payload", "id", id, packetLimit);
    }

    // ------------------------------------------------------------------ 选路

    @Test
    @DisplayName("选路：装得下走单次流式，装不下走分块追加")
    void pathSelection() {
        long packet = 1073741824L;          // MySQL 的绝对上限
        long margin = 1024 * 1024;
        assertEquals(StreamingBlobWriter.Path.SINGLE,
                StreamingBlobWriter.choosePath(512L * 1024 * 1024, packet, margin));
        // 题设的 1GB：正好等于 max_allowed_packet 上限，加上其余列必然放不下 ⇒ 只能追加。
        // 这就是"1GB 必须有 B 路径"的判定点。
        assertEquals(StreamingBlobWriter.Path.APPEND,
                StreamingBlobWriter.choosePath(1073741824L, packet, margin));
        // 卡在余量边界上也要判 APPEND，不能等服务端报 packet too large
        assertEquals(StreamingBlobWriter.Path.SINGLE,
                StreamingBlobWriter.choosePath(packet - margin, packet, margin));
        assertEquals(StreamingBlobWriter.Path.APPEND,
                StreamingBlobWriter.choosePath(packet - margin + 1, packet, margin));
    }

    // ------------------------------------------------------------------ 路径 A

    @Test
    @DisplayName("A 路径：单条语句流式写入，内容逐字节一致")
    void singlePathWritesExactBytes() throws Exception {
        insertThinRow(1);
        byte[] data = payload(300_000, 7);
        MemSource src = new MemSource(data);
        try (StreamingBlobWriter w = writer(1, LobWriteOptions.defaults(), 16 * 1024 * 1024)) {
            StreamingBlobWriter.Result r = w.write(src);
            assertEquals(StreamingBlobWriter.Path.SINGLE, r.path);
            assertEquals(1, r.statements);
        }
        assertArrayEquals(data, readBack(1));
        assertEquals(md5(data), md5(readBack(1)));
    }

    // ------------------------------------------------------------------ 路径 B

    @Test
    @DisplayName("B 路径：分块追加拼回完整内容，块数符合预期")
    void appendPathReassemblesExactBytes() throws Exception {
        insertThinRow(2);
        byte[] data = payload(1000, 3);
        // 用"余量几乎吃掉整个预算"强制走追加（包上限本身仍放得下整值——
        // 把包上限压到值以下是个 MySQL 上根本存不进去的配置，见 refusesValueLargerThanPacketLimit）；
        // 块 300 ⇒ 4 块（300+300+300+100）
        LobWriteOptions opts = new LobWriteOptions(64, 300, 1900, 0);
        try (StreamingBlobWriter w = writer(2, opts, 2000)) {
            StreamingBlobWriter.Result r = w.write(new MemSource(data));
            assertEquals(StreamingBlobWriter.Path.APPEND, r.path);
            assertEquals(4, r.statements, "1000 字节按 300 追加应为 4 条语句");
        }
        assertArrayEquals(data, readBack(2));
    }

    @Test
    @DisplayName("目标为 NULL 时首块用 SET：CONCAT(NULL,x) 会把整个值变成 NULL")
    void appendFirstBlockUsesSetOnNull() throws Exception {
        insertThinRow(3);
        byte[] data = payload(1000, 3);
        LobWriteOptions opts = new LobWriteOptions(64, 400, 1900, 0);
        try (StreamingBlobWriter w = writer(3, opts, 2000)) {
            StreamingBlobWriter.Result r = w.write(new MemSource(data));
            assertEquals(3, r.statements);
        }
        // 首块若也走 CONCAT，NULL 会把结果吞成 NULL（MySQL 的 CONCAT 语义），这里正面锁住
        assertArrayEquals(data, readBack(3));
    }

    @Test
    @DisplayName("续传：目标端写了一半时从断点接着写，不从 0 重来")
    void resumesFromPartialLength() throws Exception {
        insertThinRow(4);
        byte[] data = payload(1000, 5);
        // 先写前 400 字节，模拟崩在中途
        try (PreparedStatement ps = conn.prepareStatement("UPDATE " + TABLE + " SET payload = ? WHERE id = 4")) {
            ps.setBytes(1, sub(data, 0, 400));
            ps.executeUpdate();
        }
        MemSource src = new MemSource(data);
        LobWriteOptions opts = new LobWriteOptions(64, 300, 1900, 0);
        try (StreamingBlobWriter w = writer(4, opts, 2000)) {
            StreamingBlobWriter.Result r = w.write(src);
            assertEquals(StreamingBlobWriter.Path.APPEND, r.path);
            assertEquals(2, r.statements, "剩 600 字节按 300 块 ⇒ 2 条语句，而不是从 0 重来的 4 条");
        }
        assertArrayEquals(data, readBack(4));
        // 断点之前的区间一次都不该去源端拉
        assertTrue(src.fetches <= 600 / 64 + 2, "续传不应重拉已写部分, fetches=" + src.fetches);
    }

    @Test
    @DisplayName("幂等：已是完整长度时直接跳过，不发任何写语句")
    void skipsWhenAlreadyComplete() throws Exception {
        insertThinRow(5);
        byte[] data = payload(800, 11);
        try (StreamingBlobWriter w = writer(5, LobWriteOptions.defaults(), 16 * 1024 * 1024)) {
            w.write(new MemSource(data));
        }
        MemSource again = new MemSource(data);
        try (StreamingBlobWriter w = writer(5, LobWriteOptions.defaults(), 16 * 1024 * 1024)) {
            StreamingBlobWriter.Result r = w.write(again);
            assertTrue(r.skipped, "重跑应识别为已完成");
            assertEquals(0, r.statements);
        }
        assertEquals(0, again.fetches, "跳过时一次都不该去源端拉");
        assertArrayEquals(data, readBack(5));
    }

    @Test
    @DisplayName("目标比源长（上轮写坏）→ 整值重写而不是继续追加")
    void rewritesWhenTargetLongerThanSource() throws Exception {
        insertThinRow(6);
        try (PreparedStatement ps = conn.prepareStatement("UPDATE " + TABLE + " SET payload = ? WHERE id = 6")) {
            ps.setBytes(1, payload(2000, 42));
            ps.executeUpdate();
        }
        byte[] data = payload(900, 5);
        LobWriteOptions opts = new LobWriteOptions(64, 500, 1900, 0);
        try (StreamingBlobWriter w = writer(6, opts, 3000)) {
            w.write(new MemSource(data));
        }
        assertArrayEquals(data, readBack(6), "追加语义缩不回去，必须整值重写");
    }

    @Test
    @DisplayName("空值写成空串而不是 NULL")
    void emptyValue() throws Exception {
        insertThinRow(7);
        try (StreamingBlobWriter w = writer(7, LobWriteOptions.defaults(), 16 * 1024 * 1024)) {
            StreamingBlobWriter.Result r = w.write(new MemSource(new byte[0]));
            assertEquals(1, r.statements);
        }
        assertArrayEquals(new byte[0], readBack(7));
    }

    @Test
    @DisplayName("单值超过 max_allowed_packet → 明确报错，不要让 MySQL 甩 truncated")
    void refusesValueLargerThanPacketLimit() throws Exception {
        insertThinRow(10);
        // 实测踩过的坑：max_allowed_packet 同时限制 CONCAT() 的<b>结果</b>大小，
        // 所以"块切小一点再追加"根本绕不过去。MySQL 只会回一句
        // "Result of concat() was larger than max_allowed_packet - truncated"，
        // 既不说哪张表哪一列、也不说该调到多大——必须在这里换成能照着做的报错。
        try (StreamingBlobWriter w = writer(10, new LobWriteOptions(64, 100, 0, 0), 500)) {
            SQLException e = assertThrows(SQLException.class, () -> w.write(new MemSource(payload(1000, 1))));
            assertTrue(e.getMessage().contains("max_allowed_packet"), e.getMessage());
            assertTrue(e.getMessage().contains("1000"), "要报出实际需要的字节数: " + e.getMessage());
        }
        assertEquals(null, readBack(10), "拒绝之后不能留下半截值");
    }

    @Test
    @DisplayName("目标行不存在 → 立刻报错（大字段必须写在瘦行 INSERT 之后）")
    void failsWhenRowMissing() {
        StreamingBlobWriter w = writer(999, LobWriteOptions.defaults(), 16 * 1024 * 1024);
        SQLException e = assertThrows(SQLException.class, () -> w.write(new MemSource(payload(10, 1))));
        assertTrue(e.getMessage().contains("目标行不存在"), e.getMessage());
    }

    @Test
    @DisplayName("块级进度回调按块推进，供上层落断点")
    void blockProgressCallbacks() throws Exception {
        insertThinRow(8);
        List<Long> marks = new ArrayList<>();
        LobWriteOptions opts = new LobWriteOptions(64, 250, 1900, 0);
        try (StreamingBlobWriter w = writer(8, opts, 2000)) {
            w.onBlock((written, total) -> marks.add(written)).write(new MemSource(payload(1000, 2)));
        }
        assertEquals(List.of(250L, 500L, 750L, 1000L), marks);
    }

    @Test
    @DisplayName("源内容短于声明长度 → 报错，不写出被截断的值")
    void truncatedSourceAborts() throws Exception {
        insertThinRow(9);
        // 声明 1000 但只给得出 400
        LobChunkSource lying = new LobChunkSource() {
            @Override
            public long length() {
                return 1000;
            }

            @Override
            public byte[] fetch(long offset, int len) {
                return offset >= 400 ? new byte[0] : payload(Math.min(len, (int) (400 - offset)), 1);
            }

            @Override
            public String describe() {
                return "lying";
            }
        };
        LobWriteOptions opts = new LobWriteOptions(200, 400, 1900, 0);
        try (StreamingBlobWriter w = writer(9, opts, 2000)) {
            // 流在驱动内部被读取，IOException 会被包成 SQLException 抛出来——
            // 关键不是异常类型，而是"必须炸、且原因可追溯到截断"，不能写出一个短值就当成功
            Exception e = assertThrows(Exception.class, () -> w.write(lying));
            assertTrue(causeChain(e).contains("比声明的长度短"), causeChain(e));
        }
        // 失败后目标端不能留下一个"长度像模像样"的错值
        byte[] left = readBack(9);
        assertTrue(left == null || left.length != 1000, "截断失败后不该留下声称完整的值");
    }

    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append(" | ");
        }
        return sb.toString();
    }

    private static byte[] sub(byte[] b, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(b, from, out, 0, out.length);
        return out;
    }
}
