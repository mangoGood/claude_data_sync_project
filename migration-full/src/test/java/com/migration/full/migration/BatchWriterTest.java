package com.migration.full.migration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全量批量装载通道（P2-2）。
 *
 * <p>这里守的是开启"批量语句重写"之后<b>必然会踩</b>的两件事：
 * 一是结果码口径变了（重写后返回 SUCCESS_NO_INFO，按老口径会把全部行报成失败），
 * 二是失败粒度变了（一行冲突整条多值 INSERT 都失败，不按行重放就是整批静默丢）。
 */
@DisplayName("全量批量装载通道")
class BatchWriterTest {

    private static final String INSERT = "INSERT INTO bw_t (id, v) VALUES (?, ?)";

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:bw_" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE bw_t (id INT PRIMARY KEY, v VARCHAR(32))");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    @DisplayName("SUCCESS_NO_INFO 必须算成功，否则开了重写全量会把所有行报成失败")
    void successNoInfoCountsAsSuccess() {
        // 驱动把一批 3 行重写成一条多值 INSERT 后的典型返回
        long[] rewritten = BatchWriter.countBatchResults(
                new int[]{Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO}, 3);
        assertEquals(3, rewritten[0], "SUCCESS_NO_INFO 是成功而不是失败");
        assertEquals(0, rewritten[1]);

        // 只有 EXECUTE_FAILED 才是真失败
        long[] mixed = BatchWriter.countBatchResults(new int[]{1, Statement.EXECUTE_FAILED, 1}, 3);
        assertEquals(2, mixed[0]);
        assertEquals(1, mixed[1]);

        // 驱动只给一个汇总结果码时按提交行数补齐，进度不会塌成 1
        long[] collapsed = BatchWriter.countBatchResults(new int[]{Statement.SUCCESS_NO_INFO}, 500);
        assertEquals(500, collapsed[0]);
        assertEquals(0, collapsed[1]);
    }

    @Test
    @DisplayName("整批提交：满批自动识别 + 计数正确")
    void flushesFullBatch() throws Exception {
        try (BatchWriter writer = new BatchWriter(conn, INSERT, "bw_t", 3)) {
            writer.add(new Object[]{1, "a"});
            writer.add(new Object[]{2, "b"});
            assertFalse(writer.isFull());
            writer.add(new Object[]{3, "c"});
            assertTrue(writer.isFull());

            long[] r = writer.flush();
            assertEquals(3, r[0]);
            assertEquals(0, r[1]);
            assertTrue(writer.isEmpty());
        }
        assertEquals(3, rowCount());
    }

    @Test
    @DisplayName("批里有一行主键冲突：其余行必须照样落库，不能整批丢")
    void replaysRowByRowOnBatchFailure() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("INSERT INTO bw_t (id, v) VALUES (2, 'existing')");
        }
        try (BatchWriter writer = new BatchWriter(conn, INSERT, "bw_t", 10)) {
            writer.add(new Object[]{1, "a"});
            writer.add(new Object[]{2, "dup"});   // 冲突行
            writer.add(new Object[]{3, "c"});
            long[] r = writer.flush();

            // 三行都算"已就位"：不冲突的两行本轮写入，冲突的那行目标端本来就有。
            // 关键是不能因为一行冲突就把整批（可能上千行）都丢掉或都不计数。
            assertEquals(3, r[0], "整批不能因一行冲突而丢失");
            assertEquals(0, r[1], "主键冲突不计失败");
        }
        assertEquals(3, rowCount());
        assertEquals("existing", valueOf(2), "冲突行保留目标端原值，不被覆盖");
    }

    @Test
    @DisplayName("目标连接重建：缓冲行必须重放，不能随旧 statement 一起消失")
    void rebindReplaysBufferedRows() throws Exception {
        try (BatchWriter writer = new BatchWriter(conn, INSERT, "bw_t", 100)) {
            writer.add(new Object[]{1, "a"});
            writer.add(new Object[]{2, "b"});

            // 模拟"目标连接断了、换了一条新连接"
            Connection second = DriverManager.getConnection(conn.getMetaData().getURL(), "sa", "");
            writer.rebind(second);

            long[] r = writer.flush();
            assertEquals(2, r[0], "重建连接后缓冲行必须重放");
            second.close();
        }
        assertEquals(2, rowCount());
    }

    private int rowCount() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM bw_t")) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    private String valueOf(int id) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT v FROM bw_t WHERE id = " + id)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }
}
