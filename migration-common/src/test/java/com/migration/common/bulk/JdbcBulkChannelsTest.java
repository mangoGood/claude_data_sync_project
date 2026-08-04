package com.migration.common.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 装载通道的选路：<b>任何不确定都往安全档降</b>。
 *
 * <p>选错档位的后果分两种，这里各守一条：
 * <ul>
 *   <li>COPY 用在非 PG 目标、direct-path 用在非 Oracle 目标 → 直接降级 BATCH（配置层面就挡掉）；</li>
 *   <li>direct-path 用在<b>单表分片并行</b>上 → 也要降级。直接路径装载持表级排他锁，
 *       几个 worker 同时写同一张表只会互相阻塞，比不开还慢——这是最容易被忽略的一条，
 *       因为它不报错，只是悄悄变慢。</li>
 * </ul>
 */
@DisplayName("装载通道选路与降级")
class JdbcBulkChannelsTest {

    private static final String INSERT = "INSERT INTO t (id, v) VALUES (?, ?)";

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:ch_" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32))");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    private JdbcBulkChannel open(BulkLoadOptions.Mode mode, String targetDbType, boolean exclusiveWriter)
            throws SQLException {
        return JdbcBulkChannels.open(conn, INSERT, "t", "id, v", "t", targetDbType,
                BulkLoadOptions.of(true, mode, 100, 0), 100, exclusiveWriter);
    }

    @Test
    @DisplayName("AUTO/MySQL 目标 → BATCH")
    void autoResolvesToBatch() throws Exception {
        try (JdbcBulkChannel ch = open(BulkLoadOptions.Mode.AUTO, "mysql", true)) {
            assertEquals(BulkLoadOptions.Mode.BATCH, ch.mode());
        }
    }

    @Test
    @DisplayName("COPY 用在非 PostgreSQL 目标 → 降级 BATCH")
    void copyOnNonPostgresDowngrades() throws Exception {
        try (JdbcBulkChannel ch = open(BulkLoadOptions.Mode.COPY, "mysql", true)) {
            assertEquals(BulkLoadOptions.Mode.BATCH, ch.mode());
        }
    }

    @Test
    @DisplayName("Oracle direct-path：独占写入者才启用")
    void directPathRequiresExclusiveWriter() throws Exception {
        try (JdbcBulkChannel exclusive = open(BulkLoadOptions.Mode.DIRECT_PATH, "oracle", true)) {
            assertEquals(BulkLoadOptions.Mode.DIRECT_PATH, exclusive.mode());
        }
        try (JdbcBulkChannel sharded = open(BulkLoadOptions.Mode.DIRECT_PATH, "oracle", false)) {
            assertEquals(BulkLoadOptions.Mode.BATCH, sharded.mode(),
                    "单表分片并行下 direct-path 会互相阻塞，必须降级");
        }
    }

    @Test
    @DisplayName("direct-path 档位下仍能正常写入（提示只是注释，不改语义）")
    void directPathStillWrites() throws Exception {
        try (JdbcBulkChannel ch = open(BulkLoadOptions.Mode.DIRECT_PATH, "oracle", true)) {
            ch.add(new Object[]{1, "a"});
            ch.add(new Object[]{2, "b"});
            long[] r = ch.flush();
            assertEquals(2, r[0]);
            assertEquals(0, r[1]);
        }
    }

    @Test
    @DisplayName("APPEND_VALUES 提示只加在 INSERT 上；语句形状不对就返回 null（调用方降级）")
    void appendValuesHint() {
        assertEquals("INSERT /*+ APPEND_VALUES */ INTO t (id) VALUES (?)",
                JdbcBulkChannels.withAppendValues("INSERT INTO t (id) VALUES (?)"));
        assertTrue(JdbcBulkChannels.withAppendValues("  insert into t values (?)")
                .startsWith("INSERT /*+ APPEND_VALUES */"), "大小写不敏感");
        assertNull(JdbcBulkChannels.withAppendValues("MERGE INTO t USING ..."));
        assertNull(JdbcBulkChannels.withAppendValues(null));
    }
}
