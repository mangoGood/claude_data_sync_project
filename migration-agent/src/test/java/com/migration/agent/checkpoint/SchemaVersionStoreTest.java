package com.migration.agent.checkpoint;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SchemaVersionStore} 里不碰数据库的那一半：jsonl 行的键解析与位点折算。
 *
 * <p><b>位点折算这一组必须与 {@code SchemaTimeline.monotonicKey} 逐字对齐</b>。两处刻意各存
 * 一份实现（放进 migration-common 会让"改 common 后 fat jar 必须 clean install"波及这条链路
 * 上的每次改动），代价就是要靠测试钉住它们的一致性——一旦漂了，agent 算出的行键与 extract
 * 落盘的版本对不上，上传去重和回灌排序全错，而且不会有任何报错。
 */
@DisplayName("表结构版本中心存储：行解析与位点折算")
class SchemaVersionStoreTest {

    @Test
    @DisplayName("解析 jsonl 行里的定位键，payload 本体保持不透明")
    void parsesRowKeys() {
        String line = "{\"f\":\"mysql-bin.000007\",\"p\":4321,\"db\":\"Test1\",\"tbl\":\"Orders\","
                + "\"kind\":\"ALTERED\",\"schema\":{\"database\":\"Test1\",\"table\":\"Orders\"}}";
        SchemaVersionStore.Row r = SchemaVersionStore.parse(line);

        assertNotNull(r);
        assertEquals("test1", r.db, "库表名统一小写，跨机比对才对得上");
        assertEquals("orders", r.table);
        assertEquals("mysql-bin.000007", r.file);
        assertEquals(4321L, r.pos);
        assertEquals("ALTERED", r.kind);
        assertEquals(SchemaVersionStore.monotonicKey("mysql-bin.000007", 4321), r.monotonicKey);
    }

    @Test
    @DisplayName("DROPPED 行（payload 里 schema 为 null）照样要能定位")
    void parsesDroppedRow() {
        SchemaVersionStore.Row r = SchemaVersionStore.parse(
                "{\"f\":\"mysql-bin.000001\",\"p\":100,\"db\":\"db\",\"tbl\":\"t\",\"kind\":\"DROPPED\"}");
        assertNotNull(r);
        assertEquals("DROPPED", r.kind);
    }

    @Test
    @DisplayName("崩溃留下的半条要跳过而不是抛——一行坏数据不能挡住整批上传")
    void skipsBrokenLine() {
        assertNull(SchemaVersionStore.parse("{\"f\":\"mysql-bin.000001\",\"p\":10,\"db\":\"db\",\"tb"));
        assertNull(SchemaVersionStore.parse(""));
        assertNull(SchemaVersionStore.parse(null));
        assertNull(SchemaVersionStore.parse("{\"f\":\"x\",\"p\":1}"), "没有表名就定位不了，按坏行处理");
    }

    @Test
    @DisplayName("去重键 = 库.表#位点，同位点的重放行会被合并")
    void dedupKey() {
        SchemaVersionStore.Row a = SchemaVersionStore.parse(
                "{\"f\":\"mysql-bin.000001\",\"p\":100,\"db\":\"db\",\"tbl\":\"t\",\"kind\":\"CREATED\"}");
        SchemaVersionStore.Row b = SchemaVersionStore.parse(
                "{\"f\":\"mysql-bin.000001\",\"p\":100,\"db\":\"DB\",\"tbl\":\"T\",\"kind\":\"CREATED\"}");
        assertEquals(a.dedupKey(), b.dedupKey());
    }

    @Test
    @DisplayName("位点折算：文件号在高位，跨文件严格递增")
    void monotonicKeyOrdering() {
        assertTrue(SchemaVersionStore.monotonicKey("mysql-bin.000002", 4)
                > SchemaVersionStore.monotonicKey("mysql-bin.000001", 999999999L));
        assertTrue(SchemaVersionStore.monotonicKey("mysql-bin.000001", 200)
                > SchemaVersionStore.monotonicKey("mysql-bin.000001", 100));
        assertEquals(100, SchemaVersionStore.monotonicKey("binlog", 100));
        assertEquals(0, SchemaVersionStore.monotonicKey(null, 0));
    }

    @Test
    @DisplayName("与 extract 侧 SchemaTimeline.monotonicKey 的同一组样例必须算出同一个值")
    void monotonicKeyMatchesExtractSide() {
        // 这几组值同时钉在 migration-extract 的 SchemaTimelineTest.MonotonicKey 里。
        // 两边任何一侧改了折算方式，这里就会红——那正是要拦的漂移。
        assertEquals((7L << 32) | 4321L, SchemaVersionStore.monotonicKey("mysql-bin.000007", 4321));
        assertEquals((1L << 32) | 100L, SchemaVersionStore.monotonicKey("mysql-bin.000001", 100));
        assertEquals((123L << 32) | 4L, SchemaVersionStore.monotonicKey("mysql-bin.000123", 4));
    }

    @Test
    @DisplayName("本地历史文件路径与 extract 的默认值一致，否则上传方与写入方各写各的")
    void historyPathMatchesExtractDefault() {
        assertEquals("./files/task-9/schema_history.jsonl",
                SchemaVersionStore.historyPath("task-9").toString());
    }
}
