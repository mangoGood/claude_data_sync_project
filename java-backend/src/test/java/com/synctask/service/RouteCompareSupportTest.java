package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RouteCompareSupport} 单元测试：行数对比的路由感知。
 *
 * <p>模板渲染与通配匹配是引擎侧 ShardTemplate/TablePattern 的镜像实现，
 * 用例特意与引擎侧保持同样的形态，防止两边语义漂移。
 */
@DisplayName("RouteCompareSupport 对比的路由感知")
class RouteCompareSupportTest {

    private static final String MERGE = "{\"mode\":\"MERGE\",\"merge\":[{"
            + "\"match\":\"shard_db_*.order_*\",\"target\":\"dw.order_all\"}]}";
    private static final String SPLIT = "{\"mode\":\"SPLIT\",\"split\":[{"
            + "\"match\":\"app.orders\",\"shardKey\":\"user_id\",\"algo\":\"HASH_MOD\",\"count\":8,"
            + "\"targetDb\":\"dw_${shard/4}\",\"targetTable\":\"orders_${shard}\"}]}";

    @Test
    @DisplayName("未配置路由：mode=NONE，两种解析都返回 null（对比走原 1:1 路径）")
    void noRouting() {
        assertEquals("NONE", RouteCompareSupport.modeOf(null));
        assertEquals("NONE", RouteCompareSupport.modeOf(""));
        assertNull(RouteCompareSupport.mergeTargetOf(null, "db", "t", "dw", "n"));
        assertNull(RouteCompareSupport.splitTargetsOf(null, "db", "t", "dw"));
    }

    @Test
    @DisplayName("汇聚：命中规则的表解析出合并表 + 来源标识过滤条件")
    void mergeTargetWithTagFilters() {
        RouteCompareSupport.MergeTarget target = RouteCompareSupport.mergeTargetOf(
                MERGE, "shard_db_2", "order_007", "fallback_db", "node-a");
        assertEquals("dw", target.database);
        assertEquals("order_all", target.table);
        assertEquals("node-a", target.tagFilters.get("_src_node"));
        assertEquals("shard_db_2", target.tagFilters.get("_src_db"));
        assertEquals("order_007", target.tagFilters.get("_src_table"));
    }

    @Test
    @DisplayName("汇聚：未命中规则的表返回 null（该表仍按 1:1 对比）")
    void mergeUnmatchedTable() {
        assertNull(RouteCompareSupport.mergeTargetOf(MERGE, "shard_db_2", "users", "dw", "node-a"));
    }

    @Test
    @DisplayName("汇聚：目标只写表名时用任务默认目标库")
    void mergeTargetWithoutDb() {
        String json = "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"db.*\",\"target\":\"order_all\"}]}";
        RouteCompareSupport.MergeTarget target =
                RouteCompareSupport.mergeTargetOf(json, "db", "t1", "task_default", "n");
        assertEquals("task_default", target.database);
        assertEquals("order_all", target.table);
    }

    @Test
    @DisplayName("汇聚：自定义来源标识列时只按配置的那几列过滤")
    void mergeCustomTagColumns() {
        String json = "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"db.*\",\"target\":\"dw.t\","
                + "\"tagColumns\":[\"_src_db\",\"_src_table\"]}]}";
        RouteCompareSupport.MergeTarget target =
                RouteCompareSupport.mergeTargetOf(json, "db", "t1", "dw", "node-a");
        assertEquals(2, target.tagFilters.size());
        assertFalse(target.tagFilters.containsKey("_src_node"));
    }

    @Test
    @DisplayName("拆分：枚举出全部分片表（库模板按分片号整除渲染）")
    void splitEnumeratesShards() {
        RouteCompareSupport.SplitTargets targets =
                RouteCompareSupport.splitTargetsOf(SPLIT, "app", "orders", "dw");
        assertEquals(8, targets.shards.size());
        assertEquals("dw_0", targets.shards.get(0)[0]);
        assertEquals("orders_0", targets.shards.get(0)[1]);
        assertEquals("dw_0", targets.shards.get(3)[0]);
        assertEquals("dw_1", targets.shards.get(4)[0]);
        assertEquals("orders_7", targets.shards.get(7)[1]);
    }

    @Test
    @DisplayName("拆分：RANGE/LIST 的分片数由表长度推导（与引擎口径一致）")
    void splitShardCountFromTables() {
        String range = "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"amount\","
                + "\"algo\":\"RANGE\",\"range\":\"0:100,100:1000,1000:10000\","
                + "\"targetTable\":\"orders_${shard}\"}]}";
        assertEquals(3, RouteCompareSupport.splitTargetsOf(range, "app", "orders", "dw").shards.size());

        String list = "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"country\","
                + "\"algo\":\"LIST\",\"list\":\"CN:0,US:1,JP:2\",\"targetTable\":\"orders_${shard}\"}]}";
        assertEquals(3, RouteCompareSupport.splitTargetsOf(list, "app", "orders", "dw").shards.size());
    }

    @Test
    @DisplayName("拆分：DATE_FORMAT 分片不可枚举，返回 null（对比无法覆盖）")
    void splitDateFormatNotEnumerable() {
        String json = "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"ct\","
                + "\"algo\":\"DATE_FORMAT\",\"dateFormat\":\"yyyyMM\",\"targetTable\":\"orders_${shard}\"}]}";
        assertNull(RouteCompareSupport.splitTargetsOf(json, "app", "orders", "dw"));
    }

    @Test
    @DisplayName("模板渲染：${shard} / ${shard/N} / ${shard%N}")
    void templateRendering() {
        assertEquals("orders_5", RouteCompareSupport.render("orders_${shard}", 5));
        assertEquals("dw_2", RouteCompareSupport.render("dw_${shard/2}", 5));
        assertEquals("dw_1", RouteCompareSupport.render("dw_${shard%2}", 5));
        assertEquals("plain", RouteCompareSupport.render("plain", 3));
    }

    @Test
    @DisplayName("通配匹配：* 与 ?，点号是字面量，regex: 前缀走正则")
    void patternMatching() {
        assertTrue(RouteCompareSupport.matches("shard_db_*.order_*", "shard_db_1", "order_001"));
        assertFalse(RouteCompareSupport.matches("shard_db_*.order_*", "shard_db_1", "users"));
        assertFalse(RouteCompareSupport.matches("db1.t1", "db1x", "t1"));
        assertTrue(RouteCompareSupport.matches("regex:shard_db_\\d+\\.order_\\d{3}", "shard_db_7", "order_042"));
    }
}
