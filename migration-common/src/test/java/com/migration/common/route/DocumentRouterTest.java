package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DocumentRouter} 单元测试：文档型引擎（Mongo 集合 / ES 索引）的落点与标识计算。
 *
 * <p>重点是<b>汇聚必须换 _id</b>：两个来源里 _id 相同的文档若沿用原 _id，
 * upsert 会互相覆盖，数据只会少、不会报错——与关系库那边"必须用复合主键"是同一件事。
 */
@DisplayName("DocumentRouter 文档型引擎的聚合路由")
class DocumentRouterTest {

    private static Properties base(String sourceType, String targetType) {
        Properties p = new Properties();
        p.setProperty("source.db.type", sourceType);
        p.setProperty("target.db.type", targetType);
        return p;
    }

    private static DocumentRouter merge() {
        Properties p = base("mongodb", "mongodb");
        p.setProperty("route.mode", "MERGE");
        p.setProperty("route.node.id", "inst-a");
        p.setProperty("route.merge.1.match", "shard_*.orders");
        p.setProperty("route.merge.1.target", "dw.orders_all");
        return DocumentRouter.fromProperties(p);
    }

    private static DocumentRouter split() {
        Properties p = base("mongodb", "mongodb");
        p.setProperty("route.mode", "SPLIT");
        p.setProperty("route.split.1.match", "app.orders");
        p.setProperty("route.split.1.shard.key", "user_id");
        p.setProperty("route.split.1.algo", "HASH_MOD");
        p.setProperty("route.split.1.count", "4");
        p.setProperty("route.split.1.target.table", "orders_${shard}");
        p.setProperty("route.split.1.unrouted", "DEADLETTER");
        return DocumentRouter.fromProperties(p);
    }

    @Test
    @DisplayName("未配置路由：isActive=false，任何集合都不命中（走原 1:1 路径）")
    void noneModeIsZeroRegression() {
        DocumentRouter router = DocumentRouter.fromProperties(base("mongodb", "mongodb"));
        assertFalse(router.isActive());
        assertFalse(router.matches("app", "orders"));
        assertNull(router.mergeTarget("app", "orders", "app"));
        assertTrue(router.allShards("app", "orders", "app").isEmpty());
    }

    @Test
    @DisplayName("汇聚：多个源集合落到同一个目标集合，未命中的不受影响")
    void mergeTargets() {
        DocumentRouter router = merge();
        assertTrue(router.isMerge());
        assertEquals("dw.orders_all", router.mergeTarget("shard_1", "orders", "shard_1").toString());
        assertEquals("dw.orders_all", router.mergeTarget("shard_2", "orders", "shard_2").toString());
        assertFalse(router.matches("shard_1", "users"));
    }

    @Test
    @DisplayName("汇聚：_id 带来源前缀，跨来源同 _id 不再互相覆盖")
    void mergedIdIsSourceQualified() {
        DocumentRouter router = merge();
        String a = router.mergedId("shard_1", "orders", 7);
        String b = router.mergedId("shard_2", "orders", 7);
        assertEquals("inst-a|shard_1|orders|7", a);
        assertEquals("inst-a|shard_2|orders|7", b);
        assertFalse(a.equals(b), "两个来源的同 _id 必须区分开");
    }

    @Test
    @DisplayName("汇聚：来源标识写进文档字段，可按来源筛选与对数")
    void mergeTagsAreDocumentFields() {
        assertEquals("{_src_node=inst-a, _src_db=shard_1, _src_table=orders}",
                merge().mergeTags("shard_1", "orders").toString());
    }

    @Test
    @DisplayName("拆分：按分片键算落点，与关系库同口径（整数按数值取模）")
    void splitTargets() {
        DocumentRouter router = split();
        assertEquals("user_id", router.shardKeyField("app", "orders"));
        assertEquals("app.orders_1", router.shardOf("app", "orders", 5L, "app").toString());
        assertEquals("app.orders_3", router.shardOf("app", "orders", 7L, "app").toString());
        // 值的表示形式不能影响落点：全量读到 Long、change stream 里可能是 Integer/String
        assertEquals(router.shardOf("app", "orders", 5L, "app").toString(),
                router.shardOf("app", "orders", "5", "app").toString());
    }

    @Test
    @DisplayName("拆分：全部分片可枚举（预建集合/索引、广播删都要用）")
    void splitAllShards() {
        List<DocumentRouter.Target> shards = split().allShards("app", "orders", "app");
        assertEquals(4, shards.size());
        assertEquals("app.orders_0", shards.get(0).toString());
        assertEquals("app.orders_3", shards.get(3).toString());
    }

    @Test
    @DisplayName("拆分：分片键为空算不出落点，交由未路由策略处置")
    void splitUnroutable() {
        DocumentRouter router = split();
        assertNull(router.shardOf("app", "orders", null, "app"));
        assertEquals(SplitRule.UnroutedPolicy.DEADLETTER, router.unroutedPolicy("app", "orders"));
    }

    @Test
    @DisplayName("mysql→elasticsearch 也是受支持的库对（索引级汇聚/拆分）")
    void elasticPairSupported() {
        Properties p = base("mysql", "elasticsearch");
        p.setProperty("route.mode", "MERGE");
        p.setProperty("route.merge.1.match", "app.order_*");
        p.setProperty("route.merge.1.target", "dw.orders_all");
        DocumentRouter router = DocumentRouter.fromProperties(p);
        assertTrue(router.matches("app", "order_001"));
        assertEquals("dw.orders_all", router.mergeTarget("app", "order_001", "app").toString());
    }
}
