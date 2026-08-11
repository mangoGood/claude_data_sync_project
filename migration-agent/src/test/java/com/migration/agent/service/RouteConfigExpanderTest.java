package com.migration.agent.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RouteConfigExpander} 单元测试：后端下发的路由 JSON → 引擎读的 route.* 属性，
 * 并用引擎自己的解析器兜底校验。
 */
@DisplayName("RouteConfigExpander 路由配置下发")
class RouteConfigExpanderTest {

    @Test
    @DisplayName("汇聚：规则展开成 route.merge.N.*，nodeId 单独下发")
    void expandMerge() {
        Properties props = new Properties();
        RouteConfigExpander.expand(props,
                "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"shard_db_*.order_*\","
                        + "\"target\":\"dw.order_all\",\"pkStrategy\":\"COMPOSITE_SOURCE\","
                        + "\"ddlPolicy\":\"FIRST_WINS\"}]}",
                "inst-b");

        assertEquals("MERGE", props.getProperty("route.mode"));
        assertEquals("inst-b", props.getProperty("route.node.id"));
        assertEquals("shard_db_*.order_*", props.getProperty("route.merge.1.match"));
        assertEquals("dw.order_all", props.getProperty("route.merge.1.target"));
        assertEquals("COMPOSITE_SOURCE", props.getProperty("route.merge.1.pk.strategy"));
        assertEquals("FIRST_WINS", props.getProperty("route.merge.1.ddl.policy"));
    }

    @Test
    @DisplayName("拆分：分片参数与模板逐项展开")
    void expandSplit() {
        Properties props = new Properties();
        RouteConfigExpander.expand(props,
                "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"user_id\","
                        + "\"algo\":\"HASH_MOD\",\"count\":16,\"targetDb\":\"dw_${shard/4}\","
                        + "\"targetTable\":\"orders_${shard}\",\"unrouted\":\"DEADLETTER\"}]}",
                null);

        assertEquals("SPLIT", props.getProperty("route.mode"));
        assertEquals("user_id", props.getProperty("route.split.1.shard.key"));
        assertEquals("16", props.getProperty("route.split.1.count"));
        assertEquals("dw_${shard/4}", props.getProperty("route.split.1.target.db"));
        assertEquals("orders_${shard}", props.getProperty("route.split.1.target.table"));
        assertEquals("DEADLETTER", props.getProperty("route.split.1.unrouted"));
        assertNull(props.getProperty("route.node.id"));
    }

    @Test
    @DisplayName("重新下发会先清掉旧的 route.*（改回 1:1 时不能留下失效规则）")
    void expandClearsStaleKeys() {
        Properties props = new Properties();
        props.setProperty("route.mode", "MERGE");
        props.setProperty("route.merge.1.match", "old.*");
        props.setProperty("route.merge.1.target", "dw.old");
        props.setProperty("target.db.database", "keep_me");

        RouteConfigExpander.expand(props, null, null);

        assertNull(props.getProperty("route.mode"));
        assertNull(props.getProperty("route.merge.1.match"));
        assertEquals("keep_me", props.getProperty("target.db.database"), "非 route.* 的键不能被误删");
    }

    @Test
    @DisplayName("模式为 NONE：不写任何 route.*")
    void noneModeWritesNothing() {
        Properties props = new Properties();
        RouteConfigExpander.expand(props, "{\"mode\":\"NONE\"}", "x");
        assertTrue(props.stringPropertyNames().stream().noneMatch(n -> n.startsWith("route.")));
    }

    @Test
    @DisplayName("非法配置在下发时就抛（引擎解析器兜底），不留到跑一半才发现")
    void invalidConfigFailsAtDispatch() {
        Properties props = new Properties();
        // 目标库/表模板都不含 ${shard}：所有分片会写进同一张表
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigExpander.expand(props,
                        "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"user_id\","
                                + "\"algo\":\"HASH_MOD\",\"count\":4,\"targetTable\":\"orders_all\"}]}",
                        null));
        assertTrue(e.getMessage().contains("不是拆分"), e.getMessage());

        assertThrows(IllegalArgumentException.class,
                () -> RouteConfigExpander.expand(props, "{not json", null));
    }

    @Test
    @DisplayName("下发时只挡真正不支持的库类型（Redis/Oracle），异构与文档型库对放行")
    void inapplicableLinksFailAtDispatch() {
        String merge = "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"db_*.t_*\",\"target\":\"dw.t\"}]}";

        Properties redis = new Properties();
        redis.setProperty("source.db.type", "redis");
        redis.setProperty("target.db.type", "redis");
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> RouteConfigExpander.expand(redis, merge, null)).getMessage().contains("Redis"));

        Properties oracle = new Properties();
        oracle.setProperty("source.db.type", "oracle");
        oracle.setProperty("target.db.type", "oracle");
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> RouteConfigExpander.expand(oracle, merge, null)).getMessage().contains("upsert"));

        // 异构关系库对与文档型库对现已支持，下发时放行
        Properties hetero = new Properties();
        hetero.setProperty("source.db.type", "mysql");
        hetero.setProperty("target.db.type", "postgresql");
        RouteConfigExpander.expand(hetero, merge, null);
        assertEquals("MERGE", hetero.getProperty("route.mode"));

        Properties mongo = new Properties();
        mongo.setProperty("source.db.type", "mongodb");
        mongo.setProperty("target.db.type", "mongodb");
        RouteConfigExpander.expand(mongo, merge, null);
        assertEquals("MERGE", mongo.getProperty("route.mode"));

        // 列处理与路由现已可以叠加，下发时不再拦
        Properties withColumnProcessing = new Properties();
        withColumnProcessing.setProperty("source.db.type", "mysql");
        withColumnProcessing.setProperty("target.db.type", "mysql");
        withColumnProcessing.setProperty("column.filter.db_1.t_1", "amount|<|100");
        RouteConfigExpander.expand(withColumnProcessing, merge, null);
        assertEquals("MERGE", withColumnProcessing.getProperty("route.mode"));
    }

    @Test
    @DisplayName("来源标识列（tagColumns）按逗号拼接下发")
    void expandTagColumns() {
        Properties props = new Properties();
        RouteConfigExpander.expand(props,
                "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"db.*\",\"target\":\"dw.t\","
                        + "\"tagColumns\":[\"_src_db\",\"_src_table\"]}]}", null);
        assertEquals("_src_db,_src_table", props.getProperty("route.merge.1.tag.columns"));
        assertFalse(props.getProperty("route.merge.1.tag.columns").contains("_src_node"));
    }
}
