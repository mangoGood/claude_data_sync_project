package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RoutingConfig} 单元测试：模式解析、汇聚规则解析与校验、目标实例组解析、
 * 非法配置 fail-stop，以及未配置路由时的零回归。
 */
@DisplayName("RoutingConfig 聚合路由配置")
class RoutingConfigTest {

    private static Properties mergeProps() {
        Properties p = new Properties();
        p.setProperty("route.mode", "MERGE");
        p.setProperty("route.merge.1.match", "shard_db_*.order_*");
        p.setProperty("route.merge.1.target", "dw.order_all");
        return p;
    }

    @Test
    @DisplayName("未配置 route.mode：isEmpty，router 恒等，任何表都走原 1:1 路径")
    void noneModeIsZeroRegression() {
        RoutingConfig config = RoutingConfig.loadFromProperties(new Properties());
        assertTrue(config.isEmpty());
        assertTrue(config.isValid());
        assertEquals(RoutingConfig.Mode.NONE, config.getMode());

        TableRouter router = config.router();
        assertTrue(router.isIdentity());
        assertFalse(router.matches("db1", "t1"));
        assertNull(router.shardKeyColumn("db1", "t1"));
        List<RouteTarget> targets = router.route("db1", "t1", null);
        assertEquals(1, targets.size());
        assertTrue(targets.get(0).isIdentity());
        assertEquals("db1", targets.get(0).getDatabase());
        assertEquals("t1", targets.get(0).getTable());
    }

    @Test
    @DisplayName("route.mode 取值非法：按 NONE 处理，不让配错的模式把任务卡死")
    void invalidModeFallsBackToNone() {
        Properties p = new Properties();
        p.setProperty("route.mode", "SHARDING");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertEquals(RoutingConfig.Mode.NONE, config.getMode());
        assertTrue(config.router().isIdentity());
    }

    @Test
    @DisplayName("汇聚：通配匹配的源表全部落到同一目标表，未命中的表退回 identity")
    void mergeRoutesMatchedTablesToOneTarget() {
        TableRouter router = RoutingConfig.loadFromProperties(mergeProps()).router();

        assertEquals(RoutingConfig.Mode.MERGE, router.mode());
        assertFalse(router.isIdentity());
        assertNull(router.shardKeyColumn("shard_db_1", "order_001"));

        for (String db : new String[]{"shard_db_1", "shard_db_7"}) {
            for (String table : new String[]{"order_001", "order_128"}) {
                List<RouteTarget> targets = router.route(db, table, null);
                assertEquals(1, targets.size());
                RouteTarget t = targets.get(0);
                assertFalse(t.isIdentity());
                assertEquals("dw", t.getDatabase());
                assertEquals("order_all", t.getTable());
                assertEquals(-1, t.getShardNo());
                assertNull(t.getNodeId());
            }
        }

        // 未命中规则的表不受影响
        assertFalse(router.matches("shard_db_1", "user"));
        assertTrue(router.route("shard_db_1", "user", null).get(0).isIdentity());
    }

    @Test
    @DisplayName("汇聚：大小写精确优先、忽略大小写回退")
    void mergeCaseFallback() {
        TableRouter router = RoutingConfig.loadFromProperties(mergeProps()).router();
        assertTrue(router.matches("shard_db_1", "order_001"));
        assertTrue(router.matches("SHARD_DB_1", "ORDER_001"));
    }

    @Test
    @DisplayName("汇聚：目标库省略时用任务默认目标库（多库任务按各自映射解析）")
    void mergeTargetDbFallsBackToTaskDefault() {
        Properties p = mergeProps();
        p.setProperty("route.merge.1.target", "order_all");
        TableRouter router = RoutingConfig.loadFromProperties(p)
                .router(srcDb -> "dw_" + srcDb);
        RouteTarget t = router.route("shard_db_1", "order_001", null).get(0);
        assertEquals("dw_shard_db_1", t.getDatabase());
        assertEquals("order_all", t.getTable());
    }

    @Test
    @DisplayName("汇聚默认：主键策略 COMPOSITE_SOURCE + 三列来源标识 + DDL FIRST_WINS")
    void mergeDefaults() {
        RoutingConfig config = RoutingConfig.loadFromProperties(mergeProps());
        MergeRule rule = config.getMergeRules().get(0);
        assertEquals(MergeRule.PkStrategy.COMPOSITE_SOURCE, rule.getPkStrategy());
        assertEquals(MergeRule.DEFAULT_TAG_COLUMNS, rule.getTagColumns());
        assertEquals(MergeRule.DdlPolicy.FIRST_WINS, rule.getDdlPolicy());

        assertEquals("node-a", rule.tagValue(MergeRule.TAG_NODE, "node-a", "shard_db_1", "order_001"));
        assertEquals("shard_db_1", rule.tagValue(MergeRule.TAG_DB, "node-a", "shard_db_1", "order_001"));
        assertEquals("order_001", rule.tagValue(MergeRule.TAG_TABLE, "node-a", "shard_db_1", "order_001"));
        // 未知列不注值（返回 null 而非空串，避免与"来源库名为空"混淆）
        assertNull(rule.tagValue("whatever", "node-a", "shard_db_1", "order_001"));
    }

    @Test
    @DisplayName("汇聚：KEEP 主键策略给出告警（源主键必须跨源全局唯一）")
    void mergeKeepPkWarns() {
        Properties p = mergeProps();
        p.setProperty("route.merge.1.pk.strategy", "KEEP");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertTrue(config.isValid());
        assertEquals(MergeRule.PkStrategy.KEEP, config.getMergeRules().get(0).getPkStrategy());
        assertTrue(config.getWarnings().stream().anyMatch(w -> w.contains("KEEP")));
    }

    @Test
    @DisplayName("汇聚：COMPOSITE_SOURCE 却把来源标识列清空 → 校验失败（复合主键无从构造）")
    void mergeCompositePkWithoutTagColumnsIsError() {
        Properties p = mergeProps();
        p.setProperty("route.merge.1.tag.columns", "");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("COMPOSITE_SOURCE"));
    }

    @Test
    @DisplayName("汇聚：不支持的来源标识列 → 校验失败")
    void mergeUnknownTagColumnIsError() {
        Properties p = mergeProps();
        p.setProperty("route.merge.1.tag.columns", "_src_db,_src_ip");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("_src_ip"));
    }

    @Test
    @DisplayName("多条规则按 id 数值序生效（10 不会排在 2 前面）")
    void ruleOrderIsNumeric() {
        Properties p = new Properties();
        p.setProperty("route.mode", "MERGE");
        p.setProperty("route.merge.2.match", "db.t2");
        p.setProperty("route.merge.2.target", "dw.t2_all");
        p.setProperty("route.merge.10.match", "db.t10");
        p.setProperty("route.merge.10.target", "dw.t10_all");
        p.setProperty("route.merge.1.match", "db.t1");
        p.setProperty("route.merge.1.target", "dw.t1_all");

        List<MergeRule> rules = RoutingConfig.loadFromProperties(p).getMergeRules();
        assertEquals(3, rules.size());
        assertEquals("1", rules.get(0).getId());
        assertEquals("2", rules.get(1).getId());
        assertEquals("10", rules.get(2).getId());
    }

    @Test
    @DisplayName("非法配置 fail-stop：router() 抛异常，而不是丢掉坏规则继续跑")
    void invalidConfigFailsFast() {
        Properties p = new Properties();
        p.setProperty("route.mode", "MERGE");
        p.setProperty("route.merge.1.match", "shard_db_*.order_*");
        // 缺 target
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertThrows(IllegalStateException.class, config::router);
    }

    @Test
    @DisplayName("模式与规则不匹配：MERGE 模式无 merge 规则 → 校验失败")
    void modeWithoutRulesIsError() {
        Properties p = new Properties();
        p.setProperty("route.mode", "MERGE");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("MERGE"));
    }

    @Test
    @DisplayName("目标实例组：按组名+序号解析，缺 host/port 记为错误")
    void nodeGroupParsing() {
        Properties p = new Properties();
        p.setProperty("route.mode", "SPLIT");
        p.setProperty("route.split.1.match", "app.order");
        p.setProperty("route.split.1.shard.key", "user_id");
        p.setProperty("route.split.1.algo", "HASH_MOD");
        p.setProperty("route.split.1.count", "4");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        p.setProperty("route.split.1.target.group", "g1");
        p.setProperty("route.node.g1.0.host", "10.0.0.1");
        p.setProperty("route.node.g1.0.port", "3306");
        p.setProperty("route.node.g1.0.username", "u");
        p.setProperty("route.node.g1.0.password", "pw");
        p.setProperty("route.node.g1.1.host", "10.0.0.2");
        p.setProperty("route.node.g1.1.port", "3306");

        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertTrue(config.isValid(), () -> String.join("; ", config.getErrors()));
        assertEquals(2, config.getNodes("g1").size());
        RouteNode node = config.getNode("g1#1");
        assertEquals("10.0.0.2", node.getHost());
        assertEquals(3306, node.getPort());
        assertNull(config.getNode("g1#9"));
    }

    @Test
    @DisplayName("拆分引用未配置的目标实例组 → 校验失败")
    void splitWithUnknownNodeGroupIsError() {
        Properties p = new Properties();
        p.setProperty("route.mode", "SPLIT");
        p.setProperty("route.split.1.match", "app.order");
        p.setProperty("route.split.1.shard.key", "user_id");
        p.setProperty("route.split.1.algo", "HASH_MOD");
        p.setProperty("route.split.1.count", "4");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        p.setProperty("route.split.1.target.group", "ghost");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("ghost"));
    }

    // ==================== 适用范围拦截 ====================

    private static Properties mergePropsWith(String... kv) {
        Properties p = mergeProps();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    private static String errorsOf(Properties p) {
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid(), "应判为非法配置");
        return String.join("; ", config.getErrors());
    }

    @Test
    @DisplayName("同构 mysql→mysql / pg→pg：放行")
    void homogeneousPairsAllowed() {
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mysql", "target.db.type", "mysql")).isValid());
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "postgresql", "target.db.type", "postgresql")).isValid());
    }

    @Test
    @DisplayName("mongo↔mongo 与 mysql→es：放行（各自引擎按集合/索引实现同一套规则）")
    void documentEnginesAllowed() {
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mongodb", "target.db.type", "mongodb")).isValid());
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mysql", "target.db.type", "elasticsearch")).isValid());
    }

    @Test
    @DisplayName("Redis：拒绝——没有表的概念，汇聚/拆分不是分库分表那回事")
    void redisRejected() {
        assertTrue(errorsOf(mergePropsWith("source.db.type", "redis", "target.db.type", "redis"))
                .contains("Redis"));
    }

    @Test
    @DisplayName("异构关系库对 mysql↔pg：放行（来源标识列进得了翻译器产出的 DDL）")
    void heterogeneousRelationalAllowed() {
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mysql", "target.db.type", "postgresql")).isValid());
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "postgresql", "target.db.type", "mysql")).isValid());
    }

    @Test
    @DisplayName("Oracle 与不成对的文档型库对：拒绝")
    void unsupportedPairsRejected() {
        assertTrue(errorsOf(mergePropsWith("source.db.type", "oracle", "target.db.type", "oracle"))
                .contains("upsert"));
        assertTrue(errorsOf(mergePropsWith("source.db.type", "mongodb", "target.db.type", "mysql"))
                .contains("mongodb→mysql"));
    }

    @Test
    @DisplayName("库类型缺失：不判——单测与直驱场景不下发库类型，把\"没声明\"当\"不支持\"会误伤")
    void missingDbTypeIsNotJudged() {
        assertTrue(RoutingConfig.loadFromProperties(mergeProps()).isValid());
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith("source.db.type", "mysql")).isValid());
    }

    @Test
    @DisplayName("叠加列处理：放行（列处理规则已按表取源库名，两者可以同时用）")
    void columnProcessingCanCoexist() {
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mysql", "target.db.type", "mysql",
                "column.filter.shard_db_1.order_001", "amount|<|100")).isValid());
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mysql", "target.db.type", "mysql",
                "column.mapping.db.t", "a:b")).isValid());
        assertTrue(RoutingConfig.loadFromProperties(mergePropsWith(
                "source.db.type", "mysql", "target.db.type", "mysql",
                "column.extra.db.t", "c:CUSTOM:src")).isValid());
    }

    @Test
    @DisplayName("route.mode=NONE 时不做任何适用性判断：1:1 任务配了列处理照常跑")
    void noneModeSkipsApplicabilityChecks() {
        Properties p = new Properties();
        p.setProperty("source.db.type", "mongodb");
        p.setProperty("target.db.type", "mongodb");
        p.setProperty("column.filter.db.t", "a|<|1");
        assertTrue(RoutingConfig.loadFromProperties(p).isValid());
    }
}
