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
}
