package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SplitRouter} 单元测试：四种分片算法、库/表模板渲染、跨实例节点划分、
 * 未路由行的三种处置，以及未命中规则时的 identity 直通。
 */
@DisplayName("SplitRouter 拆分路由")
class SplitRouterTest {

    private static Properties base() {
        Properties p = new Properties();
        p.setProperty("route.mode", "SPLIT");
        p.setProperty("route.split.1.match", "app.order");
        p.setProperty("route.split.1.shard.key", "user_id");
        return p;
    }

    private static TableRouter router(Properties p) {
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertTrue(config.isValid(), () -> "配置应合法，实际错误: " + String.join("; ", config.getErrors()));
        return config.router(srcDb -> "dw");
    }

    private static Properties hashMod16() {
        Properties p = base();
        p.setProperty("route.split.1.algo", "HASH_MOD");
        p.setProperty("route.split.1.count", "16");
        p.setProperty("route.split.1.target.db", "dw_${shard/2}");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        return p;
    }

    @Test
    @DisplayName("HASH_MOD：整型键直接取模，库/表模板按分片号渲染")
    void hashModIntegerKey() {
        TableRouter router = router(hashMod16());
        assertEquals("user_id", router.shardKeyColumn("app", "order"));

        List<RouteTarget> targets = router.route("app", "order", 33L);
        assertEquals(1, targets.size());
        RouteTarget t = targets.get(0);
        assertEquals(1, t.getShardNo());            // 33 % 16
        assertEquals("dw_0", t.getDatabase());      // 1 / 2
        assertEquals("order_1", t.getTable());
        assertFalse(t.isIdentity());

        // 负数按 floorMod，不会算出负分片号
        assertEquals(13, router.route("app", "order", -3).get(0).getShardNo());
        assertEquals(5, router.route("app", "order", 21).get(0).getShardNo());
        // Integer 与 Long 同值必须落同一片
        assertEquals(router.route("app", "order", 21).get(0),
                router.route("app", "order", 21L).get(0));
    }

    @Test
    @DisplayName("HASH_MOD：同一个整数值的各种表示形式必须落同一片（全量/增量口径一致）")
    void integralValuesHashIdenticallyAcrossRepresentations() {
        TableRouter router = router(hashMod16());
        // 全量拿到的是 Long/Integer，增量类型化值里可能是字符串或 BigDecimal——
        // 按 Java 类型分流会让同一行被算到两个分片上，两边各留一份且不报错
        int expected = router.route("app", "order", 5L).get(0).getShardNo();
        for (Object v : new Object[]{5, 5L, (short) 5, (byte) 5, "5", " 5 ",
                new java.math.BigInteger("5"), new java.math.BigDecimal("5.00"), 5.0d}) {
            assertEquals(expected, router.route("app", "order", v).get(0).getShardNo(),
                    "值 " + v + "(" + v.getClass().getSimpleName() + ") 应与 Long 5 落同一片");
        }
        assertEquals(5, expected);   // 5 % 16
    }

    @Test
    @DisplayName("HASH_MOD：非整型键走 CRC32(UTF-8)，跨进程稳定且落在分片区间内")
    void hashModStringKey() {
        TableRouter router = router(hashMod16());
        // CRC32("abc") = 0x352441C2，低 4 位 = 2
        assertEquals(2, router.route("app", "order", "abc").get(0).getShardNo());
        for (String v : new String[]{"", "u-1", "用户42", "9223372036854775807000"}) {
            int shard = router.route("app", "order", v).get(0).getShardNo();
            assertTrue(shard >= 0 && shard < 16, "分片号越界: " + shard);
            assertEquals(shard, router.route("app", "order", v).get(0).getShardNo());
        }
    }

    @Test
    @DisplayName("RANGE：左闭右开区间落片，区间外算不出分片")
    void rangeAlgorithm() {
        Properties p = base();
        p.setProperty("route.split.1.algo", "RANGE");
        p.setProperty("route.split.1.range", "0:100,100:1000,1000:10000");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        p.setProperty("route.split.1.unrouted", "DEADLETTER");
        TableRouter router = router(p);

        assertEquals(0, router.route("app", "order", 0).get(0).getShardNo());
        assertEquals(0, router.route("app", "order", 99).get(0).getShardNo());
        assertEquals(1, router.route("app", "order", 100).get(0).getShardNo());
        assertEquals(2, router.route("app", "order", 9999).get(0).getShardNo());
        // 区间外：未路由 → 死信（空列表），不是静默丢弃
        assertTrue(router.route("app", "order", 10000).isEmpty());
        assertTrue(router.route("app", "order", "not-a-number").isEmpty());
        // 分片数由区间数推导，预建 3 张表
        assertEquals(3, router.allTargets("app", "order").size());
    }

    @Test
    @DisplayName("LIST：枚举值映射落片，大小写回退，未覆盖值按未路由处置")
    void listAlgorithm() {
        Properties p = base();
        p.setProperty("route.split.1.shard.key", "country");
        p.setProperty("route.split.1.algo", "LIST");
        p.setProperty("route.split.1.list", "CN:0,US:1,JP:2");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        p.setProperty("route.split.1.unrouted", "DEADLETTER");
        TableRouter router = router(p);

        assertEquals(0, router.route("app", "order", "CN").get(0).getShardNo());
        assertEquals(1, router.route("app", "order", "US").get(0).getShardNo());
        assertEquals(1, router.route("app", "order", "us").get(0).getShardNo());
        assertTrue(router.route("app", "order", "DE").isEmpty());
    }

    @Test
    @DisplayName("DATE_FORMAT：时间键格式化成表名后缀，分片不可枚举")
    void dateFormatAlgorithm() {
        Properties p = base();
        p.setProperty("route.split.1.shard.key", "created_at");
        p.setProperty("route.split.1.algo", "DATE_FORMAT");
        p.setProperty("route.split.1.date.format", "yyyyMM");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertTrue(config.isValid(), () -> String.join("; ", config.getErrors()));
        TableRouter router = config.router(srcDb -> "dw");

        assertEquals("order_202608",
                router.route("app", "order", Timestamp.valueOf("2026-08-03 12:00:00")).get(0).getTable());
        assertEquals("order_202601",
                router.route("app", "order", "2026-01-15 08:30:00").get(0).getTable());
        assertEquals("order_202512",
                router.route("app", "order", java.time.LocalDate.of(2025, 12, 31)).get(0).getTable());

        // 不可枚举：不能预建全部目标表，也不能广播
        assertTrue(router.allTargets("app", "order").isEmpty());
        assertTrue(config.getWarnings().stream().anyMatch(w -> w.contains("DEADLETTER")));
        assertTrue(router.route("app", "order", "非时间值").isEmpty());
    }

    @Test
    @DisplayName("未路由默认广播到全部分片：不静默丢行")
    void unroutedBroadcastByDefault() {
        TableRouter router = router(hashMod16());
        List<RouteTarget> targets = router.route("app", "order", null);
        assertEquals(16, targets.size());
        Set<String> tables = new HashSet<>();
        targets.forEach(t -> tables.add(t.getTable()));
        assertEquals(16, tables.size());
        assertTrue(tables.contains("order_0"));
        assertTrue(tables.contains("order_15"));
    }

    @Test
    @DisplayName("未路由策略 ERROR：抛 UnroutedRowException 供上层 fail-stop")
    void unroutedError() {
        Properties p = hashMod16();
        p.setProperty("route.split.1.unrouted", "ERROR");
        TableRouter router = router(p);
        UnroutedRowException ex = assertThrows(UnroutedRowException.class,
                () -> router.route("app", "order", null));
        assertEquals("user_id", ex.getShardKey());
        assertEquals("order", ex.getSourceTable());
    }

    @Test
    @DisplayName("未命中规则的表原样直通（identity），保证同任务里其他表零回归")
    void unmatchedTableIsIdentity() {
        TableRouter router = router(hashMod16());
        assertFalse(router.matches("app", "user"));
        List<RouteTarget> targets = router.route("app", "user", null);
        assertEquals(1, targets.size());
        assertTrue(targets.get(0).isIdentity());
        assertEquals("user", targets.get(0).getTable());
    }

    @Test
    @DisplayName("跨实例：默认按连续块把分片划到各节点（8 库 × 16 表的常见排布）")
    void crossInstanceContiguousBlocks() {
        Properties p = hashMod16();
        p.setProperty("route.split.1.target.group", "g1");
        for (int i = 0; i < 4; i++) {
            p.setProperty("route.node.g1." + i + ".host", "10.0.0." + (i + 1));
            p.setProperty("route.node.g1." + i + ".port", "3306");
        }
        TableRouter router = router(p);
        // 16 片 4 实例 → 每 4 片一个实例
        assertEquals("g1#0", targetOfShard(router, 0).getNodeId());
        assertEquals("g1#0", targetOfShard(router, 3).getNodeId());
        assertEquals("g1#1", targetOfShard(router, 4).getNodeId());
        assertEquals("g1#3", targetOfShard(router, 15).getNodeId());
    }

    @Test
    @DisplayName("跨实例：显式 target.node 模板覆盖默认划分")
    void crossInstanceExplicitNodeTemplate() {
        Properties p = hashMod16();
        p.setProperty("route.split.1.target.group", "g1");
        p.setProperty("route.split.1.target.node", "${shard%2}");
        for (int i = 0; i < 2; i++) {
            p.setProperty("route.node.g1." + i + ".host", "10.0.0." + (i + 1));
            p.setProperty("route.node.g1." + i + ".port", "3306");
        }
        TableRouter router = router(p);
        assertEquals("g1#0", targetOfShard(router, 0).getNodeId());
        assertEquals("g1#1", targetOfShard(router, 1).getNodeId());
        assertEquals("g1#0", targetOfShard(router, 2).getNodeId());
    }

    @Test
    @DisplayName("allTargets 展开全部分片：预建目标表与 DDL 广播的输入")
    void allTargetsEnumeratesShards() {
        TableRouter router = router(hashMod16());
        List<RouteTarget> targets = router.allTargets("app", "order");
        assertEquals(16, targets.size());
        assertEquals("dw_0", targets.get(0).getDatabase());
        assertEquals("order_0", targets.get(0).getTable());
        assertEquals("dw_7", targets.get(15).getDatabase());
        assertEquals("order_15", targets.get(15).getTable());
    }

    @Test
    @DisplayName("库/表都不随分片变化 → 校验失败（这不是拆分）")
    void noShardVariationIsError() {
        Properties p = base();
        p.setProperty("route.split.1.algo", "HASH_MOD");
        p.setProperty("route.split.1.count", "4");
        p.setProperty("route.split.1.target.table", "order_all");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("不是拆分"));
    }

    @Test
    @DisplayName("DATE_FORMAT 分片配 ${shard/N} 运算 → 加载期就拒绝，而不是运行期才炸")
    void dateFormatWithArithmeticTemplateIsError() {
        Properties p = base();
        p.setProperty("route.split.1.shard.key", "created_at");
        p.setProperty("route.split.1.algo", "DATE_FORMAT");
        p.setProperty("route.split.1.date.format", "yyyyMM");
        p.setProperty("route.split.1.target.db", "dw_${shard/2}");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        RoutingConfig config = RoutingConfig.loadFromProperties(p);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("DATE_FORMAT"));
    }

    @Test
    @DisplayName("HASH_MOD 缺 count、模板渲染出非法标识符 → 校验失败")
    void invalidSplitParams() {
        Properties p = base();
        p.setProperty("route.split.1.algo", "HASH_MOD");
        p.setProperty("route.split.1.target.table", "order_${shard}");
        assertFalse(RoutingConfig.loadFromProperties(p).isValid());

        Properties q = hashMod16();
        q.setProperty("route.split.1.target.table", "order-${shard}");   // 连字符不是合法标识符
        RoutingConfig config = RoutingConfig.loadFromProperties(q);
        assertFalse(config.isValid());
        assertTrue(config.getErrors().get(0).contains("标识符"));
    }

    private static RouteTarget targetOfShard(TableRouter router, int shard) {
        for (RouteTarget t : router.allTargets("app", "order")) {
            if (t.getShardNo() == shard) {
                return t;
            }
        }
        throw new AssertionError("未找到分片 " + shard);
    }

    @Test
    @DisplayName("分片键值为空串：CRC32 仍能算出稳定分片（不算未路由）")
    void emptyStringIsRoutable() {
        TableRouter router = router(hashMod16());
        List<RouteTarget> targets = router.route("app", "order", "");
        assertEquals(1, targets.size());
        assertNotNull(targets.get(0).getTable());
    }
}
