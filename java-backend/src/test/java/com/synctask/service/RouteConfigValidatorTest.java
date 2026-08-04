package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RouteConfigValidator} 单元测试：结构校验挡住"UI 能填错的那些"，
 * 让用户在点保存时就看到错，而不是任务起不来再回来翻日志。
 */
@DisplayName("RouteConfigValidator 路由配置校验")
class RouteConfigValidatorTest {

    @Test
    @DisplayName("空配置 / NONE：返回 null（清除路由，回到 1:1）")
    void emptyMeansNoRouting() {
        assertNull(RouteConfigValidator.validate(null));
        assertNull(RouteConfigValidator.validate(""));
        assertNull(RouteConfigValidator.validate("{\"mode\":\"NONE\"}"));
    }

    @Test
    @DisplayName("合法的汇聚配置通过")
    void validMergeConfig() {
        String json = "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"shard_db_*.order_*\","
                + "\"target\":\"dw.order_all\",\"pkStrategy\":\"COMPOSITE_SOURCE\",\"ddlPolicy\":\"FIRST_WINS\"}]}";
        assertNotNull(RouteConfigValidator.validate(json));
    }

    @Test
    @DisplayName("合法的拆分配置通过")
    void validSplitConfig() {
        String json = "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"user_id\","
                + "\"algo\":\"HASH_MOD\",\"count\":16,\"targetDb\":\"dw_${shard/4}\","
                + "\"targetTable\":\"orders_${shard}\",\"unrouted\":\"BROADCAST\"}]}";
        assertNotNull(RouteConfigValidator.validate(json));
    }

    @Test
    @DisplayName("非法 JSON / 未知模式：报错")
    void invalidShape() {
        assertThrows(IllegalArgumentException.class, () -> RouteConfigValidator.validate("{not json"));
        assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"SHARDING\"}"));
    }

    @Test
    @DisplayName("汇聚：缺规则 / 缺目标表 / 目标表名非法")
    void mergeRuleErrors() {
        assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"MERGE\",\"merge\":[]}"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"MERGE\",\"merge\":"
                        + "[{\"match\":\"db.*\"}]}"));
        assertTrue(e.getMessage().contains("目标表"), e.getMessage());

        e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"MERGE\",\"merge\":"
                        + "[{\"match\":\"db.t\",\"target\":\"dw.order-all\"}]}"));
        assertTrue(e.getMessage().contains("标识符"), e.getMessage());
    }

    @Test
    @DisplayName("拆分：模板不含 ${shard} 就等于没拆分，必须报错")
    void splitWithoutShardTemplateIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\","
                        + "\"shardKey\":\"user_id\",\"algo\":\"HASH_MOD\",\"count\":4,"
                        + "\"targetTable\":\"orders_all\"}]}"));
        assertTrue(e.getMessage().contains("同一张表"), e.getMessage());
    }

    @Test
    @DisplayName("拆分：哈希取模缺分片数 / RANGE 缺区间表 / DATE_FORMAT 暂不支持")
    void splitParamErrors() {
        assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\","
                        + "\"shardKey\":\"user_id\",\"algo\":\"HASH_MOD\",\"targetTable\":\"orders_${shard}\"}]}"));
        assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\","
                        + "\"shardKey\":\"user_id\",\"algo\":\"RANGE\",\"targetTable\":\"orders_${shard}\"}]}"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\","
                        + "\"shardKey\":\"ct\",\"algo\":\"DATE_FORMAT\",\"dateFormat\":\"yyyyMM\","
                        + "\"targetTable\":\"orders_${shard}\"}]}"));
        assertTrue(e.getMessage().contains("DATE_FORMAT"), e.getMessage());
    }

    @Test
    @DisplayName("跨实例来源：实例标识重复要拦下（来源列靠它区分同名库表）")
    void duplicateLegNodeIdRejected() {
        String json = "{\"mode\":\"MERGE\",\"merge\":[{\"match\":\"db.*\",\"target\":\"dw.t\"}],"
                + "\"legs\":[{\"nodeId\":\"a\",\"host\":\"h1\",\"port\":3306},"
                + "{\"nodeId\":\"a\",\"host\":\"h2\",\"port\":3306}]}";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate(json));
        assertTrue(e.getMessage().contains("重复"), e.getMessage());
    }

    @Test
    @DisplayName("跨实例来源只对汇聚有意义：拆分带 legs 报错")
    void legsOnlyForMerge() {
        String json = "{\"mode\":\"SPLIT\",\"split\":[{\"match\":\"app.orders\",\"shardKey\":\"user_id\","
                + "\"algo\":\"HASH_MOD\",\"count\":4,\"targetTable\":\"orders_${shard}\"}],"
                + "\"legs\":[{\"nodeId\":\"a\",\"host\":\"h1\",\"port\":3306}]}";
        assertThrows(IllegalArgumentException.class, () -> RouteConfigValidator.validate(json));
    }

    @Test
    @DisplayName("多个错误一次性报出来，不用改一个试一次")
    void reportsAllErrorsAtOnce() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RouteConfigValidator.validate("{\"mode\":\"MERGE\",\"merge\":[{}]}"));
        assertEquals(2, e.getMessage().split("；").length, e.getMessage());
    }
}
