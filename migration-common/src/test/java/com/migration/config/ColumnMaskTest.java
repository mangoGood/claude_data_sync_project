package com.migration.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 列脱敏判据。
 *
 * <p>最要紧的性质不是"脱得像不像"，而是<b>确定性</b>——同一输入恒产出同一输出。
 * 增量重放、断点续传、以及全量与增量对同一行的两次处理都依赖它；
 * 一旦带随机性，幂等 upsert 会把每次重放都看成一次真实变更，
 * 目标端与源端将<b>永远对不齐</b>，而且不报任何错。
 */
class ColumnMaskTest {

    private static ColumnProcessingConfig cfg(String rules) {
        Properties p = new Properties();
        p.setProperty("column.mask.db1.t1", rules);
        p.setProperty("column.mask.salt", "test-salt");
        return ColumnProcessingConfig.loadFromProperties(p);
    }

    // ---------------- 确定性 ----------------

    @Test
    @DisplayName("所有规则都是确定性的——同值恒同结果")
    void deterministic() {
        ColumnProcessingConfig c = cfg(
                "a:MASK_ALL;b:MASK_PARTIAL:3:4;c:HASH;d:FAKE:NAME;e:FAKE:EMAIL;f:FAKE:PHONE");
        for (String col : new String[]{"a", "b", "c", "d", "e", "f"}) {
            Object first = c.maskValue("db1", "t1", col, "13800138000");
            for (int i = 0; i < 20; i++) {
                assertEquals(first, c.maskValue("db1", "t1", col, "13800138000"),
                        col + " 的脱敏结果必须可重复——不可重复会让幂等重放永远对不齐");
            }
        }
    }

    @Test
    @DisplayName("跨实例也确定：同盐同规则，另建一份配置得到同一结果")
    void deterministicAcrossInstances() {
        String rules = "c:HASH;d:FAKE:NAME";
        assertEquals(cfg(rules).maskValue("db1", "t1", "c", "alice@example.com"),
                cfg(rules).maskValue("db1", "t1", "c", "alice@example.com"));
        assertEquals(cfg(rules).maskValue("db1", "t1", "d", "张三"),
                cfg(rules).maskValue("db1", "t1", "d", "张三"));
    }

    @Test
    @DisplayName("HASH 保留可连接性：同值同结果、异值异结果")
    void hashJoinable() {
        ColumnProcessingConfig c = cfg("c:HASH");
        Object a1 = c.maskValue("db1", "t1", "c", "u-1001");
        Object a2 = c.maskValue("db1", "t1", "c", "u-1001");
        Object b = c.maskValue("db1", "t1", "c", "u-1002");
        assertEquals(a1, a2, "同值必须同结果，否则跨表 JOIN 断掉");
        assertNotEquals(a1, b);
    }

    @Test
    @DisplayName("不同盐派生不同结果——盐确实参与了")
    void saltMatters() {
        Properties p1 = new Properties();
        p1.setProperty("column.mask.db1.t1", "c:HASH");
        p1.setProperty("column.mask.salt", "salt-A");
        Properties p2 = new Properties();
        p2.setProperty("column.mask.db1.t1", "c:HASH");
        p2.setProperty("column.mask.salt", "salt-B");
        assertNotEquals(
                ColumnProcessingConfig.loadFromProperties(p1).maskValue("db1", "t1", "c", "x"),
                ColumnProcessingConfig.loadFromProperties(p2).maskValue("db1", "t1", "c", "x"));
    }

    // ---------------- 各规则语义 ----------------

    @Test
    @DisplayName("MASK_PARTIAL 保留首尾")
    void maskPartial() {
        ColumnProcessingConfig c = cfg("phone:MASK_PARTIAL:3:4");
        assertEquals("138******8000", c.maskValue("db1", "t1", "phone", "13800138000"));
    }

    @Test
    @DisplayName("MASK_PARTIAL 中间用固定长度——按原长度填会泄露原值长度")
    void maskPartialHidesLength() {
        ColumnProcessingConfig c = cfg("x:MASK_PARTIAL:1:1");
        String shortV = String.valueOf(c.maskValue("db1", "t1", "x", "a" + "0".repeat(5) + "z"));
        String longV = String.valueOf(c.maskValue("db1", "t1", "x", "a" + "0".repeat(50) + "z"));
        assertEquals(shortV.length(), longV.length(),
                "遮蔽段长度不应随原值长度变化——那对定长字段等于泄露格式");
    }

    @Test
    @DisplayName("保留位数 ≥ 原值长度时整值遮蔽，绝不退化成原样返回")
    void maskPartialNeverLeaksShortValues() {
        ColumnProcessingConfig c = cfg("x:MASK_PARTIAL:3:4");
        assertEquals("******", c.maskValue("db1", "t1", "x", "123"));
        assertEquals("******", c.maskValue("db1", "t1", "x", "1234567"));
        assertNotEquals("123", c.maskValue("db1", "t1", "x", "123"));
    }

    @Test
    @DisplayName("MASK_ALL / NULLIFY")
    void maskAllAndNullify() {
        assertEquals("******", cfg("x:MASK_ALL").maskValue("db1", "t1", "x", "secret"));
        assertEquals("###", cfg("x:MASK_ALL:###").maskValue("db1", "t1", "x", "secret"));
        assertNull(cfg("x:NULLIFY").maskValue("db1", "t1", "x", "secret"));
    }

    @Test
    @DisplayName("FAKE 产出同型假数据")
    void fakeShapes() {
        assertTrue(String.valueOf(cfg("x:FAKE:EMAIL").maskValue("db1", "t1", "x", "a@b.c"))
                .matches("user\\d+@example\\.com"));
        assertTrue(String.valueOf(cfg("x:FAKE:PHONE").maskValue("db1", "t1", "x", "13800138000"))
                .matches("138\\d{8}"));
        assertEquals(18, String.valueOf(
                cfg("x:FAKE:ID_CARD").maskValue("db1", "t1", "x", "110101199001011234")).length());
    }

    @Test
    @DisplayName("FAKE 类型不认识时退化为 HASH，绝不原样返回")
    void unknownFakeTypeFallsBackToHash() {
        Object v = cfg("x:FAKE:UNKNOWN_TYPE").maskValue("db1", "t1", "x", "secret");
        assertNotEquals("secret", v, "\"不认识的类型\"绝不能等于\"不脱敏\"");
        assertEquals(32, String.valueOf(v).length(), "应退化为哈希");
    }

    // ---------------- 边界 ----------------

    @Test
    @DisplayName("null 保持 null——脱敏不该把\"没有值\"变成\"有一个假值\"")
    void nullStaysNull() {
        assertNull(cfg("x:MASK_ALL").maskValue("db1", "t1", "x", null));
        assertNull(cfg("x:FAKE:NAME").maskValue("db1", "t1", "x", null));
        assertNull(cfg("x:HASH").maskValue("db1", "t1", "x", null));
    }

    @Test
    @DisplayName("没配规则的列原样返回")
    void unmaskedColumnUntouched() {
        ColumnProcessingConfig c = cfg("a:MASK_ALL");
        assertEquals("keep-me", c.maskValue("db1", "t1", "b", "keep-me"));
        assertEquals("keep-me", c.maskValue("db1", "other_table", "a", "keep-me"));
        assertEquals("keep-me", c.maskValue("other_db", "t1", "a", "keep-me"));
    }

    @Test
    @DisplayName("列名大小写不敏感（与其它列处理一致）")
    void columnNameCaseInsensitive() {
        ColumnProcessingConfig c = cfg("Phone:MASK_ALL");
        assertEquals("******", c.maskValue("db1", "t1", "phone", "x"));
        assertEquals("******", c.maskValue("db1", "t1", "PHONE", "x"));
    }

    @Test
    @DisplayName("非法规则名被跳过（控制面已强校验，这里只是不炸）")
    void invalidRuleSkipped() {
        ColumnProcessingConfig c = cfg("x:NOT_A_RULE;y:HASH");
        assertEquals("raw", c.maskValue("db1", "t1", "x", "raw"));
        assertNotEquals("raw", c.maskValue("db1", "t1", "y", "raw"));
    }

    // ---------------- 内容对比要用的查询接口 ----------------

    @Test
    @DisplayName("maskedColumns 供内容对比排除；未配脱敏时 hasMask=false")
    void exposesMaskedColumnsForCompare() {
        ColumnProcessingConfig c = cfg("phone:MASK_ALL;email:HASH");
        assertTrue(c.hasMask("db1", "t1"));
        assertEquals(java.util.Set.of("phone", "email"), c.maskedColumns("db1", "t1"));
        assertFalse(c.hasMask("db1", "t2"));
        assertTrue(c.maskedColumns("db1", "t2").isEmpty());
    }

    @Test
    @DisplayName("没有任何脱敏配置时零影响")
    void noMaskConfigIsNoop() {
        ColumnProcessingConfig c = ColumnProcessingConfig.loadFromProperties(new Properties());
        assertFalse(c.hasMask("db1", "t1"));
        assertEquals("raw", c.maskValue("db1", "t1", "any", "raw"));
    }
}
