package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TablePattern} 与 {@link ShardTemplate} 单元测试：通配/正则匹配、大小写口径、
 * 模板占位符渲染与非法写法的拒绝。
 */
@DisplayName("TablePattern / ShardTemplate 匹配与模板")
class TablePatternAndTemplateTest {

    @Test
    @DisplayName("通配：* 与 ? 分别匹配任意串与单字符，库表以第一个点分隔")
    void wildcardMatching() {
        TablePattern p = TablePattern.compile("shard_db_*.order_???");
        assertTrue(p.matches("shard_db_1", "order_001"));
        assertTrue(p.matches("shard_db_128", "order_128"));
        assertFalse(p.matches("shard_db_1", "order_1"));      // ??? 要求三位
        assertFalse(p.matches("other_db", "order_001"));
        assertFalse(p.matches("shard_db_1", "user"));
    }

    @Test
    @DisplayName("通配：点号是字面量，不当通配符（db.t 不该匹配 dbXt）")
    void dotIsLiteral() {
        TablePattern p = TablePattern.compile("db1.t1");
        assertTrue(p.matches("db1", "t1"));
        assertFalse(p.matches("db1x", "t1"));
        assertFalse(p.matches("db1", "t1x"));
    }

    @Test
    @DisplayName("正则：regex: 前缀整体匹配 库.表")
    void regexMatching() {
        TablePattern p = TablePattern.compile("regex:shard_db_\\d+\\.order_\\d{3}");
        assertTrue(p.matches("shard_db_7", "order_042"));
        assertFalse(p.matches("shard_db_x", "order_042"));
        assertFalse(p.matches("shard_db_7", "order_42"));
    }

    @Test
    @DisplayName("大小写：精确匹配与忽略大小写匹配分开暴露（精确优先由调用方保证）")
    void caseSensitivity() {
        TablePattern p = TablePattern.compile("db1.t1");
        assertFalse(p.matches("DB1", "T1"));
        assertTrue(p.matchesIgnoreCase("DB1", "T1"));
    }

    @Test
    @DisplayName("非法匹配式：空串、缺库表分隔点、正则语法错误")
    void invalidPatterns() {
        assertThrows(IllegalArgumentException.class, () -> TablePattern.compile(""));
        assertThrows(IllegalArgumentException.class, () -> TablePattern.compile("order_all"));
        assertThrows(IllegalArgumentException.class, () -> TablePattern.compile("regex:[unclosed"));
    }

    @Test
    @DisplayName("模板：${shard} / ${shard/N} / ${shard%N} 渲染")
    void templateRendering() {
        assertEquals("order_5", ShardTemplate.compile("order_${shard}").render(ShardKey.ofIndex(5)));
        assertEquals("dw_2", ShardTemplate.compile("dw_${shard/2}").render(ShardKey.ofIndex(5)));
        assertEquals("dw_1", ShardTemplate.compile("dw_${shard%2}").render(ShardKey.ofIndex(5)));
        assertEquals("a0b", ShardTemplate.compile("a${shard}b").render(ShardKey.ofIndex(0)));
        // 时间分片：token 直接substitution
        assertEquals("order_202608",
                ShardTemplate.compile("order_${shard}").render(ShardKey.ofToken("202608")));
    }

    @Test
    @DisplayName("模板：纯字面量合法但不含占位（用于分库不分表）")
    void literalTemplate() {
        ShardTemplate t = ShardTemplate.compile("order_all");
        assertFalse(t.hasPlaceholder());
        assertFalse(t.requiresNumericShard());
        assertEquals("order_all", t.render(ShardKey.ofIndex(3)));
    }

    @Test
    @DisplayName("模板：带运算的占位要求数值分片号，非数值分片渲染即报错")
    void arithmeticRequiresNumericShard() {
        ShardTemplate t = ShardTemplate.compile("dw_${shard/2}");
        assertTrue(t.requiresNumericShard());
        assertThrows(IllegalStateException.class, () -> t.render(ShardKey.ofToken("202608")));
    }

    @Test
    @DisplayName("模板：非法写法一律拒绝（未闭合、未知变量、非正整数运算数）")
    void invalidTemplates() {
        assertThrows(IllegalArgumentException.class, () -> ShardTemplate.compile("order_${shard"));
        assertThrows(IllegalArgumentException.class, () -> ShardTemplate.compile("order_${idx}"));
        assertThrows(IllegalArgumentException.class, () -> ShardTemplate.compile("order_${shard/0}"));
        assertThrows(IllegalArgumentException.class, () -> ShardTemplate.compile("order_${shard/-1}"));
        assertThrows(IllegalArgumentException.class, () -> ShardTemplate.compile("order_${shard+1}"));
        assertThrows(IllegalArgumentException.class, () -> ShardTemplate.compile(""));
    }
}
