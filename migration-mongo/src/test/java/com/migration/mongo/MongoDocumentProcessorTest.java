package com.migration.mongo;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MongoDocumentProcessor} 列处理（列过滤/列名映射/附加列）单元测试。
 *
 * <p>覆盖：过滤排除语义（数值/Decimal128/日期/布尔/边界比较）、列名映射（含 _id 不改名、保序）、
 * 附加列注值（create/update/custom）、组合变换、无配置直通、索引 key 改写。
 */
@DisplayName("Mongo 列处理测试")
class MongoDocumentProcessorTest {

    private static MongoDocumentProcessor processor(Properties props) {
        return MongoDocumentProcessor.fromProperties(props);
    }

    private static Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    private static Date date(int y, int mo, int d) {
        return Date.from(LocalDateTime.of(y, mo, d, 0, 0).atZone(ZoneId.systemDefault()).toInstant());
    }

    @Nested
    @DisplayName("无配置直通")
    class NoConfig {
        @Test
        @DisplayName("无任何列处理配置：isEmpty/非 active，transform 原样返回，excluded 恒 false")
        void passthrough() {
            MongoDocumentProcessor p = processor(new Properties());
            Document doc = new Document("_id", 1).append("amount", 50);
            assertTrue(p.isEmpty());
            assertFalse(p.isActive("db", "orders"));
            assertSame(doc, p.transform("db", "orders", doc));
            assertFalse(p.excluded("db", "orders", doc));
        }

        @Test
        @DisplayName("配置了别的集合时，本集合仍直通")
        void otherCollectionOnly() {
            MongoDocumentProcessor p = processor(props("column.filter.db.other", "amount|<|100"));
            assertFalse(p.isActive("db", "orders"));
            assertTrue(p.isActive("db", "other"));
        }
    }

    @Nested
    @DisplayName("列过滤（命中即排除）")
    class Filter {
        @Test
        @DisplayName("数值 < 阈值命中排除；>= 保留；NULL 保留")
        void numeric() {
            MongoDocumentProcessor p = processor(props("column.filter.db.orders", "amount|<|100"));
            assertTrue(p.excluded("db", "orders", new Document("_id", 1).append("amount", 50)));
            assertFalse(p.excluded("db", "orders", new Document("_id", 2).append("amount", 100)));
            assertFalse(p.excluded("db", "orders", new Document("_id", 3).append("amount", 150)));
            // 过滤列缺失或为 null → 保留（宁可多同步不可丢）
            assertFalse(p.excluded("db", "orders", new Document("_id", 4).append("amount", null)));
            assertFalse(p.excluded("db", "orders", new Document("_id", 5)));
        }

        @Test
        @DisplayName("Decimal128 折算为 BigDecimal 后按数值比较")
        void decimal128() {
            MongoDocumentProcessor p = processor(props("column.filter.db.orders", "price|<|100"));
            assertTrue(p.excluded("db", "orders",
                    new Document("_id", 1).append("price", new Decimal128(new BigDecimal("99.99")))));
            assertFalse(p.excluded("db", "orders",
                    new Document("_id", 2).append("price", new Decimal128(new BigDecimal("100.00")))));
        }

        @Test
        @DisplayName("Long/Double 数值类型同样比较")
        void longAndDouble() {
            MongoDocumentProcessor p = processor(props("column.filter.db.orders", "qty|>=|1000"));
            assertTrue(p.excluded("db", "orders", new Document("_id", 1).append("qty", 1000L)));
            assertTrue(p.excluded("db", "orders", new Document("_id", 2).append("qty", 2500.5d)));
            assertFalse(p.excluded("db", "orders", new Document("_id", 3).append("qty", 999L)));
        }

        @Test
        @DisplayName("日期比较：早于阈值命中排除")
        void dateFilter() {
            MongoDocumentProcessor p = processor(props("column.filter.db.orders", "ts|<|2020-01-01 00:00:00"));
            assertTrue(p.excluded("db", "orders", new Document("_id", 1).append("ts", date(2019, 6, 1))));
            assertFalse(p.excluded("db", "orders", new Document("_id", 2).append("ts", date(2021, 6, 1))));
        }

        @Test
        @DisplayName("布尔按 0/1 折算比较（bit 语义）")
        void booleanFilter() {
            MongoDocumentProcessor p = processor(props("column.filter.db.orders", "flag|=|1"));
            assertTrue(p.excluded("db", "orders", new Document("_id", 1).append("flag", true)));
            assertFalse(p.excluded("db", "orders", new Document("_id", 2).append("flag", false)));
        }

        @Test
        @DisplayName("边界比较符 <= >= = != 各自语义")
        void operators() {
            assertTrue(processor(props("column.filter.db.o", "a|<=|10"))
                    .excluded("db", "o", new Document("a", 10)));
            assertTrue(processor(props("column.filter.db.o", "a|>=|10"))
                    .excluded("db", "o", new Document("a", 10)));
            assertTrue(processor(props("column.filter.db.o", "a|=|10"))
                    .excluded("db", "o", new Document("a", 10)));
            assertFalse(processor(props("column.filter.db.o", "a|!=|10"))
                    .excluded("db", "o", new Document("a", 10)));
            assertTrue(processor(props("column.filter.db.o", "a|!=|10"))
                    .excluded("db", "o", new Document("a", 11)));
        }

        @Test
        @DisplayName("多条件任一命中即排除；列名大小写不敏感")
        void multiCondCaseInsensitive() {
            // 过滤列限定数值/日期/bit 类型（与 mysql/pg 一致，UI 层约束）；此处用两个数值条件
            MongoDocumentProcessor p = processor(props("column.filter.db.orders", "amount|<|100;qty|>|1000"));
            // 命中条件二（列名大小写不敏感 QTY→qty）
            assertTrue(p.excluded("db", "orders", new Document("Amount", 200).append("QTY", 2000)));
            // 命中条件一
            assertTrue(p.excluded("db", "orders", new Document("amount", 5).append("qty", 10)));
            // 两条都不命中
            assertFalse(p.excluded("db", "orders", new Document("amount", 200).append("qty", 10)));
        }
    }

    @Nested
    @DisplayName("列名映射")
    class Mapping {
        @Test
        @DisplayName("重命名字段，保序，其余字段与值不变")
        void rename() {
            MongoDocumentProcessor p = processor(props("column.mapping.db.orders", "note:remark,qty:quantity"));
            Document in = new Document("_id", 1).append("note", "hi").append("qty", 5).append("other", "x");
            Document out = p.transform("db", "orders", in);
            assertEquals(new ArrayList<>(java.util.Arrays.asList("_id", "remark", "quantity", "other")),
                    new ArrayList<>(out.keySet()));
            assertEquals("hi", out.get("remark"));
            assertEquals(5, out.get("quantity"));
            assertEquals("x", out.get("other"));
            assertEquals(1, out.get("_id"));
        }

        @Test
        @DisplayName("_id 永不改名，即使配置了 _id 映射")
        void idNeverRenamed() {
            MongoDocumentProcessor p = processor(props("column.mapping.db.orders", "_id:foo"));
            Document out = p.transform("db", "orders", new Document("_id", 42).append("a", 1));
            assertEquals(42, out.get("_id"));
            assertNull(out.get("foo"));
        }

        @Test
        @DisplayName("mapField 供索引 key 改写；_id 与未映射字段原样")
        void mapField() {
            MongoDocumentProcessor p = processor(props("column.mapping.db.orders", "note:remark"));
            assertEquals("remark", p.mapField("db", "orders", "note"));
            assertEquals("other", p.mapField("db", "orders", "other"));
            assertEquals("_id", p.mapField("db", "orders", "_id"));
        }
    }

    @Nested
    @DisplayName("附加列")
    class Extra {
        @Test
        @DisplayName("CREATE_TIME/UPDATE_TIME 注入当前时间；CUSTOM 注入 值@库@集合")
        void inject() {
            MongoDocumentProcessor p = processor(props(
                    "column.extra.db.orders", "ct:CREATE_TIME,ut:UPDATE_TIME,src:CUSTOM:prod"));
            Document out = p.transform("db", "orders", new Document("_id", 1).append("a", 1));
            assertTrue(out.get("ct") instanceof Date);
            assertTrue(out.get("ut") instanceof Date);
            assertEquals("prod@db@orders", out.get("src"));
            // 原字段保留
            assertEquals(1, out.get("a"));
            assertEquals(1, out.get("_id"));
        }
    }

    @Nested
    @DisplayName("组合变换（过滤 + 映射 + 附加列）")
    class Combined {
        @Test
        @DisplayName("过滤独立判定；transform 同时改名并追加附加列")
        void all() {
            MongoDocumentProcessor p = processor(props(
                    "column.filter.db.orders", "amount|<|100",
                    "column.mapping.db.orders", "note:remark",
                    "column.extra.db.orders", "src:CUSTOM:prod"));

            assertTrue(p.isActive("db", "orders"));
            // 命中过滤
            assertTrue(p.excluded("db", "orders", new Document("_id", 1).append("amount", 5).append("note", "n")));
            // 未命中 → transform 改名 + 附加列
            Document out = p.transform("db", "orders",
                    new Document("_id", 2).append("amount", 200).append("note", "hi"));
            assertEquals("hi", out.get("remark"));
            assertNull(out.get("note"));
            assertEquals(200, out.get("amount"));
            assertEquals("prod@db@orders", out.get("src"));
        }
    }
}
