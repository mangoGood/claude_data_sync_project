package com.migration.dialect;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypeTranslator} 库对分发 + 建表 DDL + 值转换矩阵测试。
 * forPair 的分发优先级与历史 SchemaMigration/DataMigration 分支保持一致，此处锁定该契约。
 */
@DisplayName("TypeTranslator 库对矩阵")
class TypeTranslatorTest {

    private static ColumnInfo col(String name, String dataType, boolean pk) {
        ColumnInfo c = new ColumnInfo();
        c.setColumnName(name);
        c.setDataType(dataType);
        c.setNullable(true);
        c.setPrimaryKey(pk);
        return c;
    }

    private static TableInfo table(String name, ColumnInfo... cols) {
        TableInfo t = new TableInfo();
        t.setTableName(name);
        for (ColumnInfo c : cols) {
            t.addColumn(c);
        }
        return t;
    }

    @Test
    @DisplayName("forPair 分发矩阵（与历史 if/else 优先级一致）")
    void forPairDispatch() {
        assertInstanceOf(MysqlToPgTranslator.class, TypeTranslator.forPair("mysql", "postgresql"));
        assertInstanceOf(PgToMysqlTranslator.class, TypeTranslator.forPair("postgresql", "mysql"));
        assertInstanceOf(OracleToPgTranslator.class, TypeTranslator.forPair("oracle", "postgresql"));
        assertInstanceOf(HomogeneousTranslator.class, TypeTranslator.forPair("mysql", "mysql"));
        assertInstanceOf(HomogeneousTranslator.class, TypeTranslator.forPair("postgresql", "postgresql"));
        // 历史行为：oracle→mysql 无专用转换路径，按同构处理（沿用源端 DDL）
        assertInstanceOf(HomogeneousTranslator.class, TypeTranslator.forPair("oracle", "mysql"));
    }

    @Test
    @DisplayName("同构翻译器：值原样透传，建表不支持（沿用源端 DDL）")
    void homogeneous() throws Exception {
        TypeTranslator t = TypeTranslator.forPair("mysql", "mysql");
        assertTrue(t.isHomogeneous());
        Object v = new Object();
        assertSame(v, t.convertValue(v, "int", null, 1));
        assertThrows(UnsupportedOperationException.class,
                () -> t.generateCreateTable(table("t"), SqlDialect.forType("mysql")));
    }

    @Test
    @DisplayName("MySQL→PG 建表：布尔/时长/无符号/位类型正确映射，主键子句齐全")
    void mysqlToPgCreateTable() {
        TypeTranslator t = TypeTranslator.forPair("mysql", "postgresql");
        assertFalse(t.isHomogeneous());
        String ddl = t.generateCreateTable(table("t1",
                        col("id", "int", true),
                        col("c_bool", "tinyint(1)", false),
                        col("c_time", "time", false),
                        col("c_bit", "bit(8)", false)),
                SqlDialect.forType("postgresql"));
        assertTrue(ddl.startsWith("CREATE TABLE \"t1\""));
        assertTrue(ddl.contains("\"c_bool\" BOOLEAN"));
        assertTrue(ddl.contains("\"c_time\" INTERVAL"), "MySQL TIME 必须映射为 INTERVAL（#1 回归）");
        assertTrue(ddl.contains("\"c_bit\" BYTEA"));
        assertTrue(ddl.contains("PRIMARY KEY (\"id\")"));
    }

    @Test
    @DisplayName("Oracle→PG 建表：表名/列名折叠小写，具名主键约束 pk_<table>")
    void oracleToPgCreateTable() {
        TypeTranslator t = TypeTranslator.forPair("oracle", "postgresql");
        String ddl = t.generateCreateTable(table("ALL_TYPES",
                        col("ID", "NUMBER", true),
                        col("C_VARCHAR2", "VARCHAR2", false)),
                SqlDialect.forType("postgresql"));
        assertTrue(ddl.startsWith("CREATE TABLE \"all_types\""));
        assertTrue(ddl.contains("\"id\""));
        assertTrue(ddl.contains("\"c_varchar2\""));
        assertTrue(ddl.contains("CONSTRAINT \"pk_all_types\" PRIMARY KEY (\"id\")"));
    }

    @Test
    @DisplayName("PG→MySQL 建表：反引号 + InnoDB 后缀")
    void pgToMysqlCreateTable() {
        TypeTranslator t = TypeTranslator.forPair("postgresql", "mysql");
        String ddl = t.generateCreateTable(table("t1", col("id", "integer", true)),
                SqlDialect.forType("mysql"));
        assertTrue(ddl.startsWith("CREATE TABLE `t1`"));
        assertTrue(ddl.endsWith("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"));
    }

    @Test
    @DisplayName("MySQL→PG 值转换：tinyint(1)→Boolean，json→String，datetime→Timestamp，year→int")
    void mysqlToPgValues() throws Exception {
        TypeTranslator t = TypeTranslator.forPair("mysql", "postgresql");
        assertEquals(Boolean.TRUE, t.convertValue(1, "tinyint(1)", null, 1));
        assertEquals(Boolean.FALSE, t.convertValue(0, "tinyint(1)", null, 1));
        assertEquals("{\"a\":1}", t.convertValue("{\"a\":1}", "json", null, 1));
        Date d = new Date(1000000L);
        assertEquals(new Timestamp(1000000L), t.convertValue(d, "datetime", null, 1));
        assertEquals(2024, t.convertValue(java.sql.Date.valueOf("2024-06-15"), "year", null, 1));
        // NULL 与未知类型透传
        assertEquals(null, t.convertValue(null, "int", null, 1));
        assertEquals(42, t.convertValue(42, "int", null, 1));
    }

    @Test
    @DisplayName("PG→MySQL 值转换：boolean→0/1，uuid/interval→字符串")
    void pgToMysqlValues() throws Exception {
        TypeTranslator t = TypeTranslator.forPair("postgresql", "mysql");
        assertEquals(1, t.convertValue(Boolean.TRUE, "boolean", null, 1));
        assertEquals(0, t.convertValue(Boolean.FALSE, "bool", null, 1));
        assertEquals(1, t.convertValue("t", "boolean", null, 1));
        assertEquals(0, t.convertValue("f", "boolean", null, 1));
        assertEquals("550e8400-e29b-41d4-a716-446655440000",
                t.convertValue(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), "uuid", null, 1));
        assertEquals("1 day", t.convertValue("1 day", "interval", null, 1));
    }

    @Test
    @DisplayName("Oracle→PG 值转换：util.Date→Timestamp，NUMBER/TSTZ 字符串透传")
    void oracleToPgValues() throws Exception {
        TypeTranslator t = TypeTranslator.forPair("oracle", "postgresql");
        Date d = new Date(5000000L);
        assertEquals(new Timestamp(5000000L), t.convertValue(d, "TIMESTAMP(6)", null, 1));
        java.math.BigDecimal n = new java.math.BigDecimal("99999999999999999999999999999999999999");
        assertSame(n, t.convertValue(n, "NUMBER", null, 1));
        // TIMESTAMP WITH TIME ZONE 已在读取端转为 String，此处透传
        assertEquals("2024-06-15 12:30:45.123 +08:00",
                t.convertValue("2024-06-15 12:30:45.123 +08:00", "TIMESTAMP(6) WITH TIME ZONE", null, 1));
    }

    @Test
    @DisplayName("convertLiteral mysql→pg：tinyint(1)→true/false，bit(8)→bytea 十六进制字面量")
    void mysqlToPgLiteral() {
        TypeTranslator t = TypeTranslator.forPair("mysql", "postgresql");
        assertEquals("true", t.convertLiteral("1", "tinyint(1)"));
        assertEquals("false", t.convertLiteral("0", "tinyint(1)"));
        assertEquals("E'\\\\xaa'", t.convertLiteral("0xaa", "bit(8)"));
        assertEquals("E'\\\\x0f'", t.convertLiteral("b'00001111'", "bit(8)"));
        // 非布尔/位类型原样返回
        assertEquals("'alice'", t.convertLiteral("'alice'", "varchar(50)"));
    }

    @Test
    @DisplayName("convertLiteral pg→mysql：boolean→1/0，bytea E'\\x..'→0x..，去 ::type 后缀")
    void pgToMysqlLiteral() {
        TypeTranslator t = TypeTranslator.forPair("postgresql", "mysql");
        assertEquals("1", t.convertLiteral("t", "boolean"));
        assertEquals("0", t.convertLiteral("f", "boolean"));
        assertEquals("0xdeadbeef", t.convertLiteral("E'\\xdeadbeef'", "bytea"));
        assertEquals("'{\"k\":1}'", t.convertLiteral("'{\"k\":1}'::jsonb", "jsonb"));
        assertEquals("NULL", t.convertLiteral("NULL", "boolean"));
    }

    @Test
    @DisplayName("convertLiteral 同构：默认原样返回（无转换）")
    void homogeneousLiteralNoop() {
        TypeTranslator t = TypeTranslator.forPair("mysql", "mysql");
        assertEquals("0xaa", t.convertLiteral("0xaa", "bit(8)"));
        assertEquals("1", t.convertLiteral("1", "tinyint(1)"));
    }
}
