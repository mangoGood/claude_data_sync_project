package com.migration.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypeMapper} 跨库类型映射矩阵测试。
 *
 * <p>锁定全类型/边界值测试中发现并修复的映射 bug：
 * <ul>
 *   <li>#1 MySQL TIME(±838:59:59 时长语义) → PG INTERVAL（PG time 上限 24h）</li>
 *   <li>#4 无精度 Oracle NUMBER（JDBC 报告精度 22 哨兵值）→ 无精度 NUMERIC，防 38 位大数溢出</li>
 *   <li>tinyint(1)→BOOLEAN、bit(1)→BOOLEAN、bit(n>1)→BYTEA、BIGINT UNSIGNED→NUMERIC(20,0)</li>
 * </ul>
 */
@DisplayName("TypeMapper 类型映射矩阵")
class TypeMapperTest {

    private static ColumnInfo col(String dataType, int size, int digits, boolean nullable) {
        ColumnInfo c = new ColumnInfo();
        c.setColumnName("c");
        c.setDataType(dataType);
        c.setColumnSize(size);
        c.setDecimalDigits(digits);
        c.setNullable(nullable);
        return c;
    }

    @Nested
    @DisplayName("MySQL → PostgreSQL 列定义")
    class MysqlToPg {

        @Test
        @DisplayName("#1 回归：TIME → INTERVAL（而非 PG time，后者装不下 ±838:59:59）")
        void timeMapsToInterval() {
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("time", 0, 0, true)).startsWith("INTERVAL"));
            assertEquals("INTERVAL", TypeMapper.mapMysqlToPg("time"));
        }

        @Test
        @DisplayName("布尔族：tinyint(1)→BOOLEAN，tinyint→SMALLINT，bit(1)→BOOLEAN，bit(8)→BYTEA")
        void booleanFamily() {
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("tinyint(1)", 0, 0, true)).startsWith("BOOLEAN"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("tinyint", 0, 0, true)).startsWith("SMALLINT"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("bit(1)", 0, 0, true)).startsWith("BOOLEAN"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("bit(8)", 0, 0, true)).startsWith("BYTEA"));
        }

        @Test
        @DisplayName("无符号整数升位：int unsigned→BIGINT，bigint unsigned→NUMERIC(20,0)")
        void unsignedWidening() {
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("int unsigned", 0, 0, true)).startsWith("BIGINT"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("bigint unsigned", 0, 0, true)).startsWith("NUMERIC(20,0)"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("bigint", 0, 0, true)).startsWith("BIGINT"));
        }

        @Test
        @DisplayName("精度保留：decimal(38,10)、varchar(255)、char(10)")
        void precisionPreserved() {
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("decimal(38,10)", 0, 0, true)).startsWith("NUMERIC(38,10)"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("varchar(255)", 0, 0, true)).startsWith("VARCHAR(255)"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("char(10)", 0, 0, true)).startsWith("CHAR(10)"));
        }

        @Test
        @DisplayName("时间/JSON/枚举：datetime→TIMESTAMP，json→JSONB，enum/set→VARCHAR(255)，year→SMALLINT")
        void temporalAndMisc() {
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("datetime", 0, 0, true)).startsWith("TIMESTAMP"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("timestamp", 0, 0, true)).startsWith("TIMESTAMP"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("json", 0, 0, true)).startsWith("JSONB"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("enum('a','b')", 0, 0, true)).startsWith("VARCHAR(255)"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("year", 0, 0, true)).startsWith("SMALLINT"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("longblob", 0, 0, true)).startsWith("BYTEA"));
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("longtext", 0, 0, true)).startsWith("TEXT"));
        }

        @Test
        @DisplayName("NOT NULL 与 CURRENT_TIMESTAMP 默认值透传")
        void constraints() {
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(col("int", 0, 0, false)).contains("NOT NULL"));
            ColumnInfo ts = col("timestamp", 0, 0, true);
            ts.setDefaultValue("CURRENT_TIMESTAMP");
            assertTrue(TypeMapper.mapMysqlToPgColumnDef(ts).contains("DEFAULT CURRENT_TIMESTAMP"));
        }
    }

    @Nested
    @DisplayName("Oracle → PostgreSQL 列定义")
    class OracleToPg {

        @Test
        @DisplayName("#4 回归：无精度 NUMBER（precision 哨兵 22/38）→ 无精度 NUMERIC，不得压成 NUMERIC(22)")
        void unboundedNumberNotTruncated() {
            // JDBC 对无约束 NUMBER 常报告 precision=22：必须映射为无精度 NUMERIC
            String def22 = TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 22, 0, true));
            assertEquals("NUMERIC", def22.split(" ")[0]);
            String def38 = TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 38, 0, true));
            assertEquals("NUMERIC", def38.split(" ")[0]);
        }

        @Test
        @DisplayName("整数 NUMBER 精度分级：≤4→SMALLINT，≤9→INTEGER，≤18→BIGINT")
        void integerNumberTiers() {
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 4, 0, true)).startsWith("SMALLINT"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 9, 0, true)).startsWith("INTEGER"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 18, 0, true)).startsWith("BIGINT"));
        }

        @Test
        @DisplayName("带小数 NUMBER(38,10) 精度保留；NUMBER(0,0)→NUMERIC")
        void decimalNumber() {
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 38, 10, true)).startsWith("NUMERIC(38,10)"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("NUMBER", 0, 0, true)).startsWith("NUMERIC"));
        }

        @Test
        @DisplayName("字符/LOB/二进制/时间族映射")
        void oracleTypeFamilies() {
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("VARCHAR2", 200, 0, true)).startsWith("VARCHAR(200)"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("NVARCHAR2", 50, 0, true)).startsWith("VARCHAR(50)"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("CLOB", 0, 0, true)).startsWith("TEXT"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("RAW", 16, 0, true)).startsWith("BYTEA"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("BLOB", 0, 0, true)).startsWith("BYTEA"));
            // Oracle DATE 含时间分量 → TIMESTAMP
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("DATE", 0, 0, true)).startsWith("TIMESTAMP"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("TIMESTAMP(6) WITH TIME ZONE", 0, 0, true))
                    .startsWith("TIMESTAMP WITH TIME ZONE"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("BINARY_DOUBLE", 0, 0, true)).startsWith("DOUBLE PRECISION"));
            assertTrue(TypeMapper.mapOracleToPgColumnDef(col("BINARY_FLOAT", 0, 0, true)).startsWith("REAL"));
        }
    }

    @Nested
    @DisplayName("PostgreSQL → MySQL 列定义")
    class PgToMysql {

        @Test
        @DisplayName("常见 PG 类型映射")
        void basics() {
            assertTrue(TypeMapper.mapPgToMysqlColumnDef(col("character varying", 255, 0, true)).startsWith("VARCHAR(255)"));
            assertTrue(TypeMapper.mapPgToMysqlColumnDef(col("numeric", 38, 10, true)).startsWith("DECIMAL(38,10)"));
            assertEquals("TINYINT(1)", TypeMapper.mapPgToMysql("boolean"));
            assertEquals("JSON", TypeMapper.mapPgToMysql("jsonb"));
            assertEquals("BLOB", TypeMapper.mapPgToMysql("bytea"));
            assertEquals("DATETIME", TypeMapper.mapPgToMysql("timestamp without time zone"));
            assertEquals("CHAR(36)", TypeMapper.mapPgToMysql("uuid"));
            assertEquals("TEXT", TypeMapper.mapPgToMysql("text[]"));
        }
    }
}
