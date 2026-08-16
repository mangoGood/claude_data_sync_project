package com.migration.dialect;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Oracle → MySQL 翻译器。
 *
 * <p>这个库对此前压根没实现：{@code SchemaMigration} 拿不到源端 CREATE TABLE SQL 就直接跳过建表，
 * 还把这一步记成"成功"，紧接着几百行数据全部以 {@code Table ... doesn't exist} 写失败。
 * 用例按两条线守：<b>建表 DDL 的类型映射</b>与<b>增量字面量翻译</b>——后者是 Oracle→MySQL 独有的坑，
 * LogMiner 的 SQL_REDO 里全是 {@code TO_DATE(...)} / {@code HEXTORAW(...)} 这类 Oracle 函数，
 * PG 恰好也有同名函数所以 Oracle→PG 透传就能跑，MySQL 透传过去整条语句直接报错。
 */
@DisplayName("Oracle → MySQL：建表类型映射 + 增量字面量翻译")
class OracleToMysqlTranslatorTest {

    private static ColumnInfo col(String name, String type, int size, int scale, boolean pk) {
        ColumnInfo c = new ColumnInfo();
        c.setColumnName(name);
        c.setDataType(type);
        c.setColumnSize(size);
        c.setDecimalDigits(scale);
        c.setNullable(true);
        c.setPrimaryKey(pk);
        return c;
    }

    private static String def(String type, int size, int scale) {
        return TypeMapper.mapOracleToMysqlColumnDef(col("c", type, size, scale, false));
    }

    private final OracleToMysqlTranslator t = new OracleToMysqlTranslator();

    // ------------------------------------------------------------------ 类型映射

    @Test
    @DisplayName("NUMBER 按精度落到最小够用的整数类型，带小数位落 DECIMAL")
    void numberMapping() {
        assertEquals("TINYINT", def("NUMBER", 2, 0));
        assertEquals("SMALLINT", def("NUMBER", 4, 0));
        assertEquals("MEDIUMINT", def("NUMBER", 6, 0));
        assertEquals("INT", def("NUMBER", 9, 0));
        assertEquals("BIGINT", def("NUMBER", 18, 0));
        // 19 位十进制的上界 1e19 已经超过 BIGINT 的 9.22e18，只能落 DECIMAL
        assertEquals("DECIMAL(19,0)", def("NUMBER", 19, 0));
        assertEquals("DECIMAL(38,0)", def("NUMBER", 38, 0));
        assertEquals("DECIMAL(10,2)", def("NUMBER", 10, 2));
    }

    @Test
    @DisplayName("无精度 NUMBER 落 DECIMAL(65,20)，不能落 DOUBLE（精确十进制变二进制浮点是静默失真）")
    void unconstrainedNumberKeepsExactness() {
        String d = def("NUMBER", 0, 0);
        assertEquals("DECIMAL(65,20)", d);
        assertFalse(d.contains("DOUBLE"));
        // 45 位整数位 > Oracle NUMBER 的 38 位有效数字上限，整数取值不会被截断
        assertEquals(45, 65 - 20);
    }

    @Test
    @DisplayName("无参 NUMBER 的 JDBC 精度是哨兵值 22，不能当真实精度用（当真会把小数位抹掉）")
    void jdbcSentinelPrecisionIsNotRealPrecision() {
        // 实测：Oracle JDBC 对无参 NUMBER 报 precision=22 / scale=0，
        // 照着建 DECIMAL(22,0)，12345678901234567890.5 落库直接丢掉 .5
        assertEquals("DECIMAL(65,20)", def("NUMBER", 22, 0));
        // 只有 22 这一个哨兵值特殊对待；真正声明出来的精度照常保留，别把 NUMBER(38,0) 也撑成宽类型
        assertEquals("DECIMAL(38,0)", def("NUMBER", 38, 0));
        assertEquals("DECIMAL(19,0)", def("NUMBER", 19, 0));
    }

    @Test
    @DisplayName("TIMESTAMP 的小数秒精度从类型名取：JDBC 的 decimalDigits 对 TIMESTAMP(6) 常报 0")
    void timestampPrecisionComesFromTypeName() {
        // scale 传 0 模拟 JDBC 的真实上报；只信 decimalDigits 会建成 DATETIME，微秒被截成 .000000
        assertEquals("DATETIME(6)", def("TIMESTAMP(6)", 0, 0));
        assertEquals("DATETIME(3)", def("TIMESTAMP(3)", 0, 0));
        assertEquals("DATETIME(6)", def("TIMESTAMP(9)", 0, 0));
        assertEquals("DATETIME", def("TIMESTAMP", 0, 0));
    }

    @Test
    @DisplayName("时间值交 LocalDateTime（墙上时间），交 Timestamp 会被驱动按连接时区再折算一次")
    void temporalValuesAreWallClock() throws Exception {
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2026-08-11 10:20:30.123456");
        Object converted = t.convertValue(ts, "TIMESTAMP(6)", null, 1);
        assertTrue(converted instanceof java.time.LocalDateTime, String.valueOf(converted));
        assertEquals("2026-08-11T10:20:30.123456", converted.toString());
    }

    @Test
    @DisplayName("Oracle DATE 落 DATETIME —— 落 DATE 会把时分秒丢掉")
    void dateKeepsTimePart() {
        assertEquals("DATETIME", def("DATE", 0, 0));
        assertEquals("DATETIME(6)", def("TIMESTAMP(6)", 0, 6));
        assertEquals("DATETIME", def("TIMESTAMP", 0, 0));
        // MySQL 小数秒最多 6 位，Oracle 允许到 9
        assertEquals("DATETIME(6)", def("TIMESTAMP(9)", 0, 9));
        assertEquals("VARCHAR(64)", def("TIMESTAMP(6) WITH TIME ZONE", 0, 6));
    }

    @Test
    @DisplayName("带时区的时间落 VARCHAR：MySQL 没有带时区类型，落 DATETIME 会静默抹掉偏移量")
    void timestampWithTimeZoneKeepsOffset() {
        assertEquals("VARCHAR(64)", def("TIMESTAMP(6) WITH TIME ZONE", 0, 6));
        assertEquals("VARCHAR(64)", def("TIMESTAMP(6) WITH LOCAL TIME ZONE", 0, 6));
    }

    @Test
    @DisplayName("字符与大对象：超长 VARCHAR2 与 CLOB 落 LONGTEXT（TEXT 只有 64KB，装不下）")
    void charAndLobMapping() {
        assertEquals("VARCHAR(100)", def("VARCHAR2", 100, 0));
        assertEquals("VARCHAR(4000)", def("VARCHAR2", 4000, 0));
        assertEquals("LONGTEXT", def("VARCHAR2", 32767, 0));
        assertEquals("LONGTEXT", def("CLOB", 0, 0));
        assertEquals("LONGTEXT", def("NCLOB", 0, 0));
        assertEquals("LONGBLOB", def("BLOB", 0, 0));
        assertEquals("VARBINARY(2000)", def("RAW", 2000, 0));
        assertEquals("CHAR(10)", def("CHAR", 10, 0));
        assertEquals("VARCHAR(1000)", def("CHAR", 1000, 0));
    }

    @Test
    @DisplayName("大对象列不带 DEFAULT：MySQL 的 TEXT/BLOB/JSON 带字面量默认值建表直接报错")
    void lobColumnsCarryNoDefault() {
        ColumnInfo c = col("c", "CLOB", 0, 0, false);
        c.setDefaultValue("'x'");
        assertEquals("LONGTEXT", TypeMapper.mapOracleToMysqlColumnDef(c));

        ColumnInfo v = col("v", "VARCHAR2", 50, 0, false);
        v.setDefaultValue("'x'");
        assertEquals("VARCHAR(50) DEFAULT 'x'", TypeMapper.mapOracleToMysqlColumnDef(v));
    }

    @Test
    @DisplayName("SYSDATE 默认值只落到时间列上；落到别的类型上是建表错误")
    void sysdateDefaultOnlyOnTimeColumns() {
        ColumnInfo d = col("d", "DATE", 0, 0, false);
        d.setDefaultValue("SYSDATE");
        assertEquals("DATETIME DEFAULT CURRENT_TIMESTAMP", TypeMapper.mapOracleToMysqlColumnDef(d));

        ColumnInfo s = col("s", "VARCHAR2", 20, 0, false);
        s.setDefaultValue("SYSDATE");
        assertEquals("VARCHAR(20)", TypeMapper.mapOracleToMysqlColumnDef(s));
    }

    @Test
    @DisplayName("建表：保持 Oracle 原样大写名（MySQL 表名大小写敏感，转小写会跟增量/对比各找各的）")
    void createTableKeepsIdentifierCase() {
        TableInfo table = new TableInfo();
        table.setTableName("AT_LOAD");
        table.addColumn(col("ID", "NUMBER", 19, 0, true));
        table.addColumn(col("VAL", "VARCHAR2", 128, 0, false));

        String ddl = t.generateCreateTable(table, SqlDialect.forType("mysql"));

        assertTrue(ddl.contains("`AT_LOAD`"), ddl);
        // NUMBER(19) 的上界约 1e19，超过 BIGINT 的 9.22e18 —— 落 BIGINT 会溢出，DECIMAL(19,0) 才是对的
        assertTrue(ddl.contains("`ID` DECIMAL(19,0)"), ddl);
        assertTrue(ddl.contains("`VAL` VARCHAR(128)"), ddl);
        assertTrue(ddl.contains("PRIMARY KEY (`ID`)"), ddl);
        assertTrue(ddl.endsWith("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"), ddl);
        assertFalse(ddl.contains("at_load"), "不能转小写：" + ddl);
    }

    // ------------------------------------------------------------------ 增量字面量

    @Test
    @DisplayName("TO_DATE/TO_TIMESTAMP 拆成纯字符串字面量（MySQL 没这些函数，透传过去整条语句报错）")
    void toDateBecomesPlainLiteral() {
        assertEquals("'2026-08-16 10:20:30'",
                t.convertLiteral("TO_DATE('2026-08-16 10:20:30','SYYYY-MM-DD HH24:MI:SS')", "DATE"));
        assertEquals("'2026-08-16 10:20:30.123456'",
                t.convertLiteral("TO_TIMESTAMP('2026-08-16 10:20:30.123456','SYYYY-MM-DD HH24:MI:SS.FF')",
                        "TIMESTAMP(6)"));
        assertEquals("'2026-08-16 10:20:30.0 +08:00'",
                t.convertLiteral("TO_TIMESTAMP_TZ('2026-08-16 10:20:30.0 +08:00','...')",
                        "TIMESTAMP(6) WITH TIME ZONE"));
    }

    @Test
    @DisplayName("HEXTORAW → 0x 字面量；空 RAW 不能产出裸 0x（那是语法错误）")
    void hextorawBecomesHexLiteral() {
        assertEquals("0xAABB", t.convertLiteral("HEXTORAW('AABB')", "RAW(2)"));
        assertEquals("''", t.convertLiteral("HEXTORAW('')", "RAW(2)"));
    }

    @Test
    @DisplayName("INTERVAL / UNISTR / EMPTY_LOB 也要拆掉函数包装")
    void otherOracleFunctionLiterals() {
        assertEquals("'1-2'", t.convertLiteral("TO_YMINTERVAL('1-2')", "INTERVAL YEAR TO MONTH"));
        assertEquals("'1 02:03:04'", t.convertLiteral("TO_DSINTERVAL('1 02:03:04')", "INTERVAL DAY TO SECOND"));
        assertEquals("'中文'", t.convertLiteral("UNISTR('中文')", "NVARCHAR2(10)"));
        assertEquals("''", t.convertLiteral("EMPTY_CLOB()", "CLOB"));
        assertEquals("''", t.convertLiteral("EMPTY_BLOB()", "BLOB"));
    }

    @Test
    @DisplayName("THL 把函数字面量整个引起来并转义过：这层壳必须先剥掉")
    void quotedAndEscapedFunctionLiteral() {
        // 实测 THL 文本行数据里的真实形态（外层一对单引号 + 内层单引号翻倍）。
        // 不剥壳的话整串 18 个字符会被当字符串塞进 VARBINARY(16)，报 Data too long。
        assertEquals("0xaabb06", t.convertLiteral("'HEXTORAW(''aabb06'')'", "RAW(16)"));
        assertEquals("'2026-08-16 10:20:30'",
                t.convertLiteral("'TO_DATE(''2026-08-16 10:20:30'',''YYYY-MM-DD HH24:MI:SS'')'", "DATE"));
        assertEquals("''", t.convertLiteral("'EMPTY_CLOB()'", "CLOB"));
    }

    @Test
    @DisplayName("普通字面量与 NULL 原样透传，不要画蛇添足")
    void plainLiteralsPassThrough() {
        assertEquals("123", t.convertLiteral("123", "NUMBER"));
        assertEquals("'abc'", t.convertLiteral("'abc'", "VARCHAR2(10)"));
        assertEquals("NULL", t.convertLiteral("NULL", "VARCHAR2(10)"));
        assertEquals("NULL", t.convertLiteral(null, "VARCHAR2(10)"));
        // 值里带 TO_DATE 字样但不是函数调用形态，不能被误拆
        assertEquals("'TO_DATE is a function'",
                t.convertLiteral("'TO_DATE is a function'", "VARCHAR2(64)"));
    }

    @Test
    @DisplayName("BOOLEAN 字面量归一成 1/0")
    void booleanLiteral() {
        assertEquals("1", t.convertLiteral("'TRUE'", "BOOLEAN"));
        assertEquals("0", t.convertLiteral("'FALSE'", "BOOLEAN"));
    }

    @Test
    @DisplayName("不是同构：必须走建表翻译，而不是沿用源端 DDL（Oracle 根本不提供 CREATE TABLE SQL）")
    void notHomogeneous() {
        assertFalse(t.isHomogeneous());
    }
}
