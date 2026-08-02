package com.migration.full;

import com.migration.config.DatabaseConfig;
import com.migration.db.DatabaseConnection;
import com.migration.full.metadata.MetadataReader;
import com.migration.full.migration.DataMigration;
import com.migration.full.migration.SchemaMigration;
import com.migration.full.progress.ProgressManager;
import com.migration.full.support.MockJdbc;
import com.migration.model.TableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全量同步「全类型/边界值」矩阵测试（数据级校验）。
 *
 * <p>在进程内直接驱动 MetadataReader → SchemaMigration → DataMigration（与 migration-full
 * 子进程相同的代码路径），锁定人工矩阵测试中发现的 6 个 bug 的数据层行为：
 * MySQL TIME(±838h)→INTERVAL(#1)、bool/bit 映射(#2/#3 全量侧)、无精度 Oracle NUMBER 38 位大数(#4)、
 * 以及各类边界值（BIGINT UNSIGNED 最大值、DECIMAL(38,10)、极端日期、unicode）。
 *
 * <p><b>不连任何数据库</b>：源端/目标端的 JDBC 层全部由 {@link MockJdbc} 以 Mockito mock 提供。
 * 断言口径也随之从"查目标库"改为"查我们发给目标库的东西"——
 * 建表断言看 {@link SchemaMigration} 实际执行的 CREATE TABLE，
 * 数据断言看 {@link DataMigration} 实际绑定到目标 INSERT 上的值。
 * 二者正是原先真库断言（information_schema + SELECT）的上游，映射/搬运逻辑出错同样会被抓住。
 *
 * <p><b>覆盖边界</b>：mock 的是"驱动返回什么"，因此驱动自身的上报口径
 * （如 MySQL {@code tinyInt1isBit} 把 tinyint(1) 报成 BIT）不在本测试覆盖内，
 * 由 {@code test_scripts/} 下的真库端到端脚本负责；下面每处 mock 元数据都按真实驱动口径构造。
 */
@DisplayName("全量同步全类型矩阵")
class FullSyncMatrixTest {

    private static final String MY_HOST = "mysql.test";
    private static final int MY_PORT = 3306;
    private static final String PG_HOST = "pg.test";
    private static final int PG_PORT = 5432;
    private static final String ORA_HOST = "oracle.test";
    private static final int ORA_PORT = 1521;
    private static final String ORA_SERVICE = "FREEPDB1";
    private static final String USER = "app_user";
    private static final String PASS = "userpassword";

    /** 进程内跑与子进程一致的全量链路：读元数据 → 建表 → 搬数。 */
    private static void runFullSync(DatabaseConnection sc, DatabaseConnection tc, Set<String> tables) throws Exception {
        List<TableInfo> infos = new MetadataReader(sc).getFilteredTablesInfo(tables);
        assertEquals(tables.size(), infos.size(), "元数据应找到全部待迁移表");
        new SchemaMigration(sc, tc, true).migrateAllTables(infos);
        new DataMigration(sc, tc, 1000, false, new ProgressManager(false)).migrateAllData(infos);
    }

    /** 从 CREATE TABLE 里取某列的类型定义（列定义行形如 {@code   "c_time" INTERVAL}）。 */
    private static String columnDef(String createSql, String columnName) {
        for (String line : createSql.split("\n")) {
            String trimmed = line.trim();
            String prefix = "\"" + columnName + "\" ";
            if (trimmed.startsWith(prefix)) {
                String def = trimmed.substring(prefix.length()).trim();
                // 只去掉行尾的分隔逗号，NUMERIC(38,10) 里的逗号必须留着
                return def.endsWith(",") ? def.substring(0, def.length() - 1).trim() : def;
            }
        }
        throw new AssertionError("建表语句里没有列 " + columnName + ":\n" + createSql);
    }

    // ==================== MySQL → PostgreSQL ====================

    @Test
    @DisplayName("MySQL→PG：全类型/边界值全量同步（#1/#2/#3 全量侧回归）")
    void mysqlToPgAllTypesBoundary() throws Exception {
        // ---- 源端：MySQL。DESCRIBE 的 Type 列驱动建表映射，ResultSetMetaData 驱动值转换 ----
        MockJdbc.FakeDatabase source = MockJdbc.database();
        source.onQuery("show tables", new MockJdbc.Rows()
                .textColumn("Tables_in_matrix_src")
                .row("all_types"));
        source.onQuery("show create table", new MockJdbc.Rows()
                .textColumn("Table").textColumn("Create Table")
                .row("all_types", "CREATE TABLE `all_types` (\n  `id` int NOT NULL AUTO_INCREMENT\n) ENGINE=InnoDB"));
        source.onQuery("describe", describeAllTypes());
        source.onQuery("show keys", new MockJdbc.Rows()
                .textColumn("Column_name")
                .row("id"));
        source.onQuery("count(*)", new MockJdbc.Rows()
                .column("cnt", Types.BIGINT, "BIGINT")
                .row(3L));
        source.onQuery("order by", allTypesRows());

        MockJdbc.FakeDatabase target = MockJdbc.database();

        DatabaseConnection sc = MockJdbc.databaseConnection(
                new DatabaseConfig(MY_HOST, MY_PORT, "matrix_src", USER, PASS, "mysql"), source);
        DatabaseConnection tc = MockJdbc.databaseConnection(
                new DatabaseConfig(PG_HOST, PG_PORT, "matrix_tgt_pg", USER, PASS, "postgresql"), target);

        runFullSync(sc, tc, Set.of("all_types"));

        // ---- 建表断言（原先查 information_schema.columns.data_type）----
        String createSql = target.recorded().soleCreateTable();
        assertEquals("INTERVAL", columnDef(createSql, "c_time"),
                "#1 MySQL TIME 是 ±838h 时长语义，超出 PG time 范围，必须映射为 INTERVAL");
        assertEquals("INTERVAL", columnDef(createSql, "c_time_neg"));
        assertEquals("BOOLEAN", columnDef(createSql, "c_bool"), "#2 tinyint(1) 必须映射为 BOOLEAN");
        assertEquals("BYTEA", columnDef(createSql, "c_bit"), "#3 bit(N) 必须映射为 BYTEA");
        assertTrue(createSql.contains("PRIMARY KEY (\"id\")"), "主键必须带过去");

        // ---- 数据断言（原先 SELECT 回目标库）----
        assertEquals("INSERT INTO \"all_types\" (\"id\", \"c_bigu\", \"c_dec\", \"c_double\", \"c_bool\", "
                        + "\"c_bit\", \"c_vc\", \"c_text\", \"c_date\", \"c_time\", \"c_time_neg\", \"c_dt\", "
                        + "\"c_year\", \"c_enum\", \"c_json\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                target.recorded().soleInsertSql());

        List<Object[]> written = target.recorded().insertedRows();
        assertEquals(3, written.size(), "3 行都要落到目标端");

        Object[] full = written.get(0);
        assertEquals(new BigDecimal("18446744073709551615"), full[1], "BIGINT UNSIGNED 最大值不能溢出成负数");
        assertEquals(new BigDecimal("9999999999999999999999999999.9999999999"), full[2], "DECIMAL(38,10) 全精度");
        assertEquals("838:59:59", full[9], "#1 TIME 上界按时长字符串搬运，不能被截成 24h 内的 time");
        assertEquals("-838:59:59", full[10], "#1 TIME 下界（负时长）同样不能丢");
        assertEquals(Boolean.TRUE, full[4], "#2 bool 真值");
        assertArrayEquals(new byte[]{(byte) 0xAA}, (byte[]) full[5], "#3 bit(8) 按字节搬运");
        assertEquals("中文unicode测试", full[6], "unicode 不能乱码");
        assertEquals("9999-12-31", String.valueOf(full[8]), "极端日期上界");
        assertEquals("9999-12-31 23:59:59.0", String.valueOf(full[11]), "极端 DATETIME 上界");
        assertEquals(2155, full[12], "YEAR 上界必须转成整数年份，不能是 java.sql.Date");
        assertEquals("c", full[13], "ENUM 按标签搬运，不是序号");
        assertTrue(String.valueOf(full[14]).contains("\"k\""), "JSON 原样搬运");

        Object[] falseRow = written.get(1);
        assertEquals(Boolean.FALSE, falseRow[4], "#2 bool 假值不能被当成 NULL 或 true");
        assertArrayEquals(new byte[]{(byte) 0x01}, (byte[]) falseRow[5], "#3 bit 最小值");

        Object[] nullRow = written.get(2);
        assertNull(nullRow[6], "全 NULL 行的可空列必须保持 NULL");
        assertNull(nullRow[4], "NULL 的 bool 不能被塞成 false");
    }

    /** MySQL {@code DESCRIBE all_types} 的输出（Type 列即建表映射的输入）。 */
    private static MockJdbc.Rows describeAllTypes() {
        return new MockJdbc.Rows()
                .textColumn("Field").textColumn("Type").textColumn("Null")
                .textColumn("Default").textColumn("Extra")
                .row("id", "int", "NO", null, "auto_increment")
                .row("c_bigu", "bigint unsigned", "YES", null, "")
                .row("c_dec", "decimal(38,10)", "YES", null, "")
                .row("c_double", "double", "YES", null, "")
                .row("c_bool", "tinyint(1)", "YES", null, "")
                .row("c_bit", "bit(8)", "YES", null, "")
                .row("c_vc", "varchar(255)", "YES", null, "")
                .row("c_text", "text", "YES", null, "")
                .row("c_date", "date", "YES", null, "")
                .row("c_time", "time", "YES", null, "")
                .row("c_time_neg", "time", "YES", null, "")
                .row("c_dt", "datetime", "YES", null, "")
                .row("c_year", "year", "YES", null, "")
                .row("c_enum", "enum('a','b','c')", "YES", null, "")
                .row("c_json", "json", "YES", null, "");
    }

    /**
     * 源端分页 SELECT 的返回。类型码/类型名按 MySQL Connector/J 的实际上报口径构造：
     * tinyint(1) 在默认 {@code tinyInt1isBit=true} 下报 BIT 且 getObject 给 Boolean；
     * BIT(N>1) 报 BIT 且给 byte[]；YEAR 在默认 {@code yearIsDateType=true} 下给 java.sql.Date；
     * ENUM 报 CHAR。
     */
    private static MockJdbc.Rows allTypesRows() {
        return new MockJdbc.Rows()
                .column("id", Types.INTEGER, "INT")
                .column("c_bigu", Types.BIGINT, "BIGINT UNSIGNED")
                .column("c_dec", Types.DECIMAL, "DECIMAL")
                .column("c_double", Types.DOUBLE, "DOUBLE")
                .column("c_bool", Types.BIT, "BIT")
                .column("c_bit", Types.BIT, "BIT")
                .column("c_vc", Types.VARCHAR, "VARCHAR")
                .column("c_text", Types.LONGVARCHAR, "TEXT")
                .column("c_date", Types.DATE, "DATE")
                .column("c_time", Types.TIME, "TIME")
                .column("c_time_neg", Types.TIME, "TIME")
                .column("c_dt", Types.TIMESTAMP, "DATETIME")
                .column("c_year", Types.DATE, "YEAR")
                .column("c_enum", Types.CHAR, "CHAR")
                .column("c_json", Types.LONGVARCHAR, "JSON")
                .row(1,
                        new BigDecimal("18446744073709551615"),
                        new BigDecimal("9999999999999999999999999999.9999999999"),
                        1.7976931348623157e308,
                        Boolean.TRUE,
                        new byte[]{(byte) 0xAA},
                        "中文unicode测试",
                        "text值",
                        java.sql.Date.valueOf("9999-12-31"),
                        "838:59:59",
                        "-838:59:59",
                        java.sql.Timestamp.valueOf("9999-12-31 23:59:59"),
                        java.sql.Date.valueOf("2155-01-01"),
                        "c",
                        "{\"k\": \"v\", \"n\": 123}")
                .row(2, null, null, null, Boolean.FALSE, new byte[]{(byte) 0x01},
                        null, null, null, null, null, null, null, null, null)
                .row(3, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null);
    }

    // ==================== MySQL → MySQL（同构） ====================

    @Test
    @DisplayName("MySQL→MySQL：同构全量结构与值逐列一致（原 CHECKSUM 断言）")
    void mysqlToMysqlIdentity() throws Exception {
        String sourceDdl = "CREATE TABLE `t` (\n"
                + "  `id` int NOT NULL AUTO_INCREMENT,\n"
                + "  `v` varchar(100) DEFAULT NULL,\n"
                + "  `n` decimal(10,2) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4";

        MockJdbc.FakeDatabase source = MockJdbc.database();
        source.onQuery("show tables", new MockJdbc.Rows().textColumn("Tables_in_matrix_src_my").row("t"));
        source.onQuery("show create table", new MockJdbc.Rows()
                .textColumn("Table").textColumn("Create Table")
                .row("t", sourceDdl));
        source.onQuery("describe", new MockJdbc.Rows()
                .textColumn("Field").textColumn("Type").textColumn("Null")
                .textColumn("Default").textColumn("Extra")
                .row("id", "int", "NO", null, "auto_increment")
                .row("v", "varchar(100)", "YES", null, "")
                .row("n", "decimal(10,2)", "YES", null, ""));
        source.onQuery("show keys", new MockJdbc.Rows().textColumn("Column_name").row("id"));
        source.onQuery("count(*)", new MockJdbc.Rows().column("cnt", Types.BIGINT, "BIGINT").row(3L));
        source.onQuery("order by", new MockJdbc.Rows()
                .column("id", Types.INTEGER, "INT")
                .column("v", Types.VARCHAR, "VARCHAR")
                .column("n", Types.DECIMAL, "DECIMAL")
                .row(1, "a", new BigDecimal("1.10"))
                .row(2, "中文", new BigDecimal("2.20"))
                .row(3, "c'quote", new BigDecimal("3.30")));

        MockJdbc.FakeDatabase target = MockJdbc.database();

        DatabaseConnection sc = MockJdbc.databaseConnection(
                new DatabaseConfig(MY_HOST, MY_PORT, "matrix_src_my", USER, PASS, "mysql"), source);
        DatabaseConnection tc = MockJdbc.databaseConnection(
                new DatabaseConfig(MY_HOST, MY_PORT, "matrix_tgt_my", USER, PASS, "mysql"), target);

        runFullSync(sc, tc, Set.of("t"));

        // 同构链路沿用源端 DDL，只把 AUTO_INCREMENT 计数归一——结构必须逐字一致，
        // 否则目标端列定义漂移，CHECKSUM 也就不可能相等。
        assertEquals(sourceDdl.replace("AUTO_INCREMENT=4", "AUTO_INCREMENT=1"),
                target.recorded().soleCreateTable());

        // 值必须原样搬运（同构无类型转换）——这是原 CHECKSUM 相等断言的等价物
        List<Object[]> written = target.recorded().insertedRows();
        assertEquals(3, written.size());
        assertArrayEquals(new Object[]{1, "a", new BigDecimal("1.10")}, written.get(0));
        assertArrayEquals(new Object[]{2, "中文", new BigDecimal("2.20")}, written.get(1));
        assertArrayEquals(new Object[]{3, "c'quote", new BigDecimal("3.30")}, written.get(2),
                "带单引号的值必须走参数绑定，不能被拼进 SQL");
    }

    // ==================== Oracle → PostgreSQL ====================

    @Test
    @DisplayName("Oracle→PG：无精度 NUMBER 38 位大数不溢出（#4 回归）+ 边界日期")
    void oracleToPgNumberBoundary() throws Exception {
        MockJdbc.FakeDatabase source = MockJdbc.database();
        source.onQuery("all_tables", new MockJdbc.Rows().textColumn("table_name").row("MATRIX_ORA_TYPES"));
        // all_tab_columns：无精度 NUMBER 的 data_precision/data_scale 在 Oracle 里是 NULL，
        // JDBC getInt 读出来是 0 —— #4 的触发前提就是这个"0 精度"。
        source.onQuery("all_tab_columns", new MockJdbc.Rows()
                .textColumn("column_name").textColumn("data_type").textColumn("data_length")
                .textColumn("data_precision").textColumn("data_scale")
                .textColumn("nullable").textColumn("data_default")
                .row("ID", "NUMBER", 22, 10, 0, "N", null)
                .row("C_NUM", "NUMBER", 22, null, null, "Y", null)
                .row("C_NUM_PS", "NUMBER", 22, 38, 10, "Y", null)
                .row("C_VC", "VARCHAR2", 200, null, null, "Y", null)
                .row("C_DATE", "DATE", 7, null, null, "Y", null)
                .row("C_RAW", "RAW", 16, null, null, "Y", null));
        source.onQuery("all_constraints", new MockJdbc.Rows().textColumn("column_name").row("ID"));
        source.onQuery("count(*)", new MockJdbc.Rows().column("cnt", Types.BIGINT, "BIGINT").row(2L));
        source.onQuery("order by", new MockJdbc.Rows()
                .column("ID", Types.NUMERIC, "NUMBER")
                .column("C_NUM", Types.NUMERIC, "NUMBER")
                .column("C_NUM_PS", Types.NUMERIC, "NUMBER")
                .column("C_VC", Types.VARCHAR, "VARCHAR2")
                .column("C_DATE", Types.TIMESTAMP, "DATE")
                .column("C_RAW", Types.VARBINARY, "RAW")
                .row(new BigDecimal("1"),
                        new BigDecimal("99999999999999999999999999999999999999"),
                        new BigDecimal("9999999999999999999999999999.9999999999"),
                        "oracle string",
                        java.sql.Timestamp.valueOf("9999-12-31 00:00:00"),
                        new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF})
                .row(new BigDecimal("2"), null, null, null, null, null));

        MockJdbc.FakeDatabase target = MockJdbc.database();

        DatabaseConnection sc = MockJdbc.databaseConnection(
                new DatabaseConfig(ORA_HOST, ORA_PORT, ORA_SERVICE, USER, PASS, "oracle"), source);
        DatabaseConnection tc = MockJdbc.databaseConnection(
                new DatabaseConfig(PG_HOST, PG_PORT, "matrix_tgt_ora", USER, PASS, "postgresql"), target);

        runFullSync(sc, tc, Set.of("MATRIX_ORA_TYPES"));

        String createSql = target.recorded().soleCreateTable();
        assertTrue(createSql.startsWith("CREATE TABLE \"matrix_ora_types\""),
                "Oracle→PG 表名统一转小写: " + createSql);
        // #4 回归：无精度 NUMBER 必须落成无精度 NUMERIC。写成 NUMERIC(22) 会把 38 位大数撑溢出。
        assertEquals("NUMERIC", columnDef(createSql, "c_num"),
                "#4 无精度 NUMBER 必须是无精度 NUMERIC，不能带 22 这种哨兵精度");
        assertEquals("NUMERIC(38,10)", columnDef(createSql, "c_num_ps"), "有精度的 NUMBER 原样保留");
        assertTrue(columnDef(createSql, "id").startsWith("BIGINT"),
                "NUMBER(10) 整数映射为 BIGINT，实际: " + columnDef(createSql, "id"));
        assertEquals("VARCHAR(200)", columnDef(createSql, "c_vc"));
        assertEquals("BYTEA", columnDef(createSql, "c_raw"));

        List<Object[]> written = target.recorded().insertedRows();
        assertEquals(2, written.size());
        Object[] full = written.get(0);
        assertEquals(new BigDecimal("99999999999999999999999999999999999999"), full[1],
                "#4 38 位大数必须无损搬运");
        assertEquals(new BigDecimal("9999999999999999999999999999.9999999999"), full[2]);
        assertEquals("oracle string", full[3]);
        assertInstanceOf(java.sql.Timestamp.class, full[4], "Oracle DATE 带时间分量，必须以 Timestamp 搬运");
        assertEquals("9999-12-31 00:00:00.0", String.valueOf(full[4]), "极端日期上界");
        assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, (byte[]) full[5],
                "RAW 按字节搬运");

        assertNull(written.get(1)[3], "可空列保持 NULL");
    }
}
