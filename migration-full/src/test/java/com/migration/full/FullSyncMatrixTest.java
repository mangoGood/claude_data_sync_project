package com.migration.full;

import com.migration.config.DatabaseConfig;
import com.migration.db.DatabaseConnection;
import com.migration.full.metadata.MetadataReader;
import com.migration.full.migration.DataMigration;
import com.migration.full.migration.SchemaMigration;
import com.migration.full.progress.ProgressManager;
import com.migration.model.TableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 全量同步「全类型/边界值」集成矩阵测试（数据级校验）。
 *
 * <p>在进程内直接驱动 MetadataReader → SchemaMigration → DataMigration（与 migration-full
 * 子进程相同的代码路径），对真实数据库做端到端断言。锁定人工矩阵测试中发现的 6 个 bug 的数据层行为：
 * MySQL TIME(±838h)→INTERVAL(#1)、bool/bit 映射(#2/#3 全量侧)、无精度 Oracle NUMBER 38 位大数(#4)、
 * 以及各类边界值（BIGINT UNSIGNED 最大值、DECIMAL(38,10)、极端日期、unicode）。
 *
 * <p><b>环境守卫</b>：依赖本地数据库，端口不可达时自动跳过（CI 无库环境不会失败）——
 * MySQL {@code localhost:33306}（root/rootpassword）、PG {@code localhost:5432}（app_user/userpassword）、
 * Oracle {@code localhost:1521/FREEPDB1}（app_user/userpassword）。可用系统属性
 * {@code matrix.mysql.port} 等覆盖。
 */
@DisplayName("全量同步全类型矩阵（集成）")
class FullSyncMatrixTest {

    private static final String MY_HOST = System.getProperty("matrix.mysql.host", "127.0.0.1");
    private static final int MY_PORT = Integer.getInteger("matrix.mysql.port", 33306);
    private static final String MY_USER = System.getProperty("matrix.mysql.user", "root");
    private static final String MY_PASS = System.getProperty("matrix.mysql.pass", "rootpassword");

    private static final String PG_HOST = System.getProperty("matrix.pg.host", "127.0.0.1");
    private static final int PG_PORT = Integer.getInteger("matrix.pg.port", 5432);
    private static final String PG_USER = System.getProperty("matrix.pg.user", "app_user");
    private static final String PG_PASS = System.getProperty("matrix.pg.pass", "userpassword");

    private static final String ORA_HOST = System.getProperty("matrix.oracle.host", "127.0.0.1");
    private static final int ORA_PORT = Integer.getInteger("matrix.oracle.port", 1521);
    private static final String ORA_SERVICE = System.getProperty("matrix.oracle.service", "FREEPDB1");
    private static final String ORA_USER = System.getProperty("matrix.oracle.user", "app_user");
    private static final String ORA_PASS = System.getProperty("matrix.oracle.pass", "userpassword");

    private static boolean reachable(String host, int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), 1500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Oracle 需要 JDBC 层真握手探测：观测到 OrbStack 端口转发会出现
     * “TCP 可连但监听器无响应（ORA-17800 read -1）”的卡死态，仅测 TCP 会把环境问题误报为测试失败。
     */
    private static boolean oracleJdbcReachable() {
        java.util.Properties p = new java.util.Properties();
        p.put("user", ORA_USER);
        p.put("password", ORA_PASS);
        p.put("oracle.net.CONNECT_TIMEOUT", "5000");
        p.put("oracle.net.READ_TIMEOUT", "5000");
        String url = "jdbc:oracle:thin:@" + ORA_HOST + ":" + ORA_PORT + "/" + ORA_SERVICE;
        try (Connection ignored = DriverManager.getConnection(url, p)) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** 进程内跑与子进程一致的全量链路：读元数据 → 建表 → 搬数。 */
    private static void runFullSync(DatabaseConfig src, DatabaseConfig tgt, Set<String> tables) throws Exception {
        DatabaseConnection sc = new DatabaseConnection(src);
        DatabaseConnection tc = new DatabaseConnection(tgt);
        try {
            List<TableInfo> infos = new MetadataReader(sc).getFilteredTablesInfo(tables);
            assertEquals(tables.size(), infos.size(), "元数据应找到全部待迁移表");
            new SchemaMigration(sc, tc, true).migrateAllTables(infos);
            new DataMigration(sc, tc, 1000, false, new ProgressManager(false)).migrateAllData(infos);
        } finally {
            sc.close();
            tc.close();
        }
    }

    private static Connection mysql(String db) throws Exception {
        return DriverManager.getConnection(
                "jdbc:mysql://" + MY_HOST + ":" + MY_PORT + "/" + db
                        + "?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true",
                MY_USER, MY_PASS);
    }

    private static Connection pg(String db) throws Exception {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + db + "?stringtype=unspecified",
                PG_USER, PG_PASS);
    }

    private static void recreatePgDatabase(String db) throws Exception {
        try (Connection c = pg("myapp_db"); Statement st = c.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)");
            st.execute("CREATE DATABASE " + db);
        }
    }

    private static String queryString(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ==================== MySQL → PostgreSQL ====================

    @Test
    @DisplayName("MySQL→PG：全类型/边界值全量同步（#1/#2/#3 全量侧回归）")
    void mysqlToPgAllTypesBoundary() throws Exception {
        assumeTrue(reachable(MY_HOST, MY_PORT), "MySQL 不可达，跳过");
        assumeTrue(reachable(PG_HOST, PG_PORT), "PostgreSQL 不可达，跳过");

        try (Connection c = mysql(""); Statement st = c.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS matrix_src");
            st.execute("CREATE DATABASE matrix_src CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
            st.execute("CREATE TABLE matrix_src.all_types (" +
                    "id INT PRIMARY KEY AUTO_INCREMENT," +
                    "c_bigu BIGINT UNSIGNED, c_dec DECIMAL(38,10), c_double DOUBLE," +
                    "c_bool BOOLEAN, c_bit BIT(8)," +
                    "c_vc VARCHAR(255), c_text TEXT," +
                    "c_date DATE, c_time TIME, c_time_neg TIME, c_dt DATETIME, c_year YEAR," +
                    "c_enum ENUM('a','b','c'), c_json JSON)");
            st.execute("SET NAMES utf8mb4");
            st.execute("INSERT INTO matrix_src.all_types VALUES " +
                    "(1, 18446744073709551615, '9999999999999999999999999999.9999999999', 1.7976931348623157e308," +
                    " 1, b'10101010', '中文unicode测试', 'text值'," +
                    " '9999-12-31', '838:59:59', '-838:59:59', '9999-12-31 23:59:59', 2155, 'c'," +
                    " '{\"k\": \"v\", \"n\": 123}')");
            st.execute("INSERT INTO matrix_src.all_types (id, c_bool, c_bit) VALUES (2, 0, b'00000001')");
            st.execute("INSERT INTO matrix_src.all_types (id) VALUES (3)"); // 全 NULL 行
        }
        recreatePgDatabase("matrix_tgt_pg");

        runFullSync(
                new DatabaseConfig(MY_HOST, MY_PORT, "matrix_src", MY_USER, MY_PASS, "mysql"),
                new DatabaseConfig(PG_HOST, PG_PORT, "matrix_tgt_pg", PG_USER, PG_PASS, "postgresql"),
                Set.of("all_types"));

        try (Connection c = pg("matrix_tgt_pg")) {
            // 列类型断言（#1 TIME→INTERVAL，#2/#3 bool/bit）
            assertEquals("interval", queryString(c, "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_name='all_types' AND column_name='c_time'"));
            assertEquals("boolean", queryString(c, "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_name='all_types' AND column_name='c_bool'"));
            assertEquals("bytea", queryString(c, "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_name='all_types' AND column_name='c_bit'"));

            assertEquals("3", queryString(c, "SELECT count(*) FROM all_types"));
            // 边界值断言
            assertEquals("18446744073709551615", queryString(c, "SELECT c_bigu::text FROM all_types WHERE id=1"));
            assertEquals("9999999999999999999999999999.9999999999",
                    queryString(c, "SELECT c_dec::text FROM all_types WHERE id=1"));
            assertEquals("838:59:59", queryString(c, "SELECT c_time::text FROM all_types WHERE id=1"));
            assertEquals("-838:59:59", queryString(c, "SELECT c_time_neg::text FROM all_types WHERE id=1"));
            assertEquals("true", queryString(c, "SELECT c_bool::text FROM all_types WHERE id=1"));
            assertEquals("\\xaa", queryString(c, "SELECT c_bit::text FROM all_types WHERE id=1"));
            assertEquals("中文unicode测试", queryString(c, "SELECT c_vc FROM all_types WHERE id=1"));
            assertEquals("9999-12-31", queryString(c, "SELECT c_date::text FROM all_types WHERE id=1"));
            assertEquals("9999-12-31 23:59:59",
                    queryString(c, "SELECT to_char(c_dt,'YYYY-MM-DD HH24:MI:SS') FROM all_types WHERE id=1"));
            assertEquals("2155", queryString(c, "SELECT c_year::text FROM all_types WHERE id=1"));
            assertEquals("c", queryString(c, "SELECT c_enum FROM all_types WHERE id=1"));
            assertTrue(queryString(c, "SELECT c_json::text FROM all_types WHERE id=1").contains("\"k\""));
            // false/最小 bit 与 NULL 行
            assertEquals("false", queryString(c, "SELECT c_bool::text FROM all_types WHERE id=2"));
            assertEquals("\\x01", queryString(c, "SELECT c_bit::text FROM all_types WHERE id=2"));
            assertNull(queryString(c, "SELECT c_vc FROM all_types WHERE id=3"));
        }
    }

    // ==================== MySQL → MySQL（同构） ====================

    @Test
    @DisplayName("MySQL→MySQL：同构全量 CHECKSUM 一致")
    void mysqlToMysqlChecksum() throws Exception {
        assumeTrue(reachable(MY_HOST, MY_PORT), "MySQL 不可达，跳过");

        try (Connection c = mysql(""); Statement st = c.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS matrix_src_my");
            st.execute("CREATE DATABASE matrix_src_my CHARACTER SET utf8mb4");
            st.execute("CREATE TABLE matrix_src_my.t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(100), n DECIMAL(10,2))");
            st.execute("INSERT INTO matrix_src_my.t(v,n) VALUES ('a',1.10),('中文',2.20),('c''quote',3.30)");
            st.execute("DROP DATABASE IF EXISTS matrix_tgt_my");
            st.execute("CREATE DATABASE matrix_tgt_my CHARACTER SET utf8mb4");
        }

        runFullSync(
                new DatabaseConfig(MY_HOST, MY_PORT, "matrix_src_my", MY_USER, MY_PASS, "mysql"),
                new DatabaseConfig(MY_HOST, MY_PORT, "matrix_tgt_my", MY_USER, MY_PASS, "mysql"),
                Set.of("t"));

        try (Connection c = mysql("")) {
            try (Statement st = c.createStatement();
                 ResultSet rs1 = st.executeQuery("CHECKSUM TABLE matrix_src_my.t")) {
                rs1.next();
                long srcSum = rs1.getLong(2);
                try (Statement st2 = c.createStatement();
                     ResultSet rs2 = st2.executeQuery("CHECKSUM TABLE matrix_tgt_my.t")) {
                    rs2.next();
                    assertEquals(srcSum, rs2.getLong(2), "同构迁移后 CHECKSUM 必须一致");
                }
            }
        }
    }

    // ==================== Oracle → PostgreSQL ====================

    @Test
    @DisplayName("Oracle→PG：无精度 NUMBER 38 位大数不溢出（#4 回归）+ 边界日期")
    void oracleToPgNumberBoundary() throws Exception {
        assumeTrue(reachable(PG_HOST, PG_PORT), "PostgreSQL 不可达，跳过");
        assumeTrue(oracleJdbcReachable(),
                "Oracle JDBC 握手不可达（TCP 通但监听器无响应时同样跳过），跳过");

        String oraUrl = "jdbc:oracle:thin:@" + ORA_HOST + ":" + ORA_PORT + "/" + ORA_SERVICE;
        try (Connection c = DriverManager.getConnection(oraUrl, ORA_USER, ORA_PASS);
             Statement st = c.createStatement()) {
            try {
                st.execute("DROP TABLE MATRIX_ORA_TYPES");
            } catch (Exception ignore) { /* 表不存在 */ }
            st.execute("CREATE TABLE MATRIX_ORA_TYPES (" +
                    "ID NUMBER(10) PRIMARY KEY, C_NUM NUMBER, C_NUM_PS NUMBER(38,10)," +
                    "C_VC VARCHAR2(200), C_DATE DATE, C_RAW RAW(16))");
            st.execute("INSERT INTO MATRIX_ORA_TYPES VALUES (1, " +
                    "99999999999999999999999999999999999999, 9999999999999999999999999999.9999999999," +
                    "'oracle string', TO_DATE('9999-12-31','YYYY-MM-DD'), HEXTORAW('DEADBEEF'))");
            st.execute("INSERT INTO MATRIX_ORA_TYPES (ID) VALUES (2)");
            c.commit();
        }
        recreatePgDatabase("matrix_tgt_ora");

        runFullSync(
                new DatabaseConfig(ORA_HOST, ORA_PORT, ORA_SERVICE, ORA_USER, ORA_PASS, "oracle"),
                new DatabaseConfig(PG_HOST, PG_PORT, "matrix_tgt_ora", PG_USER, PG_PASS, "postgresql"),
                Set.of("MATRIX_ORA_TYPES"));

        try (Connection c = pg("matrix_tgt_ora")) {
            // #4 回归：无精度 NUMBER 必须映射为无精度 NUMERIC（information_schema 精度为 NULL）
            assertNull(queryString(c, "SELECT numeric_precision::text FROM information_schema.columns " +
                    "WHERE table_name='matrix_ora_types' AND column_name='c_num'"));
            assertEquals("99999999999999999999999999999999999999",
                    queryString(c, "SELECT c_num::text FROM matrix_ora_types WHERE id=1"));
            assertEquals("9999999999999999999999999999.9999999999",
                    queryString(c, "SELECT c_num_ps::text FROM matrix_ora_types WHERE id=1"));
            assertEquals("oracle string", queryString(c, "SELECT c_vc FROM matrix_ora_types WHERE id=1"));
            assertEquals("9999-12-31", queryString(c,
                    "SELECT to_char(c_date,'YYYY-MM-DD') FROM matrix_ora_types WHERE id=1"));
            assertEquals("\\xdeadbeef", queryString(c, "SELECT c_raw::text FROM matrix_ora_types WHERE id=1"));
            assertNull(queryString(c, "SELECT c_vc FROM matrix_ora_types WHERE id=2"));
        }
    }
}
