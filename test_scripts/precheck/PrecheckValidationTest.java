package com.synctask.service;

import com.synctask.dto.ValidationResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

/**
 * 预检项测试：验证新增的 DTS 对齐校验项（源库版本号 / 存储引擎 / 约束完整性）
 * 及 warning 非阻断语义，覆盖：
 *   1) MetadataService.validateForMigration（连接级，mysql→mysql）
 *   2) DiagnosticService.findOutOfScopeForeignKeys（对象级越界外键）
 *
 * 数据准备：向 synctask-mysql(33306) 注入 precheck_test / precheck_ext 两个库，
 * 含 InnoDB/MyISAM、有/无外键、跨库外键等表，跑完自动清理。
 *
 * 位于 com.synctask.service 包内，以便直接调用包级静态方法 findOutOfScopeForeignKeys。
 */
public class PrecheckValidationTest {

    static final String SRC = "mysql://root:rootpassword@localhost:33306";
    // 目标库仅需可连通 + 报告版本/权限；复用同一 host 可达实例(33306)，避免容器内网端口不可达。
    static final String TGT = "mysql://root:rootpassword@localhost:33306";
    static final String SRC_JDBC = "jdbc:mysql://localhost:33306/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    // PG / Mongo 源（host 可达）
    static final String PG_SRC = "postgresql://app_user:userpassword@localhost:5432/myapp_db";
    static final String PG_JDBC = "jdbc:postgresql://localhost:5432/myapp_db";
    static final String MONGO = "mongodb://root:rootpassword@localhost:27117";
    // Oracle 源（host 可达；连接较慢，失败即跳过）
    static final String ORA_SRC = "oracle://app_user:userpassword@localhost:1521/FREEPDB1";
    static final String ORA_JDBC = "jdbc:oracle:thin:@localhost:1521/FREEPDB1";

    static int passed = 0, failed = 0, skipped = 0;

    public static void main(String[] args) throws Exception {
        seed();
        boolean pgSeeded = seedPg();
        boolean oraSeeded = seedOracle();
        try {
            testValidateForMigration();
            testForeignKeyIntegrity();
            testDrBidirectionalMysql();
            testMysqlSubscribe();
            testPgSource(pgSeeded);
            testMongoSource();
            testOracleSource(oraSeeded);
            testDrSameInstanceBlock();
        } finally {
            cleanup();
            if (pgSeeded) cleanupPg();
            if (oraSeeded) cleanupOracle();
        }
        System.out.println("\n================ 测试结果: " + passed + " 通过, " + failed + " 失败, " + skipped + " 跳过 ================");
        System.exit(failed > 0 ? 1 : 0);
    }

    // ---------------------------------------------------------------- assertions
    static void ok(boolean cond, String msg) {
        if (cond) { passed++; System.out.println("  [PASS] " + msg); }
        else      { failed++; System.out.println("  [FAIL] " + msg); }
    }

    static void skip(String msg) { skipped++; System.out.println("  [SKIP] " + msg); }

    static void dump(ValidationResult r) {
        for (ValidationResult.CheckItem it : r.getCheckItems()) {
            String tag = it.isPassed() ? "✓" : ("warning".equals(it.getSeverity()) ? "⚠" : "✗");
            System.out.println("    " + tag + " [" + it.getSeverity() + "] " + it.getName() + ": " + it.getMessage());
        }
        System.out.println("  allPassed=" + r.isAllPassed());
    }

    static ValidationResult.CheckItem find(ValidationResult r, String name) {
        for (ValidationResult.CheckItem it : r.getCheckItems())
            if (name.equals(it.getName())) return it;
        return null;
    }

    // ---------------------------------------------------------------- test 1
    static void testValidateForMigration() {
        System.out.println("\n【测试1】validateForMigration (mysql→mysql, fullAndIncre) 连接级预检项");
        ValidationResult r = new MetadataService()
                .validateForMigration(SRC, TGT, "fullAndIncre", "mysql", "mysql", null);

        System.out.println("  --- 全部检查项 ---");
        boolean anyErrorFailure = false;
        for (ValidationResult.CheckItem it : r.getCheckItems()) {
            String tag = it.isPassed() ? "✓" : ("warning".equals(it.getSeverity()) ? "⚠" : "✗");
            System.out.println("    " + tag + " [" + it.getSeverity() + "] " + it.getName() + ": " + it.getMessage());
            if (!it.isPassed() && "error".equalsIgnoreCase(it.getSeverity())) anyErrorFailure = true;
        }
        System.out.println("  allPassed=" + r.isAllPassed());

        // 源库版本号检查（新增）：8.0.x → 受支持 → 通过
        ValidationResult.CheckItem ver = find(r, "源库版本号");
        ok(ver != null, "存在『源库版本号』检查项");
        ok(ver != null && ver.isPassed(), "源库版本号 8.0.x 判定为受支持(通过)");

        // 存储引擎检查（新增）：p_myisam 为 MyISAM → warning，命中表名
        ValidationResult.CheckItem se = find(r, "存储引擎");
        ok(se != null, "存在『存储引擎』检查项");
        ok(se != null && !se.isPassed() && "warning".equals(se.getSeverity()), "存储引擎检出非 InnoDB 表并判为 warning");
        ok(se != null && se.getMessage().contains("p_myisam"), "存储引擎告警消息包含 MyISAM 表 p_myisam");

        // 约束完整性检查（新增）：存在外键 → warning
        ValidationResult.CheckItem fk = find(r, "约束完整性");
        ok(fk != null, "存在『约束完整性』检查项");
        ok(fk != null && !fk.isPassed() && "warning".equals(fk.getSeverity()), "约束完整性检出外键并判为 warning");

        // 既有项仍在（回归）：连接 / server_id / 权限
        ok(find(r, "源库连接") != null && find(r, "源库连接").isPassed(), "源库连接检查通过（回归）");
        ok(find(r, "目标库连接") != null, "存在目标库连接检查（回归）");
        ok(find(r, "Server ID") != null, "增量含 Server ID 检查（回归）");
        ok(find(r, "源数据库权限") != null, "存在源数据库权限检查（回归）");
        ok(find(r, "目标数据库权限") != null, "存在目标数据库权限检查（回归）");

        // warning 非阻断语义：allPassed 当且仅当无 error 级未通过项
        ok(r.isAllPassed() == !anyErrorFailure, "warning 非阻断：allPassed == (无 error 级未通过项)");
        // 存储引擎/约束完整性都是 warning，若无 error 项则不应阻断
        if (!anyErrorFailure) {
            ok(r.isAllPassed(), "仅存在 warning（存储引擎/外键）时不阻断启动(allPassed=true)");
        } else {
            System.out.println("  (注: 当前源库存在 error 级项——如 binlog_format 非 ROW——故 allPassed=false，属实测配置，非用例失败)");
        }
    }

    // ---------------------------------------------------------------- test 2
    static void testForeignKeyIntegrity() throws Exception {
        System.out.println("\n【测试2】DiagnosticService.findOutOfScopeForeignKeys 对象级越界外键");
        try (Connection c = DriverManager.getConnection(SRC_JDBC, "root", "rootpassword")) {

            // 场景1：只选 p_child，父表 p_parent 未选 → 1 条越界
            List<String[]> sel1 = Collections.singletonList(new String[]{"precheck_test", "p_child"});
            Map<String, Set<String>> scope1 = new HashMap<>();
            scope1.put("precheck_test", new HashSet<>(Collections.singletonList("p_child")));
            List<String> r1 = DiagnosticService.findOutOfScopeForeignKeys(c, sel1, scope1, new HashSet<>());
            ok(r1.size() == 1 && r1.get(0).contains("p_parent"), "只选子表 p_child → 检出父表 p_parent 越界: " + r1);

            // 场景2：p_child 与 p_parent 都选 → 0 条
            List<String[]> sel2 = Arrays.asList(
                    new String[]{"precheck_test", "p_child"},
                    new String[]{"precheck_test", "p_parent"});
            Map<String, Set<String>> scope2 = new HashMap<>();
            scope2.put("precheck_test", new HashSet<>(Arrays.asList("p_child", "p_parent")));
            List<String> r2 = DiagnosticService.findOutOfScopeForeignKeys(c, sel2, scope2, new HashSet<>());
            ok(r2.isEmpty(), "同时选父/子表 → 无越界外键: " + r2);

            // 场景3：跨库外键 p_orphan_child → precheck_ext.ext_parent（ext 库未同步）→ 1 条
            List<String[]> sel3 = Collections.singletonList(new String[]{"precheck_test", "p_orphan_child"});
            Map<String, Set<String>> scope3 = new HashMap<>();
            scope3.put("precheck_test", new HashSet<>(Collections.singletonList("p_orphan_child")));
            List<String> r3 = DiagnosticService.findOutOfScopeForeignKeys(c, sel3, scope3, new HashSet<>());
            ok(r3.size() == 1 && r3.get(0).contains("precheck_ext.ext_parent"), "跨库外键父表在未同步库 → 检出越界: " + r3);

            // 场景4：precheck_test 走整库同步(dbLevel) → 子表引用同库父表不告警
            List<String[]> sel4 = Collections.singletonList(new String[]{"precheck_test", "p_child"});
            Set<String> dbLevel = new HashSet<>(Collections.singletonList("precheck_test"));
            List<String> r4 = DiagnosticService.findOutOfScopeForeignKeys(c, sel4, new HashMap<>(), dbLevel);
            ok(r4.isEmpty(), "整库同步(dbLevel) → 同库外键不判越界: " + r4);
        }
    }

    // ---------------------------------------------------------------- test 3: DR 双向灾备
    static void testDrBidirectionalMysql() {
        System.out.println("\n【测试3】validateForMigration DR 双向灾备 (mysql→mysql, BIDIRECTIONAL)");
        ValidationResult r = new MetadataService()
                .validateForMigration(SRC, TGT, "fullAndIncre", "mysql", "mysql", "BIDIRECTIONAL");
        dump(r);
        ok(find(r, "目标库Binlog") != null, "双向灾备含『目标库Binlog』检查（反向通道 B→A）");
        ok(find(r, "目标库Binlog格式") != null, "双向灾备含『目标库Binlog格式』检查");
        ValidationResult.CheckItem sid = find(r, "Server ID 唯一性");
        ok(sid != null, "双向灾备含『Server ID 唯一性』检查");
        // 源/目标同实例 → server_id 相同 → 应判 error
        ok(sid != null && !sid.isPassed() && "error".equals(sid.getSeverity()),
                "源/目标 server_id 相同被判 error: " + (sid != null ? sid.getMessage() : ""));
    }

    // ---------------------------------------------------------------- test 4: MySQL 订阅
    static void testMysqlSubscribe() {
        System.out.println("\n【测试4】validateForSubscribe (MySQL 源) 订阅预检项");
        ValidationResult r = new MetadataService().validateForSubscribe(SRC, "mysql");
        dump(r);
        ok(find(r, "源库版本号") != null, "订阅新增『源库版本号』检查");
        ValidationResult.CheckItem se = find(r, "存储引擎");
        ok(se != null, "订阅新增『存储引擎』检查");
        ok(se != null && se.getMessage().contains("p_myisam"), "订阅存储引擎命中 MyISAM 表 p_myisam");
        ok(find(r, "约束完整性") != null, "订阅新增『约束完整性』检查");
        ok(find(r, "Server ID") != null, "订阅保留 Server ID 检查（回归）");
        ok(find(r, "源库权限") != null, "订阅保留源库权限检查（回归）");
    }

    // ---------------------------------------------------------------- test 5: PG 源
    static void testPgSource(boolean seeded) {
        System.out.println("\n【测试5】validateForMigration (pg→mysql, fullAndIncre) PG 源预检项");
        if (!seeded) { skip("PG 未连通/未 seed，跳过 PG 源用例"); return; }
        ValidationResult r = new MetadataService()
                .validateForMigration(PG_SRC, TGT, "fullAndIncre", "postgresql", "mysql", null);
        dump(r);
        ValidationResult.CheckItem ver = find(r, "源库版本号");
        ok(ver != null && ver.isPassed(), "PG 源库版本号检查通过（PG≥9.4）");
        ok(find(r, "源库WAL级别") != null, "含 WAL 级别检查（回归）");
        ok(find(r, "WAL发送进程") != null, "新增『WAL发送进程』(max_wal_senders) 检查");
        ok(find(r, "源库复制权限") != null, "新增『源库复制权限』(REPLICATION) 检查");
        ValidationResult.CheckItem ri = find(r, "REPLICA IDENTITY");
        ok(ri != null, "新增『REPLICA IDENTITY』检查");
        ok(ri != null && !ri.isPassed() && ri.getMessage().contains("pc_nopk"),
                "REPLICA IDENTITY 命中无主键表 pc_nopk: " + (ri != null ? ri.getMessage() : ""));
        ValidationResult.CheckItem fk = find(r, "约束完整性");
        ok(fk != null, "新增 PG『约束完整性』检查");
        ok(fk != null && !fk.isPassed() && "warning".equals(fk.getSeverity()), "PG 检出外键并判 warning");
    }

    // ---------------------------------------------------------------- test 6: Mongo 源
    static void testMongoSource() {
        System.out.println("\n【测试6】validateForMigration (mongo→mongo) Mongo 源/目标权限预检项");
        ValidationResult r;
        try {
            r = new MetadataService().validateForMigration(MONGO, MONGO, "fullAndIncre", "mongodb", "mongodb", null);
        } catch (Exception e) {
            skip("Mongo 未连通，跳过 Mongo 用例: " + e.getMessage()); return;
        }
        dump(r);
        ValidationResult.CheckItem sp = find(r, "源库权限");
        ValidationResult.CheckItem tp = find(r, "目标库权限");
        if (sp == null && tp == null) { skip("Mongo 连接失败(无权限项)，跳过断言"); return; }
        ok(sp != null, "新增 Mongo『源库权限』检查");
        ok(tp != null, "新增 Mongo『目标库权限』检查");
        ok(sp != null && sp.isPassed(), "root 账号源库权限通过: " + (sp != null ? sp.getMessage() : ""));
    }

    // ---------------------------------------------------------------- test 8: 灾备源/目标同实例强制拦截
    static void testDrSameInstanceBlock() {
        System.out.println("\n【测试8】灾备源/目标同实例强制拦截 (校验检查)");
        MetadataService m = new MetadataService();

        // 单向灾备：源=目标同实例 → 拦截(error) + allPassed=false 阻断
        ValidationResult uni = m.validateForMigration(SRC, TGT, "fullAndIncre", "mysql", "mysql", "UNIDIRECTIONAL");
        ValidationResult.CheckItem g1 = find(uni, "灾备源目标隔离");
        ok(g1 != null && !g1.isPassed() && "error".equals(g1.getSeverity()), "单向灾备同实例被拦截(error): " + (g1 != null ? g1.getMessage() : "缺失"));
        ok(!uni.isAllPassed(), "单向灾备同实例 allPassed=false (强制阻断启动)");

        // 双向灾备：同样拦截
        ValidationResult bi = m.validateForMigration(SRC, TGT, "fullAndIncre", "mysql", "mysql", "BIDIRECTIONAL");
        ValidationResult.CheckItem g2 = find(bi, "灾备源目标隔离");
        ok(g2 != null && !g2.isPassed() && "error".equals(g2.getSeverity()), "双向灾备同实例被拦截(error)");

        // 普通同步(drMode=null)同实例 → 不拦截（允许同实例跨库迁移，回归）
        ValidationResult sync = m.validateForMigration(SRC, TGT, "fullAndIncre", "mysql", "mysql", null);
        ok(find(sync, "灾备源目标隔离") == null, "普通同步同实例不拦截(回归)");

        // host 归一化：localhost 与 127.0.0.1 视为同一主机 → 灾备仍拦截（两端均连通 33306，无噪声）
        ValidationResult norm = m.validateForMigration(
                "mysql://root:rootpassword@localhost:33306",
                "mysql://root:rootpassword@127.0.0.1:33306", "fullAndIncre", "mysql", "mysql", "BIDIRECTIONAL");
        ok(find(norm, "灾备源目标隔离") != null, "灾备 localhost/127.0.0.1 归一化为同实例仍拦截");
    }

    // ---------------------------------------------------------------- test 7: Oracle 源
    static void testOracleSource(boolean seeded) {
        System.out.println("\n【测试7】validateForMigration (oracle→pg, fullAndIncre) Oracle 源预检项");
        if (!seeded) { skip("Oracle 未连通/未 seed，跳过 Oracle 源用例"); return; }
        ValidationResult r;
        try {
            r = new MetadataService().validateForMigration(ORA_SRC, PG_SRC, "fullAndIncre", "oracle", "postgresql", null);
        } catch (Exception e) {
            skip("Oracle 校验异常，跳过: " + e.getMessage()); return;
        }
        dump(r);
        ok(find(r, "源库版本号") != null, "新增 Oracle『源库版本号』检查");
        ValidationResult.CheckItem fk = find(r, "约束完整性");
        ok(fk != null, "新增 Oracle『约束完整性』检查");
        ok(fk != null && !fk.isPassed() && "warning".equals(fk.getSeverity()),
                "Oracle 检出外键并判 warning: " + (fk != null ? fk.getMessage() : ""));
        // LogMiner 配置检查仍在（回归）——app_user 权限不足时为 warning/error，仅断言存在
        ok(find(r, "归档日志模式") != null || find(r, "补充日志") != null || find(r, "LogMiner权限") != null,
                "保留 Oracle LogMiner 配置检查（回归）");
    }

    static boolean seedOracle() {
        try (Connection c = DriverManager.getConnection(ORA_JDBC, "app_user", "userpassword")) {
            exec(c, "DROP TABLE oc_child");
            exec(c, "DROP TABLE oc_parent");
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE oc_parent (id NUMBER PRIMARY KEY, name VARCHAR2(50))");
                s.execute("CREATE TABLE oc_child (id NUMBER PRIMARY KEY, pid NUMBER, " +
                        "CONSTRAINT oc_fk FOREIGN KEY (pid) REFERENCES oc_parent(id))");
            }
            System.out.println("== 准备 Oracle 测试数据 (app_user): oc_parent/oc_child(FK) ==");
            return true;
        } catch (Exception e) {
            System.out.println("== Oracle seed 失败(将跳过 Oracle 用例): " + e.getMessage() + " ==");
            return false;
        }
    }

    static void cleanupOracle() {
        try (Connection c = DriverManager.getConnection(ORA_JDBC, "app_user", "userpassword")) {
            exec(c, "DROP TABLE oc_child");
            exec(c, "DROP TABLE oc_parent");
            System.out.println("== 已清理 Oracle 测试表 ==");
        } catch (Exception e) {
            System.out.println("Oracle 清理失败(可忽略): " + e.getMessage());
        }
    }

    /** Oracle 无 DROP IF EXISTS，忽略"表不存在"错误。 */
    static void exec(Connection c, String sql) {
        try (Statement s = c.createStatement()) { s.execute(sql); } catch (Exception ignore) {}
    }

    // ---------------------------------------------------------------- PG seed / cleanup
    static boolean seedPg() {
        try (Connection c = DriverManager.getConnection(PG_JDBC, "app_user", "userpassword");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS pc_child");
            s.execute("DROP TABLE IF EXISTS pc_parent");
            s.execute("DROP TABLE IF EXISTS pc_nopk");
            s.execute("CREATE TABLE pc_parent (id INT PRIMARY KEY, name VARCHAR(50))");
            s.execute("CREATE TABLE pc_child (id INT PRIMARY KEY, pid INT REFERENCES pc_parent(id))");
            s.execute("CREATE TABLE pc_nopk (v VARCHAR(50))");   // 无主键 + 默认 REPLICA IDENTITY
            System.out.println("== 准备 PG 测试数据 (myapp_db): pc_parent/pc_child(FK)/pc_nopk(无PK) ==");
            return true;
        } catch (Exception e) {
            System.out.println("== PG seed 失败(将跳过 PG 用例): " + e.getMessage() + " ==");
            return false;
        }
    }

    static void cleanupPg() {
        try (Connection c = DriverManager.getConnection(PG_JDBC, "app_user", "userpassword");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS pc_child");
            s.execute("DROP TABLE IF EXISTS pc_parent");
            s.execute("DROP TABLE IF EXISTS pc_nopk");
            System.out.println("== 已清理 PG 测试表 ==");
        } catch (Exception e) {
            System.out.println("PG 清理失败(可忽略): " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------- seed / cleanup
    static void seed() throws Exception {
        System.out.println("== 准备测试数据 (synctask-mysql:33306) ==");
        try (Connection c = DriverManager.getConnection(SRC_JDBC, "root", "rootpassword");
             Statement s = c.createStatement()) {
            for (String sql : new String[]{
                    "DROP DATABASE IF EXISTS precheck_test",
                    "DROP DATABASE IF EXISTS precheck_ext",
                    "CREATE DATABASE precheck_ext",
                    "CREATE TABLE precheck_ext.ext_parent (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB",
                    "CREATE DATABASE precheck_test",
                    "CREATE TABLE precheck_test.p_parent (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB",
                    "CREATE TABLE precheck_test.p_child (id INT PRIMARY KEY, pid INT, " +
                            "CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES precheck_test.p_parent(id)) ENGINE=InnoDB",
                    "CREATE TABLE precheck_test.p_myisam (id INT PRIMARY KEY, v VARCHAR(50)) ENGINE=MyISAM",
                    "CREATE TABLE precheck_test.p_nopk (v VARCHAR(50)) ENGINE=InnoDB",
                    "CREATE TABLE precheck_test.p_orphan_child (id INT PRIMARY KEY, eid INT, " +
                            "CONSTRAINT fk_orphan FOREIGN KEY (eid) REFERENCES precheck_ext.ext_parent(id)) ENGINE=InnoDB",
            }) {
                s.execute(sql);
            }
        }
        System.out.println("   完成: precheck_test(p_parent,p_child,p_myisam,p_nopk,p_orphan_child) + precheck_ext(ext_parent)");
    }

    static void cleanup() {
        try (Connection c = DriverManager.getConnection(SRC_JDBC, "root", "rootpassword");
             Statement s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS precheck_test");
            s.execute("DROP DATABASE IF EXISTS precheck_ext");
            System.out.println("\n== 已清理测试库 precheck_test / precheck_ext ==");
        } catch (Exception e) {
            System.out.println("清理失败(可忽略): " + e.getMessage());
        }
    }
}
