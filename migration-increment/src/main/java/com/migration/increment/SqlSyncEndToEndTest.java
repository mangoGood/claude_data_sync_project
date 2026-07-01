package com.migration.increment;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlSyncEndToEndTest {
    private final String targetUrl;
    private final String targetUser;
    private final String targetPass;
    private final MySqlVersion targetVersion;
    private final SqlClassifier classifier;

    private final AtomicInteger totalTests = new AtomicInteger(0);
    private final AtomicInteger passedTests = new AtomicInteger(0);
    private final AtomicInteger failedTests = new AtomicInteger(0);
    private final AtomicInteger skippedTests = new AtomicInteger(0);
    private final List<TestResult> failures = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, AtomicInteger> failByCategory = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failBySubCategory = new ConcurrentHashMap<>();

    private Connection conn;
    private static final String DB = "sync_test";
    private String lastSubCategory = "";

    public SqlSyncEndToEndTest(String targetUrl, String targetUser, String targetPass, MySqlVersion targetVersion) {
        this.targetUrl = targetUrl;
        this.targetUser = targetUser;
        this.targetPass = targetPass;
        this.targetVersion = targetVersion;
        this.classifier = new SqlClassifier();
    }

    public static class TestResult {
        private final SqlTestCaseGenerator.TestCase testCase;
        private final boolean passed;
        private final boolean skipped;
        private final String errorMessage;
        private final String errorSqlState;
        private final int errorCode;
        private final long durationMs;

        public TestResult(SqlTestCaseGenerator.TestCase tc, boolean passed, boolean skipped,
                          String errorMessage, String errorSqlState, int errorCode, long durationMs) {
            this.testCase = tc;
            this.passed = passed;
            this.skipped = skipped;
            this.errorMessage = errorMessage;
            this.errorSqlState = errorSqlState;
            this.errorCode = errorCode;
            this.durationMs = durationMs;
        }

        public boolean isPassed() { return passed; }
        public boolean isSkipped() { return skipped; }
        public String getErrorMessage() { return errorMessage; }
        public String getErrorSqlState() { return errorSqlState; }
        public int getErrorCode() { return errorCode; }
        public SqlTestCaseGenerator.TestCase getTestCase() { return testCase; }
        public long getDurationMs() { return durationMs; }
    }

    public void runAllTests() {
        SqlTestCaseGenerator generator = new SqlTestCaseGenerator(targetVersion);
        List<SqlTestCaseGenerator.TestCase> allCases = generator.generateAll();

        System.out.println("=== SQL Sync End-to-End Test ===");
        System.out.println("Target: " + targetUrl + " (MySQL " + targetVersion + ")");
        System.out.println("Total test cases: " + allCases.size());

        try {
            connect();
            setupDatabase();
            for (SqlTestCaseGenerator.TestCase tc : allCases) {
                checkSubCategoryChange(tc);
                TestResult result = runSingleTest(tc);
                recordResult(result);
            }
        } catch (SQLException e) {
            System.err.println("Connection error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            disconnect();
        }

        printSummary();
    }

    public void runCategoryTests(String category) {
        SqlTestCaseGenerator generator = new SqlTestCaseGenerator(targetVersion);
        List<SqlTestCaseGenerator.TestCase> cases;
        switch (category.toUpperCase()) {
            case "DML": cases = generator.generateDmlTestCases(); break;
            case "DDL": cases = generator.generateDdlTestCases(); break;
            case "DCL": cases = generator.generateDclTestCases(); break;
            default: cases = generator.generateAll(); break;
        }

        System.out.println("=== " + category + " Sync Test ===");
        System.out.println("Test cases: " + cases.size());

        try {
            connect();
            setupDatabase();
            for (SqlTestCaseGenerator.TestCase tc : cases) {
                checkSubCategoryChange(tc);
                TestResult result = runSingleTest(tc);
                recordResult(result);
            }
        } catch (SQLException e) {
            System.err.println("Connection error: " + e.getMessage());
        } finally {
            disconnect();
        }

        printSummary();
    }

    private void checkSubCategoryChange(SqlTestCaseGenerator.TestCase tc) {
        String currentSub = tc.getSubCategory();
        String currentCat = tc.getCategory();
        if (!currentSub.equals(lastSubCategory)) {
            if (!lastSubCategory.isEmpty() && "DML".equals(currentCat)) {
                try { resetDmlData(); } catch (Exception ignored) {}
            }
            lastSubCategory = currentSub;
        }
    }

    private void connect() throws SQLException {
        String url = targetUrl;
        if (!url.contains("?")) {
            url += "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        } else {
            if (!url.contains("useSSL")) url += "&useSSL=false";
            if (!url.contains("allowPublicKeyRetrieval")) url += "&allowPublicKeyRetrieval=true";
        }
        conn = DriverManager.getConnection(url, targetUser, targetPass);
        conn.setAutoCommit(true);
        System.out.println("Connected to MySQL: " + url);
    }

    private void disconnect() {
        if (conn != null) {
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    private void setupDatabase() throws SQLException {
        executeSilent("CREATE DATABASE IF NOT EXISTS " + DB);
        executeSilent("USE " + DB);
        executeSilent("SET GLOBAL event_scheduler = ON");

        cleanupDdlTables();
        cleanupDclUsers();

        String[] dropTables = {
            "DROP TABLE IF EXISTS t_dml", "DROP TABLE IF EXISTS t_dml_copy",
            "DROP TABLE IF EXISTS t_timestamp", "DROP TABLE IF EXISTS t_blob",
            "DROP TABLE IF EXISTS t_decimal", "DROP TABLE IF EXISTS t_boolean",
            "DROP TABLE IF EXISTS t_enum", "DROP TABLE IF EXISTS t_set",
            "DROP TABLE IF EXISTS t_json", "DROP TABLE IF EXISTS t_year",
            "DROP TABLE IF EXISTS t_bit", "DROP TABLE IF EXISTS t_auto",
            "DROP TABLE IF EXISTS t_log",
        };
        for (String sql : dropTables) executeSilent(sql);

        String[] createTables = {
            "CREATE TABLE t_dml (id INT PRIMARY KEY, name VARCHAR(255), age INT)",
            "CREATE TABLE t_dml_copy (id INT PRIMARY KEY, name VARCHAR(255), age INT)",
            "CREATE TABLE t_timestamp (id INT PRIMARY KEY, ts TIMESTAMP NULL, dt DATETIME, d DATE)",
            "CREATE TABLE t_blob (id INT PRIMARY KEY, data BLOB)",
            "CREATE TABLE t_decimal (id INT PRIMARY KEY, amount DECIMAL(15,2))",
            "CREATE TABLE t_boolean (id INT PRIMARY KEY, flag BOOLEAN)",
            "CREATE TABLE t_enum (id INT PRIMARY KEY, status ENUM('a','b','c'))",
            "CREATE TABLE t_set (id INT PRIMARY KEY, tags SET('x','y','z'))",
            "CREATE TABLE t_year (id INT PRIMARY KEY, yr YEAR)",
            "CREATE TABLE t_bit (id INT PRIMARY KEY, flags BIT(8))",
            "CREATE TABLE t_auto (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), age INT)",
            "CREATE TABLE t_log (id INT PRIMARY KEY AUTO_INCREMENT, action VARCHAR(50), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        };
        for (String sql : createTables) executeSilent(sql);

        if (targetVersion.isAtLeast(MySqlVersion.V5_7)) {
            executeSilent("CREATE TABLE IF NOT EXISTS t_json (id INT PRIMARY KEY, data JSON)");
        }

        for (int i = 0; i < 50; i++)
            executeSilent("CREATE USER IF NOT EXISTS 'grantuser_"+i+"'@'%' IDENTIFIED BY 'grantpass_"+i+"'");
        executeSilent("INSERT IGNORE INTO mysql.proxies_priv (Host, User, Proxied_host, Proxied_user, With_grant, Grantor) VALUES ('%', 'root', '', '', 1, '')");
        executeSilent("FLUSH PRIVILEGES");

        System.out.println("Test database and tables created.");
    }

    private void resetDmlData() throws SQLException {
        executeSilent("USE " + DB);
        String[] truncates = {
            "TRUNCATE TABLE t_dml", "TRUNCATE TABLE t_dml_copy",
            "TRUNCATE TABLE t_timestamp", "TRUNCATE TABLE t_blob",
            "TRUNCATE TABLE t_decimal", "TRUNCATE TABLE t_boolean",
            "TRUNCATE TABLE t_enum", "TRUNCATE TABLE t_set",
            "TRUNCATE TABLE t_json", "TRUNCATE TABLE t_year",
            "TRUNCATE TABLE t_bit", "TRUNCATE TABLE t_auto",
            "TRUNCATE TABLE t_log",
        };
        for (String sql : truncates) {
            try { executeSilent(sql); } catch (Exception ignored) {}
        }
    }

    private void cleanupDdlTables() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + DB + "' AND TABLE_NAME LIKE 'ddl_%'");
            List<String> tables = new ArrayList<>();
            while (rs.next()) tables.add(rs.getString(1));
            rs.close();
            for (String t : tables) executeSilent("DROP TABLE IF EXISTS " + t);
        } catch (Exception ignored) {}
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='" + DB + "'");
            List<String[]> routines = new ArrayList<>();
            while (rs.next()) routines.add(new String[]{rs.getString(1), rs.getString(2)});
            rs.close();
            for (String[] r : routines) executeSilent("DROP " + r[1] + " IF EXISTS " + r[0]);
        } catch (Exception ignored) {}
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='" + DB + "'");
            List<String> triggers = new ArrayList<>();
            while (rs.next()) triggers.add(rs.getString(1));
            rs.close();
            for (String t : triggers) executeSilent("DROP TRIGGER IF EXISTS " + t);
        } catch (Exception ignored) {}
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA='" + DB + "'");
            List<String> events = new ArrayList<>();
            while (rs.next()) events.add(rs.getString(1));
            rs.close();
            for (String e : events) executeSilent("DROP EVENT IF EXISTS " + e);
        } catch (Exception ignored) {}
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA='" + DB + "'");
            List<String> views = new ArrayList<>();
            while (rs.next()) views.add(rs.getString(1));
            rs.close();
            for (String v : views) executeSilent("DROP VIEW IF EXISTS " + v);
        } catch (Exception ignored) {}
    }

    private void cleanupDclUsers() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT CONCAT('''',USER,'''@''',HOST,'''') FROM mysql.user WHERE USER LIKE 'testuser%' OR USER LIKE 'grantuser%' OR USER LIKE 'hostuser%' OR USER LIKE 'nopwd%' OR USER LIKE 'renamed%' OR USER LIKE 'authuser%' OR USER LIKE 'sha256user%' OR USER LIKE 'role_%'");
            List<String> users = new ArrayList<>();
            while (rs.next()) users.add(rs.getString(1));
            rs.close();
            for (String u : users) executeSilent("DROP USER IF EXISTS " + u);
        } catch (Exception ignored) {}
    }

    private void executeSilent(String sql) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException ignored) {}
    }

    private TestResult runSingleTest(SqlTestCaseGenerator.TestCase tc) {
        long start = System.currentTimeMillis();

        if (!tc.isSupported(targetVersion)) {
            skippedTests.incrementAndGet();
            return new TestResult(tc, false, true, "Version not supported", null, 0, System.currentTimeMillis() - start);
        }

        totalTests.incrementAndGet();

        try {
            executeSilent("USE " + DB);
        } catch (Exception ignored) {}

        String rawSql = tc.getSql();
        if (rawSql.contains("DELIMITER //")) {
            String body = rawSql.replace("DELIMITER //\n", "").replace("END //\nDELIMITER ;", "END");
            body = body.trim();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(body);
            } catch (SQLException e) {
                failedTests.incrementAndGet();
                TestResult result = new TestResult(tc, false, false, e.getMessage(), e.getSQLState(), e.getErrorCode(), System.currentTimeMillis() - start);
                failures.add(result);
                failByCategory.computeIfAbsent(tc.getCategory(), k -> new AtomicInteger()).incrementAndGet();
                failBySubCategory.computeIfAbsent(tc.getSubCategory(), k -> new AtomicInteger()).incrementAndGet();
                return result;
            }
        } else {
            String[] sqls = rawSql.split(";(?=(?:[^']*'[^']*')*[^']*$)");
            for (String sql : sqls) {
                sql = sql.trim();
                if (sql.isEmpty()) continue;

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    failedTests.incrementAndGet();
                    TestResult result = new TestResult(tc, false, false, e.getMessage(), e.getSQLState(), e.getErrorCode(), System.currentTimeMillis() - start);
                    failures.add(result);
                    failByCategory.computeIfAbsent(tc.getCategory(), k -> new AtomicInteger()).incrementAndGet();
                    failBySubCategory.computeIfAbsent(tc.getSubCategory(), k -> new AtomicInteger()).incrementAndGet();
                    return result;
                }
            }
        }

        passedTests.incrementAndGet();
        return new TestResult(tc, true, false, null, null, 0, System.currentTimeMillis() - start);
    }

    private void recordResult(TestResult result) {
        if (result.isSkipped()) return;
        int total = totalTests.get();
        if (total % 200 == 0) {
            System.out.println("Progress: " + total + " tested, " + passedTests.get() + " passed, " + failedTests.get() + " failed");
        }
        if (!result.isPassed() && !result.isSkipped()) {
            SqlTestCaseGenerator.TestCase tc = result.getTestCase();
            System.err.println("  FAIL [" + tc.getCategory() + "/" + tc.getSubCategory() + "] " + tc.getDescription());
            System.err.println("    SQL: " + truncate(tc.getSql(), 150));
            System.err.println("    Error(" + result.getErrorCode() + "/" + result.getErrorSqlState() + "): " + truncate(result.getErrorMessage(), 200));
        }
    }

    private void printSummary() {
        System.out.println("\n============================================================");
        System.out.println("=== Test Summary ===");
        System.out.println("============================================================");
        System.out.println("Total:   " + totalTests.get());
        System.out.println("Passed:  " + passedTests.get());
        System.out.println("Failed:  " + failedTests.get());
        System.out.println("Skipped: " + skippedTests.get());
        if (totalTests.get() > 0) {
            System.out.printf("Pass rate: %.2f%%\n", (passedTests.get() * 100.0 / totalTests.get()));
        }

        if (!failByCategory.isEmpty()) {
            System.out.println("\n--- Failures by Category ---");
            failByCategory.entrySet().stream()
                .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                .forEach(e -> System.out.println("  " + e.getKey() + ": " + e.getValue().get()));
        }

        if (!failBySubCategory.isEmpty()) {
            System.out.println("\n--- Failures by SubCategory (top 30) ---");
            failBySubCategory.entrySet().stream()
                .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                .limit(30)
                .forEach(e -> System.out.println("  " + e.getKey() + ": " + e.getValue().get()));
        }

        if (!failures.isEmpty()) {
            System.out.println("\n--- Failed Tests Detail (first 100) ---");
            int count = 0;
            for (TestResult f : failures) {
                if (count++ >= 100) break;
                SqlTestCaseGenerator.TestCase tc = f.getTestCase();
                System.out.println("  [" + tc.getCategory() + "/" + tc.getSubCategory() + "] " + tc.getDescription());
                System.out.println("    SQL: " + truncate(tc.getSql(), 150));
                System.out.println("    Error(" + f.getErrorCode() + "/" + f.getErrorSqlState() + "): " + truncate(f.getErrorMessage(), 200));
            }
        }
    }

    private String truncate(String s, int max) {
        return s == null ? "" : (s.length() <= max ? s : s.substring(0, max) + "...");
    }

    public int getFailedCount() { return failedTests.get(); }
    public int getPassedCount() { return passedTests.get(); }

    public static void main(String[] args) {
        String tgtUrl = args.length > 0 ? args[0] : "jdbc:mysql://192.168.107.7:3306/";
        String tgtUser = args.length > 1 ? args[1] : "root";
        String tgtPass = args.length > 2 ? args[2] : "rootpassword";
        MySqlVersion tgtVer = args.length > 3 ? MySqlVersion.parse(args[3]) : MySqlVersion.V8_0;
        String category = args.length > 4 ? args[4] : "ALL";

        SqlSyncEndToEndTest test = new SqlSyncEndToEndTest(tgtUrl, tgtUser, tgtPass, tgtVer);
        if ("ALL".equalsIgnoreCase(category)) {
            test.runAllTests();
        } else {
            test.runCategoryTests(category);
        }

        System.exit(test.getFailedCount() > 0 ? 1 : 0);
    }
}
