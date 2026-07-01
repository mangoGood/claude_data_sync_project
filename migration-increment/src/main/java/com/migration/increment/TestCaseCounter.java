package com.migration.increment;

public class TestCaseCounter {
    public static void main(String[] args) {
        MySqlVersion[] versions = {MySqlVersion.V5_6, MySqlVersion.V5_7, MySqlVersion.V8_0};

        for (MySqlVersion v : versions) {
            System.out.println("\n===== MySQL " + v.toCompactString() + " =====");
            SqlTestCaseGenerator gen = new SqlTestCaseGenerator(v);

            java.util.List<SqlTestCaseGenerator.TestCase> dml = gen.generateDmlTestCases();
            java.util.List<SqlTestCaseGenerator.TestCase> ddl = gen.generateDdlTestCases();
            java.util.List<SqlTestCaseGenerator.TestCase> dcl = gen.generateDclTestCases();
            java.util.List<SqlTestCaseGenerator.TestCase> all = gen.generateAll();

            System.out.println("DML: " + dml.size());
            System.out.println("DDL: " + ddl.size());
            System.out.println("DCL: " + dcl.size());
            System.out.println("Total: " + all.size());

            java.util.Map<String, java.util.Set<String>> dmlSubs = new java.util.TreeMap<>();
            java.util.Map<String, java.util.Set<String>> ddlSubs = new java.util.TreeMap<>();
            java.util.Map<String, java.util.Set<String>> dclSubs = new java.util.TreeMap<>();

            for (SqlTestCaseGenerator.TestCase tc : dml) {
                dmlSubs.computeIfAbsent(tc.getSubCategory().split("_")[0], k -> new java.util.TreeSet<>()).add(tc.getSubCategory());
            }
            for (SqlTestCaseGenerator.TestCase tc : ddl) {
                ddlSubs.computeIfAbsent(tc.getSubCategory().split("_")[0], k -> new java.util.TreeSet<>()).add(tc.getSubCategory());
            }
            for (SqlTestCaseGenerator.TestCase tc : dcl) {
                dclSubs.computeIfAbsent(tc.getSubCategory().split("_")[0], k -> new java.util.TreeSet<>()).add(tc.getSubCategory());
            }

            System.out.println("\nDML subcategories:");
            dmlSubs.forEach((k, subs) -> System.out.println("  " + k + ": " + subs.size() + " types"));
            System.out.println("DDL subcategories:");
            ddlSubs.forEach((k, subs) -> System.out.println("  " + k + ": " + subs.size() + " types"));
            System.out.println("DCL subcategories:");
            dclSubs.forEach((k, subs) -> System.out.println("  " + k + ": " + subs.size() + " types"));
        }
    }
}
