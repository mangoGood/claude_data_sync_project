package com.migration.increment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SqlTestCaseGenerator {
    private final MySqlVersion version;
    private final Random random = new Random(42);

    public SqlTestCaseGenerator(MySqlVersion version) {
        this.version = version;
    }

    public static class TestCase {
        private final String category;
        private final String subCategory;
        private final String sql;
        private final String description;
        private final MySqlVersion minVersion;

        public TestCase(String category, String subCategory, String sql, String description, MySqlVersion minVersion) {
            this.category = category;
            this.subCategory = subCategory;
            this.sql = sql;
            this.description = description;
            this.minVersion = minVersion;
        }

        public TestCase(String category, String subCategory, String sql, String description) {
            this(category, subCategory, sql, description, MySqlVersion.V5_6);
        }

        public String getCategory() { return category; }
        public String getSubCategory() { return subCategory; }
        public String getSql() { return sql; }
        public String getDescription() { return description; }
        public MySqlVersion getMinVersion() { return minVersion; }
        public boolean isSupported(MySqlVersion target) { return target.isAtLeast(minVersion); }

        @Override
        public String toString() {
            return "[" + category + "/" + subCategory + "] " + description + " (min=" + minVersion + ")";
        }
    }

    public List<TestCase> generateAll() {
        List<TestCase> cases = new ArrayList<>();
        cases.addAll(new DmlGenerator(version).generate());
        cases.addAll(new DdlGenerator(version).generate());
        cases.addAll(new DclGenerator(version).generate());
        return cases;
    }

    public List<TestCase> generateDmlTestCases() { return new DmlGenerator(version).generate(); }
    public List<TestCase> generateDdlTestCases() { return new DdlGenerator(version).generate(); }
    public List<TestCase> generateDclTestCases() { return new DclGenerator(version).generate(); }

    static String pad2(int n) { return n < 10 ? "0" + n : String.valueOf(n); }
    static String hex(int n) { return String.format("%02x", n & 0xff); }
    static String randomHex(int len) {
        StringBuilder sb = new StringBuilder();
        Random r = new Random();
        for (int i = 0; i < len; i++) sb.append(String.format("%02x", r.nextInt(256)));
        return sb.toString();
    }
    static String randomDecimal() {
        Random r = new Random();
        return String.format("%d.%02d", r.nextInt(100000), r.nextInt(100));
    }
    static String randomString(int len) {
        StringBuilder sb = new StringBuilder();
        Random r = new Random();
        for (int i = 0; i < len; i++) sb.append((char) ('a' + r.nextInt(26)));
        return sb.toString();
    }
    static String bitString(int len) {
        StringBuilder sb = new StringBuilder();
        Random r = new Random();
        for (int i = 0; i < len; i++) sb.append(r.nextInt(2));
        return sb.toString();
    }
    static String specialStr(int idx) {
        String[] specials = {
            "it's", "O'Brien", "test\"quote", "back\\slash",
            "new\nline", "tab\there", "carriage\rreturn",
            "percent%s", "under_score", "dash-ed",
            "dot.ted", "semi;colon", "colon:here",
            "exclam!ation", "at@sign", "hash#tag",
            "dollar$sign", "amp&ersand", "aster*isk",
            "plus+sign", "equal=sign", "question?mark",
            "angle<bracket", "curly{brace", "square[bracket",
            "pipe|char", "tilde~char", "backtick`char",
            "caret^char", "apostrophe'"
        };
        return specials[idx % specials.length];
    }
    static String escape(String s) {
        return s.replace("\\", "\\\\").replace("'", "\\'").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
    static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
    static String emoji(int idx) {
        try { return new String(Character.toChars(0x1F600 + idx)); }
        catch (Exception e) { return "E" + idx; }
    }
}
