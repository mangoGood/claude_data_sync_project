package com.migration.traffic.capture.oracle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle 语句里的绑定占位符（{@code :b1} / {@code :1} / {@code :name}）扫描与改写。
 *
 * <p>与 PG 同一个理由：JDBC 只认 {@code ?}。Oracle 的 thin 驱动确实也认 {@code :name}，
 * 但<b>重复引用的语义不一样</b>——同名占位符出现两次时，位置绑定要绑两次，
 * 而审计里的 {@code SQL_BINDS} 对同一个名字只给一份值。统一改写成 {@code ?}
 * 并按名字展开，两种情形就都对了。
 *
 * <p>扫描要跳过字符串字面量、双引号标识符与注释——{@code ':b1'} 在引号里就是普通文本。
 */
public final class OraclePlaceholders {

    private OraclePlaceholders() {
    }

    /** 改写结果：JDBC 形态的 SQL + 每个 {@code ?} 对应的占位符名字（按出现顺序）。 */
    public static final class Rewritten {
        public final String sql;
        public final List<String> names;

        Rewritten(String sql, List<String> names) {
            this.sql = sql;
            this.names = names;
        }
    }

    public static Rewritten rewrite(String sql) {
        if (sql == null || sql.indexOf(':') < 0) {
            return new Rewritten(sql, List.of());
        }
        StringBuilder out = new StringBuilder(sql.length());
        List<String> names = new ArrayList<>();
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = copyQuoted(sql, i, '\'', out);
            } else if (c == '"') {
                i = copyQuoted(sql, i, '"', out);
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int nl = sql.indexOf('\n', i);
                int end = nl < 0 ? n : nl + 1;
                out.append(sql, i, end);
                i = end;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int close = sql.indexOf("*/", i + 2);
                int end = close < 0 ? n : close + 2;
                out.append(sql, i, end);
                i = end;
            } else if (c == ':' && i + 1 < n && isNameStart(sql.charAt(i + 1))) {
                int b = i + 1;
                int e = b;
                while (e < n && isNameChar(sql.charAt(e))) e++;
                names.add(sql.substring(b, e).toUpperCase(java.util.Locale.ROOT));
                out.append('?');
                i = e;
            } else {
                out.append(c);
                i++;
            }
        }
        return new Rewritten(out.toString(), names);
    }

    /**
     * 把审计里的绑定值按占位符出现顺序展开。
     *
     * <p>{@code SQL_BINDS} 是<b>按绑定位置</b>给的（{@code #1 #2 …}），而占位符可能重复引用。
     * 名字第一次出现的顺序 = 绑定位置顺序，据此建立"名字 → 值"，再按出现顺序展开。
     */
    public static List<String> expand(List<String> names, List<String> values) {
        if (names == null || names.isEmpty()) return values;
        Map<String, String> byName = new LinkedHashMap<>();
        int pos = 0;
        for (String name : names) {
            if (byName.containsKey(name)) continue;
            byName.put(name, values != null && pos < values.size() ? values.get(pos) : null);
            pos++;
        }
        List<String> out = new ArrayList<>(names.size());
        for (String name : names) {
            out.add(byName.get(name));
        }
        return out;
    }

    private static int copyQuoted(String s, int i, char q, StringBuilder out) {
        int n = s.length();
        out.append(q);
        i++;
        while (i < n) {
            char c = s.charAt(i);
            out.append(c);
            i++;
            if (c == q) {
                if (i < n && s.charAt(i) == q) {
                    out.append(q);
                    i++;
                } else {
                    break;
                }
            }
        }
        return i;
    }

    private static boolean isNameStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isNameChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '#' || c == '$';
    }
}
