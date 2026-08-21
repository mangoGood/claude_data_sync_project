package com.migration.traffic.replay.dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * 把 PG 的 {@code $n} 占位符改写成 JDBC 的 {@code ?}，并按出现顺序展开绑定值。
 *
 * <p>不能用正则：{@code $1} 出现在字符串字面量、双引号标识符、美元引用块（{@code $$…$$}）
 * 或注释里时<b>不是占位符</b>，改了就把语句本身改坏了。所以这里走一遍词法扫描。
 *
 * <p>典型输入输出：
 * <pre>
 * INSERT INTO t VALUES ($1,$2)          binds=[a,b]   →  INSERT INTO t VALUES (?,?)        binds=[a,b]
 * SELECT * FROM t WHERE a=$1 OR b=$1    binds=[x]     →  SELECT * FROM t WHERE a=? OR b=?  binds=[x,x]
 * SELECT '$1'                           binds=[]      →  SELECT '$1'                       （不动）
 * </pre>
 */
final class PgPlaceholderRewriter {

    private PgPlaceholderRewriter() {
    }

    static PreparedPlan rewrite(String sql, List<String> binds) {
        if (sql == null || sql.indexOf('$') < 0) {
            return new PreparedPlan(sql, binds);
        }
        StringBuilder out = new StringBuilder(sql.length());
        List<Integer> order = new ArrayList<>();
        int i = 0;
        int n = sql.length();

        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = copySingleQuoted(sql, i, out);
            } else if (c == '"') {
                i = copyDoubleQuoted(sql, i, out);
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
            } else if (c == '$') {
                int afterTag = dollarQuoteEnd(sql, i);
                if (afterTag > i) {
                    // $tag$ … $tag$ 整段原样搬过去
                    out.append(sql, i, afterTag);
                    i = afterTag;
                    continue;
                }
                int d = i + 1;
                while (d < n && Character.isDigit(sql.charAt(d))) d++;
                if (d > i + 1) {
                    order.add(Integer.parseInt(sql.substring(i + 1, d)));
                    out.append('?');
                    i = d;
                } else {
                    out.append(c);
                    i++;
                }
            } else {
                out.append(c);
                i++;
            }
        }

        if (order.isEmpty()) {
            return new PreparedPlan(sql, binds);
        }
        List<String> expanded = new ArrayList<>(order.size());
        for (int idx : order) {
            // 序号越界（录制与语句对不上）时补 null：宁可绑一个 NULL 让它报错，
            // 也不能把上一个参数的值顺延过去——那是静默写错数据
            expanded.add(binds != null && idx >= 1 && idx <= binds.size() ? binds.get(idx - 1) : null);
        }
        return new PreparedPlan(out.toString(), expanded);
    }

    private static int copySingleQuoted(String s, int i, StringBuilder out) {
        int n = s.length();
        out.append('\'');
        i++;
        while (i < n) {
            char c = s.charAt(i);
            out.append(c);
            i++;
            if (c == '\'') {
                if (i < n && s.charAt(i) == '\'') {
                    out.append('\'');
                    i++;
                } else {
                    break;
                }
            }
        }
        return i;
    }

    private static int copyDoubleQuoted(String s, int i, StringBuilder out) {
        int n = s.length();
        out.append('"');
        i++;
        while (i < n) {
            char c = s.charAt(i);
            out.append(c);
            i++;
            if (c == '"') {
                if (i < n && s.charAt(i) == '"') {
                    out.append('"');
                    i++;
                } else {
                    break;
                }
            }
        }
        return i;
    }

    /**
     * 美元引用：{@code $$…$$} 或 {@code $tag$…$tag$}。
     * 不是美元引用的起点时返回 {@code start}（调用方据此当普通 {@code $} 处理）。
     */
    private static int dollarQuoteEnd(String s, int start) {
        int n = s.length();
        int i = start + 1;
        while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) i++;
        if (i >= n || s.charAt(i) != '$') return start;
        String tag = s.substring(start, i + 1);          // 含两端的 $
        // 纯数字的 tag 不是美元引用，是 $1 这样的占位符
        String inner = tag.substring(1, tag.length() - 1);
        if (!inner.isEmpty() && inner.chars().allMatch(Character::isDigit)) return start;
        int close = s.indexOf(tag, i + 1);
        return close < 0 ? n : close + tag.length();
    }
}
