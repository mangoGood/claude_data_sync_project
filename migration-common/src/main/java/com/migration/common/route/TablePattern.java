package com.migration.common.route;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 路由规则的源表匹配器，两种写法：
 * <ul>
 *   <li>通配：{@code shard_db_*.order_*}（{@code *} 任意串、{@code ?} 单字符），以第一个 {@code .} 分隔库与表；</li>
 *   <li>正则：{@code regex:shard_db_\d+\.order_\d{3}}，整体匹配 {@code 库.表}。</li>
 * </ul>
 *
 * <p>大小写沿用全链路既有约定——<b>精确命中优先，未命中按忽略大小写回退</b>：MySQL 源
 * {@code lower_case_table_names} 不区分大小写时，binlog/DDL 里的库表名大小写可能与配置写法不一致；
 * 而区分大小写的源（仅大小写不同的两张表）由"精确优先"保证不被误匹配。
 */
public final class TablePattern {

    private final String spec;
    private final Pattern exact;
    private final Pattern ignoreCase;

    private TablePattern(String spec, Pattern exact, Pattern ignoreCase) {
        this.spec = spec;
        this.exact = exact;
        this.ignoreCase = ignoreCase;
    }

    /**
     * 编译匹配式。
     *
     * @throws IllegalArgumentException 空串、正则语法错误、或通配式缺少库表分隔点
     */
    public static TablePattern compile(String spec) {
        if (spec == null || spec.trim().isEmpty()) {
            throw new IllegalArgumentException("匹配式为空");
        }
        String trimmed = spec.trim();
        String regex;
        if (trimmed.regionMatches(true, 0, "regex:", 0, 6)) {
            regex = trimmed.substring(6).trim();
            if (regex.isEmpty()) {
                throw new IllegalArgumentException("regex: 后缺少正则: " + spec);
            }
        } else {
            int dot = trimmed.indexOf('.');
            if (dot <= 0 || dot == trimmed.length() - 1) {
                throw new IllegalArgumentException("通配式必须写成 <库>.<表>: " + spec);
            }
            regex = wildcardToRegex(trimmed.substring(0, dot))
                    + "\\." + wildcardToRegex(trimmed.substring(dot + 1));
        }
        try {
            return new TablePattern(trimmed, Pattern.compile(regex),
                    Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("匹配式正则非法: " + spec + " (" + e.getDescription() + ")");
        }
    }

    /** 精确（区分大小写）匹配。 */
    public boolean matches(String db, String table) {
        return db != null && table != null && exact.matcher(db + "." + table).matches();
    }

    /** 忽略大小写匹配（调用方在精确匹配全部落空后才用）。 */
    public boolean matchesIgnoreCase(String db, String table) {
        return db != null && table != null && ignoreCase.matcher(db + "." + table).matches();
    }

    public String getSpec() {
        return spec;
    }

    private static String wildcardToRegex(String part) {
        StringBuilder sb = new StringBuilder(part.length() * 2);
        for (int i = 0; i < part.length(); i++) {
            char c = part.charAt(i);
            if (c == '*') {
                sb.append(".*");
            } else if (c == '?') {
                sb.append('.');
            } else {
                if ("\\.[]{}()+-^$|".indexOf(c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return spec;
    }
}
