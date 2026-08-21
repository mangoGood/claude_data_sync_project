package com.migration.traffic.capture.pg;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析 PG 日志里 {@code detail} 字段的绑定参数清单。
 *
 * <p>实测形态（PostgreSQL 18.1）：
 * <pre>
 * Parameters: $1 = '', $2 = 'has,comma and ''quote''', $3 = 'NULL'
 *             ↑空串      ↑逗号在值里、单引号成对转义        ↑这是字符串 "NULL"，带引号
 * </pre>
 *
 * <p>两条规则一条都不能错：
 * <ul>
 *   <li><b>带引号 = 文本值</b>，内部的 {@code ''} 还原成一个 {@code '}；</li>
 *   <li><b>裸 {@code NULL} = SQL NULL</b>。把它当成字符串 {@code "NULL"} 绑下去，
 *       就是本仓库 {@code silent-loss-audit-2026-08-11} 里"空串被写成字符串 'NULL'"
 *       那一类坑的镜像版本——任务全绿，数据已经错了。</li>
 * </ul>
 */
public final class PgBindParser {

    /**
     * 前缀<b>必须大小写不敏感地匹配</b>。
     *
     * <p>实测两种形态都存在：{@code execute} 行给的是 {@code Parameters: …}（PG 18.1 探针），
     * 而 {@code log_min_duration_statement=0} 下 bind/execute 行给的是
     * {@code parameters: …}（PG 16.13）。按大小写敏感地匹配，
     * 一整类语句的绑定参数会<b>静默变成"没有参数"</b>——回放时把
     * {@code SELECT $1} 原样送出去，报的是"缺少参数"，跟"少读了一个字段"完全不像。
     */
    private static final String PREFIX = "parameters: ";

    private PgBindParser() {
    }

    /** detail 不是参数清单时返回 null（不是空列表——空列表意味着"有参数但一个都没有"）。 */
    public static List<String> parse(String detail) {
        if (detail == null) return null;
        int at = detail.toLowerCase(java.util.Locale.ROOT).indexOf(PREFIX);
        if (at < 0) return null;
        String body = detail.substring(at + PREFIX.length());

        List<String> out = new ArrayList<>();
        int i = 0;
        int n = body.length();
        while (i < n) {
            // 跳到 '$'
            while (i < n && body.charAt(i) != '$') i++;
            if (i >= n) break;
            i++;                                  // 吃掉 '$'
            while (i < n && Character.isDigit(body.charAt(i))) i++;
            // 吃掉 " = "
            while (i < n && (body.charAt(i) == ' ' || body.charAt(i) == '=')) i++;
            if (i >= n) break;

            if (body.charAt(i) == '\'') {
                i++;
                StringBuilder sb = new StringBuilder();
                while (i < n) {
                    char c = body.charAt(i);
                    if (c == '\'') {
                        if (i + 1 < n && body.charAt(i + 1) == '\'') {
                            sb.append('\'');
                            i += 2;
                        } else {
                            i++;                  // 收尾引号
                            break;
                        }
                    } else {
                        sb.append(c);
                        i++;
                    }
                }
                out.add(sb.toString());
            } else {
                // 裸值：只可能是 NULL
                int b = i;
                while (i < n && body.charAt(i) != ',') i++;
                String raw = body.substring(b, i).trim();
                out.add("NULL".equalsIgnoreCase(raw) ? null : raw);
            }
        }
        return out.isEmpty() ? null : out;
    }
}
