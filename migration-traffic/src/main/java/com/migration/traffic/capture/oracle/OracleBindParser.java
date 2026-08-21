package com.migration.traffic.capture.oracle;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析 Oracle 统一审计里 {@code SQL_BINDS} 的绑定值。
 *
 * <p>实测形态（Oracle AI Database 26ai Free 23.26.2.0.0）：
 * <pre>
 *  #1(1):2 #2(9):zh中文emoji
 *   ↑序号 ↑<b>字符</b>长度 ↑值
 * </pre>
 *
 * <p><b>必须按声明长度切片，不能按分隔符切</b>：值里可以有空格、可以有 {@code #}。
 * 按 {@code #} 或空格 split，在第一个带空格的字符串上就会错位——
 * 而错位的后果是把一个参数的一半绑给下一个参数，静默写错数据。
 *
 * <p>{@code #1(0):} 是 <b>NULL</b>（实测：绑 NULL 的参数记成长度 0）。
 * Oracle 里空串本来就等于 NULL，所以这里没有歧义；
 * 但回放时必须绑 SQL NULL 而不是空串，否则 NOT NULL 列上会报错。
 */
public final class OracleBindParser {

    private OracleBindParser() {
    }

    /** 没有绑定参数时返回 null（不是空列表）。 */
    public static List<String> parse(String binds) {
        if (binds == null || binds.isEmpty()) return null;
        List<String> out = new ArrayList<>();
        int i = 0;
        int n = binds.length();
        while (i < n) {
            int hash = binds.indexOf('#', i);
            if (hash < 0) break;
            int lp = binds.indexOf('(', hash);
            if (lp < 0) break;
            int rp = binds.indexOf(')', lp);
            if (rp < 0) break;
            int colon = rp + 1;
            if (colon >= n || binds.charAt(colon) != ':') {
                i = rp + 1;
                continue;
            }
            int pos;
            int len;
            try {
                pos = Integer.parseInt(binds.substring(hash + 1, lp).trim());
                len = Integer.parseInt(binds.substring(lp + 1, rp).trim());
            } catch (NumberFormatException e) {
                i = rp + 1;
                continue;
            }
            int valStart = colon + 1;
            int valEnd = Math.min(n, valStart + len);
            String v = len == 0 ? null : binds.substring(valStart, valEnd);
            // 序号是从 1 起的；中间缺号时补 null 占位，保证下标与 :bN 对得上
            while (out.size() < pos - 1) out.add(null);
            if (out.size() == pos - 1) {
                out.add(v);
            } else if (pos - 1 < out.size()) {
                out.set(pos - 1, v);
            }
            i = valEnd;
        }
        return out.isEmpty() ? null : out;
    }
}
