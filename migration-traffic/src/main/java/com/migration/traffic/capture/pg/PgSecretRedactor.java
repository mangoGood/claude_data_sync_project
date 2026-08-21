package com.migration.traffic.capture.pg;

import java.util.Locale;

/**
 * PG 语句里的口令脱敏。
 *
 * <p><b>这是 PG 独有的一道工序</b>：MySQL 与 Oracle 在写日志时自己就把口令抹了
 * （{@code IDENTIFIED BY <secret>} / {@code IDENTIFIED BY *}），
 * 而 PG 实测<b>明文写进日志</b>：
 * <pre>
 * {"message":"statement: CREATE USER trfprobe WITH PASSWORD 'S3cretPass'"}
 * </pre>
 * 录制文件会被下载、上传、跨机传递，明文口令留在里面就是一条实打实的凭据泄露路径。
 *
 * <p>脱敏必须发生在<b>写盘之前</b>，不能等到展示层——展示层管不住已经落地的文件。
 * 脱敏过的语句同时标记为不可回放（{@code rd=true}）：占位符不是原文，
 * 照原样执行会建出一个口令是占位符的账号。
 */
public final class PgSecretRedactor {

    /** 与 MySQL 侧同一个占位符，前端与报告的处理逻辑因此可以共用。 */
    public static final String PLACEHOLDER = "<secret>";

    private PgSecretRedactor() {
    }

    /** 语句里是否含口令字面量。 */
    public static boolean hasSecret(String sql) {
        return sql != null && !sql.equals(redact(sql));
    }

    /**
     * 把 {@code PASSWORD '…'} 里的字面量换成占位符；没有口令时原样返回同一个对象。
     *
     * <p>覆盖 {@code CREATE/ALTER ROLE|USER … [ENCRYPTED] PASSWORD 'x'}，
     * 以及 {@code PASSWORD} 后面跟 {@code $$…$$} 美元引用的写法。
     */
    public static String redact(String sql) {
        if (sql == null) return null;
        String upper = sql.toUpperCase(Locale.ROOT);
        int from = 0;
        StringBuilder out = null;
        while (true) {
            int kw = indexOfWord(upper, "PASSWORD", from);
            if (kw < 0) break;
            int i = kw + "PASSWORD".length();
            while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) i++;
            if (i >= sql.length()) break;

            int end;
            if (sql.charAt(i) == '\'') {
                end = endOfSingleQuoted(sql, i);
            } else if (sql.startsWith("$$", i)) {
                int close = sql.indexOf("$$", i + 2);
                end = close < 0 ? -1 : close + 2;
            } else {
                from = i;                           // PASSWORD NULL / PASSWORD 后面不是字面量
                continue;
            }
            if (end < 0) break;

            if (out == null) out = new StringBuilder(sql);
            // 直接在副本上替换；长度会变，所以从后往前扫会更简单，但语句里一般只有一处，
            // 这里保持顺序替换并同步修正偏移
            int delta = out.length() - sql.length();
            out.replace(i + delta, end + delta, "'" + PLACEHOLDER + "'");
            from = end;
        }
        return out == null ? sql : out.toString();
    }

    private static int endOfSingleQuoted(String s, int start) {
        int i = start + 1;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\'') {
                if (i + 1 < s.length() && s.charAt(i + 1) == '\'') {
                    i += 2;
                } else {
                    return i + 1;
                }
            } else {
                i++;
            }
        }
        return -1;
    }

    private static int indexOfWord(String upper, String word, int from) {
        int i = upper.indexOf(word, from);
        while (i >= 0) {
            boolean leftOk = i == 0 || !isWordChar(upper.charAt(i - 1));
            int end = i + word.length();
            boolean rightOk = end >= upper.length() || !isWordChar(upper.charAt(end));
            if (leftOk && rightOk) return i;
            i = upper.indexOf(word, i + 1);
        }
        return -1;
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }
}
