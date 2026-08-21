package com.migration.traffic.capture.pg;

import com.migration.traffic.capture.TrafficSource.RawStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 把 PG 的日志行归并成"语句"。
 *
 * <p>一条语句在日志里可能对应 1~3 行，<b>不归并就会重放两遍</b>——这是实测出来的：
 * <pre>
 * {"line_num":3,"error_severity":"LOG","message":"statement: SELECT * FROM nope;"}
 * {"line_num":4,"error_severity":"ERROR","state_code":"42P01",
 *  "message":"relation \"nope\" does not exist","statement":"SELECT * FROM nope;"}
 * </pre>
 * 只有第一行是语句；ERROR 行只用来给它补 SQLSTATE。把 ERROR 行也当语句录进去，
 * 回放时每条失败语句都会被执行两次。
 *
 * <p>另一处同源的坑是形态会随开关组合变：
 * <ul>
 *   <li>{@code log_statement=all} + {@code log_min_duration_statement=0}
 *       → {@code statement: …} 然后单独一行 {@code duration: 0.182 ms}（<b>不重复文本</b>）；</li>
 *   <li>只开 duration 不开 statement
 *       → {@code duration: 0.182 ms  statement: …}（文本回来了）；</li>
 *   <li>再把 {@code log_duration=on} 打开
 *       → {@code parse}/{@code bind}/{@code execute} 三连，<b>同一条语句被记三次</b>。</li>
 * </ul>
 * 所以捕获侧强制一套已知组合（见 {@code PgLogTrafficSource}），
 * 解析器同时必须认识并丢弃 {@code parse:}/{@code bind:} 行。
 */
public final class PgStatementAssembler {

    /** 一行日志归属的类别。 */
    public enum Kind {
        /** 一条可回放的语句。 */
        STATEMENT,
        /** 纯耗时行，补给上一条语句。 */
        DURATION,
        /** 出错行，补给上一条语句的 SQLSTATE。 */
        ERROR,
        /** 会话建立。 */
        CONNECT,
        /** 会话断开。 */
        DISCONNECT,
        /** 与语句流无关（checkpoint / autovacuum / parse / bind …）。 */
        IGNORE
    }

    private PgStatementAssembler() {
    }

    /** 判定一行日志的类别——{@link #assemble} 与"要不要把这行留到下一轮"共用。 */
    public static Kind kindOf(PgLogEntry e) {
        if (e == null || e.message == null) return Kind.IGNORE;
        String sev = e.severity == null ? "" : e.severity;
        if ("ERROR".equals(sev) || "FATAL".equals(sev) || "PANIC".equals(sev)) {
            return e.statement != null && !e.statement.isEmpty() ? Kind.ERROR : Kind.IGNORE;
        }
        if (!"LOG".equals(sev)) return Kind.IGNORE;

        String m = e.message;
        if (m.startsWith("statement: ")) return Kind.STATEMENT;
        if (m.startsWith("execute ")) {
            // execute <unnamed>: SELECT … / execute fetch from <portal>: …
            return m.indexOf(": ", "execute ".length()) > 0 ? Kind.STATEMENT : Kind.IGNORE;
        }
        if (m.startsWith("duration: ")) {
            // 实测（PG 16.13，log_min_duration_statement=0）：扩展协议的一次执行会记<b>三行</b>
            //   duration: 0.068 ms  parse <unnamed>: SELECT $1
            //   duration: 0.037 ms  bind  <unnamed>: SELECT $1     ← detail 里有参数
            //   execute <unnamed>: SELECT $1                        ← 只有这一行是语句
            //   duration: 0.009 ms                                  ← 它的耗时
            // parse/bind 两行照单全收就是<b>同一条语句被回放三遍</b>。
            if (m.contains("  parse ") || m.contains("  bind ")) return Kind.IGNORE;
            return embeddedSqlStart(m) > 0 ? Kind.STATEMENT : Kind.DURATION;
        }
        // log_duration=on 时的 parse/bind 行：同一条语句的第 1、2 段，丢弃，只留 execute
        if (m.startsWith("parse ") || m.startsWith("bind ")) return Kind.IGNORE;
        if (m.startsWith("connection authorized") || m.startsWith("connection received")) return Kind.CONNECT;
        if (m.startsWith("disconnection: ")) return Kind.DISCONNECT;
        return Kind.IGNORE;
    }

    /**
     * 归并。
     *
     * @param entries          按文件顺序排好的日志行
     * @param ownPid           采集连接自己的后端进程号，它的行全部跳过（自噪声）
     * @param redactSecrets    是否对语句做口令脱敏（PG 明文记口令，见 {@link PgSecretRedactor}）
     */
    public static List<RawStatement> assemble(List<PgLogEntry> entries, int ownPid, boolean redactSecrets) {
        List<RawStatement> out = new ArrayList<>();
        Map<Long, RawStatement> lastOfSession = new HashMap<>();

        for (PgLogEntry e : entries) {
            if (e.pid == ownPid) continue;                 // 采集连接自身
            long key = e.sessionKey();
            switch (kindOf(e)) {
                case STATEMENT: {
                    RawStatement r = toStatement(e, redactSecrets);
                    if (r != null) {
                        out.add(r);
                        lastOfSession.put(key, r);
                    }
                    break;
                }
                case DURATION: {
                    RawStatement prev = lastOfSession.get(key);
                    if (prev != null) prev.durationUs = durationMicros(e.message);
                    break;
                }
                case ERROR: {
                    RawStatement prev = lastOfSession.get(key);
                    // 只认"同一条语句"的报错：拿不准就不补，宁可缺富化也不能张冠李戴
                    if (prev != null && sameSql(prev.argument, e.statement)) {
                        prev.sqlState = e.stateCode;
                        prev.errorCode = -1;               // PG 用 SQLSTATE，errno 只做"非 0 即出错"的标记
                    }
                    break;
                }
                case CONNECT: {
                    RawStatement r = shell(e);
                    r.commandType = "Connect";
                    out.add(r);
                    break;
                }
                case DISCONNECT: {
                    RawStatement r = shell(e);
                    r.commandType = "Quit";
                    out.add(r);
                    lastOfSession.remove(key);
                    break;
                }
                default:
                    break;
            }
        }
        return out;
    }

    private static RawStatement shell(PgLogEntry e) {
        RawStatement r = new RawStatement();
        r.epochMicros = e.epochMicros;
        r.threadId = e.sessionKey();
        r.userHost = e.userHost();
        r.database = e.dbname;
        r.argument = "";
        return r;
    }

    private static RawStatement toStatement(PgLogEntry e, boolean redactSecrets) {
        String sql = extractSql(e.message);
        if (sql == null || sql.isBlank()) return null;
        RawStatement r = shell(e);
        r.commandType = "Query";
        if (redactSecrets) {
            String safe = PgSecretRedactor.redact(sql);
            if (!safe.equals(sql)) {
                r.redacted = true;
                sql = safe;
            }
        }
        r.argument = sql;
        r.binds = PgBindParser.parse(e.detail);
        // duration: N ms  statement: … 的形态里耗时就在同一行
        if (e.message.startsWith("duration: ")) {
            r.durationUs = durationMicros(e.message);
        }
        return r;
    }

    /** 从 message 里取出 SQL 原文。 */
    static String extractSql(String m) {
        if (m == null) return null;
        if (m.startsWith("statement: ")) return m.substring("statement: ".length());
        if (m.startsWith("execute ")) {
            int sep = m.indexOf(": ", "execute ".length());
            return sep < 0 ? null : m.substring(sep + 2);
        }
        if (m.startsWith("duration: ")) {
            int at = embeddedSqlStart(m);
            return at > 0 ? m.substring(at) : null;
        }
        return null;
    }

    /**
     * {@code duration: 0.182 ms  statement: SELECT 1} 里 SQL 的起点；没有内嵌 SQL 返回 -1。
     * 注意 PG 在 duration 与后半段之间用的是<b>两个空格</b>。
     */
    static int embeddedSqlStart(String m) {
        // 只有 statement / execute 是"这一行就是一条语句"；parse/bind 是同一条语句的前两段，
        // 由 kindOf 直接判 IGNORE，不该走到这里
        for (String tag : new String[]{"  statement: ", "  execute "}) {
            int at = m.indexOf(tag);
            if (at < 0) continue;
            if (tag.endsWith("statement: ")) return at + tag.length();
            int sep = m.indexOf(": ", at + tag.length());
            return sep < 0 ? -1 : sep + 2;
        }
        return -1;
    }

    /** {@code duration: 0.182 ms} → 微秒。 */
    static long durationMicros(String m) {
        int at = m.indexOf("duration: ");
        if (at < 0) return -1L;
        int b = at + "duration: ".length();
        int e = b;
        while (e < m.length() && (Character.isDigit(m.charAt(e)) || m.charAt(e) == '.')) e++;
        try {
            double ms = Double.parseDouble(m.substring(b, e));
            return Math.round(ms * 1000.0);
        } catch (RuntimeException ex) {
            return -1L;
        }
    }

    private static boolean sameSql(String a, String b) {
        if (a == null || b == null) return false;
        if (a.equals(b)) return true;
        // 脱敏过的语句与 ERROR 行里的原文不会逐字相等，比前缀足够了
        String x = a.trim().toLowerCase(Locale.ROOT);
        String y = b.trim().toLowerCase(Locale.ROOT);
        int n = Math.min(24, Math.min(x.length(), y.length()));
        return n > 0 && x.regionMatches(0, y, 0, n);
    }
}
