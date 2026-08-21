package com.migration.traffic.capture.pg;

import com.google.gson.stream.JsonReader;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * PG 服务端日志的行解析。
 *
 * <p><b>jsonlog（PG ≥ 15）是首选，不是风格偏好</b>：
 * <ul>
 *   <li>csvlog 里一条多行 SQL 就是<b>物理多行</b>，必须用带引号状态机的 CSV 解析器；
 *       jsonlog 把换行转义成 {@code \\n}，一行一条记录；</li>
 *   <li>csvlog 是<b>按列位</b>取值的，而列集合随 PG 版本增删（13 加 backend_type、
 *       14 加 leader_pid/query_id……）；jsonlog 按键名取，天然免疫版本漂移。</li>
 * </ul>
 * csvlog 的解析仍然实现了，因为 PG 13/14 上没有 jsonlog——但它是降级档。
 */
public final class PgLogLineParser {

    /** PG 日志时间戳形如 {@code 2026-08-21 04:11:09.243 UTC}，前 23 个字符是本地时刻。 */
    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final ZoneId logZone;

    /**
     * @param logZone 源库的 {@code log_timezone}。
     *                <b>必须用 GUC 里的真实时区而不是日志末尾的缩写</b>：
     *                缩写是有歧义的（{@code CST} 既是美国中部也是中国标准时间），
     *                认错了整条时间轴会整体平移若干小时，而录制文件看上去完全正常。
     */
    public PgLogLineParser(ZoneId logZone) {
        this.logZone = logZone == null ? ZoneId.systemDefault() : logZone;
    }

    // ==================== jsonlog ====================

    /** 解析一行 jsonlog。不是合法 JSON 或缺关键字段时返回 null（半行/垃圾行整行丢弃）。 */
    public PgLogEntry parseJson(String line) {
        if (line == null || line.isEmpty() || line.charAt(0) != '{') return null;
        PgLogEntry e = new PgLogEntry();
        try (JsonReader jr = new JsonReader(new StringReader(line))) {
            jr.beginObject();
            while (jr.hasNext()) {
                String name = jr.nextName();
                switch (name) {
                    case "timestamp": e.epochMicros = toMicros(jr.nextString()); break;
                    case "pid": e.pid = jr.nextInt(); break;
                    case "session_id": e.sessionId = jr.nextString(); break;
                    case "line_num": e.lineNum = jr.nextLong(); break;
                    case "user": e.user = jr.nextString(); break;
                    case "dbname": e.dbname = jr.nextString(); break;
                    case "remote_host": e.remoteHost = jr.nextString(); break;
                    case "error_severity": e.severity = jr.nextString(); break;
                    case "state_code": e.stateCode = jr.nextString(); break;
                    case "message": e.message = jr.nextString(); break;
                    case "detail": e.detail = jr.nextString(); break;
                    case "statement": e.statement = jr.nextString(); break;
                    case "application_name": e.applicationName = jr.nextString(); break;
                    case "backend_type": e.backendType = jr.nextString(); break;
                    default: jr.skipValue();
                }
            }
            jr.endObject();
        } catch (IOException | RuntimeException ex) {
            return null;
        }
        return e.message == null ? null : e;
    }

    // ==================== csvlog（PG 13/14 降级档） ====================

    /**
     * csvlog 的列位（PG 13+ 起前 23 列稳定；后面的 backend_type / leader_pid / query_id
     * 按版本增删，我们只用前面这些，多出来的列忽略即可）。
     */
    private static final int C_TIME = 0, C_USER = 1, C_DB = 2, C_PID = 3, C_HOST = 4,
            C_SESSION = 5, C_LINE = 6, C_SEVERITY = 11, C_STATE = 12,
            C_MESSAGE = 13, C_DETAIL = 14, C_STATEMENT = 19;

    /** 解析一条 csvlog 记录（已按引号规则拼好、可能含换行）。 */
    public PgLogEntry parseCsv(String record) {
        List<String> f = splitCsv(record);
        if (f.size() <= C_MESSAGE) return null;
        PgLogEntry e = new PgLogEntry();
        e.epochMicros = toMicros(f.get(C_TIME));
        e.user = f.get(C_USER);
        e.dbname = f.get(C_DB);
        e.pid = (int) longOf(f.get(C_PID));
        e.remoteHost = f.get(C_HOST);
        e.sessionId = f.get(C_SESSION);
        e.lineNum = longOf(f.get(C_LINE));
        e.severity = f.get(C_SEVERITY);
        e.stateCode = f.get(C_STATE);
        e.message = f.get(C_MESSAGE);
        e.detail = f.size() > C_DETAIL ? f.get(C_DETAIL) : null;
        e.statement = f.size() > C_STATEMENT ? f.get(C_STATEMENT) : null;
        return e.message == null || e.message.isEmpty() ? null : e;
    }

    /**
     * CSV 字段切分（RFC4180：双引号包裹、内部双引号成对转义）。
     *
     * <p>不能用 {@code split(",")}：SQL 里逗号遍地都是，字段里还可能有换行。
     */
    public static List<String> splitCsv(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < s.length() && s.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    cur.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }

    /**
     * csvlog 是不定行数的：字段里的换行原样落在文件里。
     * 只有<b>引号已闭合</b>的行才是一条完整记录，否则要接着拼下一行。
     */
    public static boolean csvRecordComplete(String buffered) {
        int quotes = 0;
        for (int i = 0; i < buffered.length(); i++) {
            if (buffered.charAt(i) == '"') quotes++;
        }
        return quotes % 2 == 0;
    }

    // ==================== 公共 ====================

    private long toMicros(String ts) {
        if (ts == null || ts.length() < 23) return 0L;
        try {
            LocalDateTime ldt = LocalDateTime.parse(ts.substring(0, 23), TS);
            return ldt.atZone(logZone).toInstant().toEpochMilli() * 1000L;
        } catch (RuntimeException e) {
            return 0L;
        }
    }

    private static long longOf(String s) {
        try {
            return s == null || s.isEmpty() ? 0L : Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
