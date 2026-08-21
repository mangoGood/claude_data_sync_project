package com.migration.traffic;

import com.migration.traffic.capture.TrafficSource.RawStatement;
import com.migration.traffic.capture.pg.PgBindParser;
import com.migration.traffic.capture.pg.PgLogEntry;
import com.migration.traffic.capture.pg.PgLogLineParser;
import com.migration.traffic.capture.pg.PgSecretRedactor;
import com.migration.traffic.capture.pg.PgStatementAssembler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("PG 语句流捕获")
class PgCaptureTest {

    private static final PgLogLineParser PARSER = new PgLogLineParser(ZoneId.of("UTC"));

    private static PgLogEntry json(String line) {
        return PARSER.parseJson(line);
    }

    private static PgLogEntry entry(String severity, String message, String detail) {
        PgLogEntry e = new PgLogEntry();
        e.severity = severity;
        e.message = message;
        e.detail = detail;
        e.sessionId = "6a87cfdd.147";
        e.pid = 0x147;
        e.dbname = "trfdb";
        e.user = "app";
        e.remoteHost = "10.0.0.5";
        e.epochMicros = 1_000_000L;
        return e;
    }

    @Nested
    @DisplayName("绑定参数解析")
    class Binds {

        @Test
        @DisplayName("裸 NULL 是 SQL NULL，带引号的 'NULL' 是字符串 —— 混淆了就是静默写错数据")
        void nullVsQuotedNull() {
            List<String> b = PgBindParser.parse("parameters: $1 = NULL, $2 = 'NULL'");
            assertNotNull(b);
            assertEquals(2, b.size());
            assertNull(b.get(0), "裸 NULL 必须是 SQL NULL");
            assertEquals("NULL", b.get(1), "带引号的是字符串");
        }

        @Test
        @DisplayName("空串不是 NULL")
        void emptyIsNotNull() {
            List<String> b = PgBindParser.parse("parameters: $1 = ''");
            assertEquals(1, b.size());
            assertEquals("", b.get(0));
        }

        @Test
        @DisplayName("值里的逗号与成对单引号不能把清单切错")
        void commasAndQuotesInValue() {
            List<String> b = PgBindParser.parse(
                    "parameters: $1 = 'has,comma and ''quote''', $2 = 'x'");
            assertEquals(2, b.size());
            assertEquals("has,comma and 'quote'", b.get(0));
            assertEquals("x", b.get(1));
        }

        @Test
        @DisplayName("前缀大小写不敏感 —— 实测 PG 16 写小写 parameters、PG 18 写大写 Parameters")
        void prefixCaseInsensitive() {
            assertNotNull(PgBindParser.parse("parameters: $1 = 'a'"));
            assertNotNull(PgBindParser.parse("Parameters: $1 = 'a'"));
        }

        @Test
        @DisplayName("不是参数清单的 detail 返回 null（不是空列表）")
        void notParams() {
            assertNull(PgBindParser.parse("Key (id)=(1) already exists."));
            assertNull(PgBindParser.parse(null));
        }
    }

    @Nested
    @DisplayName("口令脱敏（PG 明文记口令，MySQL/Oracle 由库自己抹）")
    class Redaction {

        @Test
        @DisplayName("CREATE/ALTER ROLE 的口令字面量被换成占位符")
        void redactsPassword() {
            String out = PgSecretRedactor.redact("CREATE USER u WITH PASSWORD 'S3cretPass'");
            assertFalse(out.contains("S3cretPass"), "落盘前必须看不到明文口令");
            assertTrue(out.contains("<secret>"));
            assertTrue(PgSecretRedactor.hasSecret("ALTER ROLE u PASSWORD 'x'"));
            assertTrue(PgSecretRedactor.hasSecret("CREATE ROLE u ENCRYPTED PASSWORD 'md5abc'"));
        }

        @Test
        @DisplayName("普通语句不能被误伤 —— 误伤即把正常语句标成不可回放")
        void doesNotTouchOrdinary() {
            String sql = "SELECT password FROM users WHERE id = 1";
            assertEquals(sql, PgSecretRedactor.redact(sql));
            String sql2 = "UPDATE t SET note = 'my password is here'";
            assertEquals(sql2, PgSecretRedactor.redact(sql2));
        }

        @Test
        @DisplayName("PASSWORD NULL 之类没有字面量的写法原样通过")
        void passwordNull() {
            String sql = "ALTER ROLE u PASSWORD NULL";
            assertEquals(sql, PgSecretRedactor.redact(sql));
        }
    }

    @Nested
    @DisplayName("日志行归并")
    class Assemble {

        @Test
        @DisplayName("失败语句只产生一条记录 —— statement 行 + ERROR 行录成两条就会重放两遍")
        void errorLineIsNotASecondStatement() {
            List<PgLogEntry> in = new ArrayList<>();
            in.add(entry("LOG", "statement: SELECT * FROM nope", null));
            PgLogEntry err = entry("ERROR", "relation \"nope\" does not exist", null);
            err.statement = "SELECT * FROM nope";
            err.stateCode = "42P01";
            in.add(err);

            List<RawStatement> out = PgStatementAssembler.assemble(in, -1, true);
            assertEquals(1, out.size(), "一条失败语句只能录一条");
            assertEquals("42P01", out.get(0).sqlState, "ERROR 行的价值是补 SQLSTATE");
        }

        @Test
        @DisplayName("扩展协议的 parse/bind 行必须丢弃 —— 实测 log_min_duration_statement=0 下一次执行记三行")
        void parseBindLinesDropped() {
            List<PgLogEntry> in = new ArrayList<>();
            in.add(entry("LOG", "duration: 0.068 ms  parse <unnamed>: SELECT $1::text", null));
            in.add(entry("LOG", "duration: 0.037 ms  bind <unnamed>: SELECT $1::text",
                    "parameters: $1 = 'p1'"));
            in.add(entry("LOG", "execute <unnamed>: SELECT $1::text", "parameters: $1 = 'p1'"));
            in.add(entry("LOG", "duration: 0.009 ms", null));

            List<RawStatement> out = PgStatementAssembler.assemble(in, -1, true);
            assertEquals(1, out.size(), "一次执行只能录一条，不是三条");
            assertEquals("SELECT $1::text", out.get(0).argument);
            assertEquals(List.of("p1"), out.get(0).binds);
            assertEquals(9L, out.get(0).durationUs, "紧跟的 duration 行补给它");
        }

        @Test
        @DisplayName("simple protocol：statement 行 + 独立 duration 行")
        void simpleProtocolWithDuration() {
            List<PgLogEntry> in = new ArrayList<>();
            in.add(entry("LOG", "statement: SELECT 1", null));
            in.add(entry("LOG", "duration: 0.182 ms", null));
            List<RawStatement> out = PgStatementAssembler.assemble(in, -1, true);
            assertEquals(1, out.size());
            assertEquals(182L, out.get(0).durationUs);
        }

        @Test
        @DisplayName("只开 duration 不开 statement 时，文本内嵌在 duration 行里")
        void durationCarriesSql() {
            List<PgLogEntry> in = new ArrayList<>();
            in.add(entry("LOG", "duration: 1.5 ms  statement: UPDATE t SET a=1", null));
            List<RawStatement> out = PgStatementAssembler.assemble(in, -1, true);
            assertEquals(1, out.size());
            assertEquals("UPDATE t SET a=1", out.get(0).argument);
            assertEquals(1500L, out.get(0).durationUs);
        }

        @Test
        @DisplayName("采集连接自己的行按 pid 过滤掉（会话级消噪失效时的兜底）")
        void ownPidFiltered() {
            List<PgLogEntry> in = new ArrayList<>();
            PgLogEntry mine = entry("LOG", "statement: SELECT pg_read_binary_file('x')", null);
            mine.pid = 999;
            in.add(mine);
            assertTrue(PgStatementAssembler.assemble(in, 999, true).isEmpty());
        }

        @Test
        @DisplayName("checkpoint/autovacuum 之类的运维日志不是语句")
        void housekeepingIgnored() {
            List<PgLogEntry> in = new ArrayList<>();
            in.add(entry("LOG", "checkpoint starting: time", null));
            in.add(entry("LOG", "automatic vacuum of table \"trfdb.public.t1\"", null));
            assertTrue(PgStatementAssembler.assemble(in, -1, true).isEmpty());
        }

        @Test
        @DisplayName("落盘前脱敏，并标记为不可回放")
        void redactedOnCapture() {
            List<PgLogEntry> in = new ArrayList<>();
            in.add(entry("LOG", "statement: CREATE USER u PASSWORD 'topsecret'", null));
            List<RawStatement> out = PgStatementAssembler.assemble(in, -1, true);
            assertEquals(1, out.size());
            assertFalse(out.get(0).argument.contains("topsecret"));
            assertTrue(out.get(0).redacted);
        }
    }

    @Nested
    @DisplayName("日志行解析")
    class LineParsing {

        @Test
        @DisplayName("jsonlog 一行一条记录，换行已转义")
        void jsonLine() {
            String line = "{\"timestamp\":\"2026-08-21 04:11:09.243 UTC\",\"user\":\"postgres\","
                    + "\"dbname\":\"probedb\",\"pid\":327,\"session_id\":\"6a87cfdd.147\","
                    + "\"line_num\":2,\"error_severity\":\"LOG\","
                    + "\"message\":\"statement: SELECT 1\\nFROM t\"}";
            PgLogEntry e = json(line);
            assertNotNull(e);
            assertEquals(327, e.pid);
            assertEquals("probedb", e.dbname);
            assertEquals("statement: SELECT 1\nFROM t", e.message);
        }

        @Test
        @DisplayName("半行 / 垃圾行整行丢弃，不能把整批读挂掉")
        void brokenLine() {
            assertNull(json("{\"timestamp\":\"2026-08-21 04:11"));
            assertNull(json("not json at all"));
            assertNull(json(""));
        }

        @Test
        @DisplayName("会话 id 折算成 long 不撞车 —— 撞了就是两条源会话共用一条目标连接")
        void sessionKeyIsUnique() {
            PgLogEntry a = new PgLogEntry();
            a.sessionId = "6a87cfdd.147";
            PgLogEntry b = new PgLogEntry();
            b.sessionId = "6a87cfdd.148";
            PgLogEntry c = new PgLogEntry();
            c.sessionId = "6a87cfde.147";
            assertTrue(a.sessionKey() != b.sessionKey());
            assertTrue(a.sessionKey() != c.sessionKey());
            assertTrue(a.sessionKey() > 0);
        }

        @Test
        @DisplayName("csvlog 的字段切分要认引号：SQL 里的逗号不是分隔符")
        void csvSplit() {
            List<String> f = PgLogLineParser.splitCsv(
                    "2026-08-21 04:09:18.917 UTC,\"postgres\",\"probedb\",207,\"[local]\","
                            + "\"6a87cf6e.cf\",1,\"SELECT\",x,x,x,\"LOG\",\"00000\","
                            + "\"statement: SELECT a, b FROM t\"");
            assertEquals("probedb", f.get(2));
            assertEquals("statement: SELECT a, b FROM t", f.get(13),
                    "值里的逗号不能把字段切开");
        }

        @Test
        @DisplayName("csvlog 的多行 SQL：引号没闭合就还不是一条完整记录")
        void csvMultiline() {
            assertFalse(PgLogLineParser.csvRecordComplete("a,\"statement: SELECT 1"));
            assertTrue(PgLogLineParser.csvRecordComplete("a,\"statement: SELECT 1\nFROM t\""));
        }
    }
}
