package com.migration.traffic;

import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficJson;
import com.migration.traffic.model.TrafficRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("录制行编解码")
class TrafficJsonTest {

    private static String enc(TrafficRecord r) {
        StringBuilder sb = new StringBuilder();
        TrafficJson.write(sb, r);
        return sb.toString();
    }

    private static TrafficRecord sample() {
        TrafficRecord r = new TrafficRecord();
        r.n = 7;
        r.t = 15_000_123L;
        r.s = 2691;
        r.c = TrafficRecord.CMD_QUERY;
        r.k = StatementClass.DML;
        r.db = "order_db";
        r.u = "app@10.0.0.5";
        r.q = "INSERT INTO t VALUES ('a')";
        return r;
    }

    @Test
    @DisplayName("往返一致")
    void roundTrip() throws IOException {
        TrafficRecord in = sample();
        TrafficRecord out = TrafficJson.read(enc(in));
        assertEquals(in.n, out.n);
        assertEquals(in.t, out.t);
        assertEquals(in.s, out.s);
        assertEquals(in.c, out.c);
        assertEquals(in.k, out.k);
        assertEquals(in.db, out.db);
        assertEquals(in.u, out.u);
        assertEquals(in.q, out.q);
        assertFalse(out.rd);
        assertFalse(out.hasEnrich);
    }

    @Test
    @DisplayName("多行 SQL 里的换行必须转义 —— 否则一条记录被切成多行，JSONL 的行边界就废了")
    void multilineSqlStaysOneLine() throws IOException {
        TrafficRecord r = sample();
        r.q = "SELECT a" + (char) 10 + "FROM t" + (char) 13 + (char) 10 + "WHERE b = 'x'" + (char) 9;
        String line = enc(r);
        assertFalse(line.indexOf((char) 10) >= 0, "序列化结果不能含裸换行");
        assertFalse(line.indexOf((char) 13) >= 0, "序列化结果不能含裸回车");
        assertFalse(line.indexOf((char) 9) >= 0, "序列化结果不能含裸制表符");
        assertEquals(r.q, TrafficJson.read(line).q);
    }

    @Test
    @DisplayName("其它控制字符走 uXXXX 转义")
    void controlCharsEscaped() throws IOException {
        TrafficRecord r = sample();
        r.q = "SELECT 'a" + (char) 1 + "b'";
        String line = enc(r);
        assertTrue(line.contains("u0001"), "控制字符应转成 uXXXX 形式");
        assertEquals(r.q, TrafficJson.read(line).q);
    }

    @Test
    @DisplayName("引号与反斜杠")
    void escaping() throws IOException {
        TrafficRecord r = sample();
        r.q = "SELECT '\"quoted\"', 'C:\\path\\to', ''";
        assertEquals(r.q, TrafficJson.read(enc(r)).q);
    }

    @Test
    @DisplayName("4 字节 UTF-8（emoji）原样往返")
    void fourByteUtf8() throws IOException {
        TrafficRecord r = sample();
        r.q = "INSERT INTO t VALUES ('中文🚀emoji')";
        assertEquals(r.q, TrafficJson.read(enc(r)).q);
    }

    @Test
    @DisplayName("缺省值不落字段：体积在千万级记录上是真金白银")
    void defaultsOmitted() {
        String line = enc(sample());
        assertFalse(line.contains("\"rd\""));
        assertFalse(line.contains("\"e\""));
    }

    @Test
    @DisplayName("富化字段与 redacted 标记")
    void enrichAndRedacted() throws IOException {
        TrafficRecord r = sample();
        r.rd = true;
        r.hasEnrich = true;
        r.errno = 1146;
        r.rows = 3;
        r.aff = 0;
        r.us = 831;
        TrafficRecord out = TrafficJson.read(enc(r));
        assertTrue(out.rd);
        assertTrue(out.hasEnrich);
        assertEquals(1146, out.errno);
        assertEquals(3, out.rows);
        assertEquals(831, out.us);
    }

    @Test
    @DisplayName("未知字段跳过：老引擎读新版本录制文件不能炸")
    void unknownFieldsIgnored() throws IOException {
        TrafficRecord out = TrafficJson.read(
                "{\"n\":1,\"t\":0,\"s\":1,\"c\":\"Q\",\"future\":{\"x\":[1,2]},\"q\":\"SELECT 1\"}");
        assertEquals("SELECT 1", out.q);
    }

    @Test
    @DisplayName("半行/坏行抛 IOException，由调用方决定丢弃 —— 崩溃残留的尾行是正常现象")
    void truncatedLineThrows() {
        assertThrows(IOException.class, () -> TrafficJson.read("{\"n\":1,\"t\":0,\"s\":1,\"c\":\"Q"));
        assertThrows(IOException.class, () -> TrafficJson.read("{\"n\":1}"));
    }
}
