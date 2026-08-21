package com.migration.traffic;

import com.google.gson.Gson;
import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import com.migration.traffic.model.TrafficJson;
import com.migration.traffic.model.TrafficRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("录制格式 v2：绑定参数 / 引擎 / 位点")
class RecordingFormatV2Test {

    private static String enc(TrafficRecord r) {
        StringBuilder sb = new StringBuilder();
        TrafficJson.write(sb, r);
        return sb.toString();
    }

    private static TrafficRecord withBinds(String... values) {
        TrafficRecord r = new TrafficRecord();
        r.n = 1;
        r.t = 100;
        r.s = 7;
        r.c = TrafficRecord.CMD_QUERY;
        r.k = StatementClass.DML;
        r.q = "INSERT INTO t VALUES ($1,$2)";
        r.b = Arrays.asList(values);
        return r;
    }

    @Nested
    @DisplayName("绑定参数")
    class Binds {

        @Test
        @DisplayName("NULL 与空串必须能区分开 —— 混淆了就是静默写错数据")
        void nullIsNotEmptyString() throws IOException {
            TrafficRecord out = TrafficJson.read(enc(withBinds("", null)));
            assertEquals(2, out.b.size());
            assertEquals("", out.b.get(0), "空串就是空串");
            assertNull(out.b.get(1), "SQL NULL 必须读回成 null，不能是空串");
            assertNotEquals(out.b.get(0), out.b.get(1));
        }

        @Test
        @DisplayName("值里的引号/逗号/换行/4 字节 UTF-8 原样往返")
        void awkwardValuesRoundTrip() throws IOException {
            String nasty = "has,comma 'quote' \"dq\"\n第二行 🚀";
            TrafficRecord out = TrafficJson.read(enc(withBinds(nasty)));
            assertEquals(nasty, out.b.get(0));
        }

        @Test
        @DisplayName("一行仍然是一行 —— 参数里的换行不能把 JSONL 的行边界切开")
        void bindsStayOnOneLine() {
            String line = enc(withBinds("a\nb"));
            assertEquals(-1, line.indexOf('\n'));
        }

        @Test
        @DisplayName("没有参数时不写 b 字段（一条记录多 20 字节，千万条就是几百 MB）")
        void absentWhenEmpty() {
            TrafficRecord r = withBinds();
            r.b = null;
            assertTrue(!enc(r).contains("\"b\""));
        }
    }

    @Nested
    @DisplayName("v1 兼容")
    class LegacyV1 {

        @Test
        @DisplayName("v1 录制行照常读得回来（没有 b/sn/state）")
        void readsV1Line() throws IOException {
            String v1 = "{\"n\":7,\"t\":15000123,\"s\":2691,\"c\":\"Q\",\"k\":\"DML\","
                    + "\"db\":\"order_db\",\"u\":\"app@1.2.3.4\",\"q\":\"INSERT INTO t VALUES (1)\"}";
            TrafficRecord r = TrafficJson.read(v1);
            assertEquals(7, r.n);
            assertEquals("order_db", r.db);
            assertNull(r.b, "v1 没有绑定参数");
            assertNull(r.sn);
        }

        @Test
        @DisplayName("v1 manifest 没有 engine 字段 → 按 MySQL 读，老录制必须还能放")
        void legacyManifestIsMysql() {
            String v1 = "{\"format\":\"synctask-traffic/1\",\"captureTaskId\":\"t1\",\"segments\":[]}";
            RecordingManifest m = new Gson().fromJson(v1, RecordingManifest.class);
            assertEquals(TrafficEngine.MYSQL, m.engineOf());
            assertTrue(m.isLegacyV1());
        }

        @Test
        @DisplayName("新录制默认写 v2 且带引擎名")
        void newManifestIsV2() {
            RecordingManifest m = new RecordingManifest();
            assertEquals("synctask-traffic/2", m.format);
            assertEquals(TrafficEngine.MYSQL, m.engineOf());
        }
    }

    @Nested
    @DisplayName("引擎与身份")
    class Identity {

        @Test
        @DisplayName("引擎名解析：null/未知一律回落 MySQL（v1 录制没有这个字段）")
        void parseEngine() {
            assertEquals(TrafficEngine.MYSQL, TrafficEngine.parse(null));
            assertEquals(TrafficEngine.MYSQL, TrafficEngine.parse(""));
            assertEquals(TrafficEngine.MYSQL, TrafficEngine.parse("mysql"));
            assertEquals(TrafficEngine.POSTGRESQL, TrafficEngine.parse("postgresql"));
            assertEquals(TrafficEngine.POSTGRESQL, TrafficEngine.parse("PG"));
            assertEquals(TrafficEngine.ORACLE, TrafficEngine.parse("Oracle"));
        }

        @Test
        @DisplayName("三种引擎各取各的身份依据")
        void identityPerEngine() {
            SourceFingerprint my = new SourceFingerprint();
            my.engine = "mysql";
            my.serverUuid = "uuid-1";
            assertEquals("uuid-1", my.identity());

            SourceFingerprint pg = new SourceFingerprint();
            pg.engine = "postgresql";
            pg.systemIdentifier = "7667564110765068325";
            assertEquals("7667564110765068325", pg.identity());

            SourceFingerprint ora = new SourceFingerprint();
            ora.engine = "oracle";
            ora.dbid = "1506701341";
            ora.conName = "FREEPDB1";
            assertEquals("1506701341/FREEPDB1", ora.identity(),
                    "Oracle 必须 DBID+容器名一起比：只比 DBID 会误拦 PDB→PDB，只比容器名会跨 CDB 撞号");
        }

        @Test
        @DisplayName("拿不到身份时返回 null —— 调用方据此降级为『无法判定』而不是『判定不同』")
        void unknownIdentityIsNull() {
            assertNull(new SourceFingerprint().identity());
            SourceFingerprint ora = new SourceFingerprint();
            ora.engine = "oracle";
            assertNull(ora.identity());
        }
    }
}
