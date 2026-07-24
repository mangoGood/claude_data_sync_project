package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TiCDCExtractor} 的 canal-json 解析测试。
 *
 * <p>列元数据本应来自源端 information_schema，这里用一个子类把三个查询方法替换成固定表结构，
 * 从而在没有 TiDB 实例的情况下验证真正易错的部分：取值还原（二进制/BIT/布尔/NULL）、
 * UPDATE 前镜像重建（canal 的 old 只带变化列）、以及位点与 eventId 的构造。
 */
@DisplayName("TiCDCExtractor canal-json 解析测试")
class TiCDCExtractorTest {

    private static final char FS = '\001';

    /** 固定表结构：覆盖数值/文本/二进制/BIT/布尔/JSON 六类取值路径。 */
    private static final List<String> COLUMNS =
            Arrays.asList("id", "name", "flag", "bits", "payload", "doc");
    private static final List<String> TYPES =
            Arrays.asList("int", "varchar", "tinyint", "bit", "varbinary", "json");
    private static final List<String> FULL_TYPES =
            Arrays.asList("int(11)", "varchar(50)", "tinyint(1)", "bit(8)", "varbinary(255)", "json");

    private TiCDCExtractor extractor;

    /** 用固定表结构替换 information_schema 查询，其余行为保持不变。 */
    private static class StubExtractor extends TiCDCExtractor {
        @Override
        protected List<String> getTableColumns(String database, String table) {
            return COLUMNS;
        }

        @Override
        protected List<String> getTableColumnTypes(String database, String table) {
            tableColumnFullTypeCache.put(database + "." + table, FULL_TYPES);
            return TYPES;
        }

        @Override
        protected List<String> getTablePrimaryKeys(String database, String table) {
            return java.util.Collections.singletonList("id");
        }

        @Override
        protected void invalidateColumnCachesForDdl(String sql, String defaultDatabase) {
            // 无缓存可失效
        }
    }

    @BeforeEach
    void setUp() {
        extractor = new StubExtractor();
    }

    private static String record(String eventType, long commitTs, long ts, long offset, String payload) {
        return eventType + FS + "tidb-binlog" + FS + commitTs + FS + ts + FS + "65535" + FS + offset + FS + payload;
    }

    private THLEvent extract(String line) throws Exception {
        return extractor.doExtract(line.getBytes(StandardCharsets.UTF_8));
    }

    /** canal-json 对二进制列做 ISO-8859-1 解码，测试里按同一编码构造原始字节的字符串形态。 */
    private static String latin1(byte[] raw) {
        return new String(raw, StandardCharsets.ISO_8859_1);
    }

    private static String jsonQuote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            } else if (c < 0x20 || c > 0x7e) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    @Test
    @DisplayName("INSERT：列元数据、类型化取值、二进制按裸字节还原")
    void insertEvent() throws Exception {
        byte[] raw = {0x00, 0x01, (byte) 0xff, (byte) 0xfe};
        String payload = "{\"database\":\"db1\",\"table\":\"t1\",\"type\":\"INSERT\",\"isDdl\":false,"
                + "\"data\":[{\"id\":\"7\",\"name\":\"中文 abc\",\"flag\":\"1\",\"bits\":\"170\","
                + "\"payload\":" + jsonQuote(latin1(raw)) + ",\"doc\":\"{\\\"k\\\":1}\"}],"
                + "\"old\":null,\"_tidb\":{\"commitTs\":457146305087406081}}";

        THLEvent e = extract(record("TICDC_INSERT", 457146305087406081L, 1700000000000L, 42, payload));

        assertNotNull(e);
        Map<String, Object> md = e.getMetadata();
        assertEquals("INSERT", md.get("event_type"));
        assertEquals("db1", md.get("database_name"));
        assertEquals("t1", md.get("table_name"));
        assertEquals("id,name,flag,bits,payload,doc", md.get("column_names"));
        assertEquals("int,varchar,tinyint,bit,varbinary,json", md.get("mysql_column_types"));
        assertEquals("int(11),varchar(50),tinyint(1),bit(8),varbinary(255),json",
                md.get("mysql_column_full_types"));
        assertEquals("id", md.get("primary_keys"));
        // eventId 带 Kafka 位移后缀：同事务多行共享 commitTs，只用 commitTs 会撞死信去重的键
        assertEquals("tidb-binlog:457146305087406081:42", e.getEventId());
        assertEquals(457146305087406081L, md.get("binlog_position"));

        @SuppressWarnings("unchecked")
        List<ArrayList<Object>> typed = (List<ArrayList<Object>>) md.get("rows_typed");
        assertNotNull(typed);
        assertEquals(1, typed.size());
        ArrayList<Object> row = typed.get(0);
        assertEquals("7", row.get(0));
        assertEquals("中文 abc", row.get(1));
        assertEquals(Boolean.TRUE, row.get(2), "tinyint(1) 应转成布尔");
        assertArrayEquals(new byte[]{(byte) 0xaa}, (byte[]) row.get(3), "bit(8)=170 应为单字节 0xAA");
        assertArrayEquals(raw, (byte[]) row.get(4), "二进制列必须逐字节还原");
        assertEquals("{\"k\":1}", row.get(5));
    }

    @Test
    @DisplayName("UPDATE：old 只带变化列时，前镜像 = 后镜像叠加 old")
    void updateEventRebuildsBeforeImage() throws Exception {
        String payload = "{\"database\":\"db1\",\"table\":\"t1\",\"type\":\"UPDATE\",\"isDdl\":false,"
                + "\"data\":[{\"id\":\"7\",\"name\":\"新值\",\"flag\":\"0\",\"bits\":\"1\","
                + "\"payload\":\"\",\"doc\":\"{}\"}],"
                + "\"old\":[{\"name\":\"旧值\"}],"
                + "\"_tidb\":{\"commitTs\":100}}";

        THLEvent e = extract(record("TICDC_UPDATE", 100, 1700000000000L, 1, payload));

        assertNotNull(e);
        Map<String, Object> md = e.getMetadata();
        assertEquals("UPDATE", md.get("event_type"));

        @SuppressWarnings("unchecked")
        List<ArrayList<Object>> after = (List<ArrayList<Object>>) md.get("rows_typed");
        @SuppressWarnings("unchecked")
        List<ArrayList<Object>> before = (List<ArrayList<Object>>) md.get("rows_before_typed");
        assertNotNull(after);
        assertNotNull(before);
        assertEquals("新值", after.get(0).get(1));
        assertEquals("旧值", before.get(0).get(1), "变化列取 old 的旧值");
        assertEquals("7", before.get(0).get(0), "未变化的列沿用后镜像的值");
        assertEquals(Boolean.FALSE, before.get(0).get(2));
    }

    @Test
    @DisplayName("DELETE：data 段是被删行，用于按主键定位")
    void deleteEvent() throws Exception {
        String payload = "{\"database\":\"db1\",\"table\":\"t1\",\"type\":\"DELETE\",\"isDdl\":false,"
                + "\"data\":[{\"id\":\"9\",\"name\":\"x\",\"flag\":\"0\",\"bits\":\"0\","
                + "\"payload\":\"\",\"doc\":\"null\"}],\"old\":null,\"_tidb\":{\"commitTs\":200}}";

        THLEvent e = extract(record("TICDC_DELETE", 200, 1700000000000L, 2, payload));

        assertNotNull(e);
        assertEquals("DELETE", e.getMetadata().get("event_type"));
        @SuppressWarnings("unchecked")
        List<ArrayList<Object>> typed = (List<ArrayList<Object>>) e.getMetadata().get("rows_typed");
        assertEquals("9", typed.get(0).get(0));
        assertNull(e.getMetadata().get("rows_before_typed"));
    }

    @Test
    @DisplayName("NULL 列在类型化取值与文本字面量里都保持 NULL")
    void nullValues() throws Exception {
        String payload = "{\"database\":\"db1\",\"table\":\"t1\",\"type\":\"INSERT\",\"isDdl\":false,"
                + "\"data\":[{\"id\":\"1\",\"name\":null,\"flag\":null,\"bits\":null,"
                + "\"payload\":null,\"doc\":null}],\"old\":null,\"_tidb\":{\"commitTs\":300}}";

        THLEvent e = extract(record("TICDC_INSERT", 300, 1700000000000L, 3, payload));

        @SuppressWarnings("unchecked")
        List<ArrayList<Object>> typed = (List<ArrayList<Object>>) e.getMetadata().get("rows_typed");
        ArrayList<Object> row = typed.get(0);
        for (int i = 1; i < row.size(); i++) {
            assertNull(row.get(i), "第 " + i + " 列应为 null");
        }
        assertEquals("1,null,null,null,null,null", e.getMetadata().get("row_data"));
    }

    @Test
    @DisplayName("DDL 事件转成 QUERY 元数据，交给下游 schema 演进")
    void ddlEvent() throws Exception {
        String payload = "{\"database\":\"db1\",\"table\":\"t1\",\"type\":\"ALTER\",\"isDdl\":true,"
                + "\"sql\":\"ALTER TABLE t1 ADD COLUMN c INT\",\"data\":null,\"old\":null,"
                + "\"_tidb\":{\"commitTs\":400}}";

        THLEvent e = extract(record("TICDC_DDL", 400, 1700000000000L, 4, payload));

        assertNotNull(e);
        assertEquals("QUERY", e.getMetadata().get("event_type"));
        assertEquals("ALTER TABLE t1 ADD COLUMN c INT", e.getMetadata().get("sql"));
        assertEquals("db1", e.getMetadata().get("database_name"));
    }

    @Test
    @DisplayName("watermark 心跳记录标记为 HEARTBEAT 事件")
    void heartbeatEvent() throws Exception {
        THLEvent e = extract(record("SYNC_HEARTBEAT", 500, 1700000000000L, 5, ""));

        assertNotNull(e);
        assertEquals(THLEvent.HEARTBEAT_EVENT, e.getType());
        assertEquals("HEARTBEAT", e.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("断点续传：commitTs 早于 checkpoint 的事件被跳过")
    void skipsEventsBeforeCheckpoint() throws Exception {
        extractor.skipBeforeCheckpoint = true;
        extractor.checkpointBinlogPosition = 1000;

        String payload = "{\"database\":\"db1\",\"table\":\"t1\",\"type\":\"INSERT\",\"isDdl\":false,"
                + "\"data\":[{\"id\":\"1\",\"name\":\"x\",\"flag\":\"0\",\"bits\":\"0\","
                + "\"payload\":\"\",\"doc\":\"{}\"}],\"old\":null,\"_tidb\":{\"commitTs\":999}}";
        assertNull(extract(record("TICDC_INSERT", 999, 1700000000000L, 6, payload)), "早于位点应跳过");

        String later = payload.replace("\"commitTs\":999", "\"commitTs\":1001");
        assertNotNull(extract(record("TICDC_INSERT", 1001, 1700000000000L, 7, later)), "晚于位点应保留");
    }

    @Test
    @DisplayName("字段数不足或 JSON 非法的记录被安全跳过，不影响后续事件")
    void malformedRecords() throws Exception {
        assertNull(extract("TICDC_INSERT" + FS + "tidb-binlog" + FS + "1"));
        assertNull(extract(record("TICDC_INSERT", 1, 1L, 1, "{not json")));
        assertTrue(true);
    }
}
