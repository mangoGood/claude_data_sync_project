package com.migration.subscribe.avro;

import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Avro 订阅消息：wire format 与<b>值保真</b>。
 *
 * <p>值保真这条是重点：订阅链路专门修过"整数变浮点""NULL 被丢"这类缺陷，
 * 换个消息格式又丢一遍就白修了。所以这里不只测"能不能序列化"，
 * 还要把字节读回来逐个断言类型。
 */
@DisplayName("Avro 订阅消息序列化")
class AvroCdcSerializerTest {

    /** 不连网的 registry 替身：只发 id，并记下注册过的 schema 文本。 */
    private static class FakeRegistry extends SchemaRegistryClient {
        final Map<String, String> registered = new LinkedHashMap<>();
        int next = 100;

        FakeRegistry() {
            super("http://unused", null, null, 1000);
        }

        @Override
        public int register(String subject, String schemaJson) {
            registered.put(subject, schemaJson);
            return next++;
        }
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static Object json(String literal) {
        return JsonParser.parseString(literal);
    }

    private Map<String, Object> meta() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("op", "c");
        m.put("ts_ms", 1700000000000L);
        m.put("seqno", 42L);
        m.put("db", "src");
        m.put("table", "orders");
        m.put("txId", "binlog.000042:1234");
        return m;
    }

    private GenericRecord decode(byte[] bytes, String schemaJson) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaJson);
        byte[] payload = Arrays.copyOfRange(bytes, 5, bytes.length);
        return new GenericDatumReader<GenericRecord>(schema)
                .read(null, DecoderFactory.get().binaryDecoder(payload, null));
    }

    @Test
    @DisplayName("Confluent wire format：magic 0x00 + 4 字节大端 schema id + Avro binary")
    void wireFormat() throws Exception {
        FakeRegistry reg = new FakeRegistry();
        AvroCdcSerializer ser = new AvroCdcSerializer(reg);

        byte[] bytes = ser.serialize("cdc.t1.src.orders", null,
                row("id", json("1"), "name", json("\"alice\"")), meta());

        assertEquals(0x00, bytes[0], "第一个字节必须是 magic 0");
        int id = ByteBuffer.wrap(bytes, 1, 4).getInt();
        assertEquals(100, id, "接着 4 字节是大端 schema id");
        assertTrue(reg.registered.containsKey("cdc.t1.src.orders-value"),
                "subject 必须是 <topic>-value（与 Confluent 默认策略一致）");
    }

    @Test
    @DisplayName("值保真：整数是 long、小数是 double、字符串是 string、NULL 还是 null")
    void valueFidelity() throws Exception {
        FakeRegistry reg = new FakeRegistry();
        AvroCdcSerializer ser = new AvroCdcSerializer(reg);

        Map<String, Object> after = row(
                "i", json("1000253"),        // 整数：不能变成 1000253.0
                "d", json("3.14"),
                "s", json("\"abc\""),
                "b", json("true"),
                "n", json("null"));
        byte[] bytes = ser.serialize("t", null, after, meta());
        GenericRecord rec = decode(bytes, reg.registered.get("t-value"));
        GenericRecord row = (GenericRecord) rec.get("after");

        assertEquals(1000253L, row.get("i"), "整数必须落在 long 分支");
        assertEquals(3.14d, row.get("d"));
        assertEquals("abc", row.get("s").toString());
        assertEquals(Boolean.TRUE, row.get("b"));
        assertNull(row.get("n"), "NULL 必须是 null，不能被丢掉或变成空串");
        assertNull(rec.get("before"), "INSERT 没有前镜像");
        assertEquals("c", rec.get("op").toString());
        assertEquals(42L, rec.get("seqno"));
    }

    @Test
    @DisplayName("超出 long 的整数按字符串带过去，不转 double 丢精度")
    void hugeIntegerKeepsPrecision() {
        Object v = AvroCdcSerializer.avroValue(json("123456789012345678901234567890"));
        assertEquals("123456789012345678901234567890", v);
    }

    @Test
    @DisplayName("同一列集只注册一次 schema；列集变了才注册新版本")
    void schemaCachedPerColumnSet() throws Exception {
        FakeRegistry reg = new FakeRegistry();
        AvroCdcSerializer ser = new AvroCdcSerializer(reg);

        ser.serialize("t", null, row("a", json("1")), meta());
        ser.serialize("t", null, row("a", json("2")), meta());
        assertEquals(100, reg.next - 1, "同一列集不该重复注册");

        ser.serialize("t", null, row("a", json("1"), "b", json("2")), meta());
        assertEquals(101, reg.next - 1, "加了一列 = 新 schema 版本");
    }

    @Test
    @DisplayName("每个列字段都带 default:null —— 加列对老消费者才是向后兼容的")
    void addedColumnsAreBackwardCompatible() throws Exception {
        FakeRegistry reg = new FakeRegistry();
        new AvroCdcSerializer(reg).serialize("t", null, row("a", json("1")), meta());
        String schema = reg.registered.get("t-value");
        assertTrue(schema.contains("\"default\":null"), schema);
    }

    @Test
    @DisplayName("列名里的非法字符换成下划线（Avro 名字规则），别让一个带连字符的列名把整批消息废掉")
    void columnNameSanitised() throws Exception {
        FakeRegistry reg = new FakeRegistry();
        AvroCdcSerializer ser = new AvroCdcSerializer(reg);
        byte[] bytes = ser.serialize("t", null, row("order-id", json("7")), meta());
        GenericRecord rec = decode(bytes, reg.registered.get("t-value"));
        assertEquals(7L, ((GenericRecord) rec.get("after")).get("order_id"));
    }

    @Test
    @DisplayName("UPDATE：前后镜像共用同一份行 schema，两边都读得出来")
    void beforeAndAfter() throws Exception {
        FakeRegistry reg = new FakeRegistry();
        AvroCdcSerializer ser = new AvroCdcSerializer(reg);
        Map<String, Object> m = meta();
        m.put("op", "u");
        byte[] bytes = ser.serialize("t", row("id", json("1"), "v", json("\"old\"")),
                row("id", json("1"), "v", json("\"new\"")), m);
        GenericRecord rec = decode(bytes, reg.registered.get("t-value"));
        assertEquals("old", ((GenericRecord) rec.get("before")).get("v").toString());
        assertEquals("new", ((GenericRecord) rec.get("after")).get("v").toString());
    }

    @Test
    @DisplayName("registry 入参里的 schema 文本被正确转义（否则 registry 直接 400）")
    void schemaJsonQuoted() {
        String quoted = SchemaRegistryClient.quoteJsonString("{\"a\":\"b\"}");
        assertEquals("\"{\\\"a\\\":\\\"b\\\"}\"", quoted);
    }
}
