package com.migration.subscribe.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 把一条 CDC 变更序列化成 Confluent wire format 的 Avro 字节。
 *
 * <p>字节布局与 Confluent 官方序列化器<b>逐字节一致</b>，下游可以直接用它们的
 * Avro 反序列化器读：
 *
 * <pre>
 *   [0x00][4 字节大端 schema id][Avro binary]
 * </pre>
 *
 * <h3>schema 怎么来</h3>
 *
 * <p>订阅侧拿到的是 THL 事件，没有目标端 DDL，所以 schema 只能<b>按事件里实际出现的列</b>生成，
 * 并按 (topic, 列集) 缓存。列集变了（源端 ADD/DROP COLUMN）就是一份新 schema，
 * 注册到同一个 subject 的新版本——这正是 Schema Registry 的兼容性检查该管的事。
 * 每个列字段都是可空且带 {@code default: null}，所以加列对老消费者是<b>向后兼容</b>的。
 *
 * <h3>为什么列值是 union 而不是全 string</h3>
 *
 * <p>全用 string 是最省事的做法，但这个项目在订阅链路上专门修过"整数变浮点""NULL 被丢"
 * 这类值保真缺陷——再把类型压平回字符串等于把修过的东西又丢一遍。
 * 每列的类型是 {@code ["null","boolean","long","double","string"]}：
 * 分支由 JSON 里值的<b>字面形态</b>决定（有小数点/指数才是 double，否则整数就是 long），
 * 与订阅 JSON 输出的判定口径一致，不会出现"同一个值 JSON 里是整数、Avro 里成了浮点"。
 */
public class AvroCdcSerializer {

    private static final Logger logger = LoggerFactory.getLogger(AvroCdcSerializer.class);

    /** 每列的类型：可空 + 保留整数/浮点/布尔/字符串之分 */
    private static final String COLUMN_UNION = "[\"null\",\"boolean\",\"long\",\"double\",\"string\"]";

    private final SchemaRegistryClient registry;
    /** key = subject + "|" + 列集签名 → 已注册的 (schema, id) */
    private final ConcurrentHashMap<String, Registered> cache = new ConcurrentHashMap<>();

    private static final class Registered {
        final Schema schema;
        final int id;
        Registered(Schema schema, int id) {
            this.schema = schema;
            this.id = id;
        }
    }

    public AvroCdcSerializer(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    /**
     * 序列化一条变更。
     *
     * @param topic     目标 topic（subject = {@code <topic>-value}，与 Confluent 默认策略一致）
     * @param before    前镜像（可为 null）
     * @param after     后镜像（可为 null）
     * @param meta      信封字段：op / ts_ms / db / table / seqno / txId
     */
    public byte[] serialize(String topic, Map<String, Object> before, Map<String, Object> after,
                            Map<String, Object> meta) throws Exception {
        List<String> columns = unionColumns(before, after);
        String subject = topic + "-value";
        String signature = String.join(",", columns);
        Registered reg = cache.get(subject + "|" + signature);
        if (reg == null) {
            String json = buildSchemaJson(meta, columns);
            Schema schema = new Schema.Parser().parse(json);
            int id = registry.register(subject, json);
            reg = new Registered(schema, id);
            cache.put(subject + "|" + signature, reg);
            logger.info("Avro schema 就绪: subject={} id={} 列数={}", subject, id, columns.size());
        }

        GenericRecord envelope = new GenericData.Record(reg.schema);
        envelope.put("op", str(meta.get("op")));
        envelope.put("ts_ms", num(meta.get("ts_ms")));
        envelope.put("seqno", num(meta.get("seqno")));
        envelope.put("db", str(meta.get("db")));
        envelope.put("table", str(meta.get("table")));
        envelope.put("tx_id", meta.get("txId") == null ? null : str(meta.get("txId")));

        Schema rowSchema = reg.schema.getField("after").schema().getTypes().get(1);
        envelope.put("before", before == null ? null : row(rowSchema, columns, before));
        envelope.put("after", after == null ? null : row(rowSchema, columns, after));

        ByteArrayOutputStream out = new ByteArrayOutputStream(256);
        out.write(0x00);                                  // magic byte
        out.write(ByteBuffer.allocate(4).putInt(reg.id).array());   // 大端 schema id
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(reg.schema).write(envelope, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private GenericRecord row(Schema rowSchema, List<String> columns, Map<String, Object> values) {
        GenericRecord rec = new GenericData.Record(rowSchema);
        for (String c : columns) {
            rec.put(safeName(c), avroValue(values.get(c)));
        }
        return rec;
    }

    /** 前后镜像的列并集，保持出现顺序——schema 字段序稳定，缓存签名才稳定。 */
    private static List<String> unionColumns(Map<String, Object> before, Map<String, Object> after) {
        Map<String, Boolean> ordered = new LinkedHashMap<>();
        if (after != null) {
            after.keySet().forEach(k -> ordered.put(k, Boolean.TRUE));
        }
        if (before != null) {
            before.keySet().forEach(k -> ordered.put(k, Boolean.TRUE));
        }
        return new java.util.ArrayList<>(ordered.keySet());
    }

    private String buildSchemaJson(Map<String, Object> meta, List<String> columns) {
        String ns = "cdc." + safeName(String.valueOf(meta.get("db")));
        String name = safeName(String.valueOf(meta.get("table")));
        StringBuilder row = new StringBuilder();
        row.append("{\"type\":\"record\",\"name\":\"").append(name).append("Row\",\"fields\":[");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) row.append(',');
            // default:null 是"加列对老消费者向后兼容"的前提，不能省
            row.append("{\"name\":\"").append(safeName(columns.get(i)))
               .append("\",\"type\":").append(COLUMN_UNION).append(",\"default\":null}");
        }
        row.append("]}");

        return "{\"type\":\"record\",\"name\":\"" + name + "Envelope\",\"namespace\":\"" + ns + "\",\"fields\":["
                + "{\"name\":\"op\",\"type\":\"string\"},"
                + "{\"name\":\"ts_ms\",\"type\":\"long\"},"
                + "{\"name\":\"seqno\",\"type\":\"long\"},"
                + "{\"name\":\"db\",\"type\":\"string\"},"
                + "{\"name\":\"table\",\"type\":\"string\"},"
                + "{\"name\":\"tx_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
                // before 里定义行记录，after 按**名字**引用同一个类型——Avro 的具名类型只能定义一次，
                // 两处都展开定义会直接 SchemaParseException（重复定义）。引用必须是带引号的字符串。
                + "{\"name\":\"before\",\"type\":[\"null\"," + row + "],\"default\":null},"
                + "{\"name\":\"after\",\"type\":[\"null\",\"" + name + "Row\"],\"default\":null}"
                + "]}";
    }

    /**
     * JSON 值 → Avro union 分支。
     *
     * <p>整数/浮点靠<b>字面形态</b>区分（有 {@code .} 或 {@code e} 才是 double），
     * 而不是靠解析后的 Java 类型——Gson 把所有数字都当 double 解析，
     * 照那个判会让每一个整数在 Avro 里变成浮点，正是订阅链路早先修过的那类保真缺陷。
     */
    static Object avroValue(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof com.google.gson.JsonElement) {
            com.google.gson.JsonElement e = (com.google.gson.JsonElement) v;
            if (e.isJsonNull()) {
                return null;
            }
            if (e.isJsonPrimitive()) {
                com.google.gson.JsonPrimitive p = e.getAsJsonPrimitive();
                if (p.isBoolean()) {
                    return p.getAsBoolean();
                }
                if (p.isNumber()) {
                    String raw = p.getAsString();
                    if (raw.indexOf('.') >= 0 || raw.indexOf('e') >= 0 || raw.indexOf('E') >= 0) {
                        return Double.parseDouble(raw);
                    }
                    try {
                        return Long.parseLong(raw);
                    } catch (NumberFormatException ex) {
                        // 超出 long 的整数（DECIMAL(38,0) 之类）宁可当字符串原样带过去，
                        // 也不要转成 double 丢精度
                        return raw;
                    }
                }
                return p.getAsString();
            }
            // 对象/数组（JSON 列、Mongo 文档）原样保留文本，交给下游按业务解析
            return e.toString();
        }
        if (v instanceof Boolean || v instanceof Long || v instanceof Double) {
            return v;
        }
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return ((Number) v).longValue();
        }
        if (v instanceof Float) {
            return ((Number) v).doubleValue();
        }
        return String.valueOf(v);
    }

    private static String str(Object v) {
        return v == null ? "" : String.valueOf(v);
    }

    private static long num(Object v) {
        return v instanceof Number ? ((Number) v).longValue() : 0L;
    }

    /** Avro 的名字只允许 [A-Za-z_][A-Za-z0-9_]*，列名里的连字符/点等一律换成下划线。 */
    static String safeName(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "_";
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
                    || (i > 0 && c >= '0' && c <= '9');
            sb.append(ok ? c : '_');
        }
        return sb.toString();
    }
}
