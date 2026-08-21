package com.migration.traffic.model;

import com.google.gson.stream.JsonReader;

import java.io.IOException;
import java.io.StringReader;

/**
 * 录制行（JSONL）的编解码。
 *
 * <p>写路径手写：它是热路径（每条语句一次），且要精确控制体积——
 * 缺省值一律不落字段（{@code rd:false}、{@code errno:0} 都省掉）。
 * 读路径用 gson 的流式 {@link JsonReader}：正确性由库保证，且对将来新增字段天然兼容
 * （未知字段 skipValue 跳过，老版本引擎读新版本文件不会炸）。
 */
public final class TrafficJson {

    private TrafficJson() {
    }

    /** 序列化一条记录为单行 JSON（不含换行符）。 */
    public static void write(StringBuilder sb, TrafficRecord r) {
        sb.append("{\"n\":").append(r.n).append(",\"t\":").append(r.t).append(",\"s\":").append(r.s);
        sb.append(",\"c\":\"").append(r.c).append('"');
        if (r.k != null) {
            sb.append(",\"k\":\"").append(r.k.name()).append('"');
        }
        if (r.db != null && !r.db.isEmpty()) {
            sb.append(",\"db\":");
            escape(sb, r.db);
        }
        if (r.u != null && !r.u.isEmpty()) {
            sb.append(",\"u\":");
            escape(sb, r.u);
        }
        if (r.q != null && !r.q.isEmpty()) {
            sb.append(",\"q\":");
            escape(sb, r.q);
        }
        if (r.b != null && !r.b.isEmpty()) {
            // 绑定参数数组：null 元素写成 JSON null，表示 SQL NULL。
            // 不能写成空串——PG 的 $1 = NULL 与 $1 = '' 是两回事，Oracle 的 #1(0): 亦然。
            sb.append(",\"b\":[");
            for (int i = 0; i < r.b.size(); i++) {
                if (i > 0) sb.append(',');
                String v = r.b.get(i);
                if (v == null) {
                    sb.append("null");
                } else {
                    escape(sb, v);
                }
            }
            sb.append(']');
        }
        if (r.sn != null && !r.sn.isEmpty()) {
            sb.append(",\"sn\":");
            escape(sb, r.sn);
        }
        if (r.rd) {
            sb.append(",\"rd\":true");
        }
        if (r.hasEnrich) {
            sb.append(",\"e\":{\"errno\":").append(r.errno)
              .append(",\"rows\":").append(r.rows)
              .append(",\"aff\":").append(r.aff)
              .append(",\"us\":").append(r.us);
            if (r.state != null && !r.state.isEmpty()) {
                sb.append(",\"st\":");
                escape(sb, r.state);
            }
            sb.append('}');
        }
        sb.append('}');
    }

    /**
     * 反序列化一行。行不完整/不是合法 JSON 时抛 {@link IOException}，由调用方决定丢弃还是中止
     * （分段最后一行不完整是正常的崩溃残留，整行丢弃即可）。
     */
    public static TrafficRecord read(String line) throws IOException {
        TrafficRecord r = new TrafficRecord();
        try (JsonReader jr = new JsonReader(new StringReader(line))) {
            jr.beginObject();
            while (jr.hasNext()) {
                String name = jr.nextName();
                switch (name) {
                    case "n": r.n = jr.nextLong(); break;
                    case "t": r.t = jr.nextLong(); break;
                    case "s": r.s = jr.nextLong(); break;
                    case "c": r.c = jr.nextString(); break;
                    case "k": r.k = StatementClass.parse(jr.nextString()); break;
                    case "db": r.db = jr.nextString(); break;
                    case "u": r.u = jr.nextString(); break;
                    case "q": r.q = jr.nextString(); break;
                    case "b": {
                        java.util.List<String> binds = new java.util.ArrayList<>();
                        jr.beginArray();
                        while (jr.hasNext()) {
                            if (jr.peek() == com.google.gson.stream.JsonToken.NULL) {
                                jr.nextNull();
                                binds.add(null);        // SQL NULL，不是空串
                            } else {
                                binds.add(jr.nextString());
                            }
                        }
                        jr.endArray();
                        r.b = binds;
                        break;
                    }
                    case "sn": r.sn = jr.nextString(); break;
                    case "rd": r.rd = jr.nextBoolean(); break;
                    case "e":
                        r.hasEnrich = true;
                        jr.beginObject();
                        while (jr.hasNext()) {
                            String en = jr.nextName();
                            switch (en) {
                                case "errno": r.errno = jr.nextInt(); break;
                                case "rows": r.rows = jr.nextLong(); break;
                                case "aff": r.aff = jr.nextLong(); break;
                                case "us": r.us = jr.nextLong(); break;
                                case "st": r.state = jr.nextString(); break;
                                default: jr.skipValue();
                            }
                        }
                        jr.endObject();
                        break;
                    default:
                        jr.skipValue();
                }
            }
            jr.endObject();
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("录制行解析失败: " + e.getMessage(), e);
        }
        if (r.c == null) {
            throw new IOException("录制行缺少命令类型(c)");
        }
        return r;
    }

    /**
     * JSON 字符串转义。
     *
     * <p>控制字符必须转成 {@code \\uXXXX}：SQL 里出现裸的 {@code \n}/{@code \t} 是常态
     * （多行 SQL），不转义会把一条记录切成多行，整个 JSONL 文件的行边界就废了。
     */
    public static void escapeJson(StringBuilder sb, String s) {
        escape(sb, s);
    }

    static void escape(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                default:
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
            }
        }
        sb.append('"');
    }
}
