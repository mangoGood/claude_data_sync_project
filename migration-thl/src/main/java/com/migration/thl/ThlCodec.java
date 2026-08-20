package com.migration.thl;

import com.migration.common.lob.LobRef;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THL 事件的自有编解码，取代 Java 原生序列化。
 *
 * <h3>为什么换</h3>
 * <p>老写法在 {@code THLFileWriter.writeEvent} 里<b>每条事件新建一个
 * {@code ObjectOutputStream}</b>，于是原生序列化的 back-reference 表每次从零开始，
 * 类描述符逐条重复写入。在本仓库一个真实 THL 上实测：
 *
 * <pre>
 *   10,043 条事件 / 7.08 MB payload，单条平均 741 B
 *   "com.migration.thl.THLEvent" / "java.util.HashMap" / "java.sql.Timestamp"
 *       三个类描述符各出现 10,043 次（每条一次）
 *   单条 741 B 中 278 B（38%）是类描述符区
 *   整文件 gzip 后 0.20 MB —— 37 倍压缩比
 * </pre>
 *
 * <p>这 38% 是全链路成本：extract 的盘写、increment/subscribe 的盘读、page cache、
 * 以及跨机接管回灌时的传输量，全部乘以 1.38；反射式序列化本身的 CPU 还在其上。
 *
 * <h3>顺带解决的安全问题</h3>
 * <p>原生反序列化会按流里写的类名实例化对象，是 gadget 链的入口
 * （见 {@link ThlObjectInputFilter}）。本编解码只认下面这张固定类型表，
 * 流里不存在"类名"这个概念，攻击面从根上没了。
 *
 * <h3>未知类型不静默丢</h3>
 * <p>类型表之外的值走 {@link #TAG_JAVA_FALLBACK}——仍用原生序列化编这一个值，
 * 读侧照样过白名单过滤器。宁可为没预料到的类型付原来的成本，
 * 也不能像"转 0 条 SQL 静默提交"那样把值悄悄丢掉。实测 74 个真实 THL、
 * 33,917 条事件里，这条兜底一次都没被触发。
 */
public final class ThlCodec {

    /** 本编码的版本号，写在文件头 magic 之后，便于将来再改格式。 */
    static final short FORMAT_VERSION = 1;

    // ---- metadata 值的类型标签 ----
    private static final byte TAG_NULL = 0;
    private static final byte TAG_STRING = 1;
    private static final byte TAG_BOOLEAN = 2;
    private static final byte TAG_BYTE = 3;
    private static final byte TAG_SHORT = 4;
    private static final byte TAG_INT = 5;
    private static final byte TAG_LONG = 6;
    private static final byte TAG_FLOAT = 7;
    private static final byte TAG_DOUBLE = 8;
    private static final byte TAG_BYTES = 9;
    private static final byte TAG_LIST = 10;
    private static final byte TAG_MAP = 11;
    private static final byte TAG_LOBREF = 12;
    private static final byte TAG_CHAR = 13;
    private static final byte TAG_TIMESTAMP = 14;
    private static final byte TAG_BIGDECIMAL = 15;
    /** 兜底：类型表之外的值退回原生序列化，只影响这一个值。 */
    private static final byte TAG_JAVA_FALLBACK = (byte) 0xFF;

    private ThlCodec() {
    }

    // ================================================================ 编码

    public static byte[] encode(THLEvent e) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
        DataOutputStream out = new DataOutputStream(baos);

        out.writeLong(e.getSeqno());
        out.writeShort(e.getFragno());
        out.writeBoolean(e.isLastFrag());
        out.writeShort(e.getType());
        out.writeLong(e.getEpochNumber());

        writeString(out, e.getSourceId());
        writeString(out, e.getComment());
        writeString(out, e.getEventId());
        writeString(out, e.getShardId());

        writeTimestamp(out, e.getSourceTstamp());
        writeTimestamp(out, e.getLocalEnqueueTstamp());

        writeBytes(out, e.getData());
        writeMap(out, e.getMetadata());

        out.flush();
        return baos.toByteArray();
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        // 不用 writeUTF：它有 64KB 上限，而 metadata 里的 column_names / enum_set_values
        // 在宽表上可以轻松超过——超了会抛 UTFDataFormatException，且是运行期才炸。
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static void writeBytes(DataOutputStream out, byte[] b) throws IOException {
        if (b == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(b.length);
        out.write(b);
    }

    private static void writeTimestamp(DataOutputStream out, Timestamp ts) throws IOException {
        if (ts == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        // getTime() 已含毫秒；nanos 单独存，否则亚毫秒精度会丢——
        // 而延迟指标就是拿 sourceTstamp 算的，精度丢了指标跟着不准。
        out.writeLong(ts.getTime());
        out.writeInt(ts.getNanos());
    }

    private static void writeMap(DataOutputStream out, Map<String, Object> map) throws IOException {
        if (map == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(map.size());
        for (Map.Entry<String, Object> en : map.entrySet()) {
            writeString(out, en.getKey());
            writeValue(out, en.getValue());
        }
    }

    private static void writeValue(DataOutputStream out, Object v) throws IOException {
        if (v == null) {
            out.writeByte(TAG_NULL);
        } else if (v instanceof String s) {
            out.writeByte(TAG_STRING);
            writeString(out, s);
        } else if (v instanceof Boolean b) {
            out.writeByte(TAG_BOOLEAN);
            out.writeBoolean(b);
        } else if (v instanceof Byte b) {
            out.writeByte(TAG_BYTE);
            out.writeByte(b);
        } else if (v instanceof Short s) {
            out.writeByte(TAG_SHORT);
            out.writeShort(s);
        } else if (v instanceof Integer i) {
            out.writeByte(TAG_INT);
            out.writeInt(i);
        } else if (v instanceof Long l) {
            out.writeByte(TAG_LONG);
            out.writeLong(l);
        } else if (v instanceof Float f) {
            out.writeByte(TAG_FLOAT);
            out.writeFloat(f);
        } else if (v instanceof Double d) {
            out.writeByte(TAG_DOUBLE);
            out.writeDouble(d);
        } else if (v instanceof Character c) {
            out.writeByte(TAG_CHAR);
            out.writeChar(c);
        } else if (v instanceof byte[] b) {
            out.writeByte(TAG_BYTES);
            writeBytes(out, b);
        } else if (v instanceof Timestamp ts) {
            out.writeByte(TAG_TIMESTAMP);
            writeTimestamp(out, ts);
        } else if (v instanceof BigDecimal bd) {
            out.writeByte(TAG_BIGDECIMAL);
            writeString(out, bd.toPlainString());
        } else if (v instanceof LobRef ref) {
            // 大字段的落盘引用。字段只有 file/length/md5，直接摊平；
            // discarded 的 LobRef 没有 file，用 null 串表示。
            out.writeByte(TAG_LOBREF);
            writeString(out, ref.file());
            out.writeLong(ref.length());
            writeString(out, ref.md5());
        } else if (v instanceof List<?> list) {
            out.writeByte(TAG_LIST);
            out.writeInt(list.size());
            for (Object o : list) {
                writeValue(out, o);
            }
        } else if (v instanceof Map<?, ?> m) {
            out.writeByte(TAG_MAP);
            out.writeInt(m.size());
            for (Map.Entry<?, ?> en : m.entrySet()) {
                writeString(out, String.valueOf(en.getKey()));
                writeValue(out, en.getValue());
            }
        } else {
            // 没预料到的类型：退回原生序列化，只影响这一个值，绝不丢
            out.writeByte(TAG_JAVA_FALLBACK);
            ByteArrayOutputStream tmp = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(tmp)) {
                oos.writeObject(v);
            }
            writeBytes(out, tmp.toByteArray());
        }
    }

    // ================================================================ 解码

    public static THLEvent decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        THLEvent e = new THLEvent();

        e.setSeqno(in.readLong());
        e.setFragno(in.readShort());
        e.setLastFrag(in.readBoolean());
        e.setType(in.readShort());
        e.setEpochNumber(in.readLong());

        e.setSourceId(readString(in));
        e.setComment(readString(in));
        e.setEventId(readString(in));
        e.setShardId(readString(in));

        e.setSourceTstamp(readTimestamp(in));
        e.setLocalEnqueueTstamp(readTimestamp(in));

        e.setData(readBytes(in));
        e.setMetadata(readMap(in));
        return e;
    }

    private static String readString(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0) {
            return null;
        }
        byte[] b = new byte[n];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0) {
            return null;
        }
        byte[] b = new byte[n];
        in.readFully(b);
        return b;
    }

    private static Timestamp readTimestamp(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        Timestamp ts = new Timestamp(in.readLong());
        ts.setNanos(in.readInt());
        return ts;
    }

    private static Map<String, Object> readMap(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0) {
            return null;
        }
        Map<String, Object> m = new HashMap<>(Math.max(8, n * 2));
        for (int i = 0; i < n; i++) {
            String k = readString(in);
            m.put(k, readValue(in));
        }
        return m;
    }

    private static Object readValue(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        switch (tag) {
            case TAG_NULL:
                return null;
            case TAG_STRING:
                return readString(in);
            case TAG_BOOLEAN:
                return in.readBoolean();
            case TAG_BYTE:
                return in.readByte();
            case TAG_SHORT:
                return in.readShort();
            case TAG_INT:
                return in.readInt();
            case TAG_LONG:
                return in.readLong();
            case TAG_FLOAT:
                return in.readFloat();
            case TAG_DOUBLE:
                return in.readDouble();
            case TAG_CHAR:
                return in.readChar();
            case TAG_BYTES:
                return readBytes(in);
            case TAG_TIMESTAMP:
                return readTimestamp(in);
            case TAG_BIGDECIMAL:
                return new BigDecimal(readString(in));
            case TAG_LOBREF: {
                String file = readString(in);
                long len = in.readLong();
                String md5 = readString(in);
                return file == null ? LobRef.discarded(len, md5) : LobRef.spilled(file, len, md5);
            }
            case TAG_LIST: {
                int n = in.readInt();
                List<Object> list = new ArrayList<>(Math.max(4, n));
                for (int i = 0; i < n; i++) {
                    list.add(readValue(in));
                }
                return list;
            }
            case TAG_MAP: {
                int n = in.readInt();
                Map<String, Object> m = new HashMap<>(Math.max(8, n * 2));
                for (int i = 0; i < n; i++) {
                    m.put(readString(in), readValue(in));
                }
                return m;
            }
            case TAG_JAVA_FALLBACK: {
                byte[] raw = readBytes(in);
                try (ObjectInputStream ois = ThlObjectInputFilter.install(
                        new ObjectInputStream(new ByteArrayInputStream(raw)))) {
                    return ois.readObject();
                } catch (ClassNotFoundException ex) {
                    throw new IOException("THL 兜底值反序列化失败", ex);
                }
            }
            default:
                // 不静默跳过：未知标签说明文件损坏或版本不匹配，继续读下去只会
                // 把错位的字节解释成业务数据（本仓库 §silent-loss 反复吃过这个亏）。
                throw new IOException("未知的 THL 值类型标签: " + tag
                        + "（文件损坏，或由更新版本的编解码写出）");
        }
    }
}
