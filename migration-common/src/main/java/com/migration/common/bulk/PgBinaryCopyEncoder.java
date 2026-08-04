package com.migration.common.bulk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * PostgreSQL {@code COPY ... WITH (FORMAT binary)} 的行编码器。
 *
 * <p><b>为什么是二进制而不是文本 COPY</b>：文本 COPY 要把每个值渲染成字符串再由服务端解析，
 * 等于绕开类型绑定——本项目增量链路正是因为文本管道踩过 5 类值保真缺陷（时间精度、二进制、布尔、
 * NULL、枚举）才统一收敛到类型化写入。二进制 COPY 按列的实际类型逐字段编码，类型语义不丢，
 * 又能拿到 COPY 相对多值 INSERT 的吞吐优势。
 *
 * <p><b>不认识就不编</b>：{@link PgType#fromTypeName} 只认一组明确、可脱库验证的类型；
 * 数组、枚举、interval、几何类型等一律返回 null，调用方据此<b>拒绝开启 COPY 通道并降级为
 * 语句重写</b>。同理，值的 Java 类型不在支持集内时 {@link #encodeRow} 抛
 * {@link UnsupportedValueException}，由 {@link JdbcCopyChannel} 整批回退到 INSERT 重放。
 * 编码器的完备性因此只影响性能，不影响正确性。
 *
 * <p>格式（PostgreSQL 官方 COPY BINARY）：文件头 {@code PGCOPY\n\377\r\n\0} + flags(int32) +
 * 扩展长度(int32)；每行 = 字段数(int16) + 每字段 [长度(int32，NULL 为 -1) + 数据]；结尾 int16 = -1。
 */
public final class PgBinaryCopyEncoder {

    /** PostgreSQL 的时间纪元是 2000-01-01，不是 1970-01-01。 */
    private static final long PG_EPOCH_SECONDS = 946684800L;
    private static final long PG_EPOCH_MICROS = PG_EPOCH_SECONDS * 1_000_000L;
    private static final long PG_EPOCH_DAYS = 10957L;

    private static final byte[] SIGNATURE = new byte[]{
            'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0
    };

    private PgBinaryCopyEncoder() {
    }

    /** 目标列类型。只收录编码明确、可脱库单测的类型；其余类型走降级。 */
    public enum PgType {
        BOOL, INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC,
        TEXT, BYTEA, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, UUID, JSON, JSONB;

        /**
         * 按 pgjdbc 的 {@code getColumnTypeName} 解析。不支持的类型返回 {@code null}——
         * 调用方必须据此放弃 COPY 而不是猜一个编码。
         */
        public static PgType fromTypeName(String typeName) {
            if (typeName == null) {
                return null;
            }
            switch (typeName.trim().toLowerCase(Locale.ROOT)) {
                case "bool":
                case "boolean":
                    return BOOL;
                case "int2":
                case "smallint":
                case "smallserial":
                    return INT2;
                case "int4":
                case "int":
                case "integer":
                case "serial":
                    return INT4;
                case "int8":
                case "bigint":
                case "bigserial":
                    return INT8;
                case "float4":
                case "real":
                    return FLOAT4;
                case "float8":
                case "double precision":
                    return FLOAT8;
                case "numeric":
                case "decimal":
                    return NUMERIC;
                case "text":
                case "varchar":
                case "character varying":
                case "bpchar":
                case "char":
                case "character":
                case "name":
                    return TEXT;
                case "bytea":
                    return BYTEA;
                case "date":
                    return DATE;
                case "time":
                case "time without time zone":
                    return TIME;
                case "timestamp":
                case "timestamp without time zone":
                    return TIMESTAMP;
                case "timestamptz":
                case "timestamp with time zone":
                    return TIMESTAMPTZ;
                case "uuid":
                    return UUID;
                case "json":
                    return JSON;
                case "jsonb":
                    return JSONB;
                default:
                    return null;
            }
        }
    }

    /** 值的 Java 类型落在编码器支持集之外。触发整批回退到 INSERT，不是任务失败。 */
    public static class UnsupportedValueException extends Exception {
        public UnsupportedValueException(String message) {
            super(message);
        }
    }

    /** COPY 二进制流的文件头。 */
    public static byte[] header() {
        ByteArrayOutputStream out = new ByteArrayOutputStream(19);
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.write(SIGNATURE);
            dos.writeInt(0);   // flags：无 OID
            dos.writeInt(0);   // 头部扩展长度
        } catch (IOException e) {
            throw new IllegalStateException("写 COPY 头失败", e);
        }
        return out.toByteArray();
    }

    /** COPY 二进制流的结尾标记。 */
    public static byte[] trailer() {
        return new byte[]{(byte) 0xFF, (byte) 0xFF};   // int16 -1
    }

    /** 编码一行。{@code types.length} 必须与 {@code row.length} 相等。 */
    public static void encodeRow(DataOutputStream out, Object[] row, PgType[] types)
            throws IOException, UnsupportedValueException {
        if (row.length != types.length) {
            throw new UnsupportedValueException("列数与类型数不一致: " + row.length + " vs " + types.length);
        }
        out.writeShort(row.length);
        for (int i = 0; i < row.length; i++) {
            if (row[i] == null) {
                out.writeInt(-1);
                continue;
            }
            byte[] encoded = encodeValue(row[i], types[i]);
            out.writeInt(encoded.length);
            out.write(encoded);
        }
    }

    static byte[] encodeValue(Object value, PgType type) throws UnsupportedValueException {
        switch (type) {
            case BOOL:
                return new byte[]{(byte) (toBoolean(value) ? 1 : 0)};
            case INT2:
                return int16(shortValue(value));
            case INT4:
                return int32((int) longValue(value));
            case INT8:
                return int64(longValue(value));
            case FLOAT4:
                return int32(Float.floatToIntBits((float) doubleValue(value)));
            case FLOAT8:
                return int64(Double.doubleToLongBits(doubleValue(value)));
            case NUMERIC:
                return numeric(bigDecimalValue(value));
            case TEXT:
            case JSON:
                return text(value).getBytes(StandardCharsets.UTF_8);
            case JSONB: {
                // jsonb 的二进制表示是 [版本号 1][json 文本]
                byte[] json = text(value).getBytes(StandardCharsets.UTF_8);
                byte[] out = new byte[json.length + 1];
                out[0] = 1;
                System.arraycopy(json, 0, out, 1, json.length);
                return out;
            }
            case BYTEA:
                if (value instanceof byte[]) {
                    return (byte[]) value;
                }
                throw new UnsupportedValueException("bytea 列收到非 byte[] 值: " + value.getClass().getName());
            case DATE:
                return int32((int) (toLocalDate(value).toEpochDay() - PG_EPOCH_DAYS));
            case TIME:
                return int64(toLocalTime(value).toNanoOfDay() / 1000L);
            case TIMESTAMP:
                return int64(localDateTimeToPgMicros(toLocalDateTime(value)));
            case TIMESTAMPTZ:
                return int64(instantToPgMicros(toInstant(value)));
            case UUID: {
                java.util.UUID uuid = toUuid(value);
                byte[] out = new byte[16];
                System.arraycopy(int64(uuid.getMostSignificantBits()), 0, out, 0, 8);
                System.arraycopy(int64(uuid.getLeastSignificantBits()), 0, out, 8, 8);
                return out;
            }
            default:
                throw new UnsupportedValueException("未实现的类型编码: " + type);
        }
    }

    // ==================== numeric ====================

    /**
     * numeric 的二进制布局：ndigits(int16) + weight(int16) + sign(int16) + dscale(int16)
     * + ndigits 个 base-10000 数位（int16，高位在前）。weight 是<b>第一个数位</b>相对小数点的
     * base-10000 位权（0 = 个位组），dscale 是小数点后的十进制位数。
     */
    static byte[] numeric(BigDecimal value) throws UnsupportedValueException {
        int sign = value.signum() < 0 ? 0x4000 : 0x0000;
        BigDecimal abs = value.abs();
        int scale = abs.scale();
        if (scale < 0) {
            // 1E+3 这类负 scale：先规整成 scale=0，数值不变
            abs = abs.setScale(0);
            scale = 0;
        }
        int dscale = scale;
        // 小数部分补齐到 4 的倍数，才能整齐地切成 base-10000 组。
        // 必须用 setScale 放大 scale（0.5 → 0.5000，unscaled 5 → 5000），
        // movePointRight 是反方向（缩小 scale），会把 0.5 编成 0.05。
        int pad = (4 - (scale % 4)) % 4;
        BigInteger unscaled = abs.setScale(scale + pad).unscaledValue();
        int fracGroups = (scale + pad) / 4;

        List<Integer> groupsLowFirst = new ArrayList<>();
        BigInteger base = BigInteger.valueOf(10000);
        while (unscaled.signum() != 0) {
            BigInteger[] qr = unscaled.divideAndRemainder(base);
            groupsLowFirst.add(qr[1].intValue());
            unscaled = qr[0];
        }
        if (groupsLowFirst.isEmpty()) {
            // 0：没有数位，但 dscale 仍要保留（0.00 与 0 在 PG 里显示不同）
            return numericBytes(0, 0, sign, dscale, new int[0]);
        }
        int weight = groupsLowFirst.size() - fracGroups - 1;

        // 高位在前，并去掉末尾的全零组（PG 允许省略；ndigits 必须与实际数组长度一致）
        int end = 0;
        while (end < groupsLowFirst.size() && groupsLowFirst.get(end) == 0) {
            end++;
        }
        int[] digits = new int[groupsLowFirst.size() - end];
        for (int i = 0; i < digits.length; i++) {
            digits[i] = groupsLowFirst.get(groupsLowFirst.size() - 1 - i);
        }
        return numericBytes(digits.length, weight, sign, dscale, digits);
    }

    private static byte[] numericBytes(int ndigits, int weight, int sign, int dscale, int[] digits) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8 + digits.length * 2);
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.writeShort(ndigits);
            dos.writeShort(weight);
            dos.writeShort(sign);
            dos.writeShort(dscale);
            for (int d : digits) {
                dos.writeShort(d);
            }
        } catch (IOException e) {
            throw new IllegalStateException("编码 numeric 失败", e);
        }
        return out.toByteArray();
    }

    // ==================== 值转换 ====================

    private static boolean toBoolean(Object v) throws UnsupportedValueException {
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue() != 0;
        }
        if (v instanceof CharSequence) {
            String s = v.toString().trim();
            return "t".equalsIgnoreCase(s) || "true".equalsIgnoreCase(s) || "1".equals(s) || "y".equalsIgnoreCase(s);
        }
        throw new UnsupportedValueException("bool 列收到不支持的值类型: " + v.getClass().getName());
    }

    private static short shortValue(Object v) throws UnsupportedValueException {
        return (short) longValue(v);
    }

    private static long longValue(Object v) throws UnsupportedValueException {
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        if (v instanceof Boolean) {
            return ((Boolean) v) ? 1 : 0;
        }
        if (v instanceof CharSequence) {
            try {
                return new BigDecimal(v.toString().trim()).longValueExact();
            } catch (RuntimeException e) {
                throw new UnsupportedValueException("整型列收到无法解析的字符串: " + v);
            }
        }
        throw new UnsupportedValueException("整型列收到不支持的值类型: " + v.getClass().getName());
    }

    private static double doubleValue(Object v) throws UnsupportedValueException {
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        if (v instanceof CharSequence) {
            try {
                return Double.parseDouble(v.toString().trim());
            } catch (NumberFormatException e) {
                throw new UnsupportedValueException("浮点列收到无法解析的字符串: " + v);
            }
        }
        throw new UnsupportedValueException("浮点列收到不支持的值类型: " + v.getClass().getName());
    }

    private static BigDecimal bigDecimalValue(Object v) throws UnsupportedValueException {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof BigInteger) {
            return new BigDecimal((BigInteger) v);
        }
        if (v instanceof Number || v instanceof CharSequence) {
            try {
                return new BigDecimal(v.toString().trim());
            } catch (NumberFormatException e) {
                throw new UnsupportedValueException("numeric 列收到无法解析的值: " + v);
            }
        }
        throw new UnsupportedValueException("numeric 列收到不支持的值类型: " + v.getClass().getName());
    }

    private static String text(Object v) throws UnsupportedValueException {
        if (v instanceof CharSequence || v instanceof Number || v instanceof Boolean
                || v instanceof java.util.UUID || v instanceof java.sql.Date || v instanceof Time
                || v instanceof Timestamp || v instanceof java.time.temporal.Temporal) {
            return v.toString();
        }
        if (v instanceof char[]) {
            return new String((char[]) v);
        }
        throw new UnsupportedValueException("文本列收到不支持的值类型: " + v.getClass().getName());
    }

    private static LocalDate toLocalDate(Object v) throws UnsupportedValueException {
        if (v instanceof java.sql.Date) {
            return ((java.sql.Date) v).toLocalDate();
        }
        if (v instanceof LocalDate) {
            return (LocalDate) v;
        }
        if (v instanceof LocalDateTime) {
            return ((LocalDateTime) v).toLocalDate();
        }
        if (v instanceof Timestamp) {
            return ((Timestamp) v).toLocalDateTime().toLocalDate();
        }
        if (v instanceof java.util.Date) {
            return Instant.ofEpochMilli(((java.util.Date) v).getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
        }
        throw new UnsupportedValueException("date 列收到不支持的值类型: " + v.getClass().getName());
    }

    private static LocalTime toLocalTime(Object v) throws UnsupportedValueException {
        if (v instanceof Time) {
            return ((Time) v).toLocalTime();
        }
        if (v instanceof LocalTime) {
            return (LocalTime) v;
        }
        if (v instanceof LocalDateTime) {
            return ((LocalDateTime) v).toLocalTime();
        }
        if (v instanceof Timestamp) {
            return ((Timestamp) v).toLocalDateTime().toLocalTime();
        }
        throw new UnsupportedValueException("time 列收到不支持的值类型: " + v.getClass().getName());
    }

    /**
     * {@code timestamp without time zone} 取的是<b>墙上时间</b>：Timestamp 要按 JVM 默认时区转成
     * LocalDateTime 再算偏移，直接用 epoch 毫秒会平移一个时区差（本项目元数据库时间戳踩过同类问题）。
     */
    private static LocalDateTime toLocalDateTime(Object v) throws UnsupportedValueException {
        if (v instanceof Timestamp) {
            return ((Timestamp) v).toLocalDateTime();
        }
        if (v instanceof LocalDateTime) {
            return (LocalDateTime) v;
        }
        if (v instanceof LocalDate) {
            return ((LocalDate) v).atStartOfDay();
        }
        if (v instanceof OffsetDateTime) {
            return ((OffsetDateTime) v).toLocalDateTime();
        }
        if (v instanceof java.sql.Date) {
            return ((java.sql.Date) v).toLocalDate().atStartOfDay();
        }
        if (v instanceof java.util.Date) {
            return Instant.ofEpochMilli(((java.util.Date) v).getTime())
                    .atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        throw new UnsupportedValueException("timestamp 列收到不支持的值类型: " + v.getClass().getName());
    }

    /** {@code timestamptz} 存的是时间点，按 UTC 纪元偏移编码。 */
    private static Instant toInstant(Object v) throws UnsupportedValueException {
        if (v instanceof Timestamp) {
            return ((Timestamp) v).toInstant();
        }
        if (v instanceof Instant) {
            return (Instant) v;
        }
        if (v instanceof OffsetDateTime) {
            return ((OffsetDateTime) v).toInstant();
        }
        if (v instanceof LocalDateTime) {
            return ((LocalDateTime) v).atZone(ZoneId.systemDefault()).toInstant();
        }
        if (v instanceof java.util.Date) {
            return Instant.ofEpochMilli(((java.util.Date) v).getTime());
        }
        throw new UnsupportedValueException("timestamptz 列收到不支持的值类型: " + v.getClass().getName());
    }

    private static java.util.UUID toUuid(Object v) throws UnsupportedValueException {
        if (v instanceof java.util.UUID) {
            return (java.util.UUID) v;
        }
        if (v instanceof CharSequence) {
            try {
                return UUID.fromString(v.toString().trim());
            } catch (IllegalArgumentException e) {
                throw new UnsupportedValueException("uuid 列收到非法字符串: " + v);
            }
        }
        throw new UnsupportedValueException("uuid 列收到不支持的值类型: " + v.getClass().getName());
    }

    static long localDateTimeToPgMicros(LocalDateTime dt) {
        long seconds = dt.toEpochSecond(java.time.ZoneOffset.UTC);
        return (seconds - PG_EPOCH_SECONDS) * 1_000_000L + dt.getNano() / 1000L;
    }

    static long instantToPgMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L - PG_EPOCH_MICROS;
    }

    private static byte[] int16(short v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    private static byte[] int32(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] int64(long v) {
        return new byte[]{
                (byte) (v >> 56), (byte) (v >> 48), (byte) (v >> 40), (byte) (v >> 32),
                (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v
        };
    }
}
