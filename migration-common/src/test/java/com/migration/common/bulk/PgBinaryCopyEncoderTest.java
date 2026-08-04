package com.migration.common.bulk;

import com.migration.common.bulk.PgBinaryCopyEncoder.PgType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PostgreSQL 二进制 COPY 的字节编码。
 *
 * <p>这条通道<b>没有服务端兜底</b>：一个格式正确但语义错误的字节流，PG 会照单全收，
 * 错的数据就这么静默落进目标库了。所以这里按 PostgreSQL 官方 COPY BINARY 的格式
 * 逐字节钉死几个代表值——尤其是 numeric（base-10000 的 weight/dscale 是最容易写反的地方）
 * 与时间类型（纪元是 2000-01-01 而不是 1970-01-01）。
 *
 * <p>另一条防线是"不认识就不编"：类型不在支持集内时必须返回 null / 抛异常，
 * 让调用方降级回 INSERT，而不是猜一个编码。
 */
@DisplayName("PG 二进制 COPY 编码")
class PgBinaryCopyEncoderTest {

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    private static byte[] encode(Object value, PgType type) throws Exception {
        return PgBinaryCopyEncoder.encodeValue(value, type);
    }

    @Test
    @DisplayName("文件头/结尾按官方格式：签名 + flags + 扩展长度，结尾 int16 -1")
    void headerAndTrailer() {
        // 11 字节签名 PGCOPY\n\377\r\n\0 + int32 flags(0) + int32 头部扩展长度(0)
        assertEquals("5047434F50590AFF0D0A00" + "00000000" + "00000000",
                hex(PgBinaryCopyEncoder.header()));
        assertEquals("FFFF", hex(PgBinaryCopyEncoder.trailer()));
    }

    @Test
    @DisplayName("整数/浮点/布尔按大端定长编码")
    void fixedWidthTypes() throws Exception {
        assertEquals("002A", hex(encode(42, PgType.INT2)));
        assertEquals("0000002A", hex(encode(42, PgType.INT4)));
        assertEquals("000000000000002A", hex(encode(42L, PgType.INT8)));
        assertEquals("01", hex(encode(Boolean.TRUE, PgType.BOOL)));
        assertEquals("00", hex(encode(Boolean.FALSE, PgType.BOOL)));
        // 1.0f = 0x3F800000, 1.0d = 0x3FF0000000000000
        assertEquals("3F800000", hex(encode(1.0f, PgType.FLOAT4)));
        assertEquals("3FF0000000000000", hex(encode(1.0d, PgType.FLOAT8)));
    }

    @Test
    @DisplayName("numeric：base-10000 数位 + weight/dscale（写反了会静默改变数值）")
    void numericLayout() throws Exception {
        // 1234.5678 → ndigits=2, weight=0, sign=0, dscale=4, digits=[1234, 5678]
        assertEquals("0002" + "0000" + "0000" + "0004" + "04D2" + "162E",
                hex(encode(new BigDecimal("1234.5678"), PgType.NUMERIC)));

        // 0.5 → 补齐成 0.5000：digits=[5000]，weight=-1（第一个数位在 10000^-1 位上）
        assertEquals("0001" + "FFFF" + "0000" + "0001" + "1388",
                hex(encode(new BigDecimal("0.5"), PgType.NUMERIC)));

        // 12345 → digits=[1, 2345]，weight=1
        assertEquals("0002" + "0001" + "0000" + "0000" + "0001" + "0929",
                hex(encode(new BigDecimal("12345"), PgType.NUMERIC)));

        // -12.34 → sign=0x4000，digits=[12, 3400]，dscale=2
        assertEquals("0002" + "0000" + "4000" + "0002" + "000C" + "0D48",
                hex(encode(new BigDecimal("-12.34"), PgType.NUMERIC)));

        // 0 → 没有数位，但 dscale 要保留（0.00 与 0 在 PG 里显示不同）
        assertEquals("0000" + "0000" + "0000" + "0000", hex(encode(BigDecimal.ZERO, PgType.NUMERIC)));
        assertEquals("0000" + "0000" + "0000" + "0002", hex(encode(new BigDecimal("0.00"), PgType.NUMERIC)));
    }

    @Test
    @DisplayName("日期/时间：纪元是 2000-01-01，不是 1970-01-01")
    void temporalEpochIsYear2000() throws Exception {
        assertEquals("00000000", hex(encode(LocalDate.of(2000, 1, 1), PgType.DATE)));
        assertEquals("00000001", hex(encode(LocalDate.of(2000, 1, 2), PgType.DATE)));
        assertEquals("FFFFFFFF", hex(encode(LocalDate.of(1999, 12, 31), PgType.DATE)));

        assertEquals("0000000000000000",
                hex(encode(LocalDateTime.of(2000, 1, 1, 0, 0, 0), PgType.TIMESTAMP)));
        // 1 秒 = 1_000_000 微秒 = 0x0F4240
        assertEquals("00000000000F4240",
                hex(encode(LocalDateTime.of(2000, 1, 1, 0, 0, 1), PgType.TIMESTAMP)));
    }

    @Test
    @DisplayName("timestamp 取墙上时间：Timestamp 按 JVM 时区转 LocalDateTime，不能直接用 epoch 毫秒")
    void timestampUsesWallClock() throws Exception {
        // 不论 JVM 在哪个时区，"2000-01-01 00:00:00" 这个墙上时间都应编码成 0
        Timestamp ts = Timestamp.valueOf("2000-01-01 00:00:00");
        assertEquals("0000000000000000", hex(encode(ts, PgType.TIMESTAMP)));
    }

    @Test
    @DisplayName("文本/jsonb/uuid/bytea")
    void variableWidthTypes() throws Exception {
        assertArrayEquals("ab".getBytes("UTF-8"), encode("ab", PgType.TEXT));
        // jsonb 的二进制表示前面多一个版本号字节 0x01
        assertEquals("017B7D", hex(encode("{}", PgType.JSONB)));
        assertEquals("7B7D", hex(encode("{}", PgType.JSON)));
        assertEquals("00000000000000010000000000000002",
                hex(encode(new UUID(1L, 2L), PgType.UUID)));
        assertArrayEquals(new byte[]{1, 2, 3}, encode(new byte[]{1, 2, 3}, PgType.BYTEA));
    }

    @Test
    @DisplayName("一行的骨架：字段数 + 每字段长度；NULL 是长度 -1 而不是 0 字节")
    void rowLayoutAndNull() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(out)) {
            PgBinaryCopyEncoder.encodeRow(dos, new Object[]{7, null},
                    new PgType[]{PgType.INT4, PgType.TEXT});
        }
        assertEquals("0002" + "00000004" + "00000007" + "FFFFFFFF", hex(out.toByteArray()));
    }

    @Test
    @DisplayName("类型不在支持集内 → fromTypeName 返回 null（调用方据此放弃 COPY）")
    void unsupportedTypesAreRejected() {
        assertEquals(PgType.INT4, PgType.fromTypeName("int4"));
        assertEquals(PgType.TIMESTAMPTZ, PgType.fromTypeName("timestamptz"));
        assertEquals(PgType.NUMERIC, PgType.fromTypeName("NUMERIC"));
        assertNotNull(PgType.fromTypeName("varchar"));

        assertNull(PgType.fromTypeName("_int4"), "数组类型不支持");
        assertNull(PgType.fromTypeName("interval"), "interval 不支持");
        assertNull(PgType.fromTypeName("my_enum"), "枚举不支持");
        assertNull(PgType.fromTypeName("money"), "money 的二进制表示与 numeric 不同，不支持");
        assertNull(PgType.fromTypeName(null));
    }

    @Test
    @DisplayName("值的 Java 类型不认识 → 抛受检异常（触发整批回退到 INSERT，不写坏数据）")
    void unsupportedValueThrows() {
        assertThrows(PgBinaryCopyEncoder.UnsupportedValueException.class,
                () -> encode(new Object(), PgType.TEXT));
        assertThrows(PgBinaryCopyEncoder.UnsupportedValueException.class,
                () -> encode("not-bytes", PgType.BYTEA));
        assertThrows(PgBinaryCopyEncoder.UnsupportedValueException.class,
                () -> encode("not-a-uuid", PgType.UUID));
    }
}
