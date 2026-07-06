package com.migration.capture.binlog;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link TimeV2Decoder} TIME2 符号感知解码测试。
 * 回归：连接器默认实现把负 TIME 解成大正数（-100:00:00 → +924:00:00）。
 */
@DisplayName("TimeV2Decoder 符号感知解码")
class TimeV2DecoderTest {

    private static long packed(long h, long m, long s) {
        return (h << 12) | (m << 6) | s;
    }

    private static long ms(long h, long m, long s) {
        return (h * 3600 + m * 60 + s) * 1000L;
    }

    @Test
    @DisplayName("正值：普通与超 24h（fsp=0）")
    void positive() {
        assertEquals(ms(12, 30, 45), TimeV2Decoder.packedToMillis(0x800000L + packed(12, 30, 45), 0, 0));
        assertEquals(ms(25, 30, 45), TimeV2Decoder.packedToMillis(0x800000L + packed(25, 30, 45), 0, 0));
        assertEquals(ms(800, 0, 1), TimeV2Decoder.packedToMillis(0x800000L + packed(800, 0, 1), 0, 0));
        assertEquals(ms(838, 59, 59), TimeV2Decoder.packedToMillis(0x800000L + packed(838, 59, 59), 0, 0));
        assertEquals(0, TimeV2Decoder.packedToMillis(0x800000L, 0, 0));
    }

    @Test
    @DisplayName("负值回归：-100:00:00 与 -838:59:59 不再变成大正数")
    void negative() {
        assertEquals(-ms(100, 0, 0), TimeV2Decoder.packedToMillis(0x800000L - packed(100, 0, 0), 0, 0));
        assertEquals(-ms(838, 59, 59), TimeV2Decoder.packedToMillis(0x800000L - packed(838, 59, 59), 0, 0));
        assertEquals(-ms(0, 0, 1), TimeV2Decoder.packedToMillis(0x800000L - packed(0, 0, 1), 0, 0));
    }

    @Test
    @DisplayName("带小数秒：正负借位在整体补码中自动成立")
    void fractional() {
        // fsp=3 → fspBytes=2（4 位十进制小数），12:00:00.2500 → +250ms
        long combinedPos = ((0x800000L + packed(12, 0, 0)) << 16) | 2500;
        assertEquals(ms(12, 0, 0) + 250, TimeV2Decoder.packedToMillis(combinedPos, 2, 3));

        // fsp=1 → fspBytes=1（2 位十进制小数），-00:00:01.50 → -1500ms（整体补码：offset - (packed<<8 | 50)）
        long combinedNeg = (0x800000L << 8) - ((packed(0, 0, 1) << 8) | 50);
        assertEquals(-1500, TimeV2Decoder.packedToMillis(combinedNeg, 1, 1));
    }
}
