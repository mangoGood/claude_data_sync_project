package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * MySQL TIME2（5.6.4+ 二进制格式）符号感知解码。
 *
 * <p>连接器默认实现丢失负号（负 TIME 按补码被解成大正数，如 -100:00:00 → +924:00:00）。
 * TIME2 磁盘格式为 offset-binary：3 字节大端整数 = 0x800000 + 有符号打包值
 * （打包位布局 [1 sign][1 保留][10 hour][6 min][6 sec]），小数秒（fsp）字节与整数部分
 * 构成一个整体补码数——把整数+小数拼成一个大整数后减去偏移，借位自动成立。
 */
public final class TimeV2Decoder {

    private TimeV2Decoder() {
    }

    /** 从流中读取 TIME2 值（meta=fsp），返回符号正确的 java.sql.Time（毫秒可为负/超 24h）。 */
    public static java.sql.Time readSignedTime(int meta, ByteArrayInputStream inputStream) throws IOException {
        int fspBytes = (meta + 1) / 2; // fsp 1-2→1字节, 3-4→2, 5-6→3
        long intPart = bigEndian(inputStream, 3);
        long frac = fspBytes > 0 ? bigEndian(inputStream, fspBytes) : 0;
        long combined = (intPart << (8 * fspBytes)) | frac;
        long millis = packedToMillis(combined, fspBytes, meta);
        return new java.sql.Time(millis);
    }

    /**
     * offset-binary 打包值 → 符号正确的时长毫秒。
     * combined = (3字节整数部分 << 8*fspBytes) | 小数部分；fsp 为列的小数秒精度（0-6）。
     */
    public static long packedToMillis(long combined, int fspBytes, int fsp) {
        long offset = 0x800000L << (8 * fspBytes);
        long shifted = combined - offset; // 有符号：负 TIME 的借位在整体补码减法中自动成立
        boolean negative = shifted < 0;
        long abs = Math.abs(shifted);

        long fracAbs = fspBytes > 0 ? (abs & ((1L << (8 * fspBytes)) - 1)) : 0;
        long intAbs = abs >>> (8 * fspBytes);

        long hours = (intAbs >> 12) & 0x3FF;
        long minutes = (intAbs >> 6) & 0x3F;
        long seconds = intAbs & 0x3F;

        // 小数字段按 fsp 位数存储（2位/4位/6位十进制），换算到微秒
        int digits = fspBytes * 2;
        long micros = digits > 0 ? fracAbs * pow10(6 - digits) : 0;

        long millis = (hours * 3600 + minutes * 60 + seconds) * 1000L + micros / 1000;
        return negative ? -millis : millis;
    }

    private static long bigEndian(ByteArrayInputStream in, int len) throws IOException {
        long v = 0;
        for (int i = 0; i < len; i++) {
            v = (v << 8) | in.readInteger(1);
        }
        return v;
    }

    private static long pow10(int n) {
        long r = 1;
        for (int i = 0; i < n; i++) r *= 10;
        return r;
    }
}
