package com.migration.common.lob;

import java.util.Properties;

/**
 * 大字段搬运的尺寸参数。
 *
 * <p>三个尺寸的含义完全不同，别混：
 * <ul>
 *   <li><b>sourceChunkBytes</b>（默认 8MB）：源端一次 {@code SUBSTRING} 拉多少。
 *       这块是<b>真进堆</b>的，直接决定进程内存下限，不能调大；</li>
 *   <li><b>appendBlockBytes</b>（默认 256MB）：分块追加时一条 UPDATE 携带多少。
 *       它是<b>流式发出去</b>的（驱动按 blobSendChunkSize 分片推），不进堆，
 *       所以可以远大于 chunk。调大它能显著减少 InnoDB 的 LOB 重写次数：
 *       1GB 用 256MB 块只追加 4 次，用 8MB 块就要 128 次、写放大差一个数量级；</li>
 *   <li><b>packetSafetyMarginBytes</b>（默认 1MB）：判定"单条语句放不放得下"时预留给
 *       其余列和协议开销的余量。</li>
 * </ul>
 */
public final class LobWriteOptions {

    public static final String KEY_SOURCE_CHUNK = "migration.lob.source.chunk.bytes";
    public static final String KEY_APPEND_BLOCK = "migration.lob.append.block.bytes";
    public static final String KEY_SAFETY_MARGIN = "migration.lob.packet.safety.margin.bytes";
    public static final String KEY_INLINE_THRESHOLD = "migration.lob.inline.threshold.bytes";

    /**
     * 源端单次拉取大小，默认 32MB。
     *
     * <p>这个值是实测定的，不是拍的。在 1GB 的值上，{@code SELECT SUBSTRING(col, pos, len)}
     * 的耗时几乎与 len 无关（8MB→1210ms、32MB→924ms、128MB→953ms）——代价压倒性地来自
     * 每次调用的固定开销（服务端要把 LOB 读出来才能切）。也就是说块开大在时间上几乎白赚，
     * 只在内存上线性增长：8MB 块要 128 次调用，32MB 块只要 32 次。
     *
     * <p>32MB 是"256MB 进程上限"下的稳妥档位（实测 -Xmx144m 时峰值堆 41MB@8MB 块）。
     * 内存更紧就调小，代价是线性变慢。
     */
    public static final int DEFAULT_SOURCE_CHUNK = 32 * 1024 * 1024;
    public static final long DEFAULT_APPEND_BLOCK = 256L * 1024 * 1024;
    public static final long DEFAULT_SAFETY_MARGIN = 1024L * 1024;
    /** 小于该长度的值不值得走流式（一次查询更快），默认 4MB。 */
    public static final long DEFAULT_INLINE_THRESHOLD = 4L * 1024 * 1024;

    private final int sourceChunkBytes;
    private final long appendBlockBytes;
    private final long packetSafetyMarginBytes;
    private final long inlineThresholdBytes;

    public LobWriteOptions(int sourceChunkBytes, long appendBlockBytes,
                           long packetSafetyMarginBytes, long inlineThresholdBytes) {
        // 只兜 0/负数（配置写错会变成死循环），不设人为下限——
        // 下限会把测试里刻意调小的尺寸悄悄放大，让"分块"这件事在单测里根本没发生。
        this.sourceChunkBytes = Math.max(1, sourceChunkBytes);
        this.appendBlockBytes = Math.max(1, appendBlockBytes);
        this.packetSafetyMarginBytes = Math.max(0, packetSafetyMarginBytes);
        this.inlineThresholdBytes = Math.max(0, inlineThresholdBytes);
    }

    public static LobWriteOptions defaults() {
        return new LobWriteOptions(DEFAULT_SOURCE_CHUNK, DEFAULT_APPEND_BLOCK,
                DEFAULT_SAFETY_MARGIN, DEFAULT_INLINE_THRESHOLD);
    }

    public static LobWriteOptions from(Properties props) {
        if (props == null) {
            return defaults();
        }
        return new LobWriteOptions(
                (int) parse(props, KEY_SOURCE_CHUNK, DEFAULT_SOURCE_CHUNK),
                parse(props, KEY_APPEND_BLOCK, DEFAULT_APPEND_BLOCK),
                parse(props, KEY_SAFETY_MARGIN, DEFAULT_SAFETY_MARGIN),
                parse(props, KEY_INLINE_THRESHOLD, DEFAULT_INLINE_THRESHOLD));
    }

    private static long parse(Properties props, String key, long fallback) {
        try {
            String v = props.getProperty(key);
            return (v == null || v.trim().isEmpty()) ? fallback : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public int sourceChunkBytes() {
        return sourceChunkBytes;
    }

    public long appendBlockBytes() {
        return appendBlockBytes;
    }

    public long packetSafetyMarginBytes() {
        return packetSafetyMarginBytes;
    }

    public long inlineThresholdBytes() {
        return inlineThresholdBytes;
    }
}
