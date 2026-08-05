package com.migration.common.position;

/**
 * 把各引擎的原生位点折算成一个<b>可比大小的标量</b>，只服务于"位点不许回退"这条守卫。
 *
 * <p>它<b>不是</b>位点本身：折算是有损的（GTID 集、resume token 都折不出来），
 * 续传永远用 {@link CheckpointRecord#getPayload()} 里的原生位点。折算失败一律返回
 * {@link #UNKNOWN}，守卫见到 UNKNOWN 自动降级为"本次不校验单调"——
 * 宁可少拦一次，也不能拿一个瞎算的数把正常推进的位点拦下来（那会让位点永久卡死）。
 *
 * <p><b>跨源不可比</b>：折算值只在"同一个源实例、同一条链路"内有意义。主备倒换换了源库之后，
 * 新源的坐标与旧源毫无关系（往往还更小），所以倒换必须删掉位点行重来，而不是指望守卫放行。
 */
public final class MonotonicKey {

    /** 折算不出可比标量（GTID 集这类）。守卫见到它就跳过单调校验。 */
    public static final long UNKNOWN = -1L;

    private MonotonicKey() {
    }

    /**
     * MySQL / TiDB binlog 坐标：{@code 文件序号 << 32 | 位点}。
     * 单个 binlog 文件上限 1GB 远小于 2^32，位点不会溢出到文件号那一段。
     */
    public static long ofBinlog(String binlogFile, long position) {
        long seq = binlogFileSeq(binlogFile);
        if (seq < 0 || position < 0) {
            return UNKNOWN;
        }
        return (seq << 32) | (position & 0xFFFFFFFFL);
    }

    /** 从 {@code mysql-bin.000123} 取出 123；取不出返回 -1。 */
    public static long binlogFileSeq(String binlogFile) {
        if (binlogFile == null) {
            return -1;
        }
        int dot = binlogFile.lastIndexOf('.');
        if (dot < 0 || dot == binlogFile.length() - 1) {
            return -1;
        }
        String suffix = binlogFile.substring(dot + 1).trim();
        try {
            return Long.parseLong(suffix);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** PostgreSQL LSN 文本 {@code 1A/2B3C4D5E} → 数值；解析失败返回 UNKNOWN。 */
    public static long ofLsn(String lsn) {
        if (lsn == null || lsn.trim().isEmpty()) {
            return UNKNOWN;
        }
        String[] parts = lsn.trim().split("/");
        if (parts.length != 2) {
            return UNKNOWN;
        }
        try {
            long segment = Long.parseLong(parts[0], 16);
            long offset = Long.parseLong(parts[1], 16);
            return (segment << 32) | offset;
        } catch (NumberFormatException e) {
            return UNKNOWN;
        }
    }

    /** Oracle SCN / TiDB TSO / THL seqno / Redis 复制偏移：数值本身即单调量。 */
    public static long ofNumeric(long value) {
        return value < 0 ? UNKNOWN : value;
    }

    /** Mongo resume token 的 clusterTime：{@code 秒 << 32 | inc}。 */
    public static long ofClusterTime(long seconds, long inc) {
        if (seconds < 0 || inc < 0) {
            return UNKNOWN;
        }
        return (seconds << 32) | (inc & 0xFFFFFFFFL);
    }

    /** Kafka：{@code 分区 << 48 | offset}（同一分区内才有比较意义，故 streamKey 要带 partition）。 */
    public static long ofKafka(int partition, long offset) {
        if (partition < 0 || offset < 0) {
            return UNKNOWN;
        }
        return ((long) partition << 48) | (offset & 0xFFFFFFFFFFFFL);
    }

    /** 中心库 {@code monotonic_key} 列不存负数：UNKNOWN 落成 0，等价于"该行不做单调校验"。 */
    public static long toColumn(long key) {
        return key == UNKNOWN || key < 0 ? 0L : key;
    }
}
