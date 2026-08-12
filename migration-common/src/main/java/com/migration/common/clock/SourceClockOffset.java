package com.migration.common.clock;

/**
 * 源库时钟与本机时钟的偏移，用来把"源端时间戳"折算到本机时钟域。
 *
 * <p><b>为什么必须折算</b>：增量延迟是在 increment 端算的
 * {@code 本机 now − 事件的源端时间戳}。两台机器的时钟差会**整体**加到这个差值上 ——
 * 时钟差一分钟，延迟就凭空多（或少）一分钟；少的那一侧还会算出负数被丢掉，
 * 指标从此停在旧值上不动。所以捕获端把源端时间戳折算成本机时钟域之后再下发，
 * 下游的减法就只剩真实链路耗时。
 *
 * <p><b>测法</b>：取源库的"当前时间"，与调用前后两次本机时间的**中点**相比。
 * 中点抵掉了一半的往返开销 —— 单取调用后的本机时间会把整个 RTT 算进偏移里。
 * 剩余误差是 RTT 的不对称部分，同机房内通常是亚毫秒级。
 *
 * <p>不做校正的口径差异是真实存在过的：MySQL 链路的 RPO 指标一直在做这个校正
 * （{@code MySQLBinlogCapture.clockOffsetMs}），而 Oracle 链路完全没做，
 * PG 链路则干脆用捕获进程读到消息的那一刻当"源端时间"，把最该量的那一段漏掉了。
 */
public final class SourceClockOffset {

    /** {@code 源库时钟 − 本机时钟}，毫秒。正数表示源库时钟更快。 */
    private volatile long offsetMs;
    private volatile boolean measured;

    /**
     * 记一次测量结果。
     *
     * @param sourceNowMs   源库报告的当前时间（毫秒）
     * @param localBeforeMs 发起查询之前的本机时间
     * @param localAfterMs  拿到结果之后的本机时间
     */
    public void observe(long sourceNowMs, long localBeforeMs, long localAfterMs) {
        long localMid = localBeforeMs + (localAfterMs - localBeforeMs) / 2;
        this.offsetMs = sourceNowMs - localMid;
        this.measured = true;
    }

    /** 把源端时间戳折算到本机时钟域；还没测到偏移时原样返回（等价于假设两端同步）。 */
    public long toLocal(long sourceTsMs) {
        return measured ? sourceTsMs - offsetMs : sourceTsMs;
    }

    public long offsetMs() {
        return measured ? offsetMs : 0;
    }

    public boolean isMeasured() {
        return measured;
    }

    /** 偏移是否大到该提醒运维（默认 5 秒）—— 这种量级说明两端 NTP 没对齐。 */
    public boolean isSuspicious(long thresholdMs) {
        return measured && Math.abs(offsetMs) > thresholdMs;
    }
}
