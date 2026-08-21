package com.migration.traffic.capture;

import com.migration.traffic.model.SourceFingerprint;

import java.util.List;
import java.util.Properties;

/**
 * 语句流来源的抽象。
 *
 * <p>v1 只有 {@link GeneralLogTrafficSource} 一个实现，但接口先立起来，
 * 是为了让将来的审计插件 / 抓包方案能平移进来而不动捕获主循环——
 * 那两条路子的取舍完全不同（侵入性 vs 完整性），迟早要并存。
 */
public interface TrafficSource extends AutoCloseable {

    /** 开启捕获。实现方需在此完成"记录源库原始状态 → 改状态"，以便 {@link #close()} 还原。 */
    void open(Properties cfg) throws Exception;

    /**
     * 拉一批语句，<b>已按源库真实发生顺序排好</b>。
     * 无新语句时返回空列表（不是 null），不阻塞。
     */
    List<RawStatement> poll() throws Exception;

    /** 源库身份与语义环境，供录制头与回放前校验使用。 */
    SourceFingerprint fingerprint();

    /** 本次 poll 之后源端还积压多少（追不上的早期信号）；未知返回 -1。 */
    long backlog();

    /** 关闭并<b>还原源库状态</b>。实现必须幂等——它会被 shutdown hook 与正常路径各调一次。 */
    @Override
    void close();

    /** 源库上一条原始语句记录。 */
    final class RawStatement {
        /** 发生时刻，epoch 微秒（源库时钟，时区无关）。 */
        public long epochMicros;
        /** 连接 id（{@code general_log.thread_id}）。 */
        public long threadId;
        /** 原始 {@code command_type}：Query / Execute / Init DB / Connect / Quit / Prepare / ... */
        public String commandType;
        /** 语句原文（已按 UTF-8 从原始字节解码）。 */
        public String argument;
        /** 归一后的 {@code user@host}。 */
        public String userHost;
    }
}
