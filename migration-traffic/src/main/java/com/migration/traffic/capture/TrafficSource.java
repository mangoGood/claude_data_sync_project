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

    /**
     * 捕获通道标识，写进 manifest（{@code GENERAL_LOG} / {@code PG_JSONLOG} /
     * {@code PG_CSVLOG} / {@code ORA_UNIFIED_AUDIT}）。不同通道的完整性与时间精度不一样，
     * 回放报告要能说清这份录制是怎么来的。
     */
    String captureBackend();

    /** 录制原点：源库时钟的 epoch 微秒。所有偏移都相对它算。 */
    long t0Micros();

    /**
     * 捕获侧的可续位点，无位点返回 null。
     *
     * <p>只有 PG 有真位点（日志文件名 + 字节偏移）。MySQL 的 general_log 读一次就没了，
     * Oracle 的审计记录会被清理——那两家停摆期间的语句是<b>真的丢了</b>，只能记空洞。
     */
    default com.migration.traffic.model.RecordingManifest.Checkpoint checkpoint() {
        return null;
    }

    /**
     * 从上次的位点恢复读取。不支持续读的实现忽略即可（默认实现就是忽略）。
     * 必须在 {@link #open} 之后、第一次 {@link #poll} 之前调用。
     */
    default void resumeFrom(com.migration.traffic.model.RecordingManifest.Checkpoint cp) {
    }

    /**
     * 捕获开始时刻已存在会话的 schema 快照（{@code 会话 id → 默认库/schema}）。
     *
     * <p>MySQL 必须有这一步：连接池里的连接在捕获开始前就建好了，它们的
     * {@code Connect}/{@code Init DB} 行永远不会再出现，不播种的话所有非限定表名语句
     * 在回放时都会撞 "No database selected"。PG 每行日志自带 {@code dbname}，不需要。
     */
    default java.util.Map<Long, String> snapshotSessionSchemas() {
        return java.util.Map.of();
    }

    /** 源库开关/审计策略是否<b>确认</b>已还原。false = 需要 agent 兜底。 */
    boolean isRestored();

    /**
     * 本次捕获改动了源端的哪些状态，供上层落盘做兜底还原。
     * 键值对的含义由各引擎自己定义（MySQL 是两个全局变量，PG 是四个 GUC，
     * Oracle 是策略名与启用范围）。
     */
    java.util.Map<String, String> restoreState();

    /**
     * 用外部给定的原值覆盖"要还原成什么"。
     *
     * <p>续录场景专用：上一轮若是被 {@code kill -9} 掉的，源端开关还是我们改过的样子，
     * 这一轮读到的"原值"其实是<b>我们自己留下的痕迹</b>。照它还原等于把源端永久留在
     * 开启状态——而且不报任何错。真正的原值在上一轮落盘的状态文件里。
     */
    default void overrideRestoreState(java.util.Map<String, String> original) {
    }

    /** 本次 poll 之后源端还积压多少（追不上的早期信号）；未知返回 -1。 */
    long backlog();

    /** 关闭并<b>还原源库状态</b>。实现必须幂等——它会被 shutdown hook 与正常路径各调一次。 */
    @Override
    void close();

    /** 源库上一条原始语句记录。 */
    final class RawStatement {
        /** 发生时刻，epoch 微秒（源库时钟，时区无关）。 */
        public long epochMicros;
        /** 会话 id（MySQL 的 thread_id / PG 的 session_id 折算 / Oracle 的 SESSIONID）。 */
        public long threadId;
        /** 原始 {@code command_type}：Query / Execute / Init DB / Connect / Quit / Prepare / ... */
        public String commandType;
        /** 语句原文（已按 UTF-8 从原始字节解码）。 */
        public String argument;
        /** 归一后的 {@code user@host}。 */
        public String userHost;

        /**
         * 绑定参数，按占位符序号排列；null 表示这条语句没有参数。
         * 元素为 null 表示 <b>SQL NULL</b>（与空串严格区分）。
         */
        public java.util.List<String> binds;
        /** 该语句所属的库（PG 的 {@code dbname}）；MySQL 靠 SessionSchemaTracker 推导，这里留 null。 */
        public String database;
        /** schema 上下文（PG 的 search_path / Oracle 的 CURRENT_SCHEMA）。 */
        public String schema;
        /** 源端已知的错误码（Oracle 的 RETURN_CODE / MySQL 富化的 errno）；0 = 成功、未知。 */
        public int errorCode;
        /** 源端已知的 SQLSTATE（PG 的 state_code）。 */
        public String sqlState;
        /** 源端耗时（微秒），未知为 -1。 */
        public long durationUs = -1L;
        /** 源端已经确定这条语句不可忠实回放（口令被抹 / 我们自己脱敏过）。 */
        public boolean redacted;
    }
}
