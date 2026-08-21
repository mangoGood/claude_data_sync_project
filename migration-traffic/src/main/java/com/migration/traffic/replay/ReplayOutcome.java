package com.migration.traffic.replay;

/** 一条录制记录在回放里的归宿。 */
public enum ReplayOutcome {
    /** 目标库执行成功。 */
    OK,
    /** 源库当时就报错，目标库也报错 —— 符合预期，不算问题。 */
    EXPECTED_ERROR,
    /** 源库成功、目标库失败 —— 这是回放要找的东西。 */
    REPLAY_ERROR,
    /** 口令被数据库抹掉（MySQL 的 {@code <secret>} / Oracle 的 {@code *}）或被捕获侧脱敏，无法忠实回放。 */
    UNREPLAYABLE_REDACTED,
    /**
     * 语句里有绑定占位符，但录制里没有对应的值，物理上无法回放。
     *
     * <p>典型来源是 sqlplus 的 {@code EXEC :v := …}——它在审计里记成
     * {@code BEGIN :v := '…'; END;}，占位符是<b>赋值目标</b>而不是输入参数，
     * 审计的 SQL_BINDS 里当然没有它的值。照原样执行只会得到 ORA-01008，
     * 而真正带着值的是紧随其后的那条 DML。单独记一类，免得它把回放错误率顶穿。
     */
    UNREPLAYABLE_NO_BINDS,
    /** 命中危险语句黑名单。 */
    BLOCKED,
    /** 被类别筛选排除。 */
    FILTERED,
    /** 超过时间轴容忍度被丢弃（SKIP 档）。 */
    SKIPPED_LATE,
    /** 会话数到顶且无可淘汰会话。 */
    SESSION_EXHAUSTED,
    /** 行数与源端不一致（compare=ROWCOUNT）。 */
    ROWCOUNT_MISMATCH
}
