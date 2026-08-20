package com.migration.traffic.replay;

/** 一条录制记录在回放里的归宿。 */
public enum ReplayOutcome {
    /** 目标库执行成功。 */
    OK,
    /** 源库当时就报错，目标库也报错 —— 符合预期，不算问题。 */
    EXPECTED_ERROR,
    /** 源库成功、目标库失败 —— 这是回放要找的东西。 */
    REPLAY_ERROR,
    /** MySQL 抹掉了口令，物理上无法忠实回放。 */
    UNREPLAYABLE_REDACTED,
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
