package com.migration.common.snapshot;

import com.migration.config.DatabaseConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 全量一致性快照（P2-3）的降级与开关语义。
 *
 * <p>这里守的核心是一条：<b>快照是增强项，不能让全量起不来</b>。源库缺 RELOAD 权限、
 * 连不上、版本不支持 —— 任何一种都必须安静地退回"无快照全量"（旧行为，数据仍最终一致），
 * 而不是把整个任务打失败。真快照的行为需要真实库，由 test_scripts 的判据脚本覆盖。
 */
@DisplayName("全量一致性快照")
class ConsistentSnapshotTest {

    private DatabaseConfig unreachable() {
        // 指向一个不会有人监听的端口：模拟"源库连不上/没权限"
        return new DatabaseConfig("127.0.0.1", 1, "nodb", "u", "p", "mysql");
    }

    @Test
    @DisplayName("NONE：不连库、不记位点，读取路径与旧行为完全一致")
    void noneModeIsInert() {
        try (ConsistentSnapshot s = ConsistentSnapshot.begin(unreachable(), "NONE", 4, null)) {
            assertEquals(ConsistentSnapshot.Mode.NONE, s.getMode());
            assertFalse(s.providesReaders());
            assertNull(s.getPosition());
            assertEquals("`t`", s.decorateTable("`t`"), "非 Oracle 不改写表引用");
        }
    }

    @Test
    @DisplayName("建立失败必须降级为 NONE，而不是让全量起不来")
    void degradesWhenSnapshotCannotBeEstablished() {
        try (ConsistentSnapshot s = ConsistentSnapshot.begin(unreachable(), "CONSISTENT", 4, null)) {
            assertEquals(ConsistentSnapshot.Mode.NONE, s.getMode());
            assertFalse(s.providesReaders(), "降级后不得再要求走快照读连接");
        }
    }

    @Test
    @DisplayName("未知模式按默认 GTID_ONLY 处理，不抛异常")
    void unknownModeFallsBackToDefault() {
        try (ConsistentSnapshot s = ConsistentSnapshot.begin(unreachable(), "MAGIC", 4, null)) {
            // 连不上库，最终仍降级为 NONE；关键是解析未知模式不能抛
            assertEquals(ConsistentSnapshot.Mode.NONE, s.getMode());
        }
    }

    @Test
    @DisplayName("GTID_ONLY 不借读连接：分页仍每页新建源连接（Oracle PGA 释放语义不变）")
    void gtidOnlyDoesNotPinReaders() {
        try (ConsistentSnapshot s = ConsistentSnapshot.begin(unreachable(), "GTID_ONLY", 4, null)) {
            assertFalse(s.providesReaders());
        }
    }

    @Test
    @DisplayName("TiDB 源同样降级而不是报错（快照手法不同，兜底哲学一致）")
    void tidbAlsoDegrades() {
        DatabaseConfig tidb = unreachable();
        tidb.setFlavor("tidb");
        try (ConsistentSnapshot s = ConsistentSnapshot.begin(tidb, "CONSISTENT", 4, null)) {
            assertEquals(ConsistentSnapshot.Mode.NONE, s.getMode());
            assertFalse(s.providesReaders(), "TiDB 是逐查询历史读，任何情况下都不借固定读会话");
            assertEquals("`t`", s.decorateTable("`t`"), "降级后不得再往 SQL 里塞 AS OF TIMESTAMP");
        }
    }

    @Test
    @DisplayName("解析 TiDB 的 GC 生命期（Go 风格时长）：ms 不能被当成 m")
    void parsesTidbGcLifeTime() {
        assertEquals(600, ConsistentSnapshot.parseGoDuration("10m0s"));
        assertEquals(86400, ConsistentSnapshot.parseGoDuration("24h"));
        assertEquals(5400, ConsistentSnapshot.parseGoDuration("1h30m"));
        // "500ms" 若按 "500m" 解析会得到 30000 秒，于是本该降级的短 GC 被判成"够长"，
        // 全量跑到一半才因历史版本被回收而失败
        assertEquals(0, ConsistentSnapshot.parseGoDuration("500ms"));
        assertEquals(-1, ConsistentSnapshot.parseGoDuration("unknown"));
        assertEquals(-1, ConsistentSnapshot.parseGoDuration(null));
    }
}
