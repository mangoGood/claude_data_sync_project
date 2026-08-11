package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双向同步的自增错开判定。
 *
 * <p>active-active 下两端各自分配自增 ID：A 端插到 id=5、B 端也插到 id=5，两条<b>内容不同</b>的
 * 行复制到对端时撞主键，而应用侧对主键冲突的处理是 upsert 覆盖 / 当成幂等重放忽略——
 * 一侧的数据静默消失，且不会触发任何冲突告警（冲突裁决管的是 UPDATE 的写写冲突）。
 *
 * <p>唯一可靠的解法是让两端各用一个同余类。这里锁死判定条件，尤其是那条不显然的：
 * MySQL 对 {@code auto_increment_offset > auto_increment_increment} 的配置是<b>直接忽略
 * offset</b> 的——写成 increment=3/offset=5 看着错开，实际两端都从同一个序列取值。
 */
@DisplayName("双向同步：两端自增是否真的错开")
class BidiAutoIncrementStaggerTest {

    @Test
    @DisplayName("标准双主配置（2/1 与 2/2）算错开")
    void classicDualMasterIsStaggered() {
        assertTrue(DiagnosticService.autoIncrementStaggered(2, 1, 2, 2));
        assertTrue(DiagnosticService.autoIncrementStaggered(2, 2, 2, 1));
        assertTrue(DiagnosticService.autoIncrementStaggered(4, 1, 4, 3));
    }

    @Test
    @DisplayName("默认配置（1/1）不算错开——两端会分配到同一批 ID")
    void defaultConfigIsNotStaggered() {
        assertFalse(DiagnosticService.autoIncrementStaggered(1, 1, 1, 1));
        assertFalse(DiagnosticService.autoIncrementStaggered(1, 1, 2, 2));
        assertFalse(DiagnosticService.autoIncrementStaggered(2, 1, 1, 1));
    }

    @Test
    @DisplayName("偏移相同不算错开")
    void sameOffsetIsNotStaggered() {
        assertFalse(DiagnosticService.autoIncrementStaggered(2, 1, 2, 1));
    }

    @Test
    @DisplayName("偏移超出步长不算错开：MySQL 会直接忽略这个 offset")
    void offsetBeyondIncrementIsNotStaggered() {
        assertFalse(DiagnosticService.autoIncrementStaggered(3, 5, 3, 1));
        assertFalse(DiagnosticService.autoIncrementStaggered(2, 1, 2, 9));
        assertFalse(DiagnosticService.autoIncrementStaggered(2, 0, 2, 1));
    }

    @Test
    @DisplayName("两端步长不一致不算错开：同余类照样相交")
    void differentIncrementsAreNotStaggered() {
        // 2/1 产出 1,3,5,7…；3/1 产出 1,4,7… —— 7 是两边都会分配到的
        assertFalse(DiagnosticService.autoIncrementStaggered(2, 1, 3, 1));
        assertFalse(DiagnosticService.autoIncrementStaggered(2, 1, 3, 2));
    }
}
