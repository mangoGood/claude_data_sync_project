package com.migration.common.bidi;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BidiLoopGuard} 前向单遍状态机测试：模拟 capture 读到的 binlog 事件序列，
 * 验证"带 origin 标记的复制事务被跳过、本地写入正常传播"，即 active-active 防回环的核心判定。
 */
@DisplayName("双向同步环路防护状态机")
class BidiLoopGuardTest {

    @Test
    @DisplayName("复制事务：BEGIN→标记→DML→XID，DML 被跳过（防回环）")
    void replicatedTransactionSkipsData() {
        BidiLoopGuard g = new BidiLoopGuard(true);

        // 对端 apply 写入的事务：先标记后业务 DML
        g.onTransactionBoundary();          // BEGIN
        assertFalse(g.shouldSkipReplicatedData(), "事务刚开始还没标记");
        g.onOriginMarker();                  // __sync_origin 行事件（标记，先于业务 DML）
        assertTrue(g.shouldSkipReplicatedData(), "见到标记后本事务的数据事件应跳过");
        // 该事务后续任意条业务 DML 都应跳过
        assertTrue(g.shouldSkipReplicatedData());
        assertTrue(g.shouldSkipReplicatedData());
        g.onTransactionBoundary();          // XID(COMMIT)
        assertFalse(g.shouldSkipReplicatedData(), "提交后标记状态复位");
    }

    @Test
    @DisplayName("本地写入：BEGIN→DML→XID，无标记，正常传播（不跳过）")
    void localWriteIsPropagated() {
        BidiLoopGuard g = new BidiLoopGuard(true);
        g.onTransactionBoundary();          // BEGIN
        // 本地应用写入没有标记
        assertFalse(g.shouldSkipReplicatedData(), "本地写入应正常传播");
        g.onTransactionBoundary();          // XID
        assertFalse(g.shouldSkipReplicatedData());
    }

    @Test
    @DisplayName("交替事务：复制→本地→复制，各自独立判定，标记不串味")
    void alternatingTransactionsAreIndependent() {
        BidiLoopGuard g = new BidiLoopGuard(true);

        // 复制事务
        g.onTransactionBoundary();
        g.onOriginMarker();
        assertTrue(g.shouldSkipReplicatedData());
        g.onTransactionBoundary();

        // 紧接一个本地事务：不应受上一个事务标记影响
        g.onTransactionBoundary();
        assertFalse(g.shouldSkipReplicatedData(), "上一个复制事务的标记不应残留到本地事务");
        g.onTransactionBoundary();

        // 再来一个复制事务
        g.onTransactionBoundary();
        g.onOriginMarker();
        assertTrue(g.shouldSkipReplicatedData());
    }

    @Test
    @DisplayName("未启用：即便读到标记也不跳过（单向同步零影响）")
    void disabledNeverSkips() {
        BidiLoopGuard g = new BidiLoopGuard(false);
        assertFalse(g.isEnabled());
        g.onTransactionBoundary();
        g.onOriginMarker();
        assertFalse(g.shouldSkipReplicatedData(), "未启用双向时永不跳过，保持单向行为");
    }
}
