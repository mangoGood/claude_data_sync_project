package com.migration.common.bidi;

/**
 * 环路防护的前向单遍状态机（capture 端使用）。
 *
 * <p>约定 apply 端把 origin 标记写在事务首条语句，故 binlog 事件顺序为：
 * {@code BEGIN → 标记行事件 → 业务 DML 行事件… → COMMIT(XID)}。本状态机据此判定：
 * <ul>
 *   <li>{@link #onTransactionBoundary()}：BEGIN 或 XID —— 清除标记状态，开启/结束一个事务；</li>
 *   <li>{@link #onOriginMarker()}：读到标记表行事件 —— 置位，本事务判定为"复制而来"；</li>
 *   <li>{@link #shouldSkipReplicatedData()}：业务数据事件是否应跳过（启用且当前事务带标记）。</li>
 * </ul>
 *
 * <p>无需缓冲整个事务：因标记必先于业务 DML 出现，读到 DML 时标记状态已就绪。
 */
public class BidiLoopGuard {

    private final boolean enabled;
    private boolean currentTxnHasMarker = false;

    public BidiLoopGuard(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** 事务边界（BEGIN 或 COMMIT/XID）：复位标记状态。 */
    public void onTransactionBoundary() {
        currentTxnHasMarker = false;
    }

    /** 读到 origin 标记行事件：当前事务判定为复制而来。 */
    public void onOriginMarker() {
        currentTxnHasMarker = true;
    }

    /** 当前是否处于"带标记"事务（供观测/测试）。 */
    public boolean currentTxnMarked() {
        return currentTxnHasMarker;
    }

    /** 业务数据事件是否应跳过：启用且当前事务带 origin 标记（复制而来，跳过防回环）。 */
    public boolean shouldSkipReplicatedData() {
        return enabled && currentTxnHasMarker;
    }
}
