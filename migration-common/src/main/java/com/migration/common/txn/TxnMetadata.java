package com.migration.common.txn;

import java.util.Map;

/**
 * 源事务边界在 THL 事件元数据里的统一约定：extract 侧下发，increment / subscribe 侧消费。
 *
 * <p><b>为什么需要它</b>：源库一个含 N 个行事件的事务，此前会在目标库被拆成 N 个独立事务落地，
 * 目标端任意时刻都可能停在"半个事务"上（转账扣款已落、入账未落）。事务边界信息其实
 * 一路都在（MySQL 的 XID、PG 的 BEGIN/COMMIT、Oracle 的 XID 列、TiDB 的 commitTs），
 * 只是没往下传，到最后一米被丢掉。
 *
 * <h3>两类源的边界表达方式</h3>
 * <ul>
 *   <li><b>显式结束标记</b>（MySQL / PostgreSQL）：事务的最后一个事件带 {@link #TX_LAST}，
 *       增量端看到即提交。</li>
 *   <li><b>按标识变化推断</b>（Oracle LogMiner 的 XID、TiDB 的 commitTs）：没有独立的提交事件，
 *       只能靠"下一个事件的 {@link #TX_ID} 变了"判定上一个事务结束。因此增量端必须<b>同时</b>
 *       支持这两种判定，并对"最后一个事务后面再没有事件"的情况用空闲超时兜底提交。</li>
 * </ul>
 *
 * <p>老 THL 文件不带这些 key，增量端自动退回逐事件提交（EVENT 语义），存量任务不受影响。
 */
public final class TxnMetadata {

    /**
     * 源事务标识。同一源事务的所有事件取值相同，且在任务生命周期内唯一：
     * MySQL 取 BEGIN 的 {@code binlog文件:位点}，PG 取 {@code pg:xid}，
     * Oracle 取 {@code ora:XID}，TiDB 取 {@code tso:commitTs}。
     */
    public static final String TX_ID = "tx_id";

    /** 事务的最后一个事件（MySQL 的 XID / PG 的 COMMIT）。仅显式结束标记的源会下发。 */
    public static final String TX_LAST = "tx_last";

    /** 源库自己的事务号（MySQL XID / PG xid / Oracle XID），供订阅下游与源库对账；可能缺失。 */
    public static final String TX_SOURCE_ID = "tx_source_id";

    private TxnMetadata() {
    }

    public static String txIdOf(Map<String, Object> metadata) {
        if (metadata == null) {
            return null;
        }
        Object v = metadata.get(TX_ID);
        if (v == null) {
            return null;
        }
        String s = v.toString();
        return s.isEmpty() ? null : s;
    }

    public static boolean isTxLast(Map<String, Object> metadata) {
        if (metadata == null) {
            return false;
        }
        Object v = metadata.get(TX_LAST);
        return (v instanceof Boolean) ? (Boolean) v : Boolean.parseBoolean(String.valueOf(v));
    }
}
