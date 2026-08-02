package com.migration.increment;

import com.migration.common.txn.TxnMetadata;
import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并行应用的分片规则。TRANSACTION 模式要同时守住两条性质，缺一条就出错：
 *
 * <ul>
 *   <li><b>同一事务只落在一个 worker</b>——否则一个源事务被拆到多个连接上，原子性无从谈起；</li>
 *   <li><b>同一张表的事件严格有序</b>——只按 tx_id 分片会破坏它：tx1 插入 X、tx2 紧接着更新 X，
 *       两个事务落到不同 worker 就可能乱序执行，UPDATE 先跑会"未影响任何行"被吞掉，
 *       最终目标端留下的是 INSERT 的旧值。</li>
 * </ul>
 *
 * <p>做法是并查集：把共享任一张表的事务并到同一分片。代价是并发度退化为"互不相交的表集合数"。
 */
@DisplayName("并行应用分片：同事务同片 + 同表有序")
class TransactionShardingTest {

    private ContinuousIncrementMain main;

    @BeforeEach
    void setUp() throws Exception {
        main = new ContinuousIncrementMain();
    }

    private void setTxMode(boolean on) throws Exception {
        Field f = ContinuousIncrementMain.class.getDeclaredField("txApplyMode");
        f.setAccessible(true);
        f.set(main, on);
    }

    private ContinuousIncrementMain.WorkItem item(long seqno, String txId, String table) {
        THLEvent e = new THLEvent();
        e.setSeqno(seqno);
        e.setEventId("e" + seqno);
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("operation", "INSERT");
        e.addMetadata("table_name", table);
        if (txId != null) {
            e.addMetadata(TxnMetadata.TX_ID, txId);
        }
        return new ContinuousIncrementMain.WorkItem(e, null, new ArrayList<>());
    }

    /** 事件 seqno → 它被分到的 worker 下标。 */
    private int shardOf(List<List<ContinuousIncrementMain.TxGroup>> shards, long seqno) {
        for (int i = 0; i < shards.size(); i++) {
            for (ContinuousIncrementMain.TxGroup g : shards.get(i)) {
                for (ContinuousIncrementMain.WorkItem wi : g.items) {
                    if (wi.event.getSeqno() == seqno) return i;
                }
            }
        }
        return -1;
    }

    @Test
    @DisplayName("TRANSACTION：同一事务的多张表事件进同一个 worker")
    void oneTransactionStaysOnOneWorker() throws Exception {
        setTxMode(true);
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(item(1, "tx-A", "accounts"));
        batch.add(item(2, "tx-A", "ledger"));
        batch.add(item(3, "tx-A", "audit"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        assertEquals(shardOf(shards, 1), shardOf(shards, 2));
        assertEquals(shardOf(shards, 1), shardOf(shards, 3));

        long groups = shards.stream().flatMap(List::stream).count();
        assertEquals(1, groups, "同一事务应聚成一个执行组，整组一个目标事务");
    }

    @Test
    @DisplayName("TRANSACTION：写同一张表的两个事务必须同片，否则同表乱序")
    void transactionsSharingATableStayOnOneWorker() throws Exception {
        setTxMode(true);
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(item(1, "tx-A", "orders"));   // INSERT X
        batch.add(item(2, "tx-B", "orders"));   // UPDATE X —— 必须在 tx-A 之后执行

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        assertEquals(shardOf(shards, 1), shardOf(shards, 2),
                "同表的两个事务分到不同 worker 会乱序：UPDATE 先跑被当成'未影响任何行'吞掉");
        assertEquals(2, shards.stream().flatMap(List::stream).count(), "仍是两个独立事务组，各自提交");
    }

    @Test
    @DisplayName("TRANSACTION：经由第三个事务间接相连的表也要同片")
    void transitivelyLinkedTablesStayTogether() throws Exception {
        setTxMode(true);
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(item(1, "tx-A", "t1"));
        batch.add(item(2, "tx-B", "t1"));
        batch.add(item(3, "tx-B", "t2"));   // tx-B 把 t1 和 t2 连起来
        batch.add(item(4, "tx-C", "t2"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        int s1 = shardOf(shards, 1);
        assertEquals(s1, shardOf(shards, 2));
        assertEquals(s1, shardOf(shards, 3));
        assertEquals(s1, shardOf(shards, 4), "t1—tx-B—t2 传递相连，四条都得在同一片上保序");
    }

    @Test
    @DisplayName("TRANSACTION：互不相交的表集合仍然并发")
    void disjointTableSetsStillParallelize() throws Exception {
        setTxMode(true);
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            batch.add(item(i + 1, "tx-" + i, "table_" + i));
        }

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        long used = shards.stream().filter(s -> !s.isEmpty()).count();
        assertTrue(used > 1, "16 张互不相干的表应该散布到多个 worker，实际只用了 " + used + " 个");
    }

    @Test
    @DisplayName("EVENT（默认）：每事件一组、按表分片，行为与改动前一致")
    void eventModeKeepsPerTableSharding() throws Exception {
        setTxMode(false);
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(item(1, "tx-A", "orders"));
        batch.add(item(2, "tx-A", "items"));
        batch.add(item(3, "tx-B", "orders"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        assertEquals(3, shards.stream().flatMap(List::stream).count(), "EVENT 模式下每个事件自成一组");
        for (List<ContinuousIncrementMain.TxGroup> shard : shards) {
            for (ContinuousIncrementMain.TxGroup g : shard) {
                assertEquals(1, g.items.size());
            }
        }
        assertEquals(shardOf(shards, 1), shardOf(shards, 3), "同表仍按表 hash 落同一片");
    }
}
