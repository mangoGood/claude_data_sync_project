package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 最终一致（EVENT）模式下的冲突矩阵调度：
 *
 * <ul>
 *   <li>不同表的 DML 必然可以并发；</li>
 *   <li>同表但主键不同的 DML 也能并发；</li>
 *   <li>同表同主键的 INSERT/UPDATE/DELETE 必须落到同一个 worker 上按序执行——
 *       颠倒后 UPDATE 会"未影响任何行"被吞掉，DELETE 先跑则 INSERT 的行会残留。</li>
 * </ul>
 *
 * <p>另外两条是安全兜底：拿不到主键的事件（无主键表 / 文本路径）必须把整张表降级成表级键，
 * 否则"行级键"与"表级键"在同一张表上并存，两者永远算不到一起，冲突就漏了。
 */
@DisplayName("最终一致：表+主键 冲突矩阵分片")
class ConflictMatrixShardingTest {

    private ContinuousIncrementMain main;

    @BeforeEach
    void setUp() throws Exception {
        main = new ContinuousIncrementMain();
        set("txApplyMode", false);
        set("rowLevelConflict", true);
    }

    private void set(String field, Object value) throws Exception {
        Field f = ContinuousIncrementMain.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(main, value);
    }

    /** 类型化路径的事件：一行 DML，带 库.表 与主键值。 */
    private ContinuousIncrementMain.WorkItem row(long seqno, String db, String table, String pk, String op) {
        THLEvent e = new THLEvent();
        e.setSeqno(seqno);
        e.setEventId("e" + seqno);
        e.addMetadata("event_type", op);
        e.addMetadata("operation", op);
        e.addMetadata("database_name", db);
        e.addMetadata("table_name", table);
        List<ParameterizedDml> dmls = new ArrayList<>();
        dmls.add(new ParameterizedDml("/* " + op + " */ ?", new ArrayList<>(), db + "." + table, pk, op));
        return new ContinuousIncrementMain.WorkItem(e, dmls, null);
    }

    /** 无主键（rowKey=null）的类型化事件：整张表得降级成表级键。 */
    private ContinuousIncrementMain.WorkItem rowWithoutPk(long seqno, String db, String table) {
        ContinuousIncrementMain.WorkItem wi = row(seqno, db, table, null, "INSERT");
        return new ContinuousIncrementMain.WorkItem(wi.event,
                Collections.singletonList(new ParameterizedDml("/* no pk */", new ArrayList<>(),
                        db + "." + table, null, "INSERT")), null);
    }

    /** 文本路径事件（没有结构化主键）。 */
    private ContinuousIncrementMain.WorkItem textRow(long seqno, String db, String table) {
        THLEvent e = new THLEvent();
        e.setSeqno(seqno);
        e.setEventId("e" + seqno);
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("operation", "INSERT");
        e.addMetadata("database_name", db);
        e.addMetadata("table_name", table);
        return new ContinuousIncrementMain.WorkItem(e, null, Collections.singletonList("INSERT INTO t VALUES (1)"));
    }

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
    @DisplayName("同表同主键的 INSERT/UPDATE/DELETE 必须同片保序")
    void sameTableSameKeyIsSerialized() {
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(row(1, "shop", "orders", "1001", "INSERT"));
        batch.add(row(2, "shop", "orders", "1001", "UPDATE"));
        batch.add(row(3, "shop", "orders", "1001", "DELETE"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 8);

        int s = shardOf(shards, 1);
        assertEquals(s, shardOf(shards, 2), "同一行的 UPDATE 与 INSERT 必须同片，否则 UPDATE 可能先跑被吞掉");
        assertEquals(s, shardOf(shards, 3), "同一行的 DELETE 也必须排在后面同片执行");
        // 同片内保持读入顺序
        List<ContinuousIncrementMain.TxGroup> shard = shards.get(s);
        assertEquals(1, shard.get(0).firstSeqno);
        assertEquals(2, shard.get(1).firstSeqno);
        assertEquals(3, shard.get(2).firstSeqno);
    }

    @Test
    @DisplayName("同表不同主键可以并发")
    void sameTableDifferentKeysParallelize() {
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            batch.add(row(i + 1, "shop", "orders", "pk-" + i, "INSERT"));
        }

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        long used = shards.stream().filter(s -> !s.isEmpty()).count();
        assertTrue(used > 1, "同一张表的 32 个不同主键应散布到多个 worker，实际只用了 " + used + " 个");
    }

    @Test
    @DisplayName("不同表必然可以并发（同名表在不同库也不算冲突）")
    void differentTablesParallelize() {
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            batch.add(row(i + 1, "db" + (i % 2), "t" + i, "1", "INSERT"));
        }

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 4);

        long used = shards.stream().filter(s -> !s.isEmpty()).count();
        assertTrue(used > 1, "16 张互不相干的表应并发，实际只用了 " + used + " 个 worker");
    }

    @Test
    @DisplayName("表级键与行级键不会在同一张表上并存：拿不到主键即整表降级")
    void tableWithoutPkDegradesWholeTable() {
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(row(1, "shop", "orders", "1001", "INSERT"));
        batch.add(rowWithoutPk(2, "shop", "orders"));      // 无主键 → 整张表降级
        batch.add(row(3, "shop", "orders", "2002", "INSERT"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 8);

        int s = shardOf(shards, 1);
        assertEquals(s, shardOf(shards, 2), "降级后同表事件必须回到同一个 worker 上保序");
        assertEquals(s, shardOf(shards, 3), "同表的其它主键也要跟着降级，不能继续按行分散");
    }

    @Test
    @DisplayName("文本路径（无结构化主键）同样整表降级，但不影响别的表")
    void textPathDegradesOnlyItsOwnTable() {
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(textRow(1, "shop", "orders"));
        batch.add(row(2, "shop", "orders", "1001", "UPDATE"));
        batch.add(row(3, "shop", "items", "9", "INSERT"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 8);

        assertEquals(shardOf(shards, 1), shardOf(shards, 2), "orders 表已降级，两条都得同片");
        assertNotEquals(shardOf(shards, 1), shardOf(shards, 3), "items 表不受 orders 降级影响，仍可并发");
    }

    @Test
    @DisplayName("granularity=TABLE 时退回按表分片（同表不同主键也串行）")
    void tableGranularityKeepsWholeTableOnOneWorker() throws Exception {
        set("rowLevelConflict", false);
        List<ContinuousIncrementMain.WorkItem> batch = new ArrayList<>();
        batch.add(row(1, "shop", "orders", "1001", "INSERT"));
        batch.add(row(2, "shop", "orders", "2002", "INSERT"));

        List<List<ContinuousIncrementMain.TxGroup>> shards = main.shardBatch(batch, 8);

        assertEquals(shardOf(shards, 1), shardOf(shards, 2), "表级粒度下整张表落在同一个 worker");
    }
}
