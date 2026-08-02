package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 多行事件按行拆分：<b>逐行元数据必须逐行切</b>。
 *
 * <p>拆分时若整份复制 {@code rows_typed}，拆出来的 N 个事件每个都带着全部 N 行，
 * 而增量端的类型化值管道读的正是它——N 行的源事件会产生 N 个 THL 事件 × 每个 N 条 SQL
 * = <b>N² 次目标写入</b>。实测 500 行的一批变更打出 62104 次写入（放大 124 倍），
 * 也是增量吞吐长期只有二三十行每秒的真正原因。
 */
@DisplayName("多行事件拆分：逐行元数据按行切片")
class MultiRowSplitTest {

    private ArrayList<Object> row(Object... values) {
        return new ArrayList<>(Arrays.asList(values));
    }

    private THLEvent multiRowInsert() {
        THLEvent e = new THLEvent();
        e.setSeqno(100);
        e.setEventId("binlog.000001:4");
        e.setSourceId("mysql");
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("operation", "INSERT");
        e.addMetadata("table_name", "t");
        e.addMetadata("database_name", "db");
        e.addMetadata("column_names", "id,name");
        e.addMetadata("multi_row", true);
        e.addMetadata("rows_data", new ArrayList<>(Arrays.asList("(1,'a')", "(2,'b')", "(3,'c')")));
        e.addMetadata("row_data", "(1,'a')");
        ArrayList<ArrayList<Object>> typed = new ArrayList<>();
        typed.add(row(1, "a"));
        typed.add(row(2, "b"));
        typed.add(row(3, "c"));
        e.addMetadata("rows_typed", typed);
        return e;
    }

    @Test
    @DisplayName("3 行 INSERT 拆成 3 条，每条只带自己那一行的 rows_typed")
    void insertSlicesTypedRowsPerEvent() {
        List<THLEvent> split = ContinuousExtractMain.splitMultiRowEvent(multiRowInsert());

        assertNotNull(split);
        assertEquals(3, split.size());
        for (int i = 0; i < 3; i++) {
            THLEvent ev = split.get(i);
            assertEquals(100 + i, ev.getSeqno());
            assertEquals("binlog.000001:4_" + i, ev.getEventId());

            @SuppressWarnings("unchecked")
            List<Object> typed = (List<Object>) ev.getMetadata().get("rows_typed");
            assertEquals(1, typed.size(),
                    "拆出的每个事件只应带一行类型化值，带 N 行会让下游生成 N 条 SQL（N² 写放大）");
            assertEquals(row(i + 1, String.valueOf((char) ('a' + i))), typed.get(0));

            assertNull(ev.getMetadata().get("multi_row"), "拆完不再是多行事件");
            assertNull(ev.getMetadata().get("rows_data"), "行集合不应整份带下去");
            assertEquals("db", ev.getMetadata().get("database_name"), "非逐行元数据照常复制");
        }
    }

    @Test
    @DisplayName("多行 UPDATE 的前镜像也按行切，不是所有行都拿第 0 行")
    void updateSlicesBeforeImagePerRow() {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.setEventId("binlog.000001:8");
        e.addMetadata("event_type", "UPDATE");
        e.addMetadata("table_name", "t");
        e.addMetadata("multi_row", true);
        e.addMetadata("rows_data", new ArrayList<>(Arrays.asList("(1,'new1')", "(2,'new2')")));
        e.addMetadata("row_data", "(1,'new1')");
        e.addMetadata("rows_data_before", new ArrayList<>(Arrays.asList("(1,'old1')", "(2,'old2')")));
        e.addMetadata("row_data_before", "(1,'old1')");
        ArrayList<ArrayList<Object>> after = new ArrayList<>();
        after.add(row(1, "new1"));
        after.add(row(2, "new2"));
        ArrayList<ArrayList<Object>> before = new ArrayList<>();
        before.add(row(1, "old1"));
        before.add(row(2, "old2"));
        e.addMetadata("rows_typed", after);
        e.addMetadata("rows_before_typed", before);

        List<THLEvent> split = ContinuousExtractMain.splitMultiRowEvent(e);

        assertNotNull(split);
        assertEquals(2, split.size());
        assertEquals("(1,'old1')", split.get(0).getMetadata().get("row_data_before"));
        assertEquals("(2,'old2')", split.get(1).getMetadata().get("row_data_before"),
                "第 2 行的前镜像必须是它自己的，整份复制会让每条都指向第 0 行");

        @SuppressWarnings("unchecked")
        List<Object> beforeTyped1 = (List<Object>) split.get(1).getMetadata().get("rows_before_typed");
        assertEquals(1, beforeTyped1.size());
        assertEquals(row(2, "old2"), beforeTyped1.get(0));
    }

    @Test
    @DisplayName("行集合行数对不上时整体不下发，让下游回退文本路径")
    void mismatchedRowCountDropsTypedMetadata() {
        THLEvent e = multiRowInsert();
        ArrayList<ArrayList<Object>> shortTyped = new ArrayList<>();
        shortTyped.add(row(1, "a"));   // 只有 1 行，rows_data 有 3 行
        e.addMetadata("rows_typed", shortTyped);

        List<THLEvent> split = ContinuousExtractMain.splitMultiRowEvent(e);

        assertNotNull(split);
        assertEquals(3, split.size());
        for (THLEvent ev : split) {
            assertNull(ev.getMetadata().get("rows_typed"),
                    "对不齐宁可回退文本路径，也不能把第 0 行的值安到第 2 行上");
        }
    }

    @Test
    @DisplayName("单行事件不拆")
    void singleRowEventNotSplit() {
        THLEvent e = new THLEvent();
        e.addMetadata("rows_data", new ArrayList<>(Arrays.asList("(1,'a')")));
        assertNull(ContinuousExtractMain.splitMultiRowEvent(e), "没有 multi_row 标记就不该拆");

        e.addMetadata("multi_row", true);
        assertNull(ContinuousExtractMain.splitMultiRowEvent(e), "只有 1 行也不该拆");
    }

    @Test
    @DisplayName("非逐行元数据按引用复制，不额外拷贝")
    void sharedMetadataIsCopiedByReference() {
        THLEvent e = multiRowInsert();
        Object columnNames = e.getMetadata().get("column_names");
        List<THLEvent> split = ContinuousExtractMain.splitMultiRowEvent(e);
        assertSame(columnNames, split.get(0).getMetadata().get("column_names"));
    }
}
