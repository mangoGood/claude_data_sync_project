package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypedDmlConverter} 拆分（route.mode=SPLIT）单元测试。
 *
 * <p>重点是<b>分片键被 UPDATE 改掉时的跨分片搬迁</b>：只发一条 UPDATE 会让旧分片留着陈行、
 * 新分片一行没有，两边都不报错。
 */
@DisplayName("TypedDmlConverter 拆分路由")
class TypedDmlConverterSplitTest {

    /** user_id % 4 分 4 片：dw_0/dw_1 两个库，各 2 张表 */
    private TypedDmlConverter converter() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("target.db.database", "app");
        props.setProperty("route.mode", "SPLIT");
        props.setProperty("route.split.1.match", "app.orders");
        props.setProperty("route.split.1.shard.key", "user_id");
        props.setProperty("route.split.1.algo", "HASH_MOD");
        props.setProperty("route.split.1.count", "4");
        props.setProperty("route.split.1.target.db", "dw_${shard/2}");
        props.setProperty("route.split.1.target.table", "orders_${shard}");
        props.setProperty("route.split.1.unrouted", "DEADLETTER");
        return new TypedDmlConverter(props);
    }

    private THLEvent event(String type, String table) {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", type);
        e.addMetadata("database_name", "app");
        e.addMetadata("table_name", table);
        e.addMetadata("column_names", "id,user_id,amount");
        e.addMetadata("primary_keys", "id");
        return e;
    }

    private static ArrayList<ArrayList<Object>> rows(Object... values) {
        ArrayList<ArrayList<Object>> rows = new ArrayList<>();
        // 用 Arrays.asList 而非 List.of：分片键为 NULL 的用例要传 null
        rows.add(new ArrayList<>(java.util.Arrays.asList(values)));
        return rows;
    }

    @Test
    @DisplayName("INSERT：按分片键路由到对应分片表（含分库）")
    void insertRoutedByShardKey() {
        THLEvent e = event("INSERT", "orders");
        e.addMetadata("rows_typed", rows(1L, 5L, 10));   // 5 % 4 = 1 → dw_0.orders_1

        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        assertTrue(dmls.get(0).getSql().startsWith("INSERT INTO `dw_0`.`orders_1` "), dmls.get(0).getSql());

        THLEvent e2 = event("INSERT", "orders");
        e2.addMetadata("rows_typed", rows(2L, 7L, 20));  // 7 % 4 = 3 → dw_1.orders_3
        assertTrue(converter().convert(e2).get(0).getSql().startsWith("INSERT INTO `dw_1`.`orders_3` "));
    }

    @Test
    @DisplayName("DELETE：按前镜像的分片键定位（DELETE 只有前镜像）")
    void deleteRoutedByBeforeImage() {
        THLEvent e = event("DELETE", "orders");
        e.addMetadata("rows_typed", rows(1L, 6L, 10));   // 6 % 4 = 2 → dw_1.orders_2

        ParameterizedDml dml = converter().convert(e).get(0);
        assertTrue(dml.getSql().startsWith("DELETE FROM `dw_1`.`orders_2` WHERE `id`=?"), dml.getSql());
        assertEquals(List.of(1L), dml.getParams());
    }

    @Test
    @DisplayName("UPDATE 未改分片键：就地更新那一片")
    void updateWithinSameShard() {
        THLEvent e = event("UPDATE", "orders");
        e.addMetadata("update_column_names", "id,user_id,amount");
        e.addMetadata("update_before_column_names", "id,user_id,amount");
        e.addMetadata("rows_typed", rows(1L, 5L, 99));
        e.addMetadata("rows_before_typed", rows(1L, 5L, 10));

        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        assertEquals("UPDATE", dmls.get(0).getOpType());
        assertTrue(dmls.get(0).getSql().startsWith("UPDATE `dw_0`.`orders_1` SET"), dmls.get(0).getSql());
    }

    @Test
    @DisplayName("UPDATE 改了分片键：旧分片 DELETE + 新分片 INSERT（跨分片搬迁）")
    void updateAcrossShardsMovesRow() {
        THLEvent e = event("UPDATE", "orders");
        e.addMetadata("update_column_names", "id,user_id,amount");
        e.addMetadata("update_before_column_names", "id,user_id,amount");
        e.addMetadata("rows_typed", rows(1L, 7L, 99));        // 新: 7 % 4 = 3
        e.addMetadata("rows_before_typed", rows(1L, 5L, 10)); // 旧: 5 % 4 = 1

        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(2, dmls.size(), "应产生一条 DELETE 和一条 INSERT");

        ParameterizedDml del = dmls.get(0);
        assertEquals("DELETE", del.getOpType());
        assertTrue(del.getSql().startsWith("DELETE FROM `dw_0`.`orders_1` WHERE `id`=?"), del.getSql());
        assertEquals(List.of(1L), del.getParams());

        ParameterizedDml ins = dmls.get(1);
        assertEquals("INSERT", ins.getOpType());
        assertTrue(ins.getSql().startsWith("INSERT INTO `dw_1`.`orders_3` "), ins.getSql());
        assertEquals(List.of(1L, 7L, 99), ins.getParams());
        // 先删后插：同一事务里顺序反了会先插出主键冲突（同分片场景）
        assertTrue(dmls.indexOf(del) < dmls.indexOf(ins));
    }

    @Test
    @DisplayName("分片键为 NULL：按 DEADLETTER 策略不应用（不静默写到某一片）")
    void unroutedRowGoesToDeadletter() {
        THLEvent e = event("INSERT", "orders");
        e.addMetadata("rows_typed", rows(1L, null, 10));
        assertTrue(converter().convert(e).isEmpty());
    }

    @Test
    @DisplayName("未配 unrouted 时默认广播：分片键为 NULL 的行进每一片")
    void unroutedRowBroadcastsByDefault() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("target.db.database", "app");
        props.setProperty("route.mode", "SPLIT");
        props.setProperty("route.split.1.match", "app.orders");
        props.setProperty("route.split.1.shard.key", "user_id");
        props.setProperty("route.split.1.algo", "HASH_MOD");
        props.setProperty("route.split.1.count", "4");
        props.setProperty("route.split.1.target.table", "orders_${shard}");

        THLEvent e = event("INSERT", "orders");
        e.addMetadata("rows_typed", rows(1L, null, 10));
        assertEquals(4, new TypedDmlConverter(props).convert(e).size());
    }

    @Test
    @DisplayName("未命中拆分规则的表：仍按原 1:1 路径")
    void unmatchedTableUnchanged() {
        THLEvent e = event("INSERT", "users");
        e.addMetadata("column_names", "id,user_id,amount");
        e.addMetadata("rows_typed", rows(1L, 5L, 10));
        assertTrue(converter().convert(e).get(0).getSql().startsWith("INSERT INTO `app`.`users` "));
    }

    @Test
    @DisplayName("跨实例目标组：增量尚未支持，构造即抛（不半通不通地跑）")
    void crossInstanceSplitRejected() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("route.mode", "SPLIT");
        props.setProperty("route.split.1.match", "app.orders");
        props.setProperty("route.split.1.shard.key", "user_id");
        props.setProperty("route.split.1.algo", "HASH_MOD");
        props.setProperty("route.split.1.count", "2");
        props.setProperty("route.split.1.target.table", "orders_${shard}");
        props.setProperty("route.split.1.target.group", "g1");
        props.setProperty("route.node.g1.0.host", "10.0.0.1");
        props.setProperty("route.node.g1.0.port", "3306");

        assertThrows(IllegalStateException.class, () -> new TypedDmlConverter(props));
    }
}
