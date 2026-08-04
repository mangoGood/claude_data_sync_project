package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypedDmlConverter} 汇聚（route.mode=MERGE）单元测试。
 *
 * <p>重点是<b>UPDATE/DELETE 的 WHERE 必须带来源标识列</b>：汇聚表里源主键不唯一，
 * 少了这个条件就会改到/删掉其它来源的同主键行——不报错，只丢数据。
 */
@DisplayName("TypedDmlConverter 汇聚路由")
class TypedDmlConverterMergeTest {

    private TypedDmlConverter converter() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("target.db.database", "shard_db_1");
        props.setProperty("route.mode", "MERGE");
        props.setProperty("route.node.id", "node-a");
        props.setProperty("route.merge.1.match", "shard_db_*.order_*");
        props.setProperty("route.merge.1.target", "dw.order_all");
        return new TypedDmlConverter(props);
    }

    private THLEvent event(String type, String db, String table) {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", type);
        e.addMetadata("database_name", db);
        e.addMetadata("table_name", table);
        e.addMetadata("column_names", "id,v");
        e.addMetadata("primary_keys", "id");
        return e;
    }

    private static ArrayList<ArrayList<Object>> rows(Object... values) {
        ArrayList<ArrayList<Object>> rows = new ArrayList<>();
        ArrayList<Object> row = new ArrayList<>();
        for (Object v : values) {
            row.add(v);
        }
        rows.add(row);
        return rows;
    }

    @Test
    @DisplayName("INSERT：写入汇聚目标表，来源标识列补在列尾并注值")
    void insertGoesToMergedTableWithTags() {
        THLEvent e = event("INSERT", "shard_db_2", "order_007");
        e.addMetadata("rows_typed", rows(1, "x"));

        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        String sql = dmls.get(0).getSql();
        assertTrue(sql.startsWith("INSERT INTO `dw`.`order_all` "
                + "(`id`, `v`, `_src_node`, `_src_db`, `_src_table`)"), sql);
        assertEquals(List.of(1, "x", "node-a", "shard_db_2", "order_007"), dmls.get(0).getParams());
    }

    @Test
    @DisplayName("UPDATE：WHERE 必须同时带主键和来源标识列，否则会改到其它来源的同主键行")
    void updateWhereCarriesSourceTags() {
        THLEvent e = event("UPDATE", "shard_db_2", "order_007");
        e.addMetadata("update_column_names", "id,v");
        e.addMetadata("update_before_column_names", "id,v");
        e.addMetadata("rows_typed", rows(1, "new"));
        e.addMetadata("rows_before_typed", rows(1, "old"));

        ParameterizedDml dml = converter().convert(e).get(0);
        String sql = dml.getSql();
        assertTrue(sql.startsWith("UPDATE `dw`.`order_all` SET"), sql);
        assertTrue(sql.contains("WHERE `id`=?"), sql);
        assertTrue(sql.contains("AND `_src_node`=?"), sql);
        assertTrue(sql.contains("AND `_src_db`=?"), sql);
        assertTrue(sql.contains("AND `_src_table`=?"), sql);
        // 参数顺序：SET 值 → 主键 → 来源标识
        assertEquals(List.of(1, "new", 1, "node-a", "shard_db_2", "order_007"), dml.getParams());
    }

    @Test
    @DisplayName("DELETE：WHERE 必须带来源标识列，否则会删掉其它来源的同主键行")
    void deleteWhereCarriesSourceTags() {
        THLEvent e = event("DELETE", "shard_db_3", "order_009");
        e.addMetadata("rows_typed", rows(5, "x"));

        ParameterizedDml dml = converter().convert(e).get(0);
        String sql = dml.getSql();
        assertTrue(sql.startsWith("DELETE FROM `dw`.`order_all` WHERE `id`=?"), sql);
        assertTrue(sql.contains("AND `_src_db`=?") && sql.contains("AND `_src_table`=?"), sql);
        assertEquals(List.of(5, "node-a", "shard_db_3", "order_009"), dml.getParams());
    }

    @Test
    @DisplayName("未命中汇聚规则的表：仍按原 1:1 路径写自己的库表")
    void unmatchedTableUnchanged() {
        THLEvent e = event("INSERT", "shard_db_1", "users");
        e.addMetadata("rows_typed", rows(1, "x"));

        String sql = converter().convert(e).get(0).getSql();
        assertTrue(sql.startsWith("INSERT INTO `shard_db_1`.`users` (`id`, `v`)"), sql);
        assertFalse(sql.contains("_src_db"), sql);
    }

    @Test
    @DisplayName("行标识带上来源：不同来源的同主键行不会被冲突消解当成同一行")
    void rowKeyIncludesSource() {
        THLEvent e2 = event("INSERT", "shard_db_2", "order_007");
        e2.addMetadata("rows_typed", rows(1, "x"));
        THLEvent e3 = event("INSERT", "shard_db_3", "order_007");
        e3.addMetadata("rows_typed", rows(1, "x"));

        TypedDmlConverter c = converter();
        String k2 = c.convert(e2).get(0).getRowKey();
        String k3 = c.convert(e3).get(0).getRowKey();
        assertFalse(k2.equals(k3), "不同来源的同主键行必须有不同的行标识: " + k2);
    }

    @Test
    @DisplayName("requiresTypedPipeline：汇聚表的行事件为 true，其它表/DDL 为 false")
    void requiresTypedPipelineOnlyForMergedRowEvents() {
        TypedDmlConverter c = converter();
        assertTrue(c.requiresTypedPipeline(event("INSERT", "shard_db_2", "order_007")));
        assertTrue(c.requiresTypedPipeline(event("UPDATE_ROWS", "shard_db_2", "order_007")));
        assertFalse(c.requiresTypedPipeline(event("INSERT", "shard_db_1", "users")));
        assertFalse(c.requiresTypedPipeline(event("QUERY", "shard_db_2", "order_007")));
    }

    @Test
    @DisplayName("PG 目标：ON CONFLICT 冲突目标带上来源标识列（否则同主键行会被 DO NOTHING 丢掉）")
    void postgresConflictTargetIncludesTags() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "postgresql");
        props.setProperty("target.db.type", "postgresql");
        props.setProperty("target.db.database", "dw");
        props.setProperty("route.mode", "MERGE");
        props.setProperty("route.merge.1.match", "shard_db_*.order_*");
        props.setProperty("route.merge.1.target", "dw.order_all");

        THLEvent e = event("INSERT", "shard_db_2", "order_007");
        e.addMetadata("rows_typed", rows(1, "x"));
        String sql = new TypedDmlConverter(props).convert(e).get(0).getSql();
        assertTrue(sql.contains("ON CONFLICT (\"id\", \"_src_node\", \"_src_db\", \"_src_table\") DO NOTHING"), sql);
    }

    @Test
    @DisplayName("route.mode 未配置：转换结果与接入路由前完全一致（零回归）")
    void noRoutingIsUnchanged() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("target.db.database", "shard_db_1");

        THLEvent e = event("INSERT", "shard_db_1", "order_001");
        e.addMetadata("rows_typed", rows(1, "x"));
        String sql = new TypedDmlConverter(props).convert(e).get(0).getSql();
        assertEquals("INSERT INTO `shard_db_1`.`order_001` (`id`, `v`) VALUES (?, ?) "
                + "ON DUPLICATE KEY UPDATE `id` = VALUES(`id`), `v` = VALUES(`v`)", sql);
    }
}
