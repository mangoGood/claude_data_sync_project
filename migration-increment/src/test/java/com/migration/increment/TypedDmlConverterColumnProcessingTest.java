package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypedDmlConverter} 列处理（mysql→mysql）单元测试：
 * INSERT/DELETE 行过滤、UPDATE 前后镜像判定（转 DELETE / 转 INSERT / 跳过）、列名映射改写。
 */
@DisplayName("TypedDmlConverter 列处理")
class TypedDmlConverterColumnProcessingTest {

    private TypedDmlConverter converter() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("target.db.database", "tdb");
        props.setProperty("column.filter.db1.t1", "amount|<|10");
        props.setProperty("column.mapping.db1.t1", "old_name:new_name");
        return new TypedDmlConverter(props);
    }

    /** pg→pg 同引擎链路的列处理开关与 mysql→mysql 一致（行过滤/列名映射类型无关）。 */
    private TypedDmlConverter pgConverter() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "postgresql");
        props.setProperty("target.db.type", "postgresql");
        props.setProperty("target.db.database", "tdb");
        props.setProperty("column.filter.db1.t1", "amount|<|10");
        props.setProperty("column.mapping.db1.t1", "old_name:new_name");
        return new TypedDmlConverter(props);
    }

    private THLEvent event(String type) {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", type);
        e.addMetadata("database_name", "db1");
        e.addMetadata("table_name", "t1");
        e.addMetadata("column_names", "id,amount,old_name");
        e.addMetadata("primary_keys", "id");
        return e;
    }

    private static ArrayList<Object> row(Object... vals) {
        return new ArrayList<>(java.util.Arrays.asList(vals));
    }

    private static ArrayList<ArrayList<Object>> rows(ArrayList<Object>... rs) {
        ArrayList<ArrayList<Object>> out = new ArrayList<>();
        java.util.Collections.addAll(out, rs);
        return out;
    }

    @Test
    @DisplayName("INSERT：命中过滤的行被跳过，列名映射改写 SQL")
    void insertFilterAndMapping() {
        THLEvent e = event("INSERT");
        e.addMetadata("rows_typed", rows(row(1, 5, "a"), row(2, 20, "b")));
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size()); // amount=5 < 10 被过滤
        String sql = dmls.get(0).getSql();
        assertTrue(sql.contains("`new_name`"), "列名映射应改写 INSERT 列表: " + sql);
        assertTrue(!sql.contains("`old_name`"), "源列名不应出现在 SQL 中: " + sql);
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        assertEquals(java.util.List.of(2, 20, "b"), dmls.get(0).getParams());
    }

    @Test
    @DisplayName("INSERT：整事件全部被过滤时返回空列表（不回退文本路径）")
    void insertAllFiltered() {
        THLEvent e = event("INSERT");
        e.addMetadata("rows_typed", rows(row(1, 5, "a")));
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(0, dmls.size());
    }

    @Test
    @DisplayName("UPDATE：后镜像命中过滤 → 转 DELETE")
    void updateBecomesDelete() {
        THLEvent e = event("UPDATE");
        e.addMetadata("update_column_names", "id,amount,old_name");
        e.addMetadata("update_before_column_names", "id,amount,old_name");
        e.addMetadata("rows_typed", rows(row(1, 5, "a")));       // after: amount 5 → 命中过滤
        e.addMetadata("rows_before_typed", rows(row(1, 20, "a"))); // before: amount 20 → 目标端已有该行
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        String sql = dmls.get(0).getSql();
        assertTrue(sql.startsWith("DELETE FROM"), "应转为 DELETE: " + sql);
        assertTrue(sql.contains("`id`=?"));
    }

    @Test
    @DisplayName("UPDATE：前镜像命中过滤而后镜像未命中 → 升级为幂等 INSERT")
    void updateBecomesInsert() {
        THLEvent e = event("UPDATE");
        e.addMetadata("update_column_names", "id,amount,old_name");
        e.addMetadata("update_before_column_names", "id,amount,old_name");
        e.addMetadata("rows_typed", rows(row(1, 30, "a")));      // after: 不命中 → 应存在
        e.addMetadata("rows_before_typed", rows(row(1, 5, "a"))); // before: 命中 → 目标端没有该行
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        String sql = dmls.get(0).getSql();
        assertTrue(sql.startsWith("INSERT INTO"), "应升级为 INSERT: " + sql);
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    @Test
    @DisplayName("UPDATE：前后镜像均命中过滤 → 跳过；均未命中 → 正常 UPDATE 且列名映射生效")
    void updateSkipAndNormal() {
        THLEvent e = event("UPDATE");
        e.addMetadata("update_column_names", "id,amount,old_name");
        e.addMetadata("update_before_column_names", "id,amount,old_name");
        e.addMetadata("rows_typed", rows(row(1, 5, "a"), row(2, 30, "b")));
        e.addMetadata("rows_before_typed", rows(row(1, 6, "a"), row(2, 25, "b")));
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size()); // 行1 前后均命中 → 跳过
        String sql = dmls.get(0).getSql();
        assertTrue(sql.startsWith("UPDATE"), "行2 应为普通 UPDATE: " + sql);
        assertTrue(sql.contains("`new_name`=?"), "SET 列名应映射: " + sql);
    }

    @Test
    @DisplayName("DELETE：命中过滤的行跳过，未命中的行正常删除")
    void deleteFiltered() {
        THLEvent e = event("DELETE");
        e.addMetadata("rows_typed", rows(row(1, 5, "a"), row(2, 20, "b")));
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        assertTrue(dmls.get(0).getSql().startsWith("DELETE FROM"));
        assertEquals(java.util.List.of(2), dmls.get(0).getParams());
    }

    @Test
    @DisplayName("pg→pg：列处理生效——命中过滤的行被跳过，列名映射改写 SQL")
    void pgInsertFilterAndMapping() {
        THLEvent e = event("INSERT");
        e.addMetadata("rows_typed", rows(row(1, 5, "a"), row(2, 20, "b")));
        List<ParameterizedDml> dmls = pgConverter().convert(e);
        assertEquals(1, dmls.size()); // amount=5 < 10 被过滤，与 mysql→mysql 一致
        String sql = dmls.get(0).getSql();
        assertTrue(sql.contains("new_name"), "列名映射应改写 INSERT 列表: " + sql);
        assertTrue(!sql.contains("old_name"), "源列名不应出现在 SQL 中: " + sql);
        assertEquals(java.util.List.of(2, 20, "b"), dmls.get(0).getParams());
    }

    @Test
    @DisplayName("无列处理配置的表不受影响")
    void otherTableUnaffected() {
        THLEvent e = event("INSERT");
        e.addMetadata("table_name", "t2");
        e.addMetadata("rows_typed", rows(row(1, 5, "a")));
        List<ParameterizedDml> dmls = converter().convert(e);
        assertEquals(1, dmls.size());
        assertTrue(dmls.get(0).getSql().contains("`old_name`"), "无映射时保留源列名");
    }
}
