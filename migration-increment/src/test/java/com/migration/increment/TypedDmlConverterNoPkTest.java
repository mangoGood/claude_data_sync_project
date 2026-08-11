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
 * 无主键表的行定位。
 *
 * <p>无主键表只能按<b>整行前镜像</b>定位，而无主键表天然允许<b>完全重复的行</b>：
 * 源端 3 条一模一样的行删掉 1 条，binlog 里是"删 1 行"，目标端那条
 * {@code DELETE ... WHERE c1=? AND c2=?} 却会把 3 条全删掉。
 * 实测：源剩 2 条 / 目标剩 0 条，不报错、不进死信，只有对数时才发现。
 *
 * <p>有意思的是文本回退路径一直有 {@code LIMIT 1}，是类型化管道接手时把这条丢了——
 * 所以这几例同时也是"别再丢一次"的回归锁。
 */
@DisplayName("无主键表：UPDATE/DELETE 只影响一行")
class TypedDmlConverterNoPkTest {

    private TypedDmlConverter converter(String targetType, String policy) {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", targetType);
        props.setProperty("target.db.database", "tgt");
        if (policy != null) {
            props.setProperty("increment.nopk.row.match", policy);
        }
        return new TypedDmlConverter(props);
    }

    /** 无主键：不下发 primary_keys。 */
    private THLEvent event(String type) {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", type);
        e.addMetadata("database_name", "src");
        e.addMetadata("table_name", "dup_nopk");
        e.addMetadata("column_names", "c1,c2");
        return e;
    }

    private THLEvent pkEvent(String type) {
        THLEvent e = event(type);
        e.addMetadata("primary_keys", "c1");
        return e;
    }

    private static ArrayList<ArrayList<Object>> rows(Object... vals) {
        ArrayList<Object> r = new ArrayList<>();
        for (Object v : vals) r.add(v);
        ArrayList<ArrayList<Object>> out = new ArrayList<>();
        out.add(r);
        return out;
    }

    @Test
    @DisplayName("MySQL 目标：无主键 DELETE 带 LIMIT 1")
    void mysqlDeleteLimitOne() {
        THLEvent e = event("DELETE");
        e.addMetadata("rows_typed", rows(1, "x"));

        List<ParameterizedDml> dmls = converter("mysql", null).convert(e);
        assertEquals(1, dmls.size());
        assertTrue(dmls.get(0).getSql().endsWith(" LIMIT 1"),
                "无主键 DELETE 必须限量，否则重复行会被一起删掉: " + dmls.get(0).getSql());
    }

    @Test
    @DisplayName("MySQL 目标：无主键 UPDATE 带 LIMIT 1")
    void mysqlUpdateLimitOne() {
        THLEvent e = event("UPDATE");
        e.addMetadata("update_column_names", "c1,c2");
        e.addMetadata("rows_typed", rows(1, "y"));
        e.addMetadata("rows_before_typed", rows(1, "x"));

        List<ParameterizedDml> dmls = converter("mysql", null).convert(e);
        assertEquals(1, dmls.size());
        assertTrue(dmls.get(0).getSql().endsWith(" LIMIT 1"),
                "无主键 UPDATE 必须限量: " + dmls.get(0).getSql());
    }

    @Test
    @DisplayName("PostgreSQL 目标：PG 的 UPDATE/DELETE 不支持 LIMIT，改写成 ctid 子查询")
    void postgresUsesCtidSubquery() {
        THLEvent e = event("DELETE");
        e.addMetadata("rows_typed", rows(1, "x"));

        List<ParameterizedDml> dmls = converter("postgresql", null).convert(e);
        assertEquals(1, dmls.size());
        String sql = dmls.get(0).getSql();
        assertTrue(sql.contains("ctid IN (SELECT ctid FROM"), "PG 应走 ctid 子查询: " + sql);
        assertTrue(sql.endsWith(" LIMIT 1)"), "子查询里要限量: " + sql);
        // 子查询的条件与原 WHERE 一模一样，所以参数顺序不用动
        assertEquals(2, dmls.get(0).getParams().size());
    }

    @Test
    @DisplayName("有主键表不受影响：WHERE 本就只命中一行，不该多这一句")
    void primaryKeyTableUntouched() {
        THLEvent e = pkEvent("DELETE");
        e.addMetadata("rows_typed", rows(1, "x"));

        List<ParameterizedDml> dmls = converter("mysql", null).convert(e);
        assertEquals(1, dmls.size());
        assertFalse(dmls.get(0).getSql().contains("LIMIT"),
                "有主键表不该被加上限量子句: " + dmls.get(0).getSql());
    }

    @Test
    @DisplayName("ALL_MATCHING 显式退回旧行为（不推荐，但得留一条退路）")
    void allMatchingKeepsOldBehaviour() {
        THLEvent e = event("DELETE");
        e.addMetadata("rows_typed", rows(1, "x"));

        List<ParameterizedDml> dmls = converter("mysql", "ALL_MATCHING").convert(e);
        assertEquals(1, dmls.size());
        assertFalse(dmls.get(0).getSql().contains("LIMIT"), dmls.get(0).getSql());
    }
}
