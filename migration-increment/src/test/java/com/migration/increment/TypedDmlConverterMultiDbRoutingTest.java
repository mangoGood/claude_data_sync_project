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
 * {@link TypedDmlConverter} 多库目标路由单元测试：每个事件按自己的源库解析目标库
 * （schema.mapping.db 命中→映射值；未命中→源库名；事件缺库名→target.db.database 兜底），
 * 修复此前单一 target.db.database 覆盖导致的多库串写。
 */
@DisplayName("TypedDmlConverter 多库目标路由")
class TypedDmlConverterMultiDbRoutingTest {

    private TypedDmlConverter converter(String... mappingPairs) {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        // 模拟多库任务的历史配置形态：target.db.database 被写成第一个库名
        props.setProperty("target.db.database", "dbm1");
        for (int i = 0; i + 1 < mappingPairs.length; i += 2) {
            props.setProperty("schema.mapping.db." + mappingPairs[i], mappingPairs[i + 1]);
        }
        return new TypedDmlConverter(props);
    }

    private THLEvent insertEvent(String db, String table) {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", "INSERT");
        if (db != null) {
            e.addMetadata("database_name", db);
        }
        e.addMetadata("table_name", table);
        e.addMetadata("column_names", "id,v");
        e.addMetadata("primary_keys", "id");
        ArrayList<ArrayList<Object>> rows = new ArrayList<>();
        rows.add(new ArrayList<>(java.util.List.of(1, "x")));
        e.addMetadata("rows_typed", rows);
        return e;
    }

    @Test
    @DisplayName("无映射多库：第二个库的事件保留自己的源库名，不被 target.db.database 覆盖")
    void secondDbKeepsOwnDatabase() {
        List<ParameterizedDml> dmls = converter().convert(insertEvent("dbm2", "tb"));
        assertEquals(1, dmls.size());
        String sql = dmls.get(0).getSql();
        assertTrue(sql.startsWith("INSERT INTO `dbm2`.`tb`"), "dbm2 的事件应写入 dbm2: " + sql);
    }

    @Test
    @DisplayName("无映射多库：第一个库的事件仍写入第一个库")
    void firstDbUnchanged() {
        List<ParameterizedDml> dmls = converter().convert(insertEvent("dbm1", "ta"));
        assertTrue(dmls.get(0).getSql().startsWith("INSERT INTO `dbm1`.`ta`"));
    }

    @Test
    @DisplayName("库名映射命中：按映射值路由（小写回退）")
    void mappedDbRouted() {
        TypedDmlConverter c = converter("dbm2", "tgt2");
        assertTrue(c.convert(insertEvent("dbm2", "tb")).get(0).getSql()
                .startsWith("INSERT INTO `tgt2`.`tb`"));
        // 小写回退：源库不区分大小写场景
        assertTrue(c.convert(insertEvent("DBM2", "tb")).get(0).getSql()
                .startsWith("INSERT INTO `tgt2`.`tb`"));
        // 未映射的库不受影响
        assertTrue(c.convert(insertEvent("dbm3", "tc")).get(0).getSql()
                .startsWith("INSERT INTO `dbm3`.`tc`"));
    }

    @Test
    @DisplayName("事件缺源库名：回退 target.db.database")
    void missingDbFallsBack() {
        List<ParameterizedDml> dmls = converter().convert(insertEvent(null, "tx"));
        assertTrue(dmls.get(0).getSql().startsWith("INSERT INTO `dbm1`.`tx`"));
    }

    @Test
    @DisplayName("单库映射任务行为不变：schema.mapping.db.src=tgt 生效")
    void singleDbMappingStillWorks() {
        TypedDmlConverter c = converter("coltest_src", "coltest_tgt");
        assertTrue(c.convert(insertEvent("coltest_src", "orders")).get(0).getSql()
                .startsWith("INSERT INTO `coltest_tgt`.`orders`"));
    }
}
