package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypedDmlConverter} 类型化值管道单元测试：
 * 参数化 SQL 形状、参数类型（Boolean/byte[]/String/null）、回退条件（缺元数据/列值不齐/非 mysql→pg）。
 */
@DisplayName("TypedDmlConverter 参数化 DML 生成")
class TypedDmlConverterTest {

    private TypedDmlConverter converter;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "postgresql");
        converter = new TypedDmlConverter(props);
    }

    private THLEvent event(String type) {
        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", type);
        e.addMetadata("database_name", "src");
        e.addMetadata("table_name", "BT");
        e.addMetadata("column_names", "id,name,c_bool,c_bit");
        e.addMetadata("primary_keys", "id");
        return e;
    }

    private static ArrayList<ArrayList<Object>> rows(ArrayList<Object>... rs) {
        ArrayList<ArrayList<Object>> out = new ArrayList<>();
        for (ArrayList<Object> r : rs) out.add(r);
        return out;
    }

    private static ArrayList<Object> typedRow(Object... vals) {
        ArrayList<Object> l = new ArrayList<>();
        for (Object v : vals) l.add(v);
        return l;
    }

    @Test
    @DisplayName("INSERT：? 占位 + ON CONFLICT，参数保持类型（Boolean/byte[]）")
    void insert() {
        THLEvent e = event("INSERT");
        e.addMetadata("rows_typed", rows(typedRow("1", "alice", Boolean.TRUE, new byte[]{(byte) 0xaa})));

        List<ParameterizedDml> dmls = converter.convert(e);
        assertEquals(1, dmls.size());
        ParameterizedDml dml = dmls.get(0);
        assertEquals("INSERT INTO \"bt\" (\"id\", \"name\", \"c_bool\", \"c_bit\") VALUES (?, ?, ?, ?)"
                + " ON CONFLICT (\"id\") DO NOTHING", dml.getSql());
        assertEquals(Boolean.TRUE, dml.getParams().get(2));
        assertArrayEquals(new byte[]{(byte) 0xaa}, (byte[]) dml.getParams().get(3));
    }

    @Test
    @DisplayName("UPDATE：SET 全列参数化，WHERE 按主键取前镜像值")
    void update() {
        THLEvent e = event("UPDATE");
        e.addMetadata("rows_typed", rows(typedRow("1", "alice", Boolean.FALSE, new byte[]{1})));
        e.addMetadata("rows_before_typed", rows(typedRow("1", "alice", Boolean.TRUE, new byte[]{(byte) 0xaa})));

        List<ParameterizedDml> dmls = converter.convert(e);
        assertEquals(1, dmls.size());
        ParameterizedDml dml = dmls.get(0);
        assertEquals("UPDATE \"bt\" SET \"id\"=?, \"name\"=?, \"c_bool\"=?, \"c_bit\"=? WHERE \"id\"=?",
                dml.getSql());
        assertEquals(5, dml.getParams().size());
        assertEquals(Boolean.FALSE, dml.getParams().get(2));
        assertEquals("1", dml.getParams().get(4)); // WHERE 主键来自前镜像
    }

    @Test
    @DisplayName("DELETE：WHERE 主键；NULL 值输出 IS NULL 且不占参数")
    void deleteAndNulls() {
        THLEvent e = event("DELETE");
        e.addMetadata("rows_typed", rows(typedRow("2", null, null, null)));

        List<ParameterizedDml> dmls = converter.convert(e);
        assertEquals("DELETE FROM \"bt\" WHERE \"id\"=?", dmls.get(0).getSql());
        assertEquals(1, dmls.get(0).getParams().size());

        // 无主键 → 整行前镜像定位，NULL 列用 IS NULL
        THLEvent e2 = event("DELETE");
        e2.getMetadata().remove("primary_keys");
        e2.addMetadata("rows_typed", rows(typedRow("2", "bob", null, null)));
        ParameterizedDml d2 = converter.convert(e2).get(0);
        assertEquals("DELETE FROM \"bt\" WHERE \"id\"=? AND \"name\"=? AND \"c_bool\" IS NULL AND \"c_bit\" IS NULL",
                d2.getSql());
        assertEquals(2, d2.getParams().size());
    }

    @Test
    @DisplayName("多行事件：每行一条参数化 DML")
    void multiRow() {
        THLEvent e = event("INSERT");
        e.addMetadata("rows_typed", rows(
                typedRow("1", "a", Boolean.TRUE, null),
                typedRow("2", "b", Boolean.FALSE, null)));
        assertEquals(2, converter.convert(e).size());
    }

    @Test
    @DisplayName("回退条件：缺 rows_typed / 列值不齐 / 非 DML / 不支持的库对均返回 null")
    void fallbackConditions() {
        // 缺 rows_typed
        assertNull(converter.convert(event("INSERT")));
        // 列值数量不齐
        THLEvent bad = event("INSERT");
        bad.addMetadata("rows_typed", rows(typedRow("1", "a")));
        assertNull(converter.convert(bad));
        // DDL/QUERY 事件
        THLEvent q = event("QUERY");
        q.addMetadata("rows_typed", rows(typedRow("1", "a", null, null)));
        assertNull(converter.convert(q));
        // 不支持的库对（oracle 为目标未覆盖）：整个转换器禁用
        Properties p = new Properties();
        p.setProperty("source.db.type", "mysql");
        p.setProperty("target.db.type", "oracle");
        assertNull(new TypedDmlConverter(p).convert(eventWithRows()));
        // 显式开关关闭
        Properties p2 = new Properties();
        p2.setProperty("source.db.type", "mysql");
        p2.setProperty("target.db.type", "postgresql");
        p2.setProperty("increment.typed.pipeline.enabled", "false");
        assertNull(new TypedDmlConverter(p2).convert(eventWithRows()));
    }

    private THLEvent eventWithRows() {
        THLEvent e = event("INSERT");
        e.addMetadata("rows_typed", rows(typedRow("1", "a", null, null)));
        return e;
    }

    @Test
    @DisplayName("oracle→pg：大写标识符折叠小写，部分列 INSERT + RAW byte[] 参数")
    void oracleSourcePgTarget() {
        Properties p = new Properties();
        p.setProperty("source.db.type", "oracle");
        p.setProperty("target.db.type", "postgresql");
        TypedDmlConverter c = new TypedDmlConverter(p);

        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("table_name", "ALL_TYPES_ORA");
        e.addMetadata("column_names", "ID,C_NUM,C_VARCHAR2,C_RAW");
        e.addMetadata("primary_keys", "ID");
        e.addMetadata("insert_column_names", "ID,C_VARCHAR2,C_RAW");
        e.addMetadata("rows_typed", rows(typedRow("5", "oracle increment row", new byte[]{(byte) 0xde, (byte) 0xad})));

        ParameterizedDml dml = c.convert(e).get(0);
        assertEquals("INSERT INTO \"all_types_ora\" (\"id\", \"c_varchar2\", \"c_raw\") VALUES (?, ?, ?)"
                + " ON CONFLICT (\"id\") DO NOTHING", dml.getSql());
        assertArrayEquals(new byte[]{(byte) 0xde, (byte) 0xad}, (byte[]) dml.getParams().get(2));
    }

    @Test
    @DisplayName("pg→mysql：反引号 + 目标库限定 + ON DUPLICATE KEY UPDATE，Boolean/byte[] 参数")
    void pgSourceMysqlTarget() {
        Properties p = new Properties();
        p.setProperty("source.db.type", "postgresql");
        p.setProperty("target.db.type", "mysql");
        p.setProperty("target.db.database", "tgt_db");
        TypedDmlConverter c = new TypedDmlConverter(p);

        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("database_name", "public");
        e.addMetadata("table_name", "bt");
        e.addMetadata("column_names", "id,c_bool,c_bin");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("rows_typed", rows(typedRow("1", Boolean.TRUE, new byte[]{1, 2})));

        ParameterizedDml dml = c.convert(e).get(0);
        assertEquals("INSERT INTO `tgt_db`.`bt` (`id`, `c_bool`, `c_bin`) VALUES (?, ?, ?)"
                + " ON DUPLICATE KEY UPDATE `id` = VALUES(`id`), `c_bool` = VALUES(`c_bool`), `c_bin` = VALUES(`c_bin`)",
                dml.getSql());
        assertEquals(Boolean.TRUE, dml.getParams().get(1));

        // UPDATE：WHERE 主键，反引号
        THLEvent u = new THLEvent();
        u.setSeqno(2);
        u.addMetadata("event_type", "UPDATE");
        u.addMetadata("database_name", "public");
        u.addMetadata("table_name", "bt");
        u.addMetadata("column_names", "id,c_bool,c_bin");
        u.addMetadata("primary_keys", "id");
        u.addMetadata("rows_typed", rows(typedRow("1", Boolean.FALSE, null)));
        u.addMetadata("rows_before_typed", rows(typedRow("1", Boolean.TRUE, null)));
        ParameterizedDml ud = c.convert(u).get(0);
        assertEquals("UPDATE `tgt_db`.`bt` SET `id`=?, `c_bool`=?, `c_bin`=? WHERE `id`=?", ud.getSql());
    }

    @Test
    @DisplayName("未覆盖的跨库对（oracle→mysql / 任何以 oracle 为目标）不启用类型化管道")
    void uncoveredPairsDisabled() {
        Properties p = new Properties();
        p.setProperty("source.db.type", "oracle");
        p.setProperty("target.db.type", "mysql");
        assertNull(new TypedDmlConverter(p).convert(eventWithRows()));

        Properties p2 = new Properties();
        p2.setProperty("source.db.type", "postgresql");
        p2.setProperty("target.db.type", "oracle");
        assertNull(new TypedDmlConverter(p2).convert(eventWithRows()));
    }

    @Test
    @DisplayName("mysql→mysql（同构）：反引号 + 目标库限定 + ON DUPLICATE KEY UPDATE")
    void mysqlToMysqlHomogeneous() {
        Properties p = new Properties();
        p.setProperty("source.db.type", "mysql");
        p.setProperty("target.db.type", "mysql");
        p.setProperty("target.db.database", "tgt_db");
        TypedDmlConverter c = new TypedDmlConverter(p);

        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("database_name", "src_db");
        e.addMetadata("table_name", "bt");
        e.addMetadata("column_names", "id,name,c_bool,c_bit");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("rows_typed", rows(typedRow("1", "alice", Boolean.TRUE, new byte[]{(byte) 0xaa})));

        ParameterizedDml dml = c.convert(e).get(0);
        assertEquals("INSERT INTO `tgt_db`.`bt` (`id`, `name`, `c_bool`, `c_bit`) VALUES (?, ?, ?, ?)"
                + " ON DUPLICATE KEY UPDATE `id` = VALUES(`id`), `name` = VALUES(`name`), "
                + "`c_bool` = VALUES(`c_bool`), `c_bit` = VALUES(`c_bit`)",
                dml.getSql());
        assertEquals(Boolean.TRUE, dml.getParams().get(2));
        assertArrayEquals(new byte[]{(byte) 0xaa}, (byte[]) dml.getParams().get(3));
    }

    @Test
    @DisplayName("postgresql→postgresql（同构）：小写双引号 + ON CONFLICT DO NOTHING")
    void pgToPgHomogeneous() {
        Properties p = new Properties();
        p.setProperty("source.db.type", "postgresql");
        p.setProperty("target.db.type", "postgresql");
        TypedDmlConverter c = new TypedDmlConverter(p);

        THLEvent e = new THLEvent();
        e.setSeqno(1);
        e.addMetadata("event_type", "INSERT");
        e.addMetadata("database_name", "public");
        e.addMetadata("table_name", "bt");
        e.addMetadata("column_names", "id,name,c_bool,c_bin");
        e.addMetadata("primary_keys", "id");
        e.addMetadata("rows_typed", rows(typedRow("1", "alice", Boolean.TRUE, new byte[]{1, 2})));

        ParameterizedDml dml = c.convert(e).get(0);
        assertEquals("INSERT INTO \"bt\" (\"id\", \"name\", \"c_bool\", \"c_bin\") VALUES (?, ?, ?, ?)"
                + " ON CONFLICT (\"id\") DO NOTHING", dml.getSql());
        assertEquals(Boolean.TRUE, dml.getParams().get(2));

        // UPDATE：WHERE 主键，小写双引号
        THLEvent u = new THLEvent();
        u.setSeqno(2);
        u.addMetadata("event_type", "UPDATE");
        u.addMetadata("database_name", "public");
        u.addMetadata("table_name", "bt");
        u.addMetadata("column_names", "id,name,c_bool,c_bin");
        u.addMetadata("primary_keys", "id");
        u.addMetadata("rows_typed", rows(typedRow("1", "bob", Boolean.FALSE, null)));
        u.addMetadata("rows_before_typed", rows(typedRow("1", "alice", Boolean.TRUE, null)));
        ParameterizedDml ud = c.convert(u).get(0);
        assertEquals("UPDATE \"bt\" SET \"id\"=?, \"name\"=?, \"c_bool\"=?, \"c_bin\"=? WHERE \"id\"=?", ud.getSql());
    }

    @Test
    @DisplayName("INSERT 优先使用 insert_column_names（部分列对齐）")
    void insertColumnNamesHonored() {
        THLEvent e = event("INSERT");
        e.addMetadata("insert_column_names", "id,name");
        e.addMetadata("rows_typed", rows(typedRow("5", "partial")));
        ParameterizedDml dml = converter.convert(e).get(0);
        assertTrue(dml.getSql().startsWith("INSERT INTO \"bt\" (\"id\", \"name\") VALUES (?, ?)"));
    }
}
