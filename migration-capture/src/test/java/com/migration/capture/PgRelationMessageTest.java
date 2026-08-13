package com.migration.capture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * pgoutput 的 Relation('R') 消息解析。
 *
 * <p>Relation 消息带的是<b>与行值同一时刻</b>的权威表结构，且 PG 在关系定义变化后会重发 ——
 * 这是 PG 侧对付 schema 漂移的正解，地位等同 MySQL 的 {@code binlog_row_metadata=FULL}。
 * 旧实现只从中取了库名表名，列名/类型改为回查 {@code information_schema} 并永久缓存，
 * 源端 {@code DROP COLUMN} 之后整行错位一格静默写坏。
 */
@DisplayName("PG Relation 消息：权威列结构 + 变更即失效")
class PgRelationMessageTest {

    private PostgresWalCapture capture;

    @BeforeEach
    void setUp() {
        capture = new PostgresWalCapture();
    }

    private String parse(byte[] msg) throws Exception {
        Method m = PostgresWalCapture.class.getDeclaredMethod("parsePgoutputMessage", byte[].class);
        m.setAccessible(true);
        return (String) m.invoke(capture, (Object) msg);
    }

    /** 构造一条 Relation 消息：'R' + oid + namespace + relname + replicaIdentity + ncols + 每列(flags,name,typeOid,typmod)。 */
    private static byte[] relation(int oid, String ns, String rel, String[][] cols) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write('R');
        int32(out, oid);
        cstring(out, ns);
        cstring(out, rel);
        out.write('d');                      // replica identity: default
        int16(out, cols.length);
        for (String[] c : cols) {
            out.write(Integer.parseInt(c[2]));   // flags：1=属于键
            cstring(out, c[0]);
            int32(out, Integer.parseInt(c[1]));  // 类型 OID
            int32(out, -1);                      // typmod
        }
        return out.toByteArray();
    }

    private static void int16(ByteArrayOutputStream o, int v) {
        o.write((v >> 8) & 0xFF);
        o.write(v & 0xFF);
    }

    private static void int32(ByteArrayOutputStream o, int v) {
        o.write((v >> 24) & 0xFF);
        o.write((v >> 16) & 0xFF);
        o.write((v >> 8) & 0xFF);
        o.write(v & 0xFF);
    }

    private static void cstring(ByteArrayOutputStream o, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        o.write(b, 0, b.length);
        o.write(0);
    }

    @Test
    @DisplayName("列名、类型、主键标志都从消息里解析出来")
    void parsesColumnsTypesAndKeys() throws Exception {
        String out = parse(relation(16400, "public", "vals", new String[][]{
                {"id", "23", "1"},           // int4，属于键
                {"tag", "1043", "0"},        // varchar
                {"blob_txt", "25", "0"}      // text
        }));

        assertTrue(out.startsWith("RELATION schema:public table:vals"), out);
        assertTrue(out.contains("columns:id,tag,blob_txt"), out);
    }

    @Test
    @DisplayName("类型 OID 译成与 information_schema.data_type 一致的写法")
    void mapsTypeOidsToInformationSchemaNames() throws Exception {
        parse(relation(16401, "public", "t", new String[][]{
                {"a", "23", "1"}, {"b", "1043", "0"}, {"c", "16", "0"},
                {"d", "1114", "0"}, {"e", "17", "0"}, {"f", "1700", "0"}
        }));
        assertEquals(java.util.Arrays.asList(
                        "integer", "character varying", "boolean",
                        "timestamp without time zone", "bytea", "numeric"),
                typesOf(16401));
    }

    @Test
    @DisplayName("重发的 Relation 消息整体替换旧结构——这就是失效机制")
    void laterRelationMessageReplacesEarlierOne() throws Exception {
        parse(relation(16402, "public", "t", new String[][]{
                {"id", "23", "1"}, {"dropme", "25", "0"}, {"keep", "25", "0"}
        }));
        assertEquals(java.util.Arrays.asList("id", "dropme", "keep"), columnsOf(16402));

        // 源端 DROP COLUMN dropme 之后 PG 会重发 Relation
        parse(relation(16402, "public", "t", new String[][]{
                {"id", "23", "1"}, {"keep", "25", "0"}
        }));
        assertEquals(java.util.Arrays.asList("id", "keep"), columnsOf(16402),
                "列定义变了必须整体替换；留着旧的就会把值按老下标配到新列上");
    }

    @Test
    @DisplayName("非 ASCII 的库表名/列名不会让后续字段错位")
    void handlesMultiByteIdentifiers() throws Exception {
        parse(relation(16403, "公共", "订单表", new String[][]{
                {"编号", "23", "1"}, {"备注", "25", "0"}
        }));
        assertEquals(java.util.Arrays.asList("编号", "备注"), columnsOf(16403),
                "按 String.length() 前进会把多字节名字数错，之后每个字段都错位");
        assertEquals(java.util.Arrays.asList("integer", "text"), typesOf(16403));
    }

    @Test
    @DisplayName("未处理的消息类型不再当作没这回事（返回空且留下告警）")
    void unknownMessageTypeIsNotSilentlyIgnored() throws Exception {
        assertEquals("", parse(new byte[]{'Z', 0, 0, 0, 1}));
    }

    @SuppressWarnings("unchecked")
    private java.util.List<String> columnsOf(long oid) throws Exception {
        Object rel = relationSchema(oid);
        java.lang.reflect.Field f = rel.getClass().getDeclaredField("columns");
        f.setAccessible(true);
        return (java.util.List<String>) f.get(rel);
    }

    @SuppressWarnings("unchecked")
    private java.util.List<String> typesOf(long oid) throws Exception {
        Object rel = relationSchema(oid);
        java.lang.reflect.Field f = rel.getClass().getDeclaredField("types");
        f.setAccessible(true);
        return (java.util.List<String>) f.get(rel);
    }

    @SuppressWarnings("unchecked")
    private Object relationSchema(long oid) throws Exception {
        java.lang.reflect.Field f = PostgresWalCapture.class.getDeclaredField("relationSchemas");
        f.setAccessible(true);
        Object v = ((java.util.Map<Long, Object>) f.get(capture)).get(oid);
        assertFalse(v == null, "relationSchemas 里没有 oid=" + oid);
        return v;
    }
}
