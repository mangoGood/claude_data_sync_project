package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PG 逻辑复制里"值未随事件下发"（未变更的 TOAST）的处理。
 *
 * <p>pgoutput 对行外存储里本次 UPDATE 没修改的列只发一个 {@code 'u'} 标记、不发值。
 * 把它当成 NULL 会生成 {@code SET col=NULL}，把目标端已经正确的大字段抹掉且全程无报错 ——
 * 正确做法是把该列整个从 SET 列表里摘掉，并用 {@code update_column_names} 告知下游。
 */
@DisplayName("PG 未变更 TOAST 值：整列摘掉而不是写 NULL")
class PgUnchangedToastTest {

    private static final char FS = '\001';

    private PostgresWalExtractor extractor;

    @BeforeEach
    void setUp() throws Exception {
        extractor = new PostgresWalExtractor();
        Properties props = new Properties();
        props.setProperty("extract.output.dir", System.getProperty("java.io.tmpdir") + "/pg-toast-ut");
        props.setProperty("source.db.host", "127.0.0.1");
        props.setProperty("source.db.port", "1");
        try {
            extractor.initialize(props);
        } catch (Exception ignored) {
            // 连不上源库是预期的（单测不连外部库），doInitialize 的其余部分已经跑完
        }
        // 列名/类型是回查源库拿的，这里直接把缓存填好，等价于"源库连得上"的运行态。
        // 不填的话列名会退化成 column0/column1，测的就不是摘列逻辑了
        primeSchemaCache("public.vals",
                java.util.Arrays.asList("id", "tag", "note", "blob_txt"),
                java.util.Arrays.asList("integer", "character varying", "text", "text"),
                java.util.Collections.singletonList("id"));
    }

    @SuppressWarnings("unchecked")
    private void primeSchemaCache(String key, java.util.List<String> cols,
                                  java.util.List<String> types, java.util.List<String> pks) throws Exception {
        put("tableSchemaCache", key, cols);
        put("tableColumnTypeCache", key, types);
        put("primaryKeyCache", key, pks);
    }

    @SuppressWarnings("unchecked")
    private void put(String field, String key, java.util.List<String> value) throws Exception {
        java.lang.reflect.Field f = PostgresWalExtractor.class.getDeclaredField(field);
        f.setAccessible(true);
        ((java.util.Map<String, java.util.List<String>>) f.get(extractor)).put(key, value);
    }

    private THLEvent extract(String eventType, String payload) throws Exception {
        String line = eventType + FS + "0/16B3748" + FS + "23803720" + FS
                + System.currentTimeMillis() + FS + "0" + FS + payload;
        return extractor.doExtract(line.getBytes(StandardCharsets.UTF_8));
    }

    private static String meta(THLEvent e, String key) {
        Object v = e.getMetadata().get(key);
        return v == null ? null : String.valueOf(v);
    }

    @Test
    @DisplayName("未变更的列被摘掉，SET 列表只剩真正带了值的列")
    void unchangedColumnIsDroppedFromSetList() throws Exception {
        THLEvent e = extract("UPDATE", "schema:public table:vals primary_keys:id "
                + "new-tuple:{id:1,tag:'t1-upd',note:'n1',blob_txt:[unchanged]}");

        assertEquals("id,tag,note", meta(e, "update_column_names"),
                "blob_txt 的值没下发，必须从 SET 列表里摘掉");
        assertEquals("id,tag,note", meta(e, "update_before_column_names"),
                "没有前镜像时下游拿后镜像当前镜像用，两侧列名必须一致");
        assertFalse(meta(e, "row_data").contains("NULL"),
                "摘掉的列不能以 NULL 的形态留在行数据里: " + meta(e, "row_data"));
    }

    @Test
    @DisplayName("真 NULL 与'值未下发'是两回事：NULL 照常同步")
    void explicitNullIsStillNull() throws Exception {
        THLEvent e = extract("UPDATE", "schema:public table:vals primary_keys:id "
                + "new-tuple:{id:1,tag:'t1',note:[null],blob_txt:[unchanged]}");

        assertEquals("id,tag,note", meta(e, "update_column_names"));
        assertTrue(meta(e, "row_data").endsWith("NULL"),
                "显式赋 NULL 的列必须留在 SET 里并写 NULL: " + meta(e, "row_data"));
    }

    @Test
    @DisplayName("没有列被摘掉时不下发列名清单，走既有的全宽路径")
    void noMetadataWhenNothingDropped() throws Exception {
        THLEvent e = extract("UPDATE", "schema:public table:vals primary_keys:id "
                + "new-tuple:{id:1,tag:'t1',note:'n1',blob_txt:'v'}");

        assertNull(meta(e, "update_column_names"),
                "全列都带了值时行为应与改造前逐字节一致");
    }

    @Test
    @DisplayName("主键的值未下发：定位不出目标行，停机而不是猜")
    void unchangedPrimaryKeyFailsStop() {
        assertThrows(PostgresWalExtractor.UnreconstructableValueException.class,
                () -> extract("UPDATE", "schema:public table:vals primary_keys:id "
                        + "new-tuple:{id:[unchanged],tag:'t1'}"));
    }

    @Test
    @DisplayName("INSERT 里出现'值未下发'：还原不出这一行，停机")
    void unchangedInInsertFailsStop() {
        assertThrows(PostgresWalExtractor.UnreconstructableValueException.class,
                () -> extract("INSERT", "schema:public table:vals primary_keys:id "
                        + "new-tuple:{id:1,tag:'t1',blob_txt:[unchanged]}"));
    }

    @Test
    @DisplayName("DELETE：有主键时非主键列的未下发值不参与 WHERE，可以放行")
    void deleteWithPkToleratesUnchangedNonKey() throws Exception {
        THLEvent e = extract("DELETE", "schema:public table:vals primary_keys:id "
                + "old-tuple:{id:2,tag:[unchanged],blob_txt:[unchanged]}");
        assertEquals("DELETE", meta(e, "operation"));
        assertTrue(meta(e, "row_data").startsWith("2"), meta(e, "row_data"));
    }

    @Test
    @DisplayName("DELETE：主键本身未下发就定位不了行，停机")
    void deleteWithUnchangedPkFailsStop() {
        assertThrows(PostgresWalExtractor.UnreconstructableValueException.class,
                () -> extract("DELETE", "schema:public table:vals primary_keys:id "
                        + "old-tuple:{id:[unchanged],tag:'t'}"));
    }
}
