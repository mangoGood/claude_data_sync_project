package com.migration.extract;

import com.migration.extract.schema.SchemaTimelineConfig;
import com.migration.extract.schema.SchemaTracker;
import com.migration.thl.THLEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 阶段 4 的切换点：{@code TABLE_MAP} 的六项元数据是从表结构时序库来的，还是从
 * {@code information_schema} 来的。
 *
 * <p>这是整条方案唯一真正改变同步行为的地方，所以断言全部落在<b>下游实际收到的元数据</b>上
 * （THL 的 {@code column_names} / {@code mysql_column_types} / {@code enum_set_values} …），
 * 而不是内部状态——下游按这几项还原每一行的值，它们错了就是静默数据损坏。
 *
 * <p>源库连接接的是 H2 内存库（{@code INFORMATION_SCHEMA.COLUMNS} 存在但查不到这些表），
 * 于是"旧路径"必然产出空列清单。这正好让两条路的产出泾渭分明：<b>非空 = 走了时序库</b>。
 */
@DisplayName("抽取端切换到按位点取版本")
class SchemaTimelineSwitchTest {

    private MySQLBinlogExtractor extractor;
    private Method doExtract;
    private SchemaTracker tracker;

    private void setUp(Path dir, String mode, String fallback) throws Exception {
        extractor = new MySQLBinlogExtractor();

        Properties props = new Properties();
        props.setProperty("extract.input.dir", "binlog_output");
        props.setProperty("extract.output.dir", "thl_output");
        props.setProperty(SchemaTimelineConfig.KEY_MODE, mode);
        props.setProperty(SchemaTimelineConfig.KEY_FALLBACK, fallback);
        props.setProperty(SchemaTimelineConfig.KEY_HISTORY_PATH,
                dir.resolve("schema_history.jsonl").toString());

        java.lang.reflect.Field propsField =
                extractor.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(extractor, props);

        Connection h2 = DriverManager.getConnection(
                "jdbc:h2:mem:timeline-switch-" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1",
                "sa", "");
        java.lang.reflect.Field connField =
                extractor.getClass().getDeclaredField("sourceConnection");
        connField.setAccessible(true);
        connField.set(extractor, h2);

        tracker = new SchemaTracker(props, "task-switch");
        java.lang.reflect.Field trackerField =
                extractor.getClass().getDeclaredField("schemaTracker");
        trackerField.setAccessible(true);
        trackerField.set(extractor, tracker);

        doExtract = extractor.getClass().getDeclaredMethod("doExtract", byte[].class);
        doExtract.setAccessible(true);
    }

    private void seed(String db, String table, String createSql, String file, long pos) {
        tracker.onBaseline(new String[]{
                "SCHEMA_BASELINE", file, String.valueOf(pos), "0", "1",
                db, table, "", createSql.replace("\n", " ")}, file, pos);
    }

    private THLEvent extract(String eventStr) throws Exception {
        try {
            return (THLEvent) doExtract.invoke(extractor, eventStr.getBytes("UTF-8"));
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    private String tableMap(String file, long pos, String db, String table, String extra) {
        return "TABLE_MAP\001" + file + "\001" + pos + "\001100\0011\001"
                + "TableMapEventData{tableId=7, database='" + db + "', table='" + table + "'"
                + (extra == null ? "" : ", " + extra) + "}";
    }

    /** 带列名的 TABLE_MAP（binlog_row_metadata=FULL 才有这一段）。 */
    private String tableMapWithColumns(String file, long pos, String db, String table, String columns) {
        return tableMap(file, pos, db, table,
                "columnTypes=3, 15, columnMetadata=0, eventMetadata=TableMapEventMetadata{"
                        + "columnNames=" + columns + ", setStrValues=null}");
    }

    @Nested
    @DisplayName("mode=ON：时序库说了算")
    class Authoritative {

        @Test
        @DisplayName("六项元数据来自事件位点上的版本，而不是源库当前定义")
        void metadataComesFromTimeline(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`id` bigint unsigned NOT NULL, "
                    + "`st` enum('new','paid'), `total` int GENERATED ALWAYS AS (`id`*2) STORED, "
                    + "PRIMARY KEY (`id`))", "mysql-bin.000001", 100);

            extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            THLEvent row = extract("WRITE_ROWS\001mysql-bin.000001\001600\001100\0011\001"
                    + "WriteRowsEventData{tableId=7, includedColumns={0, 1, 2}, rows=[[1, 1, 2]]}");

            assertNotNull(row);
            assertEquals("id,st,total", row.getMetadata().get("column_names"));
            assertEquals("bigint,enum,int", row.getMetadata().get("mysql_column_types"));
            assertEquals("bigint unsigned,enum('new','paid'),int",
                    row.getMetadata().get("mysql_column_full_types"));
            assertEquals("id", row.getMetadata().get("primary_keys"));
            assertEquals("st=new,paid", row.getMetadata().get("enum_set_values"));
            assertEquals("total", row.getMetadata().get("generated_columns"));
        }

        @Test
        @DisplayName("积压期改过表结构：早于 DDL 的事件拿到的是改之前的列布局")
        void backlogEventsSeeOldLayout(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`id` int, `old_name` varchar(10))",
                    "mysql-bin.000001", 100);
            tracker.onDdl("ALTER TABLE t RENAME COLUMN old_name TO new_name", "db",
                    "mysql-bin.000001", 800);

            // 位点 500 的事件在 DDL 之前——查 information_schema 会拿到 new_name，
            // 值就写到别的列去了；时序库必须给出 old_name
            extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            THLEvent before = extract("WRITE_ROWS\001mysql-bin.000001\001510\001100\0011\001"
                    + "WriteRowsEventData{tableId=7, includedColumns={0, 1}, rows=[[1, x]]}");
            assertEquals("id,old_name", before.getMetadata().get("column_names"));

            extract(tableMap("mysql-bin.000001", 900, "db", "t", null));
            THLEvent after = extract("WRITE_ROWS\001mysql-bin.000001\001910\001100\0011\001"
                    + "WriteRowsEventData{tableId=7, includedColumns={0, 1}, rows=[[1, x]]}");
            assertEquals("id,new_name", after.getMetadata().get("column_names"));
        }

        @Test
        @DisplayName("命中计数递增")
        void hitMetric(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`a` int)", "mysql-bin.000001", 100);
            extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            assertEquals(1L, extractor.schemaTimelineMetrics().get("timeline_hit"));
        }
    }

    @Nested
    @DisplayName("交叉校验：事件自带的列名是权威证据")
    class CrossCheck {

        @Test
        @DisplayName("时序库算出的列布局与事件列名矛盾 → fail-stop，绝不硬解")
        void mismatchFailsStop(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            // 时序库以为是 (a, b)，而事件自带的列名是 (a, c)——说明有一条 DDL 漏施加了
            seed("db", "t", "CREATE TABLE `t` (`a` int, `b` int)", "mysql-bin.000001", 100);

            MySQLBinlogExtractor.SchemaVersionMismatchException e = assertThrows(
                    MySQLBinlogExtractor.SchemaVersionMismatchException.class,
                    () -> extract(tableMapWithColumns("mysql-bin.000001", 500, "db", "t", "a, c")));
            assertTrue(e.getMessage().contains("[a, b]"));
            assertTrue(e.getMessage().contains("[a, c]"));
            assertEquals(1L, extractor.schemaTimelineMetrics().get("timeline_cross_check_failed"));
        }

        @Test
        @DisplayName("列名一致时正常放行")
        void matchPasses(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`a` int, `b` int)", "mysql-bin.000001", 100);
            assertNotNull(extract(tableMapWithColumns("mysql-bin.000001", 500, "db", "t", "a, b")));
            assertEquals(0L, extractor.schemaTimelineMetrics().get("timeline_cross_check_failed"));
        }

        @Test
        @DisplayName("MINIMAL（事件不带列名）时跳过校验，时序库照常工作")
        void noEventColumnsSkipsCheck(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`a` int, `b` int)", "mysql-bin.000001", 100);
            extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            THLEvent row = extract("WRITE_ROWS\001mysql-bin.000001\001600\001100\0011\001"
                    + "WriteRowsEventData{tableId=7, includedColumns={0, 1}, rows=[[1, 2]]}");
            assertEquals("a,b", row.getMetadata().get("column_names"));
        }
    }

    @Nested
    @DisplayName("时序库给不出版本时的降级")
    class Fallback {

        @Test
        @DisplayName("fallback=FAIL_STOP：停下来报 E3022，不退回\"用现在的结构解释过去的事件\"")
        void failStop(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "FAIL_STOP");
            MySQLBinlogExtractor.SchemaTimelineMissingException e = assertThrows(
                    MySQLBinlogExtractor.SchemaTimelineMissingException.class,
                    () -> extract(tableMap("mysql-bin.000001", 500, "db", "unknown", null)));
            assertTrue(e.getMessage().contains("db.unknown"));
        }

        @Test
        @DisplayName("fallback=RESNAPSHOT：退回旧路径并计数（H2 里查不到表，产出空列清单）")
        void resnapshot(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            THLEvent e = extract(tableMap("mysql-bin.000001", 500, "db", "unknown", null));
            assertNotNull(e);
            assertEquals(1L, extractor.schemaTimelineMetrics().get("timeline_miss"));
        }

        @Test
        @DisplayName("基线不可用的表按\"没有版本\"处置，不拿残缺结构硬顶")
        void unusableBaselineCountsAsMiss(@TempDir Path dir) throws Exception {
            setUp(dir, "ON", "RESNAPSHOT");
            tracker.onBaseline(new String[]{"SCHEMA_BASELINE", "mysql-bin.000001", "100", "0", "1",
                    "db", "t", "权限不足", ""}, "mysql-bin.000001", 100);

            extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            assertEquals(1L, extractor.schemaTimelineMetrics().get("timeline_miss"));
        }
    }

    @Nested
    @DisplayName("mode=SHADOW：两条路都算，产出仍用旧路径")
    class Shadow {

        @Test
        @DisplayName("产出走旧路径，差异只记不改")
        void producesLegacyAndRecordsDiff(@TempDir Path dir) throws Exception {
            setUp(dir, "SHADOW", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`a` int, `b` int)", "mysql-bin.000001", 100);

            extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            THLEvent row = extract("WRITE_ROWS\001mysql-bin.000001\001600\001100\0011\001"
                    + "WriteRowsEventData{tableId=7, includedColumns={0, 1}, rows=[[1, 2]]}");

            // H2 里没有这张表，旧路径产出空列清单——SHADOW 下就该是它，证明没走时序库
            assertEquals("", row.getMetadata().get("column_names"));
            assertEquals(1L, extractor.schemaTimelineMetrics().get("timeline_shadow_diff"));
        }

        @Test
        @DisplayName("SHADOW 下交叉校验照做——灰度期正是要看它会不会报")
        void crossCheckStillRuns(@TempDir Path dir) throws Exception {
            setUp(dir, "SHADOW", "RESNAPSHOT");
            seed("db", "t", "CREATE TABLE `t` (`a` int, `b` int)", "mysql-bin.000001", 100);
            assertThrows(MySQLBinlogExtractor.SchemaVersionMismatchException.class,
                    () -> extract(tableMapWithColumns("mysql-bin.000001", 500, "db", "t", "a, c")));
        }
    }

    @Nested
    @DisplayName("mode=OFF：与改造前逐字一致")
    class Off {

        @Test
        @DisplayName("不装载时序库，也不产生任何指标")
        void untouched(@TempDir Path dir) throws Exception {
            setUp(dir, "OFF", "RESNAPSHOT");
            // OFF 档下 doInitialize 不会建 tracker；这里手工注入的 tracker 也被 isEnabled() 挡住
            THLEvent e = extract(tableMap("mysql-bin.000001", 500, "db", "t", null));
            assertNotNull(e);
            assertEquals(0L, extractor.schemaTimelineMetrics().get("timeline_hit"));
            assertEquals(0L, extractor.schemaTimelineMetrics().get("timeline_miss"));
        }
    }
}
