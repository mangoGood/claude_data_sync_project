package com.migration.extract.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SchemaTracker}：基线播种 + DDL 施加 + 落盘恢复串起来的行为。
 *
 * <p>重点在两条不变量上：
 * <ul>
 *   <li><b>已持久化的时序库优先，基线只是兜底种子</b>——capture 重启会重发基线，
 *       那份基线反映"现在"的结构却标在重启位点上，采信它就等于把漂移请回来；</li>
 *   <li><b>取不到结构也要落一个不可用版本</b>——静默跳过的话，抽取端分不清
 *       "这张表没有基线"和"这张表不在同步范围"。</li>
 * </ul>
 */
@DisplayName("抽取端表结构时序库入口")
class SchemaTrackerTest {

    private static final char SEP = '\001';

    private Properties props(Path dir) {
        Properties p = new Properties();
        p.setProperty(SchemaTimelineConfig.KEY_MODE, "ON");
        p.setProperty(SchemaTimelineConfig.KEY_HISTORY_PATH,
                dir.resolve("schema_history.jsonl").toString());
        return p;
    }

    /** 拼一条 capture 写的基线记录：SCHEMA_BASELINE|file|pos|ts|serverId|db|table|error|createSql */
    private String[] baseline(String db, String table, String createSql, String error) {
        return new String[]{
                "SCHEMA_BASELINE", "mysql-bin.000001", "100", "0", "1",
                db, table, error == null ? "" : error,
                createSql == null ? "" : createSql.replace("\n", " ")
        };
    }

    @Nested
    @DisplayName("基线")
    class Baseline {

        @Test
        @DisplayName("首次基线播种成版本 v0")
        void seeds(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t",
                    "CREATE TABLE `t` (`a` int, `b` varchar(8), PRIMARY KEY (`a`))", null),
                    "mysql-bin.000001", 100);

            TableSchema s = t.at("db", "t", "mysql-bin.000001", 200);
            assertNotNull(s);
            assertEquals(List.of("a", "b"), s.columnNames());
            assertEquals(List.of("a"), s.getPrimaryKey());
        }

        @Test
        @DisplayName("已有版本时忽略基线——capture 重启重发的基线是\"现在\"的结构，采信它就是把漂移请回来")
        void ignoredWhenTableAlreadyKnown(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            t.onDdl("ALTER TABLE t ADD COLUMN b int", "db", "mysql-bin.000001", 300);

            // capture 重启，重新打了一份基线（此刻源库已经是两列了），位点标在重启位点
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int, `b` int)", null),
                    "mysql-bin.000002", 50);

            // 200 处必须还是一列——基线不能覆盖已有版本链
            assertEquals(List.of("a"), t.at("db", "t", "mysql-bin.000001", 200).columnNames());
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000002", 100).columnNames());
        }

        @Test
        @DisplayName("取不到结构时落一个不可用版本，而不是什么都不落")
        void unusableBaselineIsRecorded(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", null, "SHOW CREATE TABLE 失败: 权限不足"),
                    "mysql-bin.000001", 100);

            TableSchema s = t.at("db", "t", "mysql-bin.000001", 200);
            assertNotNull(s, "必须有一个版本，抽取端才分得清\"没有基线\"和\"不在同步范围\"");
            assertTrue(s.isUnusable());
            assertTrue(s.getUnusableReason().contains("权限不足"));
        }

        @Test
        @DisplayName("基线语法解析不了同样落不可用版本，并计入解析失败")
        void unparseableBaseline(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int", null),
                    "mysql-bin.000001", 100);

            assertTrue(t.at("db", "t", "mysql-bin.000001", 200).isUnusable());
            assertEquals(1, t.getDdlParseFailed());
        }

        @Test
        @DisplayName("字段不全的记录直接忽略，不能炸掉抽取")
        void malformedRecord(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(new String[]{"SCHEMA_BASELINE", "mysql-bin.000001", "100"},
                    "mysql-bin.000001", 100);
            assertEquals(0, t.getTimelineTableCountForTest());
        }
    }

    @Nested
    @DisplayName("DDL 施加")
    class Ddl {

        @Test
        @DisplayName("每条 DDL 推出一个新版本，按位点各查各的")
        void versionsPerPosition(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            t.onDdl("ALTER TABLE t ADD COLUMN b int", "db", "mysql-bin.000001", 300);
            t.onDdl("ALTER TABLE t ADD COLUMN c int", "db", "mysql-bin.000001", 500);

            assertEquals(List.of("a"), t.at("db", "t", "mysql-bin.000001", 299).columnNames());
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000001", 300).columnNames());
            assertEquals(List.of("a", "b", "c"), t.at("db", "t", "mysql-bin.000001", 9999).columnNames());
        }

        @Test
        @DisplayName("积压期 RENAME COLUMN：按事件位点取到的是改名前的列——这正是整个方案要救的场景")
        void renameColumnDuringBacklog(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t",
                    "CREATE TABLE `t` (`id` int, `old_name` varchar(10))", null),
                    "mysql-bin.000001", 100);
            t.onDdl("ALTER TABLE t RENAME COLUMN old_name TO new_name", "db", "mysql-bin.000001", 800);

            // 改名之前的行事件（位点 500）应当看到 old_name。
            // 查 information_schema 拿到的会是 new_name——列名对不上，值就写到别的列去了
            assertEquals(List.of("id", "old_name"),
                    t.at("db", "t", "mysql-bin.000001", 500).columnNames());
            assertEquals(List.of("id", "new_name"),
                    t.at("db", "t", "mysql-bin.000001", 900).columnNames());
        }

        @Test
        @DisplayName("enum 增删取值：旧事件按旧取值表还原，这是 FULL 元数据也救不了的一类")
        void enumValuesAtPosition(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t",
                    "CREATE TABLE `t` (`st` enum('new','paid'))", null), "mysql-bin.000001", 100);
            t.onDdl("ALTER TABLE t MODIFY COLUMN st enum('new','cancelled','paid')",
                    "db", "mysql-bin.000001", 700);

            assertEquals(List.of("new", "paid"),
                    t.at("db", "t", "mysql-bin.000001", 500).findColumn("st").getEnumValues());
            assertEquals(List.of("new", "cancelled", "paid"),
                    t.at("db", "t", "mysql-bin.000001", 800).findColumn("st").getEnumValues());
        }

        @Test
        @DisplayName("解析不了的 DDL 返回 false 并计数，不抛（由调用方按 fallback 决定停不停）")
        void parseFailureIsReported(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            assertFalse(t.onDdl("ALTER TABLE t ADD WEIRD THING", "db", "mysql-bin.000001", 300));
            assertEquals(1, t.getDdlParseFailed());
        }

        @Test
        @DisplayName("DROP TABLE 之后该位点查不到结构")
        void dropTable(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            t.onDdl("DROP TABLE t", "db", "mysql-bin.000001", 400);

            assertNotNull(t.at("db", "t", "mysql-bin.000001", 300));
            assertNull(t.at("db", "t", "mysql-bin.000001", 500));
        }
    }

    @Nested
    @DisplayName("落盘与重启")
    class Restart {

        @Test
        @DisplayName("重启后从历史文件恢复，按位点查到的结构与重启前一致")
        void survivesRestart(@TempDir Path dir) {
            SchemaTracker first = new SchemaTracker(props(dir), "task-1");
            first.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            first.onDdl("ALTER TABLE t ADD COLUMN b varchar(4)", "db", "mysql-bin.000001", 300);

            SchemaTracker second = new SchemaTracker(props(dir), "task-1");
            assertEquals(List.of("a"), second.at("db", "t", "mysql-bin.000001", 200).columnNames());
            assertEquals(List.of("a", "b"), second.at("db", "t", "mysql-bin.000001", 400).columnNames());

            // 恢复之后这张表已经有版本了，capture 重启重发的基线要被忽略
            second.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int, `b` varchar(4), `c` int)", null),
                    "mysql-bin.000002", 10);
            assertEquals(List.of("a", "b"), second.at("db", "t", "mysql-bin.000001", 400).columnNames());
        }

        @Test
        @DisplayName("同一条 DDL 重放两遍不会写出两个版本")
        void replayIsIdempotent(@TempDir Path dir) {
            SchemaTracker t = new SchemaTracker(props(dir), "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            t.onDdl("ALTER TABLE t ADD COLUMN b int", "db", "mysql-bin.000001", 300);
            t.onDdl("ALTER TABLE t ADD COLUMN b int", "db", "mysql-bin.000001", 300);

            assertEquals(2, t.getTimelineVersionCountForTest());
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000001", 400).columnNames());
        }

        @Test
        @DisplayName("mode=OFF 时完全不工作（默认档，零行为改变）")
        void disabledByDefault(@TempDir Path dir) {
            Properties p = props(dir);
            p.setProperty(SchemaTimelineConfig.KEY_MODE, "OFF");
            SchemaTracker t = new SchemaTracker(p, "task-1");
            t.onBaseline(baseline("db", "t", "CREATE TABLE `t` (`a` int)", null),
                    "mysql-bin.000001", 100);
            assertNull(t.at("db", "t", "mysql-bin.000001", 200));
        }
    }
}
