package com.migration.extract.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SchemaTimeline} 与 {@link SchemaHistoryFile}：按位点取版本、落盘与恢复。
 *
 * <p>这一层的错误是整条方案里最难发现的：取到<b>相邻</b>的错误版本时，列数往往还对得上，
 * 行事件照样解析成功，只是某几列的类型或 enum 取值表用的是别的时刻的——写进目标库
 * 全是合法值。所以断言集中在边界上：恰好等于 DDL 位点、DDL 位点前一位、跨文件、
 * 表被删之后。
 */
@DisplayName("表结构时序库：按位点取版本")
class SchemaTimelineTest {

    private final CreateTableParser parser = new CreateTableParser();

    private SchemaChange created(String createSql) {
        return SchemaChange.created(parser.parse(createSql, "db"));
    }

    private SchemaChange altered(String createSql) {
        return SchemaChange.altered(parser.parse(createSql, "db"));
    }

    @Nested
    @DisplayName("位点查询")
    class Lookup {

        @Test
        @DisplayName("取最后一个 <= 事件位点的版本")
        void floorSemantics() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            t.append(altered("CREATE TABLE t (a int, b int)"), "mysql-bin.000001", 500);
            t.append(altered("CREATE TABLE t (a int, b int, c int)"), "mysql-bin.000001", 900);

            assertEquals(List.of("a"), t.at("db", "t", "mysql-bin.000001", 100).columnNames());
            assertEquals(List.of("a"), t.at("db", "t", "mysql-bin.000001", 499).columnNames());
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000001", 500).columnNames());
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000001", 899).columnNames());
            assertEquals(List.of("a", "b", "c"), t.at("db", "t", "mysql-bin.000001", 12345).columnNames());
        }

        @Test
        @DisplayName("事件早于第一个版本时返回 null，由调用方按缺失处置（绝不拿最早的版本硬顶）")
        void beforeFirstVersion() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000005", 100);
            assertNull(t.at("db", "t", "mysql-bin.000005", 99));
            assertNull(t.at("db", "t", "mysql-bin.000001", 999999));
        }

        @Test
        @DisplayName("跨 binlog 文件按文件号排序，不能按字符串或只按位置")
        void acrossFiles() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000009", 900);
            t.append(altered("CREATE TABLE t (a int, b int)"), "mysql-bin.000010", 100);

            // 000010:100 的位置数字比 000009:900 小，只按位置比会取到错的版本
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000010", 100).columnNames());
            assertEquals(List.of("a"), t.at("db", "t", "mysql-bin.000009", 999999).columnNames());
        }

        @Test
        @DisplayName("表被 DROP 之后该位点查不到结构（不是返回删除前的旧版本）")
        void droppedTable() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            t.append(SchemaChange.dropped("db", "t"), "mysql-bin.000001", 500);

            assertNotNull(t.at("db", "t", "mysql-bin.000001", 400));
            assertNull(t.at("db", "t", "mysql-bin.000001", 600));
        }

        @Test
        @DisplayName("没有任何版本的表返回 null")
        void unknownTable() {
            assertNull(new SchemaTimeline().at("db", "ghost", "mysql-bin.000001", 100));
        }

        @Test
        @DisplayName("库表名不区分大小写")
        void caseInsensitive() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE T (a int)"), "mysql-bin.000001", 100);
            assertNotNull(t.at("DB", "t", "mysql-bin.000001", 200));
        }
    }

    @Nested
    @DisplayName("追加与幂等")
    class Append {

        @Test
        @DisplayName("同位点重复追加按幂等忽略——重放同一段 binlog 会重新算出同样的版本")
        void duplicateKeyIsIdempotent() {
            SchemaTimeline t = new SchemaTimeline();
            assertTrue(t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100));
            assertFalse(t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100));
            assertEquals(1, t.versionCount());
        }

        @Test
        @DisplayName("currentOf 取最新版本，供 DdlApplier 施加下一条")
        void currentIsLatest() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            t.append(altered("CREATE TABLE t (a int, b int)"), "mysql-bin.000001", 500);
            assertEquals(List.of("a", "b"), t.currentOf("db", "t").columnNames());
            assertNull(t.currentOf("db", "nope"));
        }

        @Test
        @DisplayName("hasTable 决定 capture 基线要不要 seed")
        void hasTable() {
            SchemaTimeline t = new SchemaTimeline();
            assertFalse(t.hasTable("db", "t"));
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            assertTrue(t.hasTable("db", "t"));
        }

        @Test
        @DisplayName("newestKey 是清/留规则的判据")
        void newestKey() {
            SchemaTimeline t = new SchemaTimeline();
            assertEquals(0, t.newestKey());
            t.append(created("CREATE TABLE a (x int)"), "mysql-bin.000001", 100);
            t.append(created("CREATE TABLE b (y int)"), "mysql-bin.000003", 700);
            assertEquals(SchemaTimeline.monotonicKey("mysql-bin.000003", 700), t.newestKey());
        }
    }

    @Nested
    @DisplayName("裁剪")
    class Prune {

        @Test
        @DisplayName("保留 <= 最小已提交位点的最后一个版本——丢了它，从该位点重放就查不到结构")
        void keepsFloorVersion() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            t.append(altered("CREATE TABLE t (a int, b int)"), "mysql-bin.000001", 200);
            t.append(altered("CREATE TABLE t (a int, b int, c int)"), "mysql-bin.000001", 300);

            int removed = t.prune(SchemaTimeline.monotonicKey("mysql-bin.000001", 250));
            assertEquals(1, removed, "只该裁掉 100 那一版");
            assertEquals(2, t.versionCount());
            // 250 处仍然查得到（靠保留下来的 200 那一版）
            assertEquals(List.of("a", "b"), t.at("db", "t", "mysql-bin.000001", 250).columnNames());
        }

        @Test
        @DisplayName("最小已提交位点早于全部版本时一条都不裁")
        void nothingToPrune() {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000005", 100);
            assertEquals(0, t.prune(SchemaTimeline.monotonicKey("mysql-bin.000001", 10)));
            assertEquals(1, t.versionCount());
        }
    }

    @Nested
    @DisplayName("落盘与恢复")
    class Persistence {

        @Test
        @DisplayName("写出再读回来，按位点查到的结构完全一致")
        void roundTrip(@TempDir Path dir) {
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (`id` bigint unsigned NOT NULL, "
                    + "`st` enum('a','b') DEFAULT 'a', PRIMARY KEY (`id`))"), "mysql-bin.000001", 100);
            t.append(altered("CREATE TABLE t (`id` bigint unsigned NOT NULL, "
                    + "`st` enum('a','b','c') DEFAULT 'a', PRIMARY KEY (`id`))"), "mysql-bin.000002", 400);
            t.append(SchemaChange.dropped("db", "gone"), "mysql-bin.000002", 500);

            SchemaHistoryFile file = new SchemaHistoryFile(dir.resolve("schema_history.jsonl").toString());
            for (SchemaTimeline.Entry e : t.allEntries()) {
                file.append(e);
            }

            SchemaTimeline back = new SchemaTimeline();
            assertEquals(3, file.load(back));
            assertEquals(List.of("a", "b"),
                    back.at("db", "t", "mysql-bin.000001", 200).findColumn("st").getEnumValues());
            assertEquals(List.of("a", "b", "c"),
                    back.at("db", "t", "mysql-bin.000002", 400).findColumn("st").getEnumValues());
            assertNull(back.at("db", "gone", "mysql-bin.000002", 600));
        }

        @Test
        @DisplayName("崩溃留下的半条记录：截断在那儿，前面的全部有效")
        void truncatedTailLine(@TempDir Path dir) throws IOException {
            Path p = dir.resolve("schema_history.jsonl");
            SchemaHistoryFile file = new SchemaHistoryFile(p.toString());

            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            t.append(altered("CREATE TABLE t (a int, b int)"), "mysql-bin.000001", 200);
            for (SchemaTimeline.Entry e : t.allEntries()) {
                file.append(e);
            }
            // 追加半条（崩在写入中间）
            Files.writeString(p, "{\"f\":\"mysql-bin.000001\",\"p\":300,\"db\":\"db\",\"tbl\":\"t\",\"ki",
                    StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);

            SchemaTimeline back = new SchemaTimeline();
            assertEquals(2, file.load(back), "半条要被丢掉，前两条必须还在");
            assertEquals(List.of("a", "b"), back.currentOf("db", "t").columnNames());
        }

        @Test
        @DisplayName("文件不存在时装载 0 条，不报错（首次启动）")
        void missingFile(@TempDir Path dir) {
            SchemaHistoryFile file = new SchemaHistoryFile(dir.resolve("nope.jsonl").toString());
            assertEquals(0, file.load(new SchemaTimeline()));
        }

        @Test
        @DisplayName("裁剪后重写：文件内容与时序库一致，且是原子替换")
        void rewriteAfterPrune(@TempDir Path dir) throws IOException {
            Path p = dir.resolve("schema_history.jsonl");
            SchemaHistoryFile file = new SchemaHistoryFile(p.toString());

            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (a int)"), "mysql-bin.000001", 100);
            t.append(altered("CREATE TABLE t (a int, b int)"), "mysql-bin.000001", 200);
            t.append(altered("CREATE TABLE t (a int, b int, c int)"), "mysql-bin.000001", 300);
            for (SchemaTimeline.Entry e : t.allEntries()) {
                file.append(e);
            }

            t.prune(SchemaTimeline.monotonicKey("mysql-bin.000001", 250));
            file.rewrite(t);

            assertEquals(2, Files.readAllLines(p).size());
            assertFalse(Files.exists(p.resolveSibling(p.getFileName() + ".tmp")), "临时文件要清掉");

            SchemaTimeline back = new SchemaTimeline();
            file.load(back);
            assertEquals(2, back.versionCount());
        }

        @Test
        @DisplayName("每条记录必须是单行——多行会让按行恢复整个失效")
        void singleLinePerRecord(@TempDir Path dir) throws IOException {
            SchemaHistoryFile file = new SchemaHistoryFile(dir.resolve("h.jsonl").toString());
            SchemaTimeline t = new SchemaTimeline();
            t.append(created("CREATE TABLE t (`a` int COMMENT '带\n换行的注释')"), "mysql-bin.000001", 100);
            for (SchemaTimeline.Entry e : t.allEntries()) {
                file.append(e);
            }
            assertEquals(1, Files.readAllLines(dir.resolve("h.jsonl")).size());
        }
    }

    @Nested
    @DisplayName("位点折算")
    class MonotonicKey {

        @Test
        @DisplayName("文件号在高位、位置在低位，跨文件严格递增")
        void ordering() {
            assertTrue(SchemaTimeline.monotonicKey("mysql-bin.000002", 4)
                    > SchemaTimeline.monotonicKey("mysql-bin.000001", 999999999L));
            assertTrue(SchemaTimeline.monotonicKey("mysql-bin.000001", 200)
                    > SchemaTimeline.monotonicKey("mysql-bin.000001", 100));
        }

        @Test
        @DisplayName("没有数字后缀的文件名退化成 0（同文件内仍可比），不能抛异常")
        void noNumericSuffix() {
            assertEquals(100, SchemaTimeline.monotonicKey("binlog", 100));
            assertEquals(0, SchemaTimeline.monotonicKey(null, 0));
        }
    }
}
