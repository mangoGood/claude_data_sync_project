package com.migration.extract.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SchemaSelfCheck#compare} 的单元测试——比对规则本身。
 *
 * <p>只测纯函数那一半：{@code readLive} 要连真库，而本仓库的单测一律不连外部库
 * （连库的部分由 {@code test_scripts} 下的判据脚本负责）。比对规则才是最容易写错的地方——
 * 漏掉一项差异，自检就会给出"全部通过"的假绿灯，那比没有自检更糟。
 */
@DisplayName("语法自检：解析结果 vs information_schema 比对")
class SchemaSelfCheckTest {

    private final CreateTableParser parser = new CreateTableParser();
    private final SchemaSelfCheck check = new SchemaSelfCheck();

    private SchemaSelfCheck.LiveSchema live(String[] columns, String[] dataTypes,
                                            String[] columnTypes, String[] generated, String[] pk) {
        SchemaSelfCheck.LiveSchema l = new SchemaSelfCheck.LiveSchema();
        l.columns.addAll(List.of(columns));
        l.dataTypes.addAll(List.of(dataTypes));
        l.columnTypes.addAll(List.of(columnTypes));
        l.generated.addAll(List.of(generated));
        l.primaryKey.addAll(List.of(pk));
        return l;
    }

    @Test
    @DisplayName("解析结果与库里一致时零差异")
    void identical() {
        TableSchema parsed = parser.parse(
                "CREATE TABLE `t` (`id` bigint unsigned NOT NULL AUTO_INCREMENT,"
                        + "`st` enum('a','b') DEFAULT 'a',"
                        + "`total` decimal(20,4) GENERATED ALWAYS AS (`id` * 2) STORED,"
                        + "PRIMARY KEY (`id`))", "db");
        SchemaSelfCheck.LiveSchema l = live(
                new String[]{"id", "st", "total"},
                new String[]{"bigint", "enum", "decimal"},
                new String[]{"bigint unsigned", "enum('a','b')", "decimal(20,4)"},
                new String[]{"total"},
                new String[]{"id"});

        assertEquals(List.of(), check.compare(parsed, l, TypeRenderMode.NO_DISPLAY_WIDTH));
    }

    @Test
    @DisplayName("列名或顺序不一致时直接报出来，不再逐列比（比了也全是噪声）")
    void columnOrderMismatchShortCircuits() {
        TableSchema parsed = parser.parse("CREATE TABLE t (a int, b int)", "db");
        SchemaSelfCheck.LiveSchema l = live(
                new String[]{"b", "a"}, new String[]{"int", "int"},
                new String[]{"int", "int"}, new String[]{}, new String[]{});

        List<String> diffs = check.compare(parsed, l, TypeRenderMode.NO_DISPLAY_WIDTH);
        assertEquals(1, diffs.size());
        assertTrue(diffs.get(0).contains("列名/顺序不一致"));
    }

    @Test
    @DisplayName("COLUMN_TYPE 差一个 unsigned 也要报——下游按它还原符号")
    void unsignedMismatch() {
        TableSchema parsed = parser.parse("CREATE TABLE t (a bigint)", "db");
        SchemaSelfCheck.LiveSchema l = live(
                new String[]{"a"}, new String[]{"bigint"},
                new String[]{"bigint unsigned"}, new String[]{}, new String[]{});

        List<String> diffs = check.compare(parsed, l, TypeRenderMode.NO_DISPLAY_WIDTH);
        assertEquals(1, diffs.size());
        assertTrue(diffs.get(0).contains("COLUMN_TYPE 不一致"), diffs.get(0));
    }

    @Test
    @DisplayName("tinyint(1) 与 tinyint 必须判成不同——布尔列全靠这个 (1)")
    void tinyintOneIsNotTinyint() {
        TableSchema parsed = parser.parse("CREATE TABLE t (flag tinyint)", "db");
        SchemaSelfCheck.LiveSchema l = live(
                new String[]{"flag"}, new String[]{"tinyint"},
                new String[]{"tinyint(1)"}, new String[]{}, new String[]{});

        assertEquals(1, check.compare(parsed, l, TypeRenderMode.NO_DISPLAY_WIDTH).size());
    }

    @Test
    @DisplayName("主键、生成列、enum 取值表各自的差异都要报")
    void otherMismatches() {
        TableSchema parsed = parser.parse(
                "CREATE TABLE t (id int, st enum('a','b'), g int AS (id+1) VIRTUAL)", "db");
        SchemaSelfCheck.LiveSchema l = live(
                new String[]{"id", "st", "g"},
                new String[]{"int", "enum", "int"},
                new String[]{"int", "enum('a','b','c')", "int"},
                new String[]{},                       // 库里没记生成列
                new String[]{"id"});                  // 库里有主键、解析出来没有

        List<String> diffs = check.compare(parsed, l, TypeRenderMode.NO_DISPLAY_WIDTH);
        assertEquals(4, diffs.size(), "COLUMN_TYPE + 生成列 + 主键 + enum 取值表，四项都该报: " + diffs);
        assertTrue(diffs.stream().anyMatch(d -> d.contains("生成列不一致")));
        assertTrue(diffs.stream().anyMatch(d -> d.contains("主键不一致")));
        assertTrue(diffs.stream().anyMatch(d -> d.contains("enum/set 取值表不一致")));
    }

    @Test
    @DisplayName("5.7 口径下按 WITH_DISPLAY_WIDTH 比，才不会把 int(11) 判成差异")
    void displayWidthModeMatters() {
        TableSchema parsed = parser.parse("CREATE TABLE t (a int)", "db");
        SchemaSelfCheck.LiveSchema l = live(
                new String[]{"a"}, new String[]{"int"},
                new String[]{"int(11)"}, new String[]{}, new String[]{});

        assertEquals(List.of(), check.compare(parsed, l, TypeRenderMode.WITH_DISPLAY_WIDTH));
        assertEquals(1, check.compare(parsed, l, TypeRenderMode.NO_DISPLAY_WIDTH).size());
    }

    @Test
    @DisplayName("enum 取值里的空串必须占位——它在 binlog 里是有序号的")
    void emptyEnumValueKeepsItsSlot() {
        // MySQL 的 enum 序号从 1 开始按声明顺序排，enum('','a') 里 1='' 2='a'。
        // 丢掉空串会让整张取值表左移一位，下游把 1 解成 'a'——静默写错。
        assertEquals(List.of("", "a"), SchemaSelfCheck.parseEnumSetValues("enum('','a')"));
        TableSchema parsed = parser.parse("CREATE TABLE t (st enum('','a'))", "db");
        assertEquals(List.of("", "a"), parsed.findColumn("st").getEnumValues());
    }
}
