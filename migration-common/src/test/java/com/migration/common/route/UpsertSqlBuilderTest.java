package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link UpsertSqlBuilder} 单元测试：MySQL / PG 幂等语句生成、全主键表的退化形式，
 * 以及不支持场景返回 null（调用方退回普通 INSERT）。
 */
@DisplayName("UpsertSqlBuilder 幂等装载语句")
class UpsertSqlBuilderTest {

    private static final List<String> MYSQL_COLUMNS =
            Arrays.asList("`id`", "`amount`", "`_src_db`", "`_src_table`");
    private static final List<String> PG_COLUMNS =
            Arrays.asList("\"id\"", "\"amount\"", "\"_src_db\"", "\"_src_table\"");
    private static final List<String> PK = Arrays.asList("id", "_src_db", "_src_table");

    @Test
    @DisplayName("MySQL：ON DUPLICATE KEY UPDATE 只更新非主键列")
    void mysqlUpsert() {
        String sql = UpsertSqlBuilder.build("mysql", "`order_all`", MYSQL_COLUMNS, PK);
        assertEquals("INSERT INTO `order_all` (`id`, `amount`, `_src_db`, `_src_table`) "
                + "VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `amount`=VALUES(`amount`)", sql);
    }

    @Test
    @DisplayName("PG：ON CONFLICT 冲突目标为主键列，DO UPDATE 只更新非主键列")
    void postgresUpsert() {
        String sql = UpsertSqlBuilder.build("postgresql", "\"order_all\"", PG_COLUMNS, PK);
        assertEquals("INSERT INTO \"order_all\" (\"id\", \"amount\", \"_src_db\", \"_src_table\") "
                + "VALUES (?, ?, ?, ?) ON CONFLICT (\"id\", \"_src_db\", \"_src_table\") "
                + "DO UPDATE SET \"amount\"=EXCLUDED.\"amount\"", sql);
    }

    @Test
    @DisplayName("全部列都是主键：MySQL 退化为自赋值，PG 退化为 DO NOTHING")
    void allColumnsArePrimaryKey() {
        List<String> cols = Arrays.asList("`a`", "`b`");
        List<String> pk = Arrays.asList("a", "b");
        assertTrue(UpsertSqlBuilder.build("mysql", "`t`", cols, pk)
                .endsWith("ON DUPLICATE KEY UPDATE `a`=VALUES(`a`), `b`=VALUES(`b`)"));
        assertTrue(UpsertSqlBuilder.build("postgresql", "\"t\"",
                Arrays.asList("\"a\"", "\"b\""), pk).endsWith("ON CONFLICT (\"a\", \"b\") DO NOTHING"));
    }

    @Test
    @DisplayName("主键列名大小写与写入列不一致时仍能匹配（源端大小写口径不统一）")
    void pkMatchIsCaseInsensitive() {
        String sql = UpsertSqlBuilder.build("mysql", "`t`",
                Arrays.asList("`ID`", "`v`"), Collections.singletonList("id"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE `v`=VALUES(`v`)"), sql);
    }

    @Test
    @DisplayName("不支持的场景返回 null：非 MySQL/PG 目标、无主键、主键列不在写入列里")
    void unsupportedReturnsNull() {
        assertNull(UpsertSqlBuilder.build("oracle", "\"T\"", MYSQL_COLUMNS, PK));
        assertNull(UpsertSqlBuilder.build("mysql", "`t`", MYSQL_COLUMNS, Collections.emptyList()));
        // 主键 id 被列过滤掉了，冲突目标不完整
        assertNull(UpsertSqlBuilder.build("mysql", "`t`",
                Arrays.asList("`amount`", "`_src_db`"), PK));
        assertNull(UpsertSqlBuilder.build("mysql", "`t`", Collections.emptyList(), PK));
    }

    @Test
    @DisplayName("supports：只有 MySQL 与 PG 支持幂等装载")
    void supportsMatrix() {
        assertTrue(UpsertSqlBuilder.supports("mysql"));
        assertTrue(UpsertSqlBuilder.supports("postgresql"));
        assertFalse(UpsertSqlBuilder.supports("oracle"));
        assertFalse(UpsertSqlBuilder.supports(null));
    }
}
