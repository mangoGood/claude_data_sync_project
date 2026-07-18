package com.migration.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ColumnProcessingConfig} 单元测试：属性解析、列名映射（小写回退）、
 * 过滤条件的 SQL 生成与 Java 行判定（数值/日期时间/NULL）、附加列建表定义。
 */
@DisplayName("ColumnProcessingConfig 列处理配置")
class ColumnProcessingConfigTest {

    private ColumnProcessingConfig load() {
        Properties props = new Properties();
        props.setProperty("column.filter.db1.t1", "c1|<|1;c2|>=|2026-07-01 00:00:00");
        props.setProperty("column.mapping.db1.t1", "old_a:new_a,old_b:new_b");
        props.setProperty("column.extra.db1.t1",
                "create_time:CREATE_TIME,update_time:UPDATE_TIME,src_flag:CUSTOM:20260714test1");
        return ColumnProcessingConfig.loadFromProperties(props);
    }

    @Test
    @DisplayName("空配置：isEmpty 且各查询返回空")
    void emptyConfig() {
        ColumnProcessingConfig config = ColumnProcessingConfig.loadFromProperties(new Properties());
        assertTrue(config.isEmpty());
        assertTrue(config.getFilters("db1", "t1").isEmpty());
        assertTrue(config.getColumnMapping("db1", "t1").isEmpty());
        assertTrue(config.getExtraColumns("db1", "t1").isEmpty());
        assertFalse(config.hasProcessing("db1", "t1"));
        assertNull(config.buildKeepClause("db1", "t1", s -> "`" + s + "`"));
    }

    @Test
    @DisplayName("解析：过滤/映射/附加列均按 db.table 命中，小写回退生效")
    void parseAndLookup() {
        ColumnProcessingConfig config = load();
        assertFalse(config.isEmpty());
        assertTrue(config.hasProcessing("db1", "t1"));
        assertEquals(2, config.getFilters("db1", "t1").size());
        assertEquals("new_a", config.mapColumn("db1", "t1", "old_a"));
        assertEquals("keep", config.mapColumn("db1", "t1", "keep"));
        // 小写回退：源库不区分大小写时语句里的大小写可能与配置不一致
        assertEquals("new_a", config.mapColumn("DB1", "T1", "old_a"));
        assertEquals(3, config.getExtraColumns("db1", "t1").size());
        // 未配置的表不受影响
        assertFalse(config.hasProcessing("db1", "t2"));
    }

    @Test
    @DisplayName("keep 子句：排除命中条件的行，NULL 行保留")
    void keepClause() {
        ColumnProcessingConfig config = load();
        String clause = config.buildKeepClause("db1", "t1", s -> "`" + s + "`");
        assertEquals("(NOT (`c1` < 1) OR `c1` IS NULL) AND "
                + "(NOT (`c2` >= '2026-07-01 00:00:00') OR `c2` IS NULL)", clause);
    }

    @Test
    @DisplayName("行判定：数值与日期时间比较，NULL/找不到列不排除")
    void rowExcluded() {
        ColumnProcessingConfig config = load();
        String[] cols = {"id", "c1", "c2"};
        // c1 = 0 < 1 → 排除
        assertTrue(config.rowExcluded("db1", "t1", cols, Arrays.asList(9, 0, null)));
        // c1 = 5 不命中，c2 早于阈值不命中 → 保留
        assertFalse(config.rowExcluded("db1", "t1", cols,
                Arrays.asList(9, 5, java.sql.Timestamp.valueOf("2026-06-30 23:59:59"))));
        // c2 >= 阈值 → 排除（BigDecimal 与 Timestamp 混合类型）
        assertTrue(config.rowExcluded("db1", "t1", cols,
                Arrays.asList(9, new java.math.BigDecimal("5.5"), java.sql.Timestamp.valueOf("2026-07-02 00:00:00"))));
        // 过滤列为 NULL → 保留
        assertFalse(config.rowExcluded("db1", "t1", cols, Arrays.asList(9, null, null)));
        // 行里没有过滤列 → 保留
        assertFalse(config.rowExcluded("db1", "t1", new String[]{"id"}, List.of(9)));
    }

    @Test
    @DisplayName("附加列建表定义：时间列走 DEFAULT/ON UPDATE，自定义列为常量 值@库@表")
    void extraColumnDefs() {
        ColumnProcessingConfig config = load();
        List<ColumnProcessingConfig.ExtraColumn> extras = config.getExtraColumns("db1", "t1");
        assertEquals("`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '首次同步到目标库的时间'",
                extras.get(0).toMysqlColumnDef("db1", "t1"));
        assertTrue(extras.get(1).toMysqlColumnDef("db1", "t1")
                .contains("DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"));
        assertTrue(extras.get(2).toMysqlColumnDef("db1", "t1")
                .contains("DEFAULT '20260714test1@db1@t1'"));
    }

    @Test
    @DisplayName("bit 列比较：Boolean/byte[]/BitSet 折算为无符号整数值")
    void bitComparison() {
        Properties props = new Properties();
        props.setProperty("column.filter.db1.t1", "flag|=|0;mask|>=|5");
        ColumnProcessingConfig config = ColumnProcessingConfig.loadFromProperties(props);
        String[] cols = {"id", "flag", "mask"};
        // bit(1) Boolean：false=0 命中 flag=0 → 排除
        assertTrue(config.rowExcluded("db1", "t1", cols, Arrays.asList(1, Boolean.FALSE, null)));
        assertFalse(config.rowExcluded("db1", "t1", cols, Arrays.asList(1, Boolean.TRUE, null)));
        // bit(n) byte[]（大端）：{0x00,0x06}=6 >= 5 → 排除；{0x04}=4 保留
        assertTrue(config.rowExcluded("db1", "t1", cols,
                Arrays.asList(1, true, new byte[]{0x00, 0x06})));
        assertFalse(config.rowExcluded("db1", "t1", cols,
                Arrays.asList(1, true, new byte[]{0x04})));
        // BitSet（binlog connector 的 BIT 表现）：bit0+bit2 = 5 >= 5 → 排除
        java.util.BitSet bs = new java.util.BitSet();
        bs.set(0); bs.set(2);
        assertTrue(config.rowExcluded("db1", "t1", cols, Arrays.asList(1, true, bs)));
    }

    @Test
    @DisplayName("非法条目跳过：错误 op/类型不入库")
    void invalidEntriesSkipped() {
        Properties props = new Properties();
        props.setProperty("column.filter.db1.t1", "c1|LIKE|x;c2|<|3");
        props.setProperty("column.extra.db1.t1", "x:UNKNOWN_KIND,y:CUSTOM");
        ColumnProcessingConfig config = ColumnProcessingConfig.loadFromProperties(props);
        assertEquals(1, config.getFilters("db1", "t1").size());
        assertEquals("c2", config.getFilters("db1", "t1").get(0).column);
        assertTrue(config.getExtraColumns("db1", "t1").isEmpty());
    }
}
