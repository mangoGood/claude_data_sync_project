package com.migration.common.bulk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批量装载配置的解析与<b>降级</b>语义。
 *
 * <p>这里守的核心是：档位选错不能让任务失败。COPY 只有 PostgreSQL 目标能用、
 * DIRECT_PATH 只有 Oracle 目标能用，用户（或迁移过来的旧配置）把它们配到别的目标端时，
 * 必须安静地退回 BATCH——装载档位是性能选项，没有理由因为它把一条同步链路打死。
 */
@DisplayName("批量装载配置：解析与降级")
class BulkLoadOptionsTest {

    private BulkLoadOptions parse(String... kv) {
        Properties props = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            props.setProperty(kv[i], kv[i + 1]);
        }
        return BulkLoadOptions.from(props);
    }

    @Test
    @DisplayName("默认：启用 + AUTO，各目标端都落到 BATCH（升级前的行为）")
    void defaultsToAutoBatch() {
        BulkLoadOptions o = parse();
        assertTrue(o.isEnabled());
        assertEquals(BulkLoadOptions.Mode.AUTO, o.getMode());
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("mysql"));
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("postgresql"));
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("oracle"));
    }

    @Test
    @DisplayName("COPY 只在 PostgreSQL 目标生效，其它目标降级 BATCH")
    void copyOnlyForPostgres() {
        BulkLoadOptions o = parse(BulkLoadOptions.KEY_MODE, "COPY");
        assertEquals(BulkLoadOptions.Mode.COPY, o.modeFor("postgresql"));
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("mysql"));
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("oracle"));
    }

    @Test
    @DisplayName("DIRECT_PATH 只在 Oracle 目标生效，其它目标降级 BATCH")
    void directPathOnlyForOracle() {
        BulkLoadOptions o = parse(BulkLoadOptions.KEY_MODE, "direct_path");
        assertEquals(BulkLoadOptions.Mode.DIRECT_PATH, o.modeFor("oracle"),
                "档位取值大小写不敏感");
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("postgresql"));
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("mysql"));
    }

    @Test
    @DisplayName("未知档位按 AUTO 处理，不抛异常")
    void unknownModeFallsBackToAuto() {
        BulkLoadOptions o = parse(BulkLoadOptions.KEY_MODE, "LOAD_DATA_INFILE");
        assertEquals(BulkLoadOptions.Mode.AUTO, o.getMode());
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("mysql"));
    }

    @Test
    @DisplayName("关闭批量装载 → 每批 1 行（逐条写）且档位恒为 BATCH")
    void disabledCollapsesToSingleRow() {
        BulkLoadOptions o = parse(BulkLoadOptions.KEY_ENABLED, "false",
                BulkLoadOptions.KEY_MODE, "COPY", BulkLoadOptions.KEY_ROWS, "5000");
        assertFalse(o.isEnabled());
        assertEquals(1, o.rows(1000), "关掉批量装载后不能还按 5000 行攒批");
        assertEquals(BulkLoadOptions.Mode.BATCH, o.modeFor("postgresql"));
    }

    @Test
    @DisplayName("行/字节阈值：未配置用引擎默认，配置了用配置值")
    void thresholdsFallBackToEngineDefaults() {
        BulkLoadOptions def = parse();
        assertEquals(512, def.rows(512), "未配置行阈值时用调用方给的引擎默认");
        assertEquals(8L * 1024 * 1024, def.bytes(8L * 1024 * 1024));

        BulkLoadOptions explicit = parse(BulkLoadOptions.KEY_ROWS, "2000",
                BulkLoadOptions.KEY_BYTES, "1048576");
        assertEquals(2000, explicit.rows(512));
        assertEquals(1048576L, explicit.bytes(8L * 1024 * 1024));
    }

    @Test
    @DisplayName("阈值写成非数字 → 回落引擎默认，不让任务起不来")
    void malformedThresholdFallsBack() {
        BulkLoadOptions o = parse(BulkLoadOptions.KEY_ROWS, "一千");
        assertEquals(512, o.rows(512));
    }
}
