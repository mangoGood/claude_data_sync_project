package com.migration.common.bulk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 全量批量装载通道的统一配置。
 *
 * <p>此前批量装载只存在于 SQL 全量一条链路（{@code migration-full}），Mongo/ES/Redis 各自
 * 硬编码批大小、订阅链路另有一套 Kafka 攒批参数——同一个"批量装载"概念散落成四份互不相干的常量。
 * 这个类把配置口径统一成一组键，所有引擎共用：
 *
 * <ul>
 *   <li>{@code migration.full.bulk.enabled}（默认 true）：关掉即退回各引擎的逐条写入路径；</li>
 *   <li>{@code migration.full.bulk.mode}：{@link Mode}，默认 {@code AUTO}；</li>
 *   <li>{@code migration.full.bulk.rows}：单批行数上限，0 = 用引擎默认；</li>
 *   <li>{@code migration.full.bulk.bytes}：单批字节上限，0 = 用引擎默认。</li>
 * </ul>
 *
 * <p><b>为什么要字节阈值</b>：只按行数攒批的通道在宽行上会撞协议上限——Mongo 的 48MB 消息上限、
 * ES 的 {@code http.max_content_length}、MySQL 的 {@code max_allowed_packet}。这三处此前都只有
 * 行数阈值，宽表全量会在"某个批恰好超限"时才炸，且报错与批大小无关，极难定位。行/字节双阈值
 * 是各家批量装载通道的标配。
 */
public final class BulkLoadOptions {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoadOptions.class);

    public static final String KEY_ENABLED = "migration.full.bulk.enabled";
    public static final String KEY_MODE = "migration.full.bulk.mode";
    public static final String KEY_ROWS = "migration.full.bulk.rows";
    public static final String KEY_BYTES = "migration.full.bulk.bytes";

    /**
     * 装载通道档位。
     *
     * <ul>
     *   <li>{@code AUTO}：各目标端选"零协议风险"的那条——JDBC 走驱动语句重写（{@code BATCH}），
     *       Mongo/ES/Redis 走各自的原生批量 API。这是默认档，行为与升级前一致。</li>
     *   <li>{@code BATCH}：显式指定驱动语句重写。</li>
     *   <li>{@code COPY}：PostgreSQL 目标专用，走 {@code COPY ... WITH (FORMAT binary)}。</li>
     *   <li>{@code DIRECT_PATH}：Oracle 目标专用，走 {@code INSERT /*+ APPEND_VALUES *&#47;}
     *       直接路径装载。</li>
     * </ul>
     *
     * <p>{@code COPY} / {@code DIRECT_PATH} 落在不匹配的目标端时<b>降级为 {@code BATCH}</b> 并告警，
     * 不让一个装载档位的选择把任务卡死。
     */
    public enum Mode { AUTO, BATCH, COPY, DIRECT_PATH }

    private final boolean enabled;
    private final Mode mode;
    private final int rows;
    private final long bytes;

    private BulkLoadOptions(boolean enabled, Mode mode, int rows, long bytes) {
        this.enabled = enabled;
        this.mode = mode;
        this.rows = rows;
        this.bytes = bytes;
    }

    public static BulkLoadOptions from(Properties props) {
        boolean enabled = Boolean.parseBoolean(get(props, KEY_ENABLED, "true"));
        Mode mode;
        String raw = get(props, KEY_MODE, "AUTO").trim().toUpperCase();
        try {
            mode = Mode.valueOf(raw);
        } catch (IllegalArgumentException e) {
            logger.warn("未知的批量装载模式 {}，按 AUTO 处理", raw);
            mode = Mode.AUTO;
        }
        return new BulkLoadOptions(enabled, mode, parseInt(props, KEY_ROWS), parseLong(props, KEY_BYTES));
    }

    /** 单元测试与"引擎自带默认"场景用：直接构造。 */
    public static BulkLoadOptions of(boolean enabled, Mode mode, int rows, long bytes) {
        return new BulkLoadOptions(enabled, mode, Math.max(0, rows), Math.max(0L, bytes));
    }

    /**
     * 派生一份指定档位的配置（行/字节阈值与开关不变）。
     *
     * <p>用于幂等装载（汇聚）：{@code COPY} 与 {@code DIRECT_PATH} 都没有 upsert 语义，
     * 目标表被多个源写时必须退回 {@code BATCH}，否则重复行会直接冲突失败。
     */
    public BulkLoadOptions withMode(Mode newMode) {
        return newMode == null || newMode == this.mode
                ? this : new BulkLoadOptions(enabled, newMode, rows, bytes);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** 配置里写的原始档位（未按目标端裁决）。 */
    public Mode getMode() {
        return mode;
    }

    /** 单批行数：未配置（0）时用引擎默认。装载关闭时统一收敛到 1 行/批 = 逐条写。 */
    public int rows(int engineDefault) {
        if (!enabled) {
            return 1;
        }
        return rows > 0 ? rows : Math.max(1, engineDefault);
    }

    /** 单批字节上限：未配置（0）时用引擎默认；返回值恒 &gt; 0。 */
    public long bytes(long engineDefault) {
        return bytes > 0 ? bytes : Math.max(1L, engineDefault);
    }

    /**
     * 按目标端裁决实际档位。{@code AUTO} → {@code BATCH}；把 {@code COPY}/{@code DIRECT_PATH}
     * 用在不支持的目标端上时降级为 {@code BATCH} 并告警。
     */
    public Mode modeFor(String targetDbType) {
        String type = targetDbType == null ? "" : targetDbType.trim().toLowerCase();
        if (!enabled) {
            return Mode.BATCH;
        }
        switch (mode) {
            case COPY:
                if (!"postgresql".equals(type)) {
                    logger.warn("批量装载模式 COPY 仅支持 PostgreSQL 目标（当前 {}），降级为 BATCH", targetDbType);
                    return Mode.BATCH;
                }
                return Mode.COPY;
            case DIRECT_PATH:
                if (!"oracle".equals(type)) {
                    logger.warn("批量装载模式 DIRECT_PATH 仅支持 Oracle 目标（当前 {}），降级为 BATCH", targetDbType);
                    return Mode.BATCH;
                }
                return Mode.DIRECT_PATH;
            case BATCH:
            case AUTO:
            default:
                return Mode.BATCH;
        }
    }

    @Override
    public String toString() {
        return "BulkLoadOptions{enabled=" + enabled + ", mode=" + mode
                + ", rows=" + (rows > 0 ? rows : "engine-default")
                + ", bytes=" + (bytes > 0 ? bytes : "engine-default") + "}";
    }

    private static String get(Properties props, String key, String def) {
        if (props == null) {
            return def;
        }
        String v = props.getProperty(key);
        return v == null || v.trim().isEmpty() ? def : v.trim();
    }

    private static int parseInt(Properties props, String key) {
        try {
            return Integer.parseInt(get(props, key, "0"));
        } catch (NumberFormatException e) {
            logger.warn("{} 不是合法整数，按引擎默认处理", key);
            return 0;
        }
    }

    private static long parseLong(Properties props, String key) {
        try {
            return Long.parseLong(get(props, key, "0"));
        } catch (NumberFormatException e) {
            logger.warn("{} 不是合法整数，按引擎默认处理", key);
            return 0L;
        }
    }
}
