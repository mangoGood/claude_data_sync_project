package com.migration.extract.schema;

import java.util.Locale;
import java.util.Properties;

/**
 * 表结构时序库的开关。
 *
 * <p>默认 {@link Mode#OFF}——这是一条会改变<b>所有</b>行事件解析口径的改造，
 * 必须先在 {@link Mode#SHADOW} 下用真实流量证明语法覆盖度（两条路都算、只记差异、
 * 仍用旧路径产出），差异率归零后才切 {@link Mode#ON}。靠单测覆盖率是证明不了的。
 */
public class SchemaTimelineConfig {

    public static final String KEY_MODE = "extract.schema.timeline.mode";
    public static final String KEY_FALLBACK = "extract.schema.timeline.fallback";
    public static final String KEY_HISTORY_PATH = "extract.schema.timeline.history.path";

    public enum Mode {
        /** 完全不启用，解析走 information_schema 旧路径（默认）。 */
        OFF,
        /** 两条路都算，产出仍用旧路径，只记录差异——灰度用。 */
        SHADOW,
        /** 解析按时序库版本走，旧路径退为降级兜底。 */
        ON
    }

    /** 时序库给不出该位点版本时怎么办。 */
    public enum Fallback {
        /** 回查源库当前定义（今天的行为）并告警——语法还没跑够时用这个。 */
        RESNAPSHOT,
        /** 停下来上报。确认语法覆盖足够后切到这个，才是真正的"不静默"。 */
        FAIL_STOP
    }

    private final Mode mode;
    private final Fallback fallback;
    private final String historyPath;

    private SchemaTimelineConfig(Mode mode, Fallback fallback, String historyPath) {
        this.mode = mode;
        this.fallback = fallback;
        this.historyPath = historyPath;
    }

    public static SchemaTimelineConfig load(Properties props, String taskId) {
        Mode mode = parseMode(props.getProperty(KEY_MODE, "OFF"));
        Fallback fallback = parseFallback(props.getProperty(KEY_FALLBACK, "RESNAPSHOT"));
        String path = props.getProperty(KEY_HISTORY_PATH,
                "./files/" + (taskId == null ? "unknown" : taskId) + "/schema_history.jsonl");
        return new SchemaTimelineConfig(mode, fallback, path);
    }

    private static Mode parseMode(String raw) {
        try {
            return Mode.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            return Mode.OFF;
        }
    }

    private static Fallback parseFallback(String raw) {
        try {
            return Fallback.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            return Fallback.RESNAPSHOT;
        }
    }

    public Mode getMode() { return mode; }
    public Fallback getFallback() { return fallback; }
    public String getHistoryPath() { return historyPath; }

    public boolean isEnabled() { return mode != Mode.OFF; }
    /** 只有 ON 才让时序库决定产出；SHADOW 下算归算，产出仍走旧路径。 */
    public boolean isAuthoritative() { return mode == Mode.ON; }
}
