package com.migration.traffic.model;

import java.util.Locale;

/**
 * 录制/回放涉及的数据库引擎。
 *
 * <p>为什么要有这个枚举：三种引擎的语句流<b>没有一行共用的捕获代码</b>，
 * 而录制文件是可下载、可上传、可跨机回放的——文件里必须自带"我是哪种引擎录的"，
 * 否则一份 PG 录制被选去回放到 MySQL，只会在目标库上制造一堆半成功的破坏
 * （SQL 方言不可能自动翻译，见 {@code TRAFFIC_REPLAY_PG_ORACLE_DESIGN_20260821.md} §4.4）。
 */
public enum TrafficEngine {

    MYSQL("mysql"),
    POSTGRESQL("postgresql"),
    ORACLE("oracle");

    private final String wireName;

    TrafficEngine(String wireName) {
        this.wireName = wireName;
    }

    /** 落进 manifest / 配置文件的名字，与后端 {@code source.db.type} 同一套口径。 */
    public String wireName() {
        return wireName;
    }

    /**
     * 解析引擎名。
     *
     * <p><b>null / 空一律当 MySQL</b>：v1 录制（`synctask-traffic/1`）根本没有 engine 字段，
     * 而那时只支持 MySQL。老录制是既有用户的资产，必须继续能放。
     */
    public static TrafficEngine parse(String raw) {
        if (raw == null || raw.isBlank()) return MYSQL;
        switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "postgresql": case "postgres": case "pg": case "pgsql":
                return POSTGRESQL;
            case "oracle": case "ora":
                return ORACLE;
            case "mysql": case "tidb": case "mariadb":
                return MYSQL;
            default:
                return MYSQL;
        }
    }

    /** 给人看的名字（报错文案、日志）。与 {@link #wireName()} 分开：后者是协议字段，不该出现在文案里。 */
    public String displayName() {
        switch (this) {
            case POSTGRESQL: return "PostgreSQL";
            case ORACLE: return "Oracle";
            default: return "MySQL";
        }
    }

    /** 该引擎里"库"这个概念在 UI 上的叫法，报错文案用。 */
    public String databaseTerm() {
        switch (this) {
            case POSTGRESQL: return "数据库";
            case ORACLE: return "schema";
            default: return "库";
        }
    }
}
