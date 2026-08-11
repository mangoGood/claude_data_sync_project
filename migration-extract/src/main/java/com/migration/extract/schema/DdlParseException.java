package com.migration.extract.schema;

/**
 * DDL 解析不了——语法覆盖之外的形态。
 *
 * <p>由调用方按 {@code extract.schema.timeline.fallback} 处置：RESNAPSHOT 则该表退回查
 * {@code information_schema} 的旧路径并告警（E3023 计数），FAIL_STOP 则停止抽取。
 * 无论哪种，都<b>不能</b>当作"这条 DDL 不影响结构"而跳过——漏施加一条 ALTER，
 * 该表之后的全部版本都是错的。
 */
public class DdlParseException extends RuntimeException {

    private final String sql;

    public DdlParseException(String message, String sql) {
        super(message + " | sql=" + truncate(sql));
        this.sql = sql;
    }

    public DdlParseException(String message, String sql, Throwable cause) {
        super(message + " | sql=" + truncate(sql), cause);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    private static String truncate(String sql) {
        if (sql == null) {
            return "null";
        }
        String oneLine = sql.replaceAll("\\s+", " ").trim();
        return oneLine.length() > 300 ? oneLine.substring(0, 300) + "..." : oneLine;
    }
}
