package com.migration.common.lob;

/**
 * 大字段流式搬运用到的四条 SQL 的方言化生成。
 *
 * <p>抽成接口不是为了将来支持更多库（当前只做 mysql→mysql），而是为了让
 * {@link StreamingBlobWriter} 的单测能在 H2 上跑——H2 的二进制拼接是 {@code ||}
 * 而不是 {@code CONCAT}，把 SQL 写死在写入器里就只能连真库测。
 */
public interface LobSqlDialect {

    /** {@code SELECT SUBSTRING(col, ?, ?) FROM t WHERE pk = ?}（pos 为 1-based 字节位置）。 */
    String substringSql(String qualifiedTable, String column, String pkColumn, boolean textColumn);

    /** {@code SELECT IFNULL(OCTET_LENGTH(col), -1) FROM t WHERE pk = ?}；无该行则无结果集行。 */
    String byteLengthSql(String qualifiedTable, String column, String pkColumn);

    /** {@code UPDATE t SET col = ? WHERE pk = ?}（整值覆盖，也是分块追加的第一块）。 */
    String setSql(String qualifiedTable, String column, String pkColumn);

    /** {@code UPDATE t SET col = CONCAT(col, ?) WHERE pk = ?}（追加一块）。 */
    String appendSql(String qualifiedTable, String column, String pkColumn);

    /** 目标端单条语句能携带的最大字节数；用于选择"单次流式"还是"分块追加"。 */
    String packetLimitSql();
}
