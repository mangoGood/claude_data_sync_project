package com.migration.common.lob;

/**
 * MySQL 的大字段搬运 SQL。
 *
 * <p>{@code SUBSTRING} 对二进制串（BLOB）按<b>字节</b>计位，对字符串（TEXT）按<b>字符</b>计位。
 * 我们的偏移量一律来自 {@code OCTET_LENGTH}，是字节口径——所以 TEXT 列必须先
 * {@code CAST(col AS BINARY)} 强制成二进制再切，否则多字节字符集下会切错位、
 * 而且错得很隐蔽（长度对得上、内容乱了）。BLOB 列不加 CAST：
 * 1GB 值上多套一层 CAST 会在服务端多生成一个 1GB 中间结果，白白撞 max_allowed_packet。
 */
public final class MySqlLobDialect implements LobSqlDialect {

    public static final MySqlLobDialect INSTANCE = new MySqlLobDialect();

    @Override
    public String substringSql(String qualifiedTable, String column, String pkColumn, boolean textColumn) {
        String expr = textColumn ? "CAST(" + q(column) + " AS BINARY)" : q(column);
        return "SELECT SUBSTRING(" + expr + ", ?, ?) FROM " + qualifiedTable
                + " WHERE " + q(pkColumn) + " = ?";
    }

    @Override
    public String byteLengthSql(String qualifiedTable, String column, String pkColumn) {
        return "SELECT IFNULL(OCTET_LENGTH(" + q(column) + "), -1) FROM " + qualifiedTable
                + " WHERE " + q(pkColumn) + " = ?";
    }

    @Override
    public String setSql(String qualifiedTable, String column, String pkColumn) {
        return "UPDATE " + qualifiedTable + " SET " + q(column) + " = ? WHERE " + q(pkColumn) + " = ?";
    }

    @Override
    public String appendSql(String qualifiedTable, String column, String pkColumn) {
        // IFNULL 是必须的：CONCAT(NULL, x) 结果是 NULL，第一块落在 NULL 上会把整个值抹掉
        return "UPDATE " + qualifiedTable + " SET " + q(column) + " = CONCAT(IFNULL(" + q(column)
                + ", ''), ?) WHERE " + q(pkColumn) + " = ?";
    }

    @Override
    public String packetLimitSql() {
        return "SELECT @@max_allowed_packet";
    }

    private static String q(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
