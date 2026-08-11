package com.migration.common.lob;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 用 {@code SELECT SUBSTRING(col, pos, len) FROM t WHERE pk = ?} 按区间拉取源端大字段。
 *
 * <p>一个 PreparedStatement 复用到底，每次只把一个 chunk 拉进堆。
 */
public final class JdbcLobChunkSource implements LobChunkSource {

    private final PreparedStatement stmt;
    private final Object pkValue;
    private final long length;
    private final String desc;

    public JdbcLobChunkSource(Connection conn, LobSqlDialect dialect, String qualifiedTable,
                              String column, String pkColumn, Object pkValue,
                              boolean textColumn, long length) throws SQLException {
        this.stmt = conn.prepareStatement(
                dialect.substringSql(qualifiedTable, column, pkColumn, textColumn));
        this.pkValue = pkValue;
        this.length = length;
        this.desc = qualifiedTable + "." + column + "#" + pkColumn + "=" + pkValue;
    }

    /** 先量长度：{@code OCTET_LENGTH} 只回传一个数字，量 1GB 的字段也不占客户端内存。 */
    public static long byteLength(Connection conn, LobSqlDialect dialect, String qualifiedTable,
                                  String column, String pkColumn, Object pkValue) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                dialect.byteLengthSql(qualifiedTable, column, pkColumn))) {
            ps.setObject(1, pkValue);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return -2;   // 行不存在
                }
                return rs.getLong(1);   // -1 表示该列为 NULL
            }
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte[] fetch(long offset, int len) throws IOException {
        try {
            stmt.setLong(1, offset + 1);   // MySQL SUBSTRING 的 pos 是 1-based
            stmt.setInt(2, len);
            stmt.setObject(3, pkValue);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IOException("源行已消失，无法继续读取大字段: " + desc);
                }
                byte[] b = rs.getBytes(1);
                return b == null ? new byte[0] : b;
            }
        } catch (SQLException e) {
            throw new IOException("读取大字段分块失败: " + desc + " offset=" + offset + " len=" + len, e);
        }
    }

    @Override
    public String describe() {
        return desc;
    }

    @Override
    public void close() throws IOException {
        try {
            stmt.close();
        } catch (SQLException e) {
            throw new IOException("关闭大字段读取语句失败: " + desc, e);
        }
    }
}
