package com.migration.common.route;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 拆分建表语句改写：把一张源表的建表语句改写成某个分片的目标表。
 *
 * <p>两件事：
 * <ul>
 *   <li><b>改写表名并加库名限定</b>——分库分表的目标表分散在多个库里，
 *       全量写入用一条连接跑限定名（MySQL 同实例跨库合法），建表也用限定名；</li>
 *   <li><b>剥掉 AUTO_INCREMENT</b>——每个分片各自维护自增序列必然撞号，
 *       拆分一律沿用源端主键值，目标表不能再带自增属性。</li>
 * </ul>
 */
public final class SplitDdlRewriter {

    /** 列定义里的 AUTO_INCREMENT 属性 */
    private static final Pattern COLUMN_AUTO_INCREMENT =
            Pattern.compile("\\s+AUTO_INCREMENT\\b", Pattern.CASE_INSENSITIVE);
    /** 表选项 AUTO_INCREMENT=n */
    private static final Pattern TABLE_AUTO_INCREMENT =
            Pattern.compile("\\s*AUTO_INCREMENT\\s*=\\s*\\d+", Pattern.CASE_INSENSITIVE);
    /** CREATE TABLE 头部的表名（反引号 / 双引号 / 裸名，可带库名限定） */
    private static final Pattern CREATE_TABLE_HEAD = Pattern.compile(
            "(CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?)"
                    + "(`[^`]+`(?:\\s*\\.\\s*`[^`]+`)?|\"[^\"]+\"(?:\\s*\\.\\s*\"[^\"]+\")?|[A-Za-z_][A-Za-z0-9_$]*(?:\\s*\\.\\s*[A-Za-z_][A-Za-z0-9_$]*)?)",
            Pattern.CASE_INSENSITIVE);

    private SplitDdlRewriter() {
    }

    /**
     * 把建表语句改写到指定分片的库表。
     *
     * @param database null/空 = 不加库名限定（目标库由连接决定）
     * @return 改写后的语句；未匹配到 CREATE TABLE 头部时原样返回
     */
    public static String retargetCreateTable(String createSql, String database, String table, boolean postgres) {
        if (createSql == null || table == null || table.isEmpty()) {
            return createSql;
        }
        Matcher m = CREATE_TABLE_HEAD.matcher(createSql);
        if (!m.find()) {
            return createSql;
        }
        String ref = quote(table, postgres);
        if (database != null && !database.isEmpty() && !postgres) {
            // PG 一条连接跨不了库，限定名只对 MySQL 有意义
            ref = quote(database, postgres) + "." + ref;
        }
        return createSql.substring(0, m.start(2)) + ref + createSql.substring(m.end(2));
    }

    /**
     * 剥掉 AUTO_INCREMENT（列属性 + 表选项）。
     *
     * <p>分片表继续带自增会出两种事故：各分片从 1 开始各自发号 → 跨分片主键重复；
     * 而源端主键值本身是要照搬的，自增列在 INSERT 显式给值时也只是徒增维护面。
     */
    public static String stripAutoIncrement(String createSql) {
        if (createSql == null) {
            return null;
        }
        String out = TABLE_AUTO_INCREMENT.matcher(createSql).replaceAll("");
        out = COLUMN_AUTO_INCREMENT.matcher(out).replaceAll("");
        return out;
    }

    private static String quote(String identifier, boolean postgres) {
        return postgres ? "\"" + identifier + "\"" : "`" + identifier + "`";
    }
}
