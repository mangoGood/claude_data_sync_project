package com.migration.common.route;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 幂等装载语句生成：把全量的 {@code INSERT} 换成目标端的 upsert。
 *
 * <p>汇聚全量必须幂等——多个源表写同一张目标表时，"上次没搬完就清表重搬"会清掉其它源已搬完的数据，
 * 于是崩溃续传只能靠 upsert 重复写同值来保证正确性。
 *
 * <p>只支持 MySQL 与 PostgreSQL 目标（首版汇聚的引擎范围）；其它目标返回 null，
 * 调用方回退普通 INSERT 并告警，而不是拼一条跑不通的语句。
 */
public final class UpsertSqlBuilder {

    private UpsertSqlBuilder() {
    }

    /**
     * 生成 upsert 语句。
     *
     * @param targetDbType   目标库类型（mysql / postgresql）
     * @param quotedTable    已按方言引用的目标表名
     * @param quotedColumns  已按方言引用的目标列名，顺序与占位符一致
     * @param pkColumns      主键列名（<b>未引用</b>，用于匹配 quotedColumns 并生成冲突目标）
     * @return upsert 语句；目标端不支持或主键为空时返回 null
     */
    public static String build(String targetDbType, String quotedTable,
                               List<String> quotedColumns, List<String> pkColumns) {
        if (quotedTable == null || quotedColumns == null || quotedColumns.isEmpty()
                || pkColumns == null || pkColumns.isEmpty()) {
            return null;
        }
        boolean mysql = "mysql".equalsIgnoreCase(targetDbType);
        boolean postgres = "postgresql".equalsIgnoreCase(targetDbType);
        if (!mysql && !postgres) {
            return null;
        }

        List<String> quotedPk = new ArrayList<>();
        List<String> nonPk = new ArrayList<>();
        for (String quoted : quotedColumns) {
            if (isPk(quoted, pkColumns)) {
                quotedPk.add(quoted);
            } else {
                nonPk.add(quoted);
            }
        }
        if (quotedPk.size() != pkColumns.size()) {
            // 主键列没有全部出现在写入列里（列过滤/列名映射改动过），冲突目标不完整 → 不生成 upsert
            return null;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(quotedTable).append(" (")
                .append(String.join(", ", quotedColumns)).append(") VALUES (");
        for (int i = 0; i < quotedColumns.size(); i++) {
            sql.append(i == 0 ? "?" : ", ?");
        }
        sql.append(")");

        if (mysql) {
            sql.append(" ON DUPLICATE KEY UPDATE ");
            List<String> assignments = nonPk.isEmpty() ? quotedPk : nonPk;
            for (int i = 0; i < assignments.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                String col = assignments.get(i);
                sql.append(col).append("=VALUES(").append(col).append(")");
            }
        } else {
            sql.append(" ON CONFLICT (").append(String.join(", ", quotedPk)).append(")");
            if (nonPk.isEmpty()) {
                // 整表都是主键列：重复行没有可更新的内容，DO NOTHING 即幂等
                sql.append(" DO NOTHING");
            } else {
                sql.append(" DO UPDATE SET ");
                for (int i = 0; i < nonPk.size(); i++) {
                    if (i > 0) {
                        sql.append(", ");
                    }
                    String col = nonPk.get(i);
                    sql.append(col).append("=EXCLUDED.").append(col);
                }
            }
        }
        return sql.toString();
    }

    private static boolean isPk(String quotedColumn, List<String> pkColumns) {
        String bare = unquote(quotedColumn);
        for (String pk : pkColumns) {
            if (bare.equalsIgnoreCase(pk)) {
                return true;
            }
        }
        return false;
    }

    private static String unquote(String s) {
        String t = s == null ? "" : s.trim();
        if (t.length() >= 2) {
            char first = t.charAt(0);
            char last = t.charAt(t.length() - 1);
            if ((first == '`' && last == '`') || (first == '"' && last == '"')) {
                return t.substring(1, t.length() - 1);
            }
        }
        return t;
    }

    /** 目标端是否支持幂等装载（决定汇聚能否开启）。 */
    public static boolean supports(String targetDbType) {
        if (targetDbType == null) {
            return false;
        }
        String t = targetDbType.toLowerCase(Locale.ROOT);
        return "mysql".equals(t) || "postgresql".equals(t);
    }
}
