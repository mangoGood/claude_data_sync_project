package com.migration.common.route;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 汇聚建表语句改写：给目标表追加来源标识列，并把它们并入主键。
 *
 * <p>输入是同构链路的源端建表语句（MySQL {@code SHOW CREATE TABLE}、PG 由元数据拼装），
 * 两者形态一致：{@code CREATE TABLE <引用名> ( 列定义..., PRIMARY KEY (列...) ) ...}，
 * 因此同一套文本改写对两端都成立。
 *
 * <p><b>为什么必须并入主键</b>：汇聚全量走幂等 upsert，冲突目标是目标表主键。
 * 若只追加来源列而不并入主键，两个分片里主键相同的行会互相覆盖——
 * 数据不会报错，只会少，是最难发现的一类丢数据。
 */
public final class MergeDdlRewriter {

    /** 来源标识列的长度：足够放实例标识/库名/表名（MySQL 标识符上限 64） */
    private static final int TAG_COLUMN_LENGTH = 64;

    private MergeDdlRewriter() {
    }

    /**
     * 在定义体末尾追加来源标识列。
     *
     * @return 改写后的建表语句；{@code tagColumns} 为空或找不到定义体闭括号时原样返回
     */
    public static String appendTagColumns(String createSql, List<String> tagColumns, boolean postgres) {
        if (createSql == null || tagColumns == null || tagColumns.isEmpty()) {
            return createSql;
        }
        int closeIdx = createSql.lastIndexOf("\n)");
        if (closeIdx < 0) {
            closeIdx = createSql.lastIndexOf(')');
            if (closeIdx < 0) {
                return createSql;
            }
        }
        StringBuilder defs = new StringBuilder();
        for (String col : tagColumns) {
            defs.append(",\n  ").append(tagColumnDef(col, postgres));
        }
        return createSql.substring(0, closeIdx) + defs + createSql.substring(closeIdx);
    }

    /** 单个来源标识列的列定义（不含前导逗号）。NOT NULL DEFAULT '' 是为了能进主键。 */
    public static String tagColumnDef(String column, boolean postgres) {
        String quoted = quote(column, postgres);
        if (postgres) {
            return quoted + " VARCHAR(" + TAG_COLUMN_LENGTH + ") NOT NULL DEFAULT ''";
        }
        return quoted + " VARCHAR(" + TAG_COLUMN_LENGTH + ") NOT NULL DEFAULT '' COMMENT '汇聚来源标识'";
    }

    /**
     * 把来源标识列并入 PRIMARY KEY 列表。
     *
     * @return 改写后的建表语句；语句里没有 PRIMARY KEY 时原样返回（调用方须据
     *         {@link #hasPrimaryKey} 提前拒绝无主键表的汇聚）
     */
    public static String extendPrimaryKey(String createSql, List<String> tagColumns, boolean postgres) {
        if (createSql == null || tagColumns == null || tagColumns.isEmpty()) {
            return createSql;
        }
        int open = primaryKeyParenIndex(createSql);
        if (open < 0) {
            return createSql;
        }
        int close = matchingParen(createSql, open);
        if (close < 0) {
            return createSql;
        }
        StringBuilder add = new StringBuilder();
        List<String> existing = primaryKeyColumns(createSql);
        for (String col : tagColumns) {
            if (existing.stream().anyMatch(c -> c.equalsIgnoreCase(col))) {
                continue;   // 幂等：已在主键里的列不重复加
            }
            add.append(", ").append(quote(col, postgres));
        }
        if (add.length() == 0) {
            return createSql;
        }
        return createSql.substring(0, close) + add + createSql.substring(close);
    }

    /** 建表语句里是否声明了主键。 */
    public static boolean hasPrimaryKey(String createSql) {
        return primaryKeyParenIndex(createSql) >= 0;
    }

    /**
     * 解析主键列名（去引用符、去 MySQL 前缀长度 {@code (10)}）；没有主键返回空列表。
     * 供 upsert 语句的冲突目标使用。
     */
    public static List<String> primaryKeyColumns(String createSql) {
        List<String> columns = new ArrayList<>();
        int open = primaryKeyParenIndex(createSql);
        if (open < 0) {
            return columns;
        }
        int close = matchingParen(createSql, open);
        if (close < 0) {
            return columns;
        }
        String body = createSql.substring(open + 1, close);
        int depth = 0;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') {
                depth++;             // MySQL 前缀索引 `col`(10)：括号内内容整体丢弃
                continue;
            }
            if (c == ')') {
                depth = Math.max(0, depth - 1);
                continue;
            }
            if (depth > 0) {
                continue;
            }
            if (c == ',') {
                addColumn(columns, current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        addColumn(columns, current.toString());
        return columns;
    }

    private static void addColumn(List<String> columns, String raw) {
        String col = raw.trim();
        if (col.isEmpty()) {
            return;
        }
        if (col.length() >= 2) {
            char first = col.charAt(0);
            char last = col.charAt(col.length() - 1);
            if ((first == '`' && last == '`') || (first == '"' && last == '"')) {
                col = col.substring(1, col.length() - 1);
            }
        }
        // 去掉可能残留的排序方向（PG/MySQL 的 ASC/DESC）
        int space = col.indexOf(' ');
        if (space > 0) {
            col = col.substring(0, space);
        }
        if (!col.isEmpty()) {
            columns.add(col);
        }
    }

    /** {@code PRIMARY KEY} 之后那个左括号的下标；没有返回 -1。 */
    private static int primaryKeyParenIndex(String createSql) {
        if (createSql == null) {
            return -1;
        }
        String upper = createSql.toUpperCase(Locale.ROOT);
        int idx = upper.indexOf("PRIMARY KEY");
        while (idx >= 0) {
            int paren = upper.indexOf('(', idx);
            if (paren > 0) {
                // "PRIMARY KEY" 与 "(" 之间只允许空白，避免误命中列定义里的 "... PRIMARY KEY," 之后的其它括号
                String between = upper.substring(idx + "PRIMARY KEY".length(), paren);
                if (between.trim().isEmpty()) {
                    return paren;
                }
            }
            idx = upper.indexOf("PRIMARY KEY", idx + 1);
        }
        return -1;
    }

    private static int matchingParen(String s, int open) {
        int depth = 0;
        for (int i = open; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static String quote(String identifier, boolean postgres) {
        return postgres ? "\"" + identifier + "\"" : "`" + identifier + "`";
    }
}
