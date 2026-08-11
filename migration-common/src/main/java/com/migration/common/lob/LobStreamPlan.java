package com.migration.common.lob;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * 一张表里哪些列要走"旁路流式搬运"。
 *
 * <p>判定<b>只看声明类型</b>，不去探测实际数据量。原因是实测出来的：
 * {@code MAX(OCTET_LENGTH(col))} 这类探测会让服务端把每一行的 LOB 都完整读一遍，
 * 在 10 行 × 1GB 的表上光探测就要跑几分钟——为了决定"要不要省内存"先付一次全表 LOB 读，
 * 完全是倒因为果。
 *
 * <p>因此按类型上限划线：
 * <ul>
 *   <li>{@code LONGBLOB/LONGTEXT}（上限 4GB）、{@code MEDIUMBLOB/MEDIUMTEXT}（上限 16MB）
 *       → 可能超过单包上限，走流式；</li>
 *   <li>{@code BLOB/TEXT}（上限 64KB）、{@code TINYBLOB/TINYTEXT} → 永远塞得进一条语句，
 *       维持原路径，零回归；</li>
 *   <li>{@code JSON} → 不走流式：下游要按 JSON 语义解析，拿不到完整字节没法处理。</li>
 * </ul>
 *
 * <p>值本身小的时候流式也不亏：分块追加对 1KB 的值就是一条语句，
 * 与原路径的差别只是多一次目标端长度探测。
 */
public final class LobStreamPlan {

    /** 参与流式搬运的一列。 */
    public static final class LobColumn {
        /** 源端列名。 */
        public final String sourceName;
        /** 目标端列名（列名映射之后）。 */
        public final String targetName;
        /** 在 SELECT 列表 / 行值数组里的 0-based 位置。 */
        public final int index;
        /** TEXT 系：{@code SUBSTRING} 按字符计位，切分前必须先 CAST 成二进制。 */
        public final boolean textColumn;

        public LobColumn(String sourceName, String targetName, int index, boolean textColumn) {
            this.sourceName = sourceName;
            this.targetName = targetName;
            this.index = index;
            this.textColumn = textColumn;
        }

        @Override
        public String toString() {
            return sourceName + (textColumn ? "(text)" : "(binary)") + "@" + index;
        }
    }

    private final List<LobColumn> columns;

    private LobStreamPlan(List<LobColumn> columns) {
        this.columns = Collections.unmodifiableList(columns);
    }

    public static final LobStreamPlan EMPTY = new LobStreamPlan(new ArrayList<>());

    /** 该类型的值是否可能大到装不进一条语句。 */
    public static boolean isStreamableLobType(String dataType) {
        if (dataType == null) {
            return false;
        }
        String t = dataType.toLowerCase(Locale.ROOT).trim();
        int paren = t.indexOf('(');
        if (paren > 0) {
            t = t.substring(0, paren).trim();
        }
        return t.equals("longblob") || t.equals("longtext")
                || t.equals("mediumblob") || t.equals("mediumtext");
    }

    /** TEXT 系（字符语义），与二进制 BLOB 系区分。 */
    public static boolean isTextLobType(String dataType) {
        if (dataType == null) {
            return false;
        }
        String t = dataType.toLowerCase(Locale.ROOT).trim();
        return t.startsWith("longtext") || t.startsWith("mediumtext");
    }

    /**
     * 按表结构生成计划。
     *
     * @param targetNameMapper 源列名 → 目标列名（列名映射；传 null 表示同名）
     */
    public static LobStreamPlan build(TableInfo table, java.util.function.UnaryOperator<String> targetNameMapper) {
        if (table == null || table.getColumns() == null) {
            return EMPTY;
        }
        List<LobColumn> found = new ArrayList<>();
        List<ColumnInfo> cols = table.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            ColumnInfo c = cols.get(i);
            if (isStreamableLobType(c.getDataType())) {
                String target = targetNameMapper == null ? c.getColumnName()
                        : targetNameMapper.apply(c.getColumnName());
                found.add(new LobColumn(c.getColumnName(), target, i, isTextLobType(c.getDataType())));
            }
        }
        return found.isEmpty() ? EMPTY : new LobStreamPlan(found);
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public List<LobColumn> columns() {
        return columns;
    }

    /** 该 0-based 列位是否是流式列（瘦扫描时要换成长度表达式、行值里置 null）。 */
    public boolean isLobIndex(int index) {
        for (LobColumn c : columns) {
            if (c.index == index) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return columns.toString();
    }
}
