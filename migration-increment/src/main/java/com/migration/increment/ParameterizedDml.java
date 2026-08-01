package com.migration.increment;

import java.util.List;

/**
 * 参数化 DML：SQL 模板（? 占位）+ 类型化参数列表。
 * 类型化值管道的执行单元——值经 PreparedStatement 绑定，永不拼接进 SQL 文本。
 */
public class ParameterizedDml {

    private final String sql;
    private final List<Object> params;
    /** 目标表名（含库名限定，形如 db.tbl）；双向冲突消解按它 + rowKey 定位行。 */
    private String targetTable;
    /** 本条 DML 作用行的主键标识（多列主键按序拼接）；无主键时为 null，冲突消解自动跳过。 */
    private String rowKey;
    /** INSERT / UPDATE / DELETE，仅供冲突消解与死信记录使用。 */
    private String opType;

    public ParameterizedDml(String sql, List<Object> params) {
        this.sql = sql;
        this.params = params;
    }

    public ParameterizedDml(String sql, List<Object> params, String targetTable, String rowKey, String opType) {
        this(sql, params);
        this.targetTable = targetTable;
        this.rowKey = rowKey;
        this.opType = opType;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getParams() {
        return params;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getRowKey() {
        return rowKey;
    }

    public String getOpType() {
        return opType;
    }

    /**
     * 冲突裁决判"来的一方赢"时用的强制覆盖版 SQL（主键定位，不带前镜像条件）。
     * 正常路径永远不用它——只有前镜像守卫命中"影响 0 行"且裁决为 APPLY 才执行。
     */
    private String overrideSql;
    private List<Object> overrideParams;

    public ParameterizedDml withOverride(String overrideSql, List<Object> overrideParams) {
        this.overrideSql = overrideSql;
        this.overrideParams = overrideParams;
        return this;
    }

    public boolean hasOverride() {
        return overrideSql != null;
    }

    public String getOverrideSql() {
        return overrideSql;
    }

    public List<Object> getOverrideParams() {
        return overrideParams;
    }

    public ParameterizedDml withRow(String targetTable, String rowKey, String opType) {
        this.targetTable = targetTable;
        this.rowKey = rowKey;
        this.opType = opType;
        return this;
    }

    @Override
    public String toString() {
        return sql + " /* params=" + params.size() + " */";
    }
}
