package com.migration.increment;

import java.util.List;

/**
 * 参数化 DML：SQL 模板（? 占位）+ 类型化参数列表。
 * 类型化值管道的执行单元——值经 PreparedStatement 绑定，永不拼接进 SQL 文本。
 */
public class ParameterizedDml {

    private final String sql;
    private final List<Object> params;

    public ParameterizedDml(String sql, List<Object> params) {
        this.sql = sql;
        this.params = params;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return sql + " /* params=" + params.size() + " */";
    }
}
