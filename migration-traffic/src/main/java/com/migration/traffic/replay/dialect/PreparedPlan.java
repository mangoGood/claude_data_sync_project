package com.migration.traffic.replay.dialect;

import java.util.List;

/**
 * 一条待执行语句改写后的形态：JDBC 能接受的 SQL + 与之对应的绑定值序列。
 *
 * <p>为什么需要改写：录制里的占位符是<b>服务端语法</b>（PG 的 {@code $1}、Oracle 的 {@code :b1}），
 * 而 JDBC 驱动只认 {@code ?}。实测把 {@code INSERT INTO t VALUES ($1,$2,$3)} 直接交给
 * pgjdbc，得到的是 <b>"栏位索引超过许可范围：1，栏位数：0"</b>——
 * 驱动在 SQL 里一个参数标记都没找到，报错跟"参数没绑上"完全不像。
 *
 * <p>改写同时要处理<b>重复引用</b>：{@code WHERE a=$1 OR b=$1} 有两个 {@code ?}，
 * 但录制里只有一个绑定值，两处都要绑同一个值。
 */
public final class PreparedPlan {

    private final String sql;
    private final List<String> binds;

    public PreparedPlan(String sql, List<String> binds) {
        this.sql = sql;
        this.binds = binds;
    }

    public String sql() {
        return sql;
    }

    public List<String> binds() {
        return binds;
    }
}
