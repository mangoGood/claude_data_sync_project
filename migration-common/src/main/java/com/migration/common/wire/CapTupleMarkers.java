package com.migration.common.wire;

/**
 * {@code .cap} 文件里 tuple 值的哨兵标记。
 *
 * <p>capture 与 extract 分属两个模块、互不依赖，这些标记此前是两边各写一份字面量。
 * 标记一旦对不上就是**静默**故障：写出方以为标了"这个值没发过来"，读入方按普通字符串收下，
 * 最后原样落进目标列。所以收敛到 migration-common 这一份，两边共用。
 *
 * <p>标记都**不带引号**下发，与带引号的文本值天然区分：真实值 {@code '[null]'} 会被渲染成
 * 带引号的形态，不会与标记 {@code [null]} 混淆。
 */
public final class CapTupleMarkers {

    /** 该列的值是 SQL NULL。 */
    public static final String NULL = "[null]";

    /**
     * 该列的值<b>没有随事件发送</b>，因此无从得知——不等于 NULL，也不等于空串。
     *
     * <p>目前唯一来源是 PostgreSQL 逻辑复制的列标志 {@code 'u'}：行外存储（TOAST）的值
     * 本次 UPDATE 没有修改时，pgoutput 不发送它。下游必须把该列整个从 SET 列表里摘掉，
     * 而不是写 NULL —— 写 NULL 会把目标端已经正确的大字段抹掉且全程无报错。
     */
    public static final String UNCHANGED = "[unchanged]";

    private CapTupleMarkers() {
    }
}
