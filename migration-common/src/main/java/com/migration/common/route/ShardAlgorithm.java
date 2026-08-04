package com.migration.common.route;

/**
 * 分片算法。
 *
 * <ul>
 *   <li>{@code HASH_MOD}：整型值直接取模（{@code user_id % 16}，与业务侧分库分表的惯例一致），
 *       非整型值取 UTF-8 字节的 CRC32 再取模。必须配 {@code count}。</li>
 *   <li>{@code RANGE}：按数值区间落片，区间由 {@code range=lo:hi,...} 给出（左闭右开），分片数 = 区间数。</li>
 *   <li>{@code LIST}：按枚举值映射落片，映射由 {@code list=值:分片号,...} 给出。</li>
 *   <li>{@code DATE_FORMAT}：按时间格式化成表名后缀（{@code order_202608}），分片标识是字符串、
 *       <b>不可枚举</b>——不能预建全部目标表，也不能广播未路由行。</li>
 * </ul>
 */
public enum ShardAlgorithm {
    HASH_MOD,
    RANGE,
    LIST,
    DATE_FORMAT;

    /** 分片集合是否可枚举（预建目标表、BROADCAST 未路由行都依赖它）。 */
    public boolean isEnumerable() {
        return this != DATE_FORMAT;
    }

    public static ShardAlgorithm parse(String s, ShardAlgorithm fallback) {
        if (s == null || s.trim().isEmpty()) {
            return fallback;
        }
        try {
            return valueOf(s.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }
}
