package com.migration.common.route;

/**
 * 一行数据算出来的分片标识。
 *
 * <p>{@link #token()} 是 {@code ${shard}} 的渲染值，{@link #index()} 是参与
 * {@code ${shard/N}} / {@code ${shard%N}} 运算的序号。两者分开是因为 {@code DATE_FORMAT}
 * 分片的标识是时间串（{@code order_202608}）而不是序号——此时 {@code index} 为 -1，
 * 带除模运算的模板在加载期就会被拒绝，不会留到运行期才炸。
 */
public final class ShardKey {

    private final int index;
    private final String token;

    private ShardKey(int index, String token) {
        this.index = index;
        this.token = token;
    }

    /** 数值分片：token 即序号本身。 */
    public static ShardKey ofIndex(int index) {
        return new ShardKey(index, String.valueOf(index));
    }

    /** 非数值分片（DATE_FORMAT）：只有 token。 */
    public static ShardKey ofToken(String token) {
        return new ShardKey(-1, token);
    }

    /** 分片序号；-1 表示非数值分片。 */
    public int index() {
        return index;
    }

    public String token() {
        return token;
    }

    public boolean isNumeric() {
        return index >= 0;
    }

    @Override
    public String toString() {
        return token;
    }
}
