package com.migration.common.bulk;

/**
 * 全量批量装载通道的统一契约。JDBC（语句重写 / COPY / direct-path）、MongoDB bulkWrite、
 * ES _bulk、Redis pipeline 都实现它，调用方的攒批循环因此长得一模一样：
 *
 * <pre>
 *   channel.add(item);
 *   if (channel.isFull()) { long[] r = channel.flush(); ok += r[0]; fail += r[1]; }
 *   ... 收尾 ...
 *   if (!channel.isEmpty()) { long[] r = channel.flush(); ... }
 * </pre>
 *
 * <p><b>flush 的语义是"尽力送达并如实计数"</b>，不是"全或无"：批级失败时实现方应降级为逐条重放，
 * 只把真正失败的条目计入 fail，其余计入 success。整批抛异常等于把已经写进目标端的行也算没写，
 * 进度与实际数据会对不上——SQL 全量此前就踩过这个（批失败只 warn 一句，静默丢掉一整批）。
 *
 * @param <T> 单条装载单元（JDBC 的一行值数组 / 一个 Mongo 文档 / 一条 ES bulk 操作 / 一个 Redis key）
 */
public interface BulkLoadChannel<T> extends AutoCloseable {

    /** 把一条记录加入当前批。不触发网络往返。 */
    void add(T item) throws Exception;

    /** 当前批是否已达行数或字节阈值，应当 flush。 */
    boolean isFull();

    /** 当前批是否为空（收尾时用来跳过空 flush）。 */
    boolean isEmpty();

    /**
     * 提交当前批并清空缓冲。
     *
     * @return {@code {成功条数, 失败条数}}，两者之和等于本批提交的条数
     */
    long[] flush() throws Exception;

    /** 关闭通道（释放语句/连接/缓冲）。不提交剩余缓冲——收尾 flush 由调用方负责。 */
    @Override
    void close();
}
