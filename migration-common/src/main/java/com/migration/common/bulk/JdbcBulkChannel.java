package com.migration.common.bulk;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 目标端的批量装载通道。相对 {@link BulkLoadChannel} 多一个 {@link #rebind(Connection)}：
 * 全量搬运期间目标连接可能被服务端掐断，重建后<b>必须把尚未落库的缓冲行重放</b>，
 * 否则这些行会随旧 statement 一起消失而计数照常推进（静默丢数据）。
 */
public interface JdbcBulkChannel extends BulkLoadChannel<Object[]> {

    @Override
    void add(Object[] row) throws SQLException;

    @Override
    long[] flush() throws SQLException;

    /** 目标连接已断开时在新连接上重建写通道，并重放尚未落库的缓冲行。 */
    void rebind(Connection newConn) throws SQLException;

    /** 本通道的实际档位（可能已从配置档位降级）。 */
    BulkLoadOptions.Mode mode();

    /** 运行计数（行数/批数/批失败/重放行数）。 */
    BulkLoadStats stats();
}
