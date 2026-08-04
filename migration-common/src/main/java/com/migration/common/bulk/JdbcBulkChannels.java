package com.migration.common.bulk;

import com.migration.common.bulk.PgBinaryCopyEncoder.PgType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * JDBC 批量装载通道的选路。按目标端与配置档位挑一条通道，<b>任何不确定都往安全档降级</b>：
 *
 * <ul>
 *   <li>{@code COPY}：目标必须是 PostgreSQL，且该表<b>每一列</b>的类型都在二进制编码器的支持集内
 *       （数组/枚举/interval/几何等一律不在）。探测失败或有不支持的列 → 降级 {@code BATCH}。</li>
 *   <li>{@code DIRECT_PATH}：目标必须是 Oracle，且本通道是该表<b>唯一的写入者</b>——直接路径装载
 *       持表级排他锁，单表 PK 分片并行下多个 worker 同时写同一张表会互相阻塞，反而更慢。
 *       分片并行时 → 降级 {@code BATCH}。</li>
 *   <li>其余（含 {@code AUTO}） → {@code BATCH}：驱动语句重写，零协议风险。</li>
 * </ul>
 */
public final class JdbcBulkChannels {
    private static final Logger logger = LoggerFactory.getLogger(JdbcBulkChannels.class);

    private JdbcBulkChannels() {
    }

    /**
     * 打开一条目标端装载通道。
     *
     * @param insertSql        逐行/批量 INSERT 语句（占位符形式）
     * @param quotedTable      已按方言引用的目标表名（COPY 用）
     * @param quotedColumnList 已按方言引用、逗号分隔的目标列名（COPY 与类型探测用），顺序与 insertSql 一致
     * @param tableName        日志用的表名
     * @param exclusiveWriter  本通道是否为该表的唯一写入者（单表分片并行时为 false）
     */
    public static JdbcBulkChannel open(Connection targetConn,
                                       String insertSql,
                                       String quotedTable,
                                       String quotedColumnList,
                                       String tableName,
                                       String targetDbType,
                                       BulkLoadOptions options,
                                       int batchRows,
                                       boolean exclusiveWriter) throws SQLException {
        BulkLoadOptions.Mode mode = options.modeFor(targetDbType);
        long batchBytes = options.bytes(JdbcBatchChannel.DEFAULT_BATCH_BYTES);

        if (mode == BulkLoadOptions.Mode.COPY) {
            PgType[] types = probePgTypes(targetConn, quotedTable, quotedColumnList, tableName);
            if (types != null) {
                String copySql = "COPY " + quotedTable + " (" + quotedColumnList
                        + ") FROM STDIN WITH (FORMAT binary)";
                logger.info("表 {} 启用二进制 COPY 装载通道（{} 列）", tableName, types.length);
                return new JdbcCopyChannel(targetConn, copySql, insertSql, tableName, types, batchRows, batchBytes);
            }
            mode = BulkLoadOptions.Mode.BATCH;
        }

        if (mode == BulkLoadOptions.Mode.DIRECT_PATH) {
            if (!exclusiveWriter) {
                logger.info("表 {} 单表分片并行写入，direct-path 会因表级排他锁互相阻塞，降级为 BATCH", tableName);
                mode = BulkLoadOptions.Mode.BATCH;
            } else {
                String hinted = withAppendValues(insertSql);
                if (hinted == null) {
                    logger.warn("表 {} 的 INSERT 语句无法插入 APPEND_VALUES 提示，降级为 BATCH", tableName);
                    mode = BulkLoadOptions.Mode.BATCH;
                } else {
                    logger.info("表 {} 启用 Oracle direct-path（APPEND_VALUES）装载通道", tableName);
                    // 重放语句故意用<b>不带提示</b>的原 SQL：APPEND_VALUES 只对数组绑定有意义，
                    // 逐行重放时用它等于每行都申请一次表级排他锁。
                    return new JdbcBatchChannel(targetConn, hinted, insertSql, tableName,
                            batchRows, batchBytes, BulkLoadOptions.Mode.DIRECT_PATH);
                }
            }
        }

        return new JdbcBatchChannel(targetConn, insertSql, insertSql, tableName,
                batchRows, batchBytes, BulkLoadOptions.Mode.BATCH);
    }

    /**
     * 探测目标表各列的 PG 类型。任一列不在二进制编码器支持集内即返回 null（调用方降级）——
     * 猜一个编码去写二进制流会静默写坏数据，这是唯一不能"尽力而为"的地方。
     */
    private static PgType[] probePgTypes(Connection conn, String quotedTable,
                                         String quotedColumnList, String tableName) {
        String probeSql = "SELECT " + quotedColumnList + " FROM " + quotedTable + " WHERE 1=0";
        try (PreparedStatement ps = conn.prepareStatement(probeSql)) {
            ResultSetMetaData meta = ps.getMetaData();
            if (meta == null) {
                logger.info("表 {} 无法获取目标列类型元数据，COPY 降级为 BATCH", tableName);
                return null;
            }
            int count = meta.getColumnCount();
            PgType[] types = new PgType[count];
            for (int i = 1; i <= count; i++) {
                PgType type = PgType.fromTypeName(meta.getColumnTypeName(i));
                if (type == null) {
                    logger.info("表 {} 的列 {}（类型 {}）不在二进制 COPY 支持集内，COPY 降级为 BATCH",
                            tableName, meta.getColumnName(i), meta.getColumnTypeName(i));
                    return null;
                }
                types[i - 1] = type;
            }
            return types;
        } catch (SQLException e) {
            logger.info("表 {} 探测目标列类型失败，COPY 降级为 BATCH: {}", tableName, e.getMessage());
            return null;
        }
    }

    /** 给 {@code INSERT INTO ...} 加上 {@code /*+ APPEND_VALUES *&#47;} 提示；语句形状不符时返回 null。 */
    static String withAppendValues(String insertSql) {
        if (insertSql == null) {
            return null;
        }
        String trimmed = insertSql.trim();
        if (!trimmed.regionMatches(true, 0, "INSERT", 0, 6)) {
            return null;
        }
        return "INSERT /*+ APPEND_VALUES */" + trimmed.substring("INSERT".length());
    }
}
