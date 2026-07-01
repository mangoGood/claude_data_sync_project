package com.migration.dialect;

import com.migration.model.TableInfo;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 跨库类型/值翻译器：按「源库类型 → 目标库类型」这一对来封装异构迁移时的
 * 建表 DDL 生成与逐值转换，取代散落在 DataMigration / SchemaMigration 里的
 * {@code if (sourceIsPostgresql) … else if (sourceIsOracle) …} 分发分支。
 *
 * <p>具体的类型名映射仍复用 {@link com.migration.model.TypeMapper}；本类只负责
 * 「按库对选择正确的映射 + 生成目标 DDL + 转换列值」。新增一种库对时，新增一个
 * 实现并在 {@link #forPair} 中登记即可，无需再去各业务类里加分支。
 */
public interface TypeTranslator {

    /**
     * 是否为同构迁移（源/目标同类型，或不需要重写 DDL / 转换值）。
     * 同构时上层直接沿用源端的 CREATE TABLE 语句、并原样写入列值。
     */
    boolean isHomogeneous();

    /** 为目标库生成 CREATE TABLE 语句（异构时使用；同构实现不支持调用）。 */
    String generateCreateTable(TableInfo table, SqlDialect targetDialect);

    /** 把源端读到的列值转换为目标库可写入的值（同构时原样返回）。 */
    Object convertValue(Object value, String sourceTypeName, ResultSet rs, int colIndex) throws SQLException;

    /**
     * 按源/目标库类型选择翻译器。分发优先级与历史实现
     * （SchemaMigration.createTable / DataMigration 值转换分支）保持一致，
     * 对全部受支持的库对（mysql↔mysql、mysql→pg、pg→mysql、oracle→pg）行为完全等价。
     */
    static TypeTranslator forPair(String sourceType, String targetType) {
        boolean srcPg = "postgresql".equalsIgnoreCase(sourceType);
        boolean srcOracle = "oracle".equalsIgnoreCase(sourceType);
        boolean tgtPg = "postgresql".equalsIgnoreCase(targetType);

        if (srcPg && !tgtPg) {
            return new PgToMysqlTranslator();
        }
        if (srcOracle && tgtPg) {
            return new OracleToPgTranslator();
        }
        if (!srcPg && !srcOracle && tgtPg) {
            return new MysqlToPgTranslator();
        }
        return new HomogeneousTranslator();
    }
}
