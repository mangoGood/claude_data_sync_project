package com.migration.dialect;

import com.migration.model.TableInfo;

import java.sql.ResultSet;

/** 同构迁移（源/目标同类型）：沿用源端 DDL，列值原样写入，无需转换。 */
public class HomogeneousTranslator implements TypeTranslator {

    @Override
    public boolean isHomogeneous() {
        return true;
    }

    @Override
    public String generateCreateTable(TableInfo table, SqlDialect targetDialect) {
        throw new UnsupportedOperationException("同构迁移应直接使用源端 CREATE TABLE 语句");
    }

    @Override
    public Object convertValue(Object value, String sourceTypeName, ResultSet rs, int colIndex) {
        return value;
    }
}
