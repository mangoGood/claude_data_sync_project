package com.migration.dialect;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle → PostgreSQL：Oracle 表名/列名通常为大写，迁移到 PG 时统一转为小写以避免
 * 大小写敏感问题；列类型走 {@code mapOracleToPgColumnDef}，主键用具名约束 {@code pk_<table>}。
 */
public class OracleToPgTranslator implements TypeTranslator {

    @Override
    public boolean isHomogeneous() {
        return false;
    }

    @Override
    public String generateCreateTable(TableInfo table, SqlDialect targetDialect) {
        StringBuilder sb = new StringBuilder();
        String pgTableName = table.getTableName().toLowerCase();
        sb.append("CREATE TABLE ").append(targetDialect.quoteIdentifier(pgTableName)).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        List<String> pkColumns = new ArrayList<>();

        for (ColumnInfo col : table.getColumns()) {
            String pgColName = col.getColumnName().toLowerCase();
            columnDefs.add("  " + targetDialect.quoteIdentifier(pgColName)
                    + " " + TypeMapper.mapOracleToPgColumnDef(col));
            if (col.isPrimaryKey()) {
                pkColumns.add(pgColName);
            }
        }

        if (!pkColumns.isEmpty()) {
            StringBuilder pkDef = new StringBuilder();
            pkDef.append("  CONSTRAINT ").append(targetDialect.quoteIdentifier("pk_" + pgTableName)).append(" PRIMARY KEY (");
            for (int i = 0; i < pkColumns.size(); i++) {
                if (i > 0) pkDef.append(", ");
                pkDef.append(targetDialect.quoteIdentifier(pkColumns.get(i)));
            }
            pkDef.append(")");
            columnDefs.add(pkDef.toString());
        }

        sb.append(String.join(",\n", columnDefs));
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public Object convertValue(Object value, String sourceTypeName, ResultSet rs, int colIndex) throws SQLException {
        if (value == null) {
            return null;
        }
        if (sourceTypeName == null) {
            return value;
        }
        String lowerType = sourceTypeName.toLowerCase().trim();

        // Oracle DATE 含日期+时间，PG 映射为 TIMESTAMP，需保留时间分量
        if (lowerType.startsWith("date")) {
            if (value instanceof java.sql.Date) {
                java.sql.Timestamp ts = rs.getTimestamp(colIndex);
                return ts != null ? ts : value;
            }
            if (value instanceof java.util.Date) {
                return new java.sql.Timestamp(((java.util.Date) value).getTime());
            }
            return value;
        }

        // TIMESTAMP WITH [LOCAL] TIME ZONE：readColumnValue 已转 String，直接返回
        if (TypeMapper.isOracleTimestampTzType(lowerType)) {
            return value;
        }

        if (lowerType.startsWith("timestamp")) {
            if (value instanceof java.sql.Timestamp) {
                return value;
            }
            if (value instanceof java.util.Date) {
                return new java.sql.Timestamp(((java.util.Date) value).getTime());
            }
            return value;
        }

        // NUMBER：PG 端为 NUMERIC/INTEGER/BIGINT，保持原值即可
        if (lowerType.startsWith("number")) {
            return value;
        }

        // BLOB/RAW 已被 readColumnValue 转为 byte[]，PG BYTEA 直接接受
        if (TypeMapper.isOracleBinaryType(lowerType)) {
            return value;
        }

        // CLOB/NCLOB/LONG 已被 readColumnValue 转为 String
        if (TypeMapper.isOracleLobType(lowerType)) {
            return value;
        }

        return value;
    }
}
