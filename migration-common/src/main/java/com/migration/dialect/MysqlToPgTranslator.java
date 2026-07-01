package com.migration.dialect;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/** MySQL → PostgreSQL：列名保持原样，列类型走 {@code mapMysqlToPgColumnDef}。 */
public class MysqlToPgTranslator implements TypeTranslator {

    @Override
    public boolean isHomogeneous() {
        return false;
    }

    @Override
    public String generateCreateTable(TableInfo table, SqlDialect targetDialect) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(targetDialect.quoteIdentifier(table.getTableName())).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        List<String> pkColumns = new ArrayList<>();

        for (ColumnInfo col : table.getColumns()) {
            columnDefs.add("  " + targetDialect.quoteIdentifier(col.getColumnName())
                    + " " + TypeMapper.mapMysqlToPgColumnDef(col));
            if (col.isPrimaryKey()) {
                pkColumns.add(col.getColumnName());
            }
        }

        if (!pkColumns.isEmpty()) {
            StringBuilder pkDef = new StringBuilder("  PRIMARY KEY (");
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
    public Object convertValue(Object value, String sourceTypeName, ResultSet rs, int colIndex) {
        if (value == null) {
            return null;
        }
        if (sourceTypeName == null) {
            return value;
        }

        String lowerType = sourceTypeName.toLowerCase().trim();

        if (lowerType.startsWith("tinyint") && lowerType.contains("(1)")) {
            if (value instanceof Number) {
                return ((Number) value).intValue() != 0;
            }
            if (value instanceof Boolean) {
                return value;
            }
        }

        if (lowerType.startsWith("json")) {
            return value.toString();
        }

        if (lowerType.startsWith("year")) {
            if (value instanceof java.sql.Date) {
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime((java.sql.Date) value);
                return cal.get(java.util.Calendar.YEAR);
            }
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }

        if (lowerType.startsWith("datetime") || lowerType.startsWith("timestamp")) {
            if (value instanceof java.sql.Timestamp) {
                return value;
            }
            if (value instanceof java.util.Date) {
                return new java.sql.Timestamp(((java.util.Date) value).getTime());
            }
        }

        return value;
    }
}
