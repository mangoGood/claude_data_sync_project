package com.migration.dialect;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** PostgreSQL → MySQL：列名保持原样，列类型走 {@code mapPgToMysqlColumnDef}，建表带 InnoDB 后缀。 */
public class PgToMysqlTranslator implements TypeTranslator {

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
                    + " " + TypeMapper.mapPgToMysqlColumnDef(col));
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
        sb.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        return sb.toString();
    }

    @Override
    public Object convertValue(Object value, String sourceTypeName, ResultSet rs, int colIndex) throws SQLException {
        if (value == null) {
            return null;
        }

        String lowerType = sourceTypeName.toLowerCase().trim();

        if (TypeMapper.isPgBooleanType(lowerType)) {
            if (value instanceof Boolean) {
                return ((Boolean) value) ? 1 : 0;
            }
            if (value instanceof String) {
                String strVal = ((String) value).trim().toLowerCase();
                if ("t".equals(strVal) || "true".equals(strVal) || "1".equals(strVal)) return 1;
                if ("f".equals(strVal) || "false".equals(strVal) || "0".equals(strVal)) return 0;
            }
            if (value instanceof Number) {
                return ((Number) value).intValue() != 0 ? 1 : 0;
            }
            return 0;
        }

        if (TypeMapper.isPgArrayType(lowerType)) {
            if (value instanceof java.sql.Array) {
                Object[] array = (Object[]) ((java.sql.Array) value).getArray();
                return Arrays.toString(array);
            }
            if (value instanceof String) {
                return value;
            }
            return value.toString();
        }

        if (TypeMapper.isPgUuidType(lowerType)) {
            return value.toString();
        }

        if (TypeMapper.isPgJsonbType(lowerType) || TypeMapper.isPgJsonType(lowerType)) {
            return value.toString();
        }

        if (TypeMapper.isPgTimestampTzType(lowerType)) {
            if (value instanceof java.sql.Timestamp) {
                return value;
            }
            return value.toString();
        }

        if (TypeMapper.isPgTimetzType(lowerType)) {
            if (value instanceof java.sql.Time) {
                return value;
            }
            if (value instanceof String) {
                String strVal = (String) value;
                int plusIdx = strVal.lastIndexOf('+');
                int minusIdx = strVal.lastIndexOf('-');
                int tzIdx = -1;
                if (plusIdx > 0) tzIdx = plusIdx;
                else if (minusIdx > strVal.indexOf(':')) tzIdx = minusIdx;
                if (tzIdx > 0) {
                    return strVal.substring(0, tzIdx).trim();
                }
                return strVal;
            }
            return value.toString();
        }

        if (TypeMapper.isPgIntervalType(lowerType)) {
            return value.toString();
        }

        if (TypeMapper.isPgNetworkType(lowerType)) {
            return value.toString();
        }

        if (TypeMapper.isPgGeometryType(lowerType)) {
            return value.toString();
        }

        if (lowerType.equals("bytea")) {
            if (value instanceof byte[]) {
                return value;
            }
            return value.toString();
        }

        if (lowerType.equals("serial") || lowerType.equals("bigserial")) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return value;
        }

        return value;
    }
}
