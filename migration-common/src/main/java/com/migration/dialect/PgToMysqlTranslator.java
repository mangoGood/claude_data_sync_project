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

    /**
     * 增量文本路径逐值转换（与上面 {@link #convertValue} 对象路径成对，同库对同一处维护，杜绝漂移）：
     * boolean → 1/0；bytea {@code E'\xHH'} → MySQL {@code 0xHH}；uuid/json/时间等去掉 PG 的 {@code ::type} 类型转换后缀。
     */
    @Override
    public String convertLiteral(String value, String sourceColumnType) {
        if (value == null || value.equalsIgnoreCase("NULL")) {
            return "NULL";
        }

        String lowerType = sourceColumnType == null ? "" : sourceColumnType.toLowerCase().trim();

        if (lowerType.equals("boolean") || lowerType.equals("bool")) {
            if (value.equals("t") || value.equals("true")) return "1";
            if (value.equals("f") || value.equals("false")) return "0";
            return value;
        }

        if (lowerType.contains("uuid")) {
            if (value.startsWith("'") && value.contains("::uuid")) {
                return value.substring(0, value.indexOf("::uuid"));
            }
            return value;
        }

        if (lowerType.contains("json") || lowerType.contains("jsonb")) {
            if (value.contains("::jsonb") || value.contains("::json")) {
                int idx = value.indexOf("::");
                if (idx > 0) {
                    return value.substring(0, idx);
                }
            }
            return value;
        }

        if (lowerType.contains("timestamp") || lowerType.contains("date") || lowerType.contains("time")) {
            if (value.contains("::")) {
                int idx = value.indexOf("::");
                if (idx > 0) {
                    return value.substring(0, idx);
                }
            }
            return value;
        }

        if (lowerType.contains("bytea")) {
            if (value.startsWith("E'\\\\x") || value.startsWith("E'\\x")) {
                String hexStr = value.replaceAll("^E'\\\\?x", "").replaceAll("'$", "");
                return "0x" + hexStr;
            }
            return value;
        }

        if (value.contains("::")) {
            int idx = value.indexOf("::");
            return value.substring(0, idx);
        }

        return value;
    }
}
