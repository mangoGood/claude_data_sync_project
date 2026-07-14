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

    /**
     * 增量文本路径逐值转换（与上面 {@link #convertValue} 对象路径成对，同库对同一处维护，杜绝漂移）：
     * tinyint(1)/bool → PG boolean 字面量 true/false；bit(N) → PG bytea 十六进制字面量 {@code E'\xHH..'}。
     * {@code sourceColumnType} 需带宽度（COLUMN_TYPE，如 tinyint(1)/bit(8)），裸 DATA_TYPE 无法区分 tinyint(1)。
     */
    @Override
    public String convertLiteral(String rawValue, String sourceColumnType) {
        if (rawValue == null) return rawValue;
        String trimmed = rawValue.trim();
        if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) return rawValue;
        String t = (sourceColumnType == null) ? "" : sourceColumnType.toLowerCase();

        // tinyint(1) / bool(ean) → PG boolean
        if (t.contains("tinyint(1)") || t.startsWith("bool")) {
            String v = stripSqlQuotes(trimmed);
            if (v.isEmpty()) return rawValue;
            return "0".equals(v) ? "false" : "true";
        }
        // bit(N) → PG bytea（把整数值转为 ceil(N/8) 字节的十六进制）
        if (t.startsWith("bit")) {
            String v = stripSqlQuotes(trimmed);
            try {
                long num;
                String vv = v.toLowerCase();
                if (vv.startsWith("0x")) {
                    num = Long.parseLong(vv.substring(2), 16);
                } else if (vv.startsWith("b'") && vv.endsWith("'")) {
                    num = Long.parseLong(vv.substring(2, vv.length() - 1), 2);
                } else {
                    num = Long.parseLong(vv);
                }
                int bits = 1;
                java.util.regex.Matcher m = java.util.regex.Pattern.compile("bit\\((\\d+)\\)").matcher(t);
                if (m.find()) bits = Integer.parseInt(m.group(1));
                int nbytes = Math.max(1, (bits + 7) / 8);
                StringBuilder hex = new StringBuilder();
                for (int b = nbytes - 1; b >= 0; b--) {
                    hex.append(String.format("%02x", (num >> (b * 8)) & 0xFF));
                }
                return "E'\\\\x" + hex + "'";
            } catch (NumberFormatException e) {
                return rawValue;
            }
        }
        return rawValue;
    }

    private String stripSqlQuotes(String v) {
        if (v.length() >= 2 && v.startsWith("'") && v.endsWith("'")) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }
}
