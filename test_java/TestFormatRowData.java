import java.util.*;
public class TestFormatRowData {
    public static void main(String[] args) {
        List<String> values = Arrays.asList(
            "7", "12345", "999888777", "1234567890123456789", "5555.1234",
            "12345.67", "1.414", "3.141592653589793", "Incremental Sync Test", "PG        ",
            "Testing incremental sync", "true", "2026-12-31", "2026-12-31 23:59:59",
            "2026-12-31 23:59:59+08", "23:59:59", "23:59:59+08",
            "{\"sync\": \"incremental\"}", "{\"test\": true}",
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "\\x42696e61727944617461", "2 years 6 mons"
        );
        
        List<String> columnTypes = Arrays.asList(
            "integer", "smallint", "integer", "bigint", "numeric", "numeric",
            "real", "double precision", "character varying", "character", "text",
            "boolean", "date", "timestamp without time zone", "timestamp with time zone",
            "time without time zone", "time with time zone", "json", "jsonb",
            "uuid", "bytea", "interval"
        );
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(",");
            String value = values.get(i);
            String type = (i < columnTypes.size()) ? columnTypes.get(i) : "";
            
            if (value == null) {
                sb.append("NULL");
            } else if (isStringType(type)) {
                sb.append("'").append(escapeString(value)).append("'");
            } else if (isNumericType(type)) {
                sb.append(value.isEmpty() ? "NULL" : value);
            } else if (isBooleanType(type)) {
                sb.append(value.isEmpty() ? "NULL" : value);
            } else if (isDatetimeType(type)) {
                if (value.isEmpty()) {
                    sb.append("NULL");
                } else {
                    sb.append("'").append(value).append("'");
                }
            } else {
                sb.append("'").append(escapeString(value)).append("'");
            }
        }
        
        String result = sb.toString();
        System.out.println("formatRowData result:");
        System.out.println(result);
        System.out.println();
        
        String[] parts = result.split(",");
        System.out.println("Number of comma-separated values: " + parts.length);
        for (int i = 0; i < parts.length; i++) {
            System.out.println("  [" + i + "] " + parts[i]);
        }
    }
    
    private static boolean isStringType(String type) {
        if (type == null) return true;
        String lower = type.toLowerCase();
        return lower.contains("char") || lower.contains("text") || lower.contains("varchar") ||
                lower.contains("uuid") || lower.contains("xml") || lower.contains("json") ||
                lower.contains("bit") || lower.contains("bytea") || lower.contains("interval") ||
                lower.contains("money") || lower.contains("macaddr") || lower.contains("inet") ||
                lower.contains("cidr") || lower.equals("character varying") || lower.equals("character");
    }
    
    private static boolean isNumericType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.contains("integer") || lower.contains("bigint") ||
                lower.contains("smallint") || lower.contains("serial") ||
                lower.contains("bigserial") || lower.equals("int") ||
                lower.equals("int4") || lower.equals("int8") ||
                lower.equals("int2") || lower.equals("oid");
    }
    
    private static boolean isBooleanType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("boolean");
    }
    
    private static boolean isDatetimeType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.contains("timestamp") || lower.contains("date") || lower.contains("time");
    }
    
    private static String escapeString(String value) {
        if (value == null) return "";
        return value.replace("'", "''").replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }
}
