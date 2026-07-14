import java.util.*;
public class TestParseTupleFull {
    public static void main(String[] args) {
        String tupleStr = "id:7,col_smallint:12345,col_integer:999888777,col_bigint:1234567890123456789,col_decimal:5555.1234,col_numeric:12345.67,col_real:1.414,col_double:3.141592653589793,col_varchar:'Incremental Sync Test',col_char:'PG        ',col_text:'Testing incremental sync',col_boolean:true,col_date:'2026-12-31',col_timestamp:'2026-12-31 23:59:59',col_timestamptz:'2026-12-31 23:59:59+08',col_time:'23:59:59',col_timetz:'23:59:59+08',col_json:'{\"sync\": \"incremental\"}',col_jsonb:'{\"test\": true}',col_uuid:'a1b2c3d4-e5f6-7890-abcd-ef1234567890',col_bytea:'\\x42696e61727944617461',col_interval:'2 years 6 mons'";
        
        List<String> parts = splitTupleParts(tupleStr);
        System.out.println("Total parts: " + parts.size());
        for (int i = 0; i < parts.size(); i++) {
            String part = parts.get(i);
            String value = part.trim();
            int colonIdx = value.indexOf(':');
            if (colonIdx >= 0) {
                value = value.substring(colonIdx + 1).trim();
            }
            if (value.startsWith("[null]")) {
                value = null;
            } else if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            System.out.println("  [" + i + "] " + parts.get(i).trim() + " => " + value);
        }
    }
    
    private static List<String> splitTupleParts(String tupleStr) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inQuote = false;

        for (int i = 0; i < tupleStr.length(); i++) {
            char c = tupleStr.charAt(i);
            if (c == '\'') {
                if (inQuote && i + 1 < tupleStr.length() && tupleStr.charAt(i + 1) == '\'') {
                    current.append("''");
                    i++;
                } else {
                    inQuote = !inQuote;
                    current.append(c);
                }
            } else if (!inQuote && c == '(') {
                depth++;
                current.append(c);
            } else if (!inQuote && c == ')') {
                depth--;
                current.append(c);
            } else if (!inQuote && c == ',' && depth == 0) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts;
    }
}
