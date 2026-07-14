import java.util.*;
public class TestSplitTuple {
    public static void main(String[] args) {
        String tupleStr = "id:7,col_smallint:12345,col_json:'{\"sync\": \"incremental\"}',col_jsonb:'{\"test\": true}'";
        List<String> parts = splitTupleParts(tupleStr);
        for (int i = 0; i < parts.size(); i++) {
            System.out.println("Part " + i + ": " + parts.get(i));
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
