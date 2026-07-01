import java.sql.*;
import java.util.*;
public class VerifyFullTypeSync {
    static String SRC_URL = "jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified";
    static String TGT_URL = "jdbc:postgresql://192.168.107.10:5432/myapp_db?stringtype=unspecified";
    static String USER = "app_user", PASS = "userpassword";

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        int totalChecks = 0, passedChecks = 0, failedChecks = 0;

        try (Connection srcConn = DriverManager.getConnection(SRC_URL, USER, PASS);
             Connection tgtConn = DriverManager.getConnection(TGT_URL, USER, PASS)) {

            System.out.println("========================================");
            System.out.println("  PG 全类型全量同步数据一致性验证");
            System.out.println("========================================\n");

            String[] tables = {"full_type_test", "user_table", "test_sync"};
            for (String table : tables) {
                System.out.println("--- " + table + " ---");
                long srcCount = getRowCount(srcConn, table);
                long tgtCount = getRowCount(tgtConn, table);
                boolean countMatch = srcCount == tgtCount;
                totalChecks++;
                if (countMatch) passedChecks++; else failedChecks++;
                System.out.println("  行数: 源=" + srcCount + " 目标=" + tgtCount + (countMatch ? " ✅" : " ❌"));
            }

            System.out.println("\n========================================");
            System.out.println("  full_type_test 逐行逐列对比");
            System.out.println("========================================\n");

            Statement srcStmt = srcConn.createStatement();
            Statement tgtStmt = tgtConn.createStatement();
            ResultSet srcRs = srcStmt.executeQuery("SELECT * FROM full_type_test ORDER BY id");
            ResultSet tgtRs = tgtStmt.executeQuery("SELECT * FROM full_type_test ORDER BY id");

            ResultSetMetaData md = srcRs.getMetaData();
            int colCount = md.getColumnCount();
            String[] colNames = new String[colCount + 1];
            String[] colTypes = new String[colCount + 1];
            for (int i = 1; i <= colCount; i++) {
                colNames[i] = md.getColumnName(i);
                colTypes[i] = md.getColumnTypeName(i);
            }

            int rowNum = 0;
            while (srcRs.next() && tgtRs.next()) {
                rowNum++;
                System.out.println("Row " + rowNum + ":");
                for (int i = 1; i <= colCount; i++) {
                    String colName = colNames[i];
                    String colType = colTypes[i];
                    String srcVal = getCellValue(srcRs, i, colType);
                    String tgtVal = getCellValue(tgtRs, i, colType);
                    
                    boolean match;
                    if (srcVal == null && tgtVal == null) {
                        match = true;
                    } else if (srcVal == null || tgtVal == null) {
                        match = false;
                    } else if (colType.equals("float4") || colType.equals("float8")) {
                        match = Math.abs(Double.parseDouble(srcVal) - Double.parseDouble(tgtVal)) < 0.001;
                    } else if (colType.equals("bytea")) {
                        match = compareBytea(srcVal, tgtVal);
                    } else {
                        match = srcVal.trim().equals(tgtVal.trim());
                    }
                    
                    totalChecks++;
                    if (match) passedChecks++; else failedChecks++;
                    
                    String status = match ? "✅" : "❌";
                    if (!match) {
                        System.out.println("  " + colName + " (" + colType + "): src=[" + srcVal + "] tgt=[" + tgtVal + "] " + status);
                    } else {
                        System.out.println("  " + colName + " (" + colType + "): " + (srcVal == null ? "NULL" : srcVal) + " " + status);
                    }
                }
                System.out.println();
            }

            srcRs.close(); tgtRs.close();

            System.out.println("========================================");
            System.out.println("  验证结果汇总");
            System.out.println("========================================");
            System.out.println("  总检查项: " + totalChecks);
            System.out.println("  通过: " + passedChecks);
            System.out.println("  失败: " + failedChecks);
            System.out.println("  通过率: " + (totalChecks > 0 ? String.format("%.1f%%", 100.0 * passedChecks / totalChecks) : "N/A"));
        }
    }

    static long getRowCount(Connection conn, String table) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            return rs.next() ? rs.getLong(1) : -1;
        }
    }

    static String getCellValue(ResultSet rs, int col, String type) throws SQLException {
        Object obj = rs.getObject(col);
        if (obj == null) return null;
        if (type.equals("bytea")) {
            byte[] bytes = rs.getBytes(col);
            if (bytes == null) return null;
            return "HEX:" + bytesToHex(bytes);
        }
        return obj.toString();
    }

    static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b & 0xFF));
        return sb.toString();
    }

    static boolean compareBytea(String src, String tgt) {
        if (src == null && tgt == null) return true;
        if (src == null || tgt == null) return false;
        return src.equals(tgt);
    }
}
