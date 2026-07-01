import java.sql.*;
import java.util.*;
public class VerifyFullSync {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection src = DriverManager.getConnection("jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword");
             Connection tgt = DriverManager.getConnection("jdbc:postgresql://192.168.107.10:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            
            String[] tables = {"full_type_test", "user_table", "test_sync"};
            boolean allMatch = true;
            
            for (String table : tables) {
                System.out.println("\n=== Table: " + table + " ===");
                
                try (Statement ss = src.createStatement(); Statement ts = tgt.createStatement()) {
                    ResultSet srcRs = ss.executeQuery("SELECT * FROM " + table + " ORDER BY id");
                    ResultSet tgtRs = ts.executeQuery("SELECT * FROM " + table + " ORDER BY id");
                    ResultSetMetaData meta = srcRs.getMetaData();
                    int colCount = meta.getColumnCount();
                    
                    int srcCount = 0, tgtCount = 0, matchCount = 0, mismatchCount = 0;
                    
                    while (srcRs.next() && tgtRs.next()) {
                        srcCount++;
                        tgtCount++;
                        boolean rowMatch = true;
                        StringBuilder diff = new StringBuilder();
                        
                        for (int i = 1; i <= colCount; i++) {
                            String colName = meta.getColumnName(i);
                            String srcVal = srcRs.getString(i);
                            String tgtVal = tgtRs.getString(i);
                            
                            if (srcVal == null && tgtVal == null) continue;
                            if (srcVal == null || tgtVal == null || !srcVal.equals(tgtVal)) {
                                rowMatch = false;
                                if (diff.length() > 0) diff.append(", ");
                                diff.append(colName).append(": src=").append(srcVal).append(" tgt=").append(tgtVal);
                            }
                        }
                        
                        if (rowMatch) {
                            matchCount++;
                        } else {
                            mismatchCount++;
                            System.out.println("  MISMATCH id=" + srcRs.getString("id") + " - " + diff);
                        }
                    }
                    
                    while (srcRs.next()) srcCount++;
                    while (tgtRs.next()) tgtCount++;
                    
                    System.out.println("  Source rows: " + srcCount + ", Target rows: " + tgtCount);
                    System.out.println("  Matched: " + matchCount + ", Mismatched: " + mismatchCount);
                    
                    if (srcCount != tgtCount || mismatchCount > 0) {
                        allMatch = false;
                    }
                } catch (Exception e) {
                    System.out.println("  ERROR: " + e.getMessage());
                    allMatch = false;
                }
            }
            
            System.out.println("\n=== Full Sync Result: " + (allMatch ? "ALL MATCH" : "MISMATCH FOUND") + " ===");
        }
    }
}
