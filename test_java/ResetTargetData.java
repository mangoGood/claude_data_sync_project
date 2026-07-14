import java.sql.*;
public class ResetTargetData {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.10:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();
            String[] tables = {"full_type_test", "user_table", "test_sync"};
            for (String table : tables) {
                try {
                    s.executeUpdate("DROP TABLE IF EXISTS " + table + " CASCADE");
                    System.out.println("Dropped target table: " + table);
                } catch (Exception e) {
                    System.out.println("Failed to drop " + table + ": " + e.getMessage());
                }
            }
            System.out.println("Target data reset done!");
        }
    }
}
