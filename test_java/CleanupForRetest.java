import java.sql.*;
public class CleanupForRetest {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("SELECT slot_name FROM pg_replication_slots");
            java.util.List<String> slots = new java.util.ArrayList<>();
            while (rs.next()) slots.add(rs.getString(1));
            rs.close();
            for (String slot : slots) {
                try { s.execute("SELECT pg_drop_replication_slot('" + slot + "')"); System.out.println("Dropped slot: " + slot); } catch (Exception e) { System.out.println("Failed: " + e.getMessage()); }
            }
            rs = s.executeQuery("SELECT pubname FROM pg_publication");
            java.util.List<String> pubs = new java.util.ArrayList<>();
            while (rs.next()) pubs.add(rs.getString(1));
            rs.close();
            for (String pub : pubs) {
                try { s.execute("DROP PUBLICATION \"" + pub + "\""); System.out.println("Dropped pub: " + pub); } catch (Exception e) { System.out.println("Failed: " + e.getMessage()); }
            }
        }
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.10:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();
            s.execute("DROP TABLE IF EXISTS full_type_test CASCADE");
            System.out.println("Cleared target full_type_test");
        }
        System.out.println("Cleanup done!");
    }
}
