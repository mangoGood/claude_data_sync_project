import java.sql.*;
public class ResetSourceData {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();
            
            s.executeUpdate("DELETE FROM full_type_test WHERE id >= 4");
            System.out.println("Deleted incremental rows from full_type_test");
            
            s.executeUpdate("UPDATE full_type_test SET " +
                "col_smallint = 100, " +
                "col_integer = 1000000, " +
                "col_bigint = 9000000000, " +
                "col_decimal = 1234.56, " +
                "col_numeric = 99999.99, " +
                "col_real = 1.5, " +
                "col_double = 2.71828, " +
                "col_varchar = 'Hello World', " +
                "col_char = 'A', " +
                "col_text = 'Sample text', " +
                "col_boolean = true, " +
                "col_json = '{\"key\": \"value\"}', " +
                "col_jsonb = '{\"num\": 42}' " +
                "WHERE id = 1");
            System.out.println("Reset full_type_test id=1");
            
            s.executeUpdate("DELETE FROM user_table WHERE id >= 200");
            System.out.println("Deleted incremental rows from user_table");
            
            s.executeUpdate("UPDATE test_sync SET name = 'bob' WHERE id = 2");
            System.out.println("Reset test_sync id=2");
            
            s.executeUpdate("INSERT INTO test_sync (id, name, created_at) VALUES (3, 'charlie', NOW()) ON CONFLICT (id) DO NOTHING");
            System.out.println("Restored test_sync id=3");
            
            System.out.println("Source data reset done!");
        }
    }
}
