import java.sql.*;
import java.util.UUID;
public class IncrementDmlTest {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();

            System.out.println("=== INSERT: 全类型新行 ===");
            String insertSql = "INSERT INTO full_type_test (" +
                "col_smallint, col_integer, col_bigint, col_decimal, col_numeric, " +
                "col_real, col_double, col_varchar, col_char, col_text, " +
                "col_boolean, col_date, col_timestamp, col_timestamptz, col_time, " +
                "col_timetz, col_json, col_jsonb, col_uuid, col_bytea, col_interval" +
                ") VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?)";
            PreparedStatement ps = c.prepareStatement(insertSql);
            ps.setShort(1, (short) 12345);
            ps.setInt(2, 999888777);
            ps.setLong(3, 1234567890123456789L);
            ps.setBigDecimal(4, new java.math.BigDecimal("5555.1234"));
            ps.setBigDecimal(5, new java.math.BigDecimal("12345.67"));
            ps.setFloat(6, 1.414f);
            ps.setDouble(7, 3.141592653589793);
            ps.setString(8, "Incremental Sync Test 增量同步测试");
            ps.setString(9, "PG");
            ps.setString(10, "Testing incremental sync with special chars: \t\n<>&\"'");
            ps.setBoolean(11, true);
            ps.setDate(12, java.sql.Date.valueOf("2026-12-31"));
            ps.setTimestamp(13, java.sql.Timestamp.valueOf("2026-12-31 23:59:59"));
            ps.setTimestamp(14, java.sql.Timestamp.valueOf("2026-12-31 23:59:59"));
            ps.setTime(15, java.sql.Time.valueOf("23:59:59"));
            ps.setTime(16, java.sql.Time.valueOf("23:59:59"));
            ps.setObject(17, "{\"sync\": \"incremental\", \"types\": [\"json\", \"jsonb\"]}", java.sql.Types.OTHER);
            ps.setObject(18, "{\"test\": true, \"data\": [1, 2, 3]}", java.sql.Types.OTHER);
            ps.setObject(19, UUID.fromString("a1b2c3d4-e5f6-7890-abcd-ef1234567890"), java.sql.Types.OTHER);
            ps.setBytes(20, "BinaryData".getBytes("UTF-8"));
            ps.setObject(21, "2 years 6 months", java.sql.Types.OTHER);
            ps.executeUpdate();
            System.out.println("INSERT row 4 (incremental test data)");

            System.out.println("\n=== UPDATE: 修改全类型行 ===");
            s.executeUpdate("UPDATE full_type_test SET " +
                "col_smallint = -999, " +
                "col_integer = -111, " +
                "col_bigint = -9876543210, " +
                "col_decimal = -1111.2222, " +
                "col_numeric = -33333.44, " +
                "col_real = -9.99, " +
                "col_double = -8.888888, " +
                "col_varchar = 'UPDATED varchar 值', " +
                "col_char = 'UPD', " +
                "col_text = 'Updated text with 中文 and emoji 🎉', " +
                "col_boolean = false, " +
                "col_date = '1999-12-31', " +
                "col_timestamp = '1999-12-31 11:11:11', " +
                "col_timestamptz = '1999-12-31 11:11:11', " +
                "col_time = '11:11:11', " +
                "col_timetz = '11:11:11', " +
                "col_json = '{\"updated\": true}', " +
                "col_jsonb = '{\"version\": 2}', " +
                "col_uuid = 'ffffffff-ffff-ffff-ffff-ffffffffffff', " +
                "col_bytea = E'\\\\x44617461', " +
                "col_interval = '3 days 4 hours' " +
                "WHERE id = 1");
            System.out.println("UPDATE row id=1 (all columns)");

            System.out.println("\n=== DELETE: 删除行 ===");
            s.executeUpdate("DELETE FROM full_type_test WHERE id = 3");
            System.out.println("DELETE row id=3 (null/zero values row)");

            System.out.println("\n=== INSERT: 另一张表增量测试 ===");
            s.executeUpdate("INSERT INTO user_table (id, username, email, age, address) VALUES (200, 'incr_user', 'incr@test.com', 35, '增量同步测试地址')");
            System.out.println("INSERT user_table id=200");

            System.out.println("\n=== UPDATE: 另一张表 ===");
            s.executeUpdate("UPDATE test_sync SET name = 'updated_by_increment' WHERE id = 2");
            System.out.println("UPDATE test_sync id=2");

            System.out.println("\n=== DELETE: 另一张表 ===");
            s.executeUpdate("DELETE FROM test_sync WHERE id = 3");
            System.out.println("DELETE test_sync id=3");

            System.out.println("\nAll incremental DML executed!");
        }
    }
}
