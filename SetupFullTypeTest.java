import java.sql.*;
import java.util.UUID;
public class SetupFullTypeTest {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");

        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();

            s.execute("DROP TABLE IF EXISTS full_type_test CASCADE");
            s.execute("DROP PUBLICATION IF EXISTS full_type_test_pub");

            s.execute("CREATE TABLE full_type_test (" +
                "id SERIAL PRIMARY KEY," +
                "col_smallint SMALLINT," +
                "col_integer INTEGER," +
                "col_bigint BIGINT," +
                "col_decimal DECIMAL(10,4)," +
                "col_numeric NUMERIC(12,2)," +
                "col_real REAL," +
                "col_double DOUBLE PRECISION," +
                "col_varchar VARCHAR(100)," +
                "col_char CHAR(10)," +
                "col_text TEXT," +
                "col_boolean BOOLEAN," +
                "col_date DATE," +
                "col_timestamp TIMESTAMP," +
                "col_timestamptz TIMESTAMP WITH TIME ZONE," +
                "col_time TIME," +
                "col_timetz TIME WITH TIME ZONE," +
                "col_json JSON," +
                "col_jsonb JSONB," +
                "col_uuid UUID," +
                "col_bytea BYTEA," +
                "col_interval INTERVAL" +
                ")");

            System.out.println("Created full_type_test table");

            String insertSql = "INSERT INTO full_type_test (" +
                "col_smallint, col_integer, col_bigint, col_decimal, col_numeric, " +
                "col_real, col_double, col_varchar, col_char, col_text, " +
                "col_boolean, col_date, col_timestamp, col_timestamptz, col_time, " +
                "col_timetz, col_json, col_jsonb, col_uuid, col_bytea, col_interval" +
                ") VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?)";

            PreparedStatement ps = c.prepareStatement(insertSql);

            ps.setShort(1, (short) 32767);
            ps.setInt(2, 2147483647);
            ps.setLong(3, 9223372036854775807L);
            ps.setBigDecimal(4, new java.math.BigDecimal("12345.6789"));
            ps.setBigDecimal(5, new java.math.BigDecimal("99999999.99"));
            ps.setFloat(6, 3.14f);
            ps.setDouble(7, 2.718281828459045);
            ps.setString(8, "Hello PostgreSQL");
            ps.setString(9, "ABC");
            ps.setString(10, "This is a long text field for testing");
            ps.setBoolean(11, true);
            ps.setDate(12, java.sql.Date.valueOf("2026-04-25"));
            ps.setTimestamp(13, java.sql.Timestamp.valueOf("2026-04-25 10:30:45"));
            ps.setTimestamp(14, java.sql.Timestamp.valueOf("2026-04-25 10:30:45"));
            ps.setTime(15, java.sql.Time.valueOf("14:30:00"));
            ps.setTime(16, java.sql.Time.valueOf("14:30:00"));
            ps.setObject(17, "{\"name\": \"test\", \"value\": 123}", java.sql.Types.OTHER);
            ps.setObject(18, "{\"active\": true, \"count\": 42}", java.sql.Types.OTHER);
            ps.setObject(19, UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), java.sql.Types.OTHER);
            ps.setBytes(20, "Hello".getBytes("UTF-8"));
            ps.setObject(21, "1 year 2 months 3 days", java.sql.Types.OTHER);
            ps.executeUpdate();
            System.out.println("Inserted row 1 (positive values)");

            ps.setShort(1, (short) -32768);
            ps.setInt(2, -2147483648);
            ps.setLong(3, -9223372036854775808L);
            ps.setBigDecimal(4, new java.math.BigDecimal("-99999.9999"));
            ps.setBigDecimal(5, new java.math.BigDecimal("-99999999.99"));
            ps.setFloat(6, -3.14f);
            ps.setDouble(7, -2.718281828459045);
            ps.setString(8, "Negative Values");
            ps.setString(9, "XYZ");
            ps.setString(10, "Another text with special chars: <>&\"'");
            ps.setBoolean(11, false);
            ps.setDate(12, java.sql.Date.valueOf("2000-01-01"));
            ps.setTimestamp(13, java.sql.Timestamp.valueOf("2000-01-01 00:00:00"));
            ps.setTimestamp(14, java.sql.Timestamp.valueOf("2000-01-01 00:00:00"));
            ps.setTime(15, java.sql.Time.valueOf("00:00:00"));
            ps.setTime(16, java.sql.Time.valueOf("00:00:00"));
            ps.setObject(17, "{\"empty\": false}", java.sql.Types.OTHER);
            ps.setObject(18, "{\"nested\": {\"key\": \"val\"}}", java.sql.Types.OTHER);
            ps.setObject(19, UUID.fromString("00000000-0000-0000-0000-000000000000"), java.sql.Types.OTHER);
            ps.setBytes(20, null);
            ps.setObject(21, "0 days", java.sql.Types.OTHER);
            ps.executeUpdate();
            System.out.println("Inserted row 2 (negative values)");

            ps.setShort(1, (short) 0);
            ps.setInt(2, 0);
            ps.setLong(3, 0L);
            ps.setBigDecimal(4, java.math.BigDecimal.ZERO);
            ps.setBigDecimal(5, java.math.BigDecimal.ZERO);
            ps.setFloat(6, 0.0f);
            ps.setDouble(7, 0.0);
            ps.setString(8, "");
            ps.setString(9, "");
            ps.setString(10, "");
            ps.setNull(11, java.sql.Types.BOOLEAN);
            ps.setNull(12, java.sql.Types.DATE);
            ps.setNull(13, java.sql.Types.TIMESTAMP);
            ps.setNull(14, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
            ps.setNull(15, java.sql.Types.TIME);
            ps.setNull(16, java.sql.Types.TIME_WITH_TIMEZONE);
            ps.setNull(17, java.sql.Types.OTHER);
            ps.setNull(18, java.sql.Types.OTHER);
            ps.setNull(19, java.sql.Types.OTHER);
            ps.setNull(20, java.sql.Types.BINARY);
            ps.setNull(21, java.sql.Types.OTHER);
            ps.executeUpdate();
            System.out.println("Inserted row 3 (null/zero values)");

            ResultSet rs = s.executeQuery("SELECT count(*) FROM full_type_test");
            if (rs.next()) System.out.println("Total rows: " + rs.getInt(1));
            rs.close();
        }

        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.10:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();
            s.execute("DROP TABLE IF EXISTS full_type_test CASCADE");
            System.out.println("Cleared target DB full_type_test table");
        }

        try (Connection c = DriverManager.getConnection("jdbc:postgresql://192.168.107.4:5432/myapp_db?stringtype=unspecified", "app_user", "userpassword")) {
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'migration_slot_%'");
            java.util.List<String> slots = new java.util.ArrayList<>();
            while (rs.next()) slots.add(rs.getString(1));
            rs.close();
            for (String slot : slots) {
                try {
                    s.execute("SELECT pg_drop_replication_slot('" + slot + "')");
                    System.out.println("Dropped slot: " + slot);
                } catch (Exception e) {
                    System.out.println("Failed to drop slot " + slot + ": " + e.getMessage());
                }
            }
            rs = s.executeQuery("SELECT pubname FROM pg_publication WHERE pubname LIKE 'migration_pub_%'");
            java.util.List<String> pubs = new java.util.ArrayList<>();
            while (rs.next()) pubs.add(rs.getString(1));
            rs.close();
            for (String pub : pubs) {
                try {
                    s.execute("DROP PUBLICATION \"" + pub + "\"");
                    System.out.println("Dropped publication: " + pub);
                } catch (Exception e) {
                    System.out.println("Failed to drop pub " + pub + ": " + e.getMessage());
                }
            }
        }

        System.out.println("Setup complete!");
    }
}
