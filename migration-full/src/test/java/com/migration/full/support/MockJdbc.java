package com.migration.full.support;

import com.migration.config.DatabaseConfig;
import com.migration.db.DatabaseConnection;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * 全量链路单测用的 JDBC mock 层：把 {@link Connection} 及其下游
 * （{@link Statement}/{@link PreparedStatement}/{@link ResultSet}/{@link ResultSetMetaData}）
 * 全部换成 Mockito mock，使 MetadataReader → SchemaMigration → DataMigration
 * 能在<b>不连任何数据库</b>的前提下跑与线上完全相同的代码路径。
 *
 * <p><b>为什么不用 Mockito 逐方法 when/thenReturn</b>：JDBC 的读取面太宽
 * （getString/getObject/getBigDecimal/getTimestamp/getBytes 各有 int 与 String 两组重载），
 * 逐个 stub 既冗长又容易漏。这里给每个 mock 装一个统一的 {@link org.mockito.stubbing.Answer}，
 * 按方法名分派到下面的行数据模型上——仍然是 Mockito mock，但一处维护即可覆盖全部重载。
 *
 * <p><b>口径边界（重要）</b>：mock 出来的是"驱动会返回什么"，所以本层只能守住
 * <i>拿到这些元数据/值之后</i>我们自己的映射与搬运逻辑；驱动本身如何上报
 * （例如 MySQL {@code tinyInt1isBit} 对 tinyint(1) 报 BIT 还是 TINYINT）不在此覆盖范围内，
 * 那部分由 test_scripts 下的真库端到端脚本负责。各 {@code Rows} 的构造处都注明了所依据的驱动口径。
 */
public final class MockJdbc {

    /** 一段查询结果：列名 + JDBC 类型码 + 驱动上报的类型名 + 行数据。 */
    public static final class Rows {
        private final List<String> columnNames = new ArrayList<>();
        private final List<Integer> columnTypes = new ArrayList<>();
        private final List<String> columnTypeNames = new ArrayList<>();
        private final List<Object[]> data = new ArrayList<>();

        /** 声明一列：{@code sqlType} 取 {@link java.sql.Types}，{@code typeName} 为驱动上报的类型名。 */
        public Rows column(String name, int sqlType, String typeName) {
            columnNames.add(name);
            columnTypes.add(sqlType);
            columnTypeNames.add(typeName);
            return this;
        }

        /** 元数据类查询（SHOW TABLES/DESCRIBE 等）用的简写：一律按 VARCHAR 列声明。 */
        public Rows textColumn(String name) {
            return column(name, java.sql.Types.VARCHAR, "VARCHAR");
        }

        public Rows row(Object... values) {
            if (values.length != columnNames.size()) {
                throw new IllegalArgumentException(
                        "行宽 " + values.length + " 与列数 " + columnNames.size() + " 不一致");
            }
            data.add(values);
            return this;
        }

        public List<Object[]> data() {
            return data;
        }
    }

    /** 一条被目标端执行掉的语句：DDL 走 {@code execute}，数据走 INSERT 绑定。 */
    public static final class Recorded {
        private final List<String> executedSql = new ArrayList<>();
        private final List<String> preparedSql = new ArrayList<>();
        private final Map<String, List<Object[]>> batchedRows = new LinkedHashMap<>();

        public List<String> executedSql() {
            return executedSql;
        }

        public List<String> preparedSql() {
            return preparedSql;
        }

        /** 取唯一一条 CREATE TABLE 语句；没有或多于一条都直接失败，避免断言落到错误的语句上。 */
        public String soleCreateTable() {
            List<String> found = executedSql.stream()
                    .filter(s -> s.toUpperCase().startsWith("CREATE TABLE"))
                    .toList();
            if (found.size() != 1) {
                throw new AssertionError("期望恰好 1 条 CREATE TABLE，实际 " + found.size() + " 条: " + found);
            }
            return found.get(0);
        }

        /** 所有 addBatch 进来的行（按 INSERT 语句聚合后展平，顺序即写入顺序）。 */
        public List<Object[]> insertedRows() {
            List<Object[]> all = new ArrayList<>();
            batchedRows.forEach((sql, rows) -> {
                if (sql.toUpperCase().startsWith("INSERT")) {
                    all.addAll(rows);
                }
            });
            return all;
        }

        public String soleInsertSql() {
            List<String> found = preparedSql.stream()
                    .filter(s -> s.toUpperCase().startsWith("INSERT"))
                    .distinct()
                    .toList();
            if (found.size() != 1) {
                throw new AssertionError("期望恰好 1 条 INSERT，实际 " + found.size() + " 条: " + found);
            }
            return found.get(0);
        }
    }

    /** 一个 mock 出来的库端：按 SQL 匹配规则返回预置结果，并记录所有写入。 */
    public static final class FakeDatabase {
        private final List<Predicate<String>> matchers = new ArrayList<>();
        private final List<Rows> results = new ArrayList<>();
        private final Recorded recorded = new Recorded();
        private final Connection connection;

        private FakeDatabase() {
            this.connection = mockConnection(this);
        }

        /** 注册一段查询结果：SQL 包含 {@code fragment}（忽略大小写）时命中。 */
        public FakeDatabase onQuery(String fragment, Rows rows) {
            String needle = fragment.toLowerCase();
            matchers.add(sql -> sql.toLowerCase().contains(needle));
            results.add(rows);
            return this;
        }

        public Recorded recorded() {
            return recorded;
        }

        public Connection connection() {
            return connection;
        }

        private Rows resolve(String sql) {
            for (int i = 0; i < matchers.size(); i++) {
                if (matchers.get(i).test(sql)) {
                    return results.get(i);
                }
            }
            // 没注册的查询返回空结果集而不是抛错：被测代码里有若干"查不到就走默认分支"的路径，
            // 抛错会把这些正常分支变成测试失败。
            return new Rows().textColumn("empty");
        }
    }

    public static FakeDatabase database() {
        return new FakeDatabase();
    }

    /**
     * 用 Mockito spy 包住真实的 {@link DatabaseConnection}，只把 {@code getConnection()} 换成 mock 连接。
     * 这样 {@code execute()}/{@code close()} 等仍走真实实现，被测覆盖面不缩水。
     */
    public static DatabaseConnection databaseConnection(DatabaseConfig config, FakeDatabase db) {
        DatabaseConnection spy = spy(new DatabaseConnection(config));
        try {
            doReturn(db.connection()).when(spy).getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return spy;
    }

    // ==================== mock 装配 ====================

    private static Connection mockConnection(FakeDatabase db) {
        return mock(Connection.class, invocation -> {
            switch (invocation.getMethod().getName()) {
                case "createStatement":
                    return mockStatement(db);
                case "prepareStatement": {
                    String sql = (String) invocation.getArgument(0);
                    db.recorded.preparedSql.add(sql);
                    return mockPreparedStatement(db, sql);
                }
                case "isClosed":
                    return false;
                case "getMetaData":
                    return mock(java.sql.DatabaseMetaData.class);
                case "toString":
                    return "MockConnection";
                default:
                    return defaultValue(invocation.getMethod().getReturnType());
            }
        });
    }

    private static Statement mockStatement(FakeDatabase db) {
        return mock(Statement.class, invocation -> {
            String name = invocation.getMethod().getName();
            switch (name) {
                case "executeQuery":
                    return mockResultSet(db.resolve((String) invocation.getArgument(0)));
                case "execute":
                    db.recorded.executedSql.add((String) invocation.getArgument(0));
                    return false;
                case "executeUpdate":
                    db.recorded.executedSql.add((String) invocation.getArgument(0));
                    return 0;
                case "isClosed":
                    return false;
                case "toString":
                    return "MockStatement";
                default:
                    return defaultValue(invocation.getMethod().getReturnType());
            }
        });
    }

    private static PreparedStatement mockPreparedStatement(FakeDatabase db, String sql) {
        // 当前 setXxx 绑定的一行；addBatch/executeUpdate 时结算
        List<Object> binding = new ArrayList<>();
        // 本 statement 尚未 executeBatch 的行——结果码必须按"本批"而不是"累计"返回，
        // 否则多批场景下 BatchWriter 的计数会被放大
        List<Object[]> pending = new ArrayList<>();
        return mock(PreparedStatement.class, invocation -> {
            String name = invocation.getMethod().getName();
            if (name.startsWith("set") && invocation.getArguments().length >= 2
                    && invocation.getArgument(0) instanceof Integer) {
                int idx = (Integer) invocation.getArgument(0);
                while (binding.size() < idx) {
                    binding.add(null);
                }
                binding.set(idx - 1, invocation.getArgument(1));
                return null;
            }
            switch (name) {
                case "addBatch": {
                    Object[] row = binding.toArray();
                    pending.add(row);
                    db.recorded.batchedRows
                            .computeIfAbsent(sql, k -> new ArrayList<>())
                            .add(row);
                    return null;
                }
                case "executeBatch": {
                    // 驱动开启批量语句重写后返回的就是 SUCCESS_NO_INFO，这里照实模拟
                    int[] codes = new int[pending.size()];
                    java.util.Arrays.fill(codes, Statement.SUCCESS_NO_INFO);
                    pending.clear();
                    return codes;
                }
                case "clearBatch":
                    pending.clear();
                    return null;
                case "executeUpdate": {
                    db.recorded.batchedRows
                            .computeIfAbsent(sql, k -> new ArrayList<>())
                            .add(binding.toArray());
                    return 1;
                }
                case "executeQuery":
                    return mockResultSet(db.resolve(sql));
                case "isClosed":
                    return false;
                case "toString":
                    return "MockPreparedStatement[" + sql + "]";
                default:
                    return defaultValue(invocation.getMethod().getReturnType());
            }
        });
    }

    private static ResultSet mockResultSet(Rows rows) {
        int[] cursor = {-1};
        boolean[] lastWasNull = {false};
        return mock(ResultSet.class, invocation -> {
            String name = invocation.getMethod().getName();
            Object[] args = invocation.getArguments();

            switch (name) {
                case "next":
                    cursor[0]++;
                    return cursor[0] < rows.data.size();
                case "getMetaData":
                    return mockResultSetMetaData(rows);
                case "wasNull":
                    return lastWasNull[0];
                case "isClosed":
                    return false;
                case "close":
                    return null;
                case "toString":
                    return "MockResultSet";
                default:
                    break;
            }

            if (!name.startsWith("get") || args.length != 1) {
                return defaultValue(invocation.getMethod().getReturnType());
            }

            Object raw = valueAt(rows, cursor[0], args[0]);
            lastWasNull[0] = (raw == null);
            return coerce(name, raw);
        });
    }

    private static ResultSetMetaData mockResultSetMetaData(Rows rows) {
        return mock(ResultSetMetaData.class, invocation -> {
            String name = invocation.getMethod().getName();
            if ("getColumnCount".equals(name)) {
                return rows.columnNames.size();
            }
            if (invocation.getArguments().length == 1 && invocation.getArgument(0) instanceof Integer) {
                int i = (Integer) invocation.getArgument(0) - 1;
                switch (name) {
                    case "getColumnName":
                    case "getColumnLabel":
                        return rows.columnNames.get(i);
                    case "getColumnType":
                        return rows.columnTypes.get(i);
                    case "getColumnTypeName":
                        return rows.columnTypeNames.get(i);
                    default:
                        break;
                }
            }
            return defaultValue(invocation.getMethod().getReturnType());
        });
    }

    // ==================== 取值与类型换算 ====================

    private static Object valueAt(Rows rows, int cursor, Object columnRef) {
        if (cursor < 0 || cursor >= rows.data.size()) {
            throw new IllegalStateException("ResultSet 游标越界（是否漏调 next()）: " + cursor);
        }
        Object[] row = rows.data.get(cursor);
        int idx;
        if (columnRef instanceof Integer) {
            idx = (Integer) columnRef - 1;
        } else {
            idx = rows.columnNames.indexOf(String.valueOf(columnRef));
            if (idx < 0) {
                throw new IllegalArgumentException("未知列名: " + columnRef + "，已声明: " + rows.columnNames);
            }
        }
        return row[idx];
    }

    /** 按 getter 名把原始值换算成该 getter 的返回类型，模拟驱动的隐式转换。 */
    private static Object coerce(String getter, Object raw) {
        switch (getter) {
            case "getString":
                return raw == null ? null : String.valueOf(raw);
            case "getInt":
                return raw == null ? 0 : ((Number) raw).intValue();
            case "getLong":
                return raw == null ? 0L : ((Number) raw).longValue();
            case "getShort":
                return raw == null ? (short) 0 : ((Number) raw).shortValue();
            case "getDouble":
                return raw == null ? 0d : ((Number) raw).doubleValue();
            case "getFloat":
                return raw == null ? 0f : ((Number) raw).floatValue();
            case "getBoolean":
                if (raw == null) {
                    return false;
                }
                return raw instanceof Boolean ? raw : ((Number) raw).intValue() != 0;
            case "getBigDecimal":
                if (raw == null) {
                    return null;
                }
                return raw instanceof BigDecimal ? raw : new BigDecimal(String.valueOf(raw));
            case "getTimestamp":
                if (raw == null) {
                    return null;
                }
                return raw instanceof Timestamp ? raw : Timestamp.valueOf(String.valueOf(raw));
            case "getBytes":
                return raw;
            case "getObject":
            default:
                return raw;
        }
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == double.class) {
            return 0d;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == char.class) {
            return (char) 0;
        }
        return null;
    }

    private MockJdbc() {
    }
}
