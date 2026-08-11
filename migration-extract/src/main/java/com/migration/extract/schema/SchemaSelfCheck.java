package com.migration.extract.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 语法覆盖度自检：对同步范围内每张表 {@code SHOW CREATE TABLE} → 解析 → 与
 * {@code information_schema} 逐列比对。
 *
 * <p>这是阶段 1 的验收方式，也是整个时序库方案的第一道量化闸门。之所以能这么检，
 * 是因为基线和后续 DDL 走的是<b>同一条解析路径</b>（见 {@link CreateTableParser}）——
 * 拿源库真实的几百张表把 CREATE TABLE 语法过一遍，等于用生产数据证明语法够用，
 * 而不是靠单测覆盖率自我安慰。
 *
 * <p>比对项就是下游真正依赖的那几样：列名与顺序、{@code DATA_TYPE}、{@code COLUMN_TYPE}、
 * 主键列序、生成列、enum/set 取值表。查询写法刻意与
 * {@code MySQLBinlogExtractor.getTableColumns/getTableColumnTypes/getTablePrimaryKeys}
 * 保持一致——自检通过就意味着时序库能原样顶替它们。
 */
public class SchemaSelfCheck {

    private static final Logger logger = LoggerFactory.getLogger(SchemaSelfCheck.class);

    private final CreateTableParser parser = new CreateTableParser();

    /** 单表结果。{@code parseError} 非空表示压根没解析出来，{@code mismatches} 非空表示解析出来但对不上。 */
    public static class TableResult {
        public final String database;
        public final String table;
        public final String parseError;
        public final List<String> mismatches;

        TableResult(String database, String table, String parseError, List<String> mismatches) {
            this.database = database;
            this.table = table;
            this.parseError = parseError;
            this.mismatches = mismatches;
        }

        public boolean isOk() {
            return parseError == null && mismatches.isEmpty();
        }

        public String qualified() {
            return database + "." + table;
        }
    }

    public static class Result {
        public final List<TableResult> tables = new ArrayList<>();

        public boolean allPassed() {
            return tables.stream().allMatch(TableResult::isOk);
        }

        public int passed() {
            return (int) tables.stream().filter(TableResult::isOk).count();
        }

        public int failed() {
            return tables.size() - passed();
        }

        /** 人读的报告：只列失败项，通过的只给个计数——几百张表全打出来没人看。 */
        public String report() {
            StringBuilder sb = new StringBuilder();
            sb.append("表结构语法自检: ").append(passed()).append(" 通过 / ")
                    .append(failed()).append(" 失败 / 共 ").append(tables.size()).append(" 张表");
            for (TableResult t : tables) {
                if (t.isOk()) {
                    continue;
                }
                sb.append("\n  ✗ ").append(t.qualified());
                if (t.parseError != null) {
                    sb.append(" 解析失败: ").append(t.parseError);
                }
                for (String m : t.mismatches) {
                    sb.append("\n      ").append(m);
                }
            }
            return sb.toString();
        }
    }

    /**
     * @param conn             源库连接
     * @param qualifiedTables  {@code db.table} 清单
     */
    public Result run(Connection conn, List<String> qualifiedTables) {
        TypeRenderMode mode = TypeRenderMode.forServerVersion(serverVersion(conn));
        logger.info("表结构语法自检开始：{} 张表，显示宽度口径={}", qualifiedTables.size(), mode);

        Result result = new Result();
        for (String qualified : qualifiedTables) {
            int dot = qualified.indexOf('.');
            if (dot <= 0 || dot == qualified.length() - 1) {
                result.tables.add(new TableResult(qualified, "", "表名格式不是 db.table", List.of()));
                continue;
            }
            result.tables.add(checkTable(conn, qualified.substring(0, dot),
                    qualified.substring(dot + 1), mode));
        }
        return result;
    }

    private TableResult checkTable(Connection conn, String db, String table, TypeRenderMode mode) {
        String createSql;
        try {
            createSql = showCreateTable(conn, db, table);
        } catch (SQLException e) {
            return new TableResult(db, table, "SHOW CREATE TABLE 失败: " + e.getMessage(), List.of());
        }
        if (createSql == null) {
            return new TableResult(db, table, "SHOW CREATE TABLE 没有返回结果（表不存在或是视图）", List.of());
        }

        TableSchema parsed;
        try {
            parsed = parser.parse(createSql, db);
        } catch (DdlParseException e) {
            return new TableResult(db, table, e.getMessage(), List.of());
        }
        if (parsed.isUnusable()) {
            return new TableResult(db, table, "结构推不出来: " + parsed.getUnusableReason(), List.of());
        }

        try {
            return new TableResult(db, table, null, compare(parsed, readLive(conn, db, table), mode));
        } catch (SQLException e) {
            return new TableResult(db, table, "读 information_schema 失败: " + e.getMessage(), List.of());
        }
    }

    /**
     * {@code information_schema} 里那一份，作为比对基准。
     *
     * <p>单列出来是为了让 {@link #compare} 变成<b>不碰 JDBC 的纯函数</b>——比对规则才是这里最容易
     * 写错、也最值得单测的部分，不该为了测它去起一个真库（本仓库的单测一律不连外部库）。
     */
    public static class LiveSchema {
        public final List<String> columns = new ArrayList<>();
        public final List<String> dataTypes = new ArrayList<>();
        public final List<String> columnTypes = new ArrayList<>();
        public final List<String> generated = new ArrayList<>();
        public final List<String> primaryKey = new ArrayList<>();
    }

    /** 查询写法与 {@code MySQLBinlogExtractor} 里那三段完全一致——自检通过才等于"能顶替它"。 */
    private LiveSchema readLive(Connection conn, String db, String table) throws SQLException {
        LiveSchema live = new LiveSchema();
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, EXTRA FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, db);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    live.columns.add(name);
                    live.dataTypes.add(rs.getString("DATA_TYPE"));
                    live.columnTypes.add(rs.getString("COLUMN_TYPE"));
                    String extra = rs.getString("EXTRA");
                    if (extra != null && extra.toUpperCase(Locale.ROOT).contains("GENERATED")) {
                        live.generated.add(name);
                    }
                }
            }
        }

        String pkSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' "
                + "ORDER BY ORDINAL_POSITION";
        try (PreparedStatement ps = conn.prepareStatement(pkSql)) {
            ps.setString(1, db);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    live.primaryKey.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        return live;
    }

    /** 逐列比对。每条差异都要说清"解析出的是什么、库里是什么"，否则补语法时无从下手。 */
    public List<String> compare(TableSchema parsed, LiveSchema live, TypeRenderMode mode) {
        List<String> diffs = new ArrayList<>();

        List<String> parsedColumns = parsed.columnNames();
        if (!parsedColumns.equals(live.columns)) {
            diffs.add("列名/顺序不一致: 解析=" + parsedColumns + " 库里=" + live.columns);
            return diffs;   // 列都对不齐，后面逐列比对没有意义
        }

        List<String> parsedDataTypes = parsed.dataTypes();
        List<String> parsedColumnTypes = parsed.columnTypes(mode);
        for (int i = 0; i < live.columns.size(); i++) {
            if (!parsedDataTypes.get(i).equalsIgnoreCase(live.dataTypes.get(i))) {
                diffs.add("列 " + live.columns.get(i) + " 的 DATA_TYPE 不一致: 解析="
                        + parsedDataTypes.get(i) + " 库里=" + live.dataTypes.get(i));
            }
            if (!parsedColumnTypes.get(i).equalsIgnoreCase(live.columnTypes.get(i))) {
                diffs.add("列 " + live.columns.get(i) + " 的 COLUMN_TYPE 不一致: 解析="
                        + parsedColumnTypes.get(i) + " 库里=" + live.columnTypes.get(i));
            }
        }

        if (!parsed.generatedColumns().equals(live.generated)) {
            diffs.add("生成列不一致: 解析=" + parsed.generatedColumns() + " 库里=" + live.generated);
        }

        if (!parsed.getPrimaryKey().equals(live.primaryKey)) {
            diffs.add("主键不一致: 解析=" + parsed.getPrimaryKey() + " 库里=" + live.primaryKey);
        }

        // enum/set 取值表：binlog 给的是序号，这张表错一位下游就把 'paid' 写成 'shipped'
        Map<String, List<String>> parsedEnums = parsed.enumSetValues();
        Map<String, List<String>> liveEnums = new LinkedHashMap<>();
        for (int i = 0; i < live.columns.size(); i++) {
            String ct = live.columnTypes.get(i);
            if (ct != null && (ct.startsWith("enum(") || ct.startsWith("set("))) {
                liveEnums.put(live.columns.get(i), parseEnumSetValues(ct));
            }
        }
        if (!parsedEnums.equals(liveEnums)) {
            diffs.add("enum/set 取值表不一致: 解析=" + parsedEnums + " 库里=" + liveEnums);
        }

        return diffs;
    }

    private String showCreateTable(Connection conn, String db, String table) throws SQLException {
        // SHOW 语句不支持参数绑定，标识符只能拼——用反引号包起来并转义内部反引号
        String ref = "`" + db.replace("`", "``") + "`.`" + table.replace("`", "``") + "`";
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW CREATE TABLE " + ref)) {
            if (rs.next()) {
                return rs.getString(2);
            }
        }
        return null;
    }

    private String serverVersion(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT VERSION()")) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            logger.warn("读不到源库版本，显示宽度按 8.0.19+ 口径: {}", e.getMessage());
        }
        return null;
    }

    /**
     * 从 {@code enum('a','b')} 里切出取值表。
     *
     * <p>与 {@code MySQLBinlogExtractor.parseEnumSetValues} 同一套规则（含 {@code \\'} 转义），
     * 两边口径必须一致，否则自检会报出一堆假差异。
     */
    static List<String> parseEnumSetValues(String columnType) {
        List<String> values = new ArrayList<>();
        int start = columnType.indexOf('(');
        int end = columnType.lastIndexOf(')');
        if (start < 0 || end < 0) {
            return values;
        }
        String inner = columnType.substring(start + 1, end);
        StringBuilder sb = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '\'' && (i == 0 || inner.charAt(i - 1) != '\\')) {
                inQuote = !inQuote;
                if (!inQuote) {
                    values.add(sb.toString());
                    sb = new StringBuilder();
                }
            } else if (inQuote) {
                if (c == '\\' && i + 1 < inner.length()) {
                    char next = inner.charAt(i + 1);
                    if (next == '\'' || next == '\\') {
                        sb.append(next);
                        i++;
                    } else {
                        sb.append(c);
                    }
                } else {
                    sb.append(c);
                }
            }
        }
        return values;
    }
}
