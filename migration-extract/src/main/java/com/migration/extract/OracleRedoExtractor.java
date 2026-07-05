package com.migration.extract;

import com.migration.common.AbstractExtractor;
import com.migration.thl.THLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Oracle Redo Log 提取器。
 *
 * <p>解析 {@link com.migration.capture.OracleRedoCapture} 输出的捕获事件，
 * 将其转换为统一的 {@link THLEvent} 格式。
 *
 * <p>输入事件格式：
 * <pre>
 *   eventType \001 scn \001 scnNumeric \001 timestamp \001 xid \001 eventData \n
 * </pre>
 * 其中 eventData 格式为：
 * <pre>
 *   schema:OWNER table:TABLE_NAME primary_keys:COL1,COL2 old-tuple:{...} new-tuple:{...}
 * </pre>
 */
public class OracleRedoExtractor extends AbstractExtractor<byte[], THLEvent> {

    private static final Logger logger = LoggerFactory.getLogger(OracleRedoExtractor.class);

    private static final char FIELD_SEP = '\001';

    private String outputDir;
    private long seqno = 1;
    private String seqnoFile;

    private String sourceHost;
    private int sourcePort;
    private String sourceDatabase;
    private String sourceUser;
    private String sourcePassword;
    private Connection sourceConnection;

    private final Map<String, List<String>> tableSchemaCache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumnTypeCache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> primaryKeyCache = new ConcurrentHashMap<>();

    private String checkpointScn;
    private long checkpointScnNumeric;
    private boolean skipBeforeCheckpoint = false;

    @Override
    protected void doInitialize() throws Exception {
        outputDir = props.getProperty("extract.output.dir", "thl_output");
        seqnoFile = outputDir + "/.extractor_seqno";

        sourceHost = props.getProperty("source.db.host", "localhost");
        sourcePort = Integer.parseInt(props.getProperty("source.db.port", "1521"));
        sourceDatabase = props.getProperty("source.db.database", "ORCL");
        sourceUser = props.getProperty("source.db.username", "SYSTEM");
        sourcePassword = props.getProperty("source.db.password", "");

        checkpointScn = props.getProperty("checkpoint.redo.scn", "");
        checkpointScnNumeric = Long.parseLong(props.getProperty("checkpoint.redo.position", "0"));
        skipBeforeCheckpoint = Boolean.parseBoolean(props.getProperty("extract.skip.before.checkpoint", "false"));

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        loadSeqno();
        connectToSourceDatabase();

        logger.info("Oracle Redo Extractor initialized - source: {}:{}/{}, output: {}, seqno: {}, skipBeforeCheckpoint: {}",
                sourceHost, sourcePort, sourceDatabase, outputDir, seqno, skipBeforeCheckpoint);
    }

    private void connectToSourceDatabase() throws SQLException {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            logger.warn("Oracle JDBC driver not found, trying default driver loading");
        }

        String url = String.format("jdbc:oracle:thin:@%s:%d/%s",
                sourceHost, sourcePort, sourceDatabase);
        sourceConnection = DriverManager.getConnection(url, sourceUser, sourcePassword);
        logger.info("Connected to Oracle source database: {}:{}/{}", sourceHost, sourcePort, sourceDatabase);
    }

    @Override
    protected THLEvent doExtract(byte[] input) throws Exception {
        String eventStr = new String(input, StandardCharsets.UTF_8);
        if (eventStr.trim().isEmpty()) {
            return null;
        }

        String[] fields = eventStr.split(String.valueOf(FIELD_SEP));
        if (fields.length < 5) {
            logger.warn("Invalid Oracle redo event format, skipping: {}",
                    eventStr.substring(0, Math.min(100, eventStr.length())));
            return null;
        }

        String eventType = fields[0].trim();
        String scn = fields[1].trim();
        long scnNumeric = 0;
        try {
            scnNumeric = Long.parseLong(fields[2].trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid SCN numeric in event: {}", fields[2]);
        }
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(fields[3].trim());
        } catch (NumberFormatException e) {
            timestamp = System.currentTimeMillis();
        }
        long xid = 0;
        try {
            xid = Long.parseLong(fields[4].trim());
        } catch (NumberFormatException e) {
            // ignore
        }

        // 跳过 checkpoint 之前的事件
        if (skipBeforeCheckpoint && checkpointScnNumeric > 0) {
            if (scnNumeric > 0 && scnNumeric < checkpointScnNumeric) {
                return null;
            }
        }

        String eventData = fields.length > 5 ? fields[5] : "";

        THLEvent thlEvent = new THLEvent();
        thlEvent.setSeqno(seqno++);
        thlEvent.setEventId(scn);
        thlEvent.setSourceId("oracle");
        thlEvent.setSourceTstamp(new Timestamp(timestamp));
        thlEvent.addMetadata("event_type", eventType);
        thlEvent.addMetadata("redo_scn", scn);
        thlEvent.addMetadata("redo_scn_numeric", scnNumeric);
        thlEvent.addMetadata("xid", xid);

        if ("INSERT".equals(eventType)) {
            parseInsertEvent(thlEvent, eventData);
        } else if ("UPDATE".equals(eventType)) {
            parseUpdateEvent(thlEvent, eventData);
        } else if ("DELETE".equals(eventType)) {
            parseDeleteEvent(thlEvent, eventData);
        } else {
            thlEvent.addMetadata("operation", eventType);
            thlEvent.addMetadata("raw_data", eventData);
        }

        return thlEvent;
    }

    private void parseInsertEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "INSERT");

        RedoRowData rowData = parseRedoRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);

            if (rowData.newValues != null && !rowData.newValues.isEmpty()) {
                // 按 new-tuple 实际列对齐类型与列名，避免部分列 INSERT 时类型错位（字符串未加引号导致语法错误）
                List<String> insertCols = extractTupleColumnNames(
                        extractBracedContent(eventData, "new-tuple:"), rowData.columnNames, rowData.newValues.size());
                List<String> insertTypes = (insertCols != null)
                        ? alignTypesToColumns(insertCols, rowData.columnNames, rowData.columnTypes)
                        : rowData.columnTypes;
                String formattedRow = formatRowData(rowData.newValues, rowData.columnNames, insertTypes);
                thlEvent.addMetadata("row_data", formattedRow);
                if (insertCols != null && !insertCols.isEmpty()) {
                    thlEvent.addMetadata("insert_column_names", String.join(",", insertCols));
                }
                attachTypedRow(thlEvent, "rows_typed", rowData.newValues, insertTypes);

                List<String> allRows = new ArrayList<>();
                allRows.add(formattedRow);
                thlEvent.addMetadata("rows_data", allRows);
            } else if (rowData.sqlRedo != null) {
                thlEvent.addMetadata("sql_redo", rowData.sqlRedo);
            }
        }
    }

    private void parseUpdateEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "UPDATE");

        RedoRowData rowData = parseRedoRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);

            if (rowData.newValues != null && !rowData.newValues.isEmpty()) {
                // UPDATE 的 new-tuple 通常只含变更列：先取列名，再按列对齐类型，避免字符串列被当数字而漏加引号
                List<String> changedCols = extractTupleColumnNames(
                        extractBracedContent(eventData, "new-tuple:"), rowData.columnNames, rowData.newValues.size());
                List<String> newTypes = (changedCols != null)
                        ? alignTypesToColumns(changedCols, rowData.columnNames, rowData.columnTypes)
                        : rowData.columnTypes;
                String formattedNew = formatRowData(rowData.newValues, rowData.columnNames, newTypes);
                thlEvent.addMetadata("row_data", formattedNew);
                if (changedCols != null && !changedCols.isEmpty()) {
                    thlEvent.addMetadata("update_column_names", String.join(",", changedCols));
                }
                attachTypedRow(thlEvent, "rows_typed", rowData.newValues, newTypes);
            }
            if (rowData.oldValues != null && !rowData.oldValues.isEmpty()) {
                List<String> beforeCols = extractTupleColumnNames(
                        extractBracedContent(eventData, "old-tuple:"), rowData.columnNames, rowData.oldValues.size());
                List<String> oldTypes = (beforeCols != null)
                        ? alignTypesToColumns(beforeCols, rowData.columnNames, rowData.columnTypes)
                        : rowData.columnTypes;
                String formattedOld = formatRowData(rowData.oldValues, rowData.columnNames, oldTypes);
                thlEvent.addMetadata("row_data_before", formattedOld);
                if (beforeCols != null && !beforeCols.isEmpty()) {
                    thlEvent.addMetadata("update_before_column_names", String.join(",", beforeCols));
                }
                attachTypedRow(thlEvent, "rows_before_typed", rowData.oldValues, oldTypes);
            }
            if (rowData.newValues == null && rowData.oldValues == null && rowData.sqlRedo != null) {
                thlEvent.addMetadata("sql_redo", rowData.sqlRedo);
            }
        }
    }

    private void parseDeleteEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "DELETE");

        RedoRowData rowData = parseRedoRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);

            if (rowData.oldValues != null && !rowData.oldValues.isEmpty()) {
                List<String> delCols = extractTupleColumnNames(
                        extractBracedContent(eventData, "old-tuple:"), rowData.columnNames, rowData.oldValues.size());
                List<String> delTypes = (delCols != null)
                        ? alignTypesToColumns(delCols, rowData.columnNames, rowData.columnTypes)
                        : rowData.columnTypes;
                String formattedRow = formatRowData(rowData.oldValues, rowData.columnNames, delTypes);
                thlEvent.addMetadata("row_data", formattedRow);
                attachTypedRow(thlEvent, "rows_typed", rowData.oldValues, delTypes);
            } else if (rowData.sqlRedo != null) {
                thlEvent.addMetadata("sql_redo", rowData.sqlRedo);
            }
        }
    }

    private void populateTableMetadata(THLEvent thlEvent, RedoRowData rowData) {
        if (rowData.schemaName != null) {
            thlEvent.addMetadata("database_name", rowData.schemaName);
        }
        if (rowData.tableName != null) {
            thlEvent.addMetadata("table_name", rowData.tableName);
        }
        if (rowData.columnNames != null && !rowData.columnNames.isEmpty()) {
            thlEvent.addMetadata("column_names", String.join(",", rowData.columnNames));
        }
        if (rowData.columnTypes != null && !rowData.columnTypes.isEmpty()) {
            thlEvent.addMetadata("oracle_column_types", String.join(",", rowData.columnTypes));
            thlEvent.addMetadata("mysql_column_types", String.join(",", rowData.columnTypes));
        }

        if (rowData.primaryKeys != null && !rowData.primaryKeys.isEmpty()) {
            thlEvent.addMetadata("primary_keys", String.join(",", rowData.primaryKeys));
        } else {
            String cacheKey = (rowData.schemaName != null ? rowData.schemaName : "") + "." +
                              (rowData.tableName != null ? rowData.tableName : "");
            List<String> pkColumns = primaryKeyCache.get(cacheKey);
            if (pkColumns != null && !pkColumns.isEmpty()) {
                thlEvent.addMetadata("primary_keys", String.join(",", pkColumns));
            }
        }
    }

    /**
     * 解析 Oracle redo 事件数据。
     * eventData 格式：schema:OWNER table:TABLE_NAME primary_keys:COL1,COL2 old-tuple:{...} new-tuple:{...}
     */
    private RedoRowData parseRedoRowEvent(String eventData) {
        RedoRowData rowData = new RedoRowData();

        java.util.regex.Matcher schemaMatcher = java.util.regex.Pattern.compile(
                "schema:\\s*([^\\s]+)").matcher(eventData);
        if (schemaMatcher.find()) {
            rowData.schemaName = schemaMatcher.group(1);
        }

        java.util.regex.Matcher tableMatcher = java.util.regex.Pattern.compile(
                "table:\\s*([^\\s]+)").matcher(eventData);
        if (tableMatcher.find()) {
            rowData.tableName = tableMatcher.group(1);
        }

        java.util.regex.Matcher pkMatcher = java.util.regex.Pattern.compile(
                "primary_keys:\\s*([^\\s]+)").matcher(eventData);
        if (pkMatcher.find()) {
            String pkStr = pkMatcher.group(1);
            if (pkStr != null && !pkStr.isEmpty()) {
                rowData.primaryKeys = new ArrayList<>();
                for (String pk : pkStr.split(",")) {
                    rowData.primaryKeys.add(pk.trim());
                }
            }
        }

        if (rowData.schemaName == null) {
            rowData.schemaName = sourceUser != null ? sourceUser.toUpperCase() : "SYSTEM";
        }

        resolveTableSchema(rowData);

        String newTupleContent = extractBracedContent(eventData, "new-tuple:");
        if (newTupleContent != null) {
            rowData.newValues = parseTupleData(newTupleContent, rowData.columnNames);
        }

        String oldTupleContent = extractBracedContent(eventData, "old-tuple:");
        if (oldTupleContent != null) {
            rowData.oldValues = parseTupleData(oldTupleContent, rowData.columnNames);
        }

        // 如果没有解析出 new/old tuple，尝试从 sql_redo 字段提取
        if (rowData.newValues == null && rowData.oldValues == null) {
            java.util.regex.Matcher sqlMatcher = java.util.regex.Pattern.compile(
                    "sql_redo:\\s*(.+)$", java.util.regex.Pattern.DOTALL).matcher(eventData);
            if (sqlMatcher.find()) {
                rowData.sqlRedo = sqlMatcher.group(1).trim();
            }
        }

        return rowData;
    }

    /**
     * 从事件数据中提取指定前缀后的花括号内容，正确处理嵌套大括号和引号。
     */
    private String extractBracedContent(String eventData, String prefix) {
        int prefixIdx = eventData.indexOf(prefix);
        if (prefixIdx < 0) return null;

        int start = prefixIdx + prefix.length();
        while (start < eventData.length() && Character.isWhitespace(eventData.charAt(start))) {
            start++;
        }
        if (start >= eventData.length() || eventData.charAt(start) != '{') {
            return null;
        }
        start++; // 跳过 '{'

        int depth = 1;
        boolean inQuote = false;
        int i = start;

        while (i < eventData.length()) {
            char c = eventData.charAt(i);
            if (inQuote) {
                if (c == '\\' && i + 1 < eventData.length()) {
                    i += 2;
                    continue;
                }
                if (c == '\'') {
                    inQuote = false;
                }
            } else {
                if (c == '\'') {
                    inQuote = true;
                } else if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                    if (depth == 0) {
                        return eventData.substring(start, i);
                    }
                }
            }
            i++;
        }
        return null;
    }

    /**
     * 解析 tuple 数据，格式为 col1:val1,col2:val2,...
     */
    private List<String> parseTupleData(String tupleStr, List<String> columnNames) {
        List<String> values = new ArrayList<>();
        if (tupleStr == null || tupleStr.isEmpty()) {
            return values;
        }

        List<String> parts = splitTupleParts(tupleStr);
        for (String part : parts) {
            String value = part.trim();
            int colonIdx = value.indexOf(':');
            if (colonIdx >= 0) {
                value = value.substring(colonIdx + 1).trim();
            }

            if (value.startsWith("[null]")) {
                values.add(null);
            } else if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
                values.add(value.substring(1, value.length() - 1));
            } else if ("NULL".equalsIgnoreCase(value)) {
                values.add(null);
            } else if ("EMPTY_CLOB()".equalsIgnoreCase(value) || "EMPTY_BLOB()".equalsIgnoreCase(value)) {
                // 空 LOB：Oracle SQL_REDO 渲染为 EMPTY_CLOB()/EMPTY_BLOB()，对目标按 NULL 处理
                values.add(null);
            } else {
                values.add(value);
            }
        }

        return values;
    }

    /**
     * 从 new-tuple/old-tuple 内容中提取列名列表。
     * 例如 "NAME:'incre_update'" 返回 ["NAME"]。
     * 如果 tuple 不含列名（无冒号分隔），返回 null 以便调用方回退到全表 columnNames。
     */
    private List<String> extractTupleColumnNames(String tupleStr, List<String> fallbackColumnNames, int expectedSize) {
        if (tupleStr == null || tupleStr.isEmpty()) {
            return null;
        }
        List<String> parts = splitTupleParts(tupleStr);
        boolean hasColumnNames = false;
        List<String> names = new ArrayList<>();
        for (String part : parts) {
            String token = part.trim();
            int colonIdx = token.indexOf(':');
            if (colonIdx > 0) {
                String candidate = token.substring(0, colonIdx).trim();
                // 列名通常由字母/数字/下划线组成，且不是引号包裹的字符串字面值
                if (!candidate.isEmpty() && !candidate.startsWith("'")
                        && candidate.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                    hasColumnNames = true;
                    names.add(candidate);
                } else {
                    names.clear();
                    hasColumnNames = false;
                    break;
                }
            } else {
                // 无冒号分隔，说明 tuple 不带列名
                names.clear();
                break;
            }
        }
        if (!hasColumnNames || names.size() != expectedSize) {
            return null;
        }
        return names;
    }

    /**
     * 按 tuple 实际出现的列，从全表 columnNames/columnTypes 取出对应类型，
     * 保证 formatRowData 中“值↔类型”对齐（部分列 INSERT/UPDATE 时不会把字符串当数字而漏加引号）。
     */
    private List<String> alignTypesToColumns(List<String> tupleCols, List<String> fullCols, List<String> fullTypes) {
        List<String> aligned = new ArrayList<>();
        for (String col : tupleCols) {
            String type = "";
            if (fullCols != null && fullTypes != null) {
                int idx = fullCols.indexOf(col);
                if (idx >= 0 && idx < fullTypes.size()) {
                    type = fullTypes.get(idx);
                }
            }
            aligned.add(type);
        }
        return aligned;
    }

    /**
     * 类型化值管道：把 redo tuple 文本值按（已对齐的）Oracle 列类型转为类型化 Java 值并挂到事件元数据，
     * 供增量端 PreparedStatement 参数绑定执行。任一值无法可靠类型化则整行放弃（回退文本路径）。
     */
    private void attachTypedRow(com.migration.thl.THLEvent thlEvent, String key,
                                List<String> values, List<String> types) {
        ArrayList<Object> typed = typeTupleValues(values, types);
        if (typed != null) {
            ArrayList<ArrayList<Object>> rows = new ArrayList<>();
            rows.add(typed);
            thlEvent.addMetadata(key, rows);
        }
    }

    /**
     * redo tuple 值 → 类型化 Java 值：RAW/BLOB → byte[]（HEXTORAW('..')/裸十六进制），
     * 其余（NUMBER/字符串/ISO 日期时间）→ String（PG stringtype=unspecified 下由服务端按列类型推断），
     * NULL → null。出现函数包装（TO_DATE 等）或未知形态返回 null 整行回退。
     */
    private ArrayList<Object> typeTupleValues(List<String> values, List<String> types) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        ArrayList<Object> typed = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            String type = (types != null && i < types.size() && types.get(i) != null) ? types.get(i) : "";
            String upperType = type.toUpperCase();

            if (value == null) {
                typed.add(null);
                continue;
            }
            String trimmed = value.trim();
            if (isBinaryType(upperType)) {
                byte[] bytes = parseOracleBinary(trimmed);
                if (bytes == null) {
                    return null;
                }
                typed.add(bytes);
            } else if (trimmed.regionMatches(true, 0, "TO_DATE(", 0, 8)
                    || trimmed.regionMatches(true, 0, "TO_TIMESTAMP(", 0, 13)
                    || trimmed.regionMatches(true, 0, "HEXTORAW(", 0, 9)
                    || trimmed.regionMatches(true, 0, "EMPTY_", 0, 6)) {
                // 非二进制列出现函数包装：交给文本路径处理
                return null;
            } else {
                typed.add(value);
            }
        }
        return typed;
    }

    /** 解析 RAW/BLOB 值：HEXTORAW('AB01') 或裸十六进制 → byte[]；无法解析返回 null。 */
    private byte[] parseOracleBinary(String v) {
        String hex = null;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(?i)HEXTORAW\\('([0-9A-Fa-f]*)'\\)").matcher(v);
        if (m.matches()) {
            hex = m.group(1);
        } else if (v.matches("[0-9A-Fa-f]*")) {
            hex = v;
        }
        if (hex == null) {
            return null;
        }
        if (hex.length() % 2 != 0) {
            return null;
        }
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /**
     * 分割 tuple 字符串，处理嵌套括号和引号中的逗号。
     */
    private List<String> splitTupleParts(String tupleStr) {
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

    private void resolveTableSchema(RedoRowData rowData) {
        if (rowData.schemaName == null || rowData.tableName == null) return;

        String cacheKey = rowData.schemaName + "." + rowData.tableName;

        if (!tableSchemaCache.containsKey(cacheKey)) {
            List<String> columns = fetchTableColumns(rowData.schemaName, rowData.tableName);
            tableSchemaCache.put(cacheKey, columns);

            List<String> columnTypes = fetchTableColumnTypes(rowData.schemaName, rowData.tableName);
            tableColumnTypeCache.put(cacheKey, columnTypes);

            List<String> pkColumns = fetchTablePrimaryKeys(rowData.schemaName, rowData.tableName);
            primaryKeyCache.put(cacheKey, pkColumns);
        }

        rowData.columnNames = tableSchemaCache.get(cacheKey);
        rowData.columnTypes = tableColumnTypeCache.get(cacheKey);
    }

    // ==================== Oracle 元数据查询 ====================

    private List<String> fetchTableColumns(String owner, String table) {
        List<String> columns = new ArrayList<>();
        if (sourceConnection == null) return columns;

        try {
            String sql = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS " +
                    "WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, owner);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching columns for {}.{}: {}", owner, table, e.getMessage());
        }
        return columns;
    }

    private List<String> fetchTableColumnTypes(String owner, String table) {
        List<String> columnTypes = new ArrayList<>();
        if (sourceConnection == null) return columnTypes;

        try {
            String sql = "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS " +
                    "WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, owner);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columnTypes.add(rs.getString("DATA_TYPE"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching column types for {}.{}: {}", owner, table, e.getMessage());
        }
        return columnTypes;
    }

    private List<String> fetchTablePrimaryKeys(String owner, String table) {
        List<String> pkColumns = new ArrayList<>();
        if (sourceConnection == null) return pkColumns;

        try {
            String sql = "SELECT CC.COLUMN_NAME FROM ALL_CONSTRAINTS C " +
                    "JOIN ALL_CONS_COLUMNS CC ON C.OWNER = CC.OWNER " +
                    "AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME " +
                    "WHERE C.OWNER = ? AND C.TABLE_NAME = ? AND C.CONSTRAINT_TYPE = 'P' " +
                    "ORDER BY CC.POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, owner);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        pkColumns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching primary keys for {}.{}: {}", owner, table, e.getMessage());
        }
        return pkColumns;
    }

    // ==================== 行数据格式化 ====================

    private String formatRowData(List<String> values, List<String> columnNames, List<String> columnTypes) {
        if (values == null || values.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(",");
            String value = values.get(i);
            String type = (columnTypes != null && i < columnTypes.size()) ? columnTypes.get(i) : "";

            if (value == null) {
                sb.append("NULL");
            } else if (isStringType(type)) {
                sb.append("'").append(escapeString(value)).append("'");
            } else if (isNumericType(type)) {
                sb.append(value.isEmpty() ? "NULL" : value);
            } else if (isDatetimeType(type)) {
                if (value.isEmpty()) {
                    sb.append("NULL");
                } else {
                    sb.append("'").append(formatDatetimeValue(value)).append("'");
                }
            } else if (isBinaryType(type)) {
                if (value.isEmpty()) {
                    sb.append("NULL");
                } else {
                    sb.append("'").append(escapeString(value)).append("'");
                }
            } else {
                sb.append("'").append(escapeString(value)).append("'");
            }
        }
        return sb.toString();
    }

    private boolean isStringType(String type) {
        if (type == null) return true;
        String upper = type.toUpperCase();
        return upper.contains("CHAR") || upper.contains("VARCHAR") || upper.contains("VARCHAR2") ||
                upper.contains("NCHAR") || upper.contains("NVARCHAR") || upper.contains("NVARCHAR2") ||
                upper.contains("CLOB") || upper.contains("NCLOB") || upper.contains("LONG") ||
                upper.contains("XMLTYPE") || upper.contains("ROWID") || upper.contains("UROWID");
    }

    private boolean isNumericType(String type) {
        if (type == null) return false;
        String upper = type.toUpperCase();
        return upper.equals("NUMBER") || upper.equals("INTEGER") || upper.equals("INT") ||
                upper.equals("SMALLINT") || upper.equals("FLOAT") || upper.equals("BINARY_FLOAT") ||
                upper.equals("BINARY_DOUBLE") || upper.contains("PRECISION");
    }

    private boolean isDatetimeType(String type) {
        if (type == null) return false;
        String upper = type.toUpperCase();
        return upper.contains("DATE") || upper.contains("TIMESTAMP") || upper.contains("INTERVAL");
    }

    private boolean isBinaryType(String type) {
        if (type == null) return false;
        String upper = type.toUpperCase();
        return upper.equals("BLOB") || upper.equals("RAW") || upper.equals("LONG RAW") ||
               upper.equals("BFILE");
    }

    private String formatDatetimeValue(String value) {
        if (value == null || value.isEmpty()) return value;
        // Oracle 日期格式：TO_DATE('...','YYYY-MM-DD HH24:MI:SS')
        // 直接返回值，下游处理
        return value;
    }

    private String escapeString(String value) {
        if (value == null) return "";
        return value.replace("'", "''").replace("\\", "\\\\")
                .replace("\n", "\\n").replace("\r", "\\r");
    }

    // ==================== Seqno 管理 ====================

    private void loadSeqno() {
        File file = new File(seqnoFile);
        if (!file.exists()) {
            logger.info("No seqno file found, starting from 1");
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            if (line != null && !line.trim().isEmpty()) {
                seqno = Long.parseLong(line.trim()) + 1;
                logger.info("Loaded seqno from file, starting from: {}", seqno);
            }
        } catch (Exception e) {
            logger.warn("Error loading seqno file, starting from 1: {}", e.getMessage());
            seqno = 1;
        }
    }

    public void saveSeqno() {
        File file = new File(seqnoFile);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(String.valueOf(seqno - 1));
        } catch (IOException e) {
            logger.error("Error saving seqno file", e);
        }
    }

    public long getCurrentSeqno() {
        return seqno;
    }

    public void incrementSeqnoForHeartbeat() {
        seqno++;
    }

    public void close() {
        saveSeqno();
        if (sourceConnection != null) {
            try {
                sourceConnection.close();
            } catch (SQLException e) {
                logger.error("Error closing source connection", e);
            }
        }
    }

    /**
     * Oracle redo 行数据。
     */
    private static class RedoRowData {
        String schemaName;
        String tableName;
        List<String> primaryKeys;
        List<String> columnNames;
        List<String> columnTypes;
        List<String> newValues;
        List<String> oldValues;
        String sqlRedo;
    }
}
