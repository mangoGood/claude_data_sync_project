package com.migration.extract;

import com.migration.common.AbstractExtractor;
import com.migration.thl.THLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresWalExtractor extends AbstractExtractor<byte[], THLEvent> {

    private static final Logger logger = LoggerFactory.getLogger(PostgresWalExtractor.class);

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

    private final Map<Long, RelationMessage> relationCache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> tableSchemaCache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumnTypeCache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> primaryKeyCache = new ConcurrentHashMap<>();

    private String checkpointLsn;
    private long checkpointLsnNumeric;
    private boolean skipBeforeCheckpoint = false;

    @Override
    protected void doInitialize() throws Exception {
        outputDir = props.getProperty("extract.output.dir", "thl_output");
        seqnoFile = outputDir + "/.extractor_seqno";

        sourceHost = props.getProperty("source.db.host", "localhost");
        sourcePort = Integer.parseInt(props.getProperty("source.db.port", "5432"));
        sourceDatabase = props.getProperty("source.db.database", "postgres");
        sourceUser = props.getProperty("source.db.username", "postgres");
        sourcePassword = props.getProperty("source.db.password", "");

        checkpointLsn = props.getProperty("checkpoint.wal.lsn", "");
        checkpointLsnNumeric = Long.parseLong(props.getProperty("checkpoint.wal.position", "0"));
        skipBeforeCheckpoint = Boolean.parseBoolean(props.getProperty("extract.skip.before.checkpoint", "false"));

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        loadSeqno();
        connectToSourceDatabase();

        logger.info("PostgreSQL WAL Extractor initialized - source: {}:{}/{}, output: {}, seqno: {}, skipBeforeCheckpoint: {}",
                sourceHost, sourcePort, sourceDatabase, outputDir, seqno, skipBeforeCheckpoint);
    }

    private void connectToSourceDatabase() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.warn("PostgreSQL JDBC driver not found, trying default driver loading");
        }

        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified",
                sourceHost, sourcePort, sourceDatabase);
        sourceConnection = DriverManager.getConnection(url, sourceUser, sourcePassword);
        logger.info("Connected to PostgreSQL source database: {}:{}/{}", sourceHost, sourcePort, sourceDatabase);
    }

    @Override
    protected THLEvent doExtract(byte[] input) throws Exception {
        String eventStr = new String(input, StandardCharsets.UTF_8);
        if (eventStr.trim().isEmpty()) {
            return null;
        }

        String[] fields = eventStr.split(String.valueOf(FIELD_SEP));
        if (fields.length < 5) {
            logger.warn("Invalid WAL event format, skipping: {}", eventStr.substring(0, Math.min(100, eventStr.length())));
            return null;
        }

        String eventType = fields[0].trim();
        String lsn = fields[1].trim();
        long lsnNumeric = 0;
        try {
            lsnNumeric = Long.parseLong(fields[2].trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid LSN numeric in event: {}", fields[2]);
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

        if (skipBeforeCheckpoint && checkpointLsnNumeric > 0) {
            if (lsnNumeric > 0 && lsnNumeric < checkpointLsnNumeric) {
                return null;
            }
        }

        String eventData = fields.length > 5 ? fields[5] : "";

        THLEvent thlEvent = new THLEvent();
        thlEvent.setSeqno(seqno++);
        thlEvent.setEventId(lsn);
        thlEvent.setSourceId("postgresql");
        thlEvent.setSourceTstamp(new Timestamp(timestamp));
        thlEvent.addMetadata("event_type", eventType);
        thlEvent.addMetadata("wal_lsn", lsn);
        thlEvent.addMetadata("wal_lsn_numeric", lsnNumeric);
        thlEvent.addMetadata("xid", xid);

        if ("BEGIN".equals(eventType)) {
            parseBeginEvent(thlEvent, eventData);
        } else if ("COMMIT".equals(eventType)) {
            parseCommitEvent(thlEvent, eventData);
        } else if ("INSERT".equals(eventType)) {
            parseInsertEvent(thlEvent, eventData);
        } else if ("UPDATE".equals(eventType)) {
            parseUpdateEvent(thlEvent, eventData);
        } else if ("DELETE".equals(eventType)) {
            parseDeleteEvent(thlEvent, eventData);
        } else if ("WAL_EVENT".equals(eventType)) {
            thlEvent.addMetadata("operation", "WAL_EVENT");
            thlEvent.addMetadata("raw_data", eventData);
        }

        return thlEvent;
    }

    private void parseBeginEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "BEGIN");
        Long xid = extractXidFromBegin(eventData);
        if (xid != null) {
            thlEvent.addMetadata("transaction_xid", xid);
        }
    }

    private void parseCommitEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "COMMIT");
        Long xid = extractXidFromCommit(eventData);
        if (xid != null) {
            thlEvent.addMetadata("transaction_xid", xid);
        }
    }

    private Long extractXidFromBegin(String eventData) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("transaction_id:\\s*(\\d+)").matcher(eventData);
        if (m.find()) return Long.parseLong(m.group(1));
        m = java.util.regex.Pattern.compile("(\\d+)").matcher(eventData);
        if (m.find()) return Long.parseLong(m.group(1));
        return null;
    }

    private Long extractXidFromCommit(String eventData) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("transaction_id:\\s*(\\d+)").matcher(eventData);
        if (m.find()) return Long.parseLong(m.group(1));
        return null;
    }

    private void parseInsertEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "INSERT");

        WalRowData rowData = parseWalRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);

            String formattedRow = formatRowData(rowData.newValues, rowData.columnNames, rowData.columnTypes);
            thlEvent.addMetadata("row_data", formattedRow);
            attachTypedRow(thlEvent, "rows_typed", rowData.newValues, rowData.columnTypes);

            if (rowData.newValues != null && rowData.newValues.size() > 1) {
                List<String> allRows = new ArrayList<>();
                allRows.add(formattedRow);
                thlEvent.addMetadata("rows_data", allRows);
            }
        }
    }

    private void parseUpdateEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "UPDATE");

        WalRowData rowData = parseWalRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);

            if (rowData.newValues != null) {
                String formattedNew = formatRowData(rowData.newValues, rowData.columnNames, rowData.columnTypes);
                thlEvent.addMetadata("row_data", formattedNew);
                attachTypedRow(thlEvent, "rows_typed", rowData.newValues, rowData.columnTypes);
            }
            if (rowData.oldValues != null) {
                String formattedOld = formatRowData(rowData.oldValues, rowData.columnNames, rowData.columnTypes);
                thlEvent.addMetadata("row_data_before", formattedOld);
                attachTypedRow(thlEvent, "rows_before_typed", rowData.oldValues, rowData.columnTypes);
            }
        }
    }

    private void parseDeleteEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "DELETE");

        WalRowData rowData = parseWalRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);

            String formattedRow = formatRowData(rowData.oldValues, rowData.columnNames, rowData.columnTypes);
            thlEvent.addMetadata("row_data", formattedRow);
            attachTypedRow(thlEvent, "rows_typed", rowData.oldValues, rowData.columnTypes);
        }
    }

    /**
     * 类型化值管道：把 WAL tuple 文本值按 PG 列类型转为类型化 Java 值挂到事件元数据，
     * 供增量端（pg→mysql）PreparedStatement 参数绑定执行。无法可靠类型化则整行放弃（回退文本路径）。
     */
    private void attachTypedRow(THLEvent thlEvent, String key, List<String> values, List<String> types) {
        ArrayList<Object> typed = typeWalValues(values, types);
        if (typed != null) {
            ArrayList<ArrayList<Object>> rows = new ArrayList<>();
            rows.add(typed);
            thlEvent.addMetadata(key, rows);
        }
    }

    /**
     * WAL tuple 值 → 类型化 Java 值：boolean → Boolean，bytea(\x十六进制) → byte[]，
     * 其余（数字/文本/ISO 时间/uuid/json 等）→ String（MySQL 预编译参数由服务端按列类型强转），
     * NULL → null。未知形态返回 null 整行回退。
     */
    private ArrayList<Object> typeWalValues(List<String> values, List<String> types) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        ArrayList<Object> typed = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            String type = (types != null && i < types.size() && types.get(i) != null)
                    ? types.get(i).toLowerCase() : "";

            if (value == null) {
                typed.add(null);
                continue;
            }
            String trimmed = value.trim();
            if (isBooleanType(type)) {
                if ("t".equalsIgnoreCase(trimmed) || "true".equalsIgnoreCase(trimmed)) {
                    typed.add(Boolean.TRUE);
                } else if ("f".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
                    typed.add(Boolean.FALSE);
                } else {
                    return null;
                }
            } else if (type.contains("bytea")) {
                String hex = trimmed.startsWith("\\x") ? trimmed.substring(2) : null;
                if (hex == null || hex.length() % 2 != 0 || !hex.matches("[0-9A-Fa-f]*")) {
                    return null;
                }
                byte[] out = new byte[hex.length() / 2];
                for (int b = 0; b < out.length; b++) {
                    out[b] = (byte) Integer.parseInt(hex.substring(b * 2, b * 2 + 2), 16);
                }
                typed.add(out);
            } else {
                typed.add(value);
            }
        }
        return typed;
    }

    private void populateTableMetadata(THLEvent thlEvent, WalRowData rowData) {
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
            thlEvent.addMetadata("pg_column_types", String.join(",", rowData.columnTypes));
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

    private WalRowData parseWalRowEvent(String eventData) {
        WalRowData rowData = new WalRowData();

        java.util.regex.Matcher schemaMatcher = java.util.regex.Pattern.compile("schema:\\s*\"?([^\",\\s]+)" + "?").matcher(eventData);
        if (schemaMatcher.find()) {
            rowData.schemaName = schemaMatcher.group(1);
        }

        java.util.regex.Matcher tableMatcher = java.util.regex.Pattern.compile("table:\\s*\"?([^\",\\s]+)" + "?").matcher(eventData);
        if (tableMatcher.find()) {
            rowData.tableName = tableMatcher.group(1);
        }

        java.util.regex.Matcher pkMatcher = java.util.regex.Pattern.compile("primary_keys:\\s*([^\\s]+)").matcher(eventData);
        if (pkMatcher.find()) {
            String pkStr = pkMatcher.group(1);
            if (pkStr != null && !pkStr.isEmpty()) {
                rowData.primaryKeys = new ArrayList<>();
                for (String pk : pkStr.split(",")) {
                    rowData.primaryKeys.add(pk.trim());
                }
            }
        }

        if (rowData.schemaName == null || rowData.tableName == null) {
            java.util.regex.Matcher relationMatcher = java.util.regex.Pattern.compile("relation:\\s*(\\S+)").matcher(eventData);
            if (relationMatcher.find()) {
                String relation = relationMatcher.group(1);
                if (relation.contains(".")) {
                    String[] parts = relation.split("\\.");
                    rowData.schemaName = parts[0].replace("\"", "");
                    rowData.tableName = parts[1].replace("\"", "");
                } else {
                    rowData.schemaName = "public";
                    rowData.tableName = relation.replace("\"", "");
                }
            }
        }

        if (rowData.schemaName == null) {
            rowData.schemaName = "public";
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

        if (rowData.newValues == null && !eventData.contains("old-tuple")) {
            String tupleContent = extractBracedContent(eventData, "tuple:");
            if (tupleContent != null) {
                rowData.newValues = parseTupleData(tupleContent, rowData.columnNames);
            }
        }

        if (rowData.newValues == null && rowData.oldValues == null) {
            rowData.newValues = parseKeyValuePairs(eventData, rowData.columnNames);
        }

        return rowData;
    }

    /**
     * 从事件数据中提取指定前缀后的花括号内容，正确处理嵌套大括号和引号。
     * 例如：对于 "new-tuple:{id:1,col_json:'{"k":"v"}'}"，
     * 返回 "id:1,col_json:'{"k":"v"}'"
     */
    private String extractBracedContent(String eventData, String prefix) {
        int prefixIdx = eventData.indexOf(prefix);
        if (prefixIdx < 0) return null;

        int start = prefixIdx + prefix.length();
        // 跳过空白
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
                    i += 2; // 跳过转义字符
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
        return null; // 未找到匹配的 '}'
    }

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
            } else if (value.startsWith("'") && value.endsWith("'")) {
                values.add(value.substring(1, value.length() - 1));
            } else {
                values.add(value);
            }
        }

        return values;
    }

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

    private List<String> parseKeyValuePairs(String eventData, List<String> columnNames) {
        List<String> values = new ArrayList<>();
        if (columnNames == null || columnNames.isEmpty()) {
            return values;
        }

        Map<String, String> keyValueMap = new LinkedHashMap<>();
        java.util.regex.Matcher kvMatcher = java.util.regex.Pattern.compile("(\\w+)\\[\\w*\\]:'([^']*)'").matcher(eventData);
        while (kvMatcher.find()) {
            keyValueMap.put(kvMatcher.group(1), kvMatcher.group(2));
        }

        if (keyValueMap.isEmpty()) {
            kvMatcher = java.util.regex.Pattern.compile("(\\w+):'([^']*)'").matcher(eventData);
            while (kvMatcher.find()) {
                keyValueMap.put(kvMatcher.group(1), kvMatcher.group(2));
            }
        }

        for (String colName : columnNames) {
            String val = keyValueMap.get(colName);
            values.add(val != null ? val : null);
        }

        return values;
    }

    private void resolveTableSchema(WalRowData rowData) {
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

    private List<String> fetchTableColumns(String schema, String table) {
        List<String> columns = new ArrayList<>();
        if (sourceConnection == null) return columns;

        try {
            String sql = "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("column_name"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching columns for {}.{}: {}", schema, table, e.getMessage());
        }
        return columns;
    }

    private List<String> fetchTableColumnTypes(String schema, String table) {
        List<String> columnTypes = new ArrayList<>();
        if (sourceConnection == null) return columnTypes;

        try {
            String sql = "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columnTypes.add(rs.getString("data_type"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching column types for {}.{}: {}", schema, table, e.getMessage());
        }
        return columnTypes;
    }

    private List<String> fetchTablePrimaryKeys(String schema, String table) {
        List<String> pkColumns = new ArrayList<>();
        if (sourceConnection == null) return pkColumns;

        try {
            String sql = "SELECT a.attname FROM pg_index i " +
                    "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                    "JOIN pg_class c ON c.oid = i.indrelid " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE i.indisprimary AND n.nspname = ? AND c.relname = ? " +
                    "ORDER BY array_position(i.indkey, a.attnum)";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        pkColumns.add(rs.getString("attname"));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching primary keys for {}.{}: {}", schema, table, e.getMessage());
        }
        return pkColumns;
    }

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
            } else if (isBooleanType(type)) {
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
        String lower = type.toLowerCase();
        return lower.contains("char") || lower.contains("text") || lower.contains("varchar") ||
                lower.contains("uuid") || lower.contains("xml") || lower.contains("json") ||
                lower.contains("bit") || lower.contains("bytea") || lower.contains("interval") ||
                lower.contains("money") || lower.contains("macaddr") || lower.contains("inet") ||
                lower.contains("cidr") || lower.equals("character varying") || lower.equals("character");
    }

    private boolean isNumericType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("integer") || lower.equals("bigint") || lower.equals("smallint") ||
                lower.equals("int") || lower.equals("real") || lower.equals("double precision") ||
                lower.equals("numeric") || lower.equals("decimal") || lower.equals("serial") ||
                lower.equals("bigserial") || lower.equals("smallserial") || lower.equals("int4") ||
                lower.equals("int8") || lower.equals("int2") || lower.equals("float4") ||
                lower.equals("float8") || lower.equals("oid");
    }

    private boolean isBooleanType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("boolean") || lower.equals("bool");
    }

    private boolean isDatetimeType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.contains("timestamp") || lower.contains("date") || lower.contains("time");
    }

    private boolean isBinaryType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("bytea") || lower.equals("blob");
    }

    private String formatDatetimeValue(String value) {
        if (value == null || value.isEmpty()) return value;
        return value;
    }

    private String escapeString(String value) {
        if (value == null) return "";
        return value.replace("'", "''").replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }

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

    private static class RelationMessage {
        long relationId;
        String schemaName;
        String tableName;
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
    }

    private static class WalRowData {
        String schemaName;
        String tableName;
        List<String> primaryKeys;
        List<String> columnNames;
        List<String> columnTypes;
        List<String> newValues;
        List<String> oldValues;
    }
}
