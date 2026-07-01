package com.migration.increment;

import com.migration.db.ConnectionPoolManager;
import com.migration.dialect.SqlDialect;
import com.migration.increment.schema.SchemaEvolutionService;
import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import com.migration.thl.THLEvent;
import com.migration.thl.THLFileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class THLToSqlConverter {
    private static final Logger logger = LoggerFactory.getLogger(THLToSqlConverter.class);

    private String inputDir;
    private String targetUrl;
    private String targetUser;
    private String targetPassword;
    private Connection targetConnection;

    private SqlClassifier sqlClassifier;
    private SqlConflictKeyParser conflictKeyParser;
    private Properties props;
    private boolean concurrentMode;
    private boolean isPostgresql;
    private boolean sourceIsPostgresql;
    private boolean sourceIsMysql;
    private boolean targetIsPostgresql;
    private SqlDialect targetDialect;
    private String targetDatabaseName;

    private SchemaEvolutionService schemaEvolutionService;

    private LinkedHashMap<Long, String> executedRecords;
    private String executedRecordFile;

    private long totalEvents = 0;
    private long successfulEvents = 0;
    private long failedEvents = 0;

    public static class SqlExecutionException extends RuntimeException {
        private final long seqno;
        private final String sql;

        public SqlExecutionException(long seqno, String sql, SQLException cause) {
            super("SQL execution failed at seqno " + seqno + ": " + cause.getMessage(), cause);
            this.seqno = seqno;
            this.sql = sql;
        }

        public long getSeqno() { return seqno; }
        public String getFailedSql() { return sql; }
    }

    public THLToSqlConverter(Properties props) {
        this.inputDir = props.getProperty("input.dir", "./thl");
        this.targetUrl = props.getProperty("target.mysql.url");
        this.targetUser = props.getProperty("target.mysql.user");
        this.targetPassword = props.getProperty("target.mysql.password");
        this.props = props;
        this.concurrentMode = "batch".equalsIgnoreCase(props.getProperty("concurrent.mode", "single"));
        this.isPostgresql = "postgresql".equalsIgnoreCase(props.getProperty("target.db.type", "mysql"));
        this.sourceIsPostgresql = "postgresql".equalsIgnoreCase(props.getProperty("source.db.type", "mysql"));
        this.sourceIsMysql = "mysql".equalsIgnoreCase(props.getProperty("source.db.type", "mysql"));
        this.targetIsPostgresql = this.isPostgresql;
        this.targetDialect = SqlDialect.forType(props.getProperty("target.db.type", "mysql"));
        this.targetDatabaseName = props.getProperty("target.db.database", "");

        this.sqlClassifier = new SqlClassifier();
        this.conflictKeyParser = new SqlConflictKeyParser();

        this.executedRecords = new LinkedHashMap<>();
        this.executedRecordFile = inputDir + "/.executed_records";

        loadExecutedRecords();
    }

    private void loadExecutedRecords() {
        File file = new File(executedRecordFile);
        if (!file.exists()) {
            logger.info("No executed records file found, starting fresh");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                int sepIdx = line.indexOf('|');
                if (sepIdx > 0) {
                    try {
                        long seqno = Long.parseLong(line.substring(0, sepIdx));
                        String sql = line.substring(sepIdx + 1);
                        executedRecords.put(seqno, sql);
                        count++;
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid record line: {}", line);
                    }
                }
            }
            logger.info("Loaded {} executed records from file", count);
            if (!executedRecords.isEmpty()) {
                Long lastSeqno = null;
                for (Long s : executedRecords.keySet()) {
                    lastSeqno = s;
                }
                logger.info("Last executed seqno: {}", lastSeqno);
            }
        } catch (IOException e) {
            logger.error("Error loading executed records", e);
        }
    }

    private void saveExecutedRecord(long seqno, String sql) {
        executedRecords.put(seqno, sql);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(executedRecordFile, true))) {
            writer.write(seqno + "|" + sql.replace("\n", "\\n"));
            writer.newLine();
        } catch (IOException e) {
            logger.error("Error saving executed record for seqno: {}", seqno, e);
        }
    }

    private void rewriteExecutedRecordsFile() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(executedRecordFile))) {
            for (Map.Entry<Long, String> entry : executedRecords.entrySet()) {
                writer.write(entry.getKey() + "|" + entry.getValue().replace("\n", "\\n"));
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("Error rewriting executed records file", e);
        }
    }

    public long getLastExecutedSeqno() {
        if (executedRecords.isEmpty()) return 0;
        Long last = null;
        for (Long s : executedRecords.keySet()) {
            last = s;
        }
        return last;
    }

    public boolean isSeqnoExecuted(long seqno) {
        return executedRecords.containsKey(seqno);
    }

    public void start() throws Exception {
        logger.info("Starting THL to SQL conversion and execution...");

        connect();

        try {
            File inputDirFile = new File(inputDir);
            File[] thlFiles = inputDirFile.listFiles((dir, name) -> name.startsWith("thl-") && name.endsWith(".thl"));

            if (thlFiles == null || thlFiles.length == 0) {
                logger.warn("No THL files found in directory: {}", inputDir);
                return;
            }

            java.util.Arrays.sort(thlFiles);

            for (File thlFile : thlFiles) {
                logger.info("Processing THL file: {}", thlFile.getName());
                processTHLFile(thlFile);
            }

            logger.info("THL to SQL conversion and execution completed");
            logger.info("Statistics - Total: {}, Successful: {}, Failed: {}",
                totalEvents, successfulEvents, failedEvents);
        } finally {
            disconnect();
        }
    }

    public void processSingleFile(File thlFile) throws Exception {
        if (targetConnection == null || targetConnection.isClosed()) {
            connect();
        }

        logger.info("Processing THL file: {}", thlFile.getName());
        processTHLFile(thlFile);
        logger.info("Statistics - Total: {}, Successful: {}, Failed: {}",
            totalEvents, successfulEvents, failedEvents);
    }

    private void connect() throws SQLException {
        String url = targetUrl;
        if (isPostgresql) {
            String jdbcUrl = props.getProperty("target.db.jdbc.url");
            if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
                url = jdbcUrl;
            }
        } else {
            if (!url.contains("serverTimezone") && !url.contains("?")) {
                url = url + "?serverTimezone=UTC&useSSL=false";
            } else if (!url.contains("serverTimezone")) {
                url = url + "&serverTimezone=UTC";
            }
        }
        targetConnection = ConnectionPoolManager.getConnection(url, targetUser, targetPassword);
        // 初始化 Schema 演进服务，用于自动应用 DDL 事件到目标库
        this.schemaEvolutionService = new SchemaEvolutionService(props, targetConnection);
        logger.info("Connected to target database (type: {}): {}", isPostgresql ? "postgresql" : "mysql", url);
    }

    public void disconnect() {
        if (targetConnection != null) {
            try {
                targetConnection.close();
                logger.info("Closed target database connection");
            } catch (SQLException e) {
                logger.error("Error closing connection", e);
            }
        }
    }

    private void processTHLFile(File thlFile) throws Exception {
        THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath());
        try {
            if (concurrentMode) {
                processTHLFileConcurrent(reader);
            } else {
                processTHLFileSequential(reader);
            }
        } finally {
            reader.close();
        }
    }

    private void processTHLFileSequential(THLFileReader reader) throws Exception {
        THLEvent event;
        while ((event = reader.readEvent()) != null) {
            totalEvents++;

            long seqno = event.getSeqno();

            if (executedRecords.containsKey(seqno)) {
                logger.debug("Skipping already executed seqno: {}", seqno);
                continue;
            }

            java.util.List<String> sqlStatements = convertToSql(event);

            if (sqlStatements == null || sqlStatements.isEmpty()) {
                continue;
            }

            int subSeqno = 0;
            for (String sql : sqlStatements) {
                String executableSql = extractExecutableSql(sql);
                if (executableSql.isEmpty()) continue;

                long statementSeqno = sqlStatements.size() == 1 ? seqno : (seqno * 1000 + subSeqno);
                subSeqno++;

                if (executedRecords.containsKey(statementSeqno)) {
                    logger.debug("Skipping already executed statement seqno: {}", statementSeqno);
                    continue;
                }

                try {
                    executeSql(executableSql);
                    logger.info("✓ [seqno={}] SQL executed successfully: {}", statementSeqno, executableSql);
                } catch (SQLException e) {
                    failedEvents++;
                    logger.error("✗ [seqno={}] SQL execution failed: {}\nError: {}", statementSeqno, executableSql, e.getMessage());
                    throw new SqlExecutionException(statementSeqno, executableSql, e);
                }

                saveExecutedRecord(statementSeqno, executableSql);
            }

            successfulEvents++;
        }
    }

    private void processTHLFileConcurrent(THLFileReader reader) throws Exception {
        List<SqlStatement> allStatements = new ArrayList<>();
        Map<Long, String> seqnoToSql = new LinkedHashMap<>();
        conflictKeyParser.reset();

        THLEvent event;
        while ((event = reader.readEvent()) != null) {
            totalEvents++;

            long seqno = event.getSeqno();

            java.util.List<String> sqlStatements = convertToSql(event);
            if (sqlStatements == null || sqlStatements.isEmpty()) continue;

            int subSeqno = 0;
            for (String sql : sqlStatements) {
                String executableSql = extractExecutableSql(sql);
                if (executableSql.isEmpty()) continue;

                long statementSeqno = sqlStatements.size() == 1 ? seqno : (seqno * 1000 + subSeqno);
                subSeqno++;

                if (executedRecords.containsKey(statementSeqno)) {
                    logger.debug("Skipping already executed statement seqno: {}", statementSeqno);
                    continue;
                }

                SqlStatement stmt = conflictKeyParser.parse(executableSql, statementSeqno);
                if (stmt != null) {
                    allStatements.add(stmt);
                    seqnoToSql.put(statementSeqno, executableSql);
                }
            }
        }

        if (allStatements.isEmpty()) {
            logger.info("No SQL statements to execute");
            return;
        }

        long dmlCount = allStatements.stream().filter(SqlStatement::isDml).count();
        long barrierCount = allStatements.stream().filter(SqlStatement::isBarrier).count();
        logger.info("Parsed {} SQL statements: {} DML (concurrent), {} barriers (DDL/DCL/COMMIT sequential)",
                allStatements.size(), dmlCount, barrierCount);

        DependencyGraph graph = new DependencyGraph(allStatements);
        ConcurrentSqlExecutor executor = new ConcurrentSqlExecutor(props);

        try {
            ConcurrentSqlExecutor.ExecutionResult result = executor.execute(graph);

            logger.info("Concurrent execution result: {}", result);

            if (result.getFailureCount() == 0) {
                for (Long seqno : seqnoToSql.keySet()) {
                    saveExecutedRecordDirectly(seqno, seqnoToSql.get(seqno));
                }
                successfulEvents += result.getSuccessCount();
            } else {
                failedEvents += result.getFailureCount();
                logger.error("Execution had {} failures, not saving progress", result.getFailureCount());
            }
        } finally {
            executor.shutdown();
        }
    }

    private String extractExecutableSql(String sql) {
        StringBuilder cleanSql = new StringBuilder();
        for (String line : sql.trim().split("\n")) {
            String trimmedLine = line.trim();
            if (!trimmedLine.startsWith("--") && !trimmedLine.isEmpty()) {
                cleanSql.append(line).append("\n");
            }
        }
        return cleanSql.toString().trim();
    }

    public java.util.List<String> convertToSql(THLEvent event) {
        java.util.List<String> sqlStatements = new java.util.ArrayList<>();

        Map<String, Object> metadata = event.getMetadata();
        String eventType = (String) metadata.get("event_type");

        if (eventType == null) {
            logger.warn("Event type not found in metadata for event seqno={}", event.getSeqno());
            return null;
        }
        
        logger.info("Converting event seqno={} type={} db={} table={}", 
            event.getSeqno(), eventType, 
            metadata.getOrDefault("database_name", ""), 
            metadata.getOrDefault("table_name", ""));

        switch (eventType) {
            case "INSERT":
            case "WRITE_ROWS":
            case "EXT_WRITE_ROWS":
                sqlStatements.addAll(generateInsertSql(event, metadata));
                break;
            case "UPDATE":
            case "UPDATE_ROWS":
            case "EXT_UPDATE_ROWS":
                sqlStatements.addAll(generateUpdateSql(event, metadata));
                break;
            case "DELETE":
            case "DELETE_ROWS":
            case "EXT_DELETE_ROWS":
                sqlStatements.addAll(generateDeleteSql(event, metadata));
                break;
            case "QUERY":
                sqlStatements.addAll(generateQuerySql(event, metadata));
                break;
            case "XID":
            case "COMMIT":
                sqlStatements.add("COMMIT;");
                break;
            default:
                logger.debug("Unsupported event type: {}", eventType);
                break;
        }

        return sqlStatements;
    }

    private java.util.List<String> generateInsertSql(THLEvent event, Map<String, Object> metadata) {
        java.util.List<String> statements = new java.util.ArrayList<>();

        String database = (String) metadata.getOrDefault("database_name", "");
        String table = (String) metadata.getOrDefault("table_name", "");

        if (database.isEmpty() || table.isEmpty()) {
            logger.warn("Database or table name not found for INSERT event");
            return statements;
        }

        if (!targetDatabaseName.isEmpty() && !targetIsPostgresql) {
            database = targetDatabaseName;
        }

        String rowDataStr = (String) metadata.get("row_data");
        if (rowDataStr == null || rowDataStr.isEmpty()) {
            logger.warn("No row data found for INSERT event");
            return statements;
        }

        String[] columnNames = getColumnNames(metadata);
        String[] columnTypes = getColumnTypes(metadata);

        // Oracle redo 的部分列 INSERT：使用 new-tuple 实际列名，保证列与 row_data 值一一对应
        String insertColsStr = (String) metadata.get("insert_column_names");
        if (insertColsStr != null && !insertColsStr.isEmpty()) {
            columnNames = insertColsStr.split(",");
        }

        // MySQL→PG：把 bool/bit 值转换为 PG 兼容字面量（tinyint(1)→true/false，bit→bytea）
        if (sourceIsMysql && targetIsPostgresql) {
            String[] fullTypes = getColumnFullTypes(metadata);
            String[] insVals = parseRowDataValues(rowDataStr);
            boolean changed = false;
            for (int i = 0; i < insVals.length; i++) {
                String type = (fullTypes != null && i < fullTypes.length) ? fullTypes[i] : "";
                String conv = convertMysqlValueToPg(insVals[i], type);
                if (!conv.equals(insVals[i])) { insVals[i] = conv; changed = true; }
            }
            if (changed) rowDataStr = String.join(",", insVals);
        }

        if (sourceIsPostgresql && !targetIsPostgresql) {
            rowDataStr = convertPgRowDataToMysql(rowDataStr, columnTypes);
        }

        StringBuilder sql = new StringBuilder();
        if (isPostgresql) {
            sql.append("INSERT INTO ").append(quoteIdentifier(table)).append(" ");
        } else {
            sql.append("INSERT INTO `").append(database).append("`.`").append(table).append("` ");
        }

        if (columnNames != null && columnNames.length > 0) {
            sql.append("(");
            for (int i = 0; i < columnNames.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(quoteIdentifier(columnNames[i]));
            }
            sql.append(") ");
        }

        sql.append("VALUES (").append(rowDataStr).append(")");

        if (isPostgresql) {
            String pkColumnsStr = (String) metadata.get("primary_keys");
            if (pkColumnsStr != null && !pkColumnsStr.isEmpty()) {
                String[] pkCols = pkColumnsStr.split(",");
                sql.append(" ON CONFLICT (");
                for (int i = 0; i < pkCols.length; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(quoteIdentifier(pkCols[i].trim()));
                }
                sql.append(") DO NOTHING");
            }
        } else {
            if (columnNames != null && columnNames.length > 0) {
                sql.append(" ON DUPLICATE KEY UPDATE ");
                boolean first = true;
                for (String col : columnNames) {
                    if (first) {
                        first = false;
                    } else {
                        sql.append(", ");
                    }
                    sql.append("`").append(col).append("` = VALUES(`").append(col).append("`)");
                }
            }
        }

        sql.append(";");
        statements.add(sql.toString());

        return statements;
    }

    private String[] getColumnNames(Map<String, Object> metadata) {
        String columnsStr = (String) metadata.get("column_names");
        if (columnsStr != null && !columnsStr.isEmpty()) {
            return columnsStr.split(",");
        }
        return null;
    }

    private String[] getColumnTypes(Map<String, Object> metadata) {
        String typesStr = (String) metadata.get("mysql_column_types");
        if (typesStr == null || typesStr.isEmpty()) {
            typesStr = (String) metadata.get("pg_column_types");
        }
        if (typesStr != null && !typesStr.isEmpty()) {
            return typesStr.split(",");
        }
        return null;
    }

    private String formatValueByType(Object value, String columnType) {
        if (value == null) {
            return "NULL";
        }
        
        if (columnType != null) {
            String lowerType = columnType.toLowerCase();

            if (isPostgresql) {
                return formatPostgresValueByType(value, lowerType);
            }

            if (sourceIsPostgresql && !targetIsPostgresql) {
                return formatPgToMysqlValueByType(value, lowerType);
            }

            switch (columnType) {
                case "enum":
                    if (value instanceof String) {
                        return "'" + escapeString((String) value) + "'";
                    }
                    if (value instanceof Integer) {
                        return value.toString();
                    }
                    break;
                case "set":
                    if (value instanceof String) {
                        return "'" + escapeString((String) value) + "'";
                    }
                    if (value instanceof Long) {
                        return value.toString();
                    }
                    if (value instanceof Integer) {
                        return value.toString();
                    }
                    break;
                case "json":
                    if (value instanceof String) {
                        return "'" + escapeString((String) value) + "'";
                    }
                    if (value instanceof byte[]) {
                        try {
                            String jsonStr = new String((byte[]) value, "UTF-8");
                            return "'" + escapeString(jsonStr) + "'";
                        } catch (Exception e) {
                            return "0x" + bytesToHex((byte[]) value);
                        }
                    }
                    break;
                case "tinytext":
                case "text":
                case "mediumtext":
                case "longtext":
                    if (value instanceof byte[]) {
                        try {
                            String textStr = new String((byte[]) value, "UTF-8");
                            return "'" + escapeString(textStr) + "'";
                        } catch (Exception e) {
                            return "0x" + bytesToHex((byte[]) value);
                        }
                    }
                    if (value instanceof String) {
                        return "'" + escapeString((String) value) + "'";
                    }
                    break;
                case "binary":
                case "varbinary":
                case "tinyblob":
                case "blob":
                case "mediumblob":
                case "longblob":
                    if (value instanceof byte[]) {
                        return "0x" + bytesToHex((byte[]) value);
                    }
                    if (value instanceof String) {
                        try {
                            return "0x" + bytesToHex(((String) value).getBytes("ISO-8859-1"));
                        } catch (Exception e) {
                            return "'" + escapeString((String) value) + "'";
                        }
                    }
                    break;
                case "bit":
                    if (value instanceof String) {
                        String strVal = ((String) value).trim();
                        if (strVal.startsWith("0x") || strVal.startsWith("0X")) {
                            return strVal;
                        }
                        if (strVal.startsWith("{") && strVal.contains(",")) {
                            long bitVal = 0;
                            String[] bits = strVal.substring(1, strVal.length() - 1).split(",");
                            for (String bit : bits) {
                                bitVal |= (1L << Integer.parseInt(bit.trim()));
                            }
                            return "0x" + Long.toHexString(bitVal);
                        }
                        if (strVal.startsWith("{") && strVal.endsWith("}")) {
                            if (strVal.equals("{}")) {
                                return "0x0";
                            }
                            long bitVal = 1L << Integer.parseInt(strVal.substring(1, strVal.length() - 1).trim());
                            return "0x" + Long.toHexString(bitVal);
                        }
                        if (strVal.matches("\\d+")) {
                            long bitVal = Long.parseLong(strVal);
                            return "0x" + Long.toHexString(bitVal);
                        }
                        return "'" + escapeString(strVal) + "'";
                    }
                    if (value instanceof Number) {
                        return "0x" + Long.toHexString(((Number) value).longValue());
                    }
                    if (value instanceof byte[]) {
                        return "0x" + bytesToHex((byte[]) value);
                    }
                    break;
                case "year":
                    if (value instanceof java.sql.Date) {
                        return "'" + ((java.sql.Date) value).toLocalDate().getYear() + "'";
                    }
                    break;
            }
        }
        
        return formatValue(value);
    }

    /**
     * MySQL→PG 增量：把 MySQL 的 bool/bit 原始值转换为 PG 兼容字面量（其余原样返回）。
     * 全量路径由 DataMigration.convertMysqlToPgValue 处理，增量此前缺失该转换，
     * 导致 tinyint(1)→boolean 列写入整数、bit→bytea 列写入整数而报类型不匹配。
     */
    private String convertMysqlValueToPg(String rawValue, String mysqlType) {
        if (rawValue == null) return rawValue;
        String trimmed = rawValue.trim();
        if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) return rawValue;
        String t = (mysqlType == null) ? "" : mysqlType.toLowerCase();

        // tinyint(1) / bool(ean) → PG boolean
        if (t.contains("tinyint(1)") || t.startsWith("bool")) {
            String v = stripSqlQuotes(trimmed);
            if (v.isEmpty()) return rawValue;
            return "0".equals(v) ? "false" : "true";
        }
        // bit(N) → PG bytea（把整数值转为 ceil(N/8) 字节的十六进制）
        if (t.startsWith("bit")) {
            String v = stripSqlQuotes(trimmed);
            try {
                long num;
                String vv = v.toLowerCase();
                if (vv.startsWith("0x")) {
                    num = Long.parseLong(vv.substring(2), 16);
                } else if (vv.startsWith("b'") && vv.endsWith("'")) {
                    num = Long.parseLong(vv.substring(2, vv.length() - 1), 2);
                } else {
                    num = Long.parseLong(vv);
                }
                int bits = 1;
                java.util.regex.Matcher m = java.util.regex.Pattern.compile("bit\\((\\d+)\\)").matcher(t);
                if (m.find()) bits = Integer.parseInt(m.group(1));
                int nbytes = Math.max(1, (bits + 7) / 8);
                StringBuilder hex = new StringBuilder();
                for (int b = nbytes - 1; b >= 0; b--) {
                    hex.append(String.format("%02x", (num >> (b * 8)) & 0xFF));
                }
                return "E'\\\\x" + hex + "'";
            } catch (NumberFormatException e) {
                return rawValue;
            }
        }
        return rawValue;
    }

    private String stripSqlQuotes(String v) {
        if (v.length() >= 2 && v.startsWith("'") && v.endsWith("'")) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }

    /** 带宽度的完整 MySQL 列类型（COLUMN_TYPE，如 tinyint(1)/bit(8)）；缺失时回退到 DATA_TYPE。 */
    private String[] getColumnFullTypes(Map<String, Object> metadata) {
        String s = (String) metadata.get("mysql_column_full_types");
        if (s == null || s.isEmpty()) {
            return getColumnTypes(metadata);
        }
        return s.split("\\s*,\\s*");
    }

    private String formatPostgresValueByType(Object value, String lowerType) {
        if (value == null) {
            return "NULL";
        }

        if (lowerType.contains("integer") || lowerType.contains("bigint") ||
            lowerType.contains("smallint") || lowerType.contains("serial") ||
            lowerType.contains("bigserial") || lowerType.equals("int") ||
            lowerType.equals("int4") || lowerType.equals("int8") ||
            lowerType.equals("int2") || lowerType.equals("oid")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return strVal;
            }
            return value.toString();
        }

        if (lowerType.contains("numeric") || lowerType.contains("decimal") ||
            lowerType.contains("real") || lowerType.contains("double") ||
            lowerType.contains("float") || lowerType.equals("float4") ||
            lowerType.equals("float8") || lowerType.equals("money")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return strVal;
            }
            if (value instanceof java.math.BigDecimal) {
                return ((java.math.BigDecimal) value).toPlainString();
            }
            return value.toString();
        }

        if (lowerType.equals("boolean")) {
            if (value instanceof Boolean) {
                return ((Boolean) value) ? "true" : "false";
            }
            if (value instanceof String) {
                String strVal = ((String) value).trim().toLowerCase();
                if ("t".equals(strVal) || "true".equals(strVal) || "1".equals(strVal)) return "true";
                if ("f".equals(strVal) || "false".equals(strVal) || "0".equals(strVal)) return "false";
                return "'" + escapeString((String) value) + "'";
            }
            return value.toString();
        }

        if (lowerType.contains("timestamp") || lowerType.contains("date") ||
            lowerType.contains("time")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return "'" + escapeString(strVal) + "'::" + lowerType;
            }
            return formatValue(value);
        }

        if (lowerType.contains("json") || lowerType.contains("jsonb")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return "'" + escapeString(strVal) + "'::" + lowerType;
            }
            return formatValue(value);
        }

        if (lowerType.contains("uuid")) {
            if (value instanceof String) {
                return "'" + escapeString((String) value) + "'::uuid";
            }
            return formatValue(value);
        }

        if (lowerType.contains("bytea")) {
            if (value instanceof byte[]) {
                return "E'\\\\x" + bytesToHex((byte[]) value) + "'";
            }
            if (value instanceof String) {
                return "'" + escapeString((String) value) + "'";
            }
            return formatValue(value);
        }

        if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        }

        return formatValue(value);
    }

    private String formatPgToMysqlValueByType(Object value, String lowerType) {
        if (value == null) {
            return "NULL";
        }

        if (lowerType.contains("integer") || lowerType.contains("bigint") ||
            lowerType.contains("smallint") || lowerType.contains("serial") ||
            lowerType.contains("bigserial") || lowerType.equals("int") ||
            lowerType.equals("int4") || lowerType.equals("int8") ||
            lowerType.equals("int2") || lowerType.equals("oid")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return strVal;
            }
            return value.toString();
        }

        if (lowerType.contains("numeric") || lowerType.contains("decimal") ||
            lowerType.contains("real") || lowerType.contains("double") ||
            lowerType.contains("float") || lowerType.equals("float4") ||
            lowerType.equals("float8") || lowerType.equals("money")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return strVal;
            }
            if (value instanceof java.math.BigDecimal) {
                return ((java.math.BigDecimal) value).toPlainString();
            }
            return value.toString();
        }

        if (lowerType.equals("boolean") || lowerType.equals("bool")) {
            if (value instanceof Boolean) {
                return ((Boolean) value) ? "1" : "0";
            }
            if (value instanceof String) {
                String strVal = ((String) value).trim().toLowerCase();
                if ("t".equals(strVal) || "true".equals(strVal) || "1".equals(strVal)) return "1";
                if ("f".equals(strVal) || "false".equals(strVal) || "0".equals(strVal)) return "0";
                return "'" + escapeString((String) value) + "'";
            }
            if (value instanceof Number) {
                return ((Number) value).intValue() != 0 ? "1" : "0";
            }
            return "0";
        }

        if (lowerType.contains("timestamp") || lowerType.contains("date") ||
            lowerType.contains("time")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return "'" + escapeString(strVal) + "'";
            }
            return formatValue(value);
        }

        if (lowerType.contains("json") || lowerType.contains("jsonb")) {
            if (value instanceof String) {
                String strVal = ((String) value).trim();
                if (strVal.isEmpty()) return "NULL";
                return "'" + escapeString(strVal) + "'";
            }
            return formatValue(value);
        }

        if (lowerType.contains("uuid")) {
            if (value instanceof String) {
                return "'" + escapeString((String) value) + "'";
            }
            return formatValue(value);
        }

        if (lowerType.contains("bytea")) {
            if (value instanceof byte[]) {
                return "0x" + bytesToHex((byte[]) value);
            }
            if (value instanceof String) {
                return "'" + escapeString((String) value) + "'";
            }
            return formatValue(value);
        }

        if (lowerType.endsWith("[]")) {
            if (value instanceof String) {
                return "'" + escapeString((String) value) + "'";
            }
            return formatValue(value);
        }

        if (lowerType.contains("inet") || lowerType.contains("cidr") ||
            lowerType.contains("macaddr") || lowerType.contains("interval") ||
            lowerType.contains("point") || lowerType.contains("line") ||
            lowerType.contains("lseg") || lowerType.contains("box") ||
            lowerType.contains("path") || lowerType.contains("polygon") ||
            lowerType.contains("circle") || lowerType.contains("xml")) {
            if (value instanceof String) {
                return "'" + escapeString((String) value) + "'";
            }
            return formatValue(value);
        }

        if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        }

        return formatValue(value);
    }

    private String convertPgRowDataToMysql(String rowDataStr, String[] columnTypes) {
        if (rowDataStr == null || rowDataStr.isEmpty()) {
            return rowDataStr;
        }

        String[] values = parseRowDataValues(rowDataStr);
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < values.length; i++) {
            if (i > 0) result.append(",");
            String val = values[i];
            String type = (columnTypes != null && i < columnTypes.length) ? columnTypes[i] : "";

            result.append(convertSinglePgValueToMysql(val, type));
        }

        return result.toString();
    }

    private String convertSinglePgValueToMysql(String value, String pgType) {
        if (value == null || value.equalsIgnoreCase("NULL")) {
            return "NULL";
        }

        String lowerType = pgType.toLowerCase().trim();

        if (lowerType.equals("boolean") || lowerType.equals("bool")) {
            if (value.equals("t") || value.equals("true")) return "1";
            if (value.equals("f") || value.equals("false")) return "0";
            return value;
        }

        if (lowerType.contains("uuid")) {
            if (value.startsWith("'") && value.contains("::uuid")) {
                return value.substring(0, value.indexOf("::uuid"));
            }
            return value;
        }

        if (lowerType.contains("json") || lowerType.contains("jsonb")) {
            if (value.contains("::jsonb") || value.contains("::json")) {
                int idx = value.indexOf("::");
                if (idx > 0) {
                    return value.substring(0, idx);
                }
            }
            return value;
        }

        if (lowerType.contains("timestamp") || lowerType.contains("date") || lowerType.contains("time")) {
            if (value.contains("::")) {
                int idx = value.indexOf("::");
                if (idx > 0) {
                    return value.substring(0, idx);
                }
            }
            return value;
        }

        if (lowerType.contains("bytea")) {
            if (value.startsWith("E'\\\\x") || value.startsWith("E'\\x")) {
                String hexStr = value.replaceAll("^E'\\\\?x", "").replaceAll("'$", "");
                return "0x" + hexStr;
            }
            return value;
        }

        if (value.contains("::")) {
            int idx = value.indexOf("::");
            return value.substring(0, idx);
        }

        return value;
    }

    private java.util.List<String> generateUpdateSql(THLEvent event, Map<String, Object> metadata) {
        java.util.List<String> statements = new java.util.ArrayList<>();

        String database = (String) metadata.getOrDefault("database_name", "");
        String table = (String) metadata.getOrDefault("table_name", "");

        if (database.isEmpty() || table.isEmpty()) {
            logger.warn("Database or table name not found for UPDATE event");
            return statements;
        }

        if (!targetDatabaseName.isEmpty() && !targetIsPostgresql) {
            database = targetDatabaseName;
        }

        String rowDataStr = (String) metadata.get("row_data");
        String rowDataBeforeStr = (String) metadata.get("row_data_before");
        
        if (rowDataStr == null || rowDataStr.isEmpty()) {
            logger.warn("No row data found for UPDATE event");
            return statements;
        }

        String[] columnNames = getColumnNames(metadata);
        String[] columnTypes = getColumnTypes(metadata);

        // UPDATE 的 new-tuple 通常只包含变更列，需要使用 extractor 记录的变更列名
        // 来正确对应 values，避免与全表 columnNames 错位。
        String updateColNamesStr = (String) metadata.get("update_column_names");
        String[] setColNames = (updateColNamesStr != null && !updateColNamesStr.isEmpty())
                ? updateColNamesStr.split("\\s*,\\s*") : columnNames;
        String beforeColNamesStr = (String) metadata.get("update_before_column_names");
        String[] whereColNames = (beforeColNamesStr != null && !beforeColNamesStr.isEmpty())
                ? beforeColNamesStr.split("\\s*,\\s*") : columnNames;

        String pkColumnsStr = (String) metadata.get("primary_keys");
        java.util.Set<String> pkColumns = new java.util.HashSet<>();
        if (pkColumnsStr != null && !pkColumnsStr.isEmpty()) {
            for (String pk : pkColumnsStr.split(",")) {
                pkColumns.add(pk.trim());
            }
        }

        String[] values = parseRowDataValues(rowDataStr);
        String[] beforeValues = rowDataBeforeStr != null ? parseRowDataValues(rowDataBeforeStr) : values;

        if (sourceIsPostgresql && !targetIsPostgresql) {
            for (int i = 0; i < values.length; i++) {
                String type = (columnTypes != null && i < columnTypes.length) ? columnTypes[i] : "";
                values[i] = convertSinglePgValueToMysql(values[i], type);
            }
            for (int i = 0; i < beforeValues.length; i++) {
                String type = (columnTypes != null && i < columnTypes.length) ? columnTypes[i] : "";
                beforeValues[i] = convertSinglePgValueToMysql(beforeValues[i], type);
            }
        }

        // MySQL→PG：把 bool/bit 值转换为 PG 兼容字面量
        if (sourceIsMysql && targetIsPostgresql) {
            String[] fullTypes = getColumnFullTypes(metadata);
            for (int i = 0; i < values.length; i++) {
                String type = (fullTypes != null && i < fullTypes.length) ? fullTypes[i] : "";
                values[i] = convertMysqlValueToPg(values[i], type);
            }
            for (int i = 0; i < beforeValues.length; i++) {
                String type = (fullTypes != null && i < fullTypes.length) ? fullTypes[i] : "";
                beforeValues[i] = convertMysqlValueToPg(beforeValues[i], type);
            }
        }

        StringBuilder sql = new StringBuilder();
        if (isPostgresql) {
            sql.append("UPDATE ").append(quoteIdentifier(table)).append(" SET ");
        } else {
            sql.append("UPDATE `").append(database).append("`.`").append(table).append("` SET ");
        }

        for (int i = 0; i < values.length; i++) {
            if (i > 0) sql.append(", ");
            String colName = (setColNames != null && i < setColNames.length) ? setColNames[i] : "column" + i;
            sql.append(quoteIdentifier(colName)).append("=").append(values[i]);
        }

        sql.append(" WHERE ");
        if (!pkColumns.isEmpty()) {
            boolean first = true;
            for (int i = 0; i < beforeValues.length; i++) {
                String colName = (whereColNames != null && i < whereColNames.length) ? whereColNames[i] : "column" + i;
                if (!pkColumns.contains(colName)) continue;
                if (!first) sql.append(" AND ");
                first = false;
                if (beforeValues[i].equals("null") || beforeValues[i].equals("NULL")) {
                    sql.append(quoteIdentifier(colName)).append(" IS NULL");
                } else {
                    sql.append(quoteIdentifier(colName)).append("=").append(beforeValues[i]);
                }
            }
        } else {
            for (int i = 0; i < beforeValues.length; i++) {
                if (i > 0) sql.append(" AND ");
                String colName = (whereColNames != null && i < whereColNames.length) ? whereColNames[i] : "column" + i;
                if (beforeValues[i].equals("null") || beforeValues[i].equals("NULL")) {
                    sql.append(quoteIdentifier(colName)).append(" IS NULL");
                } else {
                    sql.append(quoteIdentifier(colName)).append("=").append(beforeValues[i]);
                }
            }
            if (!isPostgresql) {
                sql.append(" LIMIT 1");
            }
        }
        sql.append(";");
        statements.add(sql.toString());

        return statements;
    }

    private String[] parseRowDataValues(String rowDataStr) {
        java.util.List<String> values = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;
        
        for (int i = 0; i < rowDataStr.length(); i++) {
            char c = rowDataStr.charAt(i);
            
            if (inString) {
                if (c == '\\' && i + 1 < rowDataStr.length()) {
                    current.append(c);
                    current.append(rowDataStr.charAt(i + 1));
                    i++;
                } else if (c == stringChar) {
                    current.append(c);
                    inString = false;
                } else {
                    current.append(c);
                }
            } else {
                if (c == '\'' || c == '"') {
                    inString = true;
                    stringChar = c;
                    current.append(c);
                } else if (c == ',') {
                    values.add(current.toString().trim());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
        }
        
        if (current.length() > 0) {
            values.add(current.toString().trim());
        }
        
        return values.toArray(new String[0]);
    }

    private java.util.List<String> generateDeleteSql(THLEvent event, Map<String, Object> metadata) {
        java.util.List<String> statements = new java.util.ArrayList<>();

        String database = (String) metadata.getOrDefault("database_name", "");
        String table = (String) metadata.getOrDefault("table_name", "");

        if (database.isEmpty() || table.isEmpty()) {
            logger.warn("Database or table name not found for DELETE event");
            return statements;
        }

        if (!targetDatabaseName.isEmpty() && !targetIsPostgresql) {
            database = targetDatabaseName;
        }

        String rowDataStr = (String) metadata.get("row_data");
        if (rowDataStr == null || rowDataStr.isEmpty()) {
            logger.warn("No row data found for DELETE event");
            return statements;
        }

        String[] columnNames = getColumnNames(metadata);
        String[] columnTypes = getColumnTypes(metadata);

        String pkColumnsStr = (String) metadata.get("primary_keys");
        java.util.Set<String> pkColumns = new java.util.HashSet<>();
        if (pkColumnsStr != null && !pkColumnsStr.isEmpty()) {
            for (String pk : pkColumnsStr.split(",")) {
                pkColumns.add(pk.trim());
            }
        }

        String[] values = parseRowDataValues(rowDataStr);

        if (sourceIsPostgresql && !targetIsPostgresql) {
            for (int i = 0; i < values.length; i++) {
                String type = (columnTypes != null && i < columnTypes.length) ? columnTypes[i] : "";
                values[i] = convertSinglePgValueToMysql(values[i], type);
            }
        }

        StringBuilder sql = new StringBuilder();
        if (isPostgresql) {
            sql.append("DELETE FROM ").append(quoteIdentifier(table)).append(" WHERE ");
        } else {
            sql.append("DELETE FROM `").append(database).append("`.`").append(table).append("` WHERE ");
        }

        if (!pkColumns.isEmpty()) {
            boolean first = true;
            for (int i = 0; i < values.length; i++) {
                String colName = (columnNames != null && i < columnNames.length) ? columnNames[i] : "column" + i;
                if (!pkColumns.contains(colName)) continue;
                if (!first) sql.append(" AND ");
                first = false;
                if (values[i].equals("null") || values[i].equals("NULL")) {
                    sql.append(quoteIdentifier(colName)).append(" IS NULL");
                } else {
                    sql.append(quoteIdentifier(colName)).append("=").append(values[i]);
                }
            }
        } else {
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sql.append(" AND ");
                String colName = (columnNames != null && i < columnNames.length) ? columnNames[i] : "column" + i;
                if (values[i].equals("null") || values[i].equals("NULL")) {
                    sql.append(quoteIdentifier(colName)).append(" IS NULL");
                } else {
                    sql.append(quoteIdentifier(colName)).append("=").append(values[i]);
                }
            }
            if (!isPostgresql) {
                sql.append(" LIMIT 1");
            }
        }
        sql.append(";");
        statements.add(sql.toString());

        return statements;
    }

    private java.util.List<String> generateQuerySql(THLEvent event, Map<String, Object> metadata) {
        java.util.List<String> statements = new java.util.ArrayList<>();

        String sql = (String) metadata.get("sql");
        String database = (String) metadata.get("database_name");
        String ddlDatabase = (String) metadata.get("ddl_database");
        String defaultDatabase = props != null ? props.getProperty("default.database", "") : "";

        logger.debug("Generating QUERY SQL - database: {}, ddl_database: {}, sql: {}", database, ddlDatabase, sql);

        if (sql == null || sql.isEmpty()) {
            return statements;
        }

        SqlClassifier.ClassificationResult classification = sqlClassifier.classify(sql);
        logger.debug("SQL classification result: {}", classification);

        if (classification.isTransaction()) {
            SqlClassifier.TransactionSubType txType = classification.getTransactionSubType();
            if (txType == SqlClassifier.TransactionSubType.BEGIN) {
                logger.debug("Skipping BEGIN transaction marker");
                return statements;
            }
            if (txType == SqlClassifier.TransactionSubType.COMMIT) {
                statements.add("COMMIT;");
                return statements;
            }
            if (txType == SqlClassifier.TransactionSubType.ROLLBACK) {
                statements.add("ROLLBACK;");
                return statements;
            }
        }

        if (classification.isUse()) {
            statements.add(sql.trim() + (sql.trim().endsWith(";") ? "" : ";"));
            return statements;
        }

        if (classification.isDdl()) {
            // 通过 SchemaEvolutionService 处理 DDL：支持跨库转换、映射配置、策略控制
            if (schemaEvolutionService != null) {
                String ddlSubType = classification.getDdlSubType() != null
                        ? classification.getDdlSubType().name() : "UNKNOWN_DDL";
                String effectiveDb = resolveDdlDatabase(ddlDatabase, database, defaultDatabase, sql);
                SchemaEvolutionService.ApplyResult ddlResult = schemaEvolutionService.applyDdl(sql, ddlSubType, effectiveDb);
                if (ddlResult.isSuccess()) {
                    // DDL 已由 SchemaEvolutionService 直接应用到目标库，返回空列表避免重复执行
                    logger.debug("DDL 已由 SchemaEvolutionService 应用: {}", ddlResult.getMessage());
                } else if (ddlResult.getStatus() == SchemaEvolutionService.ApplyResult.Status.MANUAL
                        || ddlResult.getStatus() == SchemaEvolutionService.ApplyResult.Status.SKIPPED) {
                    logger.info("DDL 未自动应用: {} | {}", ddlResult.getStatus(), ddlResult.getMessage());
                } else {
                    // FAILED：记录失败，但仍返回原 SQL 尝试执行（兼容旧行为）
                    logger.warn("DDL 应用失败，尝试直接执行原 SQL: {}", ddlResult.getMessage());
                    String cleanSql = sql.trim();
                    if (!cleanSql.endsWith(";")) {
                        cleanSql = cleanSql + ";";
                    }
                    statements.add(cleanSql);
                }
                return statements;
            }

            // 兼容路径：SchemaEvolutionService 未初始化时使用原有逻辑
            if (sourceIsPostgresql && !targetIsPostgresql) {
                logger.warn("Skipping PG DDL for MySQL target (not auto-convertible): {}", sql);
                return statements;
            }
            if (!sourceIsPostgresql && targetIsPostgresql) {
                logger.warn("Skipping MySQL DDL for PostgreSQL target (not auto-convertible): {}", sql);
                return statements;
            }
            String effectiveDb = resolveDdlDatabase(ddlDatabase, database, defaultDatabase, sql);
            if (effectiveDb != null && !effectiveDb.isEmpty() && classification.isNeedsDatabaseSelection()) {
                String cleanSql = sql.trim();
                if (!cleanSql.endsWith(";")) {
                    cleanSql = cleanSql + ";";
                }
                if (isPostgresql) {
                    statements.add("SET search_path TO " + quoteIdentifier(effectiveDb) + ";");
                    statements.add(cleanSql);
                } else {
                    statements.add("USE `" + effectiveDb + "`;");
                    statements.add(cleanSql);
                }
                logger.info("Generated {} for database: {} (DDL type: {})",
                        isPostgresql ? "SET search_path+DDL" : "USE+DDL", effectiveDb, classification.getDdlSubType());
            } else {
                String cleanSql = sql.trim();
                if (!cleanSql.endsWith(";")) {
                    cleanSql = cleanSql + ";";
                }
                statements.add(cleanSql);
            }
            return statements;
        }

        if (classification.isDml() && database != null && !database.isEmpty()) {
            String fullTableName = classification.getFullTableName();
            if (fullTableName != null && !fullTableName.contains(".")) {
                if (isPostgresql) {
                    statements.add("SET search_path TO " + quoteIdentifier(database) + ";");
                } else {
                    statements.add("USE `" + database + "`;");
                }
                logger.debug("Generated {} statement for DML on database: {}",
                        isPostgresql ? "SET search_path" : "USE database", database);
            }
        }

        String cleanSql = sql.trim();
        if (!cleanSql.endsWith(";")) {
            cleanSql = cleanSql + ";";
        }
        statements.add(cleanSql);

        return statements;
    }

    private String resolveDdlDatabase(String ddlDatabase, String binlogDatabase, String defaultDatabase, String sql) {
        if (ddlDatabase != null && !ddlDatabase.isEmpty()) {
            logger.debug("Using ddl_database from THL metadata: {}", ddlDatabase);
            return ddlDatabase;
        }
        if (binlogDatabase != null && !binlogDatabase.isEmpty()) {
            logger.debug("Using binlog database_name as fallback: {}", binlogDatabase);
            return binlogDatabase;
        }
        if (defaultDatabase != null && !defaultDatabase.isEmpty()) {
            logger.debug("Using default database as fallback: {}", defaultDatabase);
            return defaultDatabase;
        }
        logger.warn("Cannot determine database for DDL, sql: {}", sql != null ? sql.substring(0, Math.min(sql.length(), 100)) : "null");
        return null;
    }

    private void executeSql(String sql) throws SQLException {
        if (sql == null || sql.isEmpty()) return;

        boolean isAtomicDdl = sql.trim().toUpperCase().startsWith("USE ") ||
                sql.trim().toUpperCase().startsWith("SET SEARCH_PATH");

        if (isAtomicDdl) {
            executeAtomicDdl(sql);
        } else {
            try (Statement stmt = targetConnection.createStatement()) {
                String[] individualStatements = sql.split(";");
                for (String individualSql : individualStatements) {
                    individualSql = individualSql.trim();
                    if (!individualSql.isEmpty()) {
                        logger.debug("Executing SQL: {}", individualSql);
                        stmt.execute(individualSql);
                    }
                }
            }
        }
    }

    private void executeAtomicDdl(String sql) throws SQLException {
        try (Statement stmt = targetConnection.createStatement()) {
            String[] parts = sql.split(";");
            for (String part : parts) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    logger.debug("Executing atomic DDL part: {}", trimmed);
                    stmt.execute(trimmed);
                }
            }
            logger.debug("Atomic DDL execution completed successfully");
        } catch (SQLException e) {
            logger.error("Atomic DDL execution failed, USE and DDL may be in inconsistent state: {}", e.getMessage());
            throw e;
        }
    }

    private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");
    private static final SimpleDateFormat TS_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat DT_FMT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TM_FMT = new SimpleDateFormat("HH:mm:ss");
    static {
        TS_FMT.setTimeZone(UTC_TZ);
        DT_FMT.setTimeZone(UTC_TZ);
        TM_FMT.setTimeZone(UTC_TZ);
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        } else if (value instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) value).toPlainString();
        } else if (value instanceof Double) {
            return BigDecimal.valueOf((Double) value).toPlainString();
        } else if (value instanceof Float) {
            return BigDecimal.valueOf((Float) value).toPlainString();
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof java.sql.Timestamp) {
            synchronized (TS_FMT) {
                return "'" + TS_FMT.format((java.sql.Timestamp) value) + "'";
            }
        } else if (value instanceof java.sql.Date) {
            synchronized (DT_FMT) {
                return "'" + DT_FMT.format((java.sql.Date) value) + "'";
            }
        } else if (value instanceof java.sql.Time) {
            synchronized (TM_FMT) {
                return "'" + TM_FMT.format((java.sql.Time) value) + "'";
            }
        } else if (value instanceof java.util.Date) {
            synchronized (TS_FMT) {
                return "'" + TS_FMT.format((java.util.Date) value) + "'";
            }
        } else if (value instanceof Boolean) {
            if (isPostgresql) {
                return ((Boolean) value) ? "true" : "false";
            }
            return ((Boolean) value) ? "1" : "0";
        } else if (value instanceof byte[]) {
            return "0x" + bytesToHex((byte[]) value);
        } else {
            return "'" + escapeString(value.toString()) + "'";
        }
    }

    private String escapeString(String str) {
        if (isPostgresql) {
            return str.replace("'", "''");
        }
        return str.replace("\\", "\\\\")
                  .replace("'", "\\'")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public long getTotalEvents() { return totalEvents; }
    public long getSuccessfulEvents() { return successfulEvents; }
    public long getFailedEvents() { return failedEvents; }

    public void clearExecutedRecords() {
        executedRecords.clear();
        rewriteExecutedRecordsFile();
        logger.info("Cleared all executed records");
    }

    public void saveExecutedRecordDirectly(long seqno, String sql) {
        saveExecutedRecord(seqno, sql);
    }

    public void setSeqnoPosition(long seqno) {
        LinkedHashMap<Long, String> newRecords = new LinkedHashMap<>();
        for (Map.Entry<Long, String> entry : executedRecords.entrySet()) {
            if (entry.getKey() <= seqno) {
                newRecords.put(entry.getKey(), entry.getValue());
            }
        }
        executedRecords = newRecords;
        rewriteExecutedRecordsFile();
        logger.info("Set seqno position to {}, kept {} records", seqno, executedRecords.size());
    }

    /**
     * 注入 SchemaEvolutionService，用于在 ContinuousIncrementMain 等外部初始化场景下
     * 启用 DDL 自动应用和在线 DDL 影子表过滤。
     */
    public void setSchemaEvolutionService(SchemaEvolutionService service) {
        this.schemaEvolutionService = service;
        logger.info("SchemaEvolutionService 已注入，DDL 自动应用和在线 DDL 过滤已启用");
    }

    public void removeSeqnoBefore(long seqno) {
        LinkedHashMap<Long, String> newRecords = new LinkedHashMap<>();
        for (Map.Entry<Long, String> entry : executedRecords.entrySet()) {
            if (entry.getKey() >= seqno) {
                newRecords.put(entry.getKey(), entry.getValue());
            }
        }
        int removed = executedRecords.size() - newRecords.size();
        executedRecords = newRecords;
        rewriteExecutedRecordsFile();
        logger.info("Removed {} records before seqno {}, kept {} records", removed, seqno, executedRecords.size());
    }

    public int processFileFromEvent(File thlFile, int skipEventCount) throws Exception {
        if (targetConnection == null || targetConnection.isClosed()) {
            connect();
        }

        logger.info("Processing THL file: {} (skipping first {} events)", thlFile.getName(), skipEventCount);

        THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath());
        int eventsProcessed = 0;
        int eventsSkipped = 0;
        try {
            THLEvent event;
            while ((event = reader.readEvent()) != null) {
                if (eventsSkipped < skipEventCount) {
                    eventsSkipped++;
                    continue;
                }

                totalEvents++;
                eventsProcessed++;

                long seqno = event.getSeqno();

                if (executedRecords.containsKey(seqno)) {
                    logger.debug("Skipping already executed seqno: {}", seqno);
                    continue;
                }

                java.util.List<String> sqlStatements = convertToSql(event);

                if (sqlStatements == null || sqlStatements.isEmpty()) {
                    continue;
                }

                int subSeqno = 0;
                for (String sql : sqlStatements) {
                    String executableSql = extractExecutableSql(sql);
                    if (executableSql.isEmpty()) continue;

                    long statementSeqno = sqlStatements.size() == 1 ? seqno : (seqno * 1000 + subSeqno);
                    subSeqno++;

                    if (executedRecords.containsKey(statementSeqno)) {
                        logger.debug("Skipping already executed statement seqno: {}", statementSeqno);
                        continue;
                    }

                    try {
                        executeSql(executableSql);
                        logger.info("✓ [seqno={}] SQL executed successfully: {}", statementSeqno, executableSql);
                    } catch (SQLException e) {
                        failedEvents++;
                        logger.error("✗ [seqno={}] SQL execution failed: {}\nError: {}", statementSeqno, executableSql, e.getMessage());
                        throw new SqlExecutionException(statementSeqno, executableSql, e);
                    }

                    saveExecutedRecord(statementSeqno, executableSql);
                }

                successfulEvents++;
            }
        } finally {
            reader.close();
        }

        return eventsProcessed;
    }

    public int countEventsInFile(File thlFile) {
        try (THLFileReader reader = new THLFileReader(thlFile.getAbsolutePath())) {
            int count = 0;
            while (reader.readEvent() != null) {
                count++;
            }
            return count;
        } catch (Exception e) {
            logger.error("Error counting events in THL file: {}", thlFile.getName(), e);
            return 0;
        }
    }

    private String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        // PostgreSQL 全量建表为小写，增量 SQL 需统一转小写以匹配全量建表的表名/列名；
        // 引用字符（反引号/双引号）交由目标库方言决定。
        String id = targetIsPostgresql ? identifier.toLowerCase() : identifier;
        return targetDialect.quoteIdentifier(id);
    }
}
