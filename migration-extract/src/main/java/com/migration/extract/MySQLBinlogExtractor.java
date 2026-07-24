package com.migration.extract;

import com.migration.common.AbstractExtractor;
import com.migration.db.ConnectionPoolManager;
import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.Pipeline;
import com.migration.thl.pipeline.PipelineConfig;
import com.migration.thl.pipeline.PipelineContext;
import com.migration.thl.pipeline.PipelineContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class MySQLBinlogExtractor extends AbstractExtractor<byte[], THLEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MySQLBinlogExtractor.class);

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';

    private String inputDir;
    private String outputDir;
    // 下列成员对子类开放（TiCDCExtractor 复用同一套 seqno/管线/列元数据缓存，
    // 只替换“把一条 capture 记录解析成 THLEvent”这一步）
    protected long seqno = 1;
    private String seqnoFile;

    private String sourceHost;
    private int sourcePort;
    private String sourceUser;
    private String sourcePassword;
    protected Connection sourceConnection;

    private Map<Long, Map<String, String>> tableMapCache = new HashMap<>();
    private Map<String, List<String>> tableSchemaCache = new HashMap<>();
    private Map<String, List<String>> tableColumnTypeCache = new HashMap<>();
    protected Map<String, List<String>> tableColumnFullTypeCache = new HashMap<>();
    protected Map<String, Map<String, List<String>>> enumSetValuesCache = new HashMap<>();
    private Map<String, List<String>> primaryKeyCache = new HashMap<>();

    protected Pipeline pipeline;

    protected String checkpointBinlogFile;
    protected long checkpointBinlogPosition;
    protected boolean skipBeforeCheckpoint = false;

    @Override
    protected void doInitialize() throws Exception {
        inputDir = props.getProperty("extract.input.dir", "binlog_output");
        outputDir = props.getProperty("extract.output.dir", "thl_output");
        seqnoFile = outputDir + "/.extractor_seqno";

        sourceHost = props.getProperty("source.db.host", "localhost");
        sourcePort = Integer.parseInt(props.getProperty("source.db.port", "3306"));
        sourceUser = props.getProperty("source.db.username", "root");
        sourcePassword = props.getProperty("source.db.password", "");

        checkpointBinlogFile = props.getProperty("checkpoint.binlog.file", "");
        checkpointBinlogPosition = Long.parseLong(props.getProperty("checkpoint.binlog.position", "0"));
        skipBeforeCheckpoint = Boolean.parseBoolean(props.getProperty("extract.skip.before.checkpoint", "false"));

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        loadSeqno();
        connectToSourceDatabase();

        PipelineContext pipelineContext = new PipelineContextImpl(props);
        ((PipelineContextImpl) pipelineContext).setSourceConnection(sourceConnection);
        pipeline = PipelineConfig.loadFromProperties(props, pipelineContext);
        if (pipeline != null) {
            pipeline.prepare();
            logger.info("Pipeline initialized with {} filters", pipeline.getFilters().size());
        }

        logger.info("MySQL Binlog Extractor initialized - input: {}, output: {}, seqno: {}, skipBeforeCheckpoint: {}",
                inputDir, outputDir, seqno, skipBeforeCheckpoint);
    }

    private void connectToSourceDatabase() throws SQLException {
        String url = "jdbc:mysql://" + sourceHost + ":" + sourcePort +
                "/?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
        sourceConnection = ConnectionPoolManager.getConnection(url, sourceUser, sourcePassword);
        logger.info("Connected to source database: {}:{}", sourceHost, sourcePort);
    }

    @Override
    protected THLEvent doExtract(byte[] input) throws Exception {
        String eventStr = new String(input, "UTF-8");
        if (eventStr.trim().isEmpty()) {
            return null;
        }

        String[] fields = eventStr.split(String.valueOf(FIELD_SEP));
        if (fields.length < 5) {
            logger.warn("Invalid event format, skipping: {}", eventStr.substring(0, Math.min(100, eventStr.length())));
            return null;
        }

        String eventType = fields[0].trim();
        String binlogFile = fields[1].trim();
        long binlogPosition = 0;
        try {
            binlogPosition = Long.parseLong(fields[2].trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid binlog position in event: {}", fields[2]);
            return null;
        }
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(fields[3].trim());
        } catch (NumberFormatException e) {
            timestamp = System.currentTimeMillis();
        }
        long serverId = 0;
        try {
            serverId = Long.parseLong(fields[4].trim());
        } catch (NumberFormatException e) {
            // ignore
        }

        if (skipBeforeCheckpoint && checkpointBinlogFile != null && !checkpointBinlogFile.isEmpty()) {
            if (shouldSkipEvent(binlogFile, binlogPosition)) {
                return null;
            }
        }

        THLEvent thlEvent = new THLEvent();
        thlEvent.setSeqno(seqno++);
        thlEvent.setEventId(binlogFile + ":" + binlogPosition);
        thlEvent.setSourceId("mysql");
        thlEvent.setSourceTstamp(new Timestamp(timestamp));
        thlEvent.addMetadata("event_type", eventType);
        thlEvent.addMetadata("binlog_file", binlogFile);
        thlEvent.addMetadata("binlog_position", binlogPosition);
        thlEvent.addMetadata("server_id", serverId);

        String eventData = fields.length > 5 ? fields[5] : "";

        if ("SYNC_HEARTBEAT".equals(eventType)) {
            thlEvent.setType(THLEvent.HEARTBEAT_EVENT);
            thlEvent.addMetadata("operation", "HEARTBEAT");
            thlEvent.addMetadata("source_db_timestamp", timestamp);
            return thlEvent;
        } else if ("TABLE_MAP".equals(eventType)) {
            parseTableMapEvent(thlEvent, eventData);
        } else if (isWriteRowsEvent(eventType)) {
            parseRowEvent(thlEvent, eventData, "INSERT");
        } else if (isUpdateRowsEvent(eventType)) {
            parseRowEvent(thlEvent, eventData, "UPDATE");
        } else if (isDeleteRowsEvent(eventType)) {
            parseRowEvent(thlEvent, eventData, "DELETE");
        } else if ("QUERY".equals(eventType)) {
            parseQueryEvent(thlEvent, eventData);
        } else if ("XID".equals(eventType)) {
            thlEvent.addMetadata("operation", "COMMIT");
        } else if ("ROTATE".equals(eventType)) {
            thlEvent.addMetadata("operation", "ROTATE");
        }

        if (pipeline != null) {
            thlEvent = pipeline.process(thlEvent);
        }

        if (thlEvent != null) {
            Boolean multiRow = (Boolean) thlEvent.getMetadata().get("multi_row");
            if (multiRow != null && multiRow) {
                @SuppressWarnings("unchecked")
                java.util.List<String> rowsData = (java.util.List<String>) thlEvent.getMetadata().get("rows_data");
                if (rowsData != null && rowsData.size() > 1) {
                    long reservedSeqno = seqno - 1 + rowsData.size() - 1;
                    seqno = reservedSeqno + 1;
                    logger.debug("Reserved seqno range for multi-row event: {} to {} ({} rows)", 
                            thlEvent.getSeqno(), reservedSeqno, rowsData.size());
                }
            }
        }

        return thlEvent;
    }

    private boolean shouldSkipEvent(String binlogFile, long binlogPosition) {
        if (checkpointBinlogFile == null || checkpointBinlogFile.isEmpty()) {
            return false;
        }

        int cmp = binlogFile.compareTo(checkpointBinlogFile);
        if (cmp < 0) {
            return true;
        } else if (cmp == 0) {
            return binlogPosition < checkpointBinlogPosition;
        }
        return false;
    }

    private boolean isWriteRowsEvent(String eventType) {
        return "WRITE_ROWS".equals(eventType) || "EXT_WRITE_ROWS".equals(eventType);
    }

    private boolean isUpdateRowsEvent(String eventType) {
        return "UPDATE_ROWS".equals(eventType) || "EXT_UPDATE_ROWS".equals(eventType);
    }

    private boolean isDeleteRowsEvent(String eventType) {
        return "DELETE_ROWS".equals(eventType) || "EXT_DELETE_ROWS".equals(eventType);
    }

    private void parseTableMapEvent(THLEvent thlEvent, String eventData) {
        Long tableId = null;
        String database = null;
        String table = null;

        java.util.regex.Matcher tableIdMatcher = java.util.regex.Pattern.compile("tableId=(\\d+)").matcher(eventData);
        if (tableIdMatcher.find()) {
            try {
                tableId = Long.parseLong(tableIdMatcher.group(1));
            } catch (NumberFormatException e) { /* ignore */ }
        }

        java.util.regex.Matcher databaseMatcher = java.util.regex.Pattern.compile("database='([^']+)'").matcher(eventData);
        if (databaseMatcher.find()) {
            database = databaseMatcher.group(1);
        }

        java.util.regex.Matcher tableMatcher = java.util.regex.Pattern.compile("table='([^']+)'").matcher(eventData);
        if (tableMatcher.find()) {
            table = tableMatcher.group(1);
        }

        if (tableId != null && database != null && table != null) {
            Map<String, String> tableInfo = new HashMap<>();
            tableInfo.put("database", database);
            tableInfo.put("table", table);
            tableMapCache.put(tableId, tableInfo);

            List<String> columns = getTableColumns(database, table);
            tableInfo.put("columns", String.join(",", columns));

            List<String> columnTypes = getTableColumnTypes(database, table);
            tableInfo.put("column_types", String.join(",", columnTypes));

            List<String> columnFullTypes = tableColumnFullTypeCache.get(database + "." + table);
            if (columnFullTypes != null) {
                tableInfo.put("column_full_types", String.join(",", columnFullTypes));
            }

            List<String> pkColumns = getTablePrimaryKeys(database, table);
            tableInfo.put("primary_keys", String.join(",", pkColumns));

            String cacheKey = database + "." + table;
            Map<String, List<String>> enumSetValues = enumSetValuesCache.get(cacheKey);
            if (enumSetValues != null && !enumSetValues.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, List<String>> entry : enumSetValues.entrySet()) {
                    if (sb.length() > 0) sb.append(";");
                    sb.append(entry.getKey()).append("=").append(String.join(",", entry.getValue()));
                }
                tableInfo.put("enum_set_values", sb.toString());
            }

            thlEvent.addMetadata("database_name", database);
            thlEvent.addMetadata("table_name", table);
            thlEvent.addMetadata("table_id", tableId);
        }
    }

    private void parseRowEvent(THLEvent thlEvent, String eventData, String operation) {
        thlEvent.addMetadata("operation", operation);

        Long tableId = null;

        java.util.regex.Matcher tableIdMatcher = java.util.regex.Pattern.compile("tableId=(\\d+)").matcher(eventData);
        if (tableIdMatcher.find()) {
            try {
                tableId = Long.parseLong(tableIdMatcher.group(1));
            } catch (NumberFormatException e) { /* ignore */ }
        }

        if (tableId != null) {
            Map<String, String> tableInfo = tableMapCache.get(tableId);
            if (tableInfo != null) {
                thlEvent.addMetadata("database_name", tableInfo.get("database"));
                thlEvent.addMetadata("table_name", tableInfo.get("table"));

                String columnsStr = tableInfo.get("columns");
                if (columnsStr != null) {
                    thlEvent.addMetadata("column_names", columnsStr);
                }

                String columnTypesStr = tableInfo.get("column_types");
                if (columnTypesStr != null) {
                    thlEvent.addMetadata("mysql_column_types", columnTypesStr);
                }

                // 带精度/显示宽度的完整类型（COLUMN_TYPE，如 tinyint(1)/bit(8)），
                // 供增量转换区分 boolean(tinyint(1))/bit 等（DATA_TYPE 丢失了宽度信息）
                String columnFullTypesMeta = tableInfo.get("column_full_types");
                if (columnFullTypesMeta != null) {
                    thlEvent.addMetadata("mysql_column_full_types", columnFullTypesMeta);
                }

                String pkStr = tableInfo.get("primary_keys");
                if (pkStr != null) {
                    thlEvent.addMetadata("primary_keys", pkStr);
                }

                String enumSetValuesStr = tableInfo.get("enum_set_values");
                if (enumSetValuesStr != null) {
                    thlEvent.addMetadata("enum_set_values", enumSetValuesStr);
                }

                int columnCount = columnsStr != null ? columnsStr.split(",").length : 0;
                String[] columnTypes = columnTypesStr != null ? columnTypesStr.split(",") : new String[0];
                String[] columnNames = columnsStr != null ? columnsStr.split(",") : new String[0];
                Map<String, List<String>> enumValuesMap = parseEnumSetValuesMap(enumSetValuesStr);

                String columnFullTypesStr = tableInfo.get("column_full_types");
                // 括号感知切分：enum('a','b','c')/set(...)/decimal(20,4) 内含逗号，naive split 会导致
                // 其后所有列的完整类型整体错位（如 bigint unsigned 拿到 "'c')" 而丢失 unsigned 重建）
                String[] columnFullTypes = columnFullTypesStr != null
                        ? splitTopLevelCommas(columnFullTypesStr) : new String[0];

                if ("INSERT".equals(operation) || "DELETE".equals(operation)) {
                    List<String> allRowValues = extractAllRowValuesFromWriteDelete(eventData);
                    if (!allRowValues.isEmpty()) {
                        List<String> formattedRows = new ArrayList<>();
                        ArrayList<ArrayList<Object>> typedRows = new ArrayList<>();
                        for (String rowValues : allRowValues) {
                            List<String> values = parseValueList(rowValues, columnCount);
                            formattedRows.add(formatRowData(values, columnTypes, columnFullTypes, columnNames, enumValuesMap));
                            if (typedRows != null) {
                                ArrayList<Object> typed = typeRowValues(values, columnTypes, columnFullTypes, columnNames, enumValuesMap);
                                if (typed != null) typedRows.add(typed); else typedRows = null; // 任一行无法类型化则整体回退
                            }
                        }
                        thlEvent.addMetadata("rows_data", formattedRows);
                        thlEvent.addMetadata("row_data", formattedRows.get(0));
                        if (typedRows != null) {
                            // 类型化值管道：供增量端 PreparedStatement 参数绑定，消除 SQL 字面量拼接
                            thlEvent.addMetadata("rows_typed", typedRows);
                        }
                        if (formattedRows.size() > 1) {
                            thlEvent.addMetadata("multi_row", true);
                        }
                    }
                } else if ("UPDATE".equals(operation)) {
                    List<String[]> allBeforeAfter = extractAllBeforeAfterValues(eventData);
                    if (!allBeforeAfter.isEmpty()) {
                        List<String> formattedAfterRows = new ArrayList<>();
                        List<String> formattedBeforeRows = new ArrayList<>();
                        ArrayList<ArrayList<Object>> typedAfterRows = new ArrayList<>();
                        ArrayList<ArrayList<Object>> typedBeforeRows = new ArrayList<>();
                        for (String[] beforeAfter : allBeforeAfter) {
                            if (beforeAfter[1] != null) {
                                List<String> afterValues = parseValueList(beforeAfter[1], columnCount);
                                formattedAfterRows.add(formatRowData(afterValues, columnTypes, columnFullTypes, columnNames, enumValuesMap));
                                if (typedAfterRows != null) {
                                    ArrayList<Object> typed = typeRowValues(afterValues, columnTypes, columnFullTypes, columnNames, enumValuesMap);
                                    if (typed != null) typedAfterRows.add(typed); else typedAfterRows = null;
                                }
                            }
                            if (beforeAfter[0] != null) {
                                List<String> beforeValues = parseValueList(beforeAfter[0], columnCount);
                                formattedBeforeRows.add(formatRowData(beforeValues, columnTypes, columnFullTypes, columnNames, enumValuesMap));
                                if (typedBeforeRows != null) {
                                    ArrayList<Object> typed = typeRowValues(beforeValues, columnTypes, columnFullTypes, columnNames, enumValuesMap);
                                    if (typed != null) typedBeforeRows.add(typed); else typedBeforeRows = null;
                                }
                            }
                        }
                        if (!formattedAfterRows.isEmpty()) {
                            thlEvent.addMetadata("row_data", formattedAfterRows.get(0));
                            thlEvent.addMetadata("rows_data", formattedAfterRows);
                        }
                        if (!formattedBeforeRows.isEmpty()) {
                            thlEvent.addMetadata("row_data_before", formattedBeforeRows.get(0));
                            thlEvent.addMetadata("rows_data_before", formattedBeforeRows);
                        }
                        // 类型化值：UPDATE 需 before/after 同时可类型化且行数配对，否则整体回退文本路径
                        if (typedAfterRows != null && typedBeforeRows != null
                                && !typedAfterRows.isEmpty() && typedAfterRows.size() == typedBeforeRows.size()) {
                            thlEvent.addMetadata("rows_typed", typedAfterRows);
                            thlEvent.addMetadata("rows_before_typed", typedBeforeRows);
                        }
                        if (formattedAfterRows.size() > 1) {
                            thlEvent.addMetadata("multi_row", true);
                        }
                    }
                }
            }
        }
    }

    private List<String> extractAllRowValuesFromWriteDelete(String eventData) {
        List<String> allRows = new ArrayList<>();
        
        int rowsIdx = eventData.indexOf("rows=[");
        if (rowsIdx < 0) {
            String singleRow = extractValuesString(eventData, "values");
            if (singleRow != null) {
                allRows.add(singleRow);
            }
            return allRows;
        }

        int startIdx = rowsIdx + "rows=[".length();
        
        while (startIdx < eventData.length()) {
            while (startIdx < eventData.length() && (eventData.charAt(startIdx) == ' ' || eventData.charAt(startIdx) == '\n')) {
                startIdx++;
            }
            
            if (startIdx >= eventData.length() || eventData.charAt(startIdx) != '[') {
                break;
            }
            
            int rowStartIdx = startIdx + 1;
            int bracketCount = 1;
            int rowEndIdx = rowStartIdx;
            
            while (rowEndIdx < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(rowEndIdx);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                }
                rowEndIdx++;
            }
            
            String rowValues = eventData.substring(rowStartIdx, rowEndIdx - 1);
            allRows.add(rowValues);
            
            startIdx = rowEndIdx;
            while (startIdx < eventData.length() && eventData.charAt(startIdx) == ' ') {
                startIdx++;
            }
            if (startIdx < eventData.length() && eventData.charAt(startIdx) == ',') {
                startIdx++;
            }
        }
        
        return allRows;
    }

    private String[] extractBeforeAfterValues(String eventData) {
        String[] result = new String[2];

        int beforeIdx = eventData.indexOf("before=[");
        if (beforeIdx >= 0) {
            int startIdx = beforeIdx + "before=[".length();
            int bracketCount = 1;
            int endIdx = startIdx;

            while (endIdx < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(endIdx);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                }
                endIdx++;
            }

            result[0] = eventData.substring(startIdx, endIdx - 1);
        }

        int afterIdx = eventData.indexOf("after=[");
        if (afterIdx >= 0) {
            int startIdx = afterIdx + "after=[".length();
            int bracketCount = 1;
            int endIdx = startIdx;

            while (endIdx < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(endIdx);
                if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                }
                endIdx++;
            }

            result[1] = eventData.substring(startIdx, endIdx - 1);
        }

        return result;
    }

    private List<String[]> extractAllBeforeAfterValues(String eventData) {
        List<String[]> result = new ArrayList<>();
        int searchFrom = 0;

        while (searchFrom < eventData.length()) {
            int beforeIdx = eventData.indexOf("before=[", searchFrom);
            if (beforeIdx < 0) break;

            int beforeStart = beforeIdx + "before=[".length();
            int bracketCount = 1;
            int beforeEnd = beforeStart;
            while (beforeEnd < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(beforeEnd);
                if (c == '[') bracketCount++;
                else if (c == ']') bracketCount--;
                beforeEnd++;
            }
            String beforeVal = eventData.substring(beforeStart, beforeEnd - 1);

            int afterIdx = eventData.indexOf("after=[", beforeEnd);
            if (afterIdx < 0) break;

            int afterStart = afterIdx + "after=[".length();
            bracketCount = 1;
            int afterEnd = afterStart;
            while (afterEnd < eventData.length() && bracketCount > 0) {
                char c = eventData.charAt(afterEnd);
                if (c == '[') bracketCount++;
                else if (c == ']') bracketCount--;
                afterEnd++;
            }
            String afterVal = eventData.substring(afterStart, afterEnd - 1);

            result.add(new String[]{beforeVal, afterVal});
            searchFrom = afterEnd;
        }

        return result;
    }

    private String extractValuesString(String eventData, String keyword) {
        String searchKey = keyword + "=[";
        int startIdx = eventData.indexOf(searchKey);
        if (startIdx < 0) {
            return null;
        }

        startIdx += searchKey.length();
        int bracketCount = 1;
        int endIdx = startIdx;

        while (endIdx < eventData.length() && bracketCount > 0) {
            char c = eventData.charAt(endIdx);
            if (c == '[') {
                bracketCount++;
            } else if (c == ']') {
                bracketCount--;
            }
            endIdx++;
        }

        return eventData.substring(startIdx, endIdx - 1);
    }

    private List<String> parseValueList(String valuesStr, int expectedCount) {
        List<String> values = new ArrayList<>();
        if (valuesStr == null || valuesStr.isEmpty()) {
            return values;
        }

        List<String> parts = splitByComma(valuesStr);
        for (int i = 0; i < parts.size() && values.size() < expectedCount; i++) {
            values.add(parts.get(i).trim());
        }

        while (values.size() < expectedCount) {
            values.add(null);
        }

        return values;
    }

    private List<String> splitByComma(String str) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        int braceDepth = 0;
        boolean inQuote = false;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '\'' && (i == 0 || str.charAt(i - 1) != '\\')) {
                inQuote = !inQuote;
                current.append(c);
            } else if (!inQuote && c == '[') {
                depth++;
                current.append(c);
            } else if (!inQuote && c == ']') {
                depth--;
                current.append(c);
            } else if (!inQuote && c == '{') {
                braceDepth++;
                current.append(c);
            } else if (!inQuote && c == '}') {
                braceDepth--;
                current.append(c);
            } else if (!inQuote && c == ',' && depth == 0 && braceDepth == 0) {
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

    protected Map<String, List<String>> parseEnumSetValuesMap(String enumSetValuesStr) {
        Map<String, List<String>> map = new HashMap<>();
        if (enumSetValuesStr == null || enumSetValuesStr.isEmpty()) {
            return map;
        }

        String[] entries = enumSetValuesStr.split(";");
        for (String entry : entries) {
            int eqIdx = entry.indexOf("=");
            if (eqIdx > 0) {
                String colName = entry.substring(0, eqIdx).trim();
                String valuesStr = entry.substring(eqIdx + 1).trim();
                if (valuesStr.startsWith("[") && valuesStr.endsWith("]")) {
                    valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
                }
                List<String> values = new ArrayList<>();
                for (String v : valuesStr.split(",")) {
                    values.add(v.trim());
                }
                map.put(colName, values);
            }
        }
        return map;
    }

    private String formatRowData(List<String> values, String[] columnTypes, String[] columnFullTypes, String[] columnNames, Map<String, List<String>> enumValuesMap) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(",");
            String value = values.get(i);
            String type = i < columnTypes.length ? columnTypes[i] : "";
            String fullType = i < columnFullTypes.length ? columnFullTypes[i] : "";
            String colName = i < columnNames.length ? columnNames[i] : "";

            if (value == null || "null".equalsIgnoreCase(value)) {
                sb.append("null");
            } else if (isBinaryType(type) || isBlobType(type)) {
                if (value.matches("\\[B@[0-9a-f]+")) {
                    sb.append("null");
                } else if (value.startsWith("0x") || value.startsWith("0X")) {
                    sb.append(value);
                } else {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (isTextType(type) || isJsonType(type)) {
                if (value.startsWith("0x") || value.startsWith("0X")) {
                    if (isJsonType(type)) {
                        try {
                            byte[] jsonBytes = hexStringToByteArray(value.substring(2));
                            String jsonStr = com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary.parseAsString(jsonBytes);
                            sb.append("'").append(escapeString(jsonStr)).append("'");
                        } catch (Exception e) {
                            logger.warn("Failed to decode JSON binary value, using CAST: {}", e.getMessage());
                            sb.append("CAST(").append(value).append(" AS JSON)");
                        }
                    } else {
                        String decoded = hexToString(value.substring(2));
                        sb.append("'").append(escapeString(decoded)).append("'");
                    }
                } else {
                    sb.append("'").append(escapeString(value)).append("'");
                }
            } else if (isBitType(type)) {
                if (value.startsWith("0x") || value.startsWith("0X")) {
                    sb.append(value);
                } else if (value.matches("\\d+")) {
                    long bitVal = Long.parseLong(value);
                    sb.append("0x").append(Long.toHexString(bitVal));
                } else if (value.startsWith("{") && value.contains(",")) {
                    long bitVal = 0;
                    String[] bits = value.substring(1, value.length() - 1).split(",");
                    for (String bit : bits) {
                        bitVal |= (1L << Integer.parseInt(bit.trim()));
                    }
                    sb.append("0x").append(Long.toHexString(bitVal));
                } else if (value.startsWith("{") && value.endsWith("}")) {
                    if (value.equals("{}")) {
                        sb.append("0x0");
                    } else {
                        long bitVal = 1L << Integer.parseInt(value.substring(1, value.length() - 1).trim());
                        sb.append("0x").append(Long.toHexString(bitVal));
                    }
                } else {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (value.matches("\\[B@[0-9a-f]+")) {
                sb.append("'").append(value.replace("'", "\\'")).append("'");
            } else if (isEnumType(type) && enumValuesMap.containsKey(colName)) {
                List<String> enumValues = enumValuesMap.get(colName);
                try {
                    int idx = Integer.parseInt(value.trim()) - 1;
                    if (idx >= 0 && idx < enumValues.size()) {
                        sb.append("'").append(enumValues.get(idx).replace("'", "\\'")).append("'");
                    } else {
                        sb.append("'").append(value.replace("'", "\\'")).append("'");
                    }
                } catch (NumberFormatException e) {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (isSetType(type) && enumValuesMap.containsKey(colName)) {
                List<String> setValues = enumValuesMap.get(colName);
                try {
                    long bitMask = Long.parseLong(value.trim());
                    StringBuilder setSb = new StringBuilder();
                    for (int j = 0; j < setValues.size(); j++) {
                        if ((bitMask & (1L << j)) != 0) {
                            if (setSb.length() > 0) setSb.append(",");
                            setSb.append(setValues.get(j));
                        }
                    }
                    sb.append("'").append(escapeString(setSb.toString())).append("'");
                } catch (NumberFormatException e) {
                    sb.append("'").append(value.replace("'", "\\'")).append("'");
                }
            } else if (isDatetimeType(type)) {
                String formatted = formatDatetimeValue(value);
                sb.append("'").append(formatted).append("'");
            } else if (isUnsignedType(fullType) && value.matches("-\\d+")) {
                sb.append(convertToUnsigned(value, fullType));
            } else if (isExtractNumericType(type) || value.matches("-?\\d+(\\.\\d+)?")) {
                sb.append(value);
            } else {
                sb.append("'").append(value.replace("'", "\\'")).append("'");
            }
        }
        return sb.toString();
    }

    /**
     * 类型化值管道：把 .cap 文本值按列类型转换为可直接用于 PreparedStatement 参数绑定的 Java 对象，
     * 与 {@link #formatRowData} 逐分支对应（语义一致），但产出类型化值而非 SQL 字面量——
     * 增量端据此以参数绑定执行，机制性消除引号/字面量/日期格式一类的拼接 bug。
     *
     * <p>白名单外或无法可靠解析的值返回 null（调用方整行回退旧文本路径，行为零风险）。
     * 类型选择：bit/binary/blob → byte[]，tinyint(1) → Boolean，enum/set → 标签 String，
     * 无符号负值 → 重建后的十进制 String，其余（数字/文本/时间）→ String
     * （PG 连接串 stringtype=unspecified 下由服务端按列类型推断）。
     */
    private ArrayList<Object> typeRowValues(List<String> values, String[] columnTypes, String[] columnFullTypes,
                                            String[] columnNames, Map<String, List<String>> enumValuesMap) {
        ArrayList<Object> typed = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            String type = i < columnTypes.length ? columnTypes[i] : "";
            String fullType = i < columnFullTypes.length ? columnFullTypes[i] : "";
            String colName = i < columnNames.length ? columnNames[i] : "";
            String lowerFull = fullType == null ? "" : fullType.toLowerCase();

            if (value == null || "null".equalsIgnoreCase(value)) {
                typed.add(null);
            } else if (isBinaryType(type) || isBlobType(type)) {
                if (value.matches("\\[B@[0-9a-f]+")) {
                    typed.add(null); // 与文本路径一致：无法还原的 byte[] toString 按 null 处理
                } else if (value.startsWith("0x") || value.startsWith("0X")) {
                    try {
                        typed.add(hexStringToByteArray(value.substring(2)));
                    } catch (Exception e) {
                        return null;
                    }
                } else {
                    typed.add(value);
                }
            } else if (isTextType(type) || isJsonType(type)) {
                if (value.startsWith("0x") || value.startsWith("0X")) {
                    if (isJsonType(type)) {
                        try {
                            byte[] jsonBytes = hexStringToByteArray(value.substring(2));
                            typed.add(com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary.parseAsString(jsonBytes));
                        } catch (Exception e) {
                            return null; // JSON binary 解码失败：回退文本路径的 CAST 处理
                        }
                    } else {
                        typed.add(hexToString(value.substring(2)));
                    }
                } else {
                    typed.add(value);
                }
            } else if (isBitType(type)) {
                Long bitVal = parseBitValue(value);
                if (bitVal == null) {
                    return null;
                }
                typed.add(bitValueToBytes(bitVal, lowerFull));
            } else if (lowerFull.contains("tinyint(1)") && !lowerFull.contains("unsigned")) {
                if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
                    typed.add(Boolean.TRUE);
                } else if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
                    typed.add(Boolean.FALSE);
                } else {
                    return null;
                }
            } else if (isEnumType(type) && enumValuesMap.containsKey(colName)) {
                List<String> enumValues = enumValuesMap.get(colName);
                try {
                    int idx = Integer.parseInt(value.trim()) - 1;
                    typed.add(idx >= 0 && idx < enumValues.size() ? enumValues.get(idx) : value);
                } catch (NumberFormatException e) {
                    typed.add(value);
                }
            } else if (isSetType(type) && enumValuesMap.containsKey(colName)) {
                List<String> setValues = enumValuesMap.get(colName);
                try {
                    long bitMask = Long.parseLong(value.trim());
                    StringBuilder setSb = new StringBuilder();
                    for (int j = 0; j < setValues.size(); j++) {
                        if ((bitMask & (1L << j)) != 0) {
                            if (setSb.length() > 0) setSb.append(",");
                            setSb.append(setValues.get(j));
                        }
                    }
                    typed.add(setSb.toString());
                } catch (NumberFormatException e) {
                    typed.add(value);
                }
            } else if (isDatetimeType(type)) {
                typed.add(formatDatetimeValue(value));
            } else if (isUnsignedType(fullType) && value.matches("-\\d+")) {
                typed.add(convertToUnsigned(value, fullType));
            } else {
                // 数字/普通文本/时间字符串：原样字符串，由目标端按列类型推断
                typed.add(value);
            }
        }
        return typed;
    }

    /** 解析 .cap 中 BIT 值的各种文本形态（十进制 / 0x十六进制 / BitSet toString）为位值。 */
    private Long parseBitValue(String value) {
        try {
            if (value.startsWith("0x") || value.startsWith("0X")) {
                return Long.parseLong(value.substring(2), 16);
            }
            if (value.matches("\\d+")) {
                return Long.parseLong(value);
            }
            if (value.startsWith("{") && value.endsWith("}")) {
                String inner = value.substring(1, value.length() - 1).trim();
                if (inner.isEmpty()) return 0L;
                long bits = 0;
                for (String bit : inner.split(",")) {
                    bits |= (1L << Integer.parseInt(bit.trim()));
                }
                return bits;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    /** 按 bit(N) 宽度把位值转为大端 byte[]（与全量路径 mysql 驱动返回的 BIT byte[] 形态一致）。 */
    private byte[] bitValueToBytes(long bitVal, String lowerFullType) {
        int bits = 1;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("bit\\((\\d+)\\)").matcher(lowerFullType);
        if (m.find()) {
            bits = Integer.parseInt(m.group(1));
        }
        int nbytes = Math.max(1, (bits + 7) / 8);
        byte[] out = new byte[nbytes];
        for (int b = 0; b < nbytes; b++) {
            out[nbytes - 1 - b] = (byte) ((bitVal >> (b * 8)) & 0xFF);
        }
        return out;
    }

    private boolean isUnsignedType(String fullType) {
        if (fullType == null) return false;
        return fullType.toLowerCase().contains("unsigned");
    }

    private String convertToUnsigned(String value, String fullType) {
        try {
            long signedVal = Long.parseLong(value);
            String lower = fullType.toLowerCase();
            if (lower.startsWith("tinyint")) {
                return String.valueOf(signedVal & 0xFF);
            } else if (lower.startsWith("smallint")) {
                return String.valueOf(signedVal & 0xFFFF);
            } else if (lower.startsWith("mediumint")) {
                return String.valueOf(signedVal & 0xFFFFFF);
            } else if (lower.startsWith("int") || lower.startsWith("integer")) {
                return String.valueOf(signedVal & 0xFFFFFFFFL);
            } else if (lower.startsWith("bigint")) {
                if (signedVal < 0) {
                    BigInteger unsigned = BigInteger.valueOf(signedVal).add(BigInteger.ONE.shiftLeft(64));
                    return unsigned.toString();
                }
                return value;
            }
        } catch (NumberFormatException e) {
            return value;
        }
        return value;
    }

    private boolean isEnumType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("enum");
    }

    private boolean isSetType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("set");
    }

    /** 按顶层逗号切分（忽略括号内逗号），用于 COLUMN_TYPE 列表（enum/set/decimal 等类型体内含逗号）。 */
    static String[] splitTopLevelCommas(String s) {
        List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(s.substring(start));
        return parts.toArray(new String[0]);
    }

    protected boolean isBinaryType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("binary") || lower.equals("varbinary");
    }

    protected boolean isBlobType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("tinyblob") || lower.equals("blob") ||
                lower.equals("mediumblob") || lower.equals("longblob");
    }

    protected boolean isBitType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("bit");
    }

    protected boolean isTextType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("tinytext") || lower.equals("text") ||
                lower.equals("mediumtext") || lower.equals("longtext") ||
                lower.equals("char") || lower.equals("varchar");
    }

    protected boolean isJsonType(String type) {
        if (type == null) return false;
        return type.toLowerCase().equals("json");
    }

    private String hexToString(String hex) {
        if (hex == null || hex.isEmpty()) return "";
        try {
            // 必须按 UTF-8 整体解码：逐字节 (char) 强转是 Latin-1 语义，中文/emoji 等多字节字符会乱码
            return new String(hexStringToByteArray(hex), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return hex;
        }
    }

    private byte[] hexStringToByteArray(String hex) {
        if (hex == null || hex.isEmpty()) return new byte[0];
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    protected String escapeString(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r");
    }

    protected boolean isDatetimeType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("datetime") || lower.equals("timestamp") || lower.equals("date") || lower.equals("time");
    }

    private String formatDatetimeValue(String value) {
        if (value == null || value.isEmpty()) return value;

        if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+")) {
            if (value.length() > 23) {
                return value.substring(0, 23);
            }
            return value;
        }
        if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
            return value;
        }
        if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return value;
        }
        if (value.matches("\\d{2}:\\d{2}:\\d{2}.*")) {
            if (value.length() > 12) {
                return value.substring(0, 12);
            }
            return value;
        }

        try {
            java.text.SimpleDateFormat inputFmt = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", java.util.Locale.ENGLISH);
            java.text.SimpleDateFormat outputFmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = inputFmt.parse(value.trim());
            return outputFmt.format(date);
        } catch (Exception e) {
            return value;
        }
    }

    private boolean isExtractNumericType(String type) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        return lower.equals("int") || lower.equals("integer") || lower.equals("bigint") ||
                lower.equals("smallint") || lower.equals("tinyint") || lower.equals("mediumint") ||
                lower.equals("float") || lower.equals("double") || lower.equals("decimal") ||
                lower.equals("numeric") || lower.equals("year");
    }

    private void parseQueryEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "QUERY");
        java.util.regex.Matcher sqlMatcher = java.util.regex.Pattern.compile("sql='(.+)'\\}$").matcher(eventData);
        if (sqlMatcher.find()) {
            thlEvent.addMetadata("sql", sqlMatcher.group(1));
        } else {
            thlEvent.addMetadata("sql", eventData);
        }
        java.util.regex.Matcher dbMatcher = java.util.regex.Pattern.compile("database='([^']*)'").matcher(eventData);
        if (dbMatcher.find()) {
            String database = dbMatcher.group(1);
            thlEvent.addMetadata("database_name", database);
            String sql = thlEvent.getMetadata().getOrDefault("sql", "").toString();
            String ddlDatabase = DdlDatabaseExtractor.extractDatabase(sql, database, "");
            if (ddlDatabase != null && !ddlDatabase.isEmpty()) {
                thlEvent.addMetadata("ddl_database", ddlDatabase);
            }
            // ALTER/DROP/RENAME/TRUNCATE/CREATE TABLE 会改变列布局：失效该表的列元数据缓存，
            // 否则后续 WRITE/UPDATE_ROWS 会沿用 ALTER 前的旧列名，静默丢弃新增列的值。
            invalidateColumnCachesForDdl(sql, database);
        }
    }

    /**
     * 对表级 DDL 影响的每张表，移除全部按 {@code database.table} 键缓存的列元数据，
     * 使下一个数据事件重新查询 information_schema，拿到 DDL 后的最新列布局。
     */
    protected void invalidateColumnCachesForDdl(String sql, String defaultDatabase) {
        for (String cacheKey : DdlDatabaseAnltrExtractor.extractAffectedTables(sql, defaultDatabase)) {
            boolean removed = false;
            removed |= tableSchemaCache.remove(cacheKey) != null;
            removed |= tableColumnTypeCache.remove(cacheKey) != null;
            removed |= tableColumnFullTypeCache.remove(cacheKey) != null;
            removed |= enumSetValuesCache.remove(cacheKey) != null;
            removed |= primaryKeyCache.remove(cacheKey) != null;
            if (removed) {
                logger.info("DDL 变更表 {}, 失效列元数据缓存，下个数据事件将重新读取 information_schema", cacheKey);
            }
        }
    }

    protected List<String> getTableColumns(String database, String table) {
        String cacheKey = database + "." + table;
        if (tableSchemaCache.containsKey(cacheKey)) {
            return tableSchemaCache.get(cacheKey);
        }

        List<String> columns = new ArrayList<>();
        try {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
            tableSchemaCache.put(cacheKey, columns);
        } catch (SQLException e) {
            logger.error("Error fetching columns for {}.{}: {}", database, table, e.getMessage());
        }
        return columns;
    }

    protected List<String> getTableColumnTypes(String database, String table) {
        String cacheKey = database + "." + table;
        if (tableColumnTypeCache.containsKey(cacheKey)) {
            return tableColumnTypeCache.get(cacheKey);
        }

        List<String> columnTypes = new ArrayList<>();
        List<String> columnFullTypes = new ArrayList<>();
        Map<String, List<String>> enumSetValues = new HashMap<>();
        try {
            String sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columnTypes.add(rs.getString("DATA_TYPE"));
                        String columnType = rs.getString("COLUMN_TYPE");
                        columnFullTypes.add(columnType != null ? columnType : "");
                        String columnName = rs.getString("COLUMN_NAME");
                        if (columnType != null && (columnType.startsWith("enum(") || columnType.startsWith("set("))) {
                            List<String> values = parseEnumSetValues(columnType);
                            enumSetValues.put(columnName, values);
                        }
                    }
                }
            }
            tableColumnTypeCache.put(cacheKey, columnTypes);
            tableColumnFullTypeCache.put(cacheKey, columnFullTypes);
            if (!enumSetValues.isEmpty()) {
                enumSetValuesCache.put(cacheKey, enumSetValues);
            }
        } catch (SQLException e) {
            logger.error("Error fetching column types for {}.{}: {}", database, table, e.getMessage());
        }
        return columnTypes;
    }

    private List<String> parseEnumSetValues(String columnType) {
        List<String> values = new ArrayList<>();
        int start = columnType.indexOf('(');
        int end = columnType.lastIndexOf(')');
        if (start < 0 || end < 0) return values;

        String inner = columnType.substring(start + 1, end);
        StringBuilder sb = new StringBuilder();
        boolean inQuote = false;

        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '\'' && (i == 0 || inner.charAt(i - 1) != '\\')) {
                inQuote = !inQuote;
                if (!inQuote && sb.length() > 0) {
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

    protected List<String> getTablePrimaryKeys(String database, String table) {
        String cacheKey = database + "." + table;
        if (primaryKeyCache.containsKey(cacheKey)) {
            return primaryKeyCache.get(cacheKey);
        }

        List<String> pkColumns = new ArrayList<>();
        try {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' " +
                    "ORDER BY ORDINAL_POSITION";
            try (PreparedStatement stmt = sourceConnection.prepareStatement(sql)) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        pkColumns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
            primaryKeyCache.put(cacheKey, pkColumns);
        } catch (SQLException e) {
            logger.error("Error fetching primary key for {}.{}: {}", database, table, e.getMessage());
        }
        return pkColumns;
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
        if (pipeline != null) {
            pipeline.release();
        }
        if (sourceConnection != null) {
            try {
                sourceConnection.close();
            } catch (SQLException e) {
                logger.error("Error closing source connection", e);
            }
        }
    }
}
