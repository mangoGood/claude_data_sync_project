package com.synctask.service;

import com.synctask.dto.ContentCompareSession;
import com.synctask.dto.ContentCompareSession.*;
import com.synctask.util.DataSourcePoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ContentCompareService {

    private static final Logger logger = LoggerFactory.getLogger(ContentCompareService.class);
    private static final Pattern CONNECTION_PATTERN = Pattern.compile(
        "(?:mysql|postgresql)://([^:]+):([^@]+)@([^:]+):(\\d+)(?:/(.*))?"
    );
    private static final int BUCKET_COUNT = 100;
    private static final int CHUNK_SIZE = 1000;

    private final ConcurrentHashMap<String, ContentCompareSession> sessions = new ConcurrentHashMap<>();

    public ContentCompareSession startCompare(String sourceConnection, String targetConnection,
                                               String sourceType, String targetType,
                                               Map<String, List<String>> syncObjects) {
        return startCompare(sourceConnection, targetConnection, sourceType, targetType, syncObjects, null);
    }

    /**
     * @param explicitTableMappings 表名映射（db → {源表: 目标表}），来自任务 syncObjects 的 tableMapping。
     *                              ValidationTaskService 传入压平后的表清单时映射已丢失，需在此显式补传；
     *                              为 null 时退回从 syncObjects 原始 value 里解析（MetadataController 直传路径）。
     */
    public ContentCompareSession startCompare(String sourceConnection, String targetConnection,
                                               String sourceType, String targetType,
                                               Map<String, List<String>> syncObjects,
                                               Map<String, Map<String, String>> explicitTableMappings) {
        if (!sourceType.equalsIgnoreCase(targetType)) {
            throw new IllegalArgumentException("内容对比仅支持源库和目标库为相同类型的数据库");
        }

        ContentCompareSession session = new ContentCompareSession();
        session.setSourceConnection(sourceConnection);
        session.setTargetConnection(targetConnection);
        session.setSourceType(sourceType);
        session.setTargetType(targetType);

        boolean isPg = "postgresql".equalsIgnoreCase(sourceType);
        ParsedConn sourceConn = parseConnection(sourceConnection);
        ParsedConn targetConn = parseConnection(targetConnection);

        if (!isPg) {
            String firstDb = syncObjects.keySet().stream().findFirst().orElse(null);
            if (sourceConn.database == null && firstDb != null) {
                sourceConn.database = firstDb;
            }
            if (targetConn.database == null && firstDb != null) {
                targetConn.database = firstDb;
            }
        }

        try (Connection sourceDb = createConnection(sourceConn, isPg);
             Connection targetDb = createConnection(targetConn, isPg)) {

            for (Map.Entry<String, List<String>> entry : syncObjects.entrySet()) {
                String dbName = entry.getKey();
                List<String> tables = entry.getValue();
                List<String> actualTables = tables;
                // 表名映射（表级同步）：对比时目标端按映射后的表名取数，否则映射表必报差异
                Map<String, String> tableMapping = java.util.Collections.emptyMap();
                if (explicitTableMappings != null && explicitTableMappings.containsKey(dbName)) {
                    tableMapping = explicitTableMappings.get(dbName);
                }

                if (tables != null && !tables.isEmpty()) {
                    try {
                        Object parsed = parseSyncObjectsValue(tables);
                        if (parsed instanceof Map) {
                            Map<?, ?> map = (Map<?, ?>) parsed;
                            Object tablesObj = map.get("tables");
                            if (tablesObj instanceof List) {
                                actualTables = (List<String>) tablesObj;
                            }
                            Object mappingObj = map.get("tableMapping");
                            if (mappingObj instanceof Map && tableMapping.isEmpty()) {
                                Map<String, String> m = new java.util.HashMap<>();
                                ((Map<?, ?>) mappingObj).forEach((k, v) -> m.put(String.valueOf(k), String.valueOf(v)));
                                tableMapping = m;
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("Using tables as-is for db={}", dbName);
                    }
                }

                if (actualTables == null || actualTables.isEmpty()) continue;

                for (String tableName : actualTables) {
                    String targetTableName = tableMapping.getOrDefault(tableName, tableName);
                    TableCompareTask task = new TableCompareTask();
                    task.setSourceDb(isPg ? sourceConn.database : dbName);
                    // mysql 目标库名可能与源库名不同（如 prod -> prod_replica）：targetConn.database
                    // 在上面已从连接串或 syncObjects 兜底解析好，不能像源库那样直接用 dbName（源库名）
                    task.setTargetDb(isPg ? targetConn.database : targetConn.database);
                    task.setSourceTable(tableName);
                    task.setTargetTable(targetTableName);

                    String qualifiedSource = isPg ? tableName : dbName + "." + tableName;
                    String qualifiedTarget = isPg ? targetTableName : targetConn.database + "." + targetTableName;

                    task.setPrimaryKeyColumn(detectPrimaryKey(sourceDb, qualifiedSource, isPg, sourceConn.database));
                    task.setColumns(getColumnMeta(sourceDb, qualifiedSource, isPg, sourceConn.database));
                    task.setSourceRowCount(getRowCount(sourceDb, qualifiedSource, isPg));
                    task.setTargetRowCount(getRowCount(targetDb, qualifiedTarget, isPg));
                    task.setCursor(calculateCursorRange(sourceDb, qualifiedSource, task.getPrimaryKeyColumn(), isPg));
                    task.setStatus("PENDING");

                    session.getTables().add(task);
                }
            }
        } catch (SQLException e) {
            logger.error("初始化内容对比失败: {}", e.getMessage());
            throw new RuntimeException("初始化内容对比失败: " + e.getMessage());
        }

        sessions.put(session.getSessionId(), session);
        return session;
    }

    public ContentCompareSession getSession(String sessionId) {
        ContentCompareSession session = sessions.get(sessionId);
        if (session == null) {
            throw new RuntimeException("对比会话不存在: " + sessionId);
        }
        return session;
    }

    public void deleteSession(String sessionId) {
        sessions.remove(sessionId);
    }

    public ContentCompareSession runPhase1Checksum(String sessionId) {
        ContentCompareSession session = getSession(sessionId);
        boolean isPg = "postgresql".equalsIgnoreCase(session.getSourceType());
        ParsedConn sourceConn = parseConnection(session.getSourceConnection());
        ParsedConn targetConn = parseConnection(session.getTargetConnection());

        try (Connection sourceDb = createConnection(sourceConn, isPg);
             Connection targetDb = createConnection(targetConn, isPg)) {

            for (TableCompareTask task : session.getTables()) {
                if (task.getChecksumMatch() != null) continue;

                String sourceQualified = isPg ? task.getSourceTable() : task.getSourceDb() + "." + task.getSourceTable();
                String targetQualified = isPg ? task.getTargetTable() : task.getTargetDb() + "." + task.getTargetTable();

                try {
                    String sourceChecksum = computeTableChecksum(sourceDb, sourceQualified, isPg);
                    String targetChecksum = computeTableChecksum(targetDb, targetQualified, isPg);

                    task.setChecksumMatch(sourceChecksum != null && sourceChecksum.equals(targetChecksum));
                    task.setStatus(Boolean.TRUE.equals(task.getChecksumMatch()) ? "MATCH" : "MISMATCH");
                    logger.info("表 {} 校验和对比: source={}, target={}, match={}",
                        task.getSourceTable(), sourceChecksum, targetChecksum, task.getChecksumMatch());
                } catch (Exception e) {
                    logger.warn("表 {} 校验和计算失败: {}", task.getSourceTable(), e.getMessage());
                    task.setChecksumMatch(false);
                    task.setStatus("ERROR");
                }
            }
        } catch (SQLException e) {
            logger.error("阶段1校验和对比失败: {}", e.getMessage());
            throw new RuntimeException("校验和对比失败: " + e.getMessage());
        }

        boolean allDone = session.getTables().stream().allMatch(t -> t.getChecksumMatch() != null);
        if (allDone) {
            boolean allMatch = session.getTables().stream().allMatch(t -> Boolean.TRUE.equals(t.getChecksumMatch()));
            session.setStatus(allMatch ? "COMPLETED" : "HAS_DIFF");
        }

        return session;
    }

    public TableCompareTask findDiffs(String sessionId, int tableIndex, int limit) {
        ContentCompareSession session = getSession(sessionId);
        if (tableIndex < 0 || tableIndex >= session.getTables().size()) {
            throw new RuntimeException("表索引越界: " + tableIndex);
        }

        TableCompareTask task = session.getTables().get(tableIndex);
        if (Boolean.TRUE.equals(task.getChecksumMatch())) {
            return task;
        }
        if (task.isScanCompleted() && task.getDiffs().size() <= task.getTotalDiffsFound()) {
            return task;
        }

        boolean isPg = "postgresql".equalsIgnoreCase(session.getSourceType());
        ParsedConn sourceConn = parseConnection(session.getSourceConnection());
        ParsedConn targetConn = parseConnection(session.getTargetConnection());

        try (Connection sourceDb = createConnection(sourceConn, isPg);
             Connection targetDb = createConnection(targetConn, isPg)) {

            int existingDiffs = task.getDiffs().size();
            int newDiffsNeeded = Math.max(0, limit - existingDiffs);
            if (newDiffsNeeded <= 0) newDiffsNeeded = limit;

            String sourceQualified = isPg ? task.getSourceTable() : task.getSourceDb() + "." + task.getSourceTable();
            String targetQualified = isPg ? task.getTargetTable() : task.getTargetDb() + "." + task.getTargetTable();
            String pkCol = task.getPrimaryKeyColumn();

            if (pkCol == null || pkCol.isEmpty()) {
                task.setStatus("NO_PK");
                task.setScanCompleted(true);
                return task;
            }

            List<DataDiff> newDiffs = mergeJoinCompare(sourceDb, targetDb, sourceQualified, targetQualified,
                pkCol, task.getColumns(), task.getCursor(), newDiffsNeeded, isPg);

            task.getDiffs().addAll(newDiffs);
            task.setTotalDiffsFound(task.getTotalDiffsFound() + newDiffs.size());

            if (newDiffs.size() < newDiffsNeeded || task.getCursor().getLastProcessedPk() == null) {
                task.setScanCompleted(true);
                task.setStatus("SCANNED");
            }

        } catch (SQLException e) {
            logger.error("查找差异失败: {}", e.getMessage());
            task.setStatus("ERROR");
        }

        return task;
    }

    private List<DataDiff> mergeJoinCompare(Connection sourceDb, Connection targetDb,
                                             String sourceTable, String targetTable,
                                             String pkCol, List<ColumnMeta> columns,
                                             CompareCursor cursor, int maxDiffs, boolean isPg) throws SQLException {
        List<DataDiff> diffs = new ArrayList<>();
        Object lastPk = cursor.getLastProcessedPk();

        String columnList = buildColumnList(columns, pkCol);

        while (diffs.size() < maxDiffs) {
            String sourceSql = buildChunkQuery(sourceTable, pkCol, columnList, lastPk, isPg);
            String targetSql = buildChunkQuery(targetTable, pkCol, columnList, lastPk, isPg);

            List<Map<String, Object>> sourceRows = executeQuery(sourceDb, sourceSql);
            List<Map<String, Object>> targetRows = executeQuery(targetDb, targetSql);

            if (sourceRows.isEmpty() && targetRows.isEmpty()) {
                cursor.setLastProcessedPk(null);
                break;
            }

            int si = 0, ti = 0;
            while (si < sourceRows.size() || ti < targetRows.size()) {
                if (si >= sourceRows.size()) {
                    diffs.add(buildDiff(null, targetRows.get(ti), "TARGET_ONLY", columns, pkCol));
                    ti++;
                } else if (ti >= targetRows.size()) {
                    diffs.add(buildDiff(sourceRows.get(si), null, "SOURCE_ONLY", columns, pkCol));
                    si++;
                } else {
                    Comparable sourcePk = toComparable(sourceRows.get(si).get(pkCol));
                    Comparable targetPk = toComparable(targetRows.get(ti).get(pkCol));
                    int cmp = sourcePk.compareTo(targetPk);

                    if (cmp == 0) {
                        DataDiff contentDiff = compareRowContent(sourceRows.get(si), targetRows.get(ti), columns, pkCol);
                        if (contentDiff != null) diffs.add(contentDiff);
                        si++;
                        ti++;
                    } else if (cmp < 0) {
                        diffs.add(buildDiff(sourceRows.get(si), null, "SOURCE_ONLY", columns, pkCol));
                        si++;
                    } else {
                        diffs.add(buildDiff(null, targetRows.get(ti), "TARGET_ONLY", columns, pkCol));
                        ti++;
                    }
                }

                if (diffs.size() >= maxDiffs) break;
            }

            Object maxPk = null;
            for (Map<String, Object> row : sourceRows) {
                Object pk = row.get(pkCol);
                if (maxPk == null || toComparable(pk).compareTo(toComparable(maxPk)) > 0) maxPk = pk;
            }
            for (Map<String, Object> row : targetRows) {
                Object pk = row.get(pkCol);
                if (maxPk == null || toComparable(pk).compareTo(toComparable(maxPk)) > 0) maxPk = pk;
            }

            if (maxPk != null) {
                if (lastPk != null && toComparable(maxPk).compareTo(toComparable(lastPk)) <= 0) {
                    cursor.setLastProcessedPk(null);
                    break;
                }
                lastPk = maxPk;
                cursor.setLastProcessedPk(lastPk);
                cursor.setScannedRows(cursor.getScannedRows() + sourceRows.size());
            } else {
                cursor.setLastProcessedPk(null);
                break;
            }

            if (sourceRows.size() < CHUNK_SIZE && targetRows.size() < CHUNK_SIZE) {
                cursor.setLastProcessedPk(null);
                break;
            }
        }

        return diffs;
    }

    private DataDiff compareRowContent(Map<String, Object> sourceRow, Map<String, Object> targetRow,
                                        List<ColumnMeta> columns, String pkCol) {
        List<String> diffFields = new ArrayList<>();
        for (ColumnMeta col : columns) {
            String colName = col.getName();
            Object sourceVal = sourceRow.get(colName);
            Object targetVal = targetRow.get(colName);
            if (!Objects.equals(normalizeValue(sourceVal), normalizeValue(targetVal))) {
                diffFields.add(colName);
            }
        }

        if (diffFields.isEmpty()) return null;

        DataDiff diff = new DataDiff();
        diff.setPrimaryKeyValue(sourceRow.get(pkCol));
        diff.setDiffType("CONTENT_DIFF");
        diff.setDiffFields(diffFields);
        diff.setSourceData(toJsonString(sourceRow, columns));
        diff.setTargetData(toJsonString(targetRow, columns));
        return diff;
    }

    private DataDiff buildDiff(Map<String, Object> sourceRow, Map<String, Object> targetRow,
                                String diffType, List<ColumnMeta> columns, String pkCol) {
        DataDiff diff = new DataDiff();
        diff.setDiffType(diffType);

        if ("SOURCE_ONLY".equals(diffType) && sourceRow != null) {
            diff.setPrimaryKeyValue(sourceRow.get(pkCol));
            diff.setSourceData(toJsonString(sourceRow, columns));
            diff.setTargetData(null);
        } else if ("TARGET_ONLY".equals(diffType) && targetRow != null) {
            diff.setPrimaryKeyValue(targetRow.get(pkCol));
            diff.setSourceData(null);
            diff.setTargetData(toJsonString(targetRow, columns));
        }

        return diff;
    }

    private String computeTableChecksum(Connection conn, String qualifiedTable, boolean isPg) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (isPg) {
                String sql = String.format(
                    "SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM %s t",
                    qualifiedTable
                );
                try {
                    ResultSet rs = stmt.executeQuery(sql);
                    if (rs.next()) return rs.getString(1);
                } catch (SQLException e) {
                    logger.debug("PG md5 aggregate failed, trying row count fallback: {}", e.getMessage());
                    String countSql = String.format("SELECT count(*) FROM %s", qualifiedTable);
                    ResultSet crs = stmt.executeQuery(countSql);
                    if (crs.next()) return "count:" + crs.getLong(1);
                }
            } else {
                try {
                    ResultSet rs = stmt.executeQuery("CHECKSUM TABLE " + qualifiedTable);
                    if (rs.next()) {
                        long checksum = rs.getLong(2);
                        return String.valueOf(checksum);
                    }
                } catch (SQLException e) {
                    logger.debug("MySQL CHECKSUM TABLE failed, using aggregate: {}", e.getMessage());
                    String sql = String.format(
                        "SELECT md5(GROUP_CONCAT(md5(CONCAT_WS('|', %s)) ORDER BY id SEPARATOR '')) FROM %s",
                        "*", qualifiedTable
                    );
                    try {
                        ResultSet rs = stmt.executeQuery(sql);
                        if (rs.next()) return rs.getString(1);
                    } catch (SQLException e2) {
                        String countSql = String.format("SELECT count(*) FROM %s", qualifiedTable);
                        ResultSet crs = stmt.executeQuery(countSql);
                        if (crs.next()) return "count:" + crs.getLong(1);
                    }
                }
            }
        }
        return null;
    }

    private String detectPrimaryKey(Connection conn, String qualifiedTable, boolean isPg, String dbName) {
        String sql;
        if (isPg) {
            sql = "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid " +
                  "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '" + qualifiedTable + "'::regclass " +
                  "AND i.indisprimary ORDER BY a.attnum LIMIT 1";
        } else {
            sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                  "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' LIMIT 1";
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            if (!isPg) {
                String[] parts = qualifiedTable.split("\\.");
                pstmt.setString(1, parts.length > 1 ? parts[0] : dbName);
                pstmt.setString(2, parts.length > 1 ? parts[1] : qualifiedTable);
            }
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) return rs.getString(1);
        } catch (SQLException e) {
            logger.warn("检测主键失败 {}: {}", qualifiedTable, e.getMessage());
        }

        try (PreparedStatement pstmt = conn.prepareStatement(
            isPg ? "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position LIMIT 1" :
                   "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION LIMIT 1")) {
            if (isPg) {
                String simpleName = qualifiedTable.contains(".") ? qualifiedTable.split("\\.")[1] : qualifiedTable;
                pstmt.setString(1, simpleName);
            } else {
                String[] parts = qualifiedTable.split("\\.");
                pstmt.setString(1, parts.length > 1 ? parts[0] : dbName);
                pstmt.setString(2, parts.length > 1 ? parts[1] : qualifiedTable);
            }
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) return rs.getString(1);
        } catch (SQLException e) {
            logger.warn("获取首列失败 {}: {}", qualifiedTable, e.getMessage());
        }

        return "id";
    }

    private List<ColumnMeta> getColumnMeta(Connection conn, String qualifiedTable, boolean isPg, String dbName) throws SQLException {
        List<ColumnMeta> columns = new ArrayList<>();
        String sql = isPg ?
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position" :
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            if (isPg) {
                String simpleName = qualifiedTable.contains(".") ? qualifiedTable.split("\\.")[1] : qualifiedTable;
                pstmt.setString(1, simpleName);
            } else {
                String[] parts = qualifiedTable.split("\\.");
                pstmt.setString(1, parts.length > 1 ? parts[0] : dbName);
                pstmt.setString(2, parts.length > 1 ? parts[1] : qualifiedTable);
            }
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                columns.add(new ColumnMeta(rs.getString(1), rs.getString(2)));
            }
        }
        return columns;
    }

    private long getRowCount(Connection conn, String qualifiedTable, boolean isPg) {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable);
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            logger.warn("获取行数失败 {}: {}", qualifiedTable, e.getMessage());
        }
        return -1;
    }

    private CompareCursor calculateCursorRange(Connection conn, String qualifiedTable, String pkCol, boolean isPg) {
        CompareCursor cursor = new CompareCursor();
        cursor.setTotalBuckets(BUCKET_COUNT);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable);
            if (rs.next()) cursor.setTotalRows(rs.getLong(1));
        } catch (SQLException e) {
            logger.warn("获取总行数失败: {}", e.getMessage());
        }
        return cursor;
    }

    private String buildChunkQuery(String qualifiedTable, String pkCol, String columnList, Object lastPk, boolean isPg) {
        String escapedPk = isPg ? "\"" + pkCol + "\"" : "`" + pkCol + "`";
        if (lastPk == null) {
            return String.format("SELECT %s FROM %s ORDER BY %s ASC LIMIT %d",
                columnList, qualifiedTable, escapedPk, CHUNK_SIZE);
        } else {
            return String.format("SELECT %s FROM %s WHERE %s > %s ORDER BY %s ASC LIMIT %d",
                columnList, qualifiedTable, escapedPk, formatValue(lastPk), escapedPk, CHUNK_SIZE);
        }
    }

    private String buildColumnList(List<ColumnMeta> columns, String pkCol) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            String col = columns.get(i).getName();
            sb.append(col);
        }
        return sb.toString();
    }

    private List<Map<String, Object>> executeQuery(Connection conn, String sql) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    String colName = meta.getColumnLabel(i);
                    Object value = safeGetObject(rs, i, meta);
                    row.put(colName, value);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private Object safeGetObject(ResultSet rs, int colIndex, ResultSetMetaData meta) throws SQLException {
        String colType = meta.getColumnTypeName(colIndex).toUpperCase();
        try {
            switch (colType) {
                case "BIT":
                    return rs.getBytes(colIndex);
                case "TIME":
                    return rs.getString(colIndex);
                case "YEAR":
                    return rs.getString(colIndex);
                default:
                    return rs.getObject(colIndex);
            }
        } catch (SQLException e) {
            logger.debug("getObject failed for col {} type {}, falling back to getString: {}", colIndex, colType, e.getMessage());
            return rs.getString(colIndex);
        }
    }

    private Object parseSyncObjectsValue(Object value) {
        if (value instanceof Map) return value;
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (!list.isEmpty() && list.get(0) instanceof String) {
                Map<String, Object> map = new HashMap<>();
                map.put("tables", list);
                return map;
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private Comparable toComparable(Object val) {
        if (val == null) return 0;
        if (val instanceof Comparable) return (Comparable) val;
        return val.toString();
    }

    private String formatValue(Object val) {
        if (val == null) return "NULL";
        if (val instanceof Number) return val.toString();
        return "'" + val.toString().replace("'", "''") + "'";
    }

    private Object normalizeValue(Object val) {
        if (val == null) return null;
        if (val instanceof java.math.BigInteger) return val.toString();
        if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).toPlainString();
        if (val instanceof java.time.LocalDateTime) return val.toString();
        if (val instanceof java.sql.Timestamp) return val.toString();
        if (val instanceof java.sql.Date) return val.toString();
        if (val instanceof java.sql.Time) return val.toString();
        if (val instanceof byte[]) return Base64.getEncoder().encodeToString((byte[]) val);
        return val;
    }

    private String toJsonString(Map<String, Object> row, List<ColumnMeta> columns) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            String col = columns.get(i).getName();
            Object val = normalizeValue(row.get(col));
            sb.append("\"").append(col).append("\":");
            if (val == null) sb.append("null");
            else if (val instanceof Number) sb.append(val);
            else sb.append("\"").append(val.toString().replace("\"", "\\\"")).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    private static class ParsedConn {
        String username, password, host, database;
        int port;
        boolean isPg;

        ParsedConn(String username, String password, String host, int port, String database, boolean isPg) {
            this.username = username;
            this.password = password;
            this.host = host;
            this.port = port;
            this.database = database;
            this.isPg = isPg;
        }
    }

    private ParsedConn parseConnection(String connectionStr) {
        String dbType = connectionStr.startsWith("postgresql://") ? "postgresql" : "mysql";
        Matcher matcher = CONNECTION_PATTERN.matcher(connectionStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("连接串格式不正确");
        }
        return new ParsedConn(matcher.group(1), matcher.group(2), matcher.group(3),
            Integer.parseInt(matcher.group(4)), matcher.group(5), "postgresql".equals(dbType));
    }

    private Connection createConnection(ParsedConn conn, boolean isPg) throws SQLException {
        try {
            if (isPg) {
                Class.forName("org.postgresql.Driver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
        } catch (ClassNotFoundException e) {
            throw new SQLException("驱动未找到: " + e.getMessage());
        }

        String url;
        if (isPg) {
            url = String.format("jdbc:postgresql://%s:%d/%s?currentSchema=public&stringtype=unspecified",
                conn.host, conn.port, conn.database != null ? conn.database : "postgres");
        } else {
            url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true",
                conn.host, conn.port, conn.database != null ? conn.database : "");
        }

        return DataSourcePoolManager.getConnection(url, conn.username, conn.password);
    }
}
