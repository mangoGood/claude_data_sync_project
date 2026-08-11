package com.migration.extract;

import com.migration.common.AbstractExtractor;
import com.migration.common.txn.TxnMetadata;
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

    /**
     * "值未随事件下发"的行内哨兵（对应 {@link com.migration.common.wire.CapTupleMarkers#UNCHANGED}）。
     *
     * <p>刻意用 {@code new String} 造一个独一无二的对象，判定一律走同一性比较（见 {@code isAbsent}），
     * 这样任何真实列值都不可能被误判成哨兵。
     */
    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final String ABSENT = new String("__value_not_sent__");

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

        stampTransaction(thlEvent, eventType, lsn);

        return thlEvent;
    }

    /**
     * 当前源事务标识。PG 的逻辑解码流本身就是 {@code BEGIN … 变更 … COMMIT} 的形态，
     * 且 BEGIN/COMMIT 都带 xid，直接拿 xid 做标识。不在事务中为 null。
     */
    private String currentTxId;

    /** 给事件打上源事务边界（{@code tx_id} / {@code tx_last}）。 */
    private void stampTransaction(THLEvent thlEvent, String eventType, String lsn) {
        if ("BEGIN".equals(eventType)) {
            Object xid = thlEvent.getMetadata().get("transaction_xid");
            currentTxId = "pg:" + (xid != null ? xid : lsn);
            thlEvent.addMetadata(TxnMetadata.TX_ID, currentTxId);
            return;
        }
        if ("COMMIT".equals(eventType)) {
            Object xid = thlEvent.getMetadata().get("transaction_xid");
            // 没见过 BEGIN（断点续传落在事务中间）时退化为该事件自成一事务
            thlEvent.addMetadata(TxnMetadata.TX_ID,
                    currentTxId != null ? currentTxId : "pg:" + (xid != null ? xid : lsn));
            thlEvent.addMetadata(TxnMetadata.TX_LAST, Boolean.TRUE);
            if (xid != null) {
                thlEvent.addMetadata(TxnMetadata.TX_SOURCE_ID, String.valueOf(xid));
            }
            currentTxId = null;
            return;
        }
        if (currentTxId != null) {
            thlEvent.addMetadata(TxnMetadata.TX_ID, currentTxId);
        }
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
            // INSERT 的 tuple 按协议不会出现"值未下发"（'u' 只用于 UPDATE 的未变更 TOAST）。
            // 真出现了就是还原不出这一行，写 NULL 等于静默塞错值，停下让人看。
            requireAllPresent(rowData, rowData.newValues, "INSERT");

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
        if (rowData == null) {
            return;
        }
        populateTableMetadata(thlEvent, rowData);

        // 未随事件下发的列（行外存储里本次未变更的 TOAST 值）必须整列从 SET 里摘掉：
        // 写 NULL 会把目标端已经正确的大字段抹掉，且全程无报错。摘掉之后 SET 列表不再是
        // 全列，靠 update_column_names 告诉下游这一批值对应哪些列。
        Subset after = rowData.newValues == null ? null
                : dropAbsentColumns(rowData, rowData.newValues, "UPDATE 后镜像");
        Subset before = rowData.oldValues == null ? null
                : dropAbsentColumns(rowData, rowData.oldValues, "UPDATE 前镜像");

        if (after != null) {
            thlEvent.addMetadata("row_data",
                    formatRowData(after.values, after.names, after.types));
            attachTypedRow(thlEvent, "rows_typed", after.values, after.types);
        }
        if (before != null) {
            thlEvent.addMetadata("row_data_before",
                    formatRowData(before.values, before.names, before.types));
            attachTypedRow(thlEvent, "rows_before_typed", before.values, before.types);
        }

        // 两个列名清单只在**确实摘掉了列**时下发：没摘时事件仍是全列，走既有的全宽路径，
        // 行为与改造前逐字节一致。摘过时下游（文本与类型化两条路径都读这两个 key）按子集生成 SQL。
        boolean trimmed = (after != null && after.dropped) || (before != null && before.dropped);
        if (trimmed && after != null) {
            thlEvent.addMetadata("update_column_names", String.join(",", after.names));
            // 没有前镜像时，下游（THLToSqlConverter 与 TypedDmlConverter 都是如此）拿后镜像当前镜像用，
            // 所以 WHERE 侧的列名清单必须与 SET 侧一致，否则列名与值对不上
            thlEvent.addMetadata("update_before_column_names",
                    String.join(",", before != null ? before.names : after.names));
        }
    }

    private void parseDeleteEvent(THLEvent thlEvent, String eventData) {
        thlEvent.addMetadata("operation", "DELETE");

        WalRowData rowData = parseWalRowEvent(eventData);
        if (rowData != null) {
            populateTableMetadata(thlEvent, rowData);
            // DELETE 的值只用于定位行。有主键时非主键列压根不参与 WHERE，未下发的值置空即可；
            // 主键列本身未下发、或整表无主键（WHERE 用整行）时定位不出来，只能停机。
            List<String> values = clearAbsentForDelete(rowData);

            String formattedRow = formatRowData(values, rowData.columnNames, rowData.columnTypes);
            thlEvent.addMetadata("row_data", formattedRow);
            attachTypedRow(thlEvent, "rows_typed", values, rowData.columnTypes);
        }
    }

    /** 摘掉"值未随事件下发"的列之后，列名/类型/值三者对齐的子集。 */
    private static final class Subset {
        final List<String> names;
        final List<String> types;
        final List<String> values;
        final boolean dropped;

        Subset(List<String> names, List<String> types, List<String> values, boolean dropped) {
            this.names = names;
            this.types = types;
            this.values = values;
            this.dropped = dropped;
        }
    }

    /**
     * 摘掉值未下发的列。主键列未下发时无法定位行，抛出让抽取停下——继续跑要么写错行，
     * 要么把这一行静默丢掉。
     */
    private Subset dropAbsentColumns(WalRowData rowData, List<String> values, String what) {
        List<String> names = rowData.columnNames;
        List<String> types = rowData.columnTypes;
        if (!hasAbsent(values)) {
            return new Subset(names, types, values, false);
        }
        List<String> keptNames = new ArrayList<>();
        List<String> keptTypes = new ArrayList<>();
        List<String> keptValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            String name = (names != null && i < names.size()) ? names.get(i) : "column" + i;
            if (isAbsent(values.get(i))) {
                if (isPrimaryKey(rowData, name)) {
                    throw new UnreconstructableValueException(String.format(
                            "%s.%s 的主键列 %s 未随 WAL 事件下发（%s），无法定位目标行",
                            rowData.schemaName, rowData.tableName, name, what));
                }
                continue;
            }
            keptNames.add(name);
            keptTypes.add((types != null && i < types.size()) ? types.get(i) : "");
            keptValues.add(values.get(i));
        }
        logger.debug("{}.{} {} 摘掉 {} 个未下发的列，剩余列: {}", rowData.schemaName, rowData.tableName,
                what, values.size() - keptValues.size(), keptNames);
        return new Subset(keptNames, keptTypes, keptValues, true);
    }

    /** DELETE：非主键列的未下发值置为 null（不参与 WHERE）；定位不出行时抛出。 */
    private List<String> clearAbsentForDelete(WalRowData rowData) {
        List<String> values = rowData.oldValues;
        if (values == null || !hasAbsent(values)) {
            return values;
        }
        boolean hasPk = rowData.primaryKeys != null && !rowData.primaryKeys.isEmpty();
        List<String> out = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            String name = (rowData.columnNames != null && i < rowData.columnNames.size())
                    ? rowData.columnNames.get(i) : "column" + i;
            if (isAbsent(values.get(i))) {
                if (!hasPk || isPrimaryKey(rowData, name)) {
                    throw new UnreconstructableValueException(String.format(
                            "%s.%s 的 DELETE 前镜像里列 %s 未随 WAL 事件下发，%s",
                            rowData.schemaName, rowData.tableName, name,
                            hasPk ? "该列是主键，无法定位目标行" : "该表无主键，WHERE 需要整行前镜像"));
                }
                out.add(null);
            } else {
                out.add(values.get(i));
            }
        }
        return out;
    }

    private void requireAllPresent(WalRowData rowData, List<String> values, String what) {
        if (hasAbsent(values)) {
            throw new UnreconstructableValueException(String.format(
                    "%s.%s 的 %s 事件里有列的值未随 WAL 下发，无法还原",
                    rowData.schemaName, rowData.tableName, what));
        }
    }

    private static boolean isPrimaryKey(WalRowData rowData, String column) {
        return rowData.primaryKeys != null && rowData.primaryKeys.contains(column);
    }

    private static boolean hasAbsent(List<String> values) {
        if (values == null) {
            return false;
        }
        for (String v : values) {
            if (isAbsent(v)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 该值是否"未随事件下发"。用同一性比较而非 equals：{@link #ABSENT} 是本类唯一的产地，
     * 真实列值再怎么巧合也不会是同一个对象，杜绝"某行数据恰好等于哨兵"的误判。
     */
    @SuppressWarnings("StringEquality")
    private static boolean isAbsent(String value) {
        return value == ABSENT;
    }

    /** 值未随 WAL 事件下发，且无法从别处还原（PG 的未变更 TOAST 落在主键上等）。 */
    static class UnreconstructableValueException extends RuntimeException {
        UnreconstructableValueException(String message) {
            super(message);
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

            if (value.startsWith(com.migration.common.wire.CapTupleMarkers.NULL)) {
                values.add(null);
            } else if (value.startsWith(com.migration.common.wire.CapTupleMarkers.UNCHANGED)) {
                // 值没随事件发过来（未变更的 TOAST）——与 NULL 是两回事，绝不能合流
                values.add(ABSENT);
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
