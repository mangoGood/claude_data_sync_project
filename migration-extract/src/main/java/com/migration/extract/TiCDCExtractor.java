package com.migration.extract;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.migration.thl.THLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TiDB 增量抽取：把 {@link com.migration.capture.TiCDCCapture} 落盘的 canal-json 记录
 * 转成 THL 事件，元数据契约与 MySQL binlog 链路完全一致，因此下游 increment
 * （类型化 DML 管道 / 文本回退路径 / DDL 演进）无需为 TiDB 做任何分支。
 *
 * <p>继承 {@link MySQLBinlogExtractor} 是为了复用 seqno 管理、pipeline、列元数据缓存与
 * information_schema 查询——TiDB 讲 MySQL 协议，这些完全通用；本类只替换
 * {@link #doExtract(byte[])} 这一步解析逻辑。
 *
 * <p>与 binlog 的两点本质差异：
 * <ul>
 *   <li><b>位点</b>：binlog 是 file+offset，TiCDC 是 TSO（commitTs）。同一事务多行共享 commitTs，
 *       故 eventId 追加 Kafka 位移做唯一后缀，避免死信按 eventId 去重时误伤同事务的其它行。</li>
 *   <li><b>UPDATE 前镜像</b>：canal 语义下 {@code old} 只带“发生变化的列”的旧值，
 *       完整前镜像 = 后镜像覆盖 old 中出现的列。</li>
 * </ul>
 */
public class TiCDCExtractor extends MySQLBinlogExtractor {

    private static final Logger logger = LoggerFactory.getLogger(TiCDCExtractor.class);

    private static final char FIELD_SEP = '\001';

    private final Gson gson = new Gson();

    @Override
    protected THLEvent doExtract(byte[] input) throws Exception {
        String eventStr = new String(input, StandardCharsets.UTF_8);
        if (eventStr.trim().isEmpty()) {
            return null;
        }

        // 记录布局：eventType|file|commitTs|timestampMs|serverId|kafkaOffset|canal-json
        // limit=7 保证 JSON 载荷即便含分隔符也不会被切碎（capture 已做过滤，这里再兜一层）
        String[] fields = eventStr.split(String.valueOf(FIELD_SEP), 7);
        if (fields.length < 6) {
            logger.warn("TiCDC 记录字段数不足，跳过: {}", eventStr.substring(0, Math.min(120, eventStr.length())));
            return null;
        }

        String eventType = fields[0].trim();
        String positionFile = fields[1].trim();
        long commitTs = parseLong(fields[2], -1);
        if (commitTs < 0) {
            logger.warn("TiCDC 记录位点非法，跳过: {}", fields[2]);
            return null;
        }
        long timestamp = parseLong(fields[3], System.currentTimeMillis());
        long serverId = parseLong(fields[4], 0);
        long kafkaOffset = parseLong(fields[5], 0);
        String payload = fields.length > 6 ? fields[6] : "";

        // 断点续传：TSO 单调递增，直接比大小即可（无需 binlog 那样先比文件名再比偏移）
        if (skipBeforeCheckpoint && checkpointBinlogPosition > 0 && commitTs > 0
                && commitTs < checkpointBinlogPosition) {
            return null;
        }

        THLEvent thlEvent = new THLEvent();
        thlEvent.setSeqno(seqno++);
        thlEvent.setEventId(positionFile + ":" + commitTs + ":" + kafkaOffset);
        thlEvent.setSourceId("tidb");
        thlEvent.setSourceTstamp(new Timestamp(timestamp > 0 ? timestamp : System.currentTimeMillis()));
        thlEvent.addMetadata("binlog_file", positionFile);
        thlEvent.addMetadata("binlog_position", commitTs);
        thlEvent.addMetadata("server_id", serverId);
        thlEvent.addMetadata("ticdc_commit_ts", commitTs);

        if ("SYNC_HEARTBEAT".equals(eventType)) {
            thlEvent.addMetadata("event_type", "SYNC_HEARTBEAT");
            thlEvent.setType(THLEvent.HEARTBEAT_EVENT);
            thlEvent.addMetadata("operation", "HEARTBEAT");
            thlEvent.addMetadata("source_db_timestamp", timestamp);
            return thlEvent;
        }

        JsonObject msg;
        try {
            msg = gson.fromJson(payload, JsonObject.class);
        } catch (Exception e) {
            logger.warn("TiCDC canal-json 解析失败，跳过 (commitTs={}): {}", commitTs, e.getMessage());
            return null;
        }
        if (msg == null) {
            return null;
        }

        if ("TICDC_DDL".equals(eventType)) {
            parseDdl(thlEvent, msg);
        } else {
            String operation = operationOf(eventType);
            if (operation == null) {
                logger.debug("忽略未知 TiCDC 事件类型: {}", eventType);
                return null;
            }
            parseRow(thlEvent, msg, operation);
        }

        if (pipeline != null) {
            thlEvent = pipeline.process(thlEvent);
        }

        // 多行事件预留 seqno 区间，与 MySQL 链路一致（下游按行拆分后各占一个 seqno）
        if (thlEvent != null) {
            Boolean multiRow = (Boolean) thlEvent.getMetadata().get("multi_row");
            if (multiRow != null && multiRow) {
                @SuppressWarnings("unchecked")
                List<String> rowsData = (List<String>) thlEvent.getMetadata().get("rows_data");
                if (rowsData != null && rowsData.size() > 1) {
                    long reservedSeqno = seqno - 1 + rowsData.size() - 1;
                    seqno = reservedSeqno + 1;
                }
            }
        }

        return thlEvent;
    }

    private static String operationOf(String eventType) {
        switch (eventType) {
            case "TICDC_INSERT":
                return "INSERT";
            case "TICDC_UPDATE":
                return "UPDATE";
            case "TICDC_DELETE":
                return "DELETE";
            default:
                return null;
        }
    }

    /** DDL 事件：落成与 binlog QUERY 事件同构的元数据，交给下游 SchemaEvolutionService 处理。 */
    private void parseDdl(THLEvent thlEvent, JsonObject msg) {
        String sql = optString(msg, "sql", "");
        String database = optString(msg, "database", "");
        thlEvent.addMetadata("event_type", "QUERY");
        thlEvent.addMetadata("operation", "QUERY");
        thlEvent.addMetadata("sql", sql);
        thlEvent.addMetadata("database_name", database);
        String ddlDatabase = DdlDatabaseExtractor.extractDatabase(sql, database, "");
        if (ddlDatabase != null && !ddlDatabase.isEmpty()) {
            thlEvent.addMetadata("ddl_database", ddlDatabase);
        }
        // 列布局可能已变：失效缓存，下个数据事件重新读 information_schema
        invalidateColumnCachesForDdl(sql, database);
    }

    private void parseRow(THLEvent thlEvent, JsonObject msg, String operation) {
        String database = optString(msg, "database", "");
        String table = optString(msg, "table", "");
        if (database.isEmpty() || table.isEmpty()) {
            logger.warn("TiCDC 行事件缺少库/表名，跳过");
            return;
        }

        thlEvent.addMetadata("event_type", operation);
        thlEvent.addMetadata("operation", operation);
        thlEvent.addMetadata("database_name", database);
        thlEvent.addMetadata("table_name", table);

        // 列元数据一律以源端 information_schema 为准（canal-json 的 mysqlType 不带精度/宽度，
        // 无法区分 tinyint(1)/bit(8) 这类必须靠 COLUMN_TYPE 才能还原的语义）
        List<String> columns = getTableColumns(database, table);
        if (columns.isEmpty()) {
            logger.warn("未能读取 {}.{} 的列元数据，跳过该事件", database, table);
            return;
        }
        List<String> columnTypes = getTableColumnTypes(database, table);
        List<String> columnFullTypes = tableColumnFullTypeCache.get(database + "." + table);
        List<String> pkColumns = getTablePrimaryKeys(database, table);

        thlEvent.addMetadata("column_names", String.join(",", columns));
        thlEvent.addMetadata("mysql_column_types", String.join(",", columnTypes));
        if (columnFullTypes != null) {
            thlEvent.addMetadata("mysql_column_full_types", String.join(",", columnFullTypes));
        }
        thlEvent.addMetadata("primary_keys", String.join(",", pkColumns));

        Map<String, List<String>> enumSetValues = enumSetValuesCache.get(database + "." + table);
        if (enumSetValues != null && !enumSetValues.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, List<String>> entry : enumSetValues.entrySet()) {
                if (sb.length() > 0) sb.append(";");
                sb.append(entry.getKey()).append("=").append(String.join(",", entry.getValue()));
            }
            thlEvent.addMetadata("enum_set_values", sb.toString());
        }

        List<Map<String, JsonElement>> dataRows = readRows(msg, "data");
        if (dataRows.isEmpty()) {
            logger.warn("TiCDC {} 事件无 data 段: {}.{}", operation, database, table);
            return;
        }
        List<Map<String, JsonElement>> oldRows = readRows(msg, "old");

        List<String> formattedRows = new ArrayList<>();
        List<ArrayList<Object>> typedRows = new ArrayList<>();
        List<String> formattedBeforeRows = new ArrayList<>();
        List<ArrayList<Object>> typedBeforeRows = new ArrayList<>();

        for (int r = 0; r < dataRows.size(); r++) {
            Map<String, JsonElement> after = dataRows.get(r);
            formattedRows.add(formatRow(after, columns, columnTypes, columnFullTypes));
            ArrayList<Object> typed = typeRow(after, columns, columnTypes, columnFullTypes);
            if (typedRows != null) {
                if (typed != null) typedRows.add(typed); else typedRows = null;
            }

            if ("UPDATE".equals(operation)) {
                // canal 语义：old 只带变化列的旧值 → 完整前镜像 = 后镜像 ⊕ old
                Map<String, JsonElement> changed = r < oldRows.size() ? oldRows.get(r) : null;
                Map<String, JsonElement> before = new LinkedHashMap<>(after);
                if (changed != null) {
                    before.putAll(changed);
                }
                formattedBeforeRows.add(formatRow(before, columns, columnTypes, columnFullTypes));
                ArrayList<Object> typedBefore = typeRow(before, columns, columnTypes, columnFullTypes);
                if (typedBeforeRows != null) {
                    if (typedBefore != null) typedBeforeRows.add(typedBefore); else typedBeforeRows = null;
                }
            }
        }

        thlEvent.addMetadata("row_data", formattedRows.get(0));
        thlEvent.addMetadata("rows_data", formattedRows);
        if (formattedRows.size() > 1) {
            thlEvent.addMetadata("multi_row", true);
        }
        if (typedRows != null && !typedRows.isEmpty()) {
            thlEvent.addMetadata("rows_typed", typedRows);
        }

        if ("UPDATE".equals(operation) && !formattedBeforeRows.isEmpty()) {
            thlEvent.addMetadata("row_data_before", formattedBeforeRows.get(0));
            thlEvent.addMetadata("rows_data_before", formattedBeforeRows);
            if (typedRows != null && typedBeforeRows != null
                    && typedBeforeRows.size() == typedRows.size()) {
                thlEvent.addMetadata("rows_before_typed", typedBeforeRows);
            }
        }
    }

    /** 读取 canal-json 的 data/old 段（数组里每个元素是 列名→值 的对象）。 */
    private List<Map<String, JsonElement>> readRows(JsonObject msg, String key) {
        List<Map<String, JsonElement>> rows = new ArrayList<>();
        if (msg == null || !msg.has(key) || msg.get(key).isJsonNull()) {
            return rows;
        }
        JsonElement el = msg.get(key);
        if (!el.isJsonArray()) {
            return rows;
        }
        JsonArray arr = el.getAsJsonArray();
        for (JsonElement item : arr) {
            if (item == null || !item.isJsonObject()) {
                rows.add(new LinkedHashMap<>());
                continue;
            }
            Map<String, JsonElement> row = new LinkedHashMap<>();
            for (Map.Entry<String, JsonElement> e : item.getAsJsonObject().entrySet()) {
                row.put(e.getKey(), e.getValue());
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * 类型化取值（供 PreparedStatement 绑定）。canal-json 里所有值都是字符串或 null，
     * 因此只需按源端列类型把少数几类还原成 Java 对象，其余字符串交由目标端按列类型隐式转换
     * ——与 MySQL binlog 链路 {@code typeRowValues} 的输出形态保持一致。
     */
    private ArrayList<Object> typeRow(Map<String, JsonElement> row, List<String> columns,
                                      List<String> columnTypes, List<String> columnFullTypes) {
        ArrayList<Object> typed = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            String type = i < columnTypes.size() ? columnTypes.get(i) : "";
            String fullType = columnFullTypes != null && i < columnFullTypes.size() ? columnFullTypes.get(i) : "";
            String lowerFull = fullType == null ? "" : fullType.toLowerCase();

            JsonElement el = row.get(col);
            if (el == null || el.isJsonNull()) {
                typed.add(null);
                continue;
            }
            String value;
            try {
                value = el.getAsString();
            } catch (Exception e) {
                value = el.toString();
            }

            if (isBinaryType(type) || isBlobType(type)) {
                // TiCDC 对二进制列做 ISO-8859-1 解码后再放进 JSON 字符串，
                // 每个原始字节对应一个 U+0000..U+00FF 码点，按同一编码取回即得原始字节
                typed.add(value.getBytes(StandardCharsets.ISO_8859_1));
            } else if (isBitType(type)) {
                Long bits = parseUnsignedLongOrNull(value);
                if (bits == null) {
                    return null;
                }
                typed.add(bitValueToBytes(bits, lowerFull));
            } else if (lowerFull.contains("tinyint(1)") && !lowerFull.contains("unsigned")) {
                if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
                    typed.add(Boolean.TRUE);
                } else if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
                    typed.add(Boolean.FALSE);
                } else {
                    return null;
                }
            } else {
                // 数值/时间/文本/JSON/ENUM/SET：canal-json 给的已经是最终文本形态
                typed.add(value);
            }
        }
        return typed;
    }

    /** 文本回退路径用的 SQL 字面量行（逗号分隔），语义与 MySQL 链路 {@code formatRowData} 对齐。 */
    private String formatRow(Map<String, JsonElement> row, List<String> columns,
                             List<String> columnTypes, List<String> columnFullTypes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(",");
            String col = columns.get(i);
            String type = i < columnTypes.size() ? columnTypes.get(i) : "";
            String fullType = columnFullTypes != null && i < columnFullTypes.size() ? columnFullTypes.get(i) : "";

            JsonElement el = row.get(col);
            if (el == null || el.isJsonNull()) {
                sb.append("null");
                continue;
            }
            String value;
            try {
                value = el.getAsString();
            } catch (Exception e) {
                value = el.toString();
            }

            if (isBinaryType(type) || isBlobType(type)) {
                sb.append("0x").append(bytesToHex(value.getBytes(StandardCharsets.ISO_8859_1)));
            } else if (isBitType(type)) {
                Long bits = parseUnsignedLongOrNull(value);
                sb.append(bits == null ? "null" : String.valueOf(bits));
            } else if (isNumericType(type, fullType)) {
                sb.append(value);
            } else {
                sb.append("'").append(escapeString(value)).append("'");
            }
        }
        return sb.toString();
    }

    private boolean isNumericType(String type, String fullType) {
        if (type == null) return false;
        String lower = type.toLowerCase();
        switch (lower) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
            case "integer":
            case "bigint":
            case "float":
            case "double":
            case "decimal":
            case "numeric":
            case "year":
                return true;
            default:
                return false;
        }
    }

    /** 按 bit(N) 宽度把位值转成大端 byte[]（与全量路径 MySQL 驱动返回的 BIT 形态一致）。 */
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

    /** BIT(64) 的值可能超出有符号 long 范围，按无符号解析后仍用 long 的位模式承载。 */
    private Long parseUnsignedLongOrNull(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        if (trimmed.isEmpty()) return null;
        try {
            return Long.parseUnsignedLong(trimmed);
        } catch (NumberFormatException e) {
            try {
                return new java.math.BigInteger(trimmed).longValue();
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static String optString(JsonObject obj, String key, String def) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return def;
        try {
            return obj.get(key).getAsString();
        } catch (Exception e) {
            return def;
        }
    }

    private static long parseLong(String s, long def) {
        try {
            return Long.parseLong(s.trim());
        } catch (Exception e) {
            return def;
        }
    }
}
