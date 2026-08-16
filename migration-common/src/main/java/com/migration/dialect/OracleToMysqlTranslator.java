package com.migration.dialect;

import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Oracle → MySQL。
 *
 * <p>与 {@link OracleToPgTranslator} 的两处关键差别：
 *
 * <ol>
 *   <li><b>标识符不转小写</b>。Oracle→PG 会把大写名转成小写（PG 的非引号标识符本就折叠成小写），
 *       但 MySQL 在 Linux 上表名默认大小写敏感，这里转了小写，增量 apply 与行数对比仍按同步对象里的
 *       大写名去找表，就会各找各的。保持源端原样是唯一自洽的选择。</li>
 *   <li><b>增量字面量必须翻译</b>。LogMiner 的 SQL_REDO 里日期是 {@code TO_DATE('...','...')}、
 *       二进制是 {@code HEXTORAW('...')}。PG 恰好也有 {@code to_date/to_timestamp} 函数，所以
 *       Oracle→PG 原样透传就能跑；MySQL 没有这些函数，透传过去整条 SQL 直接报错。
 *       {@link #convertLiteral} 负责把它们拆成 MySQL 认得的字面量。</li>
 * </ol>
 */
public class OracleToMysqlTranslator implements TypeTranslator {

    /** TO_DATE('值','格式') / TO_TIMESTAMP(...) / TO_TIMESTAMP_TZ(...)：取第一个参数即可。 */
    private static final Pattern TO_DATE_LIKE = Pattern.compile(
            "^\\s*TO_(?:DATE|TIMESTAMP|TIMESTAMP_TZ)\\s*\\(\\s*('(?:[^']|'')*')\\s*(?:,.*)?\\)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** TO_YMINTERVAL('1-2') / TO_DSINTERVAL('1 02:03:04')：MySQL 侧落 VARCHAR，取字面量。 */
    private static final Pattern TO_INTERVAL = Pattern.compile(
            "^\\s*TO_(?:YM|DS)INTERVAL\\s*\\(\\s*('(?:[^']|'')*')\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** HEXTORAW('AABB') → MySQL 的 0xAABB。 */
    private static final Pattern HEXTORAW = Pattern.compile(
            "^\\s*HEXTORAW\\s*\\(\\s*'([0-9A-Fa-f]*)'\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE);

    /** UNISTR('...')：Oracle 的 Unicode 字面量包装，MySQL 直接用里面的字符串。 */
    private static final Pattern UNISTR = Pattern.compile(
            "^\\s*UNISTR\\s*\\(\\s*('(?:[^']|'')*')\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    @Override
    public boolean isHomogeneous() {
        return false;
    }

    @Override
    public String generateCreateTable(TableInfo table, SqlDialect targetDialect) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ")
          .append(targetDialect.quoteIdentifier(table.getTargetTableName()))
          .append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        List<String> pkColumns = new ArrayList<>();

        for (ColumnInfo col : table.getColumns()) {
            columnDefs.add("  " + targetDialect.quoteIdentifier(col.getColumnName())
                    + " " + TypeMapper.mapOracleToMysqlColumnDef(col));
            if (col.isPrimaryKey()) {
                pkColumns.add(col.getColumnName());
            }
        }

        if (!pkColumns.isEmpty()) {
            StringBuilder pkDef = new StringBuilder("  PRIMARY KEY (");
            for (int i = 0; i < pkColumns.size(); i++) {
                if (i > 0) {
                    pkDef.append(", ");
                }
                pkDef.append(targetDialect.quoteIdentifier(pkColumns.get(i)));
            }
            pkDef.append(")");
            columnDefs.add(pkDef.toString());
        }

        sb.append(String.join(",\n", columnDefs));
        sb.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        return sb.toString();
    }

    @Override
    public Object convertValue(Object value, String sourceTypeName, ResultSet rs, int colIndex)
            throws SQLException {
        if (value == null || sourceTypeName == null) {
            return value;
        }
        String lowerType = sourceTypeName.toLowerCase().trim();

        // Oracle DATE 含日期+时间，目标是 MySQL DATETIME —— 按 java.sql.Date 取会把时分秒抹掉
        if (lowerType.startsWith("date")) {
            if (value instanceof java.sql.Date) {
                java.sql.Timestamp ts = rs.getTimestamp(colIndex);
                return ts != null ? toLocal(ts) : value;
            }
            return toLocal(value);
        }

        // 带时区的时间：目标列是 VARCHAR，保留 readColumnValue 给出的字符串原文（含偏移量）
        if (TypeMapper.isOracleTimestampTzType(lowerType)) {
            return value instanceof String ? value : String.valueOf(value);
        }

        if (lowerType.startsWith("timestamp")) {
            return toLocal(value);
        }

        // INTERVAL 目标列是 VARCHAR：驱动给的可能是 INTERVALDS/INTERVALYM 对象
        if (lowerType.startsWith("interval")) {
            return value instanceof String ? value : String.valueOf(value);
        }

        // BOOLEAN → TINYINT(1)
        if (lowerType.startsWith("boolean") || lowerType.equals("bool")) {
            if (value instanceof Boolean) {
                return ((Boolean) value) ? 1 : 0;
            }
            if (value instanceof Number) {
                return ((Number) value).intValue() != 0 ? 1 : 0;
            }
            String s = String.valueOf(value).trim().toLowerCase();
            return ("true".equals(s) || "t".equals(s) || "1".equals(s) || "y".equals(s)) ? 1 : 0;
        }

        // NUMBER：目标是 DECIMAL/整数族，BigDecimal 原样交给驱动，别转 double（会丢精度）
        // BLOB/RAW 已被 readColumnValue 转成 byte[]，CLOB 转成 String，MySQL 都直接接受
        return value;
    }

    /**
     * 剥掉最外层的一对单引号并还原被翻倍的内层单引号；不是完整的引号字面量则返回 null。
     */
    private static String unwrapQuoted(String v) {
        if (v.length() < 2 || v.charAt(0) != '\'' || v.charAt(v.length() - 1) != '\'') {
            return null;
        }
        return v.substring(1, v.length() - 1).replace("''", "'");
    }

    /** 认得出来就返回 MySQL 字面量，认不出来返回 null（交给调用方按原样处理）。 */
    private static String convertFunctionLiteral(String v) {
        Matcher m = TO_DATE_LIKE.matcher(v);
        if (m.matches()) {
            return m.group(1);
        }
        m = TO_INTERVAL.matcher(v);
        if (m.matches()) {
            return m.group(1);
        }
        m = HEXTORAW.matcher(v);
        if (m.matches()) {
            String hex = m.group(1);
            // 0x 后面必须有内容，空 RAW 用空字符串字面量表达
            return hex.isEmpty() ? "''" : "0x" + hex;
        }
        m = UNISTR.matcher(v);
        if (m.matches()) {
            return m.group(1);
        }
        if (v.equalsIgnoreCase("EMPTY_CLOB()") || v.equalsIgnoreCase("EMPTY_BLOB()")) {
            return "''";
        }
        return null;
    }

    /**
     * 时间值一律交成 {@link java.time.LocalDateTime}（墙上时间），不要交 {@code java.sql.Timestamp}。
     *
     * <p>Timestamp 表示的是**时间点**：从 Oracle 读出来时按 JVM 默认时区解释，写进 MySQL 时驱动又按
     * 连接的 {@code serverTimezone=UTC} 折算一次，于是 {@code 10:20:30} 落库变成 {@code 02:20:30}
     * ——整整差一个时区，而且两端都"没报错"。Oracle 的 DATE/TIMESTAMP 本就不带时区，语义是墙上时间，
     * 用 LocalDateTime 交给驱动就不会再被折算。
     */
    private static Object toLocal(Object value) {
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }
        if (value instanceof java.time.LocalDateTime) {
            return value;
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().atStartOfDay();
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime()).toLocalDateTime();
        }
        return value;
    }

    /**
     * 增量文本路径逐值转换（与 {@link #convertValue} 对象路径成对，同库对同处维护）。
     *
     * <p>翻译 LogMiner SQL_REDO 里的 Oracle 函数字面量：
     * {@code TO_DATE/TO_TIMESTAMP/TO_TIMESTAMP_TZ} → 纯字符串字面量、
     * {@code HEXTORAW('AABB')} → {@code 0xAABB}、{@code TO_YMINTERVAL/TO_DSINTERVAL} → 字符串、
     * {@code EMPTY_CLOB()/EMPTY_BLOB()} → 空值字面量。
     * 不翻译的话这些函数会原样拼进 MySQL 的 SQL，整条语句直接报 FUNCTION does not exist。
     */
    @Override
    public String convertLiteral(String rawLiteral, String sourceColumnType) {
        // 与 PgToMysqlTranslator 同口径：返回值会被直接拼进 SQL，null 进来必须给出 SQL 的 NULL
        if (rawLiteral == null) {
            return "NULL";
        }
        String v = rawLiteral.trim();
        if (v.isEmpty() || v.equalsIgnoreCase("NULL")) {
            return "NULL";
        }

        // THL 的文本行数据把 Oracle 函数字面量当普通字符串**整个引起来并转义**了，
        // 实测形态是 {@code 'HEXTORAW(''aabb06'')'}（外层一对单引号 + 内层单引号翻倍）。
        // 不先剥这层壳，下面的模式一个也匹配不上，整串会被当字符串塞进目标列
        // ——实测 18 个字符塞进 VARBINARY(16)，报 Data too long。
        String unwrapped = unwrapQuoted(v);
        if (unwrapped != null) {
            String converted = convertFunctionLiteral(unwrapped);
            if (converted != null) {
                return converted;
            }
        }
        String direct = convertFunctionLiteral(v);
        if (direct != null) {
            return direct;
        }

        String lowerType = sourceColumnType == null ? "" : sourceColumnType.toLowerCase().trim();
        if (lowerType.startsWith("boolean") || lowerType.equals("bool")) {
            String bare = v.replace("'", "").trim().toLowerCase();
            if ("true".equals(bare) || "t".equals(bare) || "y".equals(bare) || "1".equals(bare)) {
                return "1";
            }
            if ("false".equals(bare) || "f".equals(bare) || "n".equals(bare) || "0".equals(bare)) {
                return "0";
            }
        }

        return v;
    }
}
