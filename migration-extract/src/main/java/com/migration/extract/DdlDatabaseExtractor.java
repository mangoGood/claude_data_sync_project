package com.migration.extract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DdlDatabaseExtractor {

    private static final Logger logger = LoggerFactory.getLogger(DdlDatabaseExtractor.class);

    private static final String IDENT = "(`[^`]+`|\\w+)";
    private static final String IDENT_NC = "(?:`[^`]+`|\\w+)";
    private static final String DOT_SEP = "\\s*\\.\\s*";

    // 捕获组 1 = 可选库名，捕获组 2 = 表名（用于失效缓存的表级 DDL 解析）
    private static final String BQ_OR_WORD = "`[^`]+`|\\w+";
    private static final String TABLE_REF = "(?:(" + BQ_OR_WORD + ")" + DOT_SEP + ")?(" + BQ_OR_WORD + ")";

    // ALTER/CREATE/TRUNCATE 单表；DROP 可多表，单独处理
    private static final Pattern SINGLE_TABLE_DDL_PATTERN = Pattern.compile(
            "(?i)^\\s*(?:ALTER\\s+TABLE"
                    + "|CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:TEMPORARY\\s+)?TABLE(?:\\s+IF\\s+NOT\\s+EXISTS)?"
                    + "|TRUNCATE(?:\\s+TABLE)?)\\s+" + TABLE_REF);

    private static final Pattern DROP_TABLE_PREFIX_PATTERN =
            Pattern.compile("(?i)^\\s*DROP\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?");

    private static final Pattern TABLE_REF_ANCHORED_PATTERN =
            Pattern.compile("(?i)^\\s*" + TABLE_REF);

    private static final Pattern RENAME_TABLE_PREFIX_PATTERN =
            Pattern.compile("(?i)^\\s*RENAME\\s+TABLE\\s+");

    // groups: 1=旧库 2=旧表 3=新库 4=新表
    private static final Pattern RENAME_PAIR_PATTERN =
            Pattern.compile("(?i)" + TABLE_REF + "\\s+TO\\s+" + TABLE_REF);

    private static final Pattern DDL_TABLE_DB_PATTERN =
            Pattern.compile("(?i)(?:ALTER\\s+TABLE|CREATE\\s+(?:TEMPORARY\\s+)?TABLE(?:\\s+IF\\s+NOT\\s+EXISTS)?|DROP\\s+TABLE(?:\\s+IF\\s+EXISTS)?|TRUNCATE(?:\\s+TABLE)?|RENAME\\s+TABLE)\\s+" + IDENT + DOT_SEP + IDENT_NC);

    private static final Pattern DDL_INDEX_DB_PATTERN =
            Pattern.compile("(?i)(?:CREATE\\s+(?:UNIQUE\\s+|FULLTEXT\\s+|SPATIAL\\s+)?INDEX|DROP\\s+INDEX(?:\\s+IF\\s+EXISTS)?)\\s+" + IDENT_NC + "\\s+ON\\s+" + IDENT + DOT_SEP + IDENT_NC);

    private static final Pattern DDL_DATABASE_PATTERN =
            Pattern.compile("(?i)(?:CREATE\\s+(?:DATABASE|SCHEMA)(?:\\s+IF\\s+NOT\\s+EXISTS)?|DROP\\s+(?:DATABASE|SCHEMA)(?:\\s+IF\\s+EXISTS)?)\\s+" + IDENT);

    private static final Pattern RENAME_TABLE_DB_PATTERN =
            Pattern.compile("(?i)RENAME\\s+TABLE\\s+" + IDENT + DOT_SEP + IDENT_NC + "\\s+TO\\s+" + IDENT_NC + DOT_SEP + IDENT_NC);

    public static String extractDatabase(String sql, String binlogDatabase, String defaultDatabase) {
        if (sql == null || sql.trim().isEmpty()) {
            return resolveDatabase(binlogDatabase, defaultDatabase);
        }

        String trimmedSql = sql.trim();

        Matcher dbMatcher = DDL_DATABASE_PATTERN.matcher(trimmedSql);
        if (dbMatcher.find()) {
            String dbName = stripBackticks(dbMatcher.group(1));
            logger.debug("Extracted database from CREATE/DROP DATABASE: {}", dbName);
            return dbName;
        }

        Matcher tableDbMatcher = DDL_TABLE_DB_PATTERN.matcher(trimmedSql);
        if (tableDbMatcher.find()) {
            String rawDb = tableDbMatcher.group(1);
            String dbName = stripBackticks(rawDb);
            logger.debug("Extracted database from DDL table reference: {}", dbName);
            return dbName;
        }

        Matcher renameDbMatcher = RENAME_TABLE_DB_PATTERN.matcher(trimmedSql);
        if (renameDbMatcher.find()) {
            String rawDb = renameDbMatcher.group(1);
            String dbName = stripBackticks(rawDb);
            logger.debug("Extracted database from RENAME TABLE reference: {}", dbName);
            return dbName;
        }

        Matcher indexDbMatcher = DDL_INDEX_DB_PATTERN.matcher(trimmedSql);
        if (indexDbMatcher.find()) {
            String rawDb = indexDbMatcher.group(1);
            String dbName = stripBackticks(rawDb);
            logger.debug("Extracted database from DDL index reference: {}", dbName);
            return dbName;
        }

        if (isDdlStatement(trimmedSql)) {
            logger.debug("DDL without explicit db prefix, using binlog database: {}", binlogDatabase);
            return resolveDatabase(binlogDatabase, defaultDatabase);
        }

        return resolveDatabase(binlogDatabase, defaultDatabase);
    }

    /**
     * 正则兜底：解析表级 DDL 影响的表，返回 {@code database.table} 键（未限定库名用 defaultDatabase 补全）。
     * 仅作为 {@link DdlDatabaseAnltrExtractor#extractAffectedTables} 的 ANTLR 解析失败回退。
     */
    public static List<String> extractAffectedTables(String sql, String defaultDatabase) {
        List<String> tables = new ArrayList<>();
        if (sql == null || sql.trim().isEmpty()) {
            return tables;
        }
        String trimmedSql = sql.trim();

        Matcher renamePrefix = RENAME_TABLE_PREFIX_PATTERN.matcher(trimmedSql);
        if (renamePrefix.find()) {
            Matcher pair = RENAME_PAIR_PATTERN.matcher(trimmedSql.substring(renamePrefix.end()));
            while (pair.find()) {
                addTableKey(tables, pair.group(1), pair.group(2), defaultDatabase);
                addTableKey(tables, pair.group(3), pair.group(4), defaultDatabase);
            }
            return tables;
        }

        // DROP TABLE t1, t2, ... 逐表失效缓存
        Matcher dropPrefix = DROP_TABLE_PREFIX_PATTERN.matcher(trimmedSql);
        if (dropPrefix.find()) {
            String rest = trimmedSql.substring(dropPrefix.end())
                    .replaceAll("(?i)\\s+(?:RESTRICT|CASCADE)\\s*$", "")
                    .replaceAll(";\\s*$", "");
            for (String part : rest.split(",")) {
                Matcher ref = TABLE_REF_ANCHORED_PATTERN.matcher(part.trim());
                if (ref.find()) {
                    addTableKey(tables, ref.group(1), ref.group(2), defaultDatabase);
                }
            }
            return tables;
        }

        Matcher single = SINGLE_TABLE_DDL_PATTERN.matcher(trimmedSql);
        if (single.find()) {
            addTableKey(tables, single.group(1), single.group(2), defaultDatabase);
        }
        return tables;
    }

    private static void addTableKey(List<String> tables, String rawSchema, String rawTable, String defaultDatabase) {
        if (rawTable == null || rawTable.isEmpty()) {
            return;
        }
        String table = stripBackticks(rawTable);
        String schema = (rawSchema != null && !rawSchema.isEmpty()) ? stripBackticks(rawSchema) : defaultDatabase;
        String key = (schema == null ? "" : schema) + "." + table;
        if (!tables.contains(key)) {
            tables.add(key);
        }
    }

    private static boolean isDdlStatement(String sql) {
        String upper = sql.toUpperCase().trim();
        return upper.startsWith("CREATE") || upper.startsWith("ALTER")
                || upper.startsWith("DROP") || upper.startsWith("TRUNCATE")
                || upper.startsWith("RENAME");
    }

    private static String resolveDatabase(String binlogDatabase, String defaultDatabase) {
        if (binlogDatabase != null && !binlogDatabase.isEmpty()) {
            return binlogDatabase;
        }
        if (defaultDatabase != null && !defaultDatabase.isEmpty()) {
            return defaultDatabase;
        }
        return null;
    }

    private static String stripBackticks(String identifier) {
        if (identifier == null) return null;
        identifier = identifier.trim();
        if (identifier.startsWith("`") && identifier.endsWith("`")) {
            return identifier.substring(1, identifier.length() - 1).replace("``", "`");
        }
        return identifier;
    }
}
