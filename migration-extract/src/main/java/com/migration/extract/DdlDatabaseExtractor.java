package com.migration.extract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DdlDatabaseExtractor {

    private static final Logger logger = LoggerFactory.getLogger(DdlDatabaseExtractor.class);

    private static final String IDENT = "(`[^`]+`|\\w+)";
    private static final String IDENT_NC = "(?:`[^`]+`|\\w+)";
    private static final String DOT_SEP = "\\s*\\.\\s*";

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
