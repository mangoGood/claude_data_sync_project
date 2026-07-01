package com.migration.increment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlConflictKeyParser {

    private static final Logger logger = LoggerFactory.getLogger(SqlConflictKeyParser.class);

    private static final Pattern INSERT_TABLE_PATTERN =
            Pattern.compile("INSERT\\s+INTO\\s+`?(\\w+)`?\\.`?(\\w+)`?", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_TABLE_PATTERN =
            Pattern.compile("UPDATE\\s+`?(\\w+)`?\\.`?(\\w+)`?\\s+SET", Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_TABLE_PATTERN =
            Pattern.compile("DELETE\\s+FROM\\s+`?(\\w+)`?\\.`?(\\w+)`?", Pattern.CASE_INSENSITIVE);
    private static final Pattern USE_DB_PATTERN =
            Pattern.compile("USE\\s+`?(\\w+)`?\\s*;", Pattern.CASE_INSENSITIVE);

    private static final Pattern WHERE_PK_PATTERN =
            Pattern.compile("WHERE\\s+`?(\\w+)`?\\s*=\\s*([^;\\s]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_VALUES_PATTERN =
            Pattern.compile("VALUES\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_COLUMNS_PATTERN =
            Pattern.compile("INSERT\\s+INTO\\s+`?\\w+`?\\.`?\\w+`?\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);

    private final Map<String, String> tablePrimaryKeyMap;
    private int idCounter = 0;

    public SqlConflictKeyParser() {
        this.tablePrimaryKeyMap = new HashMap<>();
    }

    public void registerPrimaryKey(String tableName, String primaryKeyName) {
        tablePrimaryKeyMap.put(tableName.toLowerCase(), primaryKeyName);
        logger.debug("Registered primary key for table {}: {}", tableName, primaryKeyName);
    }

    public SqlStatement parse(String sql, long seqno) {
        sql = sql.trim();
        if (sql.isEmpty()) return null;

        String ddlDatabase = null;
        String strippedSql = sql;
        Matcher useMatcher = USE_DB_PATTERN.matcher(sql);
        if (useMatcher.find()) {
            ddlDatabase = useMatcher.group(1);
            int semicolonIdx = sql.indexOf(';', useMatcher.start());
            if (semicolonIdx >= 0 && semicolonIdx < sql.length() - 1) {
                strippedSql = sql.substring(semicolonIdx + 1).trim();
            }
        }

        SqlStatement.OperationType type = detectOperationType(strippedSql.isEmpty() ? sql : strippedSql);
        String database = null;
        String tableName = null;
        String primaryKeyValue = null;

        if (type == SqlStatement.OperationType.COMMIT) {
            return new SqlStatement.Builder()
                    .id(idCounter++)
                    .seqno(seqno)
                    .sql(sql)
                    .operationType(type)
                    .ddlDatabase(ddlDatabase)
                    .isTransactionBoundary(true)
                    .build();
        }

        if (type == SqlStatement.OperationType.INSERT
                || type == SqlStatement.OperationType.UPDATE
                || type == SqlStatement.OperationType.DELETE) {
            String[] tableInfo = extractTableInfo(strippedSql.isEmpty() ? sql : strippedSql, type);
            if (tableInfo != null) {
                database = tableInfo[0];
                tableName = tableInfo[1];
            }
            primaryKeyValue = extractPrimaryKey(strippedSql.isEmpty() ? sql : strippedSql, type, tableName);
        }

        if (type == SqlStatement.OperationType.DDL) {
            return new SqlStatement.Builder()
                    .id(idCounter++)
                    .seqno(seqno)
                    .sql(sql)
                    .operationType(type)
                    .database(database)
                    .ddlDatabase(ddlDatabase)
                    .tableName(tableName)
                    .isTransactionBoundary(true)
                    .build();
        }

        if (type == SqlStatement.OperationType.DCL) {
            return new SqlStatement.Builder()
                    .id(idCounter++)
                    .seqno(seqno)
                    .sql(sql)
                    .operationType(type)
                    .ddlDatabase(ddlDatabase)
                    .isTransactionBoundary(true)
                    .build();
        }

        return new SqlStatement.Builder()
                .id(idCounter++)
                .seqno(seqno)
                .sql(sql)
                .operationType(type)
                .database(database)
                .tableName(tableName)
                .primaryKeyValue(primaryKeyValue)
                .build();
    }

    private SqlStatement.OperationType detectOperationType(String sql) {
        String upperSql = sql.toUpperCase().trim();

        if (upperSql.startsWith("USE ")) {
            String afterUse = stripUsePrefix(upperSql);
            if (afterUse != null) {
                return detectOperationType(afterUse);
            }
            return SqlStatement.OperationType.OTHER;
        }

        if (upperSql.startsWith("INSERT")) return SqlStatement.OperationType.INSERT;
        if (upperSql.startsWith("UPDATE")) return SqlStatement.OperationType.UPDATE;
        if (upperSql.startsWith("DELETE")) return SqlStatement.OperationType.DELETE;
        if (upperSql.startsWith("COMMIT") || upperSql.startsWith("ROLLBACK"))
            return SqlStatement.OperationType.COMMIT;
        if (upperSql.startsWith("CREATE") || upperSql.startsWith("ALTER")
                || upperSql.startsWith("DROP") || upperSql.startsWith("TRUNCATE")
                || upperSql.startsWith("RENAME"))
            return SqlStatement.OperationType.DDL;
        if (upperSql.startsWith("GRANT") || upperSql.startsWith("REVOKE")
                || upperSql.startsWith("SET PASSWORD") || upperSql.startsWith("SET GLOBAL")
                || upperSql.startsWith("SET SESSION") || upperSql.startsWith("SET NAMES")
                || upperSql.startsWith("SET CHARACTER") || upperSql.startsWith("SET DEFAULT ROLE")
                || upperSql.startsWith("SET ROLE") || upperSql.startsWith("CREATE USER")
                || upperSql.startsWith("ALTER USER") || upperSql.startsWith("DROP USER")
                || upperSql.startsWith("RENAME USER") || upperSql.startsWith("CREATE ROLE")
                || upperSql.startsWith("DROP ROLE") || upperSql.startsWith("LOCK TABLES")
                || upperSql.startsWith("UNLOCK TABLES") || upperSql.startsWith("FLUSH"))
            return SqlStatement.OperationType.DCL;
        return SqlStatement.OperationType.OTHER;
    }

    private String stripUsePrefix(String upperSql) {
        int semicolonIdx = upperSql.indexOf(';');
        if (semicolonIdx >= 0 && semicolonIdx < upperSql.length() - 1) {
            return upperSql.substring(semicolonIdx + 1).trim();
        }
        return null;
    }

    private String[] extractTableInfo(String sql, SqlStatement.OperationType type) {
        Pattern pattern;
        switch (type) {
            case INSERT: pattern = INSERT_TABLE_PATTERN; break;
            case UPDATE: pattern = UPDATE_TABLE_PATTERN; break;
            case DELETE: pattern = DELETE_TABLE_PATTERN; break;
            default: return null;
        }

        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            return new String[]{matcher.group(1), matcher.group(2)};
        }
        return null;
    }

    private String extractPrimaryKey(String sql, SqlStatement.OperationType type, String tableName) {
        String pkName = "id";
        if (tableName != null) {
            String registered = tablePrimaryKeyMap.get(tableName.toLowerCase());
            if (registered != null) {
                pkName = registered;
            }
        }

        switch (type) {
            case INSERT: return extractInsertPrimaryKey(sql, pkName);
            case UPDATE:
            case DELETE: return extractWherePrimaryKey(sql, pkName);
            default: return null;
        }
    }

    private String extractInsertPrimaryKey(String sql, String pkName) {
        Matcher columnsMatcher = INSERT_COLUMNS_PATTERN.matcher(sql);
        if (columnsMatcher.find()) {
            String columns = columnsMatcher.group(1);
            String[] columnArray = columns.split(",");
            int pkIndex = -1;
            for (int i = 0; i < columnArray.length; i++) {
                String col = columnArray[i].trim().replaceAll("`", "");
                if (col.equalsIgnoreCase(pkName)) {
                    pkIndex = i;
                    break;
                }
            }

            if (pkIndex >= 0) {
                Matcher valuesMatcher = INSERT_VALUES_PATTERN.matcher(sql);
                if (valuesMatcher.find()) {
                    String values = valuesMatcher.group(1);
                    String[] valueArray = splitValues(values);
                    if (pkIndex < valueArray.length) {
                        return cleanValue(valueArray[pkIndex].trim());
                    }
                }
            }
        }
        return null;
    }

    private String[] splitValues(String values) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        StringBuilder current = new StringBuilder();
        boolean inString = false;

        for (int i = 0; i < values.length(); i++) {
            char c = values.charAt(i);
            if (c == '\'' && (i == 0 || values.charAt(i - 1) != '\\')) {
                inString = !inString;
                current.append(c);
            } else if (!inString && c == '(') {
                depth++;
                current.append(c);
            } else if (!inString && c == ')') {
                depth--;
                current.append(c);
            } else if (!inString && c == ',' && depth == 0) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            result.add(current.toString());
        }
        return result.toArray(new String[0]);
    }

    private String extractWherePrimaryKey(String sql, String pkName) {
        Pattern pattern = Pattern.compile(
                "WHERE\\s+`?" + Pattern.quote(pkName) + "`?\\s*=\\s*([^;\\s]+)",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            return cleanValue(matcher.group(1));
        }
        return null;
    }

    private String cleanValue(String value) {
        if (value == null) return null;
        value = value.trim();
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }

    public void reset() {
        idCounter = 0;
    }
}
