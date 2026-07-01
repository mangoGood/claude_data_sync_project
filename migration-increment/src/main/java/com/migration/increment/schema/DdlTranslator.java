package com.migration.increment.schema;

import com.migration.model.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 跨库 DDL 翻译器：将源库 DDL 转换为目标库可执行的 DDL。
 *
 * <p>当前支持 MySQL ↔ PostgreSQL 之间的常见 DDL 转换：
 * <ul>
 *   <li>CREATE TABLE：列类型映射、AUTO_INCREMENT → SERIAL/IDENTITY、引号风格转换</li>
 *   <li>ALTER TABLE：ADD/MODIFY/CHANGE COLUMN 的类型映射</li>
 *   <li>CREATE INDEX / DROP INDEX：基础语法适配</li>
 *   <li>DROP TABLE / TRUNCATE：语法兼容，直接透传</li>
 * </ul>
 *
 * <p>对于无法自动转换的 DDL（如存储过程、触发器、分区等复杂语法），
 * 返回 null 并记录警告，由调用方决定是否跳过或人工处理。
 */
public class DdlTranslator {
    private static final Logger logger = LoggerFactory.getLogger(DdlTranslator.class);

    public enum Direction {
        MYSQL_TO_POSTGRES,
        POSTGRES_TO_MYSQL,
        SAME_ENGINE
    }

    private final Direction direction;
    private final SchemaMappingConfig mappingConfig;

    /** MySQL 类型 → PG 类型（用于 DDL 内联类型提取） */
    private static final Pattern MYSQL_COLUMN_TYPE_PATTERN = Pattern.compile(
            "\\b(tinyint|smallint|mediumint|int|integer|bigint|float|double|decimal|numeric|" +
            "char|varchar|binary|varbinary|tinyblob|blob|mediumblob|longblob|" +
            "tinytext|text|mediumtext|longtext|date|time|datetime|timestamp|year|" +
            "boolean|bool|bit|enum|set|json)\\b(\\([^)]*\\))?",
            Pattern.CASE_INSENSITIVE);

    /** PG 类型 → MySQL 类型 */
    private static final Pattern PG_COLUMN_TYPE_PATTERN = Pattern.compile(
            "\\b(smallint|int2|integer|int|int4|bigint|int8|oid|serial|bigserial|" +
            "real|float4|double precision|float8|money|decimal|numeric|" +
            "character varying|varchar|character|char|text|" +
            "boolean|bool|date|time without time zone|time|timetz|" +
            "timestamp without time zone|timestamp|timestamptz|" +
            "interval|uuid|json|jsonb|bytea|bit varying|varbit|bit|" +
            "macaddr|macaddr8|inet|cidr|xml|name|tsvector|tsquery|hstore|ltree)\\b(\\([^)]*\\))?",
            Pattern.CASE_INSENSITIVE);

    /** AUTO_INCREMENT 关键字 */
    private static final Pattern MYSQL_AUTO_INCREMENT_PATTERN = Pattern.compile(
            "\\bAUTO_INCREMENT\\b", Pattern.CASE_INSENSITIVE);

    /** 反引号包裹的标识符 */
    private static final Pattern MYSQL_BACKTICK_PATTERN = Pattern.compile("`([^`]+)`");

    /** 双引号包裹的标识符 */
    private static final Pattern PG_DOUBLE_QUOTE_PATTERN = Pattern.compile("\"([^\"]+)\"");

    /** ENGINE=xxx 等 MySQL 表选项 */
    private static final Pattern MYSQL_ENGINE_PATTERN = Pattern.compile(
            "\\s*ENGINE\\s*=\\s*\\w+", Pattern.CASE_INSENSITIVE);

    /** CHARACTER SET / CHARSET 选项 */
    private static final Pattern MYSQL_CHARSET_PATTERN = Pattern.compile(
            "\\s*(DEFAULT\\s+)?(CHARACTER\\s+SET|CHARSET)\\s*=\\s*\\w+", Pattern.CASE_INSENSITIVE);

    /** COLLATE 选项 */
    private static final Pattern MYSQL_COLLATE_PATTERN = Pattern.compile(
            "\\s*(DEFAULT\\s+)?COLLATE\\s*=\\s*\\w+", Pattern.CASE_INSENSITIVE);

    /** ROW_FORMAT 选项 */
    private static final Pattern MYSQL_ROW_FORMAT_PATTERN = Pattern.compile(
            "\\s*ROW_FORMAT\\s*=\\s*\\w+", Pattern.CASE_INSENSITIVE);

    /** COMMENT 'xxx' 选项（表级） */
    private static final Pattern MYSQL_TABLE_COMMENT_PATTERN = Pattern.compile(
            "\\s*COMMENT\\s*=\\s*'[^']*'", Pattern.CASE_INSENSITIVE);

    public DdlTranslator(Direction direction, SchemaMappingConfig mappingConfig) {
        this.direction = direction;
        this.mappingConfig = mappingConfig;
    }

    /**
     * 翻译 DDL 语句。
     *
     * @param sql         原始 DDL SQL
     * @param sourceDb    源数据库（用于映射）
     * @return 目标库可执行的 SQL，若无法转换则返回 null
     */
    public String translate(String sql, String sourceDb) {
        if (sql == null || sql.trim().isEmpty()) {
            return null;
        }

        String trimmedSql = sql.trim();
        String upper = trimmedSql.toUpperCase();

        try {
            if (direction == Direction.SAME_ENGINE) {
                return applyIdentifierMapping(trimmedSql, sourceDb);
            }

            if (upper.startsWith("CREATE TABLE") || upper.startsWith("CREATE TEMPORARY TABLE")) {
                return translateCreateTable(trimmedSql, sourceDb);
            }
            if (upper.startsWith("ALTER TABLE")) {
                return translateAlterTable(trimmedSql, sourceDb);
            }
            if (upper.startsWith("DROP TABLE")) {
                return translateDropTable(trimmedSql, sourceDb);
            }
            if (upper.startsWith("TRUNCATE")) {
                return translateTruncate(trimmedSql, sourceDb);
            }
            if (upper.startsWith("CREATE INDEX") || upper.startsWith("CREATE UNIQUE INDEX")) {
                return translateCreateIndex(trimmedSql, sourceDb);
            }
            if (upper.startsWith("DROP INDEX")) {
                return translateDropIndex(trimmedSql, sourceDb);
            }
            if (upper.startsWith("RENAME TABLE")) {
                return translateRenameTable(trimmedSql, sourceDb);
            }

            logger.warn("DDL 类型暂不支持自动转换，跳过: {}", truncateSql(trimmedSql));
            return null;
        } catch (Exception e) {
            logger.warn("DDL 转换失败: {} | sql: {}", e.getMessage(), truncateSql(trimmedSql));
            return null;
        }
    }

    private String translateCreateTable(String sql, String sourceDb) {
        String result = sql;

        if (direction == Direction.MYSQL_TO_POSTGRES) {
            // 1. 反引号 → 双引号
            result = convertBacktickToDoubleQuote(result);
            // 2. AUTO_INCREMENT 列改为 SERIAL/BIGSERIAL（简化处理：移除 AUTO_INCREMENT，由 SERIAL 替代）
            result = convertMysqlAutoIncrementToPg(result);
            // 3. 列类型映射
            result = convertMysqlTypesToPg(result);
            // 4. 移除 MySQL 特有表选项
            result = stripMysqlTableOptions(result);
            // 5. UNSIGNED 关键字移除（PG 不支持）
            result = result.replaceAll("\\s+UNSIGNED", "");
            // 6. 移除 ON UPDATE CURRENT_TIMESTAMP（PG 不支持，需要用触发器实现）
            result = result.replaceAll("(?i)\\s+ON\\s+UPDATE\\s+CURRENT_TIMESTAMP", "");
            // 7. ENGINE/MEMORY 等表选项已在 stripMysqlTableOptions 处理
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            // 1. 双引号 → 反引号
            result = convertDoubleQuoteToBacktick(result);
            // 2. SERIAL → INT AUTO_INCREMENT
            result = convertPgSerialToMysql(result);
            // 3. 列类型映射
            result = convertPgTypesToMysql(result);
            // 4. 添加 ENGINE=InnoDB（若末尾没有）
            if (!result.toUpperCase().contains("ENGINE=")) {
                result = result.replaceAll("\\)\\s*$", ") ENGINE=InnoDB");
            }
        }

        return applyIdentifierMapping(result, sourceDb);
    }

    private String translateAlterTable(String sql, String sourceDb) {
        String result = sql;

        if (direction == Direction.MYSQL_TO_POSTGRES) {
            result = convertBacktickToDoubleQuote(result);
            result = convertMysqlTypesToPg(result);
            // ALTER TABLE ... MODIFY COLUMN → ALTER COLUMN ... TYPE
            result = result.replaceAll("(?i)MODIFY\\s+COLUMN", "ALTER COLUMN");
            result = result.replaceAll("(?i)MODIFY\\s+", "ALTER COLUMN ");
            // CHANGE COLUMN 在 PG 中需拆分为 RENAME + ALTER，这里简化为 ALTER COLUMN
            result = result.replaceAll("(?i)CHANGE\\s+COLUMN\\s+", "ALTER COLUMN ");
            result = result.replaceAll("\\s+UNSIGNED", "");
            result = stripMysqlTableOptions(result);
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            result = convertDoubleQuoteToBacktick(result);
            result = convertPgTypesToMysql(result);
            // PG: ALTER COLUMN ... TYPE → MySQL: MODIFY COLUMN
            result = result.replaceAll("(?i)ALTER\\s+COLUMN\\s+(\\w+)\\s+TYPE", "MODIFY COLUMN $1");
        }

        return applyIdentifierMapping(result, sourceDb);
    }

    private String translateDropTable(String sql, String sourceDb) {
        String result = sql;
        if (direction == Direction.MYSQL_TO_POSTGRES) {
            result = convertBacktickToDoubleQuote(result);
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            result = convertDoubleQuoteToBacktick(result);
        }
        return applyIdentifierMapping(result, sourceDb);
    }

    private String translateTruncate(String sql, String sourceDb) {
        String result = sql;
        if (direction == Direction.MYSQL_TO_POSTGRES) {
            result = convertBacktickToDoubleQuote(result);
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            result = convertDoubleQuoteToBacktick(result);
        }
        return applyIdentifierMapping(result, sourceDb);
    }

    private String translateCreateIndex(String sql, String sourceDb) {
        String result = sql;
        if (direction == Direction.MYSQL_TO_POSTGRES) {
            result = convertBacktickToDoubleQuote(result);
            // MySQL: CREATE INDEX idx ON tbl (col(20)) 前缀索引 → PG 不支持，移除前缀
            result = result.replaceAll("\\([^)]*\\)\\s*\\(", "(");
            // 移除 USING BTREE（PG 默认 btree）
            result = result.replaceAll("(?i)\\s+USING\\s+BTREE", "");
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            result = convertDoubleQuoteToBacktick(result);
            // PG: CREATE INDEX ... USING btree → MySQL 兼容
            result = result.replaceAll("(?i)\\s+USING\\s+btree", "");
        }
        return applyIdentifierMapping(result, sourceDb);
    }

    private String translateDropIndex(String sql, String sourceDb) {
        String result = sql;
        if (direction == Direction.MYSQL_TO_POSTGRES) {
            result = convertBacktickToDoubleQuote(result);
            // MySQL: DROP INDEX idx ON tbl → PG: DROP INDEX idx（需 schema）
            // 简化处理：保留 ON tbl 语法由调用方处理
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            result = convertDoubleQuoteToBacktick(result);
        }
        return applyIdentifierMapping(result, sourceDb);
    }

    private String translateRenameTable(String sql, String sourceDb) {
        String result = sql;
        if (direction == Direction.MYSQL_TO_POSTGRES) {
            result = convertBacktickToDoubleQuote(result);
            // MySQL: RENAME TABLE a TO b → PG: ALTER TABLE a RENAME TO b
            result = result.replaceAll("(?i)^RENAME\\s+TABLE\\s+", "ALTER TABLE ");
            result = result.replaceAll("(?i)\\s+TO\\s+", " RENAME TO ");
        } else if (direction == Direction.POSTGRES_TO_MYSQL) {
            result = convertDoubleQuoteToBacktick(result);
        }
        return applyIdentifierMapping(result, sourceDb);
    }

    /** 反引号 → 双引号 */
    private String convertBacktickToDoubleQuote(String sql) {
        Matcher m = MYSQL_BACKTICK_PATTERN.matcher(sql);
        return m.replaceAll("\"$1\"");
    }

    /** 双引号 → 反引号 */
    private String convertDoubleQuoteToBacktick(String sql) {
        Matcher m = PG_DOUBLE_QUOTE_PATTERN.matcher(sql);
        return m.replaceAll("`$1`");
    }

    /** MySQL AUTO_INCREMENT → PG 移除（依赖 SERIAL 转换或 IDENTITY） */
    private String convertMysqlAutoIncrementToPg(String sql) {
        // 简化处理：移除 AUTO_INCREMENT 关键字
        // 注意：理想方案是将 INT AUTO_INCREMENT PRIMARY KEY → SERIAL PRIMARY KEY
        // 但需要解析列定义，这里采用简化策略
        return MYSQL_AUTO_INCREMENT_PATTERN.matcher(sql).replaceAll("");
    }

    /** PG SERIAL/BIGSERIAL → MySQL INT/BIGINT AUTO_INCREMENT */
    private String convertPgSerialToMysql(String sql) {
        String result = sql;
        result = result.replaceAll("(?i)\\bbigserial\\b", "BIGINT AUTO_INCREMENT");
        result = result.replaceAll("(?i)\\bserial\\b", "INT AUTO_INCREMENT");
        return result;
    }

    /** MySQL 列类型 → PG 列类型 */
    private String convertMysqlTypesToPg(String sql) {
        Matcher m = MYSQL_COLUMN_TYPE_PATTERN.matcher(sql);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String mysqlType = m.group(1).toLowerCase();
            String sizeSpec = m.group(2);
            String pgType = mapMysqlTypeToPg(mysqlType, sizeSpec);
            m.appendReplacement(sb, pgType);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** PG 列类型 → MySQL 列类型 */
    private String convertPgTypesToMysql(String sql) {
        Matcher m = PG_COLUMN_TYPE_PATTERN.matcher(sql);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String pgType = m.group(1).toLowerCase();
            String sizeSpec = m.group(2);
            String mysqlType = mapPgTypeToMysql(pgType, sizeSpec);
            m.appendReplacement(sb, mysqlType);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private String mapMysqlTypeToPg(String mysqlType, String sizeSpec) {
        // 处理带括号的类型，如 varchar(255)、decimal(10,2)
        switch (mysqlType) {
            case "tinyint":
                // tinyint(1) 在 MySQL 中是 BOOL 的别名，映射为 BOOLEAN
                return "(1)".equals(sizeSpec) ? "BOOLEAN" : "SMALLINT";
            case "smallint":
                return "SMALLINT";
            case "mediumint":
                return "INTEGER";
            case "int":
            case "integer":
                return "INTEGER";
            case "bigint":
                return "BIGINT";
            case "float":
                return "REAL";
            case "double":
                return "DOUBLE PRECISION";
            case "decimal":
            case "numeric":
                return sizeSpec != null ? "NUMERIC" + sizeSpec : "NUMERIC";
            case "char":
                return sizeSpec != null ? "CHAR" + sizeSpec : "CHAR(1)";
            case "varchar":
                return sizeSpec != null ? "VARCHAR" + sizeSpec : "VARCHAR(255)";
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return "BYTEA";
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                return "TEXT";
            case "date":
                return "DATE";
            case "time":
                return "TIME";
            case "datetime":
            case "timestamp":
                return "TIMESTAMP";
            case "year":
                return "SMALLINT";
            case "boolean":
            case "bool":
                return "BOOLEAN";
            case "bit":
                return sizeSpec != null ? "BIT" + sizeSpec : "BOOLEAN";
            case "enum":
            case "set":
                return "VARCHAR(255)";
            case "json":
                return "JSONB";
            default:
                return TypeMapper.mapMysqlToPg(mysqlType);
        }
    }

    private String mapPgTypeToMysql(String pgType, String sizeSpec) {
        switch (pgType) {
            case "smallint":
            case "int2":
                return "SMALLINT";
            case "integer":
            case "int":
            case "int4":
                return "INT";
            case "bigint":
            case "int8":
            case "oid":
                return "BIGINT";
            case "serial":
                return "INT";
            case "bigserial":
                return "BIGINT";
            case "real":
            case "float4":
                return "FLOAT";
            case "double precision":
            case "float8":
                return "DOUBLE";
            case "money":
                return "DECIMAL(19,2)";
            case "decimal":
            case "numeric":
                return sizeSpec != null ? "DECIMAL" + sizeSpec : "DECIMAL";
            case "character varying":
            case "varchar":
                return sizeSpec != null ? "VARCHAR" + sizeSpec : "VARCHAR(255)";
            case "character":
            case "char":
                return sizeSpec != null ? "CHAR" + sizeSpec : "CHAR(1)";
            case "text":
                return "TEXT";
            case "boolean":
            case "bool":
                return "TINYINT(1)";
            case "date":
                return "DATE";
            case "time without time zone":
            case "time":
            case "timetz":
                return "TIME";
            case "timestamp without time zone":
            case "timestamp":
            case "timestamptz":
                return "DATETIME";
            case "interval":
                return "TEXT";
            case "uuid":
                return "CHAR(36)";
            case "json":
            case "jsonb":
                return "JSON";
            case "bytea":
                return "BLOB";
            case "bit varying":
            case "varbit":
                return sizeSpec != null ? "VARCHAR" + sizeSpec : "VARCHAR(255)";
            case "bit":
                return sizeSpec != null ? "BIT" + sizeSpec : "BIT(1)";
            case "macaddr":
                return "CHAR(17)";
            case "macaddr8":
                return "CHAR(23)";
            case "inet":
            case "cidr":
                return "VARCHAR(45)";
            case "xml":
                return "LONGTEXT";
            case "name":
                return "VARCHAR(64)";
            case "tsvector":
            case "tsquery":
            case "hstore":
            case "ltree":
                return "TEXT";
            default:
                return TypeMapper.mapPgToMysql(pgType);
        }
    }

    /** 移除 MySQL 特有表选项 */
    private String stripMysqlTableOptions(String sql) {
        String result = sql;
        result = MYSQL_ENGINE_PATTERN.matcher(result).replaceAll("");
        result = MYSQL_CHARSET_PATTERN.matcher(result).replaceAll("");
        result = MYSQL_COLLATE_PATTERN.matcher(result).replaceAll("");
        result = MYSQL_ROW_FORMAT_PATTERN.matcher(result).replaceAll("");
        result = MYSQL_TABLE_COMMENT_PATTERN.matcher(result).replaceAll("");
        // 移除 AUTO_INCREMENT=N
        result = result.replaceAll("(?i)\\s*AUTO_INCREMENT\\s*=\\s*\\d+", "");
        return result;
    }

    /** 应用标识符映射（数据库名/表名） */
    private String applyIdentifierMapping(String sql, String sourceDb) {
        if (mappingConfig == null) return sql;
        String result = sql;
        // 简化处理：替换 sourceDb 为 targetDb
        if (sourceDb != null && !sourceDb.isEmpty()) {
            String targetDb = mappingConfig.mapDatabase(sourceDb);
            if (!sourceDb.equals(targetDb)) {
                // 替换 "sourceDb". 或 `sourceDb`. 或 sourceDb.
                result = result.replaceAll(
                        "(?i)([\"`])" + java.util.regex.Pattern.quote(sourceDb) + "\\1\\.",
                        "$1" + targetDb + "$1.");
                result = result.replaceAll(
                        "(?i)\\b" + java.util.regex.Pattern.quote(sourceDb) + "\\.",
                        targetDb + ".");
            }
        }
        return result;
    }

    private String truncateSql(String sql) {
        return sql.length() > 100 ? sql.substring(0, 100) + "..." : sql;
    }
}
