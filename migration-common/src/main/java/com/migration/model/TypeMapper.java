package com.migration.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class TypeMapper {

    private static final Map<String, String> PG_TO_MYSQL_TYPE_MAP = new LinkedHashMap<>();
    private static final Map<String, String> MYSQL_TO_PG_TYPE_MAP = new LinkedHashMap<>();

    static {
        PG_TO_MYSQL_TYPE_MAP.put("smallint", "SMALLINT");
        PG_TO_MYSQL_TYPE_MAP.put("int2", "SMALLINT");
        PG_TO_MYSQL_TYPE_MAP.put("integer", "INT");
        PG_TO_MYSQL_TYPE_MAP.put("int", "INT");
        PG_TO_MYSQL_TYPE_MAP.put("int4", "INT");
        PG_TO_MYSQL_TYPE_MAP.put("bigint", "BIGINT");
        PG_TO_MYSQL_TYPE_MAP.put("int8", "BIGINT");
        PG_TO_MYSQL_TYPE_MAP.put("oid", "BIGINT");
        PG_TO_MYSQL_TYPE_MAP.put("serial", "INT");
        PG_TO_MYSQL_TYPE_MAP.put("bigserial", "BIGINT");
        PG_TO_MYSQL_TYPE_MAP.put("real", "FLOAT");
        PG_TO_MYSQL_TYPE_MAP.put("float4", "FLOAT");
        PG_TO_MYSQL_TYPE_MAP.put("double precision", "DOUBLE");
        PG_TO_MYSQL_TYPE_MAP.put("float8", "DOUBLE");
        PG_TO_MYSQL_TYPE_MAP.put("money", "DECIMAL(19,2)");
        PG_TO_MYSQL_TYPE_MAP.put("character", "CHAR");
        PG_TO_MYSQL_TYPE_MAP.put("char", "CHAR");
        PG_TO_MYSQL_TYPE_MAP.put("character varying", "VARCHAR");
        PG_TO_MYSQL_TYPE_MAP.put("varchar", "VARCHAR");
        PG_TO_MYSQL_TYPE_MAP.put("text", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("boolean", "TINYINT(1)");
        PG_TO_MYSQL_TYPE_MAP.put("bool", "TINYINT(1)");
        PG_TO_MYSQL_TYPE_MAP.put("date", "DATE");
        PG_TO_MYSQL_TYPE_MAP.put("time without time zone", "TIME");
        PG_TO_MYSQL_TYPE_MAP.put("time", "TIME");
        PG_TO_MYSQL_TYPE_MAP.put("time with time zone", "TIME");
        PG_TO_MYSQL_TYPE_MAP.put("timetz", "TIME");
        PG_TO_MYSQL_TYPE_MAP.put("timestamp without time zone", "DATETIME");
        PG_TO_MYSQL_TYPE_MAP.put("timestamp", "DATETIME");
        PG_TO_MYSQL_TYPE_MAP.put("timestamp with time zone", "DATETIME");
        PG_TO_MYSQL_TYPE_MAP.put("timestamptz", "DATETIME");
        PG_TO_MYSQL_TYPE_MAP.put("interval", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("uuid", "CHAR(36)");
        PG_TO_MYSQL_TYPE_MAP.put("json", "JSON");
        PG_TO_MYSQL_TYPE_MAP.put("jsonb", "JSON");
        PG_TO_MYSQL_TYPE_MAP.put("bytea", "BLOB");
        PG_TO_MYSQL_TYPE_MAP.put("bit", "BIT");
        PG_TO_MYSQL_TYPE_MAP.put("bit varying", "VARCHAR");
        PG_TO_MYSQL_TYPE_MAP.put("varbit", "VARCHAR");
        PG_TO_MYSQL_TYPE_MAP.put("macaddr", "CHAR(17)");
        PG_TO_MYSQL_TYPE_MAP.put("macaddr8", "CHAR(23)");
        PG_TO_MYSQL_TYPE_MAP.put("inet", "VARCHAR(45)");
        PG_TO_MYSQL_TYPE_MAP.put("cidr", "VARCHAR(45)");
        PG_TO_MYSQL_TYPE_MAP.put("point", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("line", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("lseg", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("box", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("path", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("polygon", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("circle", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("xml", "LONGTEXT");
        PG_TO_MYSQL_TYPE_MAP.put("name", "VARCHAR(64)");
        PG_TO_MYSQL_TYPE_MAP.put("tsvector", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("tsquery", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("hstore", "TEXT");
        PG_TO_MYSQL_TYPE_MAP.put("ltree", "TEXT");

        MYSQL_TO_PG_TYPE_MAP.put("tinyint", "SMALLINT");
        MYSQL_TO_PG_TYPE_MAP.put("smallint", "SMALLINT");
        MYSQL_TO_PG_TYPE_MAP.put("mediumint", "INTEGER");
        MYSQL_TO_PG_TYPE_MAP.put("int", "INTEGER");
        MYSQL_TO_PG_TYPE_MAP.put("integer", "INTEGER");
        MYSQL_TO_PG_TYPE_MAP.put("bigint", "BIGINT");
        MYSQL_TO_PG_TYPE_MAP.put("float", "REAL");
        MYSQL_TO_PG_TYPE_MAP.put("double", "DOUBLE PRECISION");
        MYSQL_TO_PG_TYPE_MAP.put("decimal", "NUMERIC");
        MYSQL_TO_PG_TYPE_MAP.put("numeric", "NUMERIC");
        MYSQL_TO_PG_TYPE_MAP.put("char", "CHAR");
        MYSQL_TO_PG_TYPE_MAP.put("varchar", "VARCHAR");
        MYSQL_TO_PG_TYPE_MAP.put("binary", "BYTEA");
        MYSQL_TO_PG_TYPE_MAP.put("varbinary", "BYTEA");
        MYSQL_TO_PG_TYPE_MAP.put("tinyblob", "BYTEA");
        MYSQL_TO_PG_TYPE_MAP.put("blob", "BYTEA");
        MYSQL_TO_PG_TYPE_MAP.put("mediumblob", "BYTEA");
        MYSQL_TO_PG_TYPE_MAP.put("longblob", "BYTEA");
        MYSQL_TO_PG_TYPE_MAP.put("tinytext", "TEXT");
        MYSQL_TO_PG_TYPE_MAP.put("text", "TEXT");
        MYSQL_TO_PG_TYPE_MAP.put("mediumtext", "TEXT");
        MYSQL_TO_PG_TYPE_MAP.put("longtext", "TEXT");
        MYSQL_TO_PG_TYPE_MAP.put("date", "DATE");
        MYSQL_TO_PG_TYPE_MAP.put("time", "INTERVAL");
        MYSQL_TO_PG_TYPE_MAP.put("datetime", "TIMESTAMP");
        MYSQL_TO_PG_TYPE_MAP.put("timestamp", "TIMESTAMP");
        MYSQL_TO_PG_TYPE_MAP.put("year", "SMALLINT");
        MYSQL_TO_PG_TYPE_MAP.put("boolean", "BOOLEAN");
        MYSQL_TO_PG_TYPE_MAP.put("bool", "BOOLEAN");
        MYSQL_TO_PG_TYPE_MAP.put("bit", "BIT");
        MYSQL_TO_PG_TYPE_MAP.put("enum", "VARCHAR");
        MYSQL_TO_PG_TYPE_MAP.put("set", "VARCHAR");
        MYSQL_TO_PG_TYPE_MAP.put("json", "JSONB");
    }

    public static String mapPgToMysql(String pgType) {
        if (pgType == null) {
            return "TEXT";
        }
        String lowerType = pgType.toLowerCase().trim();

        if (lowerType.startsWith("numeric") || lowerType.startsWith("decimal")) {
            return null;
        }
        if (lowerType.startsWith("character varying") || lowerType.startsWith("varchar")) {
            return null;
        }
        if (lowerType.startsWith("character") || lowerType.startsWith("char")) {
            return null;
        }
        if (lowerType.startsWith("bit varying") || lowerType.startsWith("varbit")) {
            return null;
        }
        if (lowerType.startsWith("bit")) {
            return null;
        }

        if (lowerType.endsWith("[]")) {
            return "TEXT";
        }

        String mapped = PG_TO_MYSQL_TYPE_MAP.get(lowerType);
        if (mapped != null) {
            return mapped;
        }

        for (Map.Entry<String, String> entry : PG_TO_MYSQL_TYPE_MAP.entrySet()) {
            if (lowerType.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }

        return "TEXT";
    }

    public static String mapMysqlToPg(String mysqlType) {
        if (mysqlType == null) {
            return "TEXT";
        }
        String lowerType = mysqlType.toLowerCase().trim();

        if (lowerType.startsWith("tinyint")) {
            return "SMALLINT";
        }
        if (lowerType.startsWith("smallint")) {
            return "SMALLINT";
        }
        if (lowerType.startsWith("mediumint")) {
            return "INTEGER";
        }
        if (lowerType.startsWith("int") || lowerType.startsWith("integer")) {
            return "INTEGER";
        }
        if (lowerType.startsWith("bigint")) {
            return "BIGINT";
        }
        if (lowerType.startsWith("float")) {
            return "REAL";
        }
        if (lowerType.startsWith("double")) {
            return "DOUBLE PRECISION";
        }
        if (lowerType.startsWith("decimal") || lowerType.startsWith("numeric")) {
            return "NUMERIC";
        }
        if (lowerType.startsWith("varchar")) {
            return "VARCHAR";
        }
        if (lowerType.startsWith("char")) {
            return "CHAR";
        }
        if (lowerType.startsWith("text") || lowerType.startsWith("tinytext") ||
            lowerType.startsWith("mediumtext") || lowerType.startsWith("longtext")) {
            return "TEXT";
        }
        if (lowerType.startsWith("binary") || lowerType.startsWith("varbinary") ||
            lowerType.startsWith("blob") || lowerType.startsWith("tinyblob") ||
            lowerType.startsWith("mediumblob") || lowerType.startsWith("longblob")) {
            return "BYTEA";
        }
        if (lowerType.startsWith("datetime") || lowerType.startsWith("timestamp")) {
            return "TIMESTAMP";
        }
        if (lowerType.startsWith("date")) {
            return "DATE";
        }
        if (lowerType.startsWith("time")) {
            // MySQL TIME 是时长语义，范围 ±838:59:59 超出 PG time(0-24h)，映射为 INTERVAL
            return "INTERVAL";
        }
        if (lowerType.startsWith("year")) {
            return "SMALLINT";
        }
        if (lowerType.startsWith("boolean") || lowerType.startsWith("bool")) {
            return "BOOLEAN";
        }
        if (lowerType.startsWith("bit")) {
            return "BIT";
        }
        if (lowerType.startsWith("enum") || lowerType.startsWith("set")) {
            return "VARCHAR";
        }
        if (lowerType.startsWith("json")) {
            return "JSONB";
        }

        String mapped = MYSQL_TO_PG_TYPE_MAP.get(lowerType);
        if (mapped != null) {
            return mapped;
        }

        for (Map.Entry<String, String> entry : MYSQL_TO_PG_TYPE_MAP.entrySet()) {
            if (lowerType.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }

        return "TEXT";
    }

    /**
     * 从 MySQL 类型字符串中提取 size，例如 "varchar(255)" 提取 255。
     * 如果类型字符串中没有 size，则使用 column.getColumnSize()。
     * 注意：fillMysqlColumnInfo 使用 DESCRIBE 获取类型，不设置 columnSize，
     * 所以优先从类型字符串中提取。
     */
    private static int extractSizeFromType(String lowerType, ColumnInfo column) {
        // 优先从类型字符串中提取 size，例如 varchar(255) -> 255
        if (lowerType.contains("(") && lowerType.contains(")")) {
            String sizeStr = lowerType.replaceAll(".*\\((\\d+)\\).*", "$1");
            try {
                return Integer.parseInt(sizeStr);
            } catch (NumberFormatException e) {
                // 忽略，使用 columnSize
            }
        }
        // 回退到 column.getColumnSize()
        return column.getColumnSize();
    }

    public static String mapMysqlToPgColumnDef(ColumnInfo column) {
        String mysqlType = column.getDataType();
        if (mysqlType == null) {
            return "TEXT";
        }
        String lowerType = mysqlType.toLowerCase().trim();

        String pgType;
        boolean isAutoIncrement = column.isAutoIncrement();

        if (lowerType.startsWith("tinyint")) {
            // tinyint(1) 在 MySQL 中是 BOOL 的别名，映射为 BOOLEAN
            // 注意：fillMysqlColumnInfo 使用 DESCRIBE 获取类型，不设置 columnSize，
            // 所以这里通过类型字符串中的 "(1)" 来判断
            if (lowerType.contains("(1)") && !lowerType.contains("unsigned")) {
                pgType = "BOOLEAN";
            } else if (lowerType.contains("unsigned")) {
                pgType = "SMALLINT";
            } else {
                pgType = "SMALLINT";
            }
        } else if (lowerType.startsWith("smallint")) {
            pgType = lowerType.contains("unsigned") ? "INTEGER" : "SMALLINT";
        } else if (lowerType.startsWith("mediumint")) {
            pgType = "INTEGER";
        } else if (lowerType.startsWith("int") || lowerType.startsWith("integer")) {
            pgType = lowerType.contains("unsigned") ? "BIGINT" : "INTEGER";
        } else if (lowerType.startsWith("bigint")) {
            // MySQL BIGINT UNSIGNED 最大值 18446744073709551615 超出 PG BIGINT 范围(9223372036854775807)
            // 映射为 NUMERIC(20,0) 以支持无符号大数
            pgType = lowerType.contains("unsigned") ? "NUMERIC(20,0)" : "BIGINT";
        } else if (lowerType.startsWith("float")) {
            pgType = "REAL";
        } else if (lowerType.startsWith("double")) {
            pgType = "DOUBLE PRECISION";
        } else if (lowerType.startsWith("decimal") || lowerType.startsWith("numeric")) {
            // 从类型字符串中提取 precision 和 scale，例如 decimal(10,2)
            int precision = 0;
            int scale = 0;
            if (lowerType.contains("(") && lowerType.contains(")")) {
                String sizeContent = lowerType.replaceAll(".*\\(([^)]+)\\).*", "$1");
                String[] parts = sizeContent.split(",");
                try {
                    precision = Integer.parseInt(parts[0].trim());
                    if (parts.length > 1) {
                        scale = Integer.parseInt(parts[1].trim());
                    }
                } catch (NumberFormatException e) {
                    // 忽略，使用 column 的值
                }
            }
            if (precision == 0) {
                precision = column.getColumnSize();
                scale = column.getDecimalDigits();
            }
            if (scale > 0) {
                pgType = "NUMERIC(" + precision + "," + scale + ")";
            } else if (precision > 0) {
                pgType = "NUMERIC(" + precision + ")";
            } else {
                pgType = "NUMERIC";
            }
        } else if (lowerType.startsWith("varchar")) {
            // 从类型字符串中提取 size，例如 varchar(255)
            int size = extractSizeFromType(lowerType, column);
            pgType = size > 0 ? "VARCHAR(" + size + ")" : "VARCHAR(255)";
        } else if (lowerType.startsWith("char")) {
            // 从类型字符串中提取 size，例如 char(10)
            int size = extractSizeFromType(lowerType, column);
            pgType = size > 0 ? "CHAR(" + size + ")" : "CHAR(1)";
        } else if (lowerType.startsWith("text") || lowerType.startsWith("tinytext") ||
                   lowerType.startsWith("mediumtext") || lowerType.startsWith("longtext")) {
            pgType = "TEXT";
        } else if (lowerType.startsWith("binary") || lowerType.startsWith("varbinary") ||
                   lowerType.startsWith("blob") || lowerType.startsWith("tinyblob") ||
                   lowerType.startsWith("mediumblob") || lowerType.startsWith("longblob")) {
            pgType = "BYTEA";
        } else if (lowerType.startsWith("datetime") || lowerType.startsWith("timestamp")) {
            pgType = "TIMESTAMP";
        } else if (lowerType.startsWith("date")) {
            pgType = "DATE";
        } else if (lowerType.startsWith("time")) {
            // MySQL TIME 时长范围 ±838:59:59 超出 PG time，映射为 INTERVAL
            pgType = "INTERVAL";
        } else if (lowerType.startsWith("year")) {
            pgType = "SMALLINT";
        } else if (lowerType.startsWith("boolean") || lowerType.startsWith("bool")) {
            pgType = "BOOLEAN";
        } else if (lowerType.startsWith("bit")) {
            // MySQL BIT(n): n=1 时映射为 BOOLEAN，n>1 时映射为 BYTEA（二进制数据）
            // 注意：fillMysqlColumnInfo 使用 DESCRIBE 获取类型，不设置 columnSize，
            // 所以这里通过类型字符串中的 "(n)" 来判断
            if (lowerType.contains("(1)")) {
                pgType = "BOOLEAN";
            } else {
                // BIT(n>1) 映射为 BYTEA，因为 MySQL BIT 存储二进制数据
                pgType = "BYTEA";
            }
        } else if (lowerType.startsWith("enum") || lowerType.startsWith("set")) {
            pgType = "VARCHAR(255)";
        } else if (lowerType.startsWith("json")) {
            pgType = "JSONB";
        } else {
            pgType = mapMysqlToPg(mysqlType);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(pgType);

        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }

        if (isAutoIncrement) {
            // PG uses SERIAL or IDENTITY instead of AUTO_INCREMENT
        } else if (column.getDefaultValue() != null && !column.getDefaultValue().isEmpty()) {
            String defaultVal = column.getDefaultValue();
            if (defaultVal.equalsIgnoreCase("CURRENT_TIMESTAMP") || defaultVal.equalsIgnoreCase("NOW()")) {
                sb.append(" DEFAULT CURRENT_TIMESTAMP");
            } else if (defaultVal.equalsIgnoreCase("NULL")) {
                // skip
            } else if (defaultVal.startsWith("'") || defaultVal.matches("-?\\d+(\\.\\d+)?")) {
                sb.append(" DEFAULT ").append(defaultVal);
            } else {
                sb.append(" DEFAULT '").append(defaultVal).append("'");
            }
        }

        return sb.toString();
    }

    public static String mapPgToMysqlColumnDef(ColumnInfo column) {
        String pgType = column.getDataType();
        if (pgType == null) {
            return "TEXT";
        }
        String lowerType = pgType.toLowerCase().trim();

        String mysqlType;
        boolean isSerial = "serial".equals(lowerType) || "bigserial".equals(lowerType);

        if (lowerType.startsWith("numeric") || lowerType.startsWith("decimal")) {
            int precision = column.getColumnSize();
            int scale = column.getDecimalDigits();
            if (scale > 0) {
                mysqlType = "DECIMAL(" + precision + "," + scale + ")";
            } else if (precision > 0) {
                mysqlType = "DECIMAL(" + precision + ")";
            } else {
                mysqlType = "DECIMAL";
            }
        } else if (lowerType.startsWith("character varying") || lowerType.startsWith("varchar")) {
            int size = column.getColumnSize();
            mysqlType = size > 0 ? "VARCHAR(" + size + ")" : "VARCHAR(255)";
        } else if (lowerType.startsWith("character") || lowerType.startsWith("char")) {
            int size = column.getColumnSize();
            mysqlType = size > 0 ? "CHAR(" + size + ")" : "CHAR(1)";
        } else if (lowerType.startsWith("bit varying") || lowerType.startsWith("varbit")) {
            int size = column.getColumnSize();
            mysqlType = size > 0 ? "VARCHAR(" + size + ")" : "VARCHAR(255)";
        } else if (lowerType.startsWith("bit") && !lowerType.startsWith("bit varying")) {
            int size = column.getColumnSize();
            mysqlType = size > 0 ? "BIT(" + size + ")" : "BIT(1)";
        } else if (lowerType.endsWith("[]")) {
            mysqlType = "TEXT";
        } else {
            mysqlType = mapPgToMysql(pgType);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(mysqlType);

        if (isSerial) {
            column.setAutoIncrement(true);
        }

        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }

        if (column.isAutoIncrement()) {
            sb.append(" AUTO_INCREMENT");
        } else if (column.getDefaultValue() != null && !column.getDefaultValue().isEmpty()) {
            String defaultVal = column.getDefaultValue();
            if (isSerial) {
                // skip DEFAULT for serial columns
            } else if (defaultVal.contains("nextval(")) {
                // skip PG sequence defaults
            } else if (isPgTimestampFunction(defaultVal)) {
                sb.append(" DEFAULT ").append(convertPgTimestampDefault(defaultVal));
            } else if (defaultVal.startsWith("'") || defaultVal.matches("-?\\d+(\\.\\d+)?")) {
                sb.append(" DEFAULT ").append(defaultVal);
            } else if (defaultVal.equalsIgnoreCase("true")) {
                sb.append(" DEFAULT 1");
            } else if (defaultVal.equalsIgnoreCase("false")) {
                sb.append(" DEFAULT 0");
            } else {
                sb.append(" DEFAULT '").append(defaultVal).append("'");
            }
        }

        return sb.toString();
    }

    // ==================== Oracle → PostgreSQL 类型映射 ====================

    /**
     * 将 Oracle 列类型映射为 PostgreSQL 列定义（含 NOT NULL/DEFAULT）。
     * Oracle 类型参考: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html
     */
    public static String mapOracleToPgColumnDef(ColumnInfo column) {
        String oracleType = column.getDataType();
        if (oracleType == null) {
            return "TEXT";
        }
        String lowerType = oracleType.toLowerCase().trim();

        String pgType;

        if (lowerType.startsWith("varchar2") || lowerType.startsWith("varchar")) {
            int size = column.getColumnSize();
            pgType = size > 0 ? "VARCHAR(" + size + ")" : "VARCHAR(255)";
        } else if (lowerType.startsWith("nvarchar2") || lowerType.startsWith("nvarchar")) {
            int size = column.getColumnSize();
            // Oracle NVARCHAR2 的 char length 转换为 PG VARCHAR（按字符数）
            pgType = size > 0 ? "VARCHAR(" + size + ")" : "VARCHAR(255)";
        } else if (lowerType.startsWith("char") && !lowerType.startsWith("character")) {
            int size = column.getColumnSize();
            pgType = size > 0 ? "CHAR(" + size + ")" : "CHAR(1)";
        } else if (lowerType.startsWith("nchar")) {
            int size = column.getColumnSize();
            pgType = size > 0 ? "CHAR(" + size + ")" : "CHAR(1)";
        } else if (lowerType.startsWith("clob") || lowerType.startsWith("nclob") || lowerType.startsWith("long")) {
            pgType = "TEXT";
        } else if (lowerType.startsWith("blob") || lowerType.startsWith("raw") || lowerType.startsWith("long raw")) {
            pgType = "BYTEA";
        } else if (lowerType.startsWith("number")) {
            // Oracle NUMBER(precision, scale)
            int precision = column.getColumnSize();
            int scale = column.getDecimalDigits();
            if (precision == 0 && scale == 0) {
                // NUMBER 无精度限定，映射为 NUMERIC（任意精度）
                pgType = "NUMERIC";
            } else if (scale > 0) {
                pgType = "NUMERIC(" + precision + "," + scale + ")";
            } else if (precision > 0) {
                // 整数类型的常见映射
                if (precision <= 4) {
                    pgType = "SMALLINT";
                } else if (precision <= 9) {
                    pgType = "INTEGER";
                } else if (precision <= 18) {
                    pgType = "BIGINT";
                } else {
                    // precision > 18：可能是无精度约束的 NUMBER（JDBC 常返回 22/38 等哨兵值），
                    // 用无精度 NUMERIC 承载，避免 38 位大数被压成 NUMERIC(22) 溢出。
                    pgType = "NUMERIC";
                }
            } else {
                pgType = "NUMERIC";
            }
        } else if (lowerType.equals("float")) {
            pgType = "DOUBLE PRECISION";
        } else if (lowerType.startsWith("binary_float")) {
            pgType = "REAL";
        } else if (lowerType.startsWith("binary_double")) {
            pgType = "DOUBLE PRECISION";
        } else if (lowerType.startsWith("date")) {
            // Oracle DATE 包含日期和时间
            pgType = "TIMESTAMP";
        } else if (lowerType.startsWith("timestamp") && lowerType.contains("with time zone")) {
            pgType = "TIMESTAMP WITH TIME ZONE";
        } else if (lowerType.startsWith("timestamp") && lowerType.contains("with local time zone")) {
            pgType = "TIMESTAMP WITH TIME ZONE";
        } else if (lowerType.startsWith("timestamp")) {
            pgType = "TIMESTAMP";
        } else if (lowerType.startsWith("interval year") || lowerType.startsWith("interval day")) {
            pgType = "INTERVAL";
        } else if (lowerType.startsWith("rowid") || lowerType.startsWith("urowid")) {
            pgType = "VARCHAR(18)";
        } else if (lowerType.startsWith("bfile")) {
            pgType = "VARCHAR(530)";
        } else if (lowerType.startsWith("xmltype")) {
            pgType = "XML";
        } else if (lowerType.startsWith("json")) {
            pgType = "JSONB";
        } else if (lowerType.startsWith("boolean") || lowerType.equals("bool")) {
            pgType = "BOOLEAN";
        } else {
            // 兜底为 TEXT
            pgType = "TEXT";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(pgType);

        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }

        String defaultVal = column.getDefaultValue();
        if (defaultVal != null && !defaultVal.isEmpty()) {
            String trimmed = defaultVal.trim();
            // 处理 Oracle 常见默认值表达式
            if (trimmed.equalsIgnoreCase("CURRENT_TIMESTAMP") || trimmed.equalsIgnoreCase("SYSTIMESTAMP")
                    || trimmed.equalsIgnoreCase("SYSDATE") || trimmed.equalsIgnoreCase("CURRENT_DATE")) {
                sb.append(" DEFAULT CURRENT_TIMESTAMP");
            } else if (trimmed.equalsIgnoreCase("NULL")) {
                // skip
            } else if (trimmed.toUpperCase().startsWith("SYS_GUID")) {
                // 不能直接映射，跳过
            } else if (trimmed.toUpperCase().startsWith("USER")) {
                sb.append(" DEFAULT '").append(trimmed).append("'");
            } else if (trimmed.startsWith("'") || trimmed.matches("-?\\d+(\\.\\d+)?")) {
                sb.append(" DEFAULT ").append(trimmed);
            } else {
                sb.append(" DEFAULT '").append(trimmed).append("'");
            }
        }

        return sb.toString();
    }

    /**
     * 判断给定类型名称是否为 Oracle 的 LOB 类型（CLOB/BLOB/NCLOB/LONG/LONG RAW）。
     */
    public static boolean isOracleLobType(String oracleType) {
        if (oracleType == null) return false;
        String lower = oracleType.toLowerCase().trim();
        return lower.startsWith("clob") || lower.startsWith("nclob")
                || lower.startsWith("blob") || lower.startsWith("long")
                || lower.startsWith("raw") || lower.startsWith("bfile");
    }

    /**
     * 判断给定类型名称是否为 Oracle 的 RAW/BLOB 类型（二进制）。
     */
    public static boolean isOracleBinaryType(String oracleType) {
        if (oracleType == null) return false;
        String lower = oracleType.toLowerCase().trim();
        return lower.startsWith("blob") || lower.startsWith("raw")
                || lower.startsWith("long raw") || lower.startsWith("bfile");
    }

    /**
     * 判断给定类型名称是否为 Oracle 的 TIMESTAMP WITH TIME ZONE 类型。
     */
    public static boolean isOracleTimestampTzType(String oracleType) {
        if (oracleType == null) return false;
        String lower = oracleType.toLowerCase().trim();
        return lower.startsWith("timestamp") && lower.contains("time zone");
    }

    public static boolean isPgArrayType(String pgType) {
        return pgType != null && pgType.endsWith("[]");
    }

    public static boolean isPgBooleanType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "boolean".equals(lower) || "bool".equals(lower);
    }

    public static boolean isPgUuidType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "uuid".equals(lower);
    }

    public static boolean isPgJsonbType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "jsonb".equals(lower);
    }

    public static boolean isPgJsonType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "json".equals(lower);
    }

    public static boolean isPgTimestampTzType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "timestamp with time zone".equals(lower) || "timestamptz".equals(lower);
    }

    public static boolean isPgIntervalType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "interval".equals(lower);
    }

    public static boolean isPgNetworkType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "inet".equals(lower) || "cidr".equals(lower) || "macaddr".equals(lower) || "macaddr8".equals(lower);
    }

    public static boolean isPgTimetzType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "time with time zone".equals(lower) || "timetz".equals(lower);
    }

    public static boolean isPgGeometryType(String pgType) {
        if (pgType == null) return false;
        String lower = pgType.toLowerCase().trim();
        return "point".equals(lower) || "line".equals(lower) || "lseg".equals(lower) ||
               "box".equals(lower) || "path".equals(lower) || "polygon".equals(lower) || "circle".equals(lower);
    }

    public static boolean needsValueConversion(String pgType) {
        return isPgBooleanType(pgType) || isPgUuidType(pgType) || isPgArrayType(pgType) ||
               isPgJsonbType(pgType) || isPgTimestampTzType(pgType) || isPgIntervalType(pgType) ||
               isPgNetworkType(pgType) || isPgGeometryType(pgType);
    }

    private static boolean isPgTimestampFunction(String defaultVal) {
        if (defaultVal == null) return false;
        String lower = defaultVal.toLowerCase().trim();
        return lower.equals("now()") || lower.equals("current_timestamp") ||
               lower.equals("clock_timestamp()") || lower.equals("statement_timestamp()") ||
               lower.equals("transaction_timestamp()") || lower.startsWith("now(") ||
               lower.contains("timezone(") || lower.contains("date_trunc(");
    }

    private static String convertPgTimestampDefault(String defaultVal) {
        if (defaultVal == null) return "CURRENT_TIMESTAMP";
        String lower = defaultVal.toLowerCase().trim();
        if (lower.equals("now()") || lower.equals("current_timestamp") ||
            lower.equals("clock_timestamp()") || lower.equals("statement_timestamp()") ||
            lower.equals("transaction_timestamp()")) {
            return "CURRENT_TIMESTAMP";
        }
        return "CURRENT_TIMESTAMP";
    }
}
