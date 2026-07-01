package com.migration.full.migration;

import com.migration.db.DatabaseConnection;
import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;
import com.migration.dialect.SqlDialect;
import com.migration.dialect.TypeTranslator;
import com.migration.full.progress.MigrationProgress;
import com.migration.full.progress.ProgressManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataMigration {
    private static final Logger logger = LoggerFactory.getLogger(DataMigration.class);
    
    private DatabaseConnection sourceConnection;
    private DatabaseConnection targetConnection;
    private int batchSize;
    private boolean continueOnError;
    private ProgressManager progressManager;
    private boolean isPostgresql;
    private boolean sourceIsPostgresql;
    private boolean targetIsPostgresql;
    // SQL 方言：集中处理标识符引用、分页等各库语法差异（取代散落的 isOracle/isPostgresql 分支）
    private SqlDialect sourceDialect;
    private SqlDialect targetDialect;
    // 跨库类型/值翻译器：按源→目标库对集中处理值转换（取代散落的 convertXToYValue 分发）
    private TypeTranslator translator;

    public DataMigration(DatabaseConnection sourceConnection, DatabaseConnection targetConnection,
                        int batchSize, boolean continueOnError, ProgressManager progressManager) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.batchSize = batchSize;
        this.continueOnError = continueOnError;
        this.progressManager = progressManager;
        this.sourceIsPostgresql = "postgresql".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
        this.targetIsPostgresql = "postgresql".equalsIgnoreCase(targetConnection.getConfig().getDbType());
        this.isPostgresql = targetIsPostgresql;
        this.sourceDialect = SqlDialect.forType(sourceConnection.getConfig().getDbType());
        this.targetDialect = SqlDialect.forType(targetConnection.getConfig().getDbType());
        this.translator = TypeTranslator.forPair(sourceConnection.getConfig().getDbType(), targetConnection.getConfig().getDbType());
    }

    private boolean sourceIsOracle() {
        return "oracle".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
    }

    public void migrateAllData(List<TableInfo> tables) throws SQLException {
        logger.info("开始迁移数据，共 {} 个表", tables.size());
        
        int totalSuccessCount = 0;
        int totalFailCount = 0;
        
        for (TableInfo table : tables) {
            try {
                int[] result = migrateTableData(table);
                totalSuccessCount += result[0];
                totalFailCount += result[1];
                logger.info("表 {} 数据迁移完成，成功: {}, 失败: {}", 
                           table.getTableName(), result[0], result[1]);
            } catch (SQLException e) {
                logger.error("表 {} 数据迁移失败", table.getTableName(), e);
                if (progressManager != null && progressManager.isEnabled()) {
                    progressManager.failMigration(table.getTableName(), e.getMessage());
                }
                if (!continueOnError) {
                    throw e;
                }
            }
        }
        
        logger.info("数据迁移完成，总成功: {}, 总失败: {}", totalSuccessCount, totalFailCount);
    }

    public int[] migrateTableData(TableInfo table) throws SQLException {
        String tableName = table.getTableName();
        
        long totalRows = getTableRowCount(tableName);
        logger.info("开始迁移表 {} 的数据，总行数: {}", tableName, totalRows);
        
        if (totalRows == 0) {
            logger.info("表 {} 没有数据，跳过", tableName);
            return new int[]{0, 0};
        }
        
        List<String> columns = getColumnNames(table);
        String columnList = String.join(", ", columns);
        
        String primaryKeyColumn = getPrimaryKeyColumn(table);
        
        return migrateDataBatch(table, columnList, totalRows, primaryKeyColumn);
    }

    private int[] migrateDataBatch(TableInfo table, String columnList, long totalRows, String primaryKeyColumn) throws SQLException {
        String tableName = table.getTableName();
        int successCount = 0;
        int failCount = 0;
        
        MigrationProgress progress = null;
        Long lastMigratedId = null;
        long startOffset = 0;
        
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progress = progressManager.startMigration(tableName, totalRows);
                if (progress != null && progress.getLastMigratedId() != 0) {
                    lastMigratedId = progress.getLastMigratedId();
                    startOffset = progress.getMigratedRows();
                    logger.info("从上次中断位置继续迁移，已迁移: {}, 最后ID: {}", startOffset, lastMigratedId);
                }
            } catch (SQLException e) {
                logger.error("获取迁移进度失败", e);
            }
        }
        
        String sourceQuoteColumnList = buildSourceQuotedColumnList(table);
        String insertSql = "INSERT INTO " + targetQuoteIdentifier(tableName) + " (" + columnList + ") VALUES (" +
                          String.join(", ", createPlaceholders(table.getColumns().size())) + ")";

        Connection sourceConn = sourceConnection.getConnection();
        Connection targetConn = targetConnection.getConnection();
        PreparedStatement insertStmt = targetConn.prepareStatement(insertSql);

        // 分页大小：有主键时启用分页查询，避免大结果集（尤其含 LOB）占用源端 PGA 导致 ORA-04036
        final int pageSize = 1000;
        boolean usePaging = primaryKeyColumn != null;

        try {
            long processedRows = startOffset;
            Long currentLastId = (lastMigratedId != null) ? lastMigratedId : 0L;

            if (usePaging) {
                // ===== 分页循环：每页查询 pageSize 行，处理完关闭 ResultSet 释放源端 PGA =====
                while (true) {
                    // 每页用独立连接：上一页查询后 Oracle 会话 PGA 累积不释放（ORA-04036），
                    // 关闭并重建连接强制释放源端会话 PGA
                    Connection pageConn = sourceConnection.getConnection();
                    // 分页子句按源库方言生成：MySQL → LIMIT，Oracle/PostgreSQL → FETCH FIRST ... ROWS ONLY
                    String pageSql = "SELECT " + sourceQuoteColumnList + " FROM " + sourceQuoteIdentifier(tableName) +
                            " WHERE " + sourceQuoteIdentifier(primaryKeyColumn) + " > ? ORDER BY " +
                            sourceQuoteIdentifier(primaryKeyColumn) + " " + sourceDialect.limitClause(pageSize);
                    PreparedStatement selectStmt = pageConn.prepareStatement(pageSql);
                    selectStmt.setLong(1, currentLastId);
                    ResultSet rs = selectStmt.executeQuery();
                    // 每页重新获取 metaData：上一页 rs.close() 后旧的 metaData 会失效（ORA-17009）
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    int batchCount = 0;
                    int pageRows = 0;
                    while (rs.next()) {
                        try {
                            if (targetConn.isClosed()) {
                                logger.warn("目标数据库连接已关闭，重新建立连接");
                                targetConn = targetConnection.getConnection();
                                insertStmt = targetConn.prepareStatement(insertSql);
                            }
                            for (int i = 1; i <= columnCount; i++) {
                                Object value = readColumnValue(rs, i, metaData, table);
                                value = translator.convertValue(value, metaData.getColumnTypeName(i), rs, i);
                                insertStmt.setObject(i, value);
                            }
                            for (int i = 1; i <= columnCount; i++) {
                                if (metaData.getColumnName(i).equals(primaryKeyColumn)) {
                                    Object idValue = rs.getObject(i);
                                    if (idValue instanceof Number) {
                                        currentLastId = ((Number) idValue).longValue();
                                    }
                                    break;
                                }
                            }
                            insertStmt.addBatch();
                            batchCount++;
                            if (batchCount >= batchSize) {
                                int[] results = insertStmt.executeBatch();
                                successCount += countSuccess(results);
                                failCount += countFailures(results);
                                batchCount = 0;
                                if (progressManager != null && progressManager.isEnabled()) {
                                    progressManager.updateProgress(tableName, processedRows, currentLastId);
                                }
                            }
                            processedRows++;
                            pageRows++;
                        } catch (SQLException e) {
                            if (isDuplicateKeyError(e)) {
                                logger.warn("主键冲突，跳过该行，表: {}, 行: {}", tableName, processedRows);
                            } else {
                                failCount++;
                                logger.error("插入数据失败，表: {}, 行: {}", tableName, processedRows, e);
                                if (progressManager != null && progressManager.isEnabled()) {
                                    try { progressManager.updateProgress(tableName, processedRows, currentLastId); } catch (SQLException ex) { logger.error("更新进度失败", ex); }
                                }
                                if (!continueOnError) { throw e; }
                                try {
                                    if (targetConn.isClosed()) { targetConn = targetConnection.getConnection(); }
                                    insertStmt = targetConn.prepareStatement(insertSql);
                                } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                            }
                        }
                    }
                    if (batchCount > 0) {
                        try {
                            int[] results = insertStmt.executeBatch();
                            successCount += countSuccess(results);
                            failCount += countFailures(results);
                        } catch (SQLException e) {
                            if (!isDuplicateKeyError(e)) {
                                logger.error("执行批数据失败，表: {}", tableName, e);
                                if (!continueOnError) { throw e; }
                            }
                        }
                    }
                    if (progressManager != null && progressManager.isEnabled()) {
                        try { progressManager.updateProgress(tableName, processedRows, currentLastId); } catch (SQLException ex) { logger.error("更新进度失败", ex); }
                    }
                    rs.close();
                    selectStmt.close();
                    // 关闭源连接，强制释放 Oracle 会话 PGA，避免 ORA-04036
                    try { pageConn.close(); } catch (SQLException e) { /* ignore */ }
                    logger.info("表 {} 分页迁移一页完成，本页 {} 行，累计 {}/{}", tableName, pageRows, processedRows, totalRows);
                    if (pageRows < pageSize) break;
                }
            } else {
                // ===== 无主键 fallback：单次全表查询 =====
                String selectSql = "SELECT " + sourceQuoteColumnList + " FROM " + sourceQuoteIdentifier(tableName);
                PreparedStatement selectStmt = sourceConn.prepareStatement(selectSql);
                ResultSet rs = selectStmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                int batchCount = 0;
                while (rs.next()) {
                    try {
                        if (targetConn.isClosed()) {
                            targetConn = targetConnection.getConnection();
                            insertStmt = targetConn.prepareStatement(insertSql);
                        }
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = readColumnValue(rs, i, metaData, table);
                            value = translator.convertValue(value, metaData.getColumnTypeName(i), rs, i);
                            insertStmt.setObject(i, value);
                        }
                        insertStmt.addBatch();
                        batchCount++;
                        if (batchCount >= batchSize) {
                            int[] results = insertStmt.executeBatch();
                            successCount += countSuccess(results);
                            failCount += countFailures(results);
                            batchCount = 0;
                            if (progressManager != null && progressManager.isEnabled()) {
                                progressManager.updateProgress(tableName, processedRows, currentLastId);
                            }
                        }
                        processedRows++;
                    } catch (SQLException e) {
                        if (isDuplicateKeyError(e)) {
                            logger.warn("主键冲突，跳过该行，表: {}, 行: {}", tableName, processedRows);
                        } else {
                            failCount++;
                            logger.error("插入数据失败，表: {}, 行: {}", tableName, processedRows, e);
                            if (!continueOnError) { throw e; }
                            try { insertStmt = targetConn.prepareStatement(insertSql); } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                        }
                    }
                }
                if (batchCount > 0) {
                    int[] results = insertStmt.executeBatch();
                    successCount += countSuccess(results);
                    failCount += countFailures(results);
                }
                rs.close();
                selectStmt.close();
            }

            logger.info("表 {} 数据迁移完成，成功: {}, 失败: {}", tableName, successCount, failCount);
            if (progressManager != null && progressManager.isEnabled()) {
                try { progressManager.completeMigration(tableName); } catch (SQLException e) { logger.error("标记迁移完成失败", e); }
            }
        } finally {
            try { insertStmt.close(); } catch (SQLException e) { /* ignore */ }
        }

        return new int[]{successCount, failCount};
    }

    private Object readColumnValue(ResultSet rs, int i, ResultSetMetaData metaData, TableInfo table) throws SQLException {
        int columnType = metaData.getColumnType(i);
        String columnTypeName = metaData.getColumnTypeName(i);

        if (sourceIsPostgresql && columnTypeName != null) {
            String lowerType = columnTypeName.toLowerCase().trim();
            if ("json".equals(lowerType) || "jsonb".equals(lowerType)) {
                return rs.getString(i);
            }
            if ("bytea".equals(lowerType)) {
                return rs.getBytes(i);
            }
            if ("uuid".equals(lowerType)) {
                return rs.getString(i);
            }
            if (lowerType.endsWith("[]")) {
                return rs.getString(i);
            }
        }

        if (sourceIsOracle() && columnTypeName != null) {
            String lowerType = columnTypeName.toLowerCase().trim();
            // Oracle CLOB/NCLOB 通过 getClob 读取并转为字符串
            if ("clob".equals(lowerType) || "nclob".equals(lowerType)) {
                java.sql.Clob clob = rs.getClob(i);
                return clob == null ? null : clob.getSubString(1, (int) clob.length());
            }
            // BLOB 转为 byte[]
            if ("blob".equals(lowerType)) {
                java.sql.Blob blob = rs.getBlob(i);
                return blob == null ? null : blob.getBytes(1, (int) blob.length());
            }
            // RAW / LONG RAW 转 byte[]
            if ("raw".equals(lowerType) || "long raw".equals(lowerType)) {
                return rs.getBytes(i);
            }
            // LONG 转 String
            if ("long".equals(lowerType)) {
                return rs.getString(i);
            }
            // ROWID/UROWID 转 String
            if ("rowid".equals(lowerType) || "urowid".equals(lowerType)) {
                return rs.getString(i);
            }
            // TIMESTAMP WITH [LOCAL] TIME ZONE — 转 String，避免 oracle.sql.TIMESTAMPTZ 不可识别
            if (TypeMapper.isOracleTimestampTzType(lowerType)) {
                return rs.getString(i);
            }
            // 普通 TIMESTAMP（不带时区）— 用 getTimestamp 转为 java.sql.Timestamp，避免 oracle.sql.TIMESTAMP 对象无法被 PG JDBC 识别
            if (lowerType.startsWith("timestamp")) {
                return rs.getTimestamp(i);
            }
            // DATE — 用 getTimestamp 获取包含时间分量的值，避免丢失时分秒
            if ("date".equals(lowerType)) {
                return rs.getTimestamp(i);
            }
            // INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND — 转 String
            if (lowerType.startsWith("interval")) {
                return rs.getString(i);
            }
            // XMLTYPE / JSON 转 String
            if ("xmltype".equals(lowerType) || "json".equals(lowerType)) {
                return rs.getString(i);
            }
        }

        if (columnType == Types.TIME) {
            return rs.getString(i);
        } else if (columnType == Types.BIGINT && columnTypeName != null
                && columnTypeName.toLowerCase().contains("unsigned")) {
            return rs.getBigDecimal(i);
        } else if (columnType == Types.DATE && "YEAR".equalsIgnoreCase(columnTypeName)) {
            Object value = rs.getObject(i);
            if (value instanceof java.sql.Date) {
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime((java.sql.Date) value);
                return cal.get(java.util.Calendar.YEAR);
            } else if (value == null) {
                return null;
            } else if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return value;
        } else {
            return rs.getObject(i);
        }
    }

    // convertPgToMysqlValue → 已迁移到 com.migration.dialect.PgToMysqlTranslator.convertValue

    private String buildSourceQuotedColumnList(TableInfo table) {
        List<String> columns = new ArrayList<>();
        for (ColumnInfo column : table.getColumns()) {
            String quoted = sourceQuoteIdentifier(column.getColumnName());
            // Oracle XMLTYPE 列直接 SELECT 会返回 oracle.xdb.XMLType 对象，PG JDBC 无法识别，
            // 且缺少 xdb 依赖时会 NoClassDefFoundError。这里在 SQL 层用 GETCLOBVAL 转为 CLOB。
            if (sourceIsOracle() && column.getDataType() != null
                    && column.getDataType().toLowerCase().contains("xmltype")) {
                columns.add("XMLTYPE.GETCLOBVAL(" + quoted + ") AS " + quoted);
            } else {
                columns.add(quoted);
            }
        }
        return String.join(", ", columns);
    }

    private long getTableRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + sourceQuoteIdentifier(tableName);
        
        try (Statement stmt = sourceConnection.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        
        return 0;
    }

    private List<String> getColumnNames(TableInfo table) {
        List<String> columns = new ArrayList<>();
        for (var column : table.getColumns()) {
            // Oracle→PG 场景：源端列名通常为大写，目标 PG 表已建为小写，这里转小写以匹配
            String colName = (sourceIsOracle() && targetIsPostgresql) ? column.getColumnName().toLowerCase() : column.getColumnName();
            columns.add(quoteIdentifier(colName));
        }
        return columns;
    }

    /**
     * 目标端标识符引用。Oracle→PG 场景下将表名转为小写，与 SchemaMigration 中建表时一致。
     */
    private String targetQuoteIdentifier(String identifier) {
        if (sourceIsOracle() && targetIsPostgresql) {
            return quoteIdentifier(identifier.toLowerCase());
        }
        return quoteIdentifier(identifier);
    }

    private String getPrimaryKeyColumn(TableInfo table) {
        for (var column : table.getColumns()) {
            if (column.isPrimaryKey()) {
                return column.getColumnName();
            }
        }
        return null;
    }

    private String[] createPlaceholders(int count) {
        String[] placeholders = new String[count];
        for (int i = 0; i < count; i++) {
            placeholders[i] = "?";
        }
        return placeholders;
    }

    private int countSuccess(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result >= 0) {
                count++;
            }
        }
        return count;
    }

    private int countFailures(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result < 0) {
                count++;
            }
        }
        return count;
    }
    
    private boolean isDuplicateKeyError(SQLException e) {
        int errorCode = e.getErrorCode();
        String sqlState = e.getSQLState();
        
        if (errorCode == 1062 || "23000".equals(sqlState)) {
            return true;
        }
        
        String message = e.getMessage();
        if (message != null && (message.contains("Duplicate entry") || 
            message.contains("duplicate key value") || 
            message.contains("PRIMARY") || message.contains("UNIQUE"))) {
            return true;
        }
        
        return false;
    }

    private String quoteIdentifier(String identifier) {
        return targetDialect.quoteIdentifier(identifier);
    }

    // convertMysqlToPgValue → 已迁移到 com.migration.dialect.MysqlToPgTranslator.convertValue

    // convertOracleToPgValue → 已迁移到 com.migration.dialect.OracleToPgTranslator.convertValue

    private String sourceQuoteIdentifier(String identifier) {
        // MySQL 反引号；PostgreSQL/Oracle 双引号（Oracle 保留原始大小写）
        return sourceDialect.quoteIdentifier(identifier);
    }
}
