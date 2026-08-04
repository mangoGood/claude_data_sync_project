package com.migration.full.migration;

import com.migration.db.DatabaseConnection;
import com.migration.dialect.SqlDialect;
import com.migration.dialect.TypeTranslator;
import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SchemaMigration {
    private static final Logger logger = LoggerFactory.getLogger(SchemaMigration.class);

    private DatabaseConnection sourceConnection;
    private DatabaseConnection targetConnection;
    private boolean dropTables;
    private boolean isPostgresql;
    private boolean sourceIsPostgresql;
    private boolean targetIsPostgresql;
    private boolean sourceIsOracle;
    private SqlDialect sourceDialect;
    private SqlDialect targetDialect;
    // 跨库类型翻译器：按源→目标库对生成建表 DDL（取代散落的 createTableFromXToY 分发）
    private TypeTranslator translator;
    // 列处理（仅表级同步、mysql→mysql）：建表期列名改写 + 附加列（DEFAULT 子句承载时间/来源语义）
    private com.migration.config.ColumnProcessingConfig columnProcessing;
    /** 汇聚：本进程内已建好的目标表（小写），保证 N 个源表只建一次 */
    private final java.util.Set<String> mergeCreatedTargets = new java.util.HashSet<>();

    public SchemaMigration(DatabaseConnection sourceConnection, DatabaseConnection targetConnection, boolean dropTables) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.dropTables = dropTables;
        this.sourceIsPostgresql = "postgresql".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
        this.sourceIsOracle = "oracle".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
        this.targetIsPostgresql = "postgresql".equalsIgnoreCase(targetConnection.getConfig().getDbType());
        this.isPostgresql = targetIsPostgresql;
        this.sourceDialect = SqlDialect.forType(sourceConnection.getConfig().getDbType());
        this.targetDialect = SqlDialect.forType(targetConnection.getConfig().getDbType());
        this.translator = TypeTranslator.forPair(sourceConnection.getConfig().getDbType(), targetConnection.getConfig().getDbType());
    }

    /** 注入列处理配置（未注入 = 无列处理，行为与既有逻辑完全一致）。 */
    public void setColumnProcessing(com.migration.config.ColumnProcessingConfig columnProcessing) {
        this.columnProcessing = columnProcessing;
    }

    /** 列处理仅在同引擎链路（mysql→mysql / pg→pg）生效（异构库对建表路径不改动）。 */
    private boolean columnProcessingApplicable() {
        if (columnProcessing == null || columnProcessing.isEmpty()) {
            return false;
        }
        String src = sourceConnection.getConfig().getDbType();
        String tgt = targetConnection.getConfig().getDbType();
        return ("mysql".equalsIgnoreCase(src) && "mysql".equalsIgnoreCase(tgt))
                || ("postgresql".equalsIgnoreCase(src) && "postgresql".equalsIgnoreCase(tgt));
    }

    public void migrateAllTables(List<TableInfo> tables) throws SQLException {
        logger.info("开始迁移表结构，共 {} 个表", tables.size());

        // PG 目标：先确保目标 schema 存在。异构迁移（如 MySQL→PG 用源库名作 schema）落到全新目标库
        // 时，该 schema 尚不存在，建表会报 "no schema has been selected to create in" 而全表失败。
        // currentSchema 已在 JDBC URL 指定，这里按同一 schema 幂等建好；同构 pg→pg 默认 public 也安全。
        if ("postgresql".equalsIgnoreCase(targetConnection.getConfig().getDbType())) {
            String schema = targetConnection.getConfig().getSchema();
            if (schema == null || schema.isEmpty()) {
                schema = "public";
            }
            try {
                targetConnection.execute("CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(schema));
                logger.info("已确保目标 schema 存在: {}", schema);
            } catch (SQLException e) {
                logger.warn("创建目标 schema {} 失败（继续执行）: {}", schema, e.getMessage());
            }
        }

        // MySQL 目标关闭本会话外键检查：带 FK 的建表语句若父表尚未创建会直接失败导致表缺失
        // （建表顺序不保证父先子后）。DatabaseConnection 缓存单连接，一次设置覆盖整个建表阶段。
        if ("mysql".equalsIgnoreCase(targetConnection.getConfig().getDbType())) {
            try {
                targetConnection.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                logger.warn("设置 FOREIGN_KEY_CHECKS=0 失败（继续执行）: {}", e.getMessage());
            }
        }

        int successCount = 0;
        int failCount = 0;

        for (TableInfo table : tables) {
            try {
                migrateTable(table);
                successCount++;
                logger.info("表 {} 结构迁移成功", table.getTableName());
            } catch (SQLException e) {
                failCount++;
                // 汇聚表的结构问题（列缺失/无主键）必须 fail-stop：继续跑下去的结果是
                // 这个来源的数据整列丢失或被同主键行覆盖，而任务照样报完成
                if (isMergeTable(table)) {
                    logger.error("汇聚表 {} 结构迁移失败，终止任务", table.getTableName(), e);
                    throw e;
                }
                logger.error("表 {} 结构迁移失败，已忽略该错误继续执行", table.getTableName(), e);
            }
        }

        logger.info("表结构迁移完成，成功: {}, 失败: {}", successCount, failCount);
    }

    public void migrateTable(TableInfo table) throws SQLException {
        // 表名映射：目标端建表/删表一律用目标表名（未配置映射时 = 源表名）
        String targetTableName = table.getTargetTableName();

        // 拆分：一张源表预建出 N 张分片表（分库时先建库）
        if (table.isSplitRouted()) {
            createShardTables(table);
            return;
        }

        // 汇聚：N 个源表落到同一张目标表，只由第一个源表建表，其余源表只做结构一致性校验。
        // 目标表已存在（上一轮任务或跨实例的另一条 leg 建的）同样只校验不重建——
        // 重建会把别的来源已经搬进去的数据一起抹掉。
        if (isMergeTable(table)) {
            String key = targetTableName.toLowerCase();
            if (!mergeCreatedTargets.add(key)) {
                verifyMergeCompatibility(table);
                return;
            }
            if (tableExists(targetTableName)) {
                logger.info("汇聚目标表 {} 已存在，跳过建表，仅做结构一致性校验", targetTableName);
                verifyMergeCompatibility(table);
                return;
            }
            try {
                createTable(table);
            } catch (SQLException e) {
                // 跨实例汇聚下，多条来源通道是各自独立的进程，"查存在→建表"之间必然有竞态：
                // 两条同时查到不存在、同时 CREATE，输的那条会拿到 already exists。
                // 这与"表已存在"是同一种情况，按已存在处理即可，不能让整条通道失败。
                String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
                if (msg.contains("already exists") || msg.contains("已经存在")) {
                    logger.info("汇聚目标表 {} 已被另一条来源通道建出，改为结构一致性校验", targetTableName);
                    verifyMergeCompatibility(table);
                } else {
                    throw e;
                }
            }
            return;
        }

        if (dropTables) {
            dropTableIfExists(targetTableName);
        }

        createTable(table);
    }

    /** 是否为汇聚目标表（由路由层打标）。 */
    private boolean isMergeTable(TableInfo table) {
        return table.isUpsertLoad() && !table.getMergeTagValues().isEmpty();
    }

    /**
     * 拆分：按分片落点预建目标表（分库时先建库）。
     *
     * <p>由我们预建而不是要求用户提前建好——128 张分片表手工建一次不现实，
     * 且建错一张的后果是那一片的数据全量失败。建表语句从源表结构派生，
     * 逐分片改名 + <b>剥掉 AUTO_INCREMENT</b>（每片各自发号必然撞主键）。
     */
    private void createShardTables(TableInfo table) throws SQLException {
        String createSql = table.getCreateSql();
        if (createSql == null || createSql.isEmpty()) {
            throw new SQLException("表 " + table.getTableName() + " 没有建表语句，无法预建分片表");
        }
        createSql = cleanCreateSql(createSql);
        if (columnProcessingApplicable()) {
            String srcDb = sourceConnection.getConfig().getDatabase();
            createSql = rewriteColumnNamesInCreateSql(createSql, srcDb, table.getTableName());
            createSql = appendExtraColumnsToCreateSql(createSql, srcDb, table.getTableName());
        }
        createSql = com.migration.common.route.SplitDdlRewriter.stripAutoIncrement(createSql);

        java.util.Set<String> ensuredDbs = new java.util.HashSet<>();
        int created = 0;
        for (com.migration.common.route.RouteTarget target : table.getRouteTargets()) {
            String db = target.getDatabase();
            if (!targetIsPostgresql && db != null && !db.isEmpty() && ensuredDbs.add(db)) {
                targetConnection.execute("CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(db));
            }
            if (dropTables) {
                targetConnection.execute("DROP TABLE IF EXISTS " + shardRef(db, target.getTable()));
            }
            String shardSql = com.migration.common.route.SplitDdlRewriter.retargetCreateTable(
                    createSql, db, target.getTable(), targetIsPostgresql);
            try {
                targetConnection.execute(shardSql);
                created++;
            } catch (SQLException e) {
                // 已存在视为幂等（重跑/续跑）；其余错误如实抛出——建表失败那一片会整片丢数据
                String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
                if (msg.contains("already exists") || msg.contains("已经存在")) {
                    logger.info("分片表已存在，跳过建表: {}", shardRef(db, target.getTable()));
                } else {
                    throw e;
                }
            }
        }
        logger.info("表 {} 预建分片表完成：{} 张（共 {} 个分片落点）",
                table.getTableName(), created, table.getRouteTargets().size());
    }

    /** 分片表的目标端引用（MySQL 带库名限定；PG 一条连接跨不了库，只用表名）。 */
    private String shardRef(String db, String tableName) {
        if (targetIsPostgresql || db == null || db.isEmpty()) {
            return quoteIdentifier(tableName);
        }
        return quoteIdentifier(db) + "." + quoteIdentifier(tableName);
    }

    /**
     * 汇聚结构一致性校验：后续源表的列集必须被已建好的目标表覆盖。
     * 少一列就意味着这个源的数据会整列丢失，属于必须拦下的错误；类型差异只告警
     * （同一分表族的列类型微差在真实环境里很常见，且写入时按目标类型绑定）。
     */
    private void verifyMergeCompatibility(TableInfo table) throws SQLException {
        java.util.Set<String> targetColumns = new java.util.HashSet<>();
        String targetTable = table.getTargetTableName();
        String schema = targetIsPostgresql ? targetConnection.getConfig().getSchema() : null;
        if (targetIsPostgresql && (schema == null || schema.isEmpty())) {
            schema = "public";
        }
        try (java.sql.ResultSet cols = targetConnection.getConnection().getMetaData()
                .getColumns(null, schema, targetTable, null)) {
            while (cols.next()) {
                targetColumns.add(cols.getString("COLUMN_NAME").toLowerCase());
            }
        }
        if (targetColumns.isEmpty()) {
            logger.warn("汇聚目标表 {} 未读到列信息，跳过结构一致性校验", targetTable);
            return;
        }
        java.util.List<String> missing = new java.util.ArrayList<>();
        for (com.migration.model.ColumnInfo col : table.getColumns()) {
            if (!targetColumns.contains(col.getColumnName().toLowerCase())) {
                missing.add(col.getColumnName());
            }
        }
        if (!missing.isEmpty()) {
            throw new SQLException("汇聚结构不一致：源表 " + table.getSourceDatabase() + "."
                    + table.getTableName() + " 的列 " + missing + " 在目标表 " + targetTable
                    + " 上不存在，这些列的数据会整列丢失");
        }
        logger.info("汇聚结构一致性校验通过: {}.{} -> {}",
                table.getSourceDatabase(), table.getTableName(), targetTable);
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        // Oracle→PG 场景下，目标表已统一转为小写，DROP 时也使用小写
        String targetName = (sourceIsOracle && targetIsPostgresql) ? tableName.toLowerCase() : tableName;
        String sql = "DROP TABLE IF EXISTS " + quoteIdentifier(targetName);
        targetConnection.execute(sql);
        logger.debug("已删除表: {}", targetName);
    }

    private void createTable(TableInfo table) throws SQLException {
        // 异构迁移：按源→目标库对的翻译器生成目标建表 SQL
        if (!translator.isHomogeneous()) {
            String createSql = translator.generateCreateTable(table, targetDialect);
            logger.debug("跨库生成建表SQL: {}", createSql);
            targetConnection.execute(createSql);
            logger.debug("已创建表: {}", table.getTableName());
            return;
        }

        // 同构迁移：沿用源端 CREATE TABLE 语句
        String createSql = table.getCreateSql();
        if (createSql == null || createSql.isEmpty()) {
            // 源端没有提供 CREATE TABLE SQL（如 Oracle），并且未走上述专门路径，则跳过
            logger.warn("表 {} 未提供 CREATE TABLE SQL，跳过结构迁移", table.getTableName());
            return;
        }
        createSql = cleanCreateSql(createSql);
        createSql = renameTableInCreateSql(createSql, table.getTableName(), table.getTargetTableName());
        if (columnProcessingApplicable()) {
            String srcDb = sourceConnection.getConfig().getDatabase();
            createSql = rewriteColumnNamesInCreateSql(createSql, srcDb, table.getTableName());
            createSql = appendExtraColumnsToCreateSql(createSql, srcDb, table.getTableName());
        }
        createSql = applyMergeTagColumns(createSql, table);
        targetConnection.execute(createSql);
        logger.debug("已创建表: {}", table.getTargetTableName());
    }

    /**
     * 汇聚：在建表语句里追加来源标识列，并（COMPOSITE_SOURCE 策略下）把它们并入主键。
     *
     * <p>并入主键不是可选项——汇聚全量走幂等 upsert，冲突目标就是目标表主键；
     * 只加列不并主键的话，两个分表里主键相同的行会互相覆盖，数据只会少、不会报错。
     * 因此无主键的源表在汇聚下直接拒绝，让问题停在建表阶段而不是搬完之后。
     */
    private String applyMergeTagColumns(String createSql, TableInfo table) throws SQLException {
        java.util.Map<String, String> tags = table.getMergeTagValues();
        if (!table.isUpsertLoad() || tags.isEmpty()) {
            return createSql;
        }
        java.util.List<String> tagColumns = new java.util.ArrayList<>(tags.keySet());
        String rewritten = com.migration.common.route.MergeDdlRewriter.appendTagColumns(
                createSql, tagColumns, targetIsPostgresql);
        if (table.isMergeCompositePk()) {
            if (!com.migration.common.route.MergeDdlRewriter.hasPrimaryKey(rewritten)) {
                throw new SQLException("汇聚要求目标表有主键：源表 " + table.getSourceDatabase() + "."
                        + table.getTableName() + " 无主键，幂等装载无冲突目标可用，"
                        + "请改用有主键的表或换 KEEP 主键策略并自建唯一约束");
            }
            rewritten = com.migration.common.route.MergeDdlRewriter.extendPrimaryKey(
                    rewritten, tagColumns, targetIsPostgresql);
        }
        logger.info("汇聚建表: {} 追加来源标识列 {}{}", table.getTargetTableName(), tagColumns,
                table.isMergeCompositePk() ? "（已并入主键）" : "");
        return rewritten;
    }

    /**
     * 列名映射：把 CREATE TABLE 定义体内的源列名改写为目标列名（不改类型）。
     * 源端建表语句的列名/索引列引用均被方言引用符包裹（MySQL 反引号、PG 双引号），
     * 整体替换 {@code <q>src<q>} → {@code <q>tgt<q>} 可同时覆盖列定义与 PRIMARY KEY/KEY 里的
     * 列引用；只处理首个 '(' 之后的定义体，避免误伤语句头的表名（表名与列名同名时）。
     */
    private String rewriteColumnNamesInCreateSql(String createSql, String srcDb, String srcTable) {
        java.util.Map<String, String> mapping = columnProcessing.getColumnMapping(srcDb, srcTable);
        if (mapping.isEmpty()) {
            return createSql;
        }
        int bodyStart = createSql.indexOf('(');
        if (bodyStart < 0) {
            logger.warn("CREATE TABLE 语句无定义体，列名映射未生效: {}.{}", srcDb, srcTable);
            return createSql;
        }
        String q = sourceDialect.quoteChar();
        String head = createSql.substring(0, bodyStart);
        String body = createSql.substring(bodyStart);
        for (java.util.Map.Entry<String, String> e : mapping.entrySet()) {
            body = body.replace(q + e.getKey() + q, q + e.getValue() + q);
        }
        return head + body;
    }

    /**
     * 附加列：在 CREATE TABLE 定义体末尾追加列定义。
     * 源端建表语句的定义体闭括号固定独占一行（MySQL "\n) ENGINE=..."、PG 元数据生成 "\n)"），
     * 锚定该位置插入；列定义按目标方言生成（MySQL DATETIME/ON UPDATE、PG TIMESTAMP DEFAULT），
     * CUSTOM 为常量 DEFAULT '输入值@源库@源表'，全量与增量 INSERT 均无需注值。
     */
    private String appendExtraColumnsToCreateSql(String createSql, String srcDb, String srcTable) {
        java.util.List<com.migration.config.ColumnProcessingConfig.ExtraColumn> extraColumns =
                columnProcessing.getExtraColumns(srcDb, srcTable);
        if (extraColumns.isEmpty()) {
            return createSql;
        }
        int closeIdx = createSql.lastIndexOf("\n)");
        if (closeIdx < 0) {
            // 兜底：非 SHOW CREATE TABLE 排版（如手写单行 DDL），取最后一个闭括号
            closeIdx = createSql.lastIndexOf(')');
            if (closeIdx < 0) {
                logger.warn("CREATE TABLE 语句未找到定义体闭括号，附加列未生效: {}.{}", srcDb, srcTable);
                return createSql;
            }
        }
        StringBuilder defs = new StringBuilder();
        for (com.migration.config.ColumnProcessingConfig.ExtraColumn extra : extraColumns) {
            defs.append(",\n  ").append(extra.toColumnDef(srcDb, srcTable, targetIsPostgresql));
        }
        logger.info("附加列已加入建表语句: {}.{} 共 {} 列", srcDb, srcTable, extraColumns.size());
        return createSql.substring(0, closeIdx) + defs + createSql.substring(closeIdx);
    }

    /**
     * 表名映射：把 CREATE TABLE 语句头部的表名改写为目标表名。
     * 源端 createSql 表名固定紧跟在 "CREATE TABLE " 之后（SHOW CREATE TABLE 反引号 /
     * PG 元数据生成双引号），锚定语句头替换，不触碰列定义/注释/默认值里的同名文本。
     */
    private String renameTableInCreateSql(String createSql, String sourceName, String targetName) {
        if (targetName == null || targetName.equals(sourceName)) {
            return createSql;
        }
        String[] heads = {
                "CREATE TABLE `" + sourceName + "`",
                "CREATE TABLE \"" + sourceName + "\"",
                "CREATE TABLE " + sourceName
        };
        String[] replacements = {
                "CREATE TABLE `" + targetName + "`",
                "CREATE TABLE \"" + targetName + "\"",
                "CREATE TABLE " + quoteIdentifier(targetName)
        };
        for (int i = 0; i < heads.length; i++) {
            int idx = createSql.indexOf(heads[i]);
            if (idx >= 0) {
                return createSql.substring(0, idx) + replacements[i]
                        + createSql.substring(idx + heads[i].length());
            }
        }
        logger.warn("CREATE TABLE 语句未匹配到表名 {}，表名映射未生效: {}", sourceName, createSql);
        return createSql;
    }

    // 旧的 createTableFromXToY / generate*CreateSql 已迁移到
    // com.migration.dialect.{MysqlToPg,PgToMysql,OracleToPg}Translator.generateCreateTable

    private String cleanCreateSql(String createSql) {
        if (isPostgresql) {
            createSql = createSql.replaceAll("\"[^\"]+\"\\.\"", "\"");
            createSql = createSql.replaceAll("`[^`]+`\\.`", "\"");
            createSql = createSql.replaceAll("`", "\"");
            createSql = createSql.replaceAll("ENGINE\\s*=\\s*\\S+", "");
            createSql = createSql.replaceAll("DEFAULT\\s+CHARSET\\s*=\\s*\\S+", "");
            createSql = createSql.replaceAll("COLLATE\\s*=\\s*\\S+", "");
            createSql = createSql.replaceAll("AUTO_INCREMENT\\s*=\\s*\\d+", "");
            createSql = createSql.replaceAll(",\\s*,", ",");
            createSql = createSql.replaceAll("\\(\\s*,", "(");
            createSql = createSql.replaceAll(",\\s*\\)", ")");
            return createSql;
        }

        createSql = createSql.replaceAll("`[^`]+`\\.`", "`");
        createSql = createSql.replaceAll("AUTO_INCREMENT=\\d+", "AUTO_INCREMENT=1");

        return createSql;
    }

    public boolean tableExists(String tableName) throws SQLException {
        if (isPostgresql) {
            String schema = targetConnection.getConfig().getSchema();
            if (schema == null || schema.isEmpty()) {
                schema = "public";
            }
            try (var stmt = targetConnection.getConnection().prepareStatement(
                    "SELECT COUNT(*) FROM pg_tables WHERE schemaname = ? AND tablename = ?")) {
                stmt.setString(1, schema);
                stmt.setString(2, tableName);
                try (var rs = stmt.executeQuery()) {
                    return rs.next() && rs.getInt(1) > 0;
                }
            }
        }

        // information_schema 等值匹配取代 SHOW TABLES LIKE：LIKE 会把表名里的 _/% 当通配符，
        // 且拼接无法参数化
        try (var stmt = targetConnection.getConnection().prepareStatement(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?")) {
            stmt.setString(1, tableName);
            try (var rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private String quoteIdentifier(String identifier) {
        // 目标库方言决定引用字符（MySQL 反引号 / PostgreSQL·Oracle 双引号）
        return targetDialect.quoteIdentifier(identifier);
    }
}
