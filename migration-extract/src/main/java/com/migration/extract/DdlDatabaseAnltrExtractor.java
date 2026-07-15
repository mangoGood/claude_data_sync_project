package com.migration.extract;

import com.migration.extract.sql.DdlDatabaseExtractorBaseVisitor;
import com.migration.extract.sql.DdlDatabaseExtractorLexer;
import com.migration.extract.sql.DdlDatabaseExtractorParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DdlDatabaseAnltrExtractor {

    private static final Logger logger = LoggerFactory.getLogger(DdlDatabaseAnltrExtractor.class);

    public static String extractDatabase(String sql, String binlogDatabase, String defaultDatabase) {
        if (sql == null || sql.trim().isEmpty()) {
            return resolveDatabase(binlogDatabase, defaultDatabase);
        }

        try {
            String trimmedSql = sql.trim();
            DdlDatabaseExtractorLexer lexer = new DdlDatabaseExtractorLexer(CharStreams.fromString(trimmedSql));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            DdlDatabaseExtractorParser parser = new DdlDatabaseExtractorParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(new ThrowingErrorListener());

            ParseTree tree = parser.statement();
            DatabaseExtractVisitor visitor = new DatabaseExtractVisitor();
            String result = visitor.visit(tree);

            if (result != null && !result.isEmpty()) {
                logger.debug("ANTLR4 extracted database: {} from DDL", result);
                return result;
            }
        } catch (Exception e) {
            logger.debug("ANTLR4 parsing failed for DDL, falling back to regex: {}", e.getMessage());
            return DdlDatabaseExtractor.extractDatabase(sql, binlogDatabase, defaultDatabase);
        }

        return resolveDatabase(binlogDatabase, defaultDatabase);
    }

    /**
     * 解析表级 DDL 影响的表，返回 {@code database.table} 键列表（已按 defaultDatabase 补全库名）。
     *
     * <p>覆盖 CREATE/ALTER/DROP/TRUNCATE/RENAME TABLE；RENAME 会返回全部旧名与新名。
     * 库级 DDL（CREATE/DROP DATABASE）与索引 DDL（CREATE/DROP INDEX）不改变列布局，返回空列表。
     * 供增量抽取在 DDL 后失效列元数据缓存使用。
     *
     * @param sql             DDL 语句
     * @param defaultDatabase 未限定库名的表所在的当前库（binlog 会话库），可为 null
     * @return 受影响的 {@code database.table} 键，去重后按出现顺序返回；非表级 DDL 返回空列表
     */
    public static List<String> extractAffectedTables(String sql, String defaultDatabase) {
        if (sql == null || sql.trim().isEmpty()) {
            return new ArrayList<>();
        }

        try {
            String trimmedSql = sql.trim();
            DdlDatabaseExtractorLexer lexer = new DdlDatabaseExtractorLexer(CharStreams.fromString(trimmedSql));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            DdlDatabaseExtractorParser parser = new DdlDatabaseExtractorParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(new ThrowingErrorListener());

            ParseTree tree = parser.statement();
            AffectedTablesVisitor visitor = new AffectedTablesVisitor(defaultDatabase);
            visitor.visit(tree);
            return visitor.getTables();
        } catch (Exception e) {
            logger.debug("ANTLR4 解析 DDL 受影响表失败, 回退到正则: {}", e.getMessage());
            return DdlDatabaseExtractor.extractAffectedTables(sql, defaultDatabase);
        }
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

    private static class DatabaseExtractVisitor extends DdlDatabaseExtractorBaseVisitor<String> {

        @Override
        public String visitStatement(DdlDatabaseExtractorParser.StatementContext ctx) {
            if (ctx.ddlStatement() != null) {
                return visit(ctx.ddlStatement());
            }
            return null;
        }

        @Override
        public String visitCreateDatabaseStatement(DdlDatabaseExtractorParser.CreateDatabaseStatementContext ctx) {
            if (ctx.schemaName() != null) {
                return extractIdentifierText(ctx.schemaName().identifier());
            }
            return null;
        }

        @Override
        public String visitDropDatabaseStatement(DdlDatabaseExtractorParser.DropDatabaseStatementContext ctx) {
            if (ctx.schemaName() != null) {
                return extractIdentifierText(ctx.schemaName().identifier());
            }
            return null;
        }

        @Override
        public String visitCreateTableStatement(DdlDatabaseExtractorParser.CreateTableStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName());
        }

        @Override
        public String visitAlterTableStatement(DdlDatabaseExtractorParser.AlterTableStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName());
        }

        @Override
        public String visitDropTableStatement(DdlDatabaseExtractorParser.DropTableStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName(0));
        }

        @Override
        public String visitTruncateStatement(DdlDatabaseExtractorParser.TruncateStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName());
        }

        @Override
        public String visitRenameTableStatement(DdlDatabaseExtractorParser.RenameTableStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName(0));
        }

        @Override
        public String visitCreateIndexStatement(DdlDatabaseExtractorParser.CreateIndexStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName());
        }

        @Override
        public String visitDropIndexStatement(DdlDatabaseExtractorParser.DropIndexStatementContext ctx) {
            return extractDbFromTableName(ctx.tableName());
        }

        private String extractDbFromTableName(DdlDatabaseExtractorParser.TableNameContext tableNameCtx) {
            if (tableNameCtx == null) return null;
            if (tableNameCtx.schemaName() != null) {
                return extractIdentifierText(tableNameCtx.schemaName().identifier());
            }
            return null;
        }
    }

    /**
     * 收集表级 DDL 影响的全部表，按 {@code database.table} 键累积；未限定库名的表用 defaultDatabase 补全。
     * 只覆写会改变列布局的语句（CREATE/ALTER/DROP/TRUNCATE/RENAME TABLE），
     * 索引与库级 DDL 不覆写，默认遍历不会收集到表。
     */
    private static class AffectedTablesVisitor extends DdlDatabaseExtractorBaseVisitor<Void> {

        private final String defaultDatabase;
        private final List<String> tables = new ArrayList<>();

        AffectedTablesVisitor(String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
        }

        List<String> getTables() {
            return tables;
        }

        @Override
        public Void visitCreateTableStatement(DdlDatabaseExtractorParser.CreateTableStatementContext ctx) {
            addTable(ctx.tableName());
            return null;
        }

        @Override
        public Void visitAlterTableStatement(DdlDatabaseExtractorParser.AlterTableStatementContext ctx) {
            addTable(ctx.tableName());
            return null;
        }

        @Override
        public Void visitDropTableStatement(DdlDatabaseExtractorParser.DropTableStatementContext ctx) {
            // DROP TABLE t1, t2, ... 等价于逐表 DROP，每张表的缓存都要失效
            for (DdlDatabaseExtractorParser.TableNameContext tableNameCtx : ctx.tableName()) {
                addTable(tableNameCtx);
            }
            return null;
        }

        @Override
        public Void visitTruncateStatement(DdlDatabaseExtractorParser.TruncateStatementContext ctx) {
            addTable(ctx.tableName());
            return null;
        }

        @Override
        public Void visitRenameTableStatement(DdlDatabaseExtractorParser.RenameTableStatementContext ctx) {
            // RENAME a TO b, c TO d ... 旧名与新名的缓存都要失效
            for (DdlDatabaseExtractorParser.TableNameContext tableNameCtx : ctx.tableName()) {
                addTable(tableNameCtx);
            }
            return null;
        }

        private void addTable(DdlDatabaseExtractorParser.TableNameContext tableNameCtx) {
            if (tableNameCtx == null) return;
            String table = extractIdentifierText(tableNameCtx.identifier());
            if (table == null || table.isEmpty()) return;

            String schema = null;
            if (tableNameCtx.schemaName() != null) {
                schema = extractIdentifierText(tableNameCtx.schemaName().identifier());
            }
            if (schema == null || schema.isEmpty()) {
                schema = defaultDatabase;
            }

            String key = (schema == null ? "" : schema) + "." + table;
            if (!tables.contains(key)) {
                tables.add(key);
            }
        }
    }

    private static String extractIdentifierText(DdlDatabaseExtractorParser.IdentifierContext idCtx) {
        if (idCtx == null) return null;
        if (idCtx.BACKTICK_QUOTED() != null) {
            String text = idCtx.BACKTICK_QUOTED().getText();
            return text.substring(1, text.length() - 1).replace("``", "`");
        }
        if (idCtx.IDENTIFIER() != null) {
            return idCtx.IDENTIFIER().getText();
        }
        if (idCtx.keywordAsId() != null) {
            return idCtx.keywordAsId().getText();
        }
        return idCtx.getText();
    }

    private static class ThrowingErrorListener extends org.antlr.v4.runtime.BaseErrorListener {
        @Override
        public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line, int charPositionInLine,
                                String msg,
                                org.antlr.v4.runtime.RecognitionException e) {
            throw new RuntimeException("ANTLR parse error at line " + line + ":" + charPositionInLine + " " + msg);
        }
    }
}
