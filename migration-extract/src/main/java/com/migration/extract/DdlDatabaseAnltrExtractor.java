package com.migration.extract;

import com.migration.extract.sql.DdlDatabaseExtractorBaseVisitor;
import com.migration.extract.sql.DdlDatabaseExtractorLexer;
import com.migration.extract.sql.DdlDatabaseExtractorParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                return extractIdentifier(ctx.schemaName().identifier());
            }
            return null;
        }

        @Override
        public String visitDropDatabaseStatement(DdlDatabaseExtractorParser.DropDatabaseStatementContext ctx) {
            if (ctx.schemaName() != null) {
                return extractIdentifier(ctx.schemaName().identifier());
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
            return extractDbFromTableName(ctx.tableName());
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
                return extractIdentifier(tableNameCtx.schemaName().identifier());
            }
            return null;
        }

        private String extractIdentifier(DdlDatabaseExtractorParser.IdentifierContext idCtx) {
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
