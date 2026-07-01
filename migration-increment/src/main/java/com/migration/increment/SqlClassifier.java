package com.migration.increment;

import com.migration.increment.sql.MySqlClassifierLexer;
import com.migration.increment.sql.MySqlClassifierParser;
import com.migration.increment.sql.MySqlClassifierParser.*;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class SqlClassifier {

    private static final Logger logger = LoggerFactory.getLogger(SqlClassifier.class);

    public enum StatementType {
        DDL,
        DML,
        TRANSACTION,
        USE,
        OTHER
    }

    public enum DdlSubType {
        CREATE_TABLE,
        ALTER_TABLE,
        DROP_TABLE,
        CREATE_INDEX,
        DROP_INDEX,
        TRUNCATE,
        RENAME_TABLE,
        CREATE_DATABASE,
        DROP_DATABASE,
        UNKNOWN_DDL
    }

    public enum DmlSubType {
        INSERT,
        UPDATE,
        DELETE,
        REPLACE,
        UNKNOWN_DML
    }

    public enum TransactionSubType {
        BEGIN,
        COMMIT,
        ROLLBACK,
        START_TRANSACTION,
        SAVEPOINT,
        UNKNOWN_TX
    }

    public static class ClassificationResult {
        private StatementType statementType;
        private DdlSubType ddlSubType;
        private DmlSubType dmlSubType;
        private TransactionSubType transactionSubType;
        private String schemaName;
        private String tableName;
        private String fullTableName;
        private boolean needsDatabaseSelection;
        private boolean parseSuccess;

        public StatementType getStatementType() { return statementType; }
        public void setStatementType(StatementType statementType) { this.statementType = statementType; }

        public DdlSubType getDdlSubType() { return ddlSubType; }
        public void setDdlSubType(DdlSubType ddlSubType) { this.ddlSubType = ddlSubType; }

        public DmlSubType getDmlSubType() { return dmlSubType; }
        public void setDmlSubType(DmlSubType dmlSubType) { this.dmlSubType = dmlSubType; }

        public TransactionSubType getTransactionSubType() { return transactionSubType; }
        public void setTransactionSubType(TransactionSubType transactionSubType) { this.transactionSubType = transactionSubType; }

        public String getSchemaName() { return schemaName; }
        public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }

        public String getFullTableName() { return fullTableName; }
        public void setFullTableName(String fullTableName) { this.fullTableName = fullTableName; }

        public boolean isNeedsDatabaseSelection() { return needsDatabaseSelection; }
        public void setNeedsDatabaseSelection(boolean needsDatabaseSelection) { this.needsDatabaseSelection = needsDatabaseSelection; }

        public boolean isParseSuccess() { return parseSuccess; }
        public void setParseSuccess(boolean parseSuccess) { this.parseSuccess = parseSuccess; }

        public boolean isDdl() { return statementType == StatementType.DDL; }
        public boolean isDml() { return statementType == StatementType.DML; }
        public boolean isTransaction() { return statementType == StatementType.TRANSACTION; }
        public boolean isUse() { return statementType == StatementType.USE; }

        @Override
        public String toString() {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("statementType", statementType);
            if (ddlSubType != null) info.put("ddlSubType", ddlSubType);
            if (dmlSubType != null) info.put("dmlSubType", dmlSubType);
            if (transactionSubType != null) info.put("transactionSubType", transactionSubType);
            if (schemaName != null) info.put("schemaName", schemaName);
            if (tableName != null) info.put("tableName", tableName);
            if (fullTableName != null) info.put("fullTableName", fullTableName);
            info.put("needsDatabaseSelection", needsDatabaseSelection);
            info.put("parseSuccess", parseSuccess);
            return info.toString();
        }
    }

    public ClassificationResult classify(String sql) {
        ClassificationResult result = new ClassificationResult();

        if (sql == null || sql.trim().isEmpty()) {
            result.setStatementType(StatementType.OTHER);
            result.setParseSuccess(false);
            return result;
        }

        try {
            MySqlClassifierLexer lexer = new MySqlClassifierLexer(CharStreams.fromString(sql));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MySqlClassifierParser parser = new MySqlClassifierParser(tokens);
            parser.removeErrorListeners();

            StatementContext stmtCtx = parser.statement();
            ParseTree child = stmtCtx.getChild(0);

            if (child instanceof DdlStatementContext) {
                classifyDdl((DdlStatementContext) child, result);
            } else if (child instanceof DmlStatementContext) {
                classifyDml((DmlStatementContext) child, result);
            } else if (child instanceof TransactionStatementContext) {
                classifyTransaction((TransactionStatementContext) child, result);
            } else if (child instanceof UseStatementContext) {
                classifyUse((UseStatementContext) child, result);
            } else if (child instanceof SetStatementContext) {
                result.setStatementType(StatementType.OTHER);
                result.setParseSuccess(true);
            } else {
                result.setStatementType(StatementType.OTHER);
                result.setParseSuccess(true);
            }

        } catch (Exception e) {
            logger.warn("ANTLR4 parse error, falling back to simple classification: {}", e.getMessage());
            fallbackClassify(sql, result);
        }

        return result;
    }

    private void classifyDdl(DdlStatementContext ctx, ClassificationResult result) {
        result.setStatementType(StatementType.DDL);
        result.setParseSuccess(true);

        if (ctx.CREATE() != null && ctx.TABLE() != null) {
            result.setDdlSubType(DdlSubType.CREATE_TABLE);
            if (ctx.tableName() != null && !ctx.tableName().isEmpty()) {
                extractTableName(ctx.tableName(0), result);
            }
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.ALTER() != null && ctx.TABLE() != null) {
            result.setDdlSubType(DdlSubType.ALTER_TABLE);
            if (ctx.tableName() != null && !ctx.tableName().isEmpty()) {
                extractTableName(ctx.tableName(0), result);
            }
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.DROP() != null && ctx.TABLE() != null) {
            result.setDdlSubType(DdlSubType.DROP_TABLE);
            if (ctx.tableName() != null && !ctx.tableName().isEmpty()) {
                extractTableName(ctx.tableName(0), result);
            }
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.CREATE() != null && ctx.INDEX() != null) {
            result.setDdlSubType(DdlSubType.CREATE_INDEX);
            if (ctx.tableName() != null && !ctx.tableName().isEmpty()) {
                extractTableName(ctx.tableName(0), result);
            }
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.DROP() != null && ctx.INDEX() != null) {
            result.setDdlSubType(DdlSubType.DROP_INDEX);
            if (ctx.tableName() != null && !ctx.tableName().isEmpty()) {
                extractTableName(ctx.tableName(0), result);
            }
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.TRUNCATE() != null) {
            result.setDdlSubType(DdlSubType.TRUNCATE);
            if (ctx.tableName() != null && !ctx.tableName().isEmpty()) {
                extractTableName(ctx.tableName(0), result);
            }
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.RENAME() != null && ctx.TABLE() != null) {
            result.setDdlSubType(DdlSubType.RENAME_TABLE);
            result.setNeedsDatabaseSelection(true);

        } else if (ctx.CREATE() != null && ctx.DATABASE() != null) {
            result.setDdlSubType(DdlSubType.CREATE_DATABASE);
            if (ctx.schemaName() != null) {
                result.setSchemaName(extractIdentifierText(ctx.schemaName().identifier()));
            }
            result.setNeedsDatabaseSelection(false);

        } else if (ctx.DROP() != null && ctx.DATABASE() != null) {
            result.setDdlSubType(DdlSubType.DROP_DATABASE);
            if (ctx.schemaName() != null) {
                result.setSchemaName(extractIdentifierText(ctx.schemaName().identifier()));
            }
            result.setNeedsDatabaseSelection(false);

        } else {
            result.setDdlSubType(DdlSubType.UNKNOWN_DDL);
            result.setNeedsDatabaseSelection(true);
        }
    }

    private void classifyDml(DmlStatementContext ctx, ClassificationResult result) {
        result.setStatementType(StatementType.DML);
        result.setParseSuccess(true);

        if (ctx.INSERT() != null) {
            result.setDmlSubType(DmlSubType.INSERT);
        } else if (ctx.UPDATE() != null) {
            result.setDmlSubType(DmlSubType.UPDATE);
            if (ctx.tableName() != null) {
                extractTableName(ctx.tableName(), result);
            }
        } else if (ctx.DELETE() != null) {
            result.setDmlSubType(DmlSubType.DELETE);
        } else if (ctx.REPLACE() != null) {
            result.setDmlSubType(DmlSubType.REPLACE);
            if (ctx.tableName() != null) {
                extractTableName(ctx.tableName(), result);
            }
        } else {
            result.setDmlSubType(DmlSubType.UNKNOWN_DML);
        }

        result.setNeedsDatabaseSelection(false);
    }

    private void classifyTransaction(TransactionStatementContext ctx, ClassificationResult result) {
        result.setStatementType(StatementType.TRANSACTION);
        result.setParseSuccess(true);
        result.setNeedsDatabaseSelection(false);

        if (ctx.BEGIN() != null) {
            result.setTransactionSubType(TransactionSubType.BEGIN);
        } else if (ctx.COMMIT() != null) {
            result.setTransactionSubType(TransactionSubType.COMMIT);
        } else if (ctx.ROLLBACK() != null) {
            result.setTransactionSubType(TransactionSubType.ROLLBACK);
        } else if (ctx.START() != null && ctx.TRANSACTION() != null) {
            result.setTransactionSubType(TransactionSubType.START_TRANSACTION);
        } else if (ctx.SAVEPOINT() != null) {
            result.setTransactionSubType(TransactionSubType.SAVEPOINT);
        } else {
            result.setTransactionSubType(TransactionSubType.UNKNOWN_TX);
        }
    }

    private void classifyUse(UseStatementContext ctx, ClassificationResult result) {
        result.setStatementType(StatementType.USE);
        result.setParseSuccess(true);
        result.setNeedsDatabaseSelection(false);

        if (ctx.schemaName() != null && ctx.schemaName().identifier() != null) {
            result.setSchemaName(extractIdentifierText(ctx.schemaName().identifier()));
        }
    }

    private void extractTableName(TableNameContext ctx, ClassificationResult result) {
        if (ctx == null) return;

        String schema = null;
        String table = null;

        if (ctx.schemaName() != null && ctx.schemaName().identifier() != null) {
            schema = extractIdentifierText(ctx.schemaName().identifier());
        }

        if (ctx.identifier() != null) {
            table = extractIdentifierText(ctx.identifier());
        }

        result.setSchemaName(schema);
        result.setTableName(table);

        if (schema != null && table != null) {
            result.setFullTableName(schema + "." + table);
        } else if (table != null) {
            result.setFullTableName(table);
        }
    }

    private String extractIdentifierText(IdentifierContext ctx) {
        if (ctx == null) return null;

        if (ctx.IDENTIFIER() != null) {
            return ctx.IDENTIFIER().getText();
        } else if (ctx.BACKTICK_QUOTED() != null) {
            String text = ctx.BACKTICK_QUOTED().getText();
            return text.substring(1, text.length() - 1).replace("``", "`");
        } else if (ctx.keywordAsId() != null) {
            return ctx.keywordAsId().getText().toLowerCase();
        }
        return ctx.getText();
    }

    private void fallbackClassify(String sql, ClassificationResult result) {
        result.setParseSuccess(false);

        String trimmed = sql.replaceAll("/\\*.*?\\*/", "").trim();
        String upper = trimmed.toUpperCase();

        if (upper.startsWith("CREATE TABLE")) {
            result.setStatementType(StatementType.DDL);
            result.setDdlSubType(DdlSubType.CREATE_TABLE);
            result.setNeedsDatabaseSelection(true);
        } else if (upper.startsWith("ALTER TABLE")) {
            result.setStatementType(StatementType.DDL);
            result.setDdlSubType(DdlSubType.ALTER_TABLE);
            result.setNeedsDatabaseSelection(true);
        } else if (upper.startsWith("DROP TABLE")) {
            result.setStatementType(StatementType.DDL);
            result.setDdlSubType(DdlSubType.DROP_TABLE);
            result.setNeedsDatabaseSelection(true);
        } else if (upper.startsWith("TRUNCATE")) {
            result.setStatementType(StatementType.DDL);
            result.setDdlSubType(DdlSubType.TRUNCATE);
            result.setNeedsDatabaseSelection(true);
        } else if (upper.startsWith("INSERT")) {
            result.setStatementType(StatementType.DML);
            result.setDmlSubType(DmlSubType.INSERT);
        } else if (upper.startsWith("UPDATE")) {
            result.setStatementType(StatementType.DML);
            result.setDmlSubType(DmlSubType.UPDATE);
        } else if (upper.startsWith("DELETE")) {
            result.setStatementType(StatementType.DML);
            result.setDmlSubType(DmlSubType.DELETE);
        } else if (upper.startsWith("BEGIN")) {
            result.setStatementType(StatementType.TRANSACTION);
            result.setTransactionSubType(TransactionSubType.BEGIN);
        } else if (upper.startsWith("COMMIT")) {
            result.setStatementType(StatementType.TRANSACTION);
            result.setTransactionSubType(TransactionSubType.COMMIT);
        } else if (upper.startsWith("ROLLBACK")) {
            result.setStatementType(StatementType.TRANSACTION);
            result.setTransactionSubType(TransactionSubType.ROLLBACK);
        } else if (upper.startsWith("USE")) {
            result.setStatementType(StatementType.USE);
        } else {
            result.setStatementType(StatementType.OTHER);
        }
    }
}
