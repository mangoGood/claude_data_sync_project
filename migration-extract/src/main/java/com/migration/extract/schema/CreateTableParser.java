package com.migration.extract.schema;

import com.migration.extract.ddl.MySqlDdlLexer;
import com.migration.extract.ddl.MySqlDdlParser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code CREATE TABLE} → {@link TableSchema}。
 *
 * <p>这是表结构时序库的<b>唯一构造路径</b>：基线（{@code SHOW CREATE TABLE} 原文）和
 * binlog 里的建表 DDL 都走这里，不允许"基线走 information_schema、后续走 parser"两条路。
 * 两条路的口径差异会伪装成 DDL bug，排查代价极高；共用一条路还有个附带好处——任务一启动就把
 * 源库全部表过了一遍语法，语法缺口在启动时暴露而不是三个月后某次 ALTER 才暴露
 * （见 {@code SchemaSelfCheck}）。
 *
 * <p>类型名一律归一到 {@code information_schema.COLUMNS.DATA_TYPE} 的口径：
 * {@code INTEGER}→{@code int}、{@code DEC}/{@code NUMERIC}/{@code FIXED}→{@code decimal}、
 * {@code BOOL}→{@code tinyint(1)}、{@code REAL}→{@code double}。归一放在这里而不是语法里，
 * 是"松语法、严 listener"——语法只切 token 结构，语义判断集中一处才好核对。
 */
public class CreateTableParser {

    /** 表选项里的表级字符集：{@code DEFAULT CHARSET=utf8mb4} / {@code CHARACTER SET = utf8} */
    private static final Pattern TABLE_CHARSET = Pattern.compile(
            "(?i)(?:DEFAULT\\s+)?(?:CHARSET|CHARACTER\\s+SET)\\s*=?\\s*['\"`]?([a-z0-9_]+)");

    /** 表选项里的表级排序规则：{@code COLLATE=utf8mb4_general_ci} */
    private static final Pattern TABLE_COLLATE = Pattern.compile(
            "(?i)(?:DEFAULT\\s+)?COLLATE\\s*=?\\s*['\"`]?([a-z0-9_]+)");

    /** 尾部出现 SELECT 即 CREATE TABLE ... AS SELECT，结构推不出来 */
    private static final Pattern AS_SELECT = Pattern.compile("(?i)\\bSELECT\\b");

    /**
     * 解析一条建表语句。
     *
     * @param sql             建表 SQL（{@code SHOW CREATE TABLE} 的原文或 binlog 里的 DDL）
     * @param defaultDatabase 语句里表名没有库限定时用的库（binlog QUERY 事件的 {@code database=}）
     * @return 结构；{@code CREATE TABLE ... AS SELECT} 之类推不出结构的返回
     *         {@link TableSchema#isUnusable()} 为 true 的对象
     * @throws DdlParseException 语法覆盖之外的形态
     */
    public TableSchema parse(String sql, String defaultDatabase) {
        MySqlDdlParser.DdlStatementContext stmt = parseTree(sql);

        MySqlDdlParser.CreateTableStatementContext create = stmt.createTableStatement();
        if (create == null) {
            throw new DdlParseException("不是 CREATE TABLE 语句", sql);
        }

        if (create instanceof MySqlDdlParser.CreateTableLikeContext) {
            MySqlDdlParser.CreateTableLikeContext like = (MySqlDdlParser.CreateTableLikeContext) create;
            TableSchema schema = newSchema(like.tableName(0), defaultDatabase);
            // CREATE TABLE a LIKE b：结构等于 b 当时的结构，本类拿不到 b，交给调用方复制
            schema.markUnusable("CREATE TABLE ... LIKE，需要复制源表 "
                    + qualified(like.tableName(1), defaultDatabase) + " 的当时结构");
            return schema;
        }

        if (create instanceof MySqlDdlParser.CreateTableAsSelectContext) {
            MySqlDdlParser.CreateTableAsSelectContext as =
                    (MySqlDdlParser.CreateTableAsSelectContext) create;
            TableSchema schema = newSchema(as.tableName(), defaultDatabase);
            schema.markUnusable("CREATE TABLE ... AS SELECT，列结构由查询结果决定，DDL 里推不出来");
            return schema;
        }

        MySqlDdlParser.CreateTablePlainContext plain = (MySqlDdlParser.CreateTablePlainContext) create;
        TableSchema schema = newSchema(plain.tableName(), defaultDatabase);

        for (MySqlDdlParser.CreateDefinitionContext def : plain.createDefinitions().createDefinition()) {
            if (def instanceof MySqlDdlParser.DefColumnContext) {
                schema.getColumns().add(
                        buildColumn(((MySqlDdlParser.DefColumnContext) def).columnDefinition(), schema));
            } else {
                applyIndexDefinition(((MySqlDdlParser.DefIndexContext) def).indexDefinition(), schema);
            }
        }

        if (plain.trailing() != null) {
            String tail = text(plain.trailing());
            if (AS_SELECT.matcher(tail).find()) {
                // CREATE TABLE t (...) AS SELECT：列定义有了，但 SELECT 可能再追加列，不敢当权威
                schema.markUnusable("CREATE TABLE ... AS SELECT，SELECT 结果可能追加列，结构不完整");
                return schema;
            }
            applyTableOptions(tail, schema);
        }

        return schema;
    }

    /** 只判断"这是不是一条建表语句"，不构造模型——给调用方做事件分流用。 */
    public boolean isCreateTable(String sql) {
        try {
            return parseTree(sql).createTableStatement() != null;
        } catch (DdlParseException e) {
            return false;
        }
    }

    private MySqlDdlParser.DdlStatementContext parseTree(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new DdlParseException("空语句", sql);
        }
        try {
            MySqlDdlLexer lexer = new MySqlDdlLexer(CharStreams.fromString(sql.trim()));
            lexer.removeErrorListeners();
            lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

            MySqlDdlParser parser = new MySqlDdlParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(ThrowingErrorListener.INSTANCE);

            return parser.ddlStatement();
        } catch (DdlParseException e) {
            throw e;
        } catch (Exception e) {
            throw new DdlParseException("DDL 解析失败: " + e.getMessage(), sql, e);
        }
    }

    private TableSchema newSchema(MySqlDdlParser.TableNameContext ctx, String defaultDatabase) {
        String db = ctx.identifier().size() > 1 ? unquote(ctx.identifier(0)) : defaultDatabase;
        String table = unquote(ctx.identifier(ctx.identifier().size() - 1));
        return new TableSchema(db, table);
    }

    private String qualified(MySqlDdlParser.TableNameContext ctx, String defaultDatabase) {
        String db = ctx.identifier().size() > 1 ? unquote(ctx.identifier(0)) : defaultDatabase;
        return db + "." + unquote(ctx.identifier(ctx.identifier().size() - 1));
    }

    // ---- 列 ----

    private ColumnSchema buildColumn(MySqlDdlParser.ColumnDefinitionContext ctx, TableSchema schema) {
        ColumnSchema col = new ColumnSchema();
        col.setName(unquote(ctx.identifier()));
        applyDataType(ctx.dataType(), col);

        for (MySqlDdlParser.ColumnAttributeContext attr : ctx.columnAttribute()) {
            applyColumnAttribute(attr, col, schema);
        }

        // 生成列隐含可空性由表达式决定，MySQL 对未显式声明的生成列一律记 nullable=YES，
        // 与普通列一致，这里不做特殊处理。
        return col;
    }

    private void applyDataType(MySqlDdlParser.DataTypeContext ctx, ColumnSchema col) {
        List<MySqlDdlParser.TypeTokenContext> tokens = ctx.typeToken();
        String first = tokens.get(0).getText().toLowerCase(Locale.ROOT);
        String second = tokens.size() > 1 ? tokens.get(1).getText().toLowerCase(Locale.ROOT) : null;
        boolean national = ctx.NATIONAL() != null;

        col.setTypeName(normalizeTypeName(first, second, national, col));

        MySqlDdlParser.LengthSpecContext len = ctx.lengthSpec();
        if (len instanceof MySqlDdlParser.NumericLengthContext) {
            MySqlDdlParser.NumericLengthContext n = (MySqlDdlParser.NumericLengthContext) len;
            col.setLength(parseInt(n.signedNumber(0).getText()));
            if (n.signedNumber().size() > 1) {
                col.setScale(parseInt(n.signedNumber(1).getText()));
            }
        } else if (len instanceof MySqlDdlParser.ValueListLengthContext) {
            List<String> values = new ArrayList<>();
            for (TerminalNode s : ((MySqlDdlParser.ValueListLengthContext) len).STRING_LITERAL()) {
                values.add(unquoteString(s.getText()));
            }
            col.setEnumValues(values);
        }

        for (MySqlDdlParser.TypeSuffixContext suffix : ctx.typeSuffix()) {
            if (suffix.UNSIGNED() != null) {
                col.setUnsigned(true);
            } else if (suffix.ZEROFILL() != null) {
                // MySQL: ZEROFILL 隐含 UNSIGNED
                col.setZerofill(true);
                col.setUnsigned(true);
            } else if (suffix.charsetSpec() != null) {
                col.setCharset(unquoteAny(suffix.charsetSpec().charsetName().getText()));
            } else if (suffix.collateSpec() != null) {
                col.setCollation(unquoteAny(suffix.collateSpec().charsetName().getText()));
            } else if (suffix.BINARY() != null && isTextType(col.getTypeName())) {
                // CHAR(10) BINARY 是"用 _bin 排序规则"的老写法，不改 DATA_TYPE
                col.setCollation(col.getCollation() == null ? "binary" : col.getCollation());
            }
        }
    }

    /**
     * 类型名归一到 {@code information_schema.DATA_TYPE} 口径。
     *
     * <p>{@code BOOL}/{@code BOOLEAN} 归成 {@code tinyint(1)} 而不是 {@code tinyint}——
     * 那个 {@code (1)} 是下游区分布尔列的唯一依据（驱动的 {@code tinyInt1isBit}），
     * 丢了它布尔列在目标端就变成普通整数。
     */
    private String normalizeTypeName(String first, String second, boolean national, ColumnSchema col) {
        String combined = second == null ? first : first + " " + second;
        switch (combined) {
            case "double precision":
                return "double";
            case "long varbinary":
                return "mediumblob";
            case "long varchar":
                return "mediumtext";
            default:
                break;
        }
        switch (first) {
            case "integer":
            case "int4":
                return "int";
            case "int1":
                return "tinyint";
            case "int2":
                return "smallint";
            case "int3":
            case "middleint":
                return "mediumint";
            case "int8":
                return "bigint";
            case "dec":
            case "numeric":
            case "fixed":
                return "decimal";
            case "bool":
            case "boolean":
                col.setLength(1);
                return "tinyint";
            case "real":
                // sql_mode 不含 REAL_AS_FLOAT 时 REAL 就是 double；含它的库极少见，不为此加配置
                return "double";
            case "float4":
                return "float";
            case "float8":
                return "double";
            case "nchar":
                return "char";
            case "nvarchar":
                return "varchar";
            case "long":
                return "mediumtext";
            case "serial":
                // SERIAL = BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
                col.setUnsigned(true);
                col.setNullable(false);
                col.setAutoIncrement(true);
                return "bigint";
            case "char":
                return national ? "char" : "char";
            default:
                return first;
        }
    }

    private void applyColumnAttribute(MySqlDdlParser.ColumnAttributeContext ctx,
                                      ColumnSchema col, TableSchema schema) {
        if (ctx instanceof MySqlDdlParser.AttrNotNullContext) {
            col.setNullable(false);
        } else if (ctx instanceof MySqlDdlParser.AttrNullContext) {
            col.setNullable(true);
        } else if (ctx instanceof MySqlDdlParser.AttrDefaultContext) {
            col.setDefaultExpr(text(((MySqlDdlParser.AttrDefaultContext) ctx).defaultValue()));
        } else if (ctx instanceof MySqlDdlParser.AttrOnUpdateContext) {
            col.setOnUpdate(text(((MySqlDdlParser.AttrOnUpdateContext) ctx).defaultValue()));
        } else if (ctx instanceof MySqlDdlParser.AttrAutoIncrementContext) {
            col.setAutoIncrement(true);
        } else if (ctx instanceof MySqlDdlParser.AttrGeneratedContext) {
            MySqlDdlParser.AttrGeneratedContext g = (MySqlDdlParser.AttrGeneratedContext) ctx;
            col.setGenerated(true);
            col.setGeneratedStored(g.STORED() != null);
            col.setGenerationExpr(stripOuterParens(text(g.parenBlock())));
        } else if (ctx instanceof MySqlDdlParser.AttrPrimaryKeyContext) {
            // 列上的 PRIMARY KEY：整表主键就是这一列
            schema.setPrimaryKey(List.of(col.getName()));
            col.setNullable(false);
        } else if (ctx instanceof MySqlDdlParser.AttrUniqueContext) {
            schema.getUniqueIndexes().put(col.getName(), List.of(col.getName()));
        } else if (ctx instanceof MySqlDdlParser.AttrCommentContext) {
            col.setComment(unquoteString(((MySqlDdlParser.AttrCommentContext) ctx).STRING_LITERAL().getText()));
        } else if (ctx instanceof MySqlDdlParser.AttrCollateContext) {
            col.setCollation(unquoteAny(((MySqlDdlParser.AttrCollateContext) ctx).charsetName().getText()));
        } else if (ctx instanceof MySqlDdlParser.AttrCharsetContext) {
            col.setCharset(unquoteAny(
                    ((MySqlDdlParser.AttrCharsetContext) ctx).charsetSpec().charsetName().getText()));
        }
        // 其余属性（CHECK / VISIBLE / STORAGE / COLUMN_FORMAT / SRID / REFERENCES）不影响
        // 列布局与取值解析，解析通过即可，不入模型
    }

    // ---- 索引 ----

    private void applyIndexDefinition(MySqlDdlParser.IndexDefinitionContext ctx, TableSchema schema) {
        if (ctx instanceof MySqlDdlParser.IdxPrimaryContext) {
            schema.setPrimaryKey(keyColumns(((MySqlDdlParser.IdxPrimaryContext) ctx).keyPartList()));
        } else if (ctx instanceof MySqlDdlParser.IdxUniqueContext) {
            MySqlDdlParser.IdxUniqueContext u = (MySqlDdlParser.IdxUniqueContext) ctx;
            List<String> cols = keyColumns(u.keyPartList());
            // 索引名省略时 MySQL 用第一列列名，这里对齐同一规则，两端结构对比才对得上
            String name = u.identifier() != null ? unquote(u.identifier())
                    : (cols.isEmpty() ? "unique" : cols.get(0));
            schema.getUniqueIndexes().put(name, cols);
        }
        // 普通索引 / 全文 / 空间 / 外键 / CHECK 不影响列布局与取值解析，不入模型
    }

    private List<String> keyColumns(MySqlDdlParser.KeyPartListContext ctx) {
        List<String> cols = new ArrayList<>();
        for (MySqlDdlParser.KeyPartContext part : ctx.keyPart()) {
            if (part.identifier() != null) {
                cols.add(unquote(part.identifier()));
            } else {
                // 函数索引 ((JSON_EXTRACT(...)))：没有列名，用原文占位，保证列数与顺序不错
                cols.add(text(part));
            }
        }
        return cols;
    }

    // ---- 表选项 ----

    private void applyTableOptions(String tail, TableSchema schema) {
        Matcher cs = TABLE_CHARSET.matcher(tail);
        if (cs.find()) {
            schema.setCharset(cs.group(1));
        }
        Matcher co = TABLE_COLLATE.matcher(tail);
        if (co.find()) {
            schema.setCollation(co.group(1));
        }
    }

    // ---- 工具 ----

    private static String unquote(MySqlDdlParser.IdentifierContext ctx) {
        return unquoteAny(ctx.getText());
    }

    /** 去掉标识符/字符串外层引号并还原双写转义。 */
    static String unquoteAny(String raw) {
        if (raw == null || raw.length() < 2) {
            return raw;
        }
        char first = raw.charAt(0);
        char last = raw.charAt(raw.length() - 1);
        if (first == last && (first == '`' || first == '"' || first == '\'')) {
            String inner = raw.substring(1, raw.length() - 1);
            return inner.replace(String.valueOf(first) + first, String.valueOf(first));
        }
        return raw;
    }

    /** 字符串字面量额外要还原反斜杠转义（enum 取值里 'a\'b' 很常见）。 */
    static String unquoteString(String raw) {
        String s = unquoteAny(raw);
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case '0': sb.append('\0'); break;
                    default:  sb.append(next); break;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String stripOuterParens(String s) {
        String t = s.trim();
        if (t.length() >= 2 && t.charAt(0) == '(' && t.charAt(t.length() - 1) == ')') {
            return t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    private static boolean isTextType(String typeName) {
        return "char".equals(typeName) || "varchar".equals(typeName)
                || "text".equals(typeName) || "tinytext".equals(typeName)
                || "mediumtext".equals(typeName) || "longtext".equals(typeName);
    }

    private static Integer parseInt(String s) {
        try {
            return Integer.valueOf(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 取规则原文——必须走 token 区间而不是 {@code getText()}。
     * {@code getText()} 把 token 直接拼起来，会丢掉全部空白，
     * {@code DEFAULT CURRENT_TIMESTAMP} 变成 {@code DEFAULTCURRENT_TIMESTAMP}。
     */
    private static String text(org.antlr.v4.runtime.ParserRuleContext ctx) {
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        if (start == null || stop == null || stop.getStopIndex() < start.getStartIndex()) {
            return ctx.getText();
        }
        return start.getInputStream()
                .getText(org.antlr.v4.runtime.misc.Interval.of(start.getStartIndex(), stop.getStopIndex()));
    }

    /** 语法错误一律抛异常——默认的错误监听器只往 stderr 打一行然后继续，等于静默解析出半棵树。 */
    private static final class ThrowingErrorListener extends BaseErrorListener {
        static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                int line, int charPositionInLine, String msg, RecognitionException e) {
            throw new IllegalArgumentException("第 " + line + ":" + charPositionInLine + " 处 " + msg);
        }
    }
}
