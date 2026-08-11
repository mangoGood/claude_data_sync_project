package com.migration.extract.schema;

import com.migration.extract.ddl.MySqlDdlLexer;
import com.migration.extract.ddl.MySqlDdlParser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 把一条 DDL <b>施加</b>到表结构模型上，推出下一版——表结构时序库的核心。
 *
 * <p>与"遇到 DDL 就回查源库当前定义"的做法相比，施加式没有竞态：源库在 capture 消费到这条
 * DDL 之前又做了别的 DDL 时，回查拿到的是更新之后的结构，而施加式算出的永远是这条 DDL
 * <b>当时</b>的结构。代价是要养一份语法（见 {@code MySqlDdl.g4}）。
 *
 * <h3>三条硬规则</h3>
 * <ol>
 *   <li><b>多子句按序施加</b>：{@code ALTER TABLE t ADD a INT, DROP b, MODIFY c BIGINT}
 *       逐条作用在中间态上。子句之间有依赖（先 CHANGE 改名、后面再按新名字 MODIFY），
 *       顺序一乱模型就和源库不同了。</li>
 *   <li><b>幂等且可观测</b>：ADD 已存在的列、DROP 不存在的列都跳过并计数。基线可能比它的位点
 *       更新（capture 先读位点、后打基线，见设计文档的不变量二），那段重叠里的 DDL 会被施加
 *       两次，靠幂等吸收。但<b>必须计数并打日志</b>——如果幂等次数持续增长，说明的不是"重叠"
 *       而是模型跟丢了，那是要人看的信号，不能像目标端 {@code isIdempotentDdlError} 那样默默吞掉。</li>
 *   <li><b>RENAME 按从左到右</b>：{@code RENAME TABLE t TO _t_old, _t_new TO t}（pt-osc/gh-ost
 *       的收尾交换）必须先把 t 让出来再让 _t_new 占位。顺序反了或当成两条独立语句处理，
 *       这张表之后的所有事件就都对着错的结构解析。</li>
 * </ol>
 *
 * <p>算不出结果时一律抛 {@link DdlParseException}，绝不"当作这条 DDL 不影响结构"跳过——
 * 漏施加一条 ALTER，该表之后的每一个版本都是错的。
 */
public class DdlApplier {

    private static final Logger logger = LoggerFactory.getLogger(DdlApplier.class);

    /** 从时序库里取某张表<b>当前</b>版本；不存在返回 null。 */
    public interface SchemaLookup {
        TableSchema get(String database, String table);
    }

    /** 幂等吸收计数——持续增长意味着模型跟丢了，是要人看的信号。 */
    public static class Stats {
        public final LongAdder addColumnAlreadyExists = new LongAdder();
        public final LongAdder dropColumnMissing = new LongAdder();
        public final LongAdder modifyColumnMissing = new LongAdder();
        public final LongAdder renameColumnMissing = new LongAdder();
        public final LongAdder tableUnknown = new LongAdder();

        public long total() {
            return addColumnAlreadyExists.sum() + dropColumnMissing.sum() + modifyColumnMissing.sum()
                    + renameColumnMissing.sum() + tableUnknown.sum();
        }

        public Map<String, Long> snapshot() {
            Map<String, Long> m = new LinkedHashMap<>();
            m.put("addColumnAlreadyExists", addColumnAlreadyExists.sum());
            m.put("dropColumnMissing", dropColumnMissing.sum());
            m.put("modifyColumnMissing", modifyColumnMissing.sum());
            m.put("renameColumnMissing", renameColumnMissing.sum());
            m.put("tableUnknown", tableUnknown.sum());
            return m;
        }
    }

    private final CreateTableParser createTableParser = new CreateTableParser();
    private final Stats stats = new Stats();

    public Stats getStats() {
        return stats;
    }

    /**
     * 施加一条 DDL。
     *
     * @param sql             DDL 原文
     * @param defaultDatabase 非限定表名的库上下文（binlog QUERY 事件的 {@code database=}）
     * @param lookup          取表当前版本
     * @return 这条 DDL 造成的结构变化；与结构无关的 DDL 返回空列表
     * @throws DdlParseException 语法覆盖之外的形态
     */
    public List<SchemaChange> apply(String sql, String defaultDatabase, SchemaLookup lookup) {
        MySqlDdlParser.DdlStatementContext stmt = parse(sql);

        if (stmt.createTableStatement() != null) {
            return applyCreateTable(sql, defaultDatabase, lookup);
        }
        if (stmt.alterTableStatement() != null) {
            return applyAlterTable(stmt.alterTableStatement(), sql, defaultDatabase, lookup);
        }
        if (stmt.renameTableStatement() != null) {
            return applyRenameTable(stmt.renameTableStatement(), defaultDatabase, lookup);
        }
        if (stmt.dropTableStatement() != null) {
            return applyDropTable(stmt.dropTableStatement(), defaultDatabase);
        }
        if (stmt.createIndexStatement() != null) {
            return applyCreateIndex(stmt.createIndexStatement(), sql, defaultDatabase, lookup);
        }
        if (stmt.dropIndexStatement() != null) {
            return applyDropIndex(stmt.dropIndexStatement(), sql, defaultDatabase, lookup);
        }

        // 落到这里说明语法把它归成了"与表结构无关的语句"。但语法的兜底规则 otherStatement
        // 能匹配任何 token 串——一条<b>语法坏掉或形态没覆盖</b>的 ALTER TABLE 同样会落到这里，
        // 被当成库级 DDL 静默跳过。漏施加一条改列的 ALTER，该表之后的每个版本都是错的。
        // 所以再按关键字兜一道：看起来是我们该建模的 DDL 却没解析进对应规则，一律报解析失败。
        if (MODELLED_DDL_HEAD.matcher(stripLeadingComments(sql)).find()) {
            throw new DdlParseException("形态未覆盖：看起来是表级 DDL，但没有匹配到任何已知子句", sql);
        }
        return List.of();   // 与表结构无关的 DDL（库级、存储程序、trigger…）
    }

    /**
     * "这条 DDL 我们本该看得懂"的关键字特征。命中它却没解析出结构 = 语法缺口，必须报出来。
     */
    private static final java.util.regex.Pattern MODELLED_DDL_HEAD = java.util.regex.Pattern.compile(
            "(?is)^\\s*(?:ALTER\\s+(?:ONLINE\\s+|OFFLINE\\s+|IGNORE\\s+)*TABLE"
                    + "|CREATE\\s+(?:TEMPORARY\\s+)?TABLE"
                    + "|RENAME\\s+TABLE"
                    + "|DROP\\s+(?:TEMPORARY\\s+)?TABLE"
                    + "|CREATE\\s+(?:UNIQUE\\s+|FULLTEXT\\s+|SPATIAL\\s+)?INDEX"
                    + "|DROP\\s+INDEX)\\b");

    /** binlog 里的 DDL 常带前导版本注释（{@code /*!80000 ... *\/}），关键字判断前先剥掉。 */
    private static String stripLeadingComments(String sql) {
        if (sql == null) {
            return "";
        }
        String s = sql.trim();
        while (s.startsWith("/*")) {
            int end = s.indexOf("*/");
            if (end < 0) {
                break;
            }
            s = s.substring(end + 2).trim();
        }
        return s;
    }

    // ---- CREATE TABLE ----

    private List<SchemaChange> applyCreateTable(String sql, String defaultDatabase, SchemaLookup lookup) {
        TableSchema schema = createTableParser.parse(sql, defaultDatabase);

        if (schema.isUnusable() && schema.getUnusableReason() != null
                && schema.getUnusableReason().startsWith("CREATE TABLE ... LIKE")) {
            // LIKE 的结构等于源表当时的结构——时序库里正好有，复制过来即可
            String source = likeSourceOf(schema.getUnusableReason());
            TableSchema src = source == null ? null : lookupQualified(lookup, source);
            if (src != null && !src.isUnusable()) {
                TableSchema copy = src.copy();
                copy.setDatabase(schema.getDatabase());
                copy.setTable(schema.getTable());
                return List.of(SchemaChange.created(copy));
            }
            // 源表不在时序库里（不在同步范围 / 建于基线之前），保持 unusable 让调用方降级
        }
        return List.of(SchemaChange.created(schema));
    }

    /** 从 unusableReason 里取回 {@code CREATE TABLE ... LIKE} 的源表限定名。 */
    private String likeSourceOf(String reason) {
        int at = reason.lastIndexOf("源表 ");
        if (at < 0) {
            return null;
        }
        String rest = reason.substring(at + 3);
        int space = rest.indexOf(' ');
        return space < 0 ? rest : rest.substring(0, space);
    }

    private TableSchema lookupQualified(SchemaLookup lookup, String qualified) {
        int dot = qualified.indexOf('.');
        return dot <= 0 ? null
                : lookup.get(qualified.substring(0, dot), qualified.substring(dot + 1));
    }

    // ---- ALTER TABLE ----

    private List<SchemaChange> applyAlterTable(MySqlDdlParser.AlterTableStatementContext ctx,
                                               String sql, String defaultDatabase, SchemaLookup lookup) {
        String db = databaseOf(ctx.tableName(), defaultDatabase);
        String table = tableOf(ctx.tableName());

        TableSchema current = lookup.get(db, table);
        if (current == null) {
            // 这张表不在时序库里：不在同步范围，或建于基线之前。不能凭空造一个版本——
            // 那会让"缺失"变成"有一份残缺的版本"，比缺失更难发现
            stats.tableUnknown.increment();
            logger.debug("ALTER 的表 {}.{} 不在时序库里，跳过施加", db, table);
            return List.of();
        }
        if (current.isUnusable()) {
            return List.of();   // 已经标记不可用的表，继续施加没有意义
        }

        TableSchema next = current.copy();
        String renameTo = null;

        for (MySqlDdlParser.AlterSpecificationContext spec : ctx.alterSpecification()) {
            String target = applySpec(spec, next, sql);
            if (target != null) {
                renameTo = target;   // ALTER ... RENAME TO：本条语句最后生效
            }
        }

        if (renameTo == null) {
            return List.of(SchemaChange.altered(next));
        }

        int dot = renameTo.indexOf('.');
        String newDb = dot > 0 ? renameTo.substring(0, dot) : db;
        String newTable = dot > 0 ? renameTo.substring(dot + 1) : renameTo;
        next.setDatabase(newDb);
        next.setTable(newTable);
        return List.of(
                SchemaChange.renamedFrom(db, table, TableSchema.key(newDb, newTable)),
                SchemaChange.renamedTo(next, TableSchema.key(db, table)));
    }

    /**
     * 施加一条 ALTER 子句。返回非 null 表示这条子句把表改名了（{@code RENAME TO 新名}）。
     *
     * <p>不认识的子句（{@code ALGORITHM=}、表选项、分区操作）走 {@code altOther}，
     * 它们不影响列布局，跳过是对的——但那是<b>语法认得出来所以确定不影响</b>，
     * 与"解析失败所以假装不影响"是两回事，后者一律抛异常。
     */
    private String applySpec(MySqlDdlParser.AlterSpecificationContext spec, TableSchema schema, String sql) {
        if (spec instanceof MySqlDdlParser.AltAddColumnContext) {
            MySqlDdlParser.AltAddColumnContext c = (MySqlDdlParser.AltAddColumnContext) spec;
            addColumn(schema, buildColumn(c.identifier(), c.dataType(), c.columnAttribute(), schema),
                    c.columnPosition());

        } else if (spec instanceof MySqlDdlParser.AltAddColumnListContext) {
            MySqlDdlParser.AltAddColumnListContext c = (MySqlDdlParser.AltAddColumnListContext) spec;
            for (MySqlDdlParser.ColumnDefinitionContext def : c.columnDefinition()) {
                addColumn(schema, buildColumn(def.identifier(), def.dataType(),
                        def.columnAttribute(), schema), null);
            }

        } else if (spec instanceof MySqlDdlParser.AltDropColumnContext) {
            String name = unquote(((MySqlDdlParser.AltDropColumnContext) spec).identifier());
            int idx = schema.indexOfColumn(name);
            if (idx < 0) {
                stats.dropColumnMissing.increment();
                logger.info("DROP COLUMN {} 在模型里已不存在，幂等跳过（表 {}）", name, schema.key());
            } else {
                schema.getColumns().remove(idx);
                schema.getPrimaryKey().removeIf(pk -> pk.equalsIgnoreCase(name));
                schema.getUniqueIndexes().values().forEach(cols ->
                        cols.removeIf(col -> col.equalsIgnoreCase(name)));
                schema.getUniqueIndexes().entrySet().removeIf(e -> e.getValue().isEmpty());
            }

        } else if (spec instanceof MySqlDdlParser.AltModifyColumnContext) {
            MySqlDdlParser.AltModifyColumnContext c = (MySqlDdlParser.AltModifyColumnContext) spec;
            String name = unquote(c.identifier());
            ColumnSchema rebuilt = buildColumn(c.identifier(), c.dataType(), c.columnAttribute(), schema);
            replaceColumn(schema, name, rebuilt, c.columnPosition());

        } else if (spec instanceof MySqlDdlParser.AltChangeColumnContext) {
            MySqlDdlParser.AltChangeColumnContext c = (MySqlDdlParser.AltChangeColumnContext) spec;
            String oldName = unquote(c.identifier(0));
            ColumnSchema rebuilt = buildColumn(c.identifier(1), c.dataType(), c.columnAttribute(), schema);
            renameInKeys(schema, oldName, rebuilt.getName());
            replaceColumn(schema, oldName, rebuilt, c.columnPosition());

        } else if (spec instanceof MySqlDdlParser.AltRenameColumnContext) {
            MySqlDdlParser.AltRenameColumnContext c = (MySqlDdlParser.AltRenameColumnContext) spec;
            String oldName = unquote(c.identifier(0));
            String newName = unquote(c.identifier(1));
            ColumnSchema col = schema.findColumn(oldName);
            if (col == null) {
                stats.renameColumnMissing.increment();
                logger.info("RENAME COLUMN {} 在模型里不存在，幂等跳过（表 {}）", oldName, schema.key());
            } else {
                col.setName(newName);
                renameInKeys(schema, oldName, newName);
            }

        } else if (spec instanceof MySqlDdlParser.AltAlterColumnContext) {
            MySqlDdlParser.AltAlterColumnContext c = (MySqlDdlParser.AltAlterColumnContext) spec;
            ColumnSchema col = schema.findColumn(unquote(c.identifier()));
            if (col == null) {
                stats.modifyColumnMissing.increment();
            } else if (c.defaultValue() != null) {
                col.setDefaultExpr(text(c.defaultValue()));
            } else if (c.DROP() != null) {
                col.setDefaultExpr(null);
            }

        } else if (spec instanceof MySqlDdlParser.AltAddPrimaryKeyContext) {
            schema.setPrimaryKey(keyColumns(((MySqlDdlParser.AltAddPrimaryKeyContext) spec).keyPartList()));

        } else if (spec instanceof MySqlDdlParser.AltDropPrimaryKeyContext) {
            schema.setPrimaryKey(List.of());

        } else if (spec instanceof MySqlDdlParser.AltAddUniqueContext) {
            MySqlDdlParser.AltAddUniqueContext c = (MySqlDdlParser.AltAddUniqueContext) spec;
            List<String> cols = keyColumns(c.keyPartList());
            String name = c.identifier() != null ? unquote(c.identifier())
                    : (cols.isEmpty() ? "unique" : cols.get(0));
            schema.getUniqueIndexes().put(name, cols);

        } else if (spec instanceof MySqlDdlParser.AltDropIndexContext) {
            String name = unquote(((MySqlDdlParser.AltDropIndexContext) spec).identifier());
            schema.getUniqueIndexes().keySet().removeIf(k -> k.equalsIgnoreCase(name));

        } else if (spec instanceof MySqlDdlParser.AltConvertCharsetContext) {
            MySqlDdlParser.AltConvertCharsetContext c = (MySqlDdlParser.AltConvertCharsetContext) spec;
            String charset = CreateTableParser.unquoteAny(c.charsetName(0).getText());
            schema.setCharset(charset);
            // CONVERT TO CHARACTER SET 会把所有字符列改成这个字符集
            for (ColumnSchema col : schema.getColumns()) {
                if (col.getCharset() != null) {
                    col.setCharset(charset);
                    col.setCollation(null);
                }
            }

        } else if (spec instanceof MySqlDdlParser.AltRenameTableContext) {
            MySqlDdlParser.AltRenameTableContext c = (MySqlDdlParser.AltRenameTableContext) spec;
            return qualifiedOf(c.tableName(), schema.getDatabase());
        }
        // 其余（普通索引/全文/外键/CHECK/altOther）不影响列布局与取值解析
        return null;
    }

    private ColumnSchema buildColumn(MySqlDdlParser.IdentifierContext nameCtx,
                                     MySqlDdlParser.DataTypeContext typeCtx,
                                     List<MySqlDdlParser.ColumnAttributeContext> attrs,
                                     TableSchema schema) {
        // 复用建表那条路径构造列：ALTER 里的列定义与 CREATE 里的完全同构，
        // 两处各写一份迟早会漂（一边支持 SRID、另一边不支持之类）
        StringBuilder sb = new StringBuilder("CREATE TABLE __t (");
        sb.append(text(nameCtx)).append(' ').append(text(typeCtx));
        for (MySqlDdlParser.ColumnAttributeContext attr : attrs) {
            sb.append(' ').append(text(attr));
        }
        sb.append(')');
        TableSchema tmp = createTableParser.parse(sb.toString(), schema.getDatabase());
        ColumnSchema col = tmp.getColumns().get(0);
        // 列上写了 PRIMARY KEY / UNIQUE 时，临时表把它记在自己身上，要搬到真表上
        if (!tmp.getPrimaryKey().isEmpty()) {
            schema.setPrimaryKey(tmp.getPrimaryKey());
        }
        if (!tmp.getUniqueIndexes().isEmpty()) {
            schema.getUniqueIndexes().putAll(tmp.getUniqueIndexes());
        }
        return col;
    }

    private void addColumn(TableSchema schema, ColumnSchema col,
                           MySqlDdlParser.ColumnPositionContext position) {
        if (schema.indexOfColumn(col.getName()) >= 0) {
            stats.addColumnAlreadyExists.increment();
            logger.info("ADD COLUMN {} 在模型里已存在，幂等跳过（表 {}）", col.getName(), schema.key());
            return;
        }
        schema.getColumns().add(positionIndex(schema, position), col);
    }

    private void replaceColumn(TableSchema schema, String oldName, ColumnSchema rebuilt,
                               MySqlDdlParser.ColumnPositionContext position) {
        int idx = schema.indexOfColumn(oldName);
        if (idx < 0) {
            stats.modifyColumnMissing.increment();
            logger.info("MODIFY/CHANGE COLUMN {} 在模型里不存在，幂等跳过（表 {}）", oldName, schema.key());
            return;
        }
        schema.getColumns().remove(idx);
        // FIRST/AFTER 的下标要在移除之后算——MySQL 的语义就是"先摘下来再插到指定位置"
        int insertAt = position == null ? idx : positionIndex(schema, position);
        schema.getColumns().add(insertAt, rebuilt);
    }

    /** FIRST → 0；AFTER x → x 的下标 + 1；没写位置 → 末尾。 */
    private int positionIndex(TableSchema schema, MySqlDdlParser.ColumnPositionContext position) {
        if (position instanceof MySqlDdlParser.PosFirstContext) {
            return 0;
        }
        if (position instanceof MySqlDdlParser.PosAfterContext) {
            String after = unquote(((MySqlDdlParser.PosAfterContext) position).identifier());
            int idx = schema.indexOfColumn(after);
            if (idx < 0) {
                // AFTER 一个不存在的列：MySQL 会直接报错，模型这边放末尾并留痕，
                // 交叉校验会在下一个行事件上把它抓出来
                logger.warn("AFTER 的列 {} 在模型里不存在（表 {}），新列放到末尾", after, schema.key());
                return schema.getColumns().size();
            }
            return idx + 1;
        }
        return schema.getColumns().size();
    }

    private void renameInKeys(TableSchema schema, String oldName, String newName) {
        if (oldName.equalsIgnoreCase(newName)) {
            return;
        }
        schema.getPrimaryKey().replaceAll(c -> c.equalsIgnoreCase(oldName) ? newName : c);
        schema.getUniqueIndexes().values().forEach(cols ->
                cols.replaceAll(c -> c.equalsIgnoreCase(oldName) ? newName : c));
    }

    // ---- RENAME / DROP TABLE ----

    /**
     * {@code RENAME TABLE a TO b, c TO d} —— <b>从左到右逐对</b>处理。
     *
     * <p>pt-osc / gh-ost 收尾就是一条 {@code RENAME TABLE t TO _t_old, _t_new TO t}：
     * 先把 t 让出来，第二对才能占用 t 这个名字。整条语句在 MySQL 里是原子的，但语义等价于
     * 从左到右，所以这里顺序执行是对的——反过来或拆成两条独立语句处理，这张表之后的
     * 所有事件都会对着错的结构解析。
     */
    private List<SchemaChange> applyRenameTable(MySqlDdlParser.RenameTableStatementContext ctx,
                                                String defaultDatabase, SchemaLookup lookup) {
        List<SchemaChange> changes = new ArrayList<>();
        List<MySqlDdlParser.TableNameContext> names = ctx.tableName();

        // 本条语句内部的中间态：第二对要看得见第一对的结果
        Map<String, TableSchema> staged = new LinkedHashMap<>();
        List<String> dropped = new ArrayList<>();

        for (int i = 0; i + 1 < names.size(); i += 2) {
            String fromDb = databaseOf(names.get(i), defaultDatabase);
            String fromTable = tableOf(names.get(i));
            String toDb = databaseOf(names.get(i + 1), defaultDatabase);
            String toTable = tableOf(names.get(i + 1));
            String fromKey = TableSchema.key(fromDb, fromTable);
            String toKey = TableSchema.key(toDb, toTable);

            TableSchema source = staged.containsKey(fromKey)
                    ? staged.get(fromKey)
                    : (dropped.contains(fromKey) ? null : lookup.get(fromDb, fromTable));
            if (source == null) {
                // 源表不在时序库里（影子表通常就不在同步范围）——目标名的结构从此未知，
                // 必须记 DROPPED 让它退出时序库，不能留着旧版本继续用
                changes.add(SchemaChange.dropped(toDb, toTable));
                staged.remove(toKey);
                dropped.add(toKey);
                continue;
            }

            TableSchema renamed = source.copy();
            renamed.setDatabase(toDb);
            renamed.setTable(toTable);

            changes.add(SchemaChange.renamedFrom(fromDb, fromTable, toKey));
            changes.add(SchemaChange.renamedTo(renamed, fromKey));

            staged.remove(fromKey);
            dropped.add(fromKey);
            staged.put(toKey, renamed);
            dropped.remove(toKey);
        }
        return changes;
    }

    private List<SchemaChange> applyDropTable(MySqlDdlParser.DropTableStatementContext ctx,
                                              String defaultDatabase) {
        List<SchemaChange> changes = new ArrayList<>();
        for (MySqlDdlParser.TableNameContext name : ctx.tableName()) {
            changes.add(SchemaChange.dropped(databaseOf(name, defaultDatabase), tableOf(name)));
        }
        return changes;
    }

    // ---- CREATE / DROP INDEX ----

    private List<SchemaChange> applyCreateIndex(MySqlDdlParser.CreateIndexStatementContext ctx,
                                                String sql, String defaultDatabase, SchemaLookup lookup) {
        if (ctx.UNIQUE() == null) {
            return List.of();   // 普通索引不入模型
        }
        String db = databaseOf(ctx.tableName(), defaultDatabase);
        String table = tableOf(ctx.tableName());
        TableSchema current = lookup.get(db, table);
        if (current == null) {
            stats.tableUnknown.increment();
            return List.of();
        }
        TableSchema next = current.copy();
        next.getUniqueIndexes().put(unquote(ctx.identifier()), keyColumns(ctx.keyPartList()));
        return List.of(SchemaChange.altered(next));
    }

    private List<SchemaChange> applyDropIndex(MySqlDdlParser.DropIndexStatementContext ctx,
                                              String sql, String defaultDatabase, SchemaLookup lookup) {
        String db = databaseOf(ctx.tableName(), defaultDatabase);
        String table = tableOf(ctx.tableName());
        TableSchema current = lookup.get(db, table);
        if (current == null) {
            stats.tableUnknown.increment();
            return List.of();
        }
        String name = unquote(ctx.identifier());
        if (current.getUniqueIndexes().keySet().stream().noneMatch(k -> k.equalsIgnoreCase(name))) {
            return List.of();
        }
        TableSchema next = current.copy();
        next.getUniqueIndexes().keySet().removeIf(k -> k.equalsIgnoreCase(name));
        return List.of(SchemaChange.altered(next));
    }

    // ---- 工具 ----

    private MySqlDdlParser.DdlStatementContext parse(String sql) {
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

    private static String databaseOf(MySqlDdlParser.TableNameContext ctx, String defaultDatabase) {
        return ctx.identifier().size() > 1
                ? CreateTableParser.unquoteAny(ctx.identifier(0).getText()) : defaultDatabase;
    }

    private static String tableOf(MySqlDdlParser.TableNameContext ctx) {
        return CreateTableParser.unquoteAny(
                ctx.identifier(ctx.identifier().size() - 1).getText());
    }

    private static String qualifiedOf(MySqlDdlParser.TableNameContext ctx, String defaultDatabase) {
        return databaseOf(ctx, defaultDatabase) + "." + tableOf(ctx);
    }

    private static String unquote(MySqlDdlParser.IdentifierContext ctx) {
        return CreateTableParser.unquoteAny(ctx.getText());
    }

    private static List<String> keyColumns(MySqlDdlParser.KeyPartListContext ctx) {
        List<String> cols = new ArrayList<>();
        for (MySqlDdlParser.KeyPartContext part : ctx.keyPart()) {
            cols.add(part.identifier() != null ? unquote(part.identifier()) : text(part));
        }
        return cols;
    }

    /** 取原文（带空白）——{@code getText()} 会把 token 直接拼起来，丢掉空白就还原不出 DDL 片段。 */
    private static String text(org.antlr.v4.runtime.ParserRuleContext ctx) {
        org.antlr.v4.runtime.Token start = ctx.getStart();
        org.antlr.v4.runtime.Token stop = ctx.getStop();
        if (start == null || stop == null || stop.getStopIndex() < start.getStartIndex()) {
            return ctx.getText();
        }
        return start.getInputStream().getText(
                org.antlr.v4.runtime.misc.Interval.of(start.getStartIndex(), stop.getStopIndex()));
    }

    private static final class ThrowingErrorListener extends BaseErrorListener {
        static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                int line, int charPositionInLine, String msg, RecognitionException e) {
            throw new IllegalArgumentException("第 " + line + ":" + charPositionInLine + " 处 " + msg);
        }
    }

    /** 便于测试与调试：把幂等计数打成一行。 */
    public String statsLine() {
        return stats.snapshot().entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .map(e -> e.getKey() + "=" + e.getValue())
                .reduce((a, b) -> a + " " + b)
                .orElse("无幂等吸收")
                .toLowerCase(Locale.ROOT);
    }
}
