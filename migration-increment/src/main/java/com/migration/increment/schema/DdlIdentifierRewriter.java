package com.migration.increment.schema;

import com.migration.increment.sql.MySqlClassifierLexer;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * DDL 标识符改写器：把 DDL 语句里限定标识符的库名（schema）改写为目标库名。
 *
 * <p>基于既有的 ANTLR 词法分析器（{@link MySqlClassifierLexer}）做 token 级改写，而非朴素正则：
 * <ul>
 *   <li><b>正确</b>：词法器已把字符串字面量（{@code '...'}）、注释、反引号标识符各自识别为独立 token，
 *       故绝不会误改字符串里出现的 "库名."（正则 {@code \bdb\.} 会误伤）；反引号/无引号两种标识符均处理。</li>
 *   <li><b>高效</b>：单遍词法扫描 + 按字符偏移逆序替换，O(n)，无回溯、无整句解析开销。</li>
 * </ul>
 *
 * <p>识别规则（限定名 {@code schema.table}）：
 * <ul>
 *   <li>标识符紧跟 {@code DOT} 且处于限定链链首（前一个有效 token 不是标识符/点）→ 该标识符是 schema；</li>
 *   <li>{@code USE schema} 里 USE 之后的标识符 → schema。</li>
 * </ul>
 *
 * <p>可扩展性：{@link #rewrite} 接受一个 schema 映射函数与一个可选的表名映射函数（当前表名映射传 null 即不改表），
 * 后续实现表名/列名映射时在此扩展即可，无需改调用方。多方言：目前词法器面向 MySQL（反引号），
 * PostgreSQL/Oracle 的双引号标识符与 DDL 捕获后续接入时可换用对应词法器或通用分词器，接口不变。
 */
public final class DdlIdentifierRewriter {

    private static final Logger logger = LoggerFactory.getLogger(DdlIdentifierRewriter.class);

    private DdlIdentifierRewriter() {}

    /**
     * 仅改写库名（schema）。schemaMapper：源库名 → 目标库名（返回相同或 null 表示不改）。
     */
    public static String rewriteSchema(String sql, Function<String, String> schemaMapper) {
        return rewrite(sql, schemaMapper, null);
    }

    /**
     * 改写库名（schema），并可选改写表名（table）。
     *
     * @param schemaMapper 源库名 → 目标库名
     * @param tableMapper  (源库名, 源表名) → 目标表名；传 null 表示不改表名（当前需求仅库名映射）
     */
    public static String rewrite(String sql, Function<String, String> schemaMapper,
                                 BiFunction<String, String, String> tableMapper) {
        if (sql == null || sql.isEmpty() || schemaMapper == null) {
            return sql;
        }
        List<Token> toks;
        try {
            toks = lex(sql);
        } catch (RuntimeException e) {
            // 词法失败兜底：返回原文，绝不因改写异常阻断 DDL 应用
            logger.warn("DDL 标识符改写词法失败，返回原文: {}", e.getMessage());
            return sql;
        }

        // 收集替换区间：[startIndex, stopIndex+1) → 新文本
        List<int[]> ranges = new ArrayList<>();   // {start, endExclusive}
        List<String> repl = new ArrayList<>();

        for (int i = 0; i < toks.size(); i++) {
            Token t = toks.get(i);
            Token next = (i + 1 < toks.size()) ? toks.get(i + 1) : null;

            // 规则1：限定名链首标识符（schema.table 里的 schema）
            if (isIdent(t) && next != null && next.getType() == MySqlClassifierLexer.DOT) {
                Token prev = (i > 0) ? toks.get(i - 1) : null;
                boolean chainStart = prev == null
                        || (prev.getType() != MySqlClassifierLexer.DOT && !isIdent(prev));
                if (chainStart) {
                    String srcDb = identName(t);
                    String tgtDb = schemaMapper.apply(srcDb);
                    if (tgtDb != null && !tgtDb.equals(srcDb)) {
                        ranges.add(new int[]{t.getStartIndex(), t.getStopIndex() + 1});
                        repl.add(renderIdent(tgtDb, t));
                    }
                    // 表名映射（预留）：schema.table 的 table = next-next 标识符
                    if (tableMapper != null && i + 2 < toks.size() && isIdent(toks.get(i + 2))) {
                        Token tblTok = toks.get(i + 2);
                        String srcTbl = identName(tblTok);
                        String tgtTbl = tableMapper.apply(srcDb, srcTbl);
                        if (tgtTbl != null && !tgtTbl.equals(srcTbl)) {
                            ranges.add(new int[]{tblTok.getStartIndex(), tblTok.getStopIndex() + 1});
                            repl.add(renderIdent(tgtTbl, tblTok));
                        }
                    }
                }
            } else if (t.getType() == MySqlClassifierLexer.USE && next != null && isIdent(next)) {
                // 规则2：USE schema
                String srcDb = identName(next);
                String tgtDb = schemaMapper.apply(srcDb);
                if (tgtDb != null && !tgtDb.equals(srcDb)) {
                    ranges.add(new int[]{next.getStartIndex(), next.getStopIndex() + 1});
                    repl.add(renderIdent(tgtDb, next));
                }
            }
        }

        if (ranges.isEmpty()) {
            return sql;
        }
        // 逆序（按起点降序）替换，避免偏移错位
        StringBuilder sb = new StringBuilder(sql);
        Integer[] order = new Integer[ranges.size()];
        for (int i = 0; i < order.length; i++) order[i] = i;
        java.util.Arrays.sort(order, (a, b) -> Integer.compare(ranges.get(b)[0], ranges.get(a)[0]));
        for (int idx : order) {
            int[] r = ranges.get(idx);
            sb.replace(r[0], r[1], repl.get(idx));
        }
        return sb.toString();
    }

    private static boolean isIdent(Token t) {
        int ty = t.getType();
        return ty == MySqlClassifierLexer.IDENTIFIER || ty == MySqlClassifierLexer.BACKTICK_QUOTED;
    }

    /** 取标识符的裸名字（去反引号、解转义）。 */
    private static String identName(Token t) {
        String raw = t.getText();
        if (t.getType() == MySqlClassifierLexer.BACKTICK_QUOTED
                && raw.length() >= 2 && raw.charAt(0) == '`') {
            return raw.substring(1, raw.length() - 1).replace("``", "`");
        }
        return raw;
    }

    /** 按原 token 的引用风格渲染新名字（反引号标识符仍加反引号）。 */
    private static String renderIdent(String name, Token original) {
        if (original.getType() == MySqlClassifierLexer.BACKTICK_QUOTED) {
            return "`" + name.replace("`", "``") + "`";
        }
        return name;
    }

    private static List<Token> lex(String sql) {
        MySqlClassifierLexer lexer = new MySqlClassifierLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        List<Token> toks = new ArrayList<>();
        for (Token t = lexer.nextToken(); t.getType() != Token.EOF; t = lexer.nextToken()) {
            toks.add(t);
        }
        return toks;
    }
}
