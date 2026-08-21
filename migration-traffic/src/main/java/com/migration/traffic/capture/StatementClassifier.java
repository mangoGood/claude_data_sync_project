package com.migration.traffic.capture;

import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;

/**
 * 语句分类：只做<b>前缀识别</b>，不解析 SQL。
 *
 * <p>回放是原样执行的，我们不需要理解语句结构，只需要知道它属于哪一类，
 * 以便回放时按类别筛选、按危险程度拦截。因此这里刻意不引入 ANTLR——
 * 解析全量 SQL（含任意 SELECT）的成本与风险都远大于收益。
 *
 * <p>两个必须做对的地方：
 * <ol>
 *   <li><b>前导注释要剥掉。</b> 大量 ORM/中间件会给每条 SQL 加 {@code /*+ hint * /} 或
 *       {@code /* traceId=... * /} 前缀，只看首字母会把它们全归到 OTHER，
 *       用户按 "DML" 筛选时这些语句会<b>整批消失</b>。</li>
 *   <li><b>CREATE/ALTER/DROP/RENAME 要看第二个词。</b> {@code CREATE USER} 是 DCL 不是 DDL，
 *       混淆会让"不回放 DCL"这个安全开关直接失效。</li>
 * </ol>
 */
public final class StatementClassifier {

    /**
     * MySQL 写日志时对口令的占位符。见实测：
     * {@code CREATE USER 'x'@'%' IDENTIFIED BY <secret>}。
     */
    private static final String SECRET_PLACEHOLDER = "<secret>";

    private StatementClassifier() {
    }

    /** 供回放侧复用的词法入口（剥前导注释 / 取第 n 个词）。 */
    public static int skipLeadingPublic(String sql) {
        return skipLeading(sql);
    }

    public static String wordAtPublic(String sql, int start, int index) {
        return wordAt(sql, start, index);
    }

    /** 语句是否被 MySQL 抹掉了口令 —— 抹掉即不可忠实回放。 */
    public static boolean isRedacted(String sql) {
        return sql != null && sql.contains(SECRET_PLACEHOLDER);
    }

    /**
     * 剥掉前导空白与注释，返回正文起始下标。
     * 未闭合的块注释返回 {@code sql.length()}（整条都是注释，归 OTHER）。
     */
    static int skipLeading(String sql) {
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                if (end < 0) return n;
                i = end + 2;
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                // MySQL 的行注释是 "--" 后跟空白（"--" 紧跟非空白是减号运算）
                if (i + 2 < n && !Character.isWhitespace(sql.charAt(i + 2))) return i;
                i = endOfLine(sql, i);
            } else if (c == '#') {
                i = endOfLine(sql, i);
            } else if (c == '(') {
                // 括号包起来的 SELECT：(SELECT ...) UNION (SELECT ...)
                i++;
            } else {
                return i;
            }
        }
        return n;
    }

    private static int endOfLine(String sql, int from) {
        int nl = sql.indexOf('\n', from);
        return nl < 0 ? sql.length() : nl + 1;
    }

    /** 取第 {@code index} 个词（从 {@code start} 起，0 基）；越界返回空串。 */
    static String wordAt(String sql, int start, int index) {
        int i = start;
        int n = sql.length();
        for (int w = 0; ; w++) {
            while (i < n && !isWordChar(sql.charAt(i))) {
                if (w == 0) break;      // 第 0 个词：调用方保证 start 已在正文上
                i++;
            }
            int b = i;
            while (i < n && isWordChar(sql.charAt(i))) i++;
            if (b == i) return "";
            if (w == index) return sql.substring(b, i).toUpperCase(java.util.Locale.ROOT);
            if (i >= n) return "";
        }
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    /**
     * 按引擎分类。
     *
     * <p>三家的关键字大体重合，但有<b>几处必须分开</b>，错了会直接改变回放语义：
     * <ul>
     *   <li><b>Oracle 的 {@code BEGIN} 是匿名 PL/SQL 块的开头，不是事务开始</b>
     *       （Oracle 压根没有 BEGIN 事务语句）。按 MySQL 那套判成 TCL，
     *       整块 PL/SQL 会被当成事务控制语句，既不受"只回放 DML"的筛选约束，
     *       还会把事务状态跟踪带偏。</li>
     *   <li><b>PG 的 {@code END} 是 COMMIT 的同义词</b>，MySQL 没有这个用法。</li>
     *   <li><b>PG 的 {@code SET ROLE} 是会话状态</b>（必须恒回放），
     *       而 MySQL 的 {@code SET ROLE} 归 DCL（可被"不回放 DCL"滤掉）。</li>
     * </ul>
     */
    public static StatementClass classify(TrafficEngine engine, String sql) {
        StatementClass base = classify(sql);
        if (engine == null) return base;
        switch (engine) {
            case POSTGRESQL: return adjustPg(sql, base);
            case ORACLE: return adjustOracle(sql, base);
            default: return base;
        }
    }

    private static StatementClass adjustPg(String sql, StatementClass base) {
        if (sql == null) return base;
        int s = skipLeading(sql);
        if (s >= sql.length()) return base;
        String w0 = wordAt(sql, s, 0);
        switch (w0) {
            case "END":                 // PG 里 END 就是 COMMIT
                return StatementClass.TCL;
            case "COPY":
                return StatementClass.DML;
            case "VACUUM": case "REINDEX": case "CLUSTER": case "REFRESH": case "ANALYZE":
                return StatementClass.DDL;      // 维护类，划进 DDL 便于按类别筛选
            case "LISTEN": case "UNLISTEN": case "NOTIFY":
                return StatementClass.OTHER;
            case "DISCARD":
                return StatementClass.SET;      // 清会话状态，属于会话状态类
            case "SET": {
                String w1 = wordAt(sql, s, 1);
                // PG 的 SET ROLE / SET SESSION AUTHORIZATION 是会话状态，不是授权操作
                if ("ROLE".equals(w1) || "SESSION".equals(w1)) return StatementClass.SET;
                return base;
            }
            case "CREATE": case "ALTER": case "DROP": {
                String w1 = wordAt(sql, s, 1);
                if ("GROUP".equals(w1)) return StatementClass.DCL;
                return base;
            }
            default:
                return base;
        }
    }

    private static StatementClass adjustOracle(String sql, StatementClass base) {
        if (sql == null) return base;
        int s = skipLeading(sql);
        if (s >= sql.length()) return base;
        String w0 = wordAt(sql, s, 0);
        switch (w0) {
            case "BEGIN": case "DECLARE":
                // 匿名 PL/SQL 块。Oracle 没有 BEGIN 事务语句，判成 TCL 是错的
                return StatementClass.DML;
            case "MERGE":
                return StatementClass.DML;
            case "LOCK":
                return StatementClass.OTHER;
            case "ALTER": {
                String w1 = wordAt(sql, s, 1);
                // ALTER SESSION 是会话状态（NLS/CURRENT_SCHEMA），必须恒回放
                if ("SESSION".equals(w1)) return StatementClass.SET;
                if ("USER".equals(w1) || "ROLE".equals(w1) || "PROFILE".equals(w1)) return StatementClass.DCL;
                return base;
            }
            case "CREATE": case "DROP": {
                String w1 = wordAt(sql, s, 1);
                if ("PROFILE".equals(w1)) return StatementClass.DCL;
                return base;
            }
            case "SET": {
                String w1 = wordAt(sql, s, 1);
                if ("TRANSACTION".equals(w1) || "CONSTRAINT".equals(w1) || "CONSTRAINTS".equals(w1)) {
                    return StatementClass.TCL;
                }
                if ("ROLE".equals(w1)) return StatementClass.DCL;
                return base;
            }
            default:
                return base;
        }
    }

    /** 对一条语句原文分类（MySQL 口径）。{@code null}/空 → OTHER。 */
    public static StatementClass classify(String sql) {
        if (sql == null) return StatementClass.OTHER;
        int s = skipLeading(sql);
        if (s >= sql.length()) return StatementClass.OTHER;

        String w0 = wordAt(sql, s, 0);
        switch (w0) {
            case "SELECT": case "SHOW": case "DESC": case "DESCRIBE": case "EXPLAIN":
            case "ANALYZE": case "CHECKSUM": case "HELP": case "VALUES": case "TABLE":
                return StatementClass.SELECT;

            case "INSERT": case "UPDATE": case "DELETE": case "REPLACE": case "CALL":
            case "DO": case "HANDLER": case "IMPORT":
                return StatementClass.DML;

            case "LOAD":
                return StatementClass.DML;   // LOAD DATA / LOAD XML

            case "WITH":
                // CTE：MySQL 8 支持 WITH ... UPDATE/DELETE。找不到写操作就是 SELECT。
                return hasDmlKeyword(sql, s) ? StatementClass.DML : StatementClass.SELECT;

            case "CREATE": case "ALTER": case "DROP": case "RENAME": {
                String w1 = wordAt(sql, s, 1);
                if ("USER".equals(w1) || "ROLE".equals(w1)) return StatementClass.DCL;
                // CREATE [OR REPLACE] ... / ALTER ... —— 只有 USER/ROLE 是 DCL，其余都是 DDL
                if ("OR".equals(w1) && "REPLACE".equals(wordAt(sql, s, 2))) {
                    String w3 = wordAt(sql, s, 3);
                    if ("USER".equals(w3) || "ROLE".equals(w3)) return StatementClass.DCL;
                }
                return StatementClass.DDL;
            }

            case "TRUNCATE": case "COMMENT":
                return StatementClass.DDL;

            case "GRANT": case "REVOKE":
                return StatementClass.DCL;

            case "FLUSH":
                // FLUSH PRIVILEGES 是权限相关，其余（FLUSH TABLES/LOGS）不是
                return "PRIVILEGES".equals(wordAt(sql, s, 1)) ? StatementClass.DCL : StatementClass.OTHER;

            case "BEGIN": case "COMMIT": case "ROLLBACK": case "SAVEPOINT": case "XA":
                return StatementClass.TCL;

            case "RELEASE":
                return "SAVEPOINT".equals(wordAt(sql, s, 1)) ? StatementClass.TCL : StatementClass.OTHER;

            case "START":
                return "TRANSACTION".equals(wordAt(sql, s, 1)) ? StatementClass.TCL : StatementClass.OTHER;

            case "SET": {
                String w1 = wordAt(sql, s, 1);
                if ("PASSWORD".equals(w1)) return StatementClass.DCL;
                if ("DEFAULT".equals(w1) && "ROLE".equals(wordAt(sql, s, 2))) return StatementClass.DCL;
                if ("ROLE".equals(w1)) return StatementClass.DCL;
                if ("TRANSACTION".equals(w1)) return StatementClass.TCL;
                return StatementClass.SET;
            }

            case "USE":
                return StatementClass.USE;

            default:
                return StatementClass.OTHER;
        }
    }

    /** WITH 之后是否出现写关键字（粗判，宁可判成 DML —— DML 的筛选/拦截比 SELECT 严）。 */
    private static boolean hasDmlKeyword(String sql, int from) {
        String upper = sql.toUpperCase(java.util.Locale.ROOT);
        return containsWord(upper, from, "INSERT") || containsWord(upper, from, "UPDATE")
                || containsWord(upper, from, "DELETE") || containsWord(upper, from, "REPLACE");
    }

    private static boolean containsWord(String upper, int from, String word) {
        int i = upper.indexOf(word, from);
        while (i >= 0) {
            boolean leftOk = i == 0 || !isWordChar(upper.charAt(i - 1));
            int end = i + word.length();
            boolean rightOk = end >= upper.length() || !isWordChar(upper.charAt(end));
            if (leftOk && rightOk) return true;
            i = upper.indexOf(word, i + 1);
        }
        return false;
    }

    /**
     * 是否是 SQL 语法预处理语句的"噪声行"，必须<b>整条丢弃</b>。
     *
     * <p>实测（MySQL 8.0.44）一条 {@code PREPARE st FROM 'SELECT ... ?'} 在 general_log 里产生：
     * <pre>
     * Query    PREPARE st FROM ...              ← 预处理文本被 MySQL 抹成 "..."，回放必然语法错
     * Prepare  SELECT v FROM t1 WHERE id = ?    ← 模板，参数未绑定，无法执行
     * Query    EXECUTE st USING @a, @b          ← 句柄在目标库不存在
     * Execute  SELECT v FROM t1 WHERE id = 7    ← **只有这一行可回放：参数已替换**
     * Query    DEALLOCATE PREPARE st            ← 句柄不存在
     * </pre>
     * 所以：{@code Prepare} 命令类型整类丢弃，{@code Query} 里的
     * PREPARE/EXECUTE/DEALLOCATE PREPARE 丢弃，只留 {@code Execute}。
     */
    public static boolean isPreparedStatementNoise(String sql) {
        if (sql == null) return false;
        int s = skipLeading(sql);
        if (s >= sql.length()) return false;
        String w0 = wordAt(sql, s, 0);
        if ("PREPARE".equals(w0) || "EXECUTE".equals(w0)) return true;
        return "DEALLOCATE".equals(w0) && "PREPARE".equals(wordAt(sql, s, 1));
    }

    /** {@code USE db} 里的库名；不是 USE 语句返回 null。 */
    public static String useTarget(String sql) {
        if (sql == null) return null;
        int s = skipLeading(sql);
        if (s >= sql.length()) return null;
        if (!"USE".equals(wordAt(sql, s, 0))) return null;
        String rest = sql.substring(s + 3).trim();
        // 去掉结尾分号与反引号
        while (rest.endsWith(";")) rest = rest.substring(0, rest.length() - 1).trim();
        if (rest.length() >= 2 && rest.charAt(0) == '`' && rest.endsWith("`")) {
            rest = rest.substring(1, rest.length() - 1).replace("``", "`");
        }
        return rest.isEmpty() ? null : rest;
    }
}
