package com.migration.traffic.replay;

import com.migration.traffic.capture.StatementClassifier;

import java.util.Locale;

/**
 * 回放前的危险语句拦截。
 *
 * <p>回放和同步/灾备有本质区别：后者写的是"源库已经发生过的数据变更"，而回放写的是
 * <b>任意 SQL，含 DDL 与 DCL</b>。录制里出现一条 {@code DROP DATABASE}，
 * 照原样回放就是把目标库整个删掉——本仓库已经有过
 * "DROP DATABASE 直穿目标库" 的教训，这里默认必须拦住。
 *
 * <p>拦下来的语句记 {@code BLOCKED} 并在报告里点名，而不是静默跳过：
 * 用户需要知道"这次回放没有完整重现录制"。
 */
public final class DangerousStatementFilter {

    private final boolean allowDangerous;

    public DangerousStatementFilter(boolean allowDangerous) {
        this.allowDangerous = allowDangerous;
    }

    /** 命中返回拦截理由；不危险返回 null。 */
    public String blockReason(String sql) {
        if (sql == null) return null;
        String reason = match(sql);
        if (reason == null) return null;
        return allowDangerous ? null : reason;
    }

    /** 不看开关，纯判定这条语句危不危险（预检要用）。 */
    public static String match(String sql) {
        if (sql == null) return null;
        int s = StatementClassifier.skipLeadingPublic(sql);
        if (s >= sql.length()) return null;
        String w0 = word(sql, s, 0);
        String w1 = word(sql, s, 1);
        String w2 = word(sql, s, 2);

        switch (w0) {
            case "DROP":
                if ("DATABASE".equals(w1) || "SCHEMA".equals(w1)) return "DROP DATABASE/SCHEMA：会删掉目标库的整个库";
                if ("USER".equals(w1)) return "DROP USER：会删掉目标实例上的账号";
                if ("ROLE".equals(w1)) return "DROP ROLE：会删掉目标实例上的角色";
                return null;
            case "RENAME":
                return "USER".equals(w1) ? "RENAME USER：改的是目标实例的账号" : null;
            case "SET":
                if ("GLOBAL".equals(w1)) return "SET GLOBAL：改的是目标实例的全局状态，不是业务流量";
                if ("PERSIST".equals(w1) || "PERSIST_ONLY".equals(w1)) return "SET PERSIST：会把目标实例的全局配置持久化改掉";
                return null;
            case "SHUTDOWN":
                return "SHUTDOWN：会把目标实例关掉";
            case "RESET":
                return "RESET " + w1 + "：会重置目标实例的复制/日志状态";
            case "PURGE":
                return "PURGE BINARY LOGS：会删掉目标实例的 binlog";
            case "FLUSH":
                if ("TABLES".equals(w1) && "WITH".equals(w2)) return "FLUSH TABLES WITH READ LOCK：会把目标库整库锁死";
                return null;
            case "INSTALL":
            case "UNINSTALL":
                return w0 + " PLUGIN/COMPONENT：会改变目标实例的插件装配";
            case "GRANT":
                // GRANT ALL ... ON *.* 是提权
                if (containsAllOnAll(sql)) return "GRANT ALL ON *.*：会在目标实例上提权";
                return null;
            case "ALTER":
                return "INSTANCE".equals(w1) ? "ALTER INSTANCE：改的是目标实例本身" : null;
            case "LOCK":
                return "INSTANCE".equals(w1) ? "LOCK INSTANCE FOR BACKUP：会阻塞目标实例的写入" : null;
            default:
                return null;
        }
    }

    private static boolean containsAllOnAll(String sql) {
        String u = sql.toUpperCase(Locale.ROOT);
        int on = u.indexOf(" ON ");
        if (on < 0) return false;
        // GRANT ALL [PRIVILEGES] ON *.*
        return u.substring(0, on).contains("ALL") && u.substring(on).replace(" ", "").startsWith("ON*.*");
    }

    private static String word(String sql, int start, int index) {
        return StatementClassifier.wordAtPublic(sql, start, index);
    }
}
