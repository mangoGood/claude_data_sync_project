package com.migration.traffic.replay;

import com.migration.traffic.model.TrafficEngine;
import com.migration.traffic.replay.dialect.TargetDialect;

/**
 * 回放前的危险语句拦截。
 *
 * <p>回放和同步/灾备有本质区别：后者写的是"源库已经发生过的数据变更"，而回放写的是
 * <b>任意 SQL，含 DDL 与 DCL</b>。录制里出现一条 {@code DROP DATABASE}，
 * 照原样回放就是把目标库整个删掉——本仓库已经有过
 * "DROP DATABASE 直穿目标库" 的教训，这里默认必须拦住。
 *
 * <p>黑名单<b>按引擎分</b>：三家"会毁掉目标实例"的语句集合完全不同
 * （PG 的 {@code COPY … FROM PROGRAM} 能在数据库服务器上执行 shell 命令，
 * Oracle 的 {@code ALTER DATABASE} 能改实例开闭状态，MySQL 都没有对应物），
 * 所以判定委托给 {@link TargetDialect}。
 *
 * <p>拦下来的语句记 {@code BLOCKED} 并在报告里点名，而不是静默跳过：
 * 用户需要知道"这次回放没有完整重现录制"。
 */
public final class DangerousStatementFilter {

    private final boolean allowDangerous;
    private final TargetDialect dialect;

    /** 默认 MySQL 方言（向后兼容既有调用点）。 */
    public DangerousStatementFilter(boolean allowDangerous) {
        this(allowDangerous, TargetDialect.of(TrafficEngine.MYSQL));
    }

    public DangerousStatementFilter(boolean allowDangerous, TargetDialect dialect) {
        this.allowDangerous = allowDangerous;
        this.dialect = dialect == null ? TargetDialect.of(TrafficEngine.MYSQL) : dialect;
    }

    /** 命中返回拦截理由；不危险返回 null。 */
    public String blockReason(String sql) {
        if (sql == null) return null;
        String reason = dialect.dangerousReason(sql);
        if (reason == null) return null;
        return allowDangerous ? null : reason;
    }

    /** 不看开关，纯判定这条语句在<b>指定引擎</b>上危不危险（预检要用）。 */
    public static String match(TrafficEngine engine, String sql) {
        return TargetDialect.of(engine).dangerousReason(sql);
    }

    /** 不看开关的 MySQL 判定（向后兼容既有调用点）。 */
    public static String match(String sql) {
        return match(TrafficEngine.MYSQL, sql);
    }
}
