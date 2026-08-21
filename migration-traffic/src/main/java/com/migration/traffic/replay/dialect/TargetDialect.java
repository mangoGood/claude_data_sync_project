package com.migration.traffic.replay.dialect;

import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import com.migration.traffic.model.TrafficRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * 回放侧的引擎方言。
 *
 * <p>调度器（绝对 deadline）、会话 1:1:1 模型、报告与指标都是<b>与引擎无关</b>的，
 * 真正因引擎而异的只有下面这几件事，全部收在这个接口里：
 * <ul>
 *   <li><b>连接</b>——PG 的连接终生绑定一个库（没有 {@code USE}），MySQL 不绑；</li>
 *   <li><b>会话环境对齐</b>——MySQL 的 {@code sql_mode}/{@code time_zone}、
 *       PG 的 {@code search_path}/{@code DateStyle}、Oracle 的 {@code NLS_*}。
 *       不对齐时同一条 SQL 在两边的行为就是不一样的，而且多半不报错；</li>
 *   <li><b>身份互锁</b>——{@code server_uuid} / {@code system_identifier} / {@code DBID+CON_UID}；</li>
 *   <li><b>事务边界</b>——Oracle 没有 {@code BEGIN}，事务由首条 DML 隐式开启；</li>
 *   <li><b>危险语句黑名单</b>——三家的"会毁掉目标实例"的语句集合完全不同。</li>
 * </ul>
 */
public interface TargetDialect {

    /** 取该引擎的方言实现。 */
    static TargetDialect of(TrafficEngine engine) {
        switch (engine) {
            case POSTGRESQL: return new PostgresDialect();
            case ORACLE: return new OracleDialect();
            case MYSQL: return new MySqlDialect();
            default: throw new IllegalArgumentException("未知的回放引擎: " + engine);
        }
    }

    TrafficEngine engine();

    /**
     * 建一条到目标库的连接。
     *
     * <p>为什么是 {@code connect} 而不是"给我一个 URL"：Oracle 的 thin URL <b>没有查询串</b>，
     * TLS 信任材料只能走连接属性（见 {@code SslMaterial.applyOracleProperties}），
     * 而 PG 要把库名拼进 URL 路径。三家连接的构造方式本来就不同形。
     *
     * @param db 该会话要连的库；{@link #connectionBoundToDatabase()} 为 false 的引擎忽略它
     */
    Connection connect(Properties props, String db) throws SQLException;

    /** 目标地址的可读描述（日志与报错文案用），<b>不含口令</b>。 */
    String describeTarget(Properties props, String db);

    /** 连接是否与库绑定（PG=true：换库只能换连接）。 */
    boolean connectionBoundToDatabase();

    /** 目标库默认端口，配置里没写时用。 */
    String defaultPort();

    /**
     * 新连接建立后的对齐动作：会话环境、autocommit 等。
     * 失败只记警告不抛——环境对不齐会让报告里多一些差异，但不该让整条会话跑不起来。
     */
    void onConnect(Connection c, SourceFingerprint fp) throws SQLException;

    /**
     * 把连接的 schema 上下文切到录制里这条语句当时的样子。
     *
     * @param db     记录里的库（MySQL 的默认库 / PG 的 dbname / Oracle 的 CURRENT_SCHEMA）
     * @param schema 记录里的 schema 快照（PG 的 search_path），可能为 null
     * @return 实际切到的库标识，供调用方缓存；不需要切返回 null
     */
    String switchSchema(Connection c, String db, String schema) throws SQLException;

    /** 目标实例的身份串。 */
    String identity(Connection c) throws SQLException;

    /** 命中危险语句黑名单时返回拦截理由，否则 null。 */
    String dangerousReason(String sql);

    /** 该语句对事务状态的影响。 */
    TxEffect txEffect(String sql, StatementClass k);

    /**
     * 是否需要关闭 JDBC 的 autocommit。
     *
     * <p>Oracle 必须关：默认 autocommit=true 会把录制里的
     * {@code INSERT…INSERT…ROLLBACK} 变成"两条已提交 + 一条空回滚"，事务语义整个消失。
     * MySQL/PG 靠录制里的 {@code BEGIN} 显式开启事务，保持默认即可。
     */
    boolean disableAutoCommit();

    /** 该异常是否意味着连接已断（需要重连）。 */
    boolean isConnectionLost(SQLException e);

    /**
     * 绑定参数。值为 {@code null} 的元素必须绑成 <b>SQL NULL</b> 而不是空串——
     * 绑错了在 NOT NULL 列上报错、在可空列上静默写错。
     */
    void bind(PreparedStatement ps, List<String> binds) throws SQLException;

    /**
     * 把录制里的语句改写成 JDBC 能接受的形态。
     *
     * <p>占位符是<b>服务端语法</b>（PG 的 {@code $1}、Oracle 的 {@code :b1}），JDBC 只认 {@code ?}。
     * 默认实现原样返回（MySQL 的录制里参数已经替换进 SQL 文本，没有占位符）。
     */
    default PreparedPlan prepare(String sql, List<String> binds) {
        return new PreparedPlan(sql, binds);
    }

    /**
     * 这条记录物理上就没法回放时返回原因，可以回放返回 null。
     *
     * <p>与危险语句黑名单不同：那是"能放但不该放"，这里是"想放也放不了"。
     * 两者在报告里分开计数，否则用户看到一堆"回放错误"却查不出原因。
     */
    default String unreplayableReason(TrafficRecord r) {
        return null;
    }

    /**
     * 这条记录是否要走 {@link PreparedStatement}。
     *
     * <p>没有参数就走普通 {@code Statement}：simple query 里可能是<b>多条语句串在一起</b>
     * （PG 实测 {@code SET …; SELECT …} 会被记成一行），{@code PreparedStatement} 送不出去。
     */
    default boolean needsPrepared(TrafficRecord r) {
        return r.b != null && !r.b.isEmpty();
    }

    /** 语句对事务状态的影响。 */
    enum TxEffect {
        /** 无影响。 */
        NONE,
        /** 开启事务。 */
        OPEN,
        /** 结束事务（提交或回滚）。 */
        CLOSE
    }
}
