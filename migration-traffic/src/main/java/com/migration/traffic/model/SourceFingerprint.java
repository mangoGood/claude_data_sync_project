package com.migration.traffic.model;

/**
 * 录制来源库的身份与语义环境。
 *
 * <p>不是装饰性元数据：回放前要拿它跟目标库逐项比对（见 {@code TrafficPrecheck}）。
 * 语义环境（MySQL 的 {@code sql_mode}/{@code time_zone}、PG 的 {@code DateStyle}/{@code search_path}、
 * Oracle 的 {@code NLS_*}）不一致时，<b>同一条 SQL 在两边的行为就是不一样的</b>——
 * 而且多半不报错，只是结果不同。
 *
 * <p>{@link #identity()} 相同则说明"回放目标就是录制源库自己"，必须硬拦截：
 * 回放会把源库上刚发生的一切再做一遍，{@code n=n+1} 变二次累加、{@code DROP} 是真的删。
 *
 * <p>字段按引擎分族。三种引擎共存在一个类里而不是三个子类，是因为它要被 gson
 * 直接映射进 manifest.json：多态反序列化要额外的类型适配器，收益不抵成本。
 * 不属于本引擎的字段留 null 即可（gson 不会写出 null 字段）。
 */
public final class SourceFingerprint {

    /** 引擎名（{@link TrafficEngine#wireName()}）。v1 录制没有这个字段，读回来是 null = MySQL。 */
    public String engine;

    /** 版本串，三种引擎共用。 */
    public String version;

    // ==================== MySQL ====================
    public String serverUuid;
    public long serverId;
    public String sqlMode;
    public String timeZone;
    public String charset;
    public String collation;
    public int lowerCaseTableNames;

    // ==================== PostgreSQL ====================
    /** {@code pg_control_system().system_identifier}——PG 版的 server_uuid。 */
    public String systemIdentifier;
    /** 录制所在的数据库名。PG 的连接终生绑定一个库，回放建连时就要用它。 */
    public String dbName;
    public String serverEncoding;
    public String searchPath;
    public String dateStyle;
    public String intervalStyle;
    public String standardConformingStrings;
    /** 源库 {@code log_timezone}，用于解释日志里的时间戳。 */
    public String logTimeZone;

    // ==================== Oracle ====================
    /** {@code V$DATABASE.DBID}。 */
    public String dbid;
    /** {@code V$PDBS.CON_UID}；非 CDB 或无权限时为空。只作参考，不参与身份比对（见 {@link #identity()}）。 */
    public String conUid;
    /**
     * {@code SYS_CONTEXT('USERENV','CON_NAME')}——容器名。
     *
     * <p>身份比对用它而不是 {@code CON_UID}：实测 {@code CON_UID} 不是合法的 USERENV 属性
     * （ORA-02003），要读 {@code V$PDBS} 才拿得到，而回放账号未必有那个权限。
     * 两侧算出的身份串<b>形状必须完全一致</b>，否则"同实例"会被判成"不同实例"——
     * 而那正是危险的那个方向（会真的放行一次自我回放）。CON_NAME 是任何账号都读得到的。
     */
    public String conName;
    public String nlsDateFormat;
    public String nlsTimestampFormat;
    public String nlsTimestampTzFormat;
    public String nlsNumericCharacters;
    public String nlsSort;
    public String nlsComp;
    public String characterSet;
    /** 录制时的默认 schema（登录用户），回放建会话后要显式 {@code ALTER SESSION SET CURRENT_SCHEMA}。 */
    public String defaultSchema;

    /** 本指纹属于哪种引擎。 */
    public TrafficEngine engineOf() {
        return TrafficEngine.parse(engine);
    }

    /**
     * 实例身份串——"回放到源库自己"的判据。
     *
     * <p>Oracle 必须 <b>DBID + 容器名一起比</b>：只比 DBID 会把合法的 PDB→PDB 回放误拦
     * （同一个 CDB 下的两个 PDB 是两个独立的目标库），只比容器名则跨 CDB 时会撞号
     * （到处都有叫 {@code FREEPDB1} 的库）。
     *
     * @return 拿不到身份信息时返回 null（调用方据此降级为"无法判定"而不是"判定不同"）
     */
    public String identity() {
        switch (engineOf()) {
            case POSTGRESQL:
                return blank(systemIdentifier) ? null : systemIdentifier;
            case ORACLE:
                if (blank(dbid)) return null;
                return dbid + "/" + (conName == null ? "" : conName);
            default:
                return blank(serverUuid) ? null : serverUuid;
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isEmpty();
    }

    @Override
    public String toString() {
        return "SourceFingerprint{engine=" + engineOf().wireName() + ", version=" + version
                + ", identity=" + identity() + '}';
    }
}
