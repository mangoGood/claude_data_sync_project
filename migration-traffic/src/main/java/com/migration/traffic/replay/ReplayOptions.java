package com.migration.traffic.replay;

import com.migration.traffic.model.StatementClass;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/** 回放档位。 */
public final class ReplayOptions {

    /** 落后处置：保序等待 / 丢弃过期语句 / 拉长时间轴。 */
    public enum LagPolicy { WAIT, SKIP, STRETCH }

    /** 时间轴空洞处置：按原时长等待 / 压成 0。 */
    public enum GapPolicy { PRESERVE, COMPRESS }

    /** 结果比对：不比 / 只比行数（需录制时开了富化）。 */
    public enum CompareMode { NONE, ROWCOUNT }

    public double speed = 1.0;
    public Set<StatementClass> classes = new HashSet<>();
    public LagPolicy lagPolicy = LagPolicy.WAIT;
    public long lagSkipMs = 5000;
    public GapPolicy gapPolicy = GapPolicy.PRESERVE;
    public int maxSessions = 200;
    public int sessionQueueSize = 10_000;
    public CompareMode compare = CompareMode.NONE;
    public boolean allowDcl;
    public boolean allowDangerous;
    /** 错误率超过它即整体 fail-stop，避免继续在目标库上制造破坏。 */
    public double abortErrorRate = 0.5;
    /** 错误率判定的最小样本，样本太少时一两条错误不该把任务判死。 */
    public long abortMinSamples = 100;
    /** SELECT 结果集最多读多少行（只为算行数）；超过即停读并标记截断。 */
    public long maxFetchRows = 1_000_000L;
    /** 允许回放到与录制源相同的实例（危险，默认关）。 */
    public boolean allowSameInstance;

    public static ReplayOptions from(Properties p) {
        ReplayOptions o = new ReplayOptions();
        o.speed = dbl(p, "traffic.replay.speed", 1.0);
        if (o.speed <= 0) o.speed = 1.0;
        for (String s : p.getProperty("traffic.replay.classes", "SELECT,DML").split(",")) {
            StatementClass c = StatementClass.parse(s);
            if (c != null) o.classes.add(c);
        }
        o.lagPolicy = enumOf(LagPolicy.class, p.getProperty("traffic.replay.lag.policy"), LagPolicy.WAIT);
        o.lagSkipMs = lng(p, "traffic.replay.lag.skip.ms", 5000);
        o.gapPolicy = enumOf(GapPolicy.class, p.getProperty("traffic.replay.gap.policy"), GapPolicy.PRESERVE);
        o.maxSessions = (int) lng(p, "traffic.replay.max.sessions", 200);
        o.sessionQueueSize = (int) lng(p, "traffic.replay.session.queue.size", 10_000);
        o.compare = enumOf(CompareMode.class, p.getProperty("traffic.replay.compare"), CompareMode.NONE);
        o.allowDcl = Boolean.parseBoolean(p.getProperty("traffic.replay.allow.dcl", "false"));
        o.allowDangerous = Boolean.parseBoolean(p.getProperty("traffic.replay.allow.dangerous", "false"));
        o.abortErrorRate = dbl(p, "traffic.replay.abort.error.rate", 0.5);
        o.abortMinSamples = lng(p, "traffic.replay.abort.min.samples", 100);
        o.maxFetchRows = lng(p, "traffic.replay.max.fetch.rows", 1_000_000L);
        o.allowSameInstance = Boolean.parseBoolean(p.getProperty("traffic.replay.allow.same.instance", "false"));
        return o;
    }

    /**
     * 这条语句要不要执行。
     *
     * <p><b>会话状态类恒为 true</b>：{@code SET}/{@code USE}/{@code TCL} 不是负载而是会话状态。
     * 用户勾了"只回放 DML"就把 {@code SET autocommit=0} 滤掉的话，其后所有 DML 的
     * 事务语义就全变了；滤掉 {@code USE} 则后续非限定表名会落到错误的库。
     */
    public boolean shouldExecute(StatementClass k) {
        if (k == null) return false;
        if (!k.isPayload()) return true;
        if (k == StatementClass.DCL && !allowDcl) return false;
        return classes.isEmpty() || classes.contains(k);
    }

    private static <E extends Enum<E>> E enumOf(Class<E> type, String raw, E def) {
        if (raw == null || raw.isBlank()) return def;
        for (E e : type.getEnumConstants()) {
            if (e.name().equalsIgnoreCase(raw.trim())) return e;
        }
        return def;
    }

    private static double dbl(Properties p, String k, double def) {
        try {
            String v = p.getProperty(k);
            return v == null || v.isBlank() ? def : Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long lng(Properties p, String k, long def) {
        try {
            String v = p.getProperty(k);
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
