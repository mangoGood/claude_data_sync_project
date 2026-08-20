package com.migration.traffic.model;

/**
 * 录制来源库的身份与语义环境。
 *
 * <p>不是装饰性元数据：回放前要拿它跟目标库逐项比对（见 {@code TrafficPrecheck}）。
 * {@code sqlMode}/{@code timeZone}/{@code charset} 不一致时，<b>同一条 SQL 在两边的行为就是不一样的</b>；
 * {@code serverUuid} 相同则说明"回放目标就是录制源库自己"，必须硬拦截。
 */
public final class SourceFingerprint {
    public String serverUuid;
    public String version;
    public long serverId;
    public String sqlMode;
    public String timeZone;
    public String charset;
    public String collation;
    public int lowerCaseTableNames;

    @Override
    public String toString() {
        return "SourceFingerprint{uuid=" + serverUuid + ", version=" + version
                + ", serverId=" + serverId + ", lctn=" + lowerCaseTableNames + '}';
    }
}
