package com.migration.traffic.capture.pg;

/**
 * PostgreSQL 服务端日志里的一条记录（jsonlog 的一行 / csvlog 的一条记录）。
 *
 * <p>这是<b>日志行</b>而不是语句：一条语句在日志里可能对应 1~3 行
 * （{@code statement:} + {@code duration:} + 出错时的 {@code ERROR}），
 * 归并成语句是 {@link PgStatementAssembler} 的事。
 */
public final class PgLogEntry {

    /** 日志时刻，epoch 微秒。PG 只给到毫秒，末三位恒为 0。 */
    public long epochMicros;
    /** 后端进程号。采集连接自己的行按它过滤掉。 */
    public int pid;
    /** 会话 id，形如 {@code 6a87cfdd.147}（启动时刻.PID 的十六进制）。 */
    public String sessionId;
    /** 会话内单调递增的行号——会话内定序的权威。 */
    public long lineNum;
    public String user;
    public String dbname;
    public String remoteHost;
    /** LOG / ERROR / FATAL / PANIC / WARNING …… */
    public String severity;
    /** SQLSTATE（jsonlog 的 {@code state_code}）。 */
    public String stateCode;
    /** 正文：{@code statement: …} / {@code execute <unnamed>: …} / {@code duration: 0.182 ms} …… */
    public String message;
    /** 明细：{@code Parameters: $1 = 'x'}。 */
    public String detail;
    /** ERROR 行里引发错误的语句原文。 */
    public String statement;
    public String applicationName;
    public String backendType;

    /**
     * 会话 id 折算成 long。
     *
     * <p>{@code 6a87cfdd.147} = {@code 十六进制(会话启动 epoch 秒).十六进制(PID)}。
     * 拆开后拼成 {@code (epoch << 22) | pid}：PID 在 Linux 上最大 4194304（2^22），
     * 拼起来 54 位，long 装得下且<b>不会撞</b>——直接对字符串做哈希才会撞，
     * 而撞了的后果是两条源会话被回放到同一条目标连接上，事务和会话变量全乱。
     */
    public long sessionKey() {
        if (sessionId == null || sessionId.isEmpty()) return pid;
        int dot = sessionId.indexOf('.');
        if (dot <= 0) return pid;
        try {
            long start = Long.parseLong(sessionId.substring(0, dot), 16);
            long p = Long.parseLong(sessionId.substring(dot + 1), 16);
            return (start << 22) | (p & 0x3FFFFFL);
        } catch (NumberFormatException e) {
            return pid;
        }
    }

    /** 归一成 {@code user@host}，与 MySQL 侧口径一致。 */
    public String userHost() {
        String u = user == null || user.isEmpty() ? "unknown" : user;
        String h = remoteHost == null || remoteHost.isEmpty() ? "localhost" : remoteHost;
        return u + "@" + h;
    }
}
