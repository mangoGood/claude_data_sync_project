package com.migration.traffic.model;

/**
 * 一条录制记录。字段名在 JSON 里压到 1~2 个字符是有意的：
 * 一条记录的固定开销约 60 字节，10k QPS 的源库跑 30 分钟就是 1800 万条，
 * 键名每多 20 字节就是多 360 MB。
 */
public final class TrafficRecord {

    /** 全局序号：<b>排序的唯一权威</b>（general_log 的物理读序），从 1 起单调递增。 */
    public long n;

    /** 相对录制原点 t0 的偏移，<b>微秒</b>。回放调度只看它。 */
    public long t;

    /** 源会话 id（{@code general_log.thread_id}，即连接 id）。 */
    public long s;

    /** 命令类型：Q=Query、E=Execute（服务端预处理，参数已替换）、I=Init DB、C=Connect、D=Quit。 */
    public String c;

    /** 语句类别。 */
    public StatementClass k;

    /** 该会话此刻的默认库；未知为 null。 */
    public String db;

    /** 来源账号 {@code user@host}。 */
    public String u;

    /** 语句原文。Connect/Quit 无原文。 */
    public String q;

    /**
     * MySQL 在写日志时抹掉了口令（{@code IDENTIFIED BY <secret>}）。
     * 这类语句<b>物理上不可忠实回放</b>——占位符是 MySQL 自己写进去的，谁也拿不到原文。
     */
    public boolean rd;

    /** 富化是否可用（{@code performance_schema} 尽力而为，缺省即 false）。 */
    public boolean hasEnrich;
    /** 源端错误码，0 = 成功。 */
    public int errno;
    /** 源端返回行数。 */
    public long rows;
    /** 源端影响行数。 */
    public long aff;
    /** 源端耗时（微秒）。 */
    public long us;

    public static final String CMD_QUERY = "Q";
    public static final String CMD_EXECUTE = "E";
    public static final String CMD_INIT_DB = "I";
    public static final String CMD_CONNECT = "C";
    public static final String CMD_QUIT = "D";

    /** 是否是一条要在目标库上执行的语句（Connect/Quit 只用于管理会话生命周期）。 */
    public boolean isExecutable() {
        return CMD_QUERY.equals(c) || CMD_EXECUTE.equals(c);
    }

    @Override
    public String toString() {
        return "TrafficRecord{n=" + n + ", t=" + t + ", s=" + s + ", c=" + c + ", k=" + k
                + ", db=" + db + ", q=" + (q == null ? "null"
                : q.length() > 80 ? q.substring(0, 80) + "…" : q) + '}';
    }
}
