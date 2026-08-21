package com.migration.traffic.capture;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 跟踪每个源会话此刻的默认库。
 *
 * <p>为什么必须跟：录制里大量语句用的是<b>非限定表名</b>（{@code SELECT * FROM t1}），
 * 它落在哪个库完全取决于该会话当时的默认库。不记下来，回放时这些语句要么报
 * "No database selected"，要么更糟——落到目标连接碰巧选中的<b>另一个库</b>上。
 *
 * <p>三个信息来源，缺一不可：
 * <ul>
 *   <li>{@code Connect} 行的 {@code user@host on <db> using <proto>} —— 连接时就带库的情形；</li>
 *   <li>{@code Init DB} 行 —— MySQL 协议层的库切换（CLI 的 {@code USE}、JDBC 的
 *       {@code setCatalog}、连接串里的库都走这条）；</li>
 *   <li>{@code Query} 里的 {@code USE db} —— 应用直接发 SQL 切库。</li>
 * </ul>
 * 实测 MySQL CLI 的 {@code USE mysql} 只产生 {@code Init DB [mysql]}，<b>不产生 Query 行</b>，
 * 所以只认 {@code USE} 语句是不够的。
 */
public final class SessionSchemaTracker {

    /** 会话上限：源库短连接风暴时（每秒上千次连接）不能让映射表无界增长。 */
    private final int maxSessions;

    private final LinkedHashMap<Long, String> schemaBySession;
    private long peakConcurrent;

    public SessionSchemaTracker(int maxSessions) {
        this.maxSessions = Math.max(64, maxSessions);
        this.schemaBySession = new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
                return size() > SessionSchemaTracker.this.maxSessions;
            }
        };
    }

    /** 用捕获开始时的 PROCESSLIST 快照播种：这些会话的 Connect 行已经过去了，只能这样拿到。 */
    public void seed(Map<Long, String> snapshot) {
        if (snapshot == null) return;
        for (Map.Entry<Long, String> e : snapshot.entrySet()) {
            if (e.getValue() != null && !e.getValue().isEmpty()) {
                put(e.getKey(), e.getValue());
            }
        }
    }

    /** {@code Connect} 行：{@code root@localhost on testdb using TCP/IP}。 */
    public void onConnect(long threadId, String argument) {
        String db = parseConnectSchema(argument);
        if (db != null && !db.isEmpty()) {
            put(threadId, db);
        } else {
            put(threadId, null);
        }
    }

    /** {@code Init DB} 行：argument 就是库名。 */
    public void onInitDb(long threadId, String argument) {
        put(threadId, argument == null ? null : argument.trim());
    }

    /** {@code Query} 行：只有 {@code USE db} 会改变默认库。 */
    public void onQuery(long threadId, String sql) {
        String target = StatementClassifier.useTarget(sql);
        if (target != null) {
            put(threadId, target);
        }
    }

    public void onQuit(long threadId) {
        schemaBySession.remove(threadId);
    }

    /** 该会话此刻的默认库；未知返回 null。 */
    public String schemaOf(long threadId) {
        return schemaBySession.get(threadId);
    }

    public int activeSessions() {
        return schemaBySession.size();
    }

    public long peakConcurrentSessions() {
        return peakConcurrent;
    }

    private void put(long threadId, String db) {
        schemaBySession.put(threadId, db);
        if (schemaBySession.size() > peakConcurrent) {
            peakConcurrent = schemaBySession.size();
        }
    }

    /**
     * 从 Connect 行取默认库。格式：{@code <user>@<host> on <db> using <protocol>}，
     * 无默认库时 db 段为空（实测是 {@code "root@localhost on  using Socket"}，两个空格）。
     */
    public static String parseConnectSchema(String argument) {
        if (argument == null) return null;
        int on = argument.indexOf(" on ");
        if (on < 0) return null;
        int using = argument.lastIndexOf(" using ");
        // 无默认库时实测是 "root@localhost on  using Socket"（两个空格）：
        // 此时 " using " 恰好<b>紧贴</b> " on " 之后，using == on+4，中间那段是空串。
        // 早先写成 using <= on+4 就回退去取整个尾巴，于是每个无库连接的默认库
        // 都被记成 "using Socket"——回放时这些会话会去 USE 一个不存在的库。
        if (using < on + 4) {
            return null;    // 格式不认识，宁可记成"未知"，不猜
        }
        String db = argument.substring(on + 4, using).trim();
        return db.isEmpty() ? null : db;
    }

    /**
     * 归一 {@code user_host} 列成 {@code user@host}。
     *
     * <p>MySQL 有两种写法，必须都认：已认证的会话是 {@code root[root] @ localhost []}，
     * 而 {@code Connect} 行（认证刚完成那一刻）是 {@code [root] @ localhost []}——
     * 用户名前缀是空的。只处理前一种，Connect 行的账号就会记成 {@code [root]@...}，
     * 同一个会话在录制里出现两个不同的账号写法。
     */
    public static String normalizeUserHost(String raw) {
        if (raw == null) return null;
        int at = raw.indexOf(" @ ");
        if (at < 0) return raw.trim();
        String user = stripBrackets(raw.substring(0, at).trim());
        String host = raw.substring(at + 3).trim();
        // "localhost []" → localhost ; "[10.0.0.5]" → 10.0.0.5
        int lb = host.indexOf('[');
        if (lb >= 0) {
            String inside = host.substring(lb + 1, Math.max(lb + 1, host.lastIndexOf(']')));
            String outside = host.substring(0, lb).trim();
            host = !outside.isEmpty() ? outside : inside;
        }
        return user + "@" + host;
    }

    /** {@code root[root]} → root；{@code [root]} → root；{@code root} → root。 */
    private static String stripBrackets(String user) {
        int lb = user.indexOf('[');
        if (lb < 0) return user;
        if (lb > 0) return user.substring(0, lb);
        int rb = user.lastIndexOf(']');
        return rb > 1 ? user.substring(1, rb) : user;
    }
}
