package com.migration.traffic.capture;

import com.migration.traffic.model.StatementClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * 捕获侧过滤：库白名单 / 账号白名单 / 语句类别 / 采样。
 *
 * <p>两条不能违反的规则：
 * <ol>
 *   <li><b>会话状态类语句永远不过滤</b>（{@code SET}/{@code USE}/{@code TCL}）。
 *       录制里少一条 {@code SET autocommit=0} 或 {@code USE db}，
 *       整个录制就不再可回放了——后续语句会落到错误的库、或事务语义整体改变。</li>
 *   <li><b>采样按会话（thread_id）哈希，不按语句</b>。按语句采样会把事务切碎、
 *       把会话状态采丢，得到一个"看起来有 10% 数据"实则完全不可回放的文件。</li>
 * </ol>
 */
public final class CaptureFilter {

    private final Set<String> databases;
    private final Set<String> users;
    private final Set<StatementClass> classes;
    private final double sampleRate;

    public CaptureFilter(Set<String> databases, Set<String> users,
                         Set<StatementClass> classes, double sampleRate) {
        this.databases = databases == null ? Collections.emptySet() : databases;
        this.users = users == null ? Collections.emptySet() : users;
        this.classes = classes == null ? Collections.emptySet() : classes;
        this.sampleRate = sampleRate <= 0 ? 1.0 : Math.min(1.0, sampleRate);
    }

    public static CaptureFilter from(Properties p) {
        return new CaptureFilter(
                lowerSet(p.getProperty("traffic.capture.databases", "")),
                rawSet(p.getProperty("traffic.capture.users", "")),
                classSet(p.getProperty("traffic.capture.classes", "SELECT,DML,DDL")),
                doubleProp(p, "traffic.capture.sample.rate", 1.0));
    }

    /** 该会话是否入样。同一 thread_id 恒定同一结论。 */
    public boolean sessionSampled(long threadId) {
        if (sampleRate >= 1.0) return true;
        // 乘性散列后取低位，避免 thread_id 单调递增时按模采样退化成"每 N 个取一个"的规律性偏差
        long h = threadId * 0x9E3779B97F4A7C15L;
        h ^= (h >>> 32);
        return Math.floorMod(h, 10_000L) < (long) (sampleRate * 10_000);
    }

    /** 语句是否保留。会话状态类恒为 true。 */
    public boolean accept(StatementClass k, String db, String userHost) {
        if (k == null) return false;
        if (!k.isPayload()) {
            return true;    // SET / USE / TCL / OTHER：会话状态，恒保留
        }
        if (!classes.isEmpty() && !classes.contains(k)) return false;
        if (!databases.isEmpty()) {
            if (db == null || !databases.contains(db.toLowerCase(Locale.ROOT))) return false;
        }
        if (!users.isEmpty() && (userHost == null || !matchesUser(userHost))) return false;
        return true;
    }

    private boolean matchesUser(String userHost) {
        if (users.contains(userHost)) return true;
        int at = userHost.indexOf('@');
        return at > 0 && users.contains(userHost.substring(0, at));
    }

    public Set<String> databases() {
        return databases;
    }

    public Set<String> users() {
        return users;
    }

    public Set<StatementClass> classes() {
        return classes;
    }

    public double sampleRate() {
        return sampleRate;
    }

    private static Set<String> lowerSet(String csv) {
        Set<String> out = new LinkedHashSet<>();
        for (String s : csv.split(",")) {
            String v = s.trim();
            if (!v.isEmpty()) out.add(v.toLowerCase(Locale.ROOT));
        }
        return out;
    }

    private static Set<String> rawSet(String csv) {
        Set<String> out = new LinkedHashSet<>();
        for (String s : csv.split(",")) {
            String v = s.trim();
            if (!v.isEmpty()) out.add(v);
        }
        return out;
    }

    private static Set<StatementClass> classSet(String csv) {
        Set<StatementClass> out = new HashSet<>();
        for (String s : csv.split(",")) {
            StatementClass c = StatementClass.parse(s);
            if (c != null) out.add(c);
        }
        return out;
    }

    private static double doubleProp(Properties p, String key, double def) {
        try {
            String v = p.getProperty(key);
            return v == null || v.isBlank() ? def : Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
