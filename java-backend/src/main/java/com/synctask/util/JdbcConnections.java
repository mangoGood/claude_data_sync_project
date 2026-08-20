package com.synctask.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC 连接的统一构造：<b>URL 只放标识符，驱动参数一律走 {@link Properties}</b>。
 *
 * <h3>为什么不再拼查询串</h3>
 * <p>连接串里的 host / database 来自用户输入，而它们此前是直接
 * {@code String.format} 拼进 JDBC URL 的查询串前面。只要 database 里混进一个
 * {@code ?}，后面的东西就变成了驱动参数——{@code allowLoadLocalInfile} 让恶意
 * MySQL 服务端读走本机任意文件，{@code socketFactory} / {@code autoDeserialize}
 * 直接是 RCE。
 *
 * <p>{@link JdbcUrlSafety} 已经从字符集上堵住了这条路，但那是"靠校验挡住"。
 * 这里换成"结构上不可能"：URL 里<b>根本没有 {@code ?}</b>，
 * 于是任何混进标识符的内容都只会被驱动当作库名去解析并报"库不存在"，
 * 而不是被当成参数执行。两道一起上——校验会有人绕过去加新拼装点，
 * 结构性约束不会。
 *
 * <h3>池 key 的影响</h3>
 * <p>URL 不再带参数之后，{@code DataSourcePoolManager} 的池 key 也随之变短。
 * 这是好事：过去同一个库因为参数顺序不同会开出两个池；现在参数在 Properties 里，
 * 同库同用户必然复用同一个池。
 */
public final class JdbcConnections {

    private JdbcConnections() {
    }

    // ---------------------------------------------------------------- URL

    /**
     * 纯标识符 URL，不带任何查询串。
     *
     * @param database null / 空表示"不指定库"（先连上再切库的链路）
     */
    public static String mysqlUrl(String host, int port, String database) {
        String h = JdbcUrlSafety.requireSafeHost(host);
        int p = JdbcUrlSafety.requireSafePort(port);
        String d = JdbcUrlSafety.requireSafeDatabase(database);
        return "jdbc:mysql://" + h + ":" + p + "/" + (d == null ? "" : d);
    }

    /** 同上，PostgreSQL。 */
    public static String postgresUrl(String host, int port, String database) {
        String h = JdbcUrlSafety.requireSafeHost(host);
        int p = JdbcUrlSafety.requireSafePort(port);
        String d = JdbcUrlSafety.requireSafeDatabase(database);
        return "jdbc:postgresql://" + h + ":" + p + "/" + (d == null ? "" : d);
    }

    // ---------------------------------------------------------------- Properties

    /**
     * MySQL 驱动参数。取值与改造前拼在 URL 里的那串<b>逐项一致</b>，
     * 只是换了传递方式——行为不变是这次改造的硬要求。
     *
     * @param overrides 逐链路的差异项（如更短的 connectTimeout）。传 null 表示无覆盖。
     */
    public static Properties mysqlProps(String user, String password, Map<String, String> overrides) {
        Properties p = base(user, password);
        p.setProperty("serverTimezone", "UTC");
        p.setProperty("characterEncoding", "utf8");
        // 这个参数是历史行为的一部分：caching_sha2 认证在无 TLS 时需要它才能取到公钥。
        // 它本身有 MITM 风险，但去掉会让现有部署连不上——收敛靠的是把 sslMode 提上去，
        // 而不是在这里悄悄改语义。
        p.setProperty("allowPublicKeyRetrieval", "true");
        // 加密参数不在这里重新推导，而是解析 JdbcSslOptions 自己的输出。
        // 重新推导必然漂移：那边有 DISABLED→useSSL=false、rootCert→trustCertificateKeyStoreUrl、
        // .p12→trustCertificateKeyStoreType 这一串分支，抄一遍就是抄一遍 bug。
        // 解析法保证按构造等价，且 JdbcSslOptionsMirrorTest 继续替我们守着那份逻辑。
        putAll(p, parseParams(JdbcSslOptions.mysql()));
        merge(p, overrides);
        return p;
    }

    /** PostgreSQL 驱动参数。 */
    public static Properties postgresProps(String user, String password, Map<String, String> overrides) {
        Properties p = base(user, password);
        // 让 setString 能写进 uuid/json/inet 这些非文本列，与改造前的 URL 参数一致
        p.setProperty("stringtype", "unspecified");
        putAll(p, parseParams(JdbcSslOptions.postgres()));
        merge(p, overrides);
        return p;
    }

    /**
     * 把 {@code a=b&c=d} 形态的参数段拆成 Properties。
     *
     * <p>这是"URL 拼装"到"Properties 传参"之间的唯一桥梁：加密档位那套逻辑
     * 仍然只有 {@link JdbcSslOptions} 一份实现，这里只搬运不判断。
     */
    public static Properties parseParams(String fragment) {
        Properties p = new Properties();
        if (fragment == null || fragment.isBlank()) {
            return p;
        }
        for (String kv : fragment.split("&")) {
            if (kv.isBlank()) {
                continue;
            }
            int eq = kv.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            p.setProperty(kv.substring(0, eq), kv.substring(eq + 1));
        }
        return p;
    }

    private static void putAll(Properties target, Properties src) {
        for (String name : src.stringPropertyNames()) {
            target.setProperty(name, src.getProperty(name));
        }
    }

    private static Properties base(String user, String password) {
        Properties p = new Properties();
        if (user != null) {
            p.setProperty("user", user);
        }
        // 口令走 Properties 而不是 getConnection(url, user, pass) 的第三参：
        // 两者等价，但统一成一处便于审计"口令到底从哪来"
        p.setProperty("password", password == null ? "" : password);
        return p;
    }

    private static void merge(Properties p, Map<String, String> overrides) {
        if (overrides == null) {
            return;
        }
        for (Map.Entry<String, String> e : overrides.entrySet()) {
            if (e.getValue() == null) {
                p.remove(e.getKey());
            } else {
                // 覆盖项同样过黑名单：调用方也可能把不该给的参数传进来
                assertNotForbidden(e.getKey());
                p.setProperty(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Properties 这条路径同样要过危险参数黑名单。
     *
     * <p>换成 Properties 之后攻击面小了但没有消失：如果将来有人把用户可控的
     * key/value 直接 merge 进来，{@code socketFactory} 照样能生效——
     * 它是驱动参数，跟从 URL 来还是从 Properties 来无关。
     */
    private static void assertNotForbidden(String key) {
        // 复用 JdbcUrlSafety 的黑名单：造一个只含该参数的查询串让它去判，
        // 避免两处各维护一份必然会漂移的名单
        JdbcUrlSafety.requireSafeUrl("jdbc:x://h/db?" + key + "=1");
    }

    // ---------------------------------------------------------------- 便捷构造

    /** 常用覆盖项：连接/读超时（毫秒，MySQL 口径）。 */
    public static Map<String, String> mysqlTimeouts(int connectMs, int socketMs) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("connectTimeout", String.valueOf(connectMs));
        m.put("socketTimeout", String.valueOf(socketMs));
        return m;
    }

    /** 常用覆盖项：连接/读超时（秒，PostgreSQL 口径）。 */
    public static Map<String, String> postgresTimeouts(int connectSec, int socketSec) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("connectTimeout", String.valueOf(connectSec));
        m.put("socketTimeout", String.valueOf(socketSec));
        return m;
    }

    /** PostgreSQL 的 currentSchema。 */
    public static Map<String, String> pgSchema(String schema) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("currentSchema", schema == null || schema.isBlank() ? "public" : schema);
        return m;
    }
}
