package com.migration.common.security;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * JDBC URL 拼装的输入闸门（引擎侧镜像，与后端 com.synctask.util.JdbcUrlSafety 逐字一致）。
 *
 * <p><b>为什么是镜像而不是共用</b>：后端刻意不依赖 migration-common（Spring BOM 与
 * mysql-connector/ojdbc/HikariCP/logback 的版本仲裁会打架，本仓库在 migration-mongo 上
 * 吃过一次驱动降级混包的亏）。CredentialCipher / KafkaSecurity / JdbcSslOptions 用的也是
 * 同一个做法。两侧一致性由 JdbcUrlSafetyMirrorTest 的用例矩阵守住。
 *
 * <p><b>为什么需要它</b>：连接串按正则拆成 host / port / database 之后，这三段是直接
 * {@code String.format} 拼进 JDBC URL 的。而原来那条正则里 database 组是 {@code (.*)}
 * ——问号和 {@code &} 全部放行，于是任何能建连接的账号都可以把驱动参数塞进 URL：
 *
 * <ul>
 *   <li>MySQL {@code allowLoadLocalInfile=true&allowUrlInLocalInfile=true} + 一台恶意
 *       MySQL 服务端 = <b>读走本机任意文件</b>（含主密钥文件，那把钥匙能解开库里全部凭证）；</li>
 *   <li>MySQL {@code autoDeserialize=true&queryInterceptors=…ServerStatusDiffInterceptor}
 *       = Connector/J 的经典反序列化 <b>RCE</b>；</li>
 *   <li>PostgreSQL {@code socketFactory=…ClassPathXmlApplicationContext&socketFactoryArg=http://…}
 *       = <b>RCE</b>，而后端恰好带着完整 Spring。</li>
 * </ul>
 *
 * <p><b>两道防线，都要</b>：字符集白名单挡住"能不能拼出第二个参数"，危险参数黑名单挡住
 * "万一哪条拼装路径漏了校验"。只做黑名单不行——参数名大小写、URL 编码、驱动别名都能绕；
 * 只做白名单也不够——拼装点有十几处，将来新增的那处很可能忘了调。
 *
 * <p>字符集依据各驱动实际允许的标识符：主机名走 RFC 1123（字母数字、点、连字符，
 * 另放 IPv6 的冒号与方括号），库名走各引擎标识符的交集再加上 Oracle 服务名的点。
 */
public final class JdbcUrlSafety {

    /** 主机名 / IP。允许 IPv6 的 {@code [::1]} 形式。 */
    private static final Pattern HOST =
            Pattern.compile("^(?:\\[[0-9A-Fa-f:.]{2,45}]|[A-Za-z0-9][A-Za-z0-9._-]{0,252}[A-Za-z0-9]|[A-Za-z0-9])$");

    /** 库名 / schema / Oracle 服务名。 */
    private static final Pattern DATABASE =
            Pattern.compile("^[A-Za-z0-9_$][A-Za-z0-9_$.-]{0,127}$");

    /**
     * 危险驱动参数（小写比较）。这些参数的共同点是：能让**服务端或配置方**驱动客户端
     * 去读文件、反序列化、或加载任意类。
     */
    private static final Set<String> FORBIDDEN_PARAMS = Set.of(
            // MySQL Connector/J
            "allowloadlocalinfile",
            "allowurlinlocalinfile",
            "autodeserialize",
            "queryinterceptors",
            "statementinterceptors",
            "propertiestransform",
            "detectcustomcollations",
            "servertimezone.class",
            // PostgreSQL JDBC
            "socketfactory",
            "socketfactoryarg",
            "sslfactory",
            "sslfactoryarg",
            "sslhostnameverifier",
            "sslpasswordcallback",
            "loggerfile",
            // Oracle / 通用
            "oracle.net.wallet_location",
            "oracle.jdbc.javanetnio",
            "init",
            "initsql"
    );

    private JdbcUrlSafety() {
    }

    /** 主机名校验。不合法直接抛——静默清洗会得到一个连向意外主机的连接。 */
    public static String requireSafeHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("主机名不能为空");
        }
        String h = host.trim();
        if (!HOST.matcher(h).matches()) {
            throw new IllegalArgumentException(
                    "非法主机名: '" + host + "'。只允许字母、数字、点、连字符（IPv6 用方括号形式）");
        }
        return h;
    }

    /** 端口校验。 */
    public static int requireSafePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("非法端口: " + port + "（应在 1~65535）");
        }
        return port;
    }

    /**
     * 库名校验。允许 null / 空——不少链路是"不指定库连上去再切"，这时返回原值。
     */
    public static String requireSafeDatabase(String database) {
        if (database == null || database.isBlank()) {
            return database;
        }
        String d = database.trim();
        if (!DATABASE.matcher(d).matches()) {
            throw new IllegalArgumentException(
                    "非法库名: '" + database + "'。只允许字母、数字、下划线、$、点、连字符"
                            + "（出现 ? 或 & 通常意味着有人在往 JDBC URL 里注入驱动参数）");
        }
        return d;
    }

    /**
     * 拼好的 URL 的最后一道闸：扫描危险参数。
     *
     * <p>放在拼装之后而不是之前，是为了覆盖那些**不经过上面三个方法**的历史拼装点——
     * 它们把整段连接串拿去 format，字符集校验够不着。
     */
    public static String requireSafeUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        int q = jdbcUrl.indexOf('?');
        if (q < 0) {
            return jdbcUrl;
        }
        String query = jdbcUrl.substring(q + 1).toLowerCase(Locale.ROOT);
        for (String bad : FORBIDDEN_PARAMS) {
            // 匹配 "名=" 且前面是 ? 或 &，避免 "xxxinit=" 这种子串误伤
            int from = 0;
            while (true) {
                int i = query.indexOf(bad + "=", from);
                if (i < 0) {
                    break;
                }
                if (i == 0 || query.charAt(i - 1) == '&' || query.charAt(i - 1) == ';') {
                    throw new IllegalArgumentException(
                            "连接参数 '" + bad + "' 被禁止：该参数可被用于读取本机文件或加载任意类");
                }
                from = i + 1;
            }
        }
        return jdbcUrl;
    }
}
