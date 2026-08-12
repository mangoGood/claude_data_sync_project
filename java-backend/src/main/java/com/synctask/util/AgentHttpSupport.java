package com.synctask.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/**
 * 后端到 agent 的 HTTP(S) 连接。
 *
 * <p>agent 的接口带的是运维动作（failover / switchover-drain / start-increment）与位点、
 * 指标等诊断信息，鉴权靠 {@code AGENT_API_TOKEN}——明文 HTTP 下这个 token 可被嗅探，
 * 拿到即可对任意任务发起倒换。agent 侧开了 TLS（{@code AGENT_TLS_KEYSTORE}）之后，
 * 后端这一侧必须跟着走 https，否则所有代理调用直接连不上。
 *
 * <p>agent 用的是自签证书，不在 JVM 默认信任库里，所以要显式加载
 * {@code AGENT_TLS_TRUSTSTORE}。监控页早已改成走后端代理而不是直连 agent，
 * 因此<b>只有后端这一个客户端</b>需要信任它，改造面比看起来小。
 */
public final class AgentHttpSupport {

    private static final Logger logger = LoggerFactory.getLogger(AgentHttpSupport.class);

    private static volatile javax.net.ssl.SSLSocketFactory cachedFactory;
    private static volatile boolean factoryInitialized;

    private AgentHttpSupport() {
    }

    /** agent 侧是否开了 TLS —— 由是否配了 truststore 判定（后端只需要信任材料）。 */
    public static boolean tlsEnabled() {
        String ts = System.getenv("AGENT_TLS_TRUSTSTORE");
        return ts != null && !ts.trim().isEmpty();
    }

    public static String scheme() {
        return tlsEnabled() ? "https" : "http";
    }

    /** 把 agent 基址的协议头对齐当前配置（历史上各处都硬编码了 http://）。 */
    public static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isEmpty()) {
            return baseUrl;
        }
        if (!tlsEnabled()) {
            return baseUrl;
        }
        if (baseUrl.startsWith("http://")) {
            return "https://" + baseUrl.substring("http://".length());
        }
        return baseUrl;
    }

    /**
     * 打开到 agent 的连接。https 时装上 agent 的信任库。
     *
     * <p>不做主机名校验降级：agent 证书由 gen_certs.sh 生成，SAN 里带了 localhost 与
     * 127.0.0.1；连不上说明证书或地址确实不对，此时应当失败而不是绕过校验。
     */
    public static HttpURLConnection open(String url) throws Exception {
        URL u = new URL(normalizeBaseUrl(url));
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        if (conn instanceof javax.net.ssl.HttpsURLConnection) {
            javax.net.ssl.SSLSocketFactory f = trustFactory();
            if (f != null) {
                ((javax.net.ssl.HttpsURLConnection) conn).setSSLSocketFactory(f);
            }
        }
        return conn;
    }

    private static javax.net.ssl.SSLSocketFactory trustFactory() {
        if (factoryInitialized) {
            return cachedFactory;
        }
        synchronized (AgentHttpSupport.class) {
            if (factoryInitialized) {
                return cachedFactory;
            }
            factoryInitialized = true;
            String path = System.getenv("AGENT_TLS_TRUSTSTORE");
            if (path == null || path.trim().isEmpty()) {
                return null;
            }
            String pass = System.getenv().getOrDefault("AGENT_TLS_TRUSTSTORE_PASSWORD", "");
            try {
                KeyStore ts = KeyStore.getInstance("PKCS12");
                try (InputStream in = Files.newInputStream(Path.of(path.trim()))) {
                    ts.load(in, pass.toCharArray());
                }
                javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                        javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
                ctx.init(null, tmf.getTrustManagers(), null);
                cachedFactory = ctx.getSocketFactory();
                logger.info("后端到 agent 的连接已启用 TLS（truststore={}）", path);
            } catch (Exception e) {
                // 配了 truststore 却装不起来：不静默退回明文（那会让 https 调用全部失败得莫名其妙），
                // 明确报出来
                logger.error("加载 agent 信任库失败: {} —— {}", path, e.getMessage());
            }
            return cachedFactory;
        }
    }
}
