package com.migration.agent.service;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.ssl.CertBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * agent 侧的 TLS 证书取用：按 {@code certId} 从元数据库取 PEM，在本机物化成各驱动要的形态。
 *
 * <p><b>为什么 agent 要自己取、而不是让后端把证书随任务消息发过来</b>：
 * <ul>
 *   <li>控制面消息经 Kafka，可能落到 broker 磁盘上并被保留数天——私钥不该在那条路上流动；</li>
 *   <li>平台是多 agent 集群，任务可能调度到任意一台。证书只在后端磁盘上，
 *       任务一旦落到别的主机就连不上，而这个错误要等<b>任务已经跑起来之后</b>才暴露。</li>
 * </ul>
 * agent 本来就有一条到元数据库的连接（位点中心库、注册、配额都走它），复用即可。
 *
 * <p>物化目录 {@code files/<taskId>/certs/<certId>/}，跟着任务走：任务清理时一并删掉
 * （{@code TaskFilesJanitor}），不会在磁盘上留下一堆无主私钥。
 *
 * <p>库口令的派生方式必须与后端 {@code CertificateService#storePasswordFor} 逐字节一致，
 * 否则 agent 生成的 p12 后端读不了、反之亦然。两边都由 {@code SYNCTASK_MASTER_KEY} + certId
 * 经 HMAC-SHA256 派生，不额外存一份秘密。
 */
public class CertificateStore {

    private static final Logger logger = LoggerFactory.getLogger(CertificateStore.class);

    private static volatile CertificateStore instance;

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public CertificateStore(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public static synchronized CertificateStore initialize(String dbUrl, String dbUser, String dbPassword) {
        if (instance == null) {
            instance = new CertificateStore(dbUrl, dbUser, dbPassword);
        }
        return instance;
    }

    public static CertificateStore getInstance() {
        return instance;
    }

    /**
     * 取出证书并物化到任务目录下。
     *
     * @return 物化好的证书包；{@code certId} 为空时返回 null（REQUIRED 及以下档位本就不需要证书）
     * @throws IllegalStateException 指定了证书却取不到/转换不了——**不能降级继续**：
     *         调用方是"要用 TLS 连库"，拿不到证书就连不上，静默跳过只会让它退回明文
     */
    public CertBundle materialize(String certId, String taskId, Path taskDir) {
        if (certId == null || certId.trim().isEmpty()) {
            return null;
        }
        String id = certId.trim();
        String caPem = null;
        String clientCert = null;
        String clientKey = null;

        String sql = "SELECT ca_cert, client_cert, client_key FROM db_certificates WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("任务引用的 TLS 证书不存在（可能已被删除）: " + id);
                }
                caPem = rs.getString(1);
                clientCert = rs.getString(2);
                // 私钥在库里是 ENC: 密文（与连接串口令同一套 AES-GCM）
                clientKey = CredentialCipher.decrypt(rs.getString(3));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取 TLS 证书失败: " + id + " —— " + e.getMessage(), e);
        }

        try {
            Path dir = taskDir.resolve("certs").resolve(id);
            CertBundle bundle = CertBundle.materialize(dir, caPem, clientCert, clientKey,
                    storePasswordFor(id));
            logger.info("任务 {} 的 TLS 证书已物化: certId={}, dir={}", taskId, id, dir);
            return bundle;
        } catch (Exception e) {
            throw new IllegalStateException("TLS 证书物化失败（无法转换成驱动可用的格式）: "
                    + id + " —— " + e.getMessage(), e);
        }
    }

    /**
     * keystore/truststore 的本机口令。
     *
     * <p>与后端 {@code com.synctask.service.CertificateService#storePasswordFor} <b>必须逐字节一致</b>：
     * 同一个证书在任何节点算出来的口令都一样，两侧才能读对方生成的 p12。
     * 口令本身只保护本机磁盘上的 p12，不是对外的认证凭据。
     */
    public static String storePasswordFor(String certId) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            String master = System.getenv("SYNCTASK_MASTER_KEY");
            byte[] key = (master == null || master.isEmpty())
                    ? "synctask-cert-store".getBytes(java.nio.charset.StandardCharsets.UTF_8)
                    : master.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            byte[] d = mac.doFinal(("cert-store:" + certId)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(d).substring(0, 32);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("派生证书库口令失败", e);
        }
    }
}
