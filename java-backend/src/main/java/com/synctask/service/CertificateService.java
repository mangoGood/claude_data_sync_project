package com.synctask.service;

import com.synctask.entity.DbCertificate;
import com.synctask.repository.DbCertificateRepository;
import com.synctask.util.CertMaterial;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 证书库：上传（解析 + 归一）、查询、删除，以及为控制面连接按需物化证书包。
 *
 * <p><b>物化缓存</b>：驱动要的是<b>文件路径</b>，而库里存的是 PEM 文本。每次连接都重新
 * 转一遍 p12 既慢又会在磁盘上留一堆临时文件，所以按 certId 物化一次并缓存目录。
 * 缓存目录在 {@code synctask.cert.dir}（默认 {@code ${user.dir}/certs}）下，权限 0700。
 *
 * <p><b>keystore 口令</b>不随机：它要跟着证书走（agent 侧要用同一个口令读回同一批文件），
 * 因此由 certId 派生 —— 派生源是 {@code SYNCTASK_MASTER_KEY} 保护下的稳定值，
 * 不落额外的秘密。口令本身只保护本机磁盘上的 p12，不是对外的认证凭据。
 */
@Service
public class CertificateService {

    private static final Logger logger = LoggerFactory.getLogger(CertificateService.class);

    @Autowired
    private DbCertificateRepository certificateRepository;

    @Value("${synctask.cert.dir:${user.dir}/certs}")
    private String certBaseDir;

    /** certId → 已物化的证书包目录。进程内缓存，重启后按需重建。 */
    private final Map<String, CertMaterial> materialized = new ConcurrentHashMap<>();

    // ==================== 上传 ====================

    @Transactional
    public DbCertificate upload(Long userId, String name, String caPem, String clientPem, String clientKey) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("证书名称不能为空");
        }
        String trimmedName = name.trim();
        if (certificateRepository.existsByUserIdAndName(userId, trimmedName)) {
            throw new IllegalArgumentException("已存在同名证书: " + trimmedName);
        }
        if (isBlank(caPem) && isBlank(clientPem)) {
            throw new IllegalArgumentException("至少要提供 CA 证书或客户端证书");
        }

        // 解析即校验：解析不了的内容不该进库。让它在上传时报错，而不是等任务跑起来才炸。
        List<X509Certificate> caCerts = parseOrThrow(caPem, "CA 证书");
        parseOrThrow(clientPem, "客户端证书");
        if (!isBlank(clientKey)) {
            try {
                CertMaterial.toPkcs8Der(clientKey);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("客户端私钥无法解析: " + e.getMessage(), e);
            }
        }
        if (isBlank(clientPem) != isBlank(clientKey)) {
            throw new IllegalArgumentException("客户端证书与私钥必须成对提供（双向认证 mTLS）");
        }

        DbCertificate cert = new DbCertificate();
        cert.setId(UUID.randomUUID().toString());
        cert.setName(trimmedName);
        cert.setUserId(userId);
        cert.setCaCert(blankToNull(caPem));
        cert.setClientCert(blankToNull(clientPem));
        cert.setClientKey(blankToNull(clientKey));
        cert.setHasClientCert(!isBlank(clientPem));
        cert.setCreatedAt(LocalDateTime.now());
        cert.setUpdatedAt(LocalDateTime.now());

        if (!caCerts.isEmpty()) {
            X509Certificate ca = caCerts.get(0);
            cert.setSubjectCn(cnOf(ca.getSubjectX500Principal().getName()));
            cert.setNotAfter(LocalDateTime.ofInstant(ca.getNotAfter().toInstant(), ZoneId.systemDefault()));
            try {
                cert.setFingerprint(CertMaterial.fingerprint(ca));
            } catch (GeneralSecurityException e) {
                logger.warn("计算证书指纹失败: {}", e.getMessage());
            }
        }

        DbCertificate saved = certificateRepository.save(cert);
        // 立刻物化一次：上传后紧接着就是"测试连接"，这一步失败要现在就暴露
        materialize(saved);
        logger.info("证书已上传: id={}, name={}, mTLS={}", saved.getId(), saved.getName(), saved.getHasClientCert());
        return saved;
    }

    // ==================== 查询 / 删除 ====================

    public List<Map<String, Object>> list(Long userId) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (DbCertificate c : certificateRepository.findByUserIdOrderByCreatedAtDesc(userId)) {
            out.add(toSafeMap(c));
        }
        return out;
    }

    /** 对外表示：**绝不**回私钥，连密文也不回。 */
    public Map<String, Object> toSafeMap(DbCertificate c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", c.getId());
        m.put("name", c.getName());
        m.put("subjectCn", c.getSubjectCn());
        m.put("fingerprint", c.getFingerprint());
        m.put("notAfter", c.getNotAfter());
        m.put("hasClientCert", Boolean.TRUE.equals(c.getHasClientCert()));
        m.put("hasCaCert", c.getCaCert() != null);
        m.put("createdAt", c.getCreatedAt());
        m.put("expired", c.getNotAfter() != null && c.getNotAfter().isBefore(LocalDateTime.now()));
        m.put("expiringSoon", c.getNotAfter() != null
                && c.getNotAfter().isAfter(LocalDateTime.now())
                && c.getNotAfter().isBefore(LocalDateTime.now().plusDays(30)));
        return m;
    }

    public DbCertificate require(String certId, Long userId) {
        return certificateRepository.findByIdAndUserId(certId, userId)
                .orElseThrow(() -> new IllegalArgumentException("证书不存在或无权访问: " + certId));
    }

    @Transactional
    public void delete(String certId, Long userId) {
        DbCertificate cert = require(certId, userId);
        long refs = certificateRepository.countReferencingWorkflows(certId);
        if (refs > 0) {
            // 删掉正在被引用的证书，任务会在下一次重启/续传时才失败——那时没人会把它
            // 和几天前的一次删除联系起来。挡在这一侧。
            throw new IllegalArgumentException(
                    "该证书正被 " + refs + " 个任务引用，无法删除。请先把这些任务改用其它证书或关闭 SSL");
        }
        certificateRepository.delete(cert);
        materialized.remove(certId);
        logger.info("证书已删除: id={}, name={}", certId, cert.getName());
    }

    // ==================== 物化 ====================

    /**
     * 取得可直接喂给驱动的证书包（含 truststore.p12 / ca.pem / client-key.pk8 等）。
     * 未指定证书时返回 null —— REQUIRED 及以下档位本来就不需要证书。
     */
    public CertMaterial materialFor(String certId, Long userId) {
        if (certId == null || certId.trim().isEmpty()) {
            return null;
        }
        CertMaterial cached = materialized.get(certId);
        if (cached != null) {
            return cached;
        }
        return materialize(require(certId, userId));
    }

    private CertMaterial materialize(DbCertificate cert) {
        try {
            Path dir = Path.of(certBaseDir, cert.getId());
            CertMaterial m = CertMaterial.materialize(dir, cert.getCaCert(), cert.getClientCert(),
                    cert.getClientKey(), storePasswordFor(cert.getId()));
            materialized.put(cert.getId(), m);
            return m;
        } catch (IOException | GeneralSecurityException | RuntimeException e) {
            throw new IllegalArgumentException("证书物化失败（无法转换成驱动可用的格式）: " + e.getMessage(), e);
        }
    }

    /**
     * keystore/truststore 的本机口令。由 certId 与主密钥共同派生，保证：
     * 同一个证书在任何时候、任何节点算出来的口令一致（agent 侧要读回同一批文件），
     * 而不同证书之间互不相同。
     */
    public static String storePasswordFor(String certId) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            String master = System.getenv("SYNCTASK_MASTER_KEY");
            byte[] key = (master == null || master.isEmpty())
                    ? "synctask-cert-store".getBytes(StandardCharsets.UTF_8)
                    : master.getBytes(StandardCharsets.UTF_8);
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            byte[] d = mac.doFinal(("cert-store:" + certId).getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(d).substring(0, 32);
        } catch (GeneralSecurityException e) {
            // 理论不可达（HmacSHA256 是 JDK 必备算法）；真出了就退随机，宁可 agent 侧重新物化
            byte[] raw = new byte[24];
            new SecureRandom().nextBytes(raw);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        }
    }

    // ==================== 辅助 ====================

    private static List<X509Certificate> parseOrThrow(String pem, String what) {
        if (isBlank(pem)) {
            return List.of();
        }
        try {
            return CertMaterial.parseCertificates(pem);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException(what + "无法解析，请确认上传的是 PEM 格式（-----BEGIN CERTIFICATE-----）: "
                    + e.getMessage(), e);
        }
    }

    /** 从 X500 DN 里取 CN；取不到就把整个 DN 带回去，总比空着强。 */
    private static String cnOf(String dn) {
        if (dn == null) {
            return null;
        }
        for (String part : dn.split(",")) {
            String s = part.trim();
            if (s.regionMatches(true, 0, "CN=", 0, 3)) {
                return s.substring(3);
            }
        }
        return dn;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String blankToNull(String s) {
        return isBlank(s) ? null : s.trim();
    }

    /** 到期与临期证书（供告警/巡检用）。 */
    public Map<String, Object> expiryReport(Long userId) {
        Map<String, Object> out = new HashMap<>();
        List<Map<String, Object>> expired = new ArrayList<>();
        List<Map<String, Object>> expiring = new ArrayList<>();
        for (DbCertificate c : certificateRepository.findByUserIdOrderByCreatedAtDesc(userId)) {
            Map<String, Object> m = toSafeMap(c);
            if (Boolean.TRUE.equals(m.get("expired"))) {
                expired.add(m);
            } else if (Boolean.TRUE.equals(m.get("expiringSoon"))) {
                expiring.add(m);
            }
        }
        out.put("expired", expired);
        out.put("expiringSoon", expiring);
        return out;
    }
}
