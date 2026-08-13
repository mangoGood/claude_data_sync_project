package com.migration.common.ssl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 证书归一：用户只上传 PEM，平台转出所有驱动各自要的形态。
 *
 * <p><b>为什么必须归一</b>：同一份证书，每种客户端要的形态都不一样——
 * <pre>
 *   MySQL Connector/J  keystore（p12/jks），不吃 PEM
 *   PostgreSQL JDBC    PEM，且私钥必须是 PKCS8 <b>DER</b>（.pk8）
 *   Kafka              keystore
 *   Oracle             keystore，且走连接属性不走 URL
 *   Mongo/Redis/ES/binlog  一个 SSLContext 对象
 * </pre>
 * 让用户按链路自备五种格式是不现实的（他手上只有 openssl 或数据库自带工具产出的 PEM）。
 * 因此入库时转换一次，产出一个"证书包"目录，之后各链路各取所需。
 *
 * <p>转换全部走 JDK 自带 API，不 shell-out 调 openssl——agent 主机不保证装了它，
 * 而"证书包在 A 机器生成得了、在 B 机器生成不了"是最难查的一类部署问题。
 *
 * <p>产物：
 * <pre>
 *   ca.pem            原样（PG / TiCDC / Mongo 直用）
 *   client-cert.pem   原样
 *   client-key.pem    原样（PKCS8 PEM）
 *   client-key.pk8    PKCS8 DER —— PG JDBC 专用
 *   truststore.p12    CA 导入 —— MySQL / Oracle / Kafka
 *   keystore.p12      客户端证书 + 私钥 —— mTLS
 * </pre>
 */
public final class CertBundle {

    public static final String CA_PEM = "ca.pem";
    public static final String CLIENT_CERT_PEM = "client-cert.pem";
    public static final String CLIENT_KEY_PEM = "client-key.pem";
    public static final String CLIENT_KEY_PK8 = "client-key.pk8";
    public static final String TRUSTSTORE_P12 = "truststore.p12";
    public static final String KEYSTORE_P12 = "keystore.p12";

    /** 证书文件大小上限：正常的 PEM 都在几 KB，给到 64KB 已经很宽。 */
    public static final int MAX_PEM_BYTES = 64 * 1024;

    private static final String PKCS1_BEGIN = "-----BEGIN RSA PRIVATE KEY-----";
    private static final String ENCRYPTED_BEGIN = "-----BEGIN ENCRYPTED PRIVATE KEY-----";

    private final Path dir;
    private final String storePassword;

    private CertBundle(Path dir, String storePassword) {
        this.dir = dir;
        this.storePassword = storePassword;
    }

    public Path dir() {
        return dir;
    }

    public String storePassword() {
        return storePassword;
    }

    public Path path(String name) {
        return dir.resolve(name);
    }

    private boolean has(String name) {
        return Files.isRegularFile(path(name));
    }

    /** 按这个证书包构造一个指定档位的 {@link SslMaterial}。 */
    public SslMaterial toMaterial(String mode) {
        return SslMaterial.of(
                mode,
                has(TRUSTSTORE_P12) ? path(TRUSTSTORE_P12).toString() : null,
                has(CLIENT_CERT_PEM) ? path(CLIENT_CERT_PEM).toString() : null,
                has(CLIENT_KEY_PK8) ? path(CLIENT_KEY_PK8).toString() : null,
                has(KEYSTORE_P12) ? path(KEYSTORE_P12).toString() : null,
                storePassword);
    }

    /** PG 链路要的是 PEM 形态的信任材料，不是 p12。 */
    public SslMaterial toPgMaterial(String mode) {
        return SslMaterial.of(
                mode,
                has(CA_PEM) ? path(CA_PEM).toString() : null,
                has(CLIENT_CERT_PEM) ? path(CLIENT_CERT_PEM).toString() : null,
                has(CLIENT_KEY_PK8) ? path(CLIENT_KEY_PK8).toString() : null,
                null,
                storePassword);
    }

    // ==================== 归一 ====================

    /**
     * 把上传的 PEM 材料写成一个证书包。
     *
     * @param dir        目标目录（会被创建，权限收到仅属主可读写）
     * @param caPem      CA 证书 PEM，VERIFY_CA 及以上必填
     * @param clientPem  客户端证书 PEM，mTLS 时给
     * @param clientKey  客户端私钥 PEM（PKCS8 或 PKCS1），mTLS 时给
     */
    public static CertBundle materialize(Path dir, String caPem, String clientPem, String clientKey)
            throws IOException, GeneralSecurityException {
        return materialize(dir, caPem, clientPem, clientKey, randomPassword());
    }

    /**
     * 同上，但由调用方指定 keystore/truststore 口令。
     *
     * <p>跨进程共享同一批证书文件时必须用这个重载：口令随机生成的话，
     * agent 生成的 p12 后端读不了、反之亦然。平台里两侧都由
     * {@code SYNCTASK_MASTER_KEY + certId} 经 HMAC 派生出同一个口令。
     */
    public static CertBundle materialize(Path dir, String caPem, String clientPem, String clientKey,
                                         String storePass)
            throws IOException, GeneralSecurityException {
        Files.createDirectories(dir);
        restrictToOwner(dir);

        if (isNotBlank(caPem)) {
            writeOwnerOnly(dir.resolve(CA_PEM), caPem.getBytes(StandardCharsets.UTF_8));
            KeyStore ts = emptyPkcs12();
            int i = 0;
            for (X509Certificate cert : parseCertificates(caPem)) {
                ts.setCertificateEntry("ca-" + (i++), cert);
            }
            storeTo(ts, dir.resolve(TRUSTSTORE_P12), storePass);
        }

        if (isNotBlank(clientPem) && isNotBlank(clientKey)) {
            writeOwnerOnly(dir.resolve(CLIENT_CERT_PEM), clientPem.getBytes(StandardCharsets.UTF_8));
            writeOwnerOnly(dir.resolve(CLIENT_KEY_PEM), clientKey.getBytes(StandardCharsets.UTF_8));

            byte[] pkcs8 = toPkcs8Der(clientKey);
            writeOwnerOnly(dir.resolve(CLIENT_KEY_PK8), pkcs8);

            PrivateKey key = KeyFactory.getInstance("RSA")
                    .generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
            List<X509Certificate> chain = parseCertificates(clientPem);
            KeyStore ks = emptyPkcs12();
            ks.setKeyEntry("client", key, storePass.toCharArray(),
                           chain.toArray(new Certificate[0]));
            storeTo(ks, dir.resolve(KEYSTORE_P12), storePass);
        } else if (isNotBlank(clientPem) != isNotBlank(clientKey)) {
            throw new IllegalArgumentException(
                    "客户端证书与私钥必须成对提供（双向认证 mTLS）；只给一个无法建立连接");
        }

        return new CertBundle(dir, storePass);
    }

    /** 已归一好的目录 + 口令（agent 从元数据库取回证书落盘后用这个入口）。 */
    public static CertBundle at(Path dir, String storePassword) {
        return new CertBundle(dir, storePassword);
    }

    // ==================== PEM 解析 ====================

    /** 解析 PEM 里的全部证书；顺带就是一次格式校验——解析不了的内容不该进库。 */
    public static List<X509Certificate> parseCertificates(String pem)
            throws GeneralSecurityException {
        if (!isNotBlank(pem)) {
            return List.of();
        }
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        List<X509Certificate> out = new ArrayList<>();
        for (Certificate c : cf.generateCertificates(
                new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)))) {
            out.add((X509Certificate) c);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("PEM 中没有解析到任何 X.509 证书");
        }
        return out;
    }

    /**
     * 私钥 PEM → PKCS8 DER。
     *
     * <p>PKCS1（{@code BEGIN RSA PRIVATE KEY}）必须支持：MySQL 自带的
     * {@code mysql_ssl_rsa_setup} 和 {@code openssl genrsa} 产出的都是它，而 JDK 的
     * {@link PKCS8EncodedKeySpec} 只吃 PKCS8。转换本身很简单——PKCS8 就是在 PKCS1 的
     * {@code RSAPrivateKey} 外面套一层
     * {@code SEQUENCE { INTEGER 0, AlgorithmIdentifier(rsaEncryption, NULL), OCTET STRING }}，
     * 手写 ASN.1 即可，不必为此引入 BouncyCastle（本仓库在混包上已经吃过亏）。
     */
    public static byte[] toPkcs8Der(String keyPem) {
        String trimmed = keyPem.trim();
        if (trimmed.startsWith(ENCRYPTED_BEGIN)) {
            throw new IllegalArgumentException(
                    "不支持带口令的私钥。请先解密后再上传：\n"
                            + "  openssl pkcs8 -topk8 -nocrypt -in client-key.pem -out client-key-plain.pem");
        }
        byte[] der = base64Body(trimmed);
        if (trimmed.startsWith(PKCS1_BEGIN)) {
            return wrapPkcs1AsPkcs8(der);
        }
        return der;
    }

    private static byte[] base64Body(String pem) {
        StringBuilder b64 = new StringBuilder();
        for (String line : pem.split("\\R")) {
            String s = line.trim();
            if (s.isEmpty() || s.startsWith("-----")) {
                continue;
            }
            b64.append(s);
        }
        if (b64.length() == 0) {
            throw new IllegalArgumentException("PEM 内容为空或格式不正确（未找到 base64 主体）");
        }
        try {
            return Base64.getDecoder().decode(b64.toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("PEM base64 主体无法解码: " + e.getMessage(), e);
        }
    }

    /** rsaEncryption OID 1.2.840.113549.1.1.1 的 AlgorithmIdentifier（含 NULL 参数），DER 常量。 */
    private static final byte[] RSA_ALG_ID = {
            0x30, 0x0d,
            0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01,
            0x05, 0x00
    };

    private static byte[] wrapPkcs1AsPkcs8(byte[] pkcs1) {
        byte[] version = {0x02, 0x01, 0x00};                       // INTEGER 0
        byte[] octet = derTagged(0x04, pkcs1);                     // OCTET STRING { pkcs1 }
        byte[] body = concat(version, RSA_ALG_ID, octet);
        return derTagged(0x30, body);                              // SEQUENCE { ... }
    }

    /** DER 的 tag + 长度（短式/长式）+ 内容。 */
    private static byte[] derTagged(int tag, byte[] content) {
        byte[] len = derLength(content.length);
        byte[] out = new byte[1 + len.length + content.length];
        out[0] = (byte) tag;
        System.arraycopy(len, 0, out, 1, len.length);
        System.arraycopy(content, 0, out, 1 + len.length, content.length);
        return out;
    }

    private static byte[] derLength(int n) {
        if (n < 0x80) {
            return new byte[]{(byte) n};
        }
        int bytes = 0;
        for (int v = n; v > 0; v >>= 8) {
            bytes++;
        }
        byte[] out = new byte[1 + bytes];
        out[0] = (byte) (0x80 | bytes);
        for (int i = bytes; i >= 1; i--) {
            out[i] = (byte) (n & 0xff);
            n >>= 8;
        }
        return out;
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }

    // ==================== 落盘 ====================

    private static KeyStore emptyPkcs12() throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        return ks;
    }

    private static void storeTo(KeyStore ks, Path path, String password)
            throws GeneralSecurityException, IOException {
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, password.toCharArray());
        }
        restrictToOwner(path);
    }

    private static void writeOwnerOnly(Path path, byte[] content) throws IOException {
        if (content.length > MAX_PEM_BYTES) {
            throw new IllegalArgumentException(
                    "证书文件过大（" + content.length + " 字节，上限 " + MAX_PEM_BYTES + "）");
        }
        Files.write(path, content);
        restrictToOwner(path);
    }

    /**
     * 私钥落到 agent 主机上，权限必须收紧到仅属主。非 POSIX 文件系统（Windows）静默跳过——
     * 这条链路的部署目标是 Linux/macOS。
     */
    private static void restrictToOwner(Path path) {
        try {
            boolean isDir = Files.isDirectory(path);
            Set<PosixFilePermission> perms = isDir
                    ? Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                             PosixFilePermission.OWNER_EXECUTE)
                    : Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(path, perms);
        } catch (UnsupportedOperationException | IOException ignored) {
            // 非 POSIX 文件系统：跳过
        }
    }

    private static String randomPassword() {
        byte[] raw = new byte[24];
        new SecureRandom().nextBytes(raw);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    /** 证书指纹（SHA-256，冒号分隔大写十六进制）——UI 展示与去重用。 */
    public static String fingerprint(X509Certificate cert) throws GeneralSecurityException {
        byte[] d = java.security.MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
        StringBuilder sb = new StringBuilder(d.length * 3);
        for (byte b : d) {
            if (sb.length() > 0) {
                sb.append(':');
            }
            sb.append(String.format(Locale.ROOT, "%02X", b));
        }
        return sb.toString();
    }
}
