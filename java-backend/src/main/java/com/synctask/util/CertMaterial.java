package com.synctask.util;

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
import java.security.MessageDigest;
import java.security.PrivateKey;
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
 * 证书归一：把用户上传的 PEM 转成各驱动各自要的形态。
 *
 * <p>这是数据面 {@code com.migration.common.ssl.CertBundle} 的镜像——后端刻意不依赖
 * migration-common（会把 mysql-connector / ojdbc / HikariCP / logback 拽进 Spring BOM 的
 * 版本仲裁里，本仓库在 migration-mongo 上吃过一次混包的亏），对 {@code CredentialCipher}、
 * {@code KafkaSecurity}、{@code JdbcSslOptions} 采用的也是同一处置。
 * 两侧的行为一致由 {@code CertMaterialMirrorTest} 守住。
 *
 * <p><b>为什么必须归一</b>：同一份证书，每种客户端要的形态都不一样——
 * MySQL/Kafka/Oracle 要 keystore（p12），PostgreSQL 要 PEM 且私钥必须是 PKCS8 <b>DER</b>
 * （{@code .pk8}，给 PKCS1 PEM 会报 {@code Unsupported key type}，与"证书不对"完全不像），
 * Mongo/Redis/ES/binlog 客户端只认 {@code SSLContext} 对象。让用户按链路自备五种格式不现实：
 * 他手上只有 openssl 或数据库自带工具产出的 PEM。
 *
 * <p>转换全部走 JDK 自带 API，不 shell-out 调 openssl——后端与 agent 主机都不保证装了它。
 */
public final class CertMaterial {

    public static final String CA_PEM = "ca.pem";
    public static final String CLIENT_CERT_PEM = "client-cert.pem";
    public static final String CLIENT_KEY_PK8 = "client-key.pk8";
    public static final String TRUSTSTORE_P12 = "truststore.p12";
    public static final String KEYSTORE_P12 = "keystore.p12";

    /** 证书文件大小上限：正常 PEM 都是几 KB，64KB 已经很宽。 */
    public static final int MAX_PEM_BYTES = 64 * 1024;

    private static final String PKCS1_BEGIN = "-----BEGIN RSA PRIVATE KEY-----";
    private static final String ENCRYPTED_BEGIN = "-----BEGIN ENCRYPTED PRIVATE KEY-----";

    private final Path dir;
    private final String storePassword;

    private CertMaterial(Path dir, String storePassword) {
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

    public boolean has(String name) {
        return Files.isRegularFile(path(name));
    }

    /** MySQL / Oracle / Kafka 用的信任库路径；没有 CA 时返回 null（REQUIRED 档位不需要）。 */
    public String truststore() {
        return has(TRUSTSTORE_P12) ? path(TRUSTSTORE_P12).toString() : null;
    }

    /** PostgreSQL 用的是 PEM 形态的 CA，不是 p12。 */
    public String caPem() {
        return has(CA_PEM) ? path(CA_PEM).toString() : null;
    }

    public String clientCertPem() {
        return has(CLIENT_CERT_PEM) ? path(CLIENT_CERT_PEM).toString() : null;
    }

    public String clientKeyPk8() {
        return has(CLIENT_KEY_PK8) ? path(CLIENT_KEY_PK8).toString() : null;
    }

    public String keystore() {
        return has(KEYSTORE_P12) ? path(KEYSTORE_P12).toString() : null;
    }

    // ==================== 归一 ====================

    /**
     * 把 PEM 材料写成一个证书包目录。
     *
     * @param storePassword keystore/truststore 口令。由调用方提供（与证书一起落库），
     *                      而不是每次随机——agent 侧要用同一个口令读回同一批文件。
     */
    public static CertMaterial materialize(Path dir, String caPem, String clientPem,
                                           String clientKey, String storePassword)
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
            storeTo(ts, dir.resolve(TRUSTSTORE_P12), storePassword);
        }

        if (isNotBlank(clientPem) && isNotBlank(clientKey)) {
            writeOwnerOnly(dir.resolve(CLIENT_CERT_PEM), clientPem.getBytes(StandardCharsets.UTF_8));
            byte[] pkcs8 = toPkcs8Der(clientKey);
            writeOwnerOnly(dir.resolve(CLIENT_KEY_PK8), pkcs8);

            PrivateKey key = KeyFactory.getInstance("RSA")
                    .generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
            List<X509Certificate> chain = parseCertificates(clientPem);
            KeyStore ks = emptyPkcs12();
            ks.setKeyEntry("client", key, storePassword.toCharArray(),
                           chain.toArray(new Certificate[0]));
            storeTo(ks, dir.resolve(KEYSTORE_P12), storePassword);
        } else if (isNotBlank(clientPem) != isNotBlank(clientKey)) {
            throw new IllegalArgumentException(
                    "客户端证书与私钥必须成对提供（双向认证 mTLS）；只给一个无法建立连接");
        }

        return new CertMaterial(dir, storePassword);
    }

    /** 已归一好的目录。 */
    public static CertMaterial at(Path dir, String storePassword) {
        return new CertMaterial(dir, storePassword);
    }

    // ==================== PEM 解析 ====================

    /** 解析 PEM 里的全部证书；顺带就是一次格式校验——解析不了的内容不该进库。 */
    public static List<X509Certificate> parseCertificates(String pem) throws GeneralSecurityException {
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
     * {@code mysql_ssl_rsa_setup} 与 {@code openssl genrsa} 产出的都是它，而 JDK 的
     * {@link PKCS8EncodedKeySpec} 只吃 PKCS8。转换就是在 PKCS1 的 {@code RSAPrivateKey}
     * 外面套一层 {@code SEQUENCE { INTEGER 0, AlgorithmIdentifier(rsaEncryption,NULL),
     * OCTET STRING }}，手写 ASN.1 即可，不必为此引入 BouncyCastle。
     */
    public static byte[] toPkcs8Der(String keyPem) {
        String trimmed = keyPem.trim();
        if (trimmed.startsWith(ENCRYPTED_BEGIN)) {
            throw new IllegalArgumentException(
                    "不支持带口令的私钥。请先解密后再上传：\n"
                            + "  openssl pkcs8 -topk8 -nocrypt -in client-key.pem -out client-key-plain.pem");
        }
        byte[] der = base64Body(trimmed);
        return trimmed.startsWith(PKCS1_BEGIN) ? wrapPkcs1AsPkcs8(der) : der;
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
        byte[] version = {0x02, 0x01, 0x00};
        byte[] octet = derTagged(0x04, pkcs1);
        return derTagged(0x30, concat(version, RSA_ALG_ID, octet));
    }

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

    /** 私钥落盘权限收紧到仅属主。非 POSIX 文件系统静默跳过。 */
    private static void restrictToOwner(Path path) {
        try {
            Set<PosixFilePermission> perms = Files.isDirectory(path)
                    ? Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                             PosixFilePermission.OWNER_EXECUTE)
                    : Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(path, perms);
        } catch (UnsupportedOperationException | IOException ignored) {
            // 非 POSIX 文件系统：跳过
        }
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    /** 证书指纹（SHA-256，冒号分隔大写十六进制）。 */
    public static String fingerprint(X509Certificate cert) throws GeneralSecurityException {
        byte[] d = MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
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
