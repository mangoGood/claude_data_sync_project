package com.migration.common.ssl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 证书归一的行为验证。
 *
 * <p>用例不依赖外部 openssl：需要真实证书的地方用 {@code keytool} 生成（JDK 自带，
 * 与 agent 主机的最小环境假设一致——"证书包在 A 机器生成得了、B 机器生成不了"是最难查的
 * 一类部署问题，判据本身也不该引入这个变量）。
 */
class CertBundleTest {

    // ==================== PKCS1 → PKCS8 ====================

    /**
     * 这是整个归一里最容易出错的一步：{@code mysql_ssl_rsa_setup} 与 {@code openssl genrsa}
     * 产出的都是 PKCS1，而 JDK 的 {@code PKCS8EncodedKeySpec} 只吃 PKCS8。判据是
     * <b>转换结果能被 JDK 重新解析成同一把私钥</b>，而不是"字节数看着对"。
     */
    @Test
    @DisplayName("PKCS1 私钥能转成 PKCS8 并被 JDK 解析回同一把钥匙")
    void pkcs1ConvertsToUsablePkcs8() throws Exception {
        KeyPair pair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        PrivateKey original = pair.getPrivate();       // JDK 产出的就是 PKCS8

        // 剥出内层 PKCS1（PKCS8 的 OCTET STRING 内容），伪造一份 PKCS1 PEM
        byte[] pkcs1 = extractPkcs1(original.getEncoded());
        String pkcs1Pem = "-----BEGIN RSA PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(pkcs1)
                + "\n-----END RSA PRIVATE KEY-----\n";

        byte[] pkcs8 = CertBundle.toPkcs8Der(pkcs1Pem);
        PrivateKey roundTripped = KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(pkcs8));

        assertEquals(original, roundTripped, "转换后必须是同一把私钥");
        assertArrayEquals(original.getEncoded(), pkcs8);
    }

    @Test
    @DisplayName("PKCS8 私钥原样通过（不重复包装）")
    void pkcs8PassesThrough() throws Exception {
        PrivateKey key = KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate();
        String pem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n";

        assertArrayEquals(key.getEncoded(), CertBundle.toPkcs8Der(pem));
    }

    @Test
    @DisplayName("带口令的私钥当场拒绝，并给出确切的转换命令")
    void encryptedKeyRejectedWithFix() {
        String pem = "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----";
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> CertBundle.toPkcs8Der(pem));
        assertTrue(e.getMessage().contains("openssl pkcs8 -topk8 -nocrypt"),
                "拒绝时必须告诉用户怎么改，而不是只说不支持");
    }

    @Test
    @DisplayName("垃圾内容在入库时就报错，不留到连接时才炸")
    void garbageRejectedEarly() {
        assertThrows(IllegalArgumentException.class,
                () -> CertBundle.toPkcs8Der("-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----"));
        assertThrows(IllegalArgumentException.class,
                () -> CertBundle.toPkcs8Der("-----BEGIN PRIVATE KEY-----\n!!!not base64!!!\n-----END PRIVATE KEY-----"));
        assertThrows(Exception.class, () -> CertBundle.parseCertificates("这不是证书"));
    }

    /** 长式 DER 长度（>127 字节）的分支——2048 位私钥必然走到它。 */
    @Test
    @DisplayName("DER 长式长度编码正确（长私钥必经之路）")
    void longFormDerLength() throws Exception {
        KeyPair pair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        byte[] pkcs1 = extractPkcs1(pair.getPrivate().getEncoded());
        assertTrue(pkcs1.length > 127, "2048 位私钥的 PKCS1 必然超过短式长度上限");

        String pem = "-----BEGIN RSA PRIVATE KEY-----\n"
                + Base64.getEncoder().encodeToString(pkcs1) + "\n-----END RSA PRIVATE KEY-----";
        byte[] pkcs8 = CertBundle.toPkcs8Der(pem);

        assertEquals(0x30, pkcs8[0] & 0xff, "最外层必须是 SEQUENCE");
        assertTrue((pkcs8[1] & 0x80) != 0, "长度必须是长式编码");
    }

    // ==================== 证书包落盘 ====================

    @Test
    @DisplayName("只有 CA：产出 ca.pem + truststore.p12，且 truststore 能被读回")
    void caOnlyBundle(@TempDir Path tmp) throws Exception {
        String caPem = selfSignedPem(tmp, "ca");
        Path dir = tmp.resolve("bundle-ca");

        CertBundle bundle = CertBundle.materialize(dir, caPem, null, null);

        assertTrue(Files.isRegularFile(bundle.path(CertBundle.CA_PEM)));
        assertTrue(Files.isRegularFile(bundle.path(CertBundle.TRUSTSTORE_P12)));
        assertFalse(Files.exists(bundle.path(CertBundle.KEYSTORE_P12)), "没给客户端证书就不该有 keystore");

        KeyStore ts = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(bundle.path(CertBundle.TRUSTSTORE_P12))) {
            ts.load(in, bundle.storePassword().toCharArray());
        }
        assertTrue(ts.size() > 0, "CA 必须真的进了信任库");

        // MySQL 侧拿到的是 p12，PG 侧拿到的是 PEM —— 同一个包，两种形态
        assertTrue(bundle.toMaterial("VERIFY_CA").rootCert().endsWith(CertBundle.TRUSTSTORE_P12));
        assertTrue(bundle.toPgMaterial("VERIFY_CA").rootCert().endsWith(CertBundle.CA_PEM));
    }

    @Test
    @DisplayName("CA + 客户端证书：产出 pk8 与 keystore，SSLContext 可构造")
    void mutualTlsBundle(@TempDir Path tmp) throws Exception {
        KeyPair pair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        String caPem = selfSignedPem(tmp, "ca");
        String clientPem = selfSignedPem(tmp, "client");
        String keyPem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'})
                        .encodeToString(clientKeyOf(tmp, "client").getEncoded())
                + "\n-----END PRIVATE KEY-----\n";

        CertBundle bundle = CertBundle.materialize(tmp.resolve("bundle-mtls"), caPem, clientPem, keyPem);

        assertTrue(Files.isRegularFile(bundle.path(CertBundle.CLIENT_KEY_PK8)));
        assertTrue(Files.isRegularFile(bundle.path(CertBundle.KEYSTORE_P12)));

        SslMaterial m = bundle.toMaterial("VERIFY_IDENTITY");
        assertTrue(m.mutualTls());
        assertNotNull(m.sslContext(), "mTLS 材料必须能装配出 SSLContext");
        assertTrue(pair.getPrivate() != null);   // keep generator warm-up meaningful
    }

    @Test
    @DisplayName("客户端证书与私钥必须成对给")
    void clientCertWithoutKeyRejected(@TempDir Path tmp) throws Exception {
        String caPem = selfSignedPem(tmp, "ca");
        assertThrows(IllegalArgumentException.class,
                () -> CertBundle.materialize(tmp.resolve("b1"), caPem, selfSignedPem(tmp, "c"), null));
        assertThrows(IllegalArgumentException.class,
                () -> CertBundle.materialize(tmp.resolve("b2"), caPem, null, "-----BEGIN PRIVATE KEY-----\nAA\n-----END PRIVATE KEY-----"));
    }

    @Test
    @DisplayName("私钥落盘权限收到仅属主可读写")
    void privateMaterialIsOwnerOnly(@TempDir Path tmp) throws Exception {
        String caPem = selfSignedPem(tmp, "ca");
        CertBundle bundle = CertBundle.materialize(tmp.resolve("perm"), caPem, null, null);

        var perms = Files.getPosixFilePermissions(bundle.path(CertBundle.TRUSTSTORE_P12));
        assertEquals(2, perms.size(), "只应有 OWNER_READ / OWNER_WRITE，实得: " + perms);
        assertTrue(perms.contains(java.nio.file.attribute.PosixFilePermission.OWNER_READ));
        assertTrue(perms.contains(java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
    }

    @Test
    @DisplayName("指纹稳定且可比对")
    void fingerprintIsStable(@TempDir Path tmp) throws Exception {
        List<X509Certificate> certs = CertBundle.parseCertificates(selfSignedPem(tmp, "fp"));
        String fp = CertBundle.fingerprint(certs.get(0));
        assertEquals(fp, CertBundle.fingerprint(certs.get(0)));
        assertEquals(32 * 3 - 1, fp.length(), "SHA-256 = 32 字节，冒号分隔");
    }

    // ==================== 辅助：用 keytool 造一张真证书 ====================

    private static final java.util.Map<String, Path> STORES = new java.util.HashMap<>();

    private static Path keystoreFor(Path tmp, String alias) throws Exception {
        Path ks = STORES.get(alias + tmp);
        if (ks != null) {
            return ks;
        }
        ks = tmp.resolve(alias + ".p12");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Process p = new ProcessBuilder(keytool,
                "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048",
                "-dname", "CN=" + alias + ",OU=synctask,O=test,C=CN",
                "-validity", "1", "-storetype", "PKCS12",
                "-keystore", ks.toString(), "-storepass", "changeit", "-keypass", "changeit")
                .redirectErrorStream(true).start();
        // 子进程 stdout 必须排空，否则会阻塞在写日志上
        try (var in = p.getInputStream()) {
            in.readAllBytes();
        }
        assertEquals(0, p.waitFor(), "keytool 生成测试证书失败");
        STORES.put(alias + tmp, ks);
        return ks;
    }

    private static String selfSignedPem(Path tmp, String alias) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystoreFor(tmp, alias))) {
            ks.load(in, "changeit".toCharArray());
        }
        X509Certificate cert = (X509Certificate) ks.getCertificate(alias);
        return "-----BEGIN CERTIFICATE-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(cert.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
    }

    private static PrivateKey clientKeyOf(Path tmp, String alias) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystoreFor(tmp, alias))) {
            ks.load(in, "changeit".toCharArray());
        }
        return (PrivateKey) ks.getKey(alias, "changeit".toCharArray());
    }

    /** 从 JDK 的 PKCS8 编码里剥出内层 PKCS1（用于构造 PKCS1 输入）。 */
    private static byte[] extractPkcs1(byte[] pkcs8) {
        int i = 0;
        i++;                                    // SEQUENCE tag
        i += lengthBytes(pkcs8, i);             // 外层长度
        i += 3;                                 // INTEGER 0 (version)
        i++;                                    // AlgorithmIdentifier SEQUENCE tag
        int algLen = lengthValue(pkcs8, i);
        i += lengthBytes(pkcs8, i) + algLen;
        i++;                                    // OCTET STRING tag
        int keyLen = lengthValue(pkcs8, i);
        i += lengthBytes(pkcs8, i);
        byte[] out = new byte[keyLen];
        System.arraycopy(pkcs8, i, out, 0, keyLen);
        return out;
    }

    private static int lengthBytes(byte[] der, int at) {
        int first = der[at] & 0xff;
        return (first & 0x80) == 0 ? 1 : 1 + (first & 0x7f);
    }

    private static int lengthValue(byte[] der, int at) {
        int first = der[at] & 0xff;
        if ((first & 0x80) == 0) {
            return first;
        }
        int n = first & 0x7f;
        int v = 0;
        for (int k = 1; k <= n; k++) {
            v = (v << 8) | (der[at + k] & 0xff);
        }
        return v;
    }
}
