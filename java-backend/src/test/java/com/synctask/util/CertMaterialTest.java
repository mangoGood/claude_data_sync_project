package com.synctask.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CertMaterial} 是数据面 {@code com.migration.common.ssl.CertBundle} 的镜像
 * （后端刻意不依赖 migration-common）。镜像的风险是漂移，因此这里覆盖的是<b>两侧必须一致</b>
 * 的那几条契约：产物文件名、PKCS1→PKCS8 转换结果、落盘权限、成对校验。
 */
class CertMaterialTest {

    private static final String PASS = "test-store-pass";

    @Test
    @DisplayName("PKCS1 私钥转成的 PKCS8 能被 JDK 解析回同一把钥匙")
    void pkcs1ConvertsToUsablePkcs8() throws Exception {
        PrivateKey original = KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate();
        byte[] pkcs1 = extractPkcs1(original.getEncoded());
        String pem = "-----BEGIN RSA PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(pkcs1)
                + "\n-----END RSA PRIVATE KEY-----\n";

        byte[] pkcs8 = CertMaterial.toPkcs8Der(pem);
        PrivateKey roundTripped = KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(pkcs8));

        assertEquals(original, roundTripped);
        assertArrayEquals(original.getEncoded(), pkcs8);
    }

    @Test
    @DisplayName("PKCS8 原样通过")
    void pkcs8PassesThrough() throws Exception {
        PrivateKey key = KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate();
        String pem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        assertArrayEquals(key.getEncoded(), CertMaterial.toPkcs8Der(pem));
    }

    @Test
    @DisplayName("带口令私钥当场拒绝并给出转换命令")
    void encryptedKeyRejectedWithFix() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CertMaterial.toPkcs8Der(
                        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----"));
        assertTrue(e.getMessage().contains("openssl pkcs8 -topk8 -nocrypt"));
    }

    @Test
    @DisplayName("垃圾内容在入库时就报错，不留到建连时才炸")
    void garbageRejectedEarly() {
        assertThrows(IllegalArgumentException.class,
                () -> CertMaterial.toPkcs8Der("-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----"));
        assertThrows(Exception.class, () -> CertMaterial.parseCertificates("这不是证书"));
    }

    @Test
    @DisplayName("只有 CA：产出 ca.pem + truststore.p12，truststore 能读回")
    void caOnlyBundle(@TempDir Path tmp) throws Exception {
        String caPem = selfSignedPem(tmp, "ca");
        CertMaterial m = CertMaterial.materialize(tmp.resolve("b"), caPem, null, null, PASS);

        assertNotNull(m.caPem());
        assertNotNull(m.truststore());
        assertNull(m.keystore(), "没给客户端证书就不该有 keystore");

        KeyStore ts = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(m.truststore()))) {
            ts.load(in, PASS.toCharArray());
        }
        assertTrue(ts.size() > 0);
    }

    private static void assertNull(Object o, String msg) {
        org.junit.jupiter.api.Assertions.assertNull(o, msg);
    }

    @Test
    @DisplayName("mTLS：产出 client-key.pk8 与 keystore.p12（PG 要 pk8，MySQL 要 p12）")
    void mutualTlsBundle(@TempDir Path tmp) throws Exception {
        String caPem = selfSignedPem(tmp, "ca");
        String clientPem = selfSignedPem(tmp, "client");
        PrivateKey key = keyOf(tmp, "client");
        String keyPem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n";

        CertMaterial m = CertMaterial.materialize(tmp.resolve("mtls"), caPem, clientPem, keyPem, PASS);

        assertNotNull(m.clientKeyPk8());
        assertTrue(m.clientKeyPk8().endsWith(".pk8"), "PG 的 sslkey 必须是 .pk8");
        assertNotNull(m.keystore());
        assertNotNull(m.clientCertPem());

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(m.keystore()))) {
            ks.load(in, PASS.toCharArray());
        }
        assertNotNull(ks.getKey("client", PASS.toCharArray()));
    }

    @Test
    @DisplayName("客户端证书与私钥必须成对给")
    void clientCertWithoutKeyRejected(@TempDir Path tmp) throws Exception {
        String caPem = selfSignedPem(tmp, "ca");
        assertThrows(IllegalArgumentException.class,
                () -> CertMaterial.materialize(tmp.resolve("x"), caPem, selfSignedPem(tmp, "c"), null, PASS));
    }

    @Test
    @DisplayName("私钥材料落盘权限收到仅属主")
    void materialIsOwnerOnly(@TempDir Path tmp) throws Exception {
        CertMaterial m = CertMaterial.materialize(tmp.resolve("perm"), selfSignedPem(tmp, "ca"), null, null, PASS);
        var perms = Files.getPosixFilePermissions(Path.of(m.truststore()));
        assertEquals(2, perms.size(), "实得: " + perms);
        assertTrue(perms.contains(PosixFilePermission.OWNER_READ));
        assertFalse(perms.contains(PosixFilePermission.OTHERS_READ));
    }

    // ==================== 辅助 ====================

    private static Path keystoreFor(Path tmp, String alias) throws Exception {
        Path ks = tmp.resolve(alias + "-src.p12");
        if (Files.exists(ks)) {
            return ks;
        }
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Process p = new ProcessBuilder(keytool, "-genkeypair", "-alias", alias,
                "-keyalg", "RSA", "-keysize", "2048",
                "-dname", "CN=" + alias + ",O=synctask,C=CN", "-validity", "1",
                "-storetype", "PKCS12", "-keystore", ks.toString(),
                "-storepass", "changeit", "-keypass", "changeit")
                .redirectErrorStream(true).start();
        // 子进程 stdout 必须排空，否则会阻塞在写日志上
        try (var in = p.getInputStream()) {
            in.readAllBytes();
        }
        assertEquals(0, p.waitFor(), "keytool 生成测试证书失败");
        return ks;
    }

    private static String selfSignedPem(Path tmp, String alias) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystoreFor(tmp, alias))) {
            ks.load(in, "changeit".toCharArray());
        }
        var cert = (java.security.cert.X509Certificate) ks.getCertificate(alias);
        return "-----BEGIN CERTIFICATE-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(cert.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
    }

    private static PrivateKey keyOf(Path tmp, String alias) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystoreFor(tmp, alias))) {
            ks.load(in, "changeit".toCharArray());
        }
        return (PrivateKey) ks.getKey(alias, "changeit".toCharArray());
    }

    private static byte[] extractPkcs1(byte[] pkcs8) {
        int i = 1;
        i += lengthBytes(pkcs8, i);
        i += 3;
        i++;
        int algLen = lengthValue(pkcs8, i);
        i += lengthBytes(pkcs8, i) + algLen;
        i++;
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
