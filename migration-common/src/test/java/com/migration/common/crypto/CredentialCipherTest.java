package com.migration.common.crypto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link CredentialCipher} 单元测试：往返、幂等、旧明文兼容、Properties 批量解密。
 * （未设 SYNCTASK_MASTER_KEY 时用内置开发默认密钥，测试自洽。）
 */
@DisplayName("CredentialCipher 凭证加解密")
class CredentialCipherTest {

    @Test
    @DisplayName("往返：加密后能解回原文，且密文带当前前缀、不等于原文")
    void roundTrip() {
        String plain = "mysql://root:rootpassword@localhost:33306";
        String enc = CredentialCipher.encrypt(plain);
        assertTrue(enc.startsWith(CredentialCipher.PREFIX), "密文应带当前前缀 " + CredentialCipher.PREFIX);
        assertNotEquals(plain, enc);
        assertEquals(plain, CredentialCipher.decrypt(enc));
    }

    @Test
    @DisplayName("随机 IV：同一明文两次加密密文不同，但都能解回")
    void nondeterministic() {
        String plain = "rootpassword";
        String a = CredentialCipher.encrypt(plain);
        String b = CredentialCipher.encrypt(plain);
        assertNotEquals(a, b, "随机 IV 应使两次密文不同");
        assertEquals(plain, CredentialCipher.decrypt(a));
        assertEquals(plain, CredentialCipher.decrypt(b));
    }

    @Test
    @DisplayName("幂等：已加密值再 encrypt 不二次加密；null/空原样返回")
    void encryptIdempotent() {
        String enc = CredentialCipher.encrypt("secret");
        assertEquals(enc, CredentialCipher.encrypt(enc));
        assertNull(CredentialCipher.encrypt(null));
        assertEquals("", CredentialCipher.encrypt(""));
    }

    @Test
    @DisplayName("兼容旧明文：无前缀的历史值 decrypt 原样返回")
    void decryptLegacyPlaintext() {
        assertEquals("plainpwd", CredentialCipher.decrypt("plainpwd"));
        assertNull(CredentialCipher.decrypt(null));
    }

    @Test
    @DisplayName("decryptProperties：仅解密密文值，其余（含明文口令）保持不变")
    void decryptPropertiesInPlace() {
        Properties p = new Properties();
        p.setProperty("source.db.password", CredentialCipher.encrypt("s3cr3t"));
        p.setProperty("target.db.password", "legacy-plain");   // 历史明文
        p.setProperty("source.db.host", "localhost");
        CredentialCipher.decryptProperties(p);
        assertEquals("s3cr3t", p.getProperty("source.db.password"));
        assertEquals("legacy-plain", p.getProperty("target.db.password"));
        assertEquals("localhost", p.getProperty("source.db.host"));
    }

    @Test
    @DisplayName("篡改密文触发 GCM 校验失败")
    void tamperDetection() {
        String enc = CredentialCipher.encrypt("payload");
        // 可靠篡改：解出 base64 字节、翻转一个密文字节（越过 12 字节 IV，落在 ciphertext+tag 区）、
        // 再编码回 ENC: 密文。这样必然改变密文，GCM tag 校验必失败。
        // （旧写法改 base64 末位 + 补 '='，在填充边界偶尔解出相同字节导致检测不到，是 flaky 根因。）
        String prefix = CredentialCipher.PREFIX;
        byte[] raw = java.util.Base64.getDecoder().decode(enc.substring(prefix.length()));
        int idx = raw.length - 1; // 落在 GCM tag 上，翻转必被检测
        raw[idx] ^= 0x01;
        String tampered = prefix + java.util.Base64.getEncoder().encodeToString(raw);
        assertThrows(RuntimeException.class, () -> CredentialCipher.decrypt(tampered));
    }

    @Test
    @DisplayName("向后兼容：ENC: 老密文（裸 SHA-256 派生）仍能解开")
    void decryptsLegacyCiphertext() throws Exception {
        // 用历史算法手工造一条 ENC: 密文：SHA-256(主密钥) 前 32 字节 + AES-GCM，
        // 格式 ENC:base64(iv[12] || ct+tag)。库里与 files/*/config.properties 里
        // 存量全是这个形状，解不开就等于所有历史任务的凭证全部作废。
        String plain = "legacy-secret";
        byte[] key = java.security.MessageDigest.getInstance("SHA-256")
                .digest("synctask-dev-master-key-change-me".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        byte[] iv = new byte[12];
        new java.security.SecureRandom().nextBytes(iv);
        javax.crypto.Cipher c = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
        c.init(javax.crypto.Cipher.ENCRYPT_MODE,
                new javax.crypto.spec.SecretKeySpec(java.util.Arrays.copyOf(key, 32), "AES"),
                new javax.crypto.spec.GCMParameterSpec(128, iv));
        byte[] ct = c.doFinal(plain.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        byte[] all = new byte[iv.length + ct.length];
        System.arraycopy(iv, 0, all, 0, iv.length);
        System.arraycopy(ct, 0, all, iv.length, ct.length);
        String legacy = CredentialCipher.PREFIX_LEGACY + java.util.Base64.getEncoder().encodeToString(all);

        assertEquals(plain, CredentialCipher.decrypt(legacy), "老密文必须仍可解");
        assertTrue(CredentialCipher.isEncrypted(legacy), "老前缀也算已加密");
        assertEquals(legacy, CredentialCipher.encrypt(legacy), "老密文不应被二次加密");
    }

    @Test
    @DisplayName("新密文携带 keyId，是轮转的前提")
    void newCiphertextCarriesKeyId() {
        String enc = CredentialCipher.encrypt("x");
        byte[] raw = java.util.Base64.getDecoder()
                .decode(enc.substring(CredentialCipher.PREFIX.length()));
        assertEquals(1, raw[0], "默认 keyId 应为 1");
        // keyId(1) + iv(12) + ct(1) + tag(16)
        assertEquals(1 + 12 + 1 + 16, raw.length, "密文布局应为 keyId||iv||ct+tag");
    }

    @Test
    @DisplayName("两代前缀不会互相误认——老密文首字节是随机 IV，不能靠首字节区分版本")
    void prefixesAreDistinct() {
        assertNotEquals(CredentialCipher.PREFIX, CredentialCipher.PREFIX_LEGACY);
        assertFalse(CredentialCipher.PREFIX_LEGACY.startsWith(CredentialCipher.PREFIX));
        // 关键：新前缀必须以老前缀无法匹配的方式开头，否则 startsWith 判断会串
        assertTrue(CredentialCipher.PREFIX.startsWith("ENC"));
        assertNotEquals(CredentialCipher.PREFIX.charAt(3), ':');
    }

    @Test
    @DisplayName("PBKDF2 迭代轮数不低于 OWASP 推荐下限")
    void kdfStrength() {
        assertTrue(CredentialCipher.PBKDF2_ITERATIONS >= 310_000,
                "PBKDF2-HMAC-SHA256 至少 310,000 轮，实际 " + CredentialCipher.PBKDF2_ITERATIONS);
    }
}
