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
    @DisplayName("往返：加密后能解回原文，且密文带 ENC: 前缀、不等于原文")
    void roundTrip() {
        String plain = "mysql://root:rootpassword@localhost:33306";
        String enc = CredentialCipher.encrypt(plain);
        assertTrue(enc.startsWith(CredentialCipher.PREFIX), "密文应带 ENC: 前缀");
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
    @DisplayName("兼容旧明文：无 ENC: 前缀的历史值 decrypt 原样返回")
    void decryptLegacyPlaintext() {
        assertEquals("plainpwd", CredentialCipher.decrypt("plainpwd"));
        assertNull(CredentialCipher.decrypt(null));
    }

    @Test
    @DisplayName("decryptProperties：仅解密 ENC: 值，其余（含明文口令）保持不变")
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
}
