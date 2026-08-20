package com.migration.thl.crypto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * THL 加密的密钥来源判据。
 *
 * <p>原实现在"开了加密但没配口令"时回退到一个**写在源码里的公开常量**
 * （{@code default-thl-encryption-key-please-change}）。那样产出的密文任何人都能解开，
 * 而运维会以为落盘数据受保护了——比不加密更危险。
 */
class ThlEncryptionKeyTest {

    @Test
    @DisplayName("开了加密但既无口令也无主密钥 → 拒绝启动，不用可预测密钥")
    void refusesPredictableKey() {
        String saved = System.getProperty("synctask.master.key");
        System.clearProperty("synctask.master.key");
        try {
            Properties p = new Properties();
            p.setProperty("thl.encryption.enabled", "true");
            // 环境变量 SYNCTASK_MASTER_KEY 在测试进程里通常没有；若有则跳过该断言，
            // 因为那时行为本来就应该是"用主密钥派生"
            if (System.getenv("SYNCTASK_MASTER_KEY") == null
                    || System.getenv("SYNCTASK_MASTER_KEY").isEmpty()) {
                IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> new ThlEncryptionService(p));
                assertTrue(e.getMessage().contains("拒绝"), "错误信息应说明为什么拒绝: " + e.getMessage());
            }
        } finally {
            if (saved != null) System.setProperty("synctask.master.key", saved);
        }
    }

    @Test
    @DisplayName("配了口令即可用，且加解密往返正确")
    void roundTripWithExplicitPassword() {
        Properties p = new Properties();
        p.setProperty("thl.encryption.enabled", "true");
        p.setProperty("thl.encryption.password", "a-real-secret-not-in-source");
        ThlEncryptionService svc = new ThlEncryptionService(p);
        assertTrue(svc.isEnabled());

        byte[] plain = "INSERT INTO t VALUES (1)".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] enc = svc.encryptRecord(plain);
        assertNotEquals(new String(plain), new String(enc), "密文不应等于明文");
        assertArrayEquals(plain, svc.decryptRecord(enc));
    }

    @Test
    @DisplayName("没配口令时从主密钥派生（-Dsynctask.master.key）")
    void derivesFromMasterKey() {
        String saved = System.getProperty("synctask.master.key");
        System.setProperty("synctask.master.key", "master-key-for-test");
        try {
            Properties p = new Properties();
            p.setProperty("thl.encryption.enabled", "true");
            ThlEncryptionService svc = new ThlEncryptionService(p);
            assertTrue(svc.isEnabled());
            byte[] plain = "x".getBytes();
            assertArrayEquals(plain, svc.decryptRecord(svc.encryptRecord(plain)));
        } finally {
            if (saved == null) System.clearProperty("synctask.master.key");
            else System.setProperty("synctask.master.key", saved);
        }
    }

    @Test
    @DisplayName("不同口令派生出不同密钥——派生确实用上了口令")
    void differentPasswordsDifferentKeys() {
        Properties a = new Properties();
        a.setProperty("thl.encryption.enabled", "true");
        a.setProperty("thl.encryption.password", "pw-A");
        Properties b = new Properties();
        b.setProperty("thl.encryption.enabled", "true");
        b.setProperty("thl.encryption.password", "pw-B");

        byte[] enc = new ThlEncryptionService(a).encryptRecord("payload".getBytes());
        assertThrows(RuntimeException.class,
                () -> new ThlEncryptionService(b).decryptRecord(enc),
                "换口令后不应还能解开");
    }

    @Test
    @DisplayName("KDF 迭代轮数不低于 OWASP 推荐下限")
    void kdfStrength() {
        assertTrue(ThlEncryptionService.PBKDF2_ITERATIONS >= 310_000,
                "实际 " + ThlEncryptionService.PBKDF2_ITERATIONS);
    }

    @Test
    @DisplayName("默认关闭时不加密，行为与改造前一致")
    void disabledIsPassThrough() {
        ThlEncryptionService svc = new ThlEncryptionService(new Properties());
        assertFalse(svc.isEnabled());
        byte[] plain = "x".getBytes();
        assertArrayEquals(plain, svc.encryptRecord(plain));
    }
}
