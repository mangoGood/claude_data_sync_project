package com.migration.common.crypto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

/**
 * 凭证加密工具（AES-256-GCM）：对落盘/落库的敏感值（数据库口令等）做静态加密，取代明文存储。
 *
 * <p>主密钥注入：启动时从环境变量 {@code SYNCTASK_MASTER_KEY}（或系统属性 {@code synctask.master.key}）读取，
 * 经 SHA-256 派生 32 字节 AES-256 密钥。未配置时退化为内置开发默认密钥并打印一次告警——
 * 生产务必注入自己的主密钥。agent 与各子进程共享同一主密钥即可互相解密 config.properties 中的密文。
 *
 * <p>密文格式：{@code ENC:base64(iv[12] || ciphertext+tag)}。带 {@code ENC:} 前缀便于识别：
 * <ul>
 *   <li>{@link #encrypt(String)} 幂等：null/空/已是 ENC: 前缀原样返回，不重复加密；</li>
 *   <li>{@link #decrypt(String)} 兼容旧数据：无 ENC: 前缀视为历史明文，原样返回。</li>
 * </ul>
 */
public final class CredentialCipher {
    private static final Logger logger = LoggerFactory.getLogger(CredentialCipher.class);

    public static final String PREFIX = "ENC:";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int GCM_IV_LENGTH = 12;
    private static final int KEY_LENGTH = 32;
    private static final String DEV_DEFAULT_KEY = "synctask-dev-master-key-change-me";

    private static volatile SecretKey KEY;
    private static final SecureRandom RANDOM = new SecureRandom();

    private CredentialCipher() {}

    private static SecretKey key() {
        SecretKey k = KEY;
        if (k == null) {
            synchronized (CredentialCipher.class) {
                k = KEY;
                if (k == null) {
                    KEY = k = deriveKey(resolveMasterKey());
                }
            }
        }
        return k;
    }

    private static String resolveMasterKey() {
        String mk = System.getenv("SYNCTASK_MASTER_KEY");
        if (mk == null || mk.isEmpty()) {
            mk = System.getProperty("synctask.master.key", "");
        }
        if (mk == null || mk.isEmpty()) {
            logger.warn("未配置主密钥（SYNCTASK_MASTER_KEY / -Dsynctask.master.key），使用内置开发默认密钥。生产环境务必注入自己的主密钥！");
            mk = DEV_DEFAULT_KEY;
        }
        return mk;
    }

    private static SecretKey deriveKey(String masterKey) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(masterKey.getBytes(StandardCharsets.UTF_8));
            return new SecretKeySpec(Arrays.copyOf(hash, KEY_LENGTH), "AES");
        } catch (Exception e) {
            throw new IllegalStateException("主密钥派生失败", e);
        }
    }

    /** 加密（幂等）：null/空/已加密原样返回；否则返回 {@code ENC:...}。 */
    public static String encrypt(String plain) {
        if (plain == null || plain.isEmpty() || plain.startsWith(PREFIX)) {
            return plain;
        }
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key(), new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ct = cipher.doFinal(plain.getBytes(StandardCharsets.UTF_8));
            ByteBuffer buf = ByteBuffer.allocate(iv.length + ct.length);
            buf.put(iv).put(ct);
            return PREFIX + Base64.getEncoder().encodeToString(buf.array());
        } catch (Exception e) {
            throw new IllegalStateException("凭证加密失败", e);
        }
    }

    /** 解密（兼容旧明文）：无 {@code ENC:} 前缀原样返回；否则解密。 */
    public static String decrypt(String value) {
        if (value == null || !value.startsWith(PREFIX)) {
            return value;
        }
        try {
            byte[] all = Base64.getDecoder().decode(value.substring(PREFIX.length()));
            ByteBuffer buf = ByteBuffer.wrap(all);
            byte[] iv = new byte[GCM_IV_LENGTH];
            buf.get(iv);
            byte[] ct = new byte[buf.remaining()];
            buf.get(ct);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key(), new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(ct), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("凭证解密失败（主密钥不匹配或密文损坏）", e);
        }
    }

    public static boolean isEncrypted(String value) {
        return value != null && value.startsWith(PREFIX);
    }

    /**
     * 就地解密 Properties 中所有 {@code ENC:} 前缀的值。子进程 {@code props.load()} 后调用一次，
     * 之后所有 getProperty 读到的都是明文；非加密值（含历史明文 config）保持不变，向后兼容。
     */
    public static void decryptProperties(Properties props) {
        if (props == null) return;
        for (String name : props.stringPropertyNames()) {
            String v = props.getProperty(name);
            if (isEncrypted(v)) {
                props.setProperty(name, decrypt(v));
            }
        }
    }
}
