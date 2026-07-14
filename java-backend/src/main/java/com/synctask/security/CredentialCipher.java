package com.synctask.security;

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

/**
 * 凭证加密工具（AES-256-GCM），用于后端对落库的连接串/口令做静态加密，取代明文存储。
 *
 * <p>主密钥：环境变量 {@code SYNCTASK_MASTER_KEY} 或系统属性 {@code synctask.master.key}，
 * SHA-256 派生 32 字节 AES-256 密钥。未配置退化为内置开发默认（打印一次告警，生产务必注入）。
 * 与 agent/子进程侧的同名工具算法/格式一致、共用同一主密钥。
 *
 * <p>密文格式 {@code ENC:base64(iv[12] || ct+tag)}。encrypt 幂等（已 ENC: 前缀不重复加密），
 * decrypt 兼容旧数据（无前缀视为历史明文原样返回）。
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
                if (k == null) KEY = k = deriveKey(resolveMasterKey());
            }
        }
        return k;
    }

    private static String resolveMasterKey() {
        String mk = System.getenv("SYNCTASK_MASTER_KEY");
        if (mk == null || mk.isEmpty()) mk = System.getProperty("synctask.master.key", "");
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

    public static String encrypt(String plain) {
        if (plain == null || plain.isEmpty() || plain.startsWith(PREFIX)) return plain;
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

    public static String decrypt(String value) {
        if (value == null || !value.startsWith(PREFIX)) return value;
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
}
