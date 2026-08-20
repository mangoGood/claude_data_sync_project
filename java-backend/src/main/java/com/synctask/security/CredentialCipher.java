package com.synctask.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 凭证加密工具（AES-256-GCM），用于后端对落库的连接串/口令做静态加密，取代明文存储。
 *
 * <p>主密钥：环境变量 {@code SYNCTASK_MASTER_KEY} 或系统属性 {@code synctask.master.key}。
 * 未配置退化为内置开发默认（打印一次告警，生产务必注入）。
 * 与 agent/子进程侧的同名工具算法/格式一致、共用同一主密钥。
 *
 * <h3>两代密文格式</h3>
 * <pre>
 *   ENC2:base64( keyId[1] || iv[12] || ct+tag )   ← 当前，PBKDF2 派生
 *   ENC :base64(            iv[12] || ct+tag )   ← 历史，裸 SHA-256 派生，只解不再产
 * </pre>
 *
 * <p><b>为什么改</b>：老版本用 {@code SHA-256(主密钥)} 取前 32 字节做 AES 密钥——
 * 无盐、无迭代，主密钥熵不足时可直接字典攻击。更要命的是密文里<b>不带密钥标识</b>，
 * 于是"换一次主密钥"等于"库里全部存量凭证立刻不可解密"——轮转通道被堵死。
 *
 * <p><b>怎么改</b>：
 * <ul>
 *   <li>派生换 PBKDF2-HMAC-SHA256，{@value #PBKDF2_ITERATIONS} 轮。盐由 keyId 确定性导出，
 *       不需要额外存储；每条密文<b>不</b>重跑 PBKDF2——密钥按 keyId 缓存，
 *       派生只在进程启动后首次用到时发生一次（约 200ms）。</li>
 *   <li>密文头放 1 字节 keyId，解密按 keyId 选密钥。轮转期同时配
 *       {@code SYNCTASK_MASTER_KEY}（新，负责加密与解密）与
 *       {@code SYNCTASK_MASTER_KEY_PREV}（旧，只解密），存量密文照常读，
 *       新写入的自动用新密钥；等重写完存量即可摘掉 PREV。</li>
 *   <li>换 {@code ENC2:} 前缀而不是在 {@code ENC:} 里加版本字节：老密文的第一个字节
 *       是随机 IV，有 1/256 的概率撞上任何版本号，靠首字节区分两代格式必然出错。</li>
 * </ul>
 */
public final class CredentialCipher {

    private static final Logger logger = LoggerFactory.getLogger(CredentialCipher.class);

    /** 历史前缀。只解不产。 */
    public static final String PREFIX_LEGACY = "ENC:";
    /** 当前前缀。 */
    public static final String PREFIX = "ENC2:";

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int GCM_IV_LENGTH = 12;
    private static final int KEY_LENGTH = 32;
    /** OWASP 对 PBKDF2-HMAC-SHA256 的推荐下限。 */
    static final int PBKDF2_ITERATIONS = 310_000;
    private static final String DEV_DEFAULT_KEY = "synctask-dev-master-key-change-me";

    private static final SecureRandom RANDOM = new SecureRandom();
    /** keyId → 派生好的密钥。PBKDF2 很贵，每个 keyId 只算一次。 */
    private static final Map<Byte, SecretKey> KEY_CACHE = new ConcurrentHashMap<>();

    private CredentialCipher() {
    }

    // ---------------------------------------------------------------- 主密钥解析

    private static String env(String name, String sysProp) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) {
            v = System.getProperty(sysProp, "");
        }
        return v;
    }

    /** 当前密钥的 keyId（写入新密文用）。取值 1~127。 */
    static byte currentKeyId() {
        String raw = env("SYNCTASK_MASTER_KEY_ID", "synctask.master.key.id");
        return parseKeyId(raw, (byte) 1);
    }

    private static byte parseKeyId(String raw, byte dflt) {
        if (raw == null || raw.isEmpty()) {
            return dflt;
        }
        int v;
        try {
            v = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("SYNCTASK_MASTER_KEY_ID 必须是 1~127 的整数: " + raw);
        }
        if (v < 1 || v > 127) {
            throw new IllegalStateException("SYNCTASK_MASTER_KEY_ID 必须是 1~127: " + v);
        }
        return (byte) v;
    }

    private static String masterKeyFor(byte keyId) {
        byte prevId = parseKeyId(env("SYNCTASK_MASTER_KEY_PREV_ID", "synctask.master.key.prev.id"),
                (byte) -1);
        if (keyId == prevId) {
            String prev = env("SYNCTASK_MASTER_KEY_PREV", "synctask.master.key.prev");
            if (prev.isEmpty()) {
                throw new IllegalStateException(
                        "密文用 keyId=" + keyId + " 加密，但未配置 SYNCTASK_MASTER_KEY_PREV");
            }
            return prev;
        }
        if (keyId != currentKeyId()) {
            throw new IllegalStateException("密文的 keyId=" + keyId
                    + " 既不是当前密钥（" + currentKeyId() + "）也不是上一代密钥。"
                    + "轮转期请同时配置 SYNCTASK_MASTER_KEY_PREV 与 SYNCTASK_MASTER_KEY_PREV_ID。");
        }
        String mk = env("SYNCTASK_MASTER_KEY", "synctask.master.key");
        if (mk.isEmpty()) {
            logger.warn("未配置主密钥（SYNCTASK_MASTER_KEY / -Dsynctask.master.key），"
                    + "使用内置开发默认密钥。生产环境务必注入自己的主密钥！");
            mk = DEV_DEFAULT_KEY;
        }
        return mk;
    }

    // ---------------------------------------------------------------- 密钥派生

    private static SecretKey key(byte keyId) {
        return KEY_CACHE.computeIfAbsent(keyId, id -> derivePbkdf2(masterKeyFor(id), id));
    }

    /**
     * PBKDF2 派生。盐由 keyId 确定性导出——主密钥不是用户口令，盐的作用是让不同
     * 部署/不同代密钥的派生结果彼此独立（挡跨部署的彩虹表），不需要随每条密文存一份；
     * 随密文存盐反而会逼着每次解密都重跑 31 万轮迭代。
     */
    private static SecretKey derivePbkdf2(String masterKey, byte keyId) {
        try {
            byte[] salt = MessageDigest.getInstance("SHA-256")
                    .digest(("synctask-credential-v2|keyId=" + keyId).getBytes(StandardCharsets.UTF_8));
            PBEKeySpec spec = new PBEKeySpec(
                    masterKey.toCharArray(), salt, PBKDF2_ITERATIONS, KEY_LENGTH * 8);
            byte[] bytes = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(spec).getEncoded();
            spec.clearPassword();
            return new SecretKeySpec(bytes, "AES");
        } catch (Exception e) {
            throw new IllegalStateException("主密钥派生失败", e);
        }
    }

    /** 历史派生方式：裸 SHA-256，无盐无迭代。只用于解开 {@code ENC:} 老密文。 */
    private static SecretKey deriveLegacy() {
        try {
            String mk = env("SYNCTASK_MASTER_KEY", "synctask.master.key");
            if (mk.isEmpty()) {
                mk = DEV_DEFAULT_KEY;
            }
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(mk.getBytes(StandardCharsets.UTF_8));
            return new SecretKeySpec(Arrays.copyOf(hash, KEY_LENGTH), "AES");
        } catch (Exception e) {
            throw new IllegalStateException("主密钥派生失败（legacy）", e);
        }
    }

    // ---------------------------------------------------------------- 对外 API

    /** 值是否已经是密文（两代格式都算）。 */
    public static boolean isEncrypted(String value) {
        return value != null && (value.startsWith(PREFIX) || value.startsWith(PREFIX_LEGACY));
    }

    public static String encrypt(String plain) {
        if (plain == null || plain.isEmpty() || isEncrypted(plain)) {
            return plain;
        }
        try {
            byte keyId = currentKeyId();
            byte[] iv = new byte[GCM_IV_LENGTH];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key(keyId), new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ct = cipher.doFinal(plain.getBytes(StandardCharsets.UTF_8));
            ByteBuffer buf = ByteBuffer.allocate(1 + iv.length + ct.length);
            buf.put(keyId).put(iv).put(ct);
            return PREFIX + Base64.getEncoder().encodeToString(buf.array());
        } catch (Exception e) {
            throw new IllegalStateException("凭证加密失败", e);
        }
    }

    public static String decrypt(String value) {
        if (value == null) {
            return null;
        }
        if (value.startsWith(PREFIX)) {
            return decryptV2(value.substring(PREFIX.length()));
        }
        if (value.startsWith(PREFIX_LEGACY)) {
            return decryptLegacy(value.substring(PREFIX_LEGACY.length()));
        }
        // 无前缀：历史明文，原样返回
        return value;
    }

    private static String decryptV2(String b64) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(b64));
            byte keyId = buf.get();
            byte[] iv = new byte[GCM_IV_LENGTH];
            buf.get(iv);
            byte[] ct = new byte[buf.remaining()];
            buf.get(ct);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key(keyId), new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(ct), StandardCharsets.UTF_8);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("凭证解密失败（主密钥不匹配或密文损坏）", e);
        }
    }

    private static String decryptLegacy(String b64) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(b64));
            byte[] iv = new byte[GCM_IV_LENGTH];
            buf.get(iv);
            byte[] ct = new byte[buf.remaining()];
            buf.get(ct);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, deriveLegacy(), new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(ct), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("凭证解密失败（旧格式；主密钥不匹配或密文损坏）", e);
        }
    }

    /** 测试专用：清掉派生缓存，让改过的环境变量生效。 */
    static void resetKeyCacheForTest() {
        KEY_CACHE.clear();
    }
}
