package com.migration.thl.crypto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

/**
 * THL 文件加密服务
 *
 * <p>对 capture→extract→increment 链路中的 THL 文件进行 AES-GCM 加密，
 * 保证数据在传输和存储过程中的机密性和完整性。
 *
 * <p>特性：
 * <ul>
 *   <li>AES-256-GCM 对称加密，提供机密性和完整性保护</li>
 *   <li>每条 THL 事件独立加密，支持流式读写</li>
 *   <li>密钥从配置的口令派生（PBKDF2 风格的 SHA-256 派生）</li>
 *   <li>文件头包含魔数和版本号，便于识别加密文件</li>
 *   <li>每条记录包含 12 字节 IV + 密文 + 16 字节 GCM Tag</li>
 * </ul>
 *
 * <p>文件格式：
 * <pre>
 * +-------------------+-----------------------------+
 * | Magic (4 bytes)   | "THLE"                      |
 * | Version (2 bytes) | 0x0001                      |
 * | Record 1          | IV(12) + Len(4) + Cipher(N) |
 * | Record 2          | IV(12) + Len(4) + Cipher(N) |
 * | ...               | ...                         |
 * +-------------------+-----------------------------+
 * </pre>
 */
public class ThlEncryptionService {
    private static final Logger logger = LoggerFactory.getLogger(ThlEncryptionService.class);

    private static final byte[] MAGIC = "THLE".getBytes(StandardCharsets.US_ASCII);
    /**
     * 加密文件格式版本。
     * <p>1 = 记录 payload 是 Java 原生序列化；2 = payload 走 {@link com.migration.thl.ThlCodec}。
     * 读侧按这个字段分派，因此存量 v1 加密文件仍然读得开。
     */
    private static final short VERSION = 2;
    /** payload 仍是 Java 原生序列化的历史版本。 */
    public static final short VERSION_JAVA_SER = 1;
    public static final short VERSION_CODEC = 2;
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 16; // bytes
    private static final int GCM_IV_LENGTH = 12;  // bytes
    private static final int KEY_LENGTH = 32;     // AES-256
    /** OWASP 对 PBKDF2-HMAC-SHA256 的推荐下限，与 CredentialCipher 一致。 */
    static final int PBKDF2_ITERATIONS = 310_000;

    private final boolean enabled;
    private final SecretKey secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * @param props 任务配置。{@code thl.encryption.enabled} 打开加密；
     *              {@code thl.encryption.password} 可选，缺省时从主密钥派生。
     */
    public ThlEncryptionService(Properties props) {
        this.enabled = Boolean.parseBoolean(props.getProperty("thl.encryption.enabled", "false"));
        String password = props.getProperty("thl.encryption.password", "");

        if (enabled) {
            if (password == null || password.isEmpty()) {
                // 原来这里回退到一个**写在源码里的公开常量**
                // （"default-thl-encryption-key-please-change"）——任何人都能拿它解开
                // 所谓"已加密"的 THL，比不加密更糟：运维以为数据保护住了。
                //
                // 改为从主密钥派生。主密钥（SYNCTASK_MASTER_KEY）本来就是 agent 与各
                // 子进程共享的那一把，用它意味着开启 THL 加密不需要再管第二个秘密；
                // 主密钥也没配时才拒绝启动——那种情况下无论如何都产不出真正的密文。
                password = System.getenv("SYNCTASK_MASTER_KEY");
                if (password == null || password.isEmpty()) {
                    password = System.getProperty("synctask.master.key", "");
                }
                if (password == null || password.isEmpty()) {
                    throw new IllegalStateException(
                            "thl.encryption.enabled=true 但既未配置 thl.encryption.password，"
                                    + "也没有 SYNCTASK_MASTER_KEY。拒绝用可预测的密钥加密——"
                                    + "那样产出的密文任何人都能解开。");
                }
                logger.info("THL 加密未单独配置口令，改用主密钥派生");
            }
            this.secretKey = deriveKey(password);
            logger.info("ThlEncryptionService 初始化 | enabled=true | algorithm=AES-256-GCM");
            // 覆盖范围必须说清楚：本服务只加密 .thl。数据在盘上要经过两跳——
            // capture 先写 .cap（源端原始事件，**明文**），extract 才产出 .thl。
            // 开了这个开关只保护了第二跳；同一批业务数据在 .cap 里仍是明文。
            // 不打这条日志，运维会以为"落盘加密已开启"，那比不加密更危险。
            logger.warn("注意：THL 加密只覆盖 .thl；capture 产出的 .cap 仍是明文。"
                    + "任务目录已收到 0700（同机其它用户读不到），但备份/磁盘镜像/共享存储"
                    + "仍会带走 .cap 明文——这些场景请依赖磁盘加密或卷加密。");
        } else {
            this.secretKey = null;
            logger.info("ThlEncryptionService 初始化 | enabled=false");
        }
    }

    /**
     * 从口令派生 AES 密钥。
     *
     * <p>PBKDF2-HMAC-SHA256 而不是裸 SHA-256：口令熵不足时裸哈希可直接字典攻击。
     * 盐取固定常量而不是随机——密钥要能跨进程（capture/extract/increment 各是独立
     * 子进程）重新派生出同一把，随密文存盐则每读一条记录都要重跑 31 万轮迭代。
     * 与 {@code CredentialCipher} 同一取舍。
     *
     * <p>派生只在进程启动后首次用到时发生一次（约 200ms）。
     */
    private SecretKey deriveKey(String password) {
        try {
            byte[] salt = MessageDigest.getInstance("SHA-256")
                    .digest("synctask-thl-encryption-v1".getBytes(StandardCharsets.UTF_8));
            javax.crypto.spec.PBEKeySpec spec = new javax.crypto.spec.PBEKeySpec(
                    password.toCharArray(), salt, PBKDF2_ITERATIONS, KEY_LENGTH * 8);
            byte[] keyBytes = javax.crypto.SecretKeyFactory
                    .getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
            spec.clearPassword();
            return new SecretKeySpec(keyBytes, ALGORITHM);
        } catch (Exception e) {
            throw new RuntimeException("密钥派生失败", e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** 加密单条记录（字节数组） */
    public byte[] encryptRecord(byte[] plaintext) {
        if (!enabled) return plaintext;
        if (plaintext == null) return null;
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
            byte[] ciphertext = cipher.doFinal(plaintext);

            // 输出格式：IV(12) + Len(4) + Cipher(N)
            ByteBuffer buffer = ByteBuffer.allocate(GCM_IV_LENGTH + 4 + ciphertext.length);
            buffer.put(iv);
            buffer.putInt(ciphertext.length);
            buffer.put(ciphertext);
            return buffer.array();
        } catch (Exception e) {
            throw new RuntimeException("加密记录失败", e);
        }
    }

    /** 解密单条记录 */
    public byte[] decryptRecord(byte[] encrypted) {
        if (!enabled) return encrypted;
        if (encrypted == null || encrypted.length < GCM_IV_LENGTH + 4) {
            throw new IllegalArgumentException("加密记录格式无效");
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(encrypted);
            byte[] iv = new byte[GCM_IV_LENGTH];
            buffer.get(iv);
            int cipherLen = buffer.getInt();
            byte[] ciphertext = new byte[cipherLen];
            buffer.get(ciphertext);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new RuntimeException("解密记录失败", e);
        }
    }

    /** 写入加密文件头 */
    public void writeHeader(OutputStream out) throws IOException {
        if (!enabled) return;
        out.write(MAGIC);
        out.write((VERSION >> 8) & 0xFF);
        out.write(VERSION & 0xFF);
    }

    /** 读取并验证文件头。 */
    public boolean verifyHeader(InputStream in) throws IOException {
        return readHeaderVersion(in) >= 0;
    }

    /**
     * 读取文件头并返回格式版本；magic 不符或版本不认识返回 -1。
     *
     * <p>读侧要靠这个版本号决定 payload 用哪套编解码——不能只判"等于当前 VERSION"，
     * 那样一升版本，所有存量加密文件立刻读不开。
     */
    public short readHeaderVersion(InputStream in) throws IOException {
        if (!enabled) return VERSION;
        byte[] magic = new byte[MAGIC.length];
        int read = in.read(magic);
        if (read != MAGIC.length || !Arrays.equals(magic, MAGIC)) {
            return -1;
        }
        int v1 = in.read();
        int v2 = in.read();
        if (v1 < 0 || v2 < 0) return -1;
        short version = (short) ((v1 << 8) | v2);
        if (version != VERSION_JAVA_SER && version != VERSION_CODEC) {
            return -1;
        }
        return version;
    }

    /** 判断文件是否为加密格式 */
    public boolean isEncryptedFile(File file) {
        if (!enabled || file == null || !file.exists()) return false;
        try (InputStream in = new FileInputStream(file)) {
            byte[] magic = new byte[MAGIC.length];
            int read = in.read(magic);
            return read == MAGIC.length && Arrays.equals(magic, MAGIC);
        } catch (IOException e) {
            return false;
        }
    }

    /** 加密整个文件 */
    public void encryptFile(File plainFile, File encryptedFile) throws IOException {
        if (!enabled) {
            Files.copy(plainFile.toPath(), encryptedFile.toPath());
            return;
        }
        try (InputStream in = new FileInputStream(plainFile);
             OutputStream out = new FileOutputStream(encryptedFile)) {
            writeHeader(out);
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) > 0) {
                byte[] chunk = Arrays.copyOf(buffer, len);
                byte[] encrypted = encryptRecord(chunk);
                // 写入记录长度
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                lenBuf.putInt(encrypted.length);
                out.write(lenBuf.array());
                out.write(encrypted);
            }
        }
        logger.info("文件已加密: {} -> {}", plainFile.getName(), encryptedFile.getName());
    }

    /** 解密整个文件 */
    public void decryptFile(File encryptedFile, File plainFile) throws IOException {
        if (!enabled) {
            Files.copy(encryptedFile.toPath(), plainFile.toPath());
            return;
        }
        try (InputStream in = new FileInputStream(encryptedFile);
             OutputStream out = new FileOutputStream(plainFile)) {
            if (!verifyHeader(in)) {
                throw new IOException("无效的加密文件头");
            }
            byte[] lenBuf = new byte[4];
            while (in.read(lenBuf) == 4) {
                int len = ByteBuffer.wrap(lenBuf).getInt();
                byte[] encrypted = new byte[len];
                int read = 0;
                while (read < len) {
                    int r = in.read(encrypted, read, len - read);
                    if (r < 0) break;
                    read += r;
                }
                byte[] decrypted = decryptRecord(encrypted);
                out.write(decrypted);
            }
        }
        logger.info("文件已解密: {} -> {}", encryptedFile.getName(), plainFile.getName());
    }
}
