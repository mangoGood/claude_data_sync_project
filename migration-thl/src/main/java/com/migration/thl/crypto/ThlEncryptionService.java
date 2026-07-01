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
    private static final short VERSION = 1;
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 16; // bytes
    private static final int GCM_IV_LENGTH = 12;  // bytes
    private static final int KEY_LENGTH = 32;     // AES-256

    private final boolean enabled;
    private final SecretKey secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    public ThlEncryptionService(Properties props) {
        this.enabled = Boolean.parseBoolean(props.getProperty("thl.encryption.enabled", "false"));
        String password = props.getProperty("thl.encryption.password", "");

        if (enabled) {
            if (password == null || password.isEmpty()) {
                logger.warn("THL 加密已启用但未配置密码，使用默认密码（仅限测试环境）");
                password = "default-thl-encryption-key-please-change";
            }
            this.secretKey = deriveKey(password);
            logger.info("ThlEncryptionService 初始化 | enabled=true | algorithm=AES-256-GCM");
        } else {
            this.secretKey = null;
            logger.info("ThlEncryptionService 初始化 | enabled=false");
        }
    }

    /** 从口令派生 AES 密钥 */
    private SecretKey deriveKey(String password) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] hash = sha256.digest(password.getBytes(StandardCharsets.UTF_8));
            // 取前 32 字节作为 AES-256 密钥
            byte[] keyBytes = Arrays.copyOf(hash, KEY_LENGTH);
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

    /** 读取并验证文件头 */
    public boolean verifyHeader(InputStream in) throws IOException {
        if (!enabled) return true;
        byte[] magic = new byte[MAGIC.length];
        int read = in.read(magic);
        if (read != MAGIC.length || !Arrays.equals(magic, MAGIC)) {
            return false;
        }
        int v1 = in.read();
        int v2 = in.read();
        if (v1 < 0 || v2 < 0) return false;
        short version = (short) ((v1 << 8) | v2);
        return version == VERSION;
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
