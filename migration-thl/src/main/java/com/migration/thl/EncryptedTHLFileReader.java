package com.migration.thl;

import com.migration.thl.crypto.ThlEncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * 加密版 THL 文件读取器
 *
 * <p>自动识别文件是否加密：
 * <ul>
 *   <li>加密文件：使用 ThlEncryptionService 解密每条记录</li>
 *   <li>普通文件：兼容原 {@link THLFileReader} 格式</li>
 * </ul>
 */
public class EncryptedTHLFileReader extends THLFileReader {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedTHLFileReader.class);

    private final FileInputStream fis;
    private final ThlEncryptionService encryptionService;
    private final boolean encryptionEnabled;
    private final boolean fileEncrypted;
    private ObjectInputStream cachedOis = null;

    public EncryptedTHLFileReader(String thlFile, ThlEncryptionService encryptionService) throws IOException {
        super(true); // 跳过父类文件头读取初始化

        File file = new File(thlFile);
        if (!file.exists()) {
            throw new IOException("THL file not found: " + thlFile);
        }

        this.encryptionService = encryptionService;
        this.encryptionEnabled = encryptionService != null && encryptionService.isEnabled();
        this.fis = new FileInputStream(file);

        // 检查文件是否加密
        if (encryptionEnabled) {
            fileEncrypted = encryptionService.isEncryptedFile(file);
            if (fileEncrypted) {
                if (!encryptionService.verifyHeader(fis)) {
                    throw new IOException("无效的加密文件头: " + thlFile);
                }
            }
        } else {
            fileEncrypted = false;
        }

        logger.info("Opened THL file: {} (encrypted={})", thlFile, fileEncrypted);
    }

    @Override
    public THLEvent readEvent() throws IOException, ClassNotFoundException {
        if (fileEncrypted) {
            return readEncryptedEvent();
        } else {
            return readPlainEvent();
        }
    }

    private THLEvent readEncryptedEvent() throws IOException, ClassNotFoundException {
        byte[] lenBuf = new byte[4];
        int read = fis.read(lenBuf);
        if (read < 0) return null;
        if (read < 4) throw new IOException("不完整的记录长度");

        int len = ByteBuffer.wrap(lenBuf).getInt();
        if (len <= 0 || len > 100 * 1024 * 1024) {
            throw new IOException("无效的记录长度: " + len);
        }

        byte[] encrypted = new byte[len];
        int totalRead = 0;
        while (totalRead < len) {
            int r = fis.read(encrypted, totalRead, len - totalRead);
            if (r < 0) throw new IOException("不完整的加密记录");
            totalRead += r;
        }

        byte[] decrypted = encryptionService.decryptRecord(encrypted);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(decrypted))) {
            return (THLEvent) ois.readObject();
        }
    }

    private THLEvent readPlainEvent() throws IOException, ClassNotFoundException {
        if (cachedOis == null) {
            if (fis.available() <= 0) return null;
            cachedOis = new ObjectInputStream(fis);
        }
        try {
            return (THLEvent) cachedOis.readObject();
        } catch (java.io.EOFException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (cachedOis != null) {
            try { cachedOis.close(); } catch (IOException ignored) {}
        }
        if (fis != null) {
            fis.close();
        }
        logger.info("Closed THL file reader");
    }
}
