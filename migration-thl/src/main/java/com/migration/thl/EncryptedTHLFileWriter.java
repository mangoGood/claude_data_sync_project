package com.migration.thl;

import com.migration.thl.crypto.ThlEncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * 加密版 THL 文件写入器
 *
 * <p>当 ThlEncryptionService 启用时，写入的 THL 事件会被加密；
 * 未启用时，行为与 {@link THLFileWriter} 一致。
 */
public class EncryptedTHLFileWriter extends THLFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedTHLFileWriter.class);

    private final File thlFile;
    private final FileOutputStream fos;
    private final ThlEncryptionService encryptionService;
    private final boolean encryptionEnabled;

    public EncryptedTHLFileWriter(String filePath, ThlEncryptionService encryptionService) throws IOException {
        super(true); // 跳过父类 ObjectOutputStream 头写入初始化
        this.thlFile = new File(filePath);
        this.encryptionService = encryptionService;
        this.encryptionEnabled = encryptionService != null && encryptionService.isEnabled();

        File parentDir = thlFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        // 打开文件（覆盖模式）
        fos = new FileOutputStream(thlFile);

        if (encryptionEnabled) {
            encryptionService.writeHeader(fos);
        }

        logger.info("Created THL file: {} (encrypted={})", thlFile.getAbsolutePath(), encryptionEnabled);
    }

    @Override
    public void writeEvent(THLEvent event) throws IOException {
        // 序列化事件为字节数组
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(event);
        }
        byte[] bytes = baos.toByteArray();

        if (encryptionEnabled) {
            byte[] encrypted = encryptionService.encryptRecord(bytes);
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(encrypted.length);
            fos.write(lenBuf.array());
            fos.write(encrypted);
        } else {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(bytes.length);
            fos.write(lenBuf.array());
            fos.write(bytes);
        }
        fos.flush();
    }

    @Override
    public void close() throws IOException {
        if (fos != null) {
            fos.close();
        }
        logger.info("Closed THL file writer");
    }
}
