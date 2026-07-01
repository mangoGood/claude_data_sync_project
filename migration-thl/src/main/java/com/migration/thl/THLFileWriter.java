package com.migration.thl;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * THL 文件写入器（分帧格式）。
 *
 * <p>文件以 4 字节 magic {@code THL1} 开头，其后每条事件写为一条自描述记录：
 * {@code [seqno:long][len:int][payload:len]}，payload 是该事件独立序列化的字节。
 * 这样读取端可只读 12 字节记录头、对已应用事件按字节 skip 而无需反序列化，
 * 显著加快增量进程重启时的“跳到当前位点”。读取端通过 magic 自动兼容旧的整流格式。
 */
public class THLFileWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(THLFileWriter.class);

    private File thlFile;
    private FileOutputStream fos;
    private DataOutputStream out;

    public THLFileWriter(String filePath) throws IOException {
        this.thlFile = new File(filePath);

        File parentDir = thlFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        fos = new FileOutputStream(thlFile);
        out = new DataOutputStream(new BufferedOutputStream(fos));
        out.write(THLFileReader.FRAMED_MAGIC);
        out.flush();

        logger.info("Created THL file: {} (framed)", thlFile.getAbsolutePath());
    }

    /**
     * 子类专用构造函数，跳过文件头写入初始化。
     * 子类需自行管理输出流的创建与写入。
     */
    protected THLFileWriter(boolean skipInit) throws IOException {
        if (!skipInit) {
            throw new IllegalArgumentException("This constructor is for subclasses only");
        }
        this.thlFile = null;
        this.fos = null;
        this.out = null;
    }

    public void writeEvent(THLEvent event) throws IOException {
        // 每条事件独立序列化为自包含字节，便于读取端按记录跳过/读取
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(event);
        }
        byte[] payload = baos.toByteArray();

        out.writeLong(event.getSeqno());
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            out.close();
        }
        if (fos != null) {
            fos.close();
        }
        logger.info("Closed THL file writer");
    }
}
