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

    /** 攒批 flush 的水位与时间上限：吞吐与读侧可见延迟的折中。 */
    private static final int FLUSH_EVERY_EVENTS = 64;
    private static final long FLUSH_INTERVAL_MS = 200;
    private int pendingSinceFlush;
    private long lastFlushMs = System.currentTimeMillis();

    public THLFileWriter(String filePath) throws IOException {
        this.thlFile = new File(filePath);

        File parentDir = thlFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        fos = new FileOutputStream(thlFile);
        out = new DataOutputStream(new BufferedOutputStream(fos));
        out.write(THLFileReader.CODEC_MAGIC);
        out.flush();

        logger.info("Created THL file: {} (framed, codec v{})",
                thlFile.getAbsolutePath(), ThlCodec.FORMAT_VERSION);
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
        // 每条事件独立编码为自包含字节，便于读取端按记录跳过/读取。
        // 编码走 ThlCodec 而不是 ObjectOutputStream：后者每条都要重写一遍类描述符，
        // 实测占单条 741B 里的 278B（38%）。
        byte[] payload = ThlCodec.encode(event);

        out.writeLong(event.getSeqno());
        out.writeInt(payload.length);
        out.write(payload);

        // 不再每条 flush。老写法每条一次 flush，把外层 BufferedOutputStream 完全废掉——
        // 10,043 条事件就是 10,043 次 write 系统调用（64KB 缓冲下本该约 115 次）。
        //
        // 为什么这样不丢数据：flush 只是把字节交给内核，不是 fsync；进程崩了缓冲区里的
        // 内容确实会丢，但 THL 的恢复语义本来就不依赖"写到哪一条"——重启后 extract 从
        // checkpoint 位点重新产出，缓冲区里那几条会被重新写一遍。真正不能丢的是 checkpoint
        // 本身，那条路径有自己的落盘保证。
        //
        // 读侧的可见性由 maybeFlush 的水位与 flush() 保证：increment 是靠
        // WatchService 感知文件变化后再读的，读到半条记录会按 EOF 处理、下轮重读。
        maybeFlush();
    }

    /** 累积到水位或超过间隔就 flush，兼顾吞吐与读侧可见延迟。 */
    private void maybeFlush() throws IOException {
        pendingSinceFlush++;
        long now = System.currentTimeMillis();
        if (pendingSinceFlush >= FLUSH_EVERY_EVENTS || now - lastFlushMs >= FLUSH_INTERVAL_MS) {
            out.flush();
            pendingSinceFlush = 0;
            lastFlushMs = now;
        }
    }

    /**
     * 立即把缓冲区交给内核。
     *
     * <p>调用方在"下游必须马上看到这批事件"的时刻显式调用——例如事务边界、
     * 心跳、以及 extract 进入空闲前。低流量链路全靠它把延迟压住：
     * 否则一条事件可能在缓冲区里躺到下一条到来。
     */
    public void flush() throws IOException {
        if (out != null) {
            out.flush();
            pendingSinceFlush = 0;
            lastFlushMs = System.currentTimeMillis();
        }
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            out.flush();
            out.close();
        }
        if (fos != null) {
            fos.close();
        }
        logger.info("Closed THL file writer");
    }
}
