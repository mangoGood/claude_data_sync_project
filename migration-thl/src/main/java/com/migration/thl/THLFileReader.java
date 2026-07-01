package com.migration.thl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * THL 文件读取器。
 *
 * <p>支持两种磁盘格式，构造时自动探测：
 * <ul>
 *   <li><b>分帧格式（新）</b>：文件以 4 字节 magic {@code THL1} 开头，其后为重复的
 *       {@code [seqno:long][len:int][payload:len]} 记录，payload 为单条事件独立序列化的字节。
 *       该格式支持 {@link #readEventAfter(long)} 按字节跳过已应用事件（无需反序列化），
 *       大幅加快增量进程重启时“从 seqno 跳到当前位点”的速度。</li>
 *   <li><b>旧格式（兼容）</b>：整文件是一个 {@link ObjectInputStream}（无 magic）。仍按原方式逐个
 *       {@code readObject} 读取，行为与历史完全一致。</li>
 * </ul>
 */
public class THLFileReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(THLFileReader.class);

    /** 分帧格式文件头 magic：与 Java 序列化流头(0xACED0005)不冲突，可可靠区分新旧格式。 */
    public static final byte[] FRAMED_MAGIC = {'T', 'H', 'L', '1'};

    /** {@link #readEventAfter} / peek 到达文件末尾时的哨兵 seqno。 */
    public static final long EOF_SEQNO = Long.MIN_VALUE;

    // 旧格式
    private FileInputStream fis;
    private ObjectInputStream ois;

    // 新分帧格式
    private DataInputStream framedIn;
    private boolean framed;
    private boolean emptyEof;          // 文件不足 4 字节（尚未就绪/空文件）：当作无事件
    private long pendingSeqno;         // 已读取但尚未消费 payload 的记录头
    private int pendingLen = -1;       // -1 表示当前无挂起记录头

    public THLFileReader(String thlFile) throws IOException {
        File file = new File(thlFile);
        if (!file.exists()) {
            throw new IOException("THL file not found: " + thlFile);
        }

        FileInputStream probe = new FileInputStream(file);
        byte[] head = new byte[4];
        int n = readNFully(probe, head, 4);

        if (n == 4 && Arrays.equals(head, FRAMED_MAGIC)) {
            // 新分帧格式：复用已越过 magic 的流
            this.framed = true;
            this.framedIn = new DataInputStream(new BufferedInputStream(probe));
        } else if (n < 4) {
            // 文件还不足 4 字节（extract 刚创建尚未写入）：当作无事件，下个扫描周期再读
            probe.close();
            this.framed = true;
            this.emptyEof = true;
        } else {
            // 旧格式：重新从头打开为 ObjectInputStream，行为与历史一致
            probe.close();
            this.fis = new FileInputStream(file);
            this.ois = new ObjectInputStream(fis);
        }

        logger.info("Opened THL file: {} (framed={})", thlFile, framed && !emptyEof);
    }

    /**
     * 子类专用构造函数，跳过文件头读取初始化。
     * 子类需自行管理 FileInputStream 的创建与读取。
     */
    protected THLFileReader(boolean skipInit) throws IOException {
        if (!skipInit) {
            throw new IllegalArgumentException("This constructor is for subclasses only");
        }
        this.fis = null;
        this.ois = null;
    }

    /** 是否支持按字节快速跳过（仅新分帧格式且非加密读取器支持）。 */
    public boolean supportsFastSkip() {
        return framed && !emptyEof && framedIn != null;
    }

    public THLEvent readEvent() throws IOException, ClassNotFoundException {
        if (emptyEof) {
            return null;
        }
        if (framed) {
            if (!readNextHeader()) {
                return null;
            }
            return readPendingPayload();
        }
        // 旧格式
        try {
            return (THLEvent) ois.readObject();
        } catch (EOFException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 返回 seqno 严格大于 {@code minExclusive} 的下一条事件；其余（已应用）事件被跳过。
     * 分帧格式下，被跳过的事件按字节 skip、不反序列化——这是重启快速跳到当前位点的关键。
     * 旧格式下退化为逐条反序列化再按 seqno 跳过（与历史行为一致）。
     * 到达文件末尾返回 null。
     */
    public THLEvent readEventAfter(long minExclusive) throws IOException, ClassNotFoundException {
        if (emptyEof) {
            return null;
        }
        if (framed) {
            while (readNextHeader()) {
                if (pendingSeqno <= minExclusive) {
                    if (!skipPendingPayload()) {
                        return null; // payload 尚未写完整，当作 EOF，下个周期重读
                    }
                } else {
                    return readPendingPayload();
                }
            }
            return null;
        }
        // 旧格式：逐条读取并按 seqno 跳过
        THLEvent event;
        while ((event = readEvent()) != null) {
            if (event.getSeqno() <= minExclusive) {
                continue;
            }
            return event;
        }
        return null;
    }

    /** 读取下一条记录头（seqno+len）到 pending；EOF/半条记录返回 false。 */
    private boolean readNextHeader() {
        if (pendingLen != -1) {
            return true; // 已有未消费的记录头
        }
        try {
            long seqno = framedIn.readLong();
            int len = framedIn.readInt();
            if (len < 0) {
                return false;
            }
            pendingSeqno = seqno;
            pendingLen = len;
            return true;
        } catch (EOFException e) {
            return false; // 干净 EOF 或半条头（extract 仍在写）
        } catch (IOException e) {
            logger.warn("读取 THL 记录头失败，按 EOF 处理: {}", e.getMessage());
            return false;
        }
    }

    private THLEvent readPendingPayload() throws IOException, ClassNotFoundException {
        byte[] payload = new byte[pendingLen];
        try {
            framedIn.readFully(payload);
        } catch (EOFException e) {
            // payload 尚未写完整（extract 写入中）：本轮当作 EOF，下个周期重读
            pendingLen = -1;
            return null;
        }
        pendingLen = -1;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
            return (THLEvent) ois.readObject();
        }
    }

    /** 跳过当前挂起记录的 payload；完整跳过返回 true，遇到 EOF（payload 未写完整）返回 false。 */
    private boolean skipPendingPayload() throws IOException {
        int remaining = pendingLen;
        pendingLen = -1;
        while (remaining > 0) {
            long skipped = framedIn.skip(remaining);
            if (skipped <= 0) {
                int b = framedIn.read(); // skip 无进展时用读取兜底
                if (b < 0) {
                    return false; // EOF：payload 不完整
                }
                skipped = 1;
            }
            remaining -= (int) skipped;
        }
        return true;
    }

    /** 尽量从流中读满 len 字节，返回实际读到的字节数。 */
    private static int readNFully(FileInputStream in, byte[] buf, int len) throws IOException {
        int off = 0;
        while (off < len) {
            int r = in.read(buf, off, len - off);
            if (r < 0) break;
            off += r;
        }
        return off;
    }

    @Override
    public void close() throws IOException {
        if (framedIn != null) {
            framedIn.close();
        }
        if (ois != null) {
            ois.close();
        }
        if (fis != null) {
            fis.close();
        }
        logger.info("Closed THL file reader");
    }
}
