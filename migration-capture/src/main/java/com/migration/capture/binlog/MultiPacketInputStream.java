package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.protocol.Packet;

import java.io.IOException;
import java.io.InputStream;

/**
 * 把 MySQL 协议里"一个逻辑包被切成若干 16MB 物理包"的序列，还原成一条连续的字节流。
 *
 * <p>MySQL 协议的包长字段只有 3 字节，最大 16777215（{@link Packet#MAX_LENGTH}）。
 * 超过这个长度的载荷会被切开：先发若干个<b>恰好</b> MAX_LENGTH 的包，最后跟一个
 * 长度 &lt; MAX_LENGTH 的包（可能为 0）收尾。判定规则就这一条，且必须逐字复刻——
 * 差一个边界，socket 就会停在大字段的中段，之后每一次读包头都读到垃圾，
 * 而且不会报错，只会表现为一连串莫名其妙的反序列化失败。
 *
 * <p>连接器上游的做法是把整个序列 {@code Arrays.copyOf} 拼成一块 byte[]——
 * 1GB 的行事件在这里就直接 OOM，且拷贝过程本身会产生约 32GB 的临时分配。
 * 本类改为按需从 socket 读，堆占用恒定为 0（除 drain 用的一个复用缓冲）。
 *
 * <p><b>层次很重要</b>：本类要放在
 * {@code socket → ByteArrayInputStream(包头在这层读) → 本类 → ByteArrayInputStream(事件体记账)}
 * 的中间。外层那个 ByteArrayInputStream 提供 {@code enterBlock/available} 的块记账，
 * 行事件反序列化靠 {@code while (inputStream.available() > 0)} 判断行读完没有——
 * 少了它，多行事件会读飞。
 */
public final class MultiPacketInputStream extends InputStream {

    /** drain 用的复用缓冲。binlog 监听是单线程的，一个就够。 */
    private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[64 * 1024]);

    private final ByteArrayInputStream source;
    /** 当前物理包还剩多少字节没读。 */
    private int remaining;
    /** 上一个包长 &lt; MAX_LENGTH ⇒ 序列已结束，不再有续包。 */
    private boolean lastPacket;
    /** 已从本序列交付出去的字节数（诊断用）。 */
    private long consumed;

    /**
     * @param source        连接的原始输入流（包头就是从它读的）
     * @param firstPayload  首包剩余载荷长度（上游已消费掉 4 字节包头和 1 字节 marker）
     */
    public MultiPacketInputStream(ByteArrayInputStream source, int firstPayload) {
        this.source = source;
        this.remaining = Math.max(0, firstPayload);
        // 能走到这里说明首包长度 == MAX_LENGTH，后面必然还有续包
        this.lastPacket = false;
    }

    /**
     * 保证当前包里还有可读字节；当前包读空就翻到下一个续包。
     *
     * @return false 表示整个序列已经读完
     */
    private boolean advance() throws IOException {
        while (remaining == 0) {
            if (lastPacket) {
                return false;
            }
            int len = source.readInteger(3);
            source.skip(1);                       // sequence id
            lastPacket = (len < Packet.MAX_LENGTH);
            remaining = len;                      // 可能为 0：载荷正好在包边界结束，服务端补一个空包收尾
        }
        return true;
    }

    @Override
    public int read() throws IOException {
        if (!advance()) {
            return -1;
        }
        remaining--;
        consumed++;
        return source.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (!advance()) {
            return -1;
        }
        int n = source.read(b, off, Math.min(len, remaining));
        if (n > 0) {
            remaining -= n;
            consumed += n;
        }
        return n;
    }

    /**
     * 只在<b>当前物理包内</b>报告可读字节数。
     *
     * <p>不去问底层 socket 还有多少：那个数字包含后续包的包头，会让上层把包头当成载荷。
     */
    @Override
    public int available() {
        return remaining;
    }

    /** 已交付字节数（诊断/断言用）。 */
    public long consumed() {
        return consumed;
    }

    /** 序列是否已经读到底。 */
    public boolean exhausted() {
        return lastPacket && remaining == 0;
    }

    /**
     * 把本序列在 socket 上剩余的字节全部读掉。
     *
     * <p>这是流式改造的<b>安全阀</b>。上游先把整个事件拼进 byte[]，所以无论反序列化器
     * 读没读完，socket 一定停在下一个包头上；改成流式之后就没有这个保证了：
     * 反序列化器提前返回、抛异常、或者事件类型被跳过，都会留下没读完的字节。
     *
     * @return 是否成功排空；false 表示连接已不可信，调用方必须断开重连而不是继续读下一个包
     */
    public boolean drain() {
        try {
            byte[] buf = SCRATCH.get();
            while (advance()) {
                int n = read(buf, 0, buf.length);
                if (n < 0) {
                    break;   // 底层提前结束（连接断了 / 流被截断）
                }
            }
            // 判据必须是"真的读到了序列末尾"，而不是"循环退出了"。
            // 底层流提前结束时 read() 只会返回 -1、不抛异常，此时还有已声明未读取的字节，
            // 若报成功，调用方会继续把下一段当包头读——正是要防的那种静默错位。
            return exhausted();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        byte[] buf = SCRATCH.get();
        long skipped = 0;
        while (skipped < n) {
            int want = (int) Math.min(buf.length, n - skipped);
            int got = read(buf, 0, want);
            if (got < 0) {
                break;
            }
            skipped += got;
        }
        return skipped;
    }

    /** 有意为空：底层是共享的连接流，绝不能因为一个事件读完就把它关掉。 */
    @Override
    public void close() {
    }
}
