package com.migration.common.lob;

import java.io.IOException;
import java.io.InputStream;

/**
 * 把 {@link LobChunkSource} 的分块读包装成一条普通 {@link InputStream}。
 *
 * <p>这样源端的"分块拉取"就能直接喂给 {@code PreparedStatement.setBinaryStream}，
 * 而堆里同一时刻只有一个 chunk（默认 8MB）。1GB 的字段和 1MB 的字段，
 * 这个类的内存占用完全一样。
 *
 * <p><b>不是</b>线程安全的，也不支持 mark/reset —— 调用方（JDBC 驱动）只顺序读一遍。
 */
public final class SourceChunkedBlobInputStream extends InputStream {

    public static final int DEFAULT_CHUNK_BYTES = 8 * 1024 * 1024;

    private final LobChunkSource source;
    private final int chunkSize;
    private final long length;

    private byte[] buf = new byte[0];
    private int bufPos;
    private long nextOffset;
    private boolean eof;
    private boolean closed;

    public SourceChunkedBlobInputStream(LobChunkSource source) {
        this(source, DEFAULT_CHUNK_BYTES);
    }

    public SourceChunkedBlobInputStream(LobChunkSource source, int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0: " + chunkSize);
        }
        this.source = source;
        this.chunkSize = chunkSize;
        this.length = source.length();
    }

    /** 已经交付给下游的字节数。写入侧用它做块级断点。 */
    public long delivered() {
        return nextOffset - remainingInBuffer();
    }

    private int remainingInBuffer() {
        return buf.length - bufPos;
    }

    /**
     * 保证缓冲里至少有一个字节可读。
     *
     * @return false 表示已到末尾
     */
    private boolean fill() throws IOException {
        if (bufPos < buf.length) {
            return true;
        }
        if (eof || nextOffset >= length) {
            eof = true;
            return false;
        }
        int want = (int) Math.min(chunkSize, length - nextOffset);
        byte[] chunk = source.fetch(nextOffset, want);
        if (chunk == null || chunk.length == 0) {
            // 源端提前没了：这是真错误而不是正常结束——总长度是先量好的，
            // 静默当成 EOF 会让目标端写进一个被截断的值，且没有任何报错。
            throw new IOException("LOB 分块读到空块，源内容比声明的长度短: " + source.describe()
                    + " offset=" + nextOffset + " expectedLength=" + length);
        }
        buf = chunk;
        bufPos = 0;
        nextOffset += chunk.length;
        return true;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        if (!fill()) {
            return -1;
        }
        return buf[bufPos++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (!fill()) {
            return -1;
        }
        int n = Math.min(len, remainingInBuffer());
        System.arraycopy(buf, bufPos, b, off, n);
        bufPos += n;
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        long skipped = 0;
        // 缓冲内先跳
        int inBuf = (int) Math.min(remainingInBuffer(), n);
        bufPos += inBuf;
        skipped += inBuf;
        // 剩下的直接推进偏移量，不发查询——跳过的块根本不用拉
        long rest = Math.min(n - skipped, length - nextOffset);
        if (rest > 0) {
            nextOffset += rest;
            skipped += rest;
        }
        return skipped;
    }

    @Override
    public int available() {
        long left = remainingInBuffer() + Math.max(0, length - nextOffset);
        return (int) Math.min(Integer.MAX_VALUE, left);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            buf = new byte[0];
            source.close();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("stream closed: " + source.describe());
        }
    }
}
