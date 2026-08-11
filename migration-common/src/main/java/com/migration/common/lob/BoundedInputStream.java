package com.migration.common.lob;

import java.io.IOException;
import java.io.InputStream;

/**
 * 只暴露底层流前 N 个字节的视图，且<b>不会关闭底层流</b>。
 *
 * <p>分块追加路径要把一条长流切成若干块，每块单独交给
 * {@code setBinaryStream(idx, in, blockLen)}。JDBC 驱动读完声明长度就停，
 * 但它也可能顺手 close —— 那会把整条源流一起关掉，后续块全部读不到。
 * 所以这里必须显式屏蔽 close。
 */
public final class BoundedInputStream extends InputStream {

    private final InputStream in;
    private long remaining;

    public BoundedInputStream(InputStream in, long limit) {
        this.in = in;
        this.remaining = Math.max(0, limit);
    }

    /** 本块还剩多少字节没被读走。 */
    public long remaining() {
        return remaining;
    }

    @Override
    public int read() throws IOException {
        if (remaining <= 0) {
            return -1;
        }
        int b = in.read();
        if (b >= 0) {
            remaining--;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (remaining <= 0) {
            return -1;
        }
        int n = in.read(b, off, (int) Math.min(len, remaining));
        if (n > 0) {
            remaining -= n;
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = in.skip(Math.min(n, remaining));
        remaining -= skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min(in.available(), remaining);
    }

    /** 有意为空：块读完不代表源流读完。 */
    @Override
    public void close() {
    }
}
