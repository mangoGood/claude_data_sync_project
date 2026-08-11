package com.migration.common.lob;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 源端分块读流。
 *
 * <p>这一层要守住的是"堆占用与字段大小无关"：无论内容多大，同一时刻只能有一个 chunk 在内存里。
 * 所以除了内容正确，还要断言<b>拉取次数与区间</b>——一个把 length 直接当 chunk 传下去的实现
 * 内容也是对的，但会在 1GB 上原地 OOM，只有区间断言能把它挡住。
 */
@DisplayName("源端分块读流")
class SourceChunkedBlobInputStreamTest {

    /** 记录每次拉取区间的假源；内容为 (offset % 251) 便于逐字节校验。 */
    private static final class FakeSource implements LobChunkSource {
        final long length;
        final List<long[]> calls = new ArrayList<>();
        int maxChunkReturned = Integer.MAX_VALUE;   // 模拟短读
        long returnEmptyAt = Long.MAX_VALUE;        // 模拟源内容被截断
        boolean closed;

        FakeSource(long length) {
            this.length = length;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public byte[] fetch(long offset, int len) {
            calls.add(new long[]{offset, len});
            if (offset == returnEmptyAt) {
                return new byte[0];
            }
            int n = Math.min(len, maxChunkReturned);
            byte[] out = new byte[n];
            for (int i = 0; i < n; i++) {
                out[i] = (byte) ((offset + i) % 251);
            }
            return out;
        }

        @Override
        public String describe() {
            return "fake";
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static byte[] expected(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) (i % 251);
        }
        return b;
    }

    private static byte[] drain(SourceChunkedBlobInputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[13];
        int n;
        while ((n = in.read(buf, 0, buf.length)) >= 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    @Test
    @DisplayName("内容逐字节还原，且按 chunk 切分拉取")
    void readsAllBytesInChunks() throws IOException {
        FakeSource src = new FakeSource(20);
        try (SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 7)) {
            assertArrayEquals(expected(20), drain(in));
        }
        assertEquals(3, src.calls.size(), "20 字节按 7 切应拉 3 次: " + src.calls.size());
        assertArrayEquals(new long[]{0, 7}, src.calls.get(0));
        assertArrayEquals(new long[]{7, 7}, src.calls.get(1));
        // 末块只要剩下的 6 字节，不能多要——多要会让服务端白读一段
        assertArrayEquals(new long[]{14, 6}, src.calls.get(2));
        assertTrue(src.closed, "关闭时应连带关闭源");
    }

    @Test
    @DisplayName("单字节 read() 与数组 read() 结果一致")
    void singleByteReadMatches() throws IOException {
        FakeSource src = new FakeSource(300);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 64)) {
            int b;
            while ((b = in.read()) >= 0) {
                out.write(b);
            }
        }
        assertArrayEquals(expected(300), out.toByteArray());
    }

    @Test
    @DisplayName("驱动只读部分字节时不丢不错位（短读）")
    void toleratesShortReadsFromSource() throws IOException {
        FakeSource src = new FakeSource(100);
        src.maxChunkReturned = 3;   // 源每次只给 3 字节，远小于请求的 chunk
        try (SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 40)) {
            assertArrayEquals(expected(100), drain(in));
        }
        // 每次只拿到 3 字节 ⇒ 需要 34 次拉取才能凑满 100
        assertEquals(34, src.calls.size());
    }

    @Test
    @DisplayName("skip 只推进偏移量，不去源端拉被跳过的块")
    void skipDoesNotFetch() throws IOException {
        FakeSource src = new FakeSource(1000);
        try (SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 100)) {
            assertEquals(700, in.skip(700));
            assertEquals(0, src.calls.size(), "跳过的区间不应产生任何查询");
            byte[] rest = drain(in);
            assertEquals(300, rest.length);
            assertEquals((byte) (700 % 251), rest[0]);
            assertEquals(700, src.calls.get(0)[0], "续读必须从 700 开始");
        }
    }

    @Test
    @DisplayName("空内容直接 EOF，不产生查询")
    void emptySource() throws IOException {
        FakeSource src = new FakeSource(0);
        try (SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 8)) {
            assertEquals(-1, in.read());
            assertEquals(-1, in.read(new byte[4], 0, 4));
        }
        assertEquals(0, src.calls.size());
    }

    @Test
    @DisplayName("源内容比声明长度短 → 报错，绝不能当成正常 EOF")
    void truncatedSourceFailsLoudly() {
        FakeSource src = new FakeSource(100);
        src.returnEmptyAt = 40;
        SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 40);
        IOException e = assertThrows(IOException.class, () -> drain(in));
        // 静默截断的后果是目标端写进一个短值且无任何报错，必须显式失败
        assertTrue(e.getMessage().contains("比声明的长度短"), e.getMessage());
    }

    @Test
    @DisplayName("delivered() 与 available() 反映真实进度")
    void progressAccounting() throws IOException {
        FakeSource src = new FakeSource(50);
        try (SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 10)) {
            assertEquals(50, in.available());
            byte[] buf = new byte[15];
            int n = in.read(buf, 0, 15);
            assertEquals(10, n, "一次最多交付一个 chunk");
            assertEquals(10, in.delivered());
            assertEquals(40, in.available());
        }
    }

    @Test
    @DisplayName("关闭后再读报错而不是返回脏数据")
    void readAfterCloseFails() throws IOException {
        SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(new FakeSource(10), 4);
        in.close();
        assertThrows(IOException.class, in::read);
    }

    @Test
    @DisplayName("大内容下堆占用只与 chunk 有关（拉取次数 = 总长/chunk）")
    void memoryIsBoundedByChunkNotLength() throws IOException {
        // 用 1GB 的声明长度但假源不真的分配 1GB：只断言切分行为
        long oneGb = 1024L * 1024 * 1024;
        FakeSource src = new FakeSource(oneGb);
        SourceChunkedBlobInputStream in = new SourceChunkedBlobInputStream(src, 8 * 1024 * 1024);
        byte[] buf = new byte[8 * 1024 * 1024];
        for (int i = 0; i < 4; i++) {
            assertEquals(buf.length, in.read(buf, 0, buf.length));
        }
        assertEquals(4, src.calls.size());
        for (long[] call : src.calls) {
            assertEquals(8 * 1024 * 1024, call[1], "每次请求的长度必须恒为 chunk，不能随字段大小增长");
        }
        in.close();
    }
}
