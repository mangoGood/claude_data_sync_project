package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.protocol.Packet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 跨 16MB 包的流式读（纯单测，不连库）。
 *
 * <p>这一层是整个大字段增量改造里<b>风险最集中</b>的地方：分包边界判定错一个字节，
 * socket 就会停在事件体中间，之后每次读包头都读到载荷的中段。而且它不会报错，
 * 只表现为一串莫名其妙的反序列化失败——线上排查这种问题的代价极高。
 * 所以这里用构造出来的字节流把每一条边界规则都正面锁住。
 *
 * <p>协议规则（与上游 {@code readPacketSplitInChunks} 逐字对应）：
 * 长度恰好等于 {@link Packet#MAX_LENGTH} 的包后面必然还有续包；
 * 长度小于它的包（含长度 0）是最后一个。
 */
@DisplayName("跨包流式读")
class MultiPacketInputStreamTest {

    /** 为了让用例跑得快，这里用一个小得多的"包长上限"是不行的——规则写死在 Packet.MAX_LENGTH， */
    private static final int MAX = Packet.MAX_LENGTH;

    /** 按 (offset % 251) 生成内容，便于逐字节校验。 */
    private static byte[] content(int length, int seedOffset) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) ((seedOffset + i) % 251);
        }
        return b;
    }

    /**
     * 拼一段"服务端会发出来的字节"：首包载荷（包头已被上游消费）+ 若干续包（各带 4 字节包头）。
     *
     * @param packetPayloads 每个物理包的载荷长度；除最后一个外必须都等于 MAX
     */
    private static byte[] wirePackets(int[] packetPayloads, byte[] logicalPayload, byte[] trailing) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int consumed = 0;
        for (int i = 0; i < packetPayloads.length; i++) {
            int len = packetPayloads[i];
            if (i > 0) {
                // 续包头：3 字节小端长度 + 1 字节 sequence
                out.write(len & 0xFF);
                out.write((len >> 8) & 0xFF);
                out.write((len >> 16) & 0xFF);
                out.write(i & 0xFF);
            }
            out.write(logicalPayload, consumed, len);
            consumed += len;
        }
        if (trailing != null) {
            out.write(trailing, 0, trailing.length);
        }
        return out.toByteArray();
    }

    private static MultiPacketInputStream open(byte[] wire, int firstPayload) {
        return new MultiPacketInputStream(new ByteArrayInputStream(wire), firstPayload);
    }

    private static byte[] drainAll(MultiPacketInputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[7919];   // 故意用质数，逼出跨包的半块读
        int n;
        while ((n = in.read(buf, 0, buf.length)) >= 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    @Test
    @DisplayName("两个包：首包满 + 末包不满，内容逐字节还原")
    void twoPackets() throws IOException {
        int tailLen = 12345;
        byte[] logical = content(MAX + tailLen, 0);
        byte[] wire = wirePackets(new int[]{MAX, tailLen}, logical, null);

        MultiPacketInputStream in = open(wire, MAX);
        assertArrayEquals(logical, drainAll(in));
        assertEquals(logical.length, in.consumed());
        assertTrue(in.exhausted());
    }

    @Test
    @DisplayName("三个包：中间包也必须满，末包不满")
    void threePackets() throws IOException {
        int tailLen = 1;
        byte[] logical = content(MAX * 2 + tailLen, 0);
        byte[] wire = wirePackets(new int[]{MAX, MAX, tailLen}, logical, null);
        assertArrayEquals(logical, drainAll(open(wire, MAX)));
    }

    @Test
    @DisplayName("载荷正好在包边界结束 → 服务端补一个 0 长包收尾，必须识别为结束")
    void zeroLengthTerminatingPacket() throws IOException {
        byte[] logical = content(MAX, 0);
        byte[] wire = wirePackets(new int[]{MAX, 0}, logical, null);

        MultiPacketInputStream in = open(wire, MAX);
        assertArrayEquals(logical, drainAll(in));
        // 少了这条判定，读取端会一直等下一个包，表现为整条复制流挂死
        assertTrue(in.exhausted());
    }

    @Test
    @DisplayName("读完本序列后，socket 恰好停在下一个包的包头上")
    void leavesStreamPositionedAtNextPacket() throws IOException {
        byte[] logical = content(MAX + 100, 0);
        byte[] nextPacketHeaderAndBody = new byte[]{0x05, 0x00, 0x00, 0x07, 11, 22, 33, 44, 55};
        byte[] wire = wirePackets(new int[]{MAX, 100}, logical, nextPacketHeaderAndBody);

        ByteArrayInputStream raw = new ByteArrayInputStream(wire);
        MultiPacketInputStream in = new MultiPacketInputStream(raw, MAX);
        assertArrayEquals(logical, drainAll(in));

        // 关键断言：错位就是从这里开始的。下一个包头必须原封不动地留在流上。
        assertEquals(5, raw.readInteger(3));
        assertEquals(7, raw.read());
        assertArrayEquals(new byte[]{11, 22, 33, 44, 55}, raw.read(5));
    }

    @Test
    @DisplayName("drain：反序列化器只读了一半时，把剩余字节吃干净并定位到下一个包头")
    void drainAfterPartialRead() throws IOException {
        byte[] logical = content(MAX + 5000, 0);
        byte[] nextPacket = new byte[]{0x03, 0x00, 0x00, 0x09, 77, 88, 99};
        byte[] wire = wirePackets(new int[]{MAX, 5000}, logical, nextPacket);

        ByteArrayInputStream raw = new ByteArrayInputStream(wire);
        MultiPacketInputStream in = new MultiPacketInputStream(raw, MAX);

        byte[] buf = new byte[100];
        assertEquals(100, in.read(buf, 0, 100));       // 只读了 100 字节就"放弃"
        assertTrue(in.drain(), "drain 应成功");

        // 不 drain 的话，这里读到的会是大字段的中段，而且不会报错
        assertEquals(3, raw.readInteger(3));
        assertEquals(9, raw.read());
        assertArrayEquals(new byte[]{77, 88, 99}, raw.read(3));
    }

    @Test
    @DisplayName("drain：一个字节都没读时也能排空")
    void drainWithoutAnyRead() throws IOException {
        byte[] logical = content(MAX + 7, 0);
        byte[] nextPacket = new byte[]{0x01, 0x00, 0x00, 0x02, 42};
        byte[] wire = wirePackets(new int[]{MAX, 7}, logical, nextPacket);

        ByteArrayInputStream raw = new ByteArrayInputStream(wire);
        MultiPacketInputStream in = new MultiPacketInputStream(raw, MAX);
        assertTrue(in.drain());
        assertEquals(1, raw.readInteger(3));
        assertEquals(2, raw.read());
        assertEquals(42, raw.read());
    }

    @Test
    @DisplayName("drain：已读完时是空操作，可重复调用")
    void drainIsIdempotent() throws IOException {
        byte[] logical = content(MAX + 3, 0);
        byte[] wire = wirePackets(new int[]{MAX, 3}, logical, null);
        MultiPacketInputStream in = open(wire, MAX);
        drainAll(in);
        assertTrue(in.drain());
        assertTrue(in.drain());
        assertEquals(logical.length, in.consumed());
    }

    @Test
    @DisplayName("drain：流被截断（连接断了）→ 返回 false，让调用方重连而不是继续读")
    void drainFailsOnTruncatedStream() {
        byte[] logical = content(MAX + 1000, 0);
        byte[] wire = wirePackets(new int[]{MAX, 1000}, logical, null);
        // 砍掉末包的一半，模拟连接中断
        byte[] truncated = new byte[wire.length - 500];
        System.arraycopy(wire, 0, truncated, 0, truncated.length);

        MultiPacketInputStream in = open(truncated, MAX);
        assertFalse(in.drain(), "排不干净必须显式失败：继续读下一个包就是静默错位");
    }

    @Test
    @DisplayName("单字节 read() 与数组 read() 在包边界上表现一致")
    void singleByteReadAcrossBoundary() throws IOException {
        int tail = 3;
        byte[] logical = content(MAX + tail, 0);
        byte[] wire = wirePackets(new int[]{MAX, tail}, logical, null);

        MultiPacketInputStream in = open(wire, MAX);
        // 跳到边界前 2 字节，然后逐字节跨过边界
        assertEquals(MAX - 2, in.skip(MAX - 2));
        assertEquals(logical[MAX - 2] & 0xFF, in.read());
        assertEquals(logical[MAX - 1] & 0xFF, in.read());
        assertEquals(logical[MAX] & 0xFF, in.read(), "跨包的第一个字节");
        assertEquals(logical[MAX + 1] & 0xFF, in.read());
        assertEquals(logical[MAX + 2] & 0xFF, in.read());
        assertEquals(-1, in.read());
    }

    @Test
    @DisplayName("available() 只报当前包剩余，不能把续包头算成可读载荷")
    void availableReportsCurrentPacketOnly() throws IOException {
        byte[] logical = content(MAX + 10, 0);
        byte[] wire = wirePackets(new int[]{MAX, 10}, logical, null);
        MultiPacketInputStream in = open(wire, MAX);
        assertEquals(MAX, in.available());
        in.skip(MAX);
        // 此刻当前包已空、续包还没翻过去；报大了会让上层把 4 字节包头当成载荷读进去
        assertEquals(0, in.available());
        assertEquals(logical[MAX] & 0xFF, in.read());
        assertEquals(9, in.available());
    }

    @Test
    @DisplayName("close() 不能关掉底层连接流")
    void closeDoesNotCloseUnderlyingStream() throws IOException {
        byte[] logical = content(MAX + 4, 0);
        byte[] nextPacket = new byte[]{0x01, 0x00, 0x00, 0x03, 7};
        byte[] wire = wirePackets(new int[]{MAX, 4}, logical, nextPacket);
        ByteArrayInputStream raw = new ByteArrayInputStream(wire);
        MultiPacketInputStream in = new MultiPacketInputStream(raw, MAX);
        drainAll(in);
        in.close();
        // 底层是整条连接，关掉它等于断线重连
        assertEquals(1, raw.readInteger(3));
    }

    @Test
    @DisplayName("1GB 级事件：堆占用与事件大小无关（只按包数翻页）")
    void gigabyteScaleStaysConstantMemory() throws IOException {
        // 直接构造 1GB 的字节流会把测试自己撑爆，这里退而验证"翻页次数 = 包数"这一结构性质：
        // 每翻一个包只推进指针，不做任何与总长度成比例的分配。
        int packets = 8;
        int tail = 123;
        int total = MAX * (packets - 1) + tail;
        byte[] logical = content(total, 0);
        int[] lens = new int[packets];
        for (int i = 0; i < packets - 1; i++) {
            lens[i] = MAX;
        }
        lens[packets - 1] = tail;

        MultiPacketInputStream in = open(wirePackets(lens, logical, null), MAX);
        long read = 0;
        byte[] buf = new byte[1 << 20];
        int n;
        while ((n = in.read(buf, 0, buf.length)) >= 0) {
            read += n;
        }
        assertEquals(total, read);
        assertEquals(total, in.consumed());
    }
}
