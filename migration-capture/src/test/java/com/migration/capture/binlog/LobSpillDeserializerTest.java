package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.migration.common.lob.LobRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 大字段落盘反序列化器（纯单测，不连库）。
 *
 * <p>用手工拼的 WRITE_ROWS / UPDATE_ROWS / DELETE_ROWS 事件字节驱动，覆盖：
 * 超阈值落盘、阈值以下保持原行为、前镜像丢弃、md5 与内容一致、长度溢出保护。
 */
@DisplayName("大字段落盘反序列化器")
class LobSpillDeserializerTest {

    private static final long TABLE_ID = 77L;

    @TempDir
    File spillDir;

    private Map<Long, TableMapEventData> tableMap;
    private LobSpillWriter spillWriter;

    @BeforeEach
    void setUp() {
        tableMap = new HashMap<>();
        // 两列：LONG(int) 主键 + BLOB（meta=4 即 LONGBLOB）
        TableMapEventData tm = new TableMapEventData();
        tm.setTableId(TABLE_ID);
        tm.setDatabase("db1");
        tm.setTable("t_lob");
        tm.setColumnTypes(new byte[]{(byte) ColumnType.LONG.getCode(), (byte) ColumnType.BLOB.getCode()});
        tm.setColumnMetadata(new int[]{0, 4});
        tm.setColumnNullability(new java.util.BitSet(2));
        tableMap.put(TABLE_ID, tm);
        spillWriter = new LobSpillWriter(spillDir.getAbsolutePath());
    }

    @AfterEach
    void tearDown() {
        // TempDir 自己会清
    }

    private static byte[] payload(int length, int seed) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) ((i * 37 + seed) % 251);
        }
        return b;
    }

    private static String md5(byte[] b) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (byte x : MessageDigest.getInstance("MD5").digest(b)) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    /** 一行的行内表示：null 位图 + LONG 值 + BLOB(4 字节长度 + 内容)。 */
    private static void writeRow(ByteArrayOutputStream out, int id, byte[] blob) {
        out.write(0);                          // null 位图（2 列都非空 ⇒ 1 字节 0）
        out.write(id & 0xFF);
        out.write((id >> 8) & 0xFF);
        out.write((id >> 16) & 0xFF);
        out.write((id >> 24) & 0xFF);
        int len = blob.length;                 // BLOB meta=4 ⇒ 4 字节小端长度
        out.write(len & 0xFF);
        out.write((len >> 8) & 0xFF);
        out.write((len >> 16) & 0xFF);
        out.write((len >> 24) & 0xFF);
        out.write(blob, 0, blob.length);
    }

    /** WRITE_ROWS 事件体：tableId(6) + flags(2) + 列数(packed) + 包含列位图 + 行。 */
    private static byte[] writeRowsEvent(byte[]... blobs) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeHeaderCommon(out);
        out.write(0x03);                       // 包含列位图：两列都在
        for (int i = 0; i < blobs.length; i++) {
            writeRow(out, i + 1, blobs[i]);
        }
        return out.toByteArray();
    }

    /** UPDATE_ROWS 事件体：多一个"更新前包含列位图"，行按 (前, 后) 成对出现。 */
    private static byte[] updateRowsEvent(byte[] before, byte[] after) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeHeaderCommon(out);
        out.write(0x03);                       // 更新前包含列位图
        out.write(0x03);                       // 更新后包含列位图
        writeRow(out, 1, before);
        writeRow(out, 1, after);
        return out.toByteArray();
    }

    private static void writeHeaderCommon(ByteArrayOutputStream out) {
        for (int i = 0; i < 6; i++) {
            out.write(i == 0 ? (int) TABLE_ID : 0);   // tableId 小端 6 字节
        }
        out.write(0);
        out.write(0);                          // flags
        out.write(2);                          // 列数（packed integer，< 251 直接一字节）
    }

    private ByteArrayInputStream stream(byte[] body) {
        ByteArrayInputStream in = new ByteArrayInputStream(body);
        in.enterBlock(body.length);            // 模拟 EventDeserializer 的事件体块记账
        return in;
    }

    private SignAwareRowsDeserializers.SpillPolicy policy(int threshold) {
        return new SignAwareRowsDeserializers.SpillPolicy(spillWriter, threshold);
    }

    private byte[] spillContent(LobRef ref) throws IOException {
        return Files.readAllBytes(new File(spillDir, ref.file()).toPath());
    }

    // ------------------------------------------------------------------

    @Test
    @DisplayName("超阈值：落盘并返回引用，内容与 md5 都对得上")
    void spillsLargeBlob() throws Exception {
        byte[] blob = payload(50_000, 3);
        var d = new SignAwareRowsDeserializers.Write(tableMap, policy(1024));
        WriteRowsEventData data = d.deserialize(stream(writeRowsEvent(blob)));

        Object cell = data.getRows().get(0)[1];
        LobRef ref = assertInstanceOf(LobRef.class, cell, "大值应换成引用而不是 byte[]");
        assertFalse(ref.isDiscarded());
        assertEquals(blob.length, ref.length());
        assertEquals(md5(blob), ref.md5());
        assertArrayEquals(blob, spillContent(ref), "落盘内容必须逐字节一致");
        // 主键列不受影响
        assertEquals(1, data.getRows().get(0)[0]);
    }

    @Test
    @DisplayName("阈值以下：保持上游行为返回 byte[]，零回归")
    void smallBlobStaysInline() throws Exception {
        byte[] blob = payload(100, 5);
        var d = new SignAwareRowsDeserializers.Write(tableMap, policy(1024));
        WriteRowsEventData data = d.deserialize(stream(writeRowsEvent(blob)));

        assertArrayEquals(blob, (byte[]) data.getRows().get(0)[1]);
        assertEquals(0, spillDir.listFiles().length, "小值不该产生落盘文件");
    }

    @Test
    @DisplayName("未配置落盘策略：与改造前逐字节一致")
    void noPolicyMeansUpstreamBehaviour() throws Exception {
        byte[] blob = payload(50_000, 7);
        var d = new SignAwareRowsDeserializers.Write(tableMap);
        WriteRowsEventData data = d.deserialize(stream(writeRowsEvent(blob)));
        assertArrayEquals(blob, (byte[]) data.getRows().get(0)[1]);
        assertEquals(0, spillDir.listFiles().length);
    }

    @Test
    @DisplayName("UPDATE：前镜像只留 md5 不落盘，后镜像落盘")
    void updateDiscardsBeforeImage() throws Exception {
        byte[] before = payload(40_000, 1);
        byte[] after = payload(40_000, 2);
        var d = new SignAwareRowsDeserializers.Update(tableMap, policy(1024));
        UpdateRowsEventData data = d.deserialize(stream(updateRowsEvent(before, after)));

        LobRef beforeRef = assertInstanceOf(LobRef.class, data.getRows().get(0).getKey()[1]);
        LobRef afterRef = assertInstanceOf(LobRef.class, data.getRows().get(0).getValue()[1]);

        // 前镜像只用来定位行，而定位靠主键 —— 留着那份内容纯属浪费磁盘
        assertTrue(beforeRef.isDiscarded(), "前镜像不该落盘");
        assertNull(beforeRef.file());
        assertEquals(md5(before), beforeRef.md5(), "但身份要留着，冲突检测还要用");

        assertFalse(afterRef.isDiscarded(), "后镜像是要写进目标的内容，必须落盘");
        assertArrayEquals(after, spillContent(afterRef));
        assertEquals(1, spillDir.listFiles().length, "一次 UPDATE 只应产生一个落盘文件");
    }

    @Test
    @DisplayName("UPDATE：多行时前后镜像的交替判定不会错位")
    void updateAlternationHoldsAcrossRows() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeHeaderCommon(out);
        out.write(0x03);
        out.write(0x03);
        byte[][] blobs = new byte[6][];
        for (int i = 0; i < 6; i++) {
            blobs[i] = payload(20_000 + i, i);
            writeRow(out, 1 + i / 2, blobs[i]);
        }
        var d = new SignAwareRowsDeserializers.Update(tableMap, policy(1024));
        UpdateRowsEventData data = d.deserialize(stream(out.toByteArray()));

        assertEquals(3, data.getRows().size());
        for (int r = 0; r < 3; r++) {
            LobRef b = (LobRef) data.getRows().get(r).getKey()[1];
            LobRef a = (LobRef) data.getRows().get(r).getValue()[1];
            assertTrue(b.isDiscarded(), "第 " + r + " 行的前镜像判定错位了");
            assertFalse(a.isDiscarded(), "第 " + r + " 行的后镜像判定错位了");
            assertEquals(md5(blobs[r * 2]), b.md5());
            assertEquals(md5(blobs[r * 2 + 1]), a.md5());
        }
        assertEquals(3, spillDir.listFiles().length);
    }

    @Test
    @DisplayName("UPDATE：新事件开头重置交替标志，不受上个事件影响")
    void updateResetsAlternationPerEvent() throws Exception {
        var d = new SignAwareRowsDeserializers.Update(tableMap, policy(1024));
        byte[] b1 = payload(30_000, 1);
        byte[] a1 = payload(30_000, 2);
        d.deserialize(stream(updateRowsEvent(b1, a1)));

        byte[] b2 = payload(30_000, 3);
        byte[] a2 = payload(30_000, 4);
        UpdateRowsEventData second = d.deserialize(stream(updateRowsEvent(b2, a2)));
        assertTrue(((LobRef) second.getRows().get(0).getKey()[1]).isDiscarded(),
                "第二个事件的前镜像判定必须重新从『前』开始");
        assertFalse(((LobRef) second.getRows().get(0).getValue()[1]).isDiscarded());
    }

    @Test
    @DisplayName("DELETE：行镜像一律不落盘")
    void deleteDiscardsRowImage() throws Exception {
        byte[] blob = payload(45_000, 9);
        var d = new SignAwareRowsDeserializers.Delete(tableMap, policy(1024));
        var data = d.deserialize(stream(writeRowsEvent(blob)));
        LobRef ref = assertInstanceOf(LobRef.class, data.getRows().get(0)[1]);
        assertTrue(ref.isDiscarded());
        assertEquals(0, spillDir.listFiles().length, "DELETE 不该产生任何落盘文件");
    }

    @Test
    @DisplayName("落盘走临时名 + 原子改名，最终名按位点确定 ⇒ 重放同名覆盖")
    void finalizeNameIsPositionDeterministic() throws Exception {
        byte[] blob = payload(30_000, 11);
        var d = new SignAwareRowsDeserializers.Write(tableMap, policy(1024));
        LobRef tmpRef = (LobRef) d.deserialize(stream(writeRowsEvent(blob))).getRows().get(0)[1];
        assertTrue(tmpRef.file().endsWith(LobSpillWriter.TMP_SUFFIX), "落盘中必须是临时名");

        LobRef finalRef = spillWriter.finalizeName(tmpRef, "mysql-bin.000042", 1234567L, 1);
        assertEquals("mysql-bin.000042#1234567#c1.lob", finalRef.file());
        assertArrayEquals(blob, spillContent(finalRef));

        // capture 重连重放同一个事件 ⇒ 同样的最终名 ⇒ 覆盖写而不是堆出第二份 1GB
        LobRef again = (LobRef) d.deserialize(stream(writeRowsEvent(blob))).getRows().get(0)[1];
        LobRef finalAgain = spillWriter.finalizeName(again, "mysql-bin.000042", 1234567L, 1);
        assertEquals(finalRef.file(), finalAgain.file());
        assertEquals(1, spillDir.listFiles().length, "重放不能留下第二份内容");
    }

    @Test
    @DisplayName("流提前结束 → 报错，绝不能写出被截断的落盘文件还当成功")
    void truncatedStreamFails() {
        byte[] blob = payload(50_000, 13);
        byte[] event = writeRowsEvent(blob);
        byte[] truncated = new byte[event.length - 10_000];
        System.arraycopy(event, 0, truncated, 0, truncated.length);

        var d = new SignAwareRowsDeserializers.Write(tableMap, policy(1024));
        assertThrows(IOException.class, () -> d.deserialize(stream(truncated)));
    }

    @Test
    @DisplayName("孤儿临时文件可按时长清理")
    void cleansOrphanTempFiles() throws Exception {
        File orphan = new File(spillDir, "spill-1-999" + LobSpillWriter.TMP_SUFFIX);
        assertTrue(orphan.createNewFile());
        assertTrue(orphan.setLastModified(System.currentTimeMillis() - 3600_000));
        File fresh = new File(spillDir, "spill-2-888" + LobSpillWriter.TMP_SUFFIX);
        assertTrue(fresh.createNewFile());

        assertEquals(1, spillWriter.cleanupOrphanTemp(600_000));
        assertFalse(orphan.exists());
        assertTrue(fresh.exists(), "还在写的临时文件不能被清掉");
    }

    @Test
    @DisplayName("引用的文本形式可往返解析（.cap 行要用）")
    void refMarkerRoundTrip() {
        LobRef ref = LobRef.spilled("mysql-bin.000042#1234#c0.lob", 1073741824L, "abc123");
        LobRef parsed = LobRef.parse(ref.toMarker());
        assertNotNull(parsed);
        assertEquals(ref.file(), parsed.file());
        assertEquals(ref.length(), parsed.length());
        assertEquals(ref.md5(), parsed.md5());

        LobRef discarded = LobRef.discarded(999, "def456");
        LobRef parsedDiscarded = LobRef.parse(discarded.toMarker());
        assertTrue(parsedDiscarded.isDiscarded());
        assertEquals(999, parsedDiscarded.length());

        assertNull(LobRef.parse("0xdeadbeef"), "普通十六进制值不能被误认成引用");
        assertNull(LobRef.parse(null));
    }
}
