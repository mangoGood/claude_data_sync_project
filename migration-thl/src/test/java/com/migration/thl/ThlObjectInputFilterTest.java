package com.migration.thl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * THL 反序列化白名单判据。
 *
 * <p>守两件事：<b>正常 THL 照读不误</b>（白名单别把自己业务锁死），
 * 以及<b>非白名单类被拒</b>（gadget 链进不来）。
 * 第一条比第二条更容易出事——白名单漏一个 metadata 里用到的类型，
 * 表现是任务跑到一半突然读不动 THL。
 */
class ThlObjectInputFilterTest {

    @TempDir
    Path tmp;

    private static THLEvent sampleEvent() {
        THLEvent e = new THLEvent(42L, "binlog.000035:172573", "src-1");
        e.setShardId("db1.t1");
        e.setComment("unit-test");
        e.setLocalEnqueueTstamp(new Timestamp(System.currentTimeMillis()));
        e.setData("INSERT INTO t VALUES (1)".getBytes());
        HashMap<String, Object> md = new HashMap<>();
        md.put("txId", "tx-1");          // String
        md.put("rowCount", 3);           // Integer
        md.put("lastFrag", Boolean.TRUE); // Boolean
        md.put("seqno", 42L);            // Long
        md.put("ratio", 1.5d);           // Double
        // 类型化值管道（Oracle/PG）往 metadata 里放的就是这个形状：
        // ArrayList<ArrayList<Object>>，元素是 String / Boolean / byte[] / null
        java.util.ArrayList<java.util.ArrayList<Object>> rows = new java.util.ArrayList<>();
        java.util.ArrayList<Object> row = new java.util.ArrayList<>();
        row.add("v1");
        row.add(Boolean.FALSE);
        row.add(new byte[]{1, 2, 3});
        row.add(null);
        rows.add(row);
        md.put("typed_rows", rows);
        e.setMetadata(md);
        return e;
    }

    @Test
    @DisplayName("正常 THL 事件写入后能原样读回——白名单不能把业务锁死")
    void readsNormalEvent() throws Exception {
        File f = tmp.resolve("ok.thl").toFile();
        try (THLFileWriter w = new THLFileWriter(f.getAbsolutePath())) {
            w.writeEvent(sampleEvent());
        }
        try (THLFileReader r = new THLFileReader(f.getAbsolutePath())) {
            THLEvent got = r.readEvent();
            assertNotNull(got, "应读到事件");
            assertEquals(42L, got.getSeqno());
            assertEquals("binlog.000035:172573", got.getEventId());
            assertEquals("db1.t1", got.getShardId());
            assertEquals("INSERT INTO t VALUES (1)", new String(got.getData()));
            assertEquals("tx-1", got.getMetadata().get("txId"));
            assertEquals(3, got.getMetadata().get("rowCount"));
            assertEquals(Boolean.TRUE, got.getMetadata().get("lastFrag"));
            assertEquals(42L, got.getMetadata().get("seqno"));
            assertEquals(1.5d, got.getMetadata().get("ratio"));
            @SuppressWarnings("unchecked")
            var gotRows = (java.util.List<java.util.List<Object>>) got.getMetadata().get("typed_rows");
            assertEquals(1, gotRows.size());
            assertEquals("v1", gotRows.get(0).get(0));
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) gotRows.get(0).get(2));
            assertNull(gotRows.get(0).get(3));
        }
    }

    @Test
    @DisplayName("非白名单类被拒——这正是 gadget 链的入口")
    void rejectsNonWhitelistedClass() throws Exception {
        // 直接构造一个"合法 THL 分帧、但 payload 里是别的类"的文件，
        // 模拟攻击者能落文件的场景
        File f = tmp.resolve("evil.thl").toFile();
        byte[] payload;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            // javax.swing.* / commons-collections 这类才是真 gadget，这里用一个
            // 必定在类路径上、但不在白名单里的可序列化类代表它们
            oos.writeObject(new java.util.concurrent.atomic.AtomicLong(1));
            oos.flush();
            payload = baos.toByteArray();
        }
        try (FileOutputStream fos = new FileOutputStream(f);
             DataOutputStream out = new DataOutputStream(fos)) {
            out.write(THLFileReader.FRAMED_MAGIC);
            out.writeLong(1L);
            out.writeInt(payload.length);
            out.write(payload);
        }

        try (THLFileReader r = new THLFileReader(f.getAbsolutePath())) {
            Exception ex = assertThrows(Exception.class, r::readEvent);
            String msg = String.valueOf(ex);
            assertTrue(ex instanceof InvalidClassException
                            || msg.contains("filter") || msg.contains("REJECTED"),
                    "应因过滤器拒绝而失败，实际: " + msg);
        }
    }

    @Test
    @DisplayName("过滤器本身：白名单内 ALLOWED、白名单外 REJECTED")
    void filterDecisions() {
        ObjectInputFilter f = ThlObjectInputFilter.filter();
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, THLEvent.class));
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, String.class));
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, HashMap.class));
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, Timestamp.class));
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, byte[].class));
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, int[].class));
        // 下面两条是拿 74 个真实 THL 实扫出来的遗漏（见 test_scripts/security/
        // thl_filter_realfiles.sh）：只读代码都发现不了，但漏了就有链路读不动 THL。
        assertEquals(ObjectInputFilter.Status.ALLOWED, decide(f, Object[].class),
                "ArrayList 底层是 Object[]，剥到元素类型是 java.lang.Object");
        assertEquals(ObjectInputFilter.Status.ALLOWED,
                decide(f, com.migration.common.lob.LobRef.class),
                "大字段链路把落盘引用放进 metadata");

        assertEquals(ObjectInputFilter.Status.REJECTED,
                decide(f, java.util.concurrent.atomic.AtomicLong.class));
        assertEquals(ObjectInputFilter.Status.REJECTED, decide(f, java.io.File.class));
        assertEquals(ObjectInputFilter.Status.REJECTED, decide(f, javax.naming.Reference.class));
    }

    @Test
    @DisplayName("深度与数组长度限额生效（序列化炸弹）")
    void enforcesLimits() {
        ObjectInputFilter f = ThlObjectInputFilter.filter();
        assertEquals(ObjectInputFilter.Status.REJECTED,
                f.checkInput(new Info(THLEvent.class, 1_000, 10, 10, 10)), "超深度应拒");
        assertEquals(ObjectInputFilter.Status.REJECTED,
                f.checkInput(new Info(byte[].class, 1, 128L * 1024 * 1024, 10, 10)), "超数组长度应拒");
        assertEquals(ObjectInputFilter.Status.REJECTED,
                f.checkInput(new Info(THLEvent.class, 1, -1, 100_000, 10)), "超引用数应拒");
    }

    private static ObjectInputFilter.Status decide(ObjectInputFilter f, Class<?> c) {
        return f.checkInput(new Info(c, 1, -1, 1, 1));
    }

    /** 最小 FilterInfo 实现，用于直接驱动过滤器判定。 */
    private record Info(Class<?> serialClass, long depth, long arrayLength,
                        long references, long streamBytes) implements ObjectInputFilter.FilterInfo {
    }
}
