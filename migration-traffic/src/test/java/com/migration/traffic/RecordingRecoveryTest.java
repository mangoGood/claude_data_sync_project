package com.migration.traffic;

import com.migration.traffic.capture.RecordingRecovery;
import com.migration.traffic.capture.TrafficWriter;
import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("录制崩溃恢复")
class RecordingRecoveryTest {

    private static RecordingManifest freshManifest() {
        SourceFingerprint fp = new SourceFingerprint();
        fp.serverUuid = "uuid-1";
        return TrafficWriter.newManifest("t1", "task", fp, "2026-08-20T10:00:00+08:00", 1_000_000L);
    }

    private static TrafficRecord rec(long n, long t) {
        TrafficRecord r = new TrafficRecord();
        r.n = n;
        r.t = t;
        r.s = 100;
        r.c = TrafficRecord.CMD_QUERY;
        r.k = StatementClass.DML;
        r.db = "d";
        r.q = "INSERT INTO t VALUES (" + n + ")";
        return r;
    }

    /** 写一个分段并<b>不</b>正常关闭，再把文件尾部截掉，模拟 kill -9。 */
    private static void writeThenTruncate(File dir, int records, int chopBytes) throws IOException {
        RecordingManifest m = freshManifest();
        TrafficWriter w = new TrafficWriter(dir, m, 1_000_000L, 1L << 30, 50L);
        for (int i = 1; i <= records; i++) {
            w.write(rec(i, i * 1000L));
        }
        w.flush();          // syncFlush：已写内容此刻应当可解出
        // 故意不 close()：gzip trailer 不会写出，正是 kill -9 的现场
        File seg = new File(dir, "seg-00000001.trf.gz");
        if (chopBytes > 0) {
            try (RandomAccessFile raf = new RandomAccessFile(seg, "rw")) {
                raf.setLength(Math.max(1, raf.length() - chopBytes));
            }
        }
    }

    @Test
    @DisplayName("gzip 没有 trailer 时，已写入的记录必须全部救回来 —— 不能整段判 0 条")
    void truncatedGzipKeepsWrittenRecords(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        writeThenTruncate(dir, 40, 0);

        RecordingManifest m = freshManifest();
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(dir, m);

        // BufferedReader 那条路会在第一次填充缓冲区时抛异常、连同已解出的内容一起丢掉，
        // 结果就是 40 条变 0 条、整段数据静默消失。
        assertEquals(40, scan.records, "被 kill 的分段里已落盘的记录必须全部恢复");
        assertEquals(40, scan.maxN);
        assertEquals(40_000L, scan.lastT);
        assertEquals(1, m.segments.size());
        assertEquals(40, m.segments.get(0).records);
        assertEquals(1, m.segments.get(0).firstN);
        assertEquals(40, m.segments.get(0).lastN);
    }

    @Test
    @DisplayName("文件尾被砍掉一截：救回大部分，半行丢弃，绝不把半行当完整记录")
    void choppedTailDropsPartialLineOnly(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        writeThenTruncate(dir, 40, 12);

        RecordingManifest m = freshManifest();
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(dir, m);

        assertTrue(scan.records > 0, "砍掉尾巴不该让整段作废");
        assertTrue(scan.records <= 40, "不可能恢复出比写入更多的记录");
        assertEquals(scan.records, m.stats.total);
        // 恢复出的记录必须是前缀（序号连续到 maxN）
        assertEquals(scan.records, scan.maxN, "恢复出的应是完整前缀，序号不能有洞");
    }

    @Test
    @DisplayName("正常封口的分段全量恢复")
    void sealedSegmentFullyRecovered(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        RecordingManifest m = freshManifest();
        try (TrafficWriter w = new TrafficWriter(dir, m, 1_000_000L, 1L << 30, 50L)) {
            for (int i = 1; i <= 25; i++) {
                w.write(rec(i, i * 1000L));
            }
            w.seal("2026-08-20T10:01:00+08:00", 60_000L);
        }
        RecordingManifest m2 = freshManifest();
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(dir, m2);
        assertEquals(25, scan.records);
        assertEquals(25, m2.stats.dml);
    }

    @Test
    @DisplayName("段号必须接着磁盘上已有的往下排 —— 退回 1 就会覆盖掉上一轮录到的数据")
    void segmentNumberingNeverOverwrites(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        // 第一轮：产出 seg-00000001
        RecordingManifest m = freshManifest();
        try (TrafficWriter w = new TrafficWriter(dir, m, 1_000_000L, 1L << 30, 50L)) {
            for (int i = 1; i <= 10; i++) {
                w.write(rec(i, i * 1000L));
            }
        }
        File first = new File(dir, "seg-00000001.trf.gz");
        assertTrue(first.isFile(), "第一轮应产出 seg-00000001");
        long firstLen = first.length();

        // 第二轮：manifest 里的段清单是空的（崩溃恢复最坏情况），仍不得覆盖已有文件
        RecordingManifest empty = freshManifest();
        try (TrafficWriter w2 = new TrafficWriter(dir, empty, 1_000_000L, 1L << 30, 50L)) {
            w2.write(rec(100, 100_000L));
        }
        assertTrue(new File(dir, "seg-00000002.trf.gz").isFile(), "第二轮必须写到新的段号");
        assertEquals(firstLen, first.length(), "第一轮的分段文件不能被改动");

        // 两段的数据都还在
        RecordingManifest scanned = freshManifest();
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(dir, scanned);
        assertEquals(11, scan.records, "两段的记录都应恢复出来");
        assertEquals(2, scanned.segments.size());
    }

    @Test
    @DisplayName("空目录：不报错，返回 0")
    void emptyDir(@TempDir Path tmp) {
        RecordingManifest m = freshManifest();
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(tmp.toFile(), m);
        assertEquals(0, scan.records);
        assertEquals(0, m.segments.size());
    }

    @Test
    @DisplayName("完全无法解压的垃圾文件按 0 条计，不让整个恢复流程崩掉")
    void garbageSegment(@TempDir Path tmp) throws IOException {
        File seg = new File(tmp.toFile(), "seg-00000001.trf.gz");
        try (FileOutputStream out = new FileOutputStream(seg)) {
            out.write("this is not gzip at all".getBytes("UTF-8"));
        }
        RecordingManifest m = freshManifest();
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(tmp.toFile(), m);
        assertEquals(0, scan.records);
    }
}
