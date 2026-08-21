package com.migration.traffic.capture;

import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.TrafficJson;
import com.migration.traffic.model.TrafficRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * 崩溃后重建 manifest。
 *
 * <p>运行期的 manifest 永远是 {@code sealed=false} 且统计滞后于实际写入
 * （最后一次周期性落盘之后还写进去的记录不在里面）。进程被 kill 之后，
 * <b>唯一可信的是分段文件本身</b>，所以恢复时把每个分段整体扫一遍重算。
 *
 * <p>最后一行不完整是<b>正常现象</b>（gzip 缓冲区没写完就断电/被杀），整行丢弃即可——
 * 这和仓库里 {@code .cap 半行} 的处理是同一件事：宁可少一条，不能把半条当完整的用。
 */
public final class RecordingRecovery {

    private static final Logger logger = LoggerFactory.getLogger(RecordingRecovery.class);

    private RecordingRecovery() {
    }

    /** 扫描结果。 */
    public static final class Scan {
        public long maxN;
        public long lastT;
        public long records;
        public int droppedTailLines;
    }

    /**
     * 按目录里实际存在的分段文件重建 {@code manifest.segments} 与统计，
     * 并返回全局最大序号与最后偏移（续录要接着它们往下走）。
     */
    public static Scan rebuild(File dir, RecordingManifest manifest) {
        Scan scan = new Scan();
        File[] files = dir.listFiles((d, name) -> name.startsWith("seg-") && name.endsWith(".trf.gz"));
        if (files == null || files.length == 0) {
            manifest.segments.clear();
            resetStats(manifest);
            return scan;
        }
        Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));

        List<RecordingManifest.Segment> rebuilt = new ArrayList<>();
        resetStats(manifest);
        for (File f : files) {
            RecordingManifest.Segment seg = scanSegment(f, manifest, scan);
            if (seg == null) continue;
            rebuilt.add(seg);
        }
        manifest.segments.clear();
        manifest.segments.addAll(rebuilt);
        scan.records = manifest.stats.total;
        if (scan.droppedTailLines > 0) {
            logger.warn("恢复时丢弃了 {} 条不完整的尾行（崩溃残留，属正常）", scan.droppedTailLines);
        }
        logger.info("录制恢复完成: 分段={}, 记录={}, 最大序号={}, 最后偏移={}us",
                rebuilt.size(), scan.records, scan.maxN, scan.lastT);
        return scan;
    }

    private static void resetStats(RecordingManifest manifest) {
        manifest.stats.total = 0;
        manifest.stats.select = 0;
        manifest.stats.dml = 0;
        manifest.stats.ddl = 0;
        manifest.stats.dcl = 0;
        manifest.stats.tcl = 0;
        manifest.stats.other = 0;
        manifest.stats.connEvents = 0;
        manifest.stats.redacted = 0;
    }

    private static RecordingManifest.Segment scanSegment(File f, RecordingManifest manifest, Scan scan) {
        RecordingManifest.Segment seg = new RecordingManifest.Segment();
        seg.file = f.getName();
        seg.seq = seqOf(f.getName());
        seg.bytes = f.length();
        seg.sha256 = sha256Of(f);

        long count = 0;
        LineSink sink = new LineSink();
        readGzipTolerantly(f, sink, scan);
        for (String line : sink.lines) {
            TrafficRecord rec;
            try {
                rec = TrafficJson.read(line);
            } catch (IOException bad) {
                scan.droppedTailLines++;
                continue;
            }
            if (count == 0) {
                seg.firstN = rec.n;
                seg.firstT = rec.t;
            }
            count++;
            seg.lastN = rec.n;
            seg.lastT = rec.t;
            if (rec.n > scan.maxN) scan.maxN = rec.n;
            if (rec.t > scan.lastT) scan.lastT = rec.t;
            tally(manifest, rec);
        }
        seg.records = count;
        return count == 0 ? null : seg;
    }

    /** 累积完整行；未以换行结尾的尾巴是崩溃残留，丢弃。 */
    private static final class LineSink {
        final List<String> lines = new ArrayList<>();
        final ByteArrayOutputStream pending = new ByteArrayOutputStream(4096);
        boolean droppedTail;

        void feed(byte[] buf, int len) {
            for (int i = 0; i < len; i++) {
                if (buf[i] == (byte) 10) {
                    if (pending.size() > 0) {
                        lines.add(new String(pending.toByteArray(), StandardCharsets.UTF_8));
                        pending.reset();
                    }
                } else {
                    pending.write(buf[i]);
                }
            }
        }

        void finish() {
            if (pending.size() > 0) {
                // 没有换行结尾 = 半行，整行丢弃（与 .cap 半行同一处理）
                droppedTail = true;
                pending.reset();
            }
        }
    }

    /**
     * 尽力解压：进程被 kill 时 gzip 流没有 trailer，读到末尾必抛 {@code EOFException}。
     *
     * <p>关键在于<b>不能用 {@code BufferedReader}</b>：它按 8KB 块填充，
     * 填充过程中底层抛异常时，<b>这一块里已经解出来的内容会连同缓冲区一起丢掉</b>——
     * 于是一个明明有 18 行完整数据的分段被判成 0 条，数据静默消失。
     * 这里改成自己按块读、边读边切行，异常只终止读取、不丢已得到的字节。
     */
    private static void readGzipTolerantly(File f, LineSink sink, Scan scan) {
        try (FileInputStream fin = new FileInputStream(f);
             GZIPInputStream gz = new GZIPInputStream(fin, 32 * 1024)) {
            byte[] buf = new byte[64 * 1024];
            while (true) {
                int n;
                try {
                    n = gz.read(buf);
                } catch (IOException truncated) {
                    // 没有 trailer / 最后一个 deflate 块不完整：到此为止，保留已读到的
                    logger.warn("分段 {} 的 gzip 流不完整（崩溃残留），保留已解出的内容: {}",
                            f.getName(), truncated.getMessage());
                    break;
                }
                if (n < 0) break;
                sink.feed(buf, n);
            }
        } catch (IOException e) {
            logger.warn("分段 {} 打开失败: {}", f.getName(), e.getMessage());
        }
        sink.finish();
        if (sink.droppedTail) {
            scan.droppedTailLines++;
        }
    }

    private static void tally(RecordingManifest manifest, TrafficRecord rec) {
        RecordingManifest.Stats s = manifest.stats;
        s.total++;
        if (rec.rd) s.redacted++;
        if (rec.k == null) {
            s.connEvents++;
            return;
        }
        switch (rec.k) {
            case SELECT: s.select++; break;
            case DML: s.dml++; break;
            case DDL: s.ddl++; break;
            case DCL: s.dcl++; break;
            case TCL: s.tcl++; break;
            default: s.other++;
        }
    }

    static int seqOf(String name) {
        try {
            return Integer.parseInt(name.substring("seg-".length(), name.indexOf(".trf.gz")));
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private static String sha256Of(File f) {
        try (FileInputStream in = new FileInputStream(f)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
            return TrafficWriter.hex(md.digest());
        } catch (IOException | NoSuchAlgorithmException e) {
            return null;
        }
    }
}
