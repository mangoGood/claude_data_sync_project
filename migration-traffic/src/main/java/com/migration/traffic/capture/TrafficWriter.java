package com.migration.traffic.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.migration.common.io.AtomicFileWriter;
import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficJson;
import com.migration.traffic.model.TrafficRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

/**
 * 录制写出：分段 JSONL + gzip，外加 manifest。
 *
 * <p>分段而不是单个大文件，是为了让<b>崩溃只损失最后一段的尾巴</b>：
 * 每段独立 gzip、独立算 SHA-256，读的时候一段坏了不影响前面的段。
 *
 * <p>gzip 流用 {@code syncFlush=true} 构造。默认的 {@code GZIPOutputStream.flush()}
 * 只把数据推给 Deflater，<b>不产生完整的 deflate 块</b>——进程此刻被 kill，
 * 已经"flush 过"的内容在读端依然解不出来。牺牲一点压缩率换取"写下去的就读得到"。
 */
public final class TrafficWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TrafficWriter.class);

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public static final String MANIFEST_NAME = "manifest.json";

    private final File dir;
    private final RecordingManifest manifest;
    private final long segmentMaxRecords;
    private final long segmentMaxBytes;
    private final long flushIntervalMs;

    private FileOutputStream fos;
    private DigestOutputStream digestOut;
    private BufferedWriter writer;
    private RecordingManifest.Segment current;
    private int nextSeq = 1;
    private long lastFlushAt;

    private final StringBuilder lineBuf = new StringBuilder(512);

    public TrafficWriter(File dir, RecordingManifest manifest,
                         long segmentMaxRecords, long segmentMaxBytes, long flushIntervalMs) {
        this.dir = dir;
        this.manifest = manifest;
        this.segmentMaxRecords = Math.max(1000L, segmentMaxRecords);
        this.segmentMaxBytes = Math.max(1L << 20, segmentMaxBytes);
        this.flushIntervalMs = Math.max(200L, flushIntervalMs);
        // 段号取"manifest 里的最大"与"目录里实际存在的最大"之中更大的那个。
        // 只看 manifest 会在恢复场景下把段号退回 1，然后<b>覆盖掉上一轮的分段文件</b>——
        // 那不是"少记一段"，是把已经录到的数据直接删了。
        int maxSeq = 0;
        for (RecordingManifest.Segment s : manifest.segments) {
            maxSeq = Math.max(maxSeq, s.seq);
        }
        maxSeq = Math.max(maxSeq, maxSegmentSeqOnDisk(dir));
        this.nextSeq = maxSeq + 1;
    }

    /** 追加一条记录。 */
    public void write(TrafficRecord r) throws IOException {
        if (writer == null || current.records >= segmentMaxRecords || currentBytes() >= segmentMaxBytes) {
            rollSegment();
        }
        lineBuf.setLength(0);
        TrafficJson.write(lineBuf, r);
        lineBuf.append('\n');
        writer.write(lineBuf.toString());

        if (current.records == 0) {
            current.firstN = r.n;
            current.firstT = r.t;
        }
        current.records++;
        current.lastN = r.n;
        current.lastT = r.t;
        countStats(r);

        long now = System.currentTimeMillis();
        if (now - lastFlushAt >= flushIntervalMs) {
            flush();
            lastFlushAt = now;
        }
    }

    private void countStats(TrafficRecord r) {
        RecordingManifest.Stats s = manifest.stats;
        s.total++;
        if (r.rd) s.redacted++;
        StatementClass k = r.k;
        if (k == null) {
            // Connect / Quit：不是语句，单独计
            s.connEvents++;
            return;
        }
        switch (k) {
            case SELECT: s.select++; break;
            case DML: s.dml++; break;
            case DDL: s.ddl++; break;
            case DCL: s.dcl++; break;
            case TCL: s.tcl++; break;
            default: s.other++;
        }
    }

    /** 把已写内容推到磁盘，并同步一次 manifest（运行期的 manifest 永远 {@code sealed=false}）。 */
    public void flush() throws IOException {
        if (writer != null) {
            writer.flush();
            current.bytes = currentBytes();
        }
        writeManifest(false);
    }

    /** 强制换段（供上层在时间轴出现空洞时调用，让空洞落在段边界上，恢复时更好定位）。 */
    public void rollSegment() throws IOException {
        closeSegment();
        current = new RecordingManifest.Segment();
        current.seq = nextSeq++;
        current.file = String.format("seg-%08d.trf.gz", current.seq);
        File f = new File(dir, current.file);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("无法创建录制目录: " + dir.getAbsolutePath());
        }
        fos = new FileOutputStream(f);
        try {
            digestOut = new DigestOutputStream(fos, MessageDigest.getInstance("SHA-256"));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("JVM 缺少 SHA-256", e);
        }
        // syncFlush=true：见类注释
        GZIPOutputStream gz = new GZIPOutputStream(digestOut, 32 * 1024, true);
        writer = new BufferedWriter(new OutputStreamWriter(gz, StandardCharsets.UTF_8), 64 * 1024);
        manifest.segments.add(current);
        lastFlushAt = System.currentTimeMillis();
        logger.info("录制新分段: {}", current.file);
    }

    private long currentBytes() {
        if (fos == null) return 0L;
        try {
            return fos.getChannel().position();
        } catch (IOException e) {
            return 0L;
        }
    }

    private void closeSegment() throws IOException {
        if (writer == null) return;
        writer.flush();
        writer.close();   // 关掉 gzip 流才会写出 trailer
        current.bytes = new File(dir, current.file).length();
        current.sha256 = hex(digestOut.getMessageDigest().digest());
        writer = null;
        fos = null;
        digestOut = null;
    }

    /**
     * 落 manifest。
     *
     * <p>{@code sealed=true} 只在正常结束时写一次。运行期不写，是因为进程随时可能被 kill——
     * 一个标着 sealed 但其实还在写的 manifest 会让读端相信"分段清单是完整的"，
     * 而实际上最后一段的 records/lastN 都还是旧值。
     */
    public void writeManifest(boolean seal) throws IOException {
        manifest.sealed = seal;
        if (seal) {
            manifest.sha256 = aggregateSha();
        }
        AtomicFileWriter.writeString(new File(dir, MANIFEST_NAME), GSON.toJson(manifest));
    }

    /**
     * 全局校验和：对"分段序号 + 分段 SHA-256"的清单再算一次 SHA-256。
     *
     * <p>不重读几 GB 的分段内容，也<b>绝不用 XOR/加和之类的聚合</b>——
     * 那类聚合对"两段内容互换"完全无感，等于没校验。
     */
    private String aggregateSha() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (RecordingManifest.Segment s : manifest.segments) {
                md.update((s.seq + ":" + (s.sha256 == null ? "" : s.sha256) + "\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
            return hex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    /** 正常收尾：封口 manifest。 */
    public void seal(String endWall, long durationMs) throws IOException {
        closeSegment();
        manifest.endWall = endWall;
        manifest.durationMs = durationMs;
        writeManifest(true);
        logger.info("录制已封口: 分段={}, 记录={}, 字节={}",
                manifest.segments.size(), manifest.totalRecords(), manifest.totalBytes());
    }

    @Override
    public void close() throws IOException {
        try {
            closeSegment();
        } finally {
            writeManifest(manifest.sealed);
        }
    }

    public RecordingManifest manifest() {
        return manifest;
    }

    /** 目录里已存在的最大分段号（含 manifest 没记上的那些）。 */
    static int maxSegmentSeqOnDisk(File dir) {
        File[] files = dir.listFiles((d, name) -> name.startsWith("seg-") && name.endsWith(".trf.gz"));
        int max = 0;
        if (files != null) {
            for (File f : files) {
                max = Math.max(max, RecordingRecovery.seqOf(f.getName()));
            }
        }
        return max;
    }

    /** 从磁盘读回 manifest（恢复用）；不存在返回 null。 */
    public static RecordingManifest loadManifest(File dir) {
        File f = new File(dir, MANIFEST_NAME);
        if (!f.isFile()) return null;
        try {
            String json = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            return GSON.fromJson(json, RecordingManifest.class);
        } catch (Exception e) {
            logger.warn("manifest 读取失败: {}", e.getMessage());
            return null;
        }
    }

    public static RecordingManifest newManifest(String taskId, String taskName,
                                                SourceFingerprint fp, String t0Wall, long t0EpochMicros) {
        RecordingManifest m = new RecordingManifest();
        m.captureTaskId = taskId;
        m.captureTaskName = taskName;
        m.source = fp;
        m.t0Wall = t0Wall;
        m.t0EpochMicros = t0EpochMicros;
        return m;
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format(Locale.ROOT, "%02x", x));
        return sb.toString();
    }
}
