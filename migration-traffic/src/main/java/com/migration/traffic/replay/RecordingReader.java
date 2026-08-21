package com.migration.traffic.replay;

import com.google.gson.Gson;
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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

/**
 * 流式读取录制：manifest + 按段号顺序的分段文件。
 *
 * <p>与 {@code RecordingRecovery} 同样<b>不用 BufferedReader</b>：录制文件可能来自
 * 被 kill 的捕获任务（gzip 无 trailer），按块填充的读法会把最后一块里已解出的记录连同
 * 缓冲区一起丢掉。回放要尽可能多地还原，最后半行丢弃即可。
 */
public final class RecordingReader implements Iterable<TrafficRecord>, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RecordingReader.class);

    private static final Gson GSON = new Gson();

    private final File dir;
    private final RecordingManifest manifest;
    private final List<File> segments = new ArrayList<>();
    private long droppedLines;

    public RecordingReader(File dir) throws IOException {
        this.dir = dir;
        File mf = new File(dir, "manifest.json");
        if (!mf.isFile()) {
            throw new IOException("录制文件缺少 manifest.json: " + dir.getAbsolutePath());
        }
        this.manifest = GSON.fromJson(
                new String(Files.readAllBytes(mf.toPath()), StandardCharsets.UTF_8),
                RecordingManifest.class);
        if (manifest == null || manifest.segments == null) {
            throw new IOException("manifest 内容不合法: " + mf.getAbsolutePath());
        }
        manifest.segments.sort((a, b) -> Integer.compare(a.seq, b.seq));
        for (RecordingManifest.Segment s : manifest.segments) {
            File f = new File(dir, s.file);
            if (!f.isFile()) {
                throw new IOException("录制分段缺失: " + s.file + "（录制文件不完整，拒绝回放）");
            }
            segments.add(f);
        }
        if (segments.isEmpty()) {
            logger.warn("录制里没有任何分段，回放将立即结束");
        }
    }

    public RecordingManifest manifest() {
        return manifest;
    }

    public long droppedLines() {
        return droppedLines;
    }

    @Override
    public Iterator<TrafficRecord> iterator() {
        return new RecordIterator();
    }

    @Override
    public void close() {
        // 每个分段读完即关，无需额外资源
    }

    private final class RecordIterator implements Iterator<TrafficRecord> {
        private int segIndex = -1;
        private Iterator<String> lines = List.<String>of().iterator();
        private TrafficRecord next;

        RecordIterator() {
            advance();
        }

        private void advance() {
            next = null;
            while (true) {
                while (lines.hasNext()) {
                    String line = lines.next();
                    if (line.isBlank()) continue;
                    try {
                        next = TrafficJson.read(line);
                        return;
                    } catch (IOException bad) {
                        droppedLines++;
                    }
                }
                segIndex++;
                if (segIndex >= segments.size()) return;
                lines = readSegment(segments.get(segIndex)).iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public TrafficRecord next() {
            if (next == null) throw new NoSuchElementException();
            TrafficRecord r = next;
            advance();
            return r;
        }
    }

    /** 整段解压成行。分段有大小上限（默认 64MB 压缩），一次装入是可控的。 */
    private List<String> readSegment(File f) {
        List<String> out = new ArrayList<>();
        ByteArrayOutputStream pending = new ByteArrayOutputStream(4096);
        try (FileInputStream fin = new FileInputStream(f);
             GZIPInputStream gz = new GZIPInputStream(fin, 32 * 1024)) {
            byte[] buf = new byte[64 * 1024];
            while (true) {
                int n;
                try {
                    n = gz.read(buf);
                } catch (IOException truncated) {
                    logger.warn("分段 {} 的 gzip 流不完整（录制来自被强杀的捕获任务），"
                            + "保留已解出的内容: {}", f.getName(), truncated.getMessage());
                    break;
                }
                if (n < 0) break;
                for (int i = 0; i < n; i++) {
                    if (buf[i] == (byte) 10) {
                        if (pending.size() > 0) {
                            out.add(new String(pending.toByteArray(), StandardCharsets.UTF_8));
                            pending.reset();
                        }
                    } else {
                        pending.write(buf[i]);
                    }
                }
            }
        } catch (IOException e) {
            logger.warn("分段 {} 读取失败: {}", f.getName(), e.getMessage());
        }
        if (pending.size() > 0) {
            droppedLines++;     // 没有换行结尾 = 半行，丢弃
        }
        return out;
    }
}
