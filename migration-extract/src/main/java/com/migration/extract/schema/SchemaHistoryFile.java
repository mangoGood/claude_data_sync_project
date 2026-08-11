package com.migration.extract.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 表结构时序库的落盘形态：{@code files/<taskId>/schema_history.jsonl}，一行一个版本。
 *
 * <p>存的是<b>施加之后的完整结构</b>而不是 DDL 本身。DDL 稀疏，存全量换来"重启即加载、
 * 无需从头重放"，顺带还是一份能直接读的审计记录。
 *
 * <p><b>为什么是 jsonl 而不是一个大 JSON</b>：崩溃会在文件尾部留下半条记录。一行一条时
 * 读到坏行就停在那儿、前面的全部有效；一个大 JSON 则整个文件报废。
 * 这也是 {@link SchemaJson} 刻意不开 pretty print 的原因。
 *
 * <p>每次写入后 {@code flush}——DDL 稀疏（一天可能就几条），攒批省下的那点 IO
 * 换不来"崩溃时丢掉刚算出的版本"这个代价。
 */
public class SchemaHistoryFile {

    private static final Logger logger = LoggerFactory.getLogger(SchemaHistoryFile.class);

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    /** 落盘记录的形态。字段名短一点——一张宽表的结构本身就不小。 */
    static class Record {
        String f;                     // binlog 文件
        long p;                       // binlog 位置
        String db;
        String tbl;
        String kind;                  // CREATED / ALTERED / DROPPED
        TableSchema schema;           // DROPPED 时为 null
    }

    private final Path path;

    public SchemaHistoryFile(String path) {
        this.path = new File(path).toPath();
    }

    public Path getPath() {
        return path;
    }

    /**
     * 装载整个历史到时序库。
     *
     * <p>坏行的处置：<b>停在第一个坏行</b>，不跳过它继续读后面的。历史是有序的版本链，
     * 中间少一条与末尾少一条完全不同——前者会让某张表停在错的版本上并一路用下去，
     * 后者只是少了最新几版、由重放补回来。所以只容忍尾部截断。
     *
     * @return 装载的版本数
     */
    public int load(SchemaTimeline timeline) {
        if (!Files.isRegularFile(path)) {
            return 0;
        }
        int loaded = 0;
        int lineNo = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isEmpty()) {
                    continue;
                }
                Record r;
                try {
                    r = GSON.fromJson(line, Record.class);
                } catch (Exception e) {
                    logger.warn("表结构历史第 {} 行解析失败（多半是崩溃留下的半条），"
                            + "在此截断，之后的版本由重放补回: {}", lineNo, e.getMessage());
                    break;
                }
                if (r == null || r.kind == null || r.tbl == null) {
                    logger.warn("表结构历史第 {} 行缺字段，在此截断", lineNo);
                    break;
                }
                timeline.append(SchemaTimeline.entryOf(r.f, r.p, r.db, r.tbl,
                        SchemaChange.Kind.valueOf(r.kind), r.schema));
                loaded++;
            }
        } catch (IOException e) {
            logger.error("读表结构历史失败 {}: {}", path, e.getMessage());
        }
        if (loaded > 0) {
            logger.info("从 {} 装载了 {} 个表结构版本（{} 张表）",
                    path, loaded, timeline.tableCount());
        }
        return loaded;
    }

    /** 追加一个版本并立即落盘。 */
    public void append(SchemaTimeline.Entry entry) {
        Record r = new Record();
        r.f = entry.getBinlogFile();
        r.p = entry.getBinlogPos();
        r.db = entry.getDatabase();
        r.tbl = entry.getTable();
        r.kind = entry.getKind().name();
        r.schema = entry.getSchema();

        String line = GSON.toJson(r);
        if (line.indexOf('\n') >= 0) {
            // 不该发生（Gson 不产出裸换行），但真发生了就会把一条切成两行、
            // 让后面所有记录都读不出来——宁可在这里炸掉
            throw new IllegalStateException("表结构历史记录里出现换行，会破坏 jsonl 的按行恢复");
        }
        try {
            ensureParent();
            try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(path.toFile(), true), StandardCharsets.UTF_8))) {
                w.write(line);
                w.newLine();
                w.flush();
            }
        } catch (IOException e) {
            logger.error("写表结构历史失败 {}: {}", path, e.getMessage());
        }
    }

    /**
     * 按时序库当前内容整体重写——裁剪之后调用。
     *
     * <p>先写临时文件再原子替换：直接截断重写时崩在中间，历史就只剩半截，
     * 而这是唯一一份能重建时序库的东西。
     */
    public void rewrite(SchemaTimeline timeline) {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        try {
            ensureParent();
            List<String> lines = new ArrayList<>();
            for (SchemaTimeline.Entry entry : timeline.allEntries()) {
                Record r = new Record();
                r.f = entry.getBinlogFile();
                r.p = entry.getBinlogPos();
                r.db = entry.getDatabase();
                r.tbl = entry.getTable();
                r.kind = entry.getKind().name();
                r.schema = entry.getSchema();
                lines.add(GSON.toJson(r));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            logger.info("表结构历史已重写: {} 条", lines.size());
        } catch (IOException e) {
            logger.error("重写表结构历史失败 {}: {}", path, e.getMessage());
        }
    }

    /** 清空（位点全作废时用，见 {@code CheckpointCleaner}）。 */
    public void delete() {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            logger.warn("删除表结构历史失败 {}: {}", path, e.getMessage());
        }
    }

    private void ensureParent() throws IOException {
        Path parent = path.getParent();
        if (parent != null && !Files.isDirectory(parent)) {
            Files.createDirectories(parent);
        }
    }
}
