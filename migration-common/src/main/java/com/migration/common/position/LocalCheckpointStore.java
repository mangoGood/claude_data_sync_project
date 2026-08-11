package com.migration.common.position;

import com.migration.common.io.AtomicFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地统一位点存储：{@code files/<taskId>/checkpoint/positions/<stage>[.<streamKey>].properties}。
 *
 * <p><b>它不取代任何现有载体</b>。各链路原有的落盘（capture_position.properties、两个 H2 库、
 * mongo/es 的 json、订阅进度文件）继续照原样、按原时机写——那些落盘时机是一条条缺陷修出来的
 * （"位点先于 flush 推进即丢"这类），重构它们的风险远大于收益。本存储是<b>并行写</b>的第二份，
 * 只服务两件事：① agent 把位点上卷到元数据库；② 接管方回灌后还原成老载体。
 * 子进程的续传读路径完全不变。
 *
 * <p><b>为什么 agent 读得到</b>：子进程与 agent 同在仓库根目录下运行，
 * 各链路一直用的就是相对路径 {@code files/<taskId>/...}（见 ConfigService 下发的
 * {@code capture.output.dir}），这里沿用同一约定。
 */
public final class LocalCheckpointStore {

    private static final Logger logger = LoggerFactory.getLogger(LocalCheckpointStore.class);

    /** 节流用：key = taskId/stage/streamKey，value = 上次落盘时刻。 */
    private static final ConcurrentHashMap<String, Long> LAST_SAVE = new ConcurrentHashMap<>();

    private LocalCheckpointStore() {
    }

    public static File dirFor(String taskId) {
        return new File("files/" + taskId + "/checkpoint/positions");
    }

    public static File fileFor(String taskId, CheckpointRecord.Stage stage, String streamKey) {
        String name = stage.name().toLowerCase();
        if (streamKey != null && !streamKey.isEmpty()
                && !CheckpointRecord.DEFAULT_STREAM_KEY.equals(streamKey)) {
            name = name + "." + sanitize(streamKey);
        }
        return new File(dirFor(taskId), name + ".properties");
    }

    /** 原子落盘；失败只 warn——位点保存绝不该把主流程带崩（老载体仍在，续传能力不丢）。 */
    public static boolean save(CheckpointRecord record) {
        if (record == null) {
            return false;
        }
        File dst = fileFor(record.getTaskId(), record.getStage(), record.getStreamKey());
        boolean ok = AtomicFileWriter.writePropertiesQuietly(dst, record.toProperties(),
                "Unified checkpoint: " + record.getTaskId() + "/" + record.getStage());
        if (ok) {
            LAST_SAVE.put(throttleKey(record), System.currentTimeMillis());
        }
        return ok;
    }

    /**
     * 按最小间隔节流落盘，给 apply 这类<b>每事件都推进位点</b>的调用点用。
     *
     * <p>节流只影响这份并行载体，不影响原载体的落盘时机，因此不触碰
     * "位点绝不越过未持久化数据"这条不变量：节流的结果是这份位点<b>更旧</b>，
     * 而更旧只会带来重放，不会丢数据。
     *
     * @param force 事务边界/进程退出时置 true，跳过节流强制落盘
     */
    public static boolean saveThrottled(CheckpointRecord record, long minIntervalMs, boolean force) {
        if (record == null) {
            return false;
        }
        if (!force) {
            Long last = LAST_SAVE.get(throttleKey(record));
            if (last != null && System.currentTimeMillis() - last < minIntervalMs) {
                return false;
            }
        }
        return save(record);
    }

    /** 读取某一段的位点；没有或损坏返回 null（调用方按"无位点"处理）。 */
    public static CheckpointRecord load(String taskId, CheckpointRecord.Stage stage, String streamKey) {
        File f = fileFor(taskId, stage, streamKey);
        if (!f.exists() || f.length() == 0) {
            return null;
        }
        try (FileInputStream fis = new FileInputStream(f)) {
            Properties p = new Properties();
            p.load(fis);
            return CheckpointRecord.fromProperties(p);
        } catch (Exception e) {
            logger.warn("读取统一位点 {} 失败，按无位点处理: {}", f, e.getMessage());
            return null;
        }
    }

    public static CheckpointRecord load(String taskId, CheckpointRecord.Stage stage) {
        return load(taskId, stage, CheckpointRecord.DEFAULT_STREAM_KEY);
    }

    /** 列出该任务当前全部位点记录（上卷用）。 */
    public static List<CheckpointRecord> loadAll(String taskId) {
        List<CheckpointRecord> out = new ArrayList<>();
        File dir = dirFor(taskId);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".properties"));
        if (files == null) {
            return out;
        }
        for (File f : files) {
            try (FileInputStream fis = new FileInputStream(f)) {
                Properties p = new Properties();
                p.load(fis);
                CheckpointRecord r = CheckpointRecord.fromProperties(p);
                if (r != null) {
                    out.add(r);
                }
            } catch (Exception e) {
                logger.warn("读取统一位点 {} 失败，跳过: {}", f, e.getMessage());
            }
        }
        return out;
    }

    /** 清空该任务的全部统一位点（主备倒换 / 重做全量时用，与老载体的清理保持同步）。 */
    public static void deleteAll(String taskId) {
        File dir = dirFor(taskId);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (!f.delete()) {
                    logger.warn("删除统一位点文件失败: {}", f);
                }
            }
        }
        LAST_SAVE.keySet().removeIf(k -> k.startsWith(taskId + "/"));
    }

    // ==================== 老载体兼容读 ====================

    /**
     * 从 {@code capture_position.properties} 反推一条 CAPTURE 记录。
     *
     * <p>用于"升级到本版本之前就已经在跑、只有老载体"的任务：agent 起来后仍能把它的位点上卷，
     * 不必等到子进程重启写出新载体。位点文件是自描述的（哪个键存在就是哪种源），
     * 因此这里不需要额外知道任务配置。
     *
     * @return 认不出任何已知位点键时返回 null
     */
    public static CheckpointRecord fromCapturePosition(String taskId, Properties pos) {
        if (pos == null || pos.isEmpty()) {
            return null;
        }
        String lsn = pos.getProperty("wal.lsn", "");
        if (!lsn.trim().isEmpty()) {
            Properties payload = new Properties();
            payload.setProperty("wal.lsn", lsn.trim());
            copyIfPresent(pos, payload, "wal.lsn.numeric");
            return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "postgresql",
                    CheckpointRecord.Kind.LSN, payload, MonotonicKey.ofLsn(lsn.trim()), 0);
        }

        String scn = pos.getProperty("redo.scn", "");
        if (!scn.trim().isEmpty()) {
            Properties payload = new Properties();
            payload.setProperty("redo.scn", scn.trim());
            copyIfPresent(pos, payload, "redo.scn.numeric");
            return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "oracle",
                    CheckpointRecord.Kind.SCN, payload, parseLong(scn.trim()), 0);
        }

        String commitTs = pos.getProperty("ticdc.commit.ts", "");
        if (!commitTs.trim().isEmpty()) {
            Properties payload = new Properties();
            payload.setProperty("ticdc.commit.ts", commitTs.trim());
            copyIfPresent(pos, payload, "binlog.file");
            copyIfPresent(pos, payload, "binlog.position");
            return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "tidb",
                    CheckpointRecord.Kind.TSO, payload, parseLong(commitTs.trim()), 0);
        }

        String file = pos.getProperty("binlog.file", "");
        String position = pos.getProperty("binlog.position", "");
        if (!file.trim().isEmpty() && !position.trim().isEmpty()) {
            Properties payload = new Properties();
            payload.setProperty("binlog.file", file.trim());
            payload.setProperty("binlog.position", position.trim());
            copyIfPresent(pos, payload, "gtid.set");
            return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "mysql",
                    CheckpointRecord.Kind.BINLOG_FILE_POS, payload,
                    MonotonicKey.ofBinlog(file.trim(), parseLong(position.trim())), 0);
        }
        return null;
    }

    /** 还原成 {@code capture_position.properties} 的键值（回灌时写回老载体用）。 */
    public static Properties toCapturePosition(CheckpointRecord record) {
        Properties out = new Properties();
        if (record == null) {
            return out;
        }
        for (String name : record.getPayload().stringPropertyNames()) {
            out.setProperty(name, record.getPayload().getProperty(name, ""));
        }
        return out;
    }

    private static void copyIfPresent(Properties src, Properties dst, String key) {
        String v = src.getProperty(key);
        if (v != null && !v.trim().isEmpty()) {
            dst.setProperty(key, v.trim());
        }
    }

    private static long parseLong(String v) {
        try {
            return Long.parseLong(v);
        } catch (Exception e) {
            return MonotonicKey.UNKNOWN;
        }
    }

    private static String throttleKey(CheckpointRecord r) {
        return r.getTaskId() + "/" + r.getStage() + "/" + r.getStreamKey();
    }

    /** 文件名里不允许出现路径分隔符与冒号（订阅的 topic-partition 会带这些）。 */
    private static String sanitize(String s) {
        return s.replaceAll("[^A-Za-z0-9_.-]", "_");
    }
}
