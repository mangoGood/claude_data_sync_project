package com.migration.common.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * 全量快照位点文件的统一契约：{@code files/<taskId>/full_snapshot_position}，
 * 内容一行 {@code 毫秒时间戳|模式|库类型|位点}。
 *
 * <p>各引擎的位点长得不一样（MySQL GTID / binlog 坐标、PG LSN、Oracle SCN、TiDB TSO、
 * Mongo clusterTime、Redis 复制偏移），但"这次全量对应源端的哪个点"是同一个语义。统一成一个
 * 文件契约后，agent 只需读一处就能把它上报给后端与前端，"全量完成即校验"与排障也有了统一入口。
 */
public final class SnapshotPosition {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotPosition.class);

    public static final String FILE_NAME = "full_snapshot_position";

    private SnapshotPosition() {
    }

    /** 一次全量的快照位点。 */
    public static final class Record {
        public final long timestamp;
        public final String mode;
        public final String dbType;
        public final String position;

        Record(long timestamp, String mode, String dbType, String position) {
            this.timestamp = timestamp;
            this.mode = mode;
            this.dbType = dbType;
            this.position = position;
        }
    }

    public static Path pathOf(String taskId) {
        return Paths.get("files", taskId, FILE_NAME);
    }

    /** 落盘。写失败只记 debug——位点是可观测性增强，不能反过来影响全量本身。 */
    public static void write(String taskId, String mode, String dbType, String position) {
        if (taskId == null || taskId.isEmpty() || position == null) {
            return;
        }
        File file = pathOf(taskId).toFile();
        try {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                return;
            }
            try (FileWriter w = new FileWriter(file, false)) {
                w.write(System.currentTimeMillis() + "|" + mode + "|" + dbType + "|" + position + "\n");
            }
        } catch (Exception e) {
            logger.debug("写快照位点文件失败: {}", e.getMessage());
        }
    }

    /** 读取；文件不存在或格式不对返回 null。 */
    public static Record read(String taskId) {
        Path path = pathOf(taskId);
        if (!Files.exists(path)) {
            return null;
        }
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line == null || line.trim().isEmpty()) {
                    continue;
                }
                // 位点本身可能含 '|'（如 PG 的 lsn;snapshot 组合），故只切前 3 个分隔符
                String[] parts = line.trim().split("\\|", 4);
                if (parts.length < 4) {
                    continue;
                }
                return new Record(Long.parseLong(parts[0]), parts[1], parts[2], parts[3]);
            }
        } catch (Exception e) {
            logger.debug("读快照位点文件失败: {}", e.getMessage());
        }
        return null;
    }
}
