package com.migration.common.lob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 按<b>已应用位点</b>回收大字段落盘文件。
 *
 * <p>落盘文件的命名是 {@code <binlog文件>#<位点>#c<序号>.lob}——正是为了让这件事变成
 * 一次文件名解析，而不是维护一张引用计数表。
 *
 * <p>删除的判据只能是"位点已经推进过去了"，不能是时间或数量：
 * 位点没推进就意味着这些事件可能还要重放，文件删早了，重放时就只能 fail-stop
 * （或者更糟——静默跳过那一行的大字段）。反过来，位点推进之后文件就再也用不到了，
 * 留着只会把磁盘吃光——1GB 一个的东西，攒几十个就是几十 GB。
 */
public final class LobSpillJanitor {

    private static final Logger logger = LoggerFactory.getLogger(LobSpillJanitor.class);

    private final File dir;
    /** 上次清理时的位点，避免每个事件都去扫目录。 */
    private long lastCleanedPosition = -1;
    private String lastCleanedFile;

    public LobSpillJanitor(String dirPath) {
        this.dir = dirPath == null ? null : new File(dirPath);
    }

    /**
     * 回收所有"位点早于已应用位点"的落盘文件。
     *
     * @return 删除的文件数
     */
    public int cleanupUpTo(String appliedBinlogFile, long appliedPosition) {
        if (dir == null || !dir.isDirectory() || appliedBinlogFile == null || appliedBinlogFile.isEmpty()) {
            return 0;
        }
        if (appliedBinlogFile.equals(lastCleanedFile) && appliedPosition == lastCleanedPosition) {
            return 0;
        }
        lastCleanedFile = appliedBinlogFile;
        lastCleanedPosition = appliedPosition;

        File[] files = dir.listFiles((d, name) -> name.endsWith(LobSpillNaming.LOB_SUFFIX));
        if (files == null) {
            return 0;
        }
        int removed = 0;
        for (File f : files) {
            LobSpillNaming.Parsed parsed = LobSpillNaming.parse(f.getName());
            if (parsed == null) {
                continue;   // 名字不认识的文件不碰：宁可留着也不能误删
            }
            if (isBefore(parsed, appliedBinlogFile, appliedPosition) && f.delete()) {
                removed++;
            }
        }
        if (removed > 0) {
            logger.info("已回收大字段落盘文件 {} 个（位点已推进到 {}:{}）",
                    removed, appliedBinlogFile, appliedPosition);
        }
        return removed;
    }

    /**
     * 文件的位点是否早于已应用位点。
     *
     * <p>binlog 文件名带序号（mysql-bin.000042），字典序与时间序一致，可以直接比。
     * 同一个文件内比位点；<b>同位点的不删</b>——那正是当前这一批，可能还要重放。
     */
    private static boolean isBefore(LobSpillNaming.Parsed parsed, String appliedFile, long appliedPosition) {
        int cmp = parsed.binlogFile.compareTo(appliedFile);
        if (cmp != 0) {
            return cmp < 0;
        }
        return parsed.position < appliedPosition;
    }
}
