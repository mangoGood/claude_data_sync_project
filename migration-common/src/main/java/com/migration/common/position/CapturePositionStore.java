package com.migration.common.position;

import com.migration.common.io.AtomicFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * {@code <capture.output.dir>/capture_position.properties} 的统一读写口。
 *
 * <p><b>为什么要有"持久化位点优先"这条规则</b>：capture 此前只从 {@code config.properties} 的
 * {@code capture.binlog.* / capture.wal.lsn / capture.redo.scn} 取起始位点，而这些键是<b>任务启动时
 * 写死一次</b>的（{@code AbstractTaskExecutor.updateMysqlCheckpointConfig} 等）。于是每一次子进程崩溃重启，
 * capture 都从"任务最初的那个位点"重新拉——任务跑得越久，重放量越大：实测一个跑了 10 分钟的任务，
 * 一次重启就重放出 18 万条 THL / 83MB，订阅链路的下游消息量放大到真实写入的 1.67 倍。
 * 位点文件明明一直在更新，只是从来没有人读它。
 *
 * <p>改为持久化优先后，重启只重放"上次落盘位点之后"的量。位点落盘是每 1000 事件 / 每若干秒一次，
 * 因此仍是<b>至少一次</b>语义（会重放最后这一小段），由下游幂等应用兜底——但重放量从
 * "与任务运行时长成正比"降为"与落盘间隔成正比"，这是长任务能不能长期跑的分水岭。
 *
 * <p><b>主备倒换必须清掉这个文件</b>：倒换后源库换成了原目标实例，旧实例的 binlog 位点/GTID/LSN/SCN
 * 在新源上毫无意义（GTID 尤其致命，会让服务端从新源 binlog 最开头整段重放）。
 * {@code AgentMain.cleanupFailoverArtifacts} / {@code FailoverService} 会清空整个 {@code binlog_output/}
 * 目录，天然满足该不变量，{@code FailoverCleanupInvariantTest} 锁死它。
 */
public final class CapturePositionStore {

    private static final Logger logger = LoggerFactory.getLogger(CapturePositionStore.class);

    public static final String FILE_NAME = "capture_position.properties";

    /** 关掉后回到"只认 config.properties 起始位点"的旧行为，用于排障时强制从指定位点重来。 */
    public static final String PREFER_PERSISTED_KEY = "capture.position.prefer.persisted";

    private CapturePositionStore() {
    }

    public static File fileIn(String outputDir) {
        return new File(outputDir == null ? "binlog_output" : outputDir, FILE_NAME);
    }

    /** 读取已落盘位点；文件不存在或损坏时返回空 Properties（调用方回退 config 起始位点）。 */
    public static Properties load(String outputDir) {
        Properties p = new Properties();
        File f = fileIn(outputDir);
        if (!f.exists() || f.length() == 0) {
            return p;
        }
        try (FileInputStream fis = new FileInputStream(f)) {
            p.load(fis);
        } catch (Exception e) {
            // 原子写落地后不该再出现半个文件；真出现了就当没有位点，回退 config 起点重放，
            // 宁可多重放也不能拿一个残缺位点去定位。
            logger.warn("读取已落盘位点 {} 失败，回退配置起始位点: {}", f, e.getMessage());
            return new Properties();
        }
        return p;
    }

    /** 原子写出位点文件。 */
    public static void save(String outputDir, Properties posProps, String comment) {
        AtomicFileWriter.writePropertiesQuietly(fileIn(outputDir), posProps, comment);
    }

    /**
     * 原子写出位点文件，并<b>顺带</b>写一份统一位点记录（{@link LocalCheckpointStore}）。
     *
     * <p>四种源的 capture 都汇到这一个出口，所以统一载体只需在这里挂一次；
     * 位点文件本身是自描述的（wal.lsn / redo.scn / ticdc.commit.ts / binlog.file 谁在就是谁），
     * 因此除 taskId 外不需要额外上下文。
     *
     * <p>统一载体按 1s 节流：它只被 agent 用来上卷和回灌，比原位点文件旧一点只意味着
     * 接管时多重放一点，而多一次 fsync 却要落在 capture 的热路径上。
     */
    public static void save(String outputDir, Properties posProps, String comment, String taskId) {
        save(outputDir, posProps, comment);
        if (taskId == null || taskId.isEmpty() || "unknown".equals(taskId)) {
            return;
        }
        CheckpointRecord record = LocalCheckpointStore.fromCapturePosition(taskId, posProps);
        if (record != null) {
            LocalCheckpointStore.saveThrottled(record, 1000L, false);
        }
    }

    public static boolean preferPersisted(Properties config) {
        return Boolean.parseBoolean(config.getProperty(PREFER_PERSISTED_KEY, "true"));
    }

    /**
     * 取"已落盘位点优先"的字符串值。
     *
     * @param persisted   {@link #load} 的结果
     * @param key         位点文件里的键
     * @param configValue config.properties 里的起始位点
     * @param label       日志用的位点名称
     */
    public static String prefer(Properties persisted, String key, String configValue, String label) {
        String v = persisted.getProperty(key, "");
        if (v == null || v.trim().isEmpty()) {
            return configValue;
        }
        v = v.trim();
        if (!v.equals(configValue)) {
            logger.info("使用已落盘{}续传: {}（配置起始位点 {} 仅用于首次启动）", label, v, configValue);
        }
        return v;
    }
}
