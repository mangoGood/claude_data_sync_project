package com.migration.common.position;

import com.migration.common.io.AtomicFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * 位点保留期的在线巡检结果：{@code <captureOutputDir>/retention_metric}。
 *
 * <p><b>为什么必须在线巡检，而不是只在启动时预检一次</b>：capture 启动时的预检只能回答
 * "现在能不能续上"。任务<b>运行中</b>源库 {@code PURGE BINARY LOGS} / 复制槽被删 /
 * 归档日志被 RMAN 清理，全都要等到下一次重启才炸——而那时位点已经永久失效，
 * 唯一的补救是重做全量。在线巡检把这件事提前到"还来得及延长保留期"的时候。
 *
 * <p>巡检只<b>预警</b>不阻断：位点真的丢了，启动预检那条 E3006 路径会拦住它；
 * 运行中把一个还在正常同步的任务打成 FAILED 才是更糟的结果。
 *
 * <p>文件格式（单行，与 rpo_metric/rto_metric 同风格，便于 agent 无依赖读取）：
 * <pre>时间戳|状态(OK/WARN/LOST/UNKNOWN)|余量|说明</pre>
 * "余量"的含义按引擎而定（MySQL：位点之前还留着几个 binlog 文件；Oracle：SCN 差值；
 * PG：距离 restart_lsn 的字节数），一律是"越小越危险"。
 */
public final class RetentionStatus {

    private static final Logger logger = LoggerFactory.getLogger(RetentionStatus.class);

    public static final String FILE_NAME = "retention_metric";

    public enum State {
        /** 余量充足。 */
        OK,
        /** 位点已经贴到保留期边缘，随时可能失效——此时延长保留期还来得及。 */
        WARN,
        /** 位点已经不可用，只能重做全量。 */
        LOST,
        /** 查不出来（权限不足 / 查询失败），不做判断。 */
        UNKNOWN
    }

    private RetentionStatus() {
    }

    public static File fileIn(String outputDir) {
        return new File(outputDir == null ? "binlog_output" : outputDir, FILE_NAME);
    }

    /** 写巡检结果；失败只记 debug——巡检本身绝不能影响捕获。 */
    public static void write(String outputDir, State state, long headroom, String detail) {
        String line = System.currentTimeMillis() + "|" + state.name() + "|" + headroom + "|"
                + (detail == null ? "" : detail.replace("|", "/").replace("\n", " "));
        try {
            AtomicFileWriter.writeStringQuietly(fileIn(outputDir), line + "\n");
        } catch (Exception e) {
            logger.debug("写位点保留期巡检结果失败: {}", e.getMessage());
        }
    }

    /** 读巡检结果；没有或损坏返回 null。 */
    public static Record read(String outputDir) {
        File f = fileIn(outputDir);
        if (!f.isFile()) {
            return null;
        }
        try (BufferedReader r = new BufferedReader(new FileReader(f))) {
            String line = r.readLine();
            if (line == null || line.trim().isEmpty()) {
                return null;
            }
            String[] parts = line.split("\\|", 4);
            if (parts.length < 3) {
                return null;
            }
            Record rec = new Record();
            rec.timestamp = Long.parseLong(parts[0].trim());
            rec.state = State.valueOf(parts[1].trim());
            rec.headroom = Long.parseLong(parts[2].trim());
            rec.detail = parts.length > 3 ? parts[3] : "";
            return rec;
        } catch (Exception e) {
            logger.debug("读位点保留期巡检结果失败: {}", e.getMessage());
            return null;
        }
    }

    public static class Record {
        public long timestamp;
        public State state;
        public long headroom;
        public String detail;
    }
}
