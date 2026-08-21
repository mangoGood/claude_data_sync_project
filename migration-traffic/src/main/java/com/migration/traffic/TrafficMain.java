package com.migration.traffic;

import com.migration.common.MdcUtil;
import com.migration.traffic.capture.TrafficCaptureRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 流量复制 / 回放子进程入口。
 *
 * <p>两种模式共用一个 jar（一个 Main-Class），靠 {@code --mode capture|replay} 分派——
 * agent 侧 {@code ProcessManager} 用 {@code -jar} 起进程，一个 jar 只能有一个 Main-Class，
 * 拆成两个模块只为了两个 main 并不值得。
 */
public final class TrafficMain {

    private static final Logger logger = LoggerFactory.getLogger(TrafficMain.class);

    public static final String MODE_CAPTURE = "capture";
    public static final String MODE_REPLAY = "replay";
    /**
     * 兜底还原源库开关。不需要配置文件，只需要录制目录里的
     * {@code source_state.properties}——这正是"捕获进程已经不存在了"时唯一还剩下的依据。
     * agent 的看门狗与重启扫尾走同一条逻辑（{@code TrafficSourceState.restore()}）。
     */
    public static final String MODE_RESTORE = "restore";

    private TrafficMain() {
    }

    public static void main(String[] args) {
        String mode = argOf(args, "--mode", MODE_CAPTURE);
        String configPath = argOf(args, "--config", null);

        if (MODE_RESTORE.equals(mode)) {
            System.exit(runRestore(args) ? 0 : 1);
        }

        Properties props = loadConfig(configPath);
        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));

        MdcUtil.setTaskId(taskId);
        MdcUtil.setProcessName("traffic-" + mode);

        // 单实例互斥 + 父进程看门狗（与其它引擎同一套，放在解密之前）：
        // 两个捕获进程同时轮转同一张 general_log 会让两边都静默丢数据。
        com.migration.common.proc.ChildProcessBootstrap.init(taskId, "traffic-" + mode);

        // 解密 config.properties 里的 ENC: 口令；历史明文无前缀，原样通过
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        logger.info("=== 流量{}服务启动 === taskId={}", MODE_REPLAY.equals(mode) ? "回放" : "复制", taskId);

        try {
            if (MODE_REPLAY.equals(mode)) {
                runReplay(props);
            } else {
                runCapture(props);
            }
        } catch (Exception e) {
            logger.error("流量{}服务异常退出", MODE_REPLAY.equals(mode) ? "回放" : "复制", e);
            System.exit(1);
        } finally {
            MdcUtil.clear();
        }
    }

    /** 收尾等待上限：封口要关 gzip 流、写 manifest、还原源库开关，给足时间但不能无限等。 */
    private static final long SHUTDOWN_WAIT_MS = 30_000L;

    private static void runCapture(Properties props) throws Exception {
        TrafficCaptureRunner runner = newCaptureRunner(props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("收到关闭信号，停止流量复制…");
            runner.stop();
            // 必须等：JVM 只等 hook、不等 main 线程。不等就是"最后一批语句丢掉 +
            // 源库 general_log 一直开着"，后者会把源库磁盘写满。
            if (!runner.awaitFinished(SHUTDOWN_WAIT_MS)) {
                logger.error("流量复制收尾超时（{}ms）——录制可能未封口，源库语句日志可能仍处于开启状态，"
                        + "请人工确认源库 general_log", SHUTDOWN_WAIT_MS);
            }
        }, "traffic-capture-shutdown"));
        runner.run();
    }

    /** 捕获执行体：带源库守护 + 体量护栏 + 续录/空洞。 */
    private static TrafficCaptureRunner newCaptureRunner(Properties props) {
        return new com.migration.traffic.capture.GuardedCaptureRunner(props);
    }

    private static void runReplay(Properties props) throws Exception {
        com.migration.traffic.replay.TrafficReplayRunner runner =
                new com.migration.traffic.replay.TrafficReplayRunner(props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("收到关闭信号，停止流量回放…");
            runner.stop();
            // 同捕获侧：JVM 只等 hook 不等 main。不等就会把还没跑完的会话连同报告一起丢掉
            if (!runner.awaitFinished(SHUTDOWN_WAIT_MS)) {
                logger.error("流量回放收尾超时（{}ms），回放报告可能不完整", SHUTDOWN_WAIT_MS);
            }
        }, "traffic-replay-shutdown"));
        runner.run();
    }

    /** {@code --mode restore --task <taskId>} 或 {@code --mode restore --dir <录制目录>}。 */
    private static boolean runRestore(String[] args) {
        String dir = argOf(args, "--dir", null);
        if (dir == null) {
            String taskId = argOf(args, "--task", System.getProperty("task.id", null));
            if (taskId == null) {
                logger.error("restore 模式需要 --task <taskId> 或 --dir <录制目录>");
                return false;
            }
            dir = "files/" + taskId + "/traffic";
        }
        File d = new File(dir);
        com.migration.common.traffic.TrafficSourceState state =
                com.migration.common.traffic.TrafficSourceState.load(d);
        if (state == null) {
            logger.info("{} 下没有待还原的源库状态（说明上次已正常还原）", d.getAbsolutePath());
            return true;
        }
        boolean ok = state.restore();
        if (ok) {
            com.migration.common.traffic.TrafficSourceState.clear(d);
        }
        return ok;
    }

    static Properties loadConfig(String configPath) {
        Properties props = new Properties();
        String path = configPath;
        if (path == null) {
            String taskIdHint = System.getProperty("task.id", "unknown");
            path = "files/" + taskIdHint + "/config.properties";
        }
        File f = new File(path);
        if (!f.exists()) {
            if (configPath != null) {
                logger.error("配置文件不存在: {}", path);
                System.exit(1);
            }
            return props;
        }
        try (InputStream in = new FileInputStream(f)) {
            props.load(in);
        } catch (IOException e) {
            logger.error("加载配置文件失败: {}", path, e);
            System.exit(1);
        }
        return props;
    }

    static String argOf(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) return args[i + 1];
        }
        return def;
    }
}
