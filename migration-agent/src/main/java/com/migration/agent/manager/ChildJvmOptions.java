package com.migration.agent.manager;

import com.migration.agent.service.AgentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 子进程（capture/extract/increment/full/subscribe/mongo/elastic/redis）的 JVM 启动参数解析。
 *
 * <p>此前 {@link ProcessManager} 起子进程时只给了 {@code -D} 系统属性，<b>完全没有传 -Xmx</b>，
 * 于是每个子进程都按"物理内存 1/4"自己决定堆上限。这有两个后果：一是"单进程最多用多少内存"
 * 这条运行约束在代码里根本无处落地、无法验证；二是 OOM 的表现取决于跑在哪台机器上，
 * 同一份数据在 8G 机器上炸、在 64G 机器上不炸，问题无法复现。
 *
 * <p>配置（agent.properties，或 {@code MIGRATION_AGENT_PROC_JVM_OPTS_*} 环境变量）：
 * <ul>
 *   <li>{@code proc.jvm.opts.default}：所有子进程共用的参数；</li>
 *   <li>{@code proc.jvm.opts.<kind>}：按进程类型追加（kind ∈ capture/extract/increment/full/
 *       subscribe/mongo/elastic/redis）。<b>追加而非替换</b>——同名参数（如 -Xmx）JVM 取最后一个，
 *       所以分类值天然覆盖默认值，同时又不会把默认值里的其它参数一起丢掉；</li>
 *   <li>{@code proc.jvm.exit.on.oom}：默认 true，追加 {@code -XX:+ExitOnOutOfMemoryError}；</li>
 *   <li>{@code proc.jvm.heapdump.dir}：非空则追加 {@code -XX:+HeapDumpOnOutOfMemoryError}
 *       与 {@code -XX:HeapDumpPath}。</li>
 * </ul>
 *
 * <p>{@code exit.on.oom} 默认打开是有意的：堆耗尽后的 JVM 往往不是干脆地死掉，而是卡在
 * 连续 Full GC 里——进程还在、端口还listen、心跳文件却不再刷新，恰好落进僵死看门狗的判定盲区。
 * 直接退出反而能让 {@code ProcessGuard} 立刻重启并从已落盘位点续跑。
 */
public final class ChildJvmOptions {

    private static final Logger logger = LoggerFactory.getLogger(ChildJvmOptions.class);

    static final String KEY_PREFIX = "proc.jvm.opts.";
    static final String KEY_DEFAULT = KEY_PREFIX + "default";
    static final String KEY_EXIT_ON_OOM = "proc.jvm.exit.on.oom";
    static final String KEY_HEAPDUMP_DIR = "proc.jvm.heapdump.dir";

    /** 会破坏 {@code java ... -jar <jar>} 命令结构的参数，出现即丢弃。 */
    private static final List<String> FORBIDDEN = java.util.Arrays.asList(
            "-jar", "-cp", "-classpath", "--class-path", "-m", "--module");

    private static volatile Properties cached;

    private ChildJvmOptions() {
    }

    /**
     * 按 agent 配置解析给定子进程的 JVM 参数。
     *
     * @param processName {@link ProcessManager} 的进程名，形如 {@code CaptureMain-<taskId>}
     */
    public static List<String> resolve(String processName) {
        return resolve(processName, defaults());
    }

    /** 纯函数版本：不读文件、不读环境变量，便于单测。 */
    public static List<String> resolve(String processName, Properties props) {
        List<String> opts = new ArrayList<>();
        addAll(opts, props.getProperty(KEY_DEFAULT), processName);

        String kind = kindOf(processName);
        if (!kind.isEmpty()) {
            addAll(opts, props.getProperty(KEY_PREFIX + kind), processName);
        }

        if (Boolean.parseBoolean(trimmed(props.getProperty(KEY_EXIT_ON_OOM, "true")))) {
            addIfAbsent(opts, "-XX:+ExitOnOutOfMemoryError");
        }

        String dumpDir = trimmed(props.getProperty(KEY_HEAPDUMP_DIR, ""));
        if (!dumpDir.isEmpty()) {
            addIfAbsent(opts, "-XX:+HeapDumpOnOutOfMemoryError");
            addIfAbsent(opts, "-XX:HeapDumpPath=" + dumpDir);
        }
        return opts;
    }

    /**
     * 进程名 → 配置分类键。进程名的约定是 {@code <MainClass 简称>-<taskId>}，
     * taskId 本身可能带 {@code -}（UUID），所以只按第一个 {@code -} 之前的部分识别。
     * 认不出来的进程名返回空串，只吃 {@code proc.jvm.opts.default}。
     */
    static String kindOf(String processName) {
        if (processName == null) {
            return "";
        }
        int dash = processName.indexOf('-');
        String head = (dash >= 0 ? processName.substring(0, dash) : processName).trim();
        if (head.equals("CaptureMain") || head.equals("BinlogCaptureMain")) {
            return "capture";
        }
        if (head.equals("MigrationFull")) {
            return "full";
        }
        if (head.equals("ContinuousExtractMain")) {
            return "extract";
        }
        if (head.equals("ContinuousIncrementMain")) {
            return "increment";
        }
        if (head.equals("ContinuousSubscribeMain") || head.equals("MongoSubscribeMain")) {
            return "subscribe";
        }
        if (head.equals("MongoSyncMain")) {
            return "mongo";
        }
        if (head.equals("ElasticSyncMain")) {
            return "elastic";
        }
        if (head.equals("RedisSyncMain")) {
            return "redis";
        }
        return "";
    }

    private static void addAll(List<String> opts, String raw, String processName) {
        String value = trimmed(raw);
        if (value.isEmpty()) {
            return;
        }
        for (String token : value.split("\\s+")) {
            if (token.isEmpty()) {
                continue;
            }
            if (!token.startsWith("-")) {
                // 不以 - 开头的 token 会被 java 当成主类名，直接顶掉 -jar 的位置，
                // 表现为"进程起来就报 ClassNotFoundException"，排查代价远大于这里丢弃。
                logger.warn("忽略非法 JVM 参数（未以 - 开头）: {} [{}]", token, processName);
                continue;
            }
            if (FORBIDDEN.contains(token)) {
                logger.warn("忽略会破坏启动命令的 JVM 参数: {} [{}]", token, processName);
                continue;
            }
            opts.add(token);
        }
    }

    private static void addIfAbsent(List<String> opts, String opt) {
        if (!opts.contains(opt)) {
            opts.add(opt);
        }
    }

    private static String trimmed(String value) {
        return value == null ? "" : value.trim();
    }

    private static Properties defaults() {
        Properties p = cached;
        if (p == null) {
            synchronized (ChildJvmOptions.class) {
                p = cached;
                if (p == null) {
                    p = new Properties();
                    try {
                        AgentConfig config = new AgentConfig();
                        p.setProperty(KEY_DEFAULT, config.getRawProperty(KEY_DEFAULT, ""));
                        for (String kind : new String[]{"capture", "full", "extract", "increment",
                                "subscribe", "mongo", "elastic", "redis"}) {
                            p.setProperty(KEY_PREFIX + kind, config.getRawProperty(KEY_PREFIX + kind, ""));
                        }
                        p.setProperty(KEY_EXIT_ON_OOM, config.getRawProperty(KEY_EXIT_ON_OOM, "true"));
                        p.setProperty(KEY_HEAPDUMP_DIR, config.getRawProperty(KEY_HEAPDUMP_DIR, ""));
                    } catch (Exception e) {
                        logger.warn("读取子进程 JVM 参数配置失败，按默认值启动", e);
                    }
                    cached = p;
                }
            }
        }
        return p;
    }

    /** 单测/配置热更用：丢弃已缓存的配置快照。 */
    public static void reset() {
        cached = null;
    }
}
