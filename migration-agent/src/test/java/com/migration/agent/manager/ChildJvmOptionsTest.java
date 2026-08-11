package com.migration.agent.manager;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 子进程 JVM 参数解析。
 *
 * <p>这一层的价值全在"参数确实到了 java 命令行上、且在 -jar 之前"。掉到 -jar 之后
 * 就成了 main 的入参，进程照常起、-Xmx 静默不生效——只靠跑一遍任务是发现不了的，
 * 所以把命令拼装单独抽出来做断言。
 */
@DisplayName("子进程 JVM 参数")
class ChildJvmOptionsTest {

    private Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    @Test
    @DisplayName("进程名前缀映射到配置分类")
    void kindMapping() {
        assertEquals("capture", ChildJvmOptions.kindOf("CaptureMain-abc-123"));
        assertEquals("full", ChildJvmOptions.kindOf("MigrationFull-abc-123"));
        assertEquals("extract", ChildJvmOptions.kindOf("ContinuousExtractMain-t1"));
        assertEquals("increment", ChildJvmOptions.kindOf("ContinuousIncrementMain-t1"));
        assertEquals("subscribe", ChildJvmOptions.kindOf("ContinuousSubscribeMain-t1"));
        assertEquals("mongo", ChildJvmOptions.kindOf("MongoSyncMain-t1"));
        assertEquals("elastic", ChildJvmOptions.kindOf("ElasticSyncMain-t1"));
        assertEquals("redis", ChildJvmOptions.kindOf("RedisSyncMain-t1"));
        // taskId 是 UUID，本身带 '-'：只能按第一个 '-' 之前的部分识别
        assertEquals("capture", ChildJvmOptions.kindOf("CaptureMain-8f1a-4b2c-9d3e"));
        // 认不出来的进程名不报错，只吃 default
        assertEquals("", ChildJvmOptions.kindOf("SomethingElse-t1"));
        assertEquals("", ChildJvmOptions.kindOf(null));
    }

    @Test
    @DisplayName("default 与分类值叠加，分类值在后（-Xmx 取最后一个即分类值生效）")
    void kindOverridesDefaultByOrdering() {
        List<String> opts = ChildJvmOptions.resolve("MigrationFull-t1", props(
                "proc.jvm.opts.default", "-Xmx512m -XX:+UseSerialGC",
                "proc.jvm.opts.full", "-Xmx144m"));

        assertEquals(List.of("-Xmx512m", "-XX:+UseSerialGC", "-Xmx144m", "-XX:+ExitOnOutOfMemoryError"), opts);
        // 叠加而非替换：default 里的非同名参数不能被分类值顶掉
        assertTrue(opts.contains("-XX:+UseSerialGC"));
        assertTrue(opts.lastIndexOf("-Xmx144m") > opts.indexOf("-Xmx512m"));
    }

    @Test
    @DisplayName("不匹配的分类拿不到别人的参数")
    void kindIsolation() {
        Properties p = props("proc.jvm.opts.full", "-Xmx144m", "proc.jvm.exit.on.oom", "false");
        assertEquals(List.of("-Xmx144m"), ChildJvmOptions.resolve("MigrationFull-t1", p));
        assertEquals(List.of(), ChildJvmOptions.resolve("CaptureMain-t1", p));
    }

    @Test
    @DisplayName("ExitOnOutOfMemoryError 默认打开，可关")
    void exitOnOomDefaultsOn() {
        assertTrue(ChildJvmOptions.resolve("CaptureMain-t1", props())
                .contains("-XX:+ExitOnOutOfMemoryError"));
        assertFalse(ChildJvmOptions.resolve("CaptureMain-t1", props("proc.jvm.exit.on.oom", "false"))
                .contains("-XX:+ExitOnOutOfMemoryError"));
        // 用户自己写了一遍也不重复添加
        List<String> opts = ChildJvmOptions.resolve("CaptureMain-t1",
                props("proc.jvm.opts.default", "-XX:+ExitOnOutOfMemoryError"));
        assertEquals(1, opts.stream().filter("-XX:+ExitOnOutOfMemoryError"::equals).count());
    }

    @Test
    @DisplayName("heapdump 目录非空才追加 dump 参数")
    void heapDumpOptional() {
        assertFalse(ChildJvmOptions.resolve("CaptureMain-t1", props())
                .contains("-XX:+HeapDumpOnOutOfMemoryError"));
        List<String> opts = ChildJvmOptions.resolve("CaptureMain-t1", props("proc.jvm.heapdump.dir", "logs/dump"));
        assertTrue(opts.contains("-XX:+HeapDumpOnOutOfMemoryError"));
        assertTrue(opts.contains("-XX:HeapDumpPath=logs/dump"));
    }

    @Test
    @DisplayName("会破坏启动命令的 token 被丢弃")
    void rejectsCommandBreakingTokens() {
        // 不以 - 开头的 token 会被 java 当成主类名顶掉 -jar；-cp/-jar 直接改写命令结构
        List<String> opts = ChildJvmOptions.resolve("CaptureMain-t1", props(
                "proc.jvm.opts.default", "-Xmx144m com.evil.Main -jar other.jar -cp /tmp -XX:+UseSerialGC",
                "proc.jvm.exit.on.oom", "false"));
        assertEquals(List.of("-Xmx144m", "-XX:+UseSerialGC"), opts);
    }

    @Test
    @DisplayName("空白/未配置时行为与改造前一致（除 ExitOnOOM 外不加任何参数）")
    void emptyConfigIsNoop() {
        assertEquals(List.of(), ChildJvmOptions.resolve("MigrationFull-t1",
                props("proc.jvm.opts.default", "   ", "proc.jvm.opts.full", "", "proc.jvm.exit.on.oom", "false")));
    }

    @Test
    @DisplayName("JVM 参数落在 -jar 之前，且不影响 main 入参")
    void jvmOptsLandBeforeJarInCommand() {
        ProcessManager pm = new ProcessManager("migration-full/target/migration-full-1.0.0.jar",
                "MigrationFull-t1", "t1");
        pm.setMainArgs(new String[]{"--config", "config.properties"});

        List<String> cmd = pm.buildCommand(List.of("-Xmx144m", "-XX:+ExitOnOutOfMemoryError"));

        int xmx = cmd.indexOf("-Xmx144m");
        int jar = cmd.indexOf("-jar");
        assertTrue(xmx > 0, "-Xmx 应出现在命令里: " + cmd);
        assertTrue(xmx < jar, "-Xmx 必须在 -jar 之前，否则会被当成 main 入参静默失效: " + cmd);
        assertEquals("java", cmd.get(0));
        assertEquals("migration-full/target/migration-full-1.0.0.jar", cmd.get(jar + 1));
        assertEquals(List.of("--config", "config.properties"), cmd.subList(jar + 2, cmd.size()));
        // 既有的 -D 系统属性不能被挤掉
        assertTrue(cmd.contains("-Dh2.bindAddress=127.0.0.1"));
        assertTrue(cmd.contains("-Dtask.id=t1"));
    }

    @Test
    @DisplayName("未配置 JVM 参数时命令与改造前逐字相同")
    void commandUnchangedWhenNoOpts() {
        ProcessManager pm = new ProcessManager("some.jar", "MigrationFull-t1", "t1");
        List<String> cmd = pm.buildCommand(List.of());
        assertEquals("java", cmd.get(0));
        assertEquals("-jar", cmd.get(cmd.size() - 2));
        assertEquals("some.jar", cmd.get(cmd.size() - 1));
    }
}
