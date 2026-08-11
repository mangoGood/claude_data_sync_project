package com.migration.common.position;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 统一位点模型：把全平台 10 个各写各的位点载体收敛成同一张脸。
 *
 * <p><b>为什么 payload 与 monotonicKey 要分开</b>：GTID 集、Mongo resume token 这类位点
 * <b>本身不可比较</b>——硬要归一成一个数就会丢掉续传真正需要的信息（GTID 集是一个区间集合，
 * resume token 是服务端不透明结构）；而"位点不许回退"这条守卫又必须有个可比的标量。
 * 于是 {@link #payload} 原样保存引擎原生位点（续传用），{@link #monotonicKey} 只用来比大小
 * （守卫用），两者各司其职。不可比的位点把 monotonicKey 置 {@link MonotonicKey#UNKNOWN}，
 * 单调守卫对它自动降级为"不校验"，而不是拿一个错的数去比。
 *
 * <p><b>为什么用 properties 而不是 JSON</b>：migration-common 被所有模块依赖，
 * 往它里面加 JSON 依赖会把 Spring BOM 的版本仲裁牵扯进每一个子进程（mongo 链路踩过驱动降级混包）。
 * properties 的嵌套（外层记元信息、{@code pos.*} 记引擎原生位点）零依赖且能原样round-trip
 * GTID 集里的 {@code :} {@code ,} 与 resume token 的 JSON 文本。
 */
public final class CheckpointRecord {

    /** 位点所属的链路段。一条任务同一 stage 只有一个当前位点。 */
    public enum Stage {
        /** 源端日志读取位点（binlog / WAL / redo / TiCDC commitTs / oplog） */
        CAPTURE,
        /** THL 产出位点（extract 写到哪个 seqno） */
        EXTRACT,
        /** 已应用位点（increment 应用到哪个 seqno；ES/Mongo 单进程链路也记这里） */
        APPLY,
        /** 全量快照位点 */
        FULL,
        /** 订阅投递位点 */
        SUBSCRIBE
    }

    /** 位点的原生形态，决定 payload 里有哪些键、以及 monotonicKey 怎么算。 */
    public enum Kind {
        BINLOG_FILE_POS, GTID_SET, LSN, SCN, TSO, RESUME_TOKEN, REPL_OFFSET, SEQNO, KAFKA_OFFSET
    }

    /** 单流任务的 streamKey；订阅按 topic-partition 时才会有别的值。 */
    public static final String DEFAULT_STREAM_KEY = "-";

    private static final String K_VERSION = "ckpt.version";
    private static final String K_TASK_ID = "ckpt.task.id";
    private static final String K_STAGE = "ckpt.stage";
    private static final String K_STREAM_KEY = "ckpt.stream.key";
    private static final String K_ENGINE = "ckpt.engine";
    private static final String K_KIND = "ckpt.kind";
    private static final String K_MONOTONIC = "ckpt.monotonic.key";
    private static final String K_SOURCE_TS = "ckpt.source.ts";
    private static final String K_UPDATED_AT = "ckpt.updated.at";
    private static final String K_RESET_AT = "ckpt.reset.at";
    private static final String PAYLOAD_PREFIX = "pos.";

    private static final String VERSION = "1";

    private final String taskId;
    private final Stage stage;
    private final String streamKey;
    private final String engine;
    private final Kind kind;
    private final Properties payload;
    private final long monotonicKey;
    private final long sourceTs;
    private final long updatedAt;
    /**
     * 最近一次人工重置的时刻（0 = 从未重置）。
     *
     * <p>它<b>不是</b>位点的一部分，而是"这条位点被人动过"的标记：回灌时拿中心库的 resetAt
     * 与本地的比，中心更新就强制覆盖本地。没有它，后端改完中心库，agent 一看"本地有位点"
     * 就走同机重启分支，重置永远不会生效。回灌后本地也记下同一个 resetAt，于是不会反复覆盖。
     */
    private final long resetAt;

    public CheckpointRecord(String taskId, Stage stage, String streamKey, String engine, Kind kind,
                            Properties payload, long monotonicKey, long sourceTs, long updatedAt) {
        this(taskId, stage, streamKey, engine, kind, payload, monotonicKey, sourceTs, updatedAt, 0L);
    }

    public CheckpointRecord(String taskId, Stage stage, String streamKey, String engine, Kind kind,
                            Properties payload, long monotonicKey, long sourceTs, long updatedAt,
                            long resetAt) {
        this.taskId = taskId;
        this.stage = stage;
        this.streamKey = (streamKey == null || streamKey.isEmpty()) ? DEFAULT_STREAM_KEY : streamKey;
        this.engine = engine == null ? "" : engine;
        this.kind = kind;
        this.payload = payload == null ? new Properties() : payload;
        this.monotonicKey = monotonicKey;
        this.sourceTs = sourceTs;
        this.updatedAt = updatedAt <= 0 ? System.currentTimeMillis() : updatedAt;
        this.resetAt = resetAt;
    }

    /** 常用重载：单流、updatedAt 取当前时刻。 */
    public CheckpointRecord(String taskId, Stage stage, String engine, Kind kind,
                            Properties payload, long monotonicKey, long sourceTs) {
        this(taskId, stage, DEFAULT_STREAM_KEY, engine, kind, payload, monotonicKey, sourceTs,
                System.currentTimeMillis());
    }

    public String getTaskId() { return taskId; }
    public Stage getStage() { return stage; }
    public String getStreamKey() { return streamKey; }
    public String getEngine() { return engine; }
    public Kind getKind() { return kind; }
    public Properties getPayload() { return payload; }
    public long getMonotonicKey() { return monotonicKey; }
    public long getSourceTs() { return sourceTs; }
    public long getUpdatedAt() { return updatedAt; }
    public long getResetAt() { return resetAt; }

    public String payloadValue(String key) {
        return payload.getProperty(key);
    }

    /** 整条记录序列化成 properties（落盘用）。 */
    public Properties toProperties() {
        Properties p = new Properties();
        p.setProperty(K_VERSION, VERSION);
        p.setProperty(K_TASK_ID, taskId == null ? "" : taskId);
        p.setProperty(K_STAGE, stage.name());
        p.setProperty(K_STREAM_KEY, streamKey);
        p.setProperty(K_ENGINE, engine);
        p.setProperty(K_KIND, kind.name());
        p.setProperty(K_MONOTONIC, String.valueOf(monotonicKey));
        p.setProperty(K_SOURCE_TS, String.valueOf(sourceTs));
        p.setProperty(K_UPDATED_AT, String.valueOf(updatedAt));
        p.setProperty(K_RESET_AT, String.valueOf(resetAt));
        for (String name : payload.stringPropertyNames()) {
            p.setProperty(PAYLOAD_PREFIX + name, payload.getProperty(name, ""));
        }
        return p;
    }

    /** 从 properties 还原；缺关键字段（taskId/stage/kind）返回 null，由调用方当作"没有位点"。 */
    public static CheckpointRecord fromProperties(Properties p) {
        if (p == null || p.isEmpty()) {
            return null;
        }
        String taskId = p.getProperty(K_TASK_ID, "");
        String stageName = p.getProperty(K_STAGE, "");
        String kindName = p.getProperty(K_KIND, "");
        if (taskId.isEmpty() || stageName.isEmpty() || kindName.isEmpty()) {
            return null;
        }
        Stage stage;
        Kind kind;
        try {
            stage = Stage.valueOf(stageName);
            kind = Kind.valueOf(kindName);
        } catch (IllegalArgumentException e) {
            // 未来版本写入的枚举值，当前进程不认识：当作没有位点而不是猜一个，
            // 猜错的代价是拿错位点续传（可能丢数据），猜不出的代价只是多重放。
            return null;
        }
        Properties payload = new Properties();
        for (String name : p.stringPropertyNames()) {
            if (name.startsWith(PAYLOAD_PREFIX)) {
                payload.setProperty(name.substring(PAYLOAD_PREFIX.length()), p.getProperty(name, ""));
            }
        }
        return new CheckpointRecord(taskId, stage, p.getProperty(K_STREAM_KEY, DEFAULT_STREAM_KEY),
                p.getProperty(K_ENGINE, ""), kind, payload,
                parseLong(p.getProperty(K_MONOTONIC), MonotonicKey.UNKNOWN),
                parseLong(p.getProperty(K_SOURCE_TS), 0),
                parseLong(p.getProperty(K_UPDATED_AT), 0),
                parseLong(p.getProperty(K_RESET_AT), 0));
    }

    /**
     * 只把引擎原生位点序列化成文本（中心库 {@code payload} 列存的就是它）。
     *
     * <p>刻意剥掉 {@link Properties#store} 自带的时间戳注释行：那一行每次写都不同，
     * 会让"位点没变就不必上卷"这类按内容比对的优化整个失效。
     */
    public String payloadText() {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            payload.store(buf, null);
            StringBuilder sb = new StringBuilder();
            for (String line : new String(buf.toByteArray(), StandardCharsets.ISO_8859_1).split("\n")) {
                if (line.startsWith("#")) {
                    continue;
                }
                if (!line.isEmpty()) {
                    sb.append(line).append('\n');
                }
            }
            return sb.toString();
        } catch (IOException e) {
            return "";
        }
    }

    /** {@link #payloadText()} 的逆操作。 */
    public static Properties parsePayload(String text) {
        Properties p = new Properties();
        if (text == null || text.isEmpty()) {
            return p;
        }
        try {
            p.load(new StringReader(text));
        } catch (IOException ignored) {
            // load 从 StringReader 读不会真的 IO 失败；真损坏了就返回空，按"没有位点"处理
        }
        return p;
    }

    private static long parseLong(String v, long dft) {
        if (v == null || v.trim().isEmpty()) {
            return dft;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return dft;
        }
    }

    @Override
    public String toString() {
        return "CheckpointRecord{" + taskId + "/" + stage + "/" + streamKey
                + ", engine=" + engine + ", kind=" + kind
                + ", key=" + monotonicKey + ", payload=" + payload + "}";
    }
}
