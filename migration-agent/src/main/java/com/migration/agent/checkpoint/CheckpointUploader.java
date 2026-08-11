package com.migration.agent.checkpoint;

import com.migration.common.position.CapturePositionStore;
import com.migration.common.position.CheckpointRecord;
import com.migration.common.position.LocalCheckpointStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 把本地位点周期性上卷到元数据库。
 *
 * <p><b>只读"已经落盘"的位点文件</b>，绝不读子进程内存里的当前位点——这是本设计最要命的一条不变量：
 * 中心位点必须<b>落后或等于</b>本地位点。落后只意味着接管时多重放一小段（幂等应用兜底），
 * 而一旦超前，接管方就会从"数据其实还没落地"的位点续，那才是真丢数据。
 *
 * <p><b>失败一律吞掉</b>：元数据库抖动不能变成任务失败。上卷断了，最坏结果是退回今天的水平
 * （跨机接管能力降级），本地位点与续传能力分毫不受影响。
 *
 * <p>接管的重放窗口 = 上卷间隔 × 源端速率，默认 3s。调小只减少重放量，不改变正确性。
 */
public class CheckpointUploader {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointUploader.class);

    private static volatile CheckpointUploader instance;

    private final CentralCheckpointStore store;
    private final String agentId;
    private final long intervalMs;
    private final long historySampleIntervalMs;
    private final Supplier<Set<String>> runningTasks;

    /** 已上卷内容的指纹，避免位点没变时反复写库。key = taskId/stage/streamKey。 */
    private final ConcurrentHashMap<String, String> uploaded = new ConcurrentHashMap<>();
    /** 上次采样进 history 的时刻。 */
    private final ConcurrentHashMap<String, Long> lastHistoryAt = new ConcurrentHashMap<>();

    private final AtomicLong rejectedTotal = new AtomicLong();
    /** 全量表级断点的上卷（跨机接管时不必把已搬完的表再搬一遍） */
    private FullProgressStore fullProgressStore;
    private volatile long lastFullProgressAt;
    private static final long FULL_PROGRESS_INTERVAL_MS = 30000L;

    public void setFullProgressStore(FullProgressStore store) {
        this.fullProgressStore = store;
    }

    private ScheduledExecutorService executor;

    private CheckpointUploader(CentralCheckpointStore store, String agentId, long intervalMs,
                               long historySampleIntervalMs, Supplier<Set<String>> runningTasks) {
        this.store = store;
        this.agentId = agentId;
        this.intervalMs = intervalMs;
        this.historySampleIntervalMs = historySampleIntervalMs;
        this.runningTasks = runningTasks;
    }

    public static synchronized void initialize(CentralCheckpointStore store, String agentId, long intervalMs,
                                               long historySampleIntervalMs, Supplier<Set<String>> runningTasks) {
        if (instance != null) {
            return;
        }
        instance = new CheckpointUploader(store, agentId, intervalMs, historySampleIntervalMs, runningTasks);
        instance.start();
    }

    /** 未初始化（中心位点关闭 / 单机部署）时返回 null，调用方按"没有中心位点"处理。 */
    public static CheckpointUploader getInstance() {
        return instance;
    }

    private void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "checkpoint-uploader");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::tickQuietly, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        logger.info("位点上卷已启动: 间隔 {}ms，采样间隔 {}ms，agentId={}", intervalMs, historySampleIntervalMs, agentId);
    }

    public void stop() {
        if (executor != null) {
            // 停机前再上卷一次：优雅停机后被改派的任务，接管方就能拿到最新位点
            tickQuietly();
            executor.shutdownNow();
        }
    }

    private void tickQuietly() {
        try {
            Set<String> tasks = runningTasks != null ? runningTasks.get() : Collections.emptySet();
            for (String taskId : tasks) {
                uploadTask(taskId);
            }
            // 全量表级断点：变化远比位点慢（一张表一条），单独按低频上卷，
            // 不跟着位点每 3s 走一遍 H2
            long now = System.currentTimeMillis();
            if (fullProgressStore != null && now - lastFullProgressAt >= FULL_PROGRESS_INTERVAL_MS) {
                lastFullProgressAt = now;
                for (String taskId : tasks) {
                    fullProgressStore.upload(taskId, agentId);
                }
            }
        } catch (Exception e) {
            // 巡检类代码必须吞掉所有异常：Files.walk 惰性遍历撞上边写边删会抛 UncheckedIOException
            // （catch IOException 接不住），曾把健康任务打成 FAILED
            logger.warn("位点上卷轮次异常（不影响任务）: {}", e.toString());
        }
    }

    /** 上卷单个任务的全部位点。供停机/状态跃迁时同步调用。 */
    public void uploadTask(String taskId) {
        try {
            int leaseEpoch = store.leaseEpoch(taskId);
            uploadSchemaVersions(taskId, leaseEpoch);
            for (CheckpointRecord record : collectRecords(taskId)) {
                String key = record.getTaskId() + "/" + record.getStage() + "/" + record.getStreamKey();
                String fingerprint = record.getMonotonicKey() + "|" + record.payloadText();
                if (fingerprint.equals(uploaded.get(key))) {
                    continue;   // 位点没动，不必写库
                }
                CentralCheckpointStore.WriteResult result = store.upsert(record, agentId, leaseEpoch);
                switch (result) {
                    case ACCEPTED:
                    case INSERTED:
                        uploaded.put(key, fingerprint);
                        maybeSampleHistory(key, record);
                        break;
                    case REJECTED:
                        // 位点被 fencing 或单调守卫拦下：本 agent 多半已被抢占，或位点真的回退了。
                        // 两种都不该静默——但也不该 fail：真正的主还在正常写它自己的位点。
                        rejectedTotal.incrementAndGet();
                        logger.warn("[{}] 位点上卷被拒（{} epoch={} key={}）：本 agent 可能已被抢占，或位点回退",
                                taskId, record.getStage(), leaseEpoch, record.getMonotonicKey());
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            logger.warn("[{}] 位点上卷失败（本地位点不受影响）: {}", taskId, e.toString());
        }
    }

    /**
     * 汇总该任务当前可上卷的位点。
     *
     * <p>优先用统一载体；只有统一载体里没有 CAPTURE 段时，才去读老的
     * {@code capture_position.properties} 兜底——这条路径服务的是"升级到本版本之前就已经在跑、
     * 本地只有老载体"的任务，让它们不必等子进程重启就能先把位点送进中心库。
     */
    private List<CheckpointRecord> collectRecords(String taskId) {
        List<CheckpointRecord> records = LocalCheckpointStore.loadAll(taskId);
        boolean hasCapture = records.stream()
                .anyMatch(r -> r.getStage() == CheckpointRecord.Stage.CAPTURE);
        if (!hasCapture) {
            CheckpointRecord legacy = LocalCheckpointStore.fromCapturePosition(taskId,
                    CapturePositionStore.load("files/" + taskId + "/binlog_output"));
            if (legacy != null) {
                records.add(legacy);
            }
        }
        return records;
    }

    /**
     * 把 extract 落在本地的表结构版本增量传到中心库。
     *
     * <p>跟着位点上卷的节奏走，但<b>不做"没变就跳过"的指纹优化</b>：DDL 稀疏（一天可能就几条），
     * 攒批省不下什么，而丢一条版本，该表之后的每个版本都是错的。增量起点从中心库的
     * 最大位点现取——它就是一次索引查询，比在内存里维护一个可能与库不一致的水位靠谱。
     */
    private void uploadSchemaVersions(String taskId, int leaseEpoch) {
        SchemaVersionStore schemaStore = SchemaVersionStore.getInstance();
        if (schemaStore == null) {
            return;   // 单机部署 / 中心存储没开：只有本地 schema_history.jsonl
        }
        try {
            long uploadedKey = schemaStore.maxUploadedKey(taskId);
            int n = schemaStore.uploadSince(taskId, uploadedKey, agentId, leaseEpoch);
            if (n < 0) {
                rejectedTotal.incrementAndGet();
            }
        } catch (Exception e) {
            logger.warn("[{}] 表结构版本上卷失败（不影响位点上卷）: {}", taskId, e.getMessage());
        }
    }

    private void maybeSampleHistory(String key, CheckpointRecord record) {
        if (historySampleIntervalMs <= 0) {
            return;
        }
        long nowMs = System.currentTimeMillis();
        Long last = lastHistoryAt.get(key);
        if (last != null && nowMs - last < historySampleIntervalMs) {
            return;
        }
        lastHistoryAt.put(key, nowMs);
        store.recordHistory(record, "SAMPLE", null);
    }

    /** 清掉某任务的上卷缓存（倒换作废位点后必须清，否则新位点会被当成"没变"而不写库）。 */
    public void forget(String taskId) {
        uploaded.keySet().removeIf(k -> k.startsWith(taskId + "/"));
        lastHistoryAt.keySet().removeIf(k -> k.startsWith(taskId + "/"));
    }

    public long getRejectedTotal() {
        return rejectedTotal.get();
    }
}
