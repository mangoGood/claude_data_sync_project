package com.migration.agent.checkpoint;

import com.migration.common.position.CapturePositionStore;
import com.migration.common.position.CheckpointRecord;
import com.migration.common.position.LocalCheckpointStore;
import com.migration.common.position.MonotonicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

/**
 * 启动前把中心位点回灌成本地位点，让"换一台机器接管"与"同一台机器重启"走完全相同的恢复路径。
 *
 * <p>这是本批要修的那个静默数据丢失的正面补丁：接管方本地没有 {@code files/<taskId>/}，
 * {@code AbstractTaskExecutor.initXxxCheckpoint} 找不到 checkpoint 就去取"源库此刻的位点"，
 * 把崩溃到接管之间的变更整段跳过。回灌之后它能找到 checkpoint，于是照常走续传。
 *
 * <h3>决策表</h3>
 * <table>
 *   <tr><th>本地</th><th>中心</th><th>判定</th><th>行为</th></tr>
 *   <tr><td>有</td><td>—</td><td>同机重启</td><td>{@link Result#NOT_NEEDED}，一个字节都不动</td></tr>
 *   <tr><td>无</td><td>有</td><td><b>跨机接管</b></td><td>回灌 → {@link Result#HYDRATED}；失败 → {@link Result#FAILED}</td></tr>
 *   <tr><td>无</td><td>无</td><td>真·首启</td><td>{@link Result#FIRST_START}，走原有"取源库当前位点"</td></tr>
 *   <tr><td>无</td><td>查不到</td><td>无法判定</td><td>{@link Result#FAILED}（见下）</td></tr>
 * </table>
 *
 * <p><b>为什么"中心库查不到"也要 fail-stop</b>：查不到就分不清"首启"和"接管"。猜成首启而实际是接管
 * ＝ 静默丢一段数据；猜成接管而实际是首启 ＝ 任务起不来、报一个明确的错。前者不可接受，后者可修复。
 * 何况元数据库不可达时后端本来就派发不了新任务，"首启 + 库挂了"这个组合几乎不存在。
 * 真要绕过，把 {@code checkpoint.hydrate.fail.stop} 置 false（仅排障用，代价是可能丢数据）。
 */
public class CheckpointHydrator {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointHydrator.class);

    public enum Result {
        /** 本地已有位点，无需回灌（同机重启）。 */
        NOT_NEEDED,
        /** 已从中心库回灌（跨机接管）。 */
        HYDRATED,
        /** 中心库确认没有该任务的位点：这是真·首启。 */
        FIRST_START,
        /** 本地位点已被中心库里的人工重置覆盖（PITR）。 */
        RESET_APPLIED,
        /** 该回灌却灌不成，或压根判不出是不是接管——必须 fail-stop。 */
        FAILED
    }

    private static volatile CheckpointHydrator instance;

    private final CentralCheckpointStore store;
    private final String agentId;
    private final boolean failStop;

    private CheckpointHydrator(CentralCheckpointStore store, String agentId, boolean failStop) {
        this.store = store;
        this.agentId = agentId;
        this.failStop = failStop;
    }

    public static synchronized void initialize(CentralCheckpointStore store, String agentId, boolean failStop) {
        instance = new CheckpointHydrator(store, agentId, failStop);
    }

    /** 未初始化（中心位点关闭）时返回 null，调用方按老行为走。 */
    public static CheckpointHydrator getInstance() {
        return instance;
    }

    /** 解除初始化，回到"没有中心位点"的行为。给单测隔离用，也便于运行期关掉该能力。 */
    public static synchronized void reset() {
        instance = null;
    }

    public boolean isFailStop() {
        return failStop;
    }

    /** 回灌该任务的位点。必须在<b>拉起任何子进程之前</b>调用。 */
    public Result hydrate(String taskId) {
        boolean localExists = hasLocalPosition(taskId);

        List<CheckpointRecord> central;
        try {
            central = loadCentralWithRetry(taskId);
        } catch (Exception e) {
            if (localExists) {
                // 本地位点在，就够续传了：中心库读不到只影响"能不能应用人工重置"，不该挡住启动
                logger.warn("[{}] 中心位点不可读，按同机重启继续（人工重置若有则本次不生效）: {}",
                        taskId, e.getMessage());
                return Result.NOT_NEEDED;
            }
            logger.error("[{}] 无法读取中心位点，判不出是首启还是接管: {}", taskId, e.getMessage());
            return failStop ? Result.FAILED : Result.FIRST_START;
        }

        if (localExists) {
            List<CheckpointRecord> reset = pendingResets(taskId, central);
            if (reset.isEmpty()) {
                return Result.NOT_NEEDED;
            }
            // 人工重置是全平台唯一允许位点倒退的路径（后端已校验任务处于停止态并留了审计）。
            // 本地位点还在，必须强制覆盖，否则"本地有位点"这条分支会让重置永远不生效。
            logger.warn("[{}] 检测到人工重置的位点 {} 条，强制覆盖本地位点", taskId, reset.size());
            for (CheckpointRecord record : reset) {
                try {
                    materialize(taskId, record);
                    LocalCheckpointStore.save(record);
                    logger.warn("[{}] 位点已按重置回灌: {} -> {}", taskId, record.getStage(), record.getPayload());
                } catch (Exception e) {
                    logger.error("[{}] 应用重置位点失败: {} - {}", taskId, record.getStage(), e.getMessage());
                    return Result.FAILED;
                }
            }
            return Result.RESET_APPLIED;
        }

        if (central.isEmpty()) {
            logger.info("[{}] 中心库无位点记录，按首次启动处理", taskId);
            return Result.FIRST_START;
        }

        logger.warn("[{}] 本地无位点但中心库有 {} 条：判定为跨机接管，开始回灌", taskId, central.size());
        int hydrated = 0;
        for (CheckpointRecord record : central) {
            if (!isHydratable(record)) {
                continue;
            }
            try {
                materialize(taskId, record);
                LocalCheckpointStore.save(record);
                hydrated++;
                logger.info("[{}] 已回灌位点: {} {} -> {}", taskId, record.getStage(), record.getKind(),
                        record.getPayload());
            } catch (Exception e) {
                logger.error("[{}] 回灌位点失败: {} - {}", taskId, record.getStage(), e.getMessage());
                return Result.FAILED;
            }
        }
        if (hydrated == 0) {
            // 中心库只有 seqno 这类本机坐标（见 isHydratable），没有任何源端坐标可用。
            // 这时候续不上，只能停下来让人决定重做全量，而不是偷偷从源库当前位点开始。
            logger.error("[{}] 中心位点里没有可用于跨机续传的源端位点，无法接管", taskId);
            return Result.FAILED;
        }
        return Result.HYDRATED;
    }

    /**
     * 挑出"中心库里被人工重置过、而本地还没应用"的位点。
     *
     * <p>判据是 {@code reset_at} 而不是位点大小：重置几乎总是把位点<b>往回</b>调，
     * 拿大小判会跟单调守卫的方向打架。回灌后本地记下同一个 {@code reset_at}，
     * 下次启动两边相等，不会反复覆盖。
     */
    private List<CheckpointRecord> pendingResets(String taskId, List<CheckpointRecord> central) {
        List<CheckpointRecord> out = new java.util.ArrayList<>();
        for (CheckpointRecord record : central) {
            if (record.getResetAt() <= 0 || !isHydratable(record)) {
                continue;
            }
            CheckpointRecord local = LocalCheckpointStore.load(taskId, record.getStage(), record.getStreamKey());
            if (local == null || record.getResetAt() > local.getResetAt()) {
                out.add(record);
            }
        }
        return out;
    }

    /**
     * 读中心位点，失败重试两次。
     *
     * <p>这一步的失败会导致任务 fail-stop，所以不能被一次网络抖动或元数据库主从切换带偏——
     * 那种"任务偶发起不来"最难查。真的连不上，多花 2 秒也不改变结论。
     */
    private List<CheckpointRecord> loadCentralWithRetry(String taskId) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                return store.loadAll(taskId);
            } catch (Exception e) {
                last = e;
                logger.warn("[{}] 读取中心位点失败（第 {}/3 次）: {}", taskId, attempt, e.getMessage());
                if (attempt < 3) {
                    Thread.sleep(1000L);
                }
            }
        }
        throw last;
    }

    /**
     * 本地是否已有任何位点痕迹。任何一种载体在，就说明这台机器跑过这个任务，属于同机重启。
     */
    private boolean hasLocalPosition(String taskId) {
        if (!LocalCheckpointStore.loadAll(taskId).isEmpty()) {
            return true;
        }
        Properties capturePos = CapturePositionStore.load("files/" + taskId + "/binlog_output");
        if (!capturePos.isEmpty()) {
            return true;
        }
        String[] legacy = {
                "files/" + taskId + "/checkpoint/checkpoint.mv.db",
                "files/" + taskId + "/checkpoint/mongo_resume_token.json",
                "files/" + taskId + "/checkpoint/elastic_binlog_position.json"
        };
        for (String p : legacy) {
            if (new File(p).isFile()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 这条位点能不能拿到另一台机器上用。
     *
     * <p><b>seqno 类位点一律不可回灌</b>——这是最容易踩错的一处：THL 的 seqno 是
     * {@code thl_output/} 里的<b>本机文件坐标</b>，而接管方的 THL 目录是空的、会从头编号。
     * 把"已应用到 seqno=5000"灌到一台 THL 从 0 开始的机器上，increment 会把新产出的
     * seqno ≤ 5000 的事件<b>全部跳过</b>，那是比重放严重得多的静默丢数据。
     * 接管方只回灌<b>源端坐标</b>（binlog/LSN/SCN/TSO/resume token），
     * 整条 THL 管线从那个源端位点重新产出，天然自洽。
     */
    private boolean isHydratable(CheckpointRecord record) {
        switch (record.getKind()) {
            case BINLOG_FILE_POS:
            case GTID_SET:
            case LSN:
            case SCN:
            case TSO:
            case RESUME_TOKEN:
            case REPL_OFFSET:
                return record.getStage() == CheckpointRecord.Stage.CAPTURE;
            default:
                return false;
        }
    }

    /**
     * 把统一位点还原成子进程真正会去读的<b>老载体</b>。
     *
     * <p>子进程的读路径一个字都没改，所以回灌必须落到它们认得的文件里：
     * capture 读 {@code capture_position.properties}，agent 的 {@code initXxxCheckpoint} 读 H2
     * （灌上它，接管方就不会再去查源库当前位点），Mongo/ES 读各自的 json。
     */
    private void materialize(String taskId, CheckpointRecord record) throws Exception {
        String carrier = record.payloadValue("carrier");

        if ("mongo".equals(carrier) || record.getKind() == CheckpointRecord.Kind.RESUME_TOKEN) {
            String json = record.payloadValue("mongo.checkpoint.json");
            if (json == null || json.isEmpty()) {
                throw new IllegalStateException("resume token 位点缺 mongo.checkpoint.json");
            }
            writeFile("files/" + taskId + "/checkpoint/mongo_resume_token.json", json);
            return;
        }

        if ("redis".equals(carrier) || record.getKind() == CheckpointRecord.Kind.REPL_OFFSET) {
            // Redis 直接读统一载体（LocalCheckpointStore），没有单独的老载体要还原，
            // 上层 hydrate() 里的 LocalCheckpointStore.save 就够了
            return;
        }

        if ("elastic".equals(carrier)) {
            String file = require(record, "binlog.file");
            String position = require(record, "binlog.position");
            writeFile("files/" + taskId + "/checkpoint/elastic_binlog_position.json",
                    "{\"file\":\"" + file + "\",\"position\":" + Long.parseLong(position) + "}");
            return;
        }

        // 通用 SQL 链路：capture 位点文件 + agent 侧 H2 起始位点
        Properties posProps = LocalCheckpointStore.toCapturePosition(record);
        posProps.remove("carrier");
        CapturePositionStore.save("files/" + taskId + "/binlog_output", posProps,
                "Hydrated from central checkpoint store for task: " + taskId);
        materializeAgentCheckpointDb(taskId, record);
    }

    /**
     * 灌 agent 侧的 H2 起始位点（{@code files/<taskId>/checkpoint/checkpoint}）。
     *
     * <p>不灌它也能靠"已落盘位点优先"续上，但灌了才能让 {@code initXxxCheckpoint} 完全不去碰源库——
     * 少一次"取源库当前位点"的调用，就少一条把它写进 config 的路径，接管路径与同机重启彻底等价。
     */
    private void materializeAgentCheckpointDb(String taskId, CheckpointRecord record) {
        String filename;
        long position;
        String gtid = null;
        switch (record.getKind()) {
            case LSN:
                filename = record.payloadValue("wal.lsn");
                position = parseLong(record.payloadValue("wal.lsn.numeric"));
                break;
            case SCN:
                filename = record.payloadValue("redo.scn");
                position = parseLong(record.payloadValue("redo.scn.numeric"));
                break;
            case TSO:
                filename = record.payloadValue("binlog.file");
                position = parseLong(record.payloadValue("binlog.position"));
                break;
            default:
                filename = record.payloadValue("binlog.file");
                position = parseLong(record.payloadValue("binlog.position"));
                gtid = record.payloadValue("gtid.set");
                break;
        }
        if (filename == null || filename.isEmpty()) {
            return;
        }
        CheckpointManager cm = null;
        try {
            cm = new CheckpointManager("./files/" + taskId + "/checkpoint/checkpoint");
            cm.saveCheckpoint(new CheckpointManager.BinlogPositionInfo(
                    filename, position, gtid, System.currentTimeMillis()));
        } catch (Exception e) {
            // 灌不上不算致命：capture 仍会用"已落盘位点优先"从 capture_position.properties 续。
            logger.warn("[{}] 回灌 agent H2 起始位点失败（capture 仍按落盘位点续传）: {}", taskId, e.getMessage());
        } finally {
            if (cm != null) {
                cm.close();
            }
        }
    }

    /**
     * 首次启动时把刚取到的源库当前位点<b>立刻</b>写进中心库。
     *
     * <p>不能等上卷那一拍：首启后几秒内崩溃 + 被接管，中心库还没有这条任务的任何行，
     * 接管方就会判成"真·首启"，再取一次源库当前位点——那几秒的变更就这么没了。
     * 这个窗口很小，但它正是本批要消灭的那类静默丢数据。
     */
    public void publishInitialPosition(String taskId, String sourceType, String filename, long position, String gtid) {
        if (filename == null || filename.isEmpty()) {
            return;
        }
        Properties payload = new Properties();
        CheckpointRecord.Kind kind;
        long monotonic;
        if ("postgresql".equalsIgnoreCase(sourceType)) {
            kind = CheckpointRecord.Kind.LSN;
            payload.setProperty("wal.lsn", filename);
            payload.setProperty("wal.lsn.numeric", String.valueOf(position));
            monotonic = MonotonicKey.ofLsn(filename);
        } else if ("oracle".equalsIgnoreCase(sourceType)) {
            kind = CheckpointRecord.Kind.SCN;
            payload.setProperty("redo.scn", filename);
            payload.setProperty("redo.scn.numeric", String.valueOf(position));
            monotonic = MonotonicKey.ofNumeric(position);
        } else {
            kind = CheckpointRecord.Kind.BINLOG_FILE_POS;
            payload.setProperty("binlog.file", filename);
            payload.setProperty("binlog.position", String.valueOf(position));
            if (gtid != null && !gtid.trim().isEmpty()) {
                payload.setProperty("gtid.set", gtid.replaceAll("\\s+", ""));
            }
            monotonic = MonotonicKey.ofBinlog(filename, position);
        }
        CheckpointRecord record = new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE,
                sourceType == null ? "mysql" : sourceType, kind, payload, monotonic, 0L);
        LocalCheckpointStore.save(record);
        CentralCheckpointStore.WriteResult result = store.upsert(record, agentId, store.leaseEpoch(taskId));
        logger.info("[{}] 首启位点已写入中心库: {} ({})", taskId, filename + ":" + position, result);
    }

    private static String require(CheckpointRecord record, String key) {
        String v = record.payloadValue(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException("位点缺少必需字段: " + key);
        }
        return v;
    }

    private static void writeFile(String path, String content) throws Exception {
        File f = new File(path);
        if (f.getParentFile() != null) {
            f.getParentFile().mkdirs();
        }
        Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
    }

    private static long parseLong(String v) {
        try {
            return Long.parseLong(v);
        } catch (Exception e) {
            return 0L;
        }
    }
}
