package com.migration.redis;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.migration.common.crypto.CredentialCipher;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.parser.DefaultCommandParser;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.DumpRdbVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Redis → Redis 数据同步（独立子进程，由 agent 拉起）。
 *
 * <p>与 SQL 侧的 capture/extract/increment 三进程管线不同，Redis 同步是单进程复制协议两阶段：
 * <ol>
 *   <li><b>全量</b>：向源库发起 {@code PSYNC}，源库回传 RDB 快照；借助 redis-replicator 的
 *       {@link DumpRdbVisitor}，每个键直接拿到 {@code RESTORE} 可用的序列化字节（覆盖
 *       string/list/set/hash/zset/stream 全部类型，无需自实现 RDB/listpack/LZF 解析），
 *       逐键 {@code RESTORE ... REPLACE} 写入目标（幂等可重跑）；</li>
 *   <li><b>增量</b>（migration.mode=fullAndIncre）：RDB 之后源库持续下发复制命令流，
 *       逐条原样转发到目标（{@code SELECT} 用于切库并做库过滤，其余写命令 {@code sendCommand}
 *       verbatim 重放）。这正是选择 PSYNC 而非键空间通知的原因——复制流忠实包含删除/过期/
 *       非幂等命令（INCR/LPUSH 等），断线期间的变更也不丢。</li>
 * </ol>
 *
 * <p>同步对象为 Redis 逻辑库（db0..dbN）：{@code migration.sync.objects} 形如
 * {@code {"0":{"dbLevel":true},"3":{"dbLevel":true}}}，空/缺省 = 全部逻辑库。
 *
 * <p>进度通过 files/{taskId}/redis_progress.json（原子写）暴露给 agent 的 RedisSyncTask 轮询上报。
 */
public final class RedisSyncMain {

    private static final Logger logger = LoggerFactory.getLogger(RedisSyncMain.class);
    private static final Gson gson = new Gson();

    private static final int PROGRESS_FLUSH_EVERY = 200;

    /**
     * 需要原样转发的写/控制命令白名单：为这些命令注册 {@link DefaultCommandParser}，覆盖
     * redis-replicator 内建的“类型化”解析，使命令统一以 {@link DefaultCommand}（命令名 + 原始参数
     * 字节）交付，便于 verbatim 转发。未覆盖的写命令会以类型化 Command 交付并被忽略（记 warn）。
     */
    private static final String[] REPLICATED_COMMANDS = {
            // 控制
            "SELECT", "SWAPDB", "FLUSHDB", "FLUSHALL", "MULTI", "EXEC", "PING",
            // 通用
            "DEL", "UNLINK", "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
            "RENAME", "RENAMENX", "MOVE", "COPY", "RESTORE", "SORT",
            // string
            "SET", "SETNX", "SETEX", "PSETEX", "APPEND", "SETRANGE", "GETSET", "GETDEL", "GETEX",
            "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT", "MSET", "MSETNX", "SETBIT", "BITOP", "BITFIELD",
            // list
            "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LINSERT", "LREM", "LTRIM",
            "RPOPLPUSH", "LMOVE", "BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "LMPOP", "BLMPOP",
            // set
            "SADD", "SREM", "SPOP", "SMOVE", "SDIFFSTORE", "SINTERSTORE", "SUNIONSTORE",
            // hash
            "HSET", "HSETNX", "HMSET", "HDEL", "HINCRBY", "HINCRBYFLOAT",
            "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HPERSIST", "HSETEX", "HGETDEL", "HGETEX",
            // zset
            "ZADD", "ZINCRBY", "ZREM", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK", "ZREMRANGEBYLEX",
            "ZPOPMIN", "ZPOPMAX", "BZPOPMIN", "BZPOPMAX", "ZDIFFSTORE", "ZINTERSTORE", "ZUNIONSTORE",
            "ZRANGESTORE", "ZMPOP", "BZMPOP",
            // stream
            "XADD", "XDEL", "XSETID", "XTRIM", "XGROUP", "XCLAIM", "XACK", "XAUTOCLAIM",
            // hyperloglog / geo
            "PFADD", "PFMERGE", "GEOADD", "GEOSEARCHSTORE", "GEORADIUS", "GEORADIUSBYMEMBER",
            // scripting（脚本副作用可能以 EVAL 原样传播）
            "EVAL", "EVALSHA", "FCALL", "FUNCTION"
    };

    private final String taskId;
    private final Properties props;
    private final boolean fullAndIncre;
    /** 选中的逻辑库索引；null = 全部库。 */
    private final Set<Long> selectedDbs;
    private final Path progressPath;

    private Jedis target;
    /** 全量批量装载通道（pipeline 批量 RESTORE）；增量命令流仍逐条转发以保序。 */
    private RedisRestoreChannel restoreChannel;
    private int targetSelectedDb = -1;
    /** 增量命令流当前所在源库（源端 SELECT 驱动）。 */
    private long streamDb = 0;

    // 进度状态
    private volatile String phase = "FULL";
    private volatile long totalKeys;
    private volatile long copiedKeys;
    private volatile long incrCommands;
    private volatile long currentDb;
    private volatile String error;
    private int sinceFlush;
    /** 进度文件上次落盘的墙钟时刻，供增量阶段“按时间兜底刷新”判断，让空闲时进度文件也保持活性。 */
    private volatile long lastProgressWriteMs;
    /** 增量阶段进度文件最长刷新间隔：PSYNC 每 ~10s 的 PING 会驱动刷新，据此让 agent 僵死看门狗有信号。 */
    private static final long PROGRESS_TIME_REFRESH_MS = 5000;
    /** 复制流的 Configuration：replOffset 由 replicator 边消费边推进，位点就取自它。 */
    private volatile Configuration replConfiguration;

    private RedisSyncMain(String taskId, Properties props) {
        this.taskId = taskId;
        this.props = props;
        this.fullAndIncre = "fullAndIncre".equals(props.getProperty("migration.mode", "full"));
        this.selectedDbs = parseSelectedDbs(props.getProperty("migration.sync.objects", ""));
        this.progressPath = Paths.get("files", taskId, "redis_progress.json");
    }

    public static void main(String[] args) throws Exception {
        String taskId = System.getProperty("task.id", "");
        for (int i = 0; i < args.length - 1; i++) {
            if ("--task-id".equals(args[i])) {
                taskId = args[i + 1];
            }
        }
        if (taskId.isEmpty()) {
            System.err.println("task.id is required (-Dtask.id or --task-id)");
            System.exit(1);
        }

        // 单实例互斥 + 父进程看门狗：同一 taskId 只允许一个 redis 引擎进程
        com.migration.common.proc.ChildProcessBootstrap.init(taskId, "redis");

        Properties props = new Properties();
        File configFile = new File("files/" + taskId + "/config.properties");
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }

        RedisSyncMain sync = new RedisSyncMain(taskId, props);
        try {
            sync.run();
        } catch (Exception e) {
            logger.error("Redis 同步失败", e);
            sync.error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            sync.phase = "FAILED";
            sync.writeProgress();
            System.exit(1);
        }
    }

    private void run() throws Exception {
        logger.info("Redis 同步启动: taskId={}, mode={}, selectedDbs={}",
                taskId, fullAndIncre ? "fullAndIncre" : "full",
                selectedDbs == null ? "ALL" : selectedDbs);

        target = buildJedis("target");
        com.migration.common.bulk.BulkLoadOptions bulkOptions =
                com.migration.common.bulk.BulkLoadOptions.from(props);
        restoreChannel = new RedisRestoreChannel(target, bulkOptions);
        logger.info("全量批量装载通道: {}", bulkOptions);
        totalKeys = computeTotalKeys();
        writeProgress();

        Configuration conf = Configuration.defaultSetting()
                .setConnectionTimeout(15000)
                .setReadTimeout(60000)
                // 断点/断线后由 agent 的 ProcessGuard 负责重启整进程（重跑全量幂等），
                // 这里禁用 replicator 内部无限重试，让致命错误快速冒泡为任务失败。
                .setRetries(1);
        applyAuth(conf, "source");
        replConfiguration = conf;
        tryResumeFromPersistedOffset(conf);

        String host = props.getProperty("source.db.host", "localhost");
        int port = Integer.parseInt(props.getProperty("source.db.port", "6379"));
        final Replicator replicator = new RedisReplicator(host, port, conf);
        // DumpRdbVisitor：让全量 KeyValuePair.getValue() 直接给出 RESTORE 可用的 dump 字节。
        replicator.setRdbVisitor(new DumpRdbVisitor(replicator));
        // 覆盖内建解析器，使白名单命令以 DefaultCommand（命令名 + 原始参数）交付，便于原样转发。
        for (String cmd : REPLICATED_COMMANDS) {
            replicator.addCommandParser(CommandName.name(cmd), new DefaultCommandParser());
        }

        replicator.addEventListener((rep, event) -> {
            try {
                if (event instanceof PreRdbSyncEvent) {
                    phase = "FULL";
                    copiedKeys = 0;
                    // 全量开始前清空目标选中库：RESTORE...REPLACE 只覆盖 RDB 里出现的键，绝不删除
                    // RDB 中缺席的键。崩溃重启触发的全量重跑若不先清库，源库在中断窗口里删掉的键会
                    // 作为“幽灵键”永久残留在目标，导致断点续传后目标多出这些键、数据不一致。
                    // 清库 + 全量 RESTORE = 目标严格镜像源库当前快照，随后增量继续，最终一致。
                    flushTargetSelectedDbs();
                    writeProgress();
                } else if (event instanceof KeyValuePair) {
                    applyRdbKey((KeyValuePair<?, ?>) event);
                } else if (event instanceof PostRdbSyncEvent) {
                    onFullDone(rep);
                } else if (event instanceof DefaultCommand) {
                    applyCommand((DefaultCommand) event);
                } else if (event instanceof com.moilioncircle.redis.replicator.cmd.Command) {
                    // 类型化命令（不在转发白名单中）：忽略并告警，便于测试时发现遗漏覆盖。
                    logger.warn("未转发的复制命令类型: {}", event.getClass().getSimpleName());
                }
            } catch (Exception e) {
                // 单事件失败记日志继续（RESTORE REPLACE / 命令重放多为幂等；不因单事件卡死整个流）。
                logger.error("处理复制事件失败: {}", e.getMessage());
            }
        });

        try {
            replicator.open();
        } finally {
            closeQuietly(replicator);
            closeTarget();
        }

        // full-only 正常结束（onFullDone 已 close replicator，open 返回后落终态）；
        // fullAndIncre 下 open 是长驻循环，只有异常/被杀才会走到这里。
        if (!"FAILED".equals(phase) && !fullAndIncre) {
            phase = "DONE";
            writeProgress();
            logger.info("仅全量任务完成，共复制 {} 个键", copiedKeys);
        }
    }

    // ==================== 全量（RDB → RESTORE） ====================

    private void applyRdbKey(KeyValuePair<?, ?> kv) {
        long db = kv.getDb() != null ? kv.getDb().getDbNumber() : 0;
        if (!isSelected(db)) {
            return;
        }
        Object k = kv.getKey();
        Object v = kv.getValue();
        if (!(k instanceof byte[]) || !(v instanceof byte[])) {
            logger.warn("跳过非字节键值（RdbVisitor 非 Dump 模式?）: {}", kv.getClass().getSimpleName());
            return;
        }
        byte[] key = (byte[]) k;
        byte[] dump = (byte[]) v;

        // 逻辑库切换必须先把缓冲批落库：pipeline 与 SELECT 走同一条连接，
        // 缓冲里的键属于切换前那个库，晚一步 flush 就会被写进新库。
        if (targetSelectedDb != (int) db) {
            flushRestoreBatch();
        }
        selectTarget(db);

        long ttl = 0;
        boolean absTtl = false;
        Long expiredMs = kv.getExpiredMs();
        if (expiredMs != null) {
            // ABSTTL：直接用绝对过期时间（毫秒），避免与源库的时钟差；已过期的键 RESTORE 会即刻不可见。
            absTtl = true;
            ttl = expiredMs;
        }
        restoreChannel.add(new RedisRestoreChannel.Entry(key, dump, ttl, absTtl));
        currentDb = db;
        if (restoreChannel.isFull()) {
            flushRestoreBatch();
        }
    }

    /** 提交缓冲的一批 RESTORE，并按实际落库条数推进进度。 */
    private void flushRestoreBatch() {
        if (restoreChannel == null || restoreChannel.isEmpty()) {
            return;
        }
        long[] r = restoreChannel.flush();
        copiedKeys += r[0];
        sinceFlush += r[0] + r[1];
        if (sinceFlush >= PROGRESS_FLUSH_EVERY) {
            writeProgress();
            sinceFlush = 0;
        }
    }

    private void onFullDone(Replicator rep) {
        // 收尾批必须在"全量完成"之前落库：否则最后不满一批的键会随进程停在缓冲里，
        // 任务却已标记全量完成——目标端静默少键。
        flushRestoreBatch();
        persistSnapshotPosition(rep);
        logger.info("全量完成: 共复制 {} 个键（{}）", copiedKeys, restoreChannel.stats().summary());
        if (!fullAndIncre) {
            phase = "DONE";
            writeProgress();
            closeQuietly(rep); // 结束 open() 长驻循环
        } else {
            phase = "INCREMENT";
            writeProgress();
            logger.info("进入增量（PSYNC 复制命令流）");
        }
    }

    /**
     * 落盘全量快照位点。
     *
     * <p>Redis 这条链路<b>天生就是一致性快照</b>：全量走的是 PSYNC 拿到的 RDB，那本来就是源库在
     * 某个复制偏移上的完整镜像，随后的复制命令流严格从该偏移接续——不存在 SQL 侧"各页看到各自
     * 时刻的库"的问题，也就不需要额外的快照机制。缺的只是把这个点<b>按统一格式暴露出来</b>，
     * 让它与其它链路一样可观测（复制 ID + 偏移，等价于 MySQL 的 GTID / PG 的 LSN）。
     */
    private void persistSnapshotPosition(Replicator rep) {
        try {
            com.moilioncircle.redis.replicator.Configuration conf = rep.getConfiguration();
            String position = "replid:" + conf.getReplId() + ";offset:" + conf.getReplOffset();
            com.migration.common.snapshot.SnapshotPosition.write(taskId, "CONSISTENT", "redis", position);
            logger.info("全量快照位点（RDB 天然一致）: {}", position);
        } catch (Exception e) {
            logger.debug("记录 Redis 快照位点失败: {}", e.getMessage());
        }
    }

    // ==================== 增量（复制命令流 verbatim 转发） ====================

    /**
     * 用已落盘的复制位点尝试<b>部分重同步</b>（PSYNC replid offset+1）。
     *
     * <p>Redis 这条链路此前<b>没有增量位点</b>：进程一重启就整库重来——清空目标、
     * 把源库所有键重新 RESTORE 一遍。库越大越致命，而中断窗口里的变更还得靠随后的复制流补。
     * 记下 replid + offset 之后，重启先试部分重同步：源端 backlog 还覆盖得住就直接从断点接着收，
     * 一个键都不用重搬。
     *
     * <p>覆盖不住时源端回 +FULLRESYNC，走的还是原来那条路（PreRdbSyncEvent → 清目标库 → 全量），
     * 所以这里失败没有额外代价，只是回到今天的行为。
     *
     * <p>只在<b>确实进入过增量</b>时才尝试：统一位点文件是在 applyCommand 里写的，
     * 它存在本身就等于"上次跑到过增量阶段"，不必再引入别的标记。
     */
    private void tryResumeFromPersistedOffset(Configuration conf) {
        if (!fullAndIncre) {
            return;
        }
        com.migration.common.position.CheckpointRecord record =
                com.migration.common.position.LocalCheckpointStore.load(
                        taskId, com.migration.common.position.CheckpointRecord.Stage.CAPTURE);
        if (record == null
                || record.getKind() != com.migration.common.position.CheckpointRecord.Kind.REPL_OFFSET) {
            return;
        }
        String replId = record.payloadValue("repl.id");
        String offset = record.payloadValue("repl.offset");
        if (replId == null || replId.trim().isEmpty() || offset == null || offset.trim().isEmpty()) {
            return;
        }
        try {
            long off = Long.parseLong(offset.trim());
            conf.setReplId(replId.trim()).setReplOffset(off);
            // 部分重同步成功时不会有 RDB 阶段（PreRdbSyncEvent/PostRdbSyncEvent 都不触发），
            // 阶段要在这里就摆正，否则进度文件一直显示 FULL，agent 侧的判活与展示都会跟着错。
            phase = "INCREMENT";
            writeProgress();
            logger.info("尝试部分重同步: replid={} offset={}（源端 backlog 不足会自动回退全量）", replId, off);
        } catch (NumberFormatException e) {
            logger.warn("已落盘的复制偏移格式异常，回退全量重同步: {}", offset);
        }
    }

    /**
     * 落盘复制位点（replid + offset）。
     *
     * <p>由 applyCommand 驱动：PSYNC 每 ~10s 的 PING 也会走到那里，所以源库空闲时位点照样保鲜。
     * 按 1s 节流——offset 每条命令都在变，没必要每条都 fsync。
     */
    private void saveReplPosition() {
        Configuration conf = replConfiguration;
        if (conf == null || conf.getReplId() == null || conf.getReplId().isEmpty()) {
            return;
        }
        try {
            java.util.Properties payload = new java.util.Properties();
            payload.setProperty("repl.id", conf.getReplId());
            payload.setProperty("repl.offset", String.valueOf(conf.getReplOffset()));
            payload.setProperty("carrier", "redis");
            com.migration.common.position.LocalCheckpointStore.saveThrottled(
                    new com.migration.common.position.CheckpointRecord(
                            taskId,
                            com.migration.common.position.CheckpointRecord.Stage.CAPTURE,
                            "redis",
                            com.migration.common.position.CheckpointRecord.Kind.REPL_OFFSET,
                            payload,
                            com.migration.common.position.MonotonicKey.ofNumeric(conf.getReplOffset()),
                            0L),
                    1000L, false);
        } catch (Exception e) {
            logger.debug("落盘 Redis 复制位点失败: {}", e.getMessage());
        }
    }

    private void applyCommand(DefaultCommand cmd) {
        // 按时间兜底刷新进度文件：PSYNC 每 ~10s 发 PING（也会走到这里），据此即便源库空闲、
        // 无数据命令，进度文件的 mtime 也会持续推进——agent 的僵死看门狗据此判活；引擎一旦冻结，
        // 本方法不再被回调，进度文件立刻停更、被检出。
        long nowMs = System.currentTimeMillis();
        if (nowMs - lastProgressWriteMs >= PROGRESS_TIME_REFRESH_MS) {
            writeProgress();
        }
        saveReplPosition();

        String name = new String(cmd.getCommand(), StandardCharsets.UTF_8).toUpperCase();
        byte[][] cargs = cmd.getArgs();

        switch (name) {
            case "SELECT":
                if (cargs.length >= 1) {
                    streamDb = Long.parseLong(new String(cargs[0], StandardCharsets.UTF_8).trim());
                }
                return; // 不转发：目标库切换由 selectTarget 统一管理
            case "PING":
            case "REPLCONF":
                return; // 复制保活，非数据
            default:
                break;
        }

        if (!isSelected(streamDb)) {
            return; // 该命令属于未选中的逻辑库
        }

        selectTarget(streamDb);

        // FLUSHALL 会清空目标全部库；若仅选中子集，退化为 FLUSHDB（只清当前选中库），避免误伤其它库。
        if ("FLUSHALL".equals(name) && selectedDbs != null) {
            target.sendCommand(rawCommand("FLUSHDB"));
        } else {
            target.sendCommand(rawCommand(name), cargs);
        }

        incrCommands++;
        currentDb = streamDb;
        if (++sinceFlush >= PROGRESS_FLUSH_EVERY) {
            writeProgress();
            sinceFlush = 0;
        }
    }

    // ==================== 连接 / 库过滤 ====================

    private void selectTarget(long db) {
        if (targetSelectedDb != (int) db) {
            target.select((int) db);
            targetSelectedDb = (int) db;
        }
    }

    private boolean isSelected(long db) {
        return selectedDbs == null || selectedDbs.contains(db);
    }

    private Jedis buildJedis(String prefix) {
        String host = props.getProperty(prefix + ".db.host", "localhost");
        int port = Integer.parseInt(props.getProperty(prefix + ".db.port", "6379"));
        String user = props.getProperty(prefix + ".db.username", "");
        String password = CredentialCipher.decrypt(props.getProperty(prefix + ".db.password", ""));

        DefaultJedisClientConfig.Builder cfg = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(15000)
                .socketTimeoutMillis(60000);
        // 仅 requirepass / 默认用户时用单参 AUTH（user 空或 "default"），兼容 Redis 5/6；
        // 真实 ACL 用户才走双参 AUTH。
        if (user != null && !user.isEmpty() && !"default".equalsIgnoreCase(user)) {
            cfg.user(user);
        }
        if (password != null && !password.isEmpty()) {
            cfg.password(password);
        }
        return new Jedis(new HostAndPort(host, port), cfg.build());
    }

    /** 源端认证注入 redis-replicator：单参/双参 AUTH 语义与 {@link #buildJedis} 保持一致。 */
    private void applyAuth(Configuration conf, String prefix) {
        String user = props.getProperty(prefix + ".db.username", "");
        String password = CredentialCipher.decrypt(props.getProperty(prefix + ".db.password", ""));
        if (user != null && !user.isEmpty() && !"default".equalsIgnoreCase(user)) {
            conf.setAuthUser(user);
        }
        if (password != null && !password.isEmpty()) {
            conf.setAuthPassword(password);
        }
    }

    /**
     * 全量开始前清空目标的同步范围：选中库集为空（整实例同步）则 FLUSHALL；否则逐个选中库 FLUSHDB。
     * 只清同步范围内的库，不误伤目标上其它库。清库失败仅告警不中断——最坏退回“可能残留幽灵键”，
     * 不比不清更差。
     */
    private void flushTargetSelectedDbs() {
        try {
            if (selectedDbs == null) {
                target.flushAll();
            } else {
                for (Long db : selectedDbs) {
                    target.select(db.intValue());
                    target.flushDB();
                }
                targetSelectedDb = -1; // 强制下一次 selectTarget 重新 SELECT，避免复用被 flush 期间改动的库号
            }
            logger.info("全量前已清空目标同步范围: {}", selectedDbs == null ? "ALL" : selectedDbs);
        } catch (Exception e) {
            logger.warn("全量前清空目标失败（继续，可能残留已删除键）: {}", e.getMessage());
        }
    }

    /** 全量进度分母（best-effort）：选中库 DBSIZE 之和；失败则为 0（前端仅显示计数不显示百分比）。 */
    private long computeTotalKeys() {
        try (Jedis src = buildJedis("source")) {
            long sum = 0;
            if (selectedDbs != null) {
                for (Long db : selectedDbs) {
                    src.select(db.intValue());
                    sum += src.dbSize();
                }
            } else {
                for (String line : src.info("keyspace").split("\\r?\\n")) {
                    // 形如 db0:keys=5,expires=0,avg_ttl=0
                    int idx = line.indexOf("keys=");
                    if (line.startsWith("db") && idx > 0) {
                        String n = line.substring(idx + 5);
                        int comma = n.indexOf(',');
                        sum += Long.parseLong(comma > 0 ? n.substring(0, comma) : n);
                    }
                }
            }
            return sum;
        } catch (Exception e) {
            logger.debug("计算源库键总数失败（忽略）: {}", e.getMessage());
            return 0;
        }
    }

    // ==================== 同步对象解析 ====================

    /** 解析 migration.sync.objects 的库索引（键为 "0"/"3"…）；空/全库返回 null。 */
    private static Set<Long> parseSelectedDbs(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            Map<String, Object> raw = gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
            if (raw == null || raw.isEmpty()) {
                return null;
            }
            Set<Long> dbs = new HashSet<>();
            for (String key : raw.keySet()) {
                try {
                    dbs.add(Long.parseLong(key.trim()));
                } catch (NumberFormatException nfe) {
                    logger.warn("忽略非法 Redis 库索引: {}", key);
                }
            }
            return dbs.isEmpty() ? null : dbs;
        } catch (Exception e) {
            logger.warn("解析 sync objects 失败，按全库同步: {}", e.getMessage());
            return null;
        }
    }

    // ==================== 工具 ====================

    private static ProtocolCommand rawCommand(String name) {
        final byte[] raw = name.getBytes(StandardCharsets.UTF_8);
        // ProtocolCommand 的 SAM 是 Rawable.getRaw()，可用 lambda 直接实现。
        return () -> raw;
    }

    private void closeTarget() {
        if (target != null) {
            try {
                target.close();
            } catch (Exception ignore) {
                // best-effort
            }
        }
    }

    private static void closeQuietly(Replicator replicator) {
        try {
            replicator.close();
        } catch (Exception ignore) {
            // best-effort
        }
    }

    private void writeProgress() {
        lastProgressWriteMs = System.currentTimeMillis();
        try {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("phase", phase);
            p.put("totalKeys", totalKeys);
            p.put("copiedKeys", copiedKeys);
            p.put("incrCommands", incrCommands);
            p.put("currentDb", currentDb);
            if (error != null) {
                p.put("error", error);
            }
            p.put("updatedAt", System.currentTimeMillis());

            Files.createDirectories(progressPath.getParent());
            Path tmp = progressPath.resolveSibling(progressPath.getFileName() + ".tmp");
            Files.write(tmp, gson.toJson(p).getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, progressPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            logger.debug("写进度文件失败: {}", e.getMessage());
        }
    }

    // 保留供潜在的对象级扩展（当前 db 级同步未使用）。
    @SuppressWarnings("unused")
    private static List<String> asList(String csv) {
        return Arrays.asList(csv.split(","));
    }
}
