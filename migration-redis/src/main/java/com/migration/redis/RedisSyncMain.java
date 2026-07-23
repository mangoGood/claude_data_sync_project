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
import redis.clients.jedis.params.RestoreParams;

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
        totalKeys = computeTotalKeys();
        writeProgress();

        Configuration conf = Configuration.defaultSetting()
                .setConnectionTimeout(15000)
                .setReadTimeout(60000)
                // 断点/断线后由 agent 的 ProcessGuard 负责重启整进程（重跑全量幂等），
                // 这里禁用 replicator 内部无限重试，让致命错误快速冒泡为任务失败。
                .setRetries(1);
        applyAuth(conf, "source");

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

        selectTarget(db);
        RestoreParams params = RestoreParams.restoreParams().replace();
        long ttl = 0;
        Long expiredMs = kv.getExpiredMs();
        if (expiredMs != null) {
            // ABSTTL：直接用绝对过期时间（毫秒），避免与源库的时钟差；已过期的键 RESTORE 会即刻不可见。
            params.absTtl();
            ttl = expiredMs;
        }
        target.restore(key, ttl, dump, params);

        copiedKeys++;
        currentDb = db;
        if (++sinceFlush >= PROGRESS_FLUSH_EVERY) {
            writeProgress();
            sinceFlush = 0;
        }
    }

    private void onFullDone(Replicator rep) {
        logger.info("全量完成: 共复制 {} 个键", copiedKeys);
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

    // ==================== 增量（复制命令流 verbatim 转发） ====================

    private void applyCommand(DefaultCommand cmd) {
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
