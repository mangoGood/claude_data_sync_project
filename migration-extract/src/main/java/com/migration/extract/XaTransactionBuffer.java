package com.migration.extract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 源库 XA 事务的「缓冲到提交点再下发」。
 *
 * <h3>为什么必须缓冲</h3>
 * MySQL 的 XA 事务在 binlog 里是<b>拆成两段</b>写的（实测 8.0.44）：
 * <pre>
 *   Query      XA START X'..',X'..',1     ← 行事件在 prepare 时刻就落 binlog 了
 *   Table_map / Write_rows / ...
 *   Query      XA END   X'..',X'..',1
 *   XA_prepare XA PREPARE X'..',X'..',1
 *   ……（中间穿插任意多个其它事务）……
 *   Query      XA COMMIT X'..',X'..',1    ← 提交决议在这里，也可能是 XA ROLLBACK
 * </pre>
 * 直接按流式下发会踩三个坑：行事件在 prepare 时刻就被应用到目标库，源库随后
 * {@code XA ROLLBACK} 就在目标库留下<b>永久幻影行</b>；{@code XA START} 这类控制语句被当成
 * 普通语句打到目标库会让应用连接卡在 XA ACTIVE 态（{@code COMMIT}/{@code ROLLBACK} 双双
 * 报 1399，任务永久停摆）；而且整个 XA 事务在目标库还是逐行提交、不具原子性。
 *
 * <p>所以这里把 {@code XA START … XA PREPARE} 之间的原始 {@code .cap} 行整段扣下来落盘，
 * 直到看见决议：{@code XA COMMIT} 才按<b>普通事务</b>的形态重放进 THL（同一个 {@code tx_id}、
 * 末尾补一个 COMMIT 事件带 {@code tx_last}），{@code XA ROLLBACK} 则整段丢弃。
 *
 * <h3>为什么"推迟到 XA COMMIT 才下发"不会打乱顺序</h3>
 * XA 分支在 commit 之前一直持有它改过的行锁与表 MDL，prepare 与 commit 之间的任何事务都
 * 碰不到同一行、也做不了同表 DDL，读到的更是 XA 提交前的旧值。所以"在 XA COMMIT 处应用"
 * 恰恰比"在 prepare 处应用"更贴合源库自己的可见性顺序。
 *
 * <h3>崩溃语义</h3>
 * 分支落盘文件 {@code <sha256(xid)>.part}（收集中）→ prepare 时 fsync + rename 成
 * {@code .xa}（已就绪，跨重启可用）。重启时 {@code .part} 一律删掉——收集中的分支靠
 * 调用方把 {@code .cap} 读取进度<b>压回分支起点</b>重新收集（见 ContinuousExtractMain 的
 * 进度快照）。整条链路仍是"至少一次"：重放中途崩溃会让该分支在下一轮重放一遍，
 * 应用端按主键幂等吸收。
 */
public final class XaTransactionBuffer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(XaTransactionBuffer.class);

    /** 从 {@code QueryEventData{... sql='XA START ...'}} 里取 SQL，与 parseQueryEvent 同口径 */
    private static final Pattern SQL_IN_QUERY = Pattern.compile("sql='(.+)'\\}$");

    /** XA 控制语句：动词 + xid（xid 里可能带 {@code ONE PHASE} 后缀） */
    private static final Pattern XA_STMT = Pattern.compile(
            "^\\s*XA\\s+(START|BEGIN|END|PREPARE|COMMIT|ROLLBACK)\\s+(.+?)\\s*$",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern ONE_PHASE_TAIL = Pattern.compile(
            "\\s+ONE\\s+PHASE\\s*$", Pattern.CASE_INSENSITIVE);

    /** {@code XAPrepareEventData{onePhase=true, ...}} */
    private static final Pattern ONE_PHASE_FLAG = Pattern.compile("onePhase=(true|false)");

    /** 分支落盘文件首行：{@code #XA <base64(xid)> <startFile>:<startPos> <epochMs>} */
    private static final String HEADER_PREFIX = "#XA ";

    /** 一条 {@code .cap} 行经 {@link #inspect} 判定后的去向。 */
    public enum Verdict {
        /** 与 XA 无关，照常解析下发。 */
        PASS,
        /** XA 控制语句，本身不产生任何目标端动作，直接丢弃。 */
        DROP,
        /** 属于某个未决 XA 分支，已扣下落盘，本轮不下发。 */
        HELD,
        /** 决议到达，有分支可以重放了（调用方随后取 {@link #takeReplay()}）。 */
        REPLAY
    }

    /** 一个 XA 分支：从 {@code XA START} 到决议为止的全部原始 {@code .cap} 行。 */
    public static final class Branch {
        private final String xid;
        private final String key;
        private File spool;
        private Writer writer;
        /** 留着 writer 底下的这个句柄，收口时才 fsync 得到（只读 FD 的 sync 在部分平台会抛异常） */
        private FileOutputStream sink;
        private long bytes;
        private final long openedAtMs;
        private String startFile;
        private long startPos;
        /** 决议（XA COMMIT）所在位点与时间戳：重放出来的事件一律改写成它，保证位点单调 */
        private String commitFile = "";
        private long commitPos;
        private long commitTimestamp;

        private Branch(String xid, String key, long openedAtMs) {
            this.xid = xid;
            this.key = key;
            this.openedAtMs = openedAtMs;
        }

        public String getXid() { return xid; }
        public String getKey() { return key; }
        public String getCommitFile() { return commitFile; }
        public long getCommitPos() { return commitPos; }
        public long getCommitTimestamp() { return commitTimestamp; }
        public long getBytes() { return bytes; }
    }

    /** 配额突破：extract 主循环捕获后写 error_status(E3018) 并停下，绝不静默丢分支。 */
    public static final class XaQuotaExceededException extends RuntimeException {
        public XaQuotaExceededException(String message) {
            super(message);
        }
    }

    private final boolean enabled;
    private final File spoolDir;
    private final long branchMaxBytes;
    private final int pendingMaxBranches;
    private final long pendingMaxBytes;
    private final long pendingWarnMs;

    /** 正在收集的分支（binlog 里 START…PREPARE 是连续的一段，同一时刻至多一个）。 */
    private Branch active;
    /** 已 prepare、等决议的分支：xid 归一化文本 → 分支。 */
    private final Map<String, Branch> prepared = new LinkedHashMap<>();
    /** 决议已到、等调用方重放的分支。 */
    private Branch readyToReplay;

    private long lastPendingWarnMs;

    public XaTransactionBuffer(Properties props, String outputDir) {
        this.enabled = Boolean.parseBoolean(props.getProperty("sync.xa.enabled", "true"));
        this.spoolDir = new File(outputDir, "xa_spool");
        this.branchMaxBytes = Long.parseLong(props.getProperty("sync.xa.branch.max.bytes", "1073741824"));
        this.pendingMaxBranches = Integer.parseInt(props.getProperty("sync.xa.pending.max.branches", "256"));
        this.pendingMaxBytes = Long.parseLong(props.getProperty("sync.xa.pending.max.bytes", "2147483648"));
        this.pendingWarnMs = Long.parseLong(props.getProperty("sync.xa.pending.warn.minutes", "60")) * 60_000L;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** 是否正在收集某个分支——调用方据此压住 {@code .cap} 读取进度的落盘。 */
    public boolean isBranchActive() {
        return active != null;
    }

    public int pendingBranchCount() {
        return prepared.size();
    }

    /**
     * 启动时的恢复：{@code .xa}（已 prepare）重新纳管，{@code .part}（收集到一半）一律删除。
     *
     * <p>{@code .part} 之所以能删：调用方在有活跃分支时不会把读取进度落盘，重启后会从分支
     * 起点重新读一遍 {@code .cap}，重新收集出完整的一份。留着半截的反而会拼出残缺事务。
     */
    public void recover() {
        if (!enabled) {
            return;
        }
        if (!spoolDir.exists()) {
            spoolDir.mkdirs();
            return;
        }
        File[] files = spoolDir.listFiles();
        if (files == null) {
            return;
        }
        int loaded = 0;
        int dropped = 0;
        for (File f : files) {
            if (f.getName().endsWith(".part")) {
                if (f.delete()) dropped++;
                continue;
            }
            if (!f.getName().endsWith(".xa")) {
                continue;
            }
            try (BufferedReader r = newReader(f)) {
                String header = r.readLine();
                Branch b = parseHeader(header, f);
                if (b == null) {
                    logger.warn("XA 分支落盘文件首行无法识别，跳过: {}", f.getName());
                    continue;
                }
                prepared.put(b.xid, b);
                loaded++;
            } catch (IOException e) {
                logger.warn("读取 XA 分支落盘文件失败，跳过 {}: {}", f.getName(), e.getMessage());
            }
        }
        if (loaded > 0 || dropped > 0) {
            logger.info("XA 分支恢复：已 prepare 未决 {} 个，丢弃收集中的半截分支 {} 个", loaded, dropped);
        }
    }

    /**
     * 判定一条 {@code .cap} 原始行的去向。调用方必须在<b>分配 seqno 之前</b>调用——
     * 被扣下的行不能消耗 seqno，否则 THL 里会出现永久空洞。
     *
     * @param eventType   capture 写下的事件类型名（QUERY / XA_PREPARE / WRITE_ROWS …）
     * @param eventData   事件数据段（{@code QueryEventData{...}} / {@code XAPrepareEventData{...}}）
     * @param rawLine     原始整行，扣下时原样落盘、重放时原样再解析一遍
     * @param binlogFile  该事件的 binlog 文件
     * @param binlogPos   该事件的 binlog 位点
     * @param timestamp   该事件的源库时间戳
     */
    public Verdict inspect(String eventType, String eventData, String rawLine,
                           String binlogFile, long binlogPos, long timestamp) {
        if (!enabled) {
            return Verdict.PASS;
        }

        if ("XA_PREPARE".equals(eventType)) {
            return onPrepareEvent(eventData, binlogFile, binlogPos, timestamp);
        }

        if ("QUERY".equals(eventType)) {
            String sql = sqlOf(eventData);
            Matcher m = sql == null ? null : XA_STMT.matcher(sql);
            if (m != null && m.matches()) {
                return onControlStatement(m.group(1).toUpperCase(), m.group(2),
                        binlogFile, binlogPos, timestamp);
            }
        }

        if (active != null) {
            hold(rawLine);
            return Verdict.HELD;
        }
        return Verdict.PASS;
    }

    private Verdict onControlStatement(String verb, String xidText,
                                       String binlogFile, long binlogPos, long timestamp) {
        boolean onePhase = ONE_PHASE_TAIL.matcher(xidText).find();
        String xid = normalizeXid(ONE_PHASE_TAIL.matcher(xidText).replaceAll(""));

        switch (verb) {
            case "START":
            case "BEGIN":
                openBranch(xid, binlogFile, binlogPos);
                return Verdict.DROP;

            case "END":
                // XA END 只是标记分支不再接收语句，落盘照旧等 XA_prepare 事件收口
                return Verdict.DROP;

            case "PREPARE":
                // 正常情况下 prepare 走的是 XA_prepare 事件类型；这里兜住把它写成 QUERY 的版本
                finishCollecting(onePhase, binlogFile, binlogPos, timestamp);
                return onePhase && readyToReplay != null ? Verdict.REPLAY : Verdict.DROP;

            case "COMMIT":
                if (onePhase) {
                    // XA COMMIT … ONE PHASE 在 binlog 里是 XA_prepare 事件，走不到这儿；兜底同 prepare
                    finishCollecting(true, binlogFile, binlogPos, timestamp);
                    return readyToReplay != null ? Verdict.REPLAY : Verdict.DROP;
                }
                return resolveCommit(xid, binlogFile, binlogPos, timestamp);

            case "ROLLBACK":
                resolveRollback(xid);
                return Verdict.DROP;

            default:
                return Verdict.DROP;
        }
    }

    private Verdict onPrepareEvent(String eventData, String binlogFile, long binlogPos, long timestamp) {
        boolean onePhase = false;
        Matcher m = ONE_PHASE_FLAG.matcher(eventData == null ? "" : eventData);
        if (m.find()) {
            onePhase = Boolean.parseBoolean(m.group(1));
        }
        finishCollecting(onePhase, binlogFile, binlogPos, timestamp);
        return onePhase && readyToReplay != null ? Verdict.REPLAY : Verdict.DROP;
    }

    private void openBranch(String xid, String binlogFile, long binlogPos) {
        if (active != null) {
            // binlog 里 START…PREPARE 必然连续，走到这里说明上一段被截断了
            // （extract 上一轮读到一半就收工，进度已压回起点，本轮从头再收集一遍）。
            // 直接重开：落盘文件被截断重写，不会拼出两段叠加的残缺事务。
            logger.debug("XA 分支 {} 未收口即遇到新的 XA START，丢弃重收", active.xid);
            closeQuietly(active);
            deleteQuietly(active.spool);
        }
        Branch b = new Branch(xid, keyOf(xid), System.currentTimeMillis());
        b.startFile = binlogFile;
        b.startPos = binlogPos;
        try {
            if (!spoolDir.exists()) {
                spoolDir.mkdirs();
            }
            b.spool = new File(spoolDir, b.key + ".part");
            b.sink = new FileOutputStream(b.spool, false);
            b.writer = new BufferedWriter(new OutputStreamWriter(
                    b.sink, StandardCharsets.UTF_8), 1 << 16);
            b.writer.write(HEADER_PREFIX + Base64.getEncoder().encodeToString(
                    xid.getBytes(StandardCharsets.UTF_8))
                    + " " + binlogFile + ":" + binlogPos + " " + b.openedAtMs);
            b.writer.write('\n');
        } catch (IOException e) {
            throw new XaQuotaExceededException("XA 分支落盘文件创建失败 (" + xid + "): " + e.getMessage());
        }
        active = b;
        logger.debug("XA 分支开始收集: {} @ {}:{}", xid, binlogFile, binlogPos);
    }

    private void hold(String rawLine) {
        Branch b = active;
        b.bytes += rawLine.length() + 1L;
        if (b.bytes > branchMaxBytes) {
            throw new XaQuotaExceededException("单个 XA 分支 " + b.xid + " 已缓冲 " + b.bytes
                    + " 字节，超过 sync.xa.branch.max.bytes=" + branchMaxBytes
                    + "。该分支在源库长期未提交，或阈值配得过小");
        }
        try {
            b.writer.write(rawLine);
            b.writer.write('\n');
        } catch (IOException e) {
            throw new XaQuotaExceededException("XA 分支 " + b.xid + " 落盘失败: " + e.getMessage());
        }
    }

    /** {@code XA PREPARE} 到达：落盘收口。一阶段提交等价于 XID，直接进重放队列。 */
    private void finishCollecting(boolean onePhase, String binlogFile, long binlogPos, long timestamp) {
        Branch b = active;
        active = null;
        if (b == null) {
            // 任务的抽取起点落在了 XA START 之后（首次增量取当前位点、或续传位点正好在分支中间）。
            // 这一段的行事件没被扣下、已经按普通事件下发了——如果源库最终是 XA ROLLBACK，
            // 目标库就会留下几行幻影。窗口很窄（只影响启动瞬间正在 prepare 的那个分支），
            // 但值得报出来，让人知道该对这批数据做一次一致性校验。
            logger.warn("收到 XA PREPARE 但没有对应的收集中分支 @ {}:{}——抽取起点落在了该分支的 "
                    + "XA START 之后，这段数据已按普通事件下发；若源库最终回滚该分支，"
                    + "目标库会残留这几行，建议对该时间点做一次一致性校验", binlogFile, binlogPos);
            return;
        }
        try {
            b.writer.flush();
            // fsync 在 close 之前：防的是宿主断电（进程被 KILL 不会丢页缓存，rename 原子性就够）
            b.sink.getFD().sync();
            b.writer.close();
        } catch (IOException e) {
            throw new XaQuotaExceededException("XA 分支 " + b.xid + " 落盘收口失败: " + e.getMessage());
        }
        b.writer = null;
        b.sink = null;

        if (onePhase) {
            // XA COMMIT … ONE PHASE：prepare 即提交，等价于普通事务的 XID
            b.commitFile = binlogFile;
            b.commitPos = binlogPos;
            b.commitTimestamp = timestamp;
            readyToReplay = b;
            logger.debug("XA 分支 {} 一阶段提交，直接重放", b.xid);
            return;
        }

        File ready = new File(spoolDir, b.key + ".xa");
        try {
            Files.move(b.spool.toPath(), ready.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new XaQuotaExceededException("XA 分支 " + b.xid + " 落盘 rename 失败: " + e.getMessage());
        }
        b.spool = ready;
        prepared.put(b.xid, b);
        enforcePendingQuota();
        logger.info("XA 分支已 prepare 待决议: {} ({} 字节，当前未决 {} 个)", b.xid, b.bytes, prepared.size());
    }

    private Verdict resolveCommit(String xid, String binlogFile, long binlogPos, long timestamp) {
        Branch b = prepared.remove(xid);
        if (b == null) {
            // 分支的 prepare 段不在本任务的抽取范围内（任务起点在它之后），没有数据可放
            logger.debug("收到 XA COMMIT 但没有对应的未决分支，忽略: {}", xid);
            return Verdict.DROP;
        }
        b.commitFile = binlogFile;
        b.commitPos = binlogPos;
        b.commitTimestamp = timestamp;
        readyToReplay = b;
        return Verdict.REPLAY;
    }

    private void resolveRollback(String xid) {
        Branch b = prepared.remove(xid);
        if (b == null) {
            // 分支的 prepare 段不在抽取范围内。若它是"起点落在分支中间"的那种（见 finishCollecting
            // 的告警），它的行事件已经下发过了，这里的回滚决议就无从执行——目标库留下幻影行
            logger.warn("收到 XA ROLLBACK 但没有对应的未决分支: {}。若该分支的 prepare 段曾被"
                    + "按普通事件下发过，目标库会残留它的行，请对该表做一次一致性校验", xid);
            return;
        }
        deleteQuietly(b.spool);
        logger.info("XA 分支 {} 在源库回滚，缓冲的 {} 字节整段丢弃（目标库不会留下幻影行）", xid, b.bytes);
    }

    private void enforcePendingQuota() {
        if (prepared.size() > pendingMaxBranches) {
            throw new XaQuotaExceededException("未决 XA 分支已达 " + prepared.size()
                    + " 个，超过 sync.xa.pending.max.branches=" + pendingMaxBranches
                    + "。请检查源库是否有大量长期未提交的 XA 分支（XA RECOVER）");
        }
        long total = 0;
        long oldest = 0;
        long now = System.currentTimeMillis();
        for (Branch b : prepared.values()) {
            total += b.bytes;
            oldest = Math.max(oldest, now - b.openedAtMs);
        }
        if (total > pendingMaxBytes) {
            throw new XaQuotaExceededException("未决 XA 分支累计缓冲 " + total
                    + " 字节，超过 sync.xa.pending.max.bytes=" + pendingMaxBytes
                    + "。请检查源库是否有大量长期未提交的 XA 分支（XA RECOVER）");
        }
        if (oldest > pendingWarnMs && now - lastPendingWarnMs > 60_000L) {
            lastPendingWarnMs = now;
            logger.warn("存在已 prepare 超过 {} 分钟仍未决议的 XA 分支（共 {} 个 / {} 字节）。"
                            + "这些分支的数据在源库提交前不会下发到目标库，属预期行为；"
                            + "若源库侧确实卡住，请用 XA RECOVER 排查",
                    pendingWarnMs / 60_000L, prepared.size(), total);
        }
    }

    /** 取出一个决议已到、可以重放的分支；没有则返回 null。 */
    public Branch takeReplay() {
        Branch b = readyToReplay;
        readyToReplay = null;
        return b;
    }

    /** 打开分支的重放读取器，游标已越过首行文件头。 */
    public BufferedReader openReplayReader(Branch b) throws IOException {
        BufferedReader r = newReader(b.spool);
        r.readLine();   // 文件头
        return r;
    }

    /** 重放成功：删除落盘文件。 */
    public void finishReplay(Branch b) {
        deleteQuietly(b.spool);
    }

    /** 未决分支数与最老分支年龄（毫秒），供指标落盘。 */
    public long oldestPendingAgeMs() {
        long now = System.currentTimeMillis();
        long oldest = 0;
        for (Branch b : prepared.values()) {
            oldest = Math.max(oldest, now - b.openedAtMs);
        }
        return oldest;
    }

    @Override
    public void close() {
        if (active != null) {
            closeQuietly(active);
        }
    }

    // ---- 工具 ----

    /**
     * xid 归一化：折叠空白、十六进制字面量统一大写。
     * {@code XA COMMIT} 与 {@code XA START} 两条语句里的 xid 必须能对上，靠的就是这层归一化。
     */
    static String normalizeXid(String raw) {
        String s = raw == null ? "" : raw.trim();
        s = s.replaceAll("\\s+", "");
        StringBuilder sb = new StringBuilder(s.length());
        boolean inQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                inQuote = !inQuote;
                sb.append(c);
            } else {
                sb.append(inQuote ? Character.toUpperCase(c) : c);
            }
        }
        return sb.toString();
    }

    private static String sqlOf(String eventData) {
        if (eventData == null) {
            return null;
        }
        Matcher m = SQL_IN_QUERY.matcher(eventData);
        return m.find() ? m.group(1) : null;
    }

    private static String keyOf(String xid) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(xid.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(32);
            for (int i = 0; i < 16; i++) {
                sb.append(String.format("%02x", d[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(xid.hashCode());
        }
    }

    private Branch parseHeader(String header, File spool) {
        if (header == null || !header.startsWith(HEADER_PREFIX)) {
            return null;
        }
        String[] parts = header.substring(HEADER_PREFIX.length()).split(" ");
        if (parts.length < 3) {
            return null;
        }
        String xid;
        try {
            xid = new String(Base64.getDecoder().decode(parts[0]), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
        long openedAt;
        try {
            openedAt = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            openedAt = System.currentTimeMillis();
        }
        Branch b = new Branch(xid, keyOf(xid), openedAt);
        b.spool = spool;
        b.bytes = spool.length();
        int colon = parts[1].lastIndexOf(':');
        if (colon > 0) {
            b.startFile = parts[1].substring(0, colon);
            try {
                b.startPos = Long.parseLong(parts[1].substring(colon + 1));
            } catch (NumberFormatException ignored) {
                // 起点位点只用于日志，解析不出来不影响重放
            }
        }
        return b;
    }

    private static BufferedReader newReader(File f) throws IOException {
        return new BufferedReader(new InputStreamReader(
                new FileInputStream(f), StandardCharsets.UTF_8), 1 << 16);
    }

    private static void closeQuietly(Branch b) {
        if (b.writer != null) {
            try {
                b.writer.close();
            } catch (IOException ignored) {
                // 关闭失败无所谓：这条路径上文件马上就要被删掉
            }
            b.writer = null;
            b.sink = null;
        }
    }

    private static void deleteQuietly(File f) {
        if (f != null && f.exists() && !f.delete()) {
            logger.warn("删除 XA 分支落盘文件失败: {}", f.getAbsolutePath());
        }
    }
}
