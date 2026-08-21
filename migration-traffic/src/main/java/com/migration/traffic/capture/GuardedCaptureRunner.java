package com.migration.traffic.capture;

import com.migration.common.ssl.SslMaterial;
import com.migration.common.traffic.TrafficSourceState;
import com.migration.traffic.model.RecordingManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 带源库守护与体量护栏的捕获执行体。
 *
 * <p>在 {@link TrafficCaptureRunner} 之上补三件事：
 * <ol>
 *   <li><b>源库原始开关落盘</b>（{@link TrafficSourceState}）：本进程被 {@code kill -9} 时
 *       没机会还原，得让 agent 拿着这份记录去兜底。只放内存等于没有兜底。</li>
 *   <li><b>体量护栏</b>：时长 / 字节 / 条数任一到顶就自动封口，任务转 COMPLETED 而不是 FAILED——
 *       录制到上限是<b>预期内的正常结束</b>，不是故障。没有护栏，一个忘了停的捕获任务
 *       会把 agent 磁盘和源库 datadir 一起写满。</li>
 *   <li><b>续录与时间轴空洞</b>：语句流不是持久化日志，停摆期间的语句<b>永久拿不回来</b>。
 *       续录只能沿用原时间轴、并如实记一条空洞，绝不能假装接上了。</li>
 * </ol>
 */
public class GuardedCaptureRunner extends TrafficCaptureRunner {

    private static final Logger logger = LoggerFactory.getLogger(GuardedCaptureRunner.class);

    private long maxDurationMs;
    private long maxBytes;
    private long maxRecords;

    /** 到达体量上限而正常结束（不是失败）。 */
    private volatile boolean limitReached;
    private volatile String limitReason;

    /** 连续多少轮读回的量超过高水位就上报追不上；一两轮尖峰不算。 */
    private static final int BACKLOG_ALARM_ROUNDS = 5;
    private long backlogHighWater;
    private int backlogStreak;
    private boolean backlogReported;

    public GuardedCaptureRunner(Properties props) {
        super(props);
    }

    @Override
    protected void start() throws Exception {
        maxDurationMs = longProp("traffic.capture.max.duration.ms", 2 * 60 * 60 * 1000L);
        maxBytes = longProp("traffic.capture.max.bytes", 20L * 1024 * 1024 * 1024);
        maxRecords = longProp("traffic.capture.max.records", 100_000_000L);
        backlogHighWater = longProp("traffic.capture.rotate.rows", 200_000L);
        super.start();
    }

    /**
     * 续录：目录里已有 manifest 就沿用它的时间轴原点，并把停摆的这一段记成空洞。
     *
     * <p>为什么不能重新取原点：偏移是相对原点算的。换原点等于把续录段整体平移到
     * 另一条时间轴上，回放出来的间隔全是错的，而文件本身看不出任何异常。
     */
    @Override
    protected RecordingManifest openManifest() throws Exception {
        RecordingManifest existing = TrafficWriter.loadManifest(recordingDir);
        if (existing == null || existing.t0EpochMicros <= 0) {
            return super.openManifest();
        }
        logger.info("检测到已有录制，进入续录模式: t0={}us", existing.t0EpochMicros);
        RecordingRecovery.Scan scan = RecordingRecovery.rebuild(recordingDir, existing);

        // 沿用原时间轴
        t0Micros = existing.t0EpochMicros;
        seq = scan.maxN;
        existing.sealed = false;
        existing.sha256 = null;
        existing.source = source.fingerprint();
        applyFilterToManifest(existing);

        // PG 的语句流落在服务端日志文件里，(文件名, 字节偏移) 是<b>真位点</b>：
        // 只要那个文件还没被轮转清掉，停摆期间的语句还在，能接着读回来，不产生空洞。
        // MySQL 的 general_log 被读一次就没了、Oracle 的审计记录会被清理，那两家没有这个选项。
        boolean resumed = false;
        if (existing.checkpoint != null && existing.checkpoint.file != null) {
            source.resumeFrom(existing.checkpoint);
            resumed = source.checkpoint() != null;
            if (resumed) {
                logger.info("从位点续读，停摆期间的语句可以补回来: file={}, offset={}",
                        existing.checkpoint.file, existing.checkpoint.offset);
            }
        }

        long resumeAtT = nowSourceOffsetMicros();
        if (!resumed && resumeAtT > scan.lastT) {
            existing.gaps.add(new RecordingManifest.Gap(scan.lastT, resumeAtT, "CAPTURE_RESUMED"));
            logger.warn("时间轴空洞: {}us ~ {}us（约 {}s）—— 这段时间源库执行的语句已永久丢失，"
                            + "语句流没有位点可续，只能如实记为空洞",
                    scan.lastT, resumeAtT, (resumeAtT - scan.lastT) / 1_000_000L);
        }
        return existing;
    }

    /** 相对原时间轴的当前偏移（源库时钟）。 */
    private long nowSourceOffsetMicros() {
        long nowSrc = source.t0Micros();   // open() 时刚取的源库 NOW(6)
        return Math.max(0L, nowSrc - t0Micros);
    }

    /**
     * 把源库的原始开关值落盘。
     *
     * <p>这是"四道保险"的第一道：只有它落了盘，agent 的看门狗与重启扫尾才知道
     * 该把 {@code general_log} 还原成什么。
     */
    @Override
    protected void onStarted() throws Exception {
        // 续录时空洞落在段边界上，恢复时更好定位
        if (!writer.manifest().gaps.isEmpty() && writer.manifest().segments.size() > 0) {
            writer.rollSegment();
        }
        TrafficSourceState state = new TrafficSourceState();
        state.engine = engine.wireName();
        state.host = props.getProperty("source.db.host", "localhost");
        state.port = props.getProperty("source.db.port", defaultPort());
        state.username = props.getProperty("source.db.username", "root");
        state.password = props.getProperty("source.db.password", "");
        state.urlParams = sourceUrlParams();
        state.database = props.getProperty("source.db.database", "");

        java.util.Map<String, String> restore = source.restoreState();

        // 续录时<b>必须沿用上一次落盘的原值</b>，不能用刚读到的当前值。
        //
        // 上一轮如果是被 kill -9 掉的，源端的开关还是我们改过的那副样子（general_log=ON /
        // log_statement=all / 审计策略还开着）。这一轮再去读"原值"，读到的就是<b>我们自己留下的痕迹</b>——
        // 于是收尾时忠实地把它"还原"成开着，源端从此再也回不去了，而且全程零报错。
        // 状态文件才是"我们动手之前它长什么样"的唯一权威。
        TrafficSourceState prior = TrafficSourceState.load(recordingDir);
        if (prior != null && state.engine.equalsIgnoreCase(prior.engine) && !prior.attrs.isEmpty()) {
            logger.warn("发现上一轮遗留的源端状态文件（上次多半是崩溃退出的），"
                    + "沿用它记录的原值而不是当前值: {}", prior.attrs);
            restore = prior.attrs;
        }

        state.attrs.putAll(restore);
        // MySQL 的两个键有独立的落盘位置（老状态文件的格式），保持兼容
        state.generalLog = restore.get("general_log");
        state.logOutput = restore.get("log_output");
        state.save(recordingDir);
        logger.info("源端原始状态已落盘（兜底还原依据）: engine={}, {}", state.engine, restore);
        // 引擎自己也要按这份原值还原，否则子进程正常收尾时仍会还原成"当前值"
        source.overrideRestoreState(restore);
    }

    /** 兜底还原要用的 JDBC 参数段。Oracle 的 thin URL 没有查询串，信任材料走连接属性。 */
    private String sourceUrlParams() {
        SslMaterial ssl = SslMaterial.from(props, "source");
        switch (engine) {
            case POSTGRESQL: return ssl.pgUrlParams();
            case ORACLE: return "";
            default: return ssl.mysqlUrlParams();
        }
    }

    private String defaultPort() {
        switch (engine) {
            case POSTGRESQL: return "5432";
            case ORACLE: return "1521";
            default: return "3306";
        }
    }

    /** 体量护栏。到顶即停，但这是<b>正常结束</b>。 */
    @Override
    protected boolean onTick() {
        checkBacklog();
        // 位点每轮更新进 manifest：崩溃后的续读依据就是它，只在封口时写等于白写
        RecordingManifest.Checkpoint cp = source.checkpoint();
        if (cp != null) {
            writer.manifest().checkpoint = cp;
        }
        long elapsed = System.currentTimeMillis() - startedAtMs;
        if (maxDurationMs > 0 && elapsed >= maxDurationMs) {
            return reachLimit("已达最大录制时长 " + maxDurationMs + "ms");
        }
        long bytes = writer.manifest().totalBytes();
        if (maxBytes > 0 && bytes >= maxBytes) {
            return reachLimit("已达最大录制体积 " + maxBytes + " 字节");
        }
        long records = writer.manifest().stats.total;
        if (maxRecords > 0 && records >= maxRecords) {
            return reachLimit("已达最大录制条数 " + maxRecords);
        }
        return false;
    }

    /**
     * 追不上的早期信号：连续多轮读回的语句量都压着高水位。
     *
     * <p>只报一次（{@code backlogReported}）。这不是致命错误——捕获还在跑、
     * 数据也没丢，但源库上的日志表会越堆越大并反过来拖慢源库，属于"再不干预就要出事"。
     */
    private void checkBacklog() {
        long backlog = source.backlog();
        if (backlog < 0) return;
        if (backlog > backlogHighWater) {
            backlogStreak++;
        } else {
            backlogStreak = 0;
        }
        if (backlogStreak >= BACKLOG_ALARM_ROUNDS && !backlogReported) {
            backlogReported = true;
            String msg = "捕获追不上源库产生速度：连续 " + backlogStreak + " 轮每轮读回 "
                    + backlog + " 条（高水位 " + backlogHighWater + "）";
            logger.error(msg);
            com.migration.traffic.TrafficErrorStatus.report(taskId, "E3122", msg);
        }
    }

    private boolean reachLimit(String reason) {
        limitReached = true;
        limitReason = reason;
        logger.info("流量复制到达体量上限，自动封口: {}", reason);
        return true;
    }

    @Override
    protected void shutdown() {
        super.shutdown();
        // 源库开关已由 super.shutdown() → source.close() 还原成功，状态文件就没用了。
        // 还原失败时<b>保留</b>文件：那正是 agent 兜底流程要用的东西。
        if (sourceRestored()) {
            TrafficSourceState.clear(recordingDir);
        } else {
            logger.error("源库开关未确认还原，保留 {} 供 agent 兜底还原",
                    TrafficSourceState.fileIn(recordingDir).getAbsolutePath());
        }
    }

    private boolean sourceRestored() {
        return source != null && source.isRestored();
    }

    public boolean isLimitReached() {
        return limitReached;
    }

    public String limitReason() {
        return limitReason;
    }

    @Override
    protected String resultOutcome() {
        return limitReached ? "COMPLETED" : "STOPPED";
    }

    @Override
    protected String resultReason() {
        return limitReason;
    }
}
