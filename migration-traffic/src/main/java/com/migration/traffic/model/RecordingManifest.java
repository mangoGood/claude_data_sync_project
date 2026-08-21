package com.migration.traffic.model;

import java.util.ArrayList;
import java.util.List;

/** 录制文件的头：分段清单 + 时间轴空洞 + 统计。gson 直接映射。 */
public final class RecordingManifest {

    /**
     * 当前格式版本。
     *
     * <p>v1 → v2 的唯一原因是<b>绑定参数</b>：PG 与 Oracle 都不把参数替换进 SQL 文本，
     * v1 的记录行没有地方装它们（见 {@link TrafficRecord#b}）。顺带补上 engine 与
     * 可续位点两个字段。
     */
    public static final String FORMAT = "synctask-traffic/2";
    /** v1：只有 MySQL，没有 engine / b / checkpoint 字段。读到它一律按 MySQL 处理。 */
    public static final String FORMAT_V1 = "synctask-traffic/1";

    public String format = FORMAT;

    /**
     * 录制来自哪种引擎（{@link TrafficEngine#wireName()}）。
     * v1 录制没有这个字段 → {@link #engineOf()} 回落到 MySQL。
     */
    public String engine = TrafficEngine.MYSQL.wireName();

    /**
     * 捕获通道：{@code GENERAL_LOG}（MySQL）/ {@code PG_JSONLOG} / {@code PG_CSVLOG}
     * / {@code ORA_UNIFIED_AUDIT}。写进文件是为了让回放报告能说清"这份录制是怎么来的"——
     * 不同通道的完整性与精度不一样（比如 PG 的时间轴只有毫秒）。
     */
    public String captureBackend;

    public String captureTaskId;
    public String captureTaskName;

    /** 时间轴原点的绝对时刻（ISO-8601，带时区），仅供展示；调度只用记录里的相对偏移。 */
    public String t0Wall;
    /**
     * 时间轴原点的<b>源库 epoch 微秒</b>。
     *
     * <p>续录时必须沿用它而不是重新取一次：偏移是相对原点算的，换了原点，
     * 续录段的偏移就和之前那段不在同一条时间轴上了——回放出来的间隔全是错的，
     * 而文件本身看不出任何异常。
     */
    public long t0EpochMicros;
    public String endWall;
    public long durationMs;

    public SourceFingerprint source;
    public Filter filter = new Filter();
    public List<Segment> segments = new ArrayList<>();
    public List<Gap> gaps = new ArrayList<>();
    public Stats stats = new Stats();

    /**
     * 捕获侧的可续位点。
     *
     * <p><b>只有 PG 有</b>：它的语句流落在服务端日志文件里，
     * {@code (文件名, 字节偏移)} 就是一个真正的位点——捕获重启后能接着读，
     * 通常<b>不产生时间轴空洞</b>。MySQL 的 general_log 被读一次就没了，
     * Oracle 的审计记录会被清理，两者都只能靠 {@link #gaps} 如实记录停摆。
     */
    public Checkpoint checkpoint;

    /** 是否已封口。运行期恒为 false；崩溃恢复时靠扫描分段重建。 */
    public boolean sealed;
    /** 全部分段按序号拼接后的 SHA-256（封口时算）。 */
    public String sha256;

    /** PG 的日志读位点。跨文件轮转时按文件名推进。 */
    public static final class Checkpoint {
        public String file;
        public long offset;

        public Checkpoint() {
        }

        public Checkpoint(String file, long offset) {
            this.file = file;
            this.offset = offset;
        }
    }

    public static final class Filter {
        public List<String> databases = new ArrayList<>();
        public List<String> users = new ArrayList<>();
        public List<String> classes = new ArrayList<>();
        public double sampleRate = 1.0;
        public boolean enrich;
    }

    public static final class Segment {
        public int seq;
        public String file;
        public long records;
        public long firstN;
        public long lastN;
        public long firstT;
        public long lastT;
        public long bytes;
        public String sha256;
    }

    /**
     * 时间轴空洞：捕获停摆（暂停 / 进程崩溃 / 源库重启）期间源库执行的语句<b>永久丢失</b>。
     * 语句流不是持久化日志，没有位点可续——这一段只能诚实地记成空洞，不能假装续上了。
     */
    public static final class Gap {
        public long fromT;
        public long toT;
        public String reason;

        public Gap() {
        }

        public Gap(long fromT, long toT, String reason) {
            this.fromT = fromT;
            this.toT = toT;
            this.reason = reason;
        }

        public long durationUs() {
            return Math.max(0L, toT - fromT);
        }
    }

    public static final class Stats {
        public long total;
        public long select;
        public long dml;
        public long ddl;
        public long dcl;
        public long tcl;
        public long other;
        /** 连接建立/断开的条数。它们不是语句，单独计——混进 other 会让"其它"看起来大得莫名其妙。 */
        public long connEvents;
        public long redacted;
        public long sessions;
        public long maxConcurrentSessions;
        /** 捕获侧因采样/过滤丢弃的条数，用于回答"为什么录到的比源库少"。 */
        public long filtered;
    }

    /** 本录制的引擎；v1 录制（无字段）回落 MySQL。 */
    public TrafficEngine engineOf() {
        return TrafficEngine.parse(engine);
    }

    /** 是否是 v1 格式（没有 engine / 绑定参数）。 */
    public boolean isLegacyV1() {
        return format == null || FORMAT_V1.equals(format);
    }

    public long totalRecords() {
        long sum = 0;
        for (Segment s : segments) sum += s.records;
        return sum;
    }

    public long totalBytes() {
        long sum = 0;
        for (Segment s : segments) sum += s.bytes;
        return sum;
    }
}
