package com.migration.traffic.model;

import java.util.ArrayList;
import java.util.List;

/** 录制文件的头：分段清单 + 时间轴空洞 + 统计。gson 直接映射。 */
public final class RecordingManifest {

    public static final String FORMAT = "synctask-traffic/1";

    public String format = FORMAT;
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

    /** 是否已封口。运行期恒为 false；崩溃恢复时靠扫描分段重建。 */
    public boolean sealed;
    /** 全部分段按序号拼接后的 SHA-256（封口时算）。 */
    public String sha256;

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
