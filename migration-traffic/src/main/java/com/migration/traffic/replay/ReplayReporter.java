package com.migration.traffic.replay;

import com.migration.traffic.TrafficMetrics;
import com.migration.traffic.model.TrafficJson;
import com.migration.traffic.model.TrafficRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 回放结果收口：计数、错误明细、慢语句。
 *
 * <p>错误明细写成 JSONL 落在 agent 侧（{@code files/<taskId>/traffic/replay_errors.jsonl}），
 * 由后端代理读取——与既有的 deadletter/conflicts 一致。<b>不入元数据库</b>：
 * 一次回放几百万条错误足以把元数据库冲垮。
 */
public final class ReplayReporter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ReplayReporter.class);

    public static final String ERRORS_FILE = "replay_errors.jsonl";
    /** 错误明细的落盘上限：错误太多时留前 N 条就够定位了，全落只会撑爆磁盘。 */
    private static final long MAX_ERROR_LINES = 100_000L;

    private final Map<ReplayOutcome, AtomicLong> counters = new EnumMap<>(ReplayOutcome.class);
    private final TrafficMetrics metrics;
    private final File errorsFile;
    private BufferedWriter errorWriter;
    private long errorLines;

    private final AtomicLong totalAttempted = new AtomicLong();
    private final AtomicLong slowCount = new AtomicLong();
    private volatile long slowestMicros;
    private volatile String slowestSql;

    public ReplayReporter(File dir, TrafficMetrics metrics) {
        this.metrics = metrics;
        this.errorsFile = new File(dir, ERRORS_FILE);
        for (ReplayOutcome o : ReplayOutcome.values()) {
            counters.put(o, new AtomicLong());
        }
        if (!dir.exists() && !dir.mkdirs()) {
            logger.warn("回放报告目录创建失败: {}", dir.getAbsolutePath());
        }
        // 每次回放重来一份：上一轮的错误留着会让人误以为这轮也错了
        if (errorsFile.exists() && !errorsFile.delete()) {
            logger.warn("旧的回放错误明细删除失败: {}", errorsFile.getAbsolutePath());
        }
    }

    public void record(ReplayOutcome outcome, TrafficRecord r, long elapsedMicros,
                       long plannedMicros, String detail) {
        counters.get(outcome).incrementAndGet();
        if (outcome == ReplayOutcome.OK || outcome == ReplayOutcome.EXPECTED_ERROR
                || outcome == ReplayOutcome.REPLAY_ERROR || outcome == ReplayOutcome.ROWCOUNT_MISMATCH) {
            totalAttempted.incrementAndGet();
        }
        if (elapsedMicros > slowestMicros) {
            slowestMicros = elapsedMicros;
            slowestSql = r == null ? null : r.q;
        }
        if (elapsedMicros >= 1_000_000L) {
            slowCount.incrementAndGet();
        }
        if (needsDetail(outcome)) {
            writeErrorLine(outcome, r, elapsedMicros, plannedMicros, detail);
        }
    }

    private static boolean needsDetail(ReplayOutcome o) {
        return o == ReplayOutcome.REPLAY_ERROR || o == ReplayOutcome.BLOCKED
                || o == ReplayOutcome.UNREPLAYABLE_REDACTED || o == ReplayOutcome.UNREPLAYABLE_NO_BINDS
                || o == ReplayOutcome.SESSION_EXHAUSTED
                || o == ReplayOutcome.ROWCOUNT_MISMATCH || o == ReplayOutcome.SKIPPED_LATE;
    }

    private synchronized void writeErrorLine(ReplayOutcome outcome, TrafficRecord r,
                                             long elapsedMicros, long plannedMicros, String detail) {
        if (errorLines >= MAX_ERROR_LINES) return;
        try {
            if (errorWriter == null) {
                errorWriter = new BufferedWriter(new FileWriter(errorsFile, true));
            }
            StringBuilder sb = new StringBuilder(256);
            sb.append("{\"outcome\":\"").append(outcome.name()).append('"');
            if (r != null) {
                sb.append(",\"n\":").append(r.n).append(",\"t\":").append(r.t)
                        .append(",\"s\":").append(r.s);
                if (r.k != null) sb.append(",\"k\":\"").append(r.k.name()).append('"');
                if (r.db != null) {
                    sb.append(",\"db\":");
                    TrafficJson.escapeJson(sb, r.db);
                }
                if (r.q != null) {
                    sb.append(",\"q\":");
                    TrafficJson.escapeJson(sb, r.q.length() > 2000 ? r.q.substring(0, 2000) : r.q);
                }
                if (r.hasEnrich) {
                    sb.append(",\"srcErrno\":").append(r.errno).append(",\"srcRows\":").append(r.rows);
                }
            }
            sb.append(",\"elapsedUs\":").append(elapsedMicros)
              .append(",\"plannedUs\":").append(plannedMicros);
            if (detail != null) {
                sb.append(",\"detail\":");
                TrafficJson.escapeJson(sb, detail.length() > 1000 ? detail.substring(0, 1000) : detail);
            }
            sb.append('}');
            errorWriter.write(sb.toString());
            errorWriter.newLine();
            errorLines++;
            if (errorLines % 100 == 0) {
                errorWriter.flush();
            }
        } catch (IOException e) {
            logger.debug("回放错误明细写出失败: {}", e.getMessage());
        }
    }

    public long count(ReplayOutcome o) {
        return counters.get(o).get();
    }

    /** 回放错误率：只把"尝试执行过"的语句算进分母，被筛掉/拦下的不算。 */
    public double errorRate() {
        long attempted = totalAttempted.get();
        if (attempted == 0) return 0;
        return (double) (count(ReplayOutcome.REPLAY_ERROR)) / attempted;
    }

    public long attempted() {
        return totalAttempted.get();
    }

    public void publishMetrics(long progressed, long total, ReplayScheduler scheduler) {
        metrics.set("traffic_replay_progress", progressed);
        metrics.set("traffic_replay_total", total);
        metrics.set("traffic_replay_errors", count(ReplayOutcome.REPLAY_ERROR));
        metrics.set("traffic_replay_blocked", count(ReplayOutcome.BLOCKED));
        metrics.set("traffic_replay_skipped", count(ReplayOutcome.SKIPPED_LATE));
        if (scheduler != null) {
            // 主指标取<b>执行</b>侧：派发侧偏差有会话队列兜着，永远好看，报它等于没报
            metrics.set("traffic_replay_skew_us", scheduler.execMaxLagMicros());
            metrics.set("traffic_replay_skew_p99_ms", scheduler.execPercentileLagMs(0.99));
            metrics.set("traffic_replay_lag_ms", scheduler.execLastLagMs());
            metrics.set("traffic_replay_dispatch_skew_us", scheduler.maxLagMicros());
        }
    }

    /** 汇总报告，落 {@code replay_report.properties} 供 agent/后端读。 */
    public void writeSummary(File dir, ReplayScheduler scheduler, long total, String outcome, String reason) {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("outcome", outcome);
        p.setProperty("reason", reason == null ? "" : reason);
        p.setProperty("total", String.valueOf(total));
        for (ReplayOutcome o : ReplayOutcome.values()) {
            p.setProperty("count." + o.name(), String.valueOf(count(o)));
        }
        p.setProperty("errorRate", String.format(java.util.Locale.ROOT, "%.6f", errorRate()));
        if (scheduler != null) {
            // skew.* = 执行侧（语句真正开始跑的时刻 vs 计划时刻），这才是"回放像不像"
            p.setProperty("skew.maxUs", String.valueOf(scheduler.execMaxLagMicros()));
            p.setProperty("skew.avgUs", String.valueOf(scheduler.execAvgLagMicros()));
            p.setProperty("skew.p50Ms", String.valueOf(scheduler.execPercentileLagMs(0.50)));
            p.setProperty("skew.p95Ms", String.valueOf(scheduler.execPercentileLagMs(0.95)));
            p.setProperty("skew.p99Ms", String.valueOf(scheduler.execPercentileLagMs(0.99)));
            p.setProperty("skew.samples", String.valueOf(scheduler.execSamples()));
            // dispatchSkew.* = 派发侧，排障用（两者差得远 = 目标库跟不上，而不是调度不准）
            p.setProperty("dispatchSkew.maxUs", String.valueOf(scheduler.maxLagMicros()));
            p.setProperty("dispatchSkew.p99Ms", String.valueOf(scheduler.percentileLagMs(0.99)));
            p.setProperty("elapsedMs", String.valueOf(scheduler.elapsedMs()));
        }
        p.setProperty("slowestUs", String.valueOf(slowestMicros));
        p.setProperty("slowestSql", slowestSql == null ? ""
                : (slowestSql.length() > 500 ? slowestSql.substring(0, 500) : slowestSql));
        p.setProperty("slowCount", String.valueOf(slowCount.get()));
        try {
            com.migration.common.io.AtomicFileWriter.writeProperties(
                    new File(dir, "replay_report.properties"), p, "流量回放报告");
        } catch (IOException e) {
            logger.warn("回放报告写出失败: {}", e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        if (errorWriter != null) {
            try {
                errorWriter.flush();
                errorWriter.close();
            } catch (IOException e) {
                logger.debug("关闭回放错误明细失败: {}", e.getMessage());
            }
            errorWriter = null;
        }
    }
}
