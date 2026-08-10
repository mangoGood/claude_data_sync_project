package com.migration.full.migration;

import com.migration.common.ratelimit.RowRateLimiter;
import com.migration.config.MigrationConfig;

/**
 * 全量装载限速器的持有者（进程内单例）。
 *
 * <p><b>为什么必须共享一个实例</b>：全量是多表并行 + 单表分片并行的，每个 worker 各建一个
 * 限速器等于把配额乘以并发数——配了 1000 行/秒、开 4 个 worker，实际跑 4000 行/秒，
 * 限速形同虚设。配额说的是"这个任务总共多快"，那就只能有一个计数窗口。
 */
public final class FullRateLimiter {

    private static volatile RowRateLimiter instance;
    private static volatile long configuredRate = Long.MIN_VALUE;

    private FullRateLimiter() {
    }

    /** 按配置取（同一进程内同一配额只建一次）。 */
    public static synchronized RowRateLimiter get(MigrationConfig config) {
        long rate = config == null ? 0 : config.getFullRateLimitRowsPerSec();
        if (instance == null || configuredRate != rate) {
            instance = new RowRateLimiter(rate);
            configuredRate = rate;
        }
        return instance;
    }
}
