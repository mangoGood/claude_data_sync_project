package com.migration.common.bulk;

import com.migration.common.route.RouteTarget;
import com.migration.common.route.TableRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 拆分（1:N）的装载通道：每行按分片键路由到对应分片的子通道。
 *
 * <p>对调用方而言它就是一条普通的 {@link JdbcBulkChannel}——{@code add/isFull/flush/close}
 * 语义不变，因此全量搬运的分页、断点、重连、进度那一整套循环<b>一行都不用改</b>，
 * 只是把"写哪张表"从固定值换成了按行计算。
 *
 * <p>未路由的行（分片键为 NULL、区间/枚举未覆盖）由 {@link TableRouter} 按规则的
 * unrouted 策略处置：广播则每个分片都写一份，投递死信则这里计数并告警，
 * ERROR 则 router 直接抛异常。<b>没有静默丢弃这条路</b>。
 */
public final class ShardedJdbcBulkChannel implements JdbcBulkChannel {

    private static final Logger logger = LoggerFactory.getLogger(ShardedJdbcBulkChannel.class);

    /** 按落点开子通道（目标表的 INSERT 语句、连接、装载档位都由调用方决定） */
    public interface ChannelFactory {
        JdbcBulkChannel open(RouteTarget target) throws SQLException;
    }

    private final TableRouter router;
    private final String sourceDb;
    private final String sourceTable;
    /** 分片键在行值数组里的下标；-1 表示行内取不到分片键（一律走未路由策略） */
    private final int shardKeyIndex;
    private final ChannelFactory factory;
    /** 落点 key → 子通道（懒开：没数据落到的分片不建连接/语句） */
    private final Map<String, JdbcBulkChannel> children = new LinkedHashMap<>();
    private final Map<String, RouteTarget> childTargets = new LinkedHashMap<>();
    private long unroutedRows;

    public ShardedJdbcBulkChannel(TableRouter router, String sourceDb, String sourceTable,
                                  int shardKeyIndex, ChannelFactory factory) {
        this.router = router;
        this.sourceDb = sourceDb;
        this.sourceTable = sourceTable;
        this.shardKeyIndex = shardKeyIndex;
        this.factory = factory;
    }

    @Override
    public void add(Object[] row) throws SQLException {
        Object shardValue = (shardKeyIndex >= 0 && row != null && shardKeyIndex < row.length)
                ? row[shardKeyIndex] : null;
        List<RouteTarget> targets = router.route(sourceDb, sourceTable, shardValue);
        if (targets.isEmpty()) {
            unroutedRows++;
            if (unroutedRows <= 10 || unroutedRows % 1000 == 0) {
                logger.warn("表 {}.{} 有行算不出分片（分片键值={}），按死信策略未写入，累计 {} 行",
                        sourceDb, sourceTable, shardValue, unroutedRows);
            }
            return;
        }
        for (RouteTarget target : targets) {
            childOf(target).add(row);
        }
    }

    private JdbcBulkChannel childOf(RouteTarget target) throws SQLException {
        String key = target.key();
        JdbcBulkChannel child = children.get(key);
        if (child == null) {
            child = factory.open(target);
            children.put(key, child);
            childTargets.put(key, target);
            logger.info("表 {}.{} 打开分片写通道: {}", sourceDb, sourceTable, target);
        }
        return child;
    }

    @Override
    public boolean isFull() {
        for (JdbcBulkChannel child : children.values()) {
            if (child.isFull()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        for (JdbcBulkChannel child : children.values()) {
            if (!child.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 提交所有分片的当前批。<b>任一分片失败也要把其余分片提交完</b>再抛——
     * 提前返回会让已经缓冲的行随通道一起消失，而计数照常推进（静默丢数据）。
     */
    @Override
    public long[] flush() throws SQLException {
        long ok = 0;
        long fail = 0;
        SQLException first = null;
        for (Map.Entry<String, JdbcBulkChannel> e : children.entrySet()) {
            try {
                long[] r = e.getValue().flush();
                ok += r[0];
                fail += r[1];
            } catch (SQLException ex) {
                logger.error("分片 {} 提交失败: {}", e.getKey(), ex.getMessage());
                if (first == null) {
                    first = ex;
                }
            }
        }
        if (first != null) {
            throw first;
        }
        return new long[]{ok, fail};
    }

    /**
     * 目标连接断开后重建：<b>只重绑默认实例上的子通道</b>。跨实例分片的子通道各自连着
     * 自己的实例，把它们一并绑到默认连接上会让那些分片的数据写错实例（且不报错）。
     */
    @Override
    public void rebind(Connection newConn) throws SQLException {
        for (Map.Entry<String, JdbcBulkChannel> e : children.entrySet()) {
            RouteTarget target = childTargets.get(e.getKey());
            if (target == null || target.getNodeId() == null) {
                e.getValue().rebind(newConn);
            }
        }
    }

    @Override
    public BulkLoadOptions.Mode mode() {
        for (JdbcBulkChannel child : children.values()) {
            return child.mode();
        }
        return BulkLoadOptions.Mode.BATCH;
    }

    @Override
    public BulkLoadStats stats() {
        BulkLoadStats aggregate = new BulkLoadStats();
        for (JdbcBulkChannel child : children.values()) {
            BulkLoadStats s = child.stats();
            aggregate.recordBatch(s.getRows(), s.getFailedRows(), s.getBytes());
        }
        return aggregate;
    }

    /** 算不出分片而未写入的行数（死信策略下的丢弃量，调用方要报出去）。 */
    public long getUnroutedRows() {
        return unroutedRows;
    }

    /** 实际写过数据的分片落点（用于日志/校验）。 */
    public List<RouteTarget> touchedTargets() {
        return new ArrayList<>(childTargets.values());
    }

    @Override
    public void close() {
        for (JdbcBulkChannel child : children.values()) {
            try {
                child.close();
            } catch (RuntimeException e) {
                logger.warn("关闭分片通道失败: {}", e.getMessage());
            }
        }
        children.clear();
    }
}
