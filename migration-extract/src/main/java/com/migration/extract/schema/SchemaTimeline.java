package com.migration.extract.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按 binlog 位点索引的表结构版本序列——"这条事件发生<b>当时</b>，这张表长什么样"。
 *
 * <p>每张表一条按位点排序的版本链，查询是 {@code floorEntry}：取最后一个
 * {@code effectiveFrom <= 事件位点} 的版本。链的头是 capture 打的基线，之后每条 DDL
 * 由 {@link DdlApplier} 推出一个新版本。
 *
 * <p><b>位点可比性</b>：用 {@code 文件号 << 32 | 位置} 折成一个标量（与 {@code task_checkpoints}
 * 的 {@code monotonic_key} 同一个思路）。capture 记的是连接器当时的位点（约等于该事件的结束
 * 位点），时序库只依赖它<b>单调递增</b>，不依赖它是事件的起点还是终点——DDL 版本的位点必然
 * 小于其后每一个行事件的位点，{@code floorEntry} 就总能取到正确的那一版。
 * {@code RESET MASTER} 会让文件号回退，那是现有位点体系共有的局限（{@code shouldSkipEvent}
 * 的字符串比较同样如此），本类不额外处理。
 *
 * <p>线程安全：extract 是单线程消费 {@code .cap} 的，但上传线程会并发读
 * {@link #entriesSince}，所以外层用 {@link ConcurrentHashMap}，每张表的版本链只在消费线程写。
 */
public class SchemaTimeline {

    private static final Logger logger = LoggerFactory.getLogger(SchemaTimeline.class);

    /** 一个版本。{@code schema} 为 null 表示这张表在该位点之后不存在（DROP / RENAME 走掉）。 */
    public static class Entry {
        private final long monotonicKey;
        private final String binlogFile;
        private final long binlogPos;
        private final TableSchema schema;
        private final SchemaChange.Kind kind;
        private final String database;
        private final String table;

        Entry(long monotonicKey, String binlogFile, long binlogPos,
              String database, String table, SchemaChange.Kind kind, TableSchema schema) {
            this.monotonicKey = monotonicKey;
            this.binlogFile = binlogFile;
            this.binlogPos = binlogPos;
            this.database = database;
            this.table = table;
            this.kind = kind;
            this.schema = schema;
        }

        public long getMonotonicKey() { return monotonicKey; }
        public String getBinlogFile() { return binlogFile; }
        public long getBinlogPos() { return binlogPos; }
        public String getDatabase() { return database; }
        public String getTable() { return table; }
        public SchemaChange.Kind getKind() { return kind; }
        public TableSchema getSchema() { return schema; }

        public String key() {
            return TableSchema.key(database, table);
        }

        @Override
        public String toString() {
            return key() + "@" + binlogFile + ":" + binlogPos + " " + kind;
        }
    }

    private final Map<String, NavigableMap<Long, Entry>> byTable = new ConcurrentHashMap<>();

    /**
     * 把 {@code (文件, 位置)} 折成可比标量。
     *
     * <p>文件名取末尾数字段（{@code mysql-bin.000123} → 123）；取不到时按 0 处理并告警——
     * 那种情况下同一文件内仍然可比，跨文件会乱序，属于"不该发生但发生了"，要看得见。
     */
    public static long monotonicKey(String binlogFile, long pos) {
        long fileNo = fileNumber(binlogFile);
        return (fileNo << 32) | (pos & 0xFFFFFFFFL);
    }

    private static long fileNumber(String binlogFile) {
        if (binlogFile == null || binlogFile.isEmpty()) {
            return 0;
        }
        int end = binlogFile.length();
        int start = end;
        while (start > 0 && Character.isDigit(binlogFile.charAt(start - 1))) {
            start--;
        }
        if (start == end) {
            logger.warn("binlog 文件名 {} 没有数字后缀，跨文件的版本顺序会不准", binlogFile);
            return 0;
        }
        try {
            return Long.parseLong(binlogFile.substring(start, end));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * 取该位点生效的表结构。
     *
     * @return 该位点上这张表的结构；位点早于全部版本、或该位点上表已被删掉时返回 null
     */
    public TableSchema at(String database, String table, String binlogFile, long pos) {
        NavigableMap<Long, Entry> versions = byTable.get(TableSchema.key(database, table));
        if (versions == null) {
            return null;
        }
        Map.Entry<Long, Entry> floor = versions.floorEntry(monotonicKey(binlogFile, pos));
        if (floor == null) {
            return null;   // 事件早于这张表的第一个版本：基线之前的事件，调用方按缺失处置
        }
        return floor.getValue().getSchema();   // DROPPED 的 schema 是 null，正是"那时它不存在"
    }

    /**
     * 追加一个版本。
     *
     * @return false 表示同位点已有版本（重放同一段 binlog 会重新算出同样的版本），按幂等忽略
     */
    public boolean append(SchemaChange change, String binlogFile, long pos) {
        long key = monotonicKey(binlogFile, pos);
        String tableKey = change.key();
        NavigableMap<Long, Entry> versions =
                byTable.computeIfAbsent(tableKey, k -> new TreeMap<>());

        synchronized (versions) {
            if (versions.containsKey(key)) {
                return false;
            }
            versions.put(key, new Entry(key, binlogFile, pos, change.getDatabase(),
                    change.getTable(), change.getKind(), change.getSchema()));
        }
        return true;
    }

    /** 供从历史文件/中心库恢复时直接装载，不做幂等判断之外的加工。 */
    public boolean append(Entry entry) {
        NavigableMap<Long, Entry> versions =
                byTable.computeIfAbsent(entry.key(), k -> new TreeMap<>());
        synchronized (versions) {
            if (versions.containsKey(entry.getMonotonicKey())) {
                return false;
            }
            versions.put(entry.getMonotonicKey(), entry);
        }
        return true;
    }

    /** 这张表有没有任何版本——决定 capture 基线要不要 seed（已有则基线只是兜底种子，忽略）。 */
    public boolean hasTable(String database, String table) {
        NavigableMap<Long, Entry> versions = byTable.get(TableSchema.key(database, table));
        return versions != null && !versions.isEmpty();
    }

    /** 该表当前（最新）版本，供 {@link DdlApplier.SchemaLookup} 用。 */
    public TableSchema currentOf(String database, String table) {
        NavigableMap<Long, Entry> versions = byTable.get(TableSchema.key(database, table));
        if (versions == null || versions.isEmpty()) {
            return null;
        }
        return versions.lastEntry().getValue().getSchema();
    }

    public DdlApplier.SchemaLookup asLookup() {
        return this::currentOf;
    }

    /**
     * 全库最新版本的位点。用于"清还是留"的判断：
     * 新起始位点 ≤ 它 → 时序库覆盖得到，保留；> 它 → 中间有它没看过的空档，必须清空重打基线。
     */
    public long newestKey() {
        long newest = 0;
        for (NavigableMap<Long, Entry> versions : byTable.values()) {
            if (!versions.isEmpty()) {
                newest = Math.max(newest, versions.lastKey());
            }
        }
        return newest;
    }

    /** 增量上传用：位点严格大于 {@code afterKey} 的版本，按位点升序。 */
    public List<Entry> entriesSince(long afterKey) {
        List<Entry> out = new ArrayList<>();
        for (NavigableMap<Long, Entry> versions : byTable.values()) {
            synchronized (versions) {
                out.addAll(versions.tailMap(afterKey, false).values());
            }
        }
        out.sort((a, b) -> Long.compare(a.getMonotonicKey(), b.getMonotonicKey()));
        return out;
    }

    public Collection<Entry> allEntries() {
        return entriesSince(Long.MIN_VALUE);
    }

    /**
     * 裁剪：每张表保留"≤ 最小已提交位点的<b>最后一个</b>版本"及其之后的全部。
     *
     * <p>那个"最后一个"不能丢——它是回答最小已提交位点处查询的唯一依据，丢了之后
     * 从该位点重放的第一个事件就查不到版本了。
     *
     * @return 裁掉的版本数
     */
    public int prune(long minCommittedKey) {
        int removed = 0;
        for (NavigableMap<Long, Entry> versions : byTable.values()) {
            synchronized (versions) {
                Long keep = versions.floorKey(minCommittedKey);
                if (keep == null) {
                    continue;
                }
                NavigableMap<Long, Entry> stale = versions.headMap(keep, false);
                removed += stale.size();
                stale.clear();
            }
        }
        return removed;
    }

    public void clear() {
        byTable.clear();
    }

    public int tableCount() {
        return byTable.size();
    }

    public int versionCount() {
        int n = 0;
        for (NavigableMap<Long, Entry> versions : byTable.values()) {
            n += versions.size();
        }
        return n;
    }

    /** 排障用：每张表的版本数。 */
    public Map<String, Integer> versionsPerTable() {
        Map<String, Integer> m = new LinkedHashMap<>();
        byTable.forEach((k, v) -> m.put(k, v.size()));
        return m;
    }

    /** 从历史记录构造一个 Entry（供 {@link SchemaHistoryFile} 与中心库回灌复用）。 */
    public static Entry entryOf(String binlogFile, long pos, String database, String table,
                                SchemaChange.Kind kind, TableSchema schema) {
        return new Entry(monotonicKey(binlogFile, pos), binlogFile, pos, database, table, kind, schema);
    }
}
