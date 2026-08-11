package com.migration.common.lob;

/**
 * 大字段落盘文件的命名约定：{@code <binlog文件>#<位点>#c<事件内序号>.lob}。
 *
 * <p>用位点而不是自增计数器命名，换来两个白拿的性质：
 * <ul>
 *   <li><b>重放幂等</b>：capture 断线后从旧位点重连会重放同一个事件，
 *       生成的文件名完全相同，覆盖写即可，不会堆出第二份 1GB；</li>
 *   <li><b>回收是范围扫描</b>：位点推进到 P 之后，位点小于 P 的文件一律可删，
 *       不需要维护引用计数（见 {@link LobSpillJanitor}）。</li>
 * </ul>
 *
 * <p>写入侧在 migration-capture（{@code LobSpillWriter}），这里放的是两侧共用的
 * 常量与解析——增量侧要按名字回收，必须用同一套规则。
 */
public final class LobSpillNaming {

    public static final String LOB_SUFFIX = ".lob";
    public static final String TMP_SUFFIX = ".lobtmp";

    public static final class Parsed {
        public final String binlogFile;
        public final long position;
        public final int cellIndex;

        Parsed(String binlogFile, long position, int cellIndex) {
            this.binlogFile = binlogFile;
            this.position = position;
            this.cellIndex = cellIndex;
        }
    }

    private LobSpillNaming() {
    }

    public static String fileName(String binlogFile, long position, int cellIndex) {
        String safe = binlogFile == null ? "unknown" : binlogFile.replace('/', '_');
        return safe + "#" + position + "#c" + cellIndex + LOB_SUFFIX;
    }

    /** 解析文件名；不符合约定返回 null（调用方应当保守地不去动这类文件）。 */
    public static Parsed parse(String fileName) {
        if (fileName == null || !fileName.endsWith(LOB_SUFFIX)) {
            return null;
        }
        String body = fileName.substring(0, fileName.length() - LOB_SUFFIX.length());
        int second = body.lastIndexOf('#');
        if (second <= 0) {
            return null;
        }
        int first = body.lastIndexOf('#', second - 1);
        if (first <= 0) {
            return null;
        }
        String cellPart = body.substring(second + 1);
        if (!cellPart.startsWith("c")) {
            return null;
        }
        try {
            long position = Long.parseLong(body.substring(first + 1, second));
            int cell = Integer.parseInt(cellPart.substring(1));
            return new Parsed(body.substring(0, first), position, cell);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
