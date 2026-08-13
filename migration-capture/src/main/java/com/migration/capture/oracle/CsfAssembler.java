package com.migration.capture.oracle;

/**
 * LogMiner 续行（CSF, Continuation SQL Flag）拼接。
 *
 * <p>{@code V$LOGMNR_CONTENTS.SQL_REDO} 单行最多 4000 字节，超了 Oracle 会把**一条语句**
 * 拆成多行返回，除最后一行外每行的 {@code CSF=1}。宽表、长 {@code VARCHAR2}、
 * 带 LOB 的行都很容易越过 4000。
 *
 * <p>不拼接的后果是静默的：截断处之后的列在解析时根本不存在，于是按残缺列集写目标端
 * （缺的列悄悄不写）；而续行本身会被当成独立事件，正则匹配不到列值，落到"原样透传
 * SQL_REDO"的兜底分支上，下游拿到半截 SQL。两头都不报错。
 *
 * <p>用法：每读到一行就 {@link #accept}，返回非 null 表示"一条完整语句到手了"，
 * 返回 null 表示"还没拼完，继续读下一行"。
 */
public final class CsfAssembler {

    /** 单条语句拼接的字节上限，防止异常数据把内存撑爆。 */
    private final int maxLength;

    private StringBuilder pending;
    private int pendingRows;

    public CsfAssembler() {
        this(16 * 1024 * 1024);
    }

    public CsfAssembler(int maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * 收下一行 SQL_REDO。
     *
     * @param sqlRedo 本行的 SQL_REDO（可能为 null）
     * @param csf     本行的 CSF：1 表示"语句未结束，下一行是它的续行"
     * @return 拼完整的语句；还没拼完返回 null
     */
    public String accept(String sqlRedo, int csf) {
        String piece = sqlRedo == null ? "" : sqlRedo;
        if (csf == 1) {
            if (pending == null) {
                pending = new StringBuilder(piece.length() * 2);
                pendingRows = 0;
            }
            if (pending.length() + piece.length() <= maxLength) {
                pending.append(piece);
            }
            pendingRows++;
            return null;
        }
        if (pending == null) {
            return piece;                       // 常规情况：一行就是一条完整语句
        }
        pending.append(piece);
        String complete = pending.toString();
        pending = null;
        pendingRows = 0;
        return complete;
    }

    /** 是否还有没拼完的语句（读完一批之后应当为 false）。 */
    public boolean hasPending() {
        return pending != null;
    }

    /** 已累计的续行数，仅用于日志/告警。 */
    public int pendingRows() {
        return pendingRows;
    }

    /**
     * 丢弃未拼完的残句。
     *
     * <p>只在"本批查询结束时还挂着半条语句"这种反常情况下调用：那说明 CSF=1 的最后一行
     * 落在了查询边界之外，下一批会从同一个 SCN 重新读到它，残句留着反而会与新的一批拼串。
     */
    public void reset() {
        pending = null;
        pendingRows = 0;
    }
}
