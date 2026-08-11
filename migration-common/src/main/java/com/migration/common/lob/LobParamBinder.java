package com.migration.common.lob;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 把增量事件里的大字段引用绑成流式参数。
 *
 * <p>{@link LobRef} 走 {@code setBinaryStream}——配合连接上的
 * {@code useServerPrepStmts=true}，驱动会用 COM_STMT_SEND_LONG_DATA 把内容分片推给服务端，
 * 客户端只需要一个小缓冲。其它值仍走 {@code setObject}，行为不变。
 *
 * <p>用法上必须成对：绑完执行，执行完 {@link #close()} 关掉打开的文件流。
 */
public final class LobParamBinder implements Closeable {

    private final File spillDir;
    private final List<InputStream> opened = new ArrayList<>();

    public LobParamBinder(String spillDir) {
        this.spillDir = spillDir == null ? null : new File(spillDir);
    }

    /** 绑定一整组参数；返回是否绑定过大字段（调用方据此决定是否禁用批量）。 */
    public boolean bindAll(PreparedStatement ps, List<Object> params) throws SQLException {
        boolean hasLob = false;
        for (int i = 0; i < params.size(); i++) {
            Object v = params.get(i);
            if (v instanceof LobRef) {
                bindLob(ps, i + 1, (LobRef) v);
                hasLob = true;
            } else {
                ps.setObject(i + 1, v);
            }
        }
        return hasLob;
    }

    private void bindLob(PreparedStatement ps, int index, LobRef ref) throws SQLException {
        if (ref.isDiscarded()) {
            // 只留身份的引用只可能出现在"定位行"的位置。有主键时定位不需要它，
            // 走到这里说明是无主键表在用全列匹配定位——那需要把 1GB 放进 WHERE，不可行。
            // 这类表由预检拦截；真漏到这里必须明确报错，不能拿 md5 冒充内容写进目标列。
            throw new SQLException("SYNC_LOB_DISCARDED_IN_PARAM: 大字段的前镜像（只留身份，长度 "
                    + ref.length() + "，md5 " + ref.md5() + "）被用作 SQL 参数。"
                    + "含大字段的表必须有主键，否则增量无法定位行。");
        }
        if (spillDir == null) {
            throw new SQLException("SYNC_LOB_SPILL_DIR_MISSING: 未配置大字段落盘目录，无法读取 " + ref.file());
        }
        File f = new File(spillDir, ref.file());
        if (!f.isFile()) {
            // 典型场景：跨机接管。落盘文件是<b>本机</b>状态，中心位点回灌到另一台机器后文件并不在。
            // 这里必须 fail-stop 并说清楚——继续跑就意味着这一行的大字段被静默跳过。
            throw new SQLException("SYNC_LOB_FILE_MISSING: 大字段落盘文件不存在: " + f.getAbsolutePath()
                    + "（长度 " + ref.length() + "，md5 " + ref.md5() + "）。"
                    + "若是跨机接管，落盘文件留在原节点，需回到原节点续跑或从该位点之前重做增量；"
                    + "若是文件被清理，请检查大字段保留期与位点推进是否匹配。");
        }
        if (f.length() != ref.length()) {
            throw new SQLException("SYNC_LOB_FILE_TRUNCATED: 大字段落盘文件长度不符: " + f.getAbsolutePath()
                    + " 实际 " + f.length() + " 期望 " + ref.length());
        }
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(f), 64 * 1024);
            opened.add(in);
            ps.setBinaryStream(index, in, ref.length());
        } catch (IOException e) {
            throw new SQLException("读取大字段落盘文件失败: " + f.getAbsolutePath(), e);
        }
    }

    /** 参数里是否含大字段引用。 */
    public static boolean containsLob(List<Object> params) {
        if (params == null) {
            return false;
        }
        for (Object v : params) {
            if (v instanceof LobRef) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (InputStream in : opened) {
            try {
                in.close();
            } catch (IOException ignored) {
                // 关闭失败不该盖掉执行阶段的异常
            }
        }
        opened.clear();
    }
}
