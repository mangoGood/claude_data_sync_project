import com.migration.common.lob.JdbcLobChunkSource;
import com.migration.common.lob.LobChunkSource;
import com.migration.common.lob.LobWriteOptions;
import com.migration.common.lob.MySqlLobDialect;
import com.migration.common.lob.StreamingBlobWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 步骤 2 的真库探针：在受限堆下把一行的大字段从源库流式搬到目标库。
 *
 * <p>验的是整个方案里最关键、也最不能靠推断的一个假设：
 * {@code setBinaryStream} + {@code useServerPrepStmts=true} 时，Connector/J 是否真的
 * 走 COM_STMT_SEND_LONG_DATA 分片推送——如果它把流读进内存再组包，那 1GB 照样 OOM，
 * 后面几步全都白搭。所以这一步单独探，不跟别的逻辑混在一起。
 *
 * <p>参数：srcUrl srcDb tgtUrl tgtDb table pkCol pkVal blobCol [useServerPrepStmts]
 * 由 lob_writer_probe.py 编译并调用，不进生产 jar。
 */
public class LobCopyProbe {

    public static void main(String[] args) throws Exception {
        String srcUrl = args[0];
        String srcDb = args[1];
        String tgtUrl = args[2];
        String tgtDb = args[3];
        String table = args[4];
        String pkCol = args[5];
        String pkVal = args[6];
        String blobCol = args[7];
        boolean serverPrep = args.length <= 8 || Boolean.parseBoolean(args[8]);

        String srcFull = srcUrl + (srcUrl.contains("?") ? "&" : "?") + "useServerPrepStmts=false";
        String tgtFull = tgtUrl + (tgtUrl.contains("?") ? "&" : "?")
                + "useServerPrepStmts=" + serverPrep + "&blobSendChunkSize=1048576";

        System.out.println("[probe] serverPrepStmts=" + serverPrep);
        System.out.println("[probe] maxMemory=" + (Runtime.getRuntime().maxMemory() >> 20) + "MB");

        try (Connection src = DriverManager.getConnection(srcFull, "root", "rootpassword");
             Connection tgt = DriverManager.getConnection(tgtFull, "root", "rootpassword")) {

            String srcTable = "`" + srcDb + "`.`" + table + "`";
            String tgtTable = "`" + tgtDb + "`.`" + table + "`";
            MySqlLobDialect dialect = MySqlLobDialect.INSTANCE;

            long len = JdbcLobChunkSource.byteLength(src, dialect, srcTable, blobCol, pkCol, pkVal);
            long packet = StreamingBlobWriter.probePacketLimit(tgt, dialect);
            System.out.println("[probe] 源值长度=" + len + " 目标 max_allowed_packet=" + packet
                    + " 选路=" + StreamingBlobWriter.choosePath(len, packet, LobWriteOptions.DEFAULT_SAFETY_MARGIN));

            // 目标端瘦行：大字段先置 NULL，内容随后流式补齐
            try (PreparedStatement ps = tgt.prepareStatement(
                    "INSERT INTO " + tgtTable + " (" + pkCol + ", " + blobCol + ") VALUES (?, NULL) "
                            + "ON DUPLICATE KEY UPDATE " + blobCol + " = " + blobCol)) {
                ps.setObject(1, pkVal);
                ps.executeUpdate();
            }

            long t0 = System.currentTimeMillis();
            try (LobChunkSource source = new JdbcLobChunkSource(src, dialect, srcTable, blobCol,
                    pkCol, pkVal, false, len);
                 StreamingBlobWriter writer = new StreamingBlobWriter(tgt, dialect,
                         LobWriteOptions.defaults(), tgtTable, blobCol, pkCol, pkVal, packet)) {
                StreamingBlobWriter.Result r = writer.onBlock((written, total) ->
                        System.out.println("[probe] 块进度 " + written + "/" + total)).write(source);
                System.out.println("[probe] 写入结果 " + r + " 耗时="
                        + (System.currentTimeMillis() - t0) / 1000 + "s");
            }

            System.out.println("[probe] srcMd5=" + md5(src, srcTable, blobCol, pkCol, pkVal));
            System.out.println("[probe] tgtMd5=" + md5(tgt, tgtTable, blobCol, pkCol, pkVal));
            System.out.println("[probe] peakHeapUsed="
                    + (usedHeap() >> 20) + "MB");
        }
        System.out.println("[probe] OK");
    }

    private static String md5(Connection conn, String table, String col, String pkCol, String pkVal)
            throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT MD5(`" + col + "`), OCTET_LENGTH(`" + col + "`) FROM " + table
                        + " WHERE `" + pkCol + "` = ?")) {
            ps.setObject(1, pkVal);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) + "/" + rs.getLong(2) : "(no row)";
            }
        }
    }

    private static long usedHeap() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
