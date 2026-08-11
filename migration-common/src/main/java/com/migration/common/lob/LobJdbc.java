package com.migration.common.lob;

/**
 * 大字段流式搬运对 JDBC 连接参数的硬性要求。
 *
 * <p>{@code useServerPrepStmts=true} 不是调优项而是<b>前提</b>：
 * 客户端预编译（Connector/J 默认）下，{@code setBinaryStream} 的内容会被驱动整个读进内存
 * 再拼成一个数据包，1GB 的值照样 OOM——这条有对照实测（同一份代码，
 * 关掉该参数即 {@code OutOfMemoryError}）。打开后驱动才会走 COM_STMT_SEND_LONG_DATA，
 * 按 {@code blobSendChunkSize} 分片推给服务端。
 */
public final class LobJdbc {

    /** 驱动向服务端分片推送大参数的片大小。 */
    public static final String BLOB_SEND_CHUNK_SIZE = "1048576";

    private LobJdbc() {
    }

    /** 给 MySQL URL 补上流式必需的参数（已存在则不覆盖调用方的显式取值）。 */
    public static String withStreamingParams(String url) {
        if (url == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(url);
        if (!containsParam(url, "useServerPrepStmts")) {
            append(sb, "useServerPrepStmts", "true");
        }
        if (!containsParam(url, "blobSendChunkSize")) {
            append(sb, "blobSendChunkSize", BLOB_SEND_CHUNK_SIZE);
        }
        return sb.toString();
    }

    /** 该连接串是否具备流式写入能力（预检与 fail-fast 用）。 */
    public static boolean supportsStreaming(String url) {
        return url != null && url.toLowerCase().contains("useserverprepstmts=true");
    }

    private static boolean containsParam(String url, String key) {
        return url.toLowerCase().contains(key.toLowerCase() + "=");
    }

    private static void append(StringBuilder sb, String key, String value) {
        sb.append(sb.indexOf("?") >= 0 ? '&' : '?').append(key).append('=').append(value);
    }
}
