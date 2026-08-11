package com.migration.common.lob;

import java.io.Closeable;
import java.io.IOException;

/**
 * 大字段内容的<b>按字节区间</b>读取源。
 *
 * <p>存在的理由：JDBC 驱动读一行时会把整行（含 1GB 的 LOB 列）读进堆，
 * {@code rs.getBytes} / {@code rs.getBinaryStream} 拿到的都已经是内存里的副本，
 * "流式读列"在 MySQL 协议上并不存在。要让客户端内存与字段大小脱钩，
 * 唯一的办法是<b>按区间多次查询</b>（{@code SUBSTRING(col, pos, len)}），
 * 每次只把一小块拉进来。
 *
 * <p>增量链路的 spill 文件也实现这个接口，于是"从源库拉"和"从本地落盘文件读"
 * 对写入侧是同一种东西。
 */
public interface LobChunkSource extends Closeable {

    /** 内容总字节数。 */
    long length();

    /**
     * 读取 {@code [offset, offset+len)}。
     *
     * @return 实际读到的字节数；已到末尾返回 -1。允许短读（返回值 &lt; len）。
     */
    byte[] fetch(long offset, int len) throws IOException;

    /** 供日志/错误信息定位用的描述，例如 {@code db.t.c_blob#id=7}。 */
    String describe();

    @Override
    default void close() throws IOException {
    }
}
