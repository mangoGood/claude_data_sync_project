package com.migration.common.lob;

import java.io.Serializable;

/**
 * 大字段的<b>引用</b>：内容在磁盘上，事件里只带路径、长度和校验和。
 *
 * <p>增量链路上，一个 1GB 的字段如果按值传递，会在
 * capture 的 byte[] → 十六进制串 → {@code .cap} 行 → THL 载荷 → SQL 字面量
 * 这一路上被复制五六次。更要命的是十六进制串这一步有个<b>无论堆多大都过不去</b>的硬墙：
 * 1GB 二进制的 hex 是 2,147,483,648 个字符，超过 Java 字符串/数组的长度上限。
 * 所以大字段必须改成传引用，内容在链路外走磁盘。
 *
 * <p>{@link #isDiscarded()} 的引用表示"这里原本有个大值，但我们没留"——
 * UPDATE 的前镜像和 DELETE 的行镜像都属于这种：定位行只需要主键，
 * 留着那 1GB 纯属浪费磁盘。md5 仍然算，好让冲突检测还能比对身份。
 */
public final class LobRef implements Serializable {

    private static final long serialVersionUID = 1L;

    /** {@code .cap} 行里的引用前缀。 */
    public static final String MARKER = "@lob:";

    private final String file;
    private final long length;
    private final String md5;

    private LobRef(String file, long length, String md5) {
        this.file = file;
        this.length = length;
        this.md5 = md5;
    }

    public static LobRef spilled(String file, long length, String md5) {
        return new LobRef(file, length, md5);
    }

    /** 只留身份、不留内容（前镜像 / DELETE 镜像）。 */
    public static LobRef discarded(long length, String md5) {
        return new LobRef(null, length, md5);
    }

    /** 落盘文件名（相对 spill 目录）；{@link #isDiscarded()} 时为 null。 */
    public String file() {
        return file;
    }

    public long length() {
        return length;
    }

    public String md5() {
        return md5;
    }

    public boolean isDiscarded() {
        return file == null;
    }

    /** 换一个文件名（capture 把临时名改成按位点命名的最终名时用）。 */
    public LobRef withFile(String newFile) {
        return new LobRef(newFile, length, md5);
    }

    /** {@code .cap} 行里的文本形式：{@code @lob:<文件名>:<长度>:<md5>}。 */
    public String toMarker() {
        return MARKER + (file == null ? "-" : file) + ":" + length + ":" + md5;
    }

    /** 解析 {@link #toMarker()}；不是引用格式则返回 null。 */
    public static LobRef parse(String text) {
        if (text == null || !text.startsWith(MARKER)) {
            return null;
        }
        String body = text.substring(MARKER.length());
        int lastColon = body.lastIndexOf(':');
        if (lastColon <= 0) {
            return null;
        }
        int prevColon = body.lastIndexOf(':', lastColon - 1);
        if (prevColon <= 0) {
            return null;
        }
        String file = body.substring(0, prevColon);
        try {
            long length = Long.parseLong(body.substring(prevColon + 1, lastColon));
            String md5 = body.substring(lastColon + 1);
            return new LobRef("-".equals(file) ? null : file, length, md5);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return toMarker();
    }
}
