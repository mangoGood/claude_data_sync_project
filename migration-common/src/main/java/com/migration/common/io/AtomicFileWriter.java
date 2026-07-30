package com.migration.common.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * 位点/进度类小文件的原子落盘：<b>写临时文件 → fsync → rename 覆盖</b>。
 *
 * <p>为什么必须原子写：这些文件是崩溃恢复的唯一依据，而
 * {@code new FileOutputStream(positionFile)} 是<b>先截断再写</b>——进程恰好崩在两者之间，
 * 重启读到的就是空文件或半行内容，位点直接作废，退化成整段重放（或更糟：解析失败被当成"无位点"）。
 * rename 在同一文件系统内是原子的，读者要么看到旧的完整版本、要么看到新的完整版本。
 *
 * <p>fsync 的作用范围：{@code fd.sync()} 只保证内容在 rename 前已落到磁盘，防的是<b>宿主断电</b>；
 * 单纯的进程被 SIGKILL 不会丢页缓存，rename 原子性就够了。两者一起做才覆盖两类故障。
 */
public final class AtomicFileWriter {

    private static final Logger logger = LoggerFactory.getLogger(AtomicFileWriter.class);

    private AtomicFileWriter() {
    }

    /** 原子写出一个 properties 文件。 */
    public static void writeProperties(File dst, Properties props, String comment) throws IOException {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        props.store(buf, comment);
        write(dst, buf.toByteArray());
    }

    /** 原子写出一段文本（UTF-8）。 */
    public static void writeString(File dst, String content) throws IOException {
        write(dst, content.getBytes(StandardCharsets.UTF_8));
    }

    /** 原子写出字节内容；失败抛 IOException，调用方自行决定告警还是中止。 */
    public static void write(File dst, byte[] content) throws IOException {
        File dir = dst.getParentFile();
        if (dir != null && !dir.exists()) {
            dir.mkdirs();
        }
        File tmp = new File(dir, "." + dst.getName() + ".tmp");
        try {
            try (FileOutputStream fos = new FileOutputStream(tmp)) {
                fos.write(content);
                fos.flush();
                fos.getFD().sync();
            }
            try {
                Files.move(tmp.toPath(), dst.toPath(),
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                // 少数文件系统（部分网络盘）不支持 ATOMIC_MOVE，退化成普通替换：
                // 仍远好于"截断后再写"，窗口从"整个写入过程"缩到 rename 本身。
                Files.move(tmp.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            syncDirectory(dir);
        } finally {
            if (tmp.exists()) {
                tmp.delete();
            }
        }
    }

    /**
     * 尽力 fsync 目录项，让 rename 本身也落盘。
     * 目录 fsync 在部分平台（Windows / 某些 JDK）不被支持，失败只记 debug，不影响正确性下限。
     */
    private static void syncDirectory(File dir) {
        if (dir == null) return;
        try (java.nio.channels.FileChannel ch =
                     java.nio.channels.FileChannel.open(dir.toPath(), java.nio.file.StandardOpenOption.READ)) {
            ch.force(true);
        } catch (Exception e) {
            logger.debug("目录 fsync 跳过（平台不支持或权限不足）: {} - {}", dir, e.getMessage());
        }
    }

    /**
     * 便捷方法：写 properties，失败只告警不抛出——位点保存不该把主流程带崩。
     * @return 是否写入成功
     */
    public static boolean writePropertiesQuietly(File dst, Properties props, String comment) {
        try {
            writeProperties(dst, props, comment);
            return true;
        } catch (Exception e) {
            logger.warn("原子写入 {} 失败: {}", dst, e.getMessage());
            return false;
        }
    }

    /** 便捷方法：写文本，失败只告警不抛出。 */
    public static boolean writeStringQuietly(File dst, String content) {
        try {
            writeString(dst, content);
            return true;
        } catch (Exception e) {
            logger.warn("原子写入 {} 失败: {}", dst, e.getMessage());
            return false;
        }
    }

    /** 供不便改造流式写法的调用方复用：拿到一个写到临时文件的输出流，close 后手动 commit。 */
    public static OutputStream openTemp(File dst) throws IOException {
        File dir = dst.getParentFile();
        if (dir != null && !dir.exists()) {
            dir.mkdirs();
        }
        return new FileOutputStream(new File(dir, "." + dst.getName() + ".tmp"));
    }
}
