package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.migration.common.lob.LobRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 把 binlog 事件里的大字段<b>边读边落盘</b>，事件里只留一个 {@link LobRef}。
 *
 * <p>堆占用恒定为一个 64KB 缓冲，与字段大小无关——1GB 的 BLOB 和 8KB 的 BLOB
 * 在这里花的内存一样多。
 *
 * <p>写入走"临时名 + 原子改名"：下游看到最终文件名时，内容一定是完整的。
 * 最终名由 capture 在序列化事件时按<b>位点</b>改（见 MySQLBinlogCapture），
 * 而不是在这里按计数器定——位点是确定性的，capture 重连重放同一个事件会生成同名文件，
 * 覆盖写即幂等，不会堆出一堆重复的 1GB 垃圾；按位点排序也让"位点推进后批量清理"变成一次范围扫描。
 */
public final class LobSpillWriter {

    private static final Logger logger = LoggerFactory.getLogger(LobSpillWriter.class);

    /** 落盘缓冲。单线程（binlog 监听线程）复用一个即可。 */
    private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[64 * 1024]);

    /** 临时文件后缀：崩溃残留的孤儿文件按它识别清理。 */
    public static final String TMP_SUFFIX = com.migration.common.lob.LobSpillNaming.TMP_SUFFIX;
    /** 最终文件后缀。 */
    public static final String LOB_SUFFIX = com.migration.common.lob.LobSpillNaming.LOB_SUFFIX;

    private final File dir;
    private final AtomicLong sequence = new AtomicLong();

    public LobSpillWriter(String dirPath) {
        this.dir = new File(dirPath);
        if (!dir.exists() && !dir.mkdirs()) {
            logger.warn("创建大字段落盘目录失败: {}", dir.getAbsolutePath());
        }
    }

    public File dir() {
        return dir;
    }

    /**
     * 从流里读走 {@code length} 个字节。
     *
     * @param discard true 时只算 md5 不落盘（前镜像 / DELETE 镜像：定位行只要主键）
     */
    public LobRef consume(ByteArrayInputStream in, int length, boolean discard) throws IOException {
        MessageDigest md5 = newDigest();
        byte[] buf = SCRATCH.get();

        Path tmp = null;
        OutputStream out = null;
        try {
            if (!discard) {
                tmp = new File(dir, "spill-" + sequence.incrementAndGet() + "-"
                        + System.nanoTime() + TMP_SUFFIX).toPath();
                out = new BufferedOutputStream(Files.newOutputStream(tmp));
            }
            int left = length;
            while (left > 0) {
                int want = Math.min(buf.length, left);
                int n = in.read(buf, 0, want);
                if (n < 0) {
                    throw new EOFException("大字段落盘时流提前结束，还差 " + left + "/" + length + " 字节");
                }
                if (out != null) {
                    out.write(buf, 0, n);
                }
                md5.update(buf, 0, n);
                left -= n;
            }
            if (out != null) {
                out.close();
                out = null;
            }
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ignored) {
                    // 关闭失败不该盖掉真正的读失败
                }
            }
        }

        String digest = hex(md5.digest());
        if (discard) {
            return LobRef.discarded(length, digest);
        }
        return LobRef.spilled(tmp.getFileName().toString(), length, digest);
    }

    /**
     * 把临时文件改成按位点命名的最终文件，返回换名后的引用。
     *
     * <p>同名覆盖是<b>刻意</b>的：capture 从旧位点重连时会重放同一个事件，
     * 生成的最终名完全相同，覆盖即幂等。
     */
    public LobRef finalizeName(LobRef ref, String binlogFile, long position, int cellIndex) {
        if (ref == null || ref.isDiscarded() || ref.file() == null || !ref.file().endsWith(TMP_SUFFIX)) {
            return ref;
        }
        // 命名规则放在 migration-common：增量侧要按同一套规则解析出位点来回收，
        // 两边各写一份迟早会漂
        String finalName = com.migration.common.lob.LobSpillNaming.fileName(binlogFile, position, cellIndex);
        Path from = new File(dir, ref.file()).toPath();
        Path to = new File(dir, finalName).toPath();
        try {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return ref.withFile(finalName);
        } catch (IOException e) {
            logger.error("大字段落盘文件改名失败 {} → {}: {}", from, to, e.getMessage());
            return ref;   // 保留临时名，下游仍能读到内容
        }
    }

    /** 清理超过给定时长的孤儿临时文件（崩在落盘与改名之间会留下）。 */
    public int cleanupOrphanTemp(long olderThanMs) {
        File[] files = dir.listFiles((d, name) -> name.endsWith(TMP_SUFFIX));
        if (files == null) {
            return 0;
        }
        long deadline = System.currentTimeMillis() - olderThanMs;
        int removed = 0;
        for (File f : files) {
            if (f.lastModified() < deadline && f.delete()) {
                removed++;
            }
        }
        return removed;
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM 不支持 MD5", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
