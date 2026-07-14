package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * 排障压缩包：把一个任务的日志尾部 + 脱敏配置 + checkpoint + THL 尾部打包成 zip，
 * 供支持人员下载排查，不需要登录服务器翻文件。
 *
 * <p>刻意只取"尾部"而非全量：日志/THL 文件在长跑任务下可达数百 MB～50MB/文件
 * （THL 单文件轮转上限即 50MB），全量打包会让排障包本身变得难以传输；
 * 尾部通常已包含最近的报错/进度信息，满足排障场景。
 */
public class DiagnosticsBundleService {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticsBundleService.class);

    /** 每个日志文件保留的尾部字节数 */
    private static final long LOG_TAIL_BYTES = 512 * 1024L;
    /** THL 文件保留的尾部字节数（只取最近修改的一个文件） */
    private static final long THL_TAIL_BYTES = 256 * 1024L;

    /**
     * 构建任务 taskId 的排障 zip，返回字节数组（调用方直接作为 HTTP 响应体写出）。
     *
     * @throws IOException 任务目录不存在，或 zip 写入失败
     */
    public byte[] buildBundle(String taskId) throws IOException {
        File taskDir = new File("files/" + taskId);
        if (!taskDir.exists() || !taskDir.isDirectory()) {
            throw new IOException("任务目录不存在: " + taskDir.getAbsolutePath());
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(buffer, StandardCharsets.UTF_8)) {
            addMaskedConfig(zip, new File(taskDir, "config.properties"));
            addDirWhole(zip, new File(taskDir, "checkpoint"), "checkpoint/");
            addLogTails(zip, new File(taskDir, "logs"));
            addLatestThlTail(zip, new File(taskDir, "thl_output"));
        }
        logger.info("排障压缩包已生成: taskId={}, 大小={} bytes", taskId, buffer.size());
        return buffer.toByteArray();
    }

    /** config.properties 脱敏后写入 zip：凡 key 含 "password" 一律替换为 ***MASKED***（不管是否已加密）。 */
    private void addMaskedConfig(ZipOutputStream zip, File configFile) throws IOException {
        if (!configFile.exists()) return;
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }
        for (String key : props.stringPropertyNames()) {
            if (key.toLowerCase().contains("password")) {
                props.setProperty(key, "***MASKED***");
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        props.store(out, "Masked for diagnostics bundle (all *password* values redacted)");

        zip.putNextEntry(new ZipEntry("config.properties"));
        zip.write(out.toByteArray());
        zip.closeEntry();
    }

    /** 整个目录原样打入 zip（checkpoint 目录体积小，无需裁剪）。 */
    private void addDirWhole(ZipOutputStream zip, File dir, String zipPrefix) throws IOException {
        if (!dir.exists() || !dir.isDirectory()) return;
        File[] files = dir.listFiles(File::isFile);
        if (files == null) return;
        for (File f : files) {
            zip.putNextEntry(new ZipEntry(zipPrefix + f.getName()));
            try (FileInputStream in = new FileInputStream(f)) {
                in.transferTo(zip);
            }
            zip.closeEntry();
        }
    }

    /** 每个日志文件只取尾部 LOG_TAIL_BYTES 字节，避免长跑任务的日志把排障包撑大。 */
    private void addLogTails(ZipOutputStream zip, File logsDir) throws IOException {
        if (!logsDir.exists() || !logsDir.isDirectory()) return;
        File[] files = logsDir.listFiles(File::isFile);
        if (files == null) return;
        for (File f : files) {
            byte[] tail = readTail(f, LOG_TAIL_BYTES);
            zip.putNextEntry(new ZipEntry("logs/" + f.getName() + ".tail"));
            zip.write(tail);
            zip.closeEntry();
        }
    }

    /** 只取最近修改的一个 THL 文件的尾部（THL 按 50MB 轮转，全量打包不现实）。 */
    private void addLatestThlTail(ZipOutputStream zip, File thlDir) throws IOException {
        if (!thlDir.exists() || !thlDir.isDirectory()) return;
        File[] files = thlDir.listFiles((dir, name) -> name.endsWith(".thl") && !name.startsWith("."));
        if (files == null || files.length == 0) return;

        File latest = Arrays.stream(files).max(Comparator.comparingLong(File::lastModified)).orElse(null);
        if (latest == null) return;

        byte[] tail = readTail(latest, THL_TAIL_BYTES);
        zip.putNextEntry(new ZipEntry("thl_tail/" + latest.getName() + ".tail"));
        zip.write(tail);
        zip.closeEntry();
    }

    /** 读取文件最后 maxBytes 字节；文件本身小于 maxBytes 时返回全部内容。 */
    private byte[] readTail(File file, long maxBytes) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long length = raf.length();
            long start = Math.max(0, length - maxBytes);
            long readLen = length - start;
            raf.seek(start);
            byte[] data = new byte[(int) readLen];
            raf.readFully(data);
            return data;
        }
    }
}
