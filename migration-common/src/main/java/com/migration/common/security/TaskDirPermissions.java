package com.migration.common.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

/**
 * 任务目录的权限收口。
 *
 * <p><b>问题</b>：任务目录里放着整条链路的中间数据——{@code .cap}（capture 原始事件）、
 * {@code .thl}（抽取后的事件）、{@code sql_output}、checkpoint、以及带加密连接串的
 * {@code config.properties}。这些文件默认按 umask 落成 {@code 0644}、目录 {@code 0755}，
 * 也就是**同机任何用户都能读走全部业务数据**。
 *
 * <p><b>为什么收目录而不是逐个收文件</b>：全仓有 54 处创建文件/目录的地方，
 * 逐处加权限必然漏，且新增一处就少一处。而 POSIX 的语义是——
 * 目录没有 {@code x} 权限就无法进入，里面的文件是 {@code 0644} 还是 {@code 0600}
 * 都够不着。所以把 {@code files/<taskId>/} 及其子目录收成 {@code 0700}，
 * 一处覆盖全部现有与将来的文件。
 *
 * <p>文件本身也顺手收成 {@code 0600}：目录权限挡的是同机其它用户，
 * 而备份、打包、rsync 这些会把文件模式带走，收紧一层没有坏处。
 *
 * <p>非 POSIX 文件系统（Windows）静默跳过：那边的访问控制是 ACL，
 * 强行设置只会抛 {@code UnsupportedOperationException}。
 */
public final class TaskDirPermissions {

    private static final Logger logger = LoggerFactory.getLogger(TaskDirPermissions.class);

    private static final Set<PosixFilePermission> DIR_0700 = Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);

    private static final Set<PosixFilePermission> FILE_0600 = Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE);

    /** 只告警一次，别让不支持 POSIX 的平台刷屏。 */
    private static volatile boolean warnedUnsupported = false;

    private TaskDirPermissions() {
    }

    /** 目录收成 0700。目录不存在或非 POSIX 文件系统时安静返回。 */
    public static void hardenDir(File dir) {
        apply(dir, DIR_0700);
    }

    /** 文件收成 0600。 */
    public static void hardenFile(File file) {
        apply(file, FILE_0600);
    }

    /**
     * 递归收紧一个任务目录：目录 0700、文件 0600。
     *
     * <p>在任务目录创建完之后调一次即可；后续新建的文件由所在目录的
     * 0700 兜住（同机其它用户进不来），不需要每次落文件都来一趟。
     */
    public static void hardenTree(File root) {
        if (root == null || !root.exists()) {
            return;
        }
        apply(root, root.isDirectory() ? DIR_0700 : FILE_0600);
        File[] children = root.listFiles();
        if (children == null) {
            return;
        }
        for (File c : children) {
            hardenTree(c);
        }
    }

    private static void apply(File f, Set<PosixFilePermission> perms) {
        if (f == null || !f.exists()) {
            return;
        }
        try {
            Path p = f.toPath();
            if (!p.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                if (!warnedUnsupported) {
                    warnedUnsupported = true;
                    logger.warn("当前文件系统不支持 POSIX 权限，任务目录权限收口已跳过（Windows 请用 ACL 限制 files/ 目录）");
                }
                return;
            }
            Files.setPosixFilePermissions(p, perms);
        } catch (IOException | UnsupportedOperationException | SecurityException e) {
            // 收权限失败不能让任务起不来：它是加固，不是功能前提
            logger.warn("收紧权限失败（忽略）: {} - {}", f.getAbsolutePath(), e.toString());
        }
    }
}
