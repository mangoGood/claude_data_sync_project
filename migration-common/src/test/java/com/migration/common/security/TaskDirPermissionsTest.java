package com.migration.common.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 任务目录权限收口判据。
 *
 * <p>任务目录里是整条链路的中间数据：{@code .cap}（源端原始事件）、{@code .thl}、
 * {@code sql_output}、checkpoint、带连接串的 {@code config.properties}。
 * 默认 umask 落成 {@code 0755}/{@code 0644}——同机任何用户都能读走全部业务数据。
 */
class TaskDirPermissionsTest {

    @TempDir
    Path tmp;

    private static String mode(Path p) throws Exception {
        return PosixFilePermissions.toString(Files.getPosixFilePermissions(p));
    }

    private static boolean posixSupported(Path p) {
        return p.getFileSystem().supportedFileAttributeViews().contains("posix");
    }

    @Test
    @DisplayName("目录收成 0700、文件收成 0600")
    void hardensTree() throws Exception {
        Path task = tmp.resolve("files/t1");
        Files.createDirectories(task.resolve("thl_output"));
        Files.createDirectories(task.resolve("binlog_output"));
        Path thl = Files.writeString(task.resolve("thl_output/a.thl"), "x");
        Path cap = Files.writeString(task.resolve("binlog_output/a.cap"), "x");
        Path cfg = Files.writeString(task.resolve("config.properties"), "k=v");

        assertTrue(posixSupported(task), "本测试需要 POSIX 文件系统");
        // 收紧前：世界可读（这正是问题本身）
        assertTrue(mode(task).endsWith("r-x") || mode(task).contains("r"),
                "前置条件：默认 umask 下目录是可遍历的");

        TaskDirPermissions.hardenTree(task.toFile());

        assertEquals("rwx------", mode(task), "任务目录必须 0700");
        assertEquals("rwx------", mode(task.resolve("thl_output")), "子目录必须 0700");
        assertEquals("rwx------", mode(task.resolve("binlog_output")));
        assertEquals("rw-------", mode(thl), ".thl 必须 0600");
        assertEquals("rw-------", mode(cap), ".cap 必须 0600（它从来没有被加密过）");
        assertEquals("rw-------", mode(cfg), "config.properties 必须 0600");
    }

    @Test
    @DisplayName("单独收目录 / 单独收文件")
    void hardensIndividually() throws Exception {
        Path d = Files.createDirectory(tmp.resolve("d"));
        Path f = Files.writeString(tmp.resolve("f"), "x");
        Files.setPosixFilePermissions(d, PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.setPosixFilePermissions(f, PosixFilePermissions.fromString("rw-r--r--"));

        TaskDirPermissions.hardenDir(d.toFile());
        TaskDirPermissions.hardenFile(f.toFile());

        assertEquals("rwx------", mode(d));
        assertEquals("rw-------", mode(f));
    }

    @Test
    @DisplayName("不存在的路径安静返回——收权限是加固，不能让任务起不来")
    void toleratesMissingPaths() {
        assertDoesNotThrow(() -> TaskDirPermissions.hardenDir(new File("/no/such/dir")));
        assertDoesNotThrow(() -> TaskDirPermissions.hardenFile(new File("/no/such/file")));
        assertDoesNotThrow(() -> TaskDirPermissions.hardenTree(new File("/no/such/tree")));
        assertDoesNotThrow(() -> TaskDirPermissions.hardenDir(null));
    }

    @Test
    @DisplayName("0700 目录意味着其它用户进不去——这是收目录而不是逐个收文件的理由")
    void dirModeBlocksTraversal() throws Exception {
        Path task = Files.createDirectories(tmp.resolve("files/t2"));
        Files.writeString(task.resolve("x.thl"), "secret");
        TaskDirPermissions.hardenDir(task.toFile());

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(task);
        assertFalse(perms.contains(PosixFilePermission.GROUP_EXECUTE), "组不应能进入目录");
        assertFalse(perms.contains(PosixFilePermission.OTHERS_EXECUTE), "其它用户不应能进入目录");
        assertFalse(perms.contains(PosixFilePermission.GROUP_READ));
        assertFalse(perms.contains(PosixFilePermission.OTHERS_READ));
    }
}
