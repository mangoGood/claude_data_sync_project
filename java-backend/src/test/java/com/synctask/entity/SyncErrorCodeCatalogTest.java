package com.synctask.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 错误码目录的一致性门禁。
 *
 * <p>这个系统里错误码有<b>三份</b>互相独立的写法，谁也不引用谁：引擎子进程/agent 里直接写
 * 字面量上报（{@code writeErrorStatus("E3013", ...)}），后端 {@link SyncErrorCode} 枚举是
 * 人读的目录，页面 {@code admin-dashboard.js} 的 {@code ERROR_CODE_MAP} 才是失败详情区真正
 * 查表的地方。后端只把裸码透传给前端，枚举压根不在运行链路上——所以漏一项<b>不会有任何
 * 编译或运行期报错</b>，只会让用户在页面上看到「未知错误 (E3013)」加一句通用的看日志建议。
 *
 * <p>历史上就是这么漂的：E3013（汇聚/拆分守卫）、E3101/E3102（Elastic）三个码发出来了却两份
 * 目录都没有，E2007/E2008 反过来只有页面有。这个测试把三份的漂移变成 CI 红灯：新增错误码时
 * 只写发出点就会挂，必须同步补枚举和 ERROR_CODE_MAP。
 */
@DisplayName("错误码目录：发出点 / 枚举 / 前端 ERROR_CODE_MAP 三者一致")
class SyncErrorCodeCatalogTest {

    /** 引擎侧上报错误码一律写成字符串字面量，扫这个形态即可覆盖 writeErrorStatus/sendFailedStatus/mapper 返回值 */
    private static final Pattern CODE_LITERAL = Pattern.compile("\"(E\\d{4})\"");

    /** ERROR_CODE_MAP 的一条：'E3013': { desc: '...', solution: '...' }, */
    private static final Pattern JS_ENTRY = Pattern.compile(
            "'(E\\d{4})'\\s*:\\s*\\{\\s*desc\\s*:\\s*'([^']*)'\\s*,\\s*solution\\s*:\\s*'([^']*)'\\s*}");

    /** 行首形态先认出「这是一条错误码条目」，再要求它能被 JS_ENTRY 解析——写坏的条目要报错而不是被静默跳过 */
    private static final Pattern JS_ENTRY_HEAD = Pattern.compile("^\\s*'(E\\d{4})'\\s*:");

    @Test
    @DisplayName("引擎侧发出的每个错误码都在枚举里")
    void everyEmittedCodeIsInEnum() {
        Map<String, String> emitted = scanEmittedCodes();
        assertFalse(emitted.isEmpty(), "没扫到任何错误码发出点，扫描路径或字面量形态变了，这个测试已经失去意义");

        Set<String> known = enumCodes();
        List<String> missing = emitted.entrySet().stream()
                .filter(e -> !known.contains(e.getKey()))
                .map(e -> e.getKey() + "（发出点 " + e.getValue() + "）")
                .collect(Collectors.toList());

        assertTrue(missing.isEmpty(), () -> "以下错误码被上报了但 SyncErrorCode 枚举里没有，"
                + "页面只会显示裸码而没有中文名和处置建议:\n  " + String.join("\n  ", missing));
    }

    @Test
    @DisplayName("枚举与前端 ERROR_CODE_MAP 码集一致、名称一致")
    void enumMatchesDashboardMap() {
        Map<String, String[]> js = parseDashboardMap();
        Set<String> jsCodes = new TreeSet<>(js.keySet());
        Set<String> enumCodes = new TreeSet<>(enumCodes());

        Set<String> onlyInEnum = new TreeSet<>(enumCodes);
        onlyInEnum.removeAll(jsCodes);
        assertTrue(onlyInEnum.isEmpty(), () -> "这些码枚举里有、admin-dashboard.js 的 ERROR_CODE_MAP 里没有，"
                + "页面会显示「未知错误 (码)」: " + onlyInEnum);

        Set<String> onlyInJs = new TreeSet<>(jsCodes);
        onlyInJs.removeAll(enumCodes);
        assertTrue(onlyInJs.isEmpty(), () -> "这些码页面有、SyncErrorCode 枚举里没有: " + onlyInJs);

        // 名称必须逐字一致：同一个码在页面和后端目录里叫两个名字，排障时对不上号。
        // 处置建议不比对——页面那份是刻意压缩过的短版（如 E3008/E3009/E3011），只要求非空。
        for (String code : enumCodes) {
            SyncErrorCode entry = SyncErrorCode.fromCode(code);
            assertEquals(js.get(code)[0], entry.getDescription(), "错误码 " + code + " 的名称在枚举与页面之间不一致");
            assertFalse(entry.getSolution().trim().isEmpty(), "错误码 " + code + " 的枚举处置建议为空");
            assertFalse(js.get(code)[1].trim().isEmpty(), "错误码 " + code + " 的页面处置建议为空");
        }
    }

    @Test
    @DisplayName("枚举内码值唯一且可反查")
    void enumCodesAreUniqueAndResolvable() {
        Set<String> seen = new LinkedHashSet<>();
        for (SyncErrorCode value : SyncErrorCode.values()) {
            assertTrue(seen.add(value.getCode()),
                    "错误码 " + value.getCode() + " 在枚举里重复定义（" + value.name() + "），fromCode 只会命中先声明的那个");
        }
        for (SyncErrorCode value : SyncErrorCode.values()) {
            assertEquals(value, SyncErrorCode.fromCode(value.getCode()));
        }
        // 未知码兜底成 UNKNOWN_ERROR 而不是 null，页面才有话可说
        assertEquals(SyncErrorCode.UNKNOWN_ERROR, SyncErrorCode.fromCode("E7777"));
    }

    private static Set<String> enumCodes() {
        Set<String> codes = new LinkedHashSet<>();
        for (SyncErrorCode value : SyncErrorCode.values()) {
            codes.add(value.getCode());
        }
        return codes;
    }

    /** 扫引擎/agent 模块的 main 源码，返回 码 → 首个发出点（path:line） */
    private static Map<String, String> scanEmittedCodes() {
        Path root = repoRoot();
        Map<String, String> emitted = new LinkedHashMap<>();
        List<Path> moduleRoots;
        try (Stream<Path> children = Files.list(root)) {
            moduleRoots = children
                    .filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().endsWith("-migration-common")
                            || p.getFileName().toString().endsWith("-migration-thl")
                            || p.getFileName().toString().startsWith("migration-"))
                    .map(p -> p.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        for (Path moduleRoot : moduleRoots) {
            try (Stream<Path> files = Files.walk(moduleRoot)) {
                List<Path> javaFiles = files
                        .filter(p -> p.getFileName().toString().endsWith(".java"))
                        .sorted()
                        .collect(Collectors.toList());
                for (Path file : javaFiles) {
                    List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                    for (int i = 0; i < lines.size(); i++) {
                        Matcher m = CODE_LITERAL.matcher(lines.get(i));
                        while (m.find()) {
                            emitted.putIfAbsent(m.group(1), root.relativize(file) + ":" + (i + 1));
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return emitted;
    }

    /** 解析 admin-dashboard.js 的 ERROR_CODE_MAP，返回 码 → [desc, solution] */
    private static Map<String, String[]> parseDashboardMap() {
        Path dashboard = repoRoot().resolve("admin-dashboard.js");
        List<String> lines;
        try {
            lines = Files.readAllLines(dashboard, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        int start = -1;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains("const ERROR_CODE_MAP")) {
                start = i;
                break;
            }
        }
        if (start < 0) {
            fail("admin-dashboard.js 里找不到 ERROR_CODE_MAP —— 前端错误码目录被挪走或改名了，"
                    + "请同步改这个测试，别让门禁变成摆设");
        }

        Map<String, String[]> map = new LinkedHashMap<>();
        List<String> unparsable = new ArrayList<>();
        boolean closed = false;
        for (int i = start + 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.trim().startsWith("};")) {
                closed = true;
                break;
            }
            if (!JS_ENTRY_HEAD.matcher(line).find()) {
                continue;
            }
            Matcher m = JS_ENTRY.matcher(line);
            if (m.find()) {
                map.put(m.group(1), new String[]{m.group(2), m.group(3)});
            } else {
                unparsable.add((i + 1) + ": " + line.trim());
            }
        }
        assertTrue(closed, "ERROR_CODE_MAP 没找到结束的 '};'，解析范围不可信");
        assertTrue(unparsable.isEmpty(), () -> "ERROR_CODE_MAP 里这些条目写法不符合 "
                + "'Exxxx': { desc: '...', solution: '...' }，解析不出来:\n  " + String.join("\n  ", unparsable));
        assertFalse(map.isEmpty(), "ERROR_CODE_MAP 解析结果为空");
        return map;
    }

    /**
     * 从测试工作目录（java-backend/）向上找仓库根。找不到就直接失败——静默 skip 等于门禁不存在，
     * 这个测试宁可挂在"跑不了"上，也不要假装通过。
     */
    private static Path repoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            if (Files.isRegularFile(dir.resolve("admin-dashboard.js"))
                    && Files.isDirectory(dir.resolve("java-backend"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        return fail("从 " + Paths.get("").toAbsolutePath()
                + " 向上找不到仓库根（含 admin-dashboard.js 与 java-backend/ 的目录），无法比对错误码目录");
    }
}
