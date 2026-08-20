import com.migration.thl.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/** 把仓库里所有真实 .thl 拿过滤器跑一遍，报告任何被拒的类。 */
public class ScanRealThl {
    public static void main(String[] args) throws Exception {
        List<Path> files = new ArrayList<>();
        try (var s = Files.walk(Paths.get("files"))) {
            s.filter(p -> p.toString().endsWith(".thl")).forEach(files::add);
        }
        System.out.println("发现 THL 文件: " + files.size());
        long ok = 0, events = 0;
        Map<String, Integer> rejects = new TreeMap<>();
        Set<String> valueTypes = new TreeSet<>();
        for (Path p : files) {
            try (THLFileReader r = new THLFileReader(p.toString())) {
                THLEvent e;
                while ((e = r.readEvent()) != null) {
                    events++;
                    collect(e.getMetadata(), valueTypes);
                }
                ok++;
            } catch (Exception ex) {
                String k = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                rejects.merge(k, 1, Integer::sum);
            }
        }
        System.out.println("成功读完: " + ok + "/" + files.size() + " 文件, 事件 " + events);
        System.out.println("\n--- metadata 里出现过的值类型 ---");
        valueTypes.forEach(t -> System.out.println("  " + t));
        System.out.println("\n--- 读取失败 ---");
        if (rejects.isEmpty()) System.out.println("  (无)");
        else rejects.forEach((k, v) -> System.out.println("  x" + v + "  " + k));
    }

    static void collect(Map<String, Object> md, Set<String> out) {
        if (md == null) return;
        for (Object v : md.values()) collectVal(v, out, 0);
    }
    static void collectVal(Object v, Set<String> out, int d) {
        if (v == null || d > 4) return;
        out.add(v.getClass().getName());
        if (v instanceof Collection<?> c) for (Object o : c) collectVal(o, out, d + 1);
        if (v instanceof Map<?, ?> m) for (Object o : m.values()) collectVal(o, out, d + 1);
    }
}
