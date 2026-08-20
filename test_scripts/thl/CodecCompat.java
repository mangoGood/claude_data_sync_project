import com.migration.thl.*;
import java.io.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;
import com.migration.common.lob.LobRef;

/**
 * 判据：ThlCodec 对真实数据必须逐字段无损，且确实把体积降下来。
 *
 * 拿仓库里全部历史 THL（THL1，Java 原生序列化）的每一条事件做：
 *   原事件 → ThlCodec.encode → decode → 与原事件逐字段比对
 * 同时统计两种编码的字节数，给出真实的体积对比。
 */
public class CodecCompat {
    static int events = 0, mismatches = 0;
    static long oldBytes = 0, newBytes = 0;
    static List<String> problems = new ArrayList<>();

    public static void main(String[] a) throws Exception {
        List<Path> files = new ArrayList<>();
        try (var s = Files.walk(Paths.get("files"))) {
            s.filter(p -> p.toString().endsWith(".thl")).sorted().forEach(files::add);
        }
        for (Path p : files) {
            try (THLFileReader r = new THLFileReader(p.toString())) {
                THLEvent e;
                while ((e = r.readEvent()) != null) check(e);
            } catch (Exception ex) {
                problems.add("读取失败 " + p + ": " + ex);
            }
        }
        System.out.println("=== ThlCodec 兼容性与体积判据 ===");
        System.out.printf("文件 %d，事件 %d，字段比对不一致 %d%n", files.size(), events, mismatches);
        System.out.println();
        System.out.printf("原生序列化 payload 合计 : %,d B  (%.2f MB)%n", oldBytes, oldBytes/1048576.0);
        System.out.printf("ThlCodec   payload 合计 : %,d B  (%.2f MB)%n", newBytes, newBytes/1048576.0);
        System.out.printf("体积降幅                : %.1f%%   (单条均值 %d B → %d B)%n",
                100.0*(oldBytes-newBytes)/oldBytes, oldBytes/Math.max(1,events), newBytes/Math.max(1,events));
        System.out.println();
        if (!problems.isEmpty()) {
            System.out.println("--- 问题 ---");
            problems.stream().limit(20).forEach(x -> System.out.println("  " + x));
        }
        if (mismatches == 0 && problems.isEmpty()) {
            System.out.println("✓ 判据通过：逐字段无损");
            System.exit(0);
        }
        System.out.println("✗ 判据失败");
        System.exit(1);
    }

    static void check(THLEvent e) throws Exception {
        events++;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) { oos.writeObject(e); }
        oldBytes += baos.size();

        byte[] enc = ThlCodec.encode(e);
        newBytes += enc.length;
        THLEvent d = ThlCodec.decode(enc);

        eq(e.getSeqno(), d.getSeqno(), "seqno");
        eq(e.getFragno(), d.getFragno(), "fragno");
        eq(e.isLastFrag(), d.isLastFrag(), "lastFrag");
        eq(e.getType(), d.getType(), "type");
        eq(e.getEpochNumber(), d.getEpochNumber(), "epochNumber");
        eq(e.getSourceId(), d.getSourceId(), "sourceId");
        eq(e.getComment(), d.getComment(), "comment");
        eq(e.getEventId(), d.getEventId(), "eventId");
        eq(e.getShardId(), d.getShardId(), "shardId");
        ts(e.getSourceTstamp(), d.getSourceTstamp(), "sourceTstamp");
        ts(e.getLocalEnqueueTstamp(), d.getLocalEnqueueTstamp(), "localEnqueueTstamp");
        if (!Arrays.equals(e.getData(), d.getData())) fail("data");
        deepEq(e.getMetadata(), d.getMetadata(), "metadata");
    }

    static void ts(Timestamp x, Timestamp y, String f) {
        if (x == null && y == null) return;
        if (x == null || y == null) { fail(f + " (null 不一致)"); return; }
        // 毫秒 + nanos 都要一致：延迟指标就是拿 sourceTstamp 算的
        if (x.getTime() != y.getTime() || x.getNanos() != y.getNanos()) fail(f);
    }

    @SuppressWarnings("unchecked")
    static void deepEq(Object x, Object y, String f) {
        if (x == null && y == null) return;
        if (x == null || y == null) { fail(f + " (null 不一致)"); return; }
        if (x instanceof byte[] bx) { if (!Arrays.equals(bx, (byte[]) y)) fail(f); return; }
        // LobRef 没有实现 equals/hashCode，Object 的同一性比较必然不等——按结构比。
        // （这不是编解码的问题；判据自己用 equals 才是错的。）
        if (x instanceof com.migration.common.lob.LobRef lx) {
            com.migration.common.lob.LobRef ly = (com.migration.common.lob.LobRef) y;
            if (!Objects.equals(lx.file(), ly.file())
                    || lx.length() != ly.length()
                    || !Objects.equals(lx.md5(), ly.md5())) {
                fail(f + " (LobRef " + lx + " vs " + ly + ")");
            }
            return;
        }
        if (x instanceof Map<?,?> mx) {
            Map<Object,Object> my = (Map<Object,Object>) y;
            if (mx.size() != my.size()) { fail(f + " (size " + mx.size() + " vs " + my.size() + ")"); return; }
            for (Map.Entry<?,?> en : mx.entrySet()) {
                if (!my.containsKey(en.getKey())) { fail(f + "." + en.getKey() + " 缺失"); return; }
                deepEq(en.getValue(), my.get(en.getKey()), f + "." + en.getKey());
            }
            return;
        }
        if (x instanceof List<?> lx) {
            List<Object> ly = (List<Object>) y;
            if (lx.size() != ly.size()) { fail(f + " (list size)"); return; }
            for (int i = 0; i < lx.size(); i++) deepEq(lx.get(i), ly.get(i), f + "[" + i + "]");
            return;
        }
        if (!x.equals(y)) fail(f + " (" + x.getClass().getSimpleName() + ": " + x + " vs " + y + ")");
    }

    static void eq(Object x, Object y, String f) { if (!Objects.equals(x, y)) fail(f); }
    static void fail(String f) {
        mismatches++;
        if (problems.size() < 20) problems.add("字段不一致: " + f);
    }
}
