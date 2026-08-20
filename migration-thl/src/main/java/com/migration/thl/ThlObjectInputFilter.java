package com.migration.thl;

import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.Set;

/**
 * THL 反序列化白名单。
 *
 * <p><b>为什么必须有</b>：THL 走的是 Java 原生序列化，而 {@code readObject()} 会按流里
 * 写着的类名去实例化——攻击者只要能让一个 {@code .thl} 文件被读到，就能借类路径上的
 * gadget 链（Spring / Jackson / Gson / Connector-J 都在）在读取进程里执行代码。
 * THL 不是内部临时文件：它是 capture → extract → apply/subscribe 的主通道，
 * 跨机接管时还会被回灌，落文件的路径不止一条。
 *
 * <p><b>白名单而不是黑名单</b>：黑名单要枚举所有已知 gadget，新依赖随时会引入新的；
 * 白名单只需要枚举 {@link THLEvent} 真正用到的那几个类型，而这个集合是封闭且稳定的。
 *
 * <p>三道限额（深度 / 引用数 / 字节数）挡的是另一类攻击：白名单内的类也能构造出
 * 深度嵌套或超大数组来打爆内存（"序列化炸弹"）。限额取值按 LOB 链路的实际上限留了余量——
 * 大字段走的是旁路流式 + 落盘引用，不会整个塞进 THLEvent 的 {@code data} 里。
 */
public final class ThlObjectInputFilter {

    /**
     * 允许出现在 THL 流里的类。
     *
     * <p>对应 {@link THLEvent} 的字段：{@code String}（sourceId/comment/eventId/shardId）、
     * {@code Timestamp}（sourceTstamp/localEnqueueTstamp，其父类 Date 也要放）、
     * {@code HashMap}（metadata）、{@code byte[]}（data），加上 metadata 里可能出现的装箱类型。
     */
    private static final Set<String> ALLOWED = Set.of(
            "com.migration.thl.THLEvent",
            // 大字段链路把落盘引用放进 metadata（字段只有 String/long/String，无递归风险）。
            // 这一条是拿仓库里 74 个真实 THL 实扫出来的——只读代码发现不了：
            // LobRef 是 migration-common 的类型，THL 模块里没有任何静态引用。
            "com.migration.common.lob.LobRef",
            // Object[] 是 ArrayList 的底层数组；剥到元素类型就是 java.lang.Object。
            // 放它是安全的：过滤器对流里**每一个**被解析的类都会回调一次，数组里真装了
            // gadget，那个 gadget 自己的类会单独触发一次判定并被拒。不放它的后果是
            // 74 个真实 THL 里有 72 个读不动（实测），因为几乎每条事件都带 ArrayList。
            "java.lang.Object",
            "java.lang.String",
            "java.lang.Number",
            "java.lang.Boolean",
            "java.lang.Byte",
            "java.lang.Short",
            "java.lang.Integer",
            "java.lang.Long",
            "java.lang.Float",
            "java.lang.Double",
            "java.lang.Character",
            "java.lang.Enum",
            "java.util.HashMap",
            "java.util.LinkedHashMap",
            "java.util.Map",
            // HashMap.readObject 内部会对 Map.Entry[] 走一次 checkArray——不放它，
            // 读任何带 metadata 的正常 THL 事件都会被自己的过滤器拒掉。
            // （判据 readsNormalEvent 就是为了钉住这个坑。）
            "java.util.Map$Entry",
            "java.util.HashMap$Node",
            "java.util.ArrayList",
            "java.util.List",
            "java.util.Arrays$ArrayList",
            "java.util.Collections$UnmodifiableMap",
            "java.util.Collections$EmptyMap",
            "java.sql.Timestamp",
            "java.util.Date",
            "java.math.BigDecimal",
            "java.math.BigInteger",
            "java.sql.Date",
            "java.sql.Time"
    );

    private static final long MAX_DEPTH = 32;
    private static final long MAX_REFS = 10_000;
    /** 单条事件上限 256 MB：LOB 走旁路流式，不会整条塞进 data。 */
    private static final long MAX_BYTES = 256L * 1024 * 1024;
    private static final long MAX_ARRAY = 64L * 1024 * 1024;

    private static final ObjectInputFilter FILTER = ThlObjectInputFilter::check;

    private ThlObjectInputFilter() {
    }

    /**
     * 给一个 {@link ObjectInputStream} 装上白名单。所有读 THL 的地方都必须调它。
     *
     * @return 传入的同一个流，便于链式写法
     */
    public static ObjectInputStream install(ObjectInputStream ois) {
        ois.setObjectInputFilter(FILTER);
        return ois;
    }

    /** 供判据/单测直接拿到过滤器本身。 */
    public static ObjectInputFilter filter() {
        return FILTER;
    }

    private static ObjectInputFilter.Status check(ObjectInputFilter.FilterInfo info) {
        if (info.depth() > MAX_DEPTH
                || info.references() > MAX_REFS
                || info.streamBytes() > MAX_BYTES
                || (info.arrayLength() >= 0 && info.arrayLength() > MAX_ARRAY)) {
            return ObjectInputFilter.Status.REJECTED;
        }

        Class<?> clazz = info.serialClass();
        if (clazz == null) {
            // 不是类解析回调（数组长度/引用计数检查），限额已经过了就放行
            return ObjectInputFilter.Status.UNDECIDED;
        }

        // 数组：剥到元素类型再判，基本类型数组直接放行
        Class<?> c = clazz;
        while (c.isArray()) {
            c = c.getComponentType();
        }
        if (c.isPrimitive()) {
            return ObjectInputFilter.Status.ALLOWED;
        }

        return ALLOWED.contains(c.getName())
                ? ObjectInputFilter.Status.ALLOWED
                : ObjectInputFilter.Status.REJECTED;
    }
}
