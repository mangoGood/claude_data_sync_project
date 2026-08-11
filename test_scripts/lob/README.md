# 大字段（LONGBLOB/LONGTEXT）同步测试套件

针对"单值 1GB、10 行共 10GB，各进程运行时内存上限 256MB"这个场景的端到端判据。

## 拓扑

`dr-mysql-a(33320)` 作源、`dr-mysql-b(33321)` 作目标——两个独立实例，比同实例跨库更贴近真实，
也不占用 `synctask-mysql`。

## 脚本

| 脚本 | 验的是什么 |
|---|---|
| `lob_baseline_oom.py` | **基线**：改造前在受限堆下必然 OOM。后续每一步的对照 |
| `lob_writer_probe.py` | 写入器真库探针：`setBinaryStream` + `useServerPrepStmts` 是否真的分片推送（含关掉该参数的 OOM 对照） |
| `lob_full_e2e.py` | 全量：瘦扫描 + 旁路流式、幂等、块级续传、零回归 |
| `lob_incr_e2e.py` | 增量：capture→extract→increment 三进程，INSERT/UPDATE/DELETE + 落盘回收 |
| `lob_capture_e2e.py` | capture 单链路：落盘保真、前镜像丢弃、`.cap` 引用格式（含关落盘的 OOM 对照） |
| `lob_acceptance.py` | 总判据：前置体检 + 上面三个 E2E 串起来跑 |
| `loblib.py` | 公共脚手架：造数、服务端 MD5 比对、峰值 RSS 采样 |

```bash
# 冒烟（几分钟）
python3 test_scripts/lob/lob_acceptance.py

# 题设满规模（小时级，先确认磁盘 ≥ 60GB）
python3 test_scripts/lob/lob_acceptance.py --rows 10 --size 1G
```

## 几个必须知道的前提（都是实测踩出来的）

**1. `max_allowed_packet` 是单值的硬上限，分块追加也绕不过。**
它同时限制 `CONCAT()` 的**结果**大小，不只是语句/参数大小。所以"把块切小一点"完全无效，
写入侧能承载的最大单值就是 `max_allowed_packet`，而它的 MySQL 上限是 1GB——
**超过 1GB 的单值无法通过 SQL 协议写入**。1GB 的场景必须把两端都调到 1G。

**2. 增量的单值上限比全量低——1GB 的行根本进不了复制流。**
单个 binlog 事件不能超过 `replica_max_allowed_packet`（最大 1GB）。实测：1GB 的 LONGBLOB
加上其余 9 列，行事件是 **1073742110** 字节，比 1GB 上限多 286 字节，源端 dump 线程直接断开，
capture 侧表现为"binlog 读取线程无声无息地没了"。512MB 则一切正常。

这是 MySQL 复制协议本身的限制，与同步工具无关。**全量不受影响**（走普通 SQL，1GB 已验证可搬）。
更糟的是 `binlog_row_image=FULL` 下 UPDATE 的前后镜像在同一个事件里，等于上限再砍一半——
所以带大字段的表要做增量，要么把 `binlog_row_image` 改成 `NOBLOB`，要么把单值控制在
1GB 减去行开销以内。预检的"大字段搬运能力"一项会按这条规则拦截。

**3. `useServerPrepStmts=true` 是前提而不是调优。**
客户端预编译下，驱动会把 `setBinaryStream` 的内容整个读进内存再组包，1GB 照样 OOM。
`lob_writer_probe.py` 里有对照组：同一份代码，关掉该参数就 `OutOfMemoryError`。

**4. 落盘阈值要按堆大小定，不是越大越好。**
低于阈值的值仍走"0x 十六进制串"的老路径，而那条路径在 extract 里的峰值占用约是值本身的
20~30 倍。4MB 阈值配 `-Xmx144m` 时，一条 UPDATE（前后镜像各 4MB）就把 extract 顶到 273MB 并 OOM。
默认降到 1MB。内存预算更小就再调小，代价只是更多小文件。

**5. 造数是"服务端 CONCAT 自倍增"，不是客户端灌。**
1GB 的值没法用 INSERT 拼出来。倍增过程会在 binlog 里留下一串越来越大的 UPDATE，
所以测试断言要按"最终值"挑文件，不能见到第一个落盘文件就开始比对。

**6. 校验一律用服务端 `MD5(col)`。**
只回传 32 字节，测试脚本自己不会因为读校验数据而 OOM。

## 相关配置

```properties
migration.lob.stream.enabled=true            # 全量：大字段旁路流式（默认开）
migration.lob.source.chunk.bytes=33554432    # 源端一次 SUBSTRING 拉多少（真进堆）
migration.lob.append.block.bytes=268435456   # 分块追加每块多大（流式发送，不进堆）
migration.lob.packet.safety.margin.bytes=1048576
migration.lob.spill.enabled=true             # 增量：大字段落盘（默认开）
migration.lob.spill.threshold.bytes=1048576  # 超过它才落盘，低于它走原路径
migration.lob.spill.dir=files/<taskId>/lob

proc.jvm.opts.full=-Xmx144m -XX:MaxMetaspaceSize=64m -XX:MaxDirectMemorySize=24m
proc.jvm.opts.capture=-Xmx144m -XX:MaxMetaspaceSize=64m -XX:MaxDirectMemorySize=24m
proc.jvm.opts.extract=-Xmx144m -XX:MaxMetaspaceSize=64m -XX:MaxDirectMemorySize=24m
proc.jvm.opts.increment=-Xmx144m -XX:MaxMetaspaceSize=64m -XX:MaxDirectMemorySize=24m
```
