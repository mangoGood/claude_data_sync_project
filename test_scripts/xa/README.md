# 源库 XA 事务同步判据

验的是一句话：**源库 XA 提交的那一刻，目标库才提交**。

## 源库 XA 在 binlog 里长什么样（实测 MySQL 8.0.44）

```
Query      XA START X'..',X'..',1     ← 行事件在 prepare 时刻就落 binlog 了
Table_map / Write_rows / ...
Query      XA END   X'..',X'..',1
XA_prepare XA PREPARE X'..',X'..',1
 ……（中间穿插任意多个其它事务，实测隔了 7KB）……
Query      XA COMMIT X'..',X'..',1    ← 提交决议在这里，也可能是 XA ROLLBACK
```

三个要点：**没有 BEGIN、没有 Xid**；行事件写在 prepare 时刻，提交决议在任意晚的位置；
一阶段提交（`XA COMMIT … ONE PHASE`）复用 `XA_prepare` 事件，`onePhase=true`，没有独立的
`XA COMMIT` Query 事件。

## 拓扑

`dr-mysql-a(33320)` 作源、`dr-mysql-b(33321)` 作目标——两个独立实例，不占用
`synctask-mysql`（上面常有别的任务在跑）。

## 脚本

| 脚本 | 验的是什么 |
|---|---|
| `xa_e2e.py` | **主判据**：capture→extract→increment 三进程真链路，12 项 |
| `xa_baseline.py` | **对照组**：`sync.xa.enabled=false` 时复现改造前的数据面故障 |
| `xalib.py` | 公共脚手架：XA 会话、三进程编排、水位标记 |

```bash
mvn -pl migration-capture,migration-extract,migration-increment -am package -DskipTests
python3 test_scripts/xa/xa_e2e.py        # 期望 12/12 PASS
python3 test_scripts/xa/xa_baseline.py   # 期望 3/3 复现
```

## 主判据的 12 项

1. 链路打通（普通事务能同步）
2. prepare 后普通事务不受阻——未决分支不阻塞其它事务
3. **prepare 阶段目标库不落数据**——源库没提交，目标库必须查不到
4. **XA COMMIT 后整段到达**
5. **回滚分支不留幻影行**
6. 一阶段提交（ONE PHASE）
7. XA 事务在目标库原子落地（高频采样抓不到半个事务）
8. 已 prepare 分支跨 extract 重启不丢（落盘文件被重新纳管）
9. 无 fail-stop 上报
10. 三进程全部存活
11. 应用连接仍可用
12. 源库无残留未决分支

判据用**水位标记**而不是 sleep 来定：每做完一件事插一行普通数据，等它到达目标库，
就说明它之前的所有事件都已经流完整条链路。这时候断言"XA 的行不在目标库"才有意义，
否则只是没等够。

## 改造前的三个故障（`xa_baseline.py` 复现前两个）

**① prepare 就落库、② 回滚留幻影行**：行事件在 prepare 时刻被应用，源库随后
`XA ROLLBACK`，目标库那几行永远留着，没有任何机制撤回、也没有告警。
基线脚本把缓冲开关关掉即可复现。

**③ 应用连接被打死、任务永久停摆**：`XA START X'..',X'..',1` 在 `SqlClassifier` 里落进
`OTHER`，被兜底路径原样打到目标库。它在目标端**会执行成功**，把应用连接推进 XA ACTIVE 态：

```sql
SET autocommit=0;
XA START X'6161',X'6262',1;   -- 成功
COMMIT;                        -- ERROR 1399 (XAE07) XAER_RMFAIL
ROLLBACK;                      -- ERROR 1399 (XAE07) XAER_RMFAIL
```

`commitPendingTx` 抛异常 → 写 E3004 → 回滚也失败 → fail-stop，位点不推进，
重试从同一条事件重放，再次撞同一堵墙。这条在基线脚本里复现不出来——
`SqlClassifier` 对 XA 控制语句的拦截是无条件的第二道闸，不受开关控制。直接在库上跑上面
四行 SQL 即可验证。

## 相关配置

| 配置 | 默认 | 说明 |
|---|---|---|
| `sync.xa.enabled` | `true` | 关掉即退回"prepare 就下发"的旧行为（只给对照组用） |
| `sync.xa.branch.max.bytes` | 1GB | 单个分支的缓冲上限，超了报 E3018 |
| `sync.xa.pending.max.branches` | 256 | 未决分支个数上限 |
| `sync.xa.pending.max.bytes` | 2GB | 未决分支累计缓冲上限 |
| `sync.xa.pending.warn.minutes` | 60 | 未决分支超过这个时长开始告警（只 warn 不停） |

未决分支数与最老分支年龄落在 `files/<taskId>/binlog_output/xa_pending_branches`、
`xa_pending_oldest_ms`。
