# e2e 测试脚本

## e2e_smoke.py —— 冒烟测试 harness（推荐，CI 门禁用）

把"起任务 → 打数据 → 验结果 → 清理"固化为可重复、可作 CI 门禁的脚本。
**零第三方依赖**（仅 Python 3 标准库 + `docker exec` 访问 MySQL），退出码 0=全通过、非 0=有失败。

### 前置
基础设施 + agent + backend 已启动：
```bash
./create_env.sh   # 首次或 remove_env.sh 之后
./start.sh
```

### 用法
```bash
python3 test_scripts/e2e_smoke.py                 # 全部场景
python3 test_scripts/e2e_smoke.py multidb_compare  # 指定场景
```

### 场景
| 名称 | 覆盖 |
|------|------|
| `mysql_mapping`    | mysql→mysql 全量+增量、库名映射（per-db 目标库解析）、增量 I/U/D 传播 |
| `multidb_compare`  | 多库 per-db 映射贯通对比链路、行数/内容对比、投毒差异检出 |

### 环境变量覆盖（CI）
`E2E_BASE_URL`(默认 http://localhost:38080) `E2E_USER`/`E2E_PASS`(admin/admin123)
`E2E_MYSQL_CONTAINER`(synctask-mysql) `E2E_MYSQL_USER`/`E2E_MYSQL_PASS`(root/rootpassword)
`E2E_SRC_CONN`/`E2E_TGT_CONN`(mysql://root:rootpassword@localhost:33306)
`E2E_STATUS_WAIT`(等状态超时秒，默认 240) `E2E_PROPAGATE`(增量传播等待秒，默认 15)

### CI 集成要点（实测教训）
- **每轮重启 agent**：单个 agent 长跑数十个测试任务后会 wedge（任务卡在 STARTING、不再 spawn 子进程）。
  CI 里跑 harness 前应重启 agent 保证干净状态，否则会假失败。
- 无第三方依赖，`python3` 直接可跑；输出请配 `python3 -u` 避免管道缓冲看不到进度。
- 每个场景自带 `finally` 清理（删任务 + drop 库），失败也不留残渣。

## 其它历史脚本
`e2e_test*.py` / `run_ora_pg_*.sh` 等为早期零散验证脚本（部分 BASE_URL 仍是旧端口 8082），
按需参考；新验证优先加进 `e2e_smoke.py` 的场景表。
