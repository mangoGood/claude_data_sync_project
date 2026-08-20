#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""全局配置：后端地址、各数据库端点、运行期开关。

所有值都可以用环境变量覆盖（CI/Jenkins 里改环境即可，不用改代码）。
端点口径与 test_scripts/ 下既有脚本保持一致，避免两套事实。
"""
import os

# ------------------------------------------------------------------ 控制面
BASE_URL = os.environ.get("AT_BASE_URL", "http://localhost:38080")
USER = os.environ.get("AT_USER", "admin")
PASSWORD = os.environ.get("AT_PASS", "admin123")

# 项目根目录（autotest/ 的上一级）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ------------------------------------------------------------------ 数据面端点
# 同步/对比链路用的常规实例（docker-compose-synctask*.yml 起的那套）
MYSQL = dict(kind="mysql", host="127.0.0.1", port=33306, user="root", password="rootpassword",
             container="synctask-mysql")
# 流量回放的目标端：必须与 MYSQL 是**不同实例**（产品硬拦"回放到录制源库自己"）。
# 用 docker-compose-synctask.yml 里那台 synctask-mysql-b（33307）。
MYSQL_B = dict(kind="mysql", host="127.0.0.1", port=33307, user="root", password="rootpassword",
               container="synctask-mysql-b")
PG = dict(kind="pg", host="127.0.0.1", port=5432, user="app_user", password="userpassword",
          container="postgres_db")
MONGO_A = dict(kind="mongo", host="127.0.0.1", port=27117, user="root", password="rootpassword",
               container="synctask-mongo-a")
MONGO_B = dict(kind="mongo", host="127.0.0.1", port=27118, user="root", password="rootpassword",
               container="synctask-mongo-b")
REDIS_A = dict(kind="redis", host="127.0.0.1", port=6390, password="syncredis",
               container="synctask-redis-a")
REDIS_B = dict(kind="redis", host="127.0.0.1", port=6391, password="syncredis",
               container="synctask-redis-b")
ES = dict(kind="es", host="127.0.0.1", port=9200, user="elastic", password="espassword",
          container="synctask-es")
TIDB = dict(kind="mysql", host="127.0.0.1", port=14000, user="root", password="tidbpassword",
            container="synctask-tidb")
ORACLE = dict(kind="oracle", host="127.0.0.1", port=1521, user="app_user", password="userpassword",
              service="FREEPDB1", container="oracle_db")

# 灾备两端必须是**不同实例**（后端预校验强制），用 docker-compose-synctask-dr.yml 起的那套
DR_MYSQL_A = dict(kind="mysql", host="127.0.0.1", port=33320, user="root", password="rootpassword",
                  container="dr-mysql-a")
DR_MYSQL_B = dict(kind="mysql", host="127.0.0.1", port=33321, user="root", password="rootpassword",
                  container="dr-mysql-b")
DR_PG_A = dict(kind="pg", host="127.0.0.1", port=55432, user="postgres", password="rootpassword",
               container="dr-pg-a")
DR_PG_B = dict(kind="pg", host="127.0.0.1", port=55433, user="postgres", password="rootpassword",
               container="dr-pg-b")

# 订阅下游 Kafka（专用，与控制面 29092 隔离；控制面被几十万条 CDC 挤住会让任务下发/状态上报卡死）
SUB_KAFKA = os.environ.get("AT_SUB_KAFKA", "localhost:39092")
SUB_KAFKA_CONTAINER = "synctask-kafka-sub"

# ------------------------------------------------------------------ 用例规模与超时
# 单线程串行执行，规模刻意压小：默认 profile 的目标是整轮 ≈30 分钟。
SEED_ROWS = int(os.environ.get("AT_SEED_ROWS", "300"))       # 全量存量行数
INCR_ROWS = int(os.environ.get("AT_INCR_ROWS", "60"))        # 增量写入行数
LAUNCH_TIMEOUT = int(os.environ.get("AT_LAUNCH_TIMEOUT", "300"))   # 等任务进入运行态
CONVERGE_TIMEOUT = int(os.environ.get("AT_CONVERGE_TIMEOUT", "180"))  # 等目标端追平
COMPARE_TIMEOUT = int(os.environ.get("AT_COMPARE_TIMEOUT", "180"))   # 等对比任务出结论
POLL_INTERVAL = float(os.environ.get("AT_POLL", "3"))

# 测试对象统一命名：前缀 at_ 便于事后人工辨认与批量清理
PREFIX = os.environ.get("AT_PREFIX", "at")
TABLE = "at_load"

# 运行目录
AUTOTEST_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = os.environ.get("AT_REPORT_DIR", os.path.join(AUTOTEST_DIR, "reports"))
STATE_FILE = os.environ.get("AT_STATE_FILE", os.path.join(AUTOTEST_DIR, ".autotest_state.json"))
PID_FILE = os.path.join(AUTOTEST_DIR, ".autotest.pid")
