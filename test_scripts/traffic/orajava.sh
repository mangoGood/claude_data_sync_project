#!/usr/bin/env bash
# 在与 oracle_db 同一 docker 网络的容器里跑 migration-traffic。
#
# 为什么要绕这一道：本机 host→oracle_db 的 1521 数据面是坏的（TCP 能连上、
# 字节到不了监听器，监听器日志里连一条尝试都没有；同网段的容器客户端一切正常）。
# 这是本机 Docker 端口转发的环境故障，不是产品问题——同一套代码从容器里跑就通。
#
# 用法: test_scripts/traffic/orajava.sh --mode capture --config files/xxx/config.properties
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec docker run --rm --network oracle_db_network \
  -v "$ROOT:/work" -w /work --entrypoint java \
  docker.1ms.run/library/flink:scala_2.12-java21 \
  -jar migration-traffic/target/migration-traffic-1.0.0.jar "$@"
