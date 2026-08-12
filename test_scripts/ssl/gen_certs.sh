#!/usr/bin/env bash
# ============================================================
# 生成一整套自签 TLS 证书，供 SSL/TLS 判据与本地联调使用。
#
# 产物（默认落在 test_scripts/ssl/out/）：
#   ca.pem / ca-key.pem              自签 CA
#   server-<name>.pem / -key.pem     各服务端证书（MySQL/PG/Mongo/Redis 用）
#   client.pem / client-key.pem      客户端证书（mTLS）
#   client-key.pk8                   PKCS8 DER —— PostgreSQL JDBC 专用
#   truststore.p12                   CA 导入 —— MySQL/Kafka/Oracle/后端信任 agent
#   backend-keystore.p12             后端 HTTPS
#   agent-keystore.p12               agent HTTPS
#   kafka.server.keystore.jks        Kafka broker（JKS，Kafka 只认它和 p12）
#
# ⚠ 服务端证书的 CN/SAN 必须覆盖**实际连接用的主机名**。VERIFY_IDENTITY 档位下
#   证书里没有 127.0.0.1 就一定失败，而报错会把人带偏到"证书不对"。因此这里给每张
#   服务端证书都写上 localhost / 127.0.0.1 / 容器名 / compose 固定 IP。
#
# 用法: ./test_scripts/ssl/gen_certs.sh [输出目录]
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/../.."

OUT="${1:-test_scripts/ssl/out}"
PASS="${SYNCTASK_TLS_PASS:-synctask}"
DAYS="${SYNCTASK_TLS_DAYS:-3650}"

command -v openssl >/dev/null 2>&1 || { echo "✗ 需要 openssl"; exit 1; }
if /usr/libexec/java_home -v 21 >/dev/null 2>&1; then
  KEYTOOL="$(/usr/libexec/java_home -v 21)/bin/keytool"
else
  KEYTOOL="keytool"
fi

mkdir -p "$OUT"
chmod 700 "$OUT"
echo "[certs] 输出目录: $OUT"

# ---- CA ----
if [ ! -f "$OUT/ca.pem" ]; then
  openssl req -x509 -newkey rsa:2048 -days "$DAYS" -nodes \
    -keyout "$OUT/ca-key.pem" -out "$OUT/ca.pem" \
    -subj "/C=CN/O=synctask/CN=synctask-test-ca" 2>/dev/null
  echo "[certs] ✓ CA"
fi

# $1=名称  $2=额外 SAN（逗号分隔，可空）
gen_server_cert() {
  local name="$1" extra_san="${2:-}"
  local san="DNS:localhost,DNS:$name,IP:127.0.0.1"
  [ -n "$extra_san" ] && san="$san,$extra_san"
  [ -f "$OUT/server-$name.pem" ] && return 0
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$OUT/server-$name-key.pem" -out "$OUT/server-$name.csr" \
    -subj "/C=CN/O=synctask/CN=$name" 2>/dev/null
  openssl x509 -req -in "$OUT/server-$name.csr" -days "$DAYS" \
    -CA "$OUT/ca.pem" -CAkey "$OUT/ca-key.pem" -CAcreateserial \
    -out "$OUT/server-$name.pem" \
    -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth\n" "$san") 2>/dev/null
  rm -f "$OUT/server-$name.csr"
  # MySQL/PG 要求私钥仅属主可读，否则直接拒绝启动（"has group or world access"）
  chmod 600 "$OUT/server-$name-key.pem"
  echo "[certs] ✓ server-$name (SAN: $san)"
}

gen_server_cert mysql   "DNS:dr-mysql-a,DNS:dr-mysql-b,DNS:synctask-mysql"
gen_server_cert pg      "DNS:postgres_db,DNS:dr-pg-a,DNS:dr-pg-b"
gen_server_cert mongo   "DNS:synctask-mongo-a,DNS:synctask-mongo-b"
gen_server_cert redis   "DNS:synctask-redis-a,DNS:synctask-redis-b"
gen_server_cert kafka   "DNS:synctask-kafka,DNS:synctask-kafka-sub"
gen_server_cert backend ""
gen_server_cert agent   ""

# ---- 客户端证书（mTLS）----
if [ ! -f "$OUT/client.pem" ]; then
  openssl req -newkey rsa:2048 -nodes -keyout "$OUT/client-key.pem" -out "$OUT/client.csr" \
    -subj "/C=CN/O=synctask/CN=synctask-client" 2>/dev/null
  openssl x509 -req -in "$OUT/client.csr" -days "$DAYS" \
    -CA "$OUT/ca.pem" -CAkey "$OUT/ca-key.pem" -CAcreateserial -out "$OUT/client.pem" \
    -extfile <(printf "extendedKeyUsage=clientAuth\n") 2>/dev/null
  rm -f "$OUT/client.csr"
  # PostgreSQL JDBC 只吃 PKCS8 DER；给它 PEM 会报 "Unsupported key type"，
  # 与"证书不对"完全不像，所以这里就转好
  openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt \
    -in "$OUT/client-key.pem" -out "$OUT/client-key.pk8" 2>/dev/null
  chmod 600 "$OUT/client-key.pem" "$OUT/client-key.pk8"
  echo "[certs] ✓ client (+ PKCS8 DER)"
fi

# ---- p12 / jks（MySQL·Kafka·Oracle·后端·agent 认这些，不认 PEM）----
mk_p12() {  # $1=名称  $2=输出
  [ -f "$2" ] && return 0
  openssl pkcs12 -export -in "$OUT/server-$1.pem" -inkey "$OUT/server-$1-key.pem" \
    -certfile "$OUT/ca.pem" -name "$1" -out "$2" -passout "pass:$PASS" 2>/dev/null
  chmod 600 "$2"
  echo "[certs] ✓ $(basename "$2")"
}
mk_p12 backend "$OUT/backend-keystore.p12"
mk_p12 agent   "$OUT/agent-keystore.p12"
mk_p12 kafka   "$OUT/kafka-keystore.p12"

if [ ! -f "$OUT/truststore.p12" ]; then
  "$KEYTOOL" -importcert -noprompt -alias synctask-ca -file "$OUT/ca.pem" \
    -keystore "$OUT/truststore.p12" -storetype PKCS12 -storepass "$PASS" >/dev/null 2>&1
  chmod 600 "$OUT/truststore.p12"
  echo "[certs] ✓ truststore.p12"
fi

# 客户端 keystore（mTLS 时给驱动用）
if [ ! -f "$OUT/client-keystore.p12" ]; then
  openssl pkcs12 -export -in "$OUT/client.pem" -inkey "$OUT/client-key.pem" \
    -certfile "$OUT/ca.pem" -name client -out "$OUT/client-keystore.p12" \
    -passout "pass:$PASS" 2>/dev/null
  chmod 600 "$OUT/client-keystore.p12"
  echo "[certs] ✓ client-keystore.p12"
fi

# Mongo 要 cert+key **合并成一个 pem**（--tlsCertificateKeyFile），与别家都不一样
if [ ! -f "$OUT/mongo-combined.pem" ]; then
  cat "$OUT/server-mongo-key.pem" "$OUT/server-mongo.pem" > "$OUT/mongo-combined.pem"
  chmod 600 "$OUT/mongo-combined.pem"
  echo "[certs] ✓ mongo-combined.pem"
fi

echo ""
echo "[certs] 完成。口令统一为: $PASS"
echo "[certs] 平台组件一键开 TLS:  SYNCTASK_TLS_ALL=1 ./start.sh"
echo "[certs] 数据库侧开 SSL 见:   test_scripts/ssl/README.md"
