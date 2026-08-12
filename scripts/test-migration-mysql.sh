#!/usr/bin/env bash
set -euo pipefail

version=${1:-8.4}
case "$version" in
  8.0|8.4) ;;
  *) echo "usage: $0 [8.0|8.4]" >&2; exit 2 ;;
esac

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required" >&2
  exit 1
fi

run_id="${version//./}-$$-$RANDOM"
container="sqlspl-migration-mysql-$run_id"
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/sqlspl-mysql-${run_id}.XXXXXX")
journal_dir=$(mktemp -d "$PWD/.sqlspl-mysql-journal-${run_id}.XXXXXX")
cert_dir="$test_dir/certs"
mkdir -p "$cert_dir"

cleanup() {
  if [ -n "${lock_pid:-}" ]; then
    kill "$lock_pid" >/dev/null 2>&1 || true
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$test_dir"
  rm -rf "$journal_dir"
}
trap cleanup EXIT INT TERM

openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=sql-splitter-test-ca' \
  -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$cert_dir/server.ext"
openssl x509 -req -days 2 -in "$cert_dir/server.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -extfile "$cert_dir/server.ext" -out "$cert_dir/server-cert.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=sql-splitter-client' \
  -keyout "$cert_dir/client-key.pem" -out "$cert_dir/client.csr" >/dev/null 2>&1
printf 'extendedKeyUsage=clientAuth\n' > "$cert_dir/client.ext"
openssl x509 -req -days 2 -in "$cert_dir/client.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -extfile "$cert_dir/client.ext" -out "$cert_dir/client-cert.pem" >/dev/null 2>&1
openssl pkcs12 -export -out "$cert_dir/client.p12" \
  -inkey "$cert_dir/client-key.pem" -in "$cert_dir/client-cert.pem" \
  -certfile "$cert_dir/ca.pem" -passout pass:clientpass >/dev/null 2>&1
chmod 0600 "$cert_dir/client.p12"
chmod 0644 "$cert_dir/ca.pem" "$cert_dir/server-cert.pem" "$cert_dir/server-key.pem"

port_file="$test_dir/port"
docker run -d --name "$container" \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -p 127.0.0.1::3306 \
  -v "$cert_dir:/certs:ro" \
  "mysql:$version" \
  --require-secure-transport=ON \
  --ssl-ca=/certs/ca.pem \
  --ssl-cert=/certs/server-cert.pem \
  --ssl-key=/certs/server-key.pem >/dev/null
docker port "$container" 3306/tcp | sed 's/.*://' > "$port_file"
port=$(cat "$port_file")

for _ in $(seq 1 90); do
  if docker exec "$container" mysql -uroot -prootpass -Nse 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$container" mysql -uroot -prootpass -Nse 'SELECT 1' >/dev/null

docker exec -i "$container" mysql -uroot -prootpass <<'SQL'
CREATE DATABASE migration_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE MIGRATION_SOURCE CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'migration_source'@'%' IDENTIFIED BY 'sourcepass' REQUIRE X509;
CREATE USER 'migration_target'@'%' IDENTIFIED BY 'targetpass' REQUIRE X509;
CREATE USER 'migration_admin'@'%' IDENTIFIED BY 'adminpass' REQUIRE X509;
CREATE USER 'business_reader'@'%' IDENTIFIED BY 'unusedpass' REQUIRE X509;
GRANT SELECT, SHOW VIEW ON migration_source.* TO 'migration_source'@'%';
GRANT SELECT ON MIGRATION_SOURCE.* TO 'migration_source'@'%';
GRANT ALL PRIVILEGES ON migration_target.* TO 'migration_target'@'%';
GRANT ALL PRIVILEGES ON migration_source.* TO 'migration_admin'@'%';
GRANT PROCESS ON *.* TO 'migration_admin'@'%';
GRANT SELECT ON performance_schema.* TO 'migration_admin'@'%';
FLUSH PRIVILEGES;
USE migration_source;
CREATE TABLE items (
  id BIGINT NOT NULL PRIMARY KEY,
  name VARCHAR(64) COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO items VALUES (1, 'one'), (2, 'two');
CREATE VIEW items_view AS SELECT id, name FROM items;
CREATE TABLE copy_items (
  id BIGINT NOT NULL PRIMARY KEY,
  payload VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO copy_items VALUES (1, 'one'), (2, 'two');
GRANT SELECT (name) ON migration_source.items TO 'business_reader'@'%';
CREATE INDEX items_lower_name ON items ((lower(name)));
CREATE TABLE ci_keys (
  name VARCHAR(64) COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE
) ENGINE=InnoDB;
CREATE TABLE auto_items (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO auto_items(name) VALUES ('first');
CREATE TABLE legacy (id BIGINT NOT NULL PRIMARY KEY) ENGINE=MyISAM;
CREATE TABLE MIGRATION_SOURCE.case_collision (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
SQL

write_config() {
  path=$1
  database=$2
  user=$3
  credential_env=$4
  cat > "$path" <<EOF
host = "127.0.0.1"
port = $port
database = "$database"
user = "$user"
credential_env = "$credential_env"
connect_timeout_seconds = 10
max_batch_rows = 2
max_batch_bytes = 1048576

[tls]
ca_certificate = "$cert_dir/ca.pem"
client_identity_pkcs12 = "$cert_dir/client.p12"
client_identity_password_env = "SQL_SPLITTER_MYSQL_CLIENT_IDENTITY_PASSWORD"
insecure = false
EOF
}

source_config="$test_dir/source.toml"
target_config="$test_dir/target.toml"
admin_config="$test_dir/admin.toml"
write_config "$source_config" migration_source migration_source SQL_SPLITTER_MYSQL_SOURCE_PASSWORD
write_config "$target_config" migration_target migration_target SQL_SPLITTER_MYSQL_TARGET_PASSWORD
write_config "$admin_config" migration_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD

export SQL_SPLITTER_MYSQL_SOURCE_PASSWORD=sourcepass
export SQL_SPLITTER_MYSQL_TARGET_PASSWORD=targetpass
export SQL_SPLITTER_MYSQL_ADMIN_PASSWORD=adminpass
export SQL_SPLITTER_MYSQL_CLIENT_IDENTITY_PASSWORD=clientpass
export SQL_SPLITTER_MYSQL_TEST_SOURCE_CONFIG="$source_config"
export SQL_SPLITTER_MYSQL_TEST_TARGET_CONFIG="$target_config"
export SQL_SPLITTER_MYSQL_TEST_ADMIN_CONFIG="$admin_config"
export SQL_SPLITTER_MYSQL_TEST_PLAN_OUTPUT="$test_dir/plan.json"
export SQL_SPLITTER_MYSQL_TEST_JOURNAL_OUTPUT="$journal_dir/state.journal"

cargo test --no-default-features --features enterprise-migration-spike \
  --test migration_mysql_plan_test live_mysql_snapshot_catalog_and_blocked_plan \
  -- --ignored --exact --nocapture

docker exec "$container" mysql -uroot -prootpass -Nse \
  "SET PERSIST super_read_only = ON"
docker exec "$container" mysql -uroot -prootpass -Nse \
  "LOCK INSTANCE FOR BACKUP; SELECT SLEEP(300)" >/dev/null 2>&1 &
lock_pid=$!

backup_lock_connection_id=""
for _ in $(seq 1 50); do
  backup_lock_connection_id=$(docker exec "$container" mysql -uroot -prootpass -Nse \
    "SELECT t.PROCESSLIST_ID FROM performance_schema.metadata_locks ml JOIN performance_schema.threads t ON t.THREAD_ID = ml.OWNER_THREAD_ID WHERE ml.OBJECT_TYPE = 'BACKUP LOCK' AND ml.LOCK_STATUS = 'GRANTED' LIMIT 1" 2>/dev/null || true)
  if [ -n "$backup_lock_connection_id" ]; then
    break
  fi
  sleep 0.1
done
if [ -z "$backup_lock_connection_id" ]; then
  echo "external MySQL backup lock did not become observable" >&2
  exit 1
fi
read -r backup_lock_owner_user backup_lock_owner_host <<EOF
$(docker exec "$container" mysql -uroot -prootpass -Nse \
  "SELECT PROCESSLIST_USER, PROCESSLIST_HOST FROM performance_schema.threads WHERE PROCESSLIST_ID = $backup_lock_connection_id")
EOF
export SQL_SPLITTER_MYSQL_BACKUP_LOCK_CONNECTION_ID="$backup_lock_connection_id"
export SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_USER="$backup_lock_owner_user"
export SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_HOST="$backup_lock_owner_host"

cargo test --no-default-features --features enterprise-migration-spike \
  --test migration_mysql_plan_test live_mysql_external_freeze_attestation \
  -- --ignored --exact --nocapture
