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
target_container="sqlspl-migration-mysql-target-$run_id"
test_dir=$(mktemp -d "$PWD/.sqlspl-mysql-test-${run_id}.XXXXXX")
journal_dir=$(mktemp -d "$PWD/.sqlspl-mysql-journal-${run_id}.XXXXXX")
test_dir=$(cd "$test_dir" && pwd -P)
journal_dir=$(cd "$journal_dir" && pwd -P)
cert_dir="$test_dir/certs"
mkdir -p "$cert_dir"

cleanup() {
  if [ -n "${lock_pid:-}" ]; then
    kill "$lock_pid" >/dev/null 2>&1 || true
  fi
  docker rm -fv "$container" >/dev/null 2>&1 || true
  docker rm -fv "$target_container" >/dev/null 2>&1 || true
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
  --tmpfs /var/lib/mysql:rw,size=1g \
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
CREATE DATABASE migration_execution_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'migration_source'@'%' IDENTIFIED BY 'sourcepass' REQUIRE X509;
CREATE USER 'migration_target'@'%' IDENTIFIED BY 'targetpass' REQUIRE X509;
CREATE USER 'migration_admin'@'%' IDENTIFIED BY 'adminpass' REQUIRE X509;
CREATE USER 'source_metadata_admin'@'%' IDENTIFIED BY 'sourcemetapass' REQUIRE X509;
CREATE USER 'target_metadata_admin'@'%' IDENTIFIED BY 'targetmetapass' REQUIRE X509;
CREATE USER 'business_reader'@'%' IDENTIFIED BY 'unusedpass' REQUIRE X509;
CREATE USER 'partially_restricted_reader'@'%' IDENTIFIED BY 'unusedpass' REQUIRE X509;
CREATE USER 'migration_execution_source'@'%' IDENTIFIED BY 'execsourcepass' REQUIRE X509;
CREATE ROLE 'source_metadata_role'@'%';
CREATE ROLE 'target_metadata_role'@'%';
GRANT SELECT, SHOW VIEW ON migration_source.* TO 'migration_source'@'%';
GRANT SELECT ON MIGRATION_SOURCE.* TO 'migration_source'@'%';
GRANT ALL PRIVILEGES ON migration_target.* TO 'migration_target'@'%';
GRANT ALL PRIVILEGES ON migration_source.* TO 'migration_admin'@'%';
GRANT SELECT ON migration_execution_source.* TO 'migration_admin'@'%';
GRANT PROCESS ON *.* TO 'migration_admin'@'%';
GRANT SELECT ON performance_schema.* TO 'migration_admin'@'%';
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON *.* TO 'source_metadata_admin'@'%';
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON *.* TO 'target_metadata_admin'@'%';
GRANT SHOW_ROUTINE ON *.* TO 'source_metadata_role'@'%';
GRANT SHOW_ROUTINE ON *.* TO 'target_metadata_role'@'%';
GRANT 'source_metadata_role'@'%' TO 'source_metadata_admin'@'%';
GRANT 'target_metadata_role'@'%' TO 'target_metadata_admin'@'%';
GRANT PROXY ON ''@'' TO 'migration_admin'@'%' WITH GRANT OPTION;
SET PERSIST partial_revokes = ON;
GRANT SELECT ON *.* TO 'partially_restricted_reader'@'%';
REVOKE SELECT ON migration_source.* FROM 'partially_restricted_reader'@'%';
GRANT SELECT, SHOW VIEW ON migration_execution_source.* TO 'migration_execution_source'@'%';
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
USE migration_execution_source;
CREATE TABLE copy_items (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  payload VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO copy_items(id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three');
ALTER TABLE copy_items AUTO_INCREMENT = 10;
SQL

write_config() {
  path=$1
  endpoint_port=$2
  database=$3
  user=$4
  credential_env=$5
  server_admin_user=${6:-}
  cat > "$path" <<EOF
host = "127.0.0.1"
port = $endpoint_port
database = "$database"
user = "$user"
credential_env = "$credential_env"
connect_timeout_seconds = 10
max_batch_rows = 2
max_batch_bytes = 1048576
EOF
  if [ -n "$server_admin_user" ]; then
    printf '%s\n' \
      'operational_server_administrators = [{ user = "mysql.infoschema", host = "localhost" }, { user = "mysql.session", host = "localhost" }, { user = "mysql.sys", host = "localhost" }, { user = "root", host = "%" }, { user = "root", host = "localhost" }]' \
      >> "$path"
  fi
  cat >> "$path" <<EOF

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
source_metadata_config="$test_dir/source-metadata.toml"
target_metadata_config="$test_dir/target-metadata.toml"
write_config "$source_config" "$port" migration_source migration_source SQL_SPLITTER_MYSQL_SOURCE_PASSWORD
write_config "$target_config" "$port" migration_target migration_target SQL_SPLITTER_MYSQL_TARGET_PASSWORD
write_config "$admin_config" "$port" migration_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$source_metadata_config" "$port" migration_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$target_metadata_config" "$port" migration_target target_metadata_admin SQL_SPLITTER_MYSQL_TARGET_METADATA_PASSWORD root

export SQL_SPLITTER_MYSQL_SOURCE_PASSWORD=sourcepass
export SQL_SPLITTER_MYSQL_TARGET_PASSWORD=targetpass
export SQL_SPLITTER_MYSQL_ADMIN_PASSWORD=adminpass
export SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD=sourcemetapass
export SQL_SPLITTER_MYSQL_TARGET_METADATA_PASSWORD=targetmetapass
export SQL_SPLITTER_MYSQL_CLIENT_IDENTITY_PASSWORD=clientpass
export SQL_SPLITTER_MYSQL_TEST_SOURCE_CONFIG="$source_config"
export SQL_SPLITTER_MYSQL_TEST_TARGET_CONFIG="$target_config"
export SQL_SPLITTER_MYSQL_TEST_ADMIN_CONFIG="$admin_config"
export SQL_SPLITTER_MYSQL_TEST_SOURCE_METADATA_CONFIG="$source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_TARGET_METADATA_CONFIG="$target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_PLAN_OUTPUT="$test_dir/plan.json"
export SQL_SPLITTER_MYSQL_TEST_JOURNAL_OUTPUT="$journal_dir/state.journal"

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_snapshot_catalog_and_blocked_plan \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_metadata_visibility_contract \
  -- --ignored --exact --nocapture

docker exec "$container" mysql -uroot -prootpass -Nse \
  "DROP USER 'business_reader'@'%', 'partially_restricted_reader'@'%', 'migration_target'@'%', 'target_metadata_admin'@'%'; DROP ROLE 'target_metadata_role'@'%'"

docker run -d --name "$target_container" \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -p 127.0.0.1::3306 \
  --tmpfs /var/lib/mysql:rw,size=1g \
  -v "$cert_dir:/certs:ro" \
  "mysql:$version" \
  --require-secure-transport=ON \
  --ssl-ca=/certs/ca.pem \
  --ssl-cert=/certs/server-cert.pem \
  --ssl-key=/certs/server-key.pem >/dev/null
target_port=$(docker port "$target_container" 3306/tcp | sed 's/.*://')
for _ in $(seq 1 90); do
  if docker exec "$target_container" mysql -uroot -prootpass -Nse 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$target_container" mysql -uroot -prootpass -Nse 'SELECT 1' >/dev/null
docker exec -i "$target_container" mysql -uroot -prootpass <<'SQL'
CREATE DATABASE migration_execution_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'migration_execution_target'@'%' IDENTIFIED BY 'exectargetpass' REQUIRE X509;
CREATE USER 'execution_target_metadata_admin'@'%' IDENTIFIED BY 'exectargetmetapass' REQUIRE X509;
CREATE ROLE 'execution_target_metadata_role'@'%';
GRANT ALL PRIVILEGES ON migration_execution_target.* TO 'migration_execution_target'@'%';
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON *.* TO 'execution_target_metadata_admin'@'%';
GRANT SHOW_ROUTINE ON *.* TO 'execution_target_metadata_role'@'%';
GRANT 'execution_target_metadata_role'@'%' TO 'execution_target_metadata_admin'@'%';
SQL

execution_source_config="$test_dir/execution-source.toml"
execution_source_metadata_config="$test_dir/execution-source-metadata.toml"
execution_freeze_config="$test_dir/execution-freeze.toml"
execution_target_config="$test_dir/execution-target.toml"
execution_target_metadata_config="$test_dir/execution-target-metadata.toml"
write_config "$execution_source_config" "$port" migration_execution_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$execution_source_metadata_config" "$port" migration_execution_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$execution_freeze_config" "$port" migration_execution_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$execution_target_config" "$target_port" migration_execution_target migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$execution_target_metadata_config" "$target_port" migration_execution_target execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
export SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD=execsourcepass
export SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD=exectargetpass
export SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD=exectargetmetapass
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG="$execution_source_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG="$execution_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG="$execution_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG="$execution_target_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG="$execution_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT="$test_dir/execution-plan.json"
export SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT="$test_dir/freeze-assertion.json"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT="$journal_dir/execution-state.journal"

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

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_external_freeze_attestation \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_two_container_execute_and_resume \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_recovery_boundary_matrix \
  -- --ignored --exact --nocapture
