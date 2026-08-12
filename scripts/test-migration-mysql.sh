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
tls_container="sqlspl-migration-mysql-tls-$run_id"
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
  docker rm -fv "$tls_container" >/dev/null 2>&1 || true
  rm -rf "$test_dir"
  rm -rf "$journal_dir"
}
trap cleanup EXIT INT TERM

wait_for_mysql() {
  local name=$1
  local consecutive=0
  for _ in $(seq 1 120); do
    if docker exec "$name" mysql -uroot -prootpass -Nse 'SELECT 1' >/dev/null 2>&1; then
      consecutive=$((consecutive + 1))
      if [ "$consecutive" -eq 3 ]; then
        return 0
      fi
    else
      consecutive=0
    fi
    sleep 1
  done
  echo "MySQL container $name did not become stably ready" >&2
  docker logs "$name" >&2 || true
  return 1
}

openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=sql-splitter-test-ca' \
  -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=sql-splitter-test-server' \
  -addext 'subjectAltName=IP:127.0.0.1' \
  -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" >/dev/null 2>&1
printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$cert_dir/server.ext"
openssl x509 -req -days 2 -in "$cert_dir/server.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -extfile "$cert_dir/server.ext" -out "$cert_dir/server-cert.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=sql-splitter-wrong-host-server' \
  -addext 'subjectAltName=IP:127.0.0.2' \
  -keyout "$cert_dir/wrong-server-key.pem" \
  -out "$cert_dir/wrong-server.csr" >/dev/null 2>&1
printf 'subjectAltName=IP:127.0.0.2\nextendedKeyUsage=serverAuth\n' \
  > "$cert_dir/wrong-server.ext"
openssl x509 -req -days 2 -in "$cert_dir/wrong-server.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -extfile "$cert_dir/wrong-server.ext" \
  -out "$cert_dir/wrong-server-cert.pem" >/dev/null 2>&1
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
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=sql-splitter-untrusted-ca' \
  -keyout "$cert_dir/untrusted-ca-key.pem" \
  -out "$cert_dir/untrusted-ca.pem" >/dev/null 2>&1
chmod 0600 "$cert_dir/client.p12"
chmod 0644 \
  "$cert_dir/ca.pem" \
  "$cert_dir/server-cert.pem" \
  "$cert_dir/server-key.pem" \
  "$cert_dir/wrong-server-cert.pem" \
  "$cert_dir/wrong-server-key.pem" \
  "$cert_dir/untrusted-ca.pem"

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
docker run -d --name "$tls_container" \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -p 127.0.0.1::3306 \
  --tmpfs /var/lib/mysql:rw,size=512m \
  -v "$cert_dir:/certs:ro" \
  "mysql:$version" \
  --require-secure-transport=ON \
  --ssl-ca=/certs/ca.pem \
  --ssl-cert=/certs/wrong-server-cert.pem \
  --ssl-key=/certs/wrong-server-key.pem >/dev/null
docker port "$container" 3306/tcp | sed 's/.*://' > "$port_file"
port=$(cat "$port_file")
tls_port=$(docker port "$tls_container" 3306/tcp | sed 's/.*://')

wait_for_mysql "$container"
wait_for_mysql "$tls_container"

docker exec -i "$tls_container" mysql -uroot -prootpass <<'SQL'
CREATE DATABASE tls_probe CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'tls_probe'@'%' IDENTIFIED BY 'tlsprobepass' REQUIRE X509;
GRANT SELECT ON tls_probe.* TO 'tls_probe'@'%';
SQL

docker exec -i "$container" mysql -uroot -prootpass <<'SQL'
SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE MIGRATION_SOURCE CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_execution_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_values_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_fk_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_integrity_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_security_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_authorization_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'migration_source'@'%' IDENTIFIED BY 'sourcepass' REQUIRE X509;
CREATE USER 'migration_target'@'%' IDENTIFIED BY 'targetpass' REQUIRE X509;
CREATE USER 'migration_admin'@'%' IDENTIFIED BY 'adminpass' REQUIRE X509;
CREATE USER 'source_metadata_admin'@'%' IDENTIFIED BY 'sourcemetapass' REQUIRE X509;
CREATE USER 'target_metadata_admin'@'%' IDENTIFIED BY 'targetmetapass' REQUIRE X509;
CREATE USER 'business_reader'@'%' IDENTIFIED BY 'unusedpass' REQUIRE X509;
CREATE USER 'partially_restricted_reader'@'%' IDENTIFIED BY 'unusedpass' REQUIRE X509;
CREATE USER 'migration_execution_source'@'%' IDENTIFIED BY 'execsourcepass' REQUIRE X509;
CREATE USER 'tls_no_client'@'%' IDENTIFIED BY 'tlsnoclientpass';
CREATE USER 'auth_reader'@'%' IDENTIFIED BY 'sourceauthpass' REQUIRE X509;
CREATE USER 'auth_global'@'%' IDENTIFIED BY 'sourceauthpass' REQUIRE X509;
CREATE USER 'auth_proxy'@'%' IDENTIFIED BY 'sourceauthpass' REQUIRE X509;
CREATE ROLE 'auth_role'@'%';
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
GRANT SELECT, SHOW VIEW ON migration_values_source.* TO 'migration_execution_source'@'%';
GRANT SELECT, SHOW VIEW ON migration_fk_source.* TO 'migration_execution_source'@'%';
GRANT SELECT, SHOW VIEW ON migration_integrity_source.* TO 'migration_execution_source'@'%';
GRANT SELECT, SHOW VIEW ON migration_security_source.* TO 'migration_execution_source'@'%';
GRANT SELECT, SHOW VIEW ON migration_authorization_source.* TO 'migration_execution_source'@'%';
GRANT SELECT, SHOW VIEW ON migration_security_source.* TO 'tls_no_client'@'%';
GRANT SYSTEM_USER ON *.* TO 'tls_no_client'@'%';
GRANT SELECT ON migration_values_source.* TO 'migration_admin'@'%';
GRANT SELECT ON migration_fk_source.* TO 'migration_admin'@'%';
GRANT SELECT ON migration_integrity_source.* TO 'migration_admin'@'%';
GRANT SELECT ON migration_security_source.* TO 'migration_admin'@'%';
GRANT SELECT ON migration_authorization_source.* TO 'migration_admin'@'%';
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
CREATE TABLE key_scalar (
  id BIGINT NOT NULL PRIMARY KEY,
  payload VARCHAR(16) COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO key_scalar VALUES
  (-9223372036854775808, 'minimum'), (0, 'zero'),
  (9223372036854775807, 'maximum');
CREATE TABLE key_composite (
  tenant_id INT NOT NULL,
  id BIGINT NOT NULL,
  payload VARCHAR(16) COLLATE utf8mb4_0900_bin NOT NULL,
  PRIMARY KEY (tenant_id, id)
) ENGINE=InnoDB;
INSERT INTO key_composite VALUES
  (-1, -9223372036854775808, 'negative'),
  (0, 0, 'zero'), (0, 1, 'repeated'),
  (1, 9223372036854775807, 'maximum');
CREATE TABLE key_text (
  key_value VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL PRIMARY KEY,
  payload INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO key_text VALUES ('', 0), ('a', 1), ('å', 2), ('水', 3);
CREATE TABLE key_binary (
  key_value VARBINARY(8) NOT NULL PRIMARY KEY,
  payload INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO key_binary VALUES (X'', 0), (X'00', 1), (X'00FF', 2), (X'FF', 3);
CREATE TABLE key_exact_n (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO key_exact_n VALUES (1), (2);
CREATE TABLE key_n_plus_one (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO key_n_plus_one VALUES (1), (2), (3);
CREATE TABLE key_one (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO key_one VALUES (1);
CREATE TABLE key_empty (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
CREATE TABLE key_byte_bound (
  id BIGINT NOT NULL PRIMARY KEY,
  payload VARCHAR(128) COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO key_byte_bound VALUES
  (1, REPEAT('x', 80)), (2, REPEAT('y', 80)), (3, REPEAT('z', 80));
CREATE TABLE key_nullable (
  key_value BIGINT NULL UNIQUE,
  payload INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO key_nullable VALUES (NULL, 1), (1, 2);
CREATE TABLE key_nonunique (
  key_value BIGINT NOT NULL,
  payload INT NOT NULL,
  INDEX (key_value)
) ENGINE=InnoDB;
INSERT INTO key_nonunique VALUES (1, 1), (1, 2);
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
USE migration_values_source;
CREATE TABLE value_matrix (
  id BIGINT NOT NULL PRIMARY KEY,
  nullable_value TEXT NULL,
  unicode_value VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  tiny_value TINYINT NOT NULL,
  small_value SMALLINT NOT NULL,
  medium_value MEDIUMINT NOT NULL,
  int_value INT NOT NULL,
  big_value BIGINT NOT NULL,
  unsigned_big_value BIGINT UNSIGNED NOT NULL,
  decimal_value DECIMAL(30,10) NOT NULL,
  float_value FLOAT NOT NULL,
  double_value DOUBLE NOT NULL,
  bit_value BIT(9) NOT NULL,
  date_value DATE NOT NULL,
  datetime6_value DATETIME(6) NOT NULL,
  datetime0_value DATETIME NOT NULL,
  timestamp6_value TIMESTAMP(6) NOT NULL,
  timestamp0_value TIMESTAMP NOT NULL,
  time6_value TIME(6) NOT NULL,
  year_value YEAR NOT NULL,
  binary_value VARBINARY(8) NOT NULL,
  blob_value BLOB NOT NULL,
  json_value JSON NOT NULL
) ENGINE=InnoDB;
INSERT INTO value_matrix VALUES
  (1, NULL, 'Unicode: åß水🧪', -128, -32768, -8388608, -2147483648,
   -9223372036854775808, 0, -99999999999999999999.9999999999, -0.0, -0.0,
   b'000000001', '1000-01-01', '2000-02-29 10:11:12.123456',
   '2000-02-29 10:11:12', '2000-02-29 10:11:12.123456',
   '2000-02-29 10:11:12', '-838:59:59.000000', 1901,
   X'00FF10', X'00FF00AA',
   '{"z":1.00,"a":1e0,"ten":12,"wide":9007199254740993}'),
  (2, '', 'combining: é', 127, 32767, 8388607, 2147483647,
   9223372036854775807, 18446744073709551615, 99999999999999999999.9999999999,
   3.25, -1.7976931348623157e308, b'111111111', '9999-12-31',
   '2038-01-19 03:14:07.999999', '2038-01-19 03:14:07',
   '2038-01-19 03:14:07.999999', '2038-01-19 03:14:07',
   '838:59:59.000000', 2155, X'FF00', X'FF0010',
   '{"array":[1,1.0,1e0,null,true,false],"nested":{"z":0,"a":1}}'),
  (3, 'present', '', 0, 0, 0, 0, 0, 42, 0.0100000000, 0.0, 0.0,
   b'000000000', '1970-01-01', '1970-01-01 00:00:00.000001',
   '1970-01-01 00:00:00', '1970-01-01 00:00:01.000001',
   '1970-01-01 00:00:01', '00:00:00.000001', 2000,
   X'', X'', '{"duplicate":1,"duplicate":2,"number":1.00}');
USE migration_fk_source;
CREATE TABLE fk_parent (
  tenant_id BIGINT NOT NULL,
  id BIGINT NOT NULL,
  payload VARCHAR(32) COLLATE utf8mb4_0900_bin NOT NULL,
  PRIMARY KEY (tenant_id, id)
) ENGINE=InnoDB;
CREATE TABLE fk_child (
  child_id BIGINT NOT NULL PRIMARY KEY,
  tenant_id BIGINT NULL,
  parent_id BIGINT NULL,
  CONSTRAINT fk_child_parent FOREIGN KEY (tenant_id, parent_id)
    REFERENCES fk_parent (tenant_id, id) ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB;
CREATE TABLE fk_node (
  id BIGINT NOT NULL PRIMARY KEY,
  parent_id BIGINT NULL,
  CONSTRAINT fk_node_parent FOREIGN KEY (parent_id)
    REFERENCES fk_node (id) ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB;
CREATE TABLE fk_cycle_a (
  id BIGINT NOT NULL PRIMARY KEY,
  b_id BIGINT NULL
) ENGINE=InnoDB;
CREATE TABLE fk_cycle_b (
  id BIGINT NOT NULL PRIMARY KEY,
  a_id BIGINT NULL
) ENGINE=InnoDB;
ALTER TABLE fk_cycle_a ADD CONSTRAINT fk_cycle_a_b FOREIGN KEY (b_id)
  REFERENCES fk_cycle_b (id) ON UPDATE CASCADE ON DELETE SET NULL;
ALTER TABLE fk_cycle_b ADD CONSTRAINT fk_cycle_b_a FOREIGN KEY (a_id)
  REFERENCES fk_cycle_a (id) ON UPDATE CASCADE ON DELETE SET NULL;
INSERT INTO fk_parent VALUES (1, 10, 'parent-1'), (2, 20, 'parent-2');
INSERT INTO fk_child VALUES
  (1, 1, 10),
  (2, NULL, 999),
  (3, 2, NULL),
  (4, NULL, NULL);
INSERT INTO fk_node VALUES (1, NULL), (2, 1);
INSERT INTO fk_cycle_a VALUES (1, NULL);
INSERT INTO fk_cycle_b VALUES (1, 1);
UPDATE fk_cycle_a SET b_id = 1 WHERE id = 1;
USE migration_integrity_source;
CREATE TABLE copy_items (
  id BIGINT NOT NULL PRIMARY KEY,
  code VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL UNIQUE,
  payload VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO copy_items VALUES
  (10, 'code-a', 'payload-a'),
  (20, 'code-b', 'payload-b'),
  (30, 'code-c', 'payload-c');
CREATE TABLE empty_items (
  id BIGINT NOT NULL PRIMARY KEY,
  payload VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
USE migration_security_source;
CREATE TABLE `hostile``table;--` (
  `id``key` BIGINT NOT NULL PRIMARY KEY,
  `payload``text` VARCHAR(96) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO `hostile``table;--` VALUES (7, 'row-secret-needle-7f99');
USE migration_authorization_source;
CREATE TABLE authorization_items (
  id BIGINT NOT NULL PRIMARY KEY,
  payload VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL
) ENGINE=InnoDB;
INSERT INTO authorization_items VALUES (1, 'authorization-one'), (2, 'authorization-two');
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
      'operational_server_administrators = [{ user = "mysql.infoschema", host = "localhost" }, { user = "mysql.session", host = "localhost" }, { user = "mysql.sys", host = "localhost" }, { user = "root", host = "%" }, { user = "root", host = "localhost" }, { user = "tls_no_client", host = "%" }]' \
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
wait_for_mysql "$target_container"
docker exec -i "$target_container" mysql -uroot -prootpass <<'SQL'
SET GLOBAL log_bin_trust_function_creators = 1;
CREATE DATABASE migration_execution_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_values_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_fk_target_prepared CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_fk_target_committed CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_fk_target_violation CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_integrity_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_security_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_authorization_target_prepared CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_authorization_target_partial CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_authorization_target_applied CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE DATABASE migration_authorization_target_committed CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'migration_execution_target'@'%' IDENTIFIED BY 'exectargetpass' REQUIRE X509;
CREATE USER 'execution_target_metadata_admin'@'%' IDENTIFIED BY 'exectargetmetapass' REQUIRE X509;
CREATE USER 'tls_no_client'@'%' IDENTIFIED BY 'tlsnoclientpass';
CREATE USER 'mapped_reader_prepared'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_global_prepared'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_proxy_prepared'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE ROLE 'mapped_role_prepared'@'%';
CREATE USER 'mapped_reader_partial'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_global_partial'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_proxy_partial'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE ROLE 'mapped_role_partial'@'%';
CREATE USER 'mapped_reader_applied'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_global_applied'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_proxy_applied'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE ROLE 'mapped_role_applied'@'%';
CREATE USER 'mapped_reader_committed'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_global_committed'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE USER 'mapped_proxy_committed'@'%' IDENTIFIED BY 'mappedpass' REQUIRE X509;
CREATE ROLE 'mapped_role_committed'@'%';
CREATE ROLE 'execution_target_metadata_role'@'%';
GRANT ALL PRIVILEGES ON migration_execution_target.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_values_target.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_fk_target_prepared.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_fk_target_committed.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_fk_target_violation.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_integrity_target.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_security_target.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_authorization_target_prepared.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_authorization_target_partial.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_authorization_target_applied.* TO 'migration_execution_target'@'%';
GRANT ALL PRIVILEGES ON migration_authorization_target_committed.* TO 'migration_execution_target'@'%';
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON *.* TO 'execution_target_metadata_admin'@'%';
GRANT SELECT, UPDATE, DELETE ON *.* TO 'execution_target_metadata_admin'@'%' WITH GRANT OPTION;
GRANT CONNECTION_ADMIN, ROLE_ADMIN ON *.* TO 'execution_target_metadata_admin'@'%' WITH GRANT OPTION;
GRANT PROXY ON ''@'' TO 'execution_target_metadata_admin'@'%' WITH GRANT OPTION;
GRANT SHOW_ROUTINE ON *.* TO 'execution_target_metadata_role'@'%';
GRANT 'execution_target_metadata_role'@'%' TO 'execution_target_metadata_admin'@'%';
GRANT SYSTEM_USER ON *.* TO 'tls_no_client'@'%';
SET PERSIST partial_revokes = ON;
SQL

execution_source_config="$test_dir/execution-source.toml"
execution_source_metadata_config="$test_dir/execution-source-metadata.toml"
execution_freeze_config="$test_dir/execution-freeze.toml"
execution_target_config="$test_dir/execution-target.toml"
execution_target_metadata_config="$test_dir/execution-target-metadata.toml"
values_source_config="$test_dir/values-source.toml"
values_source_metadata_config="$test_dir/values-source-metadata.toml"
values_freeze_config="$test_dir/values-freeze.toml"
values_target_config="$test_dir/values-target.toml"
values_target_metadata_config="$test_dir/values-target-metadata.toml"
fk_source_config="$test_dir/fk-source.toml"
fk_source_metadata_config="$test_dir/fk-source-metadata.toml"
fk_freeze_config="$test_dir/fk-freeze.toml"
fk_prepared_target_config="$test_dir/fk-prepared-target.toml"
fk_prepared_target_metadata_config="$test_dir/fk-prepared-target-metadata.toml"
fk_committed_target_config="$test_dir/fk-committed-target.toml"
fk_committed_target_metadata_config="$test_dir/fk-committed-target-metadata.toml"
fk_violation_target_config="$test_dir/fk-violation-target.toml"
fk_violation_target_metadata_config="$test_dir/fk-violation-target-metadata.toml"
integrity_source_config="$test_dir/integrity-source.toml"
integrity_source_metadata_config="$test_dir/integrity-source-metadata.toml"
integrity_freeze_config="$test_dir/integrity-freeze.toml"
integrity_target_config="$test_dir/integrity-target.toml"
integrity_target_metadata_config="$test_dir/integrity-target-metadata.toml"
security_source_config="$test_dir/security-source.toml"
security_source_metadata_config="$test_dir/security-source-metadata.toml"
security_freeze_config="$test_dir/security-freeze.toml"
security_target_config="$test_dir/security-target.toml"
security_target_metadata_config="$test_dir/security-target-metadata.toml"
authorization_source_config="$test_dir/authorization-source.toml"
authorization_source_metadata_config="$test_dir/authorization-source-metadata.toml"
authorization_freeze_config="$test_dir/authorization-freeze.toml"
wrong_hostname_config="$test_dir/wrong-hostname.toml"
untrusted_ca_config="$test_dir/untrusted-ca.toml"
missing_client_config="$test_dir/missing-client.toml"
no_client_control_config="$test_dir/no-client-control.toml"
explicit_insecure_config="$test_dir/explicit-insecure.toml"
write_config "$execution_source_config" "$port" migration_execution_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$execution_source_metadata_config" "$port" migration_execution_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$execution_freeze_config" "$port" migration_execution_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$execution_target_config" "$target_port" migration_execution_target migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$execution_target_metadata_config" "$target_port" migration_execution_target execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$values_source_config" "$port" migration_values_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$values_source_metadata_config" "$port" migration_values_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$values_freeze_config" "$port" migration_values_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$values_target_config" "$target_port" migration_values_target migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$values_target_metadata_config" "$target_port" migration_values_target execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$fk_source_config" "$port" migration_fk_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$fk_source_metadata_config" "$port" migration_fk_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$fk_freeze_config" "$port" migration_fk_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$fk_prepared_target_config" "$target_port" migration_fk_target_prepared migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$fk_prepared_target_metadata_config" "$target_port" migration_fk_target_prepared execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$fk_committed_target_config" "$target_port" migration_fk_target_committed migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$fk_committed_target_metadata_config" "$target_port" migration_fk_target_committed execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$fk_violation_target_config" "$target_port" migration_fk_target_violation migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$fk_violation_target_metadata_config" "$target_port" migration_fk_target_violation execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$integrity_source_config" "$port" migration_integrity_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$integrity_source_metadata_config" "$port" migration_integrity_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$integrity_freeze_config" "$port" migration_integrity_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$integrity_target_config" "$target_port" migration_integrity_target migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$integrity_target_metadata_config" "$target_port" migration_integrity_target execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$security_source_config" "$port" migration_security_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$security_source_metadata_config" "$port" migration_security_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$security_freeze_config" "$port" migration_security_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
write_config "$security_target_config" "$target_port" migration_security_target migration_execution_target SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
write_config "$security_target_metadata_config" "$target_port" migration_security_target execution_target_metadata_admin SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD root
write_config "$authorization_source_config" "$port" migration_authorization_source migration_execution_source SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD
write_config "$authorization_source_metadata_config" "$port" migration_authorization_source source_metadata_admin SQL_SPLITTER_MYSQL_SOURCE_METADATA_PASSWORD root
write_config "$authorization_freeze_config" "$port" migration_authorization_source migration_admin SQL_SPLITTER_MYSQL_ADMIN_PASSWORD
for authorization_case in prepared partial applied committed; do
  write_config \
    "$test_dir/authorization-${authorization_case}-target.toml" \
    "$target_port" \
    "migration_authorization_target_${authorization_case}" \
    migration_execution_target \
    SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD
  write_config \
    "$test_dir/authorization-${authorization_case}-target-metadata.toml" \
    "$target_port" \
    "migration_authorization_target_${authorization_case}" \
    execution_target_metadata_admin \
    SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD \
    root
  cat > "$test_dir/authorization-${authorization_case}-mapping.json" <<EOF
{"schema_version":1,"accounts":[{"source":{"user":"auth_global","host":"%"},"target":{"user":"mapped_global_${authorization_case}","host":"%"}},{"source":{"user":"auth_proxy","host":"%"},"target":{"user":"mapped_proxy_${authorization_case}","host":"%"}},{"source":{"user":"auth_reader","host":"%"},"target":{"user":"mapped_reader_${authorization_case}","host":"%"}},{"source":{"user":"auth_role","host":"%"},"target":{"user":"mapped_role_${authorization_case}","host":"%"}}]}
EOF
  chmod 0600 "$test_dir/authorization-${authorization_case}-mapping.json"
done
write_config "$no_client_control_config" "$port" migration_security_source tls_no_client SQL_SPLITTER_MYSQL_TLS_NO_CLIENT_PASSWORD
awk '!/^client_identity_pkcs12 = / && !/^client_identity_password_env = /' \
  "$no_client_control_config" > "$no_client_control_config.tmp"
mv "$no_client_control_config.tmp" "$no_client_control_config"

write_config "$wrong_hostname_config" "$tls_port" tls_probe tls_probe SQL_SPLITTER_MYSQL_TLS_PROBE_PASSWORD
cp "$execution_source_config" "$untrusted_ca_config"
awk -v replacement="ca_certificate = \"$cert_dir/untrusted-ca.pem\"" \
  '{ if ($0 ~ /^ca_certificate = /) print replacement; else print }' \
  "$untrusted_ca_config" > "$untrusted_ca_config.tmp"
mv "$untrusted_ca_config.tmp" "$untrusted_ca_config"
awk '!/^client_identity_pkcs12 = / && !/^client_identity_password_env = /' \
  "$execution_source_config" > "$missing_client_config"
cp "$wrong_hostname_config" "$explicit_insecure_config"
awk -v replacement="ca_certificate = \"$cert_dir/untrusted-ca.pem\"" \
  '{
    if ($0 ~ /^ca_certificate = /) print replacement;
    else if ($0 == "insecure = false") print "insecure = true";
    else print;
  }' "$explicit_insecure_config" > "$explicit_insecure_config.tmp"
mv "$explicit_insecure_config.tmp" "$explicit_insecure_config"
export SQL_SPLITTER_MYSQL_EXECUTION_SOURCE_PASSWORD=execsourcepass
export SQL_SPLITTER_MYSQL_EXECUTION_TARGET_PASSWORD=exectargetpass
export SQL_SPLITTER_MYSQL_EXECUTION_TARGET_METADATA_PASSWORD=exectargetmetapass
export SQL_SPLITTER_MYSQL_TLS_PROBE_PASSWORD=tlsprobepass
export SQL_SPLITTER_MYSQL_TLS_NO_CLIENT_PASSWORD=tlsnoclientpass
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG="$execution_source_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG="$execution_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG="$execution_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG="$execution_target_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG="$execution_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT="$test_dir/execution-plan.json"
export SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT="$test_dir/freeze-assertion.json"
export SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT="$journal_dir/execution-state.journal"
export SQL_SPLITTER_MYSQL_TEST_VALUES_SOURCE_CONFIG="$values_source_config"
export SQL_SPLITTER_MYSQL_TEST_VALUES_SOURCE_METADATA_CONFIG="$values_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_VALUES_FREEZE_CONFIG="$values_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_VALUES_TARGET_CONFIG="$values_target_config"
export SQL_SPLITTER_MYSQL_TEST_VALUES_TARGET_METADATA_CONFIG="$values_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_VALUES_PLAN_OUTPUT="$test_dir/values-plan.json"
export SQL_SPLITTER_MYSQL_TEST_VALUES_ASSERTION_OUTPUT="$test_dir/values-assertion.json"
export SQL_SPLITTER_MYSQL_TEST_VALUES_JOURNAL_OUTPUT="$journal_dir/values-state.journal"
export SQL_SPLITTER_MYSQL_TEST_FK_SOURCE_CONFIG="$fk_source_config"
export SQL_SPLITTER_MYSQL_TEST_FK_SOURCE_METADATA_CONFIG="$fk_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_FK_FREEZE_CONFIG="$fk_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_FK_PREPARED_TARGET_CONFIG="$fk_prepared_target_config"
export SQL_SPLITTER_MYSQL_TEST_FK_PREPARED_TARGET_METADATA_CONFIG="$fk_prepared_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_FK_COMMITTED_TARGET_CONFIG="$fk_committed_target_config"
export SQL_SPLITTER_MYSQL_TEST_FK_COMMITTED_TARGET_METADATA_CONFIG="$fk_committed_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_FK_VIOLATION_TARGET_CONFIG="$fk_violation_target_config"
export SQL_SPLITTER_MYSQL_TEST_FK_VIOLATION_TARGET_METADATA_CONFIG="$fk_violation_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_FK_ARTIFACT_DIR="$test_dir"
export SQL_SPLITTER_MYSQL_TEST_FK_JOURNAL_DIR="$journal_dir"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_SOURCE_CONFIG="$integrity_source_config"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_SOURCE_METADATA_CONFIG="$integrity_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_FREEZE_CONFIG="$integrity_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_TARGET_CONFIG="$integrity_target_config"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_TARGET_METADATA_CONFIG="$integrity_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_ARTIFACT_DIR="$test_dir"
export SQL_SPLITTER_MYSQL_TEST_INTEGRITY_JOURNAL_DIR="$journal_dir"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_SOURCE_CONFIG="$security_source_config"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_SOURCE_METADATA_CONFIG="$security_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_FREEZE_CONFIG="$security_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_TARGET_CONFIG="$security_target_config"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_TARGET_METADATA_CONFIG="$security_target_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_WRONG_HOSTNAME_CONFIG="$wrong_hostname_config"
export SQL_SPLITTER_MYSQL_TEST_UNTRUSTED_CA_CONFIG="$untrusted_ca_config"
export SQL_SPLITTER_MYSQL_TEST_MISSING_CLIENT_CONFIG="$missing_client_config"
export SQL_SPLITTER_MYSQL_TEST_NO_CLIENT_CONTROL_CONFIG="$no_client_control_config"
export SQL_SPLITTER_MYSQL_TEST_EXPLICIT_INSECURE_CONFIG="$explicit_insecure_config"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_ARTIFACT_DIR="$test_dir"
export SQL_SPLITTER_MYSQL_TEST_SECURITY_JOURNAL_DIR="$journal_dir"
export SQL_SPLITTER_MYSQL_TEST_AUTHORIZATION_SOURCE_CONFIG="$authorization_source_config"
export SQL_SPLITTER_MYSQL_TEST_AUTHORIZATION_SOURCE_METADATA_CONFIG="$authorization_source_metadata_config"
export SQL_SPLITTER_MYSQL_TEST_AUTHORIZATION_FREEZE_CONFIG="$authorization_freeze_config"
export SQL_SPLITTER_MYSQL_TEST_AUTHORIZATION_ARTIFACT_DIR="$test_dir"
export SQL_SPLITTER_MYSQL_TEST_AUTHORIZATION_JOURNAL_DIR="$journal_dir"
export SQL_SPLITTER_MYSQL_AUTH_MAPPED_PASSWORD=mappedpass

acquire_backup_lock() {
  docker exec "$container" mysql -uroot -prootpass -Nse \
    "LOCK INSTANCE FOR BACKUP; SELECT SLEEP(900)" >/dev/null 2>&1 &
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
}

release_backup_lock() {
  docker exec "$container" mysql -uroot -prootpass -Nse \
    "KILL CONNECTION $backup_lock_connection_id" >/dev/null
  wait "$lock_pid" 2>/dev/null || true
  lock_pid=""
  for _ in $(seq 1 50); do
    lock_owner_exists=$(docker exec "$container" mysql -uroot -prootpass -Nse \
      "SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_ID = $backup_lock_connection_id" 2>/dev/null || true)
    if [ "$lock_owner_exists" = "0" ]; then
      break
    fi
    sleep 0.1
  done
  if [ "$lock_owner_exists" != "0" ]; then
    echo "external MySQL backup-lock owner did not terminate" >&2
    exit 1
  fi
}

docker exec "$container" mysql -uroot -prootpass -Nse \
  "SET PERSIST super_read_only = ON"
acquire_backup_lock

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_external_freeze_attestation \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_tls_redaction_and_artifact_security \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_two_container_execute_and_resume \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_canonical_value_matrix \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_foreign_key_integrity_and_recovery_matrix \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_conflict_no_skip_and_target_coverage_matrix \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_recovery_boundary_matrix \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_cancellation_rolls_back_and_resumes \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_network_commit_response_loss_matrix \
  -- --ignored --exact --nocapture

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_drift_rejection_matrix \
  -- --ignored --exact --nocapture

# Business grants are installed only after the other source-database matrices
# finish, so their server-wide metadata captures remain free of this fixture.
release_backup_lock
docker exec "$container" mysql -uroot -prootpass -Nse \
  "SET PERSIST super_read_only = OFF"
docker exec -i "$container" mysql -uroot -prootpass <<'SQL'
GRANT SELECT ON migration_authorization_source.* TO 'auth_reader'@'%' WITH GRANT OPTION;
GRANT UPDATE (payload) ON migration_authorization_source.authorization_items TO 'auth_reader'@'%';
GRANT DELETE ON migration_authorization_source.authorization_items TO 'auth_role'@'%';
GRANT 'auth_role'@'%' TO 'auth_reader'@'%' WITH ADMIN OPTION;
SET DEFAULT ROLE 'auth_role'@'%' TO 'auth_reader'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'auth_reader'@'%';
GRANT SELECT ON *.* TO 'auth_global'@'%';
REVOKE SELECT ON migration_authorization_source.* FROM 'auth_global'@'%';
GRANT PROXY ON 'auth_reader'@'%' TO 'auth_proxy'@'%' WITH GRANT OPTION;
SQL
docker exec "$container" mysql -uroot -prootpass -Nse \
  "SET PERSIST super_read_only = ON"
acquire_backup_lock

cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_authorization_restoration_recovery_matrix \
  -- --ignored --exact --nocapture

# The disposable container root owns this backup-lock connection. Terminate it
# here, then prove that execution stops before journal creation. This must
# remain last.
release_backup_lock
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_mysql_plan_test live_mysql_freeze_loss_stops_before_journal \
  -- --ignored --exact --nocapture
