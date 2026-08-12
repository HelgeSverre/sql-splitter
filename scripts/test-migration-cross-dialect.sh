#!/usr/bin/env bash
set -euo pipefail

postgres_version=${1:-17}
mysql_version=${2:-8.4}
case "$postgres_version" in 15|16|17) ;; *) echo "usage: $0 [15|16|17] [8.0|8.4]" >&2; exit 2 ;; esac
case "$mysql_version" in 8.0|8.4) ;; *) echo "usage: $0 [15|16|17] [8.0|8.4]" >&2; exit 2 ;; esac
command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

run_id="${postgres_version}-${mysql_version//./}-$$-$RANDOM"
pg_container="sqlspl-cross-pg-$run_id"
mysql_container="sqlspl-cross-mysql-$run_id"
mysql_target_container="sqlspl-cross-mysql-target-$run_id"
test_dir=$(mktemp -d "$PWD/.sqlspl-cross-${run_id}.XXXXXX")
cert_dir="$test_dir/mysql-certs"
mkdir -p "$cert_dir"

cleanup() {
  if [[ -n "${lock_pid:-}" ]]; then kill "$lock_pid" >/dev/null 2>&1 || true; fi
  docker rm -fv "$pg_container" >/dev/null 2>&1 || true
  docker rm -fv "$mysql_container" >/dev/null 2>&1 || true
  docker rm -fv "$mysql_target_container" >/dev/null 2>&1 || true
  rm -rf "$test_dir"
}
trap cleanup EXIT INT TERM

wait_for_postgres() {
  for _ in $(seq 1 60); do
    docker exec "$pg_container" pg_isready -U postgres >/dev/null 2>&1 && return 0
    sleep 1
  done
  docker logs "$pg_container" >&2 || true
  return 1
}

wait_for_mysql() {
  local container=$1
  for _ in $(seq 1 90); do
    docker exec "$container" mysql -uroot -prootpass -Nse 'SELECT 1' >/dev/null 2>&1 && return 0
    sleep 1
  done
  docker logs "$container" >&2 || true
  return 1
}

openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=sql-splitter-cross-ca' \
  -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj '/CN=cross-mysql' \
  -addext 'subjectAltName=IP:127.0.0.1' \
  -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" >/dev/null 2>&1
printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$cert_dir/server.ext"
openssl x509 -req -days 2 -in "$cert_dir/server.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -extfile "$cert_dir/server.ext" -out "$cert_dir/server-cert.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj '/CN=cross-client' \
  -keyout "$cert_dir/client-key.pem" -out "$cert_dir/client.csr" >/dev/null 2>&1
printf 'extendedKeyUsage=clientAuth\n' > "$cert_dir/client.ext"
openssl x509 -req -days 2 -in "$cert_dir/client.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -extfile "$cert_dir/client.ext" -out "$cert_dir/client-cert.pem" >/dev/null 2>&1
openssl pkcs12 -export -out "$cert_dir/client.p12" \
  -inkey "$cert_dir/client-key.pem" -in "$cert_dir/client-cert.pem" \
  -certfile "$cert_dir/ca.pem" -passout pass:clientpass >/dev/null 2>&1
chmod 0600 "$cert_dir/client.p12"
chmod 0644 "$cert_dir/ca.pem" "$cert_dir/server-key.pem" "$cert_dir/server-cert.pem"

docker run -d --name "$pg_container" -e POSTGRES_PASSWORD=admin-secret \
  -p 127.0.0.1::5432 \
  "postgres:$postgres_version" >/dev/null
docker run -d --name "$mysql_container" -e MYSQL_ROOT_PASSWORD=rootpass \
  -p 127.0.0.1::3306 --tmpfs /var/lib/mysql:rw,size=1g \
  -v "$cert_dir:/certs:ro" "mysql:$mysql_version" \
  --require-secure-transport=ON --ssl-ca=/certs/ca.pem \
  --ssl-cert=/certs/server-cert.pem --ssl-key=/certs/server-key.pem >/dev/null
docker run -d --name "$mysql_target_container" -e MYSQL_ROOT_PASSWORD=rootpass \
  -p 127.0.0.1::3306 --tmpfs /var/lib/mysql:rw,size=1g \
  -v "$cert_dir:/certs:ro" "mysql:$mysql_version" \
  --require-secure-transport=ON --ssl-ca=/certs/ca.pem \
  --ssl-cert=/certs/server-cert.pem --ssl-key=/certs/server-key.pem >/dev/null
wait_for_postgres
wait_for_mysql "$mysql_container"
wait_for_mysql "$mysql_target_container"

docker exec -u 0 "$pg_container" sh -c \
  "openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=cross-pg-ca -keyout /tmp/ca.key -out /tmp/ca.crt >/dev/null 2>&1 && \
   openssl req -newkey rsa:2048 -nodes -subj /CN=localhost -keyout /var/lib/postgresql/data/server.key -out /tmp/server.csr >/dev/null 2>&1 && \
   printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >/tmp/server.ext && \
   openssl x509 -req -in /tmp/server.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -days 2 -extfile /tmp/server.ext -out /var/lib/postgresql/data/server.crt >/dev/null 2>&1 && \
   chown postgres:postgres /var/lib/postgresql/data/server.key /var/lib/postgresql/data/server.crt && chmod 600 /var/lib/postgresql/data/server.key"
docker exec -e PGPASSWORD=admin-secret "$pg_container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET ssl='on'" -c "ALTER SYSTEM SET ssl_cert_file='server.crt'" \
  -c "ALTER SYSTEM SET ssl_key_file='server.key'" >/dev/null
docker restart "$pg_container" >/dev/null
wait_for_postgres

pg_port=$(docker port "$pg_container" 5432/tcp | sed 's/.*://')
mysql_port=$(docker port "$mysql_container" 3306/tcp | sed 's/.*://')
mysql_target_port=$(docker port "$mysql_target_container" 3306/tcp | sed 's/.*://')
docker cp "$pg_container:/tmp/ca.crt" "$test_dir/pg-ca.crt" >/dev/null

docker exec -e PGPASSWORD=admin-secret "$pg_container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "CREATE ROLE cross_pg_source LOGIN PASSWORD 'pgsourcepass'" \
  -c "CREATE ROLE cross_pg_fence LOGIN SUPERUSER PASSWORD 'pgfencepass'" \
  -c "CREATE ROLE cross_pg_target LOGIN PASSWORD 'pgtargetpass'" \
  -c "CREATE DATABASE cross_pg_source OWNER cross_pg_fence" \
  -c "CREATE DATABASE cross_pg_target OWNER cross_pg_target" >/dev/null
docker exec -e PGPASSWORD=admin-secret "$pg_container" psql -U postgres -d cross_pg_source -v ON_ERROR_STOP=1 \
  -c "REVOKE CREATE,TEMP ON DATABASE cross_pg_source FROM PUBLIC" \
  -c "REVOKE CREATE ON SCHEMA public FROM PUBLIC" \
  -c "CREATE TABLE public.items (id bigint PRIMARY KEY)" \
  -c "INSERT INTO public.items VALUES (1),(2),(3)" \
  -c "CREATE TABLE public.bool_values (id bigint PRIMARY KEY, value boolean NOT NULL)" \
  -c "INSERT INTO public.bool_values VALUES (1,true),(2,false),(3,true)" \
  -c "CREATE TABLE public.decimal_values (id bigint PRIMARY KEY, value numeric(10,2) NOT NULL)" \
  -c "INSERT INTO public.decimal_values VALUES (1,1.25),(2,-2.50),(3,99999999.99)" \
  -c "CREATE TABLE public.float_values (id bigint PRIMARY KEY, single_value real NOT NULL, double_value double precision NOT NULL)" \
  -c "INSERT INTO public.float_values VALUES (1,1.25,1.25),(2,-2.5,-2.5),(3,0.0,9007199254740992.0)" \
  -c "CREATE TABLE public.text_values (id bigint PRIMARY KEY, value varchar(64) NOT NULL)" \
  -c "INSERT INTO public.text_values VALUES (1,'one'),(2,'tø'),(3,'三')" \
  -c "CREATE TABLE public.byte_values (id bigint PRIMARY KEY, value bytea NOT NULL)" \
  -c "INSERT INTO public.byte_values VALUES (1,'\\x0001'),(2,'\\xff00'),(3,'\\x1020')" \
  -c "CREATE TABLE public.json_values (id bigint PRIMARY KEY, value jsonb NOT NULL)" \
  -c "INSERT INTO public.json_values VALUES (1,'{\"k\":1}'),(2,'{\"a\":[1,2]}'),(3,'null')" \
  -c "GRANT CONNECT ON DATABASE cross_pg_source TO cross_pg_source" \
  -c "GRANT USAGE ON SCHEMA public TO cross_pg_source" \
  -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO cross_pg_source" >/dev/null

docker exec -i "$mysql_container" mysql -uroot -prootpass <<'SQL'
CREATE DATABASE cross_mysql_source CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'cross_mysql_source'@'%' IDENTIFIED BY 'mysqlsourcepass' REQUIRE X509;
CREATE USER 'cross_mysql_metadata'@'%' IDENTIFIED BY 'mysqlmetapass' REQUIRE X509;
CREATE USER 'cross_mysql_freeze'@'%' IDENTIFIED BY 'mysqlfreezepass' REQUIRE X509;
GRANT SELECT, SHOW VIEW ON cross_mysql_source.* TO 'cross_mysql_source'@'%';
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON *.* TO 'cross_mysql_metadata'@'%';
GRANT SHOW_ROUTINE ON *.* TO 'cross_mysql_metadata'@'%';
GRANT ALL PRIVILEGES ON cross_mysql_source.* TO 'cross_mysql_freeze'@'%';
GRANT PROCESS, BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN ON *.* TO 'cross_mysql_freeze'@'%';
GRANT SELECT ON performance_schema.* TO 'cross_mysql_freeze'@'%';
USE cross_mysql_source;
CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO items VALUES (1),(2),(3);
CREATE TABLE bool_values (id BIGINT NOT NULL PRIMARY KEY, value TINYINT(1) NOT NULL) ENGINE=InnoDB;
INSERT INTO bool_values VALUES (1,1),(2,0),(3,1);
CREATE TABLE decimal_values (id BIGINT NOT NULL PRIMARY KEY, value DECIMAL(10,2) NOT NULL) ENGINE=InnoDB;
INSERT INTO decimal_values VALUES (1,1.25),(2,-2.50),(3,99999999.99);
CREATE TABLE float_values (id BIGINT NOT NULL PRIMARY KEY, single_value FLOAT NOT NULL, double_value DOUBLE NOT NULL) ENGINE=InnoDB;
INSERT INTO float_values VALUES (1,1.25,1.25),(2,-2.5,-2.5),(3,0.0,9007199254740992.0);
CREATE TABLE text_values (id BIGINT NOT NULL PRIMARY KEY, value VARCHAR(64) COLLATE utf8mb4_0900_bin NOT NULL) ENGINE=InnoDB;
INSERT INTO text_values VALUES (1,'one'),(2,'tø'),(3,'三');
CREATE TABLE byte_values (id BIGINT NOT NULL PRIMARY KEY, value BLOB NOT NULL) ENGINE=InnoDB;
INSERT INTO byte_values VALUES (1,X'0001'),(2,X'ff00'),(3,X'1020');
CREATE TABLE json_values (id BIGINT NOT NULL PRIMARY KEY, value JSON NOT NULL) ENGINE=InnoDB;
INSERT INTO json_values VALUES (1,JSON_OBJECT('k',1)),(2,JSON_OBJECT('a',JSON_ARRAY(1,2))),(3,CAST('null' AS JSON));
SQL

docker exec -i "$mysql_target_container" mysql -uroot -prootpass <<'SQL'
CREATE DATABASE cross_mysql_target CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'cross_mysql_target'@'%' IDENTIFIED BY 'mysqltargetpass' REQUIRE X509;
CREATE USER 'cross_mysql_target_metadata'@'%' IDENTIFIED BY 'mysqltargetmetapass' REQUIRE X509;
GRANT ALL PRIVILEGES ON cross_mysql_target.* TO 'cross_mysql_target'@'%';
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON *.* TO 'cross_mysql_target_metadata'@'%';
GRANT SHOW_ROUTINE ON *.* TO 'cross_mysql_target_metadata'@'%';
SQL

write_pg_config() {
  local path=$1 database=$2 user=$3 credential=$4
  cat >"$path" <<EOF
host = "127.0.0.1"
port = $pg_port
database = "$database"
user = "$user"
credential_env = "$credential"
max_batch_rows = 2
max_batch_bytes = 1048576

[tls]
ca_certificate = "$test_dir/pg-ca.crt"
EOF
}

write_mysql_config() {
  local path=$1 database=$2 user=$3 credential=$4
  cat >"$path" <<EOF
host = "127.0.0.1"
port = $mysql_port
database = "$database"
user = "$user"
credential_env = "$credential"
connect_timeout_seconds = 10
max_batch_rows = 2
max_batch_bytes = 1048576
operational_server_administrators = [{ user = "mysql.infoschema", host = "localhost" }, { user = "mysql.session", host = "localhost" }, { user = "mysql.sys", host = "localhost" }, { user = "root", host = "%" }, { user = "root", host = "localhost" }]

[tls]
ca_certificate = "$cert_dir/ca.pem"
client_identity_pkcs12 = "$cert_dir/client.p12"
client_identity_password_env = "SQL_SPLITTER_MYSQL_CLIENT_IDENTITY_PASSWORD"
insecure = false
EOF
}

write_pg_config "$test_dir/pg-source.toml" cross_pg_source cross_pg_source SQL_SPLITTER_CROSS_PG_SOURCE_PASSWORD
write_pg_config "$test_dir/pg-fence.toml" cross_pg_source cross_pg_fence SQL_SPLITTER_CROSS_PG_FENCE_PASSWORD
write_pg_config "$test_dir/pg-target.toml" cross_pg_target cross_pg_target SQL_SPLITTER_CROSS_PG_TARGET_PASSWORD
write_mysql_config "$test_dir/mysql-source.toml" cross_mysql_source cross_mysql_source SQL_SPLITTER_CROSS_MYSQL_SOURCE_PASSWORD
write_mysql_config "$test_dir/mysql-metadata.toml" cross_mysql_source cross_mysql_metadata SQL_SPLITTER_CROSS_MYSQL_METADATA_PASSWORD
write_mysql_config "$test_dir/mysql-freeze.toml" cross_mysql_source cross_mysql_freeze SQL_SPLITTER_CROSS_MYSQL_FREEZE_PASSWORD
write_mysql_config "$test_dir/mysql-target.toml" cross_mysql_target cross_mysql_target SQL_SPLITTER_CROSS_MYSQL_TARGET_PASSWORD
write_mysql_config "$test_dir/mysql-target-metadata.toml" cross_mysql_target cross_mysql_target_metadata SQL_SPLITTER_CROSS_MYSQL_TARGET_METADATA_PASSWORD
sed -i.bak "s/port = $mysql_port/port = $mysql_target_port/" "$test_dir/mysql-target.toml" "$test_dir/mysql-target-metadata.toml"
rm "$test_dir/mysql-target.toml.bak" "$test_dir/mysql-target-metadata.toml.bak"

cat >"$test_dir/pg-to-mysql.json" <<'EOF'
{"schema_version":1,"source_dialect":"postgre_sql","target_dialect":"my_sql","target_defaults":{"dialect":"my_sql","character_set":"utf8mb4","collation":"utf8mb4_0900_bin"},"tables":[{"source":{"namespace":"public","name":"bool_values"},"target":{"namespace":"cross_mysql_target","name":"bool_values_from_pg"}},{"source":{"namespace":"public","name":"byte_values"},"target":{"namespace":"cross_mysql_target","name":"byte_values_from_pg"}},{"source":{"namespace":"public","name":"decimal_values"},"target":{"namespace":"cross_mysql_target","name":"decimal_values_from_pg"}},{"source":{"namespace":"public","name":"float_values"},"target":{"namespace":"cross_mysql_target","name":"float_values_from_pg"}},{"source":{"namespace":"public","name":"items"},"target":{"namespace":"cross_mysql_target","name":"items_from_pg"}},{"source":{"namespace":"public","name":"json_values"},"target":{"namespace":"cross_mysql_target","name":"json_values_from_pg"}},{"source":{"namespace":"public","name":"text_values"},"target":{"namespace":"cross_mysql_target","name":"text_values_from_pg"}}]}
EOF
cat >"$test_dir/mysql-to-pg.json" <<'EOF'
{"schema_version":1,"source_dialect":"my_sql","target_dialect":"postgre_sql","target_defaults":{"dialect":"postgre_sql","text_collation":{"namespace":"pg_catalog","name":"C"}},"tables":[{"source":{"namespace":"cross_mysql_source","name":"bool_values"},"target":{"namespace":"public","name":"bool_values_from_mysql"}},{"source":{"namespace":"cross_mysql_source","name":"byte_values"},"target":{"namespace":"public","name":"byte_values_from_mysql"}},{"source":{"namespace":"cross_mysql_source","name":"decimal_values"},"target":{"namespace":"public","name":"decimal_values_from_mysql"}},{"source":{"namespace":"cross_mysql_source","name":"float_values"},"target":{"namespace":"public","name":"float_values_from_mysql"}},{"source":{"namespace":"cross_mysql_source","name":"items"},"target":{"namespace":"public","name":"items_from_mysql"}},{"source":{"namespace":"cross_mysql_source","name":"json_values"},"target":{"namespace":"public","name":"json_values_from_mysql"}},{"source":{"namespace":"cross_mysql_source","name":"text_values"},"target":{"namespace":"public","name":"text_values_from_mysql"}}]}
EOF
chmod 0600 "$test_dir"/*.json

export SQL_SPLITTER_CROSS_PG_SOURCE_PASSWORD=pgsourcepass
export SQL_SPLITTER_CROSS_PG_FENCE_PASSWORD=pgfencepass
export SQL_SPLITTER_CROSS_PG_TARGET_PASSWORD=pgtargetpass
export SQL_SPLITTER_CROSS_MYSQL_SOURCE_PASSWORD=mysqlsourcepass
export SQL_SPLITTER_CROSS_MYSQL_METADATA_PASSWORD=mysqlmetapass
export SQL_SPLITTER_CROSS_MYSQL_FREEZE_PASSWORD=mysqlfreezepass
export SQL_SPLITTER_CROSS_MYSQL_TARGET_PASSWORD=mysqltargetpass
export SQL_SPLITTER_CROSS_MYSQL_TARGET_METADATA_PASSWORD=mysqltargetmetapass
export SQL_SPLITTER_MYSQL_CLIENT_IDENTITY_PASSWORD=clientpass
export SQL_SPLITTER_CROSS_PG_SOURCE_CONFIG="$test_dir/pg-source.toml"
export SQL_SPLITTER_CROSS_PG_FENCE_ADMIN_CONFIG="$test_dir/pg-fence.toml"
export SQL_SPLITTER_CROSS_PG_TARGET_CONFIG="$test_dir/pg-target.toml"
export SQL_SPLITTER_CROSS_MYSQL_SOURCE_CONFIG="$test_dir/mysql-source.toml"
export SQL_SPLITTER_CROSS_MYSQL_SOURCE_METADATA_CONFIG="$test_dir/mysql-metadata.toml"
export SQL_SPLITTER_CROSS_MYSQL_FREEZE_CONFIG="$test_dir/mysql-freeze.toml"
export SQL_SPLITTER_CROSS_MYSQL_TARGET_CONFIG="$test_dir/mysql-target.toml"
export SQL_SPLITTER_CROSS_MYSQL_TARGET_METADATA_CONFIG="$test_dir/mysql-target-metadata.toml"
export SQL_SPLITTER_CROSS_PG_TO_MYSQL_MAPPING="$test_dir/pg-to-mysql.json"
export SQL_SPLITTER_CROSS_MYSQL_TO_PG_MAPPING="$test_dir/mysql-to-pg.json"

docker exec "$mysql_container" mysql -uroot -prootpass -Nse "SET PERSIST super_read_only=ON"
docker exec "$mysql_container" mysql -uroot -prootpass -Nse \
  "LOCK INSTANCE FOR BACKUP; SELECT SLEEP(900)" >/dev/null 2>&1 &
lock_pid=$!
backup_lock_connection_id=""
for _ in $(seq 1 50); do
  backup_lock_connection_id=$(docker exec "$mysql_container" mysql -uroot -prootpass -Nse \
    "SELECT t.PROCESSLIST_ID FROM performance_schema.metadata_locks ml JOIN performance_schema.threads t ON t.THREAD_ID=ml.OWNER_THREAD_ID WHERE ml.OBJECT_TYPE='BACKUP LOCK' AND ml.LOCK_STATUS='GRANTED' LIMIT 1" 2>/dev/null || true)
  [[ -n "$backup_lock_connection_id" ]] && break
  sleep 0.1
done
[[ -n "$backup_lock_connection_id" ]] || { echo "backup lock was not observed" >&2; exit 1; }
read -r backup_user backup_host <<EOF
$(docker exec "$mysql_container" mysql -uroot -prootpass -Nse "SELECT PROCESSLIST_USER,PROCESSLIST_HOST FROM performance_schema.threads WHERE PROCESSLIST_ID=$backup_lock_connection_id")
EOF
export SQL_SPLITTER_MYSQL_BACKUP_LOCK_CONNECTION_ID="$backup_lock_connection_id"
export SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_USER="$backup_user"
export SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_HOST="$backup_host"

cargo test --no-default-features --features enterprise-migration-spike \
  --test migration_cross_dialect_test -- --ignored --nocapture
