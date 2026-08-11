#!/usr/bin/env bash
set -euo pipefail

postgres_version="${1:-17}"
only_test="${2:-}"
container="sqlspl-migration-pg-${postgres_version}-$$"
test_dir="$(mktemp -d "${TMPDIR:-/tmp}/sqlspl-migration-pg.XXXXXX")"
port="$(ruby -rsocket -e 'server = TCPServer.new("127.0.0.1", 0); puts server.addr[1]; server.close')"

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$test_dir"
}
trap cleanup EXIT INT TERM

docker run -d --name "$container" \
  -e POSTGRES_PASSWORD=admin-secret \
  -p "127.0.0.1:${port}:5432" \
  "postgres:${postgres_version}" >/dev/null

for _ in {1..30}; do
  if docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$container" pg_isready -U postgres >/dev/null

docker exec -u 0 "$container" sh -c \
  "openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=sqlspl-test-ca -keyout /tmp/ca.key -out /tmp/ca.crt >/dev/null 2>&1 && \
   openssl req -newkey rsa:2048 -nodes -subj /CN=localhost -keyout /var/lib/postgresql/data/server.key -out /tmp/server.csr >/dev/null 2>&1 && \
   printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >/tmp/server.ext && \
   openssl x509 -req -in /tmp/server.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -days 2 -extfile /tmp/server.ext -out /var/lib/postgresql/data/server.crt >/dev/null 2>&1 && \
   openssl req -newkey rsa:2048 -nodes -subj /CN=migration_mtls_reader -keyout /tmp/client.key -out /tmp/client.csr >/dev/null 2>&1 && \
   printf 'extendedKeyUsage=clientAuth\n' >/tmp/client.ext && \
   openssl x509 -req -in /tmp/client.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -days 2 -extfile /tmp/client.ext -out /tmp/client.crt >/dev/null 2>&1 && \
   openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=sqlspl-rogue-ca -keyout /tmp/rogue-ca.key -out /tmp/rogue-ca.crt >/dev/null 2>&1 && \
   chown postgres:postgres /var/lib/postgresql/data/server.key /var/lib/postgresql/data/server.crt && \
   chmod 600 /var/lib/postgresql/data/server.key"

docker exec -e PGPASSWORD=admin-secret "$container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET ssl = 'on'" \
  -c "ALTER SYSTEM SET ssl_cert_file = 'server.crt'" \
  -c "ALTER SYSTEM SET ssl_key_file = 'server.key'" \
  -c "ALTER SYSTEM SET ssl_ca_file = '/tmp/ca.crt'" >/dev/null
docker exec -u 0 "$container" sh -c \
  "sed -i '1i hostssl migration_mtls_source migration_mtls_reader 0.0.0.0/0 cert clientcert=verify-full' /var/lib/postgresql/data/pg_hba.conf"
docker restart "$container" >/dev/null
for _ in {1..30}; do
  if docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$container" pg_isready -U postgres >/dev/null

port="$(docker port "$container" 5432/tcp | sed 's/.*://')"
docker cp "$container:/tmp/ca.crt" "$test_dir/ca.crt" >/dev/null
docker cp "$container:/tmp/client.crt" "$test_dir/client.crt" >/dev/null
docker cp "$container:/tmp/client.key" "$test_dir/client.key" >/dev/null
docker cp "$container:/tmp/rogue-ca.crt" "$test_dir/rogue-ca.crt" >/dev/null
chmod 600 "$test_dir/client.key"
docker exec -e PGPASSWORD=admin-secret "$container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "REVOKE CREATE,TEMP ON DATABASE postgres FROM PUBLIC" \
  -c "CREATE ROLE migration_reader LOGIN PASSWORD 'reader-secret'" \
  -c "CREATE ROLE migration_mutator LOGIN PASSWORD 'mutator-secret'" \
  -c "CREATE ROLE migration_target_owner LOGIN PASSWORD 'target-secret'" \
  -c "CREATE ROLE migration_run_target_owner LOGIN PASSWORD 'run-target-secret'" \
  -c "CREATE ROLE migration_fence_admin LOGIN SUPERUSER PASSWORD 'fence-admin-secret'" \
  -c "CREATE ROLE migration_fence_target_owner LOGIN PASSWORD 'fence-target-secret'" \
  -c "CREATE ROLE migration_mtls_reader LOGIN" \
  -c "CREATE DATABASE migration_target OWNER migration_target_owner" \
  -c "CREATE DATABASE migration_run_source OWNER migration_mutator" \
  -c "CREATE DATABASE migration_run_target OWNER migration_run_target_owner" \
  -c "CREATE DATABASE migration_fence_target OWNER migration_fence_target_owner" \
  -c "CREATE DATABASE migration_mtls_source OWNER migration_mutator" \
  -c "GRANT CONNECT ON DATABASE postgres TO migration_reader, migration_mutator" \
  -c "GRANT USAGE ON SCHEMA public TO migration_reader" \
  -c "GRANT ALL ON SCHEMA public TO migration_mutator" \
  -c "CREATE TABLE public.live_snapshot_rows (id bigint PRIMARY KEY, payload text NOT NULL)" \
  -c "GRANT SELECT ON public.live_snapshot_rows TO migration_reader" \
  -c "GRANT ALL ON public.live_snapshot_rows TO migration_mutator" \
  -c "CREATE TABLE public.slow_rows (id bigint PRIMARY KEY, payload text NOT NULL)" \
  -c "INSERT INTO public.slow_rows SELECT g, repeat('x',16) FROM generate_series(1,1000000) AS g" \
  -c "GRANT SELECT ON public.slow_rows TO migration_reader" >/dev/null

docker exec -e PGPASSWORD=admin-secret "$container" psql -U postgres -d migration_mtls_source -v ON_ERROR_STOP=1 \
  -c "REVOKE CREATE,TEMP ON DATABASE migration_mtls_source FROM PUBLIC" \
  -c "REVOKE CREATE ON SCHEMA public FROM PUBLIC" \
  -c "GRANT CONNECT ON DATABASE migration_mtls_source TO migration_mtls_reader" \
  -c "GRANT USAGE ON SCHEMA public TO migration_mtls_reader" \
  -c "CREATE TABLE public.mtls_rows (id bigint PRIMARY KEY)" \
  -c "INSERT INTO public.mtls_rows VALUES (1)" \
  -c "GRANT SELECT ON public.mtls_rows TO migration_mtls_reader" >/dev/null

docker exec -e PGPASSWORD=admin-secret "$container" psql -U postgres -d migration_run_source -v ON_ERROR_STOP=1 \
  -c "REVOKE CREATE,TEMP ON DATABASE migration_run_source FROM PUBLIC" \
  -c "REVOKE CREATE ON SCHEMA public FROM PUBLIC" \
  -c "GRANT CONNECT ON DATABASE migration_run_source TO migration_reader" \
  -c "GRANT USAGE ON SCHEMA public TO migration_reader" \
  -c "CREATE TABLE public.accounts (id bigint PRIMARY KEY, name text NOT NULL)" \
  -c "INSERT INTO public.accounts VALUES (1, 'one'), (2, 'two'), (3, 'three')" \
  -c "GRANT SELECT ON public.accounts TO migration_reader" >/dev/null

cat >"$test_dir/source.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "postgres"
user = "migration_reader"
credential_env = "SQL_SPLITTER_PG_READER_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/mutator.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "postgres"
user = "migration_mutator"
credential_env = "SQL_SPLITTER_PG_MUTATOR_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/target.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_target"
user = "migration_target_owner"
credential_env = "SQL_SPLITTER_PG_TARGET_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/run-source.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_run_source"
user = "migration_reader"
credential_env = "SQL_SPLITTER_PG_READER_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/run-target.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_run_target"
user = "migration_run_target_owner"
credential_env = "SQL_SPLITTER_PG_RUN_TARGET_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/fence-admin.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_run_source"
user = "migration_fence_admin"
credential_env = "SQL_SPLITTER_PG_FENCE_ADMIN_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/fence-target.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_fence_target"
user = "migration_fence_target_owner"
credential_env = "SQL_SPLITTER_PG_FENCE_TARGET_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/mtls-source.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_mtls_source"
user = "migration_mtls_reader"
credential_env = "SQL_SPLITTER_PG_MTLS_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
client_certificate = "$test_dir/client.crt"
client_private_key = "$test_dir/client.key"
EOF

export SQL_SPLITTER_PG_TEST_SOURCE_CONFIG="$test_dir/source.toml"
export SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG="$test_dir/mutator.toml"
export SQL_SPLITTER_PG_TEST_TARGET_CONFIG="$test_dir/target.toml"
export SQL_SPLITTER_PG_RUN_SOURCE_CONFIG="$test_dir/run-source.toml"
export SQL_SPLITTER_PG_RUN_TARGET_CONFIG="$test_dir/run-target.toml"
export SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG="$test_dir/fence-admin.toml"
export SQL_SPLITTER_PG_FENCE_TARGET_CONFIG="$test_dir/fence-target.toml"
export SQL_SPLITTER_PG_MTLS_CONFIG="$test_dir/mtls-source.toml"
export SQL_SPLITTER_PG_ROGUE_CA="$test_dir/rogue-ca.crt"
export SQL_SPLITTER_PG_FENCE_ARTIFACT="$test_dir/fence.json"
export SQL_SPLITTER_PG_FENCE_PLAN="$test_dir/fence-plan.json"
export SQL_SPLITTER_PG_FENCE_STATE="$test_dir/fence-state.json"
export SQL_SPLITTER_PG_READER_PASSWORD=reader-secret
export SQL_SPLITTER_PG_MUTATOR_PASSWORD=mutator-secret
export SQL_SPLITTER_PG_TARGET_PASSWORD=target-secret
export SQL_SPLITTER_PG_RUN_TARGET_PASSWORD=run-target-secret
export SQL_SPLITTER_PG_FENCE_ADMIN_PASSWORD=fence-admin-secret
export SQL_SPLITTER_PG_FENCE_TARGET_PASSWORD=fence-target-secret
export SQL_SPLITTER_PG_MTLS_PASSWORD=unused

if [[ -n "$only_test" ]]; then
  cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
    --test migration_postgres_plan_test "$only_test" -- --ignored --exact
  exit 0
fi

test_name=live_mutual_tls_requires_valid_client_identity
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_snapshot_paging_is_stable_during_concurrent_writes
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_key_pagination_matrix_is_exact_and_bounded
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_control_session_cancels_the_active_query
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_pre_data_ddl_is_create_only_and_rechecks_emptiness
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_target_writer_round_trips_binary_protocol_values
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_reviewed_plan_executes_and_strictly_finalizes
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_write_fence_install_is_durable
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
docker restart "$container" >/dev/null
sleep 2
port="$(docker port "$container" 5432/tcp | sed 's/.*://')"
cat >"$test_dir/fence-admin.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_run_source"
user = "migration_fence_admin"
credential_env = "SQL_SPLITTER_PG_FENCE_ADMIN_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF
cat >"$test_dir/fence-target.toml" <<EOF
host = "127.0.0.1"
port = $port
database = "migration_fence_target"
user = "migration_fence_target_owner"
credential_env = "SQL_SPLITTER_PG_FENCE_TARGET_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF
for _ in {1..30}; do
  if docker exec "$container" pg_isready -U postgres >/dev/null 2>&1 && \
    nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$container" pg_isready -U postgres >/dev/null
test_name=live_write_fence_attests_after_restart_blocks_writes_and_releases
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_write_fence_rearms_without_erasing_prior_history
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_write_fence_recovery_boundary_matrix
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_network_commit_response_loss_matrix
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_standalone_unique_index_is_created_before_copy_and_resumed
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_ordinary_indexes_are_created_after_copy_and_reconciled
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_foreign_keys_are_checked_added_and_database_validated
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_foreign_key_conflict_requires_manual_reconciliation
cargo test --no-default-features --features enterprise-migration-spike,migration-fault-injection \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
