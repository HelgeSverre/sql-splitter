#!/usr/bin/env bash
set -euo pipefail

postgres_version="${1:-17}"
container="sqlspl-migration-pg-${postgres_version}-$$"
test_dir="$(mktemp -d "${TMPDIR:-/tmp}/sqlspl-migration-pg.XXXXXX")"

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$test_dir"
}
trap cleanup EXIT INT TERM

docker run -d --name "$container" \
  -e POSTGRES_PASSWORD=admin-secret \
  -p 127.0.0.1::5432 \
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
   chown postgres:postgres /var/lib/postgresql/data/server.key /var/lib/postgresql/data/server.crt && \
   chmod 600 /var/lib/postgresql/data/server.key"

docker exec -e PGPASSWORD=admin-secret "$container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET ssl = 'on'" \
  -c "ALTER SYSTEM SET ssl_cert_file = 'server.crt'" \
  -c "ALTER SYSTEM SET ssl_key_file = 'server.key'" >/dev/null
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
docker exec -e PGPASSWORD=admin-secret "$container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "REVOKE CREATE,TEMP ON DATABASE postgres FROM PUBLIC" \
  -c "CREATE ROLE migration_reader LOGIN PASSWORD 'reader-secret'" \
  -c "CREATE ROLE migration_mutator LOGIN PASSWORD 'mutator-secret'" \
  -c "GRANT CONNECT ON DATABASE postgres TO migration_reader, migration_mutator" \
  -c "GRANT USAGE ON SCHEMA public TO migration_reader" \
  -c "GRANT ALL ON SCHEMA public TO migration_mutator" \
  -c "CREATE TABLE public.live_snapshot_rows (id bigint PRIMARY KEY, payload text NOT NULL)" \
  -c "GRANT SELECT ON public.live_snapshot_rows TO migration_reader" \
  -c "GRANT ALL ON public.live_snapshot_rows TO migration_mutator" \
  -c "CREATE VIEW public.slow_rows AS SELECT g::bigint AS id, repeat('x',16)::text AS payload FROM generate_series(1,100) AS g CROSS JOIN LATERAL pg_sleep(0.05 + g * 0)" \
  -c "GRANT SELECT ON public.slow_rows TO migration_reader" >/dev/null

cat >"$test_dir/source.toml" <<EOF
host = "localhost"
port = $port
database = "postgres"
user = "migration_reader"
credential_env = "SQL_SPLITTER_PG_READER_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

cat >"$test_dir/mutator.toml" <<EOF
host = "localhost"
port = $port
database = "postgres"
user = "migration_mutator"
credential_env = "SQL_SPLITTER_PG_MUTATOR_PASSWORD"

[tls]
ca_certificate = "$test_dir/ca.crt"
EOF

export SQL_SPLITTER_PG_TEST_SOURCE_CONFIG="$test_dir/source.toml"
export SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG="$test_dir/mutator.toml"
export SQL_SPLITTER_PG_READER_PASSWORD=reader-secret
export SQL_SPLITTER_PG_MUTATOR_PASSWORD=mutator-secret

test_name=live_snapshot_paging_is_stable_during_concurrent_writes
cargo test --no-default-features --features enterprise-migration-spike \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_control_session_cancels_the_active_query
cargo test --no-default-features --features enterprise-migration-spike \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
test_name=live_target_writer_round_trips_binary_protocol_values
cargo test --no-default-features --features enterprise-migration-spike \
  --test migration_postgres_plan_test "$test_name" -- --ignored --exact
