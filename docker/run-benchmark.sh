#!/bin/bash
#
# Run benchmarks in Docker container
#
# Usage:
#   ./docker/run-benchmark.sh                    # Generate 50MB test and benchmark
#   ./docker/run-benchmark.sh --generate 100    # Generate 100MB test
#   ./docker/run-benchmark.sh /data/dump.sql    # Use mounted file
#   ./docker/run-benchmark.sh --list            # List available tools
#   ./docker/run-benchmark.sh --test /data/generated/mysql_mysqldump.sql
#

set -e

cd "$(dirname "$0")/.."

echo "Building benchmark container..."
docker compose -f docker/docker-compose.benchmark.yml build --quiet

echo "Running benchmark..."
docker compose -f docker/docker-compose.benchmark.yml run --rm benchmark "$@"
