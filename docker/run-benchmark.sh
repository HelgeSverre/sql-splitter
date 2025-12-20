#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

show_help() {
    echo "Docker Benchmark Runner for sql-splitter"
    echo ""
    echo "Usage: ./docker/run-benchmark.sh [OPTIONS] [-- BENCHMARK_ARGS]"
    echo ""
    echo "Options:"
    echo "  --build          Force rebuild the container"
    echo "  --file FILE      Mount and use a specific SQL file"
    echo "  --shell          Start an interactive shell instead"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./docker/run-benchmark.sh --build"
    echo "  ./docker/run-benchmark.sh -- --generate --rows 500000"
    echo "  ./docker/run-benchmark.sh --file /path/to/dump.sql"
    echo "  ./docker/run-benchmark.sh --file dump.sql -- --runs 10 --export /results/bench.md"
    echo "  ./docker/run-benchmark.sh --shell"
}

BUILD=false
SQL_FILE=""
SHELL_MODE=false
BENCHMARK_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            BUILD=true
            shift
            ;;
        --file)
            SQL_FILE="$2"
            shift 2
            ;;
        --shell)
            SHELL_MODE=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        --)
            shift
            BENCHMARK_ARGS=("$@")
            break
            ;;
        *)
            BENCHMARK_ARGS+=("$1")
            shift
            ;;
    esac
done

COMPOSE_FILE="docker/docker-compose.benchmark.yml"

# Build if requested or image doesn't exist
if [ "$BUILD" = true ]; then
    echo "Building benchmark container..."
    docker compose -f "$COMPOSE_FILE" build
fi

# Prepare volume mounts and args
DOCKER_ARGS=()
RUN_ARGS=()

if [ -n "$SQL_FILE" ]; then
    ABSOLUTE_PATH="$(cd "$(dirname "$SQL_FILE")" && pwd)/$(basename "$SQL_FILE")"
    if [ ! -f "$ABSOLUTE_PATH" ]; then
        echo "Error: File not found: $SQL_FILE"
        exit 1
    fi
    DOCKER_ARGS+=(-v "$ABSOLUTE_PATH:/data/input.sql:ro")
    RUN_ARGS+=("/data/input.sql")
fi

RUN_ARGS+=("${BENCHMARK_ARGS[@]}")

if [ "$SHELL_MODE" = true ]; then
    echo "Starting interactive shell..."
    docker compose -f "$COMPOSE_FILE" run --rm "${DOCKER_ARGS[@]}" --entrypoint /bin/bash benchmark
else
    docker compose -f "$COMPOSE_FILE" run --rm "${DOCKER_ARGS[@]}" benchmark "${RUN_ARGS[@]}"
fi
