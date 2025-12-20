#!/bin/bash
# Benchmark script for comparing Rust vs Go implementations
# Usage: ./scripts/benchmark.sh [sql_file]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
GO_PROJECT_DIR="$(dirname "$PROJECT_DIR")/sql-splitter"

RUST_BIN="$PROJECT_DIR/target/release/sql-splitter"
GO_BIN="$GO_PROJECT_DIR/sql-splitter"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== SQL Splitter Benchmark ===${NC}"
echo ""

# Check if binaries exist
if [ ! -f "$RUST_BIN" ]; then
    echo -e "${YELLOW}Building Rust binary with native optimizations...${NC}"
    cd "$PROJECT_DIR"
    RUSTFLAGS="-C target-cpu=native" cargo build --release
fi

if [ ! -f "$GO_BIN" ]; then
    echo -e "${YELLOW}Building Go binary...${NC}"
    cd "$GO_PROJECT_DIR"
    go build -o sql-splitter .
fi

# Use provided file or create test data
if [ -n "$1" ] && [ -f "$1" ]; then
    SQL_FILE="$1"
    FILE_SIZE=$(ls -lh "$SQL_FILE" | awk '{print $5}')
    echo -e "Testing with: ${GREEN}$SQL_FILE${NC} (${FILE_SIZE})"
else
    echo -e "${YELLOW}No SQL file provided. Generating test data...${NC}"
    SQL_FILE="/tmp/benchmark_test.sql"
    
    python3 << 'PYTHON'
import random
tables = ['users', 'posts', 'comments', 'orders', 'products', 'sessions', 'logs', 'events']
with open('/tmp/benchmark_test.sql', 'w') as f:
    for table in tables:
        f.write(f"CREATE TABLE {table} (id INT, data VARCHAR(255));\n")
    for i in range(200000):
        table = random.choice(tables)
        body = "Lorem ipsum dolor sit amet " * 3
        f.write(f"INSERT INTO {table} VALUES ({i}, '{body}');\n")
PYTHON
    
    FILE_SIZE=$(ls -lh "$SQL_FILE" | awk '{print $5}')
    echo -e "Generated test file: ${GREEN}$SQL_FILE${NC} (${FILE_SIZE})"
fi

echo ""
echo -e "${BLUE}--- Running Benchmarks ---${NC}"
echo ""

# Clean up
rm -rf /tmp/rs-bench /tmp/go-bench

# Rust benchmark
echo -e "${GREEN}Rust:${NC}"
RUST_START=$(python3 -c "import time; print(time.time())")
"$RUST_BIN" split "$SQL_FILE" -o /tmp/rs-bench 2>&1 | grep -E "(Throughput|Elapsed|Statements|Tables)"
RUST_END=$(python3 -c "import time; print(time.time())")
RUST_TIME=$(python3 -c "print(f'{$RUST_END - $RUST_START:.3f}s')")

echo ""

# Go benchmark
echo -e "${GREEN}Go:${NC}"
GO_START=$(python3 -c "import time; print(time.time())")
"$GO_BIN" split "$SQL_FILE" -o /tmp/go-bench 2>&1 | grep -E "(Throughput|Elapsed|Statements|Tables)"
GO_END=$(python3 -c "import time; print(time.time())")
GO_TIME=$(python3 -c "print(f'{$GO_END - $GO_START:.3f}s')")

echo ""

# Verify outputs match
if diff -rq /tmp/rs-bench /tmp/go-bench > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Outputs match!${NC}"
else
    echo -e "${RED}✗ Outputs differ!${NC}"
    diff -r /tmp/rs-bench /tmp/go-bench | head -20
fi

echo ""
echo -e "${BLUE}--- Summary ---${NC}"
echo -e "Rust wall time: ${GREEN}$RUST_TIME${NC}"
echo -e "Go wall time:   ${GREEN}$GO_TIME${NC}"

# Cleanup
rm -rf /tmp/rs-bench /tmp/go-bench
