#!/bin/bash
#
# SQL Splitter Benchmark
#
# Benchmarks sql-splitter on synthetic and real dumps
# Uses hyperfine for accurate measurements
#
# Usage:
#   ./scripts/benchmark.sh                    # Run with synthetic files
#   ./scripts/benchmark.sh path/to/dump.sql   # Use custom SQL file
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="/tmp/sql-splitter-bench"
RESULTS_DIR="$PROJECT_DIR/benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Synthetic test sizes (MB)
SYNTHETIC_SIZES=(10 100 1000)

# Benchmark settings
WARMUP=1
RUNS=5

RUST_BIN="$PROJECT_DIR/target/release/sql-splitter"

cleanup() {
    rm -rf "$OUTPUT_DIR" 2>/dev/null || true
}

trap cleanup EXIT

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

setup() {
    echo -e "${CYAN}Building sql-splitter with native optimizations...${NC}"
    cd "$PROJECT_DIR"
    RUSTFLAGS="-C target-cpu=native" cargo build --release 2>&1 | tail -3
    
    mkdir -p "$OUTPUT_DIR" "$RESULTS_DIR"
    
    echo -e "${GREEN}Setup complete${NC}"
    echo ""
}

run_benchmark() {
    local sql_file="$1"
    local name="$2"
    
    local file_size=$(du -h "$sql_file" | cut -f1)
    echo -e "${CYAN}Benchmarking: $name ($file_size)${NC}"
    
    local result_file="$RESULTS_DIR/${name}_$TIMESTAMP"
    
    hyperfine \
        --warmup "$WARMUP" \
        --runs "$RUNS" \
        --prepare "rm -rf $OUTPUT_DIR/output" \
        --export-markdown "${result_file}.md" \
        --export-json "${result_file}.json" \
        "$RUST_BIN split '$sql_file' -o '$OUTPUT_DIR/output'" 2>&1
    
    # Calculate throughput
    local bytes=$(stat -f%z "$sql_file" 2>/dev/null || stat -c%s "$sql_file")
    local mean_time=$(jq -r '.results[0].mean' "${result_file}.json")
    local throughput=$(echo "scale=2; $bytes / $mean_time / 1024 / 1024" | bc)
    
    echo ""
    echo -e "  ${GREEN}Throughput: ${throughput} MB/s${NC}"
    echo "  Tables extracted: $(ls -1 "$OUTPUT_DIR/output" 2>/dev/null | wc -l | tr -d ' ')"
    echo ""
    
    # Cleanup output
    rm -rf "$OUTPUT_DIR/output"
    
    # Store throughput for summary
    echo "$name|$file_size|$mean_time|$throughput" >> "$OUTPUT_DIR/results.txt"
}

run_synthetic_benchmarks() {
    print_header "Synthetic Benchmarks (mysqldump format)"
    
    for size in "${SYNTHETIC_SIZES[@]}"; do
        local test_file="$OUTPUT_DIR/benchmark_${size}mb.sql"
        
        echo -e "${YELLOW}Generating ${size}MB test file...${NC}"
        python3 "$SCRIPT_DIR/generate-test-dump.py" "$size" -o "$test_file" -q
        
        run_benchmark "$test_file" "synthetic_${size}MB"
        
        # Delete test file immediately to save disk space
        rm -f "$test_file"
    done
}

run_custom_benchmark() {
    local sql_file="$1"
    print_header "Custom File Benchmark"
    
    if [ ! -f "$sql_file" ]; then
        echo -e "${YELLOW}File not found: $sql_file${NC}"
        exit 1
    fi
    
    local name=$(basename "$sql_file" .sql)
    run_benchmark "$sql_file" "$name"
}

generate_summary() {
    print_header "Results Summary"
    
    local summary="$RESULTS_DIR/BENCHMARK_$TIMESTAMP.md"
    
    cat > "$summary" << EOF
# SQL Splitter Benchmark Results

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**CPU:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep "model name" /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || echo "Unknown")
**Version:** $($RUST_BIN --version 2>/dev/null || echo "unknown")

## Performance Summary

| File | Size | Time (mean) | Throughput |
|------|------|-------------|------------|
EOF

    # Read results
    if [ -f "$OUTPUT_DIR/results.txt" ]; then
        while IFS='|' read -r name size time throughput; do
            printf "| %s | %s | %.3fs | %s MB/s |\n" "$name" "$size" "$time" "$throughput" >> "$summary"
        done < "$OUTPUT_DIR/results.txt"
    fi

    cat >> "$summary" << EOF

## Detailed Results

EOF

    # Append individual results
    for md in "$RESULTS_DIR"/*_$TIMESTAMP.md; do
        if [ -f "$md" ] && [ "$md" != "$summary" ]; then
            echo "### $(basename "$md" "_$TIMESTAMP.md" | tr '_' ' ')" >> "$summary"
            echo "" >> "$summary"
            cat "$md" >> "$summary"
            echo "" >> "$summary"
        fi
    done

    echo -e "${GREEN}Summary: $summary${NC}"
    echo ""
    cat "$summary"
}

main() {
    print_header "SQL Splitter Benchmark"
    
    # Check dependencies
    if ! command -v hyperfine &> /dev/null; then
        echo -e "${YELLOW}hyperfine not found. Install with: brew install hyperfine${NC}"
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        echo -e "${YELLOW}jq not found. Install with: brew install jq${NC}"
        exit 1
    fi
    
    setup
    
    if [ $# -gt 0 ]; then
        run_custom_benchmark "$1"
    else
        run_synthetic_benchmarks
    fi
    
    generate_summary
    
    print_header "Benchmark Complete"
}

main "$@"
