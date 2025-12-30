#!/bin/bash
#
# Benchmark sql-splitter against shell-based competitor tools
#
# Competitors:
#   - kedarvj/mysqldumpsplitter (Bash/awk) - 540+ stars
#   - jasny/mysql_splitdump.sh (csplit)
#
# Usage:
#   ./scripts/benchmark-competitors.sh [sql_file]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPETITORS_DIR="$PROJECT_DIR/scripts/competitors"
OUTPUT_DIR="/tmp/sql-splitter-competitor-bench"
RESULTS_DIR="$PROJECT_DIR/benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

RUST_BIN="$PROJECT_DIR/target/release/sql-splitter"

# Global variables for file size
FILE_SIZE_BYTES=0
FILE_SIZE_MB=0

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BOLD}${CYAN}─── $1 ───${NC}"
    echo ""
}

cleanup() {
    rm -rf "$OUTPUT_DIR" 2>/dev/null || true
}

trap cleanup EXIT

# Get file size (cross-platform)
get_file_size() {
    local file="$1"
    if [[ "$(uname)" == "Darwin" ]]; then
        stat -f%z "$file" 2>/dev/null
    else
        stat -c%s "$file" 2>/dev/null
    fi
}

setup_competitors() {
    print_section "Setting up competitor tools"

    mkdir -p "$COMPETITORS_DIR"

    # 1. kedarvj/mysqldumpsplitter (Bash/awk) - Most popular
    local kedar="$COMPETITORS_DIR/mysqldumpsplitter.sh"
    if [ ! -f "$kedar" ]; then
        echo "Downloading kedarvj/mysqldumpsplitter..."
        curl -sL "https://raw.githubusercontent.com/kedarvj/mysqldumpsplitter/master/mysqldumpsplitter.sh" -o "$kedar"
        chmod +x "$kedar"
        echo -e "${GREEN}✓${NC} Downloaded: $kedar"
    else
        echo -e "${GREEN}✓${NC} Already installed: $kedar"
    fi

    # 2. jasny/mysql_splitdump.sh (csplit)
    local jasny="$COMPETITORS_DIR/mysql_splitdump.sh"
    if [ ! -f "$jasny" ]; then
        echo "Downloading jasny/mysql_splitdump.sh..."
        curl -sL "https://gist.githubusercontent.com/jasny/1608062/raw/mysql_splitdump.sh" -o "$jasny"
        chmod +x "$jasny"
        echo -e "${GREEN}✓${NC} Downloaded: $jasny"
    else
        echo -e "${GREEN}✓${NC} Already installed: $jasny"
    fi

    # 3. mysqldumpsplit-go (Go) - Install if Go is available
    if command -v go &>/dev/null; then
        if ! command -v mysqldumpsplit-go &>/dev/null; then
            echo "Installing mysqldumpsplit-go (Go)..."
            go install github.com/HelgeSverre/mysqldumpsplit@fix/handle-non-interleaved-dumps 2>/dev/null && \
                echo -e "${GREEN}✓${NC} Installed: mysqldumpsplit-go" || \
                echo -e "${YELLOW}⚠${NC} Failed to install mysqldumpsplit-go"
        else
            echo -e "${GREEN}✓${NC} Already installed: mysqldumpsplit-go"
        fi
    fi

    # 4. mysqldumpsplit (Node.js) - Install if npm is available
    if command -v npm &>/dev/null; then
        if ! command -v mysqldumpsplit &>/dev/null; then
            echo "Installing mysqldumpsplit (Node.js)..."
            npm install -g mysqldumpsplit 2>/dev/null && \
                echo -e "${GREEN}✓${NC} Installed: mysqldumpsplit (npm)" || \
                echo -e "${YELLOW}⚠${NC} Failed to install mysqldumpsplit (npm)"
        else
            echo -e "${GREEN}✓${NC} Already installed: mysqldumpsplit (npm)"
        fi
    fi

    # 5. mysql-dump-split (Ruby) - Download script
    local ruby_script="$COMPETITORS_DIR/mysql-dump-split.rb"
    if command -v ruby &>/dev/null; then
        if [ ! -f "$ruby_script" ]; then
            echo "Downloading mysql-dump-split.rb..."
            curl -sL "https://raw.githubusercontent.com/ripienaar/mysql-dump-split/master/split-mysql-dump.rb" -o "$ruby_script"
            chmod +x "$ruby_script"
            echo -e "${GREEN}✓${NC} Downloaded: $ruby_script"
        else
            echo -e "${GREEN}✓${NC} Already installed: $ruby_script"
        fi
    fi
}

build_rust() {
    print_section "Building sql-splitter (Rust)"

    cd "$PROJECT_DIR"
    RUSTFLAGS="-C target-cpu=native" cargo build --release 2>&1 | tail -3

    echo -e "${GREEN}✓${NC} Built: $RUST_BIN"
}

test_tool() {
    local name="$1"
    local cmd="$2"
    local output_dir="$3"
    local sql_file="$4"
    local timeout_secs="${5:-120}"

    echo -n "  $name: "

    rm -rf "$output_dir"
    mkdir -p "$output_dir"

    local start=$(date +%s.%N)

    if timeout "$timeout_secs" bash -c "$cmd" > /dev/null 2>&1; then
        local end=$(date +%s.%N)
        local elapsed=$(echo "$end - $start" | bc)
        local count=$(ls -1 "$output_dir" 2>/dev/null | wc -l | tr -d ' ')

        if [ "$count" -gt 0 ]; then
            echo -e "${GREEN}✓${NC} ($count files, ${elapsed}s)"
            return 0
        else
            echo -e "${YELLOW}⚠${NC} (no output files)"
            return 1
        fi
    else
        echo -e "${RED}✗${NC} (failed/timeout)"
        return 1
    fi
}

run_benchmark() {
    local sql_file="$1"
    local file_name=$(basename "$sql_file")
    local file_size=$(du -h "$sql_file" | cut -f1)

    # Get file size in bytes and MB for throughput calculation
    FILE_SIZE_BYTES=$(get_file_size "$sql_file")
    FILE_SIZE_MB=$(echo "scale=2; $FILE_SIZE_BYTES / 1024 / 1024" | bc)

    print_section "Benchmarking: $file_name ($file_size)"

    mkdir -p "$OUTPUT_DIR" "$RESULTS_DIR"

    # Test which tools work
    echo "Testing tools..."

    local cmds=()

    # 1. sql-splitter (Rust) - our tool
    if test_tool "sql-splitter (Rust)" \
        "$RUST_BIN split '$sql_file' -o '$OUTPUT_DIR/rust'" \
        "$OUTPUT_DIR/rust" \
        "$sql_file"; then
        cmds+=("--command-name" "sql-splitter (Rust)" "$RUST_BIN split '$sql_file' -o '$OUTPUT_DIR/rust'")
    fi

    # 2. mysqldumpsplitter (Bash/awk)
    if test_tool "mysqldumpsplitter (Bash)" \
        "bash '$COMPETITORS_DIR/mysqldumpsplitter.sh' --source '$sql_file' --extract ALLTABLES --output_dir '$OUTPUT_DIR/kedar' --compression none" \
        "$OUTPUT_DIR/kedar" \
        "$sql_file"; then
        cmds+=("--command-name" "mysqldumpsplitter (Bash)" "bash '$COMPETITORS_DIR/mysqldumpsplitter.sh' --source '$sql_file' --extract ALLTABLES --output_dir '$OUTPUT_DIR/kedar' --compression none")
    fi

    # 3. mysql_splitdump (csplit)
    if test_tool "mysql_splitdump (csplit)" \
        "bash '$COMPETITORS_DIR/mysql_splitdump_macos.sh' '$sql_file' '$OUTPUT_DIR/jasny'" \
        "$OUTPUT_DIR/jasny" \
        "$sql_file"; then
        cmds+=("--command-name" "mysql_splitdump (csplit)" "bash '$COMPETITORS_DIR/mysql_splitdump_macos.sh' '$sql_file' '$OUTPUT_DIR/jasny'")
    fi

    # 4. mysqldumpsplit-go (Go)
    if command -v mysqldumpsplit-go &>/dev/null; then
        if test_tool "mysqldumpsplit (Go)" \
            "mysqldumpsplit-go -i '$sql_file' -o '$OUTPUT_DIR/go'" \
            "$OUTPUT_DIR/go" \
            "$sql_file" 30; then
            cmds+=("--command-name" "mysqldumpsplit (Go)" "mysqldumpsplit-go -i '$sql_file' -o '$OUTPUT_DIR/go'")
        fi
    fi

    # 5. mysqldumpsplit (Node.js)
    if command -v mysqldumpsplit &>/dev/null; then
        if test_tool "mysqldumpsplit (Node.js)" \
            "mysqldumpsplit -o '$OUTPUT_DIR/node' '$sql_file'" \
            "$OUTPUT_DIR/node" \
            "$sql_file" 60; then
            cmds+=("--command-name" "mysqldumpsplit (Node.js)" "mysqldumpsplit -o '$OUTPUT_DIR/node' '$sql_file'")
        fi
    fi

    # 6. mysql-dump-split (Ruby)
    local ruby_script="$COMPETITORS_DIR/mysql-dump-split.rb"
    if [ -f "$ruby_script" ] && command -v ruby &>/dev/null; then
        mkdir -p "$OUTPUT_DIR/ruby"
        if test_tool "mysql-dump-split (Ruby)" \
            "cd '$OUTPUT_DIR/ruby' && ruby '$ruby_script' '$sql_file'" \
            "$OUTPUT_DIR/ruby/tables" \
            "$sql_file" 60; then
            cmds+=("--command-name" "mysql-dump-split (Ruby)" "cd '$OUTPUT_DIR/ruby' && ruby '$ruby_script' '$sql_file'")
        fi
    fi

    # Run hyperfine benchmark
    echo ""
    echo "Running benchmark with ${#cmds[@]} tools..."

    local result_file="$RESULTS_DIR/competitors_${file_name%.sql}_$TIMESTAMP"

    if [ ${#cmds[@]} -lt 2 ]; then
        echo -e "${YELLOW}Not enough working tools for comparison${NC}"
        return
    fi

    hyperfine \
        --warmup 1 \
        --runs 3 \
        --prepare "rm -rf '$OUTPUT_DIR'/{rust,kedar,jasny,go,node,ruby}; mkdir -p '$OUTPUT_DIR'/{rust,kedar,jasny,go,node,ruby/tables}" \
        --export-markdown "${result_file}.md" \
        --export-json "${result_file}.json" \
        "${cmds[@]}"

    echo ""
    echo -e "${GREEN}Results saved:${NC} ${result_file}.md"

    # Generate enhanced results table
    generate_enhanced_table "${result_file}.json" "${result_file}_enhanced.md"
}

# Generate enhanced markdown table with memory, CPU, and throughput
generate_enhanced_table() {
    local json_file="$1"
    local output_file="$2"

    if ! command -v jq &>/dev/null; then
        echo -e "${YELLOW}jq not found, skipping enhanced table${NC}"
        return
    fi

    # Force C locale for consistent decimal point handling
    export LC_NUMERIC=C

    echo ""
    echo -e "${BOLD}Enhanced Results (with memory & CPU):${NC}"
    echo ""

    # Print console header
    printf "  ${BOLD}%-25s %10s %10s %8s %10s${NC}\n" \
        "Tool" "Time" "Memory" "CPU" "Throughput"
    printf "  %-25s %10s %10s %8s %10s\n" \
        "-------------------------" "----------" "----------" "--------" "----------"

    # Create enhanced markdown table
    cat > "$output_file" << 'EOF'
| Tool | Mean Time | σ | Peak Memory | CPU Time | Throughput | Relative |
|:-----|----------:|--:|------------:|---------:|-----------:|---------:|
EOF

    # Parse JSON and generate table rows (handle null memory_usage_byte)
    local results=$(jq -r '.results | sort_by(.mean) | .[] | [.command, .mean, .stddev, (if .memory_usage_byte then (.memory_usage_byte | max) else 0 end), .user, .system] | @tsv' "$json_file")

    # Get fastest time for relative calculation
    local fastest=$(jq -r '.results | sort_by(.mean) | .[0].mean' "$json_file")

    while IFS=$'\t' read -r command mean stddev memory_bytes user system; do
        # Calculate metrics with proper formatting
        local mean_ms=$(printf "%.1f" "$(echo "$mean * 1000" | bc -l)")
        local stddev_ms=$(printf "%.1f" "$(echo "$stddev * 1000" | bc -l)")
        local memory_mb=$(printf "%.1f" "$(echo "$memory_bytes / 1024 / 1024" | bc -l)")
        local cpu_time=$(printf "%.2f" "$(echo "$user + $system" | bc -l)")
        local throughput=$(printf "%.0f" "$(echo "$FILE_SIZE_MB / $mean" | bc -l)")
        local relative=$(printf "%.2f" "$(echo "$mean / $fastest" | bc -l)")

        # Format and append row to markdown
        printf "| %s | %s ms | ±%s | %s MB | %ss | %s MB/s | %s |\n" \
            "$command" "$mean_ms" "$stddev_ms" "$memory_mb" "$cpu_time" "$throughput" "$relative" >> "$output_file"

        # Also print to console
        printf "  %-25s %8s ms %8s MB %6ss %8s MB/s\n" \
            "$command" "$mean_ms" "$memory_mb" "$cpu_time" "$throughput"
    done <<< "$results"

    echo ""
    echo -e "${GREEN}Enhanced table saved:${NC} $output_file"
}

generate_summary() {
    print_section "Summary"

    local summary="$RESULTS_DIR/COMPETITORS_BENCHMARK_$TIMESTAMP.md"

    # Get CPU info (cross-platform)
    local cpu_info
    if [[ "$(uname)" == "Darwin" ]]; then
        cpu_info=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "Unknown")
    else
        cpu_info=$(grep "model name" /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || echo "Unknown")
    fi

    cat > "$summary" << EOF
# SQL Splitter vs Competitors Benchmark

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**System:** $(uname -s) $(uname -m)
**CPU:** $cpu_info
**File Size:** ${FILE_SIZE_MB} MB

## Tools Compared

| Tool | Language | Stars | Source |
|------|----------|-------|--------|
| sql-splitter | Rust | - | This project |
| mysqldumpsplitter | Bash/awk | 540+ | github.com/kedarvj/mysqldumpsplitter |
| mysql_splitdump.sh | Bash/csplit | 93 | gist.github.com/jasny/1608062 |

## Key Differences

- **sql-splitter**: Parses actual SQL statements, works with any SQL format (MySQL, PostgreSQL, SQLite, MSSQL)
- **mysqldumpsplitter**: Looks for mysqldump comment markers, only works with mysqldump format
- **mysql_splitdump.sh**: Uses csplit on "Table structure" comments, only works with mysqldump format

## Results

### Timing (hyperfine)

EOF

    # Append hyperfine markdown results
    for md in "$RESULTS_DIR"/competitors_*_$TIMESTAMP.md; do
        if [ -f "$md" ] && [[ ! "$md" =~ _enhanced\.md$ ]] && [ "$md" != "$summary" ]; then
            echo "#### $(basename "$md" "_$TIMESTAMP.md" | sed 's/competitors_//' | tr '_' ' ')" >> "$summary"
            echo "" >> "$summary"
            cat "$md" >> "$summary"
            echo "" >> "$summary"
        fi
    done

    # Append enhanced results if available
    echo "### Enhanced Results (with Memory & CPU)" >> "$summary"
    echo "" >> "$summary"

    for enhanced in "$RESULTS_DIR"/competitors_*_${TIMESTAMP}_enhanced.md; do
        if [ -f "$enhanced" ]; then
            cat "$enhanced" >> "$summary"
            echo "" >> "$summary"
        fi
    done

    # Add notes about metrics
    cat >> "$summary" << 'EOF'

### Metrics Explained

- **Mean Time**: Average execution time over 3 runs (hyperfine)
- **σ (stddev)**: Standard deviation in timing
- **Peak Memory**: Maximum resident set size during execution
- **CPU Time**: User + System CPU time
- **Throughput**: File size / Mean time (MB/s)
- **Relative**: How many times slower than the fastest tool

EOF

    echo -e "${GREEN}Summary saved:${NC} $summary"
    echo ""
    cat "$summary"
}

main() {
    print_header "SQL Splitter vs Competitors Benchmark"

    # Check dependencies
    if ! command -v hyperfine &> /dev/null; then
        echo -e "${RED}hyperfine not found. Install with: brew install hyperfine${NC}"
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        echo -e "${YELLOW}Warning: jq not found. Enhanced tables will be skipped.${NC}"
        echo -e "${YELLOW}Install with: brew install jq${NC}"
    fi

    setup_competitors
    build_rust

    mkdir -p "$OUTPUT_DIR"

    if [ $# -gt 0 ]; then
        # Use provided file
        run_benchmark "$1"
    else
        # Generate test file
        local test_file="$OUTPUT_DIR/benchmark_100mb.sql"

        print_section "Generating test file"
        python3 "$SCRIPT_DIR/generate-test-dump.py" 100 -o "$test_file"

        run_benchmark "$test_file"
    fi

    generate_summary

    print_header "Benchmark Complete"
}

main "$@"
