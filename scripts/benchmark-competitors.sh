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

setup_competitors() {
    print_section "Setting up competitor tools"
    
    mkdir -p "$COMPETITORS_DIR"
    
    # Download kedarvj/mysqldumpsplitter if not present
    local kedar="$COMPETITORS_DIR/mysqldumpsplitter.sh"
    if [ ! -f "$kedar" ]; then
        echo "Downloading kedarvj/mysqldumpsplitter..."
        curl -sL "https://raw.githubusercontent.com/kedarvj/mysqldumpsplitter/master/mysqldumpsplitter.sh" -o "$kedar"
        chmod +x "$kedar"
        echo -e "${GREEN}✓${NC} Downloaded: $kedar"
    else
        echo -e "${GREEN}✓${NC} Already installed: $kedar"
    fi
    
    # Download jasny/mysql_splitdump.sh if not present
    local jasny="$COMPETITORS_DIR/mysql_splitdump.sh"
    if [ ! -f "$jasny" ]; then
        echo "Downloading jasny/mysql_splitdump.sh..."
        curl -sL "https://gist.githubusercontent.com/jasny/1608062/raw/mysql_splitdump.sh" -o "$jasny"
        chmod +x "$jasny"
        echo -e "${GREEN}✓${NC} Downloaded: $jasny"
    else
        echo -e "${GREEN}✓${NC} Already installed: $jasny"
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
    
    print_section "Benchmarking: $file_name ($file_size)"
    
    mkdir -p "$OUTPUT_DIR" "$RESULTS_DIR"
    
    # Test which tools work
    echo "Testing tools..."
    
    local rust_works=false
    local kedar_works=false
    local jasny_works=false
    
    test_tool "sql-splitter (Rust)" \
        "$RUST_BIN split '$sql_file' -o '$OUTPUT_DIR/rust'" \
        "$OUTPUT_DIR/rust" \
        "$sql_file" && rust_works=true
    
    test_tool "mysqldumpsplitter (Bash)" \
        "bash '$COMPETITORS_DIR/mysqldumpsplitter.sh' --source '$sql_file' --extract ALLTABLES --output_dir '$OUTPUT_DIR/kedar' --compression none" \
        "$OUTPUT_DIR/kedar" \
        "$sql_file" && kedar_works=true
    
    # jasny's script outputs to current directory (use macOS-compatible version)
    test_tool "mysql_splitdump (csplit)" \
        "bash '$COMPETITORS_DIR/mysql_splitdump_macos.sh' '$sql_file' '$OUTPUT_DIR/jasny'" \
        "$OUTPUT_DIR/jasny" \
        "$sql_file" && jasny_works=true
    
    # Run hyperfine benchmark
    echo ""
    echo "Running benchmark..."
    
    local result_file="$RESULTS_DIR/competitors_${file_name%.sql}_$TIMESTAMP"
    local cmds=()
    
    if [ "$rust_works" = true ]; then
        cmds+=("--command-name" "sql-splitter (Rust)" "$RUST_BIN split '$sql_file' -o '$OUTPUT_DIR/rust'")
    fi
    
    if [ "$kedar_works" = true ]; then
        cmds+=("--command-name" "mysqldumpsplitter (Bash)" "bash '$COMPETITORS_DIR/mysqldumpsplitter.sh' --source '$sql_file' --extract ALLTABLES --output_dir '$OUTPUT_DIR/kedar' --compression none")
    fi
    
    if [ "$jasny_works" = true ]; then
        cmds+=("--command-name" "mysql_splitdump (csplit)" "bash '$COMPETITORS_DIR/mysql_splitdump_macos.sh' '$sql_file' '$OUTPUT_DIR/jasny'")
    fi
    
    if [ ${#cmds[@]} -lt 2 ]; then
        echo -e "${YELLOW}Not enough working tools for comparison${NC}"
        return
    fi
    
    hyperfine \
        --warmup 1 \
        --runs 3 \
        --prepare "rm -rf '$OUTPUT_DIR/rust' '$OUTPUT_DIR/kedar' '$OUTPUT_DIR/jasny'; mkdir -p '$OUTPUT_DIR/rust' '$OUTPUT_DIR/kedar' '$OUTPUT_DIR/jasny'" \
        --export-markdown "${result_file}.md" \
        --export-json "${result_file}.json" \
        "${cmds[@]}"
    
    echo ""
    echo -e "${GREEN}Results saved:${NC} ${result_file}.md"
}

generate_summary() {
    print_section "Summary"
    
    local summary="$RESULTS_DIR/COMPETITORS_BENCHMARK_$TIMESTAMP.md"
    
    cat > "$summary" << EOF
# SQL Splitter vs Competitors Benchmark

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**System:** $(uname -s) $(uname -m)
**CPU:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || cat /proc/cpuinfo 2>/dev/null | grep "model name" | head -1 | cut -d: -f2 | xargs || echo "Unknown")

## Tools Compared

| Tool | Language | Stars | Source |
|------|----------|-------|--------|
| sql-splitter | Rust | - | This project |
| mysqldumpsplitter | Bash/awk | 540+ | github.com/kedarvj/mysqldumpsplitter |
| mysql_splitdump.sh | Bash/csplit | 93 | gist.github.com/jasny/1608062 |

## Key Differences

- **sql-splitter**: Parses actual SQL statements, works with any SQL format
- **mysqldumpsplitter**: Looks for mysqldump comment markers, only works with mysqldump format
- **mysql_splitdump.sh**: Uses csplit on "Table structure" comments, only works with mysqldump format

## Results

EOF

    # Append individual results
    for md in "$RESULTS_DIR"/competitors_*_$TIMESTAMP.md; do
        if [ -f "$md" ] && [ "$md" != "$summary" ]; then
            echo "### $(basename "$md" "_$TIMESTAMP.md" | sed 's/competitors_//' | tr '_' ' ')" >> "$summary"
            echo "" >> "$summary"
            cat "$md" >> "$summary"
            echo "" >> "$summary"
        fi
    done

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
