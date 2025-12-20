#!/bin/bash
#
# Comprehensive SQL Splitter Benchmark Suite
# Compares: sql-splitter (Rust), mysqldumpsplit (Go), @vekexasia/mysqldumpsplit (Node.js)
#
# Usage:
#   ./scripts/benchmark-all.sh                    # Generate test files and run benchmarks
#   ./scripts/benchmark-all.sh path/to/dump.sql   # Use custom SQL file
#   ./scripts/benchmark-all.sh --help             # Show help
#
# Requirements:
#   - hyperfine (benchmark runner)
#   - cargo (Rust toolchain)
#   - go (Go toolchain, optional)
#   - node/npm (Node.js, optional)
#

set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BENCH_DIR="/tmp/sql-splitter-bench"
RESULTS_DIR="$PROJECT_DIR/benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Benchmark settings
WARMUP_RUNS=2
MIN_RUNS=5
MAX_RUNS=10

# Test file sizes (in MB)
TEST_SIZES=(10 50 100)

# Tool binaries/commands
RUST_BIN="$PROJECT_DIR/target/release/sql-splitter"
GO_BIN=""  # Set dynamically
NODE_BIN=""  # Set dynamically
SQL_SPLIT_BIN=""  # ooooak/sql-split (Rust)
MYSQL_DUMP_SPLIT_BIN=""  # ripienaar/mysql-dump-split (Ruby)

# ============================================================================
# Colors and Output Helpers
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

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

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_progress() {
    echo -e "${MAGENTA}►${NC} $1"
}

# ============================================================================
# Help and Usage
# ============================================================================

show_help() {
    cat << EOF
${BOLD}SQL Splitter Benchmark Suite${NC}

${BOLD}USAGE:${NC}
    $(basename "$0") [OPTIONS] [SQL_FILE]

${BOLD}ARGUMENTS:${NC}
    SQL_FILE    Optional path to a SQL dump file to use for benchmarking.
                If not provided, test files will be generated.

${BOLD}OPTIONS:${NC}
    -h, --help          Show this help message
    -s, --sizes SIZES   Comma-separated list of test file sizes in MB
                        (default: 10,50,100)
    -r, --runs RUNS     Number of benchmark runs (default: $MIN_RUNS-$MAX_RUNS)
    -w, --warmup N      Number of warmup runs (default: $WARMUP_RUNS)
    --skip-install      Skip tool installation checks
    --rust-only         Only benchmark the Rust implementation
    --no-verify         Skip output verification

${BOLD}EXAMPLES:${NC}
    # Run with default settings (generates test files)
    $(basename "$0")

    # Use a custom SQL dump file
    $(basename "$0") /path/to/database.sql

    # Run with custom sizes
    $(basename "$0") -s 5,25,50

    # Quick benchmark with fewer runs
    $(basename "$0") -r 3 -w 1

${BOLD}REQUIREMENTS:${NC}
    Required:
      - cargo (Rust toolchain)
      - hyperfine (benchmark runner)
    
    Optional (for comparison):
      - go (for mysqldumpsplit)
      - node/npm (for @vekexasia/mysqldumpsplit)

${BOLD}OUTPUT:${NC}
    Results are saved to: $RESULTS_DIR/
    
EOF
    exit 0
}

# ============================================================================
# Argument Parsing
# ============================================================================

CUSTOM_SQL_FILE=""
SKIP_INSTALL=false
RUST_ONLY=false
NO_VERIFY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            ;;
        -s|--sizes)
            IFS=',' read -ra TEST_SIZES <<< "$2"
            shift 2
            ;;
        -r|--runs)
            MIN_RUNS=$2
            MAX_RUNS=$2
            shift 2
            ;;
        -w|--warmup)
            WARMUP_RUNS=$2
            shift 2
            ;;
        --skip-install)
            SKIP_INSTALL=true
            shift
            ;;
        --rust-only)
            RUST_ONLY=true
            shift
            ;;
        --no-verify)
            NO_VERIFY=true
            shift
            ;;
        -*)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
        *)
            CUSTOM_SQL_FILE="$1"
            shift
            ;;
    esac
done

# ============================================================================
# Dependency Checks
# ============================================================================

check_required_tools() {
    print_section "Checking Required Tools"
    
    local missing_required=false
    
    # Check hyperfine
    if command -v hyperfine &> /dev/null; then
        local hf_version=$(hyperfine --version | head -1)
        print_success "hyperfine: $hf_version"
    else
        print_error "hyperfine not found"
        echo "    Install with: brew install hyperfine (macOS)"
        echo "                  cargo install hyperfine"
        missing_required=true
    fi
    
    # Check cargo
    if command -v cargo &> /dev/null; then
        local cargo_version=$(cargo --version)
        print_success "cargo: $cargo_version"
    else
        print_error "cargo not found"
        echo "    Install from: https://rustup.rs"
        missing_required=true
    fi
    
    # Check Python (for test data generation)
    if command -v python3 &> /dev/null; then
        local py_version=$(python3 --version)
        print_success "python3: $py_version"
    else
        print_error "python3 not found (needed for test data generation)"
        missing_required=true
    fi
    
    if [ "$missing_required" = true ]; then
        echo ""
        print_error "Missing required tools. Please install them and try again."
        exit 1
    fi
}

check_optional_tools() {
    print_section "Checking Competitor Tools"
    
    # Check Go
    if command -v go &> /dev/null; then
        local go_version=$(go version | awk '{print $3}')
        print_success "go: $go_version"
        GO_AVAILABLE=true
    else
        print_warning "go not found - skipping mysqldumpsplit (Go) benchmarks"
        GO_AVAILABLE=false
    fi
    
    # Check Node.js
    if command -v node &> /dev/null && command -v npm &> /dev/null; then
        local node_version=$(node --version)
        print_success "node: $node_version"
        NODE_AVAILABLE=true
    else
        print_warning "node/npm not found - skipping @vekexasia/mysqldumpsplit benchmarks"
        NODE_AVAILABLE=false
    fi
    
    # Check sql-split (ooooak/sql-split - Rust)
    if command -v sql-split &> /dev/null; then
        print_success "sql-split: installed (ooooak/sql-split)"
        SQL_SPLIT_AVAILABLE=true
        SQL_SPLIT_BIN=$(which sql-split)
    else
        print_warning "sql-split not found - install via: cargo install sql-split"
        SQL_SPLIT_AVAILABLE=false
    fi
    
    # Check mysql-dump-split (ripienaar/mysql-dump-split - Ruby)
    if command -v mysql-dump-split &> /dev/null; then
        print_success "mysql-dump-split: installed (ripienaar/mysql-dump-split)"
        MYSQL_DUMP_SPLIT_AVAILABLE=true
        MYSQL_DUMP_SPLIT_BIN=$(which mysql-dump-split)
    else
        print_warning "mysql-dump-split not found - install via: gem install mysql-dump-split"
        MYSQL_DUMP_SPLIT_AVAILABLE=false
    fi
}

# ============================================================================
# Tool Installation/Building
# ============================================================================

build_rust_binary() {
    print_section "Building sql-splitter (Rust)"
    
    cd "$PROJECT_DIR"
    
    print_progress "Building with native CPU optimizations..."
    RUSTFLAGS="-C target-cpu=native" cargo build --release 2>&1 | tail -5
    
    if [ -f "$RUST_BIN" ]; then
        local size=$(ls -lh "$RUST_BIN" | awk '{print $5}')
        print_success "Built: $RUST_BIN ($size)"
    else
        print_error "Failed to build Rust binary"
        exit 1
    fi
}

install_go_tool() {
    if [ "$RUST_ONLY" = true ] || [ "$GO_AVAILABLE" = false ]; then
        return
    fi
    
    print_section "Installing mysqldumpsplit (Go)"
    
    # Check if already installed
    GO_BIN=$(go env GOPATH)/bin/mysqldumpsplit
    
    if [ -f "$GO_BIN" ]; then
        print_success "Already installed: $GO_BIN"
    else
        print_progress "Installing via go install..."
        if go install github.com/afrase/mysqldumpsplit@latest 2>&1; then
            print_success "Installed: $GO_BIN"
        else
            print_warning "Failed to install mysqldumpsplit (Go)"
            GO_AVAILABLE=false
        fi
    fi
}

install_node_tool() {
    if [ "$RUST_ONLY" = true ] || [ "$NODE_AVAILABLE" = false ]; then
        return
    fi
    
    print_section "Installing @vekexasia/mysqldumpsplit (Node.js)"
    
    # Check if already installed
    if command -v mysqldumpsplit &> /dev/null; then
        NODE_BIN=$(which mysqldumpsplit)
        print_success "Already installed: $NODE_BIN"
    else
        print_progress "Installing via npm..."
        if npm install -g @vekexasia/mysqldumpsplit 2>&1 | tail -3; then
            NODE_BIN=$(which mysqldumpsplit 2>/dev/null || echo "")
            if [ -n "$NODE_BIN" ]; then
                print_success "Installed: $NODE_BIN"
            else
                print_warning "Installation completed but binary not found in PATH"
                NODE_AVAILABLE=false
            fi
        else
            print_warning "Failed to install @vekexasia/mysqldumpsplit"
            NODE_AVAILABLE=false
        fi
    fi
}

install_sql_split_tool() {
    if [ "$RUST_ONLY" = true ] || [ "${SQL_SPLIT_AVAILABLE:-false}" = false ]; then
        return
    fi
    
    print_section "Checking sql-split (ooooak/sql-split)"
    
    if [ -n "$SQL_SPLIT_BIN" ]; then
        print_success "Already installed: $SQL_SPLIT_BIN"
    else
        print_progress "Installing via cargo install sql-split..."
        if cargo install sql-split 2>&1 | tail -3; then
            SQL_SPLIT_BIN=$(which sql-split 2>/dev/null || echo "")
            if [ -n "$SQL_SPLIT_BIN" ]; then
                print_success "Installed: $SQL_SPLIT_BIN"
                SQL_SPLIT_AVAILABLE=true
            else
                print_warning "Installation completed but binary not found in PATH"
                SQL_SPLIT_AVAILABLE=false
            fi
        else
            print_warning "Failed to install sql-split"
            SQL_SPLIT_AVAILABLE=false
        fi
    fi
}

install_mysql_dump_split_tool() {
    if [ "$RUST_ONLY" = true ] || [ "${MYSQL_DUMP_SPLIT_AVAILABLE:-false}" = false ]; then
        return
    fi
    
    print_section "Checking mysql-dump-split (ripienaar/mysql-dump-split)"
    
    if [ -n "$MYSQL_DUMP_SPLIT_BIN" ]; then
        print_success "Already installed: $MYSQL_DUMP_SPLIT_BIN"
    else
        # Check if Ruby/gem is available
        if command -v gem &> /dev/null; then
            print_progress "Installing via gem install mysql-dump-split..."
            if gem install mysql-dump-split 2>&1 | tail -3; then
                MYSQL_DUMP_SPLIT_BIN=$(which mysql-dump-split 2>/dev/null || echo "")
                if [ -n "$MYSQL_DUMP_SPLIT_BIN" ]; then
                    print_success "Installed: $MYSQL_DUMP_SPLIT_BIN"
                    MYSQL_DUMP_SPLIT_AVAILABLE=true
                else
                    print_warning "Installation completed but binary not found in PATH"
                    MYSQL_DUMP_SPLIT_AVAILABLE=false
                fi
            else
                print_warning "Failed to install mysql-dump-split"
                MYSQL_DUMP_SPLIT_AVAILABLE=false
            fi
        else
            print_warning "gem not found - cannot install mysql-dump-split"
            MYSQL_DUMP_SPLIT_AVAILABLE=false
        fi
    fi
}

# ============================================================================
# Test Data Generation
# ============================================================================

generate_test_file() {
    local size_mb=$1
    local output_file="$BENCH_DIR/test_${size_mb}mb.sql"
    
    if [ -f "$output_file" ]; then
        local actual_size=$(du -m "$output_file" | cut -f1)
        if [ "$actual_size" -ge "$((size_mb - 1))" ]; then
            print_info "Using existing: $output_file (${actual_size}MB)"
            return
        fi
    fi
    
    print_progress "Generating ${size_mb}MB test file..."
    
    python3 << PYTHON
import random
import os

tables = [
    'users', 'posts', 'comments', 'orders', 'products', 
    'sessions', 'logs', 'events', 'notifications', 'analytics'
]

target_size = ${size_mb} * 1024 * 1024  # Convert to bytes
output_file = "${output_file}"

with open(output_file, 'w') as f:
    # Write CREATE TABLE statements
    for table in tables:
        f.write(f"""CREATE TABLE \`{table}\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`name\` varchar(255) DEFAULT NULL,
  \`email\` varchar(255) DEFAULT NULL,
  \`data\` text,
  \`created_at\` datetime DEFAULT NULL,
  \`updated_at\` datetime DEFAULT NULL,
  PRIMARY KEY (\`id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

""")
    
    # Generate INSERT statements until we reach target size
    current_size = f.tell()
    batch_id = 0
    
    while current_size < target_size:
        table = random.choice(tables)
        # Generate varied row sizes
        data_length = random.randint(50, 500)
        data = 'x' * data_length
        
        f.write(f"INSERT INTO \`{table}\` VALUES ({batch_id},'User {batch_id}','user{batch_id}@example.com','{data}','2024-01-01 00:00:00','2024-01-01 00:00:00');\n")
        
        batch_id += 1
        if batch_id % 10000 == 0:
            current_size = f.tell()

print(f"Generated {batch_id:,} statements")
PYTHON
    
    local actual_size=$(du -h "$output_file" | cut -f1)
    print_success "Created: $output_file ($actual_size)"
}

generate_all_test_files() {
    print_section "Generating Test SQL Files"
    
    mkdir -p "$BENCH_DIR"
    
    for size in "${TEST_SIZES[@]}"; do
        generate_test_file "$size"
    done
}

# ============================================================================
# Benchmark Execution
# ============================================================================

cleanup_output() {
    rm -rf "$BENCH_DIR/output_rust" "$BENCH_DIR/output_go" "$BENCH_DIR/output_node" "$BENCH_DIR/output_sql_split" "$BENCH_DIR/output_ruby" 2>/dev/null || true
}

run_benchmark_for_file() {
    local sql_file="$1"
    local file_name=$(basename "$sql_file")
    local file_size=$(du -h "$sql_file" | cut -f1)
    
    print_section "Benchmarking: $file_name ($file_size)"
    
    mkdir -p "$RESULTS_DIR"
    local result_file="$RESULTS_DIR/benchmark_${file_name%.sql}_$TIMESTAMP"
    
    # Build command list
    local commands=()
    local names=()
    
    # Rust command
    commands+=("$RUST_BIN split '$sql_file' -o '$BENCH_DIR/output_rust'")
    names+=("sql-splitter (Rust)")
    
    # Go command
    if [ "$RUST_ONLY" = false ] && [ "$GO_AVAILABLE" = true ] && [ -n "$GO_BIN" ] && [ -f "$GO_BIN" ]; then
        commands+=("$GO_BIN -i '$sql_file' -o '$BENCH_DIR/output_go'")
        names+=("mysqldumpsplit (Go)")
    fi
    
    # Node command
    if [ "$RUST_ONLY" = false ] && [ "$NODE_AVAILABLE" = true ] && [ -n "$NODE_BIN" ]; then
        commands+=("$NODE_BIN -f '$sql_file' -o '$BENCH_DIR/output_node'")
        names+=("mysqldumpsplit (Node)")
    fi
    
    # sql-split command (ooooak/sql-split)
    if [ "$RUST_ONLY" = false ] && [ "${SQL_SPLIT_AVAILABLE:-false}" = true ] && [ -n "$SQL_SPLIT_BIN" ]; then
        commands+=("$SQL_SPLIT_BIN '$sql_file' -o '$BENCH_DIR/output_sql_split'")
        names+=("sql-split (Rust)")
    fi
    
    # mysql-dump-split command (ripienaar/mysql-dump-split)
    if [ "$RUST_ONLY" = false ] && [ "${MYSQL_DUMP_SPLIT_AVAILABLE:-false}" = true ] && [ -n "$MYSQL_DUMP_SPLIT_BIN" ]; then
        commands+=("$MYSQL_DUMP_SPLIT_BIN --out '$BENCH_DIR/output_ruby' '$sql_file'")
        names+=("mysql-dump-split (Ruby)")
    fi
    
    # Build hyperfine command
    local hyperfine_cmd="hyperfine"
    hyperfine_cmd+=" --warmup $WARMUP_RUNS"
    hyperfine_cmd+=" --min-runs $MIN_RUNS"
    hyperfine_cmd+=" --max-runs $MAX_RUNS"
    hyperfine_cmd+=" --prepare 'rm -rf $BENCH_DIR/output_*'"
    hyperfine_cmd+=" --export-markdown '${result_file}.md'"
    hyperfine_cmd+=" --export-json '${result_file}.json'"
    
    for i in "${!commands[@]}"; do
        hyperfine_cmd+=" --command-name '${names[$i]}' '${commands[$i]}'"
    done
    
    # Run benchmark
    echo ""
    eval "$hyperfine_cmd"
    echo ""
    
    print_success "Results saved to: ${result_file}.md"
    
    # Return the result file path for summary
    echo "${result_file}.md"
}

# ============================================================================
# Output Verification
# ============================================================================

verify_outputs() {
    local sql_file="$1"
    
    if [ "$NO_VERIFY" = true ]; then
        return
    fi
    
    print_section "Verifying Output Correctness"
    
    # Clean and run each tool once
    cleanup_output
    
    # Run Rust
    print_progress "Running Rust splitter..."
    "$RUST_BIN" split "$sql_file" -o "$BENCH_DIR/output_rust" > /dev/null 2>&1
    local rust_files=$(ls -1 "$BENCH_DIR/output_rust" 2>/dev/null | wc -l | tr -d ' ')
    local rust_size=$(du -sh "$BENCH_DIR/output_rust" 2>/dev/null | cut -f1)
    print_info "Rust: $rust_files tables, $rust_size total"
    
    # Run Go if available
    if [ "$RUST_ONLY" = false ] && [ "$GO_AVAILABLE" = true ] && [ -n "$GO_BIN" ] && [ -f "$GO_BIN" ]; then
        print_progress "Running Go splitter..."
        "$GO_BIN" -i "$sql_file" -o "$BENCH_DIR/output_go" > /dev/null 2>&1 || true
        local go_files=$(ls -1 "$BENCH_DIR/output_go" 2>/dev/null | wc -l | tr -d ' ')
        local go_size=$(du -sh "$BENCH_DIR/output_go" 2>/dev/null | cut -f1)
        print_info "Go: $go_files tables, $go_size total"
        
        # Compare
        if [ "$rust_files" = "$go_files" ]; then
            print_success "Table count matches between Rust and Go"
        else
            print_warning "Table count differs: Rust=$rust_files, Go=$go_files"
        fi
    fi
    
    # Run Node if available
    if [ "$RUST_ONLY" = false ] && [ "$NODE_AVAILABLE" = true ] && [ -n "$NODE_BIN" ]; then
        print_progress "Running Node splitter..."
        "$NODE_BIN" -f "$sql_file" -o "$BENCH_DIR/output_node" > /dev/null 2>&1 || true
        local node_files=$(ls -1 "$BENCH_DIR/output_node" 2>/dev/null | wc -l | tr -d ' ')
        local node_size=$(du -sh "$BENCH_DIR/output_node" 2>/dev/null | cut -f1)
        print_info "Node: $node_files tables, $node_size total"
        
        # Compare
        if [ "$rust_files" = "$node_files" ]; then
            print_success "Table count matches between Rust and Node"
        else
            print_warning "Table count differs: Rust=$rust_files, Node=$node_files"
        fi
    fi
    
    cleanup_output
}

# ============================================================================
# Summary Report Generation
# ============================================================================

generate_summary() {
    print_section "Generating Summary Report"
    
    local summary_file="$RESULTS_DIR/SUMMARY_$TIMESTAMP.md"
    
    cat > "$summary_file" << EOF
# SQL Splitter Benchmark Summary

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**System:** $(uname -s) $(uname -m)
**CPU:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || cat /proc/cpuinfo 2>/dev/null | grep "model name" | head -1 | cut -d: -f2 | xargs || echo "Unknown")

## Tools Tested

| Tool | Version | Language |
|------|---------|----------|
| sql-splitter | $("$RUST_BIN" --version 2>/dev/null | head -1 || echo "unknown") | Rust |
EOF

    if [ "$GO_AVAILABLE" = true ] && [ -n "$GO_BIN" ] && [ -f "$GO_BIN" ]; then
        echo "| mysqldumpsplit | latest | Go |" >> "$summary_file"
    fi
    
    if [ "$NODE_AVAILABLE" = true ] && [ -n "$NODE_BIN" ]; then
        echo "| @vekexasia/mysqldumpsplit | latest | Node.js |" >> "$summary_file"
    fi
    
    if [ "${SQL_SPLIT_AVAILABLE:-false}" = true ] && [ -n "$SQL_SPLIT_BIN" ]; then
        echo "| ooooak/sql-split | latest | Rust |" >> "$summary_file"
    fi
    
    if [ "${MYSQL_DUMP_SPLIT_AVAILABLE:-false}" = true ] && [ -n "$MYSQL_DUMP_SPLIT_BIN" ]; then
        echo "| ripienaar/mysql-dump-split | latest | Ruby |" >> "$summary_file"
    fi

    cat >> "$summary_file" << EOF

## Benchmark Configuration

- Warmup runs: $WARMUP_RUNS
- Min runs: $MIN_RUNS
- Max runs: $MAX_RUNS

## Results

EOF

    # Append all markdown results
    for md_file in "$RESULTS_DIR"/benchmark_*_$TIMESTAMP.md; do
        if [ -f "$md_file" ]; then
            echo "### $(basename "$md_file" .md | sed 's/_/ /g')" >> "$summary_file"
            echo "" >> "$summary_file"
            cat "$md_file" >> "$summary_file"
            echo "" >> "$summary_file"
        fi
    done

    cat >> "$summary_file" << EOF

## Notes

- All tools were run sequentially to avoid resource contention
- Output directories were cleaned between runs
- Rust binary compiled with \`-C target-cpu=native\` for optimal performance
EOF

    print_success "Summary saved to: $summary_file"
    
    # Display summary
    echo ""
    cat "$summary_file"
}

# ============================================================================
# Main Execution
# ============================================================================

main() {
    print_header "SQL Splitter Benchmark Suite"
    
    echo -e "${DIM}Comparing: sql-splitter (Rust) vs mysqldumpsplit (Go) vs mysqldumpsplit (Node)${NC}"
    echo ""
    
    # Check dependencies
    check_required_tools
    
    if [ "$SKIP_INSTALL" = false ]; then
        check_optional_tools
    fi
    
    # Build/install tools
    build_rust_binary
    
    if [ "$SKIP_INSTALL" = false ]; then
        install_go_tool
        install_node_tool
        install_sql_split_tool
        install_mysql_dump_split_tool
    fi
    
    # Prepare test files
    if [ -n "$CUSTOM_SQL_FILE" ]; then
        if [ ! -f "$CUSTOM_SQL_FILE" ]; then
            print_error "File not found: $CUSTOM_SQL_FILE"
            exit 1
        fi
        TEST_FILES=("$CUSTOM_SQL_FILE")
    else
        mkdir -p "$BENCH_DIR"
        generate_all_test_files
        TEST_FILES=()
        for size in "${TEST_SIZES[@]}"; do
            TEST_FILES+=("$BENCH_DIR/test_${size}mb.sql")
        done
    fi
    
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    
    # Run benchmarks
    print_header "Running Benchmarks"
    
    for sql_file in "${TEST_FILES[@]}"; do
        run_benchmark_for_file "$sql_file"
    done
    
    # Verify outputs (using first test file)
    verify_outputs "${TEST_FILES[0]}"
    
    # Generate summary
    generate_summary
    
    # Cleanup
    print_section "Cleanup"
    cleanup_output
    print_success "Temporary output directories cleaned"
    
    print_header "Benchmark Complete"
    
    echo -e "Results saved to: ${GREEN}$RESULTS_DIR/${NC}"
    echo ""
}

# Run main function
main "$@"
