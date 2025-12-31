#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Force C locale for consistent number formatting
export LC_NUMERIC=C

# Global for file size
FILE_SIZE_BYTES=0
FILE_SIZE_MB=0

show_help() {
    cat << EOF
SQL Splitter Benchmark Runner

Usage: benchmark-runner [OPTIONS] [SQL_FILE]

Options:
  --help           Show this help message
  --generate SIZE  Generate test data (e.g., 10, 50, 100 for MB)
  --runs N         Number of hyperfine runs (default: 3)
  --warmup N       Number of warmup runs (default: 1)
  --export FILE    Export results to markdown file
  --list           List available tools
  --test           Test which tools work with the given file

Examples:
  benchmark-runner /data/dump.sql
  benchmark-runner --generate 100
  benchmark-runner /data/dump.sql --export /results/bench.md
EOF
}

# Get file size in bytes (cross-platform)
get_file_size() {
    local file="$1"
    if [[ "$(uname)" == "Darwin" ]]; then
        stat -f%z "$file" 2>/dev/null
    else
        stat -c%s "$file" 2>/dev/null
    fi
}

list_tools() {
    echo -e "${BOLD}Available Tools:${NC}"
    echo ""
    echo -e "${BOLD}Rust:${NC}"
    echo -n "  sql-splitter: "
    command -v sql-splitter &>/dev/null && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"
    
    echo -e "${BOLD}Go:${NC}"
    echo -n "  mysqldumpsplit-helgesverre: "
    [ -x /usr/local/bin/mysqldumpsplit-helgesverre ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"
    echo -n "  mysqldumpsplit-afrase: "
    [ -x /usr/local/bin/mysqldumpsplit-afrase ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"

    echo -e "${BOLD}Bash/Shell:${NC}"
    echo -n "  mysqldumpsplitter-bash: "
    [ -x /usr/local/bin/mysqldumpsplitter-bash ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"
    echo -n "  mysql_splitdump (csplit): "
    [ -x /usr/local/bin/mysql_splitdump ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"

    echo -e "${BOLD}Node.js:${NC}"
    echo -n "  mysqldumpsplit-node: "
    command -v mysqldumpsplit &>/dev/null && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"

    echo -e "${BOLD}Python:${NC}"
    echo -n "  sql-table-splitter: "
    [ -x /usr/local/bin/sql-table-splitter ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"
    echo -n "  sql-splitter-tkaratug: "
    [ -x /usr/local/bin/sql-splitter-tkaratug ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"
    echo -n "  sql-dump-splitter: "
    [ -x /usr/local/bin/sql-dump-splitter ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"

    echo -e "${BOLD}Ruby:${NC}"
    echo -n "  mysql-dump-split: "
    [ -x /usr/local/bin/mysql-dump-split ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}"
}

generate_test_data() {
    local size_mb=$1
    local file="/tmp/benchmark_${size_mb}mb.sql"

    echo -e "${YELLOW}Generating ${size_mb}MB test data...${NC}" >&2
    python3 /usr/local/bin/generate-test-dump.py "$size_mb" -o "$file" >&2
    echo "$file"
}

test_tool() {
    local name="$1"
    local cmd="$2"
    local out_dir="$3"
    local timeout_sec="${4:-60}"

    rm -rf "$out_dir" 2>/dev/null
    mkdir -p "$out_dir"

    echo -n "  $name: "

    if timeout "$timeout_sec" bash -c "$cmd" > /dev/null 2>&1; then
        local count=$(ls -1 "$out_dir" 2>/dev/null | wc -l | tr -d ' ')
        if [ "$count" -gt 0 ]; then
            echo -e "${GREEN}OK${NC} ($count files)"
            return 0
        else
            echo -e "${YELLOW}no output${NC}"
            return 1
        fi
    else
        echo -e "${RED}failed${NC}"
        return 1
    fi
}

test_tools() {
    local sql_file="$1"

    echo -e "${BOLD}Testing tools with: $sql_file${NC}"
    echo ""

    rm -rf /tmp/test-* 2>/dev/null
    mkdir -p /tmp/test-rust /tmp/test-bash /tmp/test-csplit /tmp/test-go /tmp/test-node /tmp/test-ruby

    test_tool "sql-splitter (Rust)" \
        "sql-splitter split '$sql_file' -o /tmp/test-rust" \
        "/tmp/test-rust"

    test_tool "mysqldumpsplitter (Bash)" \
        "bash /usr/local/bin/mysqldumpsplitter-bash --source '$sql_file' --extract ALLTABLES --output_dir /tmp/test-bash --compression none --decompression none" \
        "/tmp/test-bash"

    test_tool "mysql_splitdump (csplit)" \
        "cd /tmp/test-csplit && bash /usr/local/bin/mysql_splitdump '$sql_file'" \
        "/tmp/test-csplit"

    if command -v mysqldumpsplit-go &>/dev/null; then
        test_tool "mysqldumpsplit-go (Go)" \
            "timeout 30 mysqldumpsplit-go -i '$sql_file' -o /tmp/test-go" \
            "/tmp/test-go" 30 || true
    fi

    if [ -x /usr/bin/mysqldumpsplit ]; then
        test_tool "mysqldumpsplit (Node.js)" \
            "timeout 60 /usr/bin/mysqldumpsplit -o /tmp/test-node '$sql_file'" \
            "/tmp/test-node" 60 || true
    fi

    if [ -x /usr/local/bin/mysql-dump-split.rb ]; then
        test_tool "mysql-dump-split.rb (Ruby)" \
            "cd /tmp/test-ruby && timeout 60 ruby /usr/local/bin/mysql-dump-split.rb '$sql_file'" \
            "/tmp/test-ruby/tables" 60 || true
    fi
}

# Generate enhanced markdown table with memory, CPU, and throughput
generate_enhanced_table() {
    local json_file="$1"
    local output_file="$2"

    if ! command -v jq &>/dev/null; then
        echo -e "${YELLOW}jq not found, skipping enhanced table${NC}"
        return
    fi

    if [ ! -f "$json_file" ]; then
        return
    fi

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

run_benchmark() {
    local sql_file="$1"
    local runs="$2"
    local warmup="$3"
    local export_file="$4"

    local file_size=$(ls -lh "$sql_file" | awk '{print $5}')

    # Get file size for throughput calculation
    FILE_SIZE_BYTES=$(get_file_size "$sql_file")
    FILE_SIZE_MB=$(printf "%.2f" "$(echo "$FILE_SIZE_BYTES / 1024 / 1024" | bc -l)")

    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  SQL Splitter Benchmark${NC}"
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "File: ${GREEN}$sql_file${NC} ($file_size)"
    echo -e "Runs: $runs, Warmup: $warmup"
    echo ""

    # Test which tools work first
    echo -e "${CYAN}Testing tools...${NC}"
    local working_tools=()

    rm -rf /tmp/test-* 2>/dev/null
    mkdir -p /tmp/test-csplit

    if test_tool "sql-splitter" "sql-splitter split '$sql_file' -o /tmp/test-rust" "/tmp/test-rust"; then
        working_tools+=("sql-splitter (Rust)|sql-splitter split '$sql_file' -o /tmp/bench-rust")
    fi

    if test_tool "mysqldumpsplitter (Bash)" "bash /usr/local/bin/mysqldumpsplitter-bash --source '$sql_file' --extract ALLTABLES --output_dir /tmp/test-bash --compression none --decompression none" "/tmp/test-bash"; then
        working_tools+=("mysqldumpsplitter (Bash)|bash /usr/local/bin/mysqldumpsplitter-bash --source '$sql_file' --extract ALLTABLES --output_dir /tmp/bench-bash --compression none --decompression none")
    fi

    mkdir -p /tmp/test-csplit
    if test_tool "mysql_splitdump (csplit)" "cd /tmp/test-csplit && bash /usr/local/bin/mysql_splitdump '$sql_file'" "/tmp/test-csplit"; then
        working_tools+=("mysql_splitdump (csplit)|cd /tmp/bench-csplit && bash /usr/local/bin/mysql_splitdump '$sql_file'")
    fi

    if [ -x /usr/local/bin/mysqldumpsplit-helgesverre ]; then
        mkdir -p /tmp/test-go
        if test_tool "mysqldumpsplit (Go)" "timeout 30 /usr/local/bin/mysqldumpsplit-helgesverre -i '$sql_file' -o /tmp/test-go" "/tmp/test-go"; then
            working_tools+=("mysqldumpsplit (Go)|/usr/local/bin/mysqldumpsplit-helgesverre -i '$sql_file' -o /tmp/bench-go")
        fi
    fi

    if [ -x /usr/local/bin/mysqldumpsplit-node ]; then
        mkdir -p /tmp/test-node
        if test_tool "mysqldumpsplit (Node.js)" "timeout 60 /usr/local/bin/mysqldumpsplit-node -o /tmp/test-node '$sql_file'" "/tmp/test-node"; then
            working_tools+=("mysqldumpsplit (Node.js)|/usr/local/bin/mysqldumpsplit-node -o /tmp/bench-node '$sql_file'")
        fi
    fi

    if [ -x /usr/local/bin/mysql-dump-split ]; then
        mkdir -p /tmp/test-ruby
        if test_tool "mysql-dump-split (Ruby)" "cd /tmp/test-ruby && timeout 60 ruby /usr/local/bin/mysql-dump-split '$sql_file'" "/tmp/test-ruby/tables"; then
            working_tools+=("mysql-dump-split (Ruby)|cd /tmp/bench-ruby && ruby /usr/local/bin/mysql-dump-split '$sql_file'")
        fi
    fi

    echo ""
    echo -e "${CYAN}Running benchmark with ${#working_tools[@]} working tools...${NC}"
    echo ""

    if [ ${#working_tools[@]} -lt 2 ]; then
        echo -e "${RED}Not enough working tools for comparison${NC}"
        return 1
    fi

    # Build hyperfine command
    local cmds=()
    for tool in "${working_tools[@]}"; do
        local name="${tool%%|*}"
        local cmd="${tool#*|}"
        cmds+=("--command-name" "$name" "$cmd")
    done

    # Always export JSON for enhanced table
    local json_file="/tmp/benchmark_results.json"
    local export_args="--export-json $json_file"

    if [ -n "$export_file" ]; then
        export_args="$export_args --export-markdown $export_file"
    fi

    hyperfine \
        --warmup "$warmup" \
        --runs "$runs" \
        --ignore-failure \
        --prepare 'rm -rf /tmp/bench-*; mkdir -p /tmp/bench-csplit /tmp/bench-ruby /tmp/bench-node' \
        $export_args \
        "${cmds[@]}"

    # Generate enhanced results table
    local enhanced_file="${export_file%.md}_enhanced.md"
    if [ -z "$export_file" ]; then
        enhanced_file="/tmp/benchmark_enhanced.md"
    fi
    generate_enhanced_table "$json_file" "$enhanced_file"

    if [ -n "$export_file" ]; then
        echo ""
        echo -e "${GREEN}Results exported to: $export_file${NC}"
    fi
}

# Parse arguments
SQL_FILE=""
GENERATE_SIZE=""
RUNS=3
WARMUP=1
EXPORT_FILE=""
DO_LIST=false
DO_TEST=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --generate)
            GENERATE_SIZE=$2
            shift 2
            ;;
        --runs)
            RUNS=$2
            shift 2
            ;;
        --warmup)
            WARMUP=$2
            shift 2
            ;;
        --export)
            EXPORT_FILE=$2
            shift 2
            ;;
        --list)
            DO_LIST=true
            shift
            ;;
        --test)
            DO_TEST=true
            shift
            ;;
        *)
            SQL_FILE=$1
            shift
            ;;
    esac
done

if [ "$DO_LIST" = true ]; then
    list_tools
    exit 0
fi

# Determine SQL file
if [ -n "$GENERATE_SIZE" ]; then
    SQL_FILE=$(generate_test_data "$GENERATE_SIZE")
elif [ -z "$SQL_FILE" ]; then
    echo -e "${RED}Error: No SQL file specified${NC}"
    echo "Use --generate SIZE to create test data or provide a SQL file path"
    exit 1
elif [ ! -f "$SQL_FILE" ]; then
    echo -e "${RED}Error: File not found: $SQL_FILE${NC}"
    exit 1
fi

if [ "$DO_TEST" = true ]; then
    test_tools "$SQL_FILE"
    exit 0
fi

run_benchmark "$SQL_FILE" "$RUNS" "$WARMUP" "$EXPORT_FILE"
