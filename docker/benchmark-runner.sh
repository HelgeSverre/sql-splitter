#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

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

list_tools() {
    echo -e "${BOLD}Available Tools:${NC}"
    echo ""
    
    echo -n "  sql-splitter (Rust): "
    command -v sql-splitter &>/dev/null && echo -e "${GREEN}installed${NC}" || echo -e "${RED}not found${NC}"
    
    echo -n "  mysqldumpsplitter.sh (Bash/awk): "
    [ -x /usr/local/bin/mysqldumpsplitter.sh ] && echo -e "${GREEN}installed${NC}" || echo -e "${RED}not found${NC}"
    
    echo -n "  mysql_splitdump.sh (csplit): "
    [ -x /usr/local/bin/mysql_splitdump.sh ] && echo -e "${GREEN}installed${NC}" || echo -e "${RED}not found${NC}"
    
    echo -n "  mysqldumpsplit-go (Go): "
    command -v mysqldumpsplit-go &>/dev/null && echo -e "${GREEN}installed${NC}" || echo -e "${RED}not found${NC}"
    
    echo -n "  mysqldumpsplit (Node.js): "
    command -v mysqldumpsplit &>/dev/null && echo -e "${GREEN}installed${NC}" || echo -e "${RED}not found${NC}"
    
    echo -n "  mysql-dump-split.rb (Ruby): "
    [ -x /usr/local/bin/mysql-dump-split.rb ] && echo -e "${GREEN}installed${NC}" || echo -e "${RED}not found${NC}"
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
    
    test_tool "sql-splitter (Rust)" \
        "sql-splitter split '$sql_file' -o /tmp/test-rust" \
        "/tmp/test-rust"
    
    test_tool "mysqldumpsplitter.sh (Bash)" \
        "bash /usr/local/bin/mysqldumpsplitter.sh --source '$sql_file' --extract ALLTABLES --output_dir /tmp/test-bash --compression none" \
        "/tmp/test-bash"
    
    test_tool "mysql_splitdump.sh (csplit)" \
        "cd /tmp/test-csplit && bash /usr/local/bin/mysql_splitdump.sh '$sql_file'" \
        "/tmp/test-csplit"
    
    if command -v mysqldumpsplit-go &>/dev/null; then
        test_tool "mysqldumpsplit-go (Go)" \
            "mysqldumpsplit-go -i '$sql_file' -o /tmp/test-go" \
            "/tmp/test-go" 30
    fi
    
    if command -v mysqldumpsplit &>/dev/null; then
        test_tool "mysqldumpsplit (Node.js)" \
            "mysqldumpsplit -o /tmp/test-node '$sql_file'" \
            "/tmp/test-node" 30
    fi
    
    if [ -x /usr/local/bin/mysql-dump-split.rb ]; then
        test_tool "mysql-dump-split.rb (Ruby)" \
            "ruby /usr/local/bin/mysql-dump-split.rb --out /tmp/test-ruby '$sql_file'" \
            "/tmp/test-ruby" 30
    fi
}

run_benchmark() {
    local sql_file="$1"
    local runs="$2"
    local warmup="$3"
    local export_file="$4"
    
    local file_size=$(ls -lh "$sql_file" | awk '{print $5}')
    
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
    
    if test_tool "mysqldumpsplitter.sh" "bash /usr/local/bin/mysqldumpsplitter.sh --source '$sql_file' --extract ALLTABLES --output_dir /tmp/test-bash --compression none" "/tmp/test-bash"; then
        working_tools+=("mysqldumpsplitter (Bash)|bash /usr/local/bin/mysqldumpsplitter.sh --source '$sql_file' --extract ALLTABLES --output_dir /tmp/bench-bash --compression none")
    fi
    
    mkdir -p /tmp/test-csplit
    if test_tool "mysql_splitdump.sh" "cd /tmp/test-csplit && bash /usr/local/bin/mysql_splitdump.sh '$sql_file'" "/tmp/test-csplit"; then
        working_tools+=("mysql_splitdump (csplit)|cd /tmp/bench-csplit && bash /usr/local/bin/mysql_splitdump.sh '$sql_file'")
    fi
    
    if command -v mysqldumpsplit-go &>/dev/null; then
        if test_tool "mysqldumpsplit-go" "timeout 30 mysqldumpsplit-go -i '$sql_file' -o /tmp/test-go" "/tmp/test-go"; then
            working_tools+=("mysqldumpsplit (Go)|mysqldumpsplit-go -i '$sql_file' -o /tmp/bench-go")
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
    
    local export_arg=""
    if [ -n "$export_file" ]; then
        export_arg="--export-markdown $export_file --export-json ${export_file%.md}.json"
    fi
    
    hyperfine \
        --warmup "$warmup" \
        --runs "$runs" \
        --prepare 'rm -rf /tmp/bench-*; mkdir -p /tmp/bench-csplit' \
        $export_arg \
        "${cmds[@]}"
    
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
