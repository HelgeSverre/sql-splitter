#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_help() {
    echo "SQL Splitter Docker Benchmark Runner"
    echo ""
    echo "Usage: benchmark-runner [OPTIONS] [SQL_FILE]"
    echo ""
    echo "Options:"
    echo "  --help           Show this help message"
    echo "  --generate       Generate test data instead of using a file"
    echo "  --rows N         Number of rows to generate (default: 200000)"
    echo "  --runs N         Number of hyperfine runs (default: 5)"
    echo "  --warmup N       Number of warmup runs (default: 2)"
    echo "  --export FILE    Export results to markdown file"
    echo "  --quick          Quick comparison (single run, no warmup)"
    echo ""
    echo "Examples:"
    echo "  benchmark-runner /data/dump.sql"
    echo "  benchmark-runner --generate --rows 500000"
    echo "  benchmark-runner /data/dump.sql --export /results/bench.md"
}

generate_test_data() {
    local rows=$1
    local file="/tmp/generated_test.sql"
    
    echo -e "${YELLOW}Generating test data ($rows rows)...${NC}"
    python3 << PYTHON
import random
tables = ['users', 'posts', 'comments', 'orders', 'products', 'sessions', 'logs', 'events']
with open('$file', 'w') as f:
    for table in tables:
        f.write(f"CREATE TABLE {table} (id INT, data VARCHAR(255));\n")
    for i in range($rows):
        table = random.choice(tables)
        body = "Lorem ipsum dolor sit amet " * 3
        f.write(f"INSERT INTO {table} VALUES ({i}, '{body}');\n")
PYTHON
    echo "$file"
}

run_quick_comparison() {
    local sql_file=$1
    
    echo -e "${BLUE}=== Quick Comparison ===${NC}"
    echo ""
    
    rm -rf /tmp/out-rs /tmp/out-go /tmp/out-node /tmp/out-sql-split /tmp/out-ruby
    
    echo -e "${GREEN}Rust (sql-splitter-rs):${NC}"
    time sql-splitter-rs split "$sql_file" -o /tmp/out-rs 2>&1 | grep -E "(Throughput|Elapsed|Statements|Tables)" || true
    echo ""
    
    echo -e "${GREEN}Go (mysqldumpsplit-go):${NC}"
    time mysqldumpsplit-go -o /tmp/out-go "$sql_file" 2>&1 || true
    echo ""
    
    echo -e "${GREEN}Node.js (@vekexasia/mysqldumpsplit):${NC}"
    time mysqldumpsplit -i "$sql_file" -o /tmp/out-node 2>&1 || true
    echo ""
    
    if command -v sql-split-ooooak &> /dev/null; then
        echo -e "${GREEN}Rust (ooooak/sql-split):${NC}"
        time sql-split-ooooak "$sql_file" -o /tmp/out-sql-split 2>&1 || true
        echo ""
    fi
    
    if command -v mysql-dump-split &> /dev/null; then
        echo -e "${GREEN}Ruby (ripienaar/mysql-dump-split):${NC}"
        time mysql-dump-split --out /tmp/out-ruby "$sql_file" 2>&1 || true
        echo ""
    fi
}

run_hyperfine_benchmark() {
    local sql_file=$1
    local runs=$2
    local warmup=$3
    local export_file=$4
    
    echo -e "${BLUE}=== Hyperfine Benchmark ===${NC}"
    echo -e "File: ${GREEN}$sql_file${NC}"
    echo -e "Runs: $runs, Warmup: $warmup"
    echo ""
    
    local export_arg=""
    if [ -n "$export_file" ]; then
        export_arg="--export-markdown $export_file"
    fi
    
    # Build command array dynamically based on available tools
    local cmds=()
    cmds+=("--command-name" "sql-splitter (Rust)" "sql-splitter-rs split '$sql_file' -o /tmp/out-rs")
    cmds+=("--command-name" "mysqldumpsplit (Go)" "mysqldumpsplit-go -o /tmp/out-go '$sql_file'")
    cmds+=("--command-name" "mysqldumpsplit (Node)" "mysqldumpsplit -i '$sql_file' -o /tmp/out-node")
    
    if command -v sql-split-ooooak &> /dev/null; then
        cmds+=("--command-name" "sql-split (Rust/ooooak)" "sql-split-ooooak '$sql_file' -o /tmp/out-sql-split")
    fi
    
    if command -v mysql-dump-split &> /dev/null; then
        cmds+=("--command-name" "mysql-dump-split (Ruby)" "mysql-dump-split --out /tmp/out-ruby '$sql_file'")
    fi
    
    hyperfine \
        --warmup "$warmup" \
        --runs "$runs" \
        --prepare 'rm -rf /tmp/out-rs /tmp/out-go /tmp/out-node /tmp/out-sql-split /tmp/out-ruby' \
        $export_arg \
        "${cmds[@]}"
    
    if [ -n "$export_file" ]; then
        echo -e "${GREEN}Results exported to: $export_file${NC}"
    fi
}

# Parse arguments
SQL_FILE=""
GENERATE=false
ROWS=200000
RUNS=5
WARMUP=2
EXPORT_FILE=""
QUICK=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            show_help
            exit 0
            ;;
        --generate)
            GENERATE=true
            shift
            ;;
        --rows)
            ROWS=$2
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
        --quick)
            QUICK=true
            shift
            ;;
        *)
            SQL_FILE=$1
            shift
            ;;
    esac
done

# Determine SQL file
if [ "$GENERATE" = true ]; then
    SQL_FILE=$(generate_test_data $ROWS)
elif [ -z "$SQL_FILE" ]; then
    echo -e "${RED}Error: No SQL file specified${NC}"
    echo "Use --generate to create test data or provide a SQL file path"
    exit 1
elif [ ! -f "$SQL_FILE" ]; then
    echo -e "${RED}Error: File not found: $SQL_FILE${NC}"
    exit 1
fi

FILE_SIZE=$(ls -lh "$SQL_FILE" | awk '{print $5}')
echo -e "${BLUE}=== SQL Splitter Benchmark ===${NC}"
echo -e "File: ${GREEN}$SQL_FILE${NC} ($FILE_SIZE)"
echo ""

if [ "$QUICK" = true ]; then
    run_quick_comparison "$SQL_FILE"
else
    run_hyperfine_benchmark "$SQL_FILE" "$RUNS" "$WARMUP" "$EXPORT_FILE"
fi
