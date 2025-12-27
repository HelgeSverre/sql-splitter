#!/usr/bin/env bash
#
# Memory profiling script for sql-splitter commands.
# Uses GNU time to measure peak RSS (Maximum resident set size).
#
# Usage:
#   ./scripts/profile-memory.sh [--generate-only] [--size SIZE] [--file FILE]
#
# Options:
#   --generate-only   Only generate test fixtures, don't run profiling
#   --size SIZE       Test data size: small, medium, large, xlarge (default: medium)
#   --file FILE       Use existing file instead of generating test data
#
# Requirements:
#   - GNU time (gtime on macOS, /usr/bin/time on Linux)
#   - Install on macOS: brew install gnu-time
#

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_ROOT/target/release/sql-splitter"
FIXTURES_DIR="$PROJECT_ROOT/tests/data/profile"
RESULTS_DIR="$PROJECT_ROOT/benchmark-results"

# Default options
GENERATE_ONLY=false
SIZE="medium"
CUSTOM_FILE=""
SEED=42
OUTPUT_FILE=""

# Size configurations (measured: ~100 bytes/row)
# Formula: rows × tables × 100 bytes ≈ file size
get_size_rows() {
    case $1 in
        tiny)   echo 500 ;;       # ~0.5MB
        small)  echo 2500 ;;      # ~2.5MB
        medium) echo 25000 ;;     # ~25MB
        large)  echo 125000 ;;    # ~125MB
        xlarge) echo 250000 ;;    # ~250MB
        huge)   echo 500000 ;;    # ~500MB
        mega)   echo 100000 ;;    # ~1GB (100 tables × 100k rows)
        giga)   echo 1000000 ;;   # ~10GB (100 tables × 1M rows) - MySQL only
        *) echo 25000 ;;
    esac
}

get_size_tables() {
    case $1 in
        tiny)   echo 10 ;;
        small)  echo 10 ;;
        medium) echo 10 ;;
        large)  echo 10 ;;
        xlarge) echo 10 ;;
        huge)   echo 10 ;;
        mega)   echo 100 ;;
        giga)   echo 100 ;;
        *) echo 10 ;;
    esac
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --generate-only)
            GENERATE_ONLY=true
            shift
            ;;
        --size)
            SIZE="$2"
            shift 2
            ;;
        --file)
            CUSTOM_FILE="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--generate-only] [--size SIZE] [--file FILE] [--output FILE]"
            echo ""
            echo "Options:"
            echo "  --generate-only   Only generate test fixtures"
            echo "  --size SIZE       Test data size (default: medium)"
            echo "  --file FILE       Use existing file instead of generating"
            echo "  --output FILE     Save results to file (in addition to stdout)"
            echo ""
            echo "Size configurations:"
            echo "  tiny:    500 rows/table,    10 tables (~0.5MB)"
            echo "  small:   2500 rows/table,   10 tables (~2.5MB)"
            echo "  medium:  25000 rows/table,  10 tables (~25MB)"
            echo "  large:   125000 rows/table, 10 tables (~125MB)"
            echo "  xlarge:  250000 rows/table, 10 tables (~250MB)"
            echo "  huge:    500000 rows/table, 10 tables (~500MB)"
            echo "  mega:    100000 rows/table, 100 tables (~1GB)"
            echo "  giga:    1000000 rows/table, 100 tables (~10GB, MySQL only)"
            exit 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# Detect GNU time
detect_time_cmd() {
    if command -v gtime &>/dev/null; then
        echo "gtime"
    elif [[ -x /usr/bin/time ]]; then
        if /usr/bin/time --version 2>&1 | grep -q "GNU"; then
            echo "/usr/bin/time"
        else
            echo "Error: GNU time not found. Install with 'brew install gnu-time' on macOS." >&2
            exit 1
        fi
    else
        echo "Error: GNU time not found." >&2
        exit 1
    fi
}

TIME_CMD=$(detect_time_cmd)
echo "Using time command: $TIME_CMD"

# Ensure binary exists
if [[ ! -x "$BINARY" ]]; then
    echo "Building release binary..."
    cargo build --release --manifest-path "$PROJECT_ROOT/Cargo.toml"
fi

# Create directories
mkdir -p "$FIXTURES_DIR" "$RESULTS_DIR"

# Get file size in MB (cross-platform)
get_file_size_mb() {
    local file=$1
    local bytes
    if [[ "$(uname)" == "Darwin" ]]; then
        bytes=$(stat -f%z "$file" 2>/dev/null)
    else
        bytes=$(stat -c%s "$file" 2>/dev/null)
    fi
    echo "scale=2; $bytes / 1024 / 1024" | bc
}

# Generate test fixture using Python
generate_fixture() {
    local dialect=$1
    local rows=$(get_size_rows "$SIZE")
    local tables=$(get_size_tables "$SIZE")
    local output_file="$FIXTURES_DIR/${dialect}_${SIZE}.sql"
    
    if [[ -f "$output_file" ]]; then
        echo "$output_file"
        return
    fi
    
    echo "Generating $dialect fixture: $rows rows/table, $tables tables..." >&2
    
    python3 - "$dialect" "$rows" "$tables" "$output_file" "$SEED" << 'PYTHON_SCRIPT'
import sys
import random

dialect = sys.argv[1]
rows_per_table = int(sys.argv[2])
num_tables = int(sys.argv[3])
output_file = sys.argv[4]
seed = int(sys.argv[5])

random.seed(seed)

def quote_id(name, dialect):
    if dialect == "mysql":
        return f"`{name}`"
    return f'"{name}"'

def generate_string(length=20):
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
    return ''.join(random.choice(chars) for _ in range(length))

with open(output_file, 'w') as f:
    if dialect == "mysql":
        f.write("-- MySQL dump generated for profiling\n")
        f.write("SET NAMES utf8mb4;\n")
        f.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")
    elif dialect == "postgres":
        f.write("-- PostgreSQL dump generated for profiling\n")
        f.write("SET client_encoding = 'UTF8';\n\n")
    else:
        f.write("-- SQLite dump generated for profiling\n")
        f.write("PRAGMA foreign_keys=OFF;\n\n")
    
    for t in range(num_tables):
        table_name = f"table_{t:03d}"
        q = lambda n: quote_id(n, dialect)
        
        f.write(f"DROP TABLE IF EXISTS {q(table_name)};\n")
        f.write(f"CREATE TABLE {q(table_name)} (\n")
        f.write(f"  {q('id')} INTEGER PRIMARY KEY,\n")
        f.write(f"  {q('name')} VARCHAR(100),\n")
        f.write(f"  {q('value')} INTEGER,\n")
        f.write(f"  {q('description')} TEXT")
        
        if t > 0:
            parent_table = f"table_{t-1:03d}"
            f.write(f",\n  {q('parent_id')} INTEGER")
            if dialect != "sqlite":
                f.write(f",\n  FOREIGN KEY ({q('parent_id')}) REFERENCES {q(parent_table)}({q('id')})")
        
        f.write("\n);\n\n")
        
        batch_size = 100
        for batch_start in range(0, rows_per_table, batch_size):
            batch_end = min(batch_start + batch_size, rows_per_table)
            
            if dialect == "postgres":
                cols = "id, name, value, description"
                if t > 0:
                    cols += ", parent_id"
                f.write(f"COPY {q(table_name)} ({cols}) FROM stdin;\n")
                for row_id in range(batch_start, batch_end):
                    global_id = t * rows_per_table + row_id + 1
                    name = generate_string(20).replace('\t', ' ')
                    value = random.randint(1, 1000000)
                    desc = generate_string(50).replace('\t', ' ')
                    line = f"{global_id}\t{name}\t{value}\t{desc}"
                    if t > 0:
                        parent_id = random.randint(1, (t) * rows_per_table)
                        line += f"\t{parent_id}"
                    f.write(line + "\n")
                f.write("\\.\n\n")
            else:
                cols = f"{q('id')}, {q('name')}, {q('value')}, {q('description')}"
                if t > 0:
                    cols += f", {q('parent_id')}"
                
                f.write(f"INSERT INTO {q(table_name)} ({cols}) VALUES\n")
                values = []
                for row_id in range(batch_start, batch_end):
                    global_id = t * rows_per_table + row_id + 1
                    name = generate_string(20).replace("'", "''")
                    value = random.randint(1, 1000000)
                    desc = generate_string(50).replace("'", "''")
                    row = f"({global_id}, '{name}', {value}, '{desc}'"
                    if t > 0:
                        parent_id = random.randint(1, (t) * rows_per_table)
                        row += f", {parent_id}"
                    row += ")"
                    values.append(row)
                f.write(",\n".join(values) + ";\n\n")
    
    if dialect == "mysql":
        f.write("SET FOREIGN_KEY_CHECKS = 1;\n")
    elif dialect == "sqlite":
        f.write("PRAGMA foreign_keys=ON;\n")

print(output_file)
PYTHON_SCRIPT
}

# Run a single profile test
run_profile() {
    local cmd_name=$1
    local input_file=$2
    local dialect=$3
    shift 3
    local extra_args=("$@")
    
    local time_log=$(mktemp)
    local output_log=$(mktemp)
    local output_file=$(mktemp)
    local output_dir=$(mktemp -d)
    
    # Build the command
    local cmd=("$BINARY" "$cmd_name")
    
    case $cmd_name in
        split)
            cmd+=("$input_file" "--output" "$output_dir" "--dialect" "$dialect")
            ;;
        analyze)
            cmd+=("$input_file" "--dialect" "$dialect")
            ;;
        validate)
            cmd+=("$input_file" "--dialect" "$dialect")
            ;;
        sample)
            cmd+=("$input_file" "--dialect" "$dialect" "--percent" "10" "--seed" "$SEED" "--output" "$output_file")
            ;;
        convert)
            local target_dialect="postgres"
            [[ "$dialect" == "postgres" ]] && target_dialect="mysql"
            cmd+=("$input_file" "--from" "$dialect" "--to" "$target_dialect" "--output" "$output_file")
            ;;
        diff)
            # Diff file against itself (measures memory for schema+data parsing)
            cmd+=("$input_file" "$input_file" "--dialect" "$dialect")
            ;;
        redact)
            cmd+=("$input_file" "--dialect" "$dialect" "--output" "$output_file" "--null" "*.password" "--hash" "*.email")
            ;;
        graph)
            cmd+=("$input_file" "--dialect" "$dialect" "--output" "$output_file")
            ;;
        order)
            cmd+=("$input_file" "--dialect" "$dialect" "--output" "$output_file")
            ;;
        query)
            # Query command: import dump and run a simple query
            cmd+=("$input_file" "--dialect" "$dialect" "SELECT COUNT(*) FROM table_000")
            ;;
        shard)
            # Shard command: shard by parent_id column on table_001
            cmd+=("$input_file" "--dialect" "$dialect" "--output" "$output_dir" "--column" "table_001.parent_id")
            ;;
    esac
    
    cmd+=("${extra_args[@]}")
    
    # Run with GNU time
    $TIME_CMD -v "${cmd[@]}" 2>"$time_log" >"$output_log" || true
    
    # Extract metrics
    local peak_rss_kb=$(grep "Maximum resident set size" "$time_log" | awk '{print $NF}')
    local wall_time=$(grep "Elapsed (wall clock)" "$time_log" | awk '{print $NF}')
    
    # Convert KB to MB
    local peak_rss_mb
    if [[ -n "$peak_rss_kb" ]]; then
        peak_rss_mb=$(echo "scale=2; $peak_rss_kb / 1024" | bc)
    else
        peak_rss_mb="N/A"
    fi
    
    # Get file size
    local file_size_mb=$(get_file_size_mb "$input_file")
    
    # Output result
    printf "%-12s %-10s %8s MB  %8s MB  %10s  %s\n" \
        "$cmd_name" "$dialect" "$file_size_mb" "$peak_rss_mb" "${wall_time:-N/A}" "${extra_args[*]:-}"
    
    # Cleanup
    rm -f "$time_log" "$output_log" "$output_file"
    rm -rf "$output_dir"
}

# Main profiling run
run_all_profiles() {
    local input_file=$1
    local dialect=$2
    
    echo ""
    echo "============================================================"
    echo "Memory Profile Results"
    echo "============================================================"
    echo "File: $input_file"
    echo "Size: $(du -h "$input_file" | cut -f1)"
    echo "Dialect: $dialect"
    echo "Date: $(date)"
    echo "============================================================"
    echo ""
    printf "%-12s %-10s %11s  %11s  %10s  %s\n" \
        "Command" "Dialect" "File Size" "Peak RSS" "Wall Time" "Extra Args"
    echo "------------------------------------------------------------"
    
    # Core commands
    run_profile "analyze" "$input_file" "$dialect"
    run_profile "split" "$input_file" "$dialect"
    run_profile "validate" "$input_file" "$dialect"
    run_profile "validate" "$input_file" "$dialect" "--no-fk-checks"
    run_profile "sample" "$input_file" "$dialect"
    run_profile "sample" "$input_file" "$dialect" "--preserve-relations"
    run_profile "sample" "$input_file" "$dialect" "--rows" "1000"
    run_profile "diff" "$input_file" "$dialect"
    run_profile "diff" "$input_file" "$dialect" "--schema-only"
    run_profile "redact" "$input_file" "$dialect"
    run_profile "redact" "$input_file" "$dialect" "--fake" "*.name"
    run_profile "graph" "$input_file" "$dialect"
    run_profile "graph" "$input_file" "$dialect" "--format" "json"
    run_profile "order" "$input_file" "$dialect"
    run_profile "order" "$input_file" "$dialect" "--dry-run"
    run_profile "query" "$input_file" "$dialect"
    run_profile "shard" "$input_file" "$dialect"
    
    # Convert only for mysql/postgres
    if [[ "$dialect" != "sqlite" ]]; then
        run_profile "convert" "$input_file" "$dialect"
    fi
    
    echo "------------------------------------------------------------"
    echo ""
}

# Main execution
main() {
    # Set up output file if specified
    if [[ -n "$OUTPUT_FILE" ]]; then
        mkdir -p "$(dirname "$OUTPUT_FILE")"
        echo "Results will be saved to: $OUTPUT_FILE"
        exec > >(tee "$OUTPUT_FILE") 2>&1
    fi

    if [[ -n "$CUSTOM_FILE" ]]; then
        if [[ ! -f "$CUSTOM_FILE" ]]; then
            echo "Error: File not found: $CUSTOM_FILE" >&2
            exit 1
        fi
        
        # Auto-detect dialect
        local dialect="mysql"
        if grep -q "pg_dump\|COPY.*FROM stdin" "$CUSTOM_FILE" 2>/dev/null; then
            dialect="postgres"
        elif grep -q "sqlite\|PRAGMA" "$CUSTOM_FILE" 2>/dev/null; then
            dialect="sqlite"
        fi
        
        echo "Using file: $CUSTOM_FILE (detected dialect: $dialect)"
        
        if [[ "$GENERATE_ONLY" == "true" ]]; then
            echo "Warning: --generate-only has no effect with --file"
            exit 0
        fi
        
        run_all_profiles "$CUSTOM_FILE" "$dialect"
    else
        echo "Generating test fixtures (size: $SIZE)..."
        
        # Giga size is MySQL-only to save disk space (~10GB vs ~30GB)
        if [[ "$SIZE" == "giga" ]]; then
            echo ""
            echo "WARNING: giga generates ~10GB MySQL file. This may take 10-30 minutes."
            echo ""
            mysql_file=$(generate_fixture "mysql")
            
            if [[ "$GENERATE_ONLY" == "true" ]]; then
                echo ""
                echo "Fixtures generated:"
                echo "  MySQL:    $mysql_file ($(du -h "$mysql_file" | cut -f1))"
                echo "  (giga is MySQL-only to save disk space)"
                exit 0
            fi
            
            run_all_profiles "$mysql_file" "mysql"
        else
            mysql_file=$(generate_fixture "mysql")
            postgres_file=$(generate_fixture "postgres")
            sqlite_file=$(generate_fixture "sqlite")
            
            if [[ "$GENERATE_ONLY" == "true" ]]; then
                echo ""
                echo "Fixtures generated:"
                echo "  MySQL:    $mysql_file ($(du -h "$mysql_file" | cut -f1))"
                echo "  Postgres: $postgres_file ($(du -h "$postgres_file" | cut -f1))"
                echo "  SQLite:   $sqlite_file ($(du -h "$sqlite_file" | cut -f1))"
                exit 0
            fi
            
            run_all_profiles "$mysql_file" "mysql"
            run_all_profiles "$postgres_file" "postgres"
            run_all_profiles "$sqlite_file" "sqlite"
        fi
    fi
    
    echo "Profiling complete!"
}

main
