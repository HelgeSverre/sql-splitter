#!/usr/bin/env bash
#
# Memory benchmark script for the validate command.
#
# Measures peak RSS (resident set size) for various workloads.
# Requires GNU time (gtime on macOS, /usr/bin/time on Linux).
#
# Usage:
#   ./scripts/bench-validate-memory.sh [options]
#
# Options:
#   --scales SCALES     Comma-separated scales to test (default: small,medium,large)
#   --dialects DIALECTS Comma-separated dialects (default: mysql)
#   --output DIR        Output directory for results (default: benchmark-results/memory)
#   --skip-generate     Skip regenerating test fixtures
#   --no-fk             Run with --no-fk-checks to measure baseline memory
#   --help              Show this help
#
# Examples:
#   ./scripts/bench-validate-memory.sh
#   ./scripts/bench-validate-memory.sh --scales medium,large,xlarge
#   ./scripts/bench-validate-memory.sh --scales xlarge --no-fk

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
SCALES="small,medium,large"
DIALECTS="mysql"
OUTPUT_DIR="$PROJECT_ROOT/benchmark-results/memory"
FIXTURES_DIR="$PROJECT_ROOT/benchmark-results/memory/fixtures"
SKIP_GENERATE=false
NO_FK=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --scales)
            SCALES="$2"
            shift 2
            ;;
        --dialects)
            DIALECTS="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --skip-generate)
            SKIP_GENERATE=true
            shift
            ;;
        --no-fk)
            NO_FK=true
            shift
            ;;
        --help)
            head -30 "$0" | tail -25
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Detect GNU time
if command -v gtime &>/dev/null; then
    TIME_CMD="gtime"
elif [[ -x /usr/bin/time ]]; then
    TIME_CMD="/usr/bin/time"
else
    echo "Error: GNU time not found. Install with 'brew install gnu-time' on macOS."
    exit 1
fi

# Verify GNU time supports -v (output goes to stderr)
TIME_OUTPUT=$($TIME_CMD -v true 2>&1)
if ! echo "$TIME_OUTPUT" | grep -q "Maximum resident set size"; then
    echo "Error: $TIME_CMD does not support -v flag (not GNU time?)"
    echo "Output was: $TIME_OUTPUT"
    exit 1
fi

echo "=== SQL Splitter Validate Memory Benchmark ==="
echo ""
echo "Time command: $TIME_CMD"
echo "Scales: $SCALES"
echo "Dialects: $DIALECTS"
echo "Output: $OUTPUT_DIR"
echo ""

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$FIXTURES_DIR"

# Build release binary
echo "Building release binary..."
cargo build --release --quiet
BINARY="$PROJECT_ROOT/target/release/sql-splitter"

if [[ ! -x "$BINARY" ]]; then
    echo "Error: Binary not found at $BINARY"
    exit 1
fi

# Build generator
echo "Building test data generator..."
cargo build --release --quiet -p test_data_gen
GEN_BINARY="$PROJECT_ROOT/target/release/gen-fixtures"

if [[ ! -x "$GEN_BINARY" ]]; then
    echo "Error: Generator not found at $GEN_BINARY"
    exit 1
fi

# Generate test fixtures if needed
IFS=',' read -ra SCALE_ARR <<< "$SCALES"
IFS=',' read -ra DIALECT_ARR <<< "$DIALECTS"

if [[ "$SKIP_GENERATE" == "false" ]]; then
    echo ""
    echo "Generating test fixtures..."
    for scale in "${SCALE_ARR[@]}"; do
        for dialect in "${DIALECT_ARR[@]}"; do
            fixture_file="$FIXTURES_DIR/${dialect}_${scale}.sql"
            echo "  Generating $fixture_file..."
            "$GEN_BINARY" --dialect "$dialect" --scale "$scale" --seed 12345 --output "$fixture_file"
            file_size=$(du -h "$fixture_file" | cut -f1)
            echo "    Size: $file_size"
        done
    done
fi

echo ""
echo "Running benchmarks..."
echo ""

# Results file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$OUTPUT_DIR/results_$TIMESTAMP.csv"
echo "scale,dialect,fk_checks,file_size_mb,peak_rss_kb,elapsed_secs,statements" > "$RESULTS_FILE"

# Summary for console
printf "%-10s %-10s %-8s %12s %12s %10s\n" "Scale" "Dialect" "FK" "File (MB)" "RSS (MB)" "Time"
printf "%s\n" "----------------------------------------------------------------------"

run_benchmark() {
    local scale=$1
    local dialect=$2
    local fk_flag=$3
    local fixture_file="$FIXTURES_DIR/${dialect}_${scale}.sql"
    
    if [[ ! -f "$fixture_file" ]]; then
        echo "Warning: Fixture not found: $fixture_file"
        return
    fi
    
    local file_size_bytes
    file_size_bytes=$(stat -f%z "$fixture_file" 2>/dev/null || stat -c%s "$fixture_file")
    local file_size_mb
    file_size_mb=$(echo "scale=2; $file_size_bytes / 1048576" | bc)
    
    local time_log="$OUTPUT_DIR/time_${scale}_${dialect}_${fk_flag}.log"
    local validate_log="$OUTPUT_DIR/validate_${scale}_${dialect}_${fk_flag}.log"
    
    local extra_flags=""
    local fk_label="yes"
    if [[ "$fk_flag" == "no-fk" ]]; then
        extra_flags="--no-fk-checks"
        fk_label="no"
    fi
    
    # Run with time measurement
    $TIME_CMD -v "$BINARY" validate \
        --dialect "$dialect" \
        --max-rows-per-table 100000000 \
        $extra_flags \
        "$fixture_file" \
        2> "$time_log" \
        > "$validate_log" || true
    
    # Extract metrics
    local peak_rss_kb
    peak_rss_kb=$(grep "Maximum resident set size" "$time_log" | awk '{print $NF}')
    
    local elapsed
    elapsed=$(grep "Elapsed (wall clock)" "$time_log" | awk -F': ' '{print $2}')
    
    # Convert elapsed to seconds (handles both m:ss and h:mm:ss formats)
    local elapsed_secs
    if [[ "$elapsed" =~ ^([0-9]+):([0-9.]+)$ ]]; then
        local mins=${BASH_REMATCH[1]}
        local secs=${BASH_REMATCH[2]}
        elapsed_secs=$(echo "$mins * 60 + $secs" | bc)
    elif [[ "$elapsed" =~ ^([0-9]+):([0-9]+):([0-9.]+)$ ]]; then
        local hours=${BASH_REMATCH[1]}
        local mins=${BASH_REMATCH[2]}
        local secs=${BASH_REMATCH[3]}
        elapsed_secs=$(echo "$hours * 3600 + $mins * 60 + $secs" | bc)
    else
        elapsed_secs="$elapsed"
    fi
    
    # Get statement count from validate output
    local statements
    statements=$(grep "Statements scanned" "$validate_log" | awk '{print $NF}' || echo "0")
    
    # Calculate RSS in MB
    local peak_rss_mb
    peak_rss_mb=$(echo "scale=2; $peak_rss_kb / 1024" | bc)
    
    # Output
    printf "%-10s %-10s %-8s %12s %12s %10s\n" "$scale" "$dialect" "$fk_label" "$file_size_mb" "$peak_rss_mb" "${elapsed_secs}s"
    
    # Write to CSV
    echo "$scale,$dialect,$fk_label,$file_size_mb,$peak_rss_kb,$elapsed_secs,$statements" >> "$RESULTS_FILE"
}

# Run benchmarks
for scale in "${SCALE_ARR[@]}"; do
    for dialect in "${DIALECT_ARR[@]}"; do
        run_benchmark "$scale" "$dialect" "with-fk"
        if [[ "$NO_FK" == "true" ]]; then
            run_benchmark "$scale" "$dialect" "no-fk"
        fi
    done
done

echo ""
echo "----------------------------------------------------------------------"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""

# Show memory efficiency (bytes per row)
echo "Memory Efficiency Analysis:"
echo ""
printf "%-10s %-10s %12s %15s %18s\n" "Scale" "Dialect" "Statements" "RSS (MB)" "Bytes/Statement"
printf "%s\n" "----------------------------------------------------------------------"

while IFS=',' read -r scale dialect fk file_mb rss_kb elapsed statements; do
    [[ "$scale" == "scale" ]] && continue  # Skip header
    [[ "$fk" != "yes" ]] && continue  # Only show FK-enabled runs
    
    if [[ "$statements" -gt 0 ]]; then
        bytes_per_stmt=$(echo "scale=2; ($rss_kb * 1024) / $statements" | bc)
        rss_mb=$(echo "scale=2; $rss_kb / 1024" | bc)
        printf "%-10s %-10s %12s %15s %18s\n" "$scale" "$dialect" "$statements" "$rss_mb" "$bytes_per_stmt"
    fi
done < "$RESULTS_FILE"

echo ""
echo "Done!"
