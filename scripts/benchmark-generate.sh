#!/usr/bin/env bash
#
# Reproducible wall-clock / throughput / peak-RSS harness for `generate`.
#
# Complements `cargo bench --bench generate_bench` (which measures CPU-only
# medians for the renderer primitive and the small model cases). This script
# drives the RELEASE BINARY end to end under GNU `time`, across a table×row
# matrix, recording wall time, rows/s, output bytes/s, and peak RSS — the
# numbers behind `benchmark-results/generate-baseline.md`.
#
# Cases (the renderer-only ceiling is the criterion `renderer/row_batch_1000`
# case, not a shell case):
#   - hand-authored order-family planner model (money aggregates + line items)
#   - hand-authored model with the core generators (seeded)
#   - seeded vs unseeded (RNG-draw overhead)
#   - profile depths (basic / full) against a source dump
#   - one exemplar planner (relation.children via the FK chain)
#   - family spill forced vs not forced (byte-identical output)
#   - 1 / 10 / 100 tables at a fixed rows-per-table
#   - a large single-run (10K rows/table × 10 tables) for steady-state throughput
#
# Usage:
#   ./scripts/benchmark-generate.sh [--rows N] [--big] [--output FILE]
#
# Options:
#   --rows N        Rows per table for the matrix cases (default: 2000)
#   --big           Also run the 10K-rows/table steady-state case (slower)
#   --output FILE   Tee results to FILE in addition to stdout
#
# Requirements: GNU time (gtime on macOS: `brew install gnu-time`).

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_ROOT/target/release/sql-splitter"
CHAIN_10="$SCRIPT_DIR/fixtures/bench_chain_10.yaml"
CHAIN_100="$SCRIPT_DIR/fixtures/bench_chain_100.yaml"
RENDERER_MODEL="$PROJECT_ROOT/tests/fixtures/generate/simple.yaml"
DUMP="$PROJECT_ROOT/tests/fixtures/generate/realworld_shapes.sql"
SEED=42

ROWS=2000
RUN_BIG=false
OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --rows)   ROWS="$2"; shift 2 ;;
        --big)    RUN_BIG=true; shift ;;
        --output) OUTPUT_FILE="$2"; shift 2 ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Error: unknown option: $1" >&2; exit 1 ;;
    esac
done

detect_time_cmd() {
    if command -v gtime &>/dev/null; then
        echo "gtime"
    elif [[ -x /usr/bin/time ]] && /usr/bin/time --version 2>&1 | grep -q "GNU"; then
        echo "/usr/bin/time"
    else
        echo "Error: GNU time not found. On macOS: brew install gnu-time." >&2
        exit 1
    fi
}
TIME_CMD=$(detect_time_cmd)

if [[ ! -x "$BINARY" ]]; then
    echo "Building release binary..."
    cargo build --release --manifest-path "$PROJECT_ROOT/Cargo.toml"
fi

cpu_model() {
    if [[ "$(uname)" == "Darwin" ]]; then
        sysctl -n machdep.cpu.brand_string 2>/dev/null
    else
        grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | sed 's/^ *//'
    fi
}

file_bytes() {
    if [[ "$(uname)" == "Darwin" ]]; then stat -f%z "$1" 2>/dev/null; else stat -c%s "$1" 2>/dev/null; fi
}

# Robust numeric formatting: pass values through awk `-v` so no shell text is
# ever interpolated into the awk program.
to_mb()   { awk -v kb="${1:-0}" 'BEGIN{printf "%.1f", kb/1024}'; }
rate()    { awk -v n="${1:-0}" -v s="${2:-0}" 'BEGIN{ if (s>0) printf "%.0f", n/s; else printf "n/a" }'; }
mb_rate() { awk -v b="${1:-0}" -v s="${2:-0}" 'BEGIN{ if (s>0) printf "%.1f", b/s/1048576; else printf "n/a" }'; }
# Wall clock is m:ss.xx or ss.xx — reduce to seconds.
to_secs() { awk -F: -v w="$1" 'BEGIN{ n=split(w,p,":"); if (n==2) print p[1]*60+p[2]; else print p[1] }'; }

# run_case NAME CONFIG DIALECT ROWS EXTRA...
# Generates to a temp file under GNU time and prints one result row. When EXTRA
# contains `--randomize`, the run is unseeded (the CLI rejects `--seed` there).
run_case() {
    local name=$1 config=$2 dialect=$3 rows=$4
    shift 4
    local extra=("$@")
    local out; out=$(mktemp)
    local tlog; tlog=$(mktemp)

    local seed_args=(--seed "$SEED")
    for arg in "${extra[@]}"; do
        [[ "$arg" == "--randomize" ]] && seed_args=()
    done

    $TIME_CMD -v "$BINARY" generate \
        --config "$config" --dialect "$dialect" --rows "$rows" \
        "${seed_args[@]}" --output "$out" --quiet "${extra[@]}" \
        2>"$tlog" >/dev/null || { echo "  $name: FAILED"; grep -m3 "error" "$tlog" | sed 's/^/    /' >&2; rm -f "$out" "$tlog"; return; }

    local rss_kb wall bytes secs
    rss_kb=$(grep "Maximum resident set size" "$tlog" | awk '{print $NF}')
    wall=$(grep "Elapsed (wall clock)" "$tlog" | awk '{print $NF}')
    bytes=$(file_bytes "$out")
    secs=$(to_secs "$wall")

    # chain models: every table is a root, so total rows = rows × table_count.
    local tables=1
    [[ "$config" == "$CHAIN_10" ]] && tables=10
    [[ "$config" == "$CHAIN_100" ]] && tables=100
    local rows_total=$((rows * tables))

    printf "%-26s %-9s %9s %11s %9s MB %11s %9s MB/s\n" \
        "$name" "$dialect" "$rows_total" "${wall:-n/a}" \
        "$(to_mb "$rss_kb")" "$(rate "$rows_total" "$secs")" "$(mb_rate "$bytes" "$secs")"

    rm -f "$out" "$tlog"
}

main() {
    if [[ -n "$OUTPUT_FILE" ]]; then
        mkdir -p "$(dirname "$OUTPUT_FILE")"
        exec > >(tee "$OUTPUT_FILE") 2>&1
    fi

    echo "============================================================"
    echo "generate benchmark harness"
    echo "============================================================"
    echo "Commit:  $(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "CPU:     $(cpu_model)"
    echo "Rustc:   $(rustc --version 2>/dev/null)"
    echo "Date:    $(date)"
    echo "Rows/table (matrix): $ROWS"
    echo "------------------------------------------------------------"
    printf "%-26s %-9s %9s %11s %12s %11s %14s\n" \
        "Case" "Dialect" "Rows" "Wall" "Peak RSS" "Rows/s" "Bytes/s"
    echo "------------------------------------------------------------"

    # Fixed hand-authored model exercising the commerce.order_family planner
    # (money aggregates + child line items). The renderer-only *ceiling* — pure
    # row formatting with no generator dispatch — is the criterion
    # `renderer/row_batch_1000` case, not a shell case.
    run_case "model_order_family" "$RENDERER_MODEL" mysql "$ROWS"

    # Core-generator model, seeded vs unseeded (the --randomize flag drops the seed).
    run_case "model_core_seeded" "$CHAIN_10" mysql "$ROWS"
    run_case "model_core_unseeded" "$CHAIN_10" mysql "$ROWS" --randomize

    # Exemplar planner: the chain model's parent_id FK exercises relation.children.
    run_case "planner_relation_chain" "$CHAIN_10" postgres "$ROWS"

    # Family spill forced-vs-not is a library-only option (`family_budget_bytes`,
    # no CLI flag), so it is measured by `generate/chain10_spill_forced` in
    # `cargo bench --bench generate_bench` rather than here.

    # Table-count scaling: 1 / 10 / 100 tables at fixed rows/table.
    run_case "tables_10" "$CHAIN_10" mysql "$ROWS"
    run_case "tables_100" "$CHAIN_100" mysql "$ROWS"

    # Profile depths against a source dump (profile → infer → generate).
    for depth in basic full; do
        local out; out=$(mktemp); local tlog; tlog=$(mktemp)
        if $TIME_CMD -v "$BINARY" generate "$DUMP" \
            --profile-depth "$depth" --rows "$ROWS" --seed "$SEED" \
            --output "$out" --quiet 2>"$tlog" >/dev/null; then
            :
        else
            local status=$?
            echo "profile_${depth}: FAILED" >&2
            sed -n '1,8p' "$tlog" >&2
            rm -f "$out" "$tlog"
            return "$status"
        fi
        local rss_kb wall
        rss_kb=$(grep "Maximum resident set size" "$tlog" | awk '{print $NF}')
        wall=$(grep "Elapsed (wall clock)" "$tlog" | awk '{print $NF}')
        printf "%-26s %-9s %9s %11s %9s MB %11s %9s\n" \
            "profile_${depth}" "mysql" "-" "${wall:-n/a}" \
            "$(to_mb "$rss_kb")" "-" "-"
        rm -f "$out" "$tlog"
    done

    if [[ "$RUN_BIG" == "true" ]]; then
        echo "------------------------------------------------------------"
        run_case "steady_state_10k" "$CHAIN_10" mysql 10000
    fi

    echo "------------------------------------------------------------"
    echo "Done."
}

main
