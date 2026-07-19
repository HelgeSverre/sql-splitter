#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_ROOT/target/debug/sql-splitter"
FIXTURE_DIR="$PROJECT_ROOT/tests/fixtures/generate"
ARTIFACT_ROOT="$PROJECT_ROOT/target/generate-smoke"
SEED=42

die() {
    echo "Error: $*" >&2
    exit 1
}

relative_path() {
    case $1 in
        "$PROJECT_ROOT"/*) printf '%s' "${1#"$PROJECT_ROOT"/}" ;;
        *) printf '%s' "$1" ;;
    esac
}

parse_report() {
    python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as report_file:
    report = json.load(report_file)

rows = report.get("rows_written")
if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
    raise ValueError("successful report has no valid non-negative rows_written")

warnings = {
    diagnostic["code"]
    for diagnostic in report.get("diagnostics", [])
    if diagnostic.get("severity") in {"warning", "advisory"}
    and diagnostic.get("code")
}

print(rows)
print(",".join(sorted(warnings)) or "-")
PY
}

merge_warning_codes() {
    local merged
    merged=$(
        printf '%s\n%s\n' "$1" "$2" \
            | tr ',' '\n' \
            | awk 'NF && $0 != "-" && !seen[$0]++' \
            | LC_ALL=C sort \
            | paste -sd, -
    )
    printf '%s' "${merged:--}"
}

command -v python3 >/dev/null 2>&1 || die "python3 is required"
[[ -x "$BINARY" ]] || die "debug binary not found at $(relative_path "$BINARY"); run cargo build"
[[ -d "$FIXTURE_DIR" ]] || die "fixture directory not found: $(relative_path "$FIXTURE_DIR")"
mkdir -p "$ARTIFACT_ROOT" || die "cannot create artifact directory: $(relative_path "$ARTIFACT_ROOT")"
[[ -w "$ARTIFACT_ROOT" ]] || die "artifact directory is not writable: $(relative_path "$ARTIFACT_ROOT")"

export LC_ALL=C
shopt -s nullglob
fixtures=("$FIXTURE_DIR"/*.sql)
shopt -u nullglob
((${#fixtures[@]} > 0)) || die "no SQL fixtures found in $(relative_path "$FIXTURE_DIR")"

pass_count=0
warn_count=0
empty_count=0
fail_count=0
mismatch_count=0

echo "generate fixture smoke test"
echo "Artifacts: $(relative_path "$ARTIFACT_ROOT")"
echo

for fixture in "${fixtures[@]}"; do
    fixture_name="$(basename "$fixture" .sql)"
    case_dir="$ARTIFACT_ROOT/$fixture_name"
    mkdir -p "$case_dir" || die "cannot create $(relative_path "$case_dir")"

    model="$case_dir/model.yaml"
    direct_sql="$case_dir/direct.sql"
    direct_json="$case_dir/direct.json"
    direct_stderr="$case_dir/direct.stderr"
    replay_sql="$case_dir/replay.sql"
    replay_json="$case_dir/replay.json"
    replay_stderr="$case_dir/replay.stderr"

    rm -f -- "$model" "$direct_sql" "$direct_json" "$direct_stderr" \
        "$replay_sql" "$replay_json" "$replay_stderr"

    "$BINARY" generate "$fixture" --seed "$SEED" \
        --emit-config "$model" --output "$direct_sql" --json \
        >"$direct_json" 2>"$direct_stderr"
    direct_status=$?

    if ((direct_status != 0)); then
        printf 'FAIL       %-32s direct exit %d; see %s\n' \
            "$fixture_name" "$direct_status" "$(relative_path "$case_dir")"
        fail_count=$((fail_count + 1))
        continue
    fi

    if ! direct_meta=$(parse_report "$direct_json" 2>>"$direct_stderr"); then
        printf 'FAIL       %-32s malformed direct report; see %s\n' \
            "$fixture_name" "$(relative_path "$case_dir")"
        fail_count=$((fail_count + 1))
        continue
    fi
    direct_rows=${direct_meta%%$'\n'*}
    direct_warnings=${direct_meta#*$'\n'}

    if ((direct_rows == 0)) || [[ ! -s "$direct_sql" ]]; then
        printf 'EMPTY      %-32s direct rows=%s; see %s\n' \
            "$fixture_name" "$direct_rows" "$(relative_path "$case_dir")"
        empty_count=$((empty_count + 1))
        continue
    fi

    "$BINARY" generate --config "$model" --seed "$SEED" \
        --output "$replay_sql" --json \
        >"$replay_json" 2>"$replay_stderr"
    replay_status=$?

    if ((replay_status != 0)); then
        printf 'FAIL       %-32s model replay exit %d; see %s\n' \
            "$fixture_name" "$replay_status" "$(relative_path "$case_dir")"
        fail_count=$((fail_count + 1))
        continue
    fi

    if ! replay_meta=$(parse_report "$replay_json" 2>>"$replay_stderr"); then
        printf 'FAIL       %-32s malformed replay report; see %s\n' \
            "$fixture_name" "$(relative_path "$case_dir")"
        fail_count=$((fail_count + 1))
        continue
    fi
    replay_rows=${replay_meta%%$'\n'*}
    replay_warnings=${replay_meta#*$'\n'}

    if ((replay_rows == 0)) || [[ ! -s "$replay_sql" ]]; then
        printf 'EMPTY      %-32s replay rows=%s; see %s\n' \
            "$fixture_name" "$replay_rows" "$(relative_path "$case_dir")"
        empty_count=$((empty_count + 1))
        continue
    fi

    if ! cmp -s "$direct_sql" "$replay_sql"; then
        printf 'MISMATCH   %-32s direct and replay SQL differ; see %s\n' \
            "$fixture_name" "$(relative_path "$case_dir")"
        mismatch_count=$((mismatch_count + 1))
        continue
    fi

    warning_codes=$(merge_warning_codes "$direct_warnings" "$replay_warnings")
    if [[ "$warning_codes" == "-" ]]; then
        printf 'PASS       %s\n' "$fixture_name"
        pass_count=$((pass_count + 1))
    else
        printf 'WARN       %-32s %s\n' "$fixture_name" "$warning_codes"
        warn_count=$((warn_count + 1))
    fi
done

echo
printf 'Summary: PASS=%d WARN=%d EMPTY=%d FAIL=%d MISMATCH=%d\n' \
    "$pass_count" "$warn_count" "$empty_count" "$fail_count" "$mismatch_count"

if ((empty_count > 0 || fail_count > 0 || mismatch_count > 0)); then
    exit 1
fi
