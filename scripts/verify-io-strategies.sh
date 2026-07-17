#!/usr/bin/env bash
#
# Real-hardware acceptance test for --io-strategy (see
# docs/features/ADAPTIVE_IO_PROFILES.md, "Real-hardware acceptance").
#
# Usage: ./scripts/verify-io-strategys.sh <output-mount> [fixture-size-gb]
#
# Runs split with --io-strategy fast / hdd / auto against a generated fixture
# on the target mount and checks the RATIO gates (absolute MB/s varies run to
# run on real drives — ±25% observed on ExFAT USB HDDs):
#   - rotational media: hdd >= 1.5x fast; auto >= 0.85x hdd
#   - NVMe/SSD:         auto >= 0.90x fast
#
# Requires: gtime (brew install gnu-time), a release build.

set -euo pipefail

MOUNT="${1:?usage: verify-io-strategys.sh <output-mount> [size-gb]}"
SIZE_GB="${2:-4}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/release/sql-splitter"
MODEL="$ROOT/scripts/fixtures/bench_chain_100.yaml"
T="$MOUNT/sqlsplit-io-verify"

[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 1; }
mkdir -p "$T"
trap 'rm -rf "$T"' EXIT

echo "generating ${SIZE_GB}GB fixture on $MOUNT ..."
"$BIN" generate --config "$MODEL" --rows "$((SIZE_GB * 100000))" --seed 42 --output "$T/fixture.sql" --quiet

declare -A MBPS
for p in fast hdd auto; do
  rm -rf "$T/out"
  out=$("$BIN" split "$T/fixture.sql" -o "$T/out" --io-strategy "$p" 2>&1 | grep -Eo 'Throughput: [0-9.]+' | grep -Eo '[0-9.]+')
  MBPS[$p]=$out
  echo "  $p: ${out} MB/s"
  rm -rf "$T/out"
done

ratio() { echo "scale=2; ${MBPS[$1]} / ${MBPS[$2]}" | bc; }
echo ""
echo "hdd/fast  = $(ratio hdd fast)   (gate: >= 1.5 on rotational media)"
echo "auto/hdd  = $(ratio auto hdd)   (gate: >= 0.85 on rotational media)"
echo "auto/fast = $(ratio auto fast)  (gate: >= 0.90 on SSD/NVMe)"
echo ""
echo "Interpret against the device class; append results to BENCHMARKS.md."
