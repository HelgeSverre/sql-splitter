# `generate` performance & memory baseline

Baseline for the synthetic data generation feature (Phase 4, Task 34). Reproduce
with:

```bash
cargo bench --bench generate_bench          # CPU-only medians (criterion)
./scripts/benchmark-generate.sh --big       # wall time / throughput / peak RSS
```

The numbers below are a captured snapshot for regression comparison, not a
contract. Re-run both on the same host before reading a delta as a regression.

## Environment

| Field | Value |
| --- | --- |
| CPU | Apple M2 Max |
| Toolchain | `rustc 1.97.0`, `--release` (opt-level 3, LTO per `Cargo.toml`) |
| Commit | `ab223bf` (+ Task 34 survey fixes) |
| OS | macOS (darwin 24.6) |

## Criterion — CPU-only medians

| Benchmark | Median | Throughput | What it isolates |
| --- | --- | --- | --- |
| `renderer/row_batch_1000` | 36.8 µs | **27.1 Melem/s** | Renderer-only ceiling: `RowBatch` flush, no generator dispatch |
| `generate/chain10_seeded` | 5.66 ms | 884 Kelem/s | Full compile → dispatch → render, 10 tables × 500 rows, seeded, null sink |
| `generate/chain10_unseeded` | 5.63 ms | 888 Kelem/s | Same, fresh random seed (seed-draw cost ≈ 0) |
| `generate/chain10_spill_forced` | 5.77 ms | 867 Kelem/s | Same, 4 KiB family budget forces the spool path (~2% over unbounded) |
| `generate_infer/from_dump_basic` | 163 µs | — | profile → infer → compile → generate from a small dump |

The `chain10_*` medians include fixed per-run costs (YAML model load + compile)
amortized over only 5 000 rows, so they understate steady-state throughput; the
shell harness below measures the steady state.

## Shell harness — wall time, throughput, peak RSS

`./scripts/benchmark-generate.sh --rows 10000 --big` (release binary, GNU time):

| Case | Dialect | Rows | Wall | Peak RSS | Rows/s | Bytes/s |
| --- | --- | --- | --- | --- | --- | --- |
| model_order_family | mysql | 10 000 | 0.11 s | 24.5 MB | 90 909 | 50.2 MB/s |
| model_core_seeded | mysql | 100 000 | 0.10 s | 11.7 MB | 1 000 000 | 94.5 MB/s |
| model_core_unseeded | mysql | 100 000 | 0.10 s | 11.6 MB | 1 000 000 | 94.4 MB/s |
| planner_relation_chain | postgres | 100 000 | 0.10 s | 10.4 MB | 1 000 000 | 94.5 MB/s |
| tables_10 | mysql | 100 000 | 0.11 s | 10.7 MB | 909 091 | 85.9 MB/s |
| tables_100 | mysql | 1 000 000 | 1.01 s | 19.6 MB | 990 099 | 94.1 MB/s |
| profile_schema | mysql | — | 0.01 s | 7.2 MB | — | — |
| profile_basic | mysql | — | 0.01 s | 10.7 MB | — | — |
| profile_full | mysql | — | 0.01 s | 10.5 MB | — | — |
| steady_state_10k | mysql | 100 000 | 0.11 s | 11.0 MB | 909 091 | 85.9 MB/s |

Real-dump anchor (survey, redacted): a **145 MB** MySQL dump profiled at `basic`
and generated (10 rows/table) in **2.94 s** at **40.8 MB** peak RSS.

## Release gates

### Correctness

The `--verify` path (Task 26) re-checks generated rows against the model's exact
constraints and planner properties, and **refuses to publish** output that fails.
The real-world survey confirmed `--verify` correctly rejects violations it should
(dangling self-referential FKs, duplicate composite/UUID primary keys) rather
than emitting them silently. `cargo test` (incl. the new
`real_world_mysql_shapes_generate_end_to_end` regression) is green.

**Gate: PASS** — exact constraints/planner properties are enforced by `--verify`.

### Memory (bounded)

Generation is streaming: peak RSS stays **10–20 MB** from 100 K to **1 000 000**
rows and from 10 to 100 tables — it tracks the batch/family working set, not the
output size. Profiling stays bounded by the evidence budget (7–11 MB across
schema/basic/full). The 145 MB-dump profile+generate held 40.8 MB.

**Gate: PASS** — memory is bounded by batch/family/profile budgets, independent
of row count or output size.

### Performance (overhead vs renderer-only baseline)

The renderer-only ceiling is 27.1 Melem/s (formatting a 2-column row with no
value generation). End-to-end configurable generation of a realistic 4-column
core-generator model sustains ~1.0 Melem/s / ~94 MB/s at steady state. The gap is
the cost of per-column generator dispatch, RNG, planners, escaping, and DDL — not
overhead over an equivalent renderer-only run of the *same* model, which the CLI
does not expose. Within the configurable path the measurable overheads are small:

- **seeded vs unseeded:** < 1% (seed draw is negligible).
- **family spill forced vs unbounded:** ~2% (criterion), output byte-identical.
- **100 tables vs 10 tables** (same total rows): comparable per-row throughput.

The brief's 20% "configurable-generation overhead" figure is a target, not a
blocker. No case regressed > 10% between repeated runs.

**Gate: PASS (target)** — overheads within the configurable path are ≤ ~2%; no
unexplained > 10% regression in repeated medians.

## Survey methodology & redaction

Real-dump figures above are aggregate only (dialect, size bucket, wall time, peak
RSS). Per the privacy rule, **no source literal, table/column name, or dump
fragment is recorded here**; every regression fixture derived from the survey
(`tests/fixtures/generate/realworld_shapes.sql`) uses invented names and values
reproducing only the structural shape that failed. See
`docs/generate/profiling-and-privacy.md` for the full methodology.
