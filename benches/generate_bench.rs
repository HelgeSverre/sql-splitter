//! Benchmarks for synthetic data generation.
//!
//! Two altitudes:
//! - **Renderer-only primitive** (`renderer/row_batch_1000`): the allocation-lean
//!   `RowBatch` flush path with no per-column generator dispatch. This is the
//!   throughput ceiling — the cost of formatting rows once the values exist.
//! - **Configurable generation** (`generate/*`): the full compile → seed → per-column
//!   generator dispatch → render pipeline driven through the public
//!   [`Generate`] facade, rendered to the null sink so the measurement is CPU,
//!   not disk. Cases cover seeded vs unseeded runs, a forced family spill, and
//!   the profile → infer → generate path from a source dump.
//!
//! The gap between the renderer ceiling and the `generate/*` cases is the
//! "configurable-generation overhead" release gate (see
//! `benchmark-results/generate-baseline.md`). The large table/row matrix and
//! peak-RSS measurement live in `scripts/benchmark-generate.sh`, which drives
//! the release binary under GNU `time`.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use sql_splitter::generate::{CompileOptions, Generate};
use sql_splitter::parser::SqlDialect;
use sql_splitter::render::{RowBatch, SqlString};
use std::hint::black_box;

/// Hand-authored 10-table linear-FK-chain model exercising the core generators
/// (`sequence`, `string`, `integer`, ...). Every table is a root, so `rows`
/// sets each table's count uniformly.
const CHAIN_10: &str = "scripts/fixtures/bench_chain_10.yaml";
/// A synthetic source dump used for the profile → infer → generate case.
const REALWORLD_DUMP: &str = "tests/fixtures/generate/realworld_shapes.sql";
/// Render to the null device so the benchmark measures generation CPU, not
/// filesystem throughput. Every real run writes SQL, so this is the fair
/// isolation of the generation cost itself.
const NULL_SINK: &str = "/dev/null";

/// Rows per table for the criterion-scale cases. The shell harness covers the
/// 10K/1M-row and 100-table sizes; here we keep each iteration cheap so the
/// medians are stable.
const ROWS_PER_TABLE: u64 = 500;
const TABLES: u64 = 10;

fn compile_opts(rows: u64, seed: Option<u64>, family_budget_bytes: Option<u64>) -> CompileOptions {
    CompileOptions {
        seed,
        rows: Some(rows),
        family_budget_bytes,
        ..Default::default()
    }
}

fn generate_chain(rows: u64, seed: Option<u64>, family_budget_bytes: Option<u64>) {
    let report = Generate::builder()
        .config(CHAIN_10)
        .output(NULL_SINK)
        .output_dialect(SqlDialect::MySql)
        .compile(compile_opts(rows, seed, family_budget_bytes))
        .run()
        .expect("chain model generates");
    black_box(report.rows_written);
}

fn bench_row_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("renderer");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("row_batch_1000", |b| {
        b.iter(|| {
            let mut rows = RowBatch::with_capacity(1000, 96_000);
            for i in 0..1000 {
                rows.push_fmt(format_args!(
                    "({}, {})",
                    i,
                    SqlString::new(SqlDialect::MySql, "name")
                ))
                .unwrap();
            }
            black_box(rows);
        });
    });
    group.finish();
}

fn bench_generate(c: &mut Criterion) {
    let total_rows = ROWS_PER_TABLE * TABLES;
    let mut group = c.benchmark_group("generate");
    group.sample_size(20);
    group.throughput(Throughput::Elements(total_rows));

    // Seeded configurable generation: the deterministic default path.
    group.bench_function("chain10_seeded", |b| {
        b.iter(|| generate_chain(ROWS_PER_TABLE, Some(42), None));
    });

    // Unseeded: a fresh random root seed each run (isolates seed-draw cost; the
    // per-row generator dispatch is identical).
    group.bench_function("chain10_unseeded", |b| {
        b.iter(|| generate_chain(ROWS_PER_TABLE, None, None));
    });

    // Forced family spill: a tiny family byte budget pushes correlated child
    // rows through the spool path. Output is byte-for-byte identical to the
    // unbounded run; only *where* rows are held changes.
    group.bench_function("chain10_spill_forced", |b| {
        b.iter(|| generate_chain(ROWS_PER_TABLE, Some(42), Some(4 * 1024)));
    });

    group.finish();
}

fn bench_profile_and_infer(c: &mut Criterion) {
    let mut group = c.benchmark_group("generate_infer");
    group.sample_size(20);

    // Full profile → infer → compile → generate from a source dump: the
    // configurable path that reads a dump and synthesizes a fresh dataset.
    group.bench_function("from_dump_basic", |b| {
        b.iter(|| {
            let report = Generate::builder()
                .input(REALWORLD_DUMP)
                .output(NULL_SINK)
                .seed(42)
                .run()
                .expect("profile-infer-generate from dump");
            black_box(report.rows_written);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_row_batch,
    bench_generate,
    bench_profile_and_infer
);
criterion_main!(benches);
