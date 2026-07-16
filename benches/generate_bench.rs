//! Benchmarks for the allocation-lean renderer primitives.
//!
//! Tests:
//! - Row batch throughput for a representative multi-tenant flush size

use criterion::{criterion_group, criterion_main, Criterion};
use sql_splitter::parser::SqlDialect;
use sql_splitter::render::{RowBatch, SqlString};
use std::hint::black_box;

fn bench_row_batch(c: &mut Criterion) {
    c.bench_function("generate/row_batch_1000", |b| {
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
}

criterion_group!(benches, bench_row_batch);
criterion_main!(benches);
