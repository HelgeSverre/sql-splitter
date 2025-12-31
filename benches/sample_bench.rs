//! Benchmarks for the sample command and reservoir sampling.
//!
//! Tests:
//! - Reservoir sampling performance with varying sizes
//! - Sample throughput with different reservoir capacities
//! - Sampling from parsed SQL statements

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::SeedableRng;
use sql_splitter::sample::Reservoir;
use std::hint::black_box;

/// Benchmark reservoir sampling with varying item counts
fn bench_reservoir_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("reservoir_insertion");

    let capacity = 1000;

    for item_count in [1_000, 10_000, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(item_count));
        group.bench_with_input(
            BenchmarkId::new("consider", format!("{}_items", item_count)),
            &item_count,
            |b, &item_count| {
                b.iter(|| {
                    let rng = StdRng::seed_from_u64(42);
                    let mut reservoir: Reservoir<usize> = Reservoir::new(capacity, rng);
                    for i in 0..item_count as usize {
                        reservoir.consider(black_box(i));
                    }
                    black_box(reservoir.len())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark reservoir with varying capacities
fn bench_reservoir_capacity(c: &mut Criterion) {
    let mut group = c.benchmark_group("reservoir_capacity");

    let item_count = 100_000;
    group.throughput(Throughput::Elements(item_count));

    for capacity in [10, 100, 1_000, 10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::new("cap", format!("{}", capacity)),
            &capacity,
            |b, &capacity| {
                b.iter(|| {
                    let rng = StdRng::seed_from_u64(42);
                    let mut reservoir: Reservoir<usize> = Reservoir::new(capacity, rng);
                    for i in 0..item_count as usize {
                        reservoir.consider(black_box(i));
                    }
                    black_box(reservoir.len())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark reservoir sampling with SQL statements (byte slices)
fn bench_reservoir_sql_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("reservoir_sql");

    // Generate sample SQL statements
    let statements: Vec<Vec<u8>> = (0..10_000)
        .map(|i| {
            format!(
                "INSERT INTO users VALUES ({}, 'User {}', 'user{}@example.com');",
                i, i, i
            )
            .into_bytes()
        })
        .collect();

    let total_bytes: usize = statements.iter().map(|s| s.len()).sum();

    for capacity in [100, 500, 1000, 2000] {
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("sample", format!("{}_stmts", capacity)),
            &capacity,
            |b, &capacity| {
                b.iter(|| {
                    let rng = StdRng::seed_from_u64(42);
                    let mut reservoir: Reservoir<&[u8]> = Reservoir::new(capacity, rng);
                    for stmt in &statements {
                        reservoir.consider(black_box(stmt.as_slice()));
                    }
                    black_box(reservoir.len())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark reservoir with owned strings (memory allocation)
fn bench_reservoir_owned(c: &mut Criterion) {
    let mut group = c.benchmark_group("reservoir_owned");
    group.sample_size(50);

    let capacity = 1000;
    let item_count = 50_000;

    // Benchmark with owned strings (involves allocation)
    group.bench_function("owned_strings", |b| {
        b.iter(|| {
            let rng = StdRng::seed_from_u64(42);
            let mut reservoir: Reservoir<String> = Reservoir::new(capacity, rng);
            for i in 0..item_count {
                let s = format!("INSERT INTO t VALUES ({}, 'value_{}');", i, i);
                reservoir.consider(black_box(s));
            }
            black_box(reservoir.into_items())
        })
    });

    // Benchmark with byte vectors
    group.bench_function("owned_bytes", |b| {
        b.iter(|| {
            let rng = StdRng::seed_from_u64(42);
            let mut reservoir: Reservoir<Vec<u8>> = Reservoir::new(capacity, rng);
            for i in 0..item_count {
                let s = format!("INSERT INTO t VALUES ({}, 'value_{}');", i, i).into_bytes();
                reservoir.consider(black_box(s));
            }
            black_box(reservoir.into_items())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_reservoir_insertion,
    bench_reservoir_capacity,
    bench_reservoir_sql_statements,
    bench_reservoir_owned,
);

criterion_main!(benches);
