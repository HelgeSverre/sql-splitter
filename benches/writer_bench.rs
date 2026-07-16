use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sql_splitter::splitter::Compression;
use sql_splitter::writer::{ParallelWriters, ProfileKind, ProfileValues, WriterProfile};
use std::sync::Arc;
use tempfile::TempDir;

fn generate_statement(size: usize) -> Vec<u8> {
    let data = "x".repeat(size);
    format!("INSERT INTO t VALUES ('{}');", data).into_bytes()
}

/// A writer pool configured like production `split` (SSD profile defaults).
fn make_pool(dir: &std::path::Path, num_writers: usize) -> ParallelWriters {
    let profile = WriterProfile::for_kind(ProfileKind::Ssd, 4, false);
    let values = Arc::new(ProfileValues::new(&profile));
    ParallelWriters::new(
        dir.to_path_buf(),
        num_writers,
        16,
        Compression::None,
        values,
    )
    .unwrap()
}

fn bench_single_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_writers_single_table");

    for stmt_size in [100, 500, 1000, 5000] {
        let stmt = generate_statement(stmt_size);

        group.throughput(Throughput::Bytes(stmt.len() as u64 * 100));
        group.bench_with_input(
            BenchmarkId::new("write_100_stmts", format!("{}B", stmt_size)),
            &stmt,
            |b, stmt| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let writers = make_pool(temp_dir.path(), 1);
                        (temp_dir, writers)
                    },
                    |(_temp_dir, mut writers)| {
                        for _ in 0..100 {
                            writers.write("t", stmt, b"");
                        }
                        writers.finish().unwrap();
                    },
                )
            },
        );
    }

    group.finish();
}

fn bench_multi_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_writers_multi_table");

    let stmt = generate_statement(200);

    for num_tables in [5u64, 20, 50, 100] {
        group.throughput(Throughput::Elements(num_tables * 100));
        group.bench_with_input(
            BenchmarkId::new("multi_table_write", format!("{}_tables", num_tables)),
            &num_tables,
            |b, &num_tables| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let writers = make_pool(temp_dir.path(), 4);
                        (temp_dir, writers)
                    },
                    |(_temp_dir, mut writers)| {
                        for t in 0..num_tables {
                            let table_name = format!("table_{}", t);
                            for _ in 0..100 {
                                writers.write(&table_name, &stmt, b"");
                            }
                        }
                        writers.finish().unwrap();
                    },
                )
            },
        );
    }

    group.finish();
}

fn bench_sustained_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_writers_sustained");

    let stmt = generate_statement(500);
    let total_writes = 1000;

    group.throughput(Throughput::Bytes(stmt.len() as u64 * total_writes));

    group.bench_function("sustained_1000_writes", |b| {
        b.iter_with_setup(
            || {
                let temp_dir = TempDir::new().unwrap();
                let writers = make_pool(temp_dir.path(), 1);
                (temp_dir, writers)
            },
            |(_temp_dir, mut writers)| {
                for _ in 0..total_writes {
                    writers.write("t", &stmt, b"");
                }
                writers.finish().unwrap();
            },
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_table,
    bench_multi_table,
    bench_sustained_writes
);

criterion_main!(benches);
