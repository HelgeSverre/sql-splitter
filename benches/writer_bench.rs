use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sql_splitter::writer::{TableWriter, WriterPool};
use std::fs;
use tempfile::TempDir;

fn generate_statement(size: usize) -> Vec<u8> {
    let data = "x".repeat(size);
    format!("INSERT INTO t VALUES ('{}');", data).into_bytes()
}

fn bench_table_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_writer");

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
                        let file_path = temp_dir.path().join("test.sql");
                        (temp_dir, TableWriter::new(&file_path).unwrap())
                    },
                    |(_temp_dir, mut writer)| {
                        for _ in 0..100 {
                            writer.write_statement(stmt).unwrap();
                        }
                        writer.flush().unwrap();
                    },
                )
            },
        );
    }

    group.finish();
}

fn bench_writer_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_pool");

    let stmt = generate_statement(200);

    for num_tables in [5, 20, 50, 100] {
        group.throughput(Throughput::Elements(num_tables * 100));
        group.bench_with_input(
            BenchmarkId::new("multi_table_write", format!("{}_tables", num_tables)),
            &num_tables,
            |b, &num_tables| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let output_dir = temp_dir.path().to_path_buf();
                        fs::create_dir_all(&output_dir).unwrap();
                        (temp_dir, WriterPool::new(output_dir))
                    },
                    |(_temp_dir, mut pool)| {
                        for t in 0..num_tables {
                            let table_name = format!("table_{}", t);
                            for _ in 0..100 {
                                pool.write_statement(&table_name, &stmt).unwrap();
                            }
                        }
                        pool.close_all().unwrap();
                    },
                )
            },
        );
    }

    group.finish();
}

fn bench_flush_frequency(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_patterns");

    let stmt = generate_statement(500);
    let total_writes = 1000;

    group.throughput(Throughput::Bytes(stmt.len() as u64 * total_writes));

    group.bench_function("auto_flush_every_100", |b| {
        b.iter_with_setup(
            || {
                let temp_dir = TempDir::new().unwrap();
                let file_path = temp_dir.path().join("test.sql");
                (temp_dir, TableWriter::new(&file_path).unwrap())
            },
            |(_temp_dir, mut writer)| {
                for _ in 0..total_writes {
                    writer.write_statement(&stmt).unwrap();
                }
                writer.flush().unwrap();
            },
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_table_writer,
    bench_writer_pool,
    bench_flush_frequency
);

criterion_main!(benches);
