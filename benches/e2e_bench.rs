use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sql_splitter::analyzer::Analyzer;
use sql_splitter::splitter::Splitter;
use std::fs;
use tempfile::TempDir;

fn generate_test_dump(tables: usize, rows_per_table: usize) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("test_dump.sql");

    let mut data = String::new();

    for t in 0..tables {
        let table_name = format!("table_{}", t);
        data.push_str(&format!(
            "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), data TEXT);\n",
            table_name
        ));

        for r in 0..rows_per_table {
            data.push_str(&format!(
                "INSERT INTO {} VALUES ({}, 'User {}', 'user{}@example.com', 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor.');\n",
                table_name, r, r, r
            ));
        }
    }

    fs::write(&input_file, data).unwrap();

    (temp_dir, input_file)
}

fn bench_splitter_e2e(c: &mut Criterion) {
    let mut group = c.benchmark_group("splitter_e2e");
    group.sample_size(20);

    for (tables, rows) in [(10, 1000), (50, 500), (100, 200)] {
        let (temp_dir, input_file) = generate_test_dump(tables, rows);
        let file_size = fs::metadata(&input_file).unwrap().len();

        group.throughput(Throughput::Bytes(file_size));
        group.bench_with_input(
            BenchmarkId::new("split", format!("{}t_{}r", tables, rows)),
            &input_file,
            |b, input_file| {
                b.iter_with_setup(
                    || {
                        let output_dir = temp_dir.path().join("output");
                        if output_dir.exists() {
                            fs::remove_dir_all(&output_dir).unwrap();
                        }
                        output_dir
                    },
                    |output_dir| {
                        let splitter = Splitter::new(input_file.clone(), output_dir);
                        splitter.split().unwrap()
                    },
                )
            },
        );
    }

    group.finish();
}

fn bench_analyzer_e2e(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyzer_e2e");
    group.sample_size(20);

    for (tables, rows) in [(10, 1000), (50, 500), (100, 200)] {
        let (_temp_dir, input_file) = generate_test_dump(tables, rows);
        let file_size = fs::metadata(&input_file).unwrap().len();

        group.throughput(Throughput::Bytes(file_size));
        group.bench_with_input(
            BenchmarkId::new("analyze", format!("{}t_{}r", tables, rows)),
            &input_file,
            |b, input_file| {
                b.iter(|| {
                    let analyzer = Analyzer::new(input_file.clone());
                    analyzer.analyze().unwrap()
                })
            },
        );
    }

    group.finish();
}

fn bench_dry_run(c: &mut Criterion) {
    let mut group = c.benchmark_group("dry_run");
    group.sample_size(20);

    let (temp_dir, input_file) = generate_test_dump(50, 1000);
    let file_size = fs::metadata(&input_file).unwrap().len();

    group.throughput(Throughput::Bytes(file_size));

    group.bench_function("with_dry_run", |b| {
        b.iter(|| {
            let output_dir = temp_dir.path().join("output");
            let splitter = Splitter::new(input_file.clone(), output_dir).with_dry_run(true);
            splitter.split().unwrap()
        })
    });

    group.bench_function("without_dry_run", |b| {
        b.iter_with_setup(
            || {
                let output_dir = temp_dir.path().join("output");
                if output_dir.exists() {
                    fs::remove_dir_all(&output_dir).unwrap();
                }
                output_dir
            },
            |output_dir| {
                let splitter = Splitter::new(input_file.clone(), output_dir);
                splitter.split().unwrap()
            },
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_splitter_e2e,
    bench_analyzer_e2e,
    bench_dry_run
);

criterion_main!(benches);
