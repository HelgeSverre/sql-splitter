use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use sql_splitter::parser::{Parser, StatementType, SMALL_BUFFER_SIZE};

fn generate_sql_data(num_statements: usize) -> Vec<u8> {
    let mut data = Vec::new();

    data.extend_from_slice(
        b"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255));\n",
    );

    for i in 0..num_statements {
        let stmt = format!(
            "INSERT INTO users VALUES ({}, 'User {}', 'user{}@example.com');\n",
            i, i, i
        );
        data.extend_from_slice(stmt.as_bytes());
    }

    data
}

fn generate_multi_table_data(tables: usize, rows_per_table: usize) -> Vec<u8> {
    let mut data = Vec::new();

    for t in 0..tables {
        let table_name = format!("table_{}", t);
        data.extend_from_slice(
            format!(
                "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(255), data TEXT);\n",
                table_name
            )
            .as_bytes(),
        );

        for r in 0..rows_per_table {
            let stmt = format!(
                "INSERT INTO {} VALUES ({}, 'Name {}', 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.');\n",
                table_name, r, r
            );
            data.extend_from_slice(stmt.as_bytes());
        }
    }

    data
}

fn bench_read_statement_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_throughput");

    for size in [1000, 10000, 50000] {
        let data = generate_sql_data(size);
        let data_size = data.len();

        group.throughput(Throughput::Bytes(data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("read_statement", format!("{}_stmts", size)),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut parser = Parser::new(&data[..], SMALL_BUFFER_SIZE);
                    let mut count = 0;
                    while let Ok(Some(_stmt)) = parser.read_statement() {
                        count += 1;
                    }
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

fn bench_buffer_sizes(c: &mut Criterion) {
    let data = generate_sql_data(10000);
    let data_size = data.len();

    let mut group = c.benchmark_group("buffer_sizes");
    group.throughput(Throughput::Bytes(data_size as u64));

    for buffer_size in [16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024] {
        group.bench_with_input(
            BenchmarkId::new("read_statement", format!("{}KB", buffer_size / 1024)),
            &buffer_size,
            |b, &buffer_size| {
                b.iter(|| {
                    let mut parser = Parser::new(&data[..], buffer_size);
                    let mut count = 0;
                    while let Ok(Some(_stmt)) = parser.read_statement() {
                        count += 1;
                    }
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

fn bench_parse_statement(c: &mut Criterion) {
    let stmts = vec![
        b"CREATE TABLE users (id INT PRIMARY KEY);".to_vec(),
        b"INSERT INTO users VALUES (1, 'test');".to_vec(),
        b"CREATE TABLE `my_table` (id INT);".to_vec(),
        b"INSERT INTO `posts` VALUES (1);".to_vec(),
        b"ALTER TABLE users ADD COLUMN status INT;".to_vec(),
        b"DROP TABLE temp_data;".to_vec(),
    ];

    c.bench_function("parse_statement_mixed", |b| {
        b.iter(|| {
            for stmt in &stmts {
                let result = Parser::<&[u8]>::parse_statement(black_box(stmt));
                black_box(result);
            }
        })
    });
}

fn bench_parse_statement_types(c: &mut Criterion) {
    let create_table = b"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));";
    let insert = b"INSERT INTO users VALUES (1, 'John Doe', 'john@example.com');";
    let create_backtick = b"CREATE TABLE `my_complex_table` (id INT);";
    let insert_backtick = b"INSERT INTO `my_complex_table` VALUES (1);";
    let alter_table = b"ALTER TABLE users ADD COLUMN email VARCHAR(255);";
    let drop_table = b"DROP TABLE temp_data;";
    let create_index = b"CREATE INDEX idx_users_email ON users (email);";

    let mut group = c.benchmark_group("parse_statement_types");

    group.bench_function("create_table", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(create_table)))
    });

    group.bench_function("insert", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(insert)))
    });

    group.bench_function("create_table_backtick", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(create_backtick)))
    });

    group.bench_function("insert_backtick", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(insert_backtick)))
    });

    group.bench_function("alter_table", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(alter_table)))
    });

    group.bench_function("drop_table", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(drop_table)))
    });

    group.bench_function("create_index", |b| {
        b.iter(|| Parser::<&[u8]>::parse_statement(black_box(create_index)))
    });

    group.finish();
}

fn bench_string_handling(c: &mut Criterion) {
    let simple = b"INSERT INTO t VALUES (1);";
    let with_string = b"INSERT INTO t VALUES ('hello world');";
    let with_semicolon = b"INSERT INTO t VALUES ('hello; world');";
    let with_escaped = b"INSERT INTO t VALUES ('it\\'s a test');";
    let with_long_string = b"INSERT INTO t VALUES ('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.');";

    let mut group = c.benchmark_group("string_handling");

    group.bench_function("simple", |b| {
        b.iter(|| {
            let mut parser = Parser::new(&simple[..], 1024);
            parser.read_statement().unwrap()
        })
    });

    group.bench_function("with_string", |b| {
        b.iter(|| {
            let mut parser = Parser::new(&with_string[..], 1024);
            parser.read_statement().unwrap()
        })
    });

    group.bench_function("with_semicolon_in_string", |b| {
        b.iter(|| {
            let mut parser = Parser::new(&with_semicolon[..], 1024);
            parser.read_statement().unwrap()
        })
    });

    group.bench_function("with_escaped_quote", |b| {
        b.iter(|| {
            let mut parser = Parser::new(&with_escaped[..], 1024);
            parser.read_statement().unwrap()
        })
    });

    group.bench_function("with_long_string", |b| {
        b.iter(|| {
            let mut parser = Parser::new(&with_long_string[..], 1024);
            parser.read_statement().unwrap()
        })
    });

    group.finish();
}

fn bench_multi_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_table");

    for (tables, rows) in [(5, 1000), (20, 500), (50, 200)] {
        let data = generate_multi_table_data(tables, rows);
        let data_size = data.len();

        group.throughput(Throughput::Bytes(data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("parse", format!("{}t_{}r", tables, rows)),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut parser = Parser::new(&data[..], SMALL_BUFFER_SIZE);
                    let mut statements = 0;
                    let mut tables_found = 0;
                    while let Ok(Some(stmt)) = parser.read_statement() {
                        let (stmt_type, _table) = Parser::<&[u8]>::parse_statement(&stmt);
                        if stmt_type == StatementType::CreateTable {
                            tables_found += 1;
                        }
                        statements += 1;
                    }
                    black_box((statements, tables_found))
                })
            },
        );
    }

    group.finish();
}

fn bench_large_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_statements");

    for size_kb in [1, 4, 16, 64] {
        let value_data = "x".repeat(size_kb * 1024);
        let stmt = format!("INSERT INTO t VALUES ('{}');", value_data);
        let stmt_bytes = stmt.as_bytes().to_vec();

        group.throughput(Throughput::Bytes(stmt_bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("read_large", format!("{}KB", size_kb)),
            &stmt_bytes,
            |b, data| {
                b.iter(|| {
                    let mut parser = Parser::new(&data[..], SMALL_BUFFER_SIZE);
                    parser.read_statement().unwrap()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_read_statement_throughput,
    bench_buffer_sizes,
    bench_parse_statement,
    bench_parse_statement_types,
    bench_string_handling,
    bench_multi_table,
    bench_large_statements,
);

criterion_main!(benches);
