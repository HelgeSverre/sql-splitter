//! Benchmarks for the convert command and related functionality.
//!
//! Tests:
//! - Dialect conversion throughput (MySQL → PostgreSQL, etc.)
//! - COPY → INSERT conversion performance
//! - Type mapping performance
//! - Identifier quoting conversion

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sql_splitter::convert::{copy_to_inserts, parse_copy_header, CopyHeader, Converter};
use sql_splitter::parser::SqlDialect;
use std::hint::black_box;

/// Generate MySQL dump data for benchmarking
fn generate_mysql_dump(tables: usize, rows_per_table: usize) -> Vec<u8> {
    let mut data = String::new();

    // MySQL dump header
    data.push_str("-- MySQL dump 10.13\n");
    data.push_str("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n");
    data.push_str("SET NAMES utf8mb4;\n");
    data.push_str("SET FOREIGN_KEY_CHECKS = 0;\n\n");

    for t in 0..tables {
        let table_name = format!("table_{}", t);
        data.push_str(&format!(
            "CREATE TABLE `{}` (\n  `id` INT AUTO_INCREMENT PRIMARY KEY,\n  `name` VARCHAR(255),\n  `email` VARCHAR(255),\n  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n",
            table_name
        ));

        for r in 0..rows_per_table {
            data.push_str(&format!(
                "INSERT INTO `{}` VALUES ({}, 'User {}', 'user{}@example.com', '2024-01-01 12:00:00');\n",
                table_name, r, r, r
            ));
        }
        data.push('\n');
    }

    data.into_bytes()
}

/// Generate PostgreSQL dump data for benchmarking
fn generate_postgres_dump(tables: usize, rows_per_table: usize) -> Vec<u8> {
    let mut data = String::new();

    // pg_dump header
    data.push_str("-- PostgreSQL database dump\n");
    data.push_str("SET client_encoding = 'UTF8';\n");
    data.push_str("SET standard_conforming_strings = on;\n");
    data.push_str("SET search_path = public;\n\n");

    for t in 0..tables {
        let table_name = format!("table_{}", t);
        data.push_str(&format!(
            "CREATE TABLE public.\"{}\" (\n    id SERIAL PRIMARY KEY,\n    name character varying(255),\n    email character varying(255),\n    created_at timestamp with time zone DEFAULT now()\n);\n\n",
            table_name
        ));

        // Use COPY format for PostgreSQL
        data.push_str(&format!(
            "COPY public.\"{}\" (id, name, email, created_at) FROM stdin;\n",
            table_name
        ));
        for r in 0..rows_per_table {
            data.push_str(&format!(
                "{}\tUser {}\tuser{}@example.com\t2024-01-01 12:00:00+00\n",
                r, r, r
            ));
        }
        data.push_str("\\.\n\n");
    }

    data.into_bytes()
}

/// Generate COPY data block for benchmarking
fn generate_copy_data(rows: usize, cols: usize) -> Vec<u8> {
    let mut data = String::new();
    for r in 0..rows {
        for c in 0..cols {
            if c > 0 {
                data.push('\t');
            }
            if c == 0 {
                data.push_str(&r.to_string());
            } else if r % 10 == 0 && c == 1 {
                data.push_str("\\N"); // NULL value
            } else {
                data.push_str(&format!("value_{}_{}", r, c));
            }
        }
        data.push('\n');
    }
    data.push_str("\\.\n");
    data.into_bytes()
}

/// Benchmark MySQL → PostgreSQL conversion
fn bench_mysql_to_postgres(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_mysql_to_postgres");
    group.sample_size(20);

    for (tables, rows) in [(5, 100), (10, 500), (20, 1000)] {
        let data = generate_mysql_dump(tables, rows);
        let data_size = data.len();

        group.throughput(Throughput::Bytes(data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("convert", format!("{}t_{}r", tables, rows)),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
                    let mut parser = sql_splitter::parser::Parser::with_dialect(
                        &data[..],
                        64 * 1024,
                        SqlDialect::MySql,
                    );
                    let mut converted = 0;
                    while let Ok(Some(stmt)) = parser.read_statement() {
                        if let Ok(result) = converter.convert_statement(&stmt) {
                            if !result.is_empty() {
                                converted += 1;
                            }
                        }
                    }
                    black_box(converted)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark PostgreSQL → MySQL conversion (including COPY → INSERT)
fn bench_postgres_to_mysql(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_postgres_to_mysql");
    group.sample_size(20);

    for (tables, rows) in [(5, 100), (10, 500), (20, 1000)] {
        let data = generate_postgres_dump(tables, rows);
        let data_size = data.len();

        group.throughput(Throughput::Bytes(data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("convert", format!("{}t_{}r", tables, rows)),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
                    let mut parser = sql_splitter::parser::Parser::with_dialect(
                        &data[..],
                        64 * 1024,
                        SqlDialect::Postgres,
                    );
                    let mut converted = 0;
                    while let Ok(Some(stmt)) = parser.read_statement() {
                        // Handle COPY data blocks
                        if converter.has_pending_copy() {
                            if let Ok(inserts) = converter.process_copy_data(&stmt) {
                                converted += inserts.len();
                            }
                            continue;
                        }
                        if let Ok(result) = converter.convert_statement(&stmt) {
                            if !result.is_empty() {
                                converted += 1;
                            }
                        }
                    }
                    black_box(converted)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark COPY → INSERT conversion specifically
fn bench_copy_to_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_to_insert");

    for (rows, cols) in [(100, 5), (1000, 5), (1000, 10), (10000, 5)] {
        let data = generate_copy_data(rows, cols);
        let data_size = data.len();

        let header = CopyHeader {
            schema: Some("public".to_string()),
            table: "test_table".to_string(),
            columns: (0..cols).map(|i| format!("col_{}", i)).collect(),
        };

        group.throughput(Throughput::Bytes(data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("convert", format!("{}r_{}c", rows, cols)),
            &data,
            |b, data| {
                b.iter(|| {
                    let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
                    black_box(inserts.len())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark COPY header parsing
fn bench_parse_copy_header(c: &mut Criterion) {
    let headers = vec![
        "COPY users (id, name, email) FROM stdin;",
        "COPY public.users (id, name, email, created_at, updated_at) FROM stdin;",
        r#"COPY "public"."my_table" ("id", "name", "email", "data", "flags") FROM stdin;"#,
        "-- Comment\n-- Another comment\nCOPY users (id) FROM stdin;",
    ];

    c.bench_function("parse_copy_header", |b| {
        b.iter(|| {
            for header in &headers {
                black_box(parse_copy_header(black_box(header)));
            }
        })
    });
}

/// Benchmark identifier quoting conversion
fn bench_identifier_conversion(c: &mut Criterion) {
    let mysql_stmts = vec![
        "CREATE TABLE `users` (`id` INT, `name` VARCHAR(255));",
        "INSERT INTO `users` (`id`, `name`) VALUES (1, 'test');",
        "ALTER TABLE `orders` ADD COLUMN `status` INT;",
        "DROP TABLE `temp_data`;",
        "CREATE INDEX `idx_users_name` ON `users` (`name`);",
    ];

    let postgres_stmts = vec![
        "CREATE TABLE \"users\" (\"id\" INT, \"name\" VARCHAR(255));",
        "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'test');",
        "ALTER TABLE \"orders\" ADD COLUMN \"status\" INT;",
        "DROP TABLE \"temp_data\";",
        "CREATE INDEX \"idx_users_name\" ON \"users\" (\"name\");",
    ];

    let mut group = c.benchmark_group("identifier_conversion");

    group.bench_function("backticks_to_double_quotes", |b| {
        let converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
        b.iter(|| {
            for stmt in &mysql_stmts {
                black_box(converter.backticks_to_double_quotes(black_box(stmt)));
            }
        })
    });

    group.bench_function("double_quotes_to_backticks", |b| {
        let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
        b.iter(|| {
            for stmt in &postgres_stmts {
                black_box(converter.double_quotes_to_backticks(black_box(stmt)));
            }
        })
    });

    group.finish();
}

/// Benchmark type mapping
fn bench_type_mapping(c: &mut Criterion) {
    use sql_splitter::convert::TypeMapper;

    let mysql_types = vec![
        "CREATE TABLE t (a TINYINT(1), b INT(11), c BIGINT(20), d VARCHAR(255), e LONGTEXT, f DATETIME, g JSON, h ENUM('a','b'));",
        "CREATE TABLE t (id INT AUTO_INCREMENT, data BLOB, status TINYINT UNSIGNED, price DECIMAL(10,2));",
    ];

    let postgres_types = vec![
        "CREATE TABLE t (a BOOLEAN, b INTEGER, c BIGINT, d VARCHAR(255), e TEXT, f TIMESTAMP, g JSONB, h VARCHAR(255));",
        "CREATE TABLE t (id SERIAL, data BYTEA, status SMALLINT, price NUMERIC(10,2));",
    ];

    let mut group = c.benchmark_group("type_mapping");

    group.bench_function("mysql_to_postgres", |b| {
        b.iter(|| {
            for stmt in &mysql_types {
                black_box(TypeMapper::convert(
                    black_box(stmt),
                    SqlDialect::MySql,
                    SqlDialect::Postgres,
                ));
            }
        })
    });

    group.bench_function("postgres_to_mysql", |b| {
        b.iter(|| {
            for stmt in &postgres_types {
                black_box(TypeMapper::convert(
                    black_box(stmt),
                    SqlDialect::Postgres,
                    SqlDialect::MySql,
                ));
            }
        })
    });

    group.bench_function("mysql_to_sqlite", |b| {
        b.iter(|| {
            for stmt in &mysql_types {
                black_box(TypeMapper::convert(
                    black_box(stmt),
                    SqlDialect::MySql,
                    SqlDialect::Sqlite,
                ));
            }
        })
    });

    group.finish();
}

/// Benchmark all dialect conversion pairs
fn bench_all_dialect_pairs(c: &mut Criterion) {
    let mut group = c.benchmark_group("dialect_pairs");
    group.sample_size(20);

    let mysql_data = generate_mysql_dump(10, 100);
    let postgres_data = generate_postgres_dump(10, 100);

    // MySQL → PostgreSQL
    group.bench_function("mysql_to_postgres", |b| {
        b.iter(|| {
            let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
            let mut parser = sql_splitter::parser::Parser::with_dialect(
                &mysql_data[..],
                64 * 1024,
                SqlDialect::MySql,
            );
            let mut count = 0;
            while let Ok(Some(stmt)) = parser.read_statement() {
                if converter.convert_statement(&stmt).is_ok() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // MySQL → SQLite
    group.bench_function("mysql_to_sqlite", |b| {
        b.iter(|| {
            let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Sqlite);
            let mut parser = sql_splitter::parser::Parser::with_dialect(
                &mysql_data[..],
                64 * 1024,
                SqlDialect::MySql,
            );
            let mut count = 0;
            while let Ok(Some(stmt)) = parser.read_statement() {
                if converter.convert_statement(&stmt).is_ok() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // PostgreSQL → MySQL (with COPY handling)
    group.bench_function("postgres_to_mysql", |b| {
        b.iter(|| {
            let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
            let mut parser = sql_splitter::parser::Parser::with_dialect(
                &postgres_data[..],
                64 * 1024,
                SqlDialect::Postgres,
            );
            let mut count = 0;
            while let Ok(Some(stmt)) = parser.read_statement() {
                if converter.has_pending_copy() {
                    if let Ok(inserts) = converter.process_copy_data(&stmt) {
                        count += inserts.len();
                    }
                    continue;
                }
                if converter.convert_statement(&stmt).is_ok() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // PostgreSQL → SQLite
    group.bench_function("postgres_to_sqlite", |b| {
        b.iter(|| {
            let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite);
            let mut parser = sql_splitter::parser::Parser::with_dialect(
                &postgres_data[..],
                64 * 1024,
                SqlDialect::Postgres,
            );
            let mut count = 0;
            while let Ok(Some(stmt)) = parser.read_statement() {
                if converter.has_pending_copy() {
                    if let Ok(inserts) = converter.process_copy_data(&stmt) {
                        count += inserts.len();
                    }
                    continue;
                }
                if converter.convert_statement(&stmt).is_ok() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mysql_to_postgres,
    bench_postgres_to_mysql,
    bench_copy_to_insert,
    bench_parse_copy_header,
    bench_identifier_conversion,
    bench_type_mapping,
    bench_all_dialect_pairs,
);

criterion_main!(benches);
