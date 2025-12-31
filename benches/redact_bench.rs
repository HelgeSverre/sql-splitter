//! Benchmarks for the redact command's internal components.
//!
//! Tests:
//! - Redaction strategy performance (hash, mask, fake)
//! - Value rewriting throughput
//! - Column pattern matching

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::SeedableRng;
use sql_splitter::parser::SqlDialect;
use sql_splitter::redactor::strategy::{
    ConstantStrategy, FakeStrategy, HashStrategy, MaskStrategy, NullStrategy, RedactValue, Strategy,
};
use sql_splitter::redactor::ValueRewriter;
use sql_splitter::schema::{Column, ColumnId, ColumnType, TableId, TableSchema};
use std::hint::black_box;

/// Create a test schema with typical PII columns
fn create_test_schema() -> TableSchema {
    TableSchema {
        name: "users".to_string(),
        id: TableId(0),
        columns: vec![
            Column {
                name: "id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(0),
                is_primary_key: true,
                is_nullable: false,
            },
            Column {
                name: "name".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(1),
                is_primary_key: false,
                is_nullable: false,
            },
            Column {
                name: "email".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(2),
                is_primary_key: false,
                is_nullable: false,
            },
            Column {
                name: "password".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(3),
                is_primary_key: false,
                is_nullable: false,
            },
            Column {
                name: "ssn".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(4),
                is_primary_key: false,
                is_nullable: true,
            },
        ],
        primary_key: vec![ColumnId(0)],
        foreign_keys: vec![],
        indexes: vec![],
        create_statement: None,
    }
}

/// Benchmark individual strategy implementations
fn bench_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("redaction_strategies");

    let test_values = vec![
        RedactValue::String("john.doe@example.com".to_string()),
        RedactValue::String("secret_password_123".to_string()),
        RedactValue::String("123-45-6789".to_string()),
        RedactValue::String("John Doe".to_string()),
    ];

    // Null strategy
    group.bench_function("null", |b| {
        let strategy = NullStrategy::new();
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    // Constant strategy
    group.bench_function("constant", |b| {
        let strategy = ConstantStrategy::new("REDACTED".to_string());
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    // Hash strategy (without domain preservation)
    group.bench_function("hash", |b| {
        let strategy = HashStrategy::new(false);
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    // Hash strategy (with domain preservation for emails)
    group.bench_function("hash_preserve_domain", |b| {
        let strategy = HashStrategy::new(true);
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    // Mask strategy
    group.bench_function("mask", |b| {
        let strategy = MaskStrategy::new("***-**-{4}".to_string());
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    // Fake strategy - name generator
    group.bench_function("fake_name", |b| {
        let strategy = FakeStrategy::new("name".to_string(), "en".to_string());
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    // Fake strategy - email generator
    group.bench_function("fake_email", |b| {
        let strategy = FakeStrategy::new("email".to_string(), "en".to_string());
        let mut rng = StdRng::seed_from_u64(42);
        b.iter(|| {
            for value in &test_values {
                black_box(strategy.apply(black_box(value), &mut rng));
            }
        })
    });

    group.finish();
}

/// Benchmark INSERT statement rewriting
fn bench_insert_rewriting(c: &mut Criterion) {
    use sql_splitter::redactor::StrategyKind;

    let mut group = c.benchmark_group("insert_rewriting");
    group.sample_size(50);

    let schema = create_test_schema();

    // Single row INSERT
    let single_row = b"INSERT INTO `users` VALUES (1, 'John Doe', 'john@example.com', 'secret123', '123-45-6789');";

    // Multi-row INSERT (10 rows)
    let multi_row = {
        let mut s = String::from("INSERT INTO `users` VALUES ");
        for i in 0..10 {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(&format!(
                "({}, 'User {}', 'user{}@example.com', 'pass{}', '123-45-{:04}')",
                i, i, i, i, i
            ));
        }
        s.push(';');
        s.into_bytes()
    };

    // Large multi-row INSERT (100 rows)
    let large_insert = {
        let mut s = String::from("INSERT INTO `users` VALUES ");
        for i in 0..100 {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(&format!(
                "({}, 'User {}', 'user{}@example.com', 'password{}', '123-45-{:04}')",
                i, i, i, i, i
            ));
        }
        s.push(';');
        s.into_bytes()
    };

    let strategies = vec![
        StrategyKind::Skip, // id
        StrategyKind::Fake {
            generator: "name".to_string(),
        }, // name
        StrategyKind::Hash {
            preserve_domain: true,
        }, // email
        StrategyKind::Null, // password
        StrategyKind::Mask {
            pattern: "***-**-{4}".to_string(),
        }, // ssn
    ];

    group.throughput(Throughput::Bytes(single_row.len() as u64));
    group.bench_function("single_row", |b| {
        b.iter(|| {
            let mut rewriter = ValueRewriter::new(Some(42), SqlDialect::MySql, "en".to_string());
            black_box(rewriter.rewrite_insert(
                black_box(&single_row[..]),
                "users",
                &schema,
                &strategies,
            ))
        })
    });

    group.throughput(Throughput::Bytes(multi_row.len() as u64));
    group.bench_function("10_rows", |b| {
        b.iter(|| {
            let mut rewriter = ValueRewriter::new(Some(42), SqlDialect::MySql, "en".to_string());
            black_box(rewriter.rewrite_insert(
                black_box(&multi_row[..]),
                "users",
                &schema,
                &strategies,
            ))
        })
    });

    group.throughput(Throughput::Bytes(large_insert.len() as u64));
    group.bench_function("100_rows", |b| {
        b.iter(|| {
            let mut rewriter = ValueRewriter::new(Some(42), SqlDialect::MySql, "en".to_string());
            black_box(rewriter.rewrite_insert(
                black_box(&large_insert[..]),
                "users",
                &schema,
                &strategies,
            ))
        })
    });

    group.finish();
}

/// Benchmark with varying numbers of strategies (columns to redact)
fn bench_strategy_count(c: &mut Criterion) {
    use sql_splitter::redactor::StrategyKind;

    let mut group = c.benchmark_group("strategy_count");

    // Generate a wide table with many columns
    let cols_count = 20;
    let mut schema = TableSchema {
        name: "wide_table".to_string(),
        id: TableId(0),
        columns: Vec::new(),
        primary_key: vec![ColumnId(0)],
        foreign_keys: vec![],
        indexes: vec![],
        create_statement: None,
    };

    for i in 0..cols_count {
        schema.columns.push(Column {
            name: format!("col_{}", i),
            col_type: ColumnType::Text,
            ordinal: ColumnId(i as u16),
            is_primary_key: i == 0,
            is_nullable: i > 0,
        });
    }

    // Generate INSERT with 20 columns
    let mut stmt = String::from("INSERT INTO `wide_table` VALUES (");
    for i in 0..cols_count {
        if i > 0 {
            stmt.push_str(", ");
        }
        stmt.push_str(&format!("'value_{}'", i));
    }
    stmt.push_str(");");
    let stmt_bytes = stmt.into_bytes();

    for redact_count in [1, 5, 10, 15, 20] {
        let strategies: Vec<StrategyKind> = (0..cols_count)
            .map(|i| {
                if i < redact_count {
                    StrategyKind::Hash {
                        preserve_domain: false,
                    }
                } else {
                    StrategyKind::Skip
                }
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("redact", format!("{}_of_{}", redact_count, cols_count)),
            &strategies,
            |b, strategies| {
                b.iter(|| {
                    let mut rewriter =
                        ValueRewriter::new(Some(42), SqlDialect::MySql, "en".to_string());
                    black_box(rewriter.rewrite_insert(
                        black_box(&stmt_bytes[..]),
                        "wide_table",
                        &schema,
                        strategies,
                    ))
                })
            },
        );
    }

    group.finish();
}

/// Benchmark dialect-specific formatting
fn bench_dialect_formatting(c: &mut Criterion) {
    use sql_splitter::redactor::StrategyKind;

    let mut group = c.benchmark_group("dialect_formatting");

    let schema = create_test_schema();

    let stmt =
        b"INSERT INTO `users` VALUES (1, 'John Doe', 'john@example.com', 'secret', '123-45-6789');";

    let strategies = vec![
        StrategyKind::Skip,
        StrategyKind::Fake {
            generator: "name".to_string(),
        },
        StrategyKind::Hash {
            preserve_domain: true,
        },
        StrategyKind::Null,
        StrategyKind::Mask {
            pattern: "***-**-{4}".to_string(),
        },
    ];

    for dialect in [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Mssql] {
        let dialect_name = match dialect {
            SqlDialect::MySql => "mysql",
            SqlDialect::Postgres => "postgres",
            SqlDialect::Mssql => "mssql",
            SqlDialect::Sqlite => "sqlite",
        };

        group.bench_function(dialect_name, |b| {
            b.iter(|| {
                let mut rewriter = ValueRewriter::new(Some(42), dialect, "en".to_string());
                black_box(rewriter.rewrite_insert(
                    black_box(&stmt[..]),
                    "users",
                    &schema,
                    &strategies,
                ))
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_strategies,
    bench_insert_rewriting,
    bench_strategy_count,
    bench_dialect_formatting,
);

criterion_main!(benches);
