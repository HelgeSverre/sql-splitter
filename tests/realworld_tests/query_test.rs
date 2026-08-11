//! Query command tests for real-world SQL dumps.
//!
//! Tests that sql-splitter can import and query various public SQL dumps.

use super::{Fixture, TEST_CASES};
use sql_splitter::duckdb::{QueryConfig, QueryEngine};

/// Run query test for a single test case.
///
/// A missing fixture (no network / download failed) is a skip, not a
/// failure -- that's an environment concern, not a correctness one. Every
/// other step is a hard assertion: this suite exists specifically to catch
/// silent data loss on real dumps, and a warning means the loader dropped a
/// table or rows without failing outright. Letting that through as an
/// eprintln! is exactly how a partial-import bug shipped undetected before
/// (see the MySQL zero-date and missing-table-warning regressions).
fn run_query_test(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    eprintln!(
        "Testing query: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    // Import the dump into DuckDB
    let config = QueryConfig {
        dialect: Some(fixture.dialect()),
        progress: false,
        ..Default::default()
    };

    let mut engine = QueryEngine::new(&config)
        .unwrap_or_else(|e| panic!("{}: failed to create query engine: {}", case.name, e));

    let stats = engine
        .import_dump(&fixture.sql_path)
        .unwrap_or_else(|e| panic!("{}: import failed: {}", case.name, e));

    eprintln!(
        "  Imported {} tables, {} rows in {:.2}s",
        stats.tables_created, stats.rows_inserted, stats.duration_secs
    );

    assert!(
        stats.warnings.is_empty(),
        "{}: import produced {} warning(s), meaning the loader silently dropped a table or rows \
         instead of failing outright -- first few: {:?}",
        case.name,
        stats.warnings.len(),
        stats.warnings.iter().take(5).collect::<Vec<_>>()
    );

    let tables = engine
        .list_tables()
        .unwrap_or_else(|e| panic!("{}: failed to list tables: {}", case.name, e));
    assert!(!tables.is_empty(), "{}: no tables imported", case.name);
    eprintln!("  Tables: {}", tables.join(", "));

    // Run a count query on the first table
    let first_table = &tables[0];
    let count_query = format!("SELECT COUNT(*) as count FROM \"{}\"", first_table);
    let result = engine.query(&count_query).unwrap_or_else(|e| {
        panic!(
            "{}: count query on {} failed: {}",
            case.name, first_table, e
        )
    });
    if !result.rows.is_empty() {
        eprintln!("  Query: {} has {} rows", first_table, result.rows[0][0]);
    }

    // Run an analytical query to test more complex functionality
    let analytical_query = format!("SELECT * FROM \"{}\" LIMIT 5", first_table);
    let result = engine.query(&analytical_query).unwrap_or_else(|e| {
        panic!(
            "{}: SELECT query on {} failed: {}",
            case.name, first_table, e
        )
    });
    eprintln!(
        "  SELECT query returned {} rows, {} columns",
        result.row_count(),
        result.column_count()
    );
}

// Generate individual test functions for each case

#[test]
#[ignore]
fn query_mysql_classicmodels() {
    run_query_test(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn query_mysql_sakila_schema() {
    run_query_test(super::cases::get_case("mysql-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn query_postgres_pagila_schema() {
    run_query_test(super::cases::get_case("postgres-pagila-schema").unwrap());
}

#[test]
#[ignore]
fn query_postgres_pagila_data() {
    run_query_test(super::cases::get_case("postgres-pagila-data").unwrap());
}

#[test]
#[ignore]
fn query_chinook_postgres() {
    run_query_test(super::cases::get_case("chinook-postgres").unwrap());
}

#[test]
#[ignore]
fn query_chinook_sqlite() {
    run_query_test(super::cases::get_case("chinook-sqlite").unwrap());
}

#[test]
#[ignore]
fn query_chinook_mysql() {
    run_query_test(super::cases::get_case("chinook-mysql").unwrap());
}

/// Run all query tests. Prints a full summary before asserting so a failure
/// shows every broken case in one run, not just the first `run_query_test`
/// panic.
#[test]
#[ignore]
fn all_query_tests() {
    let mut passed = 0;
    let mut skipped = 0;
    let mut failures: Vec<String> = Vec::new();

    for case in TEST_CASES {
        let fixture = match Fixture::get(case) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Skipping {}: {}", case.name, e);
                skipped += 1;
                continue;
            }
        };

        let config = QueryConfig {
            dialect: Some(fixture.dialect()),
            progress: false,
            ..Default::default()
        };

        let mut engine = match QueryEngine::new(&config) {
            Ok(e) => e,
            Err(e) => {
                failures.push(format!("{} (engine creation failed: {})", case.name, e));
                continue;
            }
        };

        let import_result = engine.import_dump(&fixture.sql_path);
        let stats = match import_result {
            Ok(stats) => stats,
            Err(e) => {
                failures.push(format!("{} (import error: {})", case.name, e));
                continue;
            }
        };

        // A warning means the loader silently dropped a table or rows rather
        // than failing outright -- exactly the class of bug this suite
        // exists to catch, so it counts as a failure, not a soft note.
        if !stats.warnings.is_empty() {
            failures.push(format!(
                "{} ({} warning(s), first: {})",
                case.name,
                stats.warnings.len(),
                stats.warnings[0]
            ));
            continue;
        }

        let (tables_created, rows_inserted) = (stats.tables_created, stats.rows_inserted);
        if tables_created == 0 {
            failures.push(format!("{} (no tables created)", case.name));
            continue;
        }

        let tables = engine.list_tables().unwrap_or_default();
        if tables.is_empty() {
            failures.push(format!("{} (no tables listed)", case.name));
            continue;
        }

        match engine.query(&format!("SELECT COUNT(*) FROM \"{}\"", tables[0])) {
            Ok(_) => {
                eprintln!(
                    "✓ {} ({} tables, {} rows)",
                    case.name, tables_created, rows_inserted
                );
                passed += 1;
            }
            Err(e) => {
                failures.push(format!("{} (query failed: {})", case.name, e));
            }
        }
    }

    for failure in &failures {
        eprintln!("✗ {failure}");
    }
    eprintln!(
        "\nQuery tests: {} passed, {} failed, {} skipped",
        passed,
        failures.len(),
        skipped
    );

    assert!(
        failures.is_empty(),
        "{} case(s) failed: {:?}",
        failures.len(),
        failures
    );
}
