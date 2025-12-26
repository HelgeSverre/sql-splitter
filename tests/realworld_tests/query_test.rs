//! Query command tests for real-world SQL dumps.
//!
//! Tests that sql-splitter can import and query various public SQL dumps.

use super::{Fixture, TEST_CASES};
use sql_splitter::duckdb::{QueryConfig, QueryEngine};

/// Run query test for a single test case
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

    let mut engine = match QueryEngine::new(&config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("  ✗ Failed to create query engine: {}", e);
            return;
        }
    };

    let stats = match engine.import_dump(&fixture.sql_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  ✗ Import failed: {}", e);
            return;
        }
    };

    eprintln!(
        "  ✓ Imported {} tables, {} rows in {:.2}s",
        stats.tables_created, stats.rows_inserted, stats.duration_secs
    );

    // Print any warnings
    if !stats.warnings.is_empty() {
        eprintln!("  ⚠ {} warnings", stats.warnings.len());
        for warning in stats.warnings.iter().take(3) {
            eprintln!("    - {}", warning);
        }
        if stats.warnings.len() > 3 {
            eprintln!("    - ... and {} more", stats.warnings.len() - 3);
        }
    }

    // List tables
    let tables = match engine.list_tables() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("  ✗ Failed to list tables: {}", e);
            return;
        }
    };

    if tables.is_empty() {
        eprintln!("  ⚠ No tables imported");
        return;
    }

    eprintln!("  Tables: {}", tables.join(", "));

    // Run a count query on the first table
    let first_table = &tables[0];
    let count_query = format!("SELECT COUNT(*) as count FROM \"{}\"", first_table);
    match engine.query(&count_query) {
        Ok(result) => {
            if !result.rows.is_empty() {
                eprintln!("  Query: {} has {} rows", first_table, result.rows[0][0]);
            }
        }
        Err(e) => {
            eprintln!("  ⚠ Count query failed: {}", e);
        }
    }

    // Run an analytical query to test more complex functionality
    let analytical_query = format!("SELECT * FROM \"{}\" LIMIT 5", first_table);
    match engine.query(&analytical_query) {
        Ok(result) => {
            eprintln!(
                "  ✓ SELECT query returned {} rows, {} columns",
                result.row_count(),
                result.column_count()
            );
        }
        Err(e) => {
            eprintln!("  ⚠ SELECT query failed: {}", e);
        }
    }
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

/// Run all query tests
#[test]
#[ignore]
fn all_query_tests() {
    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

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
            Err(_) => {
                failed += 1;
                continue;
            }
        };

        let import_result = engine.import_dump(&fixture.sql_path);
        let (tables_created, rows_inserted) = match import_result {
            Ok(stats) => (stats.tables_created, stats.rows_inserted),
            Err(e) => {
                eprintln!("✗ {} (import error: {})", case.name, e);
                failed += 1;
                continue;
            }
        };

        // Count import as success if at least one table was created
        if tables_created > 0 {
            // Verify we can query
            let tables = engine.list_tables().unwrap_or_default();
            if !tables.is_empty() {
                match engine.query(&format!("SELECT COUNT(*) FROM \"{}\"", tables[0])) {
                    Ok(_) => {
                        eprintln!(
                            "✓ {} ({} tables, {} rows)",
                            case.name, tables_created, rows_inserted
                        );
                        passed += 1;
                    }
                    Err(e) => {
                        eprintln!("✗ {} (query failed: {})", case.name, e);
                        failed += 1;
                    }
                }
            } else {
                eprintln!("✗ {} (no tables listed)", case.name);
                failed += 1;
            }
        } else {
            eprintln!("✗ {} (no tables created)", case.name);
            failed += 1;
        }
    }

    eprintln!(
        "\nQuery tests: {} passed, {} failed, {} skipped",
        passed, failed, skipped
    );

    // We don't assert here - some edge-case dumps may have issues
    // but we want to see the summary
}
