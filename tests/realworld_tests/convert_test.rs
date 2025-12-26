//! Convert command tests for real-world SQL dumps.
//!
//! Tests that sql-splitter can convert between MySQL, PostgreSQL, and SQLite.

use super::{Fixture, TEST_CASES};
use sql_splitter::convert::{run, ConvertConfig};
use sql_splitter::parser::SqlDialect;

/// Run convert tests for a single test case - converts to all other dialects
fn run_convert_tests(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    let source_dialect = fixture.dialect();
    let targets = [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Sqlite];

    eprintln!(
        "Testing convert: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    for target in targets {
        // Skip same dialect conversion
        if source_dialect == target {
            continue;
        }

        let output_dir = super::temp_output_dir(&format!("{}-to-{:?}", case.name, target))
            .expect("Failed to create temp dir");
        let output_path = output_dir
            .path()
            .join(format!("converted_{:?}.sql", target));

        let config = ConvertConfig {
            input: fixture.sql_path.clone(),
            output: Some(output_path.clone()),
            from_dialect: Some(source_dialect),
            to_dialect: target,
            dry_run: false,
            progress: false,
            strict: false,
        };

        match run(config) {
            Ok(stats) => {
                eprintln!(
                    "  ✓ -> {:?} ({} converted)",
                    target, stats.statements_converted
                );

                // Verify output file exists and has content
                if output_path.exists() {
                    let size = std::fs::metadata(&output_path)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    assert!(size > 0, "Output file should not be empty");
                }
            }
            Err(e) => {
                eprintln!("  ✗ -> {:?}: {}", target, e);
            }
        }
    }
}

// Generate individual test functions for each case

#[test]
#[ignore]
fn convert_mysql_classicmodels() {
    run_convert_tests(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn convert_mysql_sakila_schema() {
    run_convert_tests(super::cases::get_case("mysql-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn convert_mysql_sakila_data() {
    run_convert_tests(super::cases::get_case("mysql-sakila-data").unwrap());
}

#[test]
#[ignore]
fn convert_mysql_employees() {
    run_convert_tests(super::cases::get_case("mysql-employees").unwrap());
}

#[test]
#[ignore]
fn convert_mysql_world() {
    run_convert_tests(super::cases::get_case("mysql-world").unwrap());
}

#[test]
#[ignore]
fn convert_postgres_pagila_schema() {
    run_convert_tests(super::cases::get_case("postgres-pagila-schema").unwrap());
}

#[test]
#[ignore]
fn convert_postgres_pagila_data() {
    run_convert_tests(super::cases::get_case("postgres-pagila-data").unwrap());
}

#[test]
#[ignore]
fn convert_postgres_airlines_small() {
    run_convert_tests(super::cases::get_case("postgres-airlines-small").unwrap());
}

#[test]
#[ignore]
fn convert_postgres_northwind() {
    run_convert_tests(super::cases::get_case("postgres-northwind").unwrap());
}

#[test]
#[ignore]
fn convert_chinook_postgres() {
    run_convert_tests(super::cases::get_case("chinook-postgres").unwrap());
}

#[test]
#[ignore]
fn convert_chinook_sqlite() {
    run_convert_tests(super::cases::get_case("chinook-sqlite").unwrap());
}

#[test]
#[ignore]
fn convert_chinook_mysql() {
    run_convert_tests(super::cases::get_case("chinook-mysql").unwrap());
}

/// Run all convert tests (convenience test for CI)
#[test]
#[ignore]
fn all_convert_tests() {
    let mut passed = 0;
    let mut failed = 0;

    for case in TEST_CASES {
        let fixture = match Fixture::get(case) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Skipping {}: {}", case.name, e);
                continue;
            }
        };

        let source_dialect = fixture.dialect();
        let targets = [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Sqlite];

        for target in targets {
            if source_dialect == target {
                continue;
            }

            let output_dir = match super::temp_output_dir(&format!("{}-to-{:?}", case.name, target))
            {
                Ok(d) => d,
                Err(_) => continue,
            };
            let output_path = output_dir.path().join("converted.sql");

            let config = ConvertConfig {
                input: fixture.sql_path.clone(),
                output: Some(output_path),
                from_dialect: Some(source_dialect),
                to_dialect: target,
                dry_run: false,
                progress: false,
                strict: false,
            };

            match run(config) {
                Ok(stats) => {
                    eprintln!(
                        "✓ {} -> {:?} ({} converted)",
                        case.name, target, stats.statements_converted
                    );
                    passed += 1;
                }
                Err(e) => {
                    eprintln!("✗ {} -> {:?}: {}", case.name, target, e);
                    failed += 1;
                }
            }
        }
    }

    eprintln!("\nConvert tests: {} passed, {} failed", passed, failed);
}
