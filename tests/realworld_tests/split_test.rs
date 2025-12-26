//! Split command tests for real-world SQL dumps.
//!
//! Tests that sql-splitter can correctly parse and split various public SQL dumps.

use super::{Fixture, TEST_CASES};
use sql_splitter::splitter::Splitter;
use std::fs;

/// Run split test for a single test case
fn run_split_test(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    let output_dir = super::temp_output_dir(case.name).expect("Failed to create temp dir");

    eprintln!(
        "Testing split: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    // Run splitter with dry-run first to verify parsing
    let stats = Splitter::new(fixture.sql_path.clone(), output_dir.path().to_path_buf())
        .with_dialect(fixture.dialect())
        .with_dry_run(true)
        .split();

    match stats {
        Ok(s) => {
            eprintln!(
                "  ✓ {} tables, {} statements",
                s.tables_found, s.statements_processed
            );
            assert!(
                s.statements_processed > 0,
                "{}: Expected at least one statement",
                case.name
            );
        }
        Err(e) => {
            panic!("{}: Split failed: {}", case.name, e);
        }
    }

    // Now run actual split and verify files are created
    let stats = Splitter::new(fixture.sql_path.clone(), output_dir.path().to_path_buf())
        .with_dialect(fixture.dialect())
        .with_dry_run(false)
        .split()
        .expect("Split should succeed");

    if stats.tables_found > 0 {
        let files: Vec<_> = fs::read_dir(output_dir.path())
            .expect("Should read output dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
            .collect();

        assert!(
            !files.is_empty(),
            "{}: Expected output files but found none",
            case.name
        );
    }
}

// Generate individual test functions for each case
// These are ignored by default since they require network access

#[test]
#[ignore]
fn mysql_classicmodels() {
    run_split_test(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn mysql_sakila_schema() {
    run_split_test(super::cases::get_case("mysql-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn mysql_sakila_data() {
    run_split_test(super::cases::get_case("mysql-sakila-data").unwrap());
}

#[test]
#[ignore]
fn mysql_employees() {
    run_split_test(super::cases::get_case("mysql-employees").unwrap());
}

#[test]
#[ignore]
fn mysql_world() {
    run_split_test(super::cases::get_case("mysql-world").unwrap());
}

#[test]
#[ignore]
fn postgres_pagila_schema() {
    run_split_test(super::cases::get_case("postgres-pagila-schema").unwrap());
}

#[test]
#[ignore]
fn postgres_pagila_data() {
    run_split_test(super::cases::get_case("postgres-pagila-data").unwrap());
}

#[test]
#[ignore]
fn postgres_airlines_small() {
    run_split_test(super::cases::get_case("postgres-airlines-small").unwrap());
}

#[test]
#[ignore]
fn postgres_northwind() {
    run_split_test(super::cases::get_case("postgres-northwind").unwrap());
}

#[test]
#[ignore]
fn postgres_periodic() {
    run_split_test(super::cases::get_case("postgres-periodic").unwrap());
}

#[test]
#[ignore]
fn postgres_ecommerce() {
    run_split_test(super::cases::get_case("postgres-ecommerce").unwrap());
}

#[test]
#[ignore]
fn postgres_sakila_schema() {
    run_split_test(super::cases::get_case("postgres-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn postgres_sakila_data() {
    run_split_test(super::cases::get_case("postgres-sakila-data").unwrap());
}

#[test]
#[ignore]
fn postgres_adventureworks() {
    run_split_test(super::cases::get_case("postgres-adventureworks").unwrap());
}

#[test]
#[ignore]
fn chinook_postgres() {
    run_split_test(super::cases::get_case("chinook-postgres").unwrap());
}

#[test]
#[ignore]
fn chinook_sqlite() {
    run_split_test(super::cases::get_case("chinook-sqlite").unwrap());
}

#[test]
#[ignore]
fn chinook_mysql() {
    run_split_test(super::cases::get_case("chinook-mysql").unwrap());
}

#[test]
#[ignore]
fn wordpress_films() {
    run_split_test(super::cases::get_case("wordpress-films").unwrap());
}

#[test]
#[ignore]
fn mysql_northwind_data() {
    run_split_test(super::cases::get_case("mysql-northwind-data").unwrap());
}

#[test]
#[ignore]
fn mysql_countries() {
    run_split_test(super::cases::get_case("mysql-countries").unwrap());
}

#[test]
#[ignore]
fn mysql_wilayah() {
    run_split_test(super::cases::get_case("mysql-wilayah").unwrap());
}

#[test]
#[ignore]
fn mysql_coffeeshop() {
    run_split_test(super::cases::get_case("mysql-coffeeshop").unwrap());
}

#[test]
#[ignore]
fn wordpress_woocommerce() {
    run_split_test(super::cases::get_case("wordpress-woocommerce").unwrap());
}

#[test]
#[ignore]
fn wordpress_woo_replica() {
    run_split_test(super::cases::get_case("wordpress-woo-replica").unwrap());
}

#[test]
#[ignore]
fn wordpress_plugin_test() {
    run_split_test(super::cases::get_case("wordpress-plugin-test").unwrap());
}

/// Run all split tests (convenience test for CI)
#[test]
#[ignore]
fn all_split_tests() {
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

        let output_dir = match super::temp_output_dir(case.name) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Skipping {}: {}", case.name, e);
                continue;
            }
        };

        let result = Splitter::new(fixture.sql_path.clone(), output_dir.path().to_path_buf())
            .with_dialect(fixture.dialect())
            .with_dry_run(true)
            .split();

        match result {
            Ok(s) => {
                eprintln!(
                    "✓ {} ({} tables, {} stmts)",
                    case.name, s.tables_found, s.statements_processed
                );
                passed += 1;
            }
            Err(e) => {
                eprintln!("✗ {}: {}", case.name, e);
                failed += 1;
            }
        }
    }

    eprintln!("\nSplit tests: {} passed, {} failed", passed, failed);
    assert_eq!(failed, 0, "Some split tests failed");
}
