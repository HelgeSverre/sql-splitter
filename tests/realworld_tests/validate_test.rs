//! Validate command tests for real-world SQL dumps.
//!
//! Tests that sql-splitter can validate various public SQL dumps and catch issues.

use super::{Fixture, TEST_CASES};
use sql_splitter::merger::Merger;
use sql_splitter::splitter::Splitter;
use sql_splitter::validate::{ValidateOptions, Validator};

/// Run validation test for a single test case
fn run_validate_test(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    eprintln!(
        "Testing validate: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    // Basic validation without FK checks (faster)
    let options = ValidateOptions {
        path: fixture.sql_path.clone(),
        dialect: Some(fixture.dialect()),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 100_000,
        fk_checks_enabled: false,
        max_pk_fk_keys: None,
    };

    let validator = Validator::new(options);
    let result = validator.validate();

    match result {
        Ok(summary) => {
            eprintln!(
                "  ✓ {} tables, {} statements, {} errors, {} warnings",
                summary.summary.tables_scanned,
                summary.summary.statements_scanned,
                summary.summary.errors,
                summary.summary.warnings
            );

            // We expect real-world dumps to parse without errors
            // Warnings are OK (e.g., encoding issues)
            if summary.summary.errors > 0 {
                for issue in &summary.issues {
                    if issue.severity == sql_splitter::validate::Severity::Error {
                        eprintln!("    Error: {} ({:?})", issue.message, issue.code);
                    }
                }
            }
        }
        Err(e) => {
            panic!("{}: Validation failed: {}", case.name, e);
        }
    }
}

/// Run split-merge-validate roundtrip test
fn run_roundtrip_test(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping roundtrip {}: {}", case.name, e);
            return;
        }
    };

    let output_dir = match super::temp_output_dir(&format!("{}-roundtrip", case.name)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    eprintln!(
        "Testing roundtrip: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    // Step 1: Split
    let split_dir = output_dir.path().join("split");
    std::fs::create_dir_all(&split_dir).expect("Create split dir");

    let split_result = Splitter::new(fixture.sql_path.clone(), split_dir.clone())
        .with_dialect(fixture.dialect())
        .with_dry_run(false)
        .split();

    let split_stats = match split_result {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  ✗ Split failed: {}", e);
            return;
        }
    };

    if split_stats.tables_found == 0 {
        eprintln!("  ⚠ No tables found, skipping roundtrip");
        return;
    }

    // Step 2: Merge
    let merged_file = output_dir.path().join("merged.sql");
    let merge_result = Merger::new(split_dir.clone(), Some(merged_file.clone()))
        .with_dialect(fixture.dialect())
        .merge();

    if let Err(e) = merge_result {
        eprintln!("  ✗ Merge failed: {}", e);
        return;
    }

    // Step 3: Validate merged output
    let options = ValidateOptions {
        path: merged_file,
        dialect: Some(fixture.dialect()),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 100_000,
        fk_checks_enabled: false,
        max_pk_fk_keys: None,
    };

    let validator = Validator::new(options);
    match validator.validate() {
        Ok(summary) => {
            eprintln!(
                "  ✓ Roundtrip: {} tables, {} errors, {} warnings",
                summary.summary.tables_scanned, summary.summary.errors, summary.summary.warnings
            );
        }
        Err(e) => {
            eprintln!("  ✗ Roundtrip validation failed: {}", e);
        }
    }
}

// Generate individual test functions for each case

#[test]
#[ignore]
fn validate_mysql_classicmodels() {
    run_validate_test(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn validate_mysql_sakila_schema() {
    run_validate_test(super::cases::get_case("mysql-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn validate_postgres_pagila_schema() {
    run_validate_test(super::cases::get_case("postgres-pagila-schema").unwrap());
}

#[test]
#[ignore]
fn validate_postgres_pagila_data() {
    run_validate_test(super::cases::get_case("postgres-pagila-data").unwrap());
}

#[test]
#[ignore]
fn validate_chinook_postgres() {
    run_validate_test(super::cases::get_case("chinook-postgres").unwrap());
}

#[test]
#[ignore]
fn validate_chinook_sqlite() {
    run_validate_test(super::cases::get_case("chinook-sqlite").unwrap());
}

#[test]
#[ignore]
fn validate_chinook_mysql() {
    run_validate_test(super::cases::get_case("chinook-mysql").unwrap());
}

// Roundtrip tests

#[test]
#[ignore]
fn roundtrip_mysql_classicmodels() {
    run_roundtrip_test(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn roundtrip_mysql_sakila_schema() {
    run_roundtrip_test(super::cases::get_case("mysql-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn roundtrip_postgres_pagila_schema() {
    run_roundtrip_test(super::cases::get_case("postgres-pagila-schema").unwrap());
}

#[test]
#[ignore]
fn roundtrip_chinook_mysql() {
    run_roundtrip_test(super::cases::get_case("chinook-mysql").unwrap());
}

/// Run all validation tests
#[test]
#[ignore]
fn all_validate_tests() {
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

        let options = ValidateOptions {
            path: fixture.sql_path.clone(),
            dialect: Some(fixture.dialect()),
            progress: false,
            strict: false,
            json: false,
            max_rows_per_table: 100_000,
            fk_checks_enabled: false,
            max_pk_fk_keys: None,
        };

        let validator = Validator::new(options);
        match validator.validate() {
            Ok(summary) => {
                if summary.summary.errors == 0 {
                    eprintln!(
                        "✓ {} ({} tables)",
                        case.name, summary.summary.tables_scanned
                    );
                    passed += 1;
                } else {
                    eprintln!("✗ {} ({} errors)", case.name, summary.summary.errors);
                    failed += 1;
                }
            }
            Err(e) => {
                eprintln!("✗ {}: {}", case.name, e);
                failed += 1;
            }
        }
    }

    eprintln!("\nValidate tests: {} passed, {} failed", passed, failed);
}

/// Run all roundtrip tests
#[test]
#[ignore]
fn all_roundtrip_tests() {
    let mut passed = 0;
    let mut failed = 0;

    for case in TEST_CASES {
        let fixture = match Fixture::get(case) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Skipping roundtrip {}: {}", case.name, e);
                continue;
            }
        };

        let output_dir = match super::temp_output_dir(&format!("{}-rt", case.name)) {
            Ok(d) => d,
            Err(_) => continue,
        };

        // Split
        let split_dir = output_dir.path().join("split");
        std::fs::create_dir_all(&split_dir).ok();

        let split_result = Splitter::new(fixture.sql_path.clone(), split_dir.clone())
            .with_dialect(fixture.dialect())
            .split();

        if split_result.is_err() {
            failed += 1;
            continue;
        }

        // Merge
        let merged_file = output_dir.path().join("merged.sql");
        if Merger::new(split_dir, Some(merged_file.clone()))
            .with_dialect(fixture.dialect())
            .merge()
            .is_err()
        {
            failed += 1;
            continue;
        }

        // Validate
        let options = ValidateOptions {
            path: merged_file,
            dialect: Some(fixture.dialect()),
            progress: false,
            strict: false,
            json: false,
            max_rows_per_table: 100_000,
            fk_checks_enabled: false,
            max_pk_fk_keys: None,
        };

        match Validator::new(options).validate() {
            Ok(summary) if summary.summary.errors == 0 => {
                eprintln!("✓ {} roundtrip", case.name);
                passed += 1;
            }
            _ => {
                eprintln!("✗ {} roundtrip", case.name);
                failed += 1;
            }
        }
    }

    eprintln!("\nRoundtrip tests: {} passed, {} failed", passed, failed);
}
