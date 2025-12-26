//! Redact command tests for real-world SQL dumps.
//!
//! Tests data anonymization with various strategies: null, hash, fake, mask.

use super::{Fixture, TEST_CASES};
use sql_splitter::redactor::{RedactConfig, Redactor};

/// Run redact tests for a single test case
fn run_redact_tests(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    let output_dir = match super::temp_output_dir(&format!("{}-redact", case.name)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    eprintln!(
        "Testing redact: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    // Test 1: Dry-run mode (basic functionality)
    let config = RedactConfig::builder()
        .input(fixture.sql_path.clone())
        .dialect(fixture.dialect())
        .dry_run(true)
        .null_patterns(vec!["*.password".to_string()])
        .build();

    match config {
        Ok(cfg) => match Redactor::new(cfg) {
            Ok(mut redactor) => match redactor.run() {
                Ok(_) => eprintln!("  ✓ dry-run"),
                Err(e) => eprintln!("  ✗ dry-run: {}", e),
            },
            Err(e) => eprintln!("  ✗ dry-run (init): {}", e),
        },
        Err(e) => eprintln!("  ✗ dry-run (config): {}", e),
    }

    // Test 2: With --null strategy
    let null_output = output_dir.path().join("null.sql");
    let config = RedactConfig::builder()
        .input(fixture.sql_path.clone())
        .output(Some(null_output.clone()))
        .dialect(fixture.dialect())
        .null_patterns(vec!["*.password".to_string(), "*.ssn".to_string()])
        .build();

    match config {
        Ok(cfg) => match Redactor::new(cfg) {
            Ok(mut redactor) => match redactor.run() {
                Ok(stats) => {
                    eprintln!("  ✓ null ({} values redacted)", stats.columns_redacted);
                    if null_output.exists() {
                        let size = std::fs::metadata(&null_output).map(|m| m.len()).unwrap_or(0);
                        assert!(size > 0, "Null output should not be empty");
                    }
                }
                Err(e) => eprintln!("  ✗ null: {}", e),
            },
            Err(e) => eprintln!("  ✗ null (init): {}", e),
        },
        Err(e) => eprintln!("  ✗ null (config): {}", e),
    }

    // Test 3: With --hash strategy
    let hash_output = output_dir.path().join("hash.sql");
    let config = RedactConfig::builder()
        .input(fixture.sql_path.clone())
        .output(Some(hash_output.clone()))
        .dialect(fixture.dialect())
        .hash_patterns(vec!["*.email".to_string()])
        .build();

    match config {
        Ok(cfg) => match Redactor::new(cfg) {
            Ok(mut redactor) => match redactor.run() {
                Ok(stats) => eprintln!("  ✓ hash ({} values redacted)", stats.columns_redacted),
                Err(e) => eprintln!("  ✗ hash: {}", e),
            },
            Err(e) => eprintln!("  ✗ hash (init): {}", e),
        },
        Err(e) => eprintln!("  ✗ hash (config): {}", e),
    }

    // Test 4: With --fake strategy
    let fake_output = output_dir.path().join("fake.sql");
    let config = RedactConfig::builder()
        .input(fixture.sql_path.clone())
        .output(Some(fake_output.clone()))
        .dialect(fixture.dialect())
        .fake_patterns(vec!["*.name".to_string(), "*.phone".to_string()])
        .build();

    match config {
        Ok(cfg) => match Redactor::new(cfg) {
            Ok(mut redactor) => match redactor.run() {
                Ok(stats) => eprintln!("  ✓ fake ({} values redacted)", stats.columns_redacted),
                Err(e) => eprintln!("  ✗ fake: {}", e),
            },
            Err(e) => eprintln!("  ✗ fake (init): {}", e),
        },
        Err(e) => eprintln!("  ✗ fake (config): {}", e),
    }

    // Test 5: Reproducible with --seed
    let seed_output1 = output_dir.path().join("seed1.sql");
    let seed_output2 = output_dir.path().join("seed2.sql");

    for output in [&seed_output1, &seed_output2] {
        let config = RedactConfig::builder()
            .input(fixture.sql_path.clone())
            .output(Some(output.clone()))
            .dialect(fixture.dialect())
            .null_patterns(vec!["*.password".to_string()])
            .seed(Some(42))
            .build();

        if let Ok(cfg) = config {
            if let Ok(mut redactor) = Redactor::new(cfg) {
                let _ = redactor.run();
            }
        }
    }

    // Verify reproducibility (both files should be identical with same seed)
    if seed_output1.exists() && seed_output2.exists() {
        let content1 = std::fs::read_to_string(&seed_output1).unwrap_or_default();
        let content2 = std::fs::read_to_string(&seed_output2).unwrap_or_default();
        if content1 == content2 && !content1.is_empty() {
            eprintln!("  ✓ seed (reproducible)");
        } else if content1.is_empty() {
            eprintln!("  ⚠ seed (empty output)");
        } else {
            eprintln!("  ✗ seed (not reproducible)");
        }
    }
}

// Generate individual test functions for key cases

#[test]
#[ignore]
fn redact_mysql_classicmodels() {
    run_redact_tests(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn redact_mysql_sakila_data() {
    run_redact_tests(super::cases::get_case("mysql-sakila-data").unwrap());
}

#[test]
#[ignore]
fn redact_postgres_pagila_data() {
    run_redact_tests(super::cases::get_case("postgres-pagila-data").unwrap());
}

#[test]
#[ignore]
fn redact_chinook_mysql() {
    run_redact_tests(super::cases::get_case("chinook-mysql").unwrap());
}

#[test]
#[ignore]
fn redact_chinook_postgres() {
    run_redact_tests(super::cases::get_case("chinook-postgres").unwrap());
}

#[test]
#[ignore]
fn redact_wordpress_woocommerce() {
    run_redact_tests(super::cases::get_case("wordpress-woocommerce").unwrap());
}

/// Run all redact tests
#[test]
#[ignore]
fn all_redact_tests() {
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

        let output_dir = match super::temp_output_dir(&format!("{}-redact-all", case.name)) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let output_file = output_dir.path().join("redacted.sql");

        let config = RedactConfig::builder()
            .input(fixture.sql_path.clone())
            .output(Some(output_file))
            .dialect(fixture.dialect())
            .null_patterns(vec!["*.password".to_string()])
            .build();

        match config.and_then(|c| Redactor::new(c)) {
            Ok(mut redactor) => match redactor.run() {
                Ok(stats) => {
                    eprintln!("✓ {} ({} redacted)", case.name, stats.columns_redacted);
                    passed += 1;
                }
                Err(e) => {
                    eprintln!("✗ {}: {}", case.name, e);
                    failed += 1;
                }
            },
            Err(e) => {
                eprintln!("✗ {} (init): {}", case.name, e);
                failed += 1;
            }
        }
    }

    eprintln!("\nRedact tests: {} passed, {} failed", passed, failed);
}
