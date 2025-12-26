//! Real-world SQL dump verification tests.
//!
//! These tests download public SQL dumps and verify that sql-splitter can
//! correctly parse, split, convert, validate, redact, and generate ERDs from them.
//!
//! Run with: cargo test --test realworld -- --ignored
//!
//! Or run a specific test case:
//!   cargo test --test realworld mysql_classicmodels -- --ignored

#[path = "realworld_tests/mod.rs"]
mod realworld_tests;

pub use realworld_tests::*;
