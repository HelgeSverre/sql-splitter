//! Real-world SQL dump verification tests.
//!
//! These tests download public SQL dumps and verify that sql-splitter can
//! correctly parse, split, convert, validate, redact, and generate ERDs from them.
//!
//! Run with: cargo nextest run --test realworld --run-ignored only
//!
//! Or run a specific test case:
//!   cargo nextest run --test realworld mysql_classicmodels --run-ignored only

#[path = "realworld_tests/mod.rs"]
mod realworld_tests;

pub use realworld_tests::*;
