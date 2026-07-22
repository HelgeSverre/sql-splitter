//! Shared test-support helpers, included by individual integration test
//! binaries via `mod support;`. Cargo does not treat a directory containing
//! `mod.rs` as its own test binary, so this module is compiled into whichever
//! `tests/*.rs` file references it.

pub mod generated_fixture;
