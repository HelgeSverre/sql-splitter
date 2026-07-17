// Allow dead code for items that are part of the public API but only used in tests
#![allow(dead_code)]
// The binary duplicates module compilation from `lib.rs` (each `mod` below
// is the same source file compiled twice: once for the library crate,
// once inlined here). Wider modules such as `generate`/`render`/`synthetic`
// re-export a staged API meant for library consumers (see `generate`'s
// module docs); nothing in the CLI itself reaches most of it, so those
// `pub use` items are legitimately unused from the binary's perspective.
#![allow(unused_imports)]

mod analyzer;
#[cfg(feature = "archive")]
mod archive;
mod cmd;
mod convert;
mod copy_data;
mod diagnostic;
mod differ;
#[cfg(feature = "duckdb-query")]
mod duckdb;
mod fake_data;
mod generate;
mod graph;
mod json_schema;
mod merger;
mod parser;
mod pk;
mod progress;
mod redactor;
mod render;
mod sample;
mod schema;
mod shard;
mod splitter;
mod synthetic;
mod transform_common;
mod validate;
mod writer;
#[cfg(feature = "archive")]
mod zip_input;

use clap::Parser;
use cmd::Cli;

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();

    match cmd::run(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{e}");
            std::process::ExitCode::FAILURE
        }
    }
}
