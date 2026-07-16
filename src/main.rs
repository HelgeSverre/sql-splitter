// Allow dead code for items that are part of the public API but only used in tests
#![allow(dead_code)]

mod analyzer;
#[cfg(feature = "archive")]
mod archive;
mod cmd;
mod convert;
mod differ;
#[cfg(feature = "duckdb-query")]
mod duckdb;
mod graph;
mod json_schema;
mod merger;
mod parser;
mod pk;
mod progress;
mod redactor;
mod sample;
mod schema;
mod shard;
mod splitter;
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
