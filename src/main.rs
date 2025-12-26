// Allow dead code for items that are part of the public API but only used in tests
#![allow(dead_code)]

mod analyzer;
mod cmd;
mod convert;
mod differ;
mod graph;
mod merger;
mod parser;
mod pk;
mod progress;
mod redactor;
mod sample;
mod schema;
mod shard;
mod splitter;
mod validate;
mod writer;

use clap::Parser;
use cmd::Cli;

fn main() {
    let cli = Cli::parse();

    if let Err(e) = cmd::run(cli) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
