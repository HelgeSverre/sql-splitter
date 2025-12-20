mod analyzer;
mod cmd;
mod parser;
mod splitter;
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
