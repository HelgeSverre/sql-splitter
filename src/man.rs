//! Man page generation for the `sql-splitter man` developer subcommand.
//!
//! Man page conventions for subcommands (following git, cargo, docker):
//! - File name: sql-splitter-diff.1 (hyphenated)
//! - NAME section: sql-splitter-diff (hyphenated)
//! - SYNOPSIS: sql-splitter diff [OPTIONS] (space-separated, what user types)

use crate::cmd::Cli;
use anyhow::Result;
use clap::CommandFactory;
use clap_mangen::Man;
use std::fs;
use std::path::Path;

pub fn generate(output: &Path) -> Result<()> {
    fs::create_dir_all(output)?;

    let cmd = Cli::command();

    let man = Man::new(cmd.clone());
    let mut buffer = Vec::new();
    man.render(&mut buffer)?;
    fs::write(output.join("sql-splitter.1"), buffer)?;
    println!("Generated: {}", output.join("sql-splitter.1").display());

    for subcommand in cmd.get_subcommands() {
        let name = subcommand.get_name();
        if name == "help" {
            continue;
        }

        let mut sub = subcommand.clone();

        // NAME section uses hyphenated form; SYNOPSIS uses space form (what the user types).
        let page_title: &'static str = Box::leak(format!("sql-splitter-{}", name).into_boxed_str());
        sub = sub.name(page_title);
        sub = sub.bin_name(format!("sql-splitter {}", name));

        let man = Man::new(sub);
        let mut buffer = Vec::new();
        man.render(&mut buffer)?;

        let filename = format!("sql-splitter-{}.1", name);
        fs::write(output.join(&filename), buffer)?;
        println!("Generated: {}", output.join(&filename).display());
    }

    println!("\nMan pages generated in {} directory.", output.display());
    println!("View with: man {}/sql-splitter.1", output.display());

    Ok(())
}
