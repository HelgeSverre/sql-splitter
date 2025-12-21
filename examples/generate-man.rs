//! Generate man pages for sql-splitter
//!
//! Run with: cargo run --example generate-man
//!
//! This generates man pages in the `man/` directory.
//!
//! Man page conventions for subcommands (following git, cargo, docker):
//! - File name: sql-splitter-diff.1 (hyphenated)
//! - NAME section: sql-splitter-diff (hyphenated)  
//! - SYNOPSIS: sql-splitter diff [OPTIONS] (space-separated, what user types)

use clap::CommandFactory;
use clap_mangen::Man;
use sql_splitter::cmd::Cli;
use std::fs;
use std::io::Result;
use std::path::Path;

fn main() -> Result<()> {
    let man_dir = Path::new("man");
    fs::create_dir_all(man_dir)?;

    let cmd = Cli::command();

    // Generate main man page: sql-splitter(1)
    let man = Man::new(cmd.clone());
    let mut buffer = Vec::new();
    man.render(&mut buffer)?;
    fs::write(man_dir.join("sql-splitter.1"), buffer)?;
    println!("Generated: man/sql-splitter.1");

    // Generate man pages for each subcommand: sql-splitter-<cmd>(1)
    // Following git/cargo/docker convention:
    // - Man page title uses hyphenated form: sql-splitter-diff(1)
    // - SYNOPSIS uses space form: sql-splitter diff [OPTIONS]
    for subcommand in cmd.get_subcommands() {
        let name = subcommand.get_name();
        if name == "help" {
            continue;
        }

        let mut sub = subcommand.clone();
        
        // Set the page title (NAME section) to hyphenated form
        let page_title: &'static str = Box::leak(format!("sql-splitter-{}", name).into_boxed_str());
        sub = sub.name(page_title);
        
        // Set bin_name to space-separated form for SYNOPSIS
        // This is what the user actually types
        let bin_name = format!("sql-splitter {}", name);
        sub = sub.bin_name(bin_name);

        let man = Man::new(sub);
        let mut buffer = Vec::new();
        man.render(&mut buffer)?;

        let filename = format!("sql-splitter-{}.1", name);
        fs::write(man_dir.join(&filename), buffer)?;
        println!("Generated: man/{}", filename);
    }

    println!("\nMan pages generated in man/ directory.");
    println!("View with: man ./man/sql-splitter.1");
    println!("Install with: sudo cp man/*.1 /usr/local/share/man/man1/");

    Ok(())
}
