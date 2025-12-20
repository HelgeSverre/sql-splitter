//! Merger module for combining split SQL files back into a single dump.

use crate::parser::SqlDialect;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

/// Statistics from merge operation
#[derive(Debug, Default)]
pub struct MergeStats {
    pub tables_merged: usize,
    pub bytes_written: u64,
    pub table_names: Vec<String>,
}

/// Merger configuration
#[derive(Default)]
pub struct MergerConfig {
    pub dialect: SqlDialect,
    pub tables: Option<HashSet<String>>,
    pub exclude: HashSet<String>,
    pub add_transaction: bool,
    pub add_header: bool,
}

/// Merger for combining split SQL files
pub struct Merger {
    input_dir: PathBuf,
    output: Option<PathBuf>,
    config: MergerConfig,
}

impl Merger {
    pub fn new(input_dir: PathBuf, output: Option<PathBuf>) -> Self {
        Self {
            input_dir,
            output,
            config: MergerConfig::default(),
        }
    }

    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.config.dialect = dialect;
        self
    }

    pub fn with_tables(mut self, tables: HashSet<String>) -> Self {
        self.config.tables = Some(tables);
        self
    }

    pub fn with_exclude(mut self, exclude: HashSet<String>) -> Self {
        self.config.exclude = exclude;
        self
    }

    pub fn with_transaction(mut self, add_transaction: bool) -> Self {
        self.config.add_transaction = add_transaction;
        self
    }

    pub fn with_header(mut self, add_header: bool) -> Self {
        self.config.add_header = add_header;
        self
    }

    /// Run the merge operation
    pub fn merge(&self) -> anyhow::Result<MergeStats> {
        // Discover SQL files
        let sql_files = self.discover_sql_files()?;
        if sql_files.is_empty() {
            anyhow::bail!(
                "no .sql files found in directory: {}",
                self.input_dir.display()
            );
        }

        // Filter files
        let filtered_files: Vec<(String, PathBuf)> = sql_files
            .into_iter()
            .filter(|(name, _)| {
                let name_lower = name.to_lowercase();
                if let Some(ref include) = self.config.tables {
                    if !include.contains(&name_lower) {
                        return false;
                    }
                }
                !self.config.exclude.contains(&name_lower)
            })
            .collect();

        if filtered_files.is_empty() {
            anyhow::bail!("no tables remaining after filtering");
        }

        // Sort alphabetically
        let mut sorted_files = filtered_files;
        sorted_files.sort_by(|a, b| a.0.cmp(&b.0));

        // Merge to output
        if let Some(ref out_path) = self.output {
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = File::create(out_path)?;
            let writer = BufWriter::with_capacity(256 * 1024, file);
            self.merge_files(sorted_files, writer)
        } else {
            let stdout = io::stdout();
            let writer = BufWriter::new(stdout.lock());
            self.merge_files(sorted_files, writer)
        }
    }

    fn discover_sql_files(&self) -> anyhow::Result<Vec<(String, PathBuf)>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(&self.input_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext.eq_ignore_ascii_case("sql") {
                        if let Some(stem) = path.file_stem() {
                            let table_name = stem.to_string_lossy().to_string();
                            files.push((table_name, path));
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    fn merge_files<W: Write>(
        &self,
        files: Vec<(String, PathBuf)>,
        mut writer: W,
    ) -> anyhow::Result<MergeStats> {
        let mut stats = MergeStats::default();

        // Write header
        if self.config.add_header {
            self.write_header(&mut writer, files.len())?;
        }

        // Write transaction start
        if self.config.add_transaction {
            let tx_start = self.transaction_start();
            writer.write_all(tx_start.as_bytes())?;
            stats.bytes_written += tx_start.len() as u64;
        }

        // Merge each file
        for (table_name, path) in &files {
            // Write table separator
            let separator = format!(
                "\n-- ============================================================\n-- Table: {}\n-- ============================================================\n\n",
                table_name
            );
            writer.write_all(separator.as_bytes())?;
            stats.bytes_written += separator.len() as u64;

            // Stream file content
            let file = File::open(path)?;
            let reader = BufReader::with_capacity(64 * 1024, file);

            for line in reader.lines() {
                let line = line?;
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
                stats.bytes_written += line.len() as u64 + 1;
            }

            stats.table_names.push(table_name.clone());
            stats.tables_merged += 1;
        }

        // Write transaction end
        if self.config.add_transaction {
            let tx_end = "\nCOMMIT;\n";
            writer.write_all(tx_end.as_bytes())?;
            stats.bytes_written += tx_end.len() as u64;
        }

        // Write footer
        if self.config.add_header {
            self.write_footer(&mut writer)?;
        }

        writer.flush()?;

        Ok(stats)
    }

    fn write_header<W: Write>(&self, w: &mut W, table_count: usize) -> io::Result<()> {
        writeln!(w, "-- SQL Merge Output")?;
        writeln!(w, "-- Generated by sql-splitter")?;
        writeln!(w, "-- Tables: {}", table_count)?;
        writeln!(w, "-- Dialect: {}", self.config.dialect)?;
        writeln!(w)?;

        match self.config.dialect {
            SqlDialect::MySql => {
                writeln!(w, "SET NAMES utf8mb4;")?;
                writeln!(w, "SET FOREIGN_KEY_CHECKS = 0;")?;
            }
            SqlDialect::Postgres => {
                writeln!(w, "SET client_encoding = 'UTF8';")?;
            }
            SqlDialect::Sqlite => {
                writeln!(w, "PRAGMA foreign_keys = OFF;")?;
            }
        }
        writeln!(w)?;

        Ok(())
    }

    fn write_footer<W: Write>(&self, w: &mut W) -> io::Result<()> {
        writeln!(w)?;
        match self.config.dialect {
            SqlDialect::MySql => {
                writeln!(w, "SET FOREIGN_KEY_CHECKS = 1;")?;
            }
            SqlDialect::Postgres | SqlDialect::Sqlite => {}
        }
        Ok(())
    }

    fn transaction_start(&self) -> &'static str {
        match self.config.dialect {
            SqlDialect::MySql => "START TRANSACTION;\n\n",
            SqlDialect::Postgres => "BEGIN;\n\n",
            SqlDialect::Sqlite => "BEGIN TRANSACTION;\n\n",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_merge_basic() {
        let temp_dir = TempDir::new().unwrap();
        let input_dir = temp_dir.path().join("tables");
        let output_file = temp_dir.path().join("merged.sql");

        // Create input directory with some SQL files
        fs::create_dir_all(&input_dir).unwrap();
        fs::write(
            input_dir.join("users.sql"),
            "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\n",
        )
        .unwrap();
        fs::write(
            input_dir.join("posts.sql"),
            "CREATE TABLE posts (id INT);\n",
        )
        .unwrap();

        // Merge
        let merger = Merger::new(input_dir, Some(output_file.clone()))
            .with_dialect(SqlDialect::MySql)
            .with_header(true);

        let stats = merger.merge().unwrap();

        assert_eq!(stats.tables_merged, 2);
        assert!(stats.table_names.contains(&"users".to_string()));
        assert!(stats.table_names.contains(&"posts".to_string()));

        // Verify output
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(content.contains("CREATE TABLE users"));
        assert!(content.contains("CREATE TABLE posts"));
        assert!(content.contains("SET FOREIGN_KEY_CHECKS = 0"));
    }

    #[test]
    fn test_merge_with_filter() {
        let temp_dir = TempDir::new().unwrap();
        let input_dir = temp_dir.path().join("tables");
        let output_file = temp_dir.path().join("merged.sql");

        fs::create_dir_all(&input_dir).unwrap();
        fs::write(input_dir.join("users.sql"), "-- users\n").unwrap();
        fs::write(input_dir.join("posts.sql"), "-- posts\n").unwrap();
        fs::write(input_dir.join("comments.sql"), "-- comments\n").unwrap();

        let mut tables = HashSet::new();
        tables.insert("users".to_string());
        tables.insert("posts".to_string());

        let merger = Merger::new(input_dir, Some(output_file.clone()))
            .with_tables(tables)
            .with_header(false);

        let stats = merger.merge().unwrap();

        assert_eq!(stats.tables_merged, 2);
        assert!(!stats.table_names.contains(&"comments".to_string()));
    }

    #[test]
    fn test_merge_with_exclude() {
        let temp_dir = TempDir::new().unwrap();
        let input_dir = temp_dir.path().join("tables");
        let output_file = temp_dir.path().join("merged.sql");

        fs::create_dir_all(&input_dir).unwrap();
        fs::write(input_dir.join("users.sql"), "-- users\n").unwrap();
        fs::write(input_dir.join("cache.sql"), "-- cache\n").unwrap();
        fs::write(input_dir.join("sessions.sql"), "-- sessions\n").unwrap();

        let mut exclude = HashSet::new();
        exclude.insert("cache".to_string());
        exclude.insert("sessions".to_string());

        let merger = Merger::new(input_dir, Some(output_file.clone()))
            .with_exclude(exclude)
            .with_header(false);

        let stats = merger.merge().unwrap();

        assert_eq!(stats.tables_merged, 1);
        assert!(stats.table_names.contains(&"users".to_string()));
    }
}
