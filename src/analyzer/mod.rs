//! SQL dump analyzer for gathering per-table statistics.

use crate::parser::{determine_buffer_size, Parser, SqlDialect, StatementType};
use crate::splitter::{open_input, open_input_with_progress};
use ahash::AHashMap;
use serde::Serialize;
use std::io::Read;
use std::path::PathBuf;

/// Per-table statistics gathered during analysis.
#[derive(Debug, Clone, Serialize)]
pub struct TableStats {
    /// Name of the table.
    pub table_name: String,
    /// Number of INSERT (or COPY) statements for this table.
    pub insert_count: u64,
    /// Number of CREATE TABLE statements for this table.
    pub create_count: u64,
    /// Total bytes of all statements for this table.
    pub total_bytes: u64,
    /// Total number of statements for this table.
    pub statement_count: u64,
}

impl TableStats {
    fn new(table_name: String) -> Self {
        Self {
            table_name,
            insert_count: 0,
            create_count: 0,
            total_bytes: 0,
            statement_count: 0,
        }
    }
}

/// Streaming SQL dump analyzer that gathers per-table statistics.
pub struct Analyzer {
    input_file: PathBuf,
    dialect: SqlDialect,
    stats: AHashMap<String, TableStats>,
}

impl Analyzer {
    /// Create a new analyzer for the given input file.
    pub fn new(input_file: PathBuf) -> Self {
        Self {
            input_file,
            dialect: SqlDialect::default(),
            stats: AHashMap::new(),
        }
    }

    /// Set the SQL dialect for parsing.
    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Run the analysis, returning sorted table statistics.
    pub fn analyze(mut self) -> anyhow::Result<Vec<TableStats>> {
        let file_size = std::fs::metadata(&self.input_file)?.len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.dialect;

        // Open the input, transparently handling any supported compression
        // format (including zip archives).
        let reader: Box<dyn Read> = open_input(&self.input_file)?;

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            if stmt_type == StatementType::Unknown || table_name.is_empty() {
                continue;
            }

            self.update_stats(&table_name, stmt_type, stmt.len() as u64);
        }

        Ok(self.get_sorted_stats())
    }

    /// Run the analysis with a progress callback, returning sorted table statistics.
    pub fn analyze_with_progress<F: Fn(u64) + 'static>(
        mut self,
        progress_fn: F,
    ) -> anyhow::Result<Vec<TableStats>> {
        let file_size = std::fs::metadata(&self.input_file)?.len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.dialect;

        // Open the input, transparently handling any supported compression
        // format (including zip archives).
        let reader: Box<dyn Read> =
            open_input_with_progress(&self.input_file, Box::new(progress_fn))?;

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            if stmt_type == StatementType::Unknown || table_name.is_empty() {
                continue;
            }

            self.update_stats(&table_name, stmt_type, stmt.len() as u64);
        }

        Ok(self.get_sorted_stats())
    }

    fn update_stats(&mut self, table_name: &str, stmt_type: StatementType, bytes: u64) {
        let stats = self
            .stats
            .entry(table_name.to_string())
            .or_insert_with(|| TableStats::new(table_name.to_string()));

        stats.statement_count += 1;
        stats.total_bytes += bytes;

        match stmt_type {
            StatementType::CreateTable => stats.create_count += 1,
            StatementType::Insert | StatementType::Copy => stats.insert_count += 1,
            _ => {}
        }
    }

    fn get_sorted_stats(&self) -> Vec<TableStats> {
        let mut result: Vec<TableStats> = self.stats.values().cloned().collect();
        result.sort_by(|a, b| b.insert_count.cmp(&a.insert_count));
        result
    }
}
