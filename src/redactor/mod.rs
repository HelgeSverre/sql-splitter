//! Redactor module for anonymizing sensitive data in SQL dumps.
//!
//! This module provides:
//! - YAML configuration parsing for redaction rules
//! - Column pattern matching with glob support
//! - Multiple redaction strategies (null, constant, hash, mask, shuffle, fake, skip)
//! - Streaming redaction of INSERT and COPY statements
//! - Config auto-generation from schema analysis

mod config;
mod config_generator;
mod matcher;
pub mod strategy;

pub use config::RedactConfig;
// These will be used when additional features are implemented
#[allow(unused_imports)]
pub use config::{RedactConfigBuilder, RedactYamlConfig, Rule};
pub use config_generator::generate_config;
pub use matcher::ColumnMatcher;
pub use strategy::StrategyKind;

use crate::parser::{Parser, SqlDialect, StatementType};
use crate::schema::{Schema, SchemaBuilder};
use ahash::AHashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Statistics from redaction operation
#[derive(Debug, Default, serde::Serialize)]
pub struct RedactStats {
    /// Number of tables processed
    pub tables_processed: usize,
    /// Number of rows redacted
    pub rows_redacted: u64,
    /// Number of columns redacted
    pub columns_redacted: u64,
    /// Per-table statistics
    pub table_stats: Vec<TableRedactStats>,
    /// Warning messages
    pub warnings: Vec<String>,
}

/// Per-table redaction statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct TableRedactStats {
    pub name: String,
    pub rows_processed: u64,
    pub columns_redacted: u64,
}

/// Main redactor struct
pub struct Redactor {
    config: RedactConfig,
    schema: Schema,
    matcher: ColumnMatcher,
    stats: RedactStats,
}

impl Redactor {
    /// Create a new Redactor with the given configuration
    pub fn new(config: RedactConfig) -> anyhow::Result<Self> {
        // Build schema from input file (Pass 1)
        let schema = Self::build_schema(&config.input, config.dialect)?;

        // Build column matcher from config rules
        let matcher = ColumnMatcher::from_config(&config)?;

        Ok(Self {
            config,
            schema,
            matcher,
            stats: RedactStats::default(),
        })
    }

    /// Build schema from input file
    fn build_schema(input: &Path, dialect: SqlDialect) -> anyhow::Result<Schema> {
        let file = File::open(input)?;
        let mut parser = Parser::with_dialect(file, 64 * 1024, dialect);
        let mut builder = SchemaBuilder::new();

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, _table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            if stmt_type == StatementType::CreateTable {
                let stmt_str = String::from_utf8_lossy(&stmt);
                builder.parse_create_table(&stmt_str);
            }
        }

        Ok(builder.build())
    }

    /// Run the redaction process
    pub fn run(&mut self) -> anyhow::Result<RedactStats> {
        if self.config.dry_run {
            return self.dry_run();
        }

        // Open output
        let output: Box<dyn Write> = if let Some(ref path) = self.config.output {
            Box::new(BufWriter::new(File::create(path)?))
        } else {
            Box::new(std::io::stdout())
        };

        self.process_file(output)?;

        Ok(std::mem::take(&mut self.stats))
    }

    /// Dry run - analyze without writing
    fn dry_run(&mut self) -> anyhow::Result<RedactStats> {
        let file = File::open(&self.config.input)?;
        let mut parser = Parser::with_dialect(file, 64 * 1024, self.config.dialect);

        let mut tables_seen: AHashMap<String, u64> = AHashMap::new();

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, self.config.dialect);

            if !table_name.is_empty()
                && (stmt_type == StatementType::Insert || stmt_type == StatementType::Copy)
            {
                *tables_seen.entry(table_name).or_insert(0) += 1;
            }
        }

        // Build stats from dry run
        for (name, count) in tables_seen {
            if let Some(table) = self.schema.get_table(&name) {
                let columns_matched = self.matcher.count_matches(&name, table);
                if columns_matched > 0 {
                    self.stats.tables_processed += 1;
                    self.stats.rows_redacted += count;
                    self.stats.columns_redacted += columns_matched as u64 * count;
                    self.stats.table_stats.push(TableRedactStats {
                        name,
                        rows_processed: count,
                        columns_redacted: columns_matched as u64,
                    });
                }
            }
        }

        Ok(std::mem::take(&mut self.stats))
    }

    /// Process the file and write redacted output
    fn process_file(&mut self, mut output: Box<dyn Write>) -> anyhow::Result<()> {
        let file = File::open(&self.config.input)?;
        let mut parser = Parser::with_dialect(file, 64 * 1024, self.config.dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, self.config.dialect);

            let redacted = match stmt_type {
                StatementType::Insert if !table_name.is_empty() => {
                    self.redact_insert(&stmt, &table_name)?
                }
                StatementType::Copy if !table_name.is_empty() => {
                    self.redact_copy(&stmt, &table_name)?
                }
                _ => stmt,
            };

            output.write_all(&redacted)?;
        }

        output.flush()?;
        Ok(())
    }

    /// Redact an INSERT statement
    fn redact_insert(&mut self, stmt: &[u8], table_name: &str) -> anyhow::Result<Vec<u8>> {
        // Skip if table should be excluded
        if self.should_skip_table(table_name) {
            return Ok(stmt.to_vec());
        }

        // Get table schema
        let Some(table) = self.schema.get_table(table_name) else {
            self.stats.warnings.push(format!(
                "No schema for table '{}', passing through unchanged",
                table_name
            ));
            return Ok(stmt.to_vec());
        };

        // Get strategies for each column
        let strategies = self.matcher.get_strategies(table_name, table);

        // If no columns need redaction, pass through
        if strategies.iter().all(|s| matches!(s, StrategyKind::Skip)) {
            return Ok(stmt.to_vec());
        }

        // TODO: Implement actual INSERT rewriting in Phase 3
        // For now, pass through
        Ok(stmt.to_vec())
    }

    /// Redact a COPY statement (PostgreSQL)
    fn redact_copy(&mut self, stmt: &[u8], table_name: &str) -> anyhow::Result<Vec<u8>> {
        // Skip if table should be excluded
        if self.should_skip_table(table_name) {
            return Ok(stmt.to_vec());
        }

        // Get table schema
        let Some(table) = self.schema.get_table(table_name) else {
            self.stats.warnings.push(format!(
                "No schema for table '{}', passing through unchanged",
                table_name
            ));
            return Ok(stmt.to_vec());
        };

        // Get strategies for each column
        let strategies = self.matcher.get_strategies(table_name, table);

        // If no columns need redaction, pass through
        if strategies.iter().all(|s| matches!(s, StrategyKind::Skip)) {
            return Ok(stmt.to_vec());
        }

        // TODO: Implement actual COPY rewriting in Phase 3
        // For now, pass through
        Ok(stmt.to_vec())
    }

    /// Check if a table should be skipped
    fn should_skip_table(&self, name: &str) -> bool {
        // Check exclude list
        if self.config.exclude.iter().any(|e| e.eq_ignore_ascii_case(name)) {
            return true;
        }

        // Check include list (if specified)
        if let Some(ref tables) = self.config.tables_filter {
            if !tables.iter().any(|t| t.eq_ignore_ascii_case(name)) {
                return true;
            }
        }

        false
    }
}
