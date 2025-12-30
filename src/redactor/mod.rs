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
mod rewriter;
pub mod strategy;

pub use config::RedactConfig;
// These will be used when additional features are implemented
#[allow(unused_imports)]
pub use config::{RedactConfigBuilder, RedactYamlConfig, Rule};
pub use config_generator::generate_config;
pub use matcher::ColumnMatcher;
pub use rewriter::ValueRewriter;
pub use strategy::StrategyKind;

use crate::parser::postgres_copy::parse_copy_columns;
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
    rewriter: ValueRewriter,
    stats: RedactStats,
    /// Pending COPY header for PostgreSQL (header comes before data block)
    pending_copy: Option<PendingCopy>,
}

/// Pending COPY statement awaiting data block
struct PendingCopy {
    header: Vec<u8>,
    table_name: String,
    columns: Vec<String>,
}

impl Redactor {
    /// Create a new Redactor with the given configuration
    pub fn new(config: RedactConfig) -> anyhow::Result<Self> {
        // Build schema from input file (Pass 1)
        let schema = Self::build_schema(&config.input, config.dialect)?;

        // Build column matcher from config rules
        let matcher = ColumnMatcher::from_config(&config)?;

        // Create value rewriter with seed for reproducibility
        let rewriter = ValueRewriter::new(config.seed, config.dialect, config.locale.clone());

        Ok(Self {
            config,
            schema,
            matcher,
            rewriter,
            stats: RedactStats::default(),
            pending_copy: None,
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
                    // PostgreSQL COPY: store header, wait for data block
                    if self.config.dialect == SqlDialect::Postgres {
                        let header_str = String::from_utf8_lossy(&stmt);
                        let columns = parse_copy_columns(&header_str);
                        self.pending_copy = Some(PendingCopy {
                            header: stmt.clone(),
                            table_name: table_name.clone(),
                            columns,
                        });
                        // Don't output yet - wait for data block
                        continue;
                    }
                    self.redact_copy(&stmt, &table_name)?
                }
                StatementType::Unknown
                    if self.config.dialect == SqlDialect::Postgres
                        && self.pending_copy.is_some()
                        && (stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n")) =>
                {
                    // This is the COPY data block
                    self.redact_copy_data(&stmt)?
                }
                _ => {
                    // If we have a pending COPY that wasn't followed by a data block,
                    // output it as-is
                    if let Some(pending) = self.pending_copy.take() {
                        output.write_all(&pending.header)?;
                    }
                    stmt
                }
            };

            output.write_all(&redacted)?;
        }

        // Handle any remaining pending COPY header at EOF
        if let Some(pending) = self.pending_copy.take() {
            output.write_all(&pending.header)?;
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

        // Rewrite the INSERT statement with redacted values
        let (redacted, rows_redacted, cols_redacted) =
            self.rewriter
                .rewrite_insert(stmt, table_name, table, &strategies)?;

        // Update stats
        if rows_redacted > 0 {
            self.stats.rows_redacted += rows_redacted;
            self.stats.columns_redacted += cols_redacted;

            // Find or create table stats entry
            if let Some(ts) = self
                .stats
                .table_stats
                .iter_mut()
                .find(|t| t.name == table_name)
            {
                ts.rows_processed += rows_redacted;
                ts.columns_redacted += cols_redacted;
            } else {
                self.stats.tables_processed += 1;
                self.stats.table_stats.push(TableRedactStats {
                    name: table_name.to_string(),
                    rows_processed: rows_redacted,
                    columns_redacted: cols_redacted,
                });
            }
        }

        Ok(redacted)
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

        // Rewrite the COPY statement with redacted values
        let (redacted, rows_redacted, cols_redacted) =
            self.rewriter
                .rewrite_copy(stmt, table_name, table, &strategies)?;

        // Update stats
        if rows_redacted > 0 {
            self.stats.rows_redacted += rows_redacted;
            self.stats.columns_redacted += cols_redacted;

            // Find or create table stats entry
            if let Some(ts) = self
                .stats
                .table_stats
                .iter_mut()
                .find(|t| t.name == table_name)
            {
                ts.rows_processed += rows_redacted;
                ts.columns_redacted += cols_redacted;
            } else {
                self.stats.tables_processed += 1;
                self.stats.table_stats.push(TableRedactStats {
                    name: table_name.to_string(),
                    rows_processed: rows_redacted,
                    columns_redacted: cols_redacted,
                });
            }
        }

        Ok(redacted)
    }

    /// Redact a PostgreSQL COPY data block (comes after the header)
    fn redact_copy_data(&mut self, data_block: &[u8]) -> anyhow::Result<Vec<u8>> {
        let pending = self
            .pending_copy
            .take()
            .ok_or_else(|| anyhow::anyhow!("COPY data block without pending header"))?;

        let table_name = &pending.table_name;

        // Skip if table should be excluded
        if self.should_skip_table(table_name) {
            // Output header + data unchanged
            let mut result = pending.header;
            result.extend_from_slice(data_block);
            return Ok(result);
        }

        // Get table schema
        let Some(table) = self.schema.get_table(table_name) else {
            self.stats.warnings.push(format!(
                "No schema for table '{}', passing through unchanged",
                table_name
            ));
            let mut result = pending.header;
            result.extend_from_slice(data_block);
            return Ok(result);
        };

        // Get strategies for each column
        let strategies = self.matcher.get_strategies(table_name, table);

        // If no columns need redaction, pass through
        if strategies.iter().all(|s| matches!(s, StrategyKind::Skip)) {
            let mut result = pending.header;
            result.extend_from_slice(data_block);
            return Ok(result);
        }

        // Rewrite the COPY data block with redacted values
        let (redacted_data, rows_redacted, cols_redacted) =
            self.rewriter
                .rewrite_copy_data(data_block, table, &strategies, &pending.columns)?;

        // Update stats
        if rows_redacted > 0 {
            self.stats.rows_redacted += rows_redacted;
            self.stats.columns_redacted += cols_redacted;

            if let Some(ts) = self
                .stats
                .table_stats
                .iter_mut()
                .find(|t| t.name == *table_name)
            {
                ts.rows_processed += rows_redacted;
                ts.columns_redacted += cols_redacted;
            } else {
                self.stats.tables_processed += 1;
                self.stats.table_stats.push(TableRedactStats {
                    name: table_name.to_string(),
                    rows_processed: rows_redacted,
                    columns_redacted: cols_redacted,
                });
            }
        }

        // Combine header + redacted data
        // The header typically doesn't end with newline, so add one
        let mut result = pending.header;
        if !result.ends_with(b"\n") {
            result.push(b'\n');
        }
        result.extend_from_slice(&redacted_data);
        Ok(result)
    }

    /// Check if a table should be skipped
    fn should_skip_table(&self, name: &str) -> bool {
        // Check exclude list
        if self
            .config
            .exclude
            .iter()
            .any(|e| e.eq_ignore_ascii_case(name))
        {
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
