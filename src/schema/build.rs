//! Shared "stream a SQL file into a [`Schema`]" helper.
//!
//! Several commands (diff, graph, order, redact, sample, shard) need a schema
//! extracted from a dump's DDL before doing their real work. This module owns
//! that pass so every consumer gets identical CREATE TABLE / ALTER TABLE /
//! CREATE INDEX coverage instead of hand-rolling (and subtly diverging on)
//! the same loop.

use super::{Schema, SchemaBuilder};
use crate::parser::{determine_buffer_size, Parser, SqlDialect, StatementType};
use crate::splitter::open_input_opt_progress;
use std::path::Path;

impl SchemaBuilder {
    /// Feed an already-classified statement into the builder.
    ///
    /// Non-DDL statement types are ignored, so callers that classify
    /// statements for their own routing can pass everything through.
    pub fn ingest(&mut self, stmt_type: StatementType, sql: &str) {
        match stmt_type {
            StatementType::CreateTable => {
                self.parse_create_table(sql);
            }
            StatementType::AlterTable => {
                self.parse_alter_table(sql);
            }
            StatementType::CreateIndex => {
                self.parse_create_index(sql);
            }
            _ => {}
        }
    }

    /// Classify a raw statement and feed it into the builder if it is DDL.
    pub fn ingest_statement(&mut self, stmt: &[u8], dialect: SqlDialect) {
        let (stmt_type, _table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, dialect);

        if matches!(
            stmt_type,
            StatementType::CreateTable | StatementType::AlterTable | StatementType::CreateIndex
        ) {
            self.ingest(stmt_type, &String::from_utf8_lossy(stmt));
        }
    }
}

impl Schema {
    /// Build a [`Schema`] by streaming the DDL statements of a SQL file.
    ///
    /// Opens `path` via [`open_input_opt_progress`], so all supported
    /// compression formats (including zip archives) are handled
    /// transparently. When `progress_fn` is `Some`, it receives cumulative
    /// raw bytes read from disk.
    pub fn from_sql_file(
        path: &Path,
        dialect: SqlDialect,
        progress_fn: Option<Box<dyn Fn(u64)>>,
    ) -> anyhow::Result<Schema> {
        let file_size = std::fs::metadata(path)?.len();
        let buffer_size = determine_buffer_size(file_size);
        let reader = open_input_opt_progress(path, progress_fn)?;

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);
        let mut builder = SchemaBuilder::new();

        while let Some(stmt) = parser.read_statement()? {
            builder.ingest_statement(&stmt, dialect);
        }

        Ok(builder.build())
    }
}
