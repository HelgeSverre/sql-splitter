//! Shared plumbing for the row-level transform commands (`sample`, `shard`).
//!
//! Both commands follow the same pipeline: split the dump into per-table temp
//! files, build a schema graph, walk each table's data rows, spill selected
//! rows to disk, then synthesize a single SQL output file. This module owns
//! the pieces that were previously duplicated between the two commands:
//!
//! - [`UnifiedRow`] / [`RowFormat`]: dialect-agnostic row representation
//! - [`for_each_data_row`]: the INSERT/COPY statement walker
//! - [`RowSpillWriter`] / [`RowSpillReader`]: bounded-memory row spilling
//! - [`split_to_temp_tables`] / [`build_schema_graph`]: pipeline phases 0-1
//! - [`write_dialect_header`] / [`write_dialect_footer`] / [`quote_ident`]:
//!   session preamble and identifier quoting
//! - [`convert_row_to_postgres`] / [`convert_copy_to_insert_values`]: output
//!   value conversion

use crate::parser::mysql_insert::{
    parse_insert_rows_with, FkRef, ParsedRow, PkTuple, PkValue, RowExtraction,
};
use crate::parser::postgres_copy::{
    parse_copy_columns, parse_postgres_copy_rows_with, ParsedCopyRow,
};
use crate::parser::{ContentFilter, Parser, SqlDialect, StatementType};
use crate::schema::{SchemaBuilder, SchemaGraph, TableSchema};
use crate::splitter::{Splitter, Stats as SplitStats};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Row format indicator for spilled/converted rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFormat {
    /// MySQL-style `(v1, v2, ...)` value list from an INSERT statement.
    Insert,
    /// PostgreSQL COPY tab-separated row.
    Copy,
}

impl RowFormat {
    fn tag(self) -> u8 {
        match self {
            RowFormat::Insert => 0,
            RowFormat::Copy => 1,
        }
    }

    fn from_tag(tag: u8) -> Self {
        if tag == 0 {
            RowFormat::Insert
        } else {
            RowFormat::Copy
        }
    }
}

/// Combined row representation for both MySQL INSERT and PostgreSQL COPY.
pub enum UnifiedRow {
    Insert(ParsedRow),
    Copy(ParsedCopyRow),
}

impl UnifiedRow {
    pub fn pk(&self) -> Option<&PkTuple> {
        match self {
            UnifiedRow::Insert(r) => r.pk.as_ref(),
            UnifiedRow::Copy(r) => r.pk.as_ref(),
        }
    }

    pub fn fk_values(&self) -> &[(FkRef, PkTuple)] {
        match self {
            UnifiedRow::Insert(r) => &r.fk_values,
            UnifiedRow::Copy(r) => &r.fk_values,
        }
    }

    /// Get the value for a specific schema column index (requires
    /// [`RowExtraction::Full`] parsing).
    pub fn get_column_value(&self, idx: usize) -> Option<&PkValue> {
        match self {
            UnifiedRow::Insert(r) => r.get_column_value(idx),
            UnifiedRow::Copy(r) => r.get_column_value(idx),
        }
    }

    pub fn format(&self) -> RowFormat {
        match self {
            UnifiedRow::Insert(_) => RowFormat::Insert,
            UnifiedRow::Copy(_) => RowFormat::Copy,
        }
    }

    pub fn raw(&self) -> &[u8] {
        match self {
            UnifiedRow::Insert(r) => &r.raw,
            UnifiedRow::Copy(r) => &r.raw,
        }
    }
}

/// Control flow signal returned by [`for_each_data_row`] callbacks.
pub enum RowFlow {
    /// Keep iterating.
    Continue,
    /// Skip the remaining rows of the current statement.
    SkipStatement,
    /// Stop the walk entirely.
    Stop,
}

/// Returns true if `stmt` is a PostgreSQL COPY data block (ends with the
/// `\.` terminator).
pub fn is_copy_data_block(stmt: &[u8]) -> bool {
    stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n")
}

/// Walk every data row of a per-table SQL file, invoking `f` for each row.
///
/// Handles the INSERT/COPY-header/COPY-data statement dispatch (including
/// tracking the COPY column order across statements) that was previously
/// hand-rolled in every sampling/sharding loop.
pub fn for_each_data_row<F>(
    table_file: &Path,
    table_schema: &TableSchema,
    dialect: SqlDialect,
    extraction: RowExtraction,
    mut f: F,
) -> anyhow::Result<()>
where
    F: FnMut(UnifiedRow) -> anyhow::Result<RowFlow>,
{
    let file = File::open(table_file)?;
    let mut parser = Parser::with_dialect(file, 64 * 1024, dialect);
    let mut copy_columns: Vec<String> = Vec::new();

    while let Some(stmt) = parser.read_statement()? {
        let (stmt_type, _) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

        match stmt_type {
            StatementType::Insert => {
                let rows = parse_insert_rows_with(&stmt, table_schema, dialect, extraction)?;
                for row in rows {
                    match f(UnifiedRow::Insert(row))? {
                        RowFlow::Continue => {}
                        RowFlow::SkipStatement => break,
                        RowFlow::Stop => return Ok(()),
                    }
                }
            }
            StatementType::Copy => {
                let header = String::from_utf8_lossy(&stmt);
                copy_columns = parse_copy_columns(&header);
            }
            StatementType::Unknown
                if dialect == SqlDialect::Postgres && is_copy_data_block(&stmt) =>
            {
                let rows = parse_postgres_copy_rows_with(
                    &stmt,
                    table_schema,
                    copy_columns.clone(),
                    extraction,
                )?;
                for row in rows {
                    match f(UnifiedRow::Copy(row))? {
                        RowFlow::Continue => {}
                        RowFlow::SkipStatement => break,
                        RowFlow::Stop => return Ok(()),
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Writer for spilling selected rows to a temp file with bounded memory.
///
/// Record format: 1-byte format tag, 4-byte little-endian length, raw row
/// bytes. Length-prefixed so rows containing newlines or non-UTF-8 bytes
/// round-trip exactly.
pub struct RowSpillWriter {
    writer: BufWriter<File>,
}

impl RowSpillWriter {
    pub fn create(path: &Path) -> io::Result<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(path)?),
        })
    }

    pub fn write_row(&mut self, format: RowFormat, raw: &[u8]) -> io::Result<()> {
        self.writer.write_all(&[format.tag()])?;
        self.writer.write_all(&(raw.len() as u32).to_le_bytes())?;
        self.writer.write_all(raw)
    }

    pub fn finish(mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Reader for row spill files written by [`RowSpillWriter`].
pub struct RowSpillReader {
    reader: BufReader<File>,
}

impl RowSpillReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    /// Read the next spilled row, or `None` at end of file.
    pub fn next_row(&mut self) -> io::Result<Option<(RowFormat, Vec<u8>)>> {
        let mut tag = [0u8; 1];
        match self.reader.read_exact(&mut tag) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut raw = vec![0u8; len];
        self.reader.read_exact(&mut raw)?;
        Ok(Some((RowFormat::from_tag(tag[0]), raw)))
    }
}

/// Result of the phase-0 split into per-table temp files.
pub struct SplitPhase {
    /// Owns the temp directory; dropped when the pipeline finishes.
    pub temp_dir: TempDir,
    /// Directory containing the per-table `.sql` files.
    pub tables_dir: PathBuf,
    /// Statistics from the split.
    pub stats: SplitStats,
}

/// Phase 0 shared by sample/shard: split the input dump into per-table temp
/// files, with an optional byte-based progress bar.
pub fn split_to_temp_tables(
    input: &Path,
    dialect: SqlDialect,
    progress: bool,
) -> anyhow::Result<SplitPhase> {
    // Get file size for progress tracking
    let file_size = std::fs::metadata(input)?.len();

    // Progress bar setup - byte-based for the split phase
    let progress_bar = if progress {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("Splitting dump...");
        Some(pb)
    } else {
        None
    };

    let temp_dir = TempDir::new()?;
    let tables_dir = temp_dir.path().join("tables");

    let mut splitter = Splitter::new(input.to_path_buf(), tables_dir.clone())
        .with_dialect(dialect)
        .with_content_filter(ContentFilter::All);

    if let Some(ref pb) = progress_bar {
        let pb_clone = pb.clone();
        splitter = splitter.with_progress(move |bytes| {
            pb_clone.set_position(bytes);
        });
    }

    let stats = splitter.split()?;

    // Finish byte-based progress, switch to milestone messages
    if let Some(ref pb) = progress_bar {
        pb.finish_and_clear();
    }

    if progress {
        eprintln!(
            "Split complete: {} tables, {} statements",
            stats.tables_found, stats.statements_processed
        );
    }

    Ok(SplitPhase {
        temp_dir,
        tables_dir,
        stats,
    })
}

/// Build a schema graph from a directory of split per-table files.
pub fn build_schema_graph(tables_dir: &Path, dialect: SqlDialect) -> anyhow::Result<SchemaGraph> {
    let mut builder = SchemaBuilder::new();

    for entry in fs::read_dir(tables_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|e| e == "sql") {
            let file = File::open(&path)?;
            let mut parser = Parser::with_dialect(file, 64 * 1024, dialect);

            while let Some(stmt) = parser.read_statement()? {
                builder.ingest_statement(&stmt, dialect);
            }
        }
    }

    Ok(SchemaGraph::from_schema(builder.build()))
}

/// Quote an identifier for the given dialect.
pub fn quote_ident(dialect: SqlDialect, name: &str) -> String {
    match dialect {
        SqlDialect::MySql => format!("`{}`", name),
        SqlDialect::Postgres | SqlDialect::Sqlite => format!("\"{}\"", name),
        SqlDialect::Mssql => format!("[{}]", name),
    }
}

/// Write the dialect-specific session header (FK checks off, encoding, etc.).
pub fn write_dialect_header<W: Write>(writer: &mut W, dialect: SqlDialect) -> io::Result<()> {
    match dialect {
        SqlDialect::MySql => {
            writeln!(writer, "SET NAMES utf8mb4;")?;
            writeln!(writer, "SET FOREIGN_KEY_CHECKS = 0;")?;
        }
        SqlDialect::Postgres => {
            writeln!(writer, "SET client_encoding = 'UTF8';")?;
            writeln!(writer, "SET session_replication_role = replica;")?;
        }
        SqlDialect::Sqlite => {
            writeln!(writer, "PRAGMA foreign_keys = OFF;")?;
        }
        SqlDialect::Mssql => {
            writeln!(writer, "SET ANSI_NULLS ON;")?;
            writeln!(writer, "SET QUOTED_IDENTIFIER ON;")?;
            writeln!(writer, "SET NOCOUNT ON;")?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

/// Write the dialect-specific session footer (restores header settings).
pub fn write_dialect_footer<W: Write>(writer: &mut W, dialect: SqlDialect) -> io::Result<()> {
    writeln!(writer)?;
    match dialect {
        SqlDialect::MySql => {
            writeln!(writer, "SET FOREIGN_KEY_CHECKS = 1;")?;
        }
        SqlDialect::Postgres => {
            writeln!(writer, "SET session_replication_role = DEFAULT;")?;
        }
        SqlDialect::Sqlite => {
            writeln!(writer, "PRAGMA foreign_keys = ON;")?;
        }
        SqlDialect::Mssql => {
            // No footer needed
        }
    }
    Ok(())
}

/// Write a chunk of spilled rows as a single multi-row INSERT statement.
pub fn write_insert_chunk<W: Write>(
    writer: &mut W,
    quoted_name: &str,
    chunk: &[(RowFormat, Vec<u8>)],
    dialect: SqlDialect,
) -> io::Result<()> {
    writeln!(writer, "INSERT INTO {} VALUES", quoted_name)?;

    for (i, (format, row_bytes)) in chunk.iter().enumerate() {
        if i > 0 {
            writer.write_all(b",\n")?;
        }

        let values = match format {
            RowFormat::Insert => match dialect {
                SqlDialect::Postgres => convert_row_to_postgres(row_bytes),
                _ => row_bytes.clone(),
            },
            RowFormat::Copy => convert_copy_to_insert_values(row_bytes, dialect),
        };
        writer.write_all(&values)?;
    }

    writer.write_all(b";\n")?;
    Ok(())
}

/// Convert a MySQL-style row to PostgreSQL syntax.
pub fn convert_row_to_postgres(row: &[u8]) -> Vec<u8> {
    // Simple conversion: just replace escaped quotes
    // A full implementation would handle more edge cases
    let mut result = Vec::with_capacity(row.len());
    let mut i = 0;

    while i < row.len() {
        if row[i] == b'\\' && i + 1 < row.len() && row[i + 1] == b'\'' {
            // MySQL: \' -> PostgreSQL: ''
            result.push(b'\'');
            result.push(b'\'');
            i += 2;
        } else {
            result.push(row[i]);
            i += 1;
        }
    }

    result
}

/// Convert PostgreSQL COPY format (tab-separated) to INSERT VALUES format.
pub fn convert_copy_to_insert_values(row: &[u8], dialect: SqlDialect) -> Vec<u8> {
    let mut result = Vec::with_capacity(row.len() + 20);
    result.push(b'(');

    let fields: Vec<&[u8]> = row.split(|&b| b == b'\t').collect();

    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            result.extend_from_slice(b", ");
        }

        // Check for NULL marker
        if *field == b"\\N" {
            result.extend_from_slice(b"NULL");
        } else if field.is_empty() {
            result.extend_from_slice(b"''");
        } else if is_numeric(field) {
            // Numeric value - no quotes needed
            result.extend_from_slice(field);
        } else {
            // String value - needs quoting
            result.push(b'\'');
            for &b in *field {
                match b {
                    b'\'' => {
                        // Escape single quote
                        match dialect {
                            SqlDialect::MySql => result.extend_from_slice(b"\\'"),
                            SqlDialect::Postgres | SqlDialect::Sqlite | SqlDialect::Mssql => {
                                result.extend_from_slice(b"''")
                            }
                        }
                    }
                    b'\\' if dialect == SqlDialect::MySql => {
                        // Escape backslash in MySQL
                        result.extend_from_slice(b"\\\\");
                    }
                    _ => result.push(b),
                }
            }
            result.push(b'\'');
        }
    }

    result.push(b')');
    result
}

/// Check if a byte slice represents a numeric value.
pub fn is_numeric(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }

    let mut has_digit = false;
    let mut has_dot = false;
    let mut start = 0;

    // Handle leading sign
    if s[0] == b'-' || s[0] == b'+' {
        start = 1;
    }

    for &b in &s[start..] {
        match b {
            b'0'..=b'9' => has_digit = true,
            b'.' if !has_dot => has_dot = true,
            b'e' | b'E' => {
                // Scientific notation - just check rest is digits
                continue;
            }
            _ => return false,
        }
    }

    has_digit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_roundtrip_preserves_newlines_and_binary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.rows");

        let rows: Vec<(RowFormat, Vec<u8>)> = vec![
            (RowFormat::Insert, b"(1, 'a\nb')".to_vec()),
            (RowFormat::Copy, vec![0xFF, 0xFE, b'\t', b'x', b'\n']),
            (RowFormat::Insert, Vec::new()),
        ];

        let mut w = RowSpillWriter::create(&path).unwrap();
        for (f, raw) in &rows {
            w.write_row(*f, raw).unwrap();
        }
        w.finish().unwrap();

        let mut r = RowSpillReader::open(&path).unwrap();
        for (f, raw) in &rows {
            let (rf, rraw) = r.next_row().unwrap().unwrap();
            assert_eq!(rf, *f);
            assert_eq!(&rraw, raw);
        }
        assert!(r.next_row().unwrap().is_none());
    }

    #[test]
    fn quote_ident_per_dialect() {
        assert_eq!(quote_ident(SqlDialect::MySql, "t"), "`t`");
        assert_eq!(quote_ident(SqlDialect::Postgres, "t"), "\"t\"");
        assert_eq!(quote_ident(SqlDialect::Sqlite, "t"), "\"t\"");
        assert_eq!(quote_ident(SqlDialect::Mssql, "t"), "[t]");
    }

    #[test]
    fn is_numeric_basics() {
        assert!(is_numeric(b"123"));
        assert!(is_numeric(b"-1.5"));
        assert!(is_numeric(b"1e10"));
        assert!(!is_numeric(b""));
        assert!(!is_numeric(b"abc"));
        assert!(!is_numeric(b"1.2.3"));
    }
}
