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
//! - [`convert_copy_to_insert_values`]: COPY-row to INSERT-VALUES conversion

use crate::parser::mysql_insert::{
    parse_insert_tuple, FkRef, InsertRowContext, ParsedRow, PkTuple, PkValue, RowExtraction,
};
use crate::parser::postgres_copy::{parse_copy_columns, CopyParser, ParsedCopyRow};
use crate::parser::{ContentFilter, Parser, ParserEvent, SqlDialect};
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

// The row/event control-flow signal now lives in the parser layer so the
// streaming row visitors and `Parser::visit_events` can share it; re-exported
// here so the sample/shard consumers keep their existing import path.
pub use crate::parser::RowFlow;

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

    // Per-statement/block context, rebuilt lazily as headers arrive so each
    // streamed row is parsed with the same column mapping the collecting parser
    // would have used — but only the current row is ever materialized.
    let mut insert_ctx: Option<InsertRowContext> = None;
    let mut copy_ctx: Option<(CopyParser<'_>, bool)> = None;

    parser.visit_events(|event| match event {
        // DDL/control statements carry no data rows here.
        ParserEvent::Statement(_) => Ok(RowFlow::Continue),
        ParserEvent::InsertRow {
            header,
            row,
            first_in_statement,
        } => {
            if first_in_statement || insert_ctx.is_none() {
                insert_ctx = Some(InsertRowContext::from_header(header, table_schema, dialect));
            }
            let ctx = insert_ctx.as_ref().expect("insert_ctx set above");
            match parse_insert_tuple(row, table_schema, ctx, dialect, extraction) {
                Some(parsed) => f(UnifiedRow::Insert(parsed)),
                None => Ok(RowFlow::Continue),
            }
        }
        ParserEvent::CopyStart(header) => {
            let columns = parse_copy_columns(&String::from_utf8_lossy(header));
            let (parser, empty_line_is_row) = CopyParser::new(&[])
                .with_schema(table_schema)
                .with_column_order(columns)
                .with_extraction(extraction)
                .prepared();
            copy_ctx = Some((parser, empty_line_is_row));
            Ok(RowFlow::Continue)
        }
        ParserEvent::CopyRow(line) => match copy_ctx.as_ref() {
            Some((cp, empty_line_is_row)) => match cp.parse_line(line, *empty_line_is_row) {
                Some(parsed) => f(UnifiedRow::Copy(parsed)),
                None => Ok(RowFlow::Continue),
            },
            None => Ok(RowFlow::Continue),
        },
        ParserEvent::CopyEnd => Ok(RowFlow::Continue),
    })
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

/// Quote a possibly schema-qualified table identifier for the given dialect.
pub fn quote_ident(dialect: SqlDialect, name: &str) -> String {
    // Table identities are canonical dotted names. Quote each identifier part
    // separately: quoting `tenant.users` as one identifier changes it from a
    // schema-qualified table reference into a table literally named
    // `tenant.users`.
    split_qualified_identifier(name)
        .unwrap_or_else(|| vec![name.to_string()])
        .iter()
        .map(|part| quote_identifier_part(dialect, part))
        .collect::<Vec<_>>()
        .join(".")
}

/// Quote exactly one identifier component for the given dialect.
///
/// Use this for column, index, and constraint names. In contrast to
/// [`quote_ident`], dots are literal characters here rather than schema
/// separators.
pub fn quote_identifier(dialect: SqlDialect, name: &str) -> String {
    quote_identifier_part(dialect, name)
}

/// Split the dotted table identity while preserving a quoted component that
/// contains a literal dot, such as `public."user.log"`.
fn split_qualified_identifier(name: &str) -> Option<Vec<String>> {
    let bytes = name.as_bytes();
    let mut i = 0;
    let mut parts = Vec::new();

    while i < bytes.len() {
        let mut part = Vec::new();
        if bytes[i] == b'"' {
            i += 1;
            let mut terminated = false;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if bytes.get(i + 1) == Some(&b'"') {
                        part.push(b'"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    terminated = true;
                    break;
                }
                part.push(bytes[i]);
                i += 1;
            }
            if !terminated {
                return None;
            }
        } else {
            while i < bytes.len() && bytes[i] != b'.' {
                part.push(bytes[i]);
                i += 1;
            }
        }

        if part.is_empty() {
            return None;
        }
        parts.push(String::from_utf8_lossy(&part).into_owned());

        if i == bytes.len() {
            return Some(parts);
        }
        if bytes[i] != b'.' {
            return None;
        }
        i += 1;
    }

    None
}

fn quote_identifier_part(dialect: SqlDialect, name: &str) -> String {
    // Double any occurrence of the closing delimiter so an identifier can never
    // terminate its own quoting early (malformed / injectable SQL otherwise).
    match dialect {
        SqlDialect::MySql => format!("`{}`", name.replace('`', "``")),
        SqlDialect::Postgres | SqlDialect::Sqlite => {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        SqlDialect::Mssql => format!("[{}]", name.replace(']', "]]")),
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
            // A RowFormat::Insert row was captured verbatim from an input
            // INSERT in this same dialect (sample/shard never cross-convert
            // dialects), so it is re-emitted as-is. Rewriting escapes here — e.g.
            // \' -> '' — corrupts native literals such as a value ending in a
            // backslash.
            RowFormat::Insert => row_bytes.clone(),
            RowFormat::Copy => convert_copy_to_insert_values(row_bytes, dialect),
        };
        writer.write_all(&values)?;
    }

    writer.write_all(b";\n")?;
    Ok(())
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
            // Decode the COPY text-format escapes (\n \t \\ …) to the real
            // bytes first, then re-escape for the target dialect's string
            // literal. Emitting the raw COPY bytes here would leave `\n` as the
            // two characters backslash+n (and MySQL would double the backslash
            // into `\\n`), corrupting embedded newlines/tabs/backslashes.
            let decoded = decode_copy_escapes(field);
            result.push(b'\'');
            for &b in &decoded {
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

/// Decode PostgreSQL COPY text-format escape sequences in one field to the
/// bytes they represent. Mirrors the decoder in `convert::copy_to_insert` but
/// stays byte-oriented so binary/`bytea` data survives the round-trip.
fn decode_copy_escapes(field: &[u8]) -> Vec<u8> {
    // Fast path: no escapes at all -> the field is already the raw bytes.
    if !field.contains(&b'\\') {
        return field.to_vec();
    }

    let mut out = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'\\' && i + 1 < field.len() {
            let next = field[i + 1];
            match next {
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'\\' => out.push(b'\\'),
                b'b' => out.push(0x08),
                b'f' => out.push(0x0C),
                b'v' => out.push(0x0B),
                _ if next.is_ascii_digit() => {
                    // Octal escape (\NNN, up to 3 digits).
                    let mut val = 0u8;
                    let mut consumed = 0;
                    for j in 0..3 {
                        match field.get(i + 1 + j) {
                            Some(&d @ b'0'..=b'7') => {
                                val = val.wrapping_mul(8).wrapping_add(d - b'0');
                                consumed += 1;
                            }
                            _ => break,
                        }
                    }
                    if consumed > 0 {
                        out.push(val);
                        i += 1 + consumed;
                        continue;
                    }
                    // Not actually octal: keep the backslash and the byte.
                    out.push(b'\\');
                    out.push(next);
                }
                // Unknown escape: keep the backslash and the following byte.
                _ => {
                    out.push(b'\\');
                    out.push(next);
                }
            }
            i += 2;
        } else {
            out.push(field[i]);
            i += 1;
        }
    }
    out
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
    fn quote_ident_keeps_qualified_table_parts_separate() {
        assert_eq!(
            quote_ident(SqlDialect::Postgres, "tenant_a.users"),
            "\"tenant_a\".\"users\""
        );
        assert_eq!(
            quote_ident(SqlDialect::MySql, "tenant_a.users"),
            "`tenant_a`.`users`"
        );
        assert_eq!(quote_ident(SqlDialect::Mssql, "dbo.users"), "[dbo].[users]");
        assert_eq!(
            quote_ident(SqlDialect::Postgres, "public.\"user.log\""),
            "\"public\".\"user.log\""
        );
    }

    #[test]
    fn quote_ident_escapes_the_closing_delimiter() {
        // A delimiter character inside an identifier must be doubled so it cannot
        // terminate the quoted identifier early (malformed / injectable SQL).
        assert_eq!(quote_ident(SqlDialect::MySql, "a`b"), "`a``b`");
        assert_eq!(quote_ident(SqlDialect::Postgres, "a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_ident(SqlDialect::Sqlite, "a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_ident(SqlDialect::Mssql, "a]b"), "[a]]b]");
    }

    #[test]
    fn quote_identifier_keeps_literal_dots_in_one_component() {
        assert_eq!(
            quote_identifier(SqlDialect::Postgres, "settings.theme"),
            "\"settings.theme\""
        );
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
