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
use crate::parser::postgres_copy::{
    decode_copy_escapes, parse_copy_columns, CopyParser, ParsedCopyRow,
};
use crate::parser::{ContentFilter, Parser, ParserEvent, SqlDialect};
use crate::render::sql_string::push_sql_literal;
use crate::schema::{SchemaBuilder, SchemaGraph, TableSchema};
use crate::splitter::{Splitter, Stats as SplitStats};
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

    let progress_bar = progress.then(|| {
        let pb = crate::cmd::common::byte_progress_bar(file_size);
        pb.set_message("Splitting dump...");
        pb
    });

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
            push_sql_literal(&mut result, dialect, &decoded);
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

// =============================================================================
// Shared sample/shard configuration and output helpers
// =============================================================================

/// How to handle global/lookup tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GlobalTableMode {
    /// Exclude global tables
    None,
    /// Include lookup tables in full (default)
    #[default]
    Lookups,
    /// Include all global tables in full
    All,
}

impl std::str::FromStr for GlobalTableMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(GlobalTableMode::None),
            "lookups" => Ok(GlobalTableMode::Lookups),
            "all" => Ok(GlobalTableMode::All),
            _ => Err(format!(
                "Unknown global mode: {}. Valid options: none, lookups, all",
                s
            )),
        }
    }
}

impl std::fmt::Display for GlobalTableMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GlobalTableMode::None => write!(f, "none"),
            GlobalTableMode::Lookups => write!(f, "lookups"),
            GlobalTableMode::All => write!(f, "all"),
        }
    }
}

/// Well-known system table patterns (matched by prefix or exact name).
pub const SYSTEM_TABLE_PATTERNS: &[&str] = &[
    "migrations",
    "failed_jobs",
    "job_batches",
    "jobs",
    "cache",
    "cache_locks",
    "sessions",
    "password_reset_tokens",
    "personal_access_tokens",
    "telescope_entries",
    "telescope_entries_tags",
    "telescope_monitoring",
    "pulse_",
    "horizon_",
];

/// Well-known lookup/global table names (matched exactly).
pub const LOOKUP_TABLE_PATTERNS: &[&str] = &[
    "countries",
    "states",
    "provinces",
    "cities",
    "currencies",
    "languages",
    "timezones",
    "permissions",
    "roles",
    "settings",
];

/// True if `table_name` matches a well-known system table pattern.
pub fn is_system_table(table_name: &str) -> bool {
    let lower = table_name.to_lowercase();
    SYSTEM_TABLE_PATTERNS
        .iter()
        .any(|p| lower.starts_with(p) || lower == *p)
}

/// True if `table_name` is a well-known lookup/global table.
pub fn is_lookup_table(table_name: &str) -> bool {
    let lower = table_name.to_lowercase();
    LOOKUP_TABLE_PATTERNS.iter().any(|p| lower == *p)
}

/// `selected / seen` as a percentage, 0.0 when nothing was seen.
pub fn percent(selected: u64, seen: u64) -> f64 {
    if seen > 0 {
        (selected as f64 / seen as f64) * 100.0
    } else {
        0.0
    }
}

/// One table's contribution to a sample/shard output file.
pub struct OutputTable<'a> {
    pub name: &'a str,
    pub rows_selected: u64,
    /// Spill file holding the selected rows; `None` writes schema only.
    pub spill_path: Option<&'a Path>,
}

/// Write the shared trailer of a sample/shard output header comment.
pub fn write_header_totals<W: Write + ?Sized>(
    writer: &mut W,
    rows_selected: u64,
    rows_seen: u64,
    fk_orphans_label: &str,
    fk_orphans: u64,
    warnings: &[String],
) -> io::Result<()> {
    writeln!(
        writer,
        "--   Total rows: {} (from {} original, {:.1}%)",
        rows_selected,
        rows_seen,
        percent(rows_selected, rows_seen)
    )?;
    if fk_orphans > 0 {
        writeln!(
            writer,
            "--   FK orphans {}: {}",
            fk_orphans_label, fk_orphans
        )?;
    }
    if !warnings.is_empty() {
        writeln!(writer, "--   Warnings: {}", warnings.len())?;
    }
    writeln!(writer)
}

/// Write a complete sample/shard output: `header`, the dialect session header,
/// each table's schema statements (copied from its split file), then its
/// selected rows as chunked multi-row INSERTs, and the dialect footer.
pub fn write_transform_output(
    output: Option<&Path>,
    dialect: SqlDialect,
    include_schema: bool,
    tables: &[OutputTable<'_>],
    tables_dir: &Path,
    header: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> anyhow::Result<()> {
    let mut writer: Box<dyn Write> = match output {
        Some(path) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            Box::new(BufWriter::with_capacity(256 * 1024, File::create(path)?))
        }
        None => Box::new(BufWriter::new(std::io::stdout())),
    };

    header(&mut writer)?;
    write_dialect_header(&mut writer, dialect)?;

    if include_schema {
        for table in tables {
            let table_file = tables_dir.join(format!("{}.sql", table.name));
            if !table_file.exists() {
                continue;
            }
            let mut parser = Parser::with_dialect(File::open(&table_file)?, 64 * 1024, dialect);
            while let Some(stmt) = parser.read_statement()? {
                let (stmt_type, _) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);
                if stmt_type.is_schema() {
                    writer.write_all(&stmt)?;
                    writer.write_all(b"\n")?;
                }
            }
        }
    }

    const CHUNK_SIZE: usize = 1000;
    let mut chunk: Vec<(RowFormat, Vec<u8>)> = Vec::with_capacity(CHUNK_SIZE);
    for table in tables {
        let Some(spill_path) = table.spill_path else {
            continue;
        };
        writeln!(
            writer,
            "\n-- Data: {} ({} rows)",
            table.name, table.rows_selected
        )?;
        let quoted_name = quote_ident(dialect, table.name);
        let mut spill_reader = RowSpillReader::open(spill_path)?;
        while let Some(row) = spill_reader.next_row()? {
            chunk.push(row);
            if chunk.len() >= CHUNK_SIZE {
                write_insert_chunk(&mut writer, &quoted_name, &chunk, dialect)?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            write_insert_chunk(&mut writer, &quoted_name, &chunk, dialect)?;
            chunk.clear();
        }
    }

    write_dialect_footer(&mut writer, dialect)?;
    writer.flush()?;
    Ok(())
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
