//! Tests for the visitor-based, memory-bounded row parsing (Task 17).
//!
//! Covers: streaming visitor output equals the collecting parser; callbacks can
//! stop early; a live-row counter never exceeds two rows; and `visit_events`
//! keeps its buffer well under 1 MiB even for a single 100k-row INSERT/COPY.

use std::cell::Cell;
use std::io::Write;

use sql_splitter::parser::mysql_insert::{
    parse_insert_rows_with, visit_insert_rows_with, PkValue, RowExtraction,
};
use sql_splitter::parser::postgres_copy::{
    parse_postgres_copy_rows_with, visit_postgres_copy_rows_with,
};
use sql_splitter::parser::{Parser, ParserEvent, RowFlow, SqlDialect};
use sql_splitter::schema::{SchemaBuilder, TableSchema};

const ROWS: usize = 100_000;
const ONE_MIB: usize = 1024 * 1024;

fn schema_t(dialect: SqlDialect) -> sql_splitter::schema::Schema {
    let mut b = SchemaBuilder::new();
    b.ingest_statement(b"CREATE TABLE t (id INT PRIMARY KEY, name TEXT);", dialect);
    b.build()
}

/// RAII guard that tracks how many parsed rows are alive at once.
struct LiveGuard<'a> {
    live: &'a Cell<usize>,
}

impl<'a> LiveGuard<'a> {
    fn new(live: &'a Cell<usize>, max: &Cell<usize>) -> Self {
        live.set(live.get() + 1);
        max.set(max.get().max(live.get()));
        Self { live }
    }
}

impl Drop for LiveGuard<'_> {
    fn drop(&mut self) {
        self.live.set(self.live.get() - 1);
    }
}

// -----------------------------------------------------------------------------
// Equivalence: streaming visitor == collecting parser
// -----------------------------------------------------------------------------

#[test]
fn row_visitors_insert_equivalence() -> anyhow::Result<()> {
    let schema = schema_t(SqlDialect::MySql);
    let t: &TableSchema = schema.get_table("t").unwrap();

    // Representative rows: plain, comma-in-string, paren-in-string, doubled and
    // backslash-escaped quotes, and NULL.
    let stmt = br"INSERT INTO t (id, name) VALUES (1, 'a'),(2, 'b, c'),(3, 'd)e'),(4, 'f''g'),(5, 'h\'i'),(6, NULL);";

    let collected = parse_insert_rows_with(stmt, t, SqlDialect::MySql, RowExtraction::Full)?;

    let mut streamed = Vec::new();
    visit_insert_rows_with(stmt, t, SqlDialect::MySql, RowExtraction::Full, |row| {
        streamed.push(row);
        Ok(RowFlow::Continue)
    })?;

    assert_eq!(collected.len(), 6);
    assert_eq!(streamed.len(), collected.len());
    for (c, s) in collected.iter().zip(&streamed) {
        assert_eq!(format!("{c:?}"), format!("{s:?}"));
    }
    Ok(())
}

#[test]
fn row_visitors_copy_equivalence() -> anyhow::Result<()> {
    let schema = schema_t(SqlDialect::Postgres);
    let t: &TableSchema = schema.get_table("t").unwrap();

    let data = b"1\ta\n2\tb c\n3\t\\N\n4\thas\ttab\n";
    let cols = vec!["id".to_string(), "name".to_string()];

    let collected = parse_postgres_copy_rows_with(data, t, cols.clone(), RowExtraction::Full)?;

    let mut streamed = Vec::new();
    visit_postgres_copy_rows_with(data, t, cols, RowExtraction::Full, |row| {
        streamed.push(row);
        Ok(RowFlow::Continue)
    })?;

    assert_eq!(streamed.len(), collected.len());
    for (c, s) in collected.iter().zip(&streamed) {
        assert_eq!(format!("{c:?}"), format!("{s:?}"));
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Early stop
// -----------------------------------------------------------------------------

#[test]
fn row_visitors_stop_early() -> anyhow::Result<()> {
    let schema = schema_t(SqlDialect::MySql);
    let t = schema.get_table("t").unwrap();

    let mut stmt = b"INSERT INTO t (id, name) VALUES ".to_vec();
    for i in 1..=100u64 {
        if i > 1 {
            stmt.push(b',');
        }
        stmt.extend_from_slice(format!("({i}, 'row{i}')").as_bytes());
    }
    stmt.push(b';');

    let mut seen = 0_u64;
    visit_insert_rows_with(&stmt, t, SqlDialect::MySql, RowExtraction::Full, |row| {
        seen += 1;
        assert_eq!(row.get_column_value(0), Some(&PkValue::Int(seen as i64)));
        Ok(if seen == 10 {
            RowFlow::Stop
        } else {
            RowFlow::Continue
        })
    })?;

    assert_eq!(seen, 10, "callback Stop must halt after exactly 10 rows");
    Ok(())
}

// -----------------------------------------------------------------------------
// Bounded live-row retention: never accumulates the whole block
// -----------------------------------------------------------------------------

#[test]
fn row_visitors_bounded_live_rows_insert() -> anyhow::Result<()> {
    let schema = schema_t(SqlDialect::MySql);
    let t = schema.get_table("t").unwrap();

    let mut stmt = b"INSERT INTO t (id, name) VALUES ".to_vec();
    for i in 1..=ROWS {
        if i > 1 {
            stmt.push(b',');
        }
        stmt.extend_from_slice(format!("({i}, 'r{i}')").as_bytes());
    }
    stmt.push(b';');

    let live = Cell::new(0usize);
    let max = Cell::new(0usize);
    let mut seen = 0usize;
    visit_insert_rows_with(&stmt, t, SqlDialect::MySql, RowExtraction::Full, |_row| {
        let _g = LiveGuard::new(&live, &max);
        seen += 1;
        Ok(RowFlow::Continue)
    })?;

    assert_eq!(seen, ROWS);
    assert!(
        max.get() <= 2,
        "live parsed rows peaked at {} (expected <= 2)",
        max.get()
    );
    Ok(())
}

#[test]
fn row_visitors_bounded_live_rows_copy() -> anyhow::Result<()> {
    let schema = schema_t(SqlDialect::Postgres);
    let t = schema.get_table("t").unwrap();

    let mut data = Vec::new();
    for i in 1..=ROWS {
        data.extend_from_slice(format!("{i}\tr{i}\n").as_bytes());
    }
    data.extend_from_slice(b"\\.\n");
    let cols = vec!["id".to_string(), "name".to_string()];

    let live = Cell::new(0usize);
    let max = Cell::new(0usize);
    let mut seen = 0usize;
    visit_postgres_copy_rows_with(&data, t, cols, RowExtraction::Full, |_row| {
        let _g = LiveGuard::new(&live, &max);
        seen += 1;
        Ok(RowFlow::Continue)
    })?;

    assert_eq!(seen, ROWS);
    assert!(
        max.get() <= 2,
        "live parsed COPY rows peaked at {} (expected <= 2)",
        max.get()
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// Parser::visit_events keeps buffering bounded for a single huge block
// -----------------------------------------------------------------------------

#[test]
fn visit_events_bounded_buffer_insert() -> anyhow::Result<()> {
    let mut file = tempfile::NamedTempFile::new()?;
    {
        let mut w = std::io::BufWriter::new(file.as_file_mut());
        w.write_all(b"INSERT INTO t (id, name) VALUES ")?;
        for i in 1..=ROWS {
            if i > 1 {
                w.write_all(b",")?;
            }
            write!(w, "({i}, 'row number {i}')")?;
        }
        w.write_all(b";\n")?;
        w.flush()?;
    }

    let mut parser = Parser::with_dialect(file.reopen()?, 64 * 1024, SqlDialect::MySql);
    let mut rows = 0usize;
    parser.visit_events(|event| {
        if let ParserEvent::InsertRow { .. } = event {
            rows += 1;
        }
        Ok(RowFlow::Continue)
    })?;

    assert_eq!(rows, ROWS, "every INSERT tuple should surface as an event");
    assert!(
        parser.peak_buffered() < ONE_MIB,
        "peak buffering was {} bytes (expected < 1 MiB)",
        parser.peak_buffered()
    );
    Ok(())
}

#[test]
fn visit_events_bounded_buffer_copy() -> anyhow::Result<()> {
    let mut file = tempfile::NamedTempFile::new()?;
    {
        let mut w = std::io::BufWriter::new(file.as_file_mut());
        w.write_all(b"COPY t (id, name) FROM stdin;\n")?;
        for i in 1..=ROWS {
            writeln!(w, "{i}\trow number {i}")?;
        }
        w.write_all(b"\\.\n")?;
        w.flush()?;
    }

    let mut parser = Parser::with_dialect(file.reopen()?, 64 * 1024, SqlDialect::Postgres);
    let mut rows = 0usize;
    let mut ends = 0usize;
    parser.visit_events(|event| {
        match event {
            ParserEvent::CopyRow(_) => rows += 1,
            ParserEvent::CopyEnd => ends += 1,
            _ => {}
        }
        Ok(RowFlow::Continue)
    })?;

    assert_eq!(rows, ROWS, "every COPY line should surface as an event");
    assert_eq!(
        ends, 1,
        "the COPY block should close with exactly one CopyEnd"
    );
    assert!(
        parser.peak_buffered() < ONE_MIB,
        "peak buffering was {} bytes (expected < 1 MiB)",
        parser.peak_buffered()
    );
    Ok(())
}
