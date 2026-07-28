//! Regression tests for the bug-hunt sweep (2026-07-28). Each test reproduces
//! a confirmed defect from the sweep and asserts the corrected behavior.

use sql_splitter::convert::{parse_copy_data, Converter, CopyValue};
use sql_splitter::parser::mysql_insert::parse_insert_for_bulk;
use sql_splitter::parser::{Parser, ParserEvent, RowFlow, SqlDialect, StatementType};
use sql_splitter::transform_common::{
    convert_copy_to_insert_values, write_insert_chunk, RowFormat,
};

fn read_all(sql: &[u8], dialect: SqlDialect, buf: usize) -> Vec<String> {
    let mut parser = Parser::with_dialect(sql, buf, dialect);
    let mut out = Vec::new();
    while let Some(stmt) = parser.read_statement().unwrap() {
        out.push(String::from_utf8_lossy(&stmt).to_string());
    }
    out
}

// --- DELIMITER handling (src/parser/mod.rs) ---------------------------------
// A mysqldump --routines body contains ';'-terminated statements. Without
// DELIMITER awareness the parser chops the routine at each internal ';' and a
// body 'INSERT INTO audit ...' fragment leaks into the audit table file.

#[test]
fn delimiter_keeps_routine_body_intact() {
    let sql = b"CREATE TABLE audit (id INT);\n\
DELIMITER ;;\n\
CREATE DEFINER=`root`@`localhost` PROCEDURE cleanup()\n\
BEGIN\n\
  UPDATE counters SET n = n + 1;\n\
  INSERT INTO audit VALUES (999);\n\
END ;;\n\
DELIMITER ;\n";

    for buf in [16usize, 32, 64, 4096] {
        let stmts = read_all(sql, SqlDialect::MySql, buf);
        assert_eq!(stmts.len(), 2, "buffer {buf}: got {stmts:?}");
        assert!(
            stmts[0].contains("CREATE TABLE audit"),
            "buffer {buf}: {stmts:?}"
        );
        assert!(
            stmts[1].contains("CREATE DEFINER") && stmts[1].contains("PROCEDURE cleanup"),
            "buffer {buf}: routine not kept whole: {stmts:?}"
        );
        assert!(
            stmts[1].contains("INSERT INTO audit VALUES (999)"),
            "buffer {buf}: body INSERT must stay inside the routine: {stmts:?}"
        );
        assert!(
            !stmts
                .iter()
                .any(|s| s.trim_start().starts_with("INSERT INTO audit")),
            "buffer {buf}: body INSERT leaked as a standalone statement: {stmts:?}"
        );
    }
}

#[test]
fn delimiter_reset_restores_semicolon() {
    // After 'DELIMITER ;' the plain semicolon terminates statements again.
    let sql = b"DELIMITER ;;\nSELECT 1;;\nDELIMITER ;\nSELECT 2; SELECT 3;\n";
    let stmts: Vec<String> = read_all(sql, SqlDialect::MySql, 4096)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(stmts.len(), 3, "{stmts:?}");
    assert!(stmts[0].contains("SELECT 1"));
    assert!(stmts[1].contains("SELECT 2"));
    assert!(stmts[2].contains("SELECT 3"));
}

// --- VALUES-less INSERT (src/parser/mod.rs visit_insert_stmt) ---------------
// An INSERT...SELECT has no VALUES clause. The header scan must stop at the
// statement's ';' instead of locking onto a later statement's VALUES keyword,
// which would swallow the intervening statement and misattribute rows.

#[derive(Debug)]
enum Ev {
    Stmt(String),
    Row { header: String, row: String },
}

fn collect_events(sql: &[u8], dialect: SqlDialect) -> Vec<Ev> {
    let mut parser = Parser::with_dialect(sql, 4096, dialect);
    let mut events = Vec::new();
    parser
        .visit_events(|e| {
            match e {
                ParserEvent::Statement(s) => {
                    events.push(Ev::Stmt(String::from_utf8_lossy(s).to_string()))
                }
                ParserEvent::InsertRow { header, row, .. } => events.push(Ev::Row {
                    header: String::from_utf8_lossy(header).to_string(),
                    row: String::from_utf8_lossy(row).to_string(),
                }),
                _ => {}
            }
            Ok(RowFlow::Continue)
        })
        .unwrap();
    events
}

#[test]
fn insert_select_does_not_swallow_following_statement() {
    let sql = b"INSERT INTO log SELECT * FROM old_log;\n\
CREATE TABLE users (id INT PRIMARY KEY);\n\
INSERT INTO users VALUES (1);\n";
    let events = collect_events(sql, SqlDialect::MySql);

    let rows: Vec<&Ev> = events
        .iter()
        .filter(|e| matches!(e, Ev::Row { .. }))
        .collect();
    assert_eq!(rows.len(), 1, "exactly one real row expected: {events:?}");
    if let Ev::Row { header, row } = rows[0] {
        assert!(
            header.contains("INSERT INTO users"),
            "wrong header: {header:?}"
        );
        assert!(
            !header.contains("CREATE TABLE"),
            "header swallowed a DDL statement: {header:?}"
        );
        assert!(
            !header.contains("SELECT"),
            "header swallowed the INSERT...SELECT: {header:?}"
        );
        assert_eq!(row.trim(), "(1)");
    }

    let saw_create = events
        .iter()
        .any(|e| matches!(e, Ev::Stmt(s) if s.contains("CREATE TABLE users")));
    assert!(saw_create, "CREATE TABLE users was swallowed: {events:?}");
}

// --- parse_row paren balance (src/parser/mysql_insert.rs) -------------------
// A value that is a parenthesized expression containing a quoted ')' must not
// corrupt tuple-boundary detection: the ')' lives inside a string literal at
// nested paren depth and must not decrement the paren counter.

#[test]
fn parse_row_balances_quoted_paren_at_nested_depth() {
    let stmt = b"INSERT INTO t VALUES ((')'), 2), ('x', 4);";
    let parsed = parse_insert_for_bulk(stmt, SqlDialect::MySql).unwrap();
    assert_eq!(
        parsed.rows.len(),
        2,
        "quoted ')' at nested depth corrupted tuple boundaries: {:?}",
        parsed.rows
    );
}

#[test]
fn parse_row_balances_quoted_comma_at_nested_depth() {
    // A quoted ',' inside a nested paren must not be treated as a tuple
    // separator, and the outer tuple must still close correctly.
    let stmt = b"INSERT INTO t VALUES ((',,,'), 1), (2, 3);";
    let parsed = parse_insert_for_bulk(stmt, SqlDialect::MySql).unwrap();
    assert_eq!(parsed.rows.len(), 2, "{:?}", parsed.rows);
}

// --- Postgres E'...' escape strings (src/parser/mod.rs) ---------------------
// Inside an E'…' escape string backslash escapes the following quote, so a
// backslash-escaped quote must not be treated as the string's closing quote.

#[test]
fn postgres_escape_string_quote_does_not_break_split() {
    // E'a\'b' is one string containing a'b; the statement ends at the ';'
    // after the closing quote, not at the escaped quote inside.
    let sql = b"INSERT INTO t VALUES (E'a\\'b');\nINSERT INTO u VALUES (1);\n";
    for buf in [8usize, 16, 4096] {
        let stmts: Vec<String> = read_all(sql, SqlDialect::Postgres, buf)
            .into_iter()
            .filter(|s| !s.trim().is_empty())
            .collect();
        assert_eq!(stmts.len(), 2, "buffer {buf}: {stmts:?}");
        assert!(
            stmts[0].contains("INSERT INTO t"),
            "buffer {buf}: {stmts:?}"
        );
        assert!(
            stmts[1].contains("INSERT INTO u"),
            "buffer {buf}: {stmts:?}"
        );
    }
}

#[test]
fn postgres_plain_string_backslash_stays_literal() {
    // Outside an E'…' string, Postgres treats backslash as a literal byte, so a
    // trailing backslash before the closing quote must still close the string.
    let sql = b"INSERT INTO t VALUES ('C:\\');\nINSERT INTO u VALUES (1);\n";
    let stmts: Vec<String> = read_all(sql, SqlDialect::Postgres, 4096)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(stmts.len(), 2, "{stmts:?}");
}

// --- UTF-8 BOM (src/parser/mod.rs) ------------------------------------------
// A UTF-8 BOM at the start of a dump must not prevent the first statement from
// classifying (otherwise split drops it as Unknown).

#[test]
fn utf8_bom_first_statement_classifies() {
    let stmt = b"\xEF\xBB\xBFCREATE TABLE users (id INT);";
    let (ty, table) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::MySql);
    assert_eq!(
        ty,
        StatementType::CreateTable,
        "BOM-prefixed CREATE not classified"
    );
    assert_eq!(table, "users");
}

// --- COPY -> INSERT escape decoding (src/transform_common.rs) ----------------
// sample/shard turning a COPY row into INSERT VALUES must decode COPY text
// escapes (\n \t \\) to the real bytes before re-quoting, not emit them
// verbatim (and, for MySQL, must not double an escape backslash).

#[test]
fn copy_backslash_decoded_before_requoting() {
    // COPY field encoding of C:\Users\bob is C:\\Users\\bob.
    let field = b"C:\\\\Users\\\\bob";

    // Postgres (standard strings): backslash is literal -> ('C:\Users\bob').
    let pg = convert_copy_to_insert_values(field, SqlDialect::Postgres);
    assert_eq!(
        String::from_utf8_lossy(&pg),
        "('C:\\Users\\bob')",
        "postgres: COPY backslash not decoded"
    );

    // MySQL: backslash must be escaped exactly once -> ('C:\\Users\\bob').
    let my = convert_copy_to_insert_values(field, SqlDialect::MySql);
    assert_eq!(
        String::from_utf8_lossy(&my),
        "('C:\\\\Users\\\\bob')",
        "mysql: COPY backslash mis-encoded"
    );
}

#[test]
fn copy_newline_escape_decoded_to_real_newline() {
    // COPY field 'line1\nline2' encodes an embedded newline.
    let field = b"line1\\nline2";
    let pg = convert_copy_to_insert_values(field, SqlDialect::Postgres);
    assert_eq!(
        pg,
        b"('line1\nline2')",
        "postgres: COPY \\n not decoded to a real newline: {:?}",
        String::from_utf8_lossy(&pg)
    );
}

// --- native Postgres INSERT rows are not rewritten (src/transform_common.rs) -
// sample/shard parse and emit in one dialect, so an INSERT-sourced row is
// already native and must be passed through, not have \' rewritten to ''.

#[test]
fn native_postgres_insert_row_not_rewritten() {
    // ('a\') is a valid native Postgres literal (value = a\).
    let chunk = vec![(RowFormat::Insert, b"('a\\')".to_vec())];
    let mut out = Vec::new();
    write_insert_chunk(&mut out, "\"t\"", &chunk, SqlDialect::Postgres).unwrap();
    let s = String::from_utf8(out).unwrap();
    assert!(s.contains("('a\\')"), "native row was rewritten: {s:?}");
    assert!(
        !s.contains("('a'')"),
        "backslash-quote corrupted to doubled quote: {s:?}"
    );
}

// --- convert cross-dialect string escapes (src/convert/mod.rs) ---------------

#[test]
fn convert_mysql_to_mssql_converts_backslash_escape() {
    // T-SQL has no backslash escapes; MySQL \' must become a doubled '' quote.
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Mssql);
    let out = c
        .convert_statement(b"INSERT INTO t VALUES ('It\\'s');")
        .unwrap();
    let s = String::from_utf8_lossy(&out);
    assert!(
        s.contains("'It''s'"),
        "MySQL escape not converted for MSSQL: {s}"
    );
    assert!(
        !s.contains("\\'"),
        "backslash escape leaked into T-SQL: {s}"
    );
}

#[test]
fn convert_mysql_to_sqlite_decodes_backslash() {
    // MySQL literal 'C:\\Users' means C:\Users; under standard strings the
    // output must contain a single literal backslash, not a doubled one.
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Sqlite);
    let out = c
        .convert_statement(b"INSERT INTO t VALUES ('C:\\\\Users');")
        .unwrap();
    let s = String::from_utf8_lossy(&out);
    assert!(
        s.contains("'C:\\Users'"),
        "backslash escape not decoded: {s}"
    );
    assert!(!s.contains("C:\\\\Users"), "double backslash leaked: {s}");
}

// --- convert COPY parser CRLF (src/convert/copy_to_insert.rs) ----------------
// A CRLF-terminated COPY block must not produce phantom rows (from a bare \r
// line or a \.\r terminator) and must not keep the \r in the last column.

#[test]
fn convert_copy_data_handles_crlf() {
    let data = b"1\tAlice\r\n2\tBob\r\n\\.\r\n";
    let rows = parse_copy_data(data);
    assert_eq!(rows.len(), 2, "phantom rows from CRLF: {rows:?}");
    match &rows[0][1] {
        CopyValue::Text(s) => assert_eq!(s, "Alice", "trailing CR retained in last column"),
        other => panic!("unexpected {other:?}"),
    }
}
