//! Regression tests for the parser bugs documented in the 2026-07-14 review.
//! Each test reproduces the exact input from the bug register and asserts the
//! corrected behavior.

use sql_splitter::parser::mysql_insert::{parse_insert_for_bulk, InsertParser, ParsedValue};
use sql_splitter::parser::{detect_dialect, DialectConfidence, Parser, SqlDialect, StatementType};

fn read_all(sql: &[u8], dialect: SqlDialect, buf: usize) -> Vec<Vec<u8>> {
    let mut parser = Parser::with_dialect(sql, buf, dialect);
    let mut out = Vec::new();
    while let Some(stmt) = parser.read_statement().unwrap() {
        out.push(stmt);
    }
    out
}

// Bug #1: index name ending in "on" must not extract table "ON".
#[test]
fn bug1_create_index_name_ending_on() {
    let stmt = b"CREATE INDEX idx_position ON users (col);";
    let (ty, table) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Postgres);
    assert_eq!(ty, StatementType::CreateIndex);
    assert_eq!(table, "users");
}

// Bug #2: table names containing VALUES keep their column list and row count.
#[test]
fn bug2_table_name_with_values_substring() {
    let stmt = b"INSERT INTO product_values (id, name) VALUES (1, 'a'), (2, 'b');";
    let parsed = parse_insert_for_bulk(stmt, SqlDialect::MySql).unwrap();
    assert_eq!(parsed.table, "product_values");
    assert_eq!(
        parsed.columns,
        Some(vec!["id".to_string(), "name".to_string()])
    );
    assert_eq!(parsed.rows.len(), 2, "phantom row from column list");
}

#[test]
fn bug2_quoted_identifier_with_values() {
    let stmt = b"INSERT INTO `order values` (`id`) VALUES (1);";
    let parsed = parse_insert_for_bulk(stmt, SqlDialect::MySql).unwrap();
    assert_eq!(parsed.table, "order values");
    assert_eq!(parsed.columns, Some(vec!["id".to_string()]));
    assert_eq!(parsed.rows.len(), 1);
}

// Bug #3: backslash is literal for non-MySQL dialects.
#[test]
fn bug3_postgres_backslash_literal() {
    let stmt = br"INSERT INTO files (path) VALUES ('C:\temp\new');";
    let mut parser = InsertParser::new(stmt).with_dialect(SqlDialect::Postgres);
    let rows = parser.parse_rows().unwrap();
    match &rows[0].values[0] {
        ParsedValue::String { value } => assert_eq!(value, r"C:\temp\new"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn bug3_mysql_backslash_still_escaped() {
    let stmt = br"INSERT INTO files (path) VALUES ('a\nb');";
    let mut parser = InsertParser::new(stmt).with_dialect(SqlDialect::MySql);
    let rows = parser.parse_rows().unwrap();
    match &rows[0].values[0] {
        ParsedValue::String { value } => assert_eq!(value, "a\nb"),
        other => panic!("unexpected {other:?}"),
    }
}

// Bug #4: CREATE INDEX strips schema qualifier like CREATE TABLE does.
#[test]
fn bug4_create_index_strips_schema() {
    let stmt = b"CREATE INDEX idx_a ON public.users USING btree (col);";
    let (_, table) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Postgres);
    assert_eq!(table, "users");
}

// Bug #5: a ';' inside a block comment does not split the statement.
#[test]
fn bug5_semicolon_in_block_comment() {
    let sql = b"CREATE TABLE t (id int) /* has ; inside */ ;\nINSERT INTO t VALUES (1);";
    let stmts = read_all(sql, SqlDialect::MySql, 1024);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], b"CREATE TABLE t (id int) /* has ; inside */ ;");
}

// Bug #6: a two-byte token straddling a fill_buf boundary is still recognized.
#[test]
fn bug6_line_comment_straddling_boundary() {
    let sql = b"INSERT INTO t VALUES (1) -- comment ; tricky\n, (2);";
    // Force a tiny buffer so `--` and the interior ';' land across chunks.
    let stmts = read_all(sql, SqlDialect::MySql, 26);
    assert_eq!(
        stmts.len(),
        1,
        "interior ';' in a line comment split the stmt"
    );
}

// Bug #7: dollar-quote whose content begins with the tag text stays open.
#[test]
fn bug7_dollar_quote_content_starts_with_tag() {
    let sql = b"SELECT $fn$fn$ oops ; still inside $fn$;\nSELECT 2;";
    let stmts = read_all(sql, SqlDialect::Postgres, 1024);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], b"SELECT $fn$fn$ oops ; still inside $fn$;");
}

// Bug #8: MSSQL ]] escape does not close the bracket early.
#[test]
fn bug8_mssql_bracket_escape() {
    let sql = b"CREATE TABLE [we]]ird;name] (id int);\nGO\n";
    let stmts = read_all(sql, SqlDialect::Mssql, 1024);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], b"CREATE TABLE [we]]ird;name] (id int);");
}

// Bug #9: plain Postgres with array columns is not detected as MSSQL/High.
#[test]
fn bug9_postgres_array_not_mssql() {
    let header =
        b"CREATE TABLE tags (id serial, vals integer[]);\nINSERT INTO tags VALUES (1, '{1,2}');";
    let res = detect_dialect(header);
    assert_ne!(res.dialect, SqlDialect::Mssql);
    // And no false High-confidence MSSQL detection.
    assert!(!(res.dialect == SqlDialect::Mssql && res.confidence == DialectConfidence::High));
}

// Bug #10: MySQL backtick identifiers are tracked; interior ' does not merge.
#[test]
fn bug10_backtick_identifier_with_quote() {
    let sql = b"CREATE TABLE `it's` (id int);\nINSERT INTO x VALUES (1);";
    let stmts = read_all(sql, SqlDialect::MySql, 1024);
    assert_eq!(stmts.len(), 2);
}

// Bug #11: expression value with nested string/parens is not split.
#[test]
fn bug11_function_value_recovery() {
    let stmt = b"INSERT INTO geo (g) VALUES (ST_GeomFromText('POLYGON((0 0,1 1))'));";
    let mut parser = InsertParser::new(stmt);
    let rows = parser.parse_rows().unwrap();
    assert_eq!(rows[0].values.len(), 1, "value split inside its own string");
}

// Regression from the fix review: MySQL backslash escapes apply inside
// double-quoted strings too, so an escaped quote doesn't split the statement.
#[test]
fn regr_mysql_double_quote_backslash_escape() {
    let sql = br#"INSERT INTO t VALUES ("a\";b");
SELECT 2;"#;
    let stmts = read_all(sql, SqlDialect::MySql, 8);
    assert_eq!(
        stmts.len(),
        2,
        "split inside a double-quoted string literal"
    );
    assert_eq!(stmts[0], &br#"INSERT INTO t VALUES ("a\";b");"#[..]);
}

// Regression from the fix review: expression recovery must respect
// double-quoted strings, not just single-quoted ones.
#[test]
fn regr_double_quote_in_expression_value() {
    let stmt = br#"INSERT INTO t (a, b) VALUES (CONCAT("x)y", "z"), 2);"#;
    let mut parser = InsertParser::new(stmt);
    let rows = parser.parse_rows().unwrap();
    assert_eq!(
        rows[0].values.len(),
        2,
        "')' inside \"...\" ended value early"
    );
    match &rows[0].values[1] {
        ParsedValue::Integer(n) => assert_eq!(*n, 2),
        other => panic!("second value should be Integer(2), got {other:?}"),
    }
}

// Bug #6 worst-case: dollar-quote close tag straddling boundary is recognized.
#[test]
fn bug6_dollar_close_straddling_boundary() {
    let sql = b"SELECT $tag$ body here $tag$; SELECT 2;";
    for buf in [8usize, 16, 20, 23, 32] {
        let stmts = read_all(sql, SqlDialect::Postgres, buf);
        assert_eq!(
            stmts.len(),
            2,
            "buffer {buf} swallowed the rest of the file"
        );
    }
}
