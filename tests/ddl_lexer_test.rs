//! Public-API-observable consequences of the schema DDL lexer
//! (`schema::ddl::tokenize_ddl`, private) versus the convert DDL lexer
//! (`convert::ddl::tokenize`, `pub(crate)`).
//!
//! The direct side-by-side token-stream pins live in the unit test module of
//! `src/schema/ddl.rs` (both lexers are crate-private). This file pins what a
//! library user can see: how `split_table_body` splits a column list, and the
//! DEFAULT / CHECK values `SchemaBuilder` extracts, for the same tricky inputs.
//! The convert lexer's view is observed through `Converter` (MySQL→PostgreSQL
//! inline-enum rewriting walks the column list with it).

use sql_splitter::convert::Converter;
use sql_splitter::parser::SqlDialect;
use sql_splitter::schema::{split_table_body, SchemaBuilder, TableSchema};

fn table(stmt: &str) -> Option<TableSchema> {
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(stmt)?;
    builder.build().get_table("t").cloned()
}

fn column_names(stmt: &str) -> Vec<String> {
    table(stmt)
        .expect("table parsed")
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect()
}

fn default_of(stmt: &str, idx: usize) -> Option<String> {
    table(stmt).expect("table parsed").columns[idx]
        .default_sql
        .clone()
}

/// MySQL→PostgreSQL conversion of a CREATE TABLE; the enum columns that get
/// rewritten reveal which column definitions the convert lexer found.
fn mysql_to_pg(stmt: &str) -> String {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    String::from_utf8(converter.convert_statement(stmt.as_bytes()).unwrap()).unwrap()
}

// ---------------------------------------------------------------------------
// Backslash escapes: schema lexer always treats `\` as an escape
// ---------------------------------------------------------------------------

#[test]
fn backslash_quote_keeps_string_open_in_schema_lexer() {
    // The schema lexer has no dialect: `\'` never closes a string, so a
    // Postgres standard-string dump reading `'it\'` + `s, z'` is one literal.
    assert_eq!(
        split_table_body(r"a text DEFAULT 'it\'s, z', m mood"),
        vec![r"a text DEFAULT 'it\'s, z'", "m mood"]
    );
    let stmt = r"CREATE TABLE t (a text DEFAULT 'it\'s, z', m mood);";
    assert_eq!(column_names(stmt), ["a", "m"]);
    assert_eq!(default_of(stmt, 0).as_deref(), Some(r"'it\'s, z'"));
}

#[test]
fn backslash_quote_in_mysql_enum_label_is_one_column_for_convert_lexer() {
    // The convert lexer under MySQL rules decodes `\'`; the label list stays
    // in sync and both enum columns are rewritten.
    let out = mysql_to_pg(r"CREATE TABLE t (v ENUM('a\'b','c'), w ENUM('x','z'));");
    assert!(
        out.contains("CREATE TYPE enum__t__v AS ENUM ('a''b', 'c');"),
        "{out}"
    );
    assert!(
        out.ends_with("CREATE TABLE t (v enum__t__v, w enum__t__w);"),
        "{out}"
    );
}

#[test]
fn trailing_backslash_is_preserved_raw() {
    assert_eq!(split_table_body(r"'a\"), vec![r"'a\"]);
}

// ---------------------------------------------------------------------------
// Doubled quotes
// ---------------------------------------------------------------------------

#[test]
fn doubled_single_quote_stays_one_literal() {
    assert_eq!(
        split_table_body("a text DEFAULT 'it''s, z', m mood"),
        vec!["a text DEFAULT 'it''s, z'", "m mood"]
    );
    let stmt = "CREATE TABLE t (a text DEFAULT 'it''s, z', m mood);";
    assert_eq!(column_names(stmt), ["a", "m"]);
    assert_eq!(default_of(stmt, 0).as_deref(), Some("'it''s, z'"));

    let out = mysql_to_pg("CREATE TABLE t (v ENUM('it''s','c'), w ENUM('x','z'));");
    assert!(
        out.contains("CREATE TYPE enum__t__v AS ENUM ('it''s', 'c');"),
        "{out}"
    );
    assert!(
        out.ends_with("CREATE TABLE t (v enum__t__v, w enum__t__w);"),
        "{out}"
    );
}

// ---------------------------------------------------------------------------
// Quoted identifiers with embedded whitespace / delimiters
// ---------------------------------------------------------------------------

#[test]
fn schema_lexer_has_no_identifier_quoting() {
    // A doubled delimiter without whitespace survives as one opaque token...
    assert_eq!(split_table_body("`a``b` int"), vec!["`a``b` int"]);
    assert_eq!(split_table_body(r#""a""b" int"#), vec![r#""a""b" int"#]);
    assert_eq!(split_table_body("[a]]b] int"), vec!["[a]]b] int"]);

    // ...but whitespace inside a quoted identifier splits it: `\`a b\` int`
    // becomes column `a` with type `b \` int`.
    let stmt = "CREATE TABLE t (`a b` int, m mood);";
    assert_eq!(column_names(stmt), ["a", "m"]);
    assert_eq!(table(stmt).unwrap().columns[0].source_type, "b ` int");
    let stmt = r#"CREATE TABLE t ("a b" int, m mood);"#;
    assert_eq!(column_names(stmt), ["a", "m"]);
    assert_eq!(table(stmt).unwrap().columns[0].source_type, "b \" int");
}

#[test]
fn convert_lexer_decodes_quoted_identifiers() {
    // The convert lexer keeps `\`a b\`` as one identifier: the generated
    // enum type is named after the whole column.
    let out = mysql_to_pg("CREATE TABLE t (`a b` ENUM('x'), `c``d` ENUM('y'));");
    assert!(
        out.ends_with(r#"CREATE TABLE t ("a b" enum__t__a_b, "c""d" enum__t__c_d);"#),
        "{out}"
    );
}

// ---------------------------------------------------------------------------
// Nested parentheses in CHECK / DEFAULT
// ---------------------------------------------------------------------------

#[test]
fn nested_parentheses_are_atomic_groups() {
    assert_eq!(
        split_table_body("a int CHECK (a > 0 AND (b < 1)), b text DEFAULT (('x')), c int"),
        vec![
            "a int CHECK (a > 0 AND (b < 1))",
            "b text DEFAULT (('x'))",
            "c int"
        ]
    );
    let stmt = "CREATE TABLE t (a int CHECK (a > 0 AND (b < 1)), b text DEFAULT (('x')), c int);";
    let t = table(stmt).unwrap();
    assert_eq!(column_names(stmt), ["a", "b", "c"]);
    assert_eq!(t.check_constraints.len(), 1);
    assert_eq!(t.check_constraints[0].name, None);
    assert_eq!(t.check_constraints[0].expression, "a > 0 AND (b < 1)");
    assert_eq!(t.columns[1].default_sql.as_deref(), Some("(('x'))"));
    assert_eq!(t.columns[0].source_type, "int");
}

#[test]
fn parenthesis_inside_string_does_not_change_depth() {
    let stmt = "CREATE TABLE t (a text DEFAULT '(', m int);";
    assert_eq!(column_names(stmt), ["a", "m"]);
    assert_eq!(default_of(stmt, 0).as_deref(), Some("'('"));
}

// ---------------------------------------------------------------------------
// E'...' strings
// ---------------------------------------------------------------------------

#[test]
fn e_string_is_kept_raw_by_schema_lexer() {
    assert_eq!(split_table_body(r"E'a\'b\n'"), vec![r"E'a\'b\n'"]);
    let stmt = r"CREATE TABLE t (a text DEFAULT E'a\'b', m int);";
    assert_eq!(column_names(stmt), ["a", "m"]);
    assert_eq!(default_of(stmt, 0).as_deref(), Some(r"E'a\'b'"));
}

// ---------------------------------------------------------------------------
// $$ dollar-quoted bodies: schema lexer has no dollar quoting
// ---------------------------------------------------------------------------

#[test]
fn dollar_quoted_body_is_split_by_schema_lexer() {
    assert_eq!(split_table_body("$$a,b$$"), vec!["$$a", "b$$"]);
    assert_eq!(
        split_table_body("a text DEFAULT $$x, y$$, m int"),
        vec!["a text DEFAULT $$x", "y$$", "m int"]
    );
    let stmt = "CREATE TABLE t (a text DEFAULT $$x y$$, m int);";
    assert_eq!(default_of(stmt, 0).as_deref(), Some("$$x"));
    // The orphan `y$$` fragment fails the column regex and is dropped.
    assert_eq!(
        column_names("CREATE TABLE t (a text DEFAULT $$x, y$$, m int);"),
        ["a", "m"]
    );
}

// ---------------------------------------------------------------------------
// Comments: schema lexer has no comment awareness
// ---------------------------------------------------------------------------

#[test]
fn comment_text_is_tokens_for_schema_lexer() {
    assert_eq!(split_table_body("-- x\nb"), vec!["-- x\nb"]);
    assert_eq!(
        default_of("CREATE TABLE t (a int DEFAULT -- c\n 1, m int);", 0).as_deref(),
        Some("--")
    );
    assert_eq!(
        default_of("CREATE TABLE t (a int DEFAULT /* c */ 1, m int);", 0).as_deref(),
        Some("/*")
    );
}

#[test]
fn comment_containing_paren_breaks_schema_body_extraction() {
    // `(` inside a `--` comment opens a paren group that never closes.
    assert_eq!(
        split_table_body("a int, -- trailing (x,\n b int"),
        vec!["a int", "-- trailing (x,\n b int"]
    );
    assert!(table("CREATE TABLE t (a int, -- trailing (x,\n b int);").is_none());

    // The convert lexer skips the comment and still sees both columns.
    let out = mysql_to_pg("CREATE TABLE t (a ENUM('p'), -- trailing (x,\n b ENUM('q'));");
    assert!(
        out.ends_with("CREATE TABLE t (a enum__t__a, -- trailing (x,\n b enum__t__b);"),
        "{out}"
    );
}

// ---------------------------------------------------------------------------
// Unterminated strings
// ---------------------------------------------------------------------------

#[test]
fn unterminated_string_swallows_the_rest() {
    assert_eq!(split_table_body("'abc def, x"), vec!["'abc def, x"]);
    assert_eq!(split_table_body("(a b, c"), vec!["(a b, c"]);
    assert!(table("CREATE TABLE t (a text DEFAULT 'oops, m int);").is_none());
}
