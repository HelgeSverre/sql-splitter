//! Tests for the shared `Schema::from_sql_file` / `SchemaBuilder::ingest` helpers.

use sql_splitter::parser::{SqlDialect, StatementType};
use sql_splitter::schema::{Schema, SchemaBuilder};
use sql_splitter::synthetic::schema::PortableSchema;
use std::io::Write;

fn write_temp_sql(content: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new()
        .suffix(".sql")
        .tempfile()
        .expect("create temp file");
    f.write_all(content.as_bytes()).expect("write temp file");
    f.flush().expect("flush temp file");
    f
}

#[test]
fn from_sql_file_parses_create_alter_and_index() {
    let sql = r#"
CREATE TABLE users (
  id INT NOT NULL,
  email VARCHAR(255),
  PRIMARY KEY (id)
);

CREATE TABLE orders (
  id INT NOT NULL,
  user_id INT,
  PRIMARY KEY (id)
);

ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id);

CREATE INDEX idx_users_email ON users (email);

INSERT INTO users VALUES (1, 'a@example.com');
"#;
    let f = write_temp_sql(sql);

    let schema = Schema::from_sql_file(f.path(), SqlDialect::MySql, None).expect("build schema");

    assert_eq!(schema.len(), 2);

    let users = schema.get_table("users").expect("users table");
    assert_eq!(users.columns.len(), 2);
    assert_eq!(users.primary_key.len(), 1);
    assert_eq!(users.indexes.len(), 1);
    assert_eq!(users.indexes[0].name, "idx_users_email");

    let orders = schema.get_table("orders").expect("orders table");
    assert_eq!(orders.foreign_keys.len(), 1);
    assert_eq!(orders.foreign_keys[0].referenced_table, "users");
    // FK references must be resolved by build()
    assert!(orders.foreign_keys[0].referenced_table_id.is_some());
}

#[test]
fn from_sql_file_reports_progress() {
    let sql = "CREATE TABLE t (id INT PRIMARY KEY);\n";
    let f = write_temp_sql(sql);

    let seen = std::rc::Rc::new(std::cell::Cell::new(0u64));
    let seen_cb = std::rc::Rc::clone(&seen);
    let schema = Schema::from_sql_file(
        f.path(),
        SqlDialect::MySql,
        Some(Box::new(move |bytes| seen_cb.set(bytes))),
    )
    .expect("build schema");

    assert_eq!(schema.len(), 1);
    assert_eq!(seen.get(), sql.len() as u64);
}

#[test]
fn ingest_dispatches_only_ddl() {
    let mut builder = SchemaBuilder::new();
    builder.ingest(
        StatementType::CreateTable,
        "CREATE TABLE t (id INT PRIMARY KEY);",
    );
    builder.ingest(StatementType::Insert, "INSERT INTO other VALUES (1);");
    builder.ingest(StatementType::Unknown, "SET NAMES utf8mb4;");

    let schema = builder.build();
    assert_eq!(schema.len(), 1);
    assert!(schema.get_table("t").is_some());
}

#[test]
fn portable_schema_keeps_order_and_raw_ddl() {
    let sql = r#"
CREATE TABLE users (
  id INT NOT NULL,
  email VARCHAR(255),
  PRIMARY KEY (id)
);
"#;
    let f = write_temp_sql(sql);

    let schema = Schema::from_sql_file(f.path(), SqlDialect::MySql, None).unwrap();
    let portable = PortableSchema::from_runtime(&schema, SqlDialect::MySql);
    let users = portable.tables.get("users").unwrap();
    assert_eq!(
        users
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "email"]
    );
    assert!(users
        .create_statement
        .as_deref()
        .unwrap()
        .contains("CREATE TABLE"));
}

#[test]
fn ingest_statement_classifies_raw_bytes() {
    let mut builder = SchemaBuilder::new();
    builder.ingest_statement(b"CREATE TABLE t (id INT PRIMARY KEY);", SqlDialect::MySql);
    builder.ingest_statement(b"INSERT INTO t VALUES (1);", SqlDialect::MySql);

    let schema = builder.build();
    assert_eq!(schema.len(), 1);
    let t = schema.get_table("t").expect("t");
    assert_eq!(t.primary_key.len(), 1);
}
