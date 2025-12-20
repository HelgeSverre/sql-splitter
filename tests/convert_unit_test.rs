//! Unit tests for convert module, extracted from src/convert/mod.rs
//!
//! Tests that used private methods have been rewritten to test through the
//! public `convert_statement` interface.

use sql_splitter::convert::Converter;
use sql_splitter::parser::SqlDialect;

#[test]
fn test_backticks_to_double_quotes() {
    let converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    assert_eq!(converter.backticks_to_double_quotes("`users`"), "\"users\"");
    assert_eq!(
        converter.backticks_to_double_quotes("`table_name`"),
        "\"table_name\""
    );
    // Preserve strings
    assert_eq!(
        converter.backticks_to_double_quotes("'hello `world`'"),
        "'hello `world`'"
    );
}

#[test]
fn test_double_quotes_to_backticks() {
    let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    assert_eq!(converter.double_quotes_to_backticks("\"users\""), "`users`");
}

#[test]
fn test_mysql_escapes_to_standard() {
    // Test through convert_statement on INSERT
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"INSERT INTO t VALUES ('it\\'s');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("''"));
    assert!(!output_str.contains("\\'"));
}

#[test]
fn test_auto_increment_to_serial() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("SERIAL"));
    assert!(!output_str.contains("AUTO_INCREMENT"));
}

#[test]
fn test_strip_engine_clause() {
    // Test through convert_statement on CREATE TABLE
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT) ENGINE=InnoDB;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("ENGINE"));
    assert!(output_str.contains("CREATE TABLE"));
}

#[test]
fn test_strip_conditional_comments() {
    // Test through convert_statement
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"/*!40101 SET NAMES utf8 */;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    // The conditional comment content should be stripped
    assert!(!output_str.contains("40101"));
}

#[test]
fn test_skip_mysql_session_commands() {
    // Test through convert_statement - these should return empty
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"SET NAMES utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"LOCK TABLES users WRITE;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should not be empty
    let input = b"CREATE TABLE users (id INT);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_skip_postgres_session_commands() {
    // Test through convert_statement - these should return empty
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"SET client_encoding = 'UTF8';";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"SET search_path TO public;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should not be empty
    let input = b"CREATE TABLE users (id INT);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_skip_sqlite_pragmas() {
    // Test through convert_statement - these should return empty
    let mut converter = Converter::new(SqlDialect::Sqlite, SqlDialect::MySql);

    let input = b"PRAGMA foreign_keys = ON;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"PRAGMA journal_mode = WAL;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should not be empty
    let input = b"CREATE TABLE users (id INTEGER);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_serial_to_auto_increment() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"CREATE TABLE users (id SERIAL PRIMARY KEY);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("AUTO_INCREMENT"));
    assert!(!output_str.contains("SERIAL"));
}

#[test]
fn test_postgres_to_sqlite_types() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite);

    let input = b"CREATE TABLE t (id SERIAL, data BYTEA, flag BOOLEAN);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("INTEGER"));
    assert!(output_str.contains("BLOB"));
    assert!(!output_str.contains("BYTEA"));
    assert!(!output_str.contains("SERIAL"));
}

#[test]
fn test_sqlite_to_postgres_types() {
    let mut converter = Converter::new(SqlDialect::Sqlite, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INTEGER, val REAL, data BLOB);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("DOUBLE PRECISION"));
    assert!(output_str.contains("BYTEA"));
    assert!(!output_str.contains("REAL"));
    assert!(!output_str.contains("BLOB"));
}

#[test]
fn test_sqlite_to_mysql_types() {
    let mut converter = Converter::new(SqlDialect::Sqlite, SqlDialect::MySql);

    let input = b"CREATE TABLE t (id INTEGER, val REAL);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("INTEGER"));
    assert!(output_str.contains("DOUBLE"));
    assert!(!output_str.contains("REAL"));
}

#[test]
fn test_postgres_identifier_quoting_to_mysql() {
    let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = "\"users\"";
    let output = converter.double_quotes_to_backticks(input);

    assert_eq!(output, "`users`");
}

#[test]
fn test_preserve_strings_in_identifier_conversion() {
    let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = "SELECT 'hello \"world\"' FROM \"users\"";
    let output = converter.double_quotes_to_backticks(input);

    assert!(output.contains("'hello \"world\"'"));
    assert!(output.contains("`users`"));
}

#[test]
fn test_postgres_only_feature_detection() {
    // Test through convert_statement - these should return empty when converting from Postgres
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    // PostgreSQL-only features should be skipped
    let input = b"CREATE SEQUENCE my_seq;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"CREATE DOMAIN my_domain AS INTEGER;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"CREATE TYPE my_enum AS ENUM ('a', 'b');";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"CREATE TRIGGER my_trigger AFTER INSERT ON foo;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"COMMENT ON TABLE foo IS 'bar';";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should NOT be empty
    let input = b"CREATE TABLE users (id INT);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_strip_postgres_casts() {
    // Test through convert_statement
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"INSERT INTO t VALUES ('val'::text);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("::text"));
}

#[test]
fn test_convert_nextval() {
    // Test through convert_statement on ALTER TABLE
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"ALTER TABLE t ALTER COLUMN id SET DEFAULT nextval('t_id_seq'::regclass);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("nextval"));
    assert!(!output_str.contains("t_id_seq"));
}

#[test]
fn test_convert_default_now() {
    // Test through convert_statement on CREATE TABLE
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"CREATE TABLE t (created_at TIMESTAMP DEFAULT now());";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("CURRENT_TIMESTAMP"));
    assert!(!output_str.contains("now()"));
}

#[test]
fn test_strip_schema_prefix() {
    // Test through convert_statement
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"INSERT INTO public.users VALUES (1);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("public."));
    assert!(output_str.contains("users"));
}

// =============================================================================
// Warning tests (extracted from src/convert/warnings.rs)
// =============================================================================

use sql_splitter::convert::{ConvertWarning, WarningCollector};

#[test]
fn test_warning_display() {
    let w = ConvertWarning::UnsupportedFeature {
        feature: "ENUM type".to_string(),
        suggestion: Some("Use VARCHAR with CHECK constraint".to_string()),
    };
    let s = w.to_string();
    assert!(s.contains("ENUM type"));
    assert!(s.contains("CHECK constraint"));
}

#[test]
fn test_warning_collector_dedup() {
    let mut collector = WarningCollector::new();

    collector.add(ConvertWarning::UnsupportedFeature {
        feature: "ENUM".to_string(),
        suggestion: None,
    });
    collector.add(ConvertWarning::UnsupportedFeature {
        feature: "ENUM".to_string(),
        suggestion: None,
    });

    assert_eq!(collector.count(), 1);
}

#[test]
fn test_warning_collector_limit() {
    let mut collector = WarningCollector::with_limit(5);

    for i in 0..10 {
        collector.add(ConvertWarning::UnsupportedFeature {
            feature: format!("Feature {}", i),
            suggestion: None,
        });
    }

    assert_eq!(collector.count(), 5);
}

// =============================================================================
// COPY → INSERT conversion tests (from src/convert/copy_to_insert.rs)
// =============================================================================

mod copy_to_insert_tests {
    use sql_splitter::convert::{
        copy_to_inserts, parse_copy_data, parse_copy_header, CopyHeader, CopyValue,
    };
    use sql_splitter::parser::SqlDialect;

    #[test]
    fn test_parse_copy_header_simple() {
        let header = "COPY users (id, name, email) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.table, "users");
        assert_eq!(parsed.columns, vec!["id", "name", "email"]);
        assert!(parsed.schema.is_none());
    }

    #[test]
    fn test_parse_copy_header_with_schema() {
        let header = "COPY public.users (id, name) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.schema, Some("public".to_string()));
        assert_eq!(parsed.table, "users");
    }

    #[test]
    fn test_parse_copy_header_quoted() {
        let header = r#"COPY "public"."my_table" ("id", "name") FROM stdin;"#;
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.schema, Some("public".to_string()));
        assert_eq!(parsed.table, "my_table");
    }

    #[test]
    fn test_parse_copy_header_with_comments() {
        let header = "--\n-- Data for table\n--\nCOPY users (id) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.table, "users");
    }

    #[test]
    fn test_parse_copy_data() {
        let data = b"1\tAlice\talice@example.com\n2\tBob\tbob@example.com\n\\.";
        let rows = parse_copy_data(data);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 3);
    }

    #[test]
    fn test_null_handling() {
        let data = b"1\t\\N\ttest\n";
        let rows = parse_copy_data(data);
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][1], CopyValue::Null));
    }

    #[test]
    fn test_escape_sequences() {
        let data = b"hello\\tworld\\n\n";
        let rows = parse_copy_data(data);
        if let CopyValue::Text(s) = &rows[0][0] {
            assert_eq!(s, "hello\tworld\n");
        } else {
            panic!("Expected Text");
        }
    }

    #[test]
    fn test_copy_to_insert_mysql() {
        let header = CopyHeader {
            schema: None,
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n2\tBob\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        assert_eq!(inserts.len(), 1);

        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("INSERT INTO `users`"));
        assert!(sql.contains("(`id`, `name`)"));
        assert!(sql.contains("('1', 'Alice')"));
        assert!(sql.contains("('2', 'Bob')"));
    }

    #[test]
    fn test_copy_to_insert_postgres() {
        let header = CopyHeader {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Postgres);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("\"public\".\"users\""));
    }

    #[test]
    fn test_copy_to_insert_with_null() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
        };
        let data = b"1\t\\N\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("NULL"));
    }

    #[test]
    fn test_escape_quotes_mysql() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["s".to_string()],
        };
        let data = b"it's a test\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("it\\'s a test"));
    }

    #[test]
    fn test_escape_quotes_sqlite() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["s".to_string()],
        };
        let data = b"it's a test\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Sqlite);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("it''s a test"));
    }
}
