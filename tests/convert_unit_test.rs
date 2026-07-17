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
fn test_strip_mysql_table_auto_increment_option() {
    // Table-level AUTO_INCREMENT=N option (distinct from column-level AUTO_INCREMENT
    // keyword) must be fully removed, not just have "AUTO_INCREMENT" stripped and
    // "=2" left dangling. See https://github.com/HelgeSverre/sql-splitter/issues/64
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("AUTO_INCREMENT"));
    assert!(!output_str.contains("=2"));
    assert!(output_str.trim_end().ends_with(");"));
}

#[test]
fn test_strip_mysql_table_comment_option() {
    // Trailing table-level COMMENT='...' option is not valid PostgreSQL syntax
    // and must be stripped entirely.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT) COMMENT='some comment';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"));
    assert!(!output_str.contains("some comment"));
    assert!(output_str.trim_end().ends_with(");"));
}

#[test]
fn test_strip_mysql_inline_column_comment() {
    // Inline column COMMENT 'text' is MySQL-only syntax; PostgreSQL rejects it
    // inside a column definition.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT COMMENT 'the id', name VARCHAR(20) COMMENT 'name field');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"));
    assert!(!output_str.contains("the id"));
    assert!(!output_str.contains("name field"));
}

#[test]
fn test_convert_unique_key_using_btree() {
    // MySQL's `UNIQUE KEY name (col) USING BTREE` table constraint has no direct
    // PostgreSQL equivalent inline; it must become a plain `UNIQUE (col)`.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, username VARCHAR(20), UNIQUE KEY `username` (`username`) USING BTREE);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"username\")"));
    assert!(!output_str.contains("USING BTREE"));
    assert!(!output_str.contains("UNIQUE KEY"));
}

#[test]
fn test_convert_issue_64_full_reproduction() {
    // Full reproduction of https://github.com/HelgeSverre/sql-splitter/issues/64:
    // converting a realistic MySQL CREATE TABLE (inline column comments, a unique
    // key with USING BTREE, and ENGINE/AUTO_INCREMENT/CHARSET/COMMENT table
    // options) must produce syntactically clean PostgreSQL output.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE `fa_admin` (\n  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID',\n  `username` varchar(20) DEFAULT '' COMMENT 'username',\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `username` (`username`) USING BTREE\n) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='admin table';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("ENGINE"), "{output_str}");
    assert!(!output_str.contains("USING BTREE"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
    assert!(output_str.contains("UNIQUE (\"username\")"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

// --- Permutations of the AUTO_INCREMENT=N table option ---

#[test]
fn test_auto_increment_table_option_alone() {
    // No ENGINE/CHARSET around it at all.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=5;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
    assert!(!output_str.contains("=5"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_auto_increment_table_option_before_engine() {
    // Unusual but valid MySQL ordering: AUTO_INCREMENT=N before ENGINE=.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=100 ENGINE=InnoDB;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
    assert!(!output_str.contains("=100"), "{output_str}");
    assert!(!output_str.contains("ENGINE"), "{output_str}");
}

#[test]
fn test_auto_increment_table_option_lowercase() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT auto_increment PRIMARY KEY) engine=InnoDB auto_increment=2;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        !output_str.to_uppercase().contains("AUTO_INCREMENT"),
        "{output_str}"
    );
    assert!(!output_str.contains("=2"), "{output_str}");
}

#[test]
fn test_auto_increment_table_option_multi_digit() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AUTO_INCREMENT=1234567;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("1234567"), "{output_str}");
    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
}

#[test]
fn test_no_table_auto_increment_option_still_converts_column_keyword() {
    // Regression: tables with only the column-level keyword (no table option)
    // must still get SERIAL treatment.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("SERIAL"), "{output_str}");
    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
}

// --- Permutations of COMMENT clauses ---

#[test]
fn test_column_comment_with_escaped_quote() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT COMMENT 'it\\'s the id');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("it's the id"), "{output_str}");
}

#[test]
fn test_last_column_comment_before_closing_paren() {
    // No trailing comma after the COMMENT — closing paren follows directly.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT, name VARCHAR(20) COMMENT 'the name');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_table_comment_without_equals_sign() {
    // MySQL allows the table-level COMMENT option without `=`.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT) COMMENT 'my table';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("my table"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_table_comment_between_engine_and_charset() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT) ENGINE=InnoDB COMMENT='mid table' DEFAULT CHARSET=utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("ENGINE"), "{output_str}");
    assert!(!output_str.contains("CHARSET"), "{output_str}");
}

#[test]
fn test_comment_containing_parentheses() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT COMMENT 'contains (parens) here');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("contains"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_mixed_columns_with_and_without_comments() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT COMMENT 'the id', name VARCHAR(20), age INT COMMENT 'the age');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert_eq!(
        output_str.trim(),
        "CREATE TABLE t (id INT, name VARCHAR(20), age INT);"
    );
}

#[test]
fn test_standalone_comment_on_table_statement_untouched() {
    // Regression: a standalone `COMMENT ON TABLE ... IS '...'` statement is a
    // different statement type entirely and must not be touched by the
    // CREATE TABLE inline-comment stripping.
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Postgres);

    let input = b"COMMENT ON TABLE foo IS 'bar';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("COMMENT ON TABLE"), "{output_str}");
    assert!(output_str.contains("'bar'"), "{output_str}");
}

// --- Permutations of UNIQUE KEY / USING BTREE constraints ---

#[test]
fn test_unique_key_without_name() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT, email VARCHAR(50), UNIQUE KEY (`email`));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
}

#[test]
fn test_unique_key_without_using_clause() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT, email VARCHAR(50), UNIQUE KEY `email` (`email`));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
}

#[test]
fn test_unique_key_using_hash() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), UNIQUE KEY `email` (`email`) USING HASH);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("USING HASH"), "{output_str}");
}

#[test]
fn test_unique_key_using_btree_lowercase() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), unique key `email` (`email`) using btree);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.to_uppercase().contains("UNIQUE (\"EMAIL\")"),
        "{output_str}"
    );
    assert!(
        !output_str.to_uppercase().contains("USING BTREE"),
        "{output_str}"
    );
}

#[test]
fn test_unique_key_multi_column() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, a INT, b INT, UNIQUE KEY `idx_ab` (`a`,`b`) USING BTREE);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"a\",\"b\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
    assert!(!output_str.contains("USING BTREE"), "{output_str}");
}

#[test]
fn test_primary_key_untouched_by_unique_key_conversion() {
    // Regression: PRIMARY KEY must not be affected by the UNIQUE KEY regex.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), PRIMARY KEY (`id`), UNIQUE KEY `email` (`email`) USING BTREE);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("PRIMARY KEY (\"id\")"), "{output_str}");
    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
}

#[test]
fn test_named_unique_constraint_untouched() {
    // Regression: a standard `CONSTRAINT name UNIQUE (...)` (not MySQL's
    // `UNIQUE KEY` form) must pass through unchanged.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), CONSTRAINT uq_email UNIQUE (`email`));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("CONSTRAINT uq_email UNIQUE (\"email\")"),
        "{output_str}"
    );
}

// --- Adversarial findings: string-literal-aware stripping ---

#[test]
fn test_default_value_literally_comment_does_not_eat_next_column() {
    // The word "comment" appearing as a DEFAULT string value must not be
    // mistaken for a MySQL COMMENT clause and must not consume subsequent
    // column definitions.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (a VARCHAR(20) DEFAULT 'comment', b VARCHAR(20) DEFAULT 'x');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert_eq!(
        output_str.trim(),
        "CREATE TABLE t (a VARCHAR(20) DEFAULT 'comment', b VARCHAR(20) DEFAULT 'x');"
    );
}

#[test]
fn test_check_constraint_literal_comment_not_corrupted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (col VARCHAR(10) CHECK (col <> 'COMMENT'), name VARCHAR(20) DEFAULT 'bob');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("CHECK (col <> 'COMMENT')"),
        "{output_str}"
    );
    assert!(
        output_str.contains("name VARCHAR(20) DEFAULT 'bob'"),
        "{output_str}"
    );
}

#[test]
fn test_escaped_quote_pair_in_default_value_not_corrupted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (note VARCHAR(255) DEFAULT 'See comment ''123'' here', x INT);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("DEFAULT 'See comment ''123'' here'"),
        "{output_str}"
    );
    assert!(output_str.contains(", x INT"), "{output_str}");
}

#[test]
fn test_default_value_literally_auto_increment_not_corrupted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (label VARCHAR(20) DEFAULT 'AUTO_INCREMENT=5', id INT);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("DEFAULT 'AUTO_INCREMENT=5'"),
        "{output_str}"
    );
}

// --- Adversarial findings: UNIQUE KEY with prefix-length index ---

#[test]
fn test_unique_key_with_prefix_length() {
    // MySQL utf8mb4 prefix-length indexes, e.g. `(email(191))`, are extremely
    // common in real dumps and must not be left as invalid Postgres syntax.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (email VARCHAR(255), UNIQUE KEY `email` (`email`(191)));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
    assert!(!output_str.contains("(191)"), "{output_str}");
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
        // Note: public schema is stripped for DuckDB compatibility
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"name\""));
    }

    #[test]
    fn test_copy_to_insert_postgres_custom_schema() {
        // Non-standard schemas are preserved
        let header = CopyHeader {
            schema: Some("myschema".to_string()),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
        };
        let data = b"1\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Postgres);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("\"myschema\".\"users\""));
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

#[test]
fn test_mysql_sized_int_auto_increment_to_sqlite() {
    // Regression: "BIGINT AUTO_INCREMENT" used to hit the "INT AUTO_INCREMENT"
    // substring replacement and produce invalid "BIGINTEGER".
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Sqlite);

    let input =
        b"CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, n SMALLINT AUTO_INCREMENT);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("BIGINTEGER"), "got: {output_str}");
    assert!(!output_str.contains("SMALLINTEGER"), "got: {output_str}");
    assert!(
        output_str.contains("INTEGER PRIMARY KEY"),
        "got: {output_str}"
    );
    assert!(!output_str.contains("AUTO_INCREMENT"), "got: {output_str}");
}

#[test]
fn test_enum_narrows_to_varchar_cross_dialect() {
    // Task 30: the synthetic renderer flags ENUM/SET narrowing as lossy; the
    // convert command's type mapping is the shared rule and must still narrow
    // ENUM to a plain VARCHAR (regression guard, behavior unchanged).
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let input = b"CREATE TABLE t (kind ENUM('a','b') NOT NULL);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);
    assert!(output_str.contains("VARCHAR(255)"), "got: {output_str}");
    assert!(
        !output_str.to_uppercase().contains("ENUM("),
        "got: {output_str}"
    );
}
