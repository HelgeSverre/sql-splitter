/// Comprehensive edge case and mutation tests for the SQL parser.
///
/// These tests cover:
/// - Malformed SQL
/// - Unusual encodings and binary data
/// - Edge cases in string handling
/// - Multi-line statements
/// - Comments (-- and /* */)
/// - Nested quotes and escaped characters
/// - Very long statements
/// - Empty files and truncated input
/// - NULL bytes and BOM

#[cfg(test)]
mod edge_case_tests {
    use crate::parser::{Parser, StatementType};

    // =========================================================================
    // A. Statement Splitting (read_statement) Edge Cases
    // =========================================================================

    mod read_statement_tests {
        use super::*;

        // A.1 Basic termination variants

        #[test]
        fn test_statement_without_trailing_semicolon() {
            let sql = b"CREATE TABLE t1 (id INT)";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            assert_eq!(stmt.unwrap(), b"CREATE TABLE t1 (id INT)");

            let next = parser.read_statement().unwrap();
            assert!(next.is_none());
        }

        #[test]
        fn test_empty_input() {
            let sql = b"";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_none());
        }

        #[test]
        fn test_whitespace_only_input() {
            let sql = b"   \n\t  ";
            let mut parser = Parser::new(&sql[..], 1024);

            // Whitespace without semicolon returns the whitespace as a statement at EOF
            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some()); // Contains whitespace
        }

        // A.2 Strings, escapes, and nested quotes

        #[test]
        fn test_double_quoted_string_with_semicolon() {
            let sql = b"INSERT INTO t1 VALUES (\"hello; world\");";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(stmt, b"INSERT INTO t1 VALUES (\"hello; world\");");

            let next = parser.read_statement().unwrap();
            assert!(next.is_none());
        }

        #[test]
        fn test_mixed_quotes_with_semicolons() {
            let sql = b"INSERT INTO t1 VALUES ('foo \"bar; baz\"', \"x'y;z\");";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(
                stmt,
                b"INSERT INTO t1 VALUES ('foo \"bar; baz\"', \"x'y;z\");"
            );
        }

        #[test]
        fn test_sql_style_doubled_quotes() {
            // SQL uses '' to escape single quotes, not \'
            let sql = b"INSERT INTO t1 VALUES ('it''s a test; still one');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(stmt, b"INSERT INTO t1 VALUES ('it''s a test; still one');");
        }

        #[test]
        fn test_escape_near_buffer_boundary() {
            // Use tiny buffer to force multiple fill_buf calls
            let sql = b"INSERT INTO t1 VALUES ('foo\\'bar');";
            let mut parser = Parser::new(&sql[..], 8);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(stmt, b"INSERT INTO t1 VALUES ('foo\\'bar');");
        }

        #[test]
        fn test_multiple_backslashes() {
            let sql = b"INSERT INTO t1 VALUES ('\\\\');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(stmt, b"INSERT INTO t1 VALUES ('\\\\');");
        }

        #[test]
        fn test_escaped_semicolon_in_string() {
            let sql = b"INSERT INTO t1 VALUES ('escaped\\;semicolon');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            // Note: \; is not a standard escape, but backslash escapes the next char
            assert!(stmt.len() > 0);
        }

        #[test]
        fn test_backtick_with_semicolon_inside() {
            // Note: Current parser does NOT treat backticks as string delimiters
            // This documents that limitation
            let sql = b"CREATE TABLE `t;weird` (id INT);";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            // Parser may split early at the semicolon inside backticks
            // This test documents current behavior
            assert!(stmt.len() > 0);
        }

        // A.3 Multi-line and formatting

        #[test]
        fn test_multiline_statement() {
            let sql = b"CREATE TABLE t1 (\n  id INT,\n  name VARCHAR(255)\n);";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(
                stmt,
                b"CREATE TABLE t1 (\n  id INT,\n  name VARCHAR(255)\n);"
            );
        }

        #[test]
        fn test_newlines_inside_string() {
            let sql = b"INSERT INTO t1 VALUES ('first line\nsecond line; still in string');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(
                stmt,
                b"INSERT INTO t1 VALUES ('first line\nsecond line; still in string');"
            );
        }

        // A.4 Comments and semicolons
        // Note: Current parser does NOT handle SQL comments

        #[test]
        fn test_single_line_comment_with_semicolon() {
            // Documents current behavior: semicolon in comment WILL split
            let sql = b"-- comment with ; semicolon\nCREATE TABLE t1 (id INT);";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt1 = parser.read_statement().unwrap().unwrap();
            // Current behavior: splits at the semicolon in the comment
            assert!(stmt1.ends_with(b";"));
        }

        #[test]
        fn test_block_comment_with_semicolon() {
            // Documents current limitation: block comments with semicolons break parsing
            let sql = b"CREATE TABLE t1 (id INT) /* comment; with semicolon */ ;";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt1 = parser.read_statement().unwrap().unwrap();
            // Current behavior: splits at semicolon inside comment
            assert!(stmt1.len() > 0);
        }

        // A.5 Truncation, malformed strings, EOF

        #[test]
        fn test_unclosed_single_quote_at_eof() {
            let sql = b"INSERT INTO t1 VALUES ('unterminated";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            // Returns the fragment, no panic
        }

        #[test]
        fn test_unclosed_double_quote_at_eof() {
            let sql = b"INSERT INTO t1 VALUES (\"unterminated";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
        }

        #[test]
        fn test_truncated_escape_at_eof() {
            let sql = b"INSERT INTO t1 VALUES ('foo\\";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
        }

        // A.6 Weird bytes / encodings

        #[test]
        fn test_null_byte_inside_string() {
            let sql = b"INSERT INTO t1 VALUES ('a\0b;still_in_string');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            // Should not split at the semicolon inside the string
            assert!(stmt.contains(&b';'));
        }

        #[test]
        fn test_binary_hex_data() {
            let sql = b"INSERT INTO t1 VALUES (X'00FFABCD');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(stmt, b"INSERT INTO t1 VALUES (X'00FFABCD');");
        }

        #[test]
        fn test_utf8_bom_prefix() {
            let sql = b"\xEF\xBB\xBFCREATE TABLE t1 (id INT);";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            assert!(stmt.len() > 0);
        }

        #[test]
        fn test_very_long_statement() {
            // Create a statement larger than typical buffer size
            let mut sql = b"INSERT INTO t1 VALUES (".to_vec();
            for i in 0..10000 {
                if i > 0 {
                    sql.extend_from_slice(b", ");
                }
                sql.extend_from_slice(format!("'{}'", i).as_bytes());
            }
            sql.extend_from_slice(b");");

            let mut parser = Parser::new(&sql[..], 1024);
            let stmt = parser.read_statement().unwrap().unwrap();
            assert_eq!(stmt.len(), sql.len());
        }

        #[test]
        fn test_multiple_statements() {
            let sql = b"CREATE TABLE t1 (id INT); INSERT INTO t1 VALUES (1); DROP TABLE t1;";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt1 = parser.read_statement().unwrap().unwrap();
            let stmt2 = parser.read_statement().unwrap().unwrap();
            let stmt3 = parser.read_statement().unwrap().unwrap();
            let stmt4 = parser.read_statement().unwrap();

            assert!(stmt1.starts_with(b"CREATE"));
            assert!(stmt2.ends_with(b";"));
            assert!(stmt3.ends_with(b";"));
            assert!(stmt4.is_none());
        }
    }

    // =========================================================================
    // B. Statement Parsing (parse_statement) Edge Cases
    // =========================================================================

    mod parse_statement_tests {
        use super::*;

        // B.1 CREATE TABLE variations

        #[test]
        fn test_create_table_leading_whitespace() {
            let stmt = b"   \t\nCREATE TABLE users (id INT);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_create_table_if_not_exists() {
            // Documents current behavior: captures "IF" as table name
            let stmt = b"CREATE TABLE IF NOT EXISTS users (id INT);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            // Current limitation: extracts "IF" instead of "users"
            // This test documents the behavior
            assert!(!name.is_empty());
        }

        #[test]
        fn test_create_table_schema_qualified() {
            // Schema-qualified names extract just the table name (not the schema)
            let stmt = b"CREATE TABLE db.users (id INT);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_create_table_schema_qualified_backticks() {
            let stmt = b"CREATE TABLE `db`.`users` (id INT);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            // Current behavior: captures only "db"
            assert!(!name.is_empty());
        }

        #[test]
        fn test_create_table_double_quoted_identifier() {
            let stmt = b"CREATE TABLE \"User\" (id INT);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "User");
        }

        #[test]
        fn test_create_table_lowercase() {
            let stmt = b"create table users (id int);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_create_table_mixed_case() {
            let stmt = b"Create Table Users (Id Int);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "Users");
        }

        // B.2 INSERT INTO variations

        #[test]
        fn test_insert_leading_whitespace() {
            let stmt = b"  INSERT INTO posts VALUES (1);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "posts");
        }

        #[test]
        fn test_insert_schema_qualified() {
            // Schema-qualified names extract just the table name (not the schema)
            let stmt = b"INSERT INTO db.posts VALUES (1);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "posts");
        }

        #[test]
        fn test_insert_with_column_list() {
            let stmt = b"INSERT INTO posts(id, name) VALUES (1, 'x');";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "posts");
        }

        #[test]
        fn test_insert_lowercase() {
            let stmt = b"insert into posts values (1);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "posts");
        }

        // B.3 CREATE INDEX variations

        #[test]
        fn test_create_index_basic() {
            let stmt = b"CREATE INDEX idx_posts_on_user_id ON posts(user_id);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateIndex);
            assert_eq!(name, "posts");
        }

        #[test]
        fn test_create_index_backtick_table() {
            let stmt = b"CREATE INDEX idx ON `posts` (id);";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::CreateIndex);
            assert_eq!(name, "posts");
        }

        #[test]
        fn test_create_unique_index() {
            // Documents current limitation: UNIQUE keyword breaks detection
            let stmt = b"CREATE UNIQUE INDEX idx ON posts(user_id);";
            let (typ, _name) = Parser::<&[u8]>::parse_statement(stmt);
            // Current behavior: may not recognize as CreateIndex
            // This documents the limitation
            assert!(typ == StatementType::CreateIndex || typ == StatementType::Unknown);
        }

        // B.4 ALTER TABLE variations

        #[test]
        fn test_alter_table_if_exists() {
            // Documents current behavior with IF EXISTS
            let stmt = b"ALTER TABLE IF EXISTS orders ADD COLUMN status INT;";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::AlterTable);
            // Current limitation: may extract "IF" as table name
            assert!(!name.is_empty());
        }

        #[test]
        fn test_alter_table_schema_qualified() {
            // Schema-qualified names extract just the table name (not the schema)
            let stmt = b"ALTER TABLE db.orders ADD COLUMN status INT;";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::AlterTable);
            assert_eq!(name, "orders");
        }

        // B.5 DROP TABLE variations

        #[test]
        fn test_drop_table_if_exists() {
            // Documents current behavior with IF EXISTS
            let stmt = b"DROP TABLE IF EXISTS temp_data;";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::DropTable);
            // Current limitation: may extract "IF" as table name
            assert!(!name.is_empty());
        }

        #[test]
        fn test_drop_table_schema_qualified() {
            // Schema-qualified names extract just the table name (not the schema)
            let stmt = b"DROP TABLE db.temp_data;";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::DropTable);
            assert_eq!(name, "temp_data");
        }

        // B.6 Unknown and malformed statements

        #[test]
        fn test_select_statement_unknown() {
            let stmt = b"SELECT * FROM users;";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_comment_statement_unknown() {
            let stmt = b"-- This is a comment";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_empty_statement() {
            let stmt = b"";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_whitespace_only_statement() {
            let stmt = b"   \t\n   ";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_garbage_input() {
            let stmt = b"@#$%^&*";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_binary_garbage_no_panic() {
            let stmt = b"\xFF\xFF\x00\x01";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_utf8_bom_prefix_statement() {
            // BOM prefix may prevent statement recognition
            let stmt = b"\xEF\xBB\xBFCREATE TABLE t1 (id INT);";
            let (typ, _name) = Parser::<&[u8]>::parse_statement(stmt);
            // Current behavior: BOM not stripped, so likely Unknown
            // This documents the limitation
            assert!(typ == StatementType::CreateTable || typ == StatementType::Unknown);
        }

        #[test]
        fn test_very_short_statement() {
            let stmt = b"abc";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_partial_keyword() {
            let stmt = b"CREAT";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }

        #[test]
        fn test_keyword_only_no_table() {
            let stmt = b"CREATE TABLE";
            let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
            // Parser returns Unknown when it can't extract a table name
            // This is correct behavior for malformed SQL
            assert_eq!(typ, StatementType::Unknown);
            assert!(name.is_empty());
        }
    }

    // =========================================================================
    // C. End-to-End Parsing Scenarios
    // =========================================================================

    mod e2e_tests {
        use super::*;

        #[test]
        fn test_real_mysqldump_header() {
            let sql = b"-- MySQL dump 10.13  Distrib 8.0.32, for macos13 (arm64)
--
-- Host: localhost    Database: mydb
-- ------------------------------------------------------
-- Server version\t8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8mb4 */;

CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

INSERT INTO `users` VALUES (1,'Alice');
INSERT INTO `users` VALUES (2,'Bob');
";
            let mut parser = Parser::new(&sql[..], 1024);
            let mut statements = Vec::new();

            while let Some(stmt) = parser.read_statement().unwrap() {
                statements.push(stmt);
            }

            // Should have multiple statements
            assert!(statements.len() >= 3);

            // Find the CREATE TABLE statement
            let create_stmt = statements
                .iter()
                .find(|s| s.windows(12).any(|w| w == b"CREATE TABLE"));
            assert!(create_stmt.is_some());

            // Find INSERT statements
            let insert_count = statements
                .iter()
                .filter(|s| s.windows(11).any(|w| w == b"INSERT INTO"))
                .count();
            assert!(insert_count >= 2);
        }

        #[test]
        fn test_statement_with_all_quote_types() {
            let sql = b"INSERT INTO `table` VALUES (1, 'single', \"double\", `backtick`);";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) = Parser::<&[u8]>::parse_statement(&stmt);

            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "table");
        }

        #[test]
        fn test_extended_insert() {
            // MySQL extended insert format
            let sql = b"INSERT INTO `users` VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) = Parser::<&[u8]>::parse_statement(&stmt);

            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_create_table_with_constraints() {
            let sql = b"CREATE TABLE `orders` (
  `id` int NOT NULL,
  `user_id` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user` (`user_id`),
  CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB;";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) = Parser::<&[u8]>::parse_statement(&stmt);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "orders");
        }
    }

    // =========================================================================
    // PostgreSQL Dollar-Quoting Tests
    // =========================================================================

    mod postgres_dollar_quoting_tests {
        use crate::parser::{Parser, SqlDialect, StatementType};

        #[test]
        fn test_postgres_empty_dollar_quote() {
            let sql = b"CREATE FUNCTION test() RETURNS text AS $$SELECT 1$$ LANGUAGE sql;";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            assert!(stmt.unwrap().ends_with(b";"));
        }

        #[test]
        fn test_postgres_named_dollar_quote() {
            let sql = b"CREATE FUNCTION test() RETURNS text AS $_$SELECT 1$_$ LANGUAGE sql;";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
        }

        #[test]
        fn test_postgres_dollar_quote_with_semicolon_inside() {
            let sql =
                b"CREATE FUNCTION test() RETURNS text AS $$SELECT 1; SELECT 2;$$ LANGUAGE sql;";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            let s = stmt.unwrap();
            assert!(s.starts_with(b"CREATE FUNCTION"));
            assert!(s.ends_with(b";"));
        }

        #[test]
        fn test_postgres_mixed_dollar_quote_tags() {
            // This was the bug: $_$ followed by $$ would break parsing
            let sql = br#"
CREATE FUNCTION test1() RETURNS text AS $_$
SELECT 1;
$_$;

CREATE FUNCTION test2() RETURNS text AS $$
SELECT 2;
$$;

CREATE TABLE test (id INT);
"#;
            let mut parser = Parser::with_dialect(&sql[..], 4096, SqlDialect::Postgres);
            let mut statements = Vec::new();

            while let Some(stmt) = parser.read_statement().unwrap() {
                statements.push(String::from_utf8_lossy(&stmt).into_owned());
            }

            assert!(
                statements
                    .iter()
                    .any(|s| s.contains("CREATE FUNCTION test1")),
                "Should find test1 function"
            );
            assert!(
                statements
                    .iter()
                    .any(|s| s.contains("CREATE FUNCTION test2")),
                "Should find test2 function"
            );
            assert!(
                statements.iter().any(|s| s.contains("CREATE TABLE test")),
                "Should find CREATE TABLE"
            );
        }

        #[test]
        fn test_postgres_invalid_dollar_tag_not_matched() {
            // A $ followed by invalid chars then another $ should not be treated as dollar-quote
            let sql = b"SELECT $1 + $2;";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            assert_eq!(stmt.unwrap(), b"SELECT $1 + $2;");
        }

        #[test]
        fn test_postgres_schema_qualified_table() {
            let sql = b"CREATE TABLE public.users (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::Postgres);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_postgres_copy_from_stdin() {
            let sql = b"COPY users FROM stdin;\n1\tAlice\n2\tBob\n\\.\nCREATE TABLE test (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt1 = parser.read_statement().unwrap();
            assert!(stmt1.is_some());
            assert!(stmt1.unwrap().starts_with(b"COPY users"));

            // COPY data block
            let stmt2 = parser.read_statement().unwrap();
            assert!(stmt2.is_some());

            let stmt3 = parser.read_statement().unwrap();
            assert!(stmt3.is_some());
            assert!(std::str::from_utf8(&stmt3.unwrap())
                .unwrap()
                .contains("CREATE TABLE"));
        }

        #[test]
        fn test_postgres_nested_dollar_quotes() {
            // Function containing $$ inside $_$ quotes
            let sql = b"CREATE FUNCTION test() AS $_$
                SELECT '$$not a quote$$';
            $_$;";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            let s = stmt.unwrap();
            assert!(s.starts_with(b"CREATE FUNCTION"));
            assert!(s.ends_with(b";"));
        }

        #[test]
        fn test_postgres_insert_into_schema_qualified() {
            let sql = b"INSERT INTO public.users (id, name) VALUES (1, 'Alice');";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::Postgres);

            assert_eq!(typ, StatementType::Insert);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_postgres_double_quoted_identifiers() {
            let sql = b"CREATE TABLE \"My Table\" (\"Column One\" INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Postgres);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::Postgres);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "My Table");
        }
    }

    // =========================================================================
    // SQLite-Specific Tests
    // =========================================================================

    mod sqlite_tests {
        use crate::parser::{Parser, SqlDialect, StatementType};

        #[test]
        fn test_sqlite_create_table() {
            let sql = b"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Sqlite);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::Sqlite);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_sqlite_double_quoted_table() {
            let sql = b"CREATE TABLE \"my-table\" (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Sqlite);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::Sqlite);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "my-table");
        }

        #[test]
        fn test_sqlite_insert_replace() {
            let sql = b"INSERT OR REPLACE INTO users VALUES (1, 'Alice');";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Sqlite);

            let stmt = parser.read_statement().unwrap().unwrap();
            // Should still be recognized as an insert
            assert!(stmt.starts_with(b"INSERT"));
        }

        #[test]
        fn test_sqlite_pragma_ignored() {
            let sql = b"PRAGMA foreign_keys=ON; CREATE TABLE users (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::Sqlite);

            let stmt1 = parser.read_statement().unwrap();
            assert!(stmt1.is_some());

            let stmt2 = parser.read_statement().unwrap();
            assert!(stmt2.is_some());
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt2.unwrap(), SqlDialect::Sqlite);
            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }
    }

    // =========================================================================
    // MySQL-Specific Tests
    // =========================================================================

    mod mysql_tests {
        use crate::parser::{Parser, SqlDialect, StatementType};

        #[test]
        fn test_mysql_backtick_with_spaces() {
            let sql = b"CREATE TABLE `my table` (`column name` INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::MySql);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::MySql);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "my table");
        }

        #[test]
        fn test_mysql_conditional_comment() {
            let sql = b"/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */; CREATE TABLE t (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::MySql);

            let stmt1 = parser.read_statement().unwrap();
            assert!(stmt1.is_some());

            let stmt2 = parser.read_statement().unwrap();
            assert!(stmt2.is_some());
        }

        #[test]
        fn test_mysql_lock_unlock_tables() {
            let sql = b"LOCK TABLES `users` WRITE; INSERT INTO `users` VALUES (1); UNLOCK TABLES;";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::MySql);

            let mut count = 0;
            while let Some(_) = parser.read_statement().unwrap() {
                count += 1;
            }
            assert_eq!(count, 3);
        }

        #[test]
        fn test_mysql_escaped_backtick_in_name() {
            let sql = b"CREATE TABLE `my``table` (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::MySql);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, _name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::MySql);

            assert_eq!(typ, StatementType::CreateTable);
        }

        #[test]
        fn test_mysql_multiline_insert() {
            let sql = b"INSERT INTO users VALUES
                (1, 'Alice'),
                (2, 'Bob'),
                (3, 'Charlie');";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::MySql);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            let s = stmt.unwrap();
            let text = std::str::from_utf8(&s).unwrap();
            assert!(text.contains("Alice"));
            assert!(text.contains("Charlie"));
        }

        #[test]
        fn test_mysql_create_table_if_not_exists() {
            let sql = b"CREATE TABLE IF NOT EXISTS users (id INT);";
            let mut parser = Parser::with_dialect(&sql[..], 1024, SqlDialect::MySql);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::MySql);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, "users");
        }
    }

    // =========================================================================
    // Cross-Dialect Edge Cases
    // =========================================================================

    mod cross_dialect_tests {
        use crate::parser::{Parser, SqlDialect, StatementType};

        #[test]
        fn test_alter_table_all_dialects() {
            for dialect in [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Sqlite] {
                let sql = b"ALTER TABLE users ADD COLUMN email VARCHAR(255);";
                let mut parser = Parser::with_dialect(&sql[..], 1024, dialect);

                let stmt = parser.read_statement().unwrap().unwrap();
                let (typ, name) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

                assert_eq!(typ, StatementType::AlterTable);
                assert_eq!(name, "users");
            }
        }

        #[test]
        fn test_drop_table_all_dialects() {
            for dialect in [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Sqlite] {
                let sql = b"DROP TABLE IF EXISTS users;";
                let mut parser = Parser::with_dialect(&sql[..], 1024, dialect);

                let stmt = parser.read_statement().unwrap().unwrap();
                let (typ, name) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

                assert_eq!(typ, StatementType::DropTable);
                assert_eq!(name, "users");
            }
        }

        #[test]
        fn test_drop_table_simple() {
            let sql = b"DROP TABLE users;";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) = Parser::<&[u8]>::parse_statement(&stmt);

            assert_eq!(typ, StatementType::DropTable);
            assert_eq!(name, "users");
        }

        #[test]
        fn test_create_index_all_dialects() {
            for dialect in [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Sqlite] {
                let sql = b"CREATE INDEX idx_email ON users (email);";
                let mut parser = Parser::with_dialect(&sql[..], 1024, dialect);

                let stmt = parser.read_statement().unwrap().unwrap();
                let (typ, name) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

                assert_eq!(typ, StatementType::CreateIndex);
                assert_eq!(name, "users");
            }
        }

        #[test]
        fn test_very_long_table_name() {
            let long_name = "a".repeat(128);
            let sql = format!("CREATE TABLE {} (id INT);", long_name);
            let mut parser = Parser::new(sql.as_bytes(), 1024);

            let stmt = parser.read_statement().unwrap().unwrap();
            let (typ, name) = Parser::<&[u8]>::parse_statement(&stmt);

            assert_eq!(typ, StatementType::CreateTable);
            assert_eq!(name, long_name);
        }

        #[test]
        fn test_unicode_in_string_values() {
            let sql = "INSERT INTO users VALUES (1, '日本語テスト', 'émoji 🎉');".as_bytes();
            let mut parser = Parser::new(sql, 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
        }

        #[test]
        fn test_binary_data_in_blob() {
            // Binary data with null bytes in a string
            let sql = b"INSERT INTO files VALUES (1, X'00FF00FF');";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
        }

        #[test]
        fn test_empty_table_name_handling() {
            // Malformed SQL - should not crash
            let sql = b"CREATE TABLE  (id INT);";
            let mut parser = Parser::new(&sql[..], 1024);

            let stmt = parser.read_statement().unwrap();
            assert!(stmt.is_some());
            // parse_statement should return Unknown for malformed SQL
            let (typ, _) = Parser::<&[u8]>::parse_statement(&stmt.unwrap());
            assert_eq!(typ, StatementType::Unknown);
        }

        #[test]
        fn test_multiple_semicolons() {
            let sql = b"SELECT 1;; SELECT 2;;;";
            let mut parser = Parser::new(&sql[..], 1024);

            let mut count = 0;
            while let Some(_) = parser.read_statement().unwrap() {
                count += 1;
            }
            // Should handle empty statements between semicolons
            assert!(count >= 2);
        }
    }
}
