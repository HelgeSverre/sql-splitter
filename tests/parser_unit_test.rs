use sql_splitter::parser::{detect_dialect, DialectConfidence, Parser, SqlDialect, StatementType};
use std::io::Cursor;

mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let stmt = b"CREATE TABLE users (id INT);";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::CreateTable);
        assert_eq!(name, "users");
    }

    #[test]
    fn test_parse_create_table_backticks() {
        let stmt = b"CREATE TABLE `my_table` (id INT);";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::CreateTable);
        assert_eq!(name, "my_table");
    }

    #[test]
    fn test_parse_insert() {
        let stmt = b"INSERT INTO posts VALUES (1, 'test');";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::Insert);
        assert_eq!(name, "posts");
    }

    #[test]
    fn test_parse_insert_backticks() {
        let stmt = b"INSERT INTO `comments` VALUES (1);";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::Insert);
        assert_eq!(name, "comments");
    }

    #[test]
    fn test_parse_alter_table() {
        let stmt = b"ALTER TABLE orders ADD COLUMN status INT;";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::AlterTable);
        assert_eq!(name, "orders");
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = b"DROP TABLE temp_data;";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::DropTable);
        assert_eq!(name, "temp_data");
    }

    #[test]
    fn test_read_statement_basic() {
        let sql = b"CREATE TABLE t1 (id INT); INSERT INTO t1 VALUES (1);";
        let mut parser = Parser::new(&sql[..], 1024);

        let stmt1 = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt1, b"CREATE TABLE t1 (id INT);");

        let stmt2 = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt2, b" INSERT INTO t1 VALUES (1);");

        let stmt3 = parser.read_statement().unwrap();
        assert!(stmt3.is_none());
    }

    #[test]
    fn test_read_statement_with_strings() {
        let sql = b"INSERT INTO t1 VALUES ('hello; world');";
        let mut parser = Parser::new(&sql[..], 1024);

        let stmt = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt, b"INSERT INTO t1 VALUES ('hello; world');");
    }

    #[test]
    fn test_read_statement_with_escaped_quotes() {
        let sql = b"INSERT INTO t1 VALUES ('it\\'s a test');";
        let mut parser = Parser::new(&sql[..], 1024);

        let stmt = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt, b"INSERT INTO t1 VALUES ('it\\'s a test');");
    }
}

mod copy_tests {
    use super::*;

    #[test]
    fn test_copy_from_stdin_detection() {
        let data = b"COPY public.table_001 (id, col_int, col_varchar, col_text, col_decimal, created_at) FROM stdin;\n1\t6892\tvalue_1\tLorem ipsum\n\\.\n";
        let reader = Cursor::new(&data[..]);
        let mut parser = Parser::with_dialect(reader, 1024, SqlDialect::Postgres);

        // First statement should be the COPY header
        let stmt1 = parser.read_statement().unwrap().unwrap();
        let s1 = String::from_utf8_lossy(&stmt1);
        assert!(s1.starts_with("COPY"), "First statement should be COPY");
        assert!(s1.contains("FROM stdin"), "Should contain FROM stdin");

        // Second statement should be the data block
        let stmt2 = parser.read_statement().unwrap().unwrap();
        let s2 = String::from_utf8_lossy(&stmt2);
        assert!(
            s2.contains("1\t6892"),
            "Data block should contain first row"
        );
        assert!(
            s2.ends_with("\\.\n"),
            "Data block should end with terminator"
        );
    }

    #[test]
    fn test_copy_with_leading_comments() {
        // pg_dump adds -- comments before COPY statements
        let data = b"--\n-- Data for Name: table_001\n--\n\nCOPY public.table_001 (id, name) FROM stdin;\n1\tfoo\n\\.\n";
        let reader = Cursor::new(&data[..]);
        let mut parser = Parser::with_dialect(reader, 1024, SqlDialect::Postgres);

        // First statement should be the COPY header (with leading comments)
        let stmt1 = parser.read_statement().unwrap().unwrap();
        let (stmt_type, table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(&stmt1, SqlDialect::Postgres);
        assert_eq!(stmt_type, StatementType::Copy);
        assert_eq!(table_name, "table_001");

        // Second statement should be the data block
        let stmt2 = parser.read_statement().unwrap().unwrap();
        let s2 = String::from_utf8_lossy(&stmt2);
        assert!(
            s2.ends_with("\\.\n"),
            "Data block should end with terminator"
        );
    }
}

mod dialect_detection_tests {
    use super::*;

    #[test]
    fn test_detect_mysql_dump_header() {
        let header = b"-- MySQL dump 10.13  Distrib 8.0.32, for Linux (x86_64)
--
-- Host: localhost    Database: mydb
-- ------------------------------------------------------
-- Server version	8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_mariadb_dump_header() {
        let header = b"-- MariaDB dump 10.19  Distrib 10.11.2-MariaDB
--
-- Host: localhost    Database: test
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_postgres_pgdump_header() {
        let header = b"--
-- PostgreSQL database dump
--

-- Dumped from database version 15.2
-- Dumped by pg_dump version 15.2

SET statement_timeout = 0;
SET search_path = public, pg_catalog;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_postgres_copy_statement() {
        let header = b"COPY public.users (id, name, email) FROM stdin;
1\tAlice\talice@example.com
2\tBob\tbob@example.com
\\.
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_postgres_dollar_quoting() {
        let header = b"CREATE OR REPLACE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Hello';
END;
$$ LANGUAGE plpgsql;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
    }

    #[test]
    fn test_detect_sqlite_dump_header() {
        // Real sqlite3 .dump output has a comment at the top
        let header = b"-- SQLite database dump
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES(1,'Alice');
COMMIT;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        // SQLite (+10) + PRAGMA (+5) + BEGIN TRANSACTION (+2) = High
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_sqlite_pragma_only() {
        let header = b"PRAGMA foreign_keys=OFF;
CREATE TABLE test (id INT);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_mysql_backticks() {
        let header = b"CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
    }

    #[test]
    fn test_detect_mysql_conditional_comments() {
        let header = b"/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!50503 SET NAMES utf8mb4 */;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_mysql_lock_tables() {
        let header = b"LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'test');
UNLOCK TABLES;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_empty_defaults_to_mysql() {
        let header = b"";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Low);
    }

    #[test]
    fn test_detect_generic_sql_defaults_to_mysql() {
        let header = b"CREATE TABLE users (id INT, name VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice');
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Low);
    }

    #[test]
    fn test_detect_postgres_create_extension() {
        let header = b"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";
CREATE TABLE users (id uuid DEFAULT uuid_generate_v4());
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
    }

    #[test]
    fn test_detect_sqlite_comment() {
        let header = b"-- SQLite database dump
-- Created by sqlite3

CREATE TABLE test (id INTEGER);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_scoring_postgres_beats_mysql_backticks() {
        // pg_dump header with some backticks in data shouldn't confuse it
        let header = b"--
-- PostgreSQL database dump
--
-- Dumped by pg_dump version 15.2

INSERT INTO notes VALUES (1, 'Use `code` for inline code');
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_begin_transaction_alone_is_low_confidence() {
        // BEGIN TRANSACTION is generic ANSI SQL, not definitive for SQLite
        let header = b"BEGIN TRANSACTION;
CREATE TABLE t (id INTEGER);
COMMIT;
";
        let result = detect_dialect(header);
        // Should detect SQLite but with low confidence since only generic markers
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        assert_eq!(result.confidence, DialectConfidence::Low);
    }

    #[test]
    fn test_backticks_only_is_low_confidence() {
        // Backticks alone shouldn't give high confidence MySQL
        let header = b"CREATE TABLE `users` (id INT);
INSERT INTO `users` VALUES (1);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Low);
    }

    #[test]
    fn test_conflicting_markers_postgres_wins() {
        // PostgreSQL dump header should beat MySQL-style backticks in data
        let header = b"-- PostgreSQL database dump
SET search_path = public;
INSERT INTO notes VALUES (1, 'Use `backticks` for code');
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        // High confidence because we have strong Postgres markers
        assert_eq!(result.confidence, DialectConfidence::High);
    }
}

mod mysql_insert_tests {
    use sql_splitter::parser::mysql_insert::{
        parse_mysql_insert_rows, parse_mysql_insert_rows_raw, PkValue,
    };
    use sql_splitter::schema::{Column, ColumnId, ColumnType, ForeignKey, TableId, TableSchema};

    fn create_simple_schema() -> TableSchema {
        let mut schema = TableSchema::new("users".to_string(), TableId(0));
        schema.columns = vec![
            Column {
                name: "id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(0),
                is_primary_key: true,
                is_nullable: false,
            },
            Column {
                name: "name".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(1),
                is_primary_key: false,
                is_nullable: true,
            },
            Column {
                name: "company_id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(2),
                is_primary_key: false,
                is_nullable: true,
            },
        ];
        schema.primary_key = vec![ColumnId(0)];
        schema.foreign_keys = vec![ForeignKey {
            name: None,
            columns: vec![ColumnId(2)],
            column_names: vec!["company_id".to_string()],
            referenced_table: "companies".to_string(),
            referenced_columns: vec!["id".to_string()],
            referenced_table_id: Some(TableId(1)),
        }];
        schema
    }

    #[test]
    fn test_parse_simple_insert() {
        let stmt = b"INSERT INTO users VALUES (1, 'Alice', 5);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 1);
        assert!(rows[0].pk.is_some());
        assert_eq!(rows[0].pk.as_ref().unwrap().len(), 1);
        assert_eq!(rows[0].pk.as_ref().unwrap()[0], PkValue::Int(1));
    }

    #[test]
    fn test_parse_multi_row_insert() {
        let stmt = b"INSERT INTO users VALUES (1, 'Alice', 5), (2, 'Bob', 5), (3, 'Carol', NULL);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].pk.as_ref().unwrap()[0], PkValue::Int(1));
        assert_eq!(rows[1].pk.as_ref().unwrap()[0], PkValue::Int(2));
        assert_eq!(rows[2].pk.as_ref().unwrap()[0], PkValue::Int(3));
    }

    #[test]
    fn test_parse_insert_with_column_list() {
        let stmt = b"INSERT INTO users (`id`, `name`, `company_id`) VALUES (1, 'Alice', 5);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk.as_ref().unwrap()[0], PkValue::Int(1));
    }

    #[test]
    fn test_parse_escaped_strings() {
        let stmt = b"INSERT INTO users VALUES (1, 'O\\'Brien', 5);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_parse_null_values() {
        let stmt = b"INSERT INTO users VALUES (1, NULL, NULL);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 1);
        assert!(rows[0].pk.is_some());
        // FK should not be extracted when NULL
        assert!(rows[0].fk_values.is_empty());
    }

    #[test]
    fn test_extract_fk_values() {
        let stmt = b"INSERT INTO users VALUES (1, 'Alice', 5);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fk_values.len(), 1);
        let (fk_ref, fk_tuple) = &rows[0].fk_values[0];
        assert_eq!(fk_ref.fk_index, 0);
        assert_eq!(fk_tuple[0], PkValue::Int(5));
    }

    #[test]
    fn test_parse_raw_without_schema() {
        let stmt = b"INSERT INTO users VALUES (1, 'Alice', 5), (2, 'Bob', 6);";

        let rows = parse_mysql_insert_rows_raw(stmt).unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows[0].pk.is_none()); // No schema, no PK extraction
        assert!(rows[0].fk_values.is_empty());
    }

    #[test]
    fn test_parse_negative_numbers() {
        let stmt = b"INSERT INTO users VALUES (-1, 'Test', -5);";
        let schema = create_simple_schema();

        let rows = parse_mysql_insert_rows(stmt, &schema).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk.as_ref().unwrap()[0], PkValue::Int(-1));
    }
}

mod postgres_copy_tests {
    use sql_splitter::parser::mysql_insert::PkValue;
    use sql_splitter::parser::postgres_copy::{
        parse_copy_columns, parse_postgres_copy_rows, CopyParser,
    };
    use sql_splitter::schema::{Column, ColumnId, ColumnType, ForeignKey, TableId, TableSchema};

    fn create_simple_schema() -> TableSchema {
        let mut schema = TableSchema::new("users".to_string(), TableId(0));
        schema.columns = vec![
            Column {
                name: "id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(0),
                is_primary_key: true,
                is_nullable: false,
            },
            Column {
                name: "name".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(1),
                is_primary_key: false,
                is_nullable: true,
            },
            Column {
                name: "company_id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(2),
                is_primary_key: false,
                is_nullable: true,
            },
        ];
        schema.primary_key = vec![ColumnId(0)];
        schema.foreign_keys = vec![ForeignKey {
            name: None,
            columns: vec![ColumnId(2)],
            column_names: vec!["company_id".to_string()],
            referenced_table: "companies".to_string(),
            referenced_columns: vec!["id".to_string()],
            referenced_table_id: Some(TableId(1)),
        }];
        schema
    }

    #[test]
    fn test_parse_copy_data() {
        let data = b"1\tAlice\t5\n2\tBob\t5\n3\tCarol\t\\N\n\\.";
        let schema = create_simple_schema();

        let rows = parse_postgres_copy_rows(
            data,
            &schema,
            vec![
                "id".to_string(),
                "name".to_string(),
                "company_id".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].pk.as_ref().unwrap()[0], PkValue::Int(1));
        assert_eq!(rows[1].pk.as_ref().unwrap()[0], PkValue::Int(2));
        assert_eq!(rows[2].pk.as_ref().unwrap()[0], PkValue::Int(3));
    }

    #[test]
    fn test_parse_null_values() {
        let data = b"1\t\\N\t\\N\n";
        let schema = create_simple_schema();

        let rows = parse_postgres_copy_rows(
            data,
            &schema,
            vec![
                "id".to_string(),
                "name".to_string(),
                "company_id".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        // FK should not be extracted when NULL
        assert!(rows[0].fk_values.is_empty());
    }

    #[test]
    fn test_parse_copy_columns() {
        let header = r#"COPY public.users (id, name, email) FROM stdin;"#;
        let cols = parse_copy_columns(header);
        assert_eq!(cols, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_decode_escapes() {
        let parser = CopyParser::new(&[]);
        let decoded = parser.decode_copy_escapes(b"hello\\tworld\\n");
        assert_eq!(decoded, b"hello\tworld\n");
    }
}

#[test]
fn test_copy_statement_type_detection() {
    use sql_splitter::parser::{Parser, SqlDialect, StatementType};
    use std::io::Cursor;

    let sql = b"COPY users (id, name) FROM stdin;
1\tAlice
2\tBob
\\.
";

    let cursor = Cursor::new(&sql[..]);
    let mut parser = Parser::with_dialect(cursor, 4096, SqlDialect::Postgres);

    let stmt = parser.read_statement().unwrap().unwrap();
    let (stmt_type, table_name) =
        Parser::<&[u8]>::parse_statement_with_dialect(&stmt, SqlDialect::Postgres);

    assert_eq!(stmt_type, StatementType::Copy, "Should be COPY statement");
    assert_eq!(table_name, "users", "Should extract table name");
}
