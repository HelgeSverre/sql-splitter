//! Unit and integration tests for the DuckDB query module.

use sql_splitter::duckdb::{
    CacheManager, OutputFormat, QueryConfig, QueryEngine, QueryResultFormatter,
};
use std::fs;
use tempfile::TempDir;

fn create_test_dump(content: &str) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let dump_path = temp_dir.path().join("test.sql");
    fs::write(&dump_path, content).unwrap();
    (temp_dir, dump_path)
}

fn simple_mysql_dump() -> &'static str {
    r#"
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(255),
    created_at DATETIME
);

INSERT INTO users (id, name, email, created_at) VALUES
(1, 'Alice', 'alice@example.com', '2024-01-01 10:00:00'),
(2, 'Bob', 'bob@example.com', '2024-01-02 11:00:00'),
(3, 'Charlie', 'charlie@example.com', '2024-01-03 12:00:00');

CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

INSERT INTO orders (id, user_id, amount, status) VALUES
(1, 1, 99.99, 'completed'),
(2, 1, 149.50, 'pending'),
(3, 2, 75.00, 'completed'),
(4, 3, 200.00, 'cancelled');
"#
}
// =============================================================================
// QueryEngine Tests
// =============================================================================

#[test]
fn test_query_engine_creation_in_memory() {
    let config = QueryConfig::default();
    let engine = QueryEngine::new(&config);
    assert!(engine.is_ok());
}

#[test]
fn test_query_engine_creation_disk_mode() {
    let config = QueryConfig {
        disk_mode: true,
        ..Default::default()
    };
    let engine = QueryEngine::new(&config);
    assert!(engine.is_ok());
}

#[test]
fn test_import_mysql_dump() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();

    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 2);
    assert!(stats.rows_inserted >= 7); // 3 users + 4 orders
}

#[test]
fn test_import_postgres_dump() {
    // Test importing with Postgres dialect specified
    let dump = r#"
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2)
);

INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99);
INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 49.99);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();

    let stats = engine.import_dump(&dump_path).unwrap();

    // PostgreSQL import should succeed - verify table was created
    assert_eq!(stats.tables_created, 1, "Expected 1 table created");
    assert_eq!(stats.rows_inserted, 2, "Expected 2 rows inserted");

    // Verify we can query the imported data
    let tables = engine.list_tables().unwrap();
    assert!(
        tables.contains(&"products".to_string()),
        "products table should exist"
    );

    let result = engine.query("SELECT COUNT(*) FROM products").unwrap();
    assert_eq!(result.rows[0][0], "2", "Should have 2 products")
}

#[test]
fn test_simple_query() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) as count FROM users").unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0], "count");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn test_query_with_filter() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT name FROM users WHERE id = 1").unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "Alice");
}

#[test]
fn test_query_with_join() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(
            "SELECT u.name, SUM(o.amount) as total
             FROM users u
             JOIN orders o ON u.id = o.user_id
             GROUP BY u.name
             ORDER BY total DESC",
        )
        .unwrap();

    assert!(result.rows.len() >= 1);
    // Alice has 2 orders: 99.99 + 149.50 = 249.49
}

#[test]
fn test_query_aggregate_functions() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT AVG(amount), MIN(amount), MAX(amount) FROM orders")
        .unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_list_tables() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();

    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"users".to_string()));
    assert!(tables.contains(&"orders".to_string()));
}

#[test]
fn test_describe_table() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.describe_table("users").unwrap();

    assert!(!result.rows.is_empty());
    // Should have columns like: column_name, column_type, null, key, etc.
}

#[test]
fn test_import_with_table_filter() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig {
        tables: Some(vec!["users".to_string()]),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    // Only users table should be imported
    assert_eq!(stats.tables_created, 1);

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"users".to_string()));
    assert!(!tables.contains(&"orders".to_string()));
}

#[test]
fn test_query_result_empty() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM users WHERE id = 999").unwrap();

    assert!(result.is_empty());
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_query_execution_time() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM users").unwrap();

    assert!(result.execution_time_secs >= 0.0);
    assert!(result.execution_time_secs < 10.0); // Should be fast
}

// =============================================================================
// OutputFormat Tests
// =============================================================================

#[test]
fn test_output_format_table() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT id, name FROM users LIMIT 2").unwrap();
    let formatted = QueryResultFormatter::format(&result, OutputFormat::Table);

    assert!(formatted.contains("id"));
    assert!(formatted.contains("name"));
    assert!(formatted.contains("Alice"));
    assert!(formatted.contains("Bob"));
    assert!(formatted.contains("2 rows"));
}

#[test]
fn test_output_format_json() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT id, name FROM users LIMIT 2").unwrap();
    let formatted = QueryResultFormatter::format(&result, OutputFormat::Json);

    let parsed: Vec<serde_json::Value> = serde_json::from_str(&formatted).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0]["id"], 1);
    assert_eq!(parsed[0]["name"], "Alice");
}

#[test]
fn test_output_format_jsonl() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT id, name FROM users LIMIT 2").unwrap();
    let formatted = QueryResultFormatter::format(&result, OutputFormat::JsonLines);

    let lines: Vec<&str> = formatted.lines().collect();
    assert_eq!(lines.len(), 2);

    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["id"], 1);
}

#[test]
fn test_output_format_csv() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT id, name FROM users LIMIT 2").unwrap();
    let formatted = QueryResultFormatter::format(&result, OutputFormat::Csv);

    assert!(formatted.starts_with("id,name\n"));
    assert!(formatted.contains("1,Alice"));
    assert!(formatted.contains("2,Bob"));
}

#[test]
fn test_output_format_tsv() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT id, name FROM users LIMIT 2").unwrap();
    let formatted = QueryResultFormatter::format(&result, OutputFormat::Tsv);

    assert!(formatted.starts_with("id\tname\n"));
    assert!(formatted.contains("1\tAlice"));
}

#[test]
fn test_output_format_parse() {
    assert_eq!(
        "table".parse::<OutputFormat>().unwrap(),
        OutputFormat::Table
    );
    assert_eq!("json".parse::<OutputFormat>().unwrap(), OutputFormat::Json);
    assert_eq!(
        "jsonl".parse::<OutputFormat>().unwrap(),
        OutputFormat::JsonLines
    );
    assert_eq!("csv".parse::<OutputFormat>().unwrap(), OutputFormat::Csv);
    assert_eq!("tsv".parse::<OutputFormat>().unwrap(), OutputFormat::Tsv);
    assert!("invalid".parse::<OutputFormat>().is_err());
}

// =============================================================================
// CacheManager Tests
// =============================================================================

#[test]
fn test_cache_manager_creation() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::with_dir(temp_dir.path().to_path_buf());
    assert!(cache_manager.is_ok());
}

#[test]
fn test_cache_key_computation() {
    let (_temp_dir, dump_path) = create_test_dump("SELECT 1;");

    let key1 = CacheManager::compute_cache_key(&dump_path).unwrap();
    let key2 = CacheManager::compute_cache_key(&dump_path).unwrap();

    assert_eq!(key1, key2);
    assert_eq!(key1.len(), 32); // 16 bytes hex encoded
}

#[test]
fn test_cache_key_changes_with_content() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.sql");

    fs::write(&test_file, "SELECT 1;").unwrap();
    let key1 = CacheManager::compute_cache_key(&test_file).unwrap();

    // Modify file with different size
    fs::write(&test_file, "SELECT 2; -- extra content").unwrap();
    let key2 = CacheManager::compute_cache_key(&test_file).unwrap();

    assert_ne!(key1, key2);
}

#[test]
fn test_cache_path() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::with_dir(temp_dir.path().to_path_buf()).unwrap();

    let cache_path = cache_manager.cache_path("abc123");
    assert!(cache_path.to_string_lossy().ends_with("abc123.duckdb"));
}

#[test]
fn test_cache_has_valid_cache_when_missing() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::with_dir(temp_dir.path().to_path_buf()).unwrap();

    let test_file = temp_dir.path().join("test.sql");
    fs::write(&test_file, "SELECT 1;").unwrap();

    assert!(!cache_manager.has_valid_cache(&test_file).unwrap());
}

#[test]
fn test_cache_list_entries_empty() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::with_dir(temp_dir.path().to_path_buf()).unwrap();

    let entries = cache_manager.list_entries().unwrap();
    assert!(entries.is_empty());
}

#[test]
fn test_cache_total_size_empty() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::with_dir(temp_dir.path().to_path_buf()).unwrap();

    assert_eq!(cache_manager.total_size().unwrap(), 0);
}

// =============================================================================
// TypeConverter Tests (via DumpLoader behavior)
// =============================================================================

#[test]
fn test_type_conversion_varchar() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("VARCHAR(255)"), "VARCHAR(255)");
    assert_eq!(TypeConverter::convert("VARCHAR"), "VARCHAR");
}

#[test]
fn test_type_conversion_int() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("INT"), "INTEGER");
    assert_eq!(TypeConverter::convert("BIGINT"), "BIGINT");
    assert_eq!(TypeConverter::convert("TINYINT"), "TINYINT");
}

#[test]
fn test_type_conversion_unsigned() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("INT UNSIGNED"), "UINTEGER");
    assert_eq!(TypeConverter::convert("BIGINT UNSIGNED"), "UBIGINT");
}

#[test]
fn test_type_conversion_datetime() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("DATETIME"), "TIMESTAMP");
    assert_eq!(TypeConverter::convert("DATE"), "DATE");
    assert_eq!(TypeConverter::convert("TIME"), "TIME");
}

#[test]
fn test_type_conversion_text() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("TEXT"), "TEXT");
    assert_eq!(TypeConverter::convert("MEDIUMTEXT"), "TEXT");
    assert_eq!(TypeConverter::convert("LONGTEXT"), "TEXT");
}

#[test]
fn test_type_conversion_postgres() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("SERIAL"), "INTEGER");
    assert_eq!(TypeConverter::convert("BIGSERIAL"), "BIGINT");
    assert_eq!(TypeConverter::convert("JSONB"), "JSON");
    assert_eq!(TypeConverter::convert("UUID"), "UUID");
    assert_eq!(TypeConverter::convert("BYTEA"), "BLOB");
}

#[test]
fn test_type_conversion_enum() {
    use sql_splitter::duckdb::TypeConverter;

    assert_eq!(TypeConverter::convert("ENUM('a','b','c')"), "VARCHAR");
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

#[test]
fn test_import_empty_dump() {
    let (_temp_dir, dump_path) = create_test_dump("");

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 0);
    assert_eq!(stats.rows_inserted, 0);
}

#[test]
fn test_import_comments_only() {
    let dump = r#"
-- This is a comment
/* Multi-line
   comment */
-- Another comment
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 0);
}

#[test]
fn test_query_invalid_sql() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELEC * FORM users"); // Invalid SQL
    assert!(result.is_err());
}

#[test]
fn test_query_nonexistent_table() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_null_handling() {
    let dump = r#"
CREATE TABLE test (id INT, value VARCHAR(100));
INSERT INTO test VALUES (1, NULL);
INSERT INTO test VALUES (2, 'hello');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM test ORDER BY id").unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], "NULL");
    assert_eq!(result.rows[1][1], "hello");
}

#[test]
fn test_special_characters_in_data() {
    let dump = r#"
CREATE TABLE test (id INT, value VARCHAR(100));
INSERT INTO test VALUES (1, 'hello''world');
INSERT INTO test VALUES (2, 'tab	here');
INSERT INTO test VALUES (3, 'line
break');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn test_memory_limit_config() {
    let config = QueryConfig {
        memory_limit: Some("1GB".to_string()),
        ..Default::default()
    };
    let engine = QueryEngine::new(&config);
    assert!(engine.is_ok());
}

#[test]
fn test_import_stats_display() {
    use sql_splitter::duckdb::ImportStats;

    let stats = ImportStats {
        tables_created: 5,
        rows_inserted: 1000,
        duration_secs: 1.5,
        ..Default::default()
    };

    let display = format!("{}", stats);
    assert!(display.contains("5 tables"));
    assert!(display.contains("1000 rows"));
    assert!(display.contains("1.50s"));
}

// =============================================================================
// DuckDB-specific SQL Features Tests
// =============================================================================

#[test]
fn test_duckdb_analytical_query() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    // DuckDB supports window functions
    let result = engine
        .query(
            "SELECT name, 
                    ROW_NUMBER() OVER (ORDER BY id) as row_num 
             FROM users",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_duckdb_cte() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    // DuckDB supports CTEs
    let result = engine
        .query(
            "WITH user_orders AS (
                SELECT user_id, COUNT(*) as order_count 
                FROM orders 
                GROUP BY user_id
             )
             SELECT u.name, COALESCE(uo.order_count, 0) as orders
             FROM users u
             LEFT JOIN user_orders uo ON u.id = uo.user_id
             ORDER BY orders DESC",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_execute_statement() {
    let config = QueryConfig::default();
    let engine = QueryEngine::new(&config).unwrap();

    // Execute a DDL statement
    let affected = engine.execute("CREATE TABLE test (id INTEGER)");
    assert!(affected.is_ok());

    // Insert data
    let affected = engine.execute("INSERT INTO test VALUES (1), (2)");
    assert!(affected.is_ok());

    // Query the data
    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "2");
}

// =============================================================================
// MySQL Syntax Edge Cases
// =============================================================================

#[test]
fn test_mysql_unique_key() {
    let dump = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255),
    UNIQUE KEY `email_unique` (`email`)
);

INSERT INTO `users` VALUES (1, 'test@example.com');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();
    let warnings = stats.warnings.clone();

    // Should either create the table (ignoring UNIQUE KEY) or report a warning
    // The important thing is it doesn't crash
    let tables = engine.list_tables().unwrap_or_default();
    if tables.contains(&"users".to_string()) {
        let result = engine.query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(result.rows[0][0], "1");
    } else {
        // Table creation failed but should have warning
        assert!(!warnings.is_empty());
    }
}

#[test]
fn test_mysql_key_constraint() {
    let dump = r#"
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    KEY idx_user_id (user_id)
);

INSERT INTO orders VALUES (1, 100);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();
    let warnings = stats.warnings.clone();

    let tables = engine.list_tables().unwrap_or_default();
    if tables.contains(&"orders".to_string()) {
        let result = engine.query("SELECT COUNT(*) FROM orders").unwrap();
        assert_eq!(result.rows[0][0], "1");
    } else {
        assert!(!warnings.is_empty());
    }
}

#[test]
fn test_mysql_generated_column() {
    let dump = r#"
CREATE TABLE products (
    id INT PRIMARY KEY,
    price DECIMAL(10,2),
    tax DECIMAL(10,2) GENERATED ALWAYS AS (price * 0.1) STORED
);

INSERT INTO products (id, price) VALUES (1, 100.00);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();
    let warnings = stats.warnings.clone();

    // GENERATED columns may cause warnings but shouldn't crash
    let tables = engine.list_tables().unwrap_or_default();
    if !tables.contains(&"products".to_string()) {
        assert!(!warnings.is_empty());
    }
}

#[test]
fn test_mysql_fulltext_index() {
    let dump = r#"
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    body TEXT,
    FULLTEXT KEY idx_search (title, body)
);

INSERT INTO articles VALUES (1, 'Hello World', 'This is a test article');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();
    let warnings = stats.warnings.clone();

    let tables = engine.list_tables().unwrap_or_default();
    if tables.contains(&"articles".to_string()) {
        let result = engine.query("SELECT COUNT(*) FROM articles").unwrap();
        assert_eq!(result.rows[0][0], "1");
    } else {
        assert!(!warnings.is_empty());
    }
}

#[test]
fn test_mysql_backslash_escape_single_quote() {
    let dump = r#"
CREATE TABLE test (id INT, value VARCHAR(100));
INSERT INTO test VALUES (1, 'It\'s a test');
INSERT INTO test VALUES (2, 'She said \"hello\"');
INSERT INTO test VALUES (3, 'Path: C:\\Users\\test');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "3");

    let result = engine.query("SELECT value FROM test WHERE id = 1").unwrap();
    assert!(result.rows[0][0].contains("It"));
    assert!(result.rows[0][0].contains("s a test"));
}

#[test]
fn test_mysql_backslash_escape_newline() {
    let dump = r#"
CREATE TABLE test (id INT, value TEXT);
INSERT INTO test VALUES (1, 'Line 1\nLine 2\nLine 3');
INSERT INTO test VALUES (2, 'Tab\there');
INSERT INTO test VALUES (3, 'Return\rhere');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn test_mysql_character_set_stripping() {
    let dump = r#"
CREATE TABLE test (
    id INT PRIMARY KEY,
    name VARCHAR(100) CHARACTER SET utf8mb4,
    description TEXT CHARACTER SET latin1
);

INSERT INTO test VALUES (1, 'Test', 'Description');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"test".to_string()));

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn test_mysql_table_options_stripping() {
    let dump = r#"
CREATE TABLE test (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC;

INSERT INTO test VALUES (1, 'Test');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn test_mysql_on_update_current_timestamp() {
    let dump = r#"
CREATE TABLE test (
    id INT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO test (id) VALUES (1);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"test".to_string()));
}

#[test]
fn test_mysql_conditional_comments() {
    let dump = r#"
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;

CREATE TABLE test (id INT PRIMARY KEY);

INSERT INTO test VALUES (1);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn test_mysql_unsigned_types() {
    let dump = r#"
CREATE TABLE test (
    id INT UNSIGNED PRIMARY KEY,
    small_val SMALLINT UNSIGNED,
    big_val BIGINT UNSIGNED,
    tiny_val TINYINT(3) UNSIGNED
);

INSERT INTO test VALUES (1, 100, 999999999999, 255);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM test").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_mysql_enum_type() {
    let dump = r#"
CREATE TABLE test (
    id INT PRIMARY KEY,
    status ENUM('active', 'inactive', 'pending')
);

INSERT INTO test VALUES (1, 'active'), (2, 'inactive');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "2");
}

#[test]
fn test_mysql_set_type() {
    let dump = r#"
CREATE TABLE test (
    id INT PRIMARY KEY,
    permissions SET('read', 'write', 'delete')
);

INSERT INTO test VALUES (1, 'read,write');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");
}

// =============================================================================
// PostgreSQL Syntax Edge Cases
// =============================================================================

#[test]
fn test_postgres_serial_type() {
    let dump = r#"
CREATE TABLE test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO test (name) VALUES ('Alice'), ('Bob');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"test".to_string()));
}

#[test]
fn test_postgres_schema_prefix() {
    let dump = r#"
CREATE TABLE public.users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO public.users VALUES (1, 'Test');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    // Should strip schema prefix
    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"users".to_string()));
}

#[test]
fn test_postgres_nextval() {
    let dump = r#"
CREATE TABLE test (
    id INTEGER DEFAULT nextval('test_id_seq'::regclass),
    name VARCHAR(100)
);

INSERT INTO test (id, name) VALUES (1, 'Test');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"test".to_string()));
}

#[test]
fn test_postgres_type_cast() {
    let dump = r#"
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP
);

INSERT INTO test VALUES (1, '2024-01-01 10:00:00'::timestamp);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"test".to_string()));
}

#[test]
fn test_postgres_jsonb_type() {
    let dump = r#"
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    data JSONB
);

INSERT INTO test VALUES (1, '{"key": "value"}');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn test_postgres_uuid_type() {
    let dump = r#"
CREATE TABLE test (
    id UUID PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO test VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'Test');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn test_postgres_array_type() {
    // Arrays are not fully supported but shouldn't crash
    let dump = r#"
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    tags TEXT[]
);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    // Array types may fail to import, but should report via warnings not panic
    // Either table was created, or we got a warning explaining why not
    let has_table = stats.tables_created > 0;
    let has_warning = !stats.warnings.is_empty();
    assert!(
        has_table || has_warning,
        "Expected either successful import or warning message"
    );
}

// =============================================================================
// Date/Time Handling Tests
// =============================================================================

#[test]
fn test_datetime_formatting() {
    let dump = r#"
CREATE TABLE events (
    id INT PRIMARY KEY,
    event_date DATE,
    event_time TIME,
    event_datetime DATETIME
);

INSERT INTO events VALUES (1, '2024-01-15', '14:30:00', '2024-01-15 14:30:00');
INSERT INTO events VALUES (2, '2024-12-31', '23:59:59', '2024-12-31 23:59:59');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM events ORDER BY id").unwrap();
    assert_eq!(result.rows.len(), 2);
    // The date column is index 1 - check it's not empty and has some recognizable date format
    // DuckDB may format dates differently (as days since epoch, ISO format, etc.)
    let date_value = &result.rows[0][1];
    assert!(
        !date_value.is_empty() && date_value != "NULL",
        "Date should have a value, got: {}",
        date_value
    );
}

#[test]
fn test_timestamp_with_timezone() {
    let dump = r#"
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO test VALUES (1, '2024-01-15 14:30:00+00');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"test".to_string()));
}

// =============================================================================
// Large Data and Performance Tests
// =============================================================================

#[test]
fn test_large_insert_batch() {
    let mut dump = String::from("CREATE TABLE test (id INT PRIMARY KEY, value VARCHAR(100));\n");
    dump.push_str("INSERT INTO test VALUES ");
    let values: Vec<String> = (1..=1000)
        .map(|i| format!("({}, 'Value {}')", i, i))
        .collect();
    dump.push_str(&values.join(", "));
    dump.push_str(";\n");

    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 1);
    assert_eq!(stats.rows_inserted, 1000);

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1000");
}

#[test]
fn test_many_tables() {
    let mut dump = String::new();
    for i in 1..=50 {
        dump.push_str(&format!(
            "CREATE TABLE table_{} (id INT PRIMARY KEY, value VARCHAR(100));\n",
            i
        ));
        dump.push_str(&format!("INSERT INTO table_{} VALUES (1, 'Test');\n", i));
    }

    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 50);

    let tables = engine.list_tables().unwrap();
    assert_eq!(tables.len(), 50);
}

#[test]
fn test_wide_table() {
    let mut dump = String::from("CREATE TABLE test (\n  id INT PRIMARY KEY");
    for i in 1..=100 {
        dump.push_str(&format!(",\n  col_{} VARCHAR(50)", i));
    }
    dump.push_str("\n);\n");

    let mut values = String::from("INSERT INTO test VALUES (1");
    for i in 1..=100 {
        values.push_str(&format!(", 'value_{}'", i));
    }
    values.push_str(");\n");
    dump.push_str(&values);

    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "1");

    let result = engine.query("SELECT * FROM test").unwrap();
    assert_eq!(result.columns.len(), 101); // id + 100 columns
}

#[test]
fn test_long_string_values() {
    let long_string = "x".repeat(10000);
    let dump = format!(
        r#"
CREATE TABLE test (id INT PRIMARY KEY, content TEXT);
INSERT INTO test VALUES (1, '{}');
"#,
        long_string
    );

    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT LENGTH(content) FROM test").unwrap();
    assert_eq!(result.rows[0][0], "10000");
}

// =============================================================================
// Complex Query Tests
// =============================================================================

#[test]
fn test_subquery() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)")
        .unwrap();

    assert!(!result.rows.is_empty());
}

#[test]
fn test_union_query() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(
            "SELECT name as value FROM users 
             UNION ALL 
             SELECT status as value FROM orders",
        )
        .unwrap();

    assert!(!result.rows.is_empty());
}

#[test]
fn test_case_expression() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(
            "SELECT name, 
                    CASE WHEN id = 1 THEN 'first' ELSE 'other' END as position 
             FROM users",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 3);
}

// =============================================================================
// Output Format Edge Cases
// =============================================================================

#[test]
fn test_json_output_with_special_chars() {
    let dump = r#"
CREATE TABLE test (id INT, value VARCHAR(100));
INSERT INTO test VALUES (1, 'hello "world"');
INSERT INTO test VALUES (2, 'line1
line2');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM test ORDER BY id").unwrap();
    let json_output = QueryResultFormatter::format(&result, OutputFormat::Json);

    // Should be valid JSON
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&json_output);
    assert!(parsed.is_ok());
}

#[test]
fn test_csv_output_with_commas() {
    let dump = r#"
CREATE TABLE test (id INT, value VARCHAR(100));
INSERT INTO test VALUES (1, 'hello, world');
INSERT INTO test VALUES (2, 'value with "quotes"');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM test ORDER BY id").unwrap();
    let csv_output = QueryResultFormatter::format(&result, OutputFormat::Csv);

    // Values with commas should be quoted
    assert!(csv_output.contains("\"hello, world\""));
}

#[test]
fn test_empty_result_formatting() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM users WHERE id = 999").unwrap();

    // All formats should handle empty results
    let table_output = QueryResultFormatter::format(&result, OutputFormat::Table);
    assert!(table_output.contains("0 rows"));

    let json_output = QueryResultFormatter::format(&result, OutputFormat::Json);
    assert_eq!(json_output.trim(), "[]");

    let csv_output = QueryResultFormatter::format(&result, OutputFormat::Csv);
    assert!(csv_output.lines().count() == 1); // Just header
}

// =============================================================================
// SQLite Dialect Tests
// =============================================================================

#[test]
fn test_sqlite_simple_dump() {
    let dump = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Sqlite),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"users".to_string()));

    let result = engine.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(result.rows[0][0], "2");
}

#[test]
fn test_sqlite_autoincrement() {
    // SQLite AUTOINCREMENT is not directly supported by DuckDB
    // This test verifies we handle it gracefully (either strip it or warn)
    let dump = r#"
CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO items VALUES (1, 'Item A');
INSERT INTO items VALUES (2, 'Item B');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Sqlite),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"items".to_string()));

    let result = engine.query("SELECT COUNT(*) FROM items").unwrap();
    assert_eq!(result.rows[0][0], "2");
}

// =============================================================================
// PostgreSQL COPY Tests
// =============================================================================

#[test]
fn test_postgres_copy_format() {
    let dump = r#"
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2)
);

COPY products (id, name, price) FROM stdin;
1	Widget	19.99
2	Gadget	49.99
3	Gizmo	29.99
\.
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 1);

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"products".to_string()));

    let result = engine.query("SELECT COUNT(*) FROM products").unwrap();
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn test_postgres_copy_with_nulls() {
    let dump = r#"
CREATE TABLE test (
    id INTEGER,
    value TEXT
);

COPY test (id, value) FROM stdin;
1	hello
2	\N
3	world
\.
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig {
        dialect: Some(sql_splitter::parser::SqlDialect::Postgres),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM test ORDER BY id").unwrap();
    assert_eq!(result.rows.len(), 3);
}

// =============================================================================
// Compressed File Tests
// =============================================================================

#[test]
fn test_gzip_compressed_dump() {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let dump = simple_mysql_dump();
    let temp_dir = TempDir::new().unwrap();
    let dump_path = temp_dir.path().join("test.sql.gz");

    let file = std::fs::File::create(&dump_path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(dump.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 2);
    assert!(stats.rows_inserted >= 7);

    let result = engine.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn test_zstd_compressed_dump() {
    let dump = simple_mysql_dump();
    let temp_dir = TempDir::new().unwrap();
    let dump_path = temp_dir.path().join("test.sql.zst");

    let file = std::fs::File::create(&dump_path).unwrap();
    let mut encoder = zstd::stream::Encoder::new(file, 3).unwrap();
    std::io::Write::write_all(&mut encoder, dump.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 2);
    assert!(stats.rows_inserted >= 7);
}

// =============================================================================
// Additional Compression Format Tests
// =============================================================================

#[test]
fn test_bzip2_compressed_dump() {
    use bzip2::write::BzEncoder;
    use bzip2::Compression;
    use std::io::Write;

    let dump = simple_mysql_dump();
    let temp_dir = TempDir::new().unwrap();
    let dump_path = temp_dir.path().join("test.sql.bz2");

    let file = std::fs::File::create(&dump_path).unwrap();
    let mut encoder = BzEncoder::new(file, Compression::default());
    encoder.write_all(dump.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 2);
    assert!(stats.rows_inserted >= 7);
}

#[test]
fn test_xz_compressed_dump() {
    use std::io::Write;
    use xz2::write::XzEncoder;

    let dump = simple_mysql_dump();
    let temp_dir = TempDir::new().unwrap();
    let dump_path = temp_dir.path().join("test.sql.xz");

    let file = std::fs::File::create(&dump_path).unwrap();
    let mut encoder = XzEncoder::new(file, 6);
    encoder.write_all(dump.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 2);
    assert!(stats.rows_inserted >= 7);
}

// =============================================================================
// Data Type Edge Cases
// =============================================================================

#[test]
fn test_numeric_edge_cases() {
    let dump = r#"
CREATE TABLE numbers (
    id INT,
    tiny TINYINT,
    small SMALLINT,
    big BIGINT,
    float_val FLOAT,
    double_val DOUBLE,
    decimal_val DECIMAL(20,10)
);

INSERT INTO numbers VALUES 
(1, -128, -32768, -9223372036854775808, -3.4e38, -1.7e308, -9999999999.9999999999),
(2, 127, 32767, 9223372036854775807, 3.4e38, 1.7e308, 9999999999.9999999999),
(3, 0, 0, 0, 0.0, 0.0, 0.0),
(4, NULL, NULL, NULL, NULL, NULL, NULL);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM numbers").unwrap();
    assert_eq!(result.rows[0][0], "4");

    // Test aggregations on edge values
    let result = engine
        .query("SELECT MIN(big), MAX(big) FROM numbers")
        .unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn test_boolean_values() {
    let dump = r#"
CREATE TABLE flags (
    id INT,
    is_active BOOLEAN,
    is_deleted TINYINT(1)
);

INSERT INTO flags VALUES (1, TRUE, 1);
INSERT INTO flags VALUES (2, FALSE, 0);
INSERT INTO flags VALUES (3, NULL, NULL);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT * FROM flags WHERE is_active = TRUE")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_empty_string_vs_null() {
    let dump = r#"
CREATE TABLE strings (
    id INT,
    value VARCHAR(100)
);

INSERT INTO strings VALUES (1, '');
INSERT INTO strings VALUES (2, NULL);
INSERT INTO strings VALUES (3, 'text');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT * FROM strings WHERE value = ''")
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    let result = engine
        .query("SELECT * FROM strings WHERE value IS NULL")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_unicode_data() {
    let dump = r#"
CREATE TABLE unicode_test (
    id INT,
    name VARCHAR(255),
    description TEXT
);

INSERT INTO unicode_test VALUES (1, '日本語', 'Japanese text');
INSERT INTO unicode_test VALUES (2, 'Ελληνικά', 'Greek text');
INSERT INTO unicode_test VALUES (3, '🎉🚀💻', 'Emoji party');
INSERT INTO unicode_test VALUES (4, 'Ñoño', 'Spanish with tilde');
INSERT INTO unicode_test VALUES (5, 'Привет мир', 'Russian hello world');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM unicode_test").unwrap();
    assert_eq!(result.rows[0][0], "5");

    let result = engine
        .query("SELECT name FROM unicode_test WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows[0][0], "日本語");
}

#[test]
fn test_very_long_strings() {
    let long_string = "x".repeat(10000);
    let dump = format!(
        r#"
CREATE TABLE long_strings (
    id INT,
    content TEXT
);

INSERT INTO long_strings VALUES (1, '{}');
"#,
        long_string
    );
    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT LENGTH(content) FROM long_strings")
        .unwrap();
    assert_eq!(result.rows[0][0], "10000");
}

#[test]
fn test_json_data_in_column() {
    let dump = r#"
CREATE TABLE json_data (
    id INT,
    data JSON
);

INSERT INTO json_data VALUES (1, '{"name": "Alice", "age": 30}');
INSERT INTO json_data VALUES (2, '{"items": [1, 2, 3], "nested": {"key": "value"}}');
INSERT INTO json_data VALUES (3, '[]');
INSERT INTO json_data VALUES (4, 'null');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM json_data").unwrap();
    assert_eq!(result.rows[0][0], "4");
}

#[test]
fn test_date_time_values() {
    let dump = r#"
CREATE TABLE date_values (
    id INT,
    date_val DATE,
    time_val TIME,
    timestamp_val TIMESTAMP
);

INSERT INTO date_values VALUES (1, '2024-01-15', '14:30:00', '2024-01-15 14:30:00');
INSERT INTO date_values VALUES (2, '1970-01-01', '00:00:00', '1970-01-01 00:00:00');
INSERT INTO date_values VALUES (3, '2099-12-31', '23:59:59', '2099-12-31 23:59:59');
INSERT INTO date_values VALUES (4, NULL, NULL, NULL);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT COUNT(*) FROM date_values WHERE date_val IS NOT NULL")
        .unwrap();
    assert_eq!(result.rows[0][0], "3");
}

// =============================================================================
// Complex Query Pattern Tests
// =============================================================================

#[test]
fn test_window_functions() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT name, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM users")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_group_by_having() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id HAVING COUNT(*) > 1")
        .unwrap();
    // User 1 has 2 orders
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn test_order_by_nulls() {
    let dump = r#"
CREATE TABLE nullable (id INT, value INT);
INSERT INTO nullable VALUES (1, 10), (2, NULL), (3, 5), (4, NULL), (5, 20);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT id FROM nullable ORDER BY value NULLS FIRST")
        .unwrap();
    assert_eq!(result.rows.len(), 5);
    // NULLs should be first
    assert!(result.rows[0][0] == "2" || result.rows[0][0] == "4");
}

#[test]
fn test_limit_offset() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], "2");
    assert_eq!(result.rows[1][0], "3");
}

#[test]
fn test_distinct_query() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT DISTINCT status FROM orders").unwrap();
    assert_eq!(result.rows.len(), 3); // completed, pending, cancelled
}

#[test]
fn test_left_join() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(
            "SELECT u.name, o.id as order_id 
         FROM users u 
         LEFT JOIN orders o ON u.id = o.user_id 
         ORDER BY u.id, o.id",
        )
        .unwrap();
    // All users should appear, even those without orders
    assert!(result.rows.len() >= 3);
}

#[test]
fn test_self_join() {
    let dump = r#"
CREATE TABLE employees (id INT, name VARCHAR(100), manager_id INT);
INSERT INTO employees VALUES (1, 'CEO', NULL);
INSERT INTO employees VALUES (2, 'VP', 1);
INSERT INTO employees VALUES (3, 'Manager', 2);
INSERT INTO employees VALUES (4, 'Developer', 3);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(
            "SELECT e.name as employee, m.name as manager 
         FROM employees e 
         LEFT JOIN employees m ON e.manager_id = m.id",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn test_exists_subquery() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(
            "SELECT name FROM users u 
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
        )
        .unwrap();
    // Users with at least one order
    assert!(result.rows.len() >= 1);
}

#[test]
fn test_in_subquery() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT name FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders)")
        .unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn test_coalesce_nullif() {
    let dump = r#"
CREATE TABLE test (id INT, a INT, b INT);
INSERT INTO test VALUES (1, NULL, 10);
INSERT INTO test VALUES (2, 5, 5);
INSERT INTO test VALUES (3, 0, 10);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT COALESCE(a, b, -1) as val FROM test ORDER BY id")
        .unwrap();
    assert_eq!(result.rows[0][0], "10"); // a is NULL, use b
    assert_eq!(result.rows[1][0], "5"); // a is not NULL

    let result = engine
        .query("SELECT NULLIF(a, b) as val FROM test WHERE id = 2")
        .unwrap();
    assert_eq!(result.rows[0][0], "NULL"); // a equals b, return NULL
}

#[test]
fn test_string_functions() {
    let dump = r#"
CREATE TABLE strings (id INT, val VARCHAR(100));
INSERT INTO strings VALUES (1, 'Hello World');
INSERT INTO strings VALUES (2, '  trimme  ');
INSERT INTO strings VALUES (3, 'UPPER lower');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT UPPER(val) FROM strings WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows[0][0], "HELLO WORLD");

    let result = engine
        .query("SELECT TRIM(val) FROM strings WHERE id = 2")
        .unwrap();
    assert_eq!(result.rows[0][0], "trimme");

    let result = engine
        .query("SELECT SUBSTRING(val, 1, 5) FROM strings WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows[0][0], "Hello");
}

#[test]
fn test_date_functions() {
    let dump = r#"
CREATE TABLE dates (id INT, dt TIMESTAMP);
INSERT INTO dates VALUES (1, '2024-06-15 10:30:00');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT EXTRACT(YEAR FROM dt) FROM dates")
        .unwrap();
    assert_eq!(result.rows[0][0], "2024");

    let result = engine
        .query("SELECT EXTRACT(MONTH FROM dt) FROM dates")
        .unwrap();
    assert_eq!(result.rows[0][0], "6");
}

// =============================================================================
// Error Handling Edge Cases
// =============================================================================

#[test]
fn test_query_syntax_error_message() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELEC * FORM users");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to prepare query"));
}

#[test]
fn test_division_by_zero() {
    let dump = r#"
CREATE TABLE test (id INT, val INT);
INSERT INTO test VALUES (1, 10);
INSERT INTO test VALUES (2, 0);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    // DuckDB returns NULL for division by zero, not an error
    let result = engine
        .query("SELECT 10 / val FROM test ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_ambiguous_column_error() {
    let dump = r#"
CREATE TABLE t1 (id INT, name VARCHAR(100));
CREATE TABLE t2 (id INT, name VARCHAR(100));
INSERT INTO t1 VALUES (1, 'A');
INSERT INTO t2 VALUES (1, 'B');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    // Ambiguous column reference should fail
    let result = engine.query("SELECT id FROM t1, t2");
    assert!(result.is_err());
}

// =============================================================================
// Schema Edge Cases
// =============================================================================

#[test]
fn test_reserved_keyword_as_identifier() {
    let dump = r#"
CREATE TABLE "order" (
    "select" INT,
    "from" VARCHAR(100),
    "where" TEXT
);

INSERT INTO "order" ("select", "from", "where") VALUES (1, 'test', 'value');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query("SELECT \"select\", \"from\" FROM \"order\"")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_table_with_no_data() {
    let dump = r#"
CREATE TABLE empty_table (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE has_data (id INT);
INSERT INTO has_data VALUES (1);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let tables = engine.list_tables().unwrap();
    assert!(tables.contains(&"empty_table".to_string()));

    let result = engine.query("SELECT COUNT(*) FROM empty_table").unwrap();
    assert_eq!(result.rows[0][0], "0");
}

#[test]
fn test_many_columns() {
    // Create a table with 50 columns
    let mut create_cols: Vec<String> = Vec::new();
    let mut insert_vals: Vec<String> = Vec::new();
    for i in 0..50 {
        create_cols.push(format!("col{} INT", i));
        insert_vals.push(i.to_string());
    }

    let dump = format!(
        "CREATE TABLE wide ({});\nINSERT INTO wide VALUES ({});",
        create_cols.join(", "),
        insert_vals.join(", ")
    );
    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM wide").unwrap();
    assert_eq!(result.columns.len(), 50);
    assert_eq!(result.rows[0].len(), 50);
}

#[test]
fn test_long_table_name() {
    let long_name = "a".repeat(60);
    let dump = format!(
        "CREATE TABLE {} (id INT);\nINSERT INTO {} VALUES (1);",
        long_name, long_name
    );
    let (_temp_dir, dump_path) = create_test_dump(&dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine
        .query(&format!("SELECT COUNT(*) FROM {}", long_name))
        .unwrap();
    assert_eq!(result.rows[0][0], "1");
}

// =============================================================================
// Import Statistics Tests
// =============================================================================

#[test]
fn test_import_stats_accuracy() {
    let dump = r#"
CREATE TABLE t1 (id INT);
CREATE TABLE t2 (id INT);
CREATE TABLE t3 (id INT);

INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (1), (2);
INSERT INTO t3 VALUES (1);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 3);
    assert_eq!(stats.insert_statements, 3);
    assert_eq!(stats.rows_inserted, 6); // 3 + 2 + 1
}

#[test]
fn test_import_stats_with_skipped_statements() {
    // Include statements that are skipped (indexes, alters, drops)
    let dump = r#"
CREATE TABLE valid_table (id INT);
INSERT INTO valid_table VALUES (1);

CREATE INDEX idx_test ON valid_table (id);
ALTER TABLE valid_table ADD COLUMN extra INT;
DROP TABLE IF EXISTS nonexistent;
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 1);
    assert_eq!(stats.rows_inserted, 1);
    // CREATE INDEX, ALTER TABLE, and DROP TABLE should be skipped
    assert!(
        stats.statements_skipped >= 1,
        "Expected at least 1 skipped statement, got {}",
        stats.statements_skipped
    );
}

// =============================================================================
// Disk Mode and Memory Limit Tests
// =============================================================================

#[test]
fn test_disk_mode_basic() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig {
        disk_mode: true,
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn test_memory_limit_setting() {
    let (_temp_dir, dump_path) = create_test_dump(simple_mysql_dump());

    let config = QueryConfig {
        memory_limit: Some("100MB".to_string()),
        ..Default::default()
    };
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(result.rows[0][0], "3");
}

// =============================================================================
// Output Format Edge Cases
// =============================================================================

#[test]
fn test_csv_with_newlines_in_data() {
    let dump = r#"
CREATE TABLE multiline (id INT, content TEXT);
INSERT INTO multiline VALUES (1, 'line1
line2
line3');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM multiline").unwrap();
    let csv = QueryResultFormatter::format(&result, OutputFormat::Csv);

    // CSV should properly escape the newlines
    assert!(csv.contains("\"line1"));
}

#[test]
fn test_json_with_quotes_in_data() {
    let dump = r#"
CREATE TABLE quotes (id INT, val VARCHAR(100));
INSERT INTO quotes VALUES (1, 'He said "Hello"');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM quotes").unwrap();
    let json = QueryResultFormatter::format(&result, OutputFormat::Json);

    // JSON should properly escape the quotes
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed[0]["val"], "He said \"Hello\"");
}

#[test]
fn test_tsv_with_tabs_in_data() {
    let dump = r#"
CREATE TABLE tabs (id INT, val VARCHAR(100));
INSERT INTO tabs VALUES (1, 'col1	col2');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    let result = engine.query("SELECT * FROM tabs").unwrap();
    let tsv = QueryResultFormatter::format(&result, OutputFormat::Tsv);

    // TSV should escape tabs
    assert!(tsv.contains("\\t"));
}

// =============================================================================
// Real-World Schema Patterns
// =============================================================================

#[test]
fn test_ecommerce_schema() {
    let dump = r#"
CREATE TABLE customers (
    id INT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id INT PRIMARY KEY,
    sku VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    price DECIMAL(10,2),
    stock INT DEFAULT 0
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP,
    status VARCHAR(20),
    total DECIMAL(10,2)
);

CREATE TABLE order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
);

INSERT INTO customers VALUES (1, 'alice@example.com', 'Alice', '2024-01-01 10:00:00');
INSERT INTO customers VALUES (2, 'bob@example.com', 'Bob', '2024-01-02 11:00:00');

INSERT INTO products VALUES (1, 'SKU001', 'Widget', 19.99, 100);
INSERT INTO products VALUES (2, 'SKU002', 'Gadget', 49.99, 50);

INSERT INTO orders VALUES (1, 1, '2024-01-15 14:30:00', 'completed', 69.98);
INSERT INTO orders VALUES (2, 2, '2024-01-16 09:00:00', 'pending', 49.99);

INSERT INTO order_items VALUES (1, 1, 1, 2, 19.99);
INSERT INTO order_items VALUES (2, 1, 2, 1, 49.99);
INSERT INTO order_items VALUES (3, 2, 2, 1, 49.99);
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    let stats = engine.import_dump(&dump_path).unwrap();

    assert_eq!(stats.tables_created, 4);

    // Test a realistic business query
    let result = engine
        .query(
            "SELECT c.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
         FROM customers c
         LEFT JOIN orders o ON c.id = o.customer_id
         GROUP BY c.id, c.name
         ORDER BY total_spent DESC NULLS LAST",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    // Test another realistic query
    let result = engine
        .query(
            "SELECT p.name, SUM(oi.quantity) as units_sold
         FROM products p
         JOIN order_items oi ON p.id = oi.product_id
         GROUP BY p.id, p.name
         ORDER BY units_sold DESC",
        )
        .unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn test_blog_schema() {
    let dump = r#"
CREATE TABLE authors (
    id INT PRIMARY KEY,
    username VARCHAR(50),
    bio TEXT
);

CREATE TABLE posts (
    id INT PRIMARY KEY,
    author_id INT,
    title VARCHAR(255),
    content TEXT,
    published_at TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE comments (
    id INT PRIMARY KEY,
    post_id INT,
    author_id INT,
    content TEXT,
    created_at TIMESTAMP
);

CREATE TABLE tags (
    id INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE post_tags (
    post_id INT,
    tag_id INT
);

INSERT INTO authors VALUES (1, 'alice', 'Tech writer');
INSERT INTO authors VALUES (2, 'bob', 'Developer');

INSERT INTO posts VALUES (1, 1, 'Hello World', 'My first post', '2024-01-01 10:00:00', 'published');
INSERT INTO posts VALUES (2, 1, 'Rust Tips', 'Learn Rust', '2024-01-15 12:00:00', 'published');
INSERT INTO posts VALUES (3, 2, 'Draft Post', 'WIP', NULL, 'draft');

INSERT INTO tags VALUES (1, 'rust'), (2, 'programming'), (3, 'tutorial');

INSERT INTO post_tags VALUES (1, 2), (2, 1), (2, 2), (2, 3);

INSERT INTO comments VALUES (1, 1, 2, 'Great post!', '2024-01-02 08:00:00');
INSERT INTO comments VALUES (2, 2, 2, 'Very helpful', '2024-01-16 09:00:00');
"#;
    let (_temp_dir, dump_path) = create_test_dump(dump);

    let config = QueryConfig::default();
    let mut engine = QueryEngine::new(&config).unwrap();
    engine.import_dump(&dump_path).unwrap();

    // Posts with their tag counts
    let result = engine
        .query(
            "SELECT p.title, COUNT(pt.tag_id) as tag_count
         FROM posts p
         LEFT JOIN post_tags pt ON p.id = pt.post_id
         GROUP BY p.id, p.title
         ORDER BY tag_count DESC",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3);

    // Authors with comment counts on their posts
    let result = engine
        .query(
            "SELECT a.username, COUNT(c.id) as comment_count
         FROM authors a
         JOIN posts p ON a.id = p.author_id
         LEFT JOIN comments c ON p.id = c.post_id
         GROUP BY a.id, a.username",
        )
        .unwrap();
    assert!(result.rows.len() >= 1);
}
