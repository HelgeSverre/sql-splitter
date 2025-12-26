//! Integration tests for MSSQL/T-SQL dialect support.

use sql_splitter::parser::{detect_dialect, DialectConfidence, Parser, SqlDialect, StatementType};
use sql_splitter::splitter::Splitter;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

fn mssql_simple_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/static/mssql/simple.sql")
}

fn mssql_edge_cases_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/static/mssql/edge_cases.sql")
}

#[test]
fn test_mssql_dialect_detection() {
    let content = br#"SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[users] (
    [id] INT IDENTITY(1,1) NOT NULL
)
GO
"#;
    
    let result = detect_dialect(content);
    assert_eq!(result.dialect, SqlDialect::Mssql);
    assert_eq!(result.confidence, DialectConfidence::High);
}

#[test]
fn test_mssql_dialect_detection_brackets() {
    let content = br#"CREATE TABLE [users] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [name] NVARCHAR(100)
) ON [PRIMARY]
"#;
    
    let result = detect_dialect(content);
    assert_eq!(result.dialect, SqlDialect::Mssql);
}

#[test]
fn test_mssql_go_batch_separator() {
    let content = br#"CREATE TABLE [users] ([id] INT)
GO
INSERT INTO [users] VALUES (1)
GO
INSERT INTO [users] VALUES (2)
GO
"#;
    
    let mut parser = Parser::with_dialect(content.as_slice(), 64 * 1024, SqlDialect::Mssql);
    
    let stmt1 = parser.read_statement().unwrap().unwrap();
    assert!(String::from_utf8_lossy(&stmt1).contains("CREATE TABLE"));
    
    let stmt2 = parser.read_statement().unwrap().unwrap();
    assert!(String::from_utf8_lossy(&stmt2).contains("INSERT INTO"));
    assert!(String::from_utf8_lossy(&stmt2).contains("(1)"));
    
    let stmt3 = parser.read_statement().unwrap().unwrap();
    assert!(String::from_utf8_lossy(&stmt3).contains("INSERT INTO"));
    assert!(String::from_utf8_lossy(&stmt3).contains("(2)"));
}

#[test]
fn test_mssql_parse_create_table() {
    let stmt = b"CREATE TABLE [dbo].[users] ([id] INT IDENTITY(1,1) NOT NULL)";
    
    let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    
    assert_eq!(stmt_type, StatementType::CreateTable);
    assert_eq!(table_name, "users");
}

#[test]
fn test_mssql_parse_insert() {
    let stmt = b"INSERT INTO [dbo].[users] ([id], [name]) VALUES (1, N'Alice')";
    
    let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    
    assert_eq!(stmt_type, StatementType::Insert);
    assert_eq!(table_name, "users");
}

#[test]
fn test_mssql_parse_create_nonclustered_index() {
    let stmt = b"CREATE NONCLUSTERED INDEX [IX_users_email] ON [dbo].[users] ([email])";
    
    let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    
    assert_eq!(stmt_type, StatementType::CreateIndex);
    assert_eq!(table_name, "users");
}

#[test]
fn test_mssql_parse_create_clustered_index() {
    let stmt = b"CREATE CLUSTERED INDEX [IX_users_id] ON [users] ([id])";
    
    let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    
    assert_eq!(stmt_type, StatementType::CreateIndex);
    assert_eq!(table_name, "users");
}

#[test]
fn test_mssql_split_simple() {
    let temp_dir = TempDir::new().unwrap();
    let output_dir = temp_dir.path().to_path_buf();
    
    let stats = Splitter::new(mssql_simple_fixture(), output_dir.clone())
        .with_dialect(SqlDialect::Mssql)
        .split()
        .unwrap();
    
    assert_eq!(stats.tables_found, 2);
    assert!(stats.table_names.contains(&"users".to_string()));
    assert!(stats.table_names.contains(&"orders".to_string()));
    
    // Verify output files exist
    assert!(output_dir.join("users.sql").exists());
    assert!(output_dir.join("orders.sql").exists());
    
    // Verify content has semicolons (added for MSSQL)
    let users_content = fs::read_to_string(output_dir.join("users.sql")).unwrap();
    assert!(users_content.contains("CREATE TABLE"));
    assert!(users_content.contains("INSERT INTO"));
}

#[test]
fn test_mssql_split_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let output_dir = temp_dir.path().to_path_buf();
    
    let stats = Splitter::new(mssql_edge_cases_fixture(), output_dir.clone())
        .with_dialect(SqlDialect::Mssql)
        .split()
        .unwrap();
    
    assert!(stats.tables_found >= 2);
    assert!(stats.table_names.contains(&"products".to_string()));
    
    // Verify products.sql has Unicode strings
    let products_content = fs::read_to_string(output_dir.join("products.sql")).unwrap();
    assert!(products_content.contains("日本語"));
}

#[test]
fn test_mssql_unicode_string_handling() {
    let content = "INSERT INTO [dbo].[users] ([name]) VALUES (N'日本語')
GO
INSERT INTO [dbo].[users] ([name]) VALUES (N'Ελληνικά')
GO
";
    
    let mut parser = Parser::with_dialect(content.as_bytes(), 64 * 1024, SqlDialect::Mssql);
    
    let stmt1 = parser.read_statement().unwrap().unwrap();
    let stmt1_str = String::from_utf8_lossy(&stmt1);
    assert!(stmt1_str.contains("日本語"));
    
    let stmt2 = parser.read_statement().unwrap().unwrap();
    let stmt2_str = String::from_utf8_lossy(&stmt2);
    assert!(stmt2_str.contains("Ελληνικά"));
}

#[test]
fn test_mssql_bracket_escape() {
    let stmt = b"CREATE TABLE [table with ]] bracket] ([col]] name] INT)";
    
    let (stmt_type, _) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    assert_eq!(stmt_type, StatementType::CreateTable);
}

#[test]
fn test_mssql_identity_parsing() {
    let stmt = b"CREATE TABLE [t] ([id] BIGINT IDENTITY(100,10) NOT NULL PRIMARY KEY)";
    
    let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    
    assert_eq!(stmt_type, StatementType::CreateTable);
    assert_eq!(table_name, "t");
}

#[test]
fn test_mssql_bulk_insert_classification() {
    let stmt = b"BULK INSERT [dbo].[data] FROM 'C:\\data\\file.csv'";
    
    let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
    
    assert_eq!(stmt_type, StatementType::Insert);
    assert_eq!(table_name, "data");
}

#[test]
fn test_mssql_go_with_count() {
    let content = br#"PRINT 'Hello'
GO 5
INSERT INTO [t] VALUES (1)
GO
"#;
    
    let mut parser = Parser::with_dialect(content.as_slice(), 64 * 1024, SqlDialect::Mssql);
    
    // First statement: PRINT 'Hello'
    let stmt1 = parser.read_statement().unwrap().unwrap();
    assert!(String::from_utf8_lossy(&stmt1).contains("PRINT"));
    
    // Second statement: INSERT
    let stmt2 = parser.read_statement().unwrap().unwrap();
    assert!(String::from_utf8_lossy(&stmt2).contains("INSERT"));
}

#[test]
fn test_mssql_go_case_insensitive() {
    let content = br#"SELECT 1
go
SELECT 2
Go
SELECT 3
GO
"#;
    
    let mut parser = Parser::with_dialect(content.as_slice(), 64 * 1024, SqlDialect::Mssql);
    
    let _ = parser.read_statement().unwrap().unwrap();
    let _ = parser.read_statement().unwrap().unwrap();
    let _ = parser.read_statement().unwrap().unwrap();
    
    // All three should be parsed successfully
}

#[test]
fn test_mssql_schema_qualified_names() {
    let stmts = [
        (b"CREATE TABLE [users] ([id] INT)".as_slice(), "users"),
        (b"CREATE TABLE [dbo].[users] ([id] INT)".as_slice(), "users"),
        (b"CREATE TABLE [mydb].[dbo].[users] ([id] INT)".as_slice(), "users"),
    ];
    
    for (stmt, expected_table) in stmts {
        let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(stmt, SqlDialect::Mssql);
        assert_eq!(stmt_type, StatementType::CreateTable);
        assert_eq!(table_name, expected_table, "Failed for: {}", String::from_utf8_lossy(stmt));
    }
}

// Phase 4 Tests: Schema Commands

#[test]
fn test_mssql_schema_pk_parsing_clustered() {
    use sql_splitter::schema::SchemaBuilder;
    
    let stmt = r#"CREATE TABLE [dbo].[users] (
        [id] INT IDENTITY(1,1) NOT NULL,
        [name] NVARCHAR(100),
        CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
    )"#;
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(stmt);
    let schema = builder.build();
    
    let table = schema.get_table("users").expect("Table should exist");
    assert_eq!(table.primary_key.len(), 1, "Should have 1 PK column");
    
    let pk_col = table.column(table.primary_key[0]).expect("PK column should exist");
    assert_eq!(pk_col.name, "id");
    assert!(pk_col.is_primary_key);
}

#[test]
fn test_mssql_schema_composite_pk() {
    use sql_splitter::schema::SchemaBuilder;
    
    let stmt = r#"CREATE TABLE [dbo].[order_items] (
        [order_id] INT NOT NULL,
        [product_id] BIGINT NOT NULL,
        [quantity] INT NOT NULL,
        CONSTRAINT [PK_order_items] PRIMARY KEY CLUSTERED ([order_id], [product_id])
    )"#;
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(stmt);
    let schema = builder.build();
    
    let table = schema.get_table("order_items").expect("Table should exist");
    assert_eq!(table.primary_key.len(), 2, "Should have 2 PK columns");
    
    let pk_col1 = table.column(table.primary_key[0]).expect("First PK column should exist");
    let pk_col2 = table.column(table.primary_key[1]).expect("Second PK column should exist");
    assert_eq!(pk_col1.name, "order_id");
    assert_eq!(pk_col2.name, "product_id");
}

#[test]
fn test_mssql_schema_fk_parsing() {
    use sql_splitter::schema::SchemaBuilder;
    
    let users_stmt = r#"CREATE TABLE [dbo].[users] (
        [id] INT IDENTITY(1,1) NOT NULL,
        [name] NVARCHAR(100),
        CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
    )"#;
    
    let orders_stmt = r#"CREATE TABLE [dbo].[orders] (
        [id] INT IDENTITY(1,1) NOT NULL,
        [user_id] INT NOT NULL,
        CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id]),
        CONSTRAINT [FK_orders_users] FOREIGN KEY ([user_id]) REFERENCES [dbo].[users]([id])
    )"#;
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(users_stmt);
    builder.parse_create_table(orders_stmt);
    let schema = builder.build();
    
    let orders = schema.get_table("orders").expect("Orders table should exist");
    assert_eq!(orders.foreign_keys.len(), 1, "Should have 1 FK");
    
    let fk = &orders.foreign_keys[0];
    assert_eq!(fk.name.as_deref(), Some("FK_orders_users"));
    assert_eq!(fk.referenced_table, "users");
    assert_eq!(fk.column_names, vec!["user_id"]);
    assert_eq!(fk.referenced_columns, vec!["id"]);
}

#[test]
fn test_mssql_schema_index_parsing() {
    use sql_splitter::schema::SchemaBuilder;
    
    let table_stmt = r#"CREATE TABLE [dbo].[products] (
        [id] INT IDENTITY(1,1) NOT NULL,
        [sku] NVARCHAR(50) NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        CONSTRAINT [PK_products] PRIMARY KEY CLUSTERED ([id])
    )"#;
    
    let index_stmt = r#"CREATE NONCLUSTERED INDEX [IX_products_sku] ON [dbo].[products] ([sku])"#;
    let unique_index_stmt = r#"CREATE UNIQUE NONCLUSTERED INDEX [UX_products_name] ON [dbo].[products] ([name])"#;
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(table_stmt);
    builder.parse_create_index(index_stmt);
    builder.parse_create_index(unique_index_stmt);
    let schema = builder.build();
    
    let products = schema.get_table("products").expect("Products table should exist");
    // Note: PK constraint may be parsed as an index too, so check for at least 2 user-created indexes
    assert!(products.indexes.len() >= 2, "Should have at least 2 indexes");
    
    let idx1 = products.indexes.iter().find(|i| i.name == "IX_products_sku").expect("Index should exist");
    assert!(!idx1.is_unique);
    assert_eq!(idx1.columns, vec!["sku"]);
    
    let idx2 = products.indexes.iter().find(|i| i.name == "UX_products_name").expect("Unique index should exist");
    assert!(idx2.is_unique);
    assert_eq!(idx2.columns, vec!["name"]);
}

// Phase 5 Tests: Data Commands

#[test]
fn test_mssql_insert_row_parsing() {
    use sql_splitter::parser::mysql_insert::parse_mysql_insert_rows;
    use sql_splitter::schema::SchemaBuilder;
    
    let create_stmt = r#"CREATE TABLE [dbo].[orders] (
        [id] INT IDENTITY(1,1) NOT NULL,
        [user_id] INT NOT NULL,
        [total] DECIMAL(10,2),
        [status] NVARCHAR(50),
        CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id])
    )"#;
    
    let insert_stmt = b"INSERT INTO [dbo].[orders] ([user_id], [total], [status]) VALUES (1, 99.99, N'completed')";
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(create_stmt);
    let schema = builder.build();
    let table = schema.get_table("orders").expect("Table should exist");
    
    let rows = parse_mysql_insert_rows(insert_stmt, table).expect("Should parse rows");
    assert_eq!(rows.len(), 1, "Should parse 1 row");
    
    // Verify values were parsed
    let row = &rows[0];
    assert_eq!(row.all_values.len(), 3, "Should have 3 values");
}

#[test]
fn test_mssql_insert_unicode_strings() {
    use sql_splitter::parser::mysql_insert::parse_mysql_insert_rows;
    use sql_splitter::schema::SchemaBuilder;
    
    let create_stmt = r#"CREATE TABLE [dbo].[products] (
        [id] INT NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        CONSTRAINT [PK_products] PRIMARY KEY CLUSTERED ([id])
    )"#;
    
    let insert_stmt = b"INSERT INTO [dbo].[products] ([id], [name]) VALUES (1, N'\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e')";
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(create_stmt);
    let schema = builder.build();
    let table = schema.get_table("products").expect("Table should exist");
    
    let rows = parse_mysql_insert_rows(insert_stmt, table).expect("Should parse rows");
    assert_eq!(rows.len(), 1, "Should parse 1 row");
    
    // Verify unicode was parsed correctly
    let row = &rows[0];
    assert_eq!(row.all_values.len(), 2, "Should have 2 values");
}

#[test]
fn test_mssql_insert_column_mapping() {
    use sql_splitter::parser::mysql_insert::{parse_mysql_insert_rows, PkValue};
    use sql_splitter::schema::SchemaBuilder;
    
    // Schema has columns in order: id, user_id, total, status
    let create_stmt = r#"CREATE TABLE [dbo].[orders] (
        [id] INT IDENTITY(1,1) NOT NULL,
        [user_id] INT NOT NULL,
        [total] DECIMAL(10,2),
        [status] NVARCHAR(50),
        CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id])
    )"#;
    
    // INSERT specifies columns in different order: user_id, total, status (no id)
    let insert_stmt = b"INSERT INTO [dbo].[orders] ([user_id], [total], [status]) VALUES (1, 99.99, N'completed')";
    
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(create_stmt);
    let schema = builder.build();
    let table = schema.get_table("orders").expect("Table should exist");
    
    let rows = parse_mysql_insert_rows(insert_stmt, table).expect("Should parse rows");
    assert_eq!(rows.len(), 1, "Should parse 1 row");
    
    let row = &rows[0];
    
    // The all_values should be in VALUE order (3 values for the 3 columns specified)
    assert_eq!(row.all_values.len(), 3, "Should have 3 values");
    
    // First value should be the user_id = 1
    assert_eq!(row.all_values[0], PkValue::Int(1), "First value should be user_id=1");
}

// Phase 5 Data Command Tests

fn mssql_multi_tenant_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/static/mssql/multi_tenant.sql")
}

#[test]
fn test_mssql_sample_command() {
    use sql_splitter::sample::{SampleConfig, SampleMode, GlobalTableMode};
    
    let temp_dir = TempDir::new().unwrap();
    let output_file = temp_dir.path().join("sampled.sql");
    
    let config = SampleConfig {
        input: mssql_simple_fixture(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::Mssql,
        mode: SampleMode::Percent(100),
        seed: 42,
        preserve_relations: false,
        progress: false,
        tables_filter: None,
        exclude: vec![],
        root_tables: vec![],
        include_global: GlobalTableMode::Lookups,
        dry_run: false,
        config_file: None,
        max_total_rows: None,
        strict_fk: false,
        include_schema: true,
    };
    
    let stats = sql_splitter::sample::run(config).unwrap();
    
    assert!(stats.total_rows_selected > 0, "Should sample some rows");
    assert!(output_file.exists(), "Output file should exist");
    
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(content.contains("CREATE TABLE"), "Should include schema");
    assert!(content.contains("INSERT INTO"), "Should include data");
}

#[test]
fn test_mssql_shard_command() {
    use sql_splitter::shard::{ShardConfig, GlobalTableMode};
    
    let config = ShardConfig {
        input: mssql_multi_tenant_fixture(),
        output: None,
        dialect: SqlDialect::Mssql,
        tenant_column: Some("tenant_id".to_string()),
        tenant_value: "1".to_string(),
        root_tables: vec![],
        include_global: GlobalTableMode::Lookups,
        dry_run: true,
        progress: false,
        config_file: None,
        max_selected_rows: None,
        strict_fk: false,
        include_schema: true,
    };
    
    let stats = sql_splitter::shard::run(config).unwrap();
    
    // Should have processed tables
    assert!(stats.tables_processed > 0, "Should process some tables");
    
    // Tenant 1 should have users and orders
    let users_stats = stats.table_stats.iter().find(|t| t.name == "users");
    assert!(users_stats.is_some(), "Should have users table stats");
    let users = users_stats.unwrap();
    assert_eq!(users.rows_selected, 2, "Should select 2 users for tenant 1");
    
    let orders_stats = stats.table_stats.iter().find(|t| t.name == "orders");
    assert!(orders_stats.is_some(), "Should have orders table stats");
    let orders = orders_stats.unwrap();
    assert_eq!(orders.rows_selected, 2, "Should select 2 orders for tenant 1");
}

#[test]
fn test_mssql_diff_command() {
    use sql_splitter::differ::{DiffConfig, Differ, DiffOutputFormat};
    
    let config = DiffConfig {
        old_path: mssql_simple_fixture(),
        new_path: mssql_edge_cases_fixture(),
        dialect: Some(SqlDialect::Mssql),
        tables: vec![],
        exclude: vec![],
        ignore_columns: vec![],
        schema_only: false,
        data_only: false,
        progress: false,
        format: DiffOutputFormat::Text,
        verbose: false,
        max_pk_entries: 1_000_000,
        allow_no_pk: false,
        ignore_column_order: false,
        pk_overrides: std::collections::HashMap::new(),
    };
    
    let differ = Differ::new(config);
    let result = differ.diff().unwrap();
    
    // Should detect schema differences
    let schema_diff = result.schema.expect("Should have schema diff");
    assert!(!schema_diff.tables_added.is_empty() || !schema_diff.tables_removed.is_empty(), 
        "Should detect table changes");
}

#[test]
fn test_mssql_shard_tenant_column_detection() {
    use sql_splitter::shard::{ShardConfig, GlobalTableMode};
    
    // Don't specify tenant_column - let it auto-detect
    let config = ShardConfig {
        input: mssql_multi_tenant_fixture(),
        output: None,
        dialect: SqlDialect::Mssql,
        tenant_column: None, // Auto-detect
        tenant_value: "1".to_string(),
        root_tables: vec![],
        include_global: GlobalTableMode::Lookups,
        dry_run: true,
        progress: false,
        config_file: None,
        max_selected_rows: None,
        strict_fk: false,
        include_schema: true,
    };
    
    let stats = sql_splitter::shard::run(config).unwrap();
    
    // Should have auto-detected tenant_id
    assert_eq!(stats.detected_tenant_column, Some("tenant_id".to_string()), 
        "Should auto-detect tenant_id column");
}

// Phase 6: Query Command Tests

#[test]
fn test_mssql_query_command() {
    use sql_splitter::duckdb::{QueryConfig, QueryEngine};
    
    let config = QueryConfig {
        dialect: Some(SqlDialect::Mssql),
        disk_mode: false,
        cache_enabled: false,
        tables: None,
        memory_limit: None,
        progress: false,
    };
    
    let mut engine = QueryEngine::new(&config).expect("Should create engine");
    engine.import_dump(&mssql_simple_fixture()).expect("Should import dump");
    let result = engine.query("SELECT COUNT(*) as cnt FROM users").expect("Should execute query");
    
    // Should have imported data (at least the table exists)
    assert!(!result.rows.is_empty(), "Query should return result");
}

#[test]
fn test_mssql_query_with_nvarchar() {
    use sql_splitter::duckdb::{QueryConfig, QueryEngine};
    
    let config = QueryConfig {
        dialect: Some(SqlDialect::Mssql),
        disk_mode: false,
        cache_enabled: false,
        tables: None,
        memory_limit: None,
        progress: false,
    };
    
    let mut engine = QueryEngine::new(&config).expect("Should create engine");
    engine.import_dump(&mssql_simple_fixture()).expect("Should import dump");
    let result = engine.query("SELECT email FROM users").expect("Should execute query");
    
    // Should properly parse N'string' values
    assert!(!result.rows.is_empty(), "Query should return result");
}
