//! Unit tests for differ module.
//!
//! Tests cover:
//! - DiffOutputFormat parsing
//! - DiffConfig defaults
//! - should_include_table filtering
//! - SchemaDiff.has_changes / TableModification.has_changes
//! - compare_schemas with various schema changes
//! - format_diff output formatters
//! - DiffSummary via Differ.build_summary (via end-to-end)

use sql_splitter::differ::{
    compare_schemas, format_diff, should_include_table, DiffConfig, DiffOutputFormat, DiffResult,
    DiffSummary, SchemaDiff, TableInfo, TableModification,
};
use sql_splitter::parser::SqlDialect;
use sql_splitter::schema::SchemaBuilder;

// =============================================================================
// DiffOutputFormat
// =============================================================================

#[test]
fn test_diff_output_format_from_str() {
    assert_eq!("text".parse::<DiffOutputFormat>().unwrap(), DiffOutputFormat::Text);
    assert_eq!("json".parse::<DiffOutputFormat>().unwrap(), DiffOutputFormat::Json);
    assert_eq!("sql".parse::<DiffOutputFormat>().unwrap(), DiffOutputFormat::Sql);
}

#[test]
fn test_diff_output_format_case_insensitive() {
    assert_eq!("TEXT".parse::<DiffOutputFormat>().unwrap(), DiffOutputFormat::Text);
    assert_eq!("Json".parse::<DiffOutputFormat>().unwrap(), DiffOutputFormat::Json);
    assert_eq!("SQL".parse::<DiffOutputFormat>().unwrap(), DiffOutputFormat::Sql);
}

#[test]
fn test_diff_output_format_invalid() {
    let result = "xml".parse::<DiffOutputFormat>();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unknown format"));
}

#[test]
fn test_diff_output_format_default() {
    assert_eq!(DiffOutputFormat::default(), DiffOutputFormat::Text);
}

// =============================================================================
// DiffConfig defaults
// =============================================================================

#[test]
fn test_diff_config_defaults() {
    let config = DiffConfig::default();
    assert!(config.old_path.as_os_str().is_empty());
    assert!(config.new_path.as_os_str().is_empty());
    assert!(config.dialect.is_none());
    assert!(!config.schema_only);
    assert!(!config.data_only);
    assert!(config.tables.is_empty());
    assert!(config.exclude.is_empty());
    assert_eq!(config.format, DiffOutputFormat::Text);
    assert!(!config.verbose);
    assert!(!config.progress);
    assert_eq!(config.max_pk_entries, 10_000_000);
    assert!(!config.allow_no_pk);
    assert!(!config.ignore_column_order);
    assert!(config.pk_overrides.is_empty());
    assert!(config.ignore_columns.is_empty());
}

// =============================================================================
// should_include_table
// =============================================================================

#[test]
fn test_should_include_table_no_filters() {
    assert!(should_include_table("users", &[], &[]));
    assert!(should_include_table("orders", &[], &[]));
}

#[test]
fn test_should_include_table_include_list() {
    let include = vec!["users".to_string(), "orders".to_string()];
    assert!(should_include_table("users", &include, &[]));
    assert!(should_include_table("orders", &include, &[]));
    assert!(!should_include_table("products", &include, &[]));
}

#[test]
fn test_should_include_table_include_case_insensitive() {
    let include = vec!["Users".to_string()];
    assert!(should_include_table("users", &include, &[]));
    assert!(should_include_table("USERS", &include, &[]));
}

#[test]
fn test_should_include_table_exclude_list() {
    let exclude = vec!["logs".to_string()];
    assert!(should_include_table("users", &[], &exclude));
    assert!(!should_include_table("logs", &[], &exclude));
}

#[test]
fn test_should_include_table_exclude_case_insensitive() {
    let exclude = vec!["Logs".to_string()];
    assert!(!should_include_table("logs", &[], &exclude));
    assert!(!should_include_table("LOGS", &[], &exclude));
}

#[test]
fn test_should_include_table_include_and_exclude() {
    let include = vec!["users".to_string(), "logs".to_string()];
    let exclude = vec!["logs".to_string()];
    // "users" is in include and not in exclude
    assert!(should_include_table("users", &include, &exclude));
    // "logs" is in include but also in exclude -> excluded
    assert!(!should_include_table("logs", &include, &exclude));
    // "products" is not in include
    assert!(!should_include_table("products", &include, &exclude));
}

// =============================================================================
// SchemaDiff.has_changes
// =============================================================================

#[test]
fn test_schema_diff_has_changes_empty() {
    let diff = SchemaDiff {
        tables_added: vec![],
        tables_removed: vec![],
        tables_modified: vec![],
    };
    assert!(!diff.has_changes());
}

#[test]
fn test_schema_diff_has_changes_with_added() {
    let diff = SchemaDiff {
        tables_added: vec![TableInfo {
            name: "users".to_string(),
            columns: vec![],
            primary_key: vec![],
            create_statement: None,
        }],
        tables_removed: vec![],
        tables_modified: vec![],
    };
    assert!(diff.has_changes());
}

#[test]
fn test_schema_diff_has_changes_with_removed() {
    let diff = SchemaDiff {
        tables_added: vec![],
        tables_removed: vec!["old_table".to_string()],
        tables_modified: vec![],
    };
    assert!(diff.has_changes());
}

// =============================================================================
// TableModification.has_changes
// =============================================================================

fn empty_modification(table_name: &str) -> TableModification {
    TableModification {
        table_name: table_name.to_string(),
        columns_added: vec![],
        columns_removed: vec![],
        columns_modified: vec![],
        pk_changed: false,
        old_pk: None,
        new_pk: None,
        fks_added: vec![],
        fks_removed: vec![],
        indexes_added: vec![],
        indexes_removed: vec![],
    }
}

#[test]
fn test_table_modification_has_changes_empty() {
    let m = empty_modification("users");
    assert!(!m.has_changes());
}

#[test]
fn test_table_modification_has_changes_pk_changed() {
    let mut m = empty_modification("users");
    m.pk_changed = true;
    m.old_pk = Some(vec!["id".to_string()]);
    m.new_pk = Some(vec!["uuid".to_string()]);
    assert!(m.has_changes());
}

// =============================================================================
// compare_schemas
// =============================================================================

fn build_schema(sql: &str) -> sql_splitter::schema::Schema {
    let mut builder = SchemaBuilder::new();
    // Parse each statement separated by semicolons
    for stmt in sql.split(';') {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.to_uppercase().contains("CREATE TABLE") {
            builder.parse_create_table(trimmed);
        } else if trimmed.to_uppercase().contains("ALTER TABLE") {
            builder.parse_alter_table(trimmed);
        } else if trimmed.to_uppercase().contains("CREATE INDEX") {
            builder.parse_create_index(trimmed);
        }
    }
    builder.build()
}

#[test]
fn test_compare_schemas_identical() {
    let sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
    let old_schema = build_schema(sql);
    let new_schema = build_schema(sql);
    let config = DiffConfig::default();

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    assert!(!diff.has_changes());
}

#[test]
fn test_compare_schemas_table_added() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE orders (id INT PRIMARY KEY)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);
    let config = DiffConfig::default();

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    assert!(diff.has_changes());
    assert_eq!(diff.tables_added.len(), 1);
    assert_eq!(diff.tables_added[0].name, "orders");
    assert!(diff.tables_removed.is_empty());
}

#[test]
fn test_compare_schemas_table_removed() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE legacy (id INT PRIMARY KEY)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);
    let config = DiffConfig::default();

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    assert!(diff.has_changes());
    assert!(diff.tables_added.is_empty());
    assert_eq!(diff.tables_removed.len(), 1);
    assert_eq!(diff.tables_removed[0], "legacy");
}

#[test]
fn test_compare_schemas_column_added() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(255))";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);
    let config = DiffConfig::default();

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    assert!(diff.has_changes());
    assert_eq!(diff.tables_modified.len(), 1);
    assert_eq!(diff.tables_modified[0].columns_added.len(), 1);
    assert_eq!(diff.tables_modified[0].columns_added[0].name, "email");
}

#[test]
fn test_compare_schemas_column_removed() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), bio TEXT)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);
    let config = DiffConfig::default();

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    assert!(diff.has_changes());
    assert_eq!(diff.tables_modified.len(), 1);
    assert_eq!(diff.tables_modified[0].columns_removed.len(), 1);
    assert_eq!(diff.tables_modified[0].columns_removed[0].name, "bio");
}

#[test]
fn test_compare_schemas_with_table_filter() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE orders (id INT PRIMARY KEY)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));\nCREATE TABLE orders (id INT PRIMARY KEY, total DECIMAL)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);

    let mut config = DiffConfig::default();
    config.tables = vec!["users".to_string()];

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    // Only users should be in modified
    assert_eq!(diff.tables_modified.len(), 1);
    assert_eq!(diff.tables_modified[0].table_name, "users");
}

#[test]
fn test_compare_schemas_with_exclude_filter() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE logs (id INT PRIMARY KEY)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));\nCREATE TABLE logs (id INT PRIMARY KEY, level TEXT)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);

    let mut config = DiffConfig::default();
    config.exclude = vec!["logs".to_string()];

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    assert_eq!(diff.tables_modified.len(), 1);
    assert_eq!(diff.tables_modified[0].table_name, "users");
}

#[test]
fn test_compare_schemas_with_ignore_columns() {
    let old_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), updated_at DATETIME)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), updated_at DATETIME, created_at DATETIME)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);

    let mut config = DiffConfig::default();
    config.ignore_columns = vec!["*.created_at".to_string()];

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    // created_at should be filtered from added columns
    if diff.has_changes() {
        for m in &diff.tables_modified {
            for col in &m.columns_added {
                assert_ne!(col.name.to_lowercase(), "created_at");
            }
        }
    }
}

// =============================================================================
// format_diff output formatters
// =============================================================================

fn make_diff_result(schema: Option<SchemaDiff>) -> DiffResult {
    DiffResult {
        schema,
        data: None,
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 0,
            tables_modified: 0,
            rows_added: 0,
            rows_removed: 0,
            rows_modified: 0,
            truncated: false,
        },
    }
}

#[test]
fn test_format_diff_text_no_changes() {
    let result = make_diff_result(Some(SchemaDiff {
        tables_added: vec![],
        tables_removed: vec![],
        tables_modified: vec![],
    }));

    let output = format_diff(&result, DiffOutputFormat::Text, SqlDialect::MySql);
    assert!(output.contains("no schema changes"));
    assert!(output.contains("Summary:"));
}

#[test]
fn test_format_diff_json_valid() {
    let result = make_diff_result(None);
    let output = format_diff(&result, DiffOutputFormat::Json, SqlDialect::MySql);
    let parsed: serde_json::Value = serde_json::from_str(&output).expect("Should be valid JSON");
    assert!(parsed.get("summary").is_some());
}

#[test]
fn test_format_diff_sql_no_schema() {
    let result = make_diff_result(None);
    let output = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::MySql);
    assert!(output.contains("SQL Migration Script"));
    assert!(output.contains("No schema changes detected"));
}

#[test]
fn test_format_diff_sql_with_added_table() {
    let result = DiffResult {
        schema: Some(SchemaDiff {
            tables_added: vec![TableInfo {
                name: "products".to_string(),
                columns: vec![],
                primary_key: vec![],
                create_statement: Some("CREATE TABLE products (id INT PRIMARY KEY);".to_string()),
            }],
            tables_removed: vec![],
            tables_modified: vec![],
        }),
        data: None,
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 1,
            tables_removed: 0,
            tables_modified: 0,
            rows_added: 0,
            rows_removed: 0,
            rows_modified: 0,
            truncated: false,
        },
    };

    let output = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::MySql);
    assert!(output.contains("New table: products"));
    assert!(output.contains("CREATE TABLE products"));
}

#[test]
fn test_format_diff_sql_with_removed_table() {
    let result = DiffResult {
        schema: Some(SchemaDiff {
            tables_added: vec![],
            tables_removed: vec!["old_table".to_string()],
            tables_modified: vec![],
        }),
        data: None,
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 1,
            tables_modified: 0,
            rows_added: 0,
            rows_removed: 0,
            rows_modified: 0,
            truncated: false,
        },
    };

    let mysql_output = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::MySql);
    assert!(mysql_output.contains("DROP TABLE IF EXISTS `old_table`"));

    let pg_output = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::Postgres);
    assert!(pg_output.contains("DROP TABLE IF EXISTS \"old_table\""));
}

#[test]
fn test_format_diff_sql_dialect_quoting() {
    let result = DiffResult {
        schema: Some(SchemaDiff {
            tables_added: vec![],
            tables_removed: vec!["test".to_string()],
            tables_modified: vec![],
        }),
        data: None,
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 1,
            tables_modified: 0,
            rows_added: 0,
            rows_removed: 0,
            rows_modified: 0,
            truncated: false,
        },
    };

    // MySQL uses backticks
    let mysql = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::MySql);
    assert!(mysql.contains("`test`"));

    // Postgres uses double quotes
    let pg = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::Postgres);
    assert!(pg.contains("\"test\""));

    // SQLite uses double quotes
    let sqlite = format_diff(&result, DiffOutputFormat::Sql, SqlDialect::Sqlite);
    assert!(sqlite.contains("\"test\""));
}

// =============================================================================
// Text formatter with data changes
// =============================================================================

#[test]
fn test_format_text_with_data_changes() {
    use sql_splitter::differ::{DataDiff, TableDataDiff};
    use std::collections::HashMap;

    let mut tables = HashMap::new();
    tables.insert(
        "users".to_string(),
        TableDataDiff {
            old_row_count: 10,
            new_row_count: 12,
            added_count: 3,
            removed_count: 1,
            modified_count: 2,
            truncated: false,
            sample_added_pks: vec![],
            sample_removed_pks: vec![],
            sample_modified_pks: vec![],
        },
    );

    let result = DiffResult {
        schema: None,
        data: Some(DataDiff { tables }),
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 0,
            tables_modified: 1,
            rows_added: 3,
            rows_removed: 1,
            rows_modified: 2,
            truncated: false,
        },
    };

    let output = format_diff(&result, DiffOutputFormat::Text, SqlDialect::MySql);
    assert!(output.contains("+3 rows"));
    assert!(output.contains("-1 rows"));
    assert!(output.contains("~2 modified"));
}

#[test]
fn test_format_text_truncated_table() {
    use sql_splitter::differ::{DataDiff, TableDataDiff};
    use std::collections::HashMap;

    let mut tables = HashMap::new();
    tables.insert(
        "big_table".to_string(),
        TableDataDiff {
            old_row_count: 1000,
            new_row_count: 1100,
            added_count: 100,
            removed_count: 0,
            modified_count: 0,
            truncated: true,
            sample_added_pks: vec![],
            sample_removed_pks: vec![],
            sample_modified_pks: vec![],
        },
    );

    let result = DiffResult {
        schema: None,
        data: Some(DataDiff { tables }),
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 0,
            tables_modified: 1,
            rows_added: 100,
            rows_removed: 0,
            rows_modified: 0,
            truncated: true,
        },
    };

    let output = format_diff(&result, DiffOutputFormat::Text, SqlDialect::MySql);
    assert!(output.contains("[truncated]"));
    assert!(output.contains("truncated due to memory limits"));
}

#[test]
fn test_format_text_with_warnings() {
    use sql_splitter::differ::DiffWarning;

    let result = DiffResult {
        schema: None,
        data: None,
        warnings: vec![
            DiffWarning {
                table: Some("logs".to_string()),
                message: "No primary key defined".to_string(),
            },
            DiffWarning {
                table: None,
                message: "Global warning".to_string(),
            },
        ],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 0,
            tables_modified: 0,
            rows_added: 0,
            rows_removed: 0,
            rows_modified: 0,
            truncated: false,
        },
    };

    let output = format_diff(&result, DiffOutputFormat::Text, SqlDialect::MySql);
    assert!(output.contains("Table 'logs': No primary key defined"));
    assert!(output.contains("Global warning"));
}

#[test]
fn test_format_text_verbose_sample_pks() {
    use sql_splitter::differ::{DataDiff, TableDataDiff};
    use std::collections::HashMap;

    let mut tables = HashMap::new();
    tables.insert(
        "users".to_string(),
        TableDataDiff {
            old_row_count: 5,
            new_row_count: 7,
            added_count: 3,
            removed_count: 1,
            modified_count: 1,
            truncated: false,
            sample_added_pks: vec!["10".to_string(), "11".to_string()],
            sample_removed_pks: vec!["3".to_string()],
            sample_modified_pks: vec!["1".to_string()],
        },
    );

    let result = DiffResult {
        schema: None,
        data: Some(DataDiff { tables }),
        warnings: vec![],
        summary: DiffSummary {
            tables_added: 0,
            tables_removed: 0,
            tables_modified: 1,
            rows_added: 3,
            rows_removed: 1,
            rows_modified: 1,
            truncated: false,
        },
    };

    let output = format_diff(&result, DiffOutputFormat::Text, SqlDialect::MySql);
    assert!(output.contains("Added PKs: 10, 11"));
    assert!(output.contains("(+1 more)"));
    assert!(output.contains("Removed PKs: 3"));
    assert!(output.contains("Modified PKs: 1"));
}
