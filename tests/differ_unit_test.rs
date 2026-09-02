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
    assert_eq!(
        "text".parse::<DiffOutputFormat>().unwrap(),
        DiffOutputFormat::Text
    );
    assert_eq!(
        "json".parse::<DiffOutputFormat>().unwrap(),
        DiffOutputFormat::Json
    );
    assert_eq!(
        "sql".parse::<DiffOutputFormat>().unwrap(),
        DiffOutputFormat::Sql
    );
}

#[test]
fn test_diff_output_format_case_insensitive() {
    assert_eq!(
        "TEXT".parse::<DiffOutputFormat>().unwrap(),
        DiffOutputFormat::Text
    );
    assert_eq!(
        "Json".parse::<DiffOutputFormat>().unwrap(),
        DiffOutputFormat::Json
    );
    assert_eq!(
        "SQL".parse::<DiffOutputFormat>().unwrap(),
        DiffOutputFormat::Sql
    );
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
        } else if trimmed.to_uppercase().contains("INDEX") {
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
    let new_sql =
        "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE orders (id INT PRIMARY KEY)";
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
    let old_sql =
        "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE legacy (id INT PRIMARY KEY)";
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
    let old_sql =
        "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE orders (id INT PRIMARY KEY)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));\nCREATE TABLE orders (id INT PRIMARY KEY, total DECIMAL)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);

    let config = DiffConfig {
        tables: vec!["users".to_string()],
        ..Default::default()
    };

    let diff = compare_schemas(&old_schema, &new_schema, &config);
    // Only users should be in modified
    assert_eq!(diff.tables_modified.len(), 1);
    assert_eq!(diff.tables_modified[0].table_name, "users");
}

#[test]
fn test_compare_schemas_with_exclude_filter() {
    let old_sql =
        "CREATE TABLE users (id INT PRIMARY KEY);\nCREATE TABLE logs (id INT PRIMARY KEY)";
    let new_sql = "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));\nCREATE TABLE logs (id INT PRIMARY KEY, level TEXT)";
    let old_schema = build_schema(old_sql);
    let new_schema = build_schema(new_sql);

    let config = DiffConfig {
        exclude: vec!["logs".to_string()],
        ..Default::default()
    };

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

    let config = DiffConfig {
        ignore_columns: vec!["*.created_at".to_string()],
        ..Default::default()
    };

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

    let qualified_result = DiffResult {
        schema: Some(SchemaDiff {
            tables_added: vec![],
            tables_removed: vec!["tenant_a.old_table".to_string()],
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
    let qualified_pg = format_diff(
        &qualified_result,
        DiffOutputFormat::Sql,
        SqlDialect::Postgres,
    );
    assert!(qualified_pg.contains("DROP TABLE IF EXISTS \"tenant_a\".\"old_table\""));
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

// =============================================================================
// Differ end-to-end (in-process) — data.rs paths
// =============================================================================

mod data_paths {
    use sql_splitter::differ::{DiffConfig, DiffResult, Differ, TableDataDiff};
    use sql_splitter::parser::SqlDialect;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn write(dir: &TempDir, name: &str, sql: &str) -> PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, sql).unwrap();
        p
    }

    fn run(
        old: &str,
        new: &str,
        dialect: SqlDialect,
        tweak: impl FnOnce(&mut DiffConfig),
    ) -> DiffResult {
        let dir = TempDir::new().unwrap();
        let mut config = DiffConfig {
            old_path: write(&dir, "old.sql", old),
            new_path: write(&dir, "new.sql", new),
            dialect: Some(dialect),
            ..Default::default()
        };
        tweak(&mut config);
        Differ::new(config).diff().unwrap()
    }

    fn table<'a>(r: &'a DiffResult, name: &str) -> &'a TableDataDiff {
        r.data
            .as_ref()
            .expect("data diff")
            .tables
            .get(name)
            .unwrap_or_else(|| panic!("table {name} missing from {:?}", r.data))
    }

    fn counts(t: &TableDataDiff) -> (u64, u64, u64) {
        (t.added_count, t.removed_count, t.modified_count)
    }

    fn warnings(r: &DiffResult) -> Vec<String> {
        r.warnings.iter().map(|w| w.message.clone()).collect()
    }

    const USERS: &str = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));\n";

    #[test]
    fn mysql_insert_counts_and_verbose_samples() {
        let old = format!("{USERS}INSERT INTO users VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');\n");
        let new = format!("{USERS}INSERT INTO users VALUES (1,'Alice2'),(3,'Carol'),(4,'Dan');\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| c.verbose = true);
        let t = table(&r, "users");
        assert_eq!((t.old_row_count, t.new_row_count), (3, 3));
        assert_eq!(counts(t), (1, 1, 1));
        assert_eq!(t.sample_added_pks, vec!["4"]);
        assert_eq!(t.sample_removed_pks, vec!["2"]);
        assert_eq!(t.sample_modified_pks, vec!["1"]);
        assert!(!t.truncated);
        assert_eq!(r.summary.rows_added, 1);
        assert_eq!(r.summary.rows_removed, 1);
        assert_eq!(r.summary.rows_modified, 1);
        assert_eq!(r.summary.tables_modified, 1);
    }

    #[test]
    fn non_verbose_collects_no_samples() {
        let old = format!("{USERS}INSERT INTO users VALUES (1,'a');\n");
        let new = format!("{USERS}INSERT INTO users VALUES (2,'b');\n");
        let r = run(&old, &new, SqlDialect::MySql, |_| {});
        let t = table(&r, "users");
        assert_eq!(counts(t), (1, 1, 0));
        assert!(t.sample_added_pks.is_empty());
        assert!(t.sample_removed_pks.is_empty());
    }

    #[test]
    fn verbose_samples_cap_at_100_and_text_shows_remaining() {
        // Old side must hold at least one row: a table with data on only one
        // side falls into the row-count-only branch and yields no samples.
        let rows: Vec<String> = (1..=150).map(|i| format!("({i},'n{i}')")).collect();
        let old = format!("{USERS}INSERT INTO users VALUES (0,'seed');\n");
        let new = format!(
            "{USERS}INSERT INTO users VALUES (0,'seed'),{};\n",
            rows.join(",")
        );
        let r = run(&old, &new, SqlDialect::MySql, |c| c.verbose = true);
        let t = table(&r, "users");
        assert_eq!(t.added_count, 150);
        assert_eq!(t.sample_added_pks.len(), 100);
        let text = sql_splitter::differ::format_text(&r);
        assert!(text.contains("... (+50 more)"), "{text}");
    }

    #[test]
    fn composite_pk_samples_are_tuples() {
        let schema = "CREATE TABLE m (a INT, b VARCHAR(10), v INT, PRIMARY KEY (a, b));\n";
        let old = format!("{schema}INSERT INTO m VALUES (1,'x',10),(1,'y',20);\n");
        let new = format!("{schema}INSERT INTO m VALUES (1,'x',11),(2,'y',20);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| c.verbose = true);
        let t = table(&r, "m");
        assert_eq!(counts(t), (1, 1, 1));
        assert_eq!(t.sample_modified_pks, vec!["(1, x)"]);
        assert_eq!(t.sample_removed_pks, vec!["(1, y)"]);
        assert_eq!(t.sample_added_pks, vec!["(2, y)"]);
    }

    #[test]
    fn postgres_copy_counts_and_samples() {
        let schema = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));\n";
        let old = format!("{schema}COPY users (id, name) FROM stdin;\n1\tAlice\n2\tBob\n\\.\n");
        let new = format!("{schema}COPY users (id, name) FROM stdin;\n1\tAlice2\n3\tCarol\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |c| c.verbose = true);
        let t = table(&r, "users");
        assert_eq!(counts(t), (1, 1, 1));
        assert_eq!(t.sample_added_pks, vec!["3"]);
        assert_eq!(t.sample_removed_pks, vec!["2"]);
        assert_eq!(t.sample_modified_pks, vec!["1"]);
    }

    #[test]
    fn postgres_copy_reordered_columns_do_not_count_as_modified() {
        let schema = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));\n";
        let old = format!("{schema}COPY users (id, name) FROM stdin;\n1\tAlice\n\\.\n");
        let new = format!("{schema}COPY users (name, id) FROM stdin;\nAlice\t1\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |_| {});
        assert_eq!(counts(table(&r, "users")), (0, 0, 0));
    }

    #[test]
    fn postgres_copy_without_pk_warns_once_and_skips() {
        let schema = "CREATE TABLE logs (msg TEXT);\n";
        let old = format!("{schema}COPY logs (msg) FROM stdin;\na\n\\.\n");
        let new = format!("{schema}COPY logs (msg) FROM stdin;\nb\nc\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |_| {});
        assert_eq!(
            warnings(&r),
            vec!["No primary key, data comparison skipped"],
            "warned once across both files"
        );
        assert_eq!(r.warnings[0].table.as_deref(), Some("logs"));
        assert!(r.data.as_ref().unwrap().tables.is_empty());
    }

    #[test]
    fn postgres_copy_without_pk_allow_no_pk_uses_all_columns() {
        let schema = "CREATE TABLE logs (msg TEXT, lvl TEXT);\n";
        let old = format!("{schema}COPY logs (msg, lvl) FROM stdin;\na\tinfo\nb\twarn\n\\.\n");
        let new = format!("{schema}COPY logs (msg, lvl) FROM stdin;\na\tinfo\nc\twarn\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |c| c.allow_no_pk = true);
        assert!(r.warnings.is_empty());
        assert_eq!(counts(table(&r, "logs")), (1, 1, 0));
    }

    #[test]
    fn mysql_insert_without_pk_warns_once_and_skips() {
        let schema = "CREATE TABLE logs (msg TEXT);\n";
        let old =
            format!("{schema}INSERT INTO logs VALUES ('a');\nINSERT INTO logs VALUES ('b');\n");
        let new = format!("{schema}INSERT INTO logs VALUES ('c');\n");
        let r = run(&old, &new, SqlDialect::MySql, |_| {});
        assert_eq!(
            warnings(&r),
            vec!["No primary key, data comparison skipped"]
        );
        assert!(r.data.as_ref().unwrap().tables.is_empty());
        assert_eq!(r.summary.rows_added, 0);
    }

    #[test]
    fn pk_override_replaces_schema_pk_and_skips_null_keys() {
        // Schema PK is id; override to email. Row 3 has NULL email -> not tracked.
        let schema = "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(50), n INT);\n";
        let old = format!("{schema}INSERT INTO u VALUES (1,'a@x',1),(2,'b@x',1),(3,NULL,1);\n");
        let new = format!("{schema}INSERT INTO u VALUES (9,'a@x',1),(2,'b@x',2),(3,NULL,1);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.verbose = true;
            c.pk_overrides
                .insert("u".to_string(), vec!["EMAIL".to_string()]);
        });
        let t = table(&r, "u");
        // a@x: id changed -> modified (keyed by email, not id). b@x: n changed -> modified.
        assert_eq!(counts(t), (0, 0, 2));
        assert_eq!((t.old_row_count, t.new_row_count), (3, 3));
        let mut samples = t.sample_modified_pks.clone();
        samples.sort();
        assert_eq!(samples, vec!["a@x", "b@x"]);
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn pk_override_on_table_without_pk_enables_comparison() {
        let schema = "CREATE TABLE u (code VARCHAR(10), n INT);\n";
        let old = format!("{schema}INSERT INTO u VALUES ('x',1);\n");
        let new = format!("{schema}INSERT INTO u VALUES ('x',2),('y',1);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.pk_overrides
                .insert("u".to_string(), vec!["code".to_string()]);
        });
        assert!(r.warnings.is_empty());
        assert_eq!(counts(table(&r, "u")), (1, 0, 1));
    }

    #[test]
    fn pk_override_composite_via_copy() {
        let schema = "CREATE TABLE u (a INT, b INT, n INT);\n";
        let old = format!("{schema}COPY u (a, b, n) FROM stdin;\n1\t1\t0\n1\t2\t0\n\\.\n");
        let new = format!("{schema}COPY u (a, b, n) FROM stdin;\n1\t1\t5\n2\t2\t0\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |c| {
            c.verbose = true;
            c.pk_overrides
                .insert("u".to_string(), vec!["a".to_string(), "b".to_string()]);
        });
        let t = table(&r, "u");
        assert_eq!(counts(t), (1, 1, 1));
        assert_eq!(t.sample_modified_pks, vec!["(1, 1)"]);
    }

    #[test]
    fn pk_override_unknown_column_warns_once_insert_and_copy() {
        let mysql_schema = "CREATE TABLE u (id INT PRIMARY KEY, n INT);\n";
        let old =
            format!("{mysql_schema}INSERT INTO u VALUES (1,1);\nINSERT INTO u VALUES (2,1);\n");
        let new = format!("{mysql_schema}INSERT INTO u VALUES (1,1);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.pk_overrides.insert(
                "u".to_string(),
                vec!["id".to_string(), "nope".to_string(), "zip".to_string()],
            );
        });
        assert_eq!(
            warnings(&r),
            vec!["Primary key override column(s) not found: nope, zip"]
        );
        // Valid override columns still key the rows.
        assert_eq!(counts(table(&r, "u")), (0, 1, 0));

        let pg_schema = "CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER);\n";
        let old = format!("{pg_schema}COPY u (id, n) FROM stdin;\n1\t1\n\\.\n");
        let new = format!("{pg_schema}COPY u (id, n) FROM stdin;\n1\t1\n2\t1\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |c| {
            c.pk_overrides
                .insert("u".to_string(), vec!["id".to_string(), "nope".to_string()]);
        });
        assert_eq!(
            warnings(&r),
            vec!["Primary key override column(s) not found: nope"]
        );
        assert_eq!(counts(table(&r, "u")), (1, 0, 0));
    }

    #[test]
    fn pk_override_with_only_unknown_columns_tracks_nothing() {
        let schema = "CREATE TABLE u (id INT PRIMARY KEY, n INT);\n";
        let old = format!("{schema}INSERT INTO u VALUES (1,1);\n");
        let new = format!("{schema}INSERT INTO u VALUES (2,1),(3,1);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.pk_overrides
                .insert("u".to_string(), vec!["nope".to_string()]);
        });
        let t = table(&r, "u");
        // Rows are counted but no key can be extracted, so nothing is diffed.
        assert_eq!((t.old_row_count, t.new_row_count), (1, 2));
        assert_eq!(counts(t), (0, 0, 0));
        assert_eq!(warnings(&r).len(), 1);
    }

    #[test]
    fn ignore_columns_excludes_from_digest() {
        let schema =
            "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(50), updated_at VARCHAR(30));\n";
        let old = format!("{schema}INSERT INTO u VALUES (1,'a','t1'),(2,'b','t1');\n");
        let new = format!("{schema}INSERT INTO u VALUES (1,'a','t2'),(2,'B','t2');\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.ignore_columns = vec!["*.updated_at".to_string()];
        });
        assert!(r.warnings.is_empty());
        assert_eq!(counts(table(&r, "u")), (0, 0, 1));

        // Same input without the ignore pattern flags both rows.
        let r = run(&old, &new, SqlDialect::MySql, |_| {});
        assert_eq!(counts(table(&r, "u")), (0, 0, 2));
    }

    #[test]
    fn ignore_columns_matching_pk_warns() {
        let schema = "CREATE TABLE u (id INT PRIMARY KEY, n INT);\n";
        let old = format!("{schema}INSERT INTO u VALUES (1,1);\n");
        let new = format!("{schema}INSERT INTO u VALUES (1,2);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.ignore_columns = vec!["u.id".to_string()];
        });
        assert_eq!(
            warnings(&r),
            vec!["Ignoring primary key column 'id' may affect diff accuracy"]
        );
        assert_eq!(r.warnings[0].table.as_deref(), Some("u"));
        // PK is still used as the key; only the digest drops the column.
        assert_eq!(counts(table(&r, "u")), (0, 0, 1));
    }

    #[test]
    fn table_only_in_new_file_counts_all_rows_added() {
        let old = "CREATE TABLE a (id INT PRIMARY KEY);\nINSERT INTO a VALUES (1);\n";
        let new = format!(
            "CREATE TABLE a (id INT PRIMARY KEY);\nINSERT INTO a VALUES (1);\n{USERS}INSERT INTO users VALUES (1,'x'),(2,'y');\n"
        );
        let r = run(old, &new, SqlDialect::MySql, |_| {});
        let t = table(&r, "users");
        assert_eq!((t.old_row_count, t.new_row_count), (0, 2));
        assert_eq!(counts(t), (2, 0, 0));
        assert_eq!(counts(table(&r, "a")), (0, 0, 0));
        assert_eq!(r.schema.as_ref().unwrap().tables_added.len(), 1);
    }

    #[test]
    fn table_only_in_old_file_counts_all_rows_removed() {
        let old = format!("{USERS}INSERT INTO users VALUES (1,'x'),(2,'y');\n");
        let new = "CREATE TABLE other (id INT PRIMARY KEY);\n";
        let r = run(&old, new, SqlDialect::MySql, |_| {});
        let t = table(&r, "users");
        assert_eq!((t.old_row_count, t.new_row_count), (2, 0));
        assert_eq!(counts(t), (0, 2, 0));
        assert_eq!(r.summary.rows_removed, 2);
    }

    #[test]
    fn inserts_for_table_missing_from_schema_are_ignored() {
        let old = format!("{USERS}INSERT INTO ghost VALUES (1);\n");
        let new = format!("{USERS}INSERT INTO ghost VALUES (1),(2);\n");
        let r = run(&old, &new, SqlDialect::MySql, |_| {});
        assert!(r.data.as_ref().unwrap().tables.is_empty());
    }

    #[test]
    fn table_and_exclude_filters_apply_to_data() {
        let schema = "CREATE TABLE a (id INT PRIMARY KEY);\nCREATE TABLE b (id INT PRIMARY KEY);\n";
        let old = format!("{schema}INSERT INTO a VALUES (1);\nINSERT INTO b VALUES (1);\n");
        let new = format!("{schema}INSERT INTO a VALUES (1),(2);\nINSERT INTO b VALUES (1),(2);\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.tables = vec!["A".to_string()]
        });
        let tables = &r.data.as_ref().unwrap().tables;
        assert!(
            tables.contains_key("a") && !tables.contains_key("b"),
            "{tables:?}"
        );

        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.exclude = vec!["a".to_string()]
        });
        let tables = &r.data.as_ref().unwrap().tables;
        assert!(
            !tables.contains_key("a") && tables.contains_key("b"),
            "{tables:?}"
        );
    }

    #[test]
    fn postgres_copy_respects_exclude_filter() {
        let schema = "CREATE TABLE a (id INTEGER PRIMARY KEY);\n";
        let old = format!("{schema}COPY a (id) FROM stdin;\n1\n\\.\n");
        let new = format!("{schema}COPY a (id) FROM stdin;\n1\n2\n\\.\n");
        let r = run(&old, &new, SqlDialect::Postgres, |c| {
            c.exclude = vec!["a".to_string()]
        });
        assert!(r.data.as_ref().unwrap().tables.is_empty());
    }

    #[test]
    fn per_table_limit_truncates_and_falls_back_to_row_counts() {
        // max_pk_entries=100 -> per-table limit 50. 60 rows exceeds it.
        let rows: Vec<String> = (1..=60)
            .map(|i| i.to_string())
            .map(|i| format!("({i})"))
            .collect();
        let schema = "CREATE TABLE t (id INT PRIMARY KEY);\n";
        let old = format!("{schema}INSERT INTO t VALUES {};\n", rows.join(","));
        let new = format!(
            "{schema}INSERT INTO t VALUES {},(61),(62);\n",
            rows.join(",")
        );
        let r = run(&old, &new, SqlDialect::MySql, |c| {
            c.max_pk_entries = 100;
            c.verbose = true;
        });
        let t = table(&r, "t");
        assert!(t.truncated);
        assert_eq!((t.old_row_count, t.new_row_count), (60, 62));
        // Truncated tables only report the row-count delta.
        assert_eq!(counts(t), (2, 0, 0));
        assert!(t.sample_added_pks.is_empty());
        assert!(r.summary.truncated);

        // Reverse direction reports removed.
        let r = run(&new, &old, SqlDialect::MySql, |c| c.max_pk_entries = 100);
        assert_eq!(counts(table(&r, "t")), (0, 2, 0));
    }

    #[test]
    fn global_limit_truncates_every_later_table() {
        // max_pk_entries=8 -> per-table 4, global 8. a and b fill the global
        // budget; c is then tracked purely by row count.
        let schema = "CREATE TABLE a (id INT PRIMARY KEY);\nCREATE TABLE b (id INT PRIMARY KEY);\nCREATE TABLE c (id INT PRIMARY KEY);\n";
        let old = format!(
            "{schema}INSERT INTO a VALUES (1),(2),(3),(4);\nINSERT INTO b VALUES (1),(2),(3),(4);\nINSERT INTO c VALUES (1);\nINSERT INTO c VALUES (2);\n"
        );
        let new = format!(
            "{schema}INSERT INTO a VALUES (1),(2),(3),(4);\nINSERT INTO b VALUES (1),(2),(3),(4);\nINSERT INTO c VALUES (1);\n"
        );
        let r = run(&old, &new, SqlDialect::MySql, |c| c.max_pk_entries = 8);
        let c = table(&r, "c");
        assert!(c.truncated);
        assert_eq!((c.old_row_count, c.new_row_count), (2, 1));
        assert_eq!(counts(c), (0, 1, 0));
        // Once the global flag is set, every table is reported as truncated.
        assert!(table(&r, "a").truncated);
        assert!(r.summary.truncated);
    }

    #[test]
    fn allow_no_pk_via_differ_mysql() {
        let schema = "CREATE TABLE logs (msg TEXT, lvl TEXT);\n";
        let old = format!("{schema}INSERT INTO logs VALUES ('a','i'),('b','w');\n");
        let new = format!("{schema}INSERT INTO logs VALUES ('a','i'),('c','w');\n");
        let r = run(&old, &new, SqlDialect::MySql, |c| c.allow_no_pk = true);
        assert!(r.warnings.is_empty());
        assert_eq!(counts(table(&r, "logs")), (1, 1, 0));
    }

    #[test]
    fn schema_only_skips_data_and_data_only_skips_schema() {
        let old = format!("{USERS}INSERT INTO users VALUES (1,'a');\n");
        let new = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), x INT);\nINSERT INTO users VALUES (1,'a',0),(2,'b',0);\n";
        let r = run(&old, new, SqlDialect::MySql, |c| c.schema_only = true);
        assert!(r.data.is_none());
        assert_eq!(r.schema.as_ref().unwrap().tables_modified.len(), 1);
        assert_eq!(r.summary.rows_added, 0);

        let r = run(&old, new, SqlDialect::MySql, |c| c.data_only = true);
        assert!(r.schema.is_none());
        // Column added in new: old row hashes against 2 columns, new against 3.
        assert_eq!(counts(table(&r, "users")), (1, 0, 1));
        assert_eq!(r.summary.tables_modified, 1);
    }

    #[test]
    fn progress_callback_reaches_total() {
        use std::sync::{Arc, Mutex};
        let dir = TempDir::new().unwrap();
        let old = write(
            &dir,
            "o.sql",
            &format!("{USERS}INSERT INTO users VALUES (1,'a');\n"),
        );
        let new = write(
            &dir,
            "n.sql",
            &format!("{USERS}INSERT INTO users VALUES (2,'b');\n"),
        );
        let total = std::fs::metadata(&old).unwrap().len() + std::fs::metadata(&new).unwrap().len();
        let seen = Arc::new(Mutex::new((0u64, 0u64)));
        let s2 = Arc::clone(&seen);
        let r = Differ::new(DiffConfig {
            old_path: old,
            new_path: new,
            dialect: Some(SqlDialect::MySql),
            ..Default::default()
        })
        .with_progress(move |cur, tot| {
            let mut g = s2.lock().unwrap();
            g.0 = g.0.max(cur);
            g.1 = tot;
        })
        .diff()
        .unwrap();
        assert_eq!(counts(table(&r, "users")), (1, 1, 0));
        let g = seen.lock().unwrap();
        assert_eq!(g.1, total * 2, "schema pass + data pass");
        assert!(g.0 > 0);
    }

    #[test]
    fn missing_file_is_an_error() {
        let err = Differ::new(DiffConfig {
            old_path: PathBuf::from("/nonexistent/old.sql"),
            new_path: PathBuf::from("/nonexistent/new.sql"),
            ..Default::default()
        })
        .diff();
        assert!(err.is_err());
    }
}

// =============================================================================
// compare_schemas — column/PK/FK/index modifications
// =============================================================================

fn modified_table<'a>(diff: &'a SchemaDiff, name: &str) -> &'a TableModification {
    diff.tables_modified
        .iter()
        .find(|m| m.table_name == name)
        .unwrap_or_else(|| panic!("{name} not in tables_modified: {diff:?}"))
}

#[test]
fn test_compare_schemas_column_type_and_nullability_change() {
    let old = build_schema(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b VARCHAR(10), c DECIMAL(10,2), d BOOLEAN, e DATETIME, f UUID, g BIGINT)",
    );
    let new = build_schema(
        "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT NOT NULL, b VARCHAR(10) NOT NULL, c DECIMAL(10,2), d BOOLEAN, e DATETIME, f UUID, g BIGINT)",
    );
    let diff = compare_schemas(&old, &new, &DiffConfig::default());
    let m = modified_table(&diff, "t");
    assert!(m.columns_added.is_empty());
    assert!(m.columns_removed.is_empty());
    assert!(!m.pk_changed);
    assert_eq!(m.columns_modified.len(), 2, "{:?}", m.columns_modified);

    let a = &m.columns_modified[0];
    assert_eq!(a.name, "a");
    assert_eq!(a.old_type.as_deref(), Some("INT"));
    assert_eq!(a.new_type.as_deref(), Some("BIGINT"));
    assert_eq!((a.old_nullable, a.new_nullable), (None, None));

    let b = &m.columns_modified[1];
    assert_eq!(b.name, "b");
    assert_eq!((b.old_type.as_deref(), b.new_type.as_deref()), (None, None));
    assert_eq!((b.old_nullable, b.new_nullable), (Some(true), Some(false)));
}

#[test]
fn test_compare_schemas_pk_change() {
    let old = build_schema("CREATE TABLE t (id INT PRIMARY KEY, code VARCHAR(10))");
    let new = build_schema("CREATE TABLE t (id INT, code VARCHAR(10), PRIMARY KEY (code, id))");
    let diff = compare_schemas(&old, &new, &DiffConfig::default());
    let m = modified_table(&diff, "t");
    assert!(m.pk_changed);
    assert_eq!(m.old_pk.as_deref(), Some(&["id".to_string()][..]));
    assert_eq!(
        m.new_pk.as_deref(),
        Some(&["code".to_string(), "id".to_string()][..])
    );
}

#[test]
fn test_compare_schemas_fk_added_and_removed() {
    let old = build_schema(
        "CREATE TABLE p (id INT PRIMARY KEY); \
         CREATE TABLE c (id INT PRIMARY KEY, p_id INT, q_id INT, \
           CONSTRAINT fk_old FOREIGN KEY (p_id) REFERENCES p(id))",
    );
    let new = build_schema(
        "CREATE TABLE p (id INT PRIMARY KEY); \
         CREATE TABLE c (id INT PRIMARY KEY, p_id INT, q_id INT, \
           CONSTRAINT fk_new FOREIGN KEY (q_id) REFERENCES p(id))",
    );
    let diff = compare_schemas(&old, &new, &DiffConfig::default());
    let m = modified_table(&diff, "c");
    assert_eq!(m.fks_added.len(), 1);
    assert_eq!(m.fks_added[0].name.as_deref(), Some("fk_new"));
    assert_eq!(m.fks_added[0].columns, vec!["q_id"]);
    assert_eq!(m.fks_added[0].referenced_table, "p");
    assert_eq!(m.fks_added[0].referenced_columns, vec!["id"]);
    assert_eq!(m.fks_removed.len(), 1);
    assert_eq!(m.fks_removed[0].name.as_deref(), Some("fk_old"));
    assert_eq!(m.fks_removed[0].columns, vec!["p_id"]);
}

#[test]
fn test_compare_schemas_index_added_and_removed() {
    let old = build_schema(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT); \
         CREATE INDEX ix_a ON t (a)",
    );
    let new = build_schema(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT); \
         CREATE UNIQUE INDEX ix_b ON t (b)",
    );
    let diff = compare_schemas(&old, &new, &DiffConfig::default());
    let m = modified_table(&diff, "t");
    assert_eq!(m.indexes_added.len(), 1, "{:?}", m.indexes_added);
    assert_eq!(m.indexes_added[0].name, "ix_b");
    assert_eq!(m.indexes_added[0].columns, vec!["b"]);
    assert!(m.indexes_added[0].is_unique);
    assert_eq!(m.indexes_removed.len(), 1, "{:?}", m.indexes_removed);
    assert_eq!(m.indexes_removed[0].name, "ix_a");
    assert!(!m.indexes_removed[0].is_unique);
}

#[test]
fn test_compare_schemas_ignore_columns_filters_added_table() {
    let old = build_schema("CREATE TABLE a (id INT PRIMARY KEY)");
    let new = build_schema(
        "CREATE TABLE a (id INT PRIMARY KEY); \
         CREATE TABLE b (id INT PRIMARY KEY, created_at DATETIME, name TEXT)",
    );
    let config = DiffConfig {
        ignore_columns: vec!["*.created_at".to_string()],
        ..Default::default()
    };
    let diff = compare_schemas(&old, &new, &config);
    assert_eq!(diff.tables_added.len(), 1);
    let cols: Vec<&str> = diff.tables_added[0]
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(diff.tables_added[0].primary_key, vec!["id"]);
}
