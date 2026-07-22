//! Integration tests that verify JSON output matches JSON schemas.
//!
//! Each command that supports --json output is tested against its corresponding
//! schema in the schemas/ directory.

use jsonschema::Validator;
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

fn sql_splitter_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

fn create_temp_sql(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(content.as_bytes())
        .expect("Failed to write temp file");
    file.flush().expect("Failed to flush temp file");
    file
}

fn load_schema(name: &str) -> Validator {
    let schema_path = format!("schemas/{}.schema.json", name);
    let schema_str = fs::read_to_string(&schema_path)
        .unwrap_or_else(|_| panic!("Failed to read schema: {}", schema_path));
    let schema: Value = serde_json::from_str(&schema_str).expect("Invalid schema JSON");
    Validator::new(&schema).expect("Failed to compile schema")
}

fn validate_json_output(output: &std::process::Output, schema_name: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Command failed with stderr: {}",
        stderr
    );

    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("Invalid JSON output: {}\nOutput: {}", e, stdout));

    let schema = load_schema(schema_name);
    let result = schema.validate(&json);

    if let Err(error) = result {
        panic!(
            "JSON output doesn't match {} schema:\n  - {}: {}\n\nOutput was:\n{}",
            schema_name,
            error.instance_path(),
            error,
            serde_json::to_string_pretty(&json).unwrap()
        );
    }
}

// =============================================================================
// Analyze Command
// =============================================================================

#[test]
fn test_analyze_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
INSERT INTO orders VALUES (1, 1);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "analyze");
}

#[test]
fn test_analyze_empty_file_matches_schema() {
    let file = create_temp_sql("");

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "analyze");
}

// =============================================================================
// Validate Command
// =============================================================================

#[test]
fn test_validate_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("validate")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "validate");
}

#[test]
fn test_validate_with_issues_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
INSERT INTO orphans VALUES (1, 'test');
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("validate")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&stdout).expect("Invalid JSON");
    let schema = load_schema("validate");

    if let Err(error) = schema.validate(&json) {
        panic!(
            "JSON output doesn't match validate schema:\n  - {}: {}\n\nOutput was:\n{}",
            error.instance_path(),
            error,
            serde_json::to_string_pretty(&json).unwrap()
        );
    }
}

// =============================================================================
// Split Command
// =============================================================================

#[test]
fn test_split_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
INSERT INTO orders VALUES (1, 1);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let output = sql_splitter_bin()
        .arg("split")
        .arg(file.path())
        .arg("--output")
        .arg(output_dir.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "split");
}

#[test]
fn test_split_dry_run_json_matches_schema() {
    let sql = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);";
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let output = sql_splitter_bin()
        .arg("split")
        .arg(file.path())
        .arg("--output")
        .arg(output_dir.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "split");
}

// =============================================================================
// Merge Command
// =============================================================================

#[test]
fn test_merge_json_matches_schema() {
    let split_dir = TempDir::new().expect("Failed to create temp dir");
    fs::write(
        split_dir.path().join("users.sql"),
        "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\n",
    )
    .expect("Failed to write file");
    fs::write(
        split_dir.path().join("orders.sql"),
        "CREATE TABLE orders (id INT);\nINSERT INTO orders VALUES (1);\n",
    )
    .expect("Failed to write file");

    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let merged_path = output_dir.path().join("merged.sql");

    let output = sql_splitter_bin()
        .arg("merge")
        .arg(split_dir.path())
        .arg("--output")
        .arg(&merged_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "merge");
}

#[test]
fn test_merge_dry_run_json_matches_schema() {
    let split_dir = TempDir::new().expect("Failed to create temp dir");
    fs::write(split_dir.path().join("test.sql"), "SELECT 1;\n").expect("Failed to write file");

    let output = sql_splitter_bin()
        .arg("merge")
        .arg(split_dir.path())
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "merge");
}

// =============================================================================
// Sample Command
// =============================================================================

#[test]
fn test_sample_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
INSERT INTO users VALUES (4, 'Dave');
INSERT INTO users VALUES (5, 'Eve');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let sample_path = output_dir.path().join("sample.sql");

    let output = sql_splitter_bin()
        .arg("sample")
        .arg(file.path())
        .arg("--output")
        .arg(&sample_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--percent")
        .arg("50")
        .arg("--seed")
        .arg("12345")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "sample");
}

#[test]
fn test_sample_rows_mode_json_matches_schema() {
    let sql = r#"
CREATE TABLE items (id INT PRIMARY KEY);
INSERT INTO items VALUES (1);
INSERT INTO items VALUES (2);
INSERT INTO items VALUES (3);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let sample_path = output_dir.path().join("sample.sql");

    let output = sql_splitter_bin()
        .arg("sample")
        .arg(file.path())
        .arg("--output")
        .arg(&sample_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--rows")
        .arg("2")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "sample");
}

// =============================================================================
// Convert Command
// =============================================================================

#[test]
fn test_convert_json_matches_schema() {
    let sql = r#"
CREATE TABLE `users` (`id` INT AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255));
INSERT INTO `users` VALUES (1, 'Alice');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let converted_path = output_dir.path().join("converted.sql");

    let output = sql_splitter_bin()
        .arg("convert")
        .arg(file.path())
        .arg("--output")
        .arg(&converted_path)
        .arg("--from")
        .arg("mysql")
        .arg("--to")
        .arg("postgres")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "convert");
}

#[test]
fn test_convert_dry_run_json_matches_schema() {
    let sql = "CREATE TABLE test (id INT);";
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("convert")
        .arg(file.path())
        .arg("--to")
        .arg("postgres")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "convert");
}

// =============================================================================
// Redact Command
// =============================================================================

#[test]
fn test_redact_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255), name VARCHAR(255));
INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO users VALUES (2, 'bob@example.com', 'Bob');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let redacted_path = output_dir.path().join("redacted.sql");

    let output = sql_splitter_bin()
        .arg("redact")
        .arg(file.path())
        .arg("--output")
        .arg(&redacted_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--null")
        .arg("email")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "redact");
}

#[test]
fn test_redact_no_matches_json_matches_schema() {
    let sql = r#"
CREATE TABLE items (id INT PRIMARY KEY, count INT);
INSERT INTO items VALUES (1, 100);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let redacted_path = output_dir.path().join("redacted.sql");

    let output = sql_splitter_bin()
        .arg("redact")
        .arg(file.path())
        .arg("--output")
        .arg(&redacted_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--null")
        .arg("nonexistent")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "redact");
}

// =============================================================================
// Graph Command
// =============================================================================

#[test]
fn test_graph_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("graph")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "graph");
}

#[test]
fn test_graph_no_relationships_json_matches_schema() {
    let sql = r#"
CREATE TABLE standalone (
    id INT PRIMARY KEY,
    data VARCHAR(255)
);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("graph")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "graph");
}

// =============================================================================
// Shard Command
// =============================================================================

#[test]
fn test_shard_json_matches_schema() {
    let sql = r#"
CREATE TABLE tenants (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO tenants VALUES (1, 'Acme');
INSERT INTO tenants VALUES (2, 'Globex');

CREATE TABLE users (id INT PRIMARY KEY, tenant_id INT, name VARCHAR(255));
INSERT INTO users VALUES (1, 1, 'Alice');
INSERT INTO users VALUES (2, 1, 'Bob');
INSERT INTO users VALUES (3, 2, 'Charlie');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let shard_path = output_dir.path().join("shard.sql");

    let output = sql_splitter_bin()
        .arg("shard")
        .arg(file.path())
        .arg("--output")
        .arg(&shard_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--tenant-column")
        .arg("tenant_id")
        .arg("--tenant-value")
        .arg("1")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "shard");
}

#[test]
fn test_shard_dry_run_json_matches_schema() {
    let sql = r#"
CREATE TABLE data (id INT PRIMARY KEY, org_id INT);
INSERT INTO data VALUES (1, 100);
INSERT INTO data VALUES (2, 200);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("shard")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--tenant-column")
        .arg("org_id")
        .arg("--tenant-value")
        .arg("100")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "shard");
}

// =============================================================================
// PostgreSQL Dialect Tests
// =============================================================================

#[test]
fn test_analyze_postgres_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("postgres")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "analyze");
}

#[test]
fn test_graph_postgres_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id)
);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("graph")
        .arg(file.path())
        .arg("--dialect")
        .arg("postgres")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "graph");
}

// =============================================================================
// SQLite Dialect Tests
// =============================================================================

#[test]
fn test_split_sqlite_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let output = sql_splitter_bin()
        .arg("split")
        .arg(file.path())
        .arg("--output")
        .arg(output_dir.path())
        .arg("--dialect")
        .arg("sqlite")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "split");
}

// =============================================================================
// Schema File Validation
// =============================================================================

/// Test that all schema files are valid JSON
#[test]
fn test_all_schema_files_are_valid_json() {
    let schema_files = [
        "analyze", "validate", "split", "merge", "sample", "convert", "redact", "graph", "shard",
    ];

    for name in schema_files {
        let schema_path = format!("schemas/{}.schema.json", name);
        let schema_str = fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path, e));

        let _: Value = serde_json::from_str(&schema_str)
            .unwrap_or_else(|e| panic!("{} contains invalid JSON: {}", schema_path, e));
    }
}

/// Test that all schema files are valid JSON Schema (can be compiled)
#[test]
fn test_all_schema_files_are_valid_json_schema() {
    let schema_files = [
        "analyze", "validate", "split", "merge", "sample", "convert", "redact", "graph", "shard",
    ];

    for name in schema_files {
        let schema_path = format!("schemas/{}.schema.json", name);
        let schema_str = fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path, e));

        let schema: Value = serde_json::from_str(&schema_str)
            .unwrap_or_else(|e| panic!("{} contains invalid JSON: {}", schema_path, e));

        Validator::new(&schema)
            .unwrap_or_else(|e| panic!("{} is not a valid JSON Schema: {}", schema_path, e));
    }
}

/// Test that all schema files have required metadata
#[test]
fn test_all_schema_files_have_metadata() {
    let schema_files = [
        "analyze", "validate", "split", "merge", "sample", "convert", "redact", "graph", "shard",
    ];

    for name in schema_files {
        let schema_path = format!("schemas/{}.schema.json", name);
        let schema_str = fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path, e));

        let schema: Value = serde_json::from_str(&schema_str).unwrap();

        assert!(
            schema.get("$schema").is_some(),
            "{} missing $schema field",
            schema_path
        );
        assert!(
            schema.get("title").is_some(),
            "{} missing title field",
            schema_path
        );
        assert!(
            schema.get("description").is_some(),
            "{} missing description field",
            schema_path
        );
    }
}

// =============================================================================
// generate-config: the `generate` command's YAML model/overrides schema
// =============================================================================
//
// Unlike the schemas above (which validate a command's --json *output*),
// `generate-config.schema.json` validates the `generate` command's YAML
// *input* — every committed `tests/fixtures/generate/**/*.yaml` document
// must satisfy it, and a battery of hand-built invalid documents must not.

/// Every committed generate fixture that is itself a complete, standalone
/// `kind: model` or `kind: overrides` document, and is actually exercised by
/// this repo's test suite (grep the `tests/` tree for its path to confirm
/// before adding a new one to this list).
///
/// `tests/fixtures/generate/stress/**` is deliberately excluded: those seven
/// fixtures are illustrative design artifacts from the synthetic-data-gen
/// planning phase, are not read by any test or the CLI, and — being never
/// runtime-checked — carry real argument-name mistakes (e.g.
/// `weighted_choice` fixtures that pass `values` instead of the actual
/// `choices` key, `unique` modifiers that pass `attempts` instead of
/// `max_attempts`). They are aspirational examples, not validated configs;
/// see the task report for the full list of divergences found.
fn generate_fixture_paths() -> Vec<std::path::PathBuf> {
    let root = std::path::Path::new("tests/fixtures/generate");
    let mut paths: Vec<_> = walk_yaml(root)
        .into_iter()
        .filter(|path| !path.starts_with(root.join("stress")))
        .collect();
    paths.sort();
    paths
}

/// Recursively collect every `.yaml` file under `dir`.
fn walk_yaml(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_yaml(&path));
        } else if path.extension().is_some_and(|ext| ext == "yaml") {
            out.push(path);
        }
    }
    out
}

/// Parse a YAML document into a [`Value`] for schema validation, going
/// through `serde_yaml_ng`'s own `Value` (not `serde_json`) so YAML-only
/// syntax (unquoted `1.0` floats, `relation.children` dotted keys, etc.)
/// parses exactly as the real config loader sees it.
fn yaml_to_json(text: &str) -> Value {
    let yaml_value: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(text).expect("fixture must be valid YAML");
    serde_json::to_value(&yaml_value).expect("YAML value must convert to JSON")
}

#[test]
fn generate_config_schema_validates_every_fixture() {
    let schema = load_schema("generate-config");
    let fixtures = generate_fixture_paths();
    assert!(
        !fixtures.is_empty(),
        "expected at least one generate fixture to validate against"
    );

    for path in fixtures {
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let json = yaml_to_json(&text);

        if let Err(error) = schema.validate(&json) {
            panic!(
                "{} does not satisfy generate-config.schema.json:\n  - {}: {}",
                path.display(),
                error.instance_path(),
                error
            );
        }
    }
}

/// A minimal valid `kind: model` document, used as the positive baseline
/// each negative test mutates one field of.
fn valid_model_yaml() -> &'static str {
    r#"
version: 1
kind: model
seed: 42
tables:
  users:
    rows: { kind: fixed, count: 3 }
    schema:
      name: users
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: email, type: "varchar(255)", nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      email:
        generator: { kind: string, min_length: 5, max_length: 20 }
"#
}

fn valid_overrides_yaml() -> &'static str {
    r#"
version: 1
kind: overrides
tables:
  users:
    rows: { kind: fixed, count: 10 }
"#
}

#[test]
fn generate_config_schema_accepts_minimal_model_and_overrides() {
    let schema = load_schema("generate-config");
    assert!(
        schema.is_valid(&yaml_to_json(valid_model_yaml())),
        "minimal model fixture should validate"
    );
    assert!(
        schema.is_valid(&yaml_to_json(valid_overrides_yaml())),
        "minimal overrides fixture should validate"
    );
}

#[test]
fn generate_config_schema_rejects_unknown_top_level_field() {
    let schema = load_schema("generate-config");
    let yaml = format!("{}\nbogus_top_level_field: true\n", valid_model_yaml());
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unknown top-level field must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_unknown_document_kind() {
    let schema = load_schema("generate-config");
    let yaml = valid_model_yaml().replace("kind: model", "kind: bogus_role");
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unknown document `kind` must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_invalid_seed_type() {
    let schema = load_schema("generate-config");
    let yaml = valid_model_yaml().replace("seed: 42", "seed: not_a_number");
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a non-integer, non-null seed must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_unknown_generator_kind() {
    let schema = load_schema("generate-config");
    let yaml = valid_model_yaml().replace(
        "generator: { kind: sequence, start: 1 }",
        "generator: { kind: not_a_real_generator }",
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unregistered generator kind must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_generator_missing_required_argument() {
    let schema = load_schema("generate-config");
    // `weighted_choice` declares `choices` as a required argument; omitting
    // it must be rejected even though the argument set is otherwise open
    // (see `generate_config_schema`'s doc comment for why arg names aren't
    // closed).
    let yaml = valid_model_yaml().replace(
        "generator: { kind: sequence, start: 1 }",
        "generator: { kind: weighted_choice }",
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a generator missing a required argument must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_invalid_planner_kind() {
    let schema = load_schema("generate-config");
    let yaml = format!(
        "{}\n    planners:\n      - {{ kind: not_a_real_planner }}\n",
        valid_model_yaml()
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unregistered planner kind must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_planner_with_unknown_argument_name() {
    let schema = load_schema("generate-config");
    let yaml = format!(
        "{}\n    planners:\n      - kind: geo.coordinate_pair\n        columns: {{ latitude: email, longitude: email }}\n        bogus: true\n",
        valid_model_yaml()
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a planner with an unknown top-level argument must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_planner_missing_required_argument() {
    let schema = load_schema("generate-config");
    let yaml = format!(
        "{}\n    planners:\n      - {{ kind: geo.coordinate_pair }}\n",
        valid_model_yaml()
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a planner missing its required `columns` argument must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_unique_attempts_typo() {
    let schema = load_schema("generate-config");
    let yaml = valid_model_yaml().replace(
        "generator: { kind: string, min_length: 5, max_length: 20 }",
        "generator: { kind: string, min_length: 5, max_length: 20 }\n        modifiers: [{ kind: unique, attempts: 20 }]",
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "`unique` accepts `max_attempts`; the inert `attempts` typo must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_structurally_invalid_rows_rule() {
    let schema = load_schema("generate-config");
    // `relation.children` requires `parent`/`distribution`; a bare `count`
    // under that `kind` is missing required fields.
    let yaml = valid_model_yaml().replace(
        "rows: { kind: fixed, count: 3 }",
        "rows: { kind: relation.children, count: 3 }",
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a `relation.children` rows rule missing `parent`/`distribution` must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_structurally_invalid_relationship() {
    let schema = load_schema("generate-config");
    // A relationship missing its required `references` block.
    let yaml = format!(
        "{}\n    relationships:\n      - {{ columns: [email] }}\n",
        valid_model_yaml()
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a relationship missing `references` must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_generator_with_unknown_argument_name() {
    let schema = load_schema("generate-config");
    // `sequence` is a plain GeneratorConfig branch (not `unique`, not a
    // planner), so it is closed: a typo'd/unknown argument name must be
    // rejected by `additionalProperties: false`.
    let yaml = valid_model_yaml().replace(
        "generator: { kind: sequence, start: 1 }",
        "generator: { kind: sequence, strt: 1 }",
    );
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unrecognized generator argument name must be rejected on a closed branch"
    );
}

// =============================================================================
// generate-config: `kind: overrides` root rejection coverage
// =============================================================================
//
// The tests above all mutate a `kind: model` document. `kind: overrides`
// uses its own distinct types (RowsOverride/TableOverride/
// ColumnRuleOverride/RootSeedOverride+TableSeedOverride), so a wrong
// `required` list or a missing `additionalProperties: false` there would go
// undetected without dedicated coverage.

#[test]
fn generate_config_schema_accepts_minimal_overrides_document() {
    let schema = load_schema("generate-config");
    assert!(
        schema.is_valid(&yaml_to_json(valid_overrides_yaml())),
        "minimal overrides document should validate"
    );
}

#[test]
fn generate_config_schema_rejects_unknown_top_level_field_in_overrides() {
    let schema = load_schema("generate-config");
    let yaml = format!("{}\nbogus_top_level_field: true\n", valid_overrides_yaml());
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unknown top-level field on an overrides document must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_invalid_document_kind_in_overrides() {
    let schema = load_schema("generate-config");
    let yaml = valid_overrides_yaml().replace("kind: overrides", "kind: bogus_role");
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "an unknown document `kind` on an overrides-shaped document must be rejected"
    );
}

#[test]
fn generate_config_schema_rejects_invalid_seed_type_in_overrides() {
    let schema = load_schema("generate-config");
    let yaml = format!("{}\nseed: not_a_number\n", valid_overrides_yaml());
    assert!(
        !schema.is_valid(&yaml_to_json(&yaml)),
        "a non-integer, non-null seed on an overrides document must be rejected"
    );
}
