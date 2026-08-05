//! Browser bindings for the sql-splitter playground.
//!
//! Wraps the existing string-based pipeline — `DumpProfiler::profile_reader` →
//! `ModelInference::infer` → `ModelCompiler::compile` → `GenerationEngine` →
//! `SqlRenderer` — behind a small session API. No generation logic lives here;
//! the browser runs the same code the CLI does.

use serde::Serialize;
use sql_splitter::generate::{
    CompileOptions, GenerationEngine, ModelCompiler, RenderOptions, SqlRenderer,
};
use sql_splitter::parser::{detect_dialect, SqlDialect};
use sql_splitter::profile::{
    Confidence, DumpProfile, DumpProfiler, InferenceResult, ModelInference,
};
use wasm_bindgen::prelude::*;

/// Fixed profiler seed: profiling evidence should not vary between visits.
const PROFILE_SEED: u64 = 42;

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

/// One analyzed dump: profile once, generate any number of times.
#[wasm_bindgen]
pub struct PlaygroundSession {
    dialect: SqlDialect,
    dialect_detected: bool,
    profile: DumpProfile,
    inference: InferenceResult,
}

#[wasm_bindgen]
impl PlaygroundSession {
    /// Profile a dump and infer a generation model from it.
    ///
    /// `dialect` is `"mysql" | "postgres" | "sqlite" | "mssql"`, or `None` to
    /// sniff the first 8KB.
    #[wasm_bindgen(constructor)]
    pub fn new(dump: &[u8], dialect: Option<String>) -> Result<PlaygroundSession, JsError> {
        Self::try_new(dump, dialect).map_err(|e| JsError::new(&e))
    }

    /// The analyze summary as a JSON string (see `Summary` for the shape).
    pub fn summary(&self) -> Result<String, JsError> {
        self.summary_json().map_err(|e| JsError::new(&e))
    }

    /// Render synthetic SQL. `rows` is the row count for root tables (children
    /// derive their counts from relationships); `seed` makes output
    /// deterministic — same dump + rows + seed → identical SQL.
    pub fn generate(&self, rows: u32, seed: u32) -> Result<String, JsError> {
        self.generate_sql(rows, seed).map_err(|e| JsError::new(&e))
    }
}

// Core logic with plain `String` errors: `JsError` can only be constructed on
// wasm targets, and native `cargo test` exercises these paths directly.
impl PlaygroundSession {
    fn try_new(dump: &[u8], dialect: Option<String>) -> Result<PlaygroundSession, String> {
        let (dialect, dialect_detected) = match dialect.filter(|d| !d.is_empty() && d != "auto") {
            Some(name) => (name.parse::<SqlDialect>()?, false),
            None => {
                let header = &dump[..dump.len().min(8192)];
                (detect_dialect(header).dialect, true)
            }
        };

        let profile = DumpProfiler::builder()
            .seed(PROFILE_SEED)
            .build()
            .profile_reader(dump, dialect)
            .map_err(|e| format!("Failed to profile dump: {e}"))?;

        let inference = ModelInference::standard()
            .infer(&profile.schema, &profile)
            .map_err(|e| format!("No tables found — is this a SQL dump? ({e})"))?;

        Ok(PlaygroundSession {
            dialect,
            dialect_detected,
            profile,
            inference,
        })
    }

    fn summary_json(&self) -> Result<String, String> {
        serde_json::to_string(&self.build_summary()).map_err(|e| e.to_string())
    }

    fn generate_sql(&self, rows: u32, seed: u32) -> Result<String, String> {
        if rows == 0 {
            return Err("rows must be at least 1".to_string());
        }

        let plan = ModelCompiler::standard()
            .compile(
                self.inference.model.clone(),
                CompileOptions {
                    seed: Some(u64::from(seed)),
                    rows: Some(u64::from(rows)),
                    ..Default::default()
                },
            )
            .map_err(|bag| bag.to_string())?;

        let mut renderer = SqlRenderer::new(
            Vec::new(),
            RenderOptions {
                dialect: self.dialect,
                source_dialect: Some(self.dialect),
                ..Default::default()
            },
        );
        GenerationEngine::new(plan)
            .run(&mut renderer)
            .map_err(|e| e.to_string())?;
        let bytes = renderer.finish().map_err(|e| e.to_string())?;

        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

// --- Summary shape ----------------------------------------------------------
//
// Deliberately excludes ColumnEvidence::sample_values and top_k: they contain
// real values from the uploaded dump, and the playground's promise is that we
// model data without exposing it.

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    dialect: String,
    dialect_detected: bool,
    tables: Vec<TableSummary>,
    warnings: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TableSummary {
    name: String,
    row_count: Option<u64>,
    foreign_keys: Vec<ForeignKeySummary>,
    columns: Vec<ColumnSummary>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ForeignKeySummary {
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ColumnSummary {
    name: String,
    sql_type: String,
    nullable: bool,
    primary_key: bool,
    null_rate: Option<f64>,
    distinct_estimate: Option<f64>,
    generator: Option<String>,
    semantic: Option<String>,
    confidence: Option<&'static str>,
    reason: Option<String>,
}

fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::Low => "low",
        Confidence::Medium => "medium",
        Confidence::High => "high",
        Confidence::Certain => "certain",
    }
}

impl PlaygroundSession {
    fn build_summary(&self) -> Summary {
        // Evidence order is discovery order; schema-only tables follow.
        let mut names: Vec<&str> = self
            .profile
            .tables
            .iter()
            .map(|t| t.table.as_str())
            .collect();
        for name in self.profile.schema.tables.keys() {
            if !names.iter().any(|n| n == name) {
                names.push(name);
            }
        }

        let tables = names
            .into_iter()
            .filter_map(|n| self.table_summary(n))
            .collect();

        let warnings = self
            .profile
            .warnings
            .iter()
            .chain(self.inference.warnings.iter())
            .map(|d| d.to_string())
            .collect();

        Summary {
            dialect: self.dialect.to_string(),
            dialect_detected: self.dialect_detected,
            tables,
            warnings,
        }
    }

    fn table_summary(&self, name: &str) -> Option<TableSummary> {
        let table = self.profile.schema.tables.get(name)?;
        let evidence = self.profile.tables.iter().find(|t| t.table == name);

        let foreign_keys = table
            .relationships
            .iter()
            .map(|r| ForeignKeySummary {
                columns: r.columns.clone(),
                referenced_table: r.referenced_table.clone(),
                referenced_columns: r.referenced_columns.clone(),
            })
            .collect();

        let columns = table
            .columns
            .iter()
            .map(|col| {
                let col_evidence =
                    evidence.and_then(|t| t.columns.iter().find(|c| c.name == col.name));
                let decision = self.inference.decision(&format!("{}.{}", name, col.name));
                let semantic = self
                    .inference
                    .column_rule(name, &col.name)
                    .and_then(|rule| rule.semantic.clone());

                ColumnSummary {
                    name: col.name.clone(),
                    sql_type: col.source_type.clone(),
                    nullable: col.nullable,
                    primary_key: col.primary_key,
                    null_rate: col_evidence.map(|e| e.null_rate),
                    distinct_estimate: col_evidence.map(|e| e.distinct_estimate),
                    generator: decision.map(|d| d.generator_kind.clone()),
                    semantic,
                    confidence: decision.map(|d| confidence_label(d.confidence)),
                    reason: decision.map(|d| d.reason.clone()),
                }
            })
            .collect();

        Some(TableSummary {
            name: name.to_string(),
            row_count: evidence.and_then(|t| t.row_count),
            foreign_keys,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(format!(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/fixtures/generate/{}"
            ),
            name
        ))
        .unwrap()
    }

    #[test]
    fn mysql_fixture_summary_and_generate() {
        let dump = fixture("production_shape.sql");
        let session = PlaygroundSession::try_new(&dump, None).unwrap();

        let summary: serde_json::Value =
            serde_json::from_str(&session.summary_json().unwrap()).unwrap();
        assert_eq!(summary["dialect"], "mysql");
        assert_eq!(summary["dialectDetected"], true);
        let tables = summary["tables"].as_array().unwrap();
        assert!(!tables.is_empty());
        let users = tables.iter().find(|t| t["name"] == "users").unwrap();
        assert!(users["rowCount"].as_u64().unwrap() > 0);
        let email = users["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "email")
            .unwrap();
        assert!(email["generator"].is_string());

        let sql = session.generate_sql(50, 7).unwrap();
        assert!(sql.contains("INSERT INTO"));
        // Determinism: same inputs, same output
        assert_eq!(sql, session.generate_sql(50, 7).unwrap());
        // Different seed, different output
        assert_ne!(sql, session.generate_sql(50, 8).unwrap());
    }

    #[test]
    fn postgres_fixture_autodetects_and_profiles_copy() {
        let dump = fixture("production_shape_postgres.sql");
        let session = PlaygroundSession::try_new(&dump, None).unwrap();

        let summary: serde_json::Value =
            serde_json::from_str(&session.summary_json().unwrap()).unwrap();
        assert_eq!(summary["dialect"], "postgres");
        let observed = summary["tables"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["rowCount"].as_u64())
            .sum::<u64>();
        assert!(observed > 0, "COPY rows should be profiled");
    }

    #[test]
    fn schema_only_dump_generates() {
        let dump = b"CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(50));";
        let session = PlaygroundSession::try_new(dump, Some("mysql".into())).unwrap();

        let summary: serde_json::Value =
            serde_json::from_str(&session.summary_json().unwrap()).unwrap();
        assert_eq!(summary["dialectDetected"], false);
        assert_eq!(summary["tables"][0]["rowCount"], 0);

        let sql = session.generate_sql(10, 1).unwrap();
        assert!(sql.contains("INSERT INTO"));
    }

    #[test]
    fn garbage_input_errors_without_panic() {
        let result = PlaygroundSession::try_new(&[0xFF, 0xFE, 0x00, 0x42], None);
        assert!(result.is_err());
    }

    #[test]
    fn zero_rows_rejected() {
        let dump = b"CREATE TABLE t (id INT PRIMARY KEY);";
        let session = PlaygroundSession::try_new(dump, Some("mysql".into())).unwrap();
        assert!(session.generate_sql(0, 1).is_err());
    }
}
