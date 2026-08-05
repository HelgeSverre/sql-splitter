//! Browser bindings for the sql-splitter playground.
//!
//! Wraps the existing string-based pipeline — `DumpProfiler::profile_reader` →
//! `ModelInference::infer` → `ModelCompiler::compile` → `GenerationEngine` →
//! `SqlRenderer` — behind a small session API. No generation logic lives here;
//! the browser runs the same code the CLI does.

use serde::Serialize;
use sql_splitter::diagnostic::{
    codes, Diagnostic, DiagnosticBag, DiagnosticCategory, Severity, TypicalSeverity,
};
use sql_splitter::generate::{
    merge_render_warnings, resolved_model_yaml, CompileOptions, GenerationEngine, GenerationPlan,
    ModelCompiler, RenderOptions, SqlRenderer,
};
use sql_splitter::parser::{detect_dialect, SqlDialect};
use sql_splitter::profile::{
    Confidence, DumpProfile, DumpProfiler, InferenceResult, ModelInference,
};
use sql_splitter::synthetic::OutputMode;
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
    render_warnings: Vec<WarningEntry>,
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
    /// deterministic. `dialect` selects the output dialect (default: the
    /// source dialect); `mode` is `schema_and_data | schema_only | data_only`.
    pub fn generate(
        &mut self,
        rows: u32,
        seed: u32,
        dialect: Option<String>,
        mode: Option<String>,
    ) -> Result<String, JsError> {
        self.generate_sql(rows, seed, dialect, mode)
            .map_err(|e| JsError::new(&e))
    }

    /// Warnings raised by the most recent `generate` call (compile
    /// diagnostics + cross-dialect conversion losses), as a JSON array.
    #[wasm_bindgen(js_name = lastRenderWarnings)]
    pub fn last_render_warnings(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.render_warnings).map_err(|e| JsError::new(&e.to_string()))
    }

    /// The resolved `kind: model` YAML document for the current inference —
    /// the same document the CLI emits with `--emit-config`, with row counts
    /// frozen, inference disabled, and the seed pinned.
    #[wasm_bindgen(js_name = modelYaml)]
    pub fn model_yaml(
        &self,
        rows: u32,
        seed: u32,
        dialect: Option<String>,
        mode: Option<String>,
    ) -> Result<String, JsError> {
        self.model_yaml_impl(rows, seed, dialect, mode)
            .map_err(|e| JsError::new(&e))
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
            render_warnings: Vec::new(),
        })
    }

    fn summary_json(&self) -> Result<String, String> {
        serde_json::to_string(&self.build_summary()).map_err(|e| e.to_string())
    }

    fn parse_output_options(
        &self,
        dialect: Option<String>,
        mode: Option<String>,
    ) -> Result<(SqlDialect, OutputMode), String> {
        let dialect = match dialect.filter(|d| !d.is_empty()) {
            Some(name) => name.parse::<SqlDialect>()?,
            None => self.dialect,
        };
        let mode = match mode.as_deref().filter(|m| !m.is_empty()) {
            None | Some("schema_and_data") => OutputMode::SchemaAndData,
            Some("schema_only") => OutputMode::SchemaOnly,
            Some("data_only") => OutputMode::DataOnly,
            Some(other) => {
                return Err(format!(
                    "Unknown output mode: {other}. Valid options: schema_and_data, schema_only, data_only"
                ))
            }
        };
        Ok((dialect, mode))
    }

    fn compile_plan(&self, rows: u32, seed: u32) -> Result<GenerationPlan, String> {
        if rows == 0 {
            return Err("rows must be at least 1".to_string());
        }
        ModelCompiler::standard()
            .compile(
                self.inference.model.clone(),
                CompileOptions {
                    seed: Some(u64::from(seed)),
                    rows: Some(u64::from(rows)),
                    ..Default::default()
                },
            )
            .map_err(|bag| bag.to_string())
    }

    fn generate_sql(
        &mut self,
        rows: u32,
        seed: u32,
        dialect: Option<String>,
        mode: Option<String>,
    ) -> Result<String, String> {
        let (out_dialect, mode) = self.parse_output_options(dialect, mode)?;
        let plan = self.compile_plan(rows, seed)?;
        let plan_diagnostics = plan.diagnostics.clone();

        let mut renderer = SqlRenderer::new(
            Vec::new(),
            RenderOptions {
                dialect: out_dialect,
                source_dialect: Some(self.dialect),
                mode,
                ..Default::default()
            },
        );
        GenerationEngine::new(plan)
            .run(&mut renderer)
            .map_err(|e| e.to_string())?;

        let convert_warnings = renderer.warnings().to_vec();
        let mut bag = DiagnosticBag::default();
        merge_render_warnings(&mut bag, &convert_warnings);
        self.render_warnings = plan_diagnostics
            .iter()
            .chain(bag.diagnostics.iter())
            .map(warning_entry)
            .collect();

        let bytes = renderer.finish().map_err(|e| e.to_string())?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn model_yaml_impl(
        &self,
        rows: u32,
        seed: u32,
        dialect: Option<String>,
        mode: Option<String>,
    ) -> Result<String, String> {
        let (out_dialect, mode) = self.parse_output_options(dialect, mode)?;
        let plan = self.compile_plan(rows, seed)?;

        let mut model = self.inference.model.clone();
        model.output.dialect = Some(out_dialect.to_string());
        model.output.mode = Some(mode);

        resolved_model_yaml(&model, &plan, Some(u64::from(seed))).map_err(|e| e.to_string())
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
    warnings: Vec<WarningEntry>,
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

/// One diagnostic, enriched with its registry definition for the explain UI.
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct WarningEntry {
    code: String,
    severity: Severity,
    path: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    documentation_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    definition: Option<DefinitionEntry>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DefinitionEntry {
    title: &'static str,
    category: DiagnosticCategory,
    typical_severity: TypicalSeverity,
    summary: &'static str,
}

fn warning_entry(diagnostic: &Diagnostic) -> WarningEntry {
    let definition = codes::find(&diagnostic.code).map(|def| DefinitionEntry {
        title: def.title,
        category: def.category,
        typical_severity: def.typical_severity,
        summary: def.summary,
    });
    WarningEntry {
        code: diagnostic.code.clone(),
        severity: diagnostic.severity,
        path: diagnostic.path.clone(),
        message: diagnostic.message.clone(),
        help: diagnostic.help.clone(),
        documentation_url: diagnostic.documentation_url(),
        definition,
    }
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
            .map(warning_entry)
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
        let mut session = PlaygroundSession::try_new(&dump, None).unwrap();

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

        // Warnings are structured entries now
        for warning in summary["warnings"].as_array().unwrap() {
            assert!(warning["code"].is_string());
            assert!(warning["severity"].is_string());
            assert!(warning["message"].is_string());
        }

        let sql = session.generate_sql(50, 7, None, None).unwrap();
        assert!(sql.contains("INSERT INTO"));
        // Determinism: same inputs, same output
        assert_eq!(sql, session.generate_sql(50, 7, None, None).unwrap());
        // Different seed, different output
        assert_ne!(sql, session.generate_sql(50, 8, None, None).unwrap());
    }

    #[test]
    fn generate_cross_dialect_and_modes() {
        let dump = fixture("production_shape.sql");
        let mut session = PlaygroundSession::try_new(&dump, None).unwrap();

        let mssql = session
            .generate_sql(10, 1, Some("mssql".into()), None)
            .unwrap();
        assert!(mssql.contains("GO"));
        let warnings: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&session.render_warnings).unwrap())
                .unwrap();
        assert!(warnings.is_array());

        let schema_only = session
            .generate_sql(10, 1, None, Some("schema_only".into()))
            .unwrap();
        assert!(schema_only.contains("CREATE TABLE"));
        assert!(!schema_only.contains("INSERT INTO"));

        let data_only = session
            .generate_sql(10, 1, None, Some("data_only".into()))
            .unwrap();
        assert!(data_only.contains("INSERT INTO"));
        assert!(!data_only.contains("CREATE TABLE"));

        assert!(session
            .generate_sql(10, 1, Some("oracle".into()), None)
            .is_err());
        assert!(session
            .generate_sql(10, 1, None, Some("bogus".into()))
            .is_err());
    }

    #[test]
    fn model_yaml_is_deterministic_and_frozen() {
        let dump = fixture("production_shape.sql");
        let session = PlaygroundSession::try_new(&dump, None).unwrap();

        let yaml = session.model_yaml_impl(50, 7, None, None).unwrap();
        assert_eq!(yaml, session.model_yaml_impl(50, 7, None, None).unwrap());
        assert!(yaml.contains("kind: model"));
        assert!(yaml.contains("seed: 7"));
        assert!(yaml.contains("inference: disabled"));

        let pg = session
            .model_yaml_impl(50, 7, Some("postgres".into()), None)
            .unwrap();
        assert!(pg.contains("dialect: postgres"));
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
        let mut session = PlaygroundSession::try_new(dump, Some("mysql".into())).unwrap();

        let summary: serde_json::Value =
            serde_json::from_str(&session.summary_json().unwrap()).unwrap();
        assert_eq!(summary["dialectDetected"], false);
        assert_eq!(summary["tables"][0]["rowCount"], 0);

        let sql = session.generate_sql(10, 1, None, None).unwrap();
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
        let mut session = PlaygroundSession::try_new(dump, Some("mysql".into())).unwrap();
        assert!(session.generate_sql(0, 1, None, None).is_err());
    }
}
