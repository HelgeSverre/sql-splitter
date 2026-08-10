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
    merge_render_warnings, resolved_model, resolved_model_yaml, CompileOptions, GenerationEngine,
    GenerationPlan, ModelCompiler, RenderOptions, SqlRenderer,
};
use sql_splitter::parser::{detect_dialect, SqlDialect};
use sql_splitter::profile::{
    Confidence, DumpProfile, DumpProfiler, InferenceResult, ModelInference,
};
use sql_splitter::synthetic::OutputMode;
use wasm_bindgen::prelude::*;

/// Fixed profiler seed: profiling evidence should not vary between visits.
const PROFILE_SEED: u64 = 42;

/// Parser buffer for streamed (blob-backed) profiling. Each parser refill is
/// one `read()`; 1MB keeps the per-read overhead negligible against the
/// chunk-cache refills underneath.
const STREAM_PARSER_BUFFER: usize = 1024 * 1024;

/// How much of the blob one JS chunk fetch materializes. This — not the file
/// size — bounds ingestion memory: one chunk resident in wasm, one transient
/// on the JS side while it is copied in.
const CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// Wraps a reader with progress reporting against a known total. Reports are
/// throttled to >= 1% (or 64KB, whichever is larger) steps so the callback
/// overhead stays negligible, with a final `1.0` at EOF.
struct ProgressReader<'a, R> {
    inner: R,
    total: u64,
    pos: u64,
    last_reported: u64,
    min_step: u64,
    done: bool,
    callback: &'a mut dyn FnMut(f64),
}

impl<'a, R: std::io::Read> ProgressReader<'a, R> {
    fn new(inner: R, total: u64, callback: &'a mut dyn FnMut(f64)) -> Self {
        let min_step = (total / 100).max(64 * 1024);
        ProgressReader {
            inner,
            total,
            pos: 0,
            last_reported: 0,
            min_step,
            done: false,
            callback,
        }
    }
}

impl<R: std::io::Read> std::io::Read for ProgressReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pos += n as u64;

        if n == 0 {
            if !self.done {
                self.done = true;
                (self.callback)(1.0);
            }
        } else if self.pos - self.last_reported >= self.min_step {
            self.last_reported = self.pos;
            (self.callback)(self.pos as f64 / self.total.max(1) as f64);
        }
        Ok(n)
    }
}

/// A `Read` over an external random-access source, pulled in aligned chunks.
/// The fetch closure returns the bytes for `[start, end)`; only one chunk is
/// resident at a time. Target-independent so native tests cover it; the JS
/// (`FileReaderSync`) adapter lives in [`PlaygroundSession::from_blob`].
struct ChunkCache<F: FnMut(u64, u64) -> Result<Vec<u8>, String>> {
    fetch: F,
    total: u64,
    pos: u64,
    chunk: Vec<u8>,
    chunk_start: u64,
}

impl<F: FnMut(u64, u64) -> Result<Vec<u8>, String>> ChunkCache<F> {
    fn new(fetch: F, total: u64) -> Self {
        ChunkCache {
            fetch,
            total,
            pos: 0,
            chunk: Vec::new(),
            chunk_start: 0,
        }
    }
}

impl<F: FnMut(u64, u64) -> Result<Vec<u8>, String>> std::io::Read for ChunkCache<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.total {
            return Ok(0);
        }
        let chunk_end = self.chunk_start + self.chunk.len() as u64;
        if self.pos < self.chunk_start || self.pos >= chunk_end {
            let start = self.pos;
            let end = (start + CHUNK_BYTES).min(self.total);
            self.chunk = (self.fetch)(start, end).map_err(std::io::Error::other)?;
            self.chunk_start = start;
            if self.chunk.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "chunk source returned no data at byte {start} of {}",
                        self.total
                    ),
                ));
            }
            if self.chunk.len() as u64 > end - start {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "chunk source returned {} bytes for a {}-byte request",
                        self.chunk.len(),
                        end - start
                    ),
                ));
            }
        }
        let offset = (self.pos - self.chunk_start) as usize;
        let available = &self.chunk[offset..];
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.pos += n as u64;
        Ok(n)
    }
}

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

/// Adapt an optional JS callback into the `FnMut(f64)` the readers take.
fn js_progress(on_progress: &Option<js_sys::Function>) -> impl FnMut(f64) + '_ {
    move |fraction: f64| {
        if let Some(f) = on_progress {
            let _ = f.call1(&JsValue::NULL, &JsValue::from_f64(fraction));
        }
    }
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
    /// sniff the first 8KB. `on_progress`, when given, is called with a
    /// `0.0..=1.0` fraction as the profiler consumes the dump.
    #[wasm_bindgen(constructor)]
    pub fn new(
        dump: &[u8],
        dialect: Option<String>,
        on_progress: Option<js_sys::Function>,
    ) -> Result<PlaygroundSession, JsError> {
        let mut report = js_progress(&on_progress);
        let progress: Option<&mut dyn FnMut(f64)> = if on_progress.is_some() {
            Some(&mut report)
        } else {
            None
        };
        Self::try_new(dump, dialect, progress).map_err(|e| JsError::new(&e))
    }

    /// Profile a dump streamed from a blob-backed chunk source. `read_chunk`
    /// is `(start, end) -> Uint8Array` (the worker backs it with
    /// `FileReaderSync` over `blob.slice`), so ingestion memory is one chunk,
    /// not the file.
    #[wasm_bindgen(js_name = fromBlob)]
    pub fn from_blob(
        read_chunk: js_sys::Function,
        size: f64,
        dialect: Option<String>,
        on_progress: Option<js_sys::Function>,
    ) -> Result<PlaygroundSession, JsError> {
        let total = size as u64;
        let mut fetch = |start: u64, end: u64| -> Result<Vec<u8>, String> {
            let result = read_chunk
                .call2(
                    &JsValue::NULL,
                    &JsValue::from_f64(start as f64),
                    &JsValue::from_f64(end as f64),
                )
                .map_err(|_| "chunk read failed".to_string())?;
            let array = js_sys::Uint8Array::from(result);
            Ok(array.to_vec())
        };

        let (dialect, dialect_detected) = match dialect.filter(|d| !d.is_empty() && d != "auto") {
            Some(name) => (
                name.parse::<SqlDialect>().map_err(|e| JsError::new(&e))?,
                false,
            ),
            None => {
                let header = fetch(0, 8192.min(total)).map_err(|e| JsError::new(&e))?;
                (detect_dialect(&header).dialect, true)
            }
        };

        let mut report = js_progress(&on_progress);
        let reader = ProgressReader::new(ChunkCache::new(&mut fetch, total), total, &mut report);
        let profile = DumpProfiler::builder()
            .seed(PROFILE_SEED)
            .build()
            .profile_reader_sized(reader, dialect, STREAM_PARSER_BUFFER)
            .map_err(|e| JsError::new(&format!("Failed to profile dump: {e}")))?;

        Self::from_profile(dialect, dialect_detected, profile).map_err(|e| JsError::new(&e))
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

    /// The resolved model for the current inference, as JSON:
    /// `{ "yaml": <the exact --emit-config document>, "model": <the same
    /// document as a JSON tree for interactive exploration> }`. Row counts
    /// are frozen, inference disabled, and the seed pinned in both.
    #[wasm_bindgen(js_name = modelDoc)]
    pub fn model_doc(
        &self,
        rows: u32,
        seed: u32,
        dialect: Option<String>,
        mode: Option<String>,
    ) -> Result<String, JsError> {
        self.model_doc_impl(rows, seed, dialect, mode)
            .map_err(|e| JsError::new(&e))
    }
}

// Core logic with plain `String` errors: `JsError` can only be constructed on
// wasm targets, and native `cargo test` exercises these paths directly.
impl PlaygroundSession {
    fn try_new(
        dump: &[u8],
        dialect: Option<String>,
        progress: Option<&mut dyn FnMut(f64)>,
    ) -> Result<PlaygroundSession, String> {
        let (dialect, dialect_detected) = match dialect.filter(|d| !d.is_empty() && d != "auto") {
            Some(name) => (name.parse::<SqlDialect>()?, false),
            None => {
                let header = &dump[..dump.len().min(8192)];
                (detect_dialect(header).dialect, true)
            }
        };

        let profiler = DumpProfiler::builder().seed(PROFILE_SEED).build();
        let profile = match progress {
            Some(callback) => profiler.profile_reader(
                ProgressReader::new(dump, dump.len() as u64, callback),
                dialect,
            ),
            None => profiler.profile_reader(dump, dialect),
        }
        .map_err(|e| format!("Failed to profile dump: {e}"))?;

        Self::from_profile(dialect, dialect_detected, profile)
    }

    fn from_profile(
        dialect: SqlDialect,
        dialect_detected: bool,
        profile: DumpProfile,
    ) -> Result<PlaygroundSession, String> {
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

    fn model_doc_impl(
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

        let yaml =
            resolved_model_yaml(&model, &plan, Some(u64::from(seed))).map_err(|e| e.to_string())?;
        let tree = serde_json::to_value(resolved_model(&model, &plan, Some(u64::from(seed))))
            .map_err(|e| e.to_string())?;

        #[derive(Serialize)]
        struct ModelDoc {
            yaml: String,
            model: serde_json::Value,
        }
        serde_json::to_string(&ModelDoc { yaml, model: tree }).map_err(|e| e.to_string())
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
        let mut session = PlaygroundSession::try_new(&dump, None, None).unwrap();

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
        let mut session = PlaygroundSession::try_new(&dump, None, None).unwrap();

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
    fn model_doc_is_deterministic_and_frozen() {
        let dump = fixture("production_shape.sql");
        let session = PlaygroundSession::try_new(&dump, None, None).unwrap();

        let raw = session.model_doc_impl(50, 7, None, None).unwrap();
        assert_eq!(raw, session.model_doc_impl(50, 7, None, None).unwrap());
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();

        let yaml = doc["yaml"].as_str().unwrap();
        assert!(yaml.contains("kind: model"));
        assert!(yaml.contains("seed: 7"));
        assert!(yaml.contains("inference: disabled"));

        assert_eq!(doc["model"]["kind"], "model");
        assert_eq!(doc["model"]["seed"], 7);
        assert!(doc["model"]["tables"].is_object());
        assert!(doc["model"]["tables"]["users"].is_object());

        let raw_pg = session
            .model_doc_impl(50, 7, Some("postgres".into()), None)
            .unwrap();
        let doc_pg: serde_json::Value = serde_json::from_str(&raw_pg).unwrap();
        assert_eq!(doc_pg["model"]["output"]["dialect"], "postgres");
    }

    #[test]
    fn postgres_fixture_autodetects_and_profiles_copy() {
        let dump = fixture("production_shape_postgres.sql");
        let session = PlaygroundSession::try_new(&dump, None, None).unwrap();

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
        let mut session = PlaygroundSession::try_new(dump, Some("mysql".into()), None).unwrap();

        let summary: serde_json::Value =
            serde_json::from_str(&session.summary_json().unwrap()).unwrap();
        assert_eq!(summary["dialectDetected"], false);
        assert_eq!(summary["tables"][0]["rowCount"], 0);

        let sql = session.generate_sql(10, 1, None, None).unwrap();
        assert!(sql.contains("INSERT INTO"));
    }

    #[test]
    fn chunk_cache_reproduces_source_across_read_sizes() {
        let source: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let mut fetches = 0usize;
        let total = source.len() as u64;
        let mut counting_fetch = |start: u64, end: u64| -> Result<Vec<u8>, String> {
            fetches += 1;
            Ok(source[start as usize..end as usize].to_vec())
        };
        let mut reader = ChunkCache::new(&mut counting_fetch, total);

        use std::io::Read;
        let mut out = Vec::new();
        let mut buf = vec![0u8; 7_919]; // deliberately unaligned read size
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, source);
        drop(reader);
        // 100KB source with 8MB chunks: a single fetch serves everything.
        assert_eq!(fetches, 1);
    }

    #[test]
    fn chunk_cache_reads_across_chunk_boundaries() {
        let source: Vec<u8> = (0..CHUNK_BYTES as usize + 137)
            .map(|i| (i % 251) as u8)
            .collect();
        let total = source.len() as u64;
        let mut fetches = 0usize;
        let mut fetch = |start: u64, end: u64| -> Result<Vec<u8>, String> {
            fetches += 1;
            Ok(source[start as usize..end as usize].to_vec())
        };
        let mut reader = ChunkCache::new(&mut fetch, total);

        use std::io::Read;
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();

        assert_eq!(out, source);
        drop(reader);
        assert_eq!(fetches, 2);
    }

    #[test]
    fn chunk_cache_accepts_short_nonempty_chunks() {
        let source = b"CREATE TABLE t (id INT);";
        let total = source.len() as u64;
        let mut fetch = |start: u64, end: u64| -> Result<Vec<u8>, String> {
            let short_end = (start + 3).min(end);
            Ok(source[start as usize..short_end as usize].to_vec())
        };
        let mut reader = ChunkCache::new(&mut fetch, total);

        use std::io::Read;
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();

        assert_eq!(out, source);
    }

    #[test]
    fn chunk_cache_rejects_empty_chunk_before_total() {
        let mut reader = ChunkCache::new(|_, _| Ok(Vec::new()), 1);
        let error = std::io::Read::read(&mut reader, &mut [0; 1]).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn chunk_cache_propagates_fetch_error_after_data() {
        let mut calls = 0;
        let mut fetch = |_, _| -> Result<Vec<u8>, String> {
            calls += 1;
            if calls == 1 {
                Ok(vec![1, 2, 3])
            } else {
                Err("blob read failed".into())
            }
        };
        let mut reader = ChunkCache::new(&mut fetch, 4);

        use std::io::Read;
        let mut out = Vec::new();
        let error = reader.read_to_end(&mut out).unwrap_err();

        assert_eq!(out, [1, 2, 3]);
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), "blob read failed");
    }

    #[test]
    fn chunk_cache_rejects_data_past_declared_total() {
        let mut reader = ChunkCache::new(|_, _| Ok(vec![1, 2]), 1);
        let error = std::io::Read::read(&mut reader, &mut [0; 2]).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn progress_callback_reports_monotonic_fractions() {
        let dump = fixture("production_shape.sql");
        let mut fractions: Vec<f64> = Vec::new();
        let mut callback = |f: f64| fractions.push(f);
        PlaygroundSession::try_new(&dump, None, Some(&mut callback)).unwrap();

        assert!(!fractions.is_empty());
        assert!(fractions.iter().all(|f| (0.0..=1.0).contains(f)));
        assert!(fractions.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(*fractions.last().unwrap(), 1.0);
    }

    #[test]
    fn garbage_input_errors_without_panic() {
        let result = PlaygroundSession::try_new(&[0xFF, 0xFE, 0x00, 0x42], None, None);
        assert!(result.is_err());
    }

    #[test]
    fn zero_rows_rejected() {
        let dump = b"CREATE TABLE t (id INT PRIMARY KEY);";
        let mut session = PlaygroundSession::try_new(dump, Some("mysql".into()), None).unwrap();
        assert!(session.generate_sql(0, 1, None, None).is_err());
    }
}
