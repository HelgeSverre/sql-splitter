//! Synthetic data generation engine.
//!
//! This module hosts the pieces generators are built from: stable, seedable
//! RNG streams ([`seed`]) so a run is fully reproducible from a single root
//! seed, and a dialect-agnostic value representation ([`value`]) that
//! generators produce instead of writing SQL literals directly.
//!
//! # The public builder
//!
//! [`Generate`] is the one-call facade: it loads a `kind: model` document,
//! compiles it, runs the generation engine, and renders SQL to a file — the
//! complete pipeline behind a single [`GenerateBuilder`].
//!
//! ```
//! use sql_splitter::generate::Generate;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let output = dir.path().join("synthetic.sql");
//!
//! let report = Generate::builder()
//!     .config("tests/fixtures/generate/simple.yaml")
//!     .output(&output)
//!     .seed(42)
//!     .run()?;
//!
//! assert!(report.rows_written > 0);
//! assert!(std::fs::read_to_string(output)?.contains("INSERT INTO"));
//! # Ok(())
//! # }
//! ```
//!
//! # The staged API
//!
//! Callers that need to drive an individual stage directly — a custom
//! statically linked [`ExtensionRegistry`], mid-pipeline inspection, or a
//! destination that is not a plain file — can assemble the same four stages
//! [`Generate`] wires together: an [`ExtensionRegistry`] feeds a
//! [`ModelCompiler`], which produces a [`GenerationPlan`] that a
//! [`GenerationEngine`] drives against any [`RowSink`], most commonly a
//! [`crate::render::SqlRenderer`].
//!
//! (Profiling a model from a dump — [`ExtensionRegistry`] feeding a
//! `DumpProfiler`/`ModelInference` stage ahead of the compiler — lands in
//! Phase 2.)
//!
//! ```
//! use sql_splitter::generate::{CompileOptions, ExtensionRegistry, GenerationEngine, ModelCompiler};
//! use sql_splitter::render::{RenderOptions, SqlRenderer};
//! use sql_splitter::synthetic::SyntheticFile;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let model = SyntheticFile::parse_str(r#"
//! version: 1
//! kind: model
//! defaults: { inference: disabled }
//! seed: 1
//! tables:
//!   users:
//!     rows: { kind: fixed, count: 3 }
//!     schema:
//!       name: users
//!       primary_key: [id]
//!       columns:
//!         - { name: id, type: bigint, nullable: false, primary_key: true }
//!         - { name: name, type: "varchar(50)", nullable: false }
//!     columns:
//!       id:
//!         generator: { kind: sequence, start: 1 }
//!       name:
//!         generator: { kind: string, min_length: 3, max_length: 8 }
//! "#)?
//! .into_model()?;
//!
//! let registry = ExtensionRegistry::standard();
//! let plan = ModelCompiler::new(registry).compile(model, CompileOptions::default())?;
//!
//! let mut output = Vec::new();
//! let mut renderer = SqlRenderer::new(&mut output, RenderOptions::default());
//! GenerationEngine::new(plan).run(&mut renderer)?;
//! renderer.finish()?;
//!
//! assert!(String::from_utf8(output)?.contains("INSERT INTO"));
//! # Ok(())
//! # }
//! ```

pub mod compiler;
pub mod engine;
pub mod generators;
pub mod plan;
pub mod planners;
pub mod registry;
pub mod seed;
pub mod value;

use std::fs;
use std::path::{Path, PathBuf};

use crate::diagnostic::DiagnosticBag;
use crate::parser::SqlDialect;
use crate::synthetic::{ConfigLoader, SyntheticFile};

// Re-exported so the staged API (registry -> compiler -> engine -> renderer)
// is fully usable from `crate::generate` alone, without also reaching into
// `crate::render`/`crate::synthetic`.
pub use crate::render::{RenderOptions, SqlRenderer};
pub use crate::synthetic::SyntheticModel;
pub use compiler::{CompileOptions, ModelCompiler, TableCountKind, TableCountOverride};
pub use engine::{
    DenseIntegerKey, EngineReport, GeneratedRow, GenerationEngine, KeyDomain,
    RandomAccessKeyGenerator, RowSink,
};
pub use generators::ConstantFactory;
pub use plan::{
    ColumnOwner, CompiledOutput, CompiledRelationship, ExecutionPhase, GenerationPlan,
    PlanEstimates, PlannedColumn, PlannedTable, RelationshipDistribution, ResolvedTableSeed,
};
pub use registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, CompiledModifier,
    CompiledPlanner, Determinism, ExtensionRegistry, GeneratorDescriptor, GeneratorFactory,
    KeyRecipe, ModifierDescriptor, ModifierFactory, PlanContext, PlannerDescriptor, PlannerFactory,
    RowContext, RowView, Verification,
};
pub use seed::{derive_seed, SeedRoot, StreamId};
pub use value::{GenerateError, GeneratedValue};

/// How a [`Generate::run`] call should treat the compiled model.
///
/// `Check` and `DryRun` never write SQL; they exist so a caller can validate
/// a model (or preview its resolved row counts) without paying for a full
/// render. [`crate::generate::RunMode`] gains an `EmitModel` variant once
/// dump profiling (Phase 2) can produce a model worth emitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RunMode {
    /// Compile and generate a complete run, rendering SQL to `output`.
    #[default]
    Generate,
    /// Parse and compile a complete model; no generation, no SQL output.
    /// Fails the same way `Generate` would on a structurally invalid model.
    Check,
    /// Compile the model and report the resolved plan (its estimated row
    /// counts) without running generation or writing SQL.
    DryRun,
}

/// Where a [`Generate::run`] renders its SQL, or whether it renders none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputTarget {
    /// Write to this path, creating or truncating the file.
    Path(PathBuf),
    /// Produce no SQL output (the default under [`RunMode::Check`] and
    /// [`RunMode::DryRun`] when no output path was given).
    Discard,
}

/// One fully-specified generation request: everything [`Generate::run`]
/// needs, assembled by a [`GenerateBuilder`] (or constructed directly by a
/// caller that wants to bypass the builder).
#[derive(Debug, Clone)]
pub struct GenerateRequest {
    /// A source dump to profile into a base model (Phase 2; not yet
    /// implemented — see [`Generate::run`]).
    pub input: Option<PathBuf>,
    /// A `kind: model` or `kind: overrides` YAML document.
    pub config: Option<PathBuf>,
    /// Where rendered SQL is written.
    pub output: OutputTarget,
    /// Row-count and table-selection controls passed to [`ModelCompiler`].
    pub compile: CompileOptions,
    /// Dialect and formatting controls passed to [`crate::render::SqlRenderer`].
    pub render: RenderOptions,
    /// Whether to fully generate, only validate, or only report the plan.
    pub mode: RunMode,
}

/// A summary of a completed [`Generate::run`].
///
/// Diagnostics collected while loading, merging, and compiling the model
/// survive here even on success: a `--max-rows` cap or a source-fingerprint
/// mismatch is a warning, not an error, and a caller still deserves to see
/// it.
#[derive(Debug, Clone, Default)]
pub struct GenerateReport {
    /// Rows written across every table (`Generate` mode), or the compiled
    /// plan's estimated total (`DryRun`); always `0` for `Check`.
    pub rows_written: u64,
    /// Every diagnostic collected while loading, merging, and compiling the
    /// model. Warning-only on success; see [`GenerateError::Diagnostics`]
    /// for the failure path.
    pub diagnostics: DiagnosticBag,
}

/// The public generation facade: loads a model, compiles it, and — under
/// [`RunMode::Generate`] — runs the engine and renders SQL. Build a request
/// with [`Generate::builder`].
#[derive(Debug, Clone, Copy)]
pub struct Generate;

impl Generate {
    /// A fresh [`GenerateBuilder`] with default options (`RunMode::Generate`,
    /// [`RenderOptions::default`], no seed/scale/table overrides).
    pub fn builder() -> GenerateBuilder {
        GenerateBuilder::default()
    }

    /// Run one fully-specified request end to end.
    ///
    /// Loads the model named by `request.config` (`request.input` — profiling
    /// a base model from a dump — is Phase 2 and reports a clear
    /// `GEN-PROFILE-UNSUPPORTED` error today), compiles it, then generates and
    /// renders under `request.mode`.
    ///
    /// Structured problems (a missing/invalid config, a compile error) come
    /// back as [`GenerateError::Diagnostics`]; a request that cannot be
    /// satisfied at all (no config, or `kind: overrides` with no base model)
    /// comes back as [`GenerateError::InvalidInput`].
    pub fn run(request: GenerateRequest) -> Result<GenerateReport, GenerateError> {
        let GenerateRequest {
            input,
            config,
            output,
            compile,
            render,
            mode,
        } = request;

        let model = load_model(input.as_deref(), config.as_deref())?;

        let plan = ModelCompiler::standard()
            .compile(model, compile)
            .map_err(GenerateError::Diagnostics)?;

        let mut diagnostics = DiagnosticBag::default();
        diagnostics.diagnostics.extend(plan.diagnostics.clone());

        match mode {
            RunMode::Check => Ok(GenerateReport {
                rows_written: 0,
                diagnostics,
            }),
            RunMode::DryRun => Ok(GenerateReport {
                rows_written: plan.estimates.total_rows,
                diagnostics,
            }),
            RunMode::Generate => {
                let engine_report = run_generate(GenerationEngine::new(plan), output, render)?;
                Ok(GenerateReport {
                    rows_written: engine_report.rows_written,
                    diagnostics,
                })
            }
        }
    }
}

/// Load a complete model for `request.config`, per the Phase 1 invocation
/// rules: a `.config()` complete model works fully; a `.config()` overrides
/// document with no base model, or any use of `.input()` (dump profiling)
/// is a clear not-yet-supported error.
fn load_model(
    input: Option<&Path>,
    config: Option<&Path>,
) -> Result<SyntheticModel, GenerateError> {
    match (input, config) {
        (None, None) => Err(GenerateError::InvalidInput(
            "GEN-REQUEST-SOURCE: at least one of `.input()` or `.config()` is required".into(),
        )),
        (Some(_), _) => Err(profiling_unsupported()),
        (None, Some(config_path)) => {
            match ConfigLoader::load(config_path).map_err(GenerateError::Diagnostics)? {
                SyntheticFile::Model(model) => Ok(model),
                SyntheticFile::Overrides(_) => Err(GenerateError::InvalidInput(format!(
                "GEN-OVERRIDES-NO-BASE: `{}` is a `kind: overrides` document but no base model \
                 is available; merging overrides onto a profiled base (`.input()`) is not yet \
                 supported (Phase 2, Tasks 19-21)",
                config_path.display()
            ))),
            }
        }
    }
}

/// The `GEN-PROFILE-UNSUPPORTED` error for any request naming `.input()`: dump
/// profiling is Phase 2 (Tasks 19-21).
fn profiling_unsupported() -> GenerateError {
    GenerateError::InvalidInput(
        "GEN-PROFILE-UNSUPPORTED: generating from `.input()` requires dump profiling, which is \
         not yet implemented (Phase 2, Tasks 19-21); supply a complete `kind: model` document via \
         `.config()` instead"
            .into(),
    )
}

/// Run the engine under [`RunMode::Generate`], rendering to `output`.
fn run_generate(
    engine: GenerationEngine,
    output: OutputTarget,
    render: RenderOptions,
) -> Result<EngineReport, GenerateError> {
    match output {
        OutputTarget::Path(path) => {
            let file = fs::File::create(&path).map_err(|err| {
                GenerateError::InvalidInput(format!(
                    "GEN-OUTPUT-IO: failed to create `{}`: {err}",
                    path.display()
                ))
            })?;
            let mut renderer = SqlRenderer::new(file, render);
            let report = engine.run(&mut renderer)?;
            renderer.finish()?;
            Ok(report)
        }
        OutputTarget::Discard => {
            let mut sink = DiscardSink;
            engine.run(&mut sink)
        }
    }
}

/// A [`RowSink`] that counts rows (via [`GenerationEngine::run`]'s own
/// bookkeeping) but writes nothing; backs a `RunMode::Generate` request built
/// directly (bypassing [`GenerateBuilder`]) with `OutputTarget::Discard` —
/// e.g. a caller that wants a real row count without paying for a render.
/// [`GenerateBuilder`] itself never reaches this: it only produces `Discard`
/// under `Check`/`DryRun`, and those modes never call [`run_generate`].
struct DiscardSink;

impl RowSink for DiscardSink {
    fn begin_table(&mut self, _table: &PlannedTable) -> Result<(), GenerateError> {
        Ok(())
    }

    fn write_row(
        &mut self,
        _table: &PlannedTable,
        _row: &GeneratedRow,
    ) -> Result<(), GenerateError> {
        Ok(())
    }

    fn end_table(&mut self, _table: &PlannedTable) -> Result<(), GenerateError> {
        Ok(())
    }
}

/// Builds one [`GenerateRequest`] and runs it. The only validation performed
/// here is request *shape* (e.g. `RunMode::Generate` needs an output path);
/// anything about the model itself — a missing config, a compile error —
/// surfaces from [`Generate::run`] as a structured [`GenerateError`].
#[derive(Debug, Clone, Default)]
pub struct GenerateBuilder {
    input: Option<PathBuf>,
    config: Option<PathBuf>,
    output: Option<PathBuf>,
    compile: CompileOptions,
    render: RenderOptions,
    mode: RunMode,
}

impl GenerateBuilder {
    /// A source dump to profile into a base model. Phase 2 (Tasks 19-21);
    /// for now, using this always fails with `GEN-PROFILE-UNSUPPORTED`.
    pub fn input(mut self, path: impl Into<PathBuf>) -> Self {
        self.input = Some(path.into());
        self
    }

    /// The `kind: model` (or, once a base model is available, `kind:
    /// overrides`) YAML document to generate from.
    pub fn config(mut self, path: impl Into<PathBuf>) -> Self {
        self.config = Some(path.into());
        self
    }

    /// The path rendered SQL is written to (created/truncated). Required
    /// under `RunMode::Generate`; optional under `Check`/`DryRun`, which
    /// never write SQL regardless.
    pub fn output(mut self, path: impl Into<PathBuf>) -> Self {
        self.output = Some(path.into());
        self
    }

    /// The SQL dialect to render for.
    pub fn output_dialect(mut self, dialect: SqlDialect) -> Self {
        self.render.dialect = dialect;
        self
    }

    /// The run's root seed, overriding the model's own `seed:`.
    pub fn seed(mut self, seed: u64) -> Self {
        self.compile.seed = Some(seed);
        self
    }

    /// A per-table row-count scale factor (`--table-scale`). Rejects a
    /// non-finite or negative factor.
    pub fn table_scale(
        mut self,
        table: impl Into<String>,
        factor: f64,
    ) -> Result<Self, GenerateError> {
        if !factor.is_finite() || factor < 0.0 {
            return Err(GenerateError::InvalidInput(format!(
                "GEN-TABLE-SCALE-INVALID: table scale must be a finite, non-negative number, found {factor}"
            )));
        }
        self.compile
            .table_rows
            .push(TableCountOverride::scale(table, factor));
        Ok(self)
    }

    /// Replace the full [`CompileOptions`] (row-count controls, table
    /// selection) in one call.
    pub fn compile(mut self, compile: CompileOptions) -> Self {
        self.compile = compile;
        self
    }

    /// Whether to verify generated rows against the source's constraints
    /// after generation. Accepted for forward compatibility; verification
    /// itself lands in Task 26, so this is a no-op until then.
    pub fn verify(self, _verify: bool) -> Self {
        self
    }

    /// A profiling depth hint for `.input()`. Accepted for forward
    /// compatibility with Phase 2 (Task 19); discarded until dump profiling
    /// is implemented.
    pub fn profile_depth(self, _depth: u32) -> Self {
        self
    }

    /// Run in `Check`, `DryRun`, or (the default) full `Generate` mode.
    pub fn mode(mut self, mode: RunMode) -> Self {
        self.mode = mode;
        self
    }

    /// Assemble a [`GenerateRequest`], validating request shape only (e.g.
    /// `RunMode::Generate` needs `.output()`). Model-level problems surface
    /// later, from [`Generate::run`].
    fn build(self) -> Result<GenerateRequest, GenerateError> {
        let output = match (self.mode, self.output) {
            (RunMode::Generate, Some(path)) => OutputTarget::Path(path),
            (RunMode::Generate, None) => {
                return Err(GenerateError::InvalidInput(
                    "GEN-REQUEST-OUTPUT: `.output()` is required under `RunMode::Generate`".into(),
                ))
            }
            (RunMode::Check | RunMode::DryRun, Some(path)) => OutputTarget::Path(path),
            (RunMode::Check | RunMode::DryRun, None) => OutputTarget::Discard,
        };

        Ok(GenerateRequest {
            input: self.input,
            config: self.config,
            output,
            compile: self.compile,
            render: self.render,
            mode: self.mode,
        })
    }

    /// Build the request and run it end to end.
    pub fn run(self) -> Result<GenerateReport, GenerateError> {
        Generate::run(self.build()?)
    }
}
