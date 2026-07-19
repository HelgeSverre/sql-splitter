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
//! # fn main() -> anyhow::Result<()> {
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
//! assert!(std::fs::read_to_string(&output)?.contains("INSERT INTO"));
//! # Ok(())
//! # }
//! ```
//!
//! See `builder_generates_from_a_complete_model` in `tests/generate_api_test.rs`
//! for more builder-driven workflows (dump profiling, `--check`, `--dry-run`,
//! `--verify`, table/seed overrides).
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
//! ```
//! use sql_splitter::generate::{CompileOptions, ExtensionRegistry, GenerationEngine, ModelCompiler};
//! use sql_splitter::render::{RenderOptions, SqlRenderer};
//! use sql_splitter::synthetic::SyntheticFile;
//!
//! # fn main() -> anyhow::Result<()> {
//! let model = SyntheticFile::parse_str(
//!     r#"
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
//! "#,
//! )?
//! .into_model()
//! .expect("is a complete model");
//!
//! let registry = ExtensionRegistry::standard();
//! let plan = ModelCompiler::new(registry).compile(model, CompileOptions::default())?;
//!
//! let mut renderer = SqlRenderer::new(Vec::new(), RenderOptions::default());
//! GenerationEngine::new(plan).run(&mut renderer)?;
//! let bytes = renderer.finish()?;
//!
//! assert!(String::from_utf8(bytes)?.contains("INSERT INTO"));
//! # Ok(())
//! # }
//! ```
//!
//! (Profiling a model from a dump — a `DumpProfiler`/`ModelInference` stage
//! ahead of the compiler — is wired into [`Generate::run`] itself: pass
//! [`GenerateBuilder::input`] to profile a dump into a base model, optionally
//! merging a `kind: overrides` `.config()` on top.)
//!
//! See `staged_api_renders_inserts` in `tests/generate_engine_test.rs` for a
//! worked example of assembling the stages by hand, including a custom
//! [`ExtensionRegistry`].

pub mod compiler;
pub mod engine;
pub mod generators;
pub mod output;
pub mod plan;
pub mod planners;
pub mod registry;
pub mod seed;
pub mod value;
pub mod verify;

use std::collections::{BTreeMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::convert::ConvertWarning;
use crate::diagnostic::{codes, Diagnostic, DiagnosticBag, SourceLocation};
use crate::parser::SqlDialect;
use crate::profile::evidence::DumpProfile;
use crate::profile::{
    Confidence, Decision, DumpProfiler, ModelInference, Precedence, ProfileBudget, ProfileDepth,
};
use crate::synthetic::model::{InferenceMode, InsertMode, OutputMode};
use crate::synthetic::{ConfigLoader, ModelMerger, SourceValueUse, SyntheticFile};

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
pub use output::{
    install_interrupt_handler, publish_in_order, AtomicOutput, CancellationToken, FamilyBudget,
    FamilyBuffer, FamilyState, PartialPublication, ProtectedSpool, PublicationSet, SpillKind,
    SpoolReader, SpoolWriter, SpooledRow, TempConfig,
};
pub use plan::{
    ColumnOwner, CompiledOutput, CompiledRelationship, DeferredConstraints, ExecutionPhase,
    FamilyPhase, GenerationPlan, PlanEstimates, PlannedColumn, PlannedTable,
    RelationshipDistribution, ResolvedTableSeed,
};
pub use registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, CompiledModifier,
    CompiledPlanner, Determinism, ExtensionRegistry, FamilySumCheck, GeneratorDescriptor,
    GeneratorFactory, KeyRecipe, ModifierDescriptor, ModifierFactory, PlanContext,
    PlannerDescriptor, PlannerFactory, PlannerPredicate, PredicateGuard, RowContext, RowView,
    Verification,
};
pub use seed::{derive_seed, SeedRoot, StreamId};
pub use value::{GenerateError, GeneratedValue};
pub use verify::{
    CheckOutcome, CheckStatus, DistributionExpectation, GenerationVerifier, VerificationReport,
};

/// How a [`Generate::run`] call should treat the compiled model.
///
/// `Check`, `DryRun`, and `EmitModel` never write SQL; they exist so a caller
/// can validate a model, preview its resolved row counts, or serialize the
/// resolved model without paying for a full render. `--emit-config` is an
/// orthogonal side output: it fires in *any* mode whose request carries an
/// emit target, and [`RunMode::EmitModel`] is simply the mode selected when
/// emitting the resolved model is the sole requested output (no SQL).
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
    /// Compile the model and emit the resolved, self-contained model (via the
    /// request's emit target); no generation, no SQL output.
    EmitModel,
}

/// Where a [`Generate::run`] renders its SQL, or whether it renders none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputTarget {
    /// Publish to this path atomically after generation succeeds. An existing
    /// file remains untouched if staging, generation, rendering, or flushing
    /// fails.
    Path(PathBuf),
    /// Produce no SQL output (the default under [`RunMode::Check`] and
    /// [`RunMode::DryRun`] when no output path was given).
    Discard,
}

/// Controls for profiling a source dump named by [`GenerateRequest::input`].
#[derive(Debug, Clone, Default)]
pub struct SourceOptions {
    /// Dialect the source dump is written in; auto-detected when `None`.
    pub dialect: Option<SqlDialect>,
    /// How deep to profile; the profiler's default (full) when `None`.
    pub depth: Option<ProfileDepth>,
    /// Per-column retained sample size; the profiler's default when `None`.
    pub sample: Option<usize>,
}

/// One fully-specified generation request: everything [`Generate::run`]
/// needs, assembled by a [`GenerateBuilder`] (or constructed directly by a
/// caller that wants to bypass the builder).
#[derive(Debug, Clone)]
pub struct GenerateRequest {
    /// A source dump to profile into a base model.
    pub input: Option<PathBuf>,
    /// A `kind: model` or `kind: overrides` YAML document.
    pub config: Option<PathBuf>,
    /// Where rendered SQL is written.
    pub output: OutputTarget,
    /// Where the resolved, self-contained model is serialized (`--emit-config`),
    /// or `None` to emit nothing.
    pub emit: Option<OutputTarget>,
    /// Row-count and table-selection controls passed to [`ModelCompiler`].
    pub compile: CompileOptions,
    /// Dialect and formatting controls passed to [`crate::render::SqlRenderer`].
    pub render: RenderOptions,
    /// The explicitly requested output dialect (`--dialect` /
    /// [`GenerateBuilder::output_dialect`]), or `None` to resolve the dialect
    /// from the model. Precedence (see `resolve_render_options`): this explicit
    /// value > the model's `output.dialect` > the model's captured source/input
    /// dialect (preserve-source) > [`SqlDialect::MySql`]. When the dialect is
    /// chosen deliberately (here or via the model's `output` block) and differs
    /// from the model's source, the renderer maps every type across dialects and
    /// reports lossy conversions; a preserve-source run renders the schema
    /// verbatim so a model's optional `source:` block stays removable metadata.
    pub output_dialect: Option<SqlDialect>,
    /// Whether to fully generate, only validate, only report the plan, or only
    /// emit the resolved model.
    pub mode: RunMode,
    /// Whether to build the `--explain` inference report.
    pub explain: bool,
    /// Whether to verify the generated SQL against the compiled plan and publish
    /// atomically only if the audit passes (`--verify`). Requires a filesystem
    /// SQL destination under [`RunMode::Generate`].
    pub verify: bool,
    /// Treat warnings as fatal and publish no SQL or emitted model when any
    /// compile-, render-, or verification-stage warning is produced.
    pub strict: bool,
    /// How to profile `input`, if given.
    pub source: SourceOptions,
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
    /// plan's estimated total (`DryRun`); always `0` for `Check`/`EmitModel`.
    pub rows_written: u64,
    /// The root seed the run actually used. Always `Some` — an unseeded run
    /// draws a fresh random seed and records it here so the run is reproducible
    /// after the fact, even though its emitted model records no seed.
    pub effective_seed: Option<u64>,
    /// Every diagnostic collected while loading, merging, and compiling the
    /// model. Non-error diagnostics only on success; see
    /// [`GenerateError::Diagnostics`]
    /// for the failure path.
    pub diagnostics: DiagnosticBag,
    /// Every place the resolved model's rules replay literal values derived
    /// from the source dump. Locations and rule kinds only, never values — the
    /// `GEN-SOURCE-VALUES` advisory is built from this.
    pub source_values: Vec<SourceValueUse>,
    /// The per-column inference explanation, populated only when the request
    /// asked for it (`--explain`). Never carries observed values.
    pub explain: Vec<ExplainColumn>,
}

/// One column's inference decision, reshaped for `--explain` reporting. Carries
/// the winning rule's reason/confidence and the rejected alternatives — never
/// any observed values.
#[derive(Debug, Clone)]
pub struct ExplainColumn {
    /// `"table.column"`.
    pub column: String,
    /// The winning candidate's stable reason code.
    pub reason: String,
    /// The winning candidate's confidence label.
    pub confidence: String,
    /// The resolved generator kind.
    pub generator_kind: String,
    /// Whether the winning rule embeds source-derived literal values.
    pub source_derived: bool,
    /// The alternatives that lost, most-relevant first.
    pub rejected: Vec<ExplainRejected>,
}

/// One rejected alternative recorded for [`ExplainColumn`].
#[derive(Debug, Clone)]
pub struct ExplainRejected {
    pub generator_kind: String,
    pub reason: String,
    pub precedence: String,
    pub confidence: String,
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
    /// Resolves the model — profiling and inferring a base from `request.input`
    /// when given, then merging a `kind: overrides` `request.config` on top —
    /// compiles it **exactly once**, optionally serializes the resolved model
    /// (`request.emit`) from that same plan, and optionally executes the same
    /// plan under `request.mode`.
    ///
    /// Structured problems discovered during loading, compilation, or runtime
    /// come back as [`GenerateError::Diagnostics`] or
    /// [`GenerateError::Diagnostic`].
    pub fn run(request: GenerateRequest) -> Result<GenerateReport, GenerateError> {
        let GenerateRequest {
            input,
            config,
            output,
            emit,
            mut compile,
            render,
            output_dialect,
            mode,
            explain,
            verify,
            strict,
            source,
        } = request;

        if mode == RunMode::Generate {
            if let (OutputTarget::Path(sql), Some(OutputTarget::Path(model))) = (&output, &emit) {
                let sql_identity = normalized_destination(sql)
                    .map_err(|error| output_identity_error(sql, error))?;
                let model_identity = normalized_destination(model)
                    .map_err(|error| output_identity_error(model, error))?;
                if sql_identity == model_identity {
                    return Err(GenerateError::diagnostic(
                        &codes::REQUEST_OUTPUT,
                        sql.display().to_string(),
                        "generated SQL and the resolved model require different destinations",
                    ));
                }
            }
        }

        let had_source_dump = input.is_some();

        let ResolvedModel {
            model,
            mut diagnostics,
            decisions,
        } = assemble_model(input.as_deref(), config.as_deref(), &source)?;

        // Resolve the effective seed once so emit and execute share it. An
        // unseeded run draws a fresh random root (kept reproducible-after-the
        // -fact via the report) but its emitted model records no seed, so
        // re-running that model is intentionally fresh again.
        let explicit_seed = compile.seed.or(model.seed);
        let effective_seed = explicit_seed.unwrap_or_else(rand::random);
        compile.seed = Some(effective_seed);

        // Compile EXACTLY once; both emit and execute read this one plan.
        let plan = ModelCompiler::standard()
            .compile(model.clone(), compile)
            .map_err(GenerateError::Diagnostics)?;
        diagnostics.diagnostics.extend(plan.diagnostics.clone());

        // Fold the compiled model `output:` block into the render settings under
        // any explicit CLI/builder flags (dialect, mode, inserts, batch size).
        // The precedence and the "deliberate cross-dialect maps types" rule live
        // in `resolve_render_options`.
        let mut render = render;
        resolve_render_options(&mut render, output_dialect, &plan);

        // Scan the final resolved model (explicit + inferred rules) for
        // source-derived literals.
        let source_values = model.source_value_uses();
        if !source_values.is_empty() {
            let message = if had_source_dump {
                format!(
                    "{} rule(s) replay literal values that may have been derived from the source \
                     dump; review the listed rules before sharing the output",
                    source_values.len()
                )
            } else {
                format!(
                    "{} rule(s) replay hand-authored literal values; the output is synthetic, \
                     not anonymized source data",
                    source_values.len()
                )
            };
            let diagnostic = diagnostics.advisory(&codes::SOURCE_VALUES, "tables", message);
            diagnostic.related = source_values
                .iter()
                .map(|used| SourceLocation {
                    path: used.path.clone(),
                    description: Some(used.rule_kind.clone()),
                })
                .collect();
        }

        let explain = if explain {
            build_explain(&decisions)
        } else {
            Vec::new()
        };

        let report = GenerateReport {
            rows_written: 0,
            effective_seed: Some(effective_seed),
            diagnostics,
            source_values,
            explain,
        };

        // Compile/profile warnings are already known, so strict mode can stop
        // before staging or generating anything. Render and verification
        // warnings are gated inside their staged paths below, before publish.
        if strict && has_warnings(&report.diagnostics) {
            return Err(GenerateError::Diagnostics(report.diagnostics));
        }

        if verify {
            let diagnostics = report.diagnostics.clone();
            let VerifiedRun {
                rows_written,
                diagnostics,
            } = run_verified(
                &model,
                plan,
                diagnostics,
                VerifiedOptions {
                    explicit_seed,
                    output,
                    emit,
                    render,
                    strict,
                },
            )?;
            let out = GenerateReport {
                rows_written,
                diagnostics,
                ..report
            };
            return Ok(out);
        }

        // Serialize and stage the emitted model before generation, but do not
        // publish it yet. A later staging, generation, rendering, or flush
        // failure drops this protected temp and leaves every destination at
        // its previous bytes.
        let model_output = stage_model(&model, &plan, explicit_seed, emit.as_ref())?;

        match mode {
            RunMode::Check | RunMode::EmitModel => {
                publish_model(model_output)?;
                Ok(report)
            }
            RunMode::DryRun => {
                let rows_written = plan.estimates.total_rows;
                publish_model(model_output)?;
                Ok(GenerateReport {
                    rows_written,
                    ..report
                })
            }
            RunMode::Generate => {
                let diagnostics = report.diagnostics.clone();
                let (engine_report, diagnostics) = run_generate(
                    GenerationEngine::new(plan),
                    output,
                    model_output,
                    render,
                    diagnostics,
                    strict,
                )?;
                let out = GenerateReport {
                    rows_written: engine_report.rows_written,
                    diagnostics,
                    ..report
                };
                Ok(out)
            }
        }
    }
}

/// Resolve a requested output to the filesystem identity it would publish to.
///
/// Existing paths are canonicalized in full. For a not-yet-created output,
/// canonicalize the deepest existing ancestor (resolving symlinks and `..`)
/// and append the missing suffix. This compares aliases before either output is
/// staged while still allowing normal publication to a new filename.
fn normalized_destination(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        let name = ancestor.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("`{}` has no existing filesystem ancestor", path.display()),
            )
        })?;
        suffix.push(name.to_os_string());
        ancestor = ancestor.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("`{}` has no parent destination", path.display()),
            )
        })?;
    }

    let mut identity = ancestor.canonicalize()?;
    for component in suffix.iter().rev() {
        identity.push(component);
    }
    Ok(identity)
}

fn output_identity_error(path: &Path, error: io::Error) -> GenerateError {
    GenerateError::diagnostic(
        &codes::REQUEST_OUTPUT,
        path.display().to_string(),
        format!(
            "cannot resolve output destination `{}`: {error}",
            path.display()
        ),
    )
}

/// The outcome of a successful `--verify` publish: the rows written plus any
/// capabilities the audit could not evaluate exactly (surfaced as a warning).
struct VerifiedRun {
    rows_written: u64,
    diagnostics: DiagnosticBag,
}

struct VerifiedOptions {
    explicit_seed: Option<u64>,
    output: OutputTarget,
    emit: Option<OutputTarget>,
    render: RenderOptions,
    strict: bool,
}

/// Render SQL to a protected temp beside the destination, verify it against the
/// compiled plan, and publish atomically only if the full audit passes. A failed
/// audit leaves every prior destination untouched and returns
/// [`GenerateError::Diagnostic`] with `GEN-VERIFY-FAILED`. When a resolved model is also requested
/// it is published *after* SQL verification succeeds; a failure publishing the
/// second file is reported as a precise partial publication rather than as a
/// pretended pairwise-atomic write.
fn run_verified(
    model: &SyntheticModel,
    plan: GenerationPlan,
    mut diagnostics: DiagnosticBag,
    options: VerifiedOptions,
) -> Result<VerifiedRun, GenerateError> {
    let VerifiedOptions {
        explicit_seed,
        output,
        emit,
        render,
        strict,
    } = options;
    let destination = match output {
        OutputTarget::Path(path) => path,
        OutputTarget::Discard => {
            return Err(GenerateError::diagnostic(
                &codes::VERIFY_NO_FILE,
                "output",
                "--verify requires a filesystem SQL destination, not stdout",
            ))
        }
    };

    // Stage the resolved model YAML up front so a serialization error fails
    // before anything is rendered.
    let emit_plan = match &emit {
        Some(OutputTarget::Path(path)) => Some((
            path.clone(),
            resolved_model_yaml(model, &plan, explicit_seed)?,
        )),
        _ => None,
    };

    let verifier = verify::GenerationVerifier::new(&plan).dialect(render.dialect);

    // Render SQL into a protected temp file beside the destination.
    let mut sql_output = AtomicOutput::create(&destination).map_err(|error| {
        GenerateError::diagnostic(
            &codes::VERIFY_STAGE,
            destination.display().to_string(),
            format!(
                "cannot stage output beside `{}`: {error}",
                destination.display()
            ),
        )
    })?;
    let temp_path = sql_output.temp_path().to_path_buf();
    let (rows_written, render_warnings) = {
        let mut renderer = SqlRenderer::new(sql_output.writer(), render);
        let engine_report = GenerationEngine::new(plan).run(&mut renderer)?;
        let warnings = renderer.warnings().to_vec();
        renderer.finish()?;
        (engine_report.rows_written, warnings)
    };

    // Audit the freshly rendered temp file.
    let report = verifier.verify_path(&temp_path)?;
    if !report.passed() {
        let failures: Vec<String> = report
            .failures()
            .map(|check| format!("{} ({})", check.name, check.detail))
            .collect();
        // Dropping `sql_output` removes the temp; the destination is untouched.
        let mut diagnostic = Diagnostic::error(
            &codes::VERIFY_FAILED,
            destination.display().to_string(),
            format!(
                "generated output failed verification and was not published; {} check(s) failed",
                failures.len()
            ),
        );
        diagnostic.related = failures
            .into_iter()
            .map(|failure| SourceLocation {
                path: destination.display().to_string(),
                description: Some(failure),
            })
            .collect();
        return Err(GenerateError::Diagnostic(Box::new(diagnostic)));
    }
    let not_checked: Vec<String> = report
        .checks
        .iter()
        .filter(|check| check.status == verify::CheckStatus::NotChecked)
        .map(|check| check.name.clone())
        .collect();

    merge_render_warnings(&mut diagnostics, &render_warnings);
    merge_not_checked_warning(&mut diagnostics, &not_checked);
    if strict && has_warnings(&diagnostics) {
        // `sql_output` still names only a protected temp. Returning drops it,
        // and the resolved model has not been published either.
        return Err(GenerateError::Diagnostics(diagnostics));
    }

    // Verification passed: publish the SQL, then the model (if any).
    let mut outputs = vec![sql_output];
    if let Some((path, yaml)) = emit_plan {
        let mut model_output = AtomicOutput::create(&path).map_err(|error| {
            GenerateError::diagnostic(
                &codes::VERIFY_STAGE,
                path.display().to_string(),
                format!("cannot stage model beside `{}`: {error}", path.display()),
            )
        })?;
        use std::io::Write;
        model_output
            .writer()
            .write_all(yaml.as_bytes())
            .map_err(|error| {
                GenerateError::diagnostic(
                    &codes::EMIT_IO,
                    path.display().to_string(),
                    error.to_string(),
                )
            })?;
        outputs.push(model_output);
    }

    publish_in_order(outputs).map_err(|partial| {
        GenerateError::diagnostic(
            &codes::VERIFY_PARTIAL_PUBLISH,
            "output",
            partial.to_string(),
        )
    })?;

    Ok(VerifiedRun {
        rows_written,
        diagnostics,
    })
}

/// A resolved base model plus the diagnostics and inference decisions gathered
/// while assembling it.
struct ResolvedModel {
    model: SyntheticModel,
    diagnostics: DiagnosticBag,
    decisions: Vec<Decision>,
}

/// Resolve the model a request will compile: a pure `.config()` complete model,
/// a profiled+inferred base from `.input()`, or that base with a `kind:
/// overrides` `.config()` merged on top.
fn assemble_model(
    input: Option<&Path>,
    config: Option<&Path>,
    source: &SourceOptions,
) -> Result<ResolvedModel, GenerateError> {
    match (input, config) {
        (None, None) => Err(GenerateError::diagnostic(
            &codes::REQUEST_SOURCE,
            "request",
            "at least one of `.input()` or `.config()` is required",
        )),
        (None, Some(config_path)) => {
            match ConfigLoader::load(config_path).map_err(GenerateError::Diagnostics)? {
                SyntheticFile::Model(model) => Ok(ResolvedModel {
                    model,
                    diagnostics: DiagnosticBag::default(),
                    decisions: Vec::new(),
                }),
                SyntheticFile::Overrides(_) => Err(GenerateError::diagnostic(
                    &codes::OVERRIDES_NO_BASE,
                    config_path.display().to_string(),
                    format!(
                        "`{}` is a `kind: overrides` document but no base model is available; \
                         supply a source dump to profile (`.input()`) so the overrides have a \
                         base to merge onto",
                        config_path.display()
                    ),
                )),
            }
        }
        (Some(input_path), config) => {
            let profile = profile_source(input_path, source)?;
            let inference = ModelInference::standard()
                .infer(&profile.schema, &profile)
                .map_err(|error| {
                    GenerateError::diagnostic(
                        &codes::INFER_FAILED,
                        input_path.display().to_string(),
                        error.to_string(),
                    )
                })?;

            let mut diagnostics = DiagnosticBag::default();
            diagnostics.diagnostics.extend(profile.warnings);
            diagnostics.diagnostics.extend(
                inference
                    .warnings
                    .into_iter()
                    .filter(|diagnostic| diagnostic.code != codes::INFER_SOURCE_DERIVED.code),
            );

            let base = inference.model;
            let decisions = inference.decisions;

            match config {
                None => Ok(ResolvedModel {
                    model: base,
                    diagnostics,
                    decisions,
                }),
                Some(config_path) => {
                    match ConfigLoader::load(config_path).map_err(GenerateError::Diagnostics)? {
                        SyntheticFile::Overrides(patch) => {
                            let (merged, bag) = ModelMerger::merge(base, patch)
                                .map_err(GenerateError::Diagnostics)?;
                            diagnostics.diagnostics.extend(bag.diagnostics);
                            Ok(ResolvedModel {
                                model: merged,
                                diagnostics,
                                decisions,
                            })
                        }
                        SyntheticFile::Model(model) => {
                            // An explicit complete model alongside a source: the
                            // model is authoritative and stands alone, so use it
                            // and note that the profiled base was set aside.
                            diagnostics.warning(
                                crate::diagnostic::codes::CONFIG_COMPLETE_MODEL.code,
                                config_path.display().to_string(),
                                "a complete `kind: model` config was supplied with a source dump; \
                                 using the config model and ignoring the profiled base",
                            );
                            Ok(ResolvedModel {
                                model,
                                diagnostics,
                                decisions: Vec::new(),
                            })
                        }
                    }
                }
            }
        }
    }
}

/// Profile `path` into a [`DumpProfile`] carrying both its portable DDL schema
/// and value evidence. The dump is read exactly once: the profiler builds the
/// schema and sketches values in the same streaming pass, then exposes the
/// schema on [`DumpProfile::schema`].
fn profile_source(path: &Path, source: &SourceOptions) -> Result<DumpProfile, GenerateError> {
    let mut builder = DumpProfiler::builder();
    if let Some(dialect) = source.dialect {
        builder = builder.dialect(dialect);
    }
    if let Some(depth) = source.depth {
        builder = builder.depth(depth);
    }
    if let Some(sample) = source.sample {
        builder = builder.budget(ProfileBudget {
            sample_rows: sample,
            ..ProfileBudget::default()
        });
    }
    builder.build().profile_path(path).map_err(source_error)
}

/// Wrap an I/O / profiling failure as a `GEN-SOURCE-IO` invalid-input error.
fn source_error(error: impl std::fmt::Display) -> GenerateError {
    GenerateError::diagnostic(&codes::SOURCE_IO, "input", error.to_string())
}

/// Serialize the resolved model as a self-contained `--emit-config` document.
///
/// Freezes each table's resolved row count in place (keeping `kind: observed`),
/// pins `defaults.inference: disabled`, and records the seed only when the run
/// was explicitly seeded — an unseeded run emits no seed so reloading it is
/// intentionally fresh. Inference already omits raw samples, so the emitted
/// model carries only bounded, non-literal `profiles` metadata.
/// Build the resolved, self-contained `--emit-config` YAML for `model`/`plan`.
///
/// Freezes each table's resolved row count, pins `inference: disabled`, and
/// records the seed only for an explicitly-seeded run. Shared by normal and
/// verified atomic publication paths.
fn resolved_model_yaml(
    model: &SyntheticModel,
    plan: &GenerationPlan,
    explicit_seed: Option<u64>,
) -> Result<String, GenerateError> {
    let mut emit = model.clone();

    let resolved: BTreeMap<String, u64> = plan
        .tables
        .iter()
        .map(|t| (t.name.clone(), t.rows))
        .collect();
    // The resolved model represents this compiled plan, not the pre-selection
    // source model. Drop excluded tables and their optional profile metadata,
    // and carry the compiler's normalized schema (including detached FKs).
    emit.imports.clear();
    emit.tables.retain(|name, _| resolved.contains_key(name));
    emit.profiles.retain(|path, _| {
        path.split_once('.')
            .is_some_and(|(table, _)| resolved.contains_key(table))
    });
    for planned in &plan.tables {
        let Some(table) = emit.tables.get_mut(&planned.name) else {
            continue;
        };
        table.schema = planned.schema.clone();
        table.relationships.retain(|relationship| {
            planned.relationships.iter().any(|active| {
                active.name == relationship.name
                    && active.columns == relationship.columns
                    && active.parent_table == relationship.references.table
                    && active.parent_columns == relationship.references.columns
            })
        });
    }
    let family_children: HashSet<String> = plan
        .tables
        .iter()
        .flat_map(|table| table.planners.iter())
        .filter_map(|planner| planner.family_child_table().map(str::to_owned))
        .collect();
    emit.freeze_row_counts(&resolved, &family_children);
    emit.defaults.inference = InferenceMode::Disabled;
    emit.seed = explicit_seed;

    serde_yaml_ng::to_string(&emit).map_err(|error| {
        GenerateError::diagnostic(&codes::EMIT_SERIALIZE, "emit_config", error.to_string())
    })
}

fn stage_model(
    model: &SyntheticModel,
    plan: &GenerationPlan,
    explicit_seed: Option<u64>,
    target: Option<&OutputTarget>,
) -> Result<Option<AtomicOutput>, GenerateError> {
    let Some(target) = target else {
        return Ok(None);
    };
    let yaml = resolved_model_yaml(model, plan, explicit_seed)?;

    match target {
        OutputTarget::Path(path) => {
            let mut output = AtomicOutput::create(path).map_err(|error| {
                GenerateError::diagnostic(
                    &codes::EMIT_IO,
                    path.display().to_string(),
                    format!("cannot stage model beside `{}`: {error}", path.display()),
                )
            })?;
            output
                .writer()
                .write_all(yaml.as_bytes())
                .map_err(|error| {
                    GenerateError::diagnostic(
                        &codes::EMIT_IO,
                        path.display().to_string(),
                        format!("failed to stage `{}`: {error}", path.display()),
                    )
                })?;
            Ok(Some(output))
        }
        // The builder/CLI only ever routes emit to a real path (a stdout emit
        // is spooled through a temp file), so this arm is unreachable in
        // practice; discard rather than panic if a caller wires it directly.
        OutputTarget::Discard => Ok(None),
    }
}

fn publish_model(output: Option<AtomicOutput>) -> Result<(), GenerateError> {
    let Some(output) = output else {
        return Ok(());
    };
    let path = output.destination().to_path_buf();
    output.commit().map_err(|error| {
        GenerateError::diagnostic(
            &codes::EMIT_IO,
            path.display().to_string(),
            format!("failed to publish `{}`: {error}", path.display()),
        )
    })
}

/// Reshape inference [`Decision`]s into the value-free `--explain` report.
fn build_explain(decisions: &[Decision]) -> Vec<ExplainColumn> {
    decisions
        .iter()
        .map(|decision| ExplainColumn {
            column: decision.column.clone(),
            reason: decision.reason.clone(),
            confidence: confidence_label(decision.confidence).to_string(),
            generator_kind: decision.generator_kind.clone(),
            source_derived: decision.source_derived,
            rejected: decision
                .rejected
                .iter()
                .map(|rejected| ExplainRejected {
                    generator_kind: rejected.generator_kind.clone(),
                    reason: rejected.reason.clone(),
                    precedence: precedence_label(rejected.precedence).to_string(),
                    confidence: confidence_label(rejected.confidence).to_string(),
                })
                .collect(),
        })
        .collect()
}

/// Stable lowercase label for a [`Confidence`], for reporting.
fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::Low => "low",
        Confidence::Medium => "medium",
        Confidence::High => "high",
        Confidence::Certain => "certain",
    }
}

/// Stable lowercase label for a [`Precedence`] class, for reporting.
fn precedence_label(precedence: Precedence) -> &'static str {
    match precedence {
        Precedence::TypeFallback => "type_fallback",
        Precedence::ObservedDistribution => "observed_distribution",
        Precedence::StrongSemantic => "strong_semantic",
        Precedence::Relationship => "relationship",
        Precedence::CredentialGuard => "credential_guard",
        Precedence::SchemaConstraint => "schema_constraint",
        Precedence::ExplicitYaml => "explicit_yaml",
    }
}

/// Run the engine under [`RunMode::Generate`], rendering to `output`. Returns
/// the engine report plus any warnings the renderer collected (e.g. lossy
/// cross-dialect type conversions), which the caller merges into the report
/// diagnostics so they are visible and `--strict`-promotable.
fn run_generate(
    engine: GenerationEngine,
    output: OutputTarget,
    model_output: Option<AtomicOutput>,
    render: RenderOptions,
    mut diagnostics: DiagnosticBag,
    strict: bool,
) -> Result<(EngineReport, DiagnosticBag), GenerateError> {
    match output {
        OutputTarget::Path(path) => {
            let mut sql_output = AtomicOutput::create(&path).map_err(|err| {
                GenerateError::diagnostic(
                    &codes::OUTPUT_IO,
                    path.display().to_string(),
                    format!("cannot stage output beside `{}`: {err}", path.display()),
                )
            })?;
            let (report, warnings) = {
                let mut renderer = SqlRenderer::new(sql_output.writer(), render);
                let report = engine.run(&mut renderer)?;
                // Drain the renderer's warnings before `finish` consumes it.
                let warnings = renderer.warnings().to_vec();
                renderer.finish()?;
                (report, warnings)
            };

            merge_render_warnings(&mut diagnostics, &warnings);
            if strict && has_warnings(&diagnostics) {
                // Both outputs are staged only. Returning drops their temps and
                // preserves every previous destination byte.
                return Err(GenerateError::Diagnostics(diagnostics));
            }

            if let Some(model_output) = model_output {
                // SQL is the primary artifact, so publish it first. The files
                // cannot form one cross-filesystem transaction; if publishing
                // the model then fails, report exactly what already landed.
                publish_in_order(vec![sql_output, model_output]).map_err(|partial| {
                    GenerateError::diagnostic(&codes::OUTPUT_IO, "output", partial.to_string())
                })?;
            } else {
                sql_output.commit().map_err(|error| {
                    GenerateError::diagnostic(
                        &codes::OUTPUT_IO,
                        path.display().to_string(),
                        format!("failed to publish `{}`: {error}", path.display()),
                    )
                })?;
            }
            Ok((report, diagnostics))
        }
        OutputTarget::Discard => {
            let mut sink = DiscardSink;
            let report = engine.run(&mut sink)?;
            publish_model(model_output)?;
            Ok((report, diagnostics))
        }
    }
}

/// The batch size the CLI/`RenderOptions::default` land on when a run neither
/// passes `--batch-size` nor carries a model `output.batch_size`. Used as the
/// sentinel for "the caller left batch size at its neutral default", so a
/// model's `output.batch_size` can fill in under it.
const DEFAULT_RENDER_BATCH_SIZE: usize = 1_000;

/// Fold the compiled model `output:` block into `render` under the caller's
/// explicit flags, resolving every field with a single precedence: an explicit
/// CLI/builder flag wins, then the model's `output:` block, then (dialect only)
/// the model's captured source/input dialect, then the built-in default.
///
/// * **dialect** — `--dialect`/[`GenerateBuilder::output_dialect`] > model
///   `output.dialect` > `plan.input_dialect` (preserve-source) > [`SqlDialect::
///   MySql`]. When the target dialect was chosen *deliberately* (CLI or model,
///   not preserve-source/fallback) the captured source dialect is threaded into
///   `source_dialect` so the renderer maps every type across dialects and
///   reports lossy conversions; a preserve-source or fallback run leaves
///   `source_dialect` unset (source == render dialect), so a model's optional
///   `source:` block stays removable metadata that cannot change output.
/// * **mode** — only the absence of `--schema-only`/`--data-only` yields
///   [`OutputMode::SchemaAndData`], so that value is the sentinel under which the
///   model's `output.mode` fills in; an explicit `--schema-only`/`--data-only`
///   always wins.
/// * **inserts** — the CLI has only a `--no-copy` opt-in, so a `false` `no_copy`
///   falls back to the model: `output.inserts: insert` forces multi-row INSERT,
///   while `auto`/`copy` keep the COPY default. An explicit `--no-copy` wins.
/// * **batch_size** — [`DEFAULT_RENDER_BATCH_SIZE`] is the neutral sentinel under
///   which a positive `output.batch_size` fills in; any other explicit value
///   wins.
fn resolve_render_options(
    render: &mut RenderOptions,
    output_dialect: Option<SqlDialect>,
    plan: &GenerationPlan,
) {
    let deliberate_dialect = match (output_dialect, plan.output.dialect) {
        (Some(cli), _) => {
            render.dialect = cli;
            true
        }
        (None, Some(model_dialect)) => {
            render.dialect = model_dialect;
            true
        }
        (None, None) => {
            render.dialect = plan.input_dialect.unwrap_or(SqlDialect::MySql);
            false
        }
    };
    if deliberate_dialect {
        render.source_dialect = render.source_dialect.or(plan.input_dialect);
    }

    if render.mode == OutputMode::SchemaAndData {
        if let Some(mode) = plan.output.mode {
            render.mode = mode;
        }
    }

    if !render.no_copy {
        render.no_copy = matches!(plan.output.inserts, Some(InsertMode::Insert));
    }

    if render.batch_size == DEFAULT_RENDER_BATCH_SIZE {
        if let Some(batch) = plan.output.batch_size.filter(|&b| b > 0) {
            render.batch_size = batch as usize;
        }
    }
}

/// Merge renderer [`ConvertWarning`]s into `diagnostics` as `Severity::Warning`
/// entries with stable `GEN-*` codes, so both the CLI's `write_diagnostics` and
/// its `--strict` `warnings_are_fatal` gate see a lossy cross-dialect conversion
/// (which would otherwise be silently dropped on the render pass).
fn merge_render_warnings(diagnostics: &mut DiagnosticBag, warnings: &[ConvertWarning]) {
    for warning in warnings {
        let code = match warning {
            ConvertWarning::LossyConversion { .. } => crate::diagnostic::codes::LOSSY_TYPE.code,
            _ => crate::diagnostic::codes::RENDER_WARNING.code,
        };
        diagnostics.warning(code, "output", warning.to_string());
    }
}

/// Surface verification coverage gaps as warnings before the staged output is
/// published, so strict mode can reject them without changing destinations.
fn merge_not_checked_warning(diagnostics: &mut DiagnosticBag, not_checked: &[String]) {
    if not_checked.is_empty() {
        return;
    }
    diagnostics.warning(
        crate::diagnostic::codes::VERIFY_NOTCHECKED.code,
        String::new(),
        format!(
            "verification passed but {} capability/capabilities could not be checked exactly: {}",
            not_checked.len(),
            not_checked.join(", ")
        ),
    );
}

fn has_warnings(diagnostics: &DiagnosticBag) -> bool {
    diagnostics
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == crate::diagnostic::Severity::Warning)
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
    emit: Option<PathBuf>,
    compile: CompileOptions,
    render: RenderOptions,
    output_dialect: Option<SqlDialect>,
    mode: RunMode,
    explain: bool,
    verify: bool,
    strict: bool,
    source: SourceOptions,
}

impl GenerateBuilder {
    /// A source dump to profile into a base model. Profiling and inference run
    /// automatically; combine with a `kind: overrides` `.config()` to patch
    /// the profiled base.
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

    /// The path rendered SQL is published to atomically after successful
    /// generation. Required under `RunMode::Generate`; optional under
    /// `Check`/`DryRun`, which never write SQL regardless.
    pub fn output(mut self, path: impl Into<PathBuf>) -> Self {
        self.output = Some(path.into());
        self
    }

    /// The path the resolved, self-contained model is published to atomically
    /// (`--emit-config`). Emitting fires alongside whatever `mode` runs.
    pub fn emit(mut self, path: impl Into<PathBuf>) -> Self {
        self.emit = Some(path.into());
        self
    }

    /// Whether to build the `--explain` inference report.
    pub fn explain(mut self, explain: bool) -> Self {
        self.explain = explain;
        self
    }

    /// Force the dialect the source dump (`.input()`) is parsed as, instead of
    /// auto-detecting it.
    pub fn input_dialect(mut self, dialect: SqlDialect) -> Self {
        self.source.dialect = Some(dialect);
        self
    }

    /// Per-column retained sample size used while profiling `.input()`.
    pub fn profile_sample(mut self, sample: usize) -> Self {
        self.source.sample = Some(sample);
        self
    }

    /// The SQL dialect to render for, overriding the model's own `output.dialect`
    /// and captured source dialect. Choosing a dialect that differs from the
    /// model's source dialect renders every type across dialects (reporting lossy
    /// conversions); leaving it unset resolves the dialect from the model
    /// (`output.dialect`, else the source/input dialect, else MySQL).
    pub fn output_dialect(mut self, dialect: SqlDialect) -> Self {
        self.render.dialect = dialect;
        self.output_dialect = Some(dialect);
        self
    }

    /// The run's root seed, overriding the model's own `seed:`.
    pub fn seed(mut self, seed: u64) -> Self {
        self.compile.seed = Some(seed);
        self
    }

    /// Render MSSQL output in "production style" — see
    /// [`RenderOptions::mssql_production_style`]. No effect outside
    /// [`SqlDialect::Mssql`].
    pub fn mssql_production_style(mut self, production_style: bool) -> Self {
        self.render.mssql_production_style = production_style;
        self
    }

    /// Emit a `GO` batch separator every `interval` `INSERT` batches instead
    /// of after every batch — see [`RenderOptions::mssql_go`]. No effect
    /// outside [`SqlDialect::Mssql`].
    pub fn mssql_go(mut self, interval: u64) -> Self {
        self.render.mssql_go = Some(interval);
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
            return Err(GenerateError::diagnostic(
                &codes::TABLE_SCALE_INVALID,
                format!("tables.{}", table.into()),
                format!("table scale must be a finite, non-negative number, found {factor}"),
            ));
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

    /// Whether to verify the generated SQL against the compiled plan and
    /// publish atomically only if the audit passes. Requires a filesystem SQL
    /// destination (`.output()`); a stdout destination is a usage error.
    pub fn verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    /// Treat every warning as fatal and publish no requested output when one
    /// is discovered.
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// The profiling depth for `.input()` (defaults to the profiler's full
    /// depth when unset).
    pub fn profile_depth(mut self, depth: ProfileDepth) -> Self {
        self.source.depth = Some(depth);
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
        if self.verify && self.mode != RunMode::Generate {
            return Err(GenerateError::diagnostic(
                &codes::VERIFY_MODE,
                "request.verify",
                "--verify generates and publishes; it cannot be combined with \
                 check/dry-run/emit-only modes",
            ));
        }
        let output = match (self.mode, self.output) {
            (RunMode::Generate, Some(path)) => OutputTarget::Path(path),
            (RunMode::Generate, None) => {
                return Err(GenerateError::diagnostic(
                    &codes::REQUEST_OUTPUT,
                    "request.output",
                    "`.output()` is required under `RunMode::Generate`",
                ))
            }
            (RunMode::Check | RunMode::DryRun | RunMode::EmitModel, Some(path)) => {
                OutputTarget::Path(path)
            }
            (RunMode::Check | RunMode::DryRun | RunMode::EmitModel, None) => OutputTarget::Discard,
        };

        if self.verify && !matches!(output, OutputTarget::Path(_)) {
            return Err(GenerateError::diagnostic(
                &codes::VERIFY_NO_FILE,
                "request.output",
                "--verify requires a filesystem SQL destination",
            ));
        }

        Ok(GenerateRequest {
            input: self.input,
            config: self.config,
            output,
            emit: self.emit.map(OutputTarget::Path),
            compile: self.compile,
            render: self.render,
            output_dialect: self.output_dialect,
            mode: self.mode,
            explain: self.explain,
            verify: self.verify,
            strict: self.strict,
            source: self.source,
        })
    }

    /// Build the request and run it end to end.
    pub fn run(self) -> Result<GenerateReport, GenerateError> {
        Generate::run(self.build()?)
    }
}
