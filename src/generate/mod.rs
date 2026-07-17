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
//! (Profiling a model from a dump — a `DumpProfiler`/`ModelInference` stage
//! ahead of the compiler — is wired into [`Generate::run`] itself: pass
//! [`GenerateBuilder::input`] to profile a dump into a base model, optionally
//! merging a `kind: overrides` `.config()` on top.)
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
pub mod output;
pub mod plan;
pub mod planners;
pub mod registry;
pub mod seed;
pub mod value;
pub mod verify;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::diagnostic::DiagnosticBag;
use crate::parser::SqlDialect;
use crate::profile::evidence::DumpProfile;
use crate::profile::{
    Confidence, Decision, DumpProfiler, ModelInference, Precedence, ProfileBudget, ProfileDepth,
};
use crate::synthetic::model::InferenceMode;
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
    /// Write to this path, creating or truncating the file.
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
    /// Whether to fully generate, only validate, only report the plan, or only
    /// emit the resolved model.
    pub mode: RunMode,
    /// Whether to build the `--explain` inference report.
    pub explain: bool,
    /// Whether to verify the generated SQL against the compiled plan and publish
    /// atomically only if the audit passes (`--verify`). Requires a filesystem
    /// SQL destination under [`RunMode::Generate`].
    pub verify: bool,
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
    /// model. Warning-only on success; see [`GenerateError::Diagnostics`]
    /// for the failure path.
    pub diagnostics: DiagnosticBag,
    /// Every place the resolved model's rules replay literal values derived
    /// from the source dump. Locations and rule kinds only, never values — the
    /// conservative `GEN-SOURCE-VALUES` safety notice is built from this.
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
    /// Structured problems (a missing/invalid config, a compile error, a bad
    /// merge) come back as [`GenerateError::Diagnostics`]; a request that
    /// cannot be satisfied at all (no source, or `kind: overrides` with no
    /// base model) comes back as [`GenerateError::InvalidInput`].
    pub fn run(request: GenerateRequest) -> Result<GenerateReport, GenerateError> {
        let GenerateRequest {
            input,
            config,
            output,
            emit,
            mut compile,
            render,
            mode,
            explain,
            verify,
            source,
        } = request;

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

        // Scan the final resolved model (explicit + inferred rules) for
        // source-derived literals.
        let source_values = model.source_value_uses();

        // Under `--verify` the SQL is rendered to a protected temp, audited, and
        // published atomically *only if* the audit passes; an emitted model is
        // published alongside it (never before verification succeeds). So the
        // ordinary direct emit is skipped for the verify path.
        if !verify {
            if let Some(target) = &emit {
                emit_model(&model, &plan, explicit_seed, target)?;
            }
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

        if verify {
            let rows = run_verified(&model, plan, explicit_seed, output, emit, render)?;
            return Ok(GenerateReport {
                rows_written: rows,
                ..report
            });
        }

        match mode {
            RunMode::Check | RunMode::EmitModel => Ok(report),
            RunMode::DryRun => Ok(GenerateReport {
                rows_written: plan.estimates.total_rows,
                ..report
            }),
            RunMode::Generate => {
                let engine_report = run_generate(GenerationEngine::new(plan), output, render)?;
                Ok(GenerateReport {
                    rows_written: engine_report.rows_written,
                    ..report
                })
            }
        }
    }
}

/// Render SQL to a protected temp beside the destination, verify it against the
/// compiled plan, and publish atomically only if the full audit passes. A failed
/// audit leaves every prior destination untouched and returns
/// [`GenerateError::VerificationFailed`]. When a resolved model is also requested
/// it is published *after* SQL verification succeeds; a failure publishing the
/// second file is reported as a precise partial publication rather than as a
/// pretended pairwise-atomic write.
fn run_verified(
    model: &SyntheticModel,
    plan: GenerationPlan,
    explicit_seed: Option<u64>,
    output: OutputTarget,
    emit: Option<OutputTarget>,
    render: RenderOptions,
) -> Result<u64, GenerateError> {
    let destination =
        match output {
            OutputTarget::Path(path) => path,
            OutputTarget::Discard => return Err(GenerateError::InvalidInput(
                "GEN-VERIFY-NO-FILE: --verify requires a filesystem SQL destination, not stdout"
                    .into(),
            )),
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
        GenerateError::InvalidInput(format!(
            "GEN-VERIFY-STAGE: cannot stage output beside `{}`: {error}",
            destination.display()
        ))
    })?;
    let temp_path = sql_output.temp_path().to_path_buf();
    let rows_written = {
        let mut renderer = SqlRenderer::new(sql_output.writer(), render);
        let engine_report = GenerationEngine::new(plan).run(&mut renderer)?;
        renderer.finish()?;
        engine_report.rows_written
    };

    // Audit the freshly rendered temp file.
    let report = verifier.verify_path(&temp_path)?;
    if !report.passed() {
        let failures: Vec<String> = report
            .failures()
            .map(|check| format!("{} ({})", check.name, check.detail))
            .collect();
        // Dropping `sql_output` removes the temp; the destination is untouched.
        return Err(GenerateError::VerificationFailed(failures));
    }

    // Verification passed: publish the SQL, then the model (if any).
    let mut outputs = vec![sql_output];
    if let Some((path, yaml)) = emit_plan {
        let mut model_output = AtomicOutput::create(&path).map_err(|error| {
            GenerateError::InvalidInput(format!(
                "GEN-VERIFY-STAGE: cannot stage model beside `{}`: {error}",
                path.display()
            ))
        })?;
        use std::io::Write;
        model_output
            .writer()
            .write_all(yaml.as_bytes())
            .map_err(|error| GenerateError::InvalidInput(format!("GEN-EMIT-IO: {error}")))?;
        outputs.push(model_output);
    }

    publish_in_order(outputs).map_err(|partial| {
        GenerateError::InvalidInput(format!("GEN-VERIFY-PARTIAL-PUBLISH: {partial}"))
    })?;

    Ok(rows_written)
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
        (None, None) => Err(GenerateError::InvalidInput(
            "GEN-REQUEST-SOURCE: at least one of `.input()` or `.config()` is required".into(),
        )),
        (None, Some(config_path)) => {
            match ConfigLoader::load(config_path).map_err(GenerateError::Diagnostics)? {
                SyntheticFile::Model(model) => Ok(ResolvedModel {
                    model,
                    diagnostics: DiagnosticBag::default(),
                    decisions: Vec::new(),
                }),
                SyntheticFile::Overrides(_) => Err(GenerateError::InvalidInput(format!(
                    "GEN-OVERRIDES-NO-BASE: `{}` is a `kind: overrides` document but no base \
                     model is available; supply a source dump to profile (`.input()`) so the \
                     overrides have a base to merge onto",
                    config_path.display()
                ))),
            }
        }
        (Some(input_path), config) => {
            let profile = profile_source(input_path, source)?;
            let inference = ModelInference::standard()
                .infer(&profile.schema, &profile)
                .map_err(|error| {
                    GenerateError::InvalidInput(format!("GEN-INFER-FAILED: {error}"))
                })?;

            let mut diagnostics = DiagnosticBag::default();
            push_coded_warnings(&mut diagnostics, "GEN-PROFILE", &profile.warnings);
            push_coded_warnings(&mut diagnostics, "GEN-INFER", &inference.warnings);

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
                                "GEN-CONFIG-COMPLETE-MODEL",
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
    GenerateError::InvalidInput(format!("GEN-SOURCE-IO: {error}"))
}

/// Fold plain-string warnings (whose message often already begins with a
/// `GEN-*` code) into structured diagnostics, splitting the leading code off
/// the message when present and falling back to `default_code` otherwise.
fn push_coded_warnings(bag: &mut DiagnosticBag, default_code: &str, messages: &[String]) {
    for message in messages {
        match message.split_once(": ") {
            Some((code, rest))
                if code.starts_with("GEN-") && !code.contains(char::is_whitespace) =>
            {
                bag.warning(code.to_string(), String::new(), rest.to_string());
            }
            _ => {
                bag.warning(default_code.to_string(), String::new(), message.clone());
            }
        }
    }
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
/// records the seed only for an explicitly-seeded run. Shared by the direct
/// [`emit_model`] write and the `--verify` atomic-emit path.
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
    emit.freeze_row_counts(&resolved);
    emit.defaults.inference = InferenceMode::Disabled;
    emit.seed = explicit_seed;

    serde_yaml_ng::to_string(&emit)
        .map_err(|error| GenerateError::InvalidInput(format!("GEN-EMIT-SERIALIZE: {error}")))
}

fn emit_model(
    model: &SyntheticModel,
    plan: &GenerationPlan,
    explicit_seed: Option<u64>,
    target: &OutputTarget,
) -> Result<(), GenerateError> {
    let yaml = resolved_model_yaml(model, plan, explicit_seed)?;

    match target {
        OutputTarget::Path(path) => fs::write(path, yaml).map_err(|error| {
            GenerateError::InvalidInput(format!(
                "GEN-EMIT-IO: failed to write `{}`: {error}",
                path.display()
            ))
        }),
        // The builder/CLI only ever routes emit to a real path (a stdout emit
        // is spooled through a temp file), so this arm is unreachable in
        // practice; discard rather than panic if a caller wires it directly.
        OutputTarget::Discard => Ok(()),
    }
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
    emit: Option<PathBuf>,
    compile: CompileOptions,
    render: RenderOptions,
    mode: RunMode,
    explain: bool,
    verify: bool,
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

    /// The path rendered SQL is written to (created/truncated). Required
    /// under `RunMode::Generate`; optional under `Check`/`DryRun`, which
    /// never write SQL regardless.
    pub fn output(mut self, path: impl Into<PathBuf>) -> Self {
        self.output = Some(path.into());
        self
    }

    /// The path the resolved, self-contained model is written to
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

    /// Whether to verify the generated SQL against the compiled plan and
    /// publish atomically only if the audit passes. Requires a filesystem SQL
    /// destination (`.output()`); a stdout destination is a usage error.
    pub fn verify(mut self, verify: bool) -> Self {
        self.verify = verify;
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
            return Err(GenerateError::InvalidInput(
                "GEN-VERIFY-MODE: --verify generates and publishes; it cannot be combined with \
                 check/dry-run/emit-only modes"
                    .into(),
            ));
        }
        let output = match (self.mode, self.output) {
            (RunMode::Generate, Some(path)) => OutputTarget::Path(path),
            (RunMode::Generate, None) => {
                return Err(GenerateError::InvalidInput(
                    "GEN-REQUEST-OUTPUT: `.output()` is required under `RunMode::Generate`".into(),
                ))
            }
            (RunMode::Check | RunMode::DryRun | RunMode::EmitModel, Some(path)) => {
                OutputTarget::Path(path)
            }
            (RunMode::Check | RunMode::DryRun | RunMode::EmitModel, None) => OutputTarget::Discard,
        };

        if self.verify && !matches!(output, OutputTarget::Path(_)) {
            return Err(GenerateError::InvalidInput(
                "GEN-VERIFY-NO-FILE: --verify requires a filesystem SQL destination".into(),
            ));
        }

        Ok(GenerateRequest {
            input: self.input,
            config: self.config,
            output,
            emit: self.emit.map(OutputTarget::Path),
            compile: self.compile,
            render: self.render,
            mode: self.mode,
            explain: self.explain,
            verify: self.verify,
            source: self.source,
        })
    }

    /// Build the request and run it end to end.
    pub fn run(self) -> Result<GenerateReport, GenerateError> {
        Generate::run(self.build()?)
    }
}
