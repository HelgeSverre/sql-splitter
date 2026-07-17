# Library API

`sql-splitter` exposes `generate` as a Rust library, not just a CLI. Two
entry points cover the same ground the CLI does: a one-call convenience
builder, and a staged API for callers that need to drive an individual
stage (a custom registry, mid-pipeline inspection, or a non-file sink).

Both are re-exported from `sql_splitter::generate`; the renderer itself
lives in `sql_splitter::render` and is re-exported there too.

## The convenience builder

[`Generate::builder()`](../../src/generate/mod.rs) returns a
[`GenerateBuilder`] that mirrors the CLI's flags. `.run()` drives the whole
pipeline — dump profiling (if `.input()` is set), model/overrides loading,
merging, compiling, and rendering — in one call.

```rust
use sql_splitter::generate::Generate;

let dir = tempfile::tempdir()?;
let output = dir.path().join("synthetic.sql");

let report = Generate::builder()
    .config("model.yaml")
    .output(&output)
    .seed(42)
    .run()?;

println!("{} rows generated", report.rows_written);
# Ok::<(), anyhow::Error>(())
```

Builder methods, matching the CLI flags one for one:

| Method | CLI equivalent |
| --- | --- |
| `.input(path)` | `[INPUT]` — profile a dump into a base model |
| `.config(path)` | `-c, --config` |
| `.output(path)` | `-o, --output` |
| `.emit(path)` | `--emit-config` |
| `.explain(bool)` | `--explain` |
| `.input_dialect(dialect)` | `--input-dialect` |
| `.profile_depth(depth)` | `--profile-depth` |
| `.profile_sample(n)` | `--profile-sample` |
| `.output_dialect(dialect)` | `--dialect` (default when unset: the model's `output.dialect`, else the source/input dialect (preserve-source), else `mysql`) |
| `.seed(u64)` | `--seed` |
| `.mssql_production_style(bool)` | `--mssql-production-style` |
| `.mssql_go(n)` | `--mssql-go` |
| `.table_scale(table, factor)` | `--table-scale` (returns `Result<Self, GenerateError>` — a non-finite/negative factor is rejected immediately) |
| `.compile(CompileOptions)` | `--rows`/`--max-rows`/`--table-rows`/`--tables`/`--exclude` (set as a group via [`CompileOptions`]) |
| `.verify(bool)` | `--verify` |
| `.mode(RunMode)` | `--check`/`--dry-run`/(the default `Generate`)/`EmitModel` |
| `.run()` | executes the request, returning [`GenerateReport`] or a [`GenerateError`] |

`GenerateReport` carries `rows_written`, `effective_seed` (always populated,
even for an unseeded run — the drawn entropy is recorded so the run can be
reproduced), `diagnostics` (a [`DiagnosticBag`] of warnings), `source_values`
(see [Profiling and privacy](profiling-and-privacy.md)), and `explain` (when
`.explain(true)` was set).

## The staged API

Drive registry → compiler → engine → renderer directly when you need a
custom [`ExtensionRegistry`] (statically linked custom generators/planners),
want to inspect the compiled [`GenerationPlan`] before running it, or need a
[`RowSink`] that isn't a plain file.

```rust
use sql_splitter::generate::{CompileOptions, ExtensionRegistry, GenerationEngine, ModelCompiler};
use sql_splitter::render::{RenderOptions, SqlRenderer};
use sql_splitter::synthetic::SyntheticFile;

let model = SyntheticFile::parse_str(model_yaml)?
    .into_model()
    .expect("is a complete model");

let registry = ExtensionRegistry::standard(); // or ::new() + register_generator/_modifier/_planner
let plan = ModelCompiler::new(registry).compile(model, CompileOptions::default())?;

let mut renderer = SqlRenderer::new(Vec::new(), RenderOptions::default());
GenerationEngine::new(plan).run(&mut renderer)?;
let sql_bytes = renderer.finish()?;
# Ok::<(), anyhow::Error>(())
```

Both examples above are exercised as real, compiled doctests in
`src/generate/mod.rs` (`cargo test --doc`).

### Stages

1. **`ExtensionRegistry`** — the generator/modifier/planner/heuristic
   catalog. `ExtensionRegistry::new()` is empty; `ExtensionRegistry::standard()`
   preloads every built-in from [Generators](generators.md) and
   [Planners](planners.md). Register your own with `.register_generator()`,
   `.register_modifier()`, `.register_planner()` (each returns
   `Result<(), DiagnosticBag>` — a duplicate/shadowing kind name is a
   compile-time registration error, not a panic).

2. **`ModelCompiler`** — `ModelCompiler::new(registry)` or
   `ModelCompiler::standard()`. `.compile(model, options)` takes a
   `SyntheticModel` and [`CompileOptions`] (the library form of `--scale`,
   `--rows`, `--max-rows`, `--table-rows`/`--table-scale`, `--tables`,
   `--exclude`, and `--seed`), and returns a `Result<GenerationPlan,
   DiagnosticBag>`. Compilation never short-circuits on the first error —
   every independent diagnostic is collected. Warnings survive success in
   `GenerationPlan::diagnostics`.

3. **`GenerationEngine`** — `GenerationEngine::new(plan).run(sink)` drives
   the plan's execution phases (table, family, deferred-constraint) against
   any `&mut dyn RowSink`, most commonly a `SqlRenderer`. Returns
   `Result<EngineReport, GenerateError>`.

4. **`SqlRenderer`** (in `sql_splitter::render`, re-exported from
   `sql_splitter::generate`) — implements `RowSink`, so it plugs directly
   into `GenerationEngine::run`. `.finish()` flushes and returns the
   underlying writer.

### Profiling and inference (ahead of the compiler)

To build a `SyntheticModel` from a dump instead of hand-authoring YAML:

```rust
use sql_splitter::profile::{DumpProfiler, ProfileDepth};
use sql_splitter::generate::ExtensionRegistry;
use sql_splitter::synthetic::merge::ModelMerger;

let registry = ExtensionRegistry::standard();
let profile = DumpProfiler::builder()
    .depth(ProfileDepth::Basic)
    .build()
    .profile_path(dump_path)?;

let inference = sql_splitter::profile::ModelInference::standard()
    .infer(&profile.schema, &profile)?;
let model = inference.model;

// If a `kind: overrides` document is present, merge it before compiling:
let (model, _merge_warnings) = ModelMerger::merge(model, overrides)?;
# Ok::<(), anyhow::Error>(())
```

`DumpProfile` exposes its normalized `schema: PortableSchema` directly (the
dump is read once — schema and row evidence are gathered in the same pass),
so `.schema` is always available without a second read.

## `ModelMerger`

```rust
pub fn merge(
    base: SyntheticModel,
    patch: SyntheticOverrides,
) -> Result<(SyntheticModel, DiagnosticBag), DiagnosticBag>
```

Note the **success case also returns a `DiagnosticBag`**, not just the
merged model — warning-severity diagnostics (for example, a `warn`-policy
source fingerprint mismatch) are never silently discarded on an otherwise
successful merge. Always inspect the returned bag, even when `merge`
succeeds.

## Errors and diagnostics

`GenerateError` is the library's error type for the convenience builder and
staged pipeline; `GenerateError::Diagnostics(DiagnosticBag)` is the variant
carrying model/compile validation failures. `DiagnosticBag` collects
`Diagnostic { code, severity, path, message, help, related }` entries — see
[Diagnostics](diagnostics.md) for the stable code catalog. Both `Diagnostic`
and `DiagnosticBag` implement `Display`, producing the same human-readable
form the CLI prints to stderr.

## See also

- [Model reference](model-reference.md) — the `SyntheticModel`/`SyntheticOverrides` YAML shape.
- [Profiling and privacy](profiling-and-privacy.md) — what `DumpProfiler`/`ModelInference` observe and infer.
- [Diagnostics](diagnostics.md) — every stable `GEN-*` code.
