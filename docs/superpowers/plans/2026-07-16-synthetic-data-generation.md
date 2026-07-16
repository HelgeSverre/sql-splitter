# Synthetic Data Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `sql-splitter generate` and a public Rust API that profile SQL dumps, compile versioned YAML models, and stream realistic, relationally consistent synthetic SQL.

**Architecture:** Extend the production schema/parser foundation with a bounded neutral profiler, compile complete models and partial overrides through a typed registry into an immutable `GenerationPlan`, and execute that plan through an allocation-lean renderer. YAML, descriptor lookup, inference, ownership checks, count resolution, and seed derivation happen before the row hot path; planners use protected bounded state or disk spools for correlated families.

**Tech Stack:** Rust 2021, clap 4, serde/`serde_yaml_ng`, schemars 1, rand 0.10 + `rand_chacha` 0.10, fake 5, chrono 0.4, ahash, tempfile, Criterion, existing SQL parser/schema/conversion/validation modules.

**Canonical spec:** [`docs/superpowers/specs/2026-07-16-synthetic-data-generation-design.md`](../specs/2026-07-16-synthetic-data-generation-design.md)

## Global Constraints

- SQL input and output support MySQL, PostgreSQL, SQLite, and MSSQL.
- `kind: model` is self-contained; `kind: overrides` is a partial patch requiring a source/base model.
- Emitted models contain exact final integer counts, explicit resolved rules, and `defaults.inference: disabled`.
- A missing seed means fresh entropy; omitted table seed inherits, integer overrides, and YAML `null` always randomizes that table.
- Adding unrelated tables, columns, or operators must not reshuffle existing seeded streams.
- `--scale` and `--rows` conflict. Relationship children derive from final parent counts and are never globally scaled twice.
- `--exclude` wins; excluding a required dependency is a compile error.
- Unknown YAML fields, duplicate keys, import path collisions, unresolved references, and column ownership conflicts are errors before output starts.
- YAML expressions, remote/recursive imports, arbitrary config execution, runtime CLI plugins, dynamic libraries, and WASM plugins are out of scope.
- Inferred credential fields use synthetic-only defaults; explicit source-derived credential rules remain allowed and warn.
- Source-derived literal warnings never print values and are not suppressed by `--quiet`.
- Spools and verification files use exclusive unpredictable paths and Unix mode `0600`; memory is never proportional to total input rows.
- Correctness and bounded memory are release gates. The 20% configurable-generation overhead figure is a measured optimization goal.
- Use TDD for every task. Do not advance past a phase checkpoint with failing tests or clippy warnings introduced by the phase.

## Execution Sequence

Execute phases strictly in numeric order: Phase 0 (Tasks 1–6), Phase 1 (Tasks 7–16), Phase 2 (Tasks 17–21), Phase 3 (Tasks 22–29), Phase 4 (Tasks 30–34), then optional/deferred Phase 5 (Tasks 35–36). Phase 1 is printed before Phase 0 in this consolidated document, so follow task numbers rather than page order; all later phases are in execution order.

## Planned File Structure

```text
src/
├── diagnostic.rs                 # Stable human/JSON diagnostics
├── fake_data.rs                  # Shared semantic fake-value helpers
├── synthetic/
│   ├── mod.rs                    # Public model exports
│   ├── model.rs                  # Complete YAML model types
│   ├── overrides.rs              # Partial patch types
│   ├── config.rs                 # YAML parse, role/version checks, imports
│   ├── merge.rs                  # Source/model/override merge
│   └── schema.rs                 # Production Schema <-> portable schema
├── generate/
│   ├── mod.rs                    # Builder façade and public exports
│   ├── value.rs                  # Typed generated values
│   ├── seed.rs                   # Stable RNG stream derivation
│   ├── registry.rs               # Descriptor/factory catalogs
│   ├── plan.rs                   # Immutable compiled plan
│   ├── compiler.rs               # Validation, selection, counts, ownership
│   ├── engine.rs                 # Table/family/deferred execution
│   ├── output.rs                 # Atomic/protected output lifecycle
│   ├── verify.rs                 # Exact and approximate verification
│   ├── generators/
│   │   ├── mod.rs
│   │   ├── core.rs
│   │   ├── semantic.rs
│   │   ├── observed.rs
│   │   └── relation.rs
│   └── planners/
│       ├── mod.rs
│       ├── interval.rs
│       ├── progress.rs
│       ├── order_family.rs
│       └── structural.rs
├── profile/
│   ├── mod.rs                    # Public neutral evidence API
│   ├── evidence.rs               # Bounded report types
│   ├── sketches.rs               # Reservoir/top-k/distinct/histogram stats
│   ├── profiler.rs               # Streaming SQL scan
│   └── heuristics/
│       ├── mod.rs
│       ├── schema.rs
│       ├── semantic.rs
│       ├── distribution.rs
│       ├── relationship.rs
│       ├── planner.rs
│       └── credential.rs
├── render/
│   ├── mod.rs                    # Renderer API
│   ├── sql_string.rs             # Allocation-free dialect escaping
│   ├── row_batch.rs              # Reusable INSERT/COPY buffers
│   ├── random.rs                 # Buffered unbiased random sampling
│   ├── ddl.rs                    # Normalized DDL rendering/filtering
│   └── sql.rs                    # Dialect row/session renderer
└── cmd/
    └── generate.rs               # clap surface and report routing

tests/
├── generate_api_test.rs
├── generate_config_test.rs
├── generate_compiler_test.rs
├── generate_engine_test.rs
├── generate_profile_test.rs
├── generate_planner_test.rs
├── generate_cli_test.rs
├── generate_verify_test.rs
├── generate_output_test.rs
├── generate_filter_test.rs
└── fixtures/generate/
    ├── simple.yaml
    ├── multi_tenant.yaml
    └── production_shape.sql

benches/generate_bench.rs
schemas/generate-config.schema.json             # Phase 5
website/public/schemas/generate-config.schema.json
```

The focused modules above are deliberate. Do not put the model, compiler, registry, profiler, engine, and renderer in one `generate.rs` file.

---

## Phase 1: Generate from a hand-authored model

> **Execution order:** Complete Phase 0 later in this document before starting this phase. The phase was intentionally retained as one contiguous block during plan consolidation; the task and checkpoint numbers, not page order, define execution order.

### Task 7: Build the typed extension registry

**Files:**
- Create: `src/generate/registry.rs`
- Create: `src/generate/generators/mod.rs`
- Create: `src/generate/planners/mod.rs`
- Modify: `src/generate/mod.rs`
- Test: `tests/generate_compiler_test.rs`

**Interfaces:**
- Produces: `ExtensionRegistry::new()`, `ExtensionRegistry::standard()`
- Produces: `GeneratorFactory`, `ModifierFactory`, `PlannerFactory`
- Produces: descriptor types and ownership/read declarations
- Consumes: `GeneratorConfig`, `PlannerConfig`, `PortableColumn`, `DiagnosticBag`

- [ ] **Step 1: Add failing registration tests**

```rust
#[test]
fn registry_rejects_duplicate_kinds_and_resolves_aliases() {
    let mut registry = ExtensionRegistry::new();
    registry.register_generator(Box::new(ConstantFactory)).unwrap();
    assert_eq!(registry.generator("const").unwrap().descriptor().kind, "constant");
    let err = registry.register_generator(Box::new(ConstantFactory)).unwrap_err();
    assert!(err.to_string().contains("GEN-REGISTRY-DUPLICATE"));
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_compiler_test registry_rejects_duplicate_kinds_and_resolves_aliases -- --exact`

Expected: compile failure because `ExtensionRegistry` is absent.

- [ ] **Step 3: Define factory/runtime boundaries**

```rust
pub trait GeneratorFactory: Send + Sync {
    fn descriptor(&self) -> &'static GeneratorDescriptor;
    fn compile(
        &self,
        config: &Value,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag>;
}

pub trait CompiledGenerator: Send {
    fn generate(
        &mut self,
        context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError>;
}

pub trait CompiledModifier: Send {
    fn apply(
        &mut self,
        context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError>;
}
```

Define the equivalent planner trait from the spec. Descriptor fields include `kind`, `aliases`, `summary`, `arguments`, accepted `SqlTypeFamily` values, `writes`, `reads`, determinism, buffering, and verification capabilities. Task 20 adds the heuristic catalog only after neutral evidence exists.

- [ ] **Step 4: Implement deterministic catalogs**

Use `BTreeMap<String, Box<dyn ...>>` for discovery order and a second alias map. Registration validates primary kinds and aliases across each catalog; aliases cannot shadow primary names. `standard()` installs only factories implemented by the current phase.

- [ ] **Step 5: Test descriptor discovery and collisions**

Run: `cargo test --test generate_compiler_test registry`

Expected: PASS for primary resolution, aliases, duplicate primary, duplicate alias, and deterministic descriptor listing.

- [ ] **Step 6: Commit**

```bash
git add src/generate tests/generate_compiler_test.rs
git commit -m "feat(generate): add typed extension registry"
```

### Task 8: Merge source, model, and overrides into a complete candidate

**Files:**
- Create: `src/synthetic/merge.rs`
- Modify: `src/synthetic/mod.rs`
- Test: `tests/generate_config_test.rs`

**Interfaces:**
- Produces: `ModelMerger::merge(base: SyntheticModel, overrides: SyntheticOverrides) -> Result<(SyntheticModel, DiagnosticBag), DiagnosticBag>` — on success returns the merged model plus a warning-only bag (e.g. fingerprint `warn`), so warnings survive a successful merge; on any error-severity diagnostic returns `Err(bag)`. (Design-audit fix: `into_result` discards the bag on success, which would silently drop the fingerprint `warn` diagnostic.)
- Produces: compatibility diagnostics for tables, columns, and types
- Consumes: portable schema conversion and partial override types

- [ ] **Step 1: Write failing legal-override and schema-mismatch tests**

```rust
#[test]
fn overrides_change_rules_but_not_source_schema() {
    let base = model_with_users_email("varchar(255)");
    let rules = overrides_with_email_generator("internet.email");
    let (merged, _warnings) = ModelMerger::merge(base.clone(), rules).unwrap();
    assert_eq!(merged.tables["users"].columns["email"].generator.kind(), "internet.email");

    let structural = overrides_with_email_type("bigint");
    let err = ModelMerger::merge(base, structural).unwrap_err();
    assert!(err.to_string().contains("GEN-SCHEMA-MISMATCH"));
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_config_test overrides_change_rules_but_not_source_schema -- --exact`

Expected: compile failure because `ModelMerger` is absent.

- [ ] **Step 3: Implement explicit field-wise patching**

Do not serialize to generic YAML and merge at this layer. Patch typed fields:

```rust
impl ModelMerger {
    pub fn merge(
        mut base: SyntheticModel,
        patch: SyntheticOverrides,
    ) -> Result<SyntheticModel, DiagnosticBag> {
        match patch.seed {
            RootSeedOverride::Inherit => {}
            RootSeedOverride::Random => base.seed = None,
            RootSeedOverride::Fixed(seed) => base.seed = Some(seed),
        }
        if let Some(output) = patch.output { output.apply_to(&mut base.output); }
        for (name, table_patch) in patch.tables {
            match base.tables.get_mut(&name) {
                Some(table) => table_patch.apply_to(table, &mut diagnostics),
                None => diagnostics.error(
                    "GEN-MISSING-TABLE",
                    format!("tables.{name}"),
                    "override table does not exist in the source/base model",
                ),
            }
        }
        diagnostics.into_result(base)
    }
}
```

Lists replace whole lists. Generator/modifier/planner changes are legal. A schema override may only assert matching facts; any requested type/nullability/key/existence change reports `GEN-SCHEMA-MISMATCH`.

- [ ] **Step 4: Add fingerprint-policy behavior**

`ignore` emits nothing, `warn` adds `GEN-SOURCE-FINGERPRINT`, and `require` adds an error. Fingerprint differences never mutate the model silently.

- [ ] **Step 5: Verify merge behavior**

Run: `cargo test --test generate_config_test merge`

Expected: PASS for legal generation overrides, missing table/column, structural mismatch, list replacement, and all fingerprint policies.

- [ ] **Step 6: Commit**

```bash
git add src/synthetic tests/generate_config_test.rs
git commit -m "feat(generate): merge models and overrides safely"
```

### Task 9: Compile selection and exact row counts

**Files:**
- Create: `src/generate/plan.rs`
- Create: `src/generate/compiler.rs`
- Modify: `src/generate/mod.rs`
- Test: `tests/generate_compiler_test.rs`

**Interfaces:**
- Produces: `CompileOptions`, `TableCountOverride`, `GenerationPlan`, `PlannedTable`
- Produces: `ModelCompiler::compile(model, options) -> Result<GenerationPlan, DiagnosticBag>`
- Consumes: `ExtensionRegistry`, `SyntheticModel`, `SchemaGraph`, `SeedRoot`

- [ ] **Step 1: Add failing count-precedence tests**

```rust
#[test]
fn child_counts_are_not_scaled_twice() {
    let model = customers_orders_model(1_000, 4_000, 4.0);
    let options = CompileOptions { scale: Some(0.1), ..Default::default() };
    let plan = compiler().compile(model, options).unwrap();
    assert_eq!(plan.table("customers").unwrap().rows, 100);
    assert_eq!(plan.table("orders").unwrap().rows, 400);
}

#[test]
fn absolute_table_rows_win_and_max_rows_is_last() {
    let options = CompileOptions {
        scale: Some(0.1),
        table_rows: vec![TableCountOverride::rows("users", 500)],
        max_rows: Some(300),
        ..Default::default()
    };
    assert_eq!(compiler().compile(users_model(10_000), options).unwrap().table("users").unwrap().rows, 300);
}
```

Also test `--rows`/`--scale` conflict, table rows/table scale conflict, child table scale, minimum-child impossibility, required child distribution, deterministic stochastic rounding before emission, and `rows.kind: observed` with and without a resolvable source count.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_compiler_test child_counts_are_not_scaled_twice -- --exact`

Expected: compile failure because the compiler is absent.

- [ ] **Step 3: Define the immutable plan**

```rust
pub struct GenerationPlan {
    pub input_dialect: Option<SqlDialect>,
    pub output: CompiledOutput,
    pub tables: Vec<PlannedTable>,
    pub phases: Vec<ExecutionPhase>,
    pub diagnostics: Vec<Diagnostic>,
    pub estimates: PlanEstimates,
}
```

`plan.diagnostics` accumulates non-error diagnostics that must survive a
successful compile: the compiler drains the warning-only bag returned by
`ModelMerger::merge` (fingerprint `warn`, etc.) into it, then appends its own
compile-stage warnings. Warnings therefore reach the CLI/JSON report even when
compilation succeeds; error-severity diagnostics still abort via `Err(bag)`.

```rust
// (struct continues)

pub struct PlannedTable {
    pub name: String,
    pub rows: u64,
    pub schema: PortableTable,
    pub seed: ResolvedTableSeed,
    pub columns: Vec<PlannedColumn>,
    pub relationships: Vec<CompiledRelationship>,
    pub planners: Vec<Box<dyn CompiledPlanner>>,
}
```

Task 9 also defines the referenced types so the plan compiles: `PlannedColumn`, `ColumnOwner`, `ExecutionPhase` (initially the `Table` variant; `Family`/`DeferredConstraints` added in Task 22), and `CompiledRelationship`. Tasks 10, 13, and 22 populate and extend these. `CompiledRelationship` is produced here and completed in Task 13.

`PlanEstimates` fields for temporary-storage and verification cost are populated incrementally; family-state/spool estimates (Task 22) and verification cost (Task 26) fill in during Phase 3.

Keep plan fields read-only after construction. If trait-object fields prevent `Debug`, implement a manual descriptor-based `Debug` rather than dropping plan observability.

- [ ] **Step 4: Implement count traversal in dependency order**

Resolve root counts first; apply global control, root per-table override, then max. Traverse `SchemaGraph`; derive children from final parents, apply child override and max, validate distribution bounds, then continue to descendants. Use a stable seeded rounding stream named `rows.rounding`; emitted models store the resulting integer.

Define compiler completeness explicitly: every selected table has a portable schema, a resolvable row rule, and—after Task 10—an owner for each generated column. `kind: overrides` is never complete by itself. `rows.kind: observed` resolves only from an attached source/profile count; without one it is `GEN-ROWS-OBSERVED-MISSING`. Resolved/emitted models retain `kind: observed` and store the resolved integer `count` (matching the spec's emitted-model example); they do not convert the rule to `fixed`. `relation.children.distribution` is mandatory in a complete model and may be omitted only in an override that inherits it from a base.

- [ ] **Step 5: Implement table selection/exclusion**

Compile globs once with `glob::Pattern`. `--tables` chooses the initial set, `--exclude` removes matches, then dependency validation reports `GEN-EXCLUDED-DEPENDENCY` with the full parent path. Never silently re-add a table.

- [ ] **Step 6: Verify count and selection rules**

Run: `cargo test --test generate_compiler_test count -- --nocapture && cargo test --test generate_compiler_test exclude -- --nocapture`

Expected: PASS for all precedence, descendant recalculation, and dependency-path cases.

- [ ] **Step 7: Commit**

```bash
git add src/generate tests/generate_compiler_test.rs
git commit -m "feat(generate): compile selection and exact row counts"
```

### Task 10: Compile ownership, types, and dependency graphs

**Files:**
- Modify: `src/generate/compiler.rs`
- Modify: `src/generate/plan.rs`
- Test: `tests/generate_compiler_test.rs`

**Interfaces:**
- Produces: `PlannedColumn`, `ColumnOwner`, column/family dependency order
- Consumes: registry descriptor write/read sets and portable SQL types

- [ ] **Step 1: Add failing multi-diagnostic tests**

```rust
#[test]
fn compiler_reports_all_independent_ownership_and_type_errors() {
    let mut model = invalid_model();
    model.tables["orders"].columns["total"].generator = Some(generator("integer"));
    model.tables["orders"].planners.push(order_planner_owning("total"));
    model.tables["users"].columns["email"].generator = Some(generator("integer"));

    let err = compiler().compile(model, Default::default()).unwrap_err();
    assert!(err.has_code("GEN-COLUMN-OWNER-CONFLICT"));
    assert!(err.has_code("GEN-GENERATOR-TYPE"));
    assert_eq!(err.errors().count(), 2);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_compiler_test compiler_reports_all_independent_ownership_and_type_errors -- --exact`

Expected: FAIL because ownership/type validation is incomplete.

- [ ] **Step 3: Assign exactly one owner per generated column**

```rust
pub enum ColumnOwner {
    Generator { kind: String, compiled: Box<dyn CompiledGenerator> },
    Planner { kind: String, planner_index: usize },
    DatabaseDefault,
    GeneratedByDatabase,
}
```

Generated/identity columns may be omitted only when the renderer/database can supply them and relationships do not need the generated value during generation. Every other non-omitted column gets one owner. Validate descriptor accepted types and modifier input/output compatibility.

When a hand-authored complete model has an unowned column, `defaults.inference: schema` runs only schema/name/constraint heuristics that require no observations; `defaults.inference: disabled` reports `GEN-COLUMN-OWNER-MISSING`. Never silently run data-profile heuristics without source evidence.

- [ ] **Step 4: Build read/write dependency graphs**

Create graph nodes for columns and planners; edges point from reads to writes. Topologically sort and report every strongly connected component as `GEN-COLUMN-CYCLE` unless a registered planner owns the whole cycle. Table FK cycles move constraints to `DeferredConstraints`; impossible non-null construction cycles are errors.

- [ ] **Step 5: Verify diagnostic aggregation**

Run: `cargo test --test generate_compiler_test ownership -- --nocapture`

Expected: PASS for conflict, missing owner, wrong type, unknown kind, unresolved relationship name, column cycle, and allowed database default.

- [ ] **Step 6: Commit**

```bash
git add src/generate/compiler.rs src/generate/plan.rs tests/generate_compiler_test.rs
git commit -m "feat(generate): compile ownership and dependencies"
```

### Task 11: Implement Phase 1 core generators and modifiers

**Files:**
- Create: `src/generate/generators/core.rs`
- Modify: `src/generate/generators/mod.rs`
- Modify: `src/generate/registry.rs`
- Test: `tests/generate_engine_test.rs`

**Interfaces:**
- Produces factories for `constant`, `null`, `sequence`, `copy`, `template`, `pattern`, `database_default`, `json_value`, typed random, choice, weighted choice
- Produces modifiers `null_rate`, `unique`, string transforms, `clamp`, `round`, `format`
- Consumes: `GeneratedValue`, `RowContext`, per-operator RNG streams

- [ ] **Step 1: Add table-driven failing generator tests**

```rust
#[test]
fn phase_one_generators_produce_type_safe_values() {
    let cases = [
        ("constant", yaml("{ kind: constant, value: 7 }"), SqlTypeFamily::Integer),
        ("sequence", yaml("{ kind: sequence, start: 10, step: 2 }"), SqlTypeFamily::Integer),
        ("choice", yaml("{ kind: choice, values: [a, b] }"), SqlTypeFamily::Text),
        ("uuid", yaml("{ kind: uuid }"), SqlTypeFamily::Uuid),
        ("json_value", yaml("{ kind: json_value }"), SqlTypeFamily::Json),
    ];
    for (name, config, family) in cases {
        let values = generate_three(name, config, family, 42).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.iter().all(|v| v.compatible_with(&family)));
    }
}
```

Add focused tests for null on non-nullable columns, empty/invalid choices, decimal bounds/scale, sequence overflow, template unknown fields, unique exhaustion, and modifier order.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_engine_test phase_one_generators_produce_type_safe_values -- --exact`

Expected: FAIL because standard factories are not installed.

- [ ] **Step 3: Compile configs into enum-backed built-ins**

```rust
enum CoreGenerator {
    Constant(GeneratedValue),
    Null,
    Sequence(SequenceState),
    Copy { source: usize },
    Template(CompiledTemplate),
    Pattern(CompiledPattern),
    DatabaseDefault,
    Json(GenericJsonState),
    Integer(UniformInteger),
    Decimal(UniformDecimal),
    Boolean { probability: f64 },
    String(UniformString),
    Bytes(UniformBytes),
    Uuid,
    Choice(ChoiceState),
    WeightedChoice(WeightedChoiceState),
}
```

Factories validate YAML once and return this compiled enum. Do not use string dispatch in `generate`. Implement weighted selection with cumulative integer weights or `rand`'s validated distribution; reject negative, NaN, infinite, or all-zero weights.

- [ ] **Step 4: Implement ordered modifiers**

Uniqueness uses a bounded set and configured attempts. `on_exhaustion: error|widen|warn` is resolved at compile time; `widen` is legal only for generators declaring widening support. String modifiers operate on Unicode scalar boundaries and enforce SQL length after transformation.

- [ ] **Step 5: Run statistical smoke tests with tolerant assertions**

For 10,000 seeded booleans at `p=0.25`, assert the true rate lies in `0.22..0.28`; do not assert exact random counts. For weighted choice `9:1`, assert the majority value lies in `0.86..0.94`.

Run: `cargo test --test generate_engine_test generator -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/generate/generators src/generate/registry.rs tests/generate_engine_test.rs
git commit -m "feat(generate): add core generators and modifiers"
```

### Task 12: Implement semantic, credential, and temporal generators

**Files:**
- Create: `src/generate/generators/semantic.rs`
- Create: `src/fake_data.rs`
- Modify: `src/generate/generators/mod.rs`
- Modify: `src/generate/registry.rs`
- Refactor: `src/redactor/strategy/fake.rs`
- Test: `tests/generate_engine_test.rs`
- Test: `tests/redact_test.rs`

**Interfaces:**
- Produces: the Phase 1 semantic catalog from the spec
- Produces: credential generators and date/time/duration/before/after generators
- Reuses: one internal fake-data catalog from both generate and redact

The standard registry must contain this complete initial semantic inventory:

- Person: `person.first_name`, `person.last_name`, `person.full_name`, `person.username`, `person.title`.
- Internet: `internet.email`, `internet.domain`, `internet.url`, `internet.ipv4`, `internet.ipv6`, `internet.user_agent`.
- Contact/organization: `phone.number`, `phone.country_code`, `company.name`, `company.department`, `company.job_title`.
- Address: `address.line1`, `address.line2`, `address.city`, `address.region`, `address.postcode`, `address.country`, `address.latitude`, `address.longitude`.
- Commerce/text: `commerce.product_name`, `commerce.sku`, `commerce.currency`, `commerce.money`, `commerce.quantity`, `text.word`, `text.sentence`, `text.paragraph`, `text.slug`.
- Identifiers/files/network: `identifier.ulid`, `identifier.nanoid`, `identifier.token`, `identifier.hash`, `file.name`, `file.extension`, `file.mime_type`, `file.size`, `network.mac`, `network.port`.
- Credentials/temporal: `credential.password_hash`, `credential.token`, `credential.api_key`, `credential.secret`, `credential.placeholder`, `date`, `time`, `datetime`, `duration`, `before`, `after`.

- [ ] **Step 1: Add failing semantic and credential tests**

```rust
#[test]
fn semantic_generators_are_seeded_and_shape_valid() {
    let a = generate_text("internet.email", 42, 20);
    let b = generate_text("internet.email", 42, 20);
    assert_eq!(a, b);
    assert!(a.iter().all(|value| value.contains('@')));

    let token = generate_text_with("credential.token", 42, 1, yaml("{ length: 64, alphabet: alphanumeric }"));
    assert_eq!(token[0].len(), 64);
    assert!(token[0].chars().all(|c| c.is_ascii_alphanumeric()));
}
```

Add one shape assertion per catalog family, unsupported-locale compile failure, invalid private-key placeholder assertion, temporal bounds, and `after >= source` / `before <= source` tests.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_engine_test semantic_generators_are_seeded_and_shape_valid -- --exact`

Expected: FAIL because semantic factories are absent.

- [ ] **Step 3: Extract shared fake generation without changing redact output contracts**

Move locale/name-to-fake dispatch into neutral crate-private `src/fake_data.rs`, so neither redactor nor the public generate module depends on the other. Expose:

```rust
pub(crate) fn generate_semantic(
    kind: SemanticKind,
    locale: Locale,
    rng: &mut dyn rand::RngCore,
) -> String;
```

Keep redactor aliases (`name`, `safe_email`, `zip`) mapping to the new typed kinds. Run existing redactor tests before continuing.

- [ ] **Step 4: Implement credentials and temporal values**

Password hashes use a syntactically valid, clearly synthetic format; tokens/API keys preserve configured length/alphabet/prefix. Private keys return an unmistakably invalid placeholder. Date/time generation uses integer timestamp ranges and `chrono`; relative generators read the declared source column and use checked duration arithmetic.

- [ ] **Step 5: Verify both consumers**

Run: `cargo test --test generate_engine_test semantic -- --nocapture && cargo test --test redact_test`

Expected: PASS; no redactor regression.

- [ ] **Step 6: Commit**

```bash
git add src/generate/generators src/fake_data.rs src/redactor/strategy/fake.rs tests/generate_engine_test.rs tests/redact_test.rs
git commit -m "feat(generate): add semantic and credential generators"
```

### Task 13: Execute tables with simple foreign and composite keys

**Files:**
- Create: `src/generate/generators/relation.rs`
- Create: `src/generate/engine.rs`
- Modify: `src/generate/generators/mod.rs`
- Modify: `src/generate/mod.rs`
- Modify: `src/generate/plan.rs`
- Test: `tests/generate_engine_test.rs`

**Interfaces:**
- Produces: `GenerationEngine::new(plan)`, `GenerationEngine::run(&mut dyn RowSink) -> GenerateReport`
- Produces: `RowSink`, `GeneratedRow`, `KeyDomain`
- Produces: `relation.foreign_key`, `relation.composite_key`
- Consumes: compiled table order, generators/modifiers, stable streams

- [ ] **Step 1: Add failing row-order and FK tests**

```rust
#[test]
fn engine_generates_parent_before_child_with_valid_foreign_keys() {
    let plan = compile_model(customers_orders_model(10, 40, 4.0));
    let mut sink = CollectingSink::default();
    let report = GenerationEngine::new(plan).run(&mut sink).unwrap();

    assert_eq!(report.rows_written, 50);
    assert_eq!(sink.table_order(), ["customers", "orders"]);
    let customer_ids = sink.values("customers", "id").collect::<BTreeSet<_>>();
    assert!(sink.values("orders", "customer_id").all(|id| customer_ids.contains(id)));
}
```

Add composite-key atomic selection, nullable FK, uniform/sequential/weighted distribution, same-seed equality, and different-seed inequality tests.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_engine_test engine_generates_parent_before_child_with_valid_foreign_keys -- --exact`

Expected: compile failure because `GenerationEngine` is absent.

- [ ] **Step 3: Define row and sink contracts**

```rust
pub struct GeneratedRow {
    pub table_index: usize,
    pub row_index: u64,
    pub values: Vec<GeneratedValue>,
}

pub trait RowSink {
    fn begin_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError>;
    fn write_row(&mut self, table: &PlannedTable, row: &GeneratedRow) -> Result<(), GenerateError>;
    fn end_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError>;
}
```

The engine allocates one row vector per active table and reuses it. Execute column owners in compiled dependency order, then modifiers, then sink.

- [ ] **Step 4: Represent reproducible parent-key domains**

```rust
pub enum KeyDomain {
    DenseInteger { start: i128, step: i128, count: u64 },
    Deterministic { count: u64, generator: Box<dyn RandomAccessKeyGenerator> },
    Composite {
        count: u64,
        components: Vec<Box<dyn RandomAccessKeyGenerator>>,
    },
    Nullable(Box<KeyDomain>),
}
```

Sequence keys use `DenseInteger`. UUID/semantic keys used as parents compile into a random-access generator whose row seed includes `primary_key.row.<index>`, so selecting parent row `n` regenerates exactly that key without storing all keys. Composite selection chooses one parent row index, then derives every component from that row; never choose components independently.

Stateful/non-random-access parent key generators report `GEN-KEY-DOMAIN-UNSUPPORTED` until protected key spooling lands in Task 22.

- [ ] **Step 5: Compile relationship distributions**

`uniform` maps a random index into `[0,count)`. `sequential` uses `child_row % parent_count`. `weighted` and `observed` compile a bounded histogram over parent row-index buckets, not a value list. Null rate is a separate first decision.

- [ ] **Step 6: Verify engine and relation tests**

Run: `cargo test --test generate_engine_test engine -- --nocapture && cargo test --test generate_engine_test relation -- --nocapture`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/generate tests/generate_engine_test.rs
git commit -m "feat(generate): execute relational table plans"
```

### Task 14: Render normalized SQL for all four dialects

**Files:**
- Create: `src/render/ddl.rs`
- Create: `src/render/sql.rs`
- Modify: `src/render/mod.rs`
- Modify: `src/convert/types.rs`
- Test: `tests/generate_engine_test.rs`
- Create: `tests/fixtures/generate/simple.yaml`

**Interfaces:**
- Produces: `SqlRenderer<W: Write>: RowSink`
- Produces: `RenderOptions`
- Consumes: `PortableSchema`, `GeneratedValue`, `RowBatch`, existing conversion type mappings

- [ ] **Step 1: Add failing dialect golden tests**

```rust
#[test]
fn simple_model_renders_valid_dialect_shapes() {
    for dialect in [SqlDialect::MySql, SqlDialect::Postgres, SqlDialect::Sqlite, SqlDialect::Mssql] {
        let sql = render_fixture("tests/fixtures/generate/simple.yaml", dialect);
        assert!(sql.contains("CREATE TABLE"));
        match dialect {
            SqlDialect::Postgres => assert!(sql.contains("COPY ")),
            SqlDialect::Mssql => assert!(sql.contains("N'")),
            _ => assert!(sql.contains("INSERT INTO")),
        }
    }
}
```

Add exact tests for identifiers, NULL/default/bytes/decimal/date/JSON, PostgreSQL COPY escaping, MSSQL Unicode and GO separators, schema/data-only modes, and batch boundaries.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_engine_test simple_model_renders_valid_dialect_shapes -- --exact`

Expected: FAIL because `SqlRenderer` is absent.

- [ ] **Step 3: Centralize type conversion and identifier quoting**

Make the existing conversion type mapping callable from `render::ddl` rather than maintaining a generation-only mapping. Add public/crate-public functions with this contract:

```rust
pub(crate) fn map_column_type(
    source_type: &str,
    from: SqlDialect,
    to: SqlDialect,
    warnings: &mut WarningCollector,
) -> String;
```

Do not expose conversion internals publicly merely for the renderer.

- [ ] **Step 4: Implement `SqlRenderer` as a streaming `RowSink`**

Use one `BufWriter` and `RowBatch` per active table. MySQL/SQLite/MSSQL use multi-row INSERT; PostgreSQL uses COPY unless `no_copy`. Render values directly with `Display` helpers; do not allocate a formatted string per cell or row. Generated `Default` either omits the column in INSERT or renders the dialect-supported default token; reject impossible COPY/default combinations at compile time.

- [ ] **Step 5: Preserve raw DDL only under the safe condition**

If output dialect equals source and no table/schema filtering changed the set, write `create_statement`. Otherwise render normalized table DDL, keys, indexes, and deferred constraints. Task 26 adds the full excluded-object rewrite cases; this task covers unchanged and cross-dialect schemas.

- [ ] **Step 6: Validate rendered fixtures through existing validators**

Run: `cargo test --test generate_engine_test render -- --nocapture && cargo test --test validate_test test_validate_generated -- --nocapture`

Expected: PASS for all dialect outputs supported by existing validator coverage.

- [ ] **Step 7: Commit**

```bash
git add src/render src/convert/types.rs tests/generate_engine_test.rs tests/fixtures/generate/simple.yaml
git commit -m "feat(generate): render model-driven SQL"
```

### Task 15: Expose the public builder and staged library API

**Files:**
- Modify: `src/generate/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/generate_api_test.rs`

**Interfaces:**
- Produces: `Generate`, `GenerateBuilder`, `GenerateReport`
- Produces: `RunMode::{Generate,Check,DryRun}`
- Re-exports: `SyntheticModel`, `ExtensionRegistry`, `ModelCompiler`, `GenerationEngine`, renderer types; Task 19 adds `DumpProfiler`
- Consumes: model loader/compiler/engine/renderer

- [ ] **Step 1: Add a failing public API compile/run test**

```rust
#[test]
fn builder_generates_from_a_complete_model() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    let report = Generate::builder()
        .config("tests/fixtures/generate/simple.yaml")
        .output(&output)
        .seed(42)
        .run()
        .unwrap();
    assert!(report.rows_written > 0);
    assert!(fs::read_to_string(output).unwrap().contains("INSERT INTO"));
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_api_test builder_generates_from_a_complete_model -- --exact`

Expected: compile failure because `Generate` is absent.

- [ ] **Step 3: Implement a builder that produces one `GenerateRequest`**

```rust
pub struct GenerateRequest {
    pub input: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub output: OutputTarget,
    pub compile: CompileOptions,
    pub render: RenderOptions,
    pub mode: RunMode,
}

impl GenerateBuilder {
    pub fn run(self) -> Result<GenerateReport, GenerateError> {
        Generate::run(self.build()?)
    }
}
```

The builder validates only request shape. Model diagnostics remain structured and flow through `GenerateError::Diagnostics`.

- [ ] **Step 4: Keep staged APIs independently usable**

Add doctests for the Phase-1-reachable staged API subset (registry → `ModelCompiler` → `GenerationEngine` → renderer). The profiler/inference portion of the staged snippet (`DumpProfiler`, `ModelInference`) is delivered and doctested in Phase 2 (Tasks 19–20), so it is not doctested here. Accept `Read`/`Write` in profiler/engine/renderer constructors; path methods are conveniences.

- [ ] **Step 5: Verify public API under minimal features**

Run: `cargo test --no-default-features --test generate_api_test && cargo test --doc`

Expected: PASS without DuckDB/compression/archive; file helpers return a clear unsupported-format error only when an optional compression format is actually requested.

- [ ] **Step 6: Commit**

```bash
git add src/generate/mod.rs src/lib.rs tests/generate_api_test.rs
git commit -m "feat(generate): expose public generation API"
```

### Task 16: Add the Phase 1 CLI, preflight, and report routing

**Files:**
- Create: `src/cmd/generate.rs`
- Modify: `src/cmd/mod.rs`
- Create: `tests/generate_cli_test.rs`
- Modify: `tests/json_output_test.rs`

**Interfaces:**
- Produces: `GenerateArgs`, `cmd::generate::run(args) -> anyhow::Result<ExitCode>`
- Consumes: `GenerateRequest`, `CompileOptions`, `RenderOptions`, `GenerateReport`

- [ ] **Step 1: Add failing clap conflict tests**

```rust
#[test]
fn generate_cli_rejects_conflicting_modes_and_stdout_owners() {
    assert!(Cli::try_parse_from(["sql-splitter", "generate", "model.yaml", "--scale", "0.1", "--rows", "10"]).is_err());
    assert!(Cli::try_parse_from(["sql-splitter", "generate", "model.yaml", "--check", "--dry-run"]).is_err());
    assert!(Cli::try_parse_from(["sql-splitter", "generate", "model.yaml", "--json", "--output", "-"]).is_err());
    assert!(Cli::try_parse_from(["sql-splitter", "generate", "model.yaml", "--seed", "1", "--randomize"]).is_err());
}
```

Add tests for `--verify` requiring a real output, check requiring config/no input, schema/data-only, quiet/progress, JSON/config stdout, and per-table count conflicts that require post-clap resolution.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_cli_test generate_cli_rejects_conflicting_modes_and_stdout_owners -- --exact`

Expected: FAIL because the subcommand is absent.

- [ ] **Step 3: Define the complete clap surface**

The literal CLI inventory is:

- Input/model: `[INPUT]`, `-c/--config`, `--emit-config`, `--profile-depth`, `--profile-sample`, `--input-dialect`.
- Volume: `--scale`, `--rows`, repeatable `--table-rows`, repeatable `--table-scale`, `--max-rows`, `--tables`, `--exclude`.
- Randomness: `--seed`, `--randomize`.
- Rendering: `-o/--output`, `--dialect`, `--schema-only`, `--data-only`, `--batch-size`, `--no-copy`, `--compress`, `--mssql-production-style`, `--mssql-go`.
- Preflight/reporting: `--check`, `--dry-run`, `--verify`, `--explain`, `--strict`, `--progress`, `--json`, `--quiet`.

No additional public CLI option is introduced by later phases without updating the canonical spec, help/man/completion tests, website, `llms.txt`, and Agent Skill.

```rust
#[derive(Args, Debug)]
pub struct GenerateArgs {
    #[arg(value_hint = ValueHint::FilePath)]
    input: Option<PathBuf>,
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    config: Option<PathBuf>,
    #[arg(long, value_hint = ValueHint::FilePath)]
    emit_config: Option<PathBuf>,
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    output: Option<PathBuf>,
    #[arg(long, value_enum, default_value = "basic")]
    profile_depth: ProfileDepthArg,
    #[arg(long)]
    profile_sample: Option<usize>,
    #[arg(long, value_enum)]
    input_dialect: Option<SqlDialect>,
    #[arg(long, value_enum)]
    dialect: Option<SqlDialect>,
    #[arg(long, conflicts_with = "rows")]
    scale: Option<f64>,
    #[arg(long, conflicts_with = "scale")]
    rows: Option<u64>,
    #[arg(long = "table-rows")]
    table_rows: Vec<String>,
    #[arg(long = "table-scale")]
    table_scale: Vec<String>,
    #[arg(long)]
    max_rows: Option<u64>,
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,
    #[arg(long, conflicts_with = "randomize")]
    seed: Option<u64>,
    #[arg(long, conflicts_with = "seed")]
    randomize: bool,
    #[arg(long, conflicts_with = "data_only")]
    schema_only: bool,
    #[arg(long, conflicts_with = "schema_only")]
    data_only: bool,
    #[arg(long, default_value_t = 1_000)]
    batch_size: usize,
    #[arg(long)]
    no_copy: bool,
    #[arg(long)]
    compress: Option<CompressionFormat>,
    #[arg(long)]
    mssql_production_style: bool,
    #[arg(long, value_parser = clap::value_parser!(usize).range(1..))]
    mssql_go: Option<usize>,
    #[arg(long, conflicts_with_all = ["dry_run", "verify"])]
    check: bool,
    #[arg(long, conflicts_with_all = ["check", "verify"])]
    dry_run: bool,
    #[arg(long, conflicts_with_all = ["check", "dry_run"])]
    verify: bool,
    #[arg(long)]
    explain: bool,
    #[arg(long)]
    strict: bool,
    #[arg(long)]
    progress: bool,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    quiet: bool,
}
```

`ProfileDepthArg` contains exactly `Basic` and `Full`; schema-only profiling remains a library/internal mode. `try_into_request` additionally rejects `--verify` without a filesystem output, `--check` without a complete config, stdout ownership collisions (including `--json --emit-config -` and simultaneous SQL/model stdout), zero batch sizes, malformed table patterns, unsupported dialect/format combinations, `--quiet --progress`, and `--compress` with stdout output (`-o -` / no file). If the clap struct can express `--quiet`/`--progress` via `conflicts_with`, prefer that; otherwise enforce in `try_into_request`.

The input/profiling flags (`[INPUT]`, `--profile-depth`, `--profile-sample`) parse in Phase 1 but return a clear "requires Phase 2 profiling" error until Tasks 19–21 wire dump profiling; only complete-model paths are functional at the Phase 1 checkpoint.

- [ ] **Step 4: Route stdout and exit codes exactly**

JSON reports own stdout. Generated SQL defaults to stdout only without `--json` and without model-only emission. `--check`/`--dry-run` produce no SQL. Return `ExitCode::SUCCESS`, `ExitCode::FAILURE`, or Clap's usage exit `2`; do not add phase-specific process exit codes. `try_into_request` returns a typed usage error (not a generic `anyhow::Error`); `run()` maps that usage error to Clap's usage exit code `2`, not `1`, so post-clap conflicts match the spec's exit-code table.

```rust
pub fn run(args: GenerateArgs) -> anyhow::Result<ExitCode> {
    let request = args.try_into_request()?;
    match Generate::run(request) {
        Ok(report) => { write_report(&report, args.json, args.quiet)?; Ok(ExitCode::SUCCESS) }
        Err(GenerateError::Diagnostics(bag)) => {
            write_diagnostics(&bag, args.json)?;
            Ok(ExitCode::FAILURE)
        }
        Err(error) => Err(error.into()),
    }
}
```

- [ ] **Step 5: Add end-to-end Phase 1 CLI tests**

Run a complete model through normal output, stdout, `--check`, `--dry-run`, `--json -o file`, same-seed equality, and different-seed inequality. Assert usage conflicts exit `2` and invalid model exits `1`.

Run: `cargo test --test generate_cli_test -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/cmd tests/generate_cli_test.rs tests/json_output_test.rs
git commit -m "feat(generate): add model-driven CLI"
```

### Phase 1 checkpoint

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run `cargo test --no-default-features`.
- [ ] Run `cargo test`.
- [ ] Run `cargo run -- generate --config tests/fixtures/generate/simple.yaml --check` and expect exit `0`.
- [ ] Run `cargo run -- generate --config tests/fixtures/generate/simple.yaml --seed 42 -o /tmp/sql-splitter-generate-phase1.sql`.
- [ ] Run `cargo run -- validate /tmp/sql-splitter-generate-phase1.sql` and expect exit `0`.

---

## Phase 0: Production foundations and renderer baseline

### Task 1: Preserve complete portable schema evidence

**Files:**
- Modify: `src/schema/mod.rs`
- Modify: `src/schema/ddl.rs`
- Modify: `src/schema/build.rs`
- Create: `src/synthetic/mod.rs`
- Create: `src/synthetic/schema.rs`
- Modify: `src/lib.rs`
- Test: `tests/schema_unit_test.rs`
- Test: `tests/schema_build_test.rs`

**Interfaces:**
- Produces: `schema::Column.source_type`, `default_sql`, `is_unique`, `is_generated`, `is_identity`
- Produces: `synthetic::schema::{PortableSchema, PortableTable, PortableColumn}`
- Produces: `PortableSchema::from_runtime(&Schema, SqlDialect) -> PortableSchema`
- Consumes: existing `Schema`, `TableSchema`, `ColumnType`, and DDL parsing

- [ ] **Step 1: Add a failing DDL evidence test**

```rust
#[test]
fn parse_column_preserves_generation_evidence() {
    let mut builder = SchemaBuilder::new();
    builder
        .parse_create_table(
            "CREATE TABLE users (\
             id BIGINT IDENTITY(1,1) PRIMARY KEY, \
             email VARCHAR(255) NOT NULL UNIQUE, \
             state VARCHAR(20) DEFAULT 'active', \
             slug VARCHAR(255) GENERATED ALWAYS AS (LOWER(email)) STORED);",
        )
        .unwrap();
    let schema = builder.build();
    let table = schema.get_table("users").expect("table");

    let id = table.get_column("id").unwrap();
    assert_eq!(id.source_type, "BIGINT");
    assert!(id.is_identity);

    let email = table.get_column("email").unwrap();
    assert_eq!(email.source_type, "VARCHAR(255)");
    assert!(email.is_unique);

    let state = table.get_column("state").unwrap();
    assert_eq!(state.default_sql.as_deref(), Some("'active'"));

    assert!(table.get_column("slug").unwrap().is_generated);
}
```

Use the existing `parse_create_table` and `build` API; do not add a production helper solely for this assertion.

- [ ] **Step 2: Run the focused tests and confirm red**

Run: `cargo test --test schema_unit_test parse_column_preserves_generation_evidence -- --exact`

Expected: compile failure because the new `Column` fields do not exist.

- [ ] **Step 3: Extend the runtime schema without duplicating SQL type parsing**

Add these fields to `schema::Column` and populate them in `parse_column_def`:

```rust
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
    pub source_type: String,
    pub ordinal: ColumnId,
    pub is_primary_key: bool,
    pub is_nullable: bool,
    pub is_unique: bool,
    pub default_sql: Option<String>,
    pub is_generated: bool,
    pub is_identity: bool,
    pub collation: Option<String>,
}
```

Use quote/parenthesis-aware token extraction in `src/schema/ddl.rs`; do not split defaults/check expressions on whitespace. Inline/table `UNIQUE`, `CHECK`, `DEFAULT`, `COLLATE`, `GENERATED`, MySQL `AUTO_INCREMENT`, PostgreSQL `serial`/identity, SQLite autoincrement, and MSSQL `IDENTITY` must map into runtime schema fields. Add typed table-level unique/check collections while preserving their raw expressions. Update all direct `Column { ... }` and `TableSchema { ... }` test constructors explicitly.

- [ ] **Step 4: Add the portable schema conversion**

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableColumn {
    pub name: String,
    pub source_type: String,
    pub family: SqlTypeFamily,
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_sql: Option<String>,
    #[serde(default)]
    pub generated: bool,
    #[serde(default)]
    pub identity: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlTypeFamily {
    Integer,
    BigInteger,
    Decimal,
    Boolean,
    Text,
    Bytes,
    Uuid,
    DateTime,
    Json,
    Other,
}
```

`PortableTable` stores ordered columns, PK names, composite unique constraints, indexes, raw check expressions, raw same-dialect DDL, and declared relationships. Use `BTreeMap` only for name lookup/serialization; preserve column and constraint order in `Vec`.

- [ ] **Step 5: Test the runtime-to-portable conversion**

```rust
#[test]
fn portable_schema_keeps_order_and_raw_ddl() {
    let schema = Schema::from_sql_file(f.path(), SqlDialect::MySql, None).unwrap();
    let portable = PortableSchema::from_runtime(&schema, SqlDialect::MySql);
    let users = portable.tables.get("users").unwrap();
    assert_eq!(users.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["id", "email"]);
    assert!(users.create_statement.as_deref().unwrap().contains("CREATE TABLE"));
}
```

Run: `cargo test --test schema_unit_test --test schema_build_test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/schema src/synthetic src/lib.rs tests/schema_unit_test.rs tests/schema_build_test.rs
git commit -m "feat(generate): preserve portable schema evidence"
```

### Task 2: Add structured diagnostics

**Files:**
- Create: `src/diagnostic.rs`
- Modify: `src/lib.rs`
- Create: `tests/generate_compiler_test.rs`

**Interfaces:**
- Produces: `Diagnostic`, `DiagnosticCode`, `Severity`, `SourceLocation`, `DiagnosticBag`
- Produces: `DiagnosticBag::into_result<T>(value: T) -> Result<T, DiagnosticBag>`
- Consumed by: config loader, compiler, heuristics, CLI reports, verifier

- [ ] **Step 1: Write failing aggregation and JSON tests**

```rust
#[test]
fn diagnostic_bag_keeps_independent_errors() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    bag.error("GEN-MISSING-COLUMN", "tables.users.columns.email", "column does not exist");
    assert_eq!(bag.errors().count(), 2);
    assert!(serde_json::to_value(&bag).unwrap()["diagnostics"].is_array());
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_compiler_test diagnostic_bag_keeps_independent_errors -- --exact`

Expected: compile failure because `sql_splitter::diagnostic` is absent.

- [ ] **Step 3: Implement stable diagnostic data**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity { Warning, Error }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: Severity,
    pub path: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related: Vec<SourceLocation>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DiagnosticBag {
    pub diagnostics: Vec<Diagnostic>,
}
```

Implement `Display` as the exact `error[CODE] path` / indented help format in the spec. Keep codes as strings so statically linked extensions can define namespaced codes without changing an enum.

- [ ] **Step 4: Verify human and JSON representations**

Run: `cargo test --test generate_compiler_test diagnostic`

Expected: PASS for aggregation, error counts, display snapshot, and JSON shape.

- [ ] **Step 5: Commit**

```bash
git add src/diagnostic.rs src/lib.rs tests/generate_compiler_test.rs
git commit -m "feat(generate): add structured diagnostics"
```

### Task 3: Define complete model and override types

**Files:**
- Create: `src/synthetic/model.rs`
- Create: `src/synthetic/overrides.rs`
- Modify: `src/synthetic/mod.rs`
- Create: `tests/generate_config_test.rs`

**Interfaces:**
- Produces: `SyntheticFile::{Model(SyntheticModel), Overrides(SyntheticOverrides)}`
- Produces: `SyntheticFile::parse_str` and role-specific, unknown-field-safe document structs
- Produces: `TableSeed::{Inherit, Random, Fixed(u64)}` with missing/null/integer YAML semantics
- Produces: complete and partial row/schema/rule types used by the compiler

**Resolution note (design audit):**
- `PortableColumn` accepts a `type:` input alias for `source_type`; `family` is `#[serde(default)]`-derived from `source_type` when absent; emit is always canonical.
- `SyntheticModel.output` and `SyntheticModel.defaults` take `#[serde(default)]` (default inference = disabled; default output = preserve source dialect) so a minimal `kind: model` per the spec's optional-field table parses.
- `SyntheticOverrides` includes `defaults: Option<ModelDefaults>` and `source: Option<SourceModel>` (both spec-optional for overrides).
- `RowsModel` and `ChildDistribution` carry `#[serde(deny_unknown_fields)]`.

- [ ] **Step 1: Write failing role, unknown-field, and seed tests**

```rust
#[test]
fn document_role_and_table_seed_are_unambiguous() {
    let yaml = r#"
version: 1
kind: model
defaults: { inference: disabled }
output: { dialect: mysql }
tables:
  inherited: { rows: { kind: fixed, count: 1 }, schema: { name: inherited, columns: [] } }
  random: { seed: null, rows: { kind: fixed, count: 1 }, schema: { name: random, columns: [] } }
  fixed: { seed: 9, rows: { kind: fixed, count: 1 }, schema: { name: fixed, columns: [] } }
"#;
    let file = SyntheticFile::parse_str(yaml).unwrap();
    let model = file.into_model().unwrap();
    assert_eq!(model.tables["inherited"].seed, TableSeed::Inherit);
    assert_eq!(model.tables["random"].seed, TableSeed::Random);
    assert_eq!(model.tables["fixed"].seed, TableSeed::Fixed(9));
}

#[test]
fn unknown_model_fields_fail() {
    let err = SyntheticFile::parse_str(
        "version: 1\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables: {}\ntabels: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"));
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_config_test document_role_and_table_seed_are_unambiguous -- --exact`

Expected: compile failure because model types are absent.

- [ ] **Step 3: Implement the tagged document and tri-state table seed**

```rust
#[derive(Debug, Clone)]
pub enum SyntheticFile {
    Model(SyntheticModel),
    Overrides(SyntheticOverrides),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyntheticModel {
    pub version: u32,
    pub kind: ModelKind,
    #[serde(default)]
    pub imports: Vec<PathBuf>,
    pub defaults: ModelDefaults,
    #[serde(default)]
    pub seed: Option<u64>,
    pub output: OutputModel,
    pub tables: BTreeMap<String, TableModel>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableSeed {
    #[default]
    Inherit,
    Random,
    Fixed(u64),
}
```

`SyntheticOverrides` likewise contains `version` and a marker field accepting only `kind: overrides`. `SyntheticFile::parse_str` first parses a minimal `{ version, kind }` envelope from a duplicate-key-checked YAML value, rejects unsupported versions/roles, then deserializes the same value into the role-specific `#[serde(deny_unknown_fields)]` struct. Do not combine `deny_unknown_fields` with a flattened tagged enum; that makes unknown-field behavior fragile.

On `TableModel.seed`, use `#[serde(default, deserialize_with = "deserialize_table_seed", skip_serializing_if = "TableSeed::is_inherit")]`. Deserialize `Option<u64>`: YAML null becomes `Random`, integer becomes `Fixed`; the default handles omission as `Inherit`. Implement a matching serializer.

Define explicit structs/enums for `OutputModel`, `InferenceMode`, `RowsModel`, `ChildDistribution`, `ColumnRule`, `GeneratorConfig`, `ModifierConfig`, `RelationshipModel`, and `PlannerConfig`. `OutputModel` owns `output.dialect`; `RowsModel::Fixed` serializes `rows.count`. Use internally tagged `kind` enums for finite standard shapes and `BTreeMap<String, serde_yaml_ng::Value>` only for registry-owned argument payloads.

- [ ] **Step 4: Define partial override structs rather than making the model optional everywhere**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyntheticOverrides {
    pub version: u32,
    pub kind: OverridesKind,
    #[serde(default)]
    pub imports: Vec<PathBuf>,
    #[serde(
        default,
        deserialize_with = "deserialize_root_seed_override",
        serialize_with = "serialize_root_seed_override",
        skip_serializing_if = "RootSeedOverride::is_inherit"
    )]
    pub seed: RootSeedOverride,
    pub output: Option<OutputOverride>,
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverride>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RootSeedOverride {
    #[default]
    Inherit,
    Random,
    Fixed(u64),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableOverride {
    #[serde(
        default,
        deserialize_with = "deserialize_table_seed_override",
        serialize_with = "serialize_table_seed_override",
        skip_serializing_if = "TableSeedOverride::is_inherit"
    )]
    pub seed: TableSeedOverride,
    pub rows: Option<RowsOverride>,
    pub schema: Option<PortableTableOverride>,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnRuleOverride>,
    pub relationships: Option<Vec<RelationshipModel>>,
    pub planners: Option<Vec<PlannerConfig>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableSeedOverride {
    #[default]
    Inherit,
    Random,
    Fixed(u64),
}
```

For both override seed fields, omission is `Inherit`, YAML `null` is `Random`, and an integer is `Fixed`. Lists are `Option<Vec<_>>` because present lists replace; absent lists inherit.

- [ ] **Step 5: Test round-trips and completeness-neutral parsing**

Run: `cargo test --test generate_config_test`

Expected: PASS for model/override roles, unknown fields, null seed, integer seed, serialized omission of inherited seeds, and complete YAML example parsing.

- [ ] **Step 6: Commit**

```bash
git add src/synthetic tests/generate_config_test.rs
git commit -m "feat(generate): define model and override documents"
```

### Task 4: Load local imports with deterministic root-wins semantics

**Files:**
- Create: `src/synthetic/config.rs`
- Modify: `src/synthetic/mod.rs`
- Test: `tests/generate_config_test.rs`

**Interfaces:**
- Produces: `ConfigLoader::load(path: &Path) -> Result<SyntheticFile, DiagnosticBag>`
- Produces: `merge_yaml(root, imports) -> Result<serde_yaml_ng::Value, DiagnosticBag>`
- Consumes: `SyntheticFile` and `SyntheticOverrides`

- [ ] **Step 1: Add failing import tests**

```rust
#[test]
fn imports_reject_collisions_but_root_may_override() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.yaml"), "version: 1\nkind: overrides\ntables:\n  users:\n    seed: 1\n").unwrap();
    fs::write(dir.path().join("b.yaml"), "version: 1\nkind: overrides\ntables:\n  users:\n    seed: 2\n").unwrap();
    fs::write(dir.path().join("bad.yaml"), "version: 1\nkind: overrides\nimports: [a.yaml, b.yaml]\ntables: {}\n").unwrap();
    let err = ConfigLoader::load(&dir.path().join("bad.yaml")).unwrap_err();
    assert!(err.to_string().contains("GEN-IMPORT-COLLISION"));

    fs::write(dir.path().join("good.yaml"), "version: 1\nkind: overrides\nimports: [a.yaml]\ntables:\n  users:\n    seed: 9\n").unwrap();
    let loaded = ConfigLoader::load(&dir.path().join("good.yaml")).unwrap();
    assert_eq!(loaded.into_overrides().unwrap().tables["users"].seed, TableSeedOverride::Fixed(9));
}
```

Also test remote paths, nested imports, imported `kind: model`, duplicate YAML keys, and list replacement.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_config_test imports_reject_collisions_but_root_may_override -- --exact`

Expected: compile failure because `ConfigLoader` is absent.

- [ ] **Step 3: Implement a two-stage loader**

```rust
pub struct ConfigLoader;

impl ConfigLoader {
    pub fn load(path: &Path) -> Result<SyntheticFile, DiagnosticBag> {
        let root_text = fs::read_to_string(path).map_err(io_diagnostic)?;
        let root: Value = serde_yaml_ng::from_str(&root_text).map_err(parse_diagnostic)?;
        let imports = import_paths(&root)?;
        let mut merged = Value::Mapping(Default::default());
        let mut occupied = BTreeSet::<String>::new();

        for import in imports {
            reject_remote_or_absolute(&import)?;
            let base = path.parent().unwrap_or_else(|| Path::new("."));
            let value = load_non_recursive_override(&base.join(import))?;
            merge_import(&mut merged, value, &mut occupied, "")?;
        }
        merge_root(&mut merged, root)?;
        SyntheticFile::parse_value(merged).map_err(parse_diagnostic)
    }
}
```

Remove `imports` before final typed deserialization only if the typed root body stores them separately; do not let imported `version`/`kind` overwrite the root. Track leaf paths (`tables.users.seed`) so two imports cannot define the same path. Root values replace imported leaves; map children merge; list values replace whole lists.

- [ ] **Step 4: Verify import and path diagnostics**

Run: `cargo test --test generate_config_test import`

Expected: PASS; diagnostics include both colliding files and the exact path.

- [ ] **Step 5: Commit**

```bash
git add src/synthetic/config.rs src/synthetic/mod.rs tests/generate_config_test.rs
git commit -m "feat(generate): load deterministic local imports"
```

### Task 5: Add stable seed derivation and typed generated values

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `src/generate/mod.rs`
- Create: `src/generate/value.rs`
- Create: `src/generate/seed.rs`
- Modify: `src/lib.rs`
- Create: `tests/generate_engine_test.rs`

**Interfaces:**
- Produces: `GeneratedValue`
- Produces: `SeedRoot::new(seed)`, `SeedRoot::stream(StreamId) -> ChaCha8Rng`
- Produces: `StreamId::{table,column,planner,operator}` constructors

- [ ] **Step 1: Add failing stream-isolation tests**

```rust
#[test]
fn unrelated_streams_do_not_perturb_existing_values() {
    let root = SeedRoot::new(42);
    let mut before = root.stream(StreamId::column("users", "email", "internet.email"));
    let expected = before.next_u64();

    let mut unrelated = root.stream(StreamId::column("orders", "status", "weighted_choice"));
    let _ = unrelated.next_u64();

    let mut after = root.stream(StreamId::column("users", "email", "internet.email"));
    assert_eq!(after.next_u64(), expected);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_engine_test unrelated_streams_do_not_perturb_existing_values -- --exact`

Expected: compile failure because `generate::seed` is absent.

- [ ] **Step 3: Add the RNG dependency and stable derivation**

Add `rand_chacha = "0.10"` to root dependencies. Do not use `DefaultHasher` or `ahash` for stable streams because their output is not a public compatibility contract. Derive 32 bytes with SHA-256 over length-prefixed components:

```rust
pub fn derive_seed(root: u64, parts: &[&str]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"sql-splitter.generate.seed.v1\0");
    hash.update(root.to_le_bytes());
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    hash.finalize().into()
}
```

Construct `ChaCha8Rng::from_seed`. Stream identity uses normalized table, column/planner, and operator names; no traversal index may enter the hash.

- [ ] **Step 4: Implement the value representation**

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum GeneratedValue {
    Null,
    Default,
    Boolean(bool),
    Integer(i128),
    Decimal { minor: i128, scale: u32 },
    Text(String),
    Bytes(Vec<u8>),
    DateTime(String),
    Json(String),
}
```

Use integer minor units for money/planner arithmetic. Add typed accessors returning `GenerateError`, not panics.

- [ ] **Step 5: Verify stable golden seeds and value accessors**

Run: `cargo test --test generate_engine_test seed -- --nocapture`

Expected: PASS, including one checked-in hex golden for `derive_seed(42, ["users", "email", "internet.email"])` so accidental algorithm changes are visible.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/generate src/lib.rs tests/generate_engine_test.rs
git commit -m "feat(generate): add stable streams and typed values"
```

### Task 6: Restore the measured allocation-lean renderer primitives

**Files:**
- Create: `src/render/mod.rs`
- Create: `src/render/sql_string.rs`
- Create: `src/render/row_batch.rs`
- Create: `src/render/random.rs`
- Modify: `src/lib.rs`
- Create: `benches/generate_bench.rs`
- Modify: `Cargo.toml`
- Test: `tests/generate_engine_test.rs`

**Interfaces:**
- Produces: `SqlString<'a>`, `RowBatch`, `RandomBlock`, `DialectRenderer` primitives
- Consumes: `SqlDialect`, `GeneratedValue`, `ChaCha8Rng`
- Performance source: `docs/superpowers/specs/2026-07-16-gen-fixtures-performance-design.md`

- [ ] **Step 1: Write exact escaping and row-buffer tests**

```rust
#[test]
fn sql_string_escapes_each_dialect_without_intermediate_contract_changes() {
    let input = "a'b\\c\n\r\t";
    assert_eq!(SqlString::new(SqlDialect::MySql, input).to_string(), "'a\\'b\\\\c\\n\\r\\t'");
    assert_eq!(SqlString::new(SqlDialect::Postgres, input).to_string(), "'a''b\\c\n\r\t'");
    assert_eq!(SqlString::new(SqlDialect::Sqlite, input).to_string(), "'a''b\\c\n\r\t'");
    assert_eq!(SqlString::new(SqlDialect::Mssql, input).to_string(), "N'a''b\\c\n\r\t'");
}

#[test]
fn row_batch_reuses_capacity_after_clear() {
    let mut batch = RowBatch::with_capacity(4, 256);
    batch.push_fmt(format_args!("(1, 'a')")).unwrap();
    batch.push_fmt(format_args!("(2, 'b')")).unwrap();
    let capacity = batch.capacity();
    assert_eq!(batch.as_str(), "(1, 'a'),\n(2, 'b')");
    batch.clear();
    assert!(batch.capacity() >= capacity);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_engine_test sql_string -- --nocapture`

Expected: compile failure because `render` is absent.

- [ ] **Step 3: Implement borrowed SQL escaping and reusable batches**

```rust
pub struct SqlString<'a> { dialect: SqlDialect, value: &'a str }

impl Display for SqlString<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.dialect == SqlDialect::Mssql { f.write_str("N")?; }
        f.write_str("'")?;
        for ch in self.value.chars() {
            match (self.dialect, ch) {
                (SqlDialect::MySql, '\\') => f.write_str("\\\\")?,
                (SqlDialect::MySql, '\'') => f.write_str("\\'")?,
                (SqlDialect::MySql, '\n') => f.write_str("\\n")?,
                (SqlDialect::MySql, '\r') => f.write_str("\\r")?,
                (SqlDialect::MySql, '\t') => f.write_str("\\t")?,
                (_, '\'') => f.write_str("''")?,
                (_, ch) => f.write_char(ch)?,
            }
        }
        f.write_str("'")
    }
}
```

`RowBatch` owns one `String`, appends `,\n` between rows, tracks row count, and clears without shrinking. Cap its initial reservation from `batch_size` to avoid an attacker-controlled allocation.

- [ ] **Step 4: Add unbiased block random sampling**

`RandomBlock` fills a fixed `[u8; 4096]` from `ChaCha8Rng`. For the 63-character alphabet, consume the low six bits and reject `63`; refill only when exhausted. Test 10,000 characters are in the alphabet and two equal seeds produce equal bytes.

- [ ] **Step 5: Add the renderer Criterion group**

```rust
fn bench_row_batch(c: &mut Criterion) {
    c.bench_function("generate/row_batch_1000", |b| {
        b.iter(|| {
            let mut rows = RowBatch::with_capacity(1000, 96_000);
            for i in 0..1000 {
                rows.push_fmt(format_args!("({}, {})", i, SqlString::new(SqlDialect::MySql, "name"))).unwrap();
            }
            black_box(rows);
        });
    });
}
criterion_group!(benches, bench_row_batch);
criterion_main!(benches);
```

Add `[[bench]] name = "generate_bench" harness = false`.

- [ ] **Step 6: Verify and commit**

Run: `cargo test --test generate_engine_test && cargo bench --bench generate_bench --no-run`

Expected: PASS and benchmark executable builds.

```bash
git add src/render src/lib.rs tests/generate_engine_test.rs benches/generate_bench.rs Cargo.toml
git commit -m "perf(generate): add allocation-lean renderer primitives"
```

### Phase 0 checkpoint

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run `cargo test`.
- [ ] Confirm the schema evidence, diagnostic, model/config, seed, and renderer APIs are public from `src/lib.rs` exactly as named above.

---

## Phase 2: Profile dumps and infer complete models

### Task 17: Make row parsing visitor-based and memory-bounded

**Files:**
- Modify: `src/parser/mod.rs`
- Modify: `src/parser/mysql_insert.rs`
- Modify: `src/parser/postgres_copy.rs`
- Modify: `src/transform_common.rs`
- Create: `tests/parser_memory_test.rs`
- Test: `tests/sample_integration_test.rs`
- Create: `tests/shard_integration_test.rs`

**Interfaces:**
- Produces: `Parser::visit_events` and `ParserEvent::{Statement,InsertRow,CopyStart,CopyRow,CopyEnd}`
- Produces: `visit_insert_rows_with`, `visit_postgres_copy_rows_with`
- Preserves: `parse_insert_rows_with`, `parse_postgres_copy_rows_with` as collecting adapters
- Consumes: existing `UnifiedRow`, `RowExtraction`, and `RowFlow`

- [ ] **Step 1: Add visitor equivalence and bounded-retention tests**

Generate a single 100,000-row INSERT and COPY block. Assert visitor output equals the existing collecting parser for representative rows, the callback may stop early, a test-only live-row counter never exceeds two parsed rows, and reported peak parser buffering stays below 1 MiB for both blocks.

```rust
let mut seen = 0_u64;
visit_insert_rows_with(&statement, &schema, dialect, RowExtraction::Full, |row| {
    seen += 1;
    assert_eq!(row.get_column_value(0), Some(&PkValue::Integer(seen as i64)));
    Ok(if seen == 10 { RowFlow::Stop } else { RowFlow::Continue })
})?;
assert_eq!(seen, 10);
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test parser_memory_test row_visitors -- --nocapture`

Expected: compile failure because the visitor functions do not exist.

- [ ] **Step 3: Add a bounded event walker to the production parser**

`Parser::visit_events` reuses the existing quote/escape/dollar-quote lexical state but treats data rows as stream boundaries: it buffers ordinary DDL/control statements, an INSERT header plus one current row, or one COPY line—never an entire multi-row INSERT/COPY block. The callback must consume borrowed bytes before the next event. Enforce configurable maximum DDL/header/row sizes with path/offset diagnostics; the limit is a safety error, not silent truncation.

Keep `read_statement` for compatibility. Do not implement `visit_events` by calling it for data statements.

- [ ] **Step 4: Refactor each row parser around a callback**

Move the existing row loop into a visitor accepting `FnMut(ParsedRow) -> anyhow::Result<RowFlow>`. Keep statement bytes borrowed while parsing; allocate only fields required by the requested `RowExtraction`. Implement the old `Vec` return APIs by pushing in the visitor so existing consumers do not break.

Update `for_each_data_row` to consume `ParserEvent` rows directly rather than first collecting a statement-sized `Vec`.

- [ ] **Step 5: Verify all row-transform consumers**

Run: `cargo test --test parser_memory_test && cargo test --test sample_integration_test && cargo test --test shard_integration_test`

Expected: PASS with unchanged sample/shard output.

- [ ] **Step 6: Commit**

```bash
git add src/parser src/transform_common.rs tests/parser_memory_test.rs tests/sample_integration_test.rs tests/shard_integration_test.rs
git commit -m "perf(parser): stream parsed data rows to visitors"
```

### Task 18: Implement bounded profiling evidence and sketches

**Files:**
- Create: `src/profile/mod.rs`
- Create: `src/profile/evidence.rs`
- Create: `src/profile/sketches.rs`
- Modify: `src/lib.rs`
- Create: `tests/generate_profile_test.rs`

**Interfaces:**
- Produces: `ProfileDepth::{Schema,Basic,Full}` and `ProfileBudget`
- Produces: `DumpProfile`, `TableEvidence`, `ColumnEvidence`, `RelationshipEvidence`
- Produces: mergeable `Reservoir`, `SpaceSavingTopK`, `HyperLogLog`, `NumericHistogram`, `StringShapeSketch`

- [ ] **Step 1: Add deterministic sketch tests**

```rust
#[test]
fn sketches_are_bounded_mergeable_and_seeded() {
    let budget = ProfileBudget { sample_rows: 1_000, top_k: 32, histogram_bins: 32 };
    let mut left = ColumnSketches::new(&budget, 42);
    let mut right = ColumnSketches::new(&budget, 42);
    for value in 0..50_000 { left.observe(ProfileValue::Integer(value)); }
    for value in 50_000..100_000 { right.observe(ProfileValue::Integer(value)); }
    left.merge(right).unwrap();
    assert!(left.retained_items() <= budget.retained_items_per_column());
    assert_relative_eq!(left.distinct_estimate(), 100_000.0, max_relative = 0.10);
}
```

Also cover null rate, booleans, quantiles, top-k heavy hitters, decimal scale, timestamp range, string length/alphabet/prefix/suffix, JSON-valid rate, and deterministic reservoir samples.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_profile_test sketches_are_bounded_mergeable_and_seeded -- --exact`

Expected: compile failure because profile types are absent.

- [ ] **Step 3: Implement neutral evidence types**

Evidence contains observations and confidence only—no generator kinds. Store counts and bounded summaries, never every value. Every sketch implements:

```rust
pub trait EvidenceAccumulator {
    type Observation<'a>;
    type Evidence;
    fn observe(&mut self, value: Self::Observation<'_>);
    fn merge(&mut self, other: Self) -> Result<(), ProfileError>;
    fn finish(self) -> Self::Evidence;
    fn retained_bytes(&self) -> usize;
}
```

Keep this trait crate-private until a second consumer proves the abstraction. Export stable evidence structs plus `DumpProfiler`; future `infer` may consume those without freezing accumulator internals.

- [ ] **Step 4: Add adversarial budget tests**

Feed one million unique 4 KiB strings and assert `retained_bytes()` remains within the configured per-column budget. Truncate retained samples to a configured byte ceiling and record truncation evidence.

- [ ] **Step 5: Verify and commit**

Run: `cargo test --test generate_profile_test sketches`

```bash
git add src/profile src/lib.rs tests/generate_profile_test.rs Cargo.toml Cargo.lock
git commit -m "feat(profile): add bounded shape evidence sketches"
```

### Task 19: Stream SQL dumps into a neutral `DumpProfile`

**Files:**
- Create: `src/profile/profiler.rs`
- Modify: `src/profile/mod.rs`
- Modify: `src/schema/mod.rs`
- Test: `tests/generate_profile_test.rs`
- Create: `tests/fixtures/generate/production_shape.sql`

**Interfaces:**
- Produces: `DumpProfiler::builder()`, `DumpProfiler::profile_path`, `DumpProfiler::profile_reader`
- Consumes: production `Parser`, `SchemaBuilder`, row visitors, `ProfileBudget`

- [ ] **Step 1: Add schema/basic/full depth tests**

Use one fixture containing DDL, multi-row INSERT, PostgreSQL COPY-equivalent data in a dialect fixture, nulls, skew, duplicates, FKs, composite keys, timestamps, JSON, and credential-like names. Assert schema depth reads no values, basic depth fills cheap metrics, and full depth adds correlations while every depth returns the same portable schema and exact row counts.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_profile_test profile_depths_respect_their_budgets -- --exact`

Expected: compile failure because `DumpProfiler` is absent.

- [ ] **Step 3: Implement the profiler as one streaming pass**

First build schema evidence while statements arrive, then route each decoded row through per-column accumulators. Track COPY column order and explicit INSERT column lists. Basic depth computes counts, nulls, min/max, approximate distinct, length, and top-k. Full depth additionally computes pairwise evidence only for bounded candidates: declared FK pairs, same-table temporal pairs, and columns nominated by planner heuristics.

If DDL appears after data for a table, retain at most the configured sample and replay it once schema arrives; emit `GEN-PROFILE-SCHEMA-LATE` if the bounded replay cannot cover early rows.

- [ ] **Step 4: Test all dialect input paths and large statements**

Run: `cargo test --test generate_profile_test profiler -- --nocapture`

Expected: PASS for MySQL, PostgreSQL, SQLite, and MSSQL fixture variants; resident retained evidence is budget-bound, not row-count-bound.

- [ ] **Step 5: Commit**

```bash
git add src/profile src/schema tests/generate_profile_test.rs tests/fixtures/generate
git commit -m "feat(profile): stream dumps into neutral evidence"
```

### Task 20: Infer explicit models through registered heuristics

**Files:**
- Create: `src/profile/heuristics/mod.rs`
- Create: `src/profile/heuristics/schema.rs`
- Create: `src/profile/heuristics/semantic.rs`
- Create: `src/profile/heuristics/distribution.rs`
- Create: `src/profile/heuristics/relationship.rs`
- Create: `src/profile/heuristics/planner.rs`
- Create: `src/profile/heuristics/credential.rs`
- Create: `src/generate/generators/observed.rs`
- Modify: `src/generate/registry.rs`
- Test: `tests/generate_profile_test.rs`

**Interfaces:**
- Produces: `ModelInference::infer(profile, registry, options) -> InferenceResult`
- Produces: `InferenceResult { model, decisions, warnings }`
- Consumes: neutral `DumpProfile`, heuristic descriptors, confidence thresholds

- [ ] **Step 1: Add precedence, confidence, and safety tests**

```rust
#[test]
fn explicit_schema_and_safety_rules_beat_weak_name_matches() {
    let profile = profile_column("users", "password", "varchar(255)", observed_hashes());
    let inferred = ModelInference::standard().infer(profile).unwrap();
    assert_eq!(inferred.rule("users.password").generator.kind(), "credential.password_hash");
    assert!(inferred.rule("users.password").source_literals().is_empty());
    assert_eq!(inferred.decision("users.password").reason, "credential_name_guard");
}
```

Cover precedence: explicit override > schema constraint > credential guard > relationship/planner > strong semantic name+shape > observed distribution > type fallback. Test ambiguous IDs/emails/timestamps remain conservative and produce explainable low-confidence decisions.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_profile_test explicit_schema_and_safety_rules_beat_weak_name_matches -- --exact`

Expected: compile failure because `ModelInference` is absent.

- [ ] **Step 3: Implement small registered heuristics**

Each heuristic receives evidence, proposes a typed rule, confidence, reason code, and required evidence. The coordinator sorts by precedence, confidence, descriptor kind, then registration order. It records winning and rejected candidates for `--explain` without values.

Schema heuristics create identity/sequence, constant/default, boolean, numeric, decimal, timestamp, JSON, and fallback string rules. Relationship heuristics use declared FKs first and name/type candidates second. Credential heuristics default to synthetic-only regardless of observed samples. Planner heuristics only nominate the three Phase 3A planners when every required column/relationship is present.

- [ ] **Step 4: Implement the Phase 2 observed/statistical generators**

Register `observed_sample`, `histogram`, `normal`, `lognormal`, and `monotonic` plus compiled shape parameters for null rate, strings, categories, numeric/temporal ranges, and sequence gaps. Compile weights/bins/ranges once; keep only model-resolved bounded values. Reject non-finite parameters, unsorted/overlapping bins, impossible type/range combinations, and empty observed samples.

Statistical tests use tolerant shape assertions and exact seed-repeatability assertions. `observed_sample` tests also assert the source-literal risk marker propagates into the plan/report.

- [ ] **Step 5: Ensure emitted models are self-contained**

Freeze observed row counts as the resolved integer `count` while retaining `kind: observed` (do not rewrite to `fixed`), persist explicit rule arguments and exact bounded distributions, set `defaults.inference: disabled`, and omit retained raw samples. The CLI emitter includes bounded non-literal explanation summaries; a library `EmitOptions::include_profiles(false)` removes those summaries. Literal values required by an explicit resolved rule remain in that rule and trigger the source-derived warning; there is no “safe to store” inference mode.

- [ ] **Step 6: Verify and commit**

Run: `cargo test --test generate_profile_test inference -- --nocapture`

```bash
git add src/profile/heuristics src/generate/generators/observed.rs src/generate/registry.rs tests/generate_profile_test.rs
git commit -m "feat(generate): infer explicit rules from dump profiles"
```

### Task 21: Add dump-to-model and dump-to-SQL workflows

**Files:**
- Modify: `src/generate/mod.rs`
- Modify: `src/cmd/generate.rs`
- Modify: `src/synthetic/model.rs`
- Modify: `tests/generate_cli_test.rs`
- Modify: `tests/generate_profile_test.rs`

**Interfaces:**
- Produces: `RunMode::EmitModel` (extends the Phase 1 `RunMode` with the emit-model mode)
- Produces: source-literal risk scan and `--explain` report
- Consumes: `DumpProfiler`, `ModelInference`, merge/compiler/engine pipeline

- [ ] **Step 1: Add failing primary-workflow tests**

Test:

```text
generate production.sql -o synthetic.sql
generate production.sql --emit-config model.yaml --dry-run
generate --config model.yaml -o synthetic.sql
generate production.sql --config overrides.yaml --emit-config resolved.yaml -o synthetic.sql
```

Assert the first infers and generates, the second only writes a complete model, the third needs no source, and the fourth writes and executes the same resolved decisions. Removing optional profiles/source metadata from `resolved.yaml` must not alter seeded output.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_cli_test dump_workflows -- --nocapture`

Expected: FAIL because input profiling is not wired into `generate`.

- [ ] **Step 3: Wire the lifecycle without duplicate compilation**

Implement: load source schema/profile → infer base if needed → merge overrides → compile exactly once → optionally serialize the resolved model → optionally execute that same plan. When unseeded, the emitted model records no seed and the report records the effective seed; seeded emit/reload is byte-equivalent, while unseeded runs are intentionally fresh.

- [ ] **Step 4: Add conservative source-derived warnings**

Scan explicit and inferred rules before output. `observed_sample`, categorical literal values, source defaults, checks, and explicit observed credentials set `source_values_used = true`. Emit a warning to stderr even with `--quiet`; JSON reports contain paths and rule kinds but never values. Explicit use is allowed, and this safety notice is not promoted to a blocking error by `--strict`.

- [ ] **Step 5: Verify CLI routing and commit**

Run: `cargo test --test generate_cli_test && cargo test --test generate_profile_test`

```bash
git add src/generate src/cmd/generate.rs src/synthetic tests/generate_cli_test.rs tests/generate_profile_test.rs
git commit -m "feat(generate): synthesize data from existing dumps"
```

### Phase 2 checkpoint

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run `cargo test`.
- [ ] Profile each dialect fixture at schema/basic/full depth and assert bounded retained bytes in the JSON report.
- [ ] Run `cargo run -- generate tests/fixtures/generate/production_shape.sql --seed 42 --emit-config /tmp/sql-splitter-model.yaml -o /tmp/sql-splitter-synthetic.sql`.
- [ ] Remove the optional `profiles` key from the emitted model, regenerate with seed `42`, and assert byte-for-byte equality.
- [ ] Run `cargo run -- validate /tmp/sql-splitter-synthetic.sql` and expect exit `0`.

---

## Phase 3: Correlated families, planners, and verification

### Task 22: Add protected family state and atomic outputs

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `src/generate/output.rs`
- Modify: `src/generate/engine.rs`
- Modify: `src/generate/plan.rs`
- Create: `tests/generate_output_test.rs`

**Interfaces:**
- Produces: `ProtectedSpool`, `AtomicOutput`, `PublicationSet`
- Produces: `FamilyState::{ParentState,ChildSpool,TableSpool}`
- Consumes: compiled phase/budget estimates and `tempfile`

- [ ] **Step 1: Add permissions, cleanup, and destination-preservation tests**

On Unix, create a spool under umask `000` and assert mode `0600`. Simulate generation, verification, and publication failures and assert existing output/model destinations retain their original bytes. Assert ordinary errors remove all registered temporary files.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_output_test protected_spools -- --nocapture`

Expected: compile failure because protected output types are absent.

- [ ] **Step 3: Implement protected exclusive files**

Use `OpenOptions::create_new(true)` with cryptographically unpredictable names; on Unix apply `OpenOptionsExt::mode(0o600)`. Use the destination directory only for files that require same-filesystem atomic rename, otherwise the configured/OS temp directory. Never print a temporary path at normal verbosity because the name itself may reveal workflow details.

Before publishing, preserve an existing destination's mode. For a new destination on Unix, create an empty random sibling with normal `File::create` semantics, read its umask-adjusted mode, remove it, and apply that mode to the verified temporary output immediately before rename. Thus temporary data remains owner-only while the final file follows normal output-file permissions. Test both existing and new destinations.

Install one process-level Ctrl-C handler backed by an atomic cancellation flag. Profiling/generation/verification loops check it, return a cancellation error, and let registered spool/output guards clean up. Document that SIGKILL, power loss, or machine failure can still leave protected files.

Spool records are length-prefixed typed rows with a version byte, table ID, row index, and encoded `GeneratedValue` fields. Readers reject oversized lengths and version mismatches before allocation.

- [ ] **Step 4: Execute explicit family phases**

Compile `ExecutionPhase::{Table,Family,DeferredConstraints}`. A family receives an exact memory budget and chooses parent state or a protected spool before generation begins. Crossing the budget spills deterministically; it never retains all child rows in an unbounded `Vec`.

- [ ] **Step 5: Verify and commit**

Run: `cargo test --test generate_output_test && cargo test --test generate_engine_test family`

```bash
git add Cargo.toml Cargo.lock src/generate/output.rs src/generate/engine.rs src/generate/plan.rs tests/generate_output_test.rs tests/generate_engine_test.rs
git commit -m "feat(generate): add protected family spooling and atomic output"
```

### Task 23: Implement `temporal.interval`

**Files:**
- Create: `src/generate/planners/interval.rs`
- Modify: `src/generate/planners/mod.rs`
- Create: `tests/generate_planner_test.rs`

**Interfaces:**
- Produces: `TemporalIntervalFactory` and compiled interval planner
- Consumes: normative `temporal.interval` YAML, timestamp/duration evidence, ownership registry

- [ ] **Step 1: Add invariant and compile-diagnostic tests**

For 100,000 rows across DST boundaries, assert every closed row satisfies `end = start + duration`, every open row has the configured coherent null/flag state, range bounds hold, and seeded output repeats. Assert compile errors for a non-nullable end with positive open probability, negative/overflowing durations, invalid IANA zones, missing owned columns, and ownership collisions.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_planner_test temporal_interval -- --nocapture`

Expected: compile failure because `TemporalIntervalFactory` is absent.

- [ ] **Step 3: Compile all variants into one typed plan**

Support `start.kind = observed_range | range | monotonic`, `duration.kind = histogram | uniform | normal | fixed | observed`, `timezone = preserve | utc | <IANA name>`, `open_probability`, and `end_inclusive`. Convert durations to checked nanoseconds and timestamps to one internal UTC instant plus rendering-zone metadata.

- [ ] **Step 4: Generate owned values together**

The planner owns configured start/end/duration/open columns. Draw start, open state, and duration from separate stable streams, then derive rather than redraw dependent columns. Return exact verification predicates with the compiled planner.

- [ ] **Step 5: Verify and commit**

Run: `cargo test --test generate_planner_test temporal_interval`

```bash
git add src/generate/planners tests/generate_planner_test.rs Cargo.toml Cargo.lock
git commit -m "feat(generate): add temporal interval planner"
```

### Task 24: Implement `workflow.progress_counters`

**Files:**
- Create: `src/generate/planners/progress.rs`
- Modify: `src/generate/planners/mod.rs`
- Test: `tests/generate_planner_test.rs`

**Interfaces:**
- Produces: `ProgressCountersFactory`
- Consumes: normative progress YAML and integer/status/timestamp columns

- [ ] **Step 1: Add state-machine property tests**

Generate complete, active, and not-started mixtures. With `partition: exact`, assert `succeeded + failed = processed`, `pending = total - processed`, all counters are nonnegative and ordered, completed states have `processed = total` plus non-null `completed_at`, and active states are incomplete with null completion.

Add failures for unsigned overflow, absent status vocabulary, impossible non-null completion constraints, missing columns, and exact partition with incompatible observed evidence.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_planner_test progress_counters -- --nocapture`

Expected: compile failure because the planner is absent.

- [ ] **Step 3: Implement progress variants**

Support `progress.kind = mixture | complete | in_progress | not_started | observed`, `partition = exact | allow_unclassified`, configurable status groups, and optional completion timestamp. Normalize mixture weights at compile time and reject zero-total or non-finite weights.

Choose total and lifecycle state first, then partition exact integer amounts using stable substreams. Do not let independent generators own any claimed counter/status/timestamp.

- [ ] **Step 4: Verify and commit**

Run: `cargo test --test generate_planner_test progress_counters`

```bash
git add src/generate/planners tests/generate_planner_test.rs
git commit -m "feat(generate): add progress counter planner"
```

### Task 25: Implement `commerce.order_family`

**Files:**
- Create: `src/generate/planners/order_family.rs`
- Modify: `src/generate/planners/mod.rs`
- Modify: `src/generate/engine.rs`
- Test: `tests/generate_planner_test.rs`
- Modify: `tests/fixtures/generate/simple.yaml`

**Interfaces:**
- Produces: `OrderFamilyFactory`
- Consumes: named child table/relationship, parent/child mappings, required child row distribution

- [ ] **Step 1: Add exact minor-unit property tests**

Generate orders with 0/8/25% tax, discounts, shipping, mixed quantities, large values, and 0/2/3 currency scales. For each order assert child line totals, discounts, and taxes sum exactly to parent minor-unit totals and `grand_total = subtotal - discount + tax + shipping`. Test all `largest_remainder | last_line | bankers` modes.

Add compile failures for undefined child/relationship, relationship attached to another table, zero possible lines with nonzero minimum, ambiguous currency scale, decimal overflow, missing mapped columns, and ownership conflicts.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_planner_test order_family -- --nocapture`

Expected: compile failure because `OrderFamilyFactory` is absent.

- [ ] **Step 3: Compile the normative structured YAML only**

Require `children`, `relationship`, `columns` (with at least subtotal+total), `child_columns`, `currency_scale`, and `rounding`. `tax`, and the discount/shipping column mappings, are optional with defaults (default: zero tax, no discount, no shipping), so the spec's normative example (which omits `tax:`, discount, and shipping) compiles. The child's required `rows.distribution` is the sole line-count source. Reject the old flat planner form as an unknown-field error.

- [ ] **Step 4: Plan parent and children as one family**

Represent money as checked `i128` minor units; never use floats for equations. Generate line quantities/prices, allocate discounts/tax/remainders according to the selected algorithm, derive parent totals, then spool child rows until dependency order reaches the child table. Seed every order family by parent row index so spill thresholds do not change values.

- [ ] **Step 5: Verify and commit**

Run: `cargo test --test generate_planner_test order_family && cargo test --test generate_engine_test family`

```bash
git add src/generate/planners src/generate/engine.rs tests/generate_planner_test.rs tests/generate_engine_test.rs tests/fixtures/generate/simple.yaml
git commit -m "feat(generate): add exact order family planner"
```

### Task 26: Verify generated SQL before publication

**Files:**
- Create: `src/generate/verify.rs`
- Modify: `src/generate/output.rs`
- Modify: `src/generate/mod.rs`
- Create: `tests/generate_verify_test.rs`

**Interfaces:**
- Produces: `GenerationVerifier`, `VerificationReport`, `CheckStatus::{Exact,Sampled,NotChecked}`
- Consumes: compiled constraints/planner predicates, protected temporary SQL, profile tolerances

- [ ] **Step 1: Add exact, sampled, and failed-publication tests**

Corrupt one row each for arity, non-null, PK, unique, FK, composite FK, interval, progress, and order-family equations; assert the named exact check fails and the old destination remains. Test a distribution just inside/outside tolerance and assert the report labels it sampled rather than exact.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_verify_test -- --nocapture`

Expected: compile failure because `GenerationVerifier` is absent.

- [ ] **Step 3: Reparse protected output and audit declared constraints**

Use the production parser and row visitors. Keep dense/random-access key domains in memory; spill exact unique/FK membership indexes with sorted 64-bit hashes and collision confirmation data when their budget is exceeded. Verify row counts, arity, nullability, keys, FKs, generated columns, planner predicates, SQL renderability, expected DDL, and tables.

Approximate checks compare only explicitly declared/profile-derived distributions with recorded tolerances. `NotChecked` is never reported as success under another label.

- [ ] **Step 4: Publish only after the full report passes**

`--verify` requires a filesystem SQL destination. When SQL and emitted model are both requested, finish both temporary files, verify SQL, then rename each; if the second rename fails, report partial publication precisely rather than claiming pairwise atomicity. Stdout verification is a usage error.

- [ ] **Step 5: Verify and commit**

Run: `cargo test --test generate_verify_test && cargo test --test generate_output_test`

```bash
git add src/generate/verify.rs src/generate/output.rs src/generate/mod.rs tests/generate_verify_test.rs tests/generate_output_test.rs
git commit -m "feat(generate): verify output before publication"
```

### Task 27: Add common temporal lifecycle planners

This plan deliberately schedules the Phase 3B planners (Tasks 27–29) as part of the initial release rather than following the spec's softer "may ship incrementally" hedge.

**Files:**
- Create: `src/generate/planners/structural.rs`
- Modify: `src/generate/planners/mod.rs`
- Test: `tests/generate_planner_test.rs`

**Interfaces:**
- Produces: `temporal.timestamps`, `temporal.soft_delete`, `temporal.lifecycle`

- [ ] Write failing invariant tests for create/update ordering, null/non-null delete timestamps and flags, and legal lifecycle transitions.
- [ ] Run `cargo test --test generate_planner_test temporal_structural -- --nocapture` and confirm red.
- [ ] Implement each planner as a separate factory/compiled type registered through the existing catalog; share private timestamp range helpers, not one option-heavy planner.
- [ ] Reject impossible nullability, status vocabulary, or timestamp ranges at compile time.
- [ ] Run the focused tests and commit as `feat(generate): add temporal lifecycle planners`.

### Task 28: Add common relationship and hierarchy planners

**Files:**
- Modify: `src/generate/planners/structural.rs`
- Modify: `src/generate/planners/mod.rs`
- Modify: `src/generate/compiler.rs`
- Test: `tests/generate_planner_test.rs`

**Interfaces:**
- Produces: `relation.polymorphic_pair`, `relation.junction_pair`, `relation.tenant_family`, `hierarchy.tree`

- [ ] Add failing tests for type/key pairing, unique junction pairs, same-tenant FK selection, bounded-depth trees, root ratios, and cycle handling.
- [ ] Run `cargo test --test generate_planner_test relation_structural -- --nocapture` and confirm red.
- [ ] Implement target-type/key atomic selection, deterministic pair-index permutation for junction uniqueness, tenant-partitioned key domains, and parent-before-child tree levels.
- [ ] Defer supported FK constraints for self/multi-table cycles; reject a required non-null cycle with no constructible seed or deferrable constraint.
- [ ] Run focused tests and commit as `feat(generate): add structural relationship planners`.

### Task 29: Add coordinate and file metadata planners

**Files:**
- Modify: `src/generate/planners/structural.rs`
- Modify: `src/generate/planners/mod.rs`
- Test: `tests/generate_planner_test.rs`

**Interfaces:**
- Produces: `geo.coordinate_pair`, `file.metadata`

- [ ] Add failing tests for valid latitude/longitude pairs, optional bounding boxes/precision, coherent file name/extension/MIME/size/hash metadata, and seeded stability.
- [ ] Run `cargo test --test generate_planner_test metadata_structural -- --nocapture` and confirm red.
- [ ] Implement independent registered factories; file content hashes are clearly synthetic unless an explicit constant/observed rule owns them.
- [ ] Run focused tests and commit as `feat(generate): add coordinate and file planners`.

### Phase 3 checkpoint

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run `cargo test`.
- [ ] Generate and verify all three exemplar planner fixtures for every output dialect.
- [ ] Force a 1 KiB family budget and a 1 GiB family budget with the same seed; assert byte-identical SQL.
- [ ] Fail generation and verification deliberately; assert existing destinations remain and protected temporary files are cleaned up.
- [ ] Run `cargo run -- generate --config tests/fixtures/generate/multi_tenant.yaml --seed 42 --verify -o /tmp/sql-splitter-planners.sql` and expect exit `0`.

---

## Phase 4: Filtering, migration, documentation, and hardening

### Task 30: Harden DDL filtering and cross-dialect output

**Files:**
- Modify: `src/render/ddl.rs`
- Modify: `src/render/sql.rs`
- Modify: `src/generate/compiler.rs`
- Modify: `src/convert/types.rs`
- Create: `tests/generate_filter_test.rs`
- Modify: `tests/convert_unit_test.rs`
- Modify: `tests/convert_integration_test.rs`

**Interfaces:**
- Produces: normalized filtered schema and `DeferredConstraintPlan`
- Consumes: selected tables, relationship optionality, source/output dialect, strictness

- [ ] **Step 1: Add filter and dialect matrix tests**

For each source/output dialect pair, cover: all tables/same dialect preserving original DDL; excluded independent table; include/exclude collision where exclude wins; retained required FK to excluded table; optional detached FK; local and cross-table indexes; generated/default/identity columns; lossy type mapping; deferred cycles.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test generate_filter_test -- --nocapture`

Expected: at least the required-dependency and detached-FK cases fail before the new normalization pass.

- [ ] **Step 3: Compile a filtered portable schema before rendering**

Exact names beat globs within an include or exclude list; exclude always wins between lists. A retained non-null/required data dependency on an excluded table is `GEN-EXCLUDED-DEPENDENCY` (the same code Task 9 uses for the "excluded required dependency" condition, so one stable code covers the condition; the message may differ between selection-time (Task 9) and DDL-render-time (Task 30) but the code is shared). An explicitly detachable optional relationship removes its FK/standalone reference and warns; `--strict` promotes that warning to exit `1`. Keep local indexes unless an indexed expression references an absent object.

- [ ] **Step 4: Render only normalized affected DDL**

Preserve original DDL byte-for-byte only when the entire source schema remains structurally unchanged and source/output dialects match. Otherwise render affected tables and constraints through existing conversion mappings. Add warning codes for each lossy conversion and fail them under strict mode.

- [ ] **Step 5: Verify generated dumps in real engines where CI already supports them**

Run: `cargo test --test generate_filter_test && cargo test --test convert_unit_test && cargo test --test convert_integration_test`

```bash
git add src/render src/generate/compiler.rs src/convert/types.rs tests/generate_filter_test.rs tests/convert_unit_test.rs tests/convert_integration_test.rs
git commit -m "feat(generate): filter and normalize generated DDL"
```

### Task 31: Replace Rust fixture helpers with the public generation API

**Files:**
- Create: `tests/fixtures/generate/legacy_fixture.yaml`
- Create: `tests/support/generated_fixture.rs`
- Create: `tests/support/mod.rs`
- Create: `tests/fixture_migration_test.rs`
- Modify: `tests/sample_integration_test.rs`
- Modify: `tests/validate_test.rs`
- Modify: `tests/mssql_integration_test.rs`

**Interfaces:**
- Produces: test-only `generated_fixture(dialect, rows, tables, seed) -> TempPath`
- Consumes: public `GenerateBuilder`; no imports from private generation modules

- [ ] **Step 1: Snapshot the old fixture contract before migration**

Record the tables, columns, relationships, scale presets, row formulas, and dialect-specific expectations used by the three integration tests in `legacy_fixture.yaml`. Add a test that generates small fixtures with both implementations and compares schema shape, exact table counts, FK validity, and the semantic properties the tests actually rely on; byte equality is not required.

- [ ] **Step 2: Run the contract test against both implementations**

Run: `cargo test --test fixture_migration_test -- --nocapture`

Expected: PASS before consumers move; this is the characterization gate.

- [ ] **Step 3: Implement one public-API helper**

Load `legacy_fixture.yaml`, apply row/table overrides through typed builder options, set the explicit test seed, and write to a temp file. Tests that need the same dump multiple times generate it once per test case and reuse the path; do not regenerate per assertion.

- [ ] **Step 4: Migrate every Rust test consumer**

Replace `test_data_gen::{Generator,RenderConfig,Renderer,Scale}` imports in the three named tests. Keep the characterization test temporarily so both implementations remain comparable until Task 32.

- [ ] **Step 5: Verify and commit**

Run: `cargo test --test sample_integration_test && cargo test --test validate_test && cargo test --test mssql_integration_test`

```bash
git add tests/fixtures/generate/legacy_fixture.yaml tests/support tests/sample_integration_test.rs tests/validate_test.rs tests/mssql_integration_test.rs tests/fixture_migration_test.rs
git commit -m "test: migrate generated fixtures to public API"
```

### Task 32: Migrate fixture scripts and remove `test_data_gen`

**Files:**
- Modify: `scripts/profile-memory.sh`
- Modify: `scripts/bench-validate-memory.sh`
- Modify: `scripts/verify-io-strategies.sh`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Delete: `crates/test_data_gen/Cargo.toml`
- Delete: `crates/test_data_gen/src/lib.rs`
- Delete: `crates/test_data_gen/src/bin/main.rs`
- Delete: `crates/test_data_gen/src/generator.rs`
- Delete: `crates/test_data_gen/src/renderer.rs`
- Delete: `crates/test_data_gen/src/streaming.rs`
- Delete: remaining files under `crates/test_data_gen/`
- Delete: `tests/fixture_migration_test.rs`

- [ ] **Step 1: Add shell smoke coverage**

Add a `--generate-only --size tiny` test mode assertion that produces valid MySQL, PostgreSQL, and SQLite dumps through `target/release/sql-splitter generate`. Add an MSSQL generation smoke invocation to the script test even if the main profiler matrix remains three dialects.

- [ ] **Step 2: Run scripts against `generate` before deleting the crate**

Run: `cargo build --release && ./scripts/profile-memory.sh --generate-only --size tiny`

Expected: PASS and no script builds or invokes `gen-fixtures`.

- [ ] **Step 3: Preserve the useful preset UX in model/CLI overrides**

Map existing tiny/small/medium/large/xlarge/huge/mega/giga presets to explicit `--table-rows`/table-selection arguments or committed override YAML, so benchmark sizes retain their documented meaning. Keep explicit seeds in repeatable benchmark scripts.

- [ ] **Step 4: Remove the workspace member and old crate**

Remove `crates/test_data_gen` from workspace members and dev-dependencies, regenerate `Cargo.lock`, then assert:

Run: `rg 'test_data_gen|gen-fixtures' Cargo.toml Cargo.lock src tests scripts`

Expected: no matches.

- [ ] **Step 5: Verify and commit**

Run: `cargo test && ./scripts/profile-memory.sh --generate-only --size tiny`

```bash
git add Cargo.toml Cargo.lock scripts tests
git rm -r crates/test_data_gen tests/fixture_migration_test.rs
git commit -m "refactor: replace gen-fixtures with generate"
```

### Task 33: Document the product, model language, and library API

**Files:**
- Create: `docs/generate/README.md`
- Create: `docs/generate/model-reference.md`
- Create: `docs/generate/generators.md`
- Create: `docs/generate/planners.md`
- Create: `docs/generate/profiling-and-privacy.md`
- Create: `docs/generate/library-api.md`
- Create: `docs/generate/diagnostics.md`
- Modify: `README.md`
- Modify: `docs/ROADMAP.md`
- Modify: `website/src/content/docs/roadmap.mdx`
- Create: `website/src/content/docs/commands/generate.mdx`
- Modify: `website/llms.txt`
- Modify: `skills/sql-splitter/SKILL.md`
- Modify: `CHANGELOG.md`
- Modify: `examples/generate-man.rs`
- Modify: `tests/cmd_unit_test.rs`
- Create: `tests/cli_help_test.rs`

- [ ] **Step 1: Add documentation command checks**

Extend help/snapshot tests to assert the command and every CLI flag from the canonical spec. Add doctests for the builder and staged API, plus executable snippets for dump-to-SQL, dump-to-model, model-only generation, overrides, check, dry-run/explain, verify, seed inheritance/null randomization, and table selection.

- [ ] **Step 2: Run and confirm the documentation checks identify missing pages/snapshots**

Run: `cargo test --test cli_help_test && cargo test --doc`

- [ ] **Step 3: Write task-oriented documentation**

The command page starts with the four primary workflows. The reference pages document every top-level/table/schema/row/relationship/generator/modifier/planner field, accepted value, default, conflict, ownership rule, and diagnostic. Planner docs include the three fully annotated examples approved in the spec and concise recipes for Phase 3B planners.

The privacy page states that output is synthetic, not anonymized; explains source-literal warning classes; says explicit observed credentials remain allowed; documents temp-file cleanup limits after hard termination; and shows how to create a literal-free model.

- [ ] **Step 4: Update every command-discovery surface**

Regenerate clap help, man pages, and completions through the repository's generators. Update README, website, `llms.txt`, and Agent Skill decision guidance. Mark the old test-data-generator roadmap item superseded by the product feature; keep future `infer` reuse as a separate roadmap item.

- [ ] **Step 5: Verify and commit**

Run: `make man && cargo test --test cli_help_test && cargo test --doc`

```bash
git add README.md CHANGELOG.md docs website skills man src tests
git commit -m "docs: document synthetic data generation"
```

### Task 34: Benchmark, profile, and harden real-world generation

**Files:**
- Modify: `benches/generate_bench.rs`
- Modify: `scripts/profile-memory.sh`
- Create: `scripts/benchmark-generate.sh`
- Create: `benchmark-results/generate-baseline.md`
- Modify: `docs/generate/profiling-and-privacy.md`
- Modify: tests/fixtures or regression tests for accepted real-world dump shapes

- [ ] **Step 1: Establish reproducible benchmark cases**

Measure release/native builds for: renderer-only fixed schema; hand-authored model with core generators; seeded/unseeded runs; profile depths; one exemplar planner; spill forced/not forced; 1/10/100 tables; 10K/1M rows. Record wall time, user time, throughput, peak RSS, output bytes, CPU model, compiler flags, and commit.

- [ ] **Step 2: Profile before optimizing**

Run Criterion plus the repository memory profiler. On macOS, use Instruments or `sample`; on Linux, use `perf`. Attribute time to compilation, RNG/generator dispatch, escaping, batching, writes, profiling, and planner/spool I/O.

- [ ] **Step 3: Optimize only evidenced hot paths**

Preserve precompiled descriptor/factory lookup, per-column compiled function objects, reusable row/batch buffers, buffered RNG, and bounded sketches. Specialize homogeneous fixed-width rows or batch writes only when the profile shows material value. Correctness, deterministic stream stability, and memory tests run after each optimization.

- [ ] **Step 4: Survey authorized local dumps without storing source values**

Run schema/basic profile checks over `/Users/helge/Downloads/dumps`. Record only filenames redacted to stable hashes, dialect, size bucket, schema features, inferred rule/planner counts, diagnostics, runtime, and memory. Turn parser/schema/rule failures into minimal synthetic regression fixtures; never commit source literals or dump fragments.

- [ ] **Step 5: Apply release gates**

Correctness: all exact constraints/planner properties pass. Memory: generation remains bounded by batch/family budgets; profiling by configured evidence budget. Performance: report configurable-generation overhead versus renderer-only baseline; treat 20% as an optimization target, not a blocker. No unexplained regression above 10% in repeated median throughput.

- [ ] **Step 6: Verify and commit**

Run: `cargo bench --bench generate_bench && ./scripts/benchmark-generate.sh && make profile && make verify-realworld`

```bash
git add benches/generate_bench.rs scripts benchmark-results/generate-baseline.md docs/generate tests/fixtures
git commit -m "perf(generate): benchmark and harden generation"
```

### Phase 4 checkpoint

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run `cargo test --all-features` and `cargo test --no-default-features`.
- [ ] Run `make schemas`, `make man`, `make verify-realworld`, and `make profile`.
- [ ] Run `rg 'test_data_gen|gen-fixtures' Cargo.toml Cargo.lock src tests scripts README.md docs website skills` and confirm remaining matches occur only in changelog/history/supersession notes.
- [ ] Confirm every CLI example in docs runs successfully against a release build.
- [ ] Confirm a source-derived warning survives `--quiet` and no human/JSON diagnostic prints the source value.

---

## Phase 5: Publish the YAML model schema

### Task 35: Generate and validate the synthetic-model JSON Schema

**Files:**
- Modify: `src/synthetic/model.rs`
- Modify: `src/synthetic/overrides.rs`
- Modify: `src/json_schema.rs`
- Create: `src/cmd/schema.rs`
- Modify: `src/cmd/mod.rs`
- Modify: `tests/json_schema_tests.rs`
- Create: `schemas/generate-config.schema.json`
- Create: `website/public/schemas/generate-config.schema.json`
- Modify: `Makefile`
- Modify: `justfile`
- Modify: `docs/generate/model-reference.md`
- Modify: `website/src/content/docs/commands/generate.mdx`

- [ ] **Step 1: Add failing schema validation tests**

Validate every committed generate YAML fixture against the generated schema. Assert rejection of unknown fields, unknown document roles, invalid seed types, invalid finite generator/planner variants, and structurally invalid relationship/count rules. Validate both `kind: model` and `kind: overrides` roots.

- [ ] **Step 2: Run and confirm red**

Run: `cargo test --test json_schema_tests generate_config -- --nocapture`

Expected: failure because the config schema is not published.

- [ ] **Step 3: Derive stable structural schemas and compose extension arguments**

Use `schemars` on the typed model/override structs. Registry-owned `arguments` cannot be represented as unrestricted YAML: build a `oneOf` branch per standard registered generator/modifier/planner descriptor, with descriptor argument schemas and `additionalProperties: false`. Statically linked custom registries validate at runtime; the shipped schema documents the standard registry only.

- [ ] **Step 4: Fold config schemas into the existing command**

Extend `sql-splitter schema` so `make schemas` emits JSON output schemas plus `generate-config.schema.json`, validates fixtures, formats files, and copies them to the website. Do not add a second schema-generation pipeline.

- [ ] **Step 5: Add editor discovery**

Document the public schema URL and YAML-language-server header example. Do not require a `$schema` field in YAML v1; if present, treat it as recognized metadata rather than an unknown field.

- [ ] **Step 6: Verify and commit**

Run: `make schemas && cargo test --test json_schema_tests`

```bash
git add src/synthetic src/json_schema.rs src/cmd/schema.rs tests/json_schema_tests.rs schemas website/public/schemas Makefile justfile docs/generate website/src/content/docs/commands/generate.mdx
git commit -m "feat(generate): publish synthetic model schema"
```

### Task 36: Evaluate schema reuse for other YAML-backed commands

**Files:**
- Create: `docs/superpowers/specs/2026-07-16-generate-schema-reuse-assessment.md`
- Inspect: sample/shard/redact configuration types and schema command support

- [ ] Inventory every YAML-backed command, whether it has a typed serde model, unknown-field handling, and a stable public format.
- [ ] Compare reuse of the generate schema composition helper for `sample`, `shard`, and `redact`; do not merge unrelated model languages.
- [ ] Record a concrete yes/no/later decision per command with exact prerequisite and acceptance test.
- [ ] If a command is an immediate fit, add a separately approved implementation plan; do not expand this implementation silently.
- [ ] Commit the assessment as `docs: assess config schema reuse`.

### Phase 5 checkpoint

- [ ] Run `make schemas` twice and assert no diff.
- [ ] Validate every generate YAML fixture with both `jsonschema` tests and a common YAML language server.
- [ ] Confirm runtime validation remains authoritative and produces the same or better path-specific diagnostics.
- [ ] Confirm schema publication introduced no dependency or parsing work in the generation hot path.

---

## Deferred Work Packets

These are deliberately retained so “later” has a concrete starting point. None is part of the initial release or required before replacing `gen-fixtures`.

### Statistical and temporal distributions

- Seasonal/time-series distributions, business calendars/holiday-aware dates, and Zipf/power-law distributions.
- Activate after real emitted models require shapes that histograms/weighted choices cannot represent without visible error.
- Reuse registered generator factories and bounded evidence; add goodness-of-fit fixtures and performance budgets before shipping.

### Multi-column and deep structured values

- Covariance-preserving generation and deep JSON from JSON Schema or inferred shapes.
- Activate only after the profiler can retain privacy-reviewed correlation/shape evidence within a fixed budget.
- Prefer a scoped planner or typed JSON generator; do not add YAML expression evaluation.

### Rich geographic reference data

- Geospatial polygons/routes and verified country/subdivision/postcode domains.
- Activate when a versioned, redistributable data source and update policy are selected.
- Keep `geo.coordinate_pair` independent of this packet.

### Specialized accounting, inventory, subscription, and workflow planners

- Inventory ledgers/stock reconciliation, double-entry accounting, subscriptions/renewals/proration, recurrence schedules, generic finite-state machines, general cross-table aggregates, and all Phase 3C planners:
  `temporal.job_execution`, `commerce.invoice_family`, `commerce.quote_family`, `finance.tax_breakdown`, `finance.payment_balance`, `workflow.job_counters`, `workflow.import_counters`, `workflow.rolling_counters`.
- Activate one planner at a time from a concrete invariant and representative schema. Each gets normative YAML, compile diagnostics, property tests, verification predicates, and a descriptor; do not begin with a generic expression engine.

### Privacy-safe model emission

- Intentionally deferred: no automatic claim that observed/source values are safe to store.
- Activate only after a separately reviewed threat model defines classification, redaction, provenance, and false-negative handling.
- Until then, emit no source literals by default, warn conservatively when explicitly retained, and describe output as synthetic rather than anonymized.

### Non-SQL input profiling

- CSV, JSON, and JSONL profiling/input.
- Activate after the SQL evidence/model boundary is stable. Add format-specific schema mapping into the same neutral `DumpProfile`; do not teach generation factories to parse input formats.

### Parallel profiling and generation

- Activate after single-thread profiles identify CPU-parallel work and deterministic merge order is specified.
- Preserve stable seeded output across thread counts, bounded aggregate memory, ordered SQL output, and deterministic sketch merges.

### Registry-specific editor schemas

- Phase 5 publishes the standard CLI registry schema. A library API that emits a schema for a custom statically linked registry remains deferred.
- Activate when at least one external library consumer registers an extension and needs editor validation.

### Explicitly not planned

Runtime CLI plugins, dynamic-library plugins, and WASM plugins are not deferred work. Extension remains a statically linked Rust library API unless a future product decision explicitly reopens that scope.

---

## Legacy Plan Reconciliation

The restored `wip/ORIGINAL-PLAN-DATAGENERATOR.md` was reviewed as historical input. Its durable ideas are represented here: reusable models, dump-derived shape inference, semantic generators, relationships, planners, deterministic seeds, a public API, and fixture migration. The canonical spec resolves the old draft's ambiguous document roles, row-count scaling, flat planner syntax, source-literal handling, plugin scope, exit codes, and model completeness. Do not implement superseded shapes from the historical file when they conflict with the canonical spec.

---

## Specification Coverage

| Requirement or decision | Implementation tasks |
| --- | --- |
| Complete `model` vs partial `overrides`, imports, unknown fields | 3, 4, 8 |
| Complete emitted model, frozen counts/rules, disabled inference | 9, 20, 21 |
| Source/model/override compatibility boundary | 1, 8, 10 |
| Global/table seed inheritance, `null`, randomize, stable streams | 3, 5, 9, 21 |
| Count precedence, scale, relationship-child no-double-scale | 9 |
| Registry-driven generators, modifiers, planners, heuristics | 7, 11, 12, 20, 23–29 |
| Core/semantic/credential/temporal generator catalog | 11, 12 |
| Simple/composite relationships and dependency ownership | 10, 13 |
| Four-dialect SQL and DDL behavior | 14, 30 |
| CLI modes, conflicts, stdout ownership, exit codes | 16, 21, 26 |
| Public builder, staged API, custom static registry | 7, 15 |
| Bounded schema/basic/full profiler and neutral evidence | 17–19 |
| Explainable precedence/confidence and schema-only inference | 20, 21 |
| Source-derived warnings and credential safe defaults with explicit override | 12, 20, 21 |
| Correlated Phase 3A planners | 23–25 |
| Common Phase 3B planners | 27–29 |
| Family phases, bounded/protected spools, cycles | 22, 25, 28 |
| Exact/approximate verification and atomic publication | 22, 26 |
| Include/exclude precedence and detached DDL/FKs | 9, 30 |
| Existing fixture/script migration and old crate removal | 31, 32 |
| Complete documentation surfaces | 33 |
| Correctness, bounded memory, measured performance target | 6, 17–19, 22, 26, 34 |
| Existing JSON Schema pipeline reused later | 35, 36 |
| Deferred catalog retained without YAML expressions/plugins | Deferred Work Packets |

---

## Final Verification and Handoff

- [ ] Review every task diff before advancing; do not combine unrelated task commits.
- [ ] At the end of each phase, update checkbox state and record benchmark/diagnostic deviations directly under that checkpoint.
- [ ] Run `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features`, and `cargo test --no-default-features`.
- [ ] Run `make schemas`, `make man`, `make verify-realworld`, and the generation benchmark/profile scripts.
- [ ] Inspect `git diff --check`, generated artifacts, public API docs, help/man/completion output, website docs, `llms.txt`, Agent Skill, changelog, and roadmap.
- [ ] Confirm no config compiler diagnostic occurs after output publication begins.
- [ ] Confirm all exact verification checks pass for each dialect and every planner fixture.
- [ ] Confirm retained memory is controlled by explicit parser/profile/batch/family/verification budgets, not total source/generated rows.
- [ ] Confirm the final branch no longer contains a workspace `test_data_gen` crate or executable `gen-fixtures` consumer.
- [ ] Request a code review focused separately on public API compatibility, SQL correctness, privacy/temp-file behavior, and measured performance before merge.
