# Synthetic data generation

**Status:** Approved design, pending implementation plan

**Product surface:** `sql-splitter generate` and the `sql_splitter` library

**Initial input:** MySQL, PostgreSQL, SQLite, and MSSQL SQL dumps

**Canonical configuration:** Versioned YAML `SyntheticModel`

`generate` turns a schema, an existing SQL dump, or a reusable YAML model into a fast, relationally consistent synthetic SQL dump. It replaces the private `gen-fixtures` test utility with a public product feature and composable library API.

This document is the canonical design. It supersedes the old test-data-generator and schema-inference designs. The completed [`gen-fixtures` performance work](2026-07-16-gen-fixtures-performance-design.md) remains the rendering baseline and implementation evidence.

## Decision summary

- Make synthetic generation a first-class command and public library API.
- Use one versioned YAML format with explicit `kind: model | overrides` document roles.
- Let users generate directly from a dump, emit a complete model, or generate from a model without the source dump.
- Freeze every resolved rule and final integer table count in emitted models.
- Preserve input DDL when possible; render normalized DDL when filtering, overrides, or cross-dialect output change it.
- Scan the complete input for exact row counts while keeping profiling state bounded.
- Use typed, registered generators, modifiers, planners, and inference heuristics. Do not add a YAML expression language.
- Generate keys and common multi-column/cross-table invariants correctly by construction.
- Keep generation streaming and deterministic when seeded.
- Keep profiler evidence reusable without freezing internals around the future `infer` command.
- Warn when inferred output or emitted YAML may reproduce source literals. Synthetic output is not anonymized output.
- Defer privacy-safe export and editor-facing JSON Schema integration until after the core feature.

## Goals

1. Reproduce the schema, scale, sparsity, distributions, and relationship shape of an existing SQL dump with synthetic values.
2. Generate useful data from DDL alone using schema and name heuristics, with confidence reported for inferred choices.
3. Let users override row counts, seeds, generators, relationships, and planners globally or per table.
4. Keep YAML understandable, reviewable, versioned, lintable, and usable without its source after emission.
5. Make new built-ins local to one implementation, descriptor, registry entry, and test set.
6. Expose profiling, model compilation, generation, and rendering independently through the library.
7. Replace all project fixture generation with the new engine without regressing correctness or bounded-memory behavior.

## Initial release scope

The initial release covers SQL dump/schema input, the standard registry, bounded basic/full profiling, model/override YAML, all four SQL dialects, deterministic streams, relationship-aware generation, the Phase 3A planners, preflight checks, verification, and public library stages.

Phase 3B planners may ship incrementally when their common machinery makes them local additions. Their absence does not block the first usable command. Phase 3C and the deferred inventory are follow-up work.

## Explicitly out of scope

- A general expression, scripting, loop, variable, or conditional language in YAML.
- Arbitrary code execution from configuration.
- Remote imports.
- Runtime CLI plugins, dynamic libraries, and WASM plugins. Library registration is the supported extension mechanism.
- Direct database connections in the initial design.
- Binary database backup formats.
- A guarantee that generated output or emitted YAML is anonymized.

## Primary workflows

Generate directly from a dump:

```bash
sql-splitter generate production.sql -o synthetic.sql
```

Emit an editable, self-contained model:

```bash
sql-splitter generate production.sql --emit-config synthetic.yaml
sql-splitter generate --config synthetic.yaml -o synthetic.sql
```

Apply overrides and save the resolved result:

```bash
sql-splitter generate production.sql \
  --config overrides.yaml \
  --emit-config resolved.yaml \
  --scale 0.1 \
  -o synthetic.sql
```

Generate from schema-only SQL:

```bash
sql-splitter generate schema.sql --rows 1000 -o synthetic.sql
```

Validate or inspect before writing SQL:

```bash
sql-splitter generate --config synthetic.yaml --check
sql-splitter generate production.sql --config overrides.yaml --dry-run --explain
```

Generate, audit, and atomically publish:

```bash
sql-splitter generate --config synthetic.yaml --verify -o synthetic.sql
```

## Architecture

```text
SQL dump ──> schema extraction ──┐
                                ├─> SyntheticModel ─> compiler + registry ─> GenerationPlan
SQL rows ──> bounded profiler ───┘                                      │
YAML model/overrides ────────────────────────────────────────────────────┘
                                                                         │
                                                                         v
                                                            streaming generation
                                                                         │
                                                                         v
                                                              dialect renderer/output
```

The system has five public stages:

1. `DumpProfiler` streams schema and rows into a bounded `ProfileReport`.
2. `SyntheticModel` stores the portable schema, resolved rules, optional profile evidence, and overrides.
3. `ExtensionRegistry` describes and resolves generators, modifiers, planners, and heuristics.
4. `ModelCompiler` validates the merged model and produces an immutable `GenerationPlan`.
5. `GenerationEngine` executes the plan in dependency order and streams rows to a renderer.

YAML is never interpreted in a row-generation hot path. Compilation resolves names, arguments, types, seeds, ownership, dependencies, counts, and execution strategies before generation starts.

## Model lifecycle

### Document roles are explicit

Every YAML document declares one role:

```yaml
version: 1
kind: model       # model | overrides
```

A `model` is self-contained:

- Every table has a complete normalized schema.
- Every table has an exact final integer `rows.count`.
- Every generated column has an explicit owner, unless `defaults.inference: schema` opts into schema-only inference.
- Every source-dependent rule contains the facts it needs without the source dump.
- All referenced relationships, generators, modifiers, and planners resolve within the model and registry.

An `overrides` document is a partial patch:

- Tables, schemas, columns, relationships, rules, and defaults may be omitted.
- Missing fields mean “leave the source/base value unchanged.”
- It requires an input dump or a base `model` supplied through the library.
- It may change generation behavior but cannot silently redefine source DDL structure.

The explicit role prevents a misspelled or omitted field from silently turning a broken model into a valid patch.

### Inference is explicit

```yaml
defaults:
  inference: schema    # schema | disabled
```

With `schema`, columns without an owner use type-, constraint-, and name-based heuristics. Missing data profiles do not affect this mode. With `disabled`, every generated column needs an explicit generator or planner owner.

Hand-authored models can opt into `schema` to stay concise. `--emit-config` writes every selected rule explicitly and sets `inference: disabled`, so a future heuristic change cannot alter a saved model.

### Emitted models freeze decisions

`--emit-config` writes a complete `kind: model` containing:

- the normalized schema;
- exact final integer counts for every table;
- complete child-distribution parameters;
- explicit generator, modifier, relationship, and planner rules;
- resolved output settings;
- the user-supplied seed, or no seed for an unseeded run;
- optional profile and inference explanations.

`profiles` is removable metadata. Removing it cannot change generation.

An observed rule therefore includes its resolved count:

```yaml
rows:
  kind: observed
  count: 182340
```

An unresolved `observed` rule without a source/base count is a compile error. When scaling would require stochastic rounding, emission stores the already rounded integer count.

Relationship-derived tables may retain their allocation rule, but also store the exact total:

```yaml
rows:
  kind: relation.children
  parent: orders
  count: 6140
  distribution:
    kind: observed
    mean: 3.4
    min: 1
    max: 50
```

The total is stable; the distribution determines how those children are allocated across the generated parents.

### Source, model, and overrides merge predictably

Precedence is:

```text
source facts < inferred defaults < YAML overrides < broad CLI overrides < per-table CLI overrides
```

Legal overrides include counts, seeds, generators, modifiers, planners, relationship distributions, inclusion, and rendering settings. These intentional changes are not mismatches.

A compile mismatch means an instruction cannot apply:

- a referenced table or column does not exist;
- a generator cannot produce the target type;
- relationship keys are missing or incompatible;
- a planner's required columns or relationship are absent;
- an explicitly required source fingerprint does not match.

An observed fingerprint change is a warning by default because production dumps change. A model can require an exact fingerprint for CI.

Overrides cannot silently change DDL column types, nullability, keys, or existence. Structural changes require a complete `model` or a future schema-transformation feature.

When a complete model and input are supplied, the model supplies schema and generation rules while the input may refresh profile observations. Structural incompatibility is an error; data/profile differences are expected.

## CLI contract

### Invocation modes

| Input | Config | Result |
| --- | --- | --- |
| SQL dump | none | Profile, infer, compile, and generate |
| SQL dump | `kind: overrides` | Profile, patch, compile, and generate |
| SQL dump | `kind: model` | Check schema compatibility, optionally refresh observations, and generate |
| none | `kind: model` | Compile and generate without source access |
| none | `kind: overrides` | Error: no schema/base source |
| none | complete model + `--check` | Parse and compile without generation |

`INPUT` may be `-`. Operations needing a second pass spool stdin to a protected temporary file.

Without `--output` or `--emit-config`, generated SQL goes to stdout. With `--emit-config` and no `--output`, only the model is written. Supplying both writes both artifacts.

### Input and model options

| Option | Meaning |
| --- | --- |
| `[INPUT]` | SQL dump or schema file. Optional with a complete model. |
| `-c, --config <PATH>` | A complete model or overrides document. Local YAML only. |
| `--emit-config <PATH>` | Write the resolved complete model. |
| `--profile-depth <basic|full>` | Select bounded profiling depth. Default: `basic`. |
| `--profile-sample <N>` | Override the per-table sample budget, subject to safety caps. |
| `--input-dialect <DIALECT>` | Set input dialect when detection is unavailable or ambiguous. |

### Volume options

| Option | Meaning |
| --- | --- |
| `--scale <FACTOR>` | Scale independently counted/root tables. Conflicts with `--rows`. |
| `--rows <N>` | Set the broad count for independently counted/root tables. Conflicts with `--scale`. |
| `--table-rows <PATTERN=N>` | Set an exact count for matching tables. Repeatable. |
| `--table-scale <PATTERN=FACTOR>` | Replace the global scale for matching tables. Repeatable. |
| `--max-rows <N>` | Cap each table after other count rules. |
| `--tables <PATTERN,...>` | Select the initial table set. |
| `--exclude <PATTERN,...>` | Remove tables from the selected set. Exclusion wins. |

Patterns use the project's case-insensitive glob matcher. Exact table names beat globs for repeated count flags; later equally specific flags win.

Count resolution is exact:

1. Start with the source/model count.
2. Apply `--scale` or `--rows` to independently counted/root tables.
3. Apply either `--table-rows` or `--table-scale` to an explicitly matched root/independent table.
4. Apply `--max-rows` last for that table.
5. Derive each relationship child from its parent's final count and stored fan-out distribution. Do not apply the global control a second time.
6. Apply a matching child `--table-rows` or `--table-scale`, then its `--max-rows`; descendants derive from that final child count.
7. Validate relationship minima and planner invariants throughout the dependency traversal.

`--table-rows` is absolute and is never scaled again. On a root table,
`--table-scale` replaces rather than multiplies the global scale. On a relationship
child, it scales the count derived from the final parent and intentionally alters
fan-out. The two per-table controls conflict when both resolve to the same table.
Impossible totals—such as fewer requested children than the declared minimum per
parent—are compile errors.

Selection resolution is also exact:

1. `--tables` selects the initial set.
2. `--exclude` removes matching tables and always wins.
3. The compiler computes required dependencies.
4. Excluding a required dependency is an error with the dependency path; the tool never silently restores it.

### Randomness options

| Option | Meaning |
| --- | --- |
| `--seed <U64>` | Override the top-level model seed. |
| `--randomize` | Remove the top-level seed for this run. Conflicts with `--seed`. |

If no seed is present, generation uses fresh entropy. A top-level seed makes inheriting tables deterministic. Each table can inherit, override, or opt out:

This fragment omits each table's required `rows`/`schema` for brevity and is illustrative, not a complete document.

```yaml
seed: 42

tables:
  inherits:
    # omitted table seed: inherit 42
  independent:
    seed: 9001
  always_random:
    seed: null
```

`--randomize --emit-config` writes no top-level seed. The report includes the effective run seed so a user can reproduce that run manually. Exact emit/reload byte equivalence applies only to seeded models; emitted row counts remain exact in all models.

`--seed` and `--randomize` affect only the top-level seed. Explicit integer table
seeds remain deterministic, and `seed: null` tables remain random. Reusing the
reported effective run seed therefore cannot reproduce a table that explicitly
opts out with `seed: null`.

Stable streams derive from the root/table seed, normalized table identity, normalized column or planner identity, and operator identity. Adding an unrelated table, column, or generator does not reshuffle existing streams.

### Rendering options

| Option | Meaning |
| --- | --- |
| `-o, --output <PATH>` | SQL output path; `-` writes stdout. |
| `--dialect <DIALECT>` | Output dialect. Default: preserve source/model dialect. |
| `--schema-only` | Render DDL without rows. |
| `--data-only` | Render rows without DDL. |
| `--batch-size <N>` | Rows per INSERT/COPY batch where supported. |
| `--no-copy` | Render PostgreSQL INSERT statements instead of COPY. |
| `--compress <FORMAT>` | Use supported output compression for file output. |
| `--mssql-production-style` | Emit production-style MSSQL wrappers/batches. |
| `--mssql-go <N>` | Emit MSSQL `GO` after the configured batch interval. |

`--dialect` overrides `output.dialect`; otherwise output inherits the source dialect.

### Preflight and reporting options

| Option | Meaning |
| --- | --- |
| `--check` | Parse and compile a complete model without source or generation. |
| `--dry-run` | Profile if needed, compile, and report the plan without SQL output. |
| `--verify` | Generate to protected temporary storage, audit, then publish atomically. |
| `--explain` | Include inference reason, confidence, and precedence. |
| `--strict` | Promote selected warnings to failure. |
| `--progress` | Show profiling/generation progress on stderr. |
| `--json` | Write a machine-readable command report to stdout. |
| `--quiet` | Suppress non-error human output except the source-literal safety warning. |

`--json` owns stdout. It therefore requires SQL and emitted models to use real files. `--json --output -` and `--json --emit-config -` are usage errors. `--check --json` and `--dry-run --json` work because they do not produce SQL.

`--verify` requires `--output <FILE>`. `--verify --emit-config model.yaml -o synthetic.sql` publishes both only after SQL verification succeeds. `--verify --emit-config model.yaml` is a usage error; use `--check` to validate only a model.

### Conflicts

- `--check`, `--dry-run`, and `--verify` are mutually exclusive.
- `--schema-only` and `--data-only` are mutually exclusive.
- `--randomize` and `--seed` are mutually exclusive.
- `--scale` and `--rows` are mutually exclusive.
- `--quiet` and `--progress` are mutually exclusive.
- `--emit-config -` conflicts with `--output -` and `--json`.
- `--check` requires `kind: model` and conflicts with `INPUT`.
- `--verify` and `--compress` require file output.
- A table matched by both `--table-rows` and `--table-scale` is an error.

### Exit codes

Exit behavior follows the existing application and common CLI convention:

| Code | Meaning |
| ---: | --- |
| `0` | Successful generation, check, dry run, or verification. |
| `1` | Model/config invalid, strict warning, runtime/I/O failure, generation failure, or verification failure. |
| `2` | Invalid CLI arguments or conflicting options. |

Structured diagnostics and JSON report fields distinguish failure categories. A compile error and a promoted warning still exit `1`; all independent diagnostics are reported where possible.

## YAML specification

The first schema version is `1`. Unknown fields and duplicate keys are errors. New optional fields may be added within a version; breaking changes require a new version and upgrader. This strictness applies to the typed document structure; registry-owned generator/modifier/planner **argument** payloads are validated against their descriptor at compile time rather than at YAML parse time, so an unknown argument key is reported as a compile diagnostic, not a parse error.

### Complete model example

This example is self-contained and uses the normative `commerce.order_family` shape:

```yaml
version: 1
kind: model

source:
  dialect: mysql
  fingerprint: sha256:0123456789abcdef
  fingerprint_policy: warn        # ignore | warn | require

output:
  dialect: postgres
  mode: schema_and_data            # schema_and_data | schema_only | data_only
  inserts: auto                    # auto | insert | copy
  batch_size: 1000

seed: 840219

defaults:
  inference: disabled              # schema | disabled

tables:
  customers:
    rows: { kind: observed, count: 50000 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: email, type: varchar(255), nullable: false, unique: true }
        - { name: status, type: varchar(32), nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      email:
        generator: { kind: internet.email }
        modifiers:
          - { kind: unique }
      status:
        generator:
          kind: weighted_choice
          values:
            - { value: active, weight: 0.86 }
            - { value: paused, weight: 0.09 }
            - { value: closed, weight: 0.05 }

  orders:
    seed: null                      # random even though the model has a seed
    rows:
      kind: relation.children
      parent: customers
      count: 210000
      distribution: { kind: observed, mean: 4.2, min: 0, max: 30 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: false }
        - { name: subtotal, type: decimal(12,2), nullable: false }
        - { name: tax_total, type: decimal(12,2), nullable: false }
        - { name: grand_total, type: decimal(12,2), nullable: false }
    relationships:
      - name: orders_customer
        columns: [customer_id]
        references: { table: customers, columns: [id] }
        distribution: observed
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      customer_id:
        generator: { kind: relation.foreign_key, relationship: orders_customer }
    planners:
      - kind: commerce.order_family
        children: order_items
        relationship: order_items_order
        columns:
          subtotal: subtotal
          tax: tax_total
          total: grand_total
        child_columns:
          quantity: quantity
          unit_price: unit_price
          tax: tax_amount
          line_total: line_total
        currency_scale: 2
        rounding: largest_remainder

  order_items:
    rows:
      kind: relation.children
      parent: orders
      count: 714000
      distribution: { kind: observed, mean: 3.4, min: 1, max: 50 }
    schema:
      name: order_items
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: order_id, type: bigint, nullable: false }
        - { name: quantity, type: integer, nullable: false }
        - { name: unit_price, type: decimal(12,2), nullable: false }
        - { name: tax_amount, type: decimal(12,2), nullable: false }
        - { name: line_total, type: decimal(12,2), nullable: false }
    relationships:
      - name: order_items_order
        columns: [order_id]
        references: { table: orders, columns: [id] }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      order_id:
        generator: { kind: relation.foreign_key, relationship: order_items_order }
      # quantity, prices, tax, and line_total are owned by commerce.order_family.

profiles:
  customers.status:
    rows: 182340
    null_fraction: 0.0
    distinct_estimate: 3
    inference:
      selected: weighted_choice
      confidence: high
      reasons: [low_cardinality, stable_top_values, status_name]
```

### Overrides example

```yaml
version: 1
kind: overrides

seed: 42

tables:
  audit_events:
    rows: { kind: observed, scale: 0.01 }
  users:
    columns:
      email:
        generator: { kind: internet.email }
```

An overrides document can omit schema/count details because the input or base model supplies them.

### Top-level fields

| Field | `model` | `overrides` | Meaning |
| --- | --- | --- | --- |
| `version` | required | required | Model schema version. |
| `kind` | `model` | `overrides` | Explicit document role. |
| `source` | optional | optional | Provenance and fingerprint policy. |
| `output` | optional | optional | Dialect and renderer defaults. |
| `seed` | optional | optional | Top-level `u64`; omitted means random. |
| `defaults` | optional | optional | Inherited table/column behavior. |
| `imports` | optional | optional | Local partial documents. |
| `tables` | required | optional | Complete tables or partial patches. |
| `profiles` | optional | optional | Removable bounded observations/explanations. |

### Imports

Imports split large configs without adding a template language:

```yaml
version: 1
kind: model
imports:
  - tables/core.yaml
  - tables/commerce.yaml

tables:
  orders:
    seed: 42
```

Rules:

- Imported files are `kind: overrides` and resolve relative to the importing file.
- Imported files cannot import other files.
- Two imports cannot define the same configuration path.
- The root document may explicitly override imported values.
- Maps merge by key; lists replace as a whole and never concatenate implicitly.
- `null` clears a value only where that field permits null.
- The merged document must satisfy the root role.
- Remote, recursive, parameterized, and conditional imports are rejected.

### Row-count rules

Complete models always store `count`:

```yaml
rows: { kind: observed, count: 10000 }
rows: { kind: fixed, count: 5000 }
rows: { kind: scale, base: 1000, factor: 2.0, count: 2000 }
rows:
  kind: relation.children
  parent: orders
  count: 7600
  distribution:
    kind: histogram                # observed | fixed | uniform | poisson | histogram
    mean: 3.8
    min: 1
    max: 25
```

`relation.children.distribution` is required in a complete model. An overrides document may omit its count or distribution when the source/base supplies it. `--emit-config` always writes both.

### Schema representation

Each table stores its normalized name, original DDL when available, ordered columns, primary key, unique constraints, indexes, checks, and relationships. Columns store normalized and source types, nullability, default, generated/identity status, collation, and optional semantic annotation.

Hand-authored column entries may use a concise `type:` form (e.g. `{ name: id, type: bigint }`) as shorthand for `source_type`, with the `SqlTypeFamily` derived automatically; emitted models (`--emit-config`) always write the canonical `source_type` + `family` fields.

Original DDL is preferred only when the source and output dialect match and the selected schema set is unchanged. Cross-dialect output and affected tables use normalized schema rendering.

### Column rules

```yaml
columns:
  email:
    semantic: internet.email
    generator:
      kind: internet.email
      locale: en
    modifiers:
      - { kind: null_rate, probability: 0.03 }
      - { kind: unique, attempts: 32, on_exhaustion: error }
```

A generator produces the base value. Modifiers form an ordered typed pipeline. The compiler checks type compatibility, nullability, length, uniqueness feasibility, and ownership conflicts.

### Relationships

```yaml
relationships:
  - name: order_tenant
    columns: [tenant_id, customer_id]
    references:
      table: customers
      columns: [tenant_id, id]
    distribution: observed

  - name: category_parent
    columns: [parent_id]
    references: { table: categories, columns: [id] }
```

Tree hierarchies and polymorphic references are configured on planners, not on relationships: self-referential tree shape (roots ratio, max depth, branching) is owned by the `hierarchy.tree` planner reading a plain self-FK relationship by name; polymorphic type/id pairs are owned by the `relation.polymorphic_pair` planner via its own argument payload (no `relationships:` entry). Relationship-level shape sugar is not part of v1.

Relationships referenced by a generator or planner require explicit names. Anonymous relationships are allowed only when no rule refers to them. Names must be unique within the table.

Declared foreign keys are authoritative. Explicit YAML may add or correct generation relationships. Data-validated naming heuristics can add medium-confidence candidates with warnings; name-only matches remain suggestions.

## Extension architecture

Built-ins use typed factories and compiled runtime operations. Exact Rust names may change, but these boundaries are normative:

```rust
pub trait GeneratorFactory: Send + Sync {
    fn descriptor(&self) -> &'static GeneratorDescriptor;
    fn compile(
        &self,
        config: &serde_yaml_ng::Value,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, Vec<Diagnostic>>;
}

pub trait PlannerFactory: Send + Sync {
    fn descriptor(&self) -> &'static PlannerDescriptor;
    fn compile(
        &self,
        config: &serde_yaml_ng::Value,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, Vec<Diagnostic>>;
}

pub trait ModifierFactory: Send + Sync {
    fn descriptor(&self) -> &'static ModifierDescriptor;
    fn compile(
        &self,
        config: &serde_yaml_ng::Value,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, Vec<Diagnostic>>;
}

pub trait InferenceHeuristic: Send + Sync {
    fn descriptor(&self) -> &'static HeuristicDescriptor;
    fn propose(&self, evidence: &InferenceEvidence<'_>, proposals: &mut ProposalSet);
}
```

Descriptors provide stable kind names, aliases, documentation, arguments, types, defaults, bounds, ownership/read sets, determinism, buffering, verification capabilities, and discovery metadata.

An `ExtensionRegistry` has typed generator, modifier, planner, and heuristic catalogs. Heuristics propose typed rules with confidence/reasons; they never mutate a model or generate values.

Adding a built-in requires its implementation, descriptor, registration, and tests.
It must not require edits to YAML dispatch, renderer, or unrelated built-ins.
Statically linked library consumers can register custom factories and heuristics in
their own registry. Runtime CLI/WASM plugins are not planned.

## Generator catalog and delivery phases

### Phase 1: hand-authored models

| Family | Required kinds |
| --- | --- |
| Core | `constant`, `null`, `sequence`, `copy`, `template`, `pattern`, `database_default`, generic `json_value` |
| Typed random | `integer`, `decimal`, `boolean`, `string`, `bytes`, `uuid` |
| Categorical | `choice`, `weighted_choice` |
| Semantic | Person/name, email/internet, phone, company, address, commerce, text, identifiers, file, and network kinds |
| Credentials | `credential.password_hash`, `credential.token`, `credential.api_key`, `credential.secret`, `credential.placeholder` |
| Temporal | `date`, `time`, `datetime`, `duration`, `before`, `after` |
| Relationships | `relation.foreign_key`, `relation.composite_key` |
| Modifiers | `null_rate`, `unique`, prefix/suffix/truncate/case, clamp, round, type-specific format |

`template` joins literal fragments and referenced fields. It does not evaluate conditions or arbitrary expressions.

Initial semantic kinds are explicit and extensible:

| Family | Initial kinds |
| --- | --- |
| Person | `person.first_name`, `person.last_name`, `person.full_name`, `person.username`, `person.title` |
| Internet | `internet.email`, `internet.domain`, `internet.url`, `internet.ipv4`, `internet.ipv6`, `internet.user_agent` |
| Contact | `phone.number`, `phone.country_code` |
| Organization | `company.name`, `company.department`, `company.job_title` |
| Address | `address.line1`, `address.line2`, `address.city`, `address.region`, `address.postcode`, `address.country`, `address.latitude`, `address.longitude` |
| Commerce | `commerce.product_name`, `commerce.sku`, `commerce.currency`, `commerce.money`, `commerce.quantity` |
| Text | `text.word`, `text.sentence`, `text.paragraph`, `text.slug` |
| Identifiers | `identifier.ulid`, `identifier.nanoid`, `identifier.token`, `identifier.hash` |
| Files/network | `file.name`, `file.extension`, `file.mime_type`, `file.size`, `network.mac`, `network.port` |

### Phase 2: production-shape inference

| Family | Required kinds/behavior |
| --- | --- |
| Observed | `observed_sample` with bounded weighted values |
| Statistical | `histogram`, `normal`, `lognormal` |
| Shape | Observed null rate, string length, categories, ranges, temporal distributions |
| Sequence | `monotonic` and sequence-gap reproduction |
| Semantic inference | Select Phase 1 semantic generators from evidence |
| Relationship inference | Observed FK and composite-key distributions |

### Phase 3: complex relationships

- self-referential trees;
- polymorphic target/ID pairs;
- junction pairs;
- same-tenant relationships;
- cross-table family generation and protected spooling.

### Credential defaults

High-confidence credential-like columns never infer `observed_sample`, categorical source values, or source defaults. They choose synthetic-only credential generators. Token generators may preserve observed length, character classes, and prefix shape without retaining complete values. Private-key-like fields use clearly invalid placeholders.

Explicit user configuration always wins. A user may deliberately choose `observed_sample`, `constant`, or another source-derived value for a credential column. The compiler warns that output is synthetic, not anonymized, but never blocks the explicit choice—even under `--strict`.

## Planners for correlated invariants

A planner owns two or more output columns or coordinates a parent/child family. It chooses related values together instead of relying on independent generators to coincide.

The compiler records every claimed output. Two generators/planners cannot own the same column. A planner may read another owned value only when declared and acyclic.

Planner-specific reference examples define the normative YAML shape.

### `commerce.order_family`

```yaml
planners:
  - kind: commerce.order_family
    children: order_items
    relationship: order_items_order
    columns:
      subtotal: subtotal
      discount: discount_total
      tax: tax_total
      shipping: shipping_total
      total: grand_total
    child_columns:
      quantity: quantity
      unit_price: unit_price
      discount: discount_amount
      tax: tax_amount
      line_total: line_total
    currency_scale: 2
    rounding: largest_remainder     # largest_remainder | last_line | bankers
    tax:
      kind: weighted_choice
      rates: [0.0, 0.08, 0.25]
      weights: [0.05, 0.15, 0.80]
```

The child table's required `rows.distribution` supplies line counts; the planner
does not duplicate that distribution. `children` identifies the child table, and
`relationship` resolves within that table. The planner chooses quantities, prices,
discount, tax, and optional shipping as one family and computes exact decimal
minor units. `largest_remainder` distributes residuals so child sums equal the
parent exactly. Missing relationships, impossible precision, zero allowed lines
with a non-zero minimum, and ownership conflicts fail compilation.

### `workflow.progress_counters`

```yaml
planners:
  - kind: workflow.progress_counters
    columns:
      total: total_rows
      processed: processed_rows
      succeeded: imported_rows
      failed: failed_rows
      pending: pending_rows
      status: status
      completed_at: completed_at
    progress:
      kind: mixture                # mixture | complete | in_progress | not_started | observed
      complete_weight: 0.72
      active_weight: 0.23
      not_started_weight: 0.05
    partition: exact               # exact | allow_unclassified
    completed_statuses: [completed, failed]
    active_statuses: [queued, running]
```

With `partition: exact`, succeeded plus failed equals processed, and pending equals total minus processed. Completed states require processed equal total and a completion timestamp; active states remain incomplete with a null completion timestamp.

### `temporal.interval`

```yaml
planners:
  - kind: temporal.interval
    columns:
      start: started_at
      end: ended_at
      duration: duration_seconds
      open: is_running
    start:
      kind: observed_range         # observed_range | range | monotonic
      min: 2024-01-01T00:00:00Z
      max: 2026-01-01T00:00:00Z
    duration:
      kind: histogram              # histogram | uniform | normal | fixed | observed
      unit: seconds
      min: 30
      max: 43200
    open_probability: 0.07
    end_inclusive: true
    timezone: preserve             # preserve | utc | named IANA zone
```

Closed rows satisfy the end/start/duration equation. Open rows have a null end and coherent flag/duration behavior. A non-nullable end with non-zero open probability, incompatible timezone, overflow, or negative duration fails compilation.

### Planner delivery tiers

**Phase 3A — initial-release machinery and exemplars:**

- `temporal.interval`
- `workflow.progress_counters`
- `commerce.order_family`

**Phase 3B — common structural planners:**

- `temporal.timestamps`
- `temporal.soft_delete`
- `temporal.lifecycle`
- `relation.polymorphic_pair`
- `relation.junction_pair`
- `relation.tenant_family`
- `hierarchy.tree`
- `geo.coordinate_pair`
- `file.metadata`

**Phase 3C — specialized domain planners, deferred until usage validates semantics:**

- `temporal.job_execution`
- `commerce.invoice_family`
- `commerce.quote_family`
- `finance.tax_breakdown`
- `finance.payment_balance`
- `workflow.job_counters`
- `workflow.import_counters`
- `workflow.rolling_counters`

## Bounded profiling

Profiling scans the complete dump for exact schema and row counts. Per-column and cross-column statistics use bounded sketches and deterministic samples.

### Depth budgets

| Capability | `basic` | `full` |
| --- | ---: | ---: |
| Sampled rows per table | 10,000 | 100,000 |
| Retained top categorical values | 256 | 256 |
| Exact distinct hashes before sketch-only mode | 100,000 | 100,000 |
| Numeric/temporal buckets | 64 | 64 |
| Candidate column pairs | schema/name candidates | 32 data-ranked pairs |
| Row counts/null counts | complete scan | complete scan |
| Declared schema/FKs | complete scan | complete scan |
| Composite/pair correlations | limited | enabled within pair budget |
| Planner reconnaissance | obvious groups | schema plus sampled correlations |

`--profile-sample` overrides only the sample-row budget. Hard safety caps remain.

### Shape evidence

Equivalent shape is not one entropy score. The profile combines:

- exact row and null counts;
- approximate distinct count and distinct-to-row ratio;
- top-value coverage, categorical Shannon entropy, and normalized entropy;
- numeric/temporal range, mean, variance, quantiles, skew, and histogram;
- string length, empty rate, character classes, and recognized formats;
- sortedness, monotonicity, uniqueness, and sequence gaps;
- bounded JSON value/structural fingerprints;
- key membership, parent fan-out, and sampled orphan rate;
- bounded pair evidence: equality, ordering, mutual nulls, and correlation.

The model stores only signals needed by resolved rules and explanations, not every accumulator.

### Streaming algorithms

- deterministic reservoir sampling;
- HyperLogLog-style distinct sketches;
- capped hash sets for exact low/medium cardinality;
- SpaceSaving/top-k counters;
- quantile/fixed histograms;
- Welford mean/variance accumulators;
- Bloom filters or capped hashes for membership;
- fixed-size pair statistics for approved candidates.

Profiles never retain complete columns. Input size increases scan time, not proportional profile memory.

### Inference precedence and confidence

Column inference precedence:

1. explicit YAML;
2. schema constraints/type;
3. bounded profile;
4. semantic names;
5. generic type-safe fallback.

Relationship precedence:

1. declared FK;
2. explicit YAML;
3. data-validated name candidate;
4. name-only suggestion.

Every decision records selected rule, confidence, and reasons.

| Confidence | Default behavior |
| --- | --- |
| `high` | Apply automatically. |
| `medium` | Apply with warning and explanation. |
| `low` | Suggest; use generic fallback. |

`--strict` rejects configured medium-confidence automatic choices. Explicit user choices are not blocked merely because they may contain source values.

### Heuristic catalog

| Family | Evidence/proposals |
| --- | --- |
| Schema | Keys, nullability, checks, enums, defaults, identities, type bounds |
| Semantic name | Person/contact/address/commerce/money/status/token/file/network/geo/time meanings |
| Distribution | Constant, sequence, categorical, weighted, histogram, normal/lognormal, sparse, fallback |
| Relationship | Declared/composite/self-tree/junction/tenant/polymorphic/data-validated names |
| Planner reconnaissance | Timestamps, soft delete, lifecycle, interval, totals/lines, counters, coordinates, files |
| Compatibility | SQL capacity, decimal precision, length, locale, dialect limits |

Heuristics publish proposals into a ranked set. The resolver applies precedence, rejects incompatible proposals, and records why the winner beat alternatives.

### Schema-only input

Schema-only dumps use keys, nullability, checks, defaults, types, and semantic names. Decisions that depend on absent data have lower confidence and actionable explanations.

### Future `infer` reuse

`DumpProfiler` is public because library users need neutral evidence for generation: counts, distributions, relationship observations, and confidence. Sketch implementations, accumulators, sampling algorithms, and pair selection remain private.

The future `infer` command should reuse or extend this evidence where it fits, but this design does not freeze profiler internals or promise that future CSV/JSONL readers require no contract changes. Initial interfaces are validated against `generate`, not hypothetical `infer` needs.

## Source-derived values and credentials

The initial emitter is not a sanitization feature. `observed_sample`, categorical top values, source defaults, enum/check literals, and explicit constants may originate in the input.

Before direct generation or config emission, the CLI prints one conservative warning whenever inferred rules may reproduce source literals:

```text
warning[GEN-SOURCE-VALUES]:
  Generated output may reproduce values observed in the source dump.
  Output is synthetic, not anonymized.
  Run with --explain for affected rules.
```

The warning never prints actual values and is not suppressed by `--quiet`. JSON reports include a structured warning. Detailed per-column provenance classification may be deferred if it complicates the initial implementation.

Credential heuristics use the safe synthetic defaults described in the generator catalog. Explicit source-derived credential rules remain allowed and receive the same warning.

Privacy-safe emission is deferred until it has an explicit threat model and transformations that do not silently destroy useful shape.

## Compilation and diagnostics

The compiler:

1. parses local YAML and rejects unknown/duplicate fields;
2. validates version and document role;
3. resolves non-recursive imports;
4. merges source, inference, imported/root YAML, and CLI overrides;
5. validates model completeness;
6. normalizes identifiers and SQL types;
7. resolves table selection/exclusion and required dependencies;
8. resolves final exact counts and child distributions;
9. resolves registry descriptors;
10. type-checks arguments and outputs;
11. assigns ownership of each generated column;
12. builds column/table/family dependency graphs;
13. selects streaming/parent-state/spool strategies;
14. derives stable RNG streams;
15. estimates rows, bytes, memory, temporary storage, and verification cost.

Compilation gathers independent errors rather than failing at the first one.

```text
error[GEN-COLUMN-OWNER-CONFLICT] synthetic.yaml:84:7
  tables.orders.columns.total is produced by both:
    - columns.total.generator
    - planners[0] (commerce.order_family)
  help: remove the column generator or remove `total` from the planner mapping
```

Diagnostics have stable codes, severity, YAML path, source span when available, related locations, and help. Human and JSON reports contain equivalent information.

`--dry-run` prints resolved order/counts, inference, warnings, memory/spool estimates, and verification cost. It never writes SQL.

## Execution

### Phases

1. `Table` phases stream independent tables and simple relationships.
2. `Family` phases execute correlated planners.
3. `DeferredConstraints` phases render constraints that cannot safely appear earlier.

Foreign-key order is preferred; constraints may be deferred for cycles.

### Family state strategies

| Strategy | Use |
| --- | --- |
| `ParentState` | Keep compact IDs/totals/distribution state needed by children. |
| `ChildSpool` | Plan children with the parent, spool rows, render at dependency position. |
| `TableSpool` | Materialize a protected on-disk phase when a second pass is required. |

In-memory family state has explicit budgets and spills rather than growing without bound.

### Cycles

- Self trees generate roots before descendants and may defer their FK.
- Multi-table cycles defer constraints where supported.
- A required non-null cycle with no constructible seed or deferrable constraint is a compile error.
- Polymorphic references plan the target type and key together.

### Table filtering and DDL

If all tables remain and output dialect matches input, preserve original DDL unchanged. Once filtering or structural overrides alter the schema set, render affected DDL from normalized schema.

An excluded required data dependency is a compile error. A deliberately detached optional relationship can proceed; its affected FK/standalone references are omitted with a warning. Local indexes remain unless they reference an excluded object. `--strict` promotes the removed-constraint warning to failure. Output never references an absent table.

## Temporary-file and output security

Spools and verification output can contain source-derived values or generated secrets. They therefore:

- use the destination directory when atomic rename requires it, otherwise the OS temp directory;
- use unpredictable names and exclusive creation;
- are owner-only (`0600`) on Unix regardless of a permissive umask;
- never log contents or source values;
- are removed on success, ordinary failure, and handled interruption;
- may remain after hard termination or machine failure, which documentation states explicitly.

Final output follows normal output-file permission behavior rather than forced `0600`.

File output is written beside the destination and renamed atomically after success. Existing destinations are never truncated before compile/generation success. Stdout is not atomic and cannot be verified. If verified SQL and a model are both requested, neither existing destination is replaced before verification passes; publication errors are reported without pretending the pair is atomic across filesystems.

## Verification

`--verify` audits temporary output before publication.

Exact checks include:

- row counts and arity;
- non-null, primary-key, unique, and generated-key constraints;
- FK/composite-key membership;
- planner equations/state invariants;
- SQL renderability for the selected dialect;
- expected tables and DDL.

Approximate checks compare generated distributions with declared tolerances. Reports distinguish exact, sampled, and not checked. Failed verification leaves prior destinations untouched and exits `1`.

## Rendering

The optimized streaming generator becomes the renderer foundation rather than a subprocess. Preserve its allocation-lean escaping, reusable row batches, buffered random sampling, and dialect-specific COPY/INSERT paths.

Same-dialect output preserves original DDL only where safe. Cross-dialect output reuses schema normalization and conversion mappings, warns for lossy mappings, and can fail under `--strict`.

## Public library API

Convenience API:

```rust
use sql_splitter::generate::{Generate, ProfileDepth};
use sql_splitter::Dialect;

let report = Generate::builder()
    .input("production.sql")
    .output("synthetic.sql")
    .output_dialect(Dialect::Postgres)
    .profile_depth(ProfileDepth::Basic)
    .seed(42)
    .table_scale("audit_*", 0.05)?
    .verify(true)
    .run()?;

println!("{} rows generated", report.rows_written);
```

Staged API:

```rust
let mut registry = ExtensionRegistry::standard();
registry.register_generator(MyDomainIdFactory::new())?;
registry.register_planner(MyLedgerPlannerFactory::new())?;
registry.register_heuristic(MyDomainHeuristic::new())?;

let profile = DumpProfiler::builder().depth(ProfileDepth::Basic).run(reader)?;
let inference = ModelInference::infer(&profile, &registry, InferenceOptions::default())?;
let model = inference.model;

// If overrides are present, merge them before compiling:
let model = ModelMerger::merge(model, overrides)?;

let plan = ModelCompiler::new(&registry).compile(model, CompileOptions::default())?;
let report = GenerationEngine::new(plan).run(renderer)?;
```

Public modules:

- `profile`: neutral bounded evidence, reports, and heuristic interfaces;
- `synthetic`: YAML model and normalized generation schema;
- `generate`: compiler, plan, engine, builder, reports;
- `render`: row sinks and SQL renderers;
- `diagnostic`: stable structured diagnostics.

Public entry points accept `Read`/`BufRead` and `Write` where practical. Filesystem helpers are conveniences. Send/Sync contracts reflect actual behavior rather than promising parallel execution.

## Performance goals

Correctness, bounded memory, and actionable failure are release requirements. Performance targets guide profiling and optimization; they do not override correctness.

- Basic profiling aims for at least 100 MB/s on ordinary local dumps.
- Basic profiling aims to remain below 256 MB RSS for the default 100-table workload.
- Model compilation should be negligible relative to scanning/generation.
- Built-in configurable generation aims for no more than 20% CPU overhead versus an equivalent optimized hard-coded fixture.
- All sample, exact-set, pair, family-state, and spool budgets are explicit and observable.
- Memory must not grow proportionally with total input rows.

Factories compile YAML once. Built-ins may compile to concrete operations/enums where profiling justifies it. Dynamic dispatch is acceptable at table/batch boundaries but avoided with YAML lookup, allocation, or string registry lookup for every cell. Custom library generators are not held to the built-in overhead goal.

If overhead exceeds 20%, profile and document it. Do not keep a second legacy engine. Relaxing the goal is an explicit evidence-based decision, not a hidden release shortcut.

Benchmarks cover dialects, schema-only, low/high cardinality, deep FK graphs, wide tables, planners, and 100-table stress. Report bytes/s, rows/s, peak RSS, spool bytes, and deterministic hashes where applicable.

## Real-world dump survey

A read-only survey of 12 SQL dumps in `/Users/helge/Downloads/dumps` covered 604 tables, 6,932 columns, and 687 declared relationships. Only aggregate observations informed this design.

- 91 tables lacked a declared PK; 25 used composite PKs.
- 28 relationships were self-referential; 62 tables resembled junctions.
- 284 tables had no declared FK; 34 had at least four.
- 449 tables had created/updated timestamps; 164 used soft deletes.
- 132 tables had polymorphic type/ID patterns; 14 used parent hierarchies.
- 161 columns were money-like; 411 were status-like.
- 50 tables resembled audit/event/log data.
- Progress counters, intervals, rolling counters, file metadata, lifecycle, geo, contact, credential, and network patterns recurred.
- Numeric data showed skew/repeated values, supporting histograms and weighted choices.
- Interval data included both closed valid intervals and open rows.

The catalog reflects these patterns without hard-coding surveyed schemas or values.

## Migration from existing fixtures

1. Preserve the completed `gen-fixtures` performance optimizations as the renderer baseline.
2. Extract SQL formatting, random buffers, and row-batch machinery into reusable modules.
3. Add model, registry, compiler, profiler, and engine.
4. Represent the fixed multi-tenant fixture as checked-in YAML.
5. Replace incidental correlations with relationships/planners.
6. Migrate integration tests and helpers from in-memory `Generator`/`Renderer` APIs.
7. Migrate profiling, validation, and benchmark scripts to `sql-splitter generate`.
8. Retain a simple benchmark model for direct comparison.
9. Remove the heuristic in-memory renderer and unused test-data schema model.
10. Remove `test_data_gen` and `gen-fixtures` after all consumers move.
11. Keep hand-authored static SQL parser fixtures; they test exact syntax.

Generate fixtures up front for tests that do not exercise generation itself. This keeps setup from dominating test time and makes an unseeded fixture stable within a test run.

## Delivery phases

### Phase 0: consolidate the renderer

Extract the measured fast renderer, establish regression benchmarks, and remove duplication safe to remove before the model.

### Phase 1: model-driven generation

Ship document roles, parsing/imports, schema representation, registry/compiler, Phase 1 generators, deterministic streams, simple relationships, SQL renderers, `--check`, `--dry-run`, and library façade.

Documentation: hand-authored quickstart, initial YAML reference, core catalog, shipped CLI/library reference, and reproducibility rules.

### Phase 2: profile and infer rules

Ship bounded basic/full profiling, Phase 2 generators/heuristics, direct dump generation, `--emit-config`, schema-only fallback, source-literal warning, and `--explain`.

Documentation: dump-to-synthetic guide, emit/edit/reuse guide, profiling/inference explanation, and privacy caveat.

### Phase 3: complex relationships and planners

Ship family ownership/dependencies, protected state/spools, complex relationship generators, Phase 3A planners, and verification hooks. Add Phase 3B planners where the machinery makes them local.

Documentation: planner/relationship catalog, custom library extension guide, verification, and temporary-storage behavior.

### Phase 4: migrate and harden

Replace all existing generated fixtures/scripts, finish cross-dialect behavior, add atomic verification, meet correctness/memory gates, measure performance goals, and remove `test_data_gen`.

Documentation: audit and consolidate all pages; update man pages, completions, `llms.txt`, website, Agent Skill, changelog, and roadmap; ensure references match the final CLI/descriptors.

### Phase 5: deferred extensions

Evaluate deferred items from real usage. Add editor-facing YAML schemas by reusing the existing `schemars`-based `schema` command, checked-in `schemas/`, validation tests, and website publication pipeline where feasible. Start with `generate-config.schema.json`, then evaluate `sample`, `shard`, and `redact` configs.

JSON Schema handles structure, types, enums, bounds, and built-in argument shapes. Serde/compiler remain authoritative for references, ownership, relationships, feasibility, and dialect semantics. Schema publication/editor integration is not an initial-release gate, but the implementation plan must retain it explicitly.

Registry descriptors should compose the built-in generator, modifier, and planner
variants into the generated schema. A registry-specific schema API for statically
linked custom extensions can be evaluated later; it does not block publishing the
standard CLI schema.

## Deferred inventory

- seasonal/time-series distributions;
- business calendars and holiday-aware dates;
- Zipf/power-law distributions;
- covariance-preserving multi-column generation;
- deep JSON generation from JSON Schema or inferred shapes;
- geospatial polygons and routes;
- verified country/subdivision/postcode domains;
- inventory ledgers and stock reconciliation;
- double-entry accounting planners;
- subscriptions, renewals, and proration;
- recurrence schedules;
- generic finite-state-machine planners;
- a general cross-table aggregate engine beyond scoped planners;
- Phase 3C specialized planners;
- privacy-safe model emission;
- CSV, JSON, and JSONL profiling/input;
- JSON Schema publication/editor integration for YAML configs;
- parallel profiling and generation.

## Acceptance criteria

1. A user can point `generate` at supported SQL and receive a valid, equivalently shaped synthetic dump.
2. The same input can emit a complete model that works without the source.
3. Emitted models contain explicit rules and exact counts; removable profiles do not affect output.
4. Schema-only input produces useful output with transparent confidence/fallbacks.
5. Seed inheritance, `seed: null`, randomize, stable streams, and seeded reload equivalence are tested.
6. Keys and Phase 3A planner invariants are correct by construction and pass `--verify`.
7. Invalid configs report comprehensive actionable diagnostics before output starts.
8. Library users can run each stage and register custom generators/planners/heuristics.
9. Existing generated fixtures/scripts use `generate`; `test_data_gen` is removed.
10. Profiling/generation satisfy correctness and bounded-memory gates; performance is measured and reported.
11. Each phase ships its assigned documentation; Phase 4 completes the consistency audit.
12. Spools and verification artifacts follow the temporary-file security policy and cleanup tests.
