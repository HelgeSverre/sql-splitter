# Model reference

Complete field reference for the `generate` YAML model language: `kind: model`
and `kind: overrides` documents, schema representation, row-count rules,
relationships, and the generator/modifier/planner attachment shape.

For the generator/modifier catalog itself, see [Generators](generators.md).
For planner-specific fields, see [Planners](planners.md).

## Document roles

Every document declares a schema `version` (currently only `1` is accepted)
and an explicit `kind`:

```yaml
version: 1
kind: model # model | overrides
```

| Kind        | Meaning                                                                                                                                                                                                            |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `model`     | Self-contained: every table has a complete schema, an exact final `rows` count, and every generated column has an explicit owner (unless `defaults.inference: schema` opts into heuristics). Needs no source dump. |
| `overrides` | A partial patch. Tables, schemas, columns, relationships, rules, and defaults may be omitted — missing fields mean "leave the source/base value unchanged." Requires a source dump or a base model to apply to.    |

Unknown fields and duplicate YAML keys are parse errors (`GEN-CONFIG-PARSE`)
in the document envelope and in every `deny_unknown_fields` struct below.
Registry-owned generator/modifier/planner _argument_ payloads are the one
exception: an unknown argument key is a compile-time diagnostic, not a parse
error, because those payloads are validated against the registry descriptor
during compilation.

A top-level `$schema:` key is a second, narrower exception: it is recognized
metadata for editor tooling (see
[Editor validation](#editor-validation-json-schema)), never a parse error,
and never read by the parser.

`--emit-config` always writes a complete `kind: model` document with
`defaults.inference: disabled`, so a future heuristic change can never alter
a saved model.

## Top-level fields

| Field      | `model`            | `overrides`                 | Type               | Default               | Meaning                                                       |
| ---------- | ------------------ | --------------------------- | ------------------ | --------------------- | ------------------------------------------------------------- |
| `$schema`  | optional           | optional                    | `String`           | `None`                | Editor-only schema pointer; see below. Ignored by the parser. |
| `version`  | required           | required                    | `u32`              | —                     | Must equal `1`.                                               |
| `kind`     | required (`model`) | required (`overrides`)      | tag                | —                     | Explicit document role.                                       |
| `imports`  | optional           | optional                    | `[String]` (paths) | `[]`                  | Local `kind: overrides` files merged before this document.    |
| `source`   | optional           | optional                    | object             | `None`                | Provenance and fingerprint policy.                            |
| `output`   | optional           | optional (see caveat below) | object             | all fields `None`     | Dialect and renderer defaults.                                |
| `seed`     | optional           | optional                    | `u64` or `null`    | `None` (random)       | Top-level seed; tables inherit unless they opt out.           |
| `defaults` | optional           | optional                    | object             | `inference: disabled` | Inherited column-inference behavior.                          |
| `tables`   | required           | optional                    | map                | —                     | Complete tables (`model`) or partial patches (`overrides`).   |
| `profiles` | optional           | optional                    | map                | `{}`                  | Removable bounded observations/explanations; safe to delete.  |

> **Caveat:** in the current implementation, `SyntheticOverrides.output` has
> no `#[serde(default)]`, so an `overrides` document must include an
> `output:` key (it can be `output: {}` or `output: null`-per-field) even
> though the field is conceptually optional. If you hit a parse error on an
> overrides document that omits `output:` entirely, add an empty `output: {}`
> block as a workaround.

### `source`

```yaml
source:
  dialect: mysql
  fingerprint: sha256:0123456789abcdef
  fingerprint_policy: warn # ignore | warn | require
```

In an `overrides` document, `source` replaces the base's `source` wholesale
when present — it is not merged field by field.

`fingerprint`/`fingerprint_policy` are compared in exactly one place today: when
an `overrides` document is merged onto a base model, the override's
`source.fingerprint` is checked against the base model's recorded
`source.fingerprint`. If the two differ, the effective `fingerprint_policy` (the
override's own if set, else the base's, else `ignore`) decides the outcome:
`ignore` does nothing, `warn` raises `GEN-SOURCE-FINGERPRINT` as a warning,
`require` raises it as a hard error (useful for pinning a fixture in CI). The
replacement still happens either way — the comparison only decides whether you
are _told_ about the mismatch.

> **Not yet implemented:** automatic dump-fingerprint drift detection during
> _profiling_ — computing a fingerprint from a source dump and comparing it
> against a model's recorded `source.fingerprint` on a `generate <dump>` run —
> is a planned/deferred feature. The profiling path records no fingerprint
> (`fingerprint: None`), so no comparison happens on a dump-input run today. The
> only live check is the overrides-merge (YAML-vs-YAML) comparison described
> above.

### `output`

```yaml
output:
  dialect: postgres
  mode: schema_and_data # schema_and_data | schema_only | data_only
  inserts: auto # auto | insert | copy
  batch_size: 1000
```

Every field defaults to `None`/unset. When set, the `output:` block is
**honored** as the render setting, sitting one level _below_ the equivalent CLI
flag: a CLI flag wins when explicitly given, the `output:` block fills in
otherwise, and only then does the renderer's own default apply. Concretely:

| `output:` field | Falls back under                | Effect when honored                                                                                                                                                                                                                                                                                                               |
| --------------- | ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dialect`       | `--dialect`                     | Sets the render dialect. Full precedence: `--dialect` > `output.dialect` > the source/input dialect (preserve-source) > `mysql`. A deliberately chosen target (CLI or `output.dialect`) that differs from the source maps types across dialects and reports lossy conversions; a preserve-source run renders the schema verbatim. |
| `mode`          | `--schema-only` / `--data-only` | `schema_only`/`data_only` restrict the render. Only the _absence_ of both flags leaves the neutral `schema_and_data`, under which `output.mode` fills in.                                                                                                                                                                         |
| `inserts`       | `--no-copy`                     | `insert` forces multi-row `INSERT` (like `--no-copy`); `auto`/`copy` keep the PostgreSQL `COPY` default.                                                                                                                                                                                                                          |
| `batch_size`    | `--batch-size`                  | Rows per `INSERT`/`COPY` batch. Fills in only when `--batch-size` is left at its `1000` default.                                                                                                                                                                                                                                  |

### `seed`

A top-level `u64`. Omitted means an unseeded run draws fresh entropy (the
effective seed is still reported so the run can be reproduced manually).
Each table can inherit, override, or opt out:

```yaml
seed: 42

tables:
  inherits_seed:
    # omitted table `seed`: inherits 42
  independent:
    seed: 9001 # its own fixed seed
  always_random:
    seed: null # random even though the model has a top-level seed
```

`--seed`/`--randomize` on the CLI affect only the top-level seed. A table
with an explicit integer seed stays deterministic regardless; a table with
`seed: null` stays random regardless.

### `defaults`

```yaml
defaults:
  inference: disabled # schema | disabled
```

| Value                                           | Meaning                                                                                                                                                                                       |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `disabled` (default when `defaults` is omitted) | Every generated column needs an explicit generator or planner owner.                                                                                                                          |
| `schema`                                        | Columns without an owner fall back to structural, type-based heuristics (for example, a bare integer primary key gets a `sequence` generator). Missing data profiles do not affect this mode. |

`schema` inference today covers structural rules only (primary keys, foreign
keys, `NOT NULL`/type-based defaults) — it does not yet apply the richer
name/constraint semantic heuristics (email/person/address detection,
credential guarding, and so on) that dump profiling + inference uses. A
column `defaults.inference: schema` cannot resolve reports
`GEN-COLUMN-OWNER-MISSING` with a note that "schema inference has no rule
for it yet."

### `imports`

Imports split a large model into files without adding a template language:

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

- Every imported file must itself be `kind: overrides` (`GEN-IMPORT-KIND`
  otherwise) and resolves relative to the importing file.
- An imported file cannot declare its own `imports:` (`GEN-IMPORT-NESTED`).
- Import paths cannot be absolute or contain `://` (`GEN-IMPORT-REMOTE`) — no
  remote or recursive imports.
- Two imports cannot define the same configuration path (`GEN-IMPORT-COLLISION`,
  naming both source files).
- Imports merge in declaration order, then the root document merges on top
  **without** collision checking — the root always wins.
- Maps merge by key; **lists always replace as a whole and never concatenate.**
- `null` clears a value only where that field permits `null`.
- The final merged document must satisfy the root's declared role.

## Tables

```yaml
tables:
  customers:
    seed: null # tri-state: omitted=inherit, null=random, integer=fixed
    rows: { kind: fixed, count: 5000 }
    schema: { name: customers, columns: [...] }
    columns: { id: { generator: { kind: sequence, start: 1 } } }
    relationships: [...]
    planners: [...]
```

| Field           | Type                                 | Default             | Meaning                                                         |
| --------------- | ------------------------------------ | ------------------- | --------------------------------------------------------------- |
| `seed`          | tri-state (omit / `null` / integer)  | inherit             | See [`seed`](#seed) above.                                      |
| `rows`          | tagged object, `kind:` discriminator | required in `model` | Row-count rule; see below.                                      |
| `schema`        | object                               | required in `model` | Normalized table schema; see below.                             |
| `columns`       | map of column name → rule            | `{}`                | Generator/modifier attachment per column.                       |
| `relationships` | list                                 | `[]`                | Named FK-shaped relationships used by generators/planners.      |
| `planners`      | list                                 | `[]`                | Multi-column/cross-table planners; see [Planners](planners.md). |

### Row-count rules (`rows`)

```yaml
rows: { kind: fixed, count: 5000 }
rows: { kind: observed, count: 10000 }
rows: { kind: scale, base: 1000, factor: 2.0, count: 2000 }
rows:
  kind: relation.children
  parent: orders
  count: 7600
  distribution: { kind: histogram, mean: 3.8, min: 1.0, max: 25.0 }
```

| `kind`              | Fields                                         | Meaning                                                                                                                                |
| ------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `fixed`             | `count: u64`                                   | An exact, hand-chosen count.                                                                                                           |
| `observed`          | `count: u64`                                   | The count observed in a profiled source dump. Unresolved without a source/base count is a compile error (`GEN-ROWS-OBSERVED-MISSING`). |
| `scale`             | `base: u64`, `factor: f64`, `count: u64`       | `count` scales `base` by `factor` (stochastically rounded when non-integral).                                                          |
| `relation.children` | `parent: String`, `count: u64`, `distribution` | Row count derives from `parent`'s final count and `distribution`'s fan-out.                                                            |

A complete model (and every `--emit-config` output) always stores the
resolved `count`, even for `scale`/`relation.children` — this is what makes
emitted models byte-reproducible without a source dump.

`distribution` (required for `relation.children` in a complete model):

```yaml
distribution: { kind: observed, mean: 4.2, min: 0.0, max: 30.0 }
```

`kind` is one of `observed | fixed | uniform | poisson | histogram`; every
kind carries the same three fields (`mean`, `min`, `max`) — `kind` selects
the shape of the per-parent draw, but the bounds are shared across all
shapes. An `overrides` document may omit `distribution` when the source/base
already supplies it; `--emit-config` always writes it explicitly.

### Schema representation (`schema`)

```yaml
schema:
  name: customers
  primary_key: [id]
  columns:
    - { name: id, type: bigint, nullable: false, primary_key: true }
    - { name: email, type: "varchar(255)", nullable: false, unique: true }
  unique_constraints:
    - { name: uq_customers_email, columns: [email] }
  check_constraints:
    - { name: chk_balance, expression: "balance >= 0" }
  indexes:
    - { name: idx_customers_email, columns: [email], unique: false }
  relationships:
    - {
        name: fk_orders_customer,
        columns: [customer_id],
        referenced_table: customers,
        referenced_columns: [id],
      }
```

| Field                | Default  | Meaning                                                                                                                                                                                                                                                            |
| -------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`               | required | Table name.                                                                                                                                                                                                                                                        |
| `columns`            | required | Ordered list of columns (order is preserved into rendered DDL).                                                                                                                                                                                                    |
| `primary_key`        | `[]`     | Column names forming the primary key.                                                                                                                                                                                                                              |
| `unique_constraints` | `[]`     | `{ name: Option<String>, columns: [String] }`.                                                                                                                                                                                                                     |
| `check_constraints`  | `[]`     | `{ name: Option<String>, expression: String }`.                                                                                                                                                                                                                    |
| `indexes`            | `[]`     | `{ name: String, columns: [String], unique: bool, index_type: Option<String> }`.                                                                                                                                                                                   |
| `create_statement`   | `None`   | Original DDL text, preserved verbatim only when the output dialect matches the source and the table set is unchanged.                                                                                                                                              |
| `relationships`      | `[]`     | **Schema-level, DDL-declared** foreign keys — these drive the rendered `FOREIGN KEY` constraint. Distinct from the table's own `relationships:` (generation-level, drives FK _value_ selection). Both exist independently; declaring one does not imply the other. |

### Column rules (`schema.columns[]`)

```yaml
- { name: id, type: bigint, nullable: false, primary_key: true }
- {
    name: email,
    source_type: "varchar(255)",
    family: text,
    nullable: false,
    unique: true,
  }
```

| Field         | Default                               | Meaning                                                                                                                                                                                                                        |
| ------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`        | required                              | Column name.                                                                                                                                                                                                                   |
| `source_type` | required (or via `type:` shorthand)   | Original SQL type text, e.g. `"varchar(255)"`.                                                                                                                                                                                 |
| `type`        | —                                     | **Shorthand alias** for `source_type`, accepted on input only. Hand-authored models may write `type: bigint`; `--emit-config` always emits the canonical `source_type` + `family` pair, never `type:`.                         |
| `family`      | derived from `source_type` if omitted | `SqlTypeFamily`: `integer \| big_integer \| decimal \| boolean \| text \| bytes \| uuid \| date_time \| json \| other`. Drives which generators/modifiers may attach (their descriptors declare an `accepts` set of families). |
| `nullable`    | required                              | Whether `NULL` is legal.                                                                                                                                                                                                       |
| `primary_key` | `false`                               | Part of the primary key.                                                                                                                                                                                                       |
| `unique`      | `false`                               | Has a `UNIQUE` constraint.                                                                                                                                                                                                     |
| `default_sql` | `None`                                | Literal `DEFAULT` SQL text, if any.                                                                                                                                                                                            |
| `generated`   | `false`                               | A computed/generated column.                                                                                                                                                                                                   |
| `identity`    | `false`                               | An `IDENTITY`/auto-increment column.                                                                                                                                                                                           |
| `collation`   | `None`                                | Column collation, if declared.                                                                                                                                                                                                 |

### Column generator/modifier rules (`columns`)

```yaml
columns:
  email:
    semantic: internet.email
    generator:
      kind: internet.email
    modifiers:
      - { kind: unique, max_attempts: 32, on_exhaustion: error }
      - { kind: null_rate, rate: 0.03 }
```

| Field       | Default | Meaning                                                                                                            |
| ----------- | ------- | ------------------------------------------------------------------------------------------------------------------ |
| `semantic`  | `None`  | Optional semantic annotation (informational; does not itself select a generator).                                  |
| `generator` | `None`  | `{ kind: String, ...args }` — see [Generators](generators.md) for every kind and its arguments.                    |
| `modifiers` | `[]`    | Ordered pipeline of `{ kind: String, ...args }` — applied in list order after the generator produces a base value. |

The compiler checks type compatibility (`GEN-GENERATOR-TYPE`/`GEN-MODIFIER-TYPE`
against the column's `family`), nullability, and ownership conflicts
(`GEN-COLUMN-OWNER-CONFLICT` when two generators/planners claim the same
column). Every generated column needs exactly one owner: a `generator`, a
planner, a `relation.foreign_key`/`relation.composite_key` marker, or (under
`defaults.inference: schema`) a structural heuristic.

#### Key uniqueness by construction

A **single-column** primary key, or a column covered by a **single-column**
`UNIQUE` constraint (a column-level `unique: true`, a one-column
`unique_constraints` entry, or a one-column unique index), is kept unique by the
compiler automatically:

- If its generator is inherently unique — `sequence` (and any `Dense`-key
  generator) or `monotonic` — nothing is added; the values are already distinct.
- If its generator is `uuid`, nothing is added; a v4 UUID collision is
  astronomically negligible.
- Otherwise (`string`, `pattern`, `choice`, semantic text, etc.) the compiler
  auto-attaches a `unique` modifier with `on_exhaustion: error` to the column's
  pipeline. The run then either emits distinct values or **fails loudly** — it
  never silently writes a duplicate key. A `unique` modifier you declare
  yourself is honored as-is and never doubled.

**Composite** (multi-column) primary/unique keys are **not** auto-enforced:
per-column deduplication cannot guarantee that the _combination_ is unique. Make
composite-key components unique by using `sequence`/`uuid` generators for them,
or run [`--verify`](README.md) to audit uniqueness on the generated output.

### Relationships (`relationships`)

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

| Field          | Default  | Meaning                                                                                                                                                                           |
| -------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`         | `None`   | Required if any generator or planner refers to this relationship by name; must be unique within the table. Anonymous relationships are allowed only when nothing references them. |
| `columns`      | required | Local column(s) forming the foreign key.                                                                                                                                          |
| `references`   | required | `{ table: String, columns: [String] }` — the parent table/columns.                                                                                                                |
| `distribution` | `None`   | A label (e.g. `observed`) resolved into a `RelationshipDistribution` (`Uniform \| Sequential \| Weighted \| Observed`) at compile time; drives how child rows pick a parent.      |

This `relationships:` list is generation-level (it drives FK _value_
selection via `relation.foreign_key`/`relation.composite_key`); it is
distinct from `schema.relationships` (DDL-level, drives the rendered
`FOREIGN KEY` constraint). Declaring a foreign key relationship for
generation does not automatically render its DDL constraint — declare both
if you need both.

**Tree hierarchies and polymorphic references are not modeled here.**
Self-referential tree shape lives entirely on the `hierarchy.tree` planner
(reading a plain self-FK relationship by name); polymorphic type/id pairs
live entirely on `relation.polymorphic_pair`'s own argument payload, with no
`relationships:` entry at all. See [Planners](planners.md).

## `kind: overrides` documents

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

| Field      | Default                            | Merge rule                                                                                    |
| ---------- | ---------------------------------- | --------------------------------------------------------------------------------------------- |
| `source`   | `None`                             | If present, replaces the base's `source` wholesale.                                           |
| `defaults` | `None`                             | If present, replaces the base's `defaults` wholesale.                                         |
| `seed`     | inherit (tri-state)                | `Inherit` = no change; `null` clears the base seed; an integer sets it.                       |
| `output`   | must be present (see caveat above) | Present fields replace the corresponding base fields; absent fields leave the base untouched. |
| `tables`   | `{}`                               | Per-table partial patches, keyed by table name.                                               |

Per-table override fields (`TableOverride`):

| Field           | Default             | Merge rule                                                                                                                                                                                                                                                          |
| --------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seed`          | inherit (tri-state) | Same tri-state semantics as the model's table `seed`.                                                                                                                                                                                                               |
| `rows`          | `None`              | If the override's `kind` matches the base's current row-count kind, only the supplied fields change; switching to a different `kind` requires every field that kind needs (`GEN-INCOMPLETE-ROWS` otherwise — no partial fallback across kinds).                     |
| `schema`        | `None`              | **Structural assertion only** — `{ name: Option<String>, create_statement: Option<String> }`. A mismatching value is `GEN-SCHEMA-MISMATCH`; a matching value is a silent no-op. Column types, nullability, keys, and existence can never be changed by an override. |
| `columns`       | `{}`                | Per-column `{ semantic, generator, modifiers }` — each present field replaces the base's corresponding field; `modifiers` replaces the whole pipeline when present (not merged element-by-element).                                                                 |
| `relationships` | `None`              | If present, **replaces the base's list wholesale.**                                                                                                                                                                                                                 |
| `planners`      | `None`              | If present, **replaces the base's list wholesale.**                                                                                                                                                                                                                 |

An override naming a missing table is `GEN-MISSING-TABLE`; a missing column
is `GEN-MISSING-COLUMN`. Both, and every other merge diagnostic, are
collected independently rather than stopping at the first one.

## Merge precedence

```text
source facts < inferred defaults < YAML overrides < broad CLI overrides < per-table CLI overrides
```

Legal overrides at any layer include counts, seeds, generators, modifiers,
planners, relationship distributions, table inclusion, and rendering
settings — these are intentional changes, not mismatches. A **compile
mismatch** means an instruction cannot apply at all: a referenced table or
column does not exist, a generator cannot produce the target type,
relationship keys are missing or incompatible, a planner's required columns
are absent, or a required source fingerprint does not match.

`ModelMerger::merge` (the library function backing this merge) always
returns its warnings alongside the merged model, even on full success — see
[Library API](library-api.md#modelmerger).

## Editor validation (JSON Schema)

Every `kind: model` and `kind: overrides` document validates against a
published JSON Schema, generated straight from the Rust types in this
reference (`schemars` derives it from `SyntheticModel`/`SyntheticOverrides`,
and the generator/modifier/planner `{ kind, ...args }` shape is composed from
the standard registry's descriptors — see
[`src/json_schema.rs`](../../src/json_schema.rs)).

Point an editor at it with a `yaml-language-server` modeline as the first
line of a config file:

```yaml
# yaml-language-server: $schema=https://sql-splitter.dev/schemas/generate-config.schema.json
version: 1
kind: model
```

editors that support the `yaml-language-server` protocol (VS Code with the
YAML extension, Neovim's `yamlls`, and others) then validate the document and
offer completion as you type. A literal `$schema:` key is also accepted as
recognized metadata — it is never required and never read by the parser, so
either form (or neither) is fine.

The schema is regenerated by `just schemas`, which also validates every
committed fixture in `tests/fixtures/generate/` against it (see
`tests/json_schema_tests.rs`). It documents the **standard** registry only:
a statically-linked custom registry's extra generators/modifiers/planners are
still validated at runtime by the compiler, just not by this shipped schema.
Because the registry's argument metadata ([`ArgumentSpec`] in
`src/generate/registry.rs`) only advertises argument _names_ and
_required_-ness, not value types, the schema enforces `kind` (rejecting an
unregistered generator/modifier/planner) and required arguments, but does not
reject an unrecognized argument name or check an argument's value type.

## Profiles (`profiles`)

```yaml
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

Keyed by `"table.column"`. Entirely removable metadata — deleting `profiles`
cannot change generation, since every rule a model needs to generate is
already resolved and stored elsewhere. Useful only as an explanation of why
inference chose what it chose (see `--explain`).

## See also

- [Generators](generators.md) — every `generator`/`modifier` kind.
- [Planners](planners.md) — every `planners[]` kind, with worked examples.
- [Diagnostics](diagnostics.md) — every `GEN-*` code referenced above.
