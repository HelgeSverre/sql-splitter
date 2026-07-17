# Diagnostics

Every problem `generate` finds while loading, merging, or compiling a model
is a `Diagnostic`: a stable `GEN-*` code, a severity (`error` or `warning`),
a YAML path, a message, and sometimes related locations and a help string.
Diagnostics are collected independently rather than stopping at the first
one — a model with three unknown tables reports three diagnostics, not one.

```text
error[GEN-COLUMN-OWNER-CONFLICT] tables.orders.columns.total
  produced by both a column generator and planners[0] (commerce.order_family)
  help: remove the column generator or remove `total` from the planner mapping
```

`--json` reports the same information as structured fields
(`code`/`severity`/`path`/`message`/`help`). Codes are plain strings, not a
closed enum, so statically linked library extensions can mint their own
namespaced codes (e.g. `EXT-FOO-BAR`) without a dependency on this crate.

Almost every `GEN-*` code is `error` severity — these are compile-time
validation failures that block generation. The table below marks the
handful that are `warning` severity (they don't block a run, and are only
promoted to a failure under `--strict`).

## Warnings

| Code                        | Meaning                                                                                                                                                                                                                                                                                                                                                                                                        |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GEN-SOURCE-FINGERPRINT`    | When merging a `kind: overrides` document onto a base model, the override's `source.fingerprint` differs from the base model's recorded `source.fingerprint`, under an effective `fingerprint_policy: warn`. `require` promotes this to an error; `ignore` suppresses it. This is a YAML-vs-YAML comparison during the overrides merge — **not** a check against a freshly profiled dump (see the note below). |
| `GEN-CONFIG-COMPLETE-MODEL` | A complete `kind: model` config was supplied alongside a source dump; the model is authoritative and the profiled base is set aside.                                                                                                                                                                                                                                                                           |
| `GEN-DETACHED-DEPENDENCY`   | A nullable FK to an excluded table is detached; its constraint is omitted from rendered DDL. `--strict` promotes this to a failure.                                                                                                                                                                                                                                                                            |
| `GEN-MAX-ROWS-CAPPED`       | `--max-rows` actually reduced a table's resolved row count.                                                                                                                                                                                                                                                                                                                                                    |
| `GEN-PROFILE` / `GEN-INFER` | Fallback codes for a profiler/inference-stage warning that doesn't carry its own more specific code.                                                                                                                                                                                                                                                                                                           |
| `GEN-LOSSY-TYPE`            | A cross-dialect type conversion during rendering was lossy. `--strict` can fail the run.                                                                                                                                                                                                                                                                                                                       |
| `GEN-RENDER-WARNING`        | Any other renderer-stage warning.                                                                                                                                                                                                                                                                                                                                                                              |
| `GEN-VERIFY-NOTCHECKED`     | `--verify` passed, but one or more capabilities could not be exactly checked (see [Profiling and privacy — Verification](profiling-and-privacy.md#verification)).                                                                                                                                                                                                                                              |

`GEN-UNUSED-COLUMN` also appears in `DiagnosticBag`'s own unit tests as a
warning-severity example code, but nothing in the compiler currently emits
it — there is no "column declared but never produced" detection yet. Don't
expect to see it in real output today.

The `GEN-SOURCE-VALUES` stderr notice (see
[Profiling and privacy](profiling-and-privacy.md#the-gen-source-values-notice))
is deliberately **not** a `DiagnosticBag` entry — it is printed unconditionally
to stderr instead, precisely so `--strict` cannot turn an allowed,
flagged use into a blocking failure.

`GEN-SOURCE-FINGERPRINT` only ever fires from the overrides-merge comparison
above. Automatic dump-fingerprint drift detection during _profiling_ — computing
a fingerprint from a `generate <dump>` input and comparing it against a model's
recorded `source.fingerprint` — is **not yet implemented**: the profiling path
records no fingerprint, so a dump-input run raises no fingerprint diagnostic. It
is a planned/deferred feature.

## Config, import, and merge errors

| Code                    | Meaning                                                                                                          |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `GEN-CONFIG-IO`         | Reading the root or an imported YAML file failed.                                                                |
| `GEN-CONFIG-PARSE`      | YAML parse failure (including a duplicate mapping key).                                                          |
| `GEN-CONFIG-ROLE`       | The merged document fails its role-specific typed parse (wrong `kind`, unknown field).                           |
| `GEN-IMPORT-REMOTE`     | An import path is absolute or contains `://` — no remote imports.                                                |
| `GEN-IMPORT-KIND`       | An imported file's `kind` isn't `overrides`.                                                                     |
| `GEN-IMPORT-NESTED`     | An imported file declares its own `imports:` — imports cannot nest.                                              |
| `GEN-IMPORT-COLLISION`  | Two imports define the same configuration path; `related` names both files.                                      |
| `GEN-MISSING-TABLE`     | An `overrides` document names a table absent from the base model.                                                |
| `GEN-MISSING-COLUMN`    | An `overrides` document names a column absent from the table's schema.                                           |
| `GEN-SCHEMA-MISMATCH`   | An `overrides` schema assertion (`name`/`create_statement`) disagrees with the base.                             |
| `GEN-INCOMPLETE-ROWS`   | An `overrides` `rows:` block switches row-count `kind` but omits a field that kind needs, with no base fallback. |
| `GEN-OVERRIDES-NO-BASE` | A `kind: overrides` config was given with no source dump or base model to apply it to.                           |

## Selection, counts, and dependencies

| Code                         | Meaning                                                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `GEN-COUNT-CONTROL-CONFLICT` | Both `--scale` and `--rows` were given (also a clap-level conflict).                                                     |
| `GEN-TABLE-COUNT-CONFLICT`   | A table is matched by both `--table-rows` and `--table-scale`.                                                           |
| `GEN-ROWS-CYCLE`             | `relation.children` tables form a row-count dependency cycle.                                                            |
| `GEN-ROWS-OBSERVED-MISSING`  | `rows.kind: observed` has no source/profile to resolve its count from.                                                   |
| `GEN-CHILD-COUNT-IMPOSSIBLE` | A resolved child count is below `parent_count × distribution.min` — an impossible per-parent minimum.                    |
| `GEN-EXCLUDED-DEPENDENCY`    | A selected table's required dependency (a non-null FK, a `relation.children` parent, a polymorphic target) was excluded. |
| `GEN-INVALID-GLOB`           | A `--tables`/`--exclude` pattern fails to compile as a glob.                                                             |

## Ownership, types, and cycles

| Code                                                                     | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GEN-COLUMN-OWNER-CONFLICT`                                              | Two generators/planners claim the same column.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GEN-COLUMN-OWNER-MISSING`                                               | No rule and no structural fact supplies a column's value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `GEN-COLUMN-CYCLE`                                                       | The column/planner read→write dependency graph has a cycle no single planner owns end-to-end.                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `GEN-GENERATOR-UNKNOWN` / `GEN-MODIFIER-UNKNOWN` / `GEN-PLANNER-UNKNOWN` | `kind:` names a generator/modifier/planner not registered.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `GEN-GENERATOR-TYPE` / `GEN-MODIFIER-TYPE`                               | The rule's descriptor doesn't accept the column's SQL type family.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `GEN-RELATIONSHIP-UNKNOWN`                                               | A planner's `relationship:` argument names a relationship not declared on that table.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `GEN-FOREIGN-KEY-UNRESOLVED`                                             | A relationship-owned FK column has no resolvable value source.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GEN-KEY-DOMAIN-UNSUPPORTED`                                             | A relationship/planner targets a parent key generator that isn't a dense integer domain (only bare integer PK, `sequence`, and `uuid` are supported). **Note:** unlike the other codes here, this one is raised at _generation_ time as a runtime error string inside `GenerateError::InvalidInput` (the engine builds key domains only when it runs) — it is **not** a compile-time `Diagnostic`, so it does not appear in the `--json` `diagnostics` array. It surfaces as the command's error message and non-zero exit instead. |

## Generator and modifier argument errors

Each built-in generator/modifier validates its own arguments at compile
time and reports a `GEN-<FAMILY>-<PROBLEM>` code at that rule's YAML path.
The full set, by area (see [Generators](generators.md) for each kind's field
reference):

- **Typed random**: `GEN-INTEGER-RANGE`, `GEN-RANGED-INTEGER-RANGE`, `GEN-DECIMAL-RANGE`, `GEN-DECIMAL-SCALE`, `GEN-STRING-LENGTH-RANGE`, `GEN-BYTES-LENGTH-RANGE`, `GEN-BOOLEAN-PROBABILITY`.
- **Categorical**: `GEN-CHOICE-EMPTY`, `GEN-CHOICE-MISSING-VALUES`, `GEN-CHOICE-INVALID-VALUE`, `GEN-WEIGHTED-CHOICE-EMPTY`, `GEN-WEIGHTED-CHOICE-MISSING-CHOICES`, `GEN-WEIGHTED-CHOICE-ALL-ZERO`, `GEN-WEIGHTED-CHOICE-INVALID-ENTRY`, `GEN-WEIGHTED-CHOICE-INVALID-VALUE`, `GEN-WEIGHTED-CHOICE-INVALID-WEIGHT`.
- **Observed/statistical**: `GEN-OBSERVED-SAMPLE-EMPTY`, `GEN-OBSERVED-SAMPLE-MISSING-VALUES`, `GEN-OBSERVED-SAMPLE-ALL-ZERO`, `GEN-OBSERVED-SAMPLE-INVALID-VALUE`, `GEN-OBSERVED-SAMPLE-INVALID-WEIGHT`, `GEN-HISTOGRAM-EMPTY`, `GEN-HISTOGRAM-MISSING-BINS`, `GEN-HISTOGRAM-ALL-ZERO`, `GEN-HISTOGRAM-INVALID-BIN`, `GEN-HISTOGRAM-NON-FINITE`, `GEN-HISTOGRAM-RANGE`, `GEN-HISTOGRAM-UNSORTED`, `GEN-GAUSSIAN-MISSING-PARAMS`, `GEN-GAUSSIAN-NON-FINITE`, `GEN-GAUSSIAN-RANGE`, `GEN-MONOTONIC-STEP`.
- **Core structural**: `GEN-SEQUENCE-ZERO-STEP`, `GEN-COPY-MISSING-SOURCE`, `GEN-COPY-TYPE-MISMATCH`, `GEN-COPY-UNKNOWN-FIELD`, `GEN-TEMPLATE-MISSING-PARTS`, `GEN-TEMPLATE-INVALID-PART`, `GEN-TEMPLATE-UNKNOWN-FIELD`, `GEN-PATTERN-MISSING-MASK`, `GEN-JSON-VALUE-INVALID`, `GEN-NULL-ON-NON-NULLABLE`.
- **Modifiers**: `GEN-NULL-RATE-MISSING-RATE`, `GEN-NULL-RATE-RANGE`, `GEN-NULL-RATE-ON-NON-NULLABLE`, `GEN-CLAMP-MISSING-BOUNDS`, `GEN-CLAMP-RANGE`, `GEN-ROUND-MISSING-SCALE`, `GEN-ROUND-SCALE-RANGE`, `GEN-CASE-INVALID-MODE`, `GEN-AFFIX-MISSING-VALUE`, `GEN-TRUNCATE-MISSING-MAX-LENGTH`, `GEN-FORMAT-MISSING-TEMPLATE`, `GEN-UNIQUE-INVALID-ON-EXHAUSTION`, `GEN-UNIQUE-WIDEN-UNSUPPORTED`.
- **Semantic/domain**: `GEN-RANDOM-STRING-INVALID-ALPHABET`, `GEN-COMMERCE-MONEY-RANGE`, `GEN-COMMERCE-MONEY-SCALE`, `GEN-RELATIVE-MISSING-SOURCE`, `GEN-RELATIVE-UNKNOWN-SOURCE`, `GEN-RELATIVE-RANGE`.

## Planner argument errors

Each planner validates its own argument shape at compile time (see
[Planners](planners.md) for the field each code corresponds to):

- **`temporal.interval`**: `GEN-INTERVAL-COLUMN-MISSING`, `GEN-INTERVAL-OPEN-END`, `GEN-INTERVAL-DURATION`, `GEN-INTERVAL-START`, `GEN-INTERVAL-TIMEZONE`.
- **`workflow.progress_counters`**: `GEN-PROGRESS-COLUMN-MISSING`, `GEN-PROGRESS-TOTAL`, `GEN-PROGRESS-PARTITION`, `GEN-PROGRESS-OBSERVED`, `GEN-PROGRESS-WEIGHTS`, `GEN-PROGRESS-OVERFLOW`, `GEN-PROGRESS-STATUS-VOCABULARY`, `GEN-PROGRESS-COMPLETION`.
- **`commerce.order_family`**: `GEN-ORDER-FAMILY-CHILD-UNKNOWN`, `GEN-ORDER-FAMILY-COLUMN-MISSING`, `GEN-ORDER-FAMILY-CONFIG`, `GEN-ORDER-FAMILY-RELATIONSHIP`, `GEN-ORDER-FAMILY-SCALE`, `GEN-ORDER-FAMILY-UNKNOWN-FIELD`, `GEN-ORDER-FAMILY-ZERO-LINES`, `GEN-ORDER-FAMILY-OVERFLOW`.
- **`temporal.timestamps`/`temporal.soft_delete`/`temporal.lifecycle`**: `GEN-TIMESTAMPS-COLUMN-MISSING`, `GEN-TIMESTAMPS-RANGE`, `GEN-TIMESTAMPS-DELAY`, `GEN-SOFT-DELETE-COLUMN-MISSING`, `GEN-SOFT-DELETE-RANGE`, `GEN-SOFT-DELETE-NULLABILITY`, `GEN-LIFECYCLE-COLUMN-MISSING`, `GEN-LIFECYCLE-STATES`, `GEN-LIFECYCLE-STATUS-VOCABULARY`, `GEN-LIFECYCLE-WEIGHTS`, `GEN-LIFECYCLE-RANGE`, `GEN-LIFECYCLE-STEP`, `GEN-LIFECYCLE-NULLABILITY`.
- **`hierarchy.tree`**: `GEN-TREE-COLUMN-MISSING`, `GEN-TREE-DEPTH`, `GEN-TREE-ROOT-RATIO`, `GEN-TREE-BRANCHING`, `GEN-TREE-REQUIRED-CYCLE`.
- **`relation.junction_pair`/`relation.polymorphic_pair`/`relation.tenant_family`**: `GEN-JUNCTION-COLUMN-MISSING`, `GEN-JUNCTION-RELATIONSHIP`, `GEN-JUNCTION-KEY-UNSUPPORTED`, `GEN-JUNCTION-EXHAUSTED`, `GEN-POLYMORPHIC-COLUMN-MISSING`, `GEN-POLYMORPHIC-TARGETS`, `GEN-POLYMORPHIC-TARGET-UNKNOWN`, `GEN-POLYMORPHIC-KEY-UNSUPPORTED`, `GEN-TENANT-COLUMN-MISSING`, `GEN-TENANT-RELATIONSHIP`, `GEN-TENANT-KEY-UNSUPPORTED`, `GEN-TENANT-PARTITION`.
- **`geo.coordinate_pair`**: `GEN-COORDINATE-COLUMN-MISSING`, `GEN-COORDINATE-BOUNDS`, `GEN-COORDINATE-PRECISION`.
- **`file.metadata`**: `GEN-FILE-COLUMN-MISSING`, `GEN-FILE-SIZE-RANGE`, `GEN-FILE-HASH-KIND`, `GEN-FILE-EXTENSIONS`.

## Registry (extension registration)

| Code                              | Meaning                                                     |
| --------------------------------- | ----------------------------------------------------------- |
| `GEN-REGISTRY-DUPLICATE`          | A generator/modifier/planner kind name is registered twice. |
| `GEN-REGISTRY-ALIAS-DUPLICATE`    | An alias is registered twice.                               |
| `GEN-REGISTRY-ALIAS-SHADOWS-KIND` | An alias collides with an existing primary kind name.       |

## See also

- [Model reference](model-reference.md) — the YAML shapes these codes validate.
- [Generators](generators.md) and [Planners](planners.md) — per-kind field reference.
- [Profiling and privacy](profiling-and-privacy.md) — `GEN-SOURCE-VALUES` and verification honesty.
