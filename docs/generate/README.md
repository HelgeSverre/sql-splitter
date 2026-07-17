# `sql-splitter generate`

`generate` produces synthetic SQL data — either from a hand-authored YAML
model, or by profiling a real SQL dump and inferring one. It shares its
compiler and generation engine between the CLI and the public Rust library.

**The output is synthetic, not anonymized.** See
[Profiling and privacy](profiling-and-privacy.md) before using `generate` as
a substitute for redaction.

## The four primary workflows

**1. Dump straight to SQL** — profile a dump, infer a model, generate:

```bash
sql-splitter generate production.sql -o synthetic.sql
```

**2. Emit an editable model** — profile a dump, write the resolved model
instead of SQL, then generate from it later:

```bash
sql-splitter generate production.sql --emit-config synthetic.yaml
sql-splitter generate --config synthetic.yaml -o synthetic.sql
```

**3. Generate from a model only** — no source dump at all:

```bash
sql-splitter generate --config synthetic.yaml -o synthetic.sql
```

**4. Apply overrides, freeze the result, and scale it** — profile a dump,
patch it with a `kind: overrides` document, save the fully resolved model,
and scale row counts in the same run:

```bash
sql-splitter generate production.sql \
  --config overrides.yaml \
  --emit-config resolved.yaml \
  --scale 0.1 \
  -o synthetic.sql
```

## Documentation map

| Page                                              | Covers                                                                                                        |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| [Model reference](model-reference.md)             | Every YAML field: top-level, table, schema, row, relationship, column, generator/modifier/planner attachment. |
| [Generators](generators.md)                       | The generator and modifier catalog: kinds, fields, defaults, accepted types.                                  |
| [Planners](planners.md)                           | Multi-column and cross-table planners, with three fully worked examples.                                      |
| [Profiling and privacy](profiling-and-privacy.md) | What profiling observes, how inference works, and the honest privacy story.                                   |
| [Library API](library-api.md)                     | The Rust convenience builder and the staged registry → compiler → engine → renderer API.                      |
| [Diagnostics](diagnostics.md)                     | Stable `GEN-*` diagnostic codes.                                                                              |

## How it fits together

```text
SQL dump ──> schema extraction ──┐
                                  ├─> model ─> compiler + registry ─> plan
SQL rows ──> bounded profiler ───┘                                    │
YAML model/overrides ──────────────────────────────────────────────────┘
                                                                        │
                                                                        v
                                                          streaming generation
                                                                        │
                                                                        v
                                                            dialect renderer
```

A model is either self-contained (`kind: model`) or a partial patch
(`kind: overrides`) that requires a source dump or base model to apply to.
`--emit-config` always writes a complete, self-contained `kind: model`
document with every count, generator, and modifier resolved and explicit —
future heuristic or default changes cannot alter a saved model. See
[Model reference](model-reference.md#document-roles) for the full merge and
precedence rules.

## Exit codes

| Code | Meaning                                                                              |
| ---- | ------------------------------------------------------------------------------------ |
| `0`  | Successful generation, `--check`, `--dry-run`, or `--verify`.                        |
| `1`  | Invalid model, a `--strict` warning, a runtime/I/O failure, or a `--verify` failure. |
| `2`  | Invalid CLI arguments or conflicting flags.                                          |

## See also

- [`generate` command reference](/commands/generate) on the website — full flag table and stdout ownership rules.
- [Synthetic data generation design](../superpowers/specs/2026-07-16-synthetic-data-generation-design.md) — the original design spec (the pages here describe the shipped behavior; where they differ, this documentation wins).
