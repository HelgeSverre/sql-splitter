# Maintaining generate documentation

The website pages under `website/src/content/docs/commands/generate/` are the
canonical user documentation. Files beside this one are compatibility pointers,
not a second copy of the reference.

## Behavior first

When a documented contract and the implementation disagree, determine which is
intentional before changing either side. Protect the chosen behavior with a
focused test, then update the implementation and website together. Complete
YAML examples marked with `{/* validate-generate-model */}` are parsed and
compiled by `tests/generate_docs_test.rs`.

## Operator catalogs

Every built-in factory publishes a descriptor with its canonical kind, aliases,
argument names/requiredness, applicable type families, read/write scope,
determinism, buffering, and verification support. Keep descriptor argument
metadata complete: it drives the generated JSON Schema and catalog coverage
tests.

`tests/generate_docs_test.rs` fails when a standard generator, modifier,
planner, or alias is absent from the website documentation. A new operator
therefore needs code, tests, descriptor metadata, schema regeneration, and a
user-facing reference entry in the same change.

## Diagnostics

Built-in codes are defined in `src/diagnostic/codes.rs` as
`DiagnosticDefinition` values. Each definition owns the stable code, title,
category, typical severity, and central summary. Runtime occurrences use a
`Diagnostic` to add path-specific context, help, and related locations.

Do not embed raw `"GEN-*"` strings in production Rust. Use the canonical
definition. Tests enforce registration, uniqueness, canonical documentation
URLs, and one exact `{#GEN-CODE}` website heading per built-in definition.

The website uses `remark-explicit-heading-ids.mjs` so uppercase diagnostic IDs
survive Markdown slugging unchanged. Keep diagnostic headings in this form:

```markdown
## GEN-EXAMPLE — Short title \{#GEN-EXAMPLE\}
```

## Generated schemas

Run `just schemas` after model types, output shapes, or operator descriptor
arguments change. It regenerates all JSON schemas, validates generate fixtures,
and synchronizes `schemas/` with `website/public/schemas/`.

## Verification

Before considering a documentation change complete, run:

```bash
cargo nextest run --test generate_docs_test
just generate-smoke
just website-lint
just website-build
```

The full project test and clippy commands remain required before merge.
