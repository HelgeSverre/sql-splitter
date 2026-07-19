# Repository guidance

`sql-splitter` is a Rust CLI and library for inspecting and transforming SQL
dumps. It supports MySQL/MariaDB, PostgreSQL, SQLite, and MSSQL workflows,
including splitting, analysis, conversion, validation, sampling, sharding,
redaction, synthetic-data generation, schema graphs, ordering, diffing, merging,
and optional DuckDB queries.

This file is an agent-oriented map of the repository. Keep volatile CLI details,
benchmarks, and release steps in their authoritative locations instead of
duplicating them here.

## Sources of truth

Use these sources in descending order of authority:

1. Rust source and tests for behavior.
2. `sql-splitter <command> --help` for the current CLI.
3. `just --list` and `justfile` for development commands.
4. `website/src/content/docs/` for maintained user and contributor docs.
5. `README.md` and `skills/sql-splitter/SKILL.md` for secondary user- and
   agent-facing summaries.

When behavior changes, update every affected user-facing source in the same
change. Do not preserve stale prose merely because it appears in this file.

## Before editing

- Run `git status --short` and preserve unrelated worktree changes.
- Read the implementation and nearby tests before changing behavior.
- Search for the same option, command, or concept across `README.md`,
  `website/src/content/docs/`, and `skills/sql-splitter/SKILL.md`.
- Use `cargo run -- <command> --help` when validating CLI examples or flags.

## Development commands

Run `just` or `just --list` for the complete, current command list.

| Task                               | Command                 |
| ---------------------------------- | ----------------------- |
| Debug build                        | `just build`            |
| Release build                      | `just release`          |
| Type-check                         | `just check`            |
| Rust and Markdown formatting       | `just fmt`              |
| Clippy with warnings denied        | `just clippy`           |
| Full nextest suite                 | `just test`             |
| Criterion benchmarks               | `just bench`            |
| Real-world ignored tests           | `just verify-realworld` |
| Generate and validate JSON schemas | `just schemas`          |
| Generate man pages                 | `just man`              |
| Website checks                     | `just website-lint`     |
| Release preparation                | `just release-prepare`  |

Useful direct commands:

```bash
cargo run -- <command> --help
cargo nextest run <filter>
cargo test --doc
cargo fmt --all -- --check
cargo clippy -- -D warnings
```

`just fmt` rewrites Rust and all Markdown files. Use the non-mutating formatting
checks when you only need verification or when the worktree contains unrelated
Markdown changes.

## Verification expectations

Match verification to the change:

- Rust behavior: run focused tests first, then `just test` when practical.
- Parser or dialect behavior: run parser tests plus the relevant integration
  and regression tests.
- CLI arguments or help: run the command's help, then run
  `cargo nextest run --test cli_help_test`.
- Public library examples: run `cargo test --doc` in addition to nextest.
- Lint-sensitive Rust changes: run `cargo fmt --all -- --check` and
  `just clippy`.
- JSON output or synthetic config schema changes: run `just schemas` and check
  in all generated copies.
- Website changes: run `just website-lint`; use `just website-build` when routes,
  components, configuration, or generated assets change.
- Performance-sensitive changes: run the focused Criterion benchmark or the
  relevant script under `scripts/`; record the environment with any numbers.

CI behavior is defined in `.github/workflows/`. Do not infer current CI coverage
from old benchmark or test-count snapshots.

## Architecture map

### Entry points and command layer

- `src/main.rs` is the binary entry point.
- `src/lib.rs` exposes the library API. The binary currently declares the same
  source modules separately, so check both crate targets when changing module
  visibility or conditional compilation.
- `src/cmd/` owns Clap arguments, input validation, command orchestration, and
  process exit codes. Business logic belongs in the domain modules, not in the
  argument structs.
- `src/cmd/common.rs` and `src/cmd/glob_util.rs` contain shared command plumbing.

The command enum in `src/cmd/mod.rs` is the authoritative list of user-facing
subcommands. Some developer commands, such as schema and man-page generation,
are intentionally hidden from normal help.

### SQL input, parsing, and output

- `src/parser/` contains dialect detection and the streaming SQL parser. It
  handles statement events, streamed INSERT rows, PostgreSQL COPY data, and
  MSSQL-specific syntax.
- `src/splitter/` coordinates input decoding, parser events, table filtering,
  archive/compression behavior, and the writer pipeline.
- `src/writer/` owns parallel per-table output, buffering, I/O profiles, and the
  adaptive controller.
- `src/archive.rs` and `src/zip_input.rs` handle feature-gated archive input and
  output.
- `src/copy_data.rs` and `src/parser/{mysql_insert,postgres_copy}.rs` contain
  row-level parsing used by several commands.

Keep large-input paths streaming or bounded. Do not replace event-based or
spill-to-disk flows with whole-dump accumulation without an explicit design and
measurements.

### Schema and transformation domains

- `src/schema/` parses DDL into the shared schema model and dependency graph.
- `src/transform_common.rs` contains bounded row traversal and spill-file
  plumbing shared by FK-aware transformations.
- `src/analyzer/`, `src/merger/`, `src/convert/`, `src/validate/`,
  `src/differ/`, `src/redactor/`, `src/sample/`, `src/shard/`, and `src/graph/`
  own their respective domains.
- `src/duckdb/` implements the optional `duckdb-query` feature.
- `src/json_schema.rs` derives the checked-in JSON schemas from Rust types.

Reuse the shared parser, schema graph, row representation, and output plumbing
before adding command-specific copies.

### Synthetic-data generation

Synthetic generation is a staged library and CLI pipeline:

```text
dump and/or YAML
  -> schema parsing and bounded profiling
  -> model inference and override merge
  -> model compilation and generation plan
  -> seeded generation engine
  -> SQL renderer, verification, and atomic output
```

- `src/synthetic/` defines the portable schema, YAML model, overrides, and merge
  semantics.
- `src/profile/` collects bounded evidence from dumps and infers model choices.
- `src/generate/` contains registries, compiler, planners, generators, execution,
  output, and verification.
- `src/render/` renders generated values and DDL without coupling generators to
  a SQL dialect.
- `website/src/content/docs/commands/generate/` is the canonical user-facing
  model, generator, planner, diagnostics, privacy, and library API reference.
- `docs/generate/` contains compatibility pointers plus maintainer guidance.
- `tests/fixtures/generate/` contains committed models and schema fixtures;
  `tests/generate_*` cover each pipeline stage.

Generation must remain deterministic for a fixed model and seed. Profiling must
remain bounded independently of dump size. Preserve those properties in tests.

## Features and dialects

Cargo features are defined in `Cargo.toml`:

- `duckdb-query` gates DuckDB integration and the `query` command.
- `compression` gates compressed input and per-file compressed output.
- `archive` gates archive support and implies compression.
- `man-pages` enables the hidden man-page generator.

Default builds enable DuckDB queries, compression, and archives. When changing
feature-gated code, test both the relevant feature configuration and the default
build.

Dialect behavior is centralized around `parser::SqlDialect`. Update parsing,
rendering/conversion, CLI values, fixtures, and user documentation together when
adding or changing dialect support.

## Tests and fixtures

- Unit tests live beside the code they exercise.
- Integration and regression tests live in `tests/`.
- Small, hand-authored dialect fixtures live in `tests/fixtures/static/`.
- Synthetic-generation fixtures live in `tests/fixtures/generate/`.
- Large generated or real-world datasets should remain ignored and reproducible
  through scripts rather than committed.
- Shared integration-test helpers belong in `tests/support/`.

Prefer the narrowest test that proves a behavior, then run the broader affected
suite. Regression tests should describe the externally visible failure rather
than mirror private implementation details.

## Generated files and documentation sync

Do not hand-edit generated artifacts when a repository command owns them.

- `just schemas` regenerates `schemas/*.schema.json`, validates them against CLI
  output and generate fixtures, clears stale vendored schemas, copies them to
  `website/public/schemas/`, and verifies both directories match.
- `just man` regenerates `man/` from Clap definitions.
- `bun run build` generates `llms.txt`, `llms-full.txt`, and `llms-small.txt`
  through `starlight-llms-txt`; do not edit or commit those build outputs.

When changing a command, option, output format, dialect, compression/archive
support, or common workflow, review:

- `README.md`
- the matching page under `website/src/content/docs/commands/` or `reference/`
- `skills/sql-splitter/SKILL.md`
- generated man pages and JSON schemas, when applicable

When changing synthetic model semantics, update the relevant canonical page
under `website/src/content/docs/commands/generate/` and follow
`docs/generate/maintainers.md`.

Update `skills/sql-splitter/SKILL.md` when a change affects command selection,
common agent workflows, or important flags. Keep it focused on when and how to
use the tool; installation instructions belong in user documentation.

## Benchmarking and profiling

- Criterion benches live in `benches/`.
- Reproducible benchmark and profiling entry points live in `justfile`,
  `scripts/`, and `docker/`.
- Current benchmark documentation lives at
  `website/src/content/docs/contributing/benchmarking.mdx`.
- `scripts/profile-memory.sh --help` is authoritative for data-size presets and
  requirements.

Do not place machine-specific throughput, memory, elapsed-time, or test-count
snapshots in this file. Put dated, reproducible measurements in benchmark docs
or change notes with hardware and command details.

## Releases

Follow `website/src/content/docs/contributing/release-process.mdx` and the
release recipes in `justfile`. A release includes the version/changelog update,
verification, tag, GitHub release/artifacts, and crates.io publication workflow.
Check `.github/workflows/release.yml` and `.github/workflows/publish.yml` before
changing or describing automation.
