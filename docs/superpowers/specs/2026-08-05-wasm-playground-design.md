# Browser WASM playground: generate synthetic data from your SQL dump

**Status:** Implemented (feat/wasm-playground)
**Date:** 2026-08-05

## Goal

Showcase `generate` — absent from the homepage command grid until now — with a
`/playground` page where a visitor drops a real `.sql` dump, sql-splitter's
actual parser/profiler/inference/generators run in the browser via WebAssembly,
an analyze-style summary appears, and one click produces synthetic INSERTs
modeled on their dump. Privacy is the hook: the dump never leaves the machine.

## Decisions

- **No feature duplication.** The wasm crate wraps the existing pipeline:
  `DumpProfiler::profile_reader` → `ModelInference::infer` →
  `ModelCompiler::compile` → `GenerationEngine` → `SqlRenderer`. The browser
  runs the same code the CLI does, at `ProfileDepth::Full`, so
  observed-statistical generators fire on real data.
- **`crates/sql-splitter-wasm`** (cdylib+rlib): `PlaygroundSession` with
  `new(bytes, dialect?)`, `summary() -> JSON`, `generate(rows, seed) -> String`.
  Core logic uses `String` errors (natively testable); `JsError` only at the
  bindgen boundary. The summary never includes `sample_values`/`top_k` — they
  contain real data.
- **Main crate untouched** except: `dirs`/`rustyline` made optional under
  `duckdb-query` (they were manifest-level wasm blockers used only by
  duckdb-gated code), and one `cfg_attr(not(unix), allow(dead_code))`.
- **Artifact is committed** at `website/public/wasm/` (2.8 MB raw) because
  Vercel's builder has no Rust toolchain. `just wasm` rebuilds it (wasm-pack,
  `--cfg getrandom_backend="wasm_js"`, opt-level=z, bundled wasm-opt disabled —
  binaryen 117 rejects modern rustc output; a system wasm-opt is used when
  installed). `website-deploy` depends on `wasm`; `website-dev`/`website-build`
  deliberately don't.
- **UI:** Alpine.js (this page only — the rest of the site stays vanilla),
  full-width IDE-style workbench: table sidebar, Analysis / Generated SQL tabs,
  bottom action bar (rows-per-root-table, seed, Generate, Copy, Download).
  A hand-written module worker (`public/wasm/worker.js`) keeps profiling off
  the main thread; a wasm trap (`fatal`) makes the page respawn the worker.
- **No arbitrary limits** (user decision): no file-size or row-count caps.
  Perf warnings instead: ≥100 MB files warn about time/memory (~2× file size
  peak), >100k rows warns about output size. Compressed uploads are rejected
  by magic-byte sniff with a pointer to the CLI.

## Verification

- `cargo test -p sql-splitter-wasm` — 5 native tests: summary correctness,
  postgres COPY autodetect, schema-only dumps, determinism, garbage input.
- Playwright (`website/tests/playground.spec.ts`, first tests in that suite):
  example dump → real analysis in sidebar → Generate → `INSERT INTO` output;
  gzip rejection. `bunfig.toml` scopes bare `bun test` to `src/` so the two
  runners don't collide.
- `src/buildOutput.test.ts`: artifacts present in dist, homepage advertises
  13 commands + generate card + playground link.

## Deferred

Streaming/chunked reads, threaded wasm (COOP/COEP), output-dialect switcher,
"download model YAML + CLI command" funnel, gzip-in-browser. Known cosmetic
issue for a follow-up: the profiler's captured `create_statement` includes
comments that precede the CREATE TABLE in the source dump, so those comments
reappear in generated output (pre-existing `generate` behavior, not
playground-specific).
