# Planning-Label Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove numbered release-planning labels from Rust source, tests, test fixtures, CLI output, and help text while preserving genuine runtime terminology.

**Architecture:** Five workers own disjoint file sets weighted to 31–34 matching lines each. The CLI worker performs the only behavior change with a red-green test cycle; the other workers make comment and test-diagnostic copy edits only. The controller integrates, scans, formats, lints, tests, and reviews the complete diff.

**Tech Stack:** Rust 2021, clap derive help, cargo-nextest, rustfmt, clippy, ripgrep.

## Global Constraints

- Edit only assigned files under `src/` and `tests/`.
- Remove numbered task references and numbered delivery-stage references.
- Rewrite useful comments around current behavior or invariants; do not delete useful context.
- Preserve runtime types and fields such as `ExecutionPhase` and `phases`.
- Preserve technical descriptions such as a two-phase file-open algorithm and runtime pipeline stages.
- Prioritize CLI output and help text.
- Do not change program behavior except user-visible wording.
- Do not commit from a worker; all workers share one worktree and the controller owns integration commits.

---

### Task 1: CLI, help, and core plan comments (34 matches)

**Files:**

- Modify: `src/cmd/generate.rs`
- Modify: `src/generate/plan.rs`
- Modify: `src/generate/planners/mod.rs`
- Modify: `src/profile/heuristics/mod.rs`
- Modify: `tests/cli_help_test.rs`
- Modify: `tests/generate_cli_test.rs`
- Modify: `tests/parser_memory_test.rs`

**Interfaces:**

- Consumes: clap-generated `generate --help` and the existing `check_with_an_input_dump_exits_with_usage_code` integration test.
- Produces: CLI/help wording without numbered planning labels; no API or type changes.

- [ ] **Step 1: Add failing assertions for user-visible text**

Add this test to `tests/cli_help_test.rs`:

```rust
#[test]
fn generate_help_omits_release_planning_labels() {
    let help = run_help(&["generate", "--help"]);
    let normalized = help.to_ascii_lowercase();

    for label in ["phase 1", "phase 2", "phase 3", "task 19", "tasks 19"] {
        assert!(
            !normalized.contains(label),
            "generate --help exposes release-planning label `{label}`:\n{help}"
        );
    }
}
```

Extend `check_with_an_input_dump_exits_with_usage_code` in `tests/generate_cli_test.rs` to decode stderr and assert that lowercase stderr contains neither `phase` nor `task`.

- [ ] **Step 2: Run the focused tests and verify they fail for existing copy**

Run:

```bash
cargo nextest run -E 'test(generate_help_omits_release_planning_labels) | test(check_with_an_input_dump_exits_with_usage_code)'
```

Expected: both tests fail because the current help/error text contains numbered planning labels.

- [ ] **Step 3: Rewrite CLI/help text and assigned comments**

Replace planning-history wording with current behavior. In the `--check` error, retain the actionable rule that checking requires a complete config model and cannot profile an input dump. Rewrite comments in every assigned file without renaming `ExecutionPhase`, `phases`, or other runtime concepts.

- [ ] **Step 4: Run focused tests and assigned-file scan**

Run:

```bash
cargo nextest run -E 'test(generate_help_omits_release_planning_labels) | test(check_with_an_input_dump_exits_with_usage_code)'
rg -n -i '\btasks?[[:space:]_-]*#?[0-9]+\b|\bphases?[[:space:]_-]*[0-9]+[a-z]?\b' src/cmd/generate.rs src/generate/plan.rs src/generate/planners/mod.rs src/profile/heuristics/mod.rs tests/cli_help_test.rs tests/generate_cli_test.rs tests/parser_memory_test.rs
```

Expected: tests pass; scan prints no release-planning references.

### Task 2: Engine, sample/shard, and regression comments (32 matches)

**Files:**

- Modify: `tests/generate_engine_test.rs`
- Modify: `src/shard/mod.rs`
- Modify: `src/sample/mod.rs`
- Modify: `tests/redact_test.rs`
- Modify: `src/transform_common.rs`
- Modify: `tests/generate_api_test.rs`
- Modify: `src/render/ddl.rs`
- Modify: `tests/convert_unit_test.rs`
- Modify: `tests/schema_unit_test.rs`

**Interfaces:**

- Consumes: existing comments, test headings, and test failure messages.
- Produces: durable descriptions with unchanged test behavior and unchanged runtime-stage terminology.

- [ ] **Step 1: Classify assigned references**

Run:

```bash
rg -n -i '\btasks?[[:space:]_-]*#?[0-9]+\b|\bphases?[[:space:]_-]*[0-9]+[a-z]?\b' tests/generate_engine_test.rs src/shard/mod.rs src/sample/mod.rs tests/redact_test.rs src/transform_common.rs tests/generate_api_test.rs src/render/ddl.rs tests/convert_unit_test.rs tests/schema_unit_test.rs
```

Keep `src/transform_common.rs` descriptions of real split/build runtime stages. Rewrite delivery history, test headings, and diagnostic strings around the behavior they protect.

- [ ] **Step 2: Edit only assigned comments and diagnostics**

Use domain labels such as “core generators and modifiers,” “semantic generators,” “relational execution,” “split input into per-table files,” and “golden redactor baseline.” Do not alter assertions except to replace planning-history prose in their failure messages.

- [ ] **Step 3: Verify assigned files**

Run:

```bash
cargo nextest run -E 'binary(generate_engine_test) | binary(redact_test) | binary(generate_api_test) | binary(convert_unit_test) | binary(schema_unit_test)'
rg -n -i '\btasks?[[:space:]_-]*#?[0-9]+\b|\bphases?[[:space:]_-]*[0-9]+[a-z]?\b' tests/generate_engine_test.rs src/shard/mod.rs src/sample/mod.rs tests/redact_test.rs tests/generate_api_test.rs src/render/ddl.rs tests/convert_unit_test.rs tests/schema_unit_test.rs
```

Expected: selected tests pass; scan prints no numbered planning labels outside preserved runtime-stage comments in `src/transform_common.rs`.

### Task 3: Registry, generator modules, and fixture comments (33 matches)

**Files:**

- Modify: `src/generate/registry.rs`
- Modify: `src/generate/generators/mod.rs`
- Modify: `src/generate/planners/structural.rs`
- Modify: `tests/fixtures/generate/legacy_fixture.yaml`
- Modify: `src/generate/generators/semantic.rs`
- Modify: `tests/fixtures/generate/production_shape.sql`
- Modify: `tests/generate_filter_test.rs`
- Modify: `src/render/sql.rs`
- Modify: `tests/fixtures/generate/realworld_shapes.sql`
- Modify: `tests/shard_integration_test.rs`

**Interfaces:**

- Consumes: registry/generator documentation and fixture provenance comments.
- Produces: present-tense catalog and invariant descriptions with no code or fixture data changes.

- [ ] **Step 1: Inventory assigned references**

Run the numbered-label regex from the global scan against the ten assigned files and read surrounding context for every hit.

- [ ] **Step 2: Rewrite history as current contracts**

Describe what each registry hook, catalog, planner, renderer contract, or fixture covers now. Remove report-file references from fixtures. Do not change YAML values, SQL statements, Rust expressions, or public APIs.

- [ ] **Step 3: Verify assigned files**

Run:

```bash
cargo nextest run -E 'binary(generate_filter_test) | binary(shard_integration_test)'
rg -n -i '\btasks?[[:space:]_-]*#?[0-9]+\b|\bphases?[[:space:]_-]*[0-9]+[a-z]?\b' src/generate/registry.rs src/generate/generators/mod.rs src/generate/planners/structural.rs tests/fixtures/generate/legacy_fixture.yaml src/generate/generators/semantic.rs tests/fixtures/generate/production_shape.sql tests/generate_filter_test.rs src/render/sql.rs tests/fixtures/generate/realworld_shapes.sql tests/shard_integration_test.rs
```

Expected: selected tests pass and the scan prints no matches.

### Task 4: Planner, profiler, MSSQL, and support comments (33 matches)

**Files:**

- Modify: `tests/generate_planner_test.rs`
- Modify: `tests/mssql_integration_test.rs`
- Modify: `src/profile/heuristics/planner.rs`
- Modify: `tests/generate_profile_test.rs`
- Modify: `src/profile/evidence.rs`
- Modify: `tests/fixtures/generate/stress/multitenant_workflow.yaml`
- Modify: `tests/generate_verify_test.rs`
- Modify: `src/splitter/epoch.rs`
- Modify: `tests/fixtures/generate/stress/car_dealership.yaml`
- Modify: `tests/support/generated_fixture.rs`

**Interfaces:**

- Consumes: planner/profiler comments, test section headings, warning text, and fixture notes.
- Produces: domain-oriented wording with unchanged planner warnings and unchanged test data.

- [ ] **Step 1: Inventory and classify assigned references**

Run the numbered-label regex against the ten assigned files. Treat planner names and capabilities as durable domain terms; treat roadmap numbering and report links as removable history.

- [ ] **Step 2: Rewrite assigned text**

Use planner names in test headings, describe profiler evidence and nomination behavior directly, and remove milestone qualifiers from fixtures. Preserve `src/splitter/epoch.rs` wording if it describes a real runtime seam rather than a delivery stage.

- [ ] **Step 3: Verify assigned files**

Run:

```bash
cargo nextest run -E 'binary(generate_planner_test) | binary(mssql_integration_test) | binary(generate_profile_test) | binary(generate_verify_test)'
rg -n -i '\btasks?[[:space:]_-]*#?[0-9]+\b|\bphases?[[:space:]_-]*[0-9]+[a-z]?\b' tests/generate_planner_test.rs tests/mssql_integration_test.rs src/profile/heuristics/planner.rs tests/generate_profile_test.rs src/profile/evidence.rs tests/fixtures/generate/stress/multitenant_workflow.yaml tests/generate_verify_test.rs tests/fixtures/generate/stress/car_dealership.yaml tests/support/generated_fixture.rs
```

Expected: selected tests pass and the scan prints no matches outside any preserved runtime seam in `src/splitter/epoch.rs`.

### Task 5: Compiler, core generators, observed data, and stress fixtures (31 matches)

**Files:**

- Modify: `src/generate/compiler.rs`
- Modify: `src/generate/generators/core.rs`
- Modify: `tests/generate_compiler_test.rs`
- Modify: `tests/fixtures/generate/stress/everything.yaml`
- Modify: `src/generate/generators/observed.rs`
- Modify: `src/profile/profiler.rs`
- Modify: `tests/fixtures/generate/stress/odoo_erp.yaml`
- Modify: `src/generate/planners/progress.rs`
- Modify: `tests/convert_integration_test.rs`
- Modify: `tests/generate_config_test.rs`

**Interfaces:**

- Consumes: compiler/generator documentation, test headings, fixture notes, and test comments.
- Produces: current-contract wording with unchanged compilation, generation, profiling, and fixture behavior.

- [ ] **Step 1: Inventory assigned references**

Run the numbered-label regex against the ten assigned files and read enough context to retain each useful invariant.

- [ ] **Step 2: Rewrite assigned text**

Describe ownership checks, generator catalogs, observed generators, profiler behavior, and stress-fixture coverage directly. Replace report references with the constraint or reason the comment needs to communicate. Do not change code expressions, test inputs, YAML values, or SQL.

- [ ] **Step 3: Verify assigned files**

Run:

```bash
cargo nextest run -E 'binary(generate_compiler_test) | binary(convert_integration_test) | binary(generate_config_test)'
rg -n -i '\btasks?[[:space:]_-]*#?[0-9]+\b|\bphases?[[:space:]_-]*[0-9]+[a-z]?\b' src/generate/compiler.rs src/generate/generators/core.rs tests/generate_compiler_test.rs tests/fixtures/generate/stress/everything.yaml src/generate/generators/observed.rs src/profile/profiler.rs tests/fixtures/generate/stress/odoo_erp.yaml src/generate/planners/progress.rs tests/convert_integration_test.rs tests/generate_config_test.rs
```

Expected: selected tests pass and the scan prints no matches.

### Integration and review

- [ ] Review `git diff` for file ownership violations and non-copy behavior changes.
- [ ] Run the full numbered-label scan over `src/` and `tests/`; classify remaining `phase` uses as runtime terminology or fix them.
- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run `cargo nextest run`.
- [ ] Perform a diff-based code review and commit the integrated cleanup.
