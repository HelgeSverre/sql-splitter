# Generate Re-review Fixes Implementation Plan

> **For Codex:** Execute this plan in the current worktree, preserving unrelated website changes and recording RED/GREEN evidence in `.superpowers/sdd/final-review-fix-report.md`.

**Goal:** Fix the three confirmed re-review regressions in resolved-model family facts, output-path collision detection, and bounded verification scalability.

**Architecture:** Keep family child row rules as `relation.children` in emitted models so their distribution remains available to cross-table planners, while updating their stored resolved count. Normalize file destinations to filesystem identities before staging. Replace verifier spill-time full-file lookups with deferred exact checks over bounded external merge-sort runs and linear merge joins.

**Tech Stack:** Rust standard library I/O and paths, existing `ProtectedSpool` row encoding, `tempfile` tests, Cargo nextest/clippy/rustfmt.

---

### Task 1: Preserve order-family facts through resolved model emission

**Files:**
- Modify: `tests/generate_api_test.rs`
- Modify: `src/synthetic/model.rs`
- Modify: `src/generate/mod.rs`
- Modify: `website/src/content/docs/commands/generate/model-reference.mdx`

1. Add a seeded `simple.yaml` emit/reload regression that asserts 65 rows and byte-identical SQL.
2. Run the focused test and capture the current `65 -> 35` RED result.
3. Derive the selected family-child table names from the compiled plan.
4. Update `freeze_row_counts` so family children retain `RowsModel::RelationChildren` and its distribution while receiving the resolved `count`; keep converting other derived rules to `fixed` so existing override reload behavior remains authoritative.
5. Document the emitted-family exception and run the focused API tests GREEN.

### Task 2: Compare output destinations by normalized filesystem identity

**Files:**
- Modify: `tests/generate_api_test.rs`
- Modify: `src/generate/mod.rs`

1. Add API regressions for relative/absolute aliases and `..` aliases of the same destination, including preservation of pre-existing bytes.
2. Run the focused tests and capture RED (generation succeeds and the second staged artifact wins).
3. Add a destination-normalization helper that resolves relative paths against the current directory, canonicalizes the deepest existing ancestor, and lexically appends any missing suffix.
4. Use normalized identities in the preflight collision guard and map normalization failures to `GEN-REQUEST-OUTPUT` without opening or staging either destination.
5. Run the alias regressions and the existing distinct-destination behavior GREEN.

### Task 3: Replace quadratic verifier spool scans

**Files:**
- Modify: `src/generate/verify.rs`
- Modify: `tests/generate_verify_test.rs` if an end-to-end regression is needed beyond module tests

1. Add focused spill operation-count tests for uniqueness/FK sorting and family parent-child aggregation; tests must assert an algorithmic I/O bound, not elapsed time.
2. Run them RED against the current scan-per-insert and scan-children-per-parent design.
3. Change spilled `KeySet` insertion to append-only and defer duplicate detection.
4. Accumulate child FK keys in bounded/spilling stores during the row pass instead of querying parent spools row by row.
5. Implement bounded external sorting: memory-budgeted exact-key chunks, sorted protected runs, pairwise on-disk merges with a bounded run stack, and operation counters used by tests.
6. Finalize uniqueness through adjacent sorted-key detection and FK membership through a linear sorted merge join.
7. Sort family parent/child value records by exact join key and compare grouped child sums to parents in one linear merge pass.
8. Run focused unit and integration spill tests GREEN, including duplicate, missing-FK, family mismatch, and memory-retention behavior.

### Task 4: Review, verify, and report

**Files:**
- Modify: `.superpowers/sdd/final-review-fix-report.md` (ignored report; do not commit unless repository policy changes)

1. Run `cargo fmt --all -- --check`, focused nextest suites, `just test`, `cargo test --doc`, `just clippy`, and relevant non-default feature checks.
2. Review the diff for correctness, boundedness, error mapping, path alias edge cases, docs consistency, and accidental changes to the five protected website files.
3. Commit intentional code/tests/docs in coherent commits, leaving protected files unstaged.
4. Append a `Re-review fix wave` section with classification, root cause, RED/GREEN commands and results, commits, full verification, self-review, and concerns.
