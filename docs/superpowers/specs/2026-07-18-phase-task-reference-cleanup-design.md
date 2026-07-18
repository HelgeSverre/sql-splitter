# Remove planning labels from code and tests

## Goal

Remove release-planning labels from Rust source and tests, with user-visible CLI output and help text as the highest priority. Replace useful comments with durable descriptions of behavior instead of deleting context.

## Scope

The cleanup covers `src/` and `tests/`, including test fixtures. It does not change documentation, the website, scripts, benchmarks, or generated artifacts.

Remove:

- numbered task references such as `Task 12`;
- numbered delivery-stage references such as `Phase 1` when they describe an implementation roadmap;
- CLI output or help text that exposes those planning labels.

Preserve:

- runtime types and fields such as `ExecutionPhase` and `phases`;
- technical descriptions such as a two-phase file-open algorithm;
- comments where “phase” describes an actual runtime stage rather than a delivery milestone.

## Editing approach

Inventory matching lines, classify each reference by meaning, and divide the affected files into five non-overlapping groups with roughly equal numbers of matches. Each delegated task edits only its assigned files. Useful comments are rewritten around the invariant or behavior they explain; headings retain domain-oriented names without roadmap numbering.

## Verification

Add or adjust CLI tests first where user-visible text changes, confirm the regression test fails for the existing planning label, then make the smallest production edit that passes. After integrating all groups:

1. scan `src/` and `tests/` for remaining numbered planning labels;
2. inspect every remaining use of “phase” and keep only runtime terminology;
3. run `cargo fmt --check`;
4. run `cargo clippy --all-targets -- -D warnings`;
5. run the full nextest suite;
6. review the complete diff for accidental behavioral changes.
