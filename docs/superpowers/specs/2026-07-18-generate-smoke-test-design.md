# Smoke-test `generate` fixtures

Add a repeatable developer harness that runs `sql-splitter generate` against every top-level SQL fixture in `tests/fixtures/generate/`. The harness must reveal whether generation fails, produces no rows, emits warnings, or produces a resolved YAML model that cannot reproduce the direct run.

## Interface

Expose the harness as:

```console
just generate-smoke
```

The `justfile` recipe builds the debug binary once and invokes `scripts/smoke-test-generate.sh`. The script resolves paths from its own location so it behaves consistently regardless of the caller's working directory.

## Fixture workflow

Process the sorted set of `tests/fixtures/generate/*.sql` files sequentially. For each fixture:

1. Run the built binary with a fixed seed, emit the resolved model as YAML, generate SQL, and request the JSON report.
2. Classify nonzero exit status as `FAIL` and continue to the next fixture.
3. Classify a successful run with zero reported rows or an empty SQL file as `EMPTY`.
4. Record warning diagnostic codes and classify an otherwise successful run as `WARN`.
5. Reload the emitted YAML model and generate a second SQL file with the same effective seed.
6. Classify a reload failure or empty reload output as `FAIL` or `EMPTY`.
7. Compare the direct and reloaded SQL byte-for-byte. Classify a mismatch as `MISMATCH`.
8. Classify a warning-free, nonempty, byte-identical result as `PASS`.

Use the JSON report for row counts and diagnostic severity rather than parsing human-readable messages. Python 3 may parse the report because the repository already uses Python scripts and this avoids adding a `jq` dependency.

## Output and artifacts

Print one concise result line per fixture, followed by totals for `PASS`, `WARN`, `EMPTY`, `FAIL`, and `MISMATCH`. Warning lines include unique diagnostic codes. Failure lines point to the captured stderr or report artifact instead of dumping large diagnostics into the summary.

Keep each fixture's emitted model, direct SQL, reloaded SQL, JSON reports, and stderr logs under `target/generate-smoke/<fixture-name>/`. Re-running the harness may replace files for the same fixture; it must not touch files outside `target/generate-smoke/`.

Warnings are findings, not harness failures. The script exits nonzero only when at least one fixture is `EMPTY`, `FAIL`, or `MISMATCH`. This keeps expected safety warnings visible without making the recipe unusable.

## Error handling

The script must continue after an individual fixture fails so one run provides the complete failure set. It exits immediately only for harness prerequisites such as a missing binary, no matching fixtures, an unavailable Python 3 interpreter, or an unwritable artifact directory.

Malformed or missing JSON from a command is a harness-visible failure. A failed direct run skips the YAML reload because no trustworthy resolved model exists.

## Verification

Implementation starts by confirming `just generate-smoke` is absent. After adding the recipe and script:

1. Run `bash -n scripts/smoke-test-generate.sh`.
2. Run `just --dry-run generate-smoke` to verify recipe wiring.
3. Run `just generate-smoke` and inspect every classification and retained artifact.
4. Review the implementation diff for path safety, quoting, complete failure collection, and correct exit aggregation.

Failures found by the smoke run are follow-up debugging work. Each is investigated from its captured model, reports, stderr, and SQL artifacts before changing a fixture or Rust code.
