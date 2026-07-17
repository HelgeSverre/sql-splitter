# Profiling and privacy

## Output is synthetic, not anonymized

`generate` is a synthetic data generator, not a redaction tool. Its output
can legitimately reproduce values it observed in a source dump — a
`weighted_choice` generator's choices, an `observed_sample` generator's
literal values, a schema `DEFAULT`, or a hand-authored `constant` are all
allowed to hold real source-derived text. If you need irreversibly
anonymized output for sharing outside your organization, use
[`redact`](/commands/redact) instead, or build a model that avoids every
source-literal generator (see [below](#building-a-literal-free-model)).

## The `GEN-SOURCE-VALUES` notice

Whenever the resolved model would replay any source-derived literal,
`generate` prints exactly one warning to stderr, before generating or
emitting a config:

```text
warning[GEN-SOURCE-VALUES] 2 rule(s) replay literal values derived from the source dump; review before sharing the output. Locations: tables.orders.columns.status (weighted_choice), tables.customers.columns.plan (observed_sample)
```

Properties of this notice:

- It **never prints the actual values** — only the YAML path and rule kind.
- It is printed **even under `--quiet`** — it is the one thing `--quiet`
  never suppresses.
- It is **not** collected into the diagnostics bag, so `--strict` cannot
  turn it into a blocking failure — it is deliberately a heads-up about an
  allowed use, not an error condition.
- The wording differs depending on whether a source dump was actually
  profiled in this run: with a dump, it says the output "replays literal
  values derived from the source dump" (a real re-identification concern to
  review); with a `--config`-only run, it says the output "replays
  hand-authored literal values... output is synthetic, not anonymized
  source data" (nothing was read from a dump, so no dump-derived claim is
  made).

It fires on any column whose rule is `constant`, `weighted_choice`, or
`observed_sample`; a `database_default` deferring to a real source-observed
`DEFAULT`; or a verbatim `CHECK` constraint literal.

## Explicit observed credentials remain allowed

Inference never proposes `constant`/`weighted_choice`/`observed_sample` for
a credential-shaped column (see [Credentials](#credentials) below) — but if
you _explicitly_ override a credential column to one of those kinds in a
`kind: overrides` document, the compiler permits it. Nothing in the merge or
compile pipeline special-cases credential columns to reject this — it
simply surfaces through `GEN-SOURCE-VALUES` like any other literal-persisting
rule, even under `--strict`. This is deliberate: the CLI never silently
overrides an explicit, deliberate choice, but it always makes sure you saw
the warning.

## Credentials

Password/token/API-key/secret-shaped columns get special handling during
inference (`defaults.inference: schema` mode does not yet apply this — see
[Model reference](model-reference.md#defaults)):

- A high-confidence credential guard recognizes name patterns like
  `password`, `token`, `api_key`, `secret`, `private_key`, and `salt` across
  every relevant SQL type family (text, bytes, JSON, UUID) and forces a
  synthetic-only `credential.*` generator — never `observed_sample`,
  `weighted_choice`, or a source `DEFAULT`.
- Token-shaped credentials may preserve the _shape_ of the observed value
  (length, character class, prefix) without retaining any of its content.
- Private-key-like fields render an obviously invalid placeholder
  (`credential.placeholder`'s fixed string,
  `SYNTHETIC_PLACEHOLDER_NOT_A_REAL_CREDENTIAL`), never something that could
  be mistaken for a real key.
- See [Generators — credential generators](generators.md#credential-generators)
  for the exact output shapes (e.g. `credential.password_hash`'s
  `$synthetic$<64 hex chars>` format).

## Building a literal-free model

To guarantee a model reproduces nothing from its source dump:

1. Never use `constant`, `weighted_choice`, or `observed_sample` generators
   (explicitly, or by letting inference pick one from observed-distribution
   evidence).
2. Never let a nullable/defaulted column defer to a source-observed
   `DEFAULT` via `database_default`.
3. Prefer semantic generators (`internet.email`, `person.full_name`,
   `commerce.money`, and so on) and structural generators (`sequence`,
   `uuid`) over anything that replays observed values.
4. Run the model and check `report.source_values` is empty (or watch for
   the absence of the `GEN-SOURCE-VALUES` stderr line) — this is the
   authoritative way to confirm a given model produces zero source-derived
   uses. With `--json`, check the `source_values` array is empty.

## Profiling depths

`--profile-depth basic|full` (default `basic`) bounds how much of a source
dump profiling examines per table before compiling a model:

| Capability                                                    | `basic`                     | `full`               |
| ------------------------------------------------------------- | --------------------------- | -------------------- |
| Sampled rows per table                                        | 10,000                      | 100,000              |
| Retained top categorical values                               | 256                         | 256                  |
| Exact distinct-value tracking before switching to sketch-only | 100,000                     | 100,000              |
| Numeric/temporal buckets                                      | 64                          | 64                   |
| Candidate column pairs for correlation                        | schema/name candidates only | 32 data-ranked pairs |
| Row counts / null counts                                      | complete scan               | complete scan        |
| Declared schema / FKs                                         | complete scan               | complete scan        |

Row counts, null counts, and the declared schema are always exact — they
come from a complete scan regardless of depth. Everything else (categorical
values, distributions, correlations) uses bounded sketches with a fixed
memory ceiling, independent of dump size. `--profile-sample <N>` overrides
only the per-table sample-row budget; hard safety caps still apply.

Profiling reads the dump in a **single pass** — schema and per-row evidence
are captured together, so `generate production.sql --emit-config
model.yaml` never re-reads the file to gather what the compiler needs.

## Performance and memory characteristics

Generation is **streaming**: it holds a batch/family working set, not the whole
output, so peak memory is bounded by configuration, not by row count or output
size. On an Apple M2 Max release build, peak RSS stays in the **10–20 MB** band
across 100 K to **1 000 000** rows and 10 to 100 tables, and a realistic
core-generator model sustains roughly **1 M rows/s ≈ 94 MB/s** at steady state.
Profiling a source dump is bounded by the evidence budget (single-digit to
low-tens of MB regardless of dump size); a 145 MB dump profiles-and-generates in
about 3 s at ~40 MB RSS.

Measurable overheads inside the configurable path are small: seeded vs unseeded
is < 1% (the seed draw is negligible), and forcing the family-spill spool path
(`family_budget_bytes`) costs ~2% while producing byte-for-byte identical
output. Reproduce all of this with:

```bash
cargo bench --bench generate_bench      # CPU-only medians (renderer + generate)
./scripts/benchmark-generate.sh --big   # wall time / throughput / peak RSS matrix
just profile                            # per-command peak-RSS profile (incl. generate)
```

A captured baseline snapshot lives in
[`benchmark-results/generate-baseline.md`](../../benchmark-results/generate-baseline.md)
— treat it as a regression anchor, re-measured on the same host, not a contract.

## Real-world survey methodology (redaction)

The generator is hardened against real dumps by a **survey**: it is run
end-to-end (profile → infer → compile → generate → verify → validate) against
authorized local dumps and stress fixtures, and every parser/schema/inference
failure is turned into a **minimal synthetic regression fixture** — one that
reproduces the structural _shape_ that failed using invented table/column names
and values, never anything copied from a source dump. See
`tests/fixtures/generate/realworld_shapes.sql` for the pattern.

The redaction rule is non-negotiable: benchmark results and survey notes record
**only aggregate, redacted facts** — dialect, size bucket, schema-feature and
inferred rule/planner counts, diagnostics, runtime, and memory. Source literals,
real values, and source table/column names are never committed. When a survey
finding needs a regression test, reproduce the failing shape synthetically; do
not check in a dump fragment.

## Temp-file cleanup after hard termination

`generate` uses protected temporary files in two places: `--verify` stages
its output before an atomic publish, and cross-table planners (like
`commerce.order_family`) spool family state to disk once it exceeds an
in-memory budget. A separate, simpler mechanism spools SQL/model output when
it's headed to stdout (`-o -` / `--emit-config -`).

Both mechanisms:

- create files with unpredictable names and exclusive creation (no
  overwrite of an existing file, no predictable-name race);
- set owner-only (`0600`) permissions on Unix, regardless of a permissive
  umask, for the hardened `--verify`/family-spool path;
- are removed via Rust's `Drop` on a normal return, a propagated error, and
  a handled interrupt (`Ctrl-C`/`SIGINT` on Unix).

**Cleanup depends on `Drop` running.** A `SIGKILL`, a power loss, or a
`panic = "abort"` build skips `Drop` entirely — no process can trap
`SIGKILL`, and this codebase's interrupt handling installs a `SIGINT`
handler only (not `SIGTERM`). In those cases, a protected temp file can be
left behind on disk. This is a known, accepted limitation, not a bug: the
mitigation is restrictive permissions and unpredictable names, not
guaranteed cleanup. If you hard-kill a `generate` process, check your OS
temp directory for leftover files and remove them manually.

The plain stdout/`--emit-config -` spool (a `tempfile::NamedTempFile`, not
the hardened protected-spool type) has the same "may survive a hard kill"
caveat, with weaker permission hardening than the `--verify`/family-spool
path — it relies on the `tempfile` crate's own defaults rather than an
explicit forced `0600`.

Final output files are never subject to this caveat: they follow ordinary
output-file permission behavior, are written beside the destination, and are
`rename`d into place only after generation (and, under `--verify`,
verification) succeeds. An existing destination file is never truncated
before that point.

## Verification (`--verify`)

`--verify` generates to protected temporary storage, audits it, and
publishes atomically only on a full pass; a failed audit leaves any prior
destination untouched and exits `1`.

Exact checks include row counts and arity, non-null/primary-key/unique/FK
constraints, and most planner invariants. A few capabilities are **not**
exactly checkable and are honestly reported as such rather than silently
treated as passing (surfaced via `GEN-VERIFY-NOTCHECKED`):

- PostgreSQL `COPY`-format rows are audited less precisely than `INSERT`
  rows.
- `commerce.order_family` has no verification predicates for
  `subtotal`/`total`/`shipping` (only `discount`/`tax` sum checks) — the
  planner's arithmetic is exact by construction and covered by unit tests,
  but `--verify` cannot independently confirm it end-to-end.
- `relation.polymorphic_pair`'s `(type, id)` atomicity, `geo.coordinate_pair`'s
  coordinate bounds, and `file.metadata`'s name/extension/MIME coherence
  have no runtime predicate — each is guaranteed by construction and unit
  tested, but not independently re-verified by `--verify`.

A `--verify` run that passes but skipped one of these capabilities still
exits `0`, with a warning listing exactly what wasn't checked.

## See also

- [Model reference](model-reference.md) — `source`/`profiles` fields.
- [Generators](generators.md#credential-generators) — credential generator shapes.
- [Planners](planners.md) — per-planner verification caveats.
- [Diagnostics](diagnostics.md) — `GEN-SOURCE-VALUES`, `GEN-VERIFY-NOTCHECKED`, and related codes.
