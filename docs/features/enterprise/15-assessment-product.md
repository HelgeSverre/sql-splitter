# Assessment Product

> **Implementation status:** the feature-gated PostgreSQL spike implements the
> source-only artifact, deterministic Markdown report, typed target-not-assessed
> state, role/read-only evidence, and protected output. The PostgreSQL 15–17
> matrix now exercises exhaustive supported-version blocking codes, statement
> auditing, the source-only CLI, deterministic output, and protected artifacts.
> Acceptance gates 1–6 pass for this current PostgreSQL 15–17 contract.
> PostgreSQL 18 virtual generated columns and mixed-major execution diagnostics
> are outside that contract and retain separate fail-closed coverage. This is
> still feature-gated, experimental, and not for production.

Plan-only becomes a standalone product — the **assessment** — and the first
shippable milestone of the enterprise migration track, ahead of the execute
beta. It is the read-only artifact a migration engineer needs before any
engagement: everything in the source that will and will not migrate cleanly,
and why. It is safe on production sources by construction, is nearly covered
by the existing plan adapter, and produces the evidence that decides which
unsupported object classes are funded next. Origin: the 2026-08-11
market-alignment plan.

## Product boundary

- Read-only: one `REPEATABLE READ READ ONLY` transaction per endpoint, with
  server-enforced transaction evidence and a scan for direct database,
  schema, relation, view, foreign-table, and sequence write privileges. The
  scan does not claim that every executable routine is side-effect-free;
  PostgreSQL does not uniformly treat transaction read-only mode as a sandbox
  for function side effects. Safety therefore requires both the server-side
  transaction restrictions and the audited assessment statement set. The
  assessment issues no write statements or non-allowlisted function calls.
- Source-only mode: an assessment must run with only a source endpoint.
  Target-dependent checks (emptiness, ownership, capability parity) are
  reported as **not assessed**, not failed. This is the one contract change
  from today's plan extraction, which assumes both endpoints.
- Artifacts land only in protected local files with the existing guarantees
  (`0600`, exclusive creation, symlink defenses, fsync).

## Deliverables

1. The machine-readable plan artifact under a bumped schema version whose
   target sections are optional and explicitly marked **not assessed** when
   absent. A source-only assessment artifact is never a valid execution
   input: execute continues to require a two-endpoint reviewed plan with
   both catalogs, fingerprints, and TLS bindings, and rejects any artifact
   lacking them.
2. A deterministic human-readable report rendered from that same data.
   Markdown first. Any later HTML rendering or interactive flow consumes the
   same versioned data; it never issues new queries.

## Report contents

- **Identity and evidence:** server versions, TLS posture including any
  recorded insecure capability, catalog fingerprints, read-only evidence.
- **Supported inventory:** per-object-class summary of what migrates.
- **Unsupported objects:** grouped by class with the exact recorded reason
  string, split into blocking and non-blocking, including the mandatory
  `acl.report_only` capability
  ([12](./12-postgresql-first-adapter.md#privileges-acls)).
- **Execution requirements:** available consistency modes and fence profile
  requirements ([14](./14-managed-source-profiles.md)). Probe outcomes
  appear only when a separate, explicitly non-read-only preflight probe run
  recorded them; the assessment itself never runs probes.
- **Scope estimates:** `reltuples` and relation sizes, labeled as estimates,
  never presented as exact counts.
- **Projected window:** expected copy-plus-verification window from measured
  throughput where measurements exist
  ([13](./13-throughput-and-copy-path.md)), labeled as an estimate.

Confidentiality: the report contains schema identifiers and object
definitions and is customer-confidential. It never contains credentials, row
values, key values, or SQL literals derived from data, consistent with the
redaction rules in
[observability](./observability-and-operations.md).

## Determinism

Two assessments of an unchanged catalog produce byte-identical report
bodies. Volatile content — run identifier, timestamps, scope estimates, and
the projected window, which drift with statistics and load even on an
unchanged catalog — is confined to delimited volatile blocks excluded from
the determinism comparison. This is a
live-matrix gate, matching the plan-determinism posture of
[12](./12-postgresql-first-adapter.md).

## Acceptance gates

1. Every extracted catalog object class appears in exactly one report
   section. A new extraction class without a report section fails the
   render; it is never silently omitted.
2. Every blocking finding code is exercised by at least one live-matrix
   case on every supported PostgreSQL version. Codes, not reason strings,
   are the exhaustiveness unit: reasons may interpolate catalog context, so
   every rendered reason carries its stable code, and interpolated
   fragments may contain catalog identifiers and definitions but never data
   values. A code unreachable within the supported single-major contract
   (currently cross-major and newer-major generated-column diagnostics) is
   named in the registry test with the reason it is unreachable and keeps
   separate fail-closed coverage.
3. Read-only proof: the live matrix asserts the transaction is server-side
   read-only, the role has none of the direct write privileges listed above,
   and the assessment issues no write statements. The matrix includes
   routine and updatable-view escape cases so the report does not overstate
   the privilege scan.
4. Source-only mode passes against a source with no target configured.
5. The determinism gate above.
6. Artifact protections identical to plan artifacts.
