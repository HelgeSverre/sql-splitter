# Cross-Dialect Type Mapping (Phase 7)

This document is the contract of record for PostgreSQL↔MySQL scalar type
conversion. The base type table was settled in commit `b9fe5dd` and approved
in mailbox review [088]. The current contract also binds the reviewed target
table operation graph and the charset-aware MySQL width limit. The types,
bounds, CHECK constraints, and blocking decisions below are what the typed
`TableConversionPolicy`, its validator, and the create-only DDL renderers
enforce.

Origin: the 2026-08-11 market-alignment plan's Phase 7 item and the design
hazards raised in mailbox [083]. Serialization: row-conversion schema v3,
plan schema v17, PostgreSQL catalog format v6, MySQL catalog format v4.

## Scope and safety model

- **Cross-dialect execution is a separate reviewed path.** The operator CLI
  exposes direction-specific plan, execute, and resume commands. It does not
  route a conversion policy through either same-dialect runner. Each journal
  embeds the reviewed plan, exact per-table conversion policy, source
  consistency evidence, converted chunk digest, and target transaction intent.
- **PostgreSQL sources require a durable write fence.** The runner recaptures
  and normalizes the fenced catalog, re-attests the exact fence inventory
  before every target effect and final verification, and releases the fence
  only after strict verification is durable. A crash after release but before
  journal completion is recovered only from fully verified durable state.
- **MySQL sources require the retained repeatable-read snapshot and continuous
  external DML/DDL freeze evidence.** Freeze continuity and authoritative
  metadata visibility are re-attested before each target effect and before
  completion.
- Both directions can finish only as
  `CompletedWithApprovedTransformations`. Plan schema v17 is the compatibility
  boundary for these source-consistency rules; older plans fail at schema
  validation.
- **Every conversion rule is lossless-by-construction or fails closed.** This
  is load-bearing: verification digests the *converted* value on both sides,
  so a canonical-digest equality **cannot detect a lossy conversion rule** —
  only lossy storage. Correctness lives in the rule, not the check.
- Unmodeled types, collations, defaults, generated columns, identity
  semantics, and every tuple outside the table below fail closed as explicit
  blockers. `MigrationPlan::validate` checks `(source type, value rule,
  target type, CHECK constraints)` as one coherent closed mapping, plus exact
  nullability, contiguous ordinals, table/column dialects, and key
  suitability.

## PostgreSQL → MySQL

| PG source | MySQL target | Value / CHECK contract |
| --- | --- | --- |
| `bool` | `tinyint(1)` | bool→0/1, CHECK `0..1` |
| `int2`/`int4`/`int8` | corresponding signed integer | exact signed bounds |
| `numeric(p,s)`, p≤65, s≤30 | `decimal(p,s)` | exact coefficient/scale |
| unconstrained/oversized `numeric` | **blocked** | no exact MySQL DECIMAL target |
| `float4`/`float8` | `float`/`double` | exact IEEE bits; server round-trip fidelity still owes live proof |
| `bpchar`/`varchar(n)` | `varchar(n)` | exact UTF-8, non-padding target; CHAR→VARCHAR so trailing-space storage cannot change values |
| `text` | `longtext` | UTF-8 source, deterministic utf8mb4 binary target |
| `bytea` | `longblob` | exact bytes |
| `jsonb` | `JSON` | directional canonical JSON v1 (same `canonicalize_json` both sides; duplicate keys rejected) |
| `json` | **blocked** | lexical form and duplicate keys are not preserved by MySQL JSON |
| `date` | `date` | runtime range 1000..9999 |
| `time(p)` without zone | `time(p)` | exact PG source range |
| `timestamp(p)` without zone | `datetime(p)` | wall-clock preserved, MySQL 1000..9999 range enforced |
| `timetz`/`timestamptz` | **blocked** | no approved offset-preserving MySQL target |

## MySQL → PostgreSQL

| MySQL source | PG target | Value / CHECK contract |
| --- | --- | --- |
| `tinyint(1)` boolean | `boolean` | only 0/1 accepted |
| signed integer widths | widened integer | exact source-domain CHECK |
| unsigned `tiny`/`small`/`medium`/`int` | widened signed integer (holds the max, e.g. `INT UNSIGNED`→`int8`) | unsigned source-domain CHECK `0..=max` |
| unsigned `bigint` | `numeric(20,0)` | unsigned→decimal, CHECK `0..=u64::MAX` (20 digits holds u64 max exactly) |
| `decimal(p,s)` | `numeric(p,s)` | exact coefficient/scale; nonnegative CHECK when unsigned |
| `float`/`double` | `real`/`double precision` | exact IEEE bits; live proof still owed |
| `float(M,D)` / unsigned float | **blocked** | fixed-point / unsigned float not modeled |
| `char`/`varchar(n)` | `varchar(n)` | utf8mb4 only, non-padding target, length CHECK |
| `text` tiers | `text` | utf8mb4 only, source octet-tier CHECK |
| `binary`/`blob` tiers | `bytea` | exact bytes, source octet-tier CHECK |
| `JSON` | `jsonb` | directional canonical JSON v1 |
| `date` | `date` | source range CHECK |
| `time(p)` | `time(p)` | PG target-range enforcement (±838h / negative MySQL TIME rejected) |
| `datetime(p)` | `timestamp(p)` | wall-clock preserved, source range CHECK |
| `timestamp(p)` | `timestamptz(p)` | UTC-normalized→offset-aware at +00, CHECK `1970-01-01 00:00:01` .. `2038-01-19 03:14:07.999999` |

## Rules that govern the table

- **CHECK constraints are the domain-preservation mechanism.** Unsigned→signed
  and u64→decimal carry an explicit source-domain CHECK rendered into the
  target DDL; the target is never left under-constrained. CHECK literals are
  typed numeric or validated timestamp literals with quoted column names —
  no raw catalog text.
- **Temporal range quantization only narrows inward.** `at_precision` ceils
  the accepted minimum and floors the maximum, so a reviewed range never
  gains values by rounding a bound the wrong way.
- **Resumable keys must convert order-preservingly.** Only monotonic rules
  (integer widening, unsigned→signed widening, u64→decimal) may back a key.
  Float, JSON, and text rules are excluded; text keys are conservatively
  rejected cross-dialect because collation ordering diverges. MySQL binary
  keys are capped at 3072 bytes.
- **Deferred storage-fidelity proofs.** Float and JSON are lossless at the
  contract level; their real-server round-trip fidelity is proven only when
  the live cross-dialect matrix runs, and the contract says so rather than
  claiming it now.

## Open acceptance evidence

- The typed MySQL target contract caps `utf8mb4` `VARCHAR` at 16,383
  characters. Wider PostgreSQL `varchar(n)` columns now fail during policy
  derivation and cannot reach target DDL.
- The full live PostgreSQL 15/16/17 × MySQL 8.0/8.4 matrix in both directions
  remains open. It must prove the deferred float/JSON storage fidelity,
  create-only reconciliation, commit-response loss on both targets, source
  fence/freeze loss, cancellation, target drift after durable verification,
  and released-fence recovery. Phase 7 does not exit before this matrix is
  green and its exact commands and environment are recorded.
