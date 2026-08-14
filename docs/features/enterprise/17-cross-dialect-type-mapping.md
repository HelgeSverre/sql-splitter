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
plan schema v22, PostgreSQL catalog format v6, MySQL catalog format v5.

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
  `CompletedWithApprovedTransformations`. Plan schema v22 is the compatibility
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
- Cross-dialect v1 recreates tables, columns, target checks, and the selected
  primary or unique resumable key. It does not recreate ordinary non-unique
  secondary indexes. The reviewed plan lists each omitted secondary index as a
  non-blocking approved transformation because it changes target query
  performance but not stored-row integrity. Unique indexes that are not the
  selected key, foreign keys, and other schema objects remain blocking because
  they carry unimplemented data-integrity or behavioral semantics.
- MySQL text values retain their exact UTF-8 bytes, but cross-dialect v1 does
  not claim that a MySQL collation is behaviorally equivalent to the selected
  PostgreSQL collation. The reviewed plan lists each text-column collation remap
  as a non-blocking approved transformation and names both collations. Comparison,
  sort, and uniqueness behavior can differ on the target.

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
- **Storage-fidelity proofs.** Float and JSON are lossless at the contract
  level. The live matrix below proves their real-server round-trip fidelity
  for the recorded finite IEEE and canonical JSON vectors on every supported
  PostgreSQL/MySQL version pair.

## Acceptance evidence

- The typed MySQL target contract caps `utf8mb4` `VARCHAR` at 16,383
  characters. Wider PostgreSQL `varchar(n)` columns now fail during policy
  derivation and cannot reach target DDL.
- The baseline live PostgreSQL 15/16/17 × MySQL 8.0/8.4 matrix passes in both
  directions. It covers bounded multi-chunk execution and exact Boolean,
  decimal, finite `float4`/`float8`, Unicode text, binary, and canonical JSON
  target readback. Run these commands from the repository root:

  ```text
  scripts/test-migration-cross-dialect.sh 15 8.0
  scripts/test-migration-cross-dialect.sh 15 8.4
  scripts/test-migration-cross-dialect.sh 16 8.0
  scripts/test-migration-cross-dialect.sh 16 8.4
  scripts/test-migration-cross-dialect.sh 17 8.0
  scripts/test-migration-cross-dialect.sh 17 8.4
  ```

  Recorded 2026-08-12 on Darwin 24.6.0 arm64, Docker client/server 29.4.0
  with Linux arm64 containers, and Rust 1.97.1. Each command passed both
  direction-specific ignored integration tests.
- The recovery matrix passes on all six PostgreSQL 15/16/17 × MySQL 8.0/8.4
  version pairs in both directions. It covers:
  - durable table `Prepared` and create-effect-applied states;
  - durable chunk `Prepared` and target-commit-before-journal states;
  - cancellation after insert with transaction rollback;
  - COMMIT bytes not forwarded to the target;
  - target commit externally visible while its acknowledgement is withheld;
  - interruption after durable verification;
  - target drift after durable verification;
  - MySQL freeze loss and PostgreSQL fence loss;
  - PostgreSQL fence release before terminal journal publication.

  Recovery mode uses the same disposable harness and one named interruption:

  ```text
  SQL_SPLITTER_CROSS_INTERRUPTION=<boundary> \
    scripts/test-migration-cross-dialect.sh <pg-version> <mysql-version> \
      mysql-to-postgres-recovery
  SQL_SPLITTER_CROSS_INTERRUPTION=<boundary> \
    scripts/test-migration-cross-dialect.sh <pg-version> <mysql-version> \
      postgres-to-mysql-recovery
  ```

  Supported boundaries are `table-prepared`, `table-effect-applied`,
  `chunk-prepared`, `chunk-effect-applied`, `cancel-after-insert`,
  `network-not-forwarded`, `network-ack-lost`, and `after-verification`.
  PostgreSQL-source execution also supports
  `after-postgres-fence-release`. Set
  `SQL_SPLITTER_CROSS_EXPECT_TARGET_DRIFT=1` or
  `SQL_SPLITTER_CROSS_EXPECT_SOURCE_LOSS=1` with `after-verification` for the
  two fail-closed negative cases.
- The same shared-catalog checkpoint passes the complete same-dialect MySQL
  8.0/8.4 and PostgreSQL 15/16/17 live matrices. This includes the MySQL
  `BIT(9)` value fixture, which preserves its reviewed width through catalog
  extraction and target binding.

**Phase 7 exit: met at spike level on 2026-08-12.** The baseline transforming
value matrix and the 21-case recovery matrix pass on all six supported
PostgreSQL/MySQL version pairs in both directions. Mailbox review [100]
approved the combined implementation through commit `68b3e3e` with no
critical or high findings. This remains feature-gated spike evidence. The
large-dataset, two-managed-instance acceptance in mailbox [078] is still the
higher production bar and is not claimed here.
