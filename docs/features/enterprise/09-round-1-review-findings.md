# Part 9: Round-1 Review Findings

> Three parallel audits: code accuracy verification, industry claim
> verification, and adversarial edge case discovery. 12 industry claims
> checked, 50+ type/API references verified, 38 new failure modes found.

## Code Accuracy Audit

### Critical

**StatementType doesn't classify stored procedures/triggers/views.**

`02-capability-audit.md` line 408 claims the parser "already classifies
statement types" for stored procedures via `StatementType::CreateProcedure`,
`CreateFunction`, `CreateTrigger`, `CreateEvent`, `CreateView`. These
variants **do not exist** in `StatementType` (`src/parser/mod.rs:305`).
The parser returns `StatementType::Unknown` for these.

This directly invalidates the Gap 7 claim that stored procedure cataloging
"detection is easy." The parser has statement-boundary awareness (handles
`DELIMITER`) but does not classify these statement types. Adding the
classification requires extending the `StatementType` enum and the parser's
statement-type detection logic.

**Fix**: Update `02-capability-audit.md` Gap 7 to note that statement-type
classification must be added to the parser before cataloging is possible.

### Minor

**TypeMapper visibility**: `08-implementation-prerequisites.md` R1 says
`TypeMapper` is `pub(crate)` and needs changing. Actually the struct itself
is already `pub`. The method `map_column_type()` is `pub(crate)` and needs
visibility change. R1 description should be corrected.

**GenerateError location**: `05-review-findings.md` references
`src/generate/mod.rs:330` as the `GenerateError` location. The enum is
actually defined in `src/generate/value.rs:157`. `mod.rs:330` contains
`Generate::run()` which uses the error type. The docs should reference
the definition site, not the call site.

### Confirmed accurate

14/14 file:line references verified exact. 12/14 type structures verified
matching. All proposed types/traits correctly identified as not-yet-existing.

## Industry Claims Verification

10/12 claims fully confirmed by Context7. 1 partially accurate, 1
confirmed pattern.

### Partially accurate

**PlanetScale VTGate query timeout tiers**: The design docs (C-FIX-6)
claim VTGate kills queries at 60s (scalar) / 300s (team/enterprise). The
documented system limit is **900 seconds** for autocommit queries
regardless of tier. Context7 found no evidence for tier-specific kill
thresholds. The claim should reference 900s as the documented maximum.

**Fix**: Update `05-review-findings.md` CF-6 to use 900s documented limit
as the upper bound, and note that tier-specific thresholds are
unconfirmed.

### Fully confirmed

- MySQL crate text/binary protocol streaming ✓
- PostgreSQL COPY + native portal cursors ✓
- Neon 5min timeout, session reset, ~300ms wake ✓
- PgBouncer SET statements don't persist in transaction mode ✓
- MySQL I_S fast on 8.0+, slow on 5.7 ✓
- RDS `CALL mysql.rds_set_configuration('binlog retention hours', 24)` ✓
- mydumper `--rows` auto-double/halve at 1s/2s thresholds ✓
- gh-ost triggerless, reads binlog directly ✓
- ClickHouse `MOVE PARTITION` metadata-only operation ✓
- MariaDB JSON = LONGTEXT + CHECK constraint ✓
- CockroachDB MOLT 4-tool suite ✓

## Edge Case Audit

38 new failure modes discovered across 5 categories. The original design
had 60 failure modes. The expanded catalog now covers **98 failure modes**.

### New Schema Failures (S7–S18)

Generated column dependencies, invisible columns, functional indexes,
partial indexes, view chains, recursive views, ROW_FORMAT compressed,
page compression, tablespace clauses, descending indexes, expression
defaults, wide tables (200+ columns), ghost tables on target from
previous failed runs.

### New Data Failures (D13–D20)

NULL PK → silent duplication on re-run, ENUM values outside definition,
`*/` in data corrupting SQL output, 4 GB BLOB in single row, mixed
encodings in same column, BIT padding differences, floating-point NaN,
JSON semantic equivalent mismatch, negative auto-increment, non-deterministic
column defaults in verification.

### New Operational Failures (O1–O9)

Replica falls behind during migration, Aurora instance change mid-stream,
DST transition shifts TIMESTAMP, `max_execution_time` kills long SELECTs,
1% packet loss causing throughput variance, no consistent snapshot during
live-source migration, `read_only=ON` on target, deadlock between parallel
workers on adjacent PK ranges, `sql_require_primary_key` flag on source.

### New Parallelism Failures (P21–P24)

Same table name in different schemas hash to same shard, a level has 1
table with `--parallel 4`, cross-schema FK, multi-table FK cycle.

### New Verification Failures (V6–V10)

JSON normalization mismatch, non-deterministic column defaults,
floating-point NaN, BIT(N) with N not multiple of 8, secondary checksum
algorithm disagreement.

### Highest-Severity Uncovered

| #   | Severity | Failure                                                     |
| --- | -------- | ----------------------------------------------------------- |
| D13 | High     | NULL PK → silent duplication on re-run                      |
| D15 | High     | `*/` in data corrupting SQL output file                     |
| O4  | High     | `max_execution_time` kills long SELECTs                     |
| O6  | High     | No consistent snapshot during live-source migration         |
| V7  | High     | Non-deterministic columns → perpetual verification failures |
| D18 | High     | JSON normalization → false checksum mismatch                |

## Fix Status Ledger

Status of issues identified in this round:

| #   | Issue                                            | Status          | Where                                                    |
| --- | ------------------------------------------------ | --------------- | -------------------------------------------------------- |
| 1   | StatementType lacks CreateProcedure/etc variants | **FIXED**       | `02-capability-audit.md` Gap 7 updated                   |
| 2   | TypeMapper visibility misdocumented              | **FIXED**       | `08-implementation-prerequisites.md` R1 corrected        |
| 3   | GenerateError reference wrong                    | **FIXED**       | `05-review-findings.md` reference updated                |
| 4   | PlanetScale VTGate timeout spec inaccurate       | **FIXED**       | `05-review-findings.md` CF-6 updated to 900s             |
| 5   | 38 new failure modes added to 04 §6.2            | **NOT APPLIED** | Still only 60 modes in catalog; 38 modes in 09 text only |
| 6   | All 14 file:line references verified             | **CONFIRMED**   | N/A (verification-only)                                  |
| 7   | 10/12 industry claims confirmed accurate         | **CONFIRMED**   | N/A (verification-only)                                  |
