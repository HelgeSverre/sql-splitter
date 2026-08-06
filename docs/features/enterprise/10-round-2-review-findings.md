# Part 10: Round-2 Review Findings

> Five parallel audits: architecture trace (concrete scenario), performance
> model validation, security audit, concurrency/ordering audit, and feature
> flag/build audit. 6 critical gaps, 18 high-severity, 14 medium-severity.

## Architecture Trace (Concrete Scenario)

Traced a 200-table, 50GB MySQL 8.0 → MySQL 8.4 migration through every
pipeline phase. Three critical failures found:

| #      | Phase                  | Severity     | Gap                                                                                                                                                                                                                                                                                                                      |
| ------ | ---------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| GAP-M2 | Data Migration         | **CRITICAL** | Self-referential FK tables get `SELECT *` with no intra-table row ordering. Rows with `manager_id=42` inserted before row `id=42` → FK violations. HF-2 identified this but §6.1 was never updated.                                                                                                                      |
| GAP-D2 | DDL Execution          | **CRITICAL** | FK cycle constraints (a→b→c→d→a) created in Phase 3 without `FOREIGN_KEY_CHECKS=0`. MySQL rejects the cycle's ALTER TABLE with error 1215. HF-3 says Phase 3b is sequential but never adds FK-disable.                                                                                                                   |
| GAP-X1 | Schema/Data Extraction | **CRITICAL** | No consistent snapshot from live MySQL source. Data extracted across 200 tables over hours is internally inconsistent — row in `orders` references `user_id` deleted between extraction of `users` and `orders`. GTID capture, `START TRANSACTION WITH CONSISTENT SNAPSHOT`, or replica-stop-and-extract are all absent. |

Also found: composite PK metadata threading for idempotent INSERT
unverified (HIGH), BLOB single-row exceeds max_allowed_packet with no
fallback (HIGH), chunked verification for composite PKs unspecified (HIGH).

## Performance Model Validation

Traced throughput numbers through physical constraints. The model is
internally consistent and physically plausible for same-region deployments.
Two significant gaps:

| #    | Severity | Gap                                                                                                                                                                                                   |
| ---- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PM-1 | **HIGH** | Throughput model assumes RTT <5ms. Cross-region (50ms RTT): ~40% of rated throughput. Cross-continent (200ms RTT): ~15%. 2TB migration at 200ms RTT: 72h with 4 workers, not the ~22h model predicts. |
| PM-2 | **HIGH** | Pre-flight does not check target max_connections. With 4 workers (8 connections) + 40 app connections on a 51-connection managed MySQL, migration fails.                                              |

Also found: RTT-bounded throughput formula missing, post-import autovacuum
storm on PG (200 tables simultaneously), source buffer pool pollution
unquantified, `utf8` alias semantics change (3-byte→4-byte) between
MySQL 8.0 and 8.4 not flagged.

## Security Audit

Six critical/high findings:

| #     | Severity     | Gap                                                                                                                                                                                             |
| ----- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SEC-1 | **CRITICAL** | TLS is optional, not enforced. Data crosses public internet unencrypted by default. No `SslMode` enum, no enforcement in `DbSource`/`DbTarget` traits.                                          |
| SEC-2 | **CRITICAL** | `--log-level trace` logs all INSERT values including PII to plaintext JSONL file. No redaction, no encryption, no access control. Trace-level logging is taught in documentation examples.      |
| SEC-3 | **HIGH**     | Table/column names from `information_schema` injected into generated SQL without proper identifier quoting. SQL injection via pathological identifier names containing backticks/quotes.        |
| SEC-4 | **HIGH**     | Inline passwords in URIs taught in every CLI example and benchmark script. Visible in `ps aux`, `.bash_history`, CI logs. The CLI design shows `--source mysql://root:testpass@...` everywhere. |
| SEC-5 | **HIGH**     | Core dumps contain credentials + batch data. No `RLIMIT_CORE` control. `strace` captures auth handshake bytes.                                                                                  |
| SEC-6 | **HIGH**     | `ssh2` crate wraps `libssh2` (C library, 8+ CVEs in 5 years). Rust FFI boundary is a memory safety risk in otherwise-safe codebase.                                                             |

## Concurrency and Ordering Audit

The topo-levels algorithm is correct for all tested graph patterns. The
parallel import design is sound for the common case. Three gaps:

| #     | Severity   | Gap                                                                                                                                                                     |
| ----- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CON-1 | **HIGH**   | Phase 1 drops ALL existing FK constraints on target tables, including those not created by the migration. Could corrupt a shared target.                                |
| CON-2 | **MEDIUM** | Between-level orphan check could abort on transient state during re-run (parent not fully re-imported yet).                                                             |
| CON-3 | **MEDIUM** | O8 "deadlock between parallel workers" claim not reproducible under stated conditions (READ-COMMITTED + UNIQUE_CHECKS=0). Should be recategorized as "lock contention." |

## Feature Flag and Build Audit

No conflicts between proposed `migrate` feature and existing features.
Issues:

| #     | Severity   | Gap                                                                                     |
| ----- | ---------- | --------------------------------------------------------------------------------------- |
| BLD-1 | **HIGH**   | `mysql = "25"` is 3 major versions behind (latest is 28.0.0).                           |
| BLD-2 | **MEDIUM** | CI tests only 2/16 feature combinations. `migrate` feature would never be tested in CI. |
| BLD-3 | **MEDIUM** | No `cargo-deny`/`cargo-audit` for supply chain vulnerability scanning.                  |

## Applied Fixes

These specific corrections have been applied to the design documents:

1. **04 §6.1 Data Migration**: Added conditional `ORDER BY` for
   self-referential FK tables: `SELECT * FROM table WHERE manager_id IS
NULL OR manager_id IN (already imported PKs) ORDER BY manager_id IS
NULL, id`.

2. **04 §6.3d + 05 HF-3**: Phase 3b DDL connection now explicitly sets
   `SET FOREIGN_KEY_CHECKS = 0` before adding FK cycle constraints and
   `SET FOREIGN_KEY_CHECKS = 1` after.

3. **03 §5.2.6 + 04 Phase 0**: Added `START TRANSACTION WITH CONSISTENT
SNAPSHOT` for live MySQL source extraction. GTID captured before/after.
   Data extraction wrapped in single transaction for snapshot consistency.

4. **04 §4.12**: Added `SslMode` enum to `DbConfig` with default
   `VerifyFull`/`VerifyIdentity`. TLS enforcement in both `DbSource` and
   `DbTarget` traits. `--ssl-mode` flag added.

5. **04 §6.7**: Trace-level logging gated behind `#[cfg(feature = "debug-trace")]`.
   Column value redaction at trace level using credential-name heuristic.
   `AtomicOutput` for `--log-file`.

6. **04 §6.1 CLI examples**: All password-in-URI examples replaced with
   `--source-password-file` or environment variable patterns.

7. **04 §6.3b Performance Model**: Added RTT-bounded throughput note,
   pre-flight connection pool check, post-import autovacuum management.

8. **08 §6**: Updated `mysql` version to 28. Added CI matrix recommendations.
