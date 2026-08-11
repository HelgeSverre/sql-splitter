# Round-4 Review Resolution Ledger

Round 4 adversarially reviewed the 2026-08-11 market-alignment expansion:
documents [13](./13-throughput-and-copy-path.md),
[14](./14-managed-source-profiles.md), [15](./15-assessment-product.md), and
the amendments to [08](./08-implementation-prerequisites.md),
[12](./12-postgresql-first-adapter.md),
[observability](./observability-and-operations.md),
[competitive landscape](./competitive-landscape.md), and `docs/ROADMAP.md`.
Two independent review lenses ran: technical soundness against real
PostgreSQL 15–17 and MySQL 8.0/8.4 behavior, and internal consistency
against the existing contract documents. This ledger records the current
documentation state; it does not claim complete source accuracy.

| Finding                                                                                                  | Status   | Resolution                                                                                                                                                                                                 |
| -------------------------------------------------------------------------------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [13] placed parallel copy inside the beta while five documents define the beta as sequential             | Resolved | [13](./13-throughput-and-copy-path.md) subordinates parallel copy to Implementation Phase 9; the beta stays sequential; boundary documents change only with Phase 9's gates                                  |
| [08] Phases 9/10 still claimed the COPY and snapshot-sharing scope Phase 5a pulled forward               | Resolved | Phase 5a trimmed to COPY plus pipelined verification; parallel copy and the per-table journal format moved to Phase 9; Phase 10 rescoped to non-PostgreSQL COPY and remaining optimizations                  |
| Pipelined verification's second source session contradicted [03]'s one-session snapshot contract         | Resolved | [03](./03-connection-architecture.md) gains the exported-snapshot exception; [13] states the lifetime pinning and `pg_current_snapshot()` attestation rules                                                  |
| "Verification semantics do not change" was false: the completeness pass cannot be pipelined              | Resolved | [13] restricts pipelining to chunk-content equality; tail probes and the extra-row pass run after the final chunk commits, inside the fence or quiesce window                                                |
| Per-table `Prepared` invariant contradicted [04]'s one-prepared-chunk journal projection                 | Resolved | [04](./04-execution-design.md) records the Phase 9 relaxation under a bumped journal format; older journals rejected for automatic resume                                                                    |
| [14]'s probe suite could not prove requirements 1, 2, 4, or 5 as written                                 | Resolved | Rewritten as three evidence classes: catalog assertions against planned objects, transactional exercises with `NOWAIT` contention handling, and a sacrificial-session termination probe; install remains the enforcement authority |
| Sequence equality was overclaimed as quiesce evidence; `CACHE > 1` yields false passes                   | Resolved | [14] restates equality as restorability evidence only; `CACHE > 1` sequences stay write-fence-only; cache configuration recorded                                                                            |
| The `verified` tier implied a freeze proof it cannot deliver                                             | Resolved | Relabeled no-net-change evidence at re-scan time; write-then-revert churn and post-re-scan writes are named as uncovered                                                                                     |
| `attested-external-quiesce` lacked a boundary arm and a gate-2 stop analogue                             | Resolved | [14] states the profile runs under the consistent-snapshot arm; withdrawn attestation, sequence drift, or lost snapshot evidence stops execution                                                             |
| [14] had no delivery phase, no gates, no CLI surface; attestation timing contradicted [03]               | Resolved | Implementation Phase 5b added in [08]; attestation moved to execution time matching [03]; flags deferred to their phase per [04]'s future-flag rule                                                          |
| [12] "sequence execution is write-fence-only" contradicted [14]'s equality path                          | Resolved | [12] records the planned `CACHE 1` relaxation and that its gates have not run                                                                                                                                |
| [15] claimed the plan JSON "unchanged" while changing it; source-only executability was unresolved       | Resolved | Plan schema version bump with explicit not-assessed target sections; a source-only artifact is never a valid execution input                                                                                 |
| [15]'s read-only claim conflicted with embedded probe outcomes ([14] probes write and lock)              | Resolved | The report embeds only previously recorded evidence from a separate, explicitly non-read-only preflight probe run; the assessment itself never probes                                                        |
| [15]'s determinism gate failed on any live source (`reltuples`/sizes drift on an unchanged catalog)      | Resolved | Scope estimates and the projected window moved into the volatile-excluded blocks                                                                                                                            |
| The `abort` journal event had no writer, no IPC path, and no format bump                                 | Resolved | [observability](./observability-and-operations.md) defines `abort` as a request to the running process; the new event class bumps the journal format version                                                 |
| "Genesis record" and per-capability acknowledgement existed nowhere in the series' contracts             | Resolved | [04] defines the journal genesis frame; `acl.report_only` acknowledgement rides the existing `--approval-ref` review                                                                                         |
| `LOCK INSTANCE FOR BACKUP` was listed as a DML/DDL freeze candidate; it permits DML                      | Resolved | [08] qualifies it as the DDL half only, paired with `super_read_only` or `LOCK TABLES ... READ`                                                                                                              |
| MySQL `AUTO_INCREMENT` equality re-read defeated by `information_schema_stats_expiry` caching            | Resolved | [08] requires `information_schema_stats_expiry = 0` recorded as evidence, and sequences restoration after data load due to InnoDB clamp/bump behavior                                                        |
| ROADMAP's track stage 3 (CDC/online) contradicted the untouched real-time-streaming non-goal             | Resolved | The non-goal now carries the same explicit track exception, and stage 3 stays deferred until the non-goal is amended                                                                                          |

Checks that found no defect: the README document index and count, all new
cross-reference links and anchors, [13]'s COPY column-list rule for generated
columns, the identity-under-COPY deferral, COPY abort semantics, the
INSERT-fallback diagnostic path, [08]'s `START TRANSACTION WITH CONSISTENT
SNAPSHOT` and atomic-DDL statements, and the MySQL collation-binding rule.

Open work is implementation, real-engine validation of every mechanism this
round touched, and the acceptance gates in
[08](./08-implementation-prerequisites.md). No finding in this ledger changes
the offline same-dialect sequential beta boundary.
