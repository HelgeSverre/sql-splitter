# Managed Source Profiles

Enterprise sources frequently live on managed services (Amazon RDS and
Aurora, Google Cloud SQL, Azure Database, and similar). The current write
fence assumes privileges those services restrict, and the fallback
`consistent-snapshot` mode blocks on any sequence, which excludes nearly
every real application schema. This document defines source profiles that
keep the fail-closed posture while making the tool usable where enterprise
databases actually run. Origin: the 2026-08-11 market-alignment plan.

## Fence privilege requirements

Fence installation currently requires, on the source database:

1. `ACCESS EXCLUSIVE` locks on every planned table;
2. `CREATE TRIGGER` on every planned table (DML guards);
3. `CREATE EVENT TRIGGER` (DDL guard; superuser-gated on stock PostgreSQL);
4. sequence ownership transfer plus `USAGE`/`UPDATE` revocation;
5. `pg_terminate_backend` over every other session in the database;
6. registry and audit table writes.

Managed providers restrict several of these, differently per provider,
engine version, and admin role, and the rules change over time.

## Contract: probes, not vendor tables

The tool does not trust a static vendor capability table, and it does not
pretend every requirement is probeable without side effects. The preflight
probe suite has three evidence classes:

1. **Catalog assertions** for per-object rights — requirements 1, 2, and 4.
   Lock modes above `ROW SHARE` require table-level `UPDATE`, `DELETE`, or
   `TRUNCATE` privilege or ownership; `TRIGGER` is a per-table grant;
   ownership transfer depends on the current owner. These are checked with
   `has_table_privilege`/`pg_has_role`-style queries against the actual
   planned objects. Probe objects in a migration-owned scope prove nothing
   about customer tables and are not used for these requirements.
2. **Transactional exercises** where rollback is real — requirement 3
   (`CREATE EVENT TRIGGER`) and requirement 6 (registry writes). An optional
   exercised lock probe against planned tables uses `NOWAIT` with retry and
   is explicitly documented as briefly blocking production traffic. Lock
   contention is distinguished from privilege denial and is never reported
   as a missing capability.
3. **Sacrificial-session probe** for requirement 5. Termination is a signal
   and cannot be rolled back, and terminating the tool's own same-role
   backend proves nothing. The probe checks `pg_signal_backend` (or
   superuser) membership and terminates a sacrificial session opened under
   a second, distinct migration-owned role. A non-superuser administrator
   cannot terminate superuser-owned backends; the fence drain therefore
   remains the enforcement authority and hard-fails when any session
   survives, exactly as today.

Probe outcomes predict fence installation; they never replace it. Outcomes
are recorded as evidence in the reviewed plan and bound into the journal's
first durable frame (its genesis, [04](./04-execution-design.md)). Any
failed probe blocks fence installation and names the exact missing
capability. Vendor documentation is advisory context only.

## Profiles

| Profile | Freeze enforcement | Requirements |
| ------- | ------------------ | ------------ |
| `self-managed-administrator` | Full fence, current behavior | All six requirements proven by probe |
| `managed-administrator` | Full fence semantics through a provider admin role | All six requirements proven by probe under that role; any unprobeable requirement excludes this profile |
| `attested-external-quiesce` | None by the tool; freeze by external means (application maintenance mode, provider read-only flag) | Operator attestation supplied at execution, matching the write-fence acknowledgement timing in [03](./03-connection-architecture.md); sequence-stability evidence (below) always required |

This profile reads through one database-native consistent snapshot, so it
executes under the second arm of the README product boundary; the plan
records the profile, and execution binds the attestation reference at
journal genesis. The gate-2 analogue for this profile: a withdrawn
attestation, sequence-state drift, or loss of snapshot evidence stops
execution, exactly as write-fence loss stops fenced execution.

The profile has an optional `verified` tier: a fresh-session re-scan
equality proof for every planned table after verification completes, at the
stated cost of one additional full source pass. The tier is no-net-change
evidence at re-scan time, not a freeze proof: write-then-revert churn passes
it, and it constrains nothing after the re-scan. With or without the tier,
the plan and assessment report state prominently: **source freeze is NOT
enforced by the tool under this profile.**

The selected profile is a recorded plan capability. Execute and resume
re-attest the same profile; a profile change is drift and blocks. A degraded
profile exists only as an explicit, recorded, reviewer-visible capability —
never as a silent fallback.

## Sequence stability replaces the sequence dead end

`consistent-snapshot` currently emits a blocking unsupported object for any
sequence, because sequence state is outside table MVCC. Replace the
unconditional block with an equality proof:

1. Read complete sequence state (configuration, `last_value`, `is_called`)
   at snapshot establishment.
2. Re-read the same state after data copy and verification complete.
3. Equal: the sequence is restorable from the proven-stable state, and the
   equality is recorded as evidence.
4. Unequal: block, naming each drifted sequence.

Equality proves one thing: the recorded sequence state is still current and
therefore restorable. It is not quiesce evidence — UPDATE and DELETE traffic
and inserts that bypass sequence defaults touch no sequence. A sequence with
`CACHE` greater than one is excluded from this evidence class and remains
write-fence-only: a session that pre-allocated a cache batch can consume
values between the two reads without changing on-disk state, so equality
would prove nothing. The recorded evidence includes each sequence's cache
configuration. The mechanism applies to `consistent-snapshot` and
`attested-external-quiesce`; write-fence mode keeps its stronger
ownership-transfer and drain contract unchanged.

## Provider acceptance gates

A profile lists zero supported providers until the probe suite and the
relevant live matrices pass against a real instance of that provider. A
support statement names the provider, engine version, and admin role tested.
This follows the same manual reproducible-matrix posture as
[12](./12-postgresql-first-adapter.md); no CI promise is made here.

## Probe and attestation interfaces (Phase 5b)

Selected contract (mailbox [021-codex]/[023]):

- **Probe command.** Probes with side effects never run inside plan-only or
  the assessment ([15](./15-assessment-product.md)). A separate
  approval-gated command, `probe-postgres-source-profile`, consumes the
  exact planned source catalog plus an administrator endpoint, performs the
  documented probe suite for the selected profile, and publishes one
  protected, schema-versioned typed probe artifact recording
  per-requirement, per-object outcomes. The command requires an explicit
  `--execute` acknowledgement, and the optional exercised lock probe is
  documented as briefly blocking production traffic. `plan-postgres`
  accepts the probe artifact only when an administrator profile is
  selected, validates it against the exact catalog fingerprint, and binds
  its digest and outcomes into the reviewed plan. A probe artifact is
  rejected across catalog drift.
- **Profile encoding.** The plan records a typed
  `PostgresSourceProfileContract`
  (`self_managed_administrator`, `managed_administrator`,
  `attested_external_quiesce`). Profiles are never encoded as capability
  strings. The external profile also carries the required durable field
  `freeze_enforced_by_tool: false`; validation rejects a plan that claims the
  tool enforces that freeze.
- **Attestation artifact.** The external-quiesce attestation is a
  protected typed artifact: schema version, attestation reference, exact
  source endpoint and catalog fingerprint, issued and expiry times, and
  active or withdrawn status. Execute and resume take the same artifact
  input, validate it against the plan, and bind its canonical digest at
  journal genesis. A different artifact with the same reference is never
  silently substituted. Withdrawal or sequence-state drift stops execution
  (the gate-2 analogue above). Expiry follows the outage-policy precedent
  in [13](./13-throughput-and-copy-path.md): recorded admission stays
  valid for recovery, and renewal is an explicit recorded event, never a
  substitution. The initial tier is operator evidence, not independently
  verified freeze enforcement, and the plan and report say so.
- **CLI surface**, introduced only in Phase 5b per
  [04](./04-execution-design.md)'s future-flag rule — probe:
  `--source-config`, `--admin-config`, `--profile`, `--probe-output`,
  explicit `--execute`; plan: `--source-profile` and
  `--source-profile-evidence` for administrator profiles, plus
  `--verified-external-quiesce-rescan` to select the optional full re-scan
  tier; execute/resume:
  `--external-quiesce-attestation` for the external profile only.

## MySQL freeze profile (Phase 6)

Selected contract (mailbox [035-codex]/[036]). MySQL has no durable
database-enforced fence primitive: `super_read_only` blocks all client
writes (including `SUPER`/`CONNECTION_ADMIN`) but exempts replication
applier threads, `LOCK INSTANCE FOR BACKUP` blocks only DDL and
file-affecting operations and is released when its owning session dies, and
`LOCK TABLES ... READ` is session-scoped and conflicts with the snapshot
reader. No single candidate is a crash-resumable fence, so the first
supported MySQL execution profile is an **external continuous freeze
attestation**:

- A separate authenticated admin endpoint attests server UUID, database,
  TLS binding, profile generation, provider or control-plane reference,
  activation and expiry times, and a continuity token owned outside the
  migration process. The token and the backup-lock owner's session identity
  are bound into journal genesis like the Phase 5b attestation digest.
- The profile proves both DML and DDL exclusion and names the mechanisms.
  The initial locally testable pair is persistent (`SET PERSIST`) global
  `super_read_only = ON` plus an externally owned
  `LOCK INSTANCE FOR BACKUP`.
- Active replication and group-replication appliers are rejected by this
  first profile, closing the `super_read_only` applier exemption. A
  replica-as-source therefore cannot use this profile; a future profile may
  attest a stopped applier instead. This limitation is recorded, not
  hidden.
- Exact checks bind `read_only`, `super_read_only`, server UUID,
  backup-lock owner identity, and the absence of active replication
  channels before snapshot, before every target effect, on resume, before
  final verification, and before release.
- The external lock owner, not the migration process, must survive
  migration-process failure. A missing or replaced lock owner, a continuity
  token change, or a server restart (which necessarily kills the lock
  owner) permanently invalidates old journal state; reacquiring a new lock
  cannot resume it, because the gap may contain writes.
- `AUTO_INCREMENT` state is captured with `information_schema_stats_expiry
  = 0` after activation and must be exactly equal on a fresh end read
  before target restoration. GTID remains observation only. A full source
  re-scan may be required as additional profile policy but cannot repair a
  continuity gap.
- The migration tool never toggles global read-only state and never
  releases the external backup lock. Release is a separate operator or
  provider action after durable `Completed` state.

## Delivery placement

This document is Implementation Phase 5b in
[08](./08-implementation-prerequisites.md). Its CLI surface — profile
selection and the attestation input — is introduced only in that phase, per
the future-flag rule in [04](./04-execution-design.md).

## What this does not change

No online or zero-downtime claim. No CDC. Fail-closed defaults everywhere:
an unproven requirement, an unprobeable capability, a drifted sequence, or a
changed profile blocks execution rather than degrading silently.
