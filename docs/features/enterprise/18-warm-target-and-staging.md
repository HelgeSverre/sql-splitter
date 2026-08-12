# Warm Target and Staging Contract

Implementation Phase 8 adds two same-dialect target modes. It does not make
the current empty-target executor destructive by default.

1. **Warm merge** preserves target-only rows and inserts source rows whose
   complete keys do not exist on the target.
2. **Staging swap** builds and verifies migration-owned objects under staging
   names, then replaces an explicitly reviewed live namespace or table set in
   one dialect-specific cutover operation. The replaced objects remain under
   retained backup names until a separate cleanup operation.

These modes are offline migration features. They do not provide CDC, online
catch-up, automatic application rollback, or zero-downtime cutover.

## Reviewed target mode

The reviewed plan contains a typed target mode. A source-only assessment marks
the mode `not_assessed`. An execution plan uses exactly one of:

- `empty_owned` — the existing create-only contract;
- `warm_merge` — the disjoint-key union contract in this document;
- `staging_swap` — the retained-backup replacement contract in this document.

The target mode is part of the plan hash, approval, and journal genesis.
Mode-specific operations also include their typed mode contract in their
parameters and operation identities when those execution paths are added.
Execute and resume reject an absent or different mode before opening a mutating
target session. A plan for one mode cannot authorize another mode. There is no
command-line override after review.

Phase 8 introduces new CLI options only when their complete execution paths
exist. Until then, the current commands continue to build `empty_owned` plans.

The target mode also contains one typed protection contract. PostgreSQL uses a
migration-token fence whose trigger exemption is available only to sessions
that prove the per-migration secret. MySQL uses migration-token DML guards plus
a separately held external DDL freeze, or a provider can attest one continuous
external quiesce that excludes both DML and DDL. The reviewed plan binds the
mechanism and external provider identity, but never the runtime token. Execute
cannot substitute a different mechanism.

The existing source fence is not a target fence. Its PostgreSQL DML guard
rejects the migration writer, and MySQL `super_read_only` rejects privileged
client updates as well. Phase 8 requires separate target-fence installation,
attestation, release, and recovery paths with a narrow migration exemption.

## Ownership manifest

Warm merge and staging swap require a closed ownership manifest over the exact
reviewed target catalog. Every user namespace and catalog object appears once
with one disposition:

- `preserve` — the migration must not modify or rename it;
- `merge_rows` — an existing compatible table participates in warm merge;
- `replace_at_cutover` — an existing object is replaced by a verified staging
  object and retained under its reviewed backup identity.

The manifest binds the target catalog fingerprint and the stable catalog
identity of every classified object. Duplicate, missing, unknown, or
mode-incompatible classifications invalidate the plan. Objects created by the
migration are derived from reviewed operations and are not presented as
pre-existing target ownership.

The authenticated target role must prove the exact authority needed for every
`merge_rows` or `replace_at_cutover` object. Authority over one namespace or
table is not evidence for another object. The executor does not take ownership
of preserved objects. Owner, ACL, role, and grant semantics that the selected
dialect adapter cannot reproduce remain blocking unsupported semantics.

## Warm merge

### Supported conflict policy

The first complete policy is `reject_any_key_collision`.

- Every participating table has a complete immutable non-null unique key that
  satisfies the existing keyset contract.
- Source and target schemas, types, collations, generated expressions,
  constraints, and other admitted semantics are exactly compatible before any
  row write.
- A source key already present on the reviewed target is a conflict, even when
  its canonical row is equal. The executor cannot infer ownership of an equal
  pre-existing row.
- Target-only rows are preserved. Source-only rows are inserted. No target row
  is updated, replaced, ignored, or deleted.
- Foreign-key checks and final verification cover the union, not only rows
  written by this invocation.

The reviewed plan records the exact baseline-acceptance policy, target tables,
key contracts, and target-fence requirement. It does not record volatile row
evidence. At execute preflight, after the target fence or external quiesce is
active and before journal genesis or any target write, the executor performs a
complete ordered source/target key anti-join. Any collision rejects execution.
The same pass computes a canonical target-only row count and table hash. The
journal genesis binds that accepted baseline, the target-fence evidence, and a
digest of both. It does not contain row values or key values.

The target fence makes the collision result durable evidence: a source key was
absent when the accepted baseline was captured, and no other writer can create
it later. The migration therefore does not need a separate row-ownership table.
Resume re-attests the same target fence and accepted baseline before it
reconciles or starts another effect. A missing or discontinuous fence cannot be
replaced with a fresh baseline because that would adopt unreviewed rows.

Prepared-chunk reconciliation classifies one interval as:

1. every expected source key is absent — retry the same intent;
2. every expected source key has exactly the reviewed converted source row —
   mark the same intent committed;
3. a mixture, a changed value, an extra matching key, or any target-fence loss
   — durable manual reconciliation.

Baseline rows can be interleaved with a prepared source interval. Reconciliation
does not classify them from row presence. It relies on continuous fence
attestation and probes only the complete expected source-key set for that
durable intent.

The target must be protected by a continuously attested write fence or an
external quiesce contract from the baseline read through final verification.
Source consistency remains independently required. Losing either boundary
stops before the next effect.

Final verification performs one bounded ordered merge of the frozen source and
target tables. Source-key matches must contain the exact reviewed source row;
target-only rows feed an independently recomputed baseline hash. The pass
proves all three statements independently:

- every reviewed baseline row remains exact;
- every source row appears once as its reviewed canonical target value;
- no other row or object appeared.

This mode does not support overwrite, upsert, last-writer-wins, timestamp
comparison, user callbacks, deletes, or target-to-source reconciliation. Each
would require a separate typed policy and expected-value contract.

## Staging swap

### Naming and ownership

The reviewed plan records a typed mapping for every replacement unit:

- live identity;
- migration-owned staging identity;
- retained backup identity.

All three identities are distinct. Staging and backup identities must be absent
at preflight. Names are rendered only from validated identifiers. A staging
identity includes the migration identity so another plan cannot adopt it.

The migration creates, copies, and verifies staging objects without modifying
the live objects. The staging catalog and rows must pass the same exact schema,
canonical data, dependency, and completeness checks as an empty-target run.
The target role must own the staging objects it creates.

Execute captures and journals a canonical live-object baseline before it
creates staging objects. Unlike warm merge, staging build does not require the
live objects to remain fenced for the complete copy. Immediately before
cutover, the executor activates the target fence or verifies the external
quiesce, then requires the complete live baseline to remain exact. Any live
write during staging build therefore aborts cutover; it is never silently
discarded or adopted into a new baseline.

### Cutover boundary

Cutover requires:

1. strict staging verification is durably complete;
2. source consistency is still valid;
3. the exact live target catalog and accepted data baseline still match the
   journal genesis;
4. a target write fence or explicit external quiesce is active;
5. the destructive approval reference is bound in the journal;
6. all required metadata locks are acquired inside a reviewed finite timeout.

PostgreSQL uses one database transaction for the complete reviewed rename set.
The transaction obtains the required locks in stable identity order, renames
live objects to retained names, renames staging objects to live names, verifies
the resulting catalog in the same transaction, and commits once. PostgreSQL
transactional DDL makes pre-commit failure rollback the rename set.

MySQL uses one multi-table `RENAME TABLE` statement and only supports this mode
when every table uses an engine with atomic DDL. MySQL acquires metadata locks
in name order, so the plan records the complete rename set and the executor
uses one stable statement. Foreign-key-related lock expansion, account grants,
views, routines, triggers, and other semantics that cannot be proven exact for
the rename set block this mode.

A metadata-lock timeout is a failed cutover attempt, not permission to continue
or to run a partial rename. The journal keeps the verified staging state and a
new attempt must re-attest every cutover prerequisite.

### Durable states and recovery

The journal uses explicit states for the cutover effect:

`StagingBuilt → StagingVerified → CutoverPrepared → CutoverCommitted →
CutoverVerified → Completed`

`CutoverPrepared` is durable before lock acquisition or rename SQL. After an
error, disconnect, restart, or process death, resume inspects all live, staging,
and retained identities:

- exact pre-cutover state — retry after all prerequisites pass again;
- exact post-cutover state — append `CutoverCommitted`, verify, then continue;
- any partial, mixed, missing, or different state — durable manual
  reconciliation.

The executor never repairs a partial cutover by dropping, renaming, or copying
objects automatically.

The old live objects remain at retained names after successful completion.
This is recovery material, not an automatic rollback guarantee. A separate,
reviewed cleanup command may remove retained objects only after its own age,
identity, dependency, ownership, and approval checks. Phase 8 completion does
not require or imply automatic cleanup.

## Verification and evidence

The journal genesis binds:

- target mode and ownership-manifest digest;
- the accepted target baseline and target-fence evidence digest when the mode
  requires them;
- live, staging, and retained identities;
- source and target endpoint/TLS/catalog bindings;
- source and target consistency or fence evidence;
- conflict policy or cutover contract;
- approval reference and plan hash.

Monitoring exposes the selected mode and durable state without identifiers that
contain customer data. Logs never include row values, key values, credentials,
connection strings, raw SQL, or unredacted endpoint identities.

Completion requires exact final catalog and row verification for the selected
mode. Row counts, affected-row counts, absence of SQL errors, or successful
rename statements are not completion evidence.

## Dialect limits

- Phase 8 is same-dialect first. Cross-dialect warm merge or staging requires a
  separate reviewed mapping for every target object and stays blocked.
- PostgreSQL and MySQL use different cutover adapters. The plan does not claim
  a generic atomic-rename primitive.
- PostgreSQL schema rename can preserve object identity, but schema-qualified
  programmable definitions, external dependencies, ownership, and ACLs still
  require exact modeling.
- MySQL table-specific grants do not automatically migrate with a renamed
  table. Any affected grant must be modeled and verified or the plan blocks.
- Partition trees, sequences or auto-increment state, generated columns,
  programmable objects, and foreign keys are admitted only when their existing
  same-dialect adapter can prove the complete staging or merge semantics.
- A retained backup is not a data rollback once applications write to the new
  live objects. Online rollback requires CDC and remains deferred.

## Phase 8 acceptance gates

All supported PostgreSQL 15–17 and MySQL 8.0/8.4 versions must pass the
applicable gates.

1. **Plan and ownership:** target mode is hash-bound; every target object is
   classified once; stale catalog, owner, ACL, authority, or name evidence
   blocks before writes.
2. **Warm conflicts:** disjoint keys merge exactly; equal and unequal key
   collisions, baseline drift, partial effects, and target-only changes stop
   without overwrite or ignore behavior.
3. **Warm recovery:** every Prepared, commit-acknowledgement, journal-sync,
   verification, cancellation, source-fence-loss, and target-fence-loss
   boundary resumes once with the exact union.
4. **Staging isolation:** staging creation and copy never alter live objects;
   hostile identifiers and name collisions fail before effects.
5. **Metadata locks:** a deliberately held conflicting lock causes the reviewed
   timeout, leaves the pre-cutover state exact, and permits a later verified
   retry. Lock acquisition order is deterministic.
6. **Cutover recovery:** faults before rename, after server effect but before
   acknowledgement, after acknowledgement but before journal publication, and
   after journal publication all classify as exact pre-state, exact post-state,
   or manual reconciliation. No mixed state completes.
7. **Retained backup:** completion preserves the exact old live objects under
   retained identities. Automatic drop and automatic rollback are absent.
8. **Destructive negatives:** wrong approval, unowned objects, unsupported
   grants/dependencies, non-atomic MySQL engines, partial pre-existing staging,
   extra target objects, and changed live data all perform no cutover.
9. **Complete verification:** missing, changed, duplicate, and extra rows or
   objects fail in the live, staging, retained, and merged inventories required
   by the selected mode.
10. **Production inertness:** fault hooks and internal destructive test APIs are
    unavailable without the dedicated fault-injection feature.

Phase 8 exits only when both warm merge and staging swap meet these gates for
their documented support subsets. A plan-schema foundation or one green rename
test is not the Phase 8 exit.
