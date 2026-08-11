# Enterprise Migration Market Alignment Plan

> **Status: WIP course-correction document.** The
> `spike/enterprise-migration-phases-1-5` branch implements only part of the
> enterprise migration design (see
> [docs/features/enterprise/README.md](../../features/enterprise/README.md)).
> This plan records externally validated requirements the current plan series
> does not cover. The migration track is not "done" — at any phase — until the
> gaps below are either folded into the enterprise plan series and
> `docs/ROADMAP.md`, or explicitly rejected with a written reason.

**Goal:** Expand the enterprise migration roadmap and plan series to match
real market requirements observed on 2026-08-11, so subsequent implementation
phases build toward a sellable tool rather than a correctness proof.

**Trigger:** Comparison of this branch against two Laravel job postings
(2026-08-11). Laravel is hiring a human to perform exactly the workflow this
tool wants to productise — strong validation of the problem, and a concrete
external spec to check our scope against.

---

## Market signal (evidence)

1. **Database Migrations Engineer — EMEA** (posted 2026-07-16,
   <https://apply.workable.com/laravel/j/3856F5A4E3/>). Enterprise customers
   onboarding to Laravel Cloud face database migration as "a common, stubborn
   blocker." The hire runs migrations with customers directly — "primarily
   MySQL and Postgres," deliberately ~80% strength in both. Core mandate:
   "productise the repeatable parts … the most common downtime-tolerant
   migrations … with an eye towards eventually bringing that knowledge
   in-product, whether through documentation, wizards, or self-serve flows."
2. **Senior Infrastructure Engineer, PostgreSQL** (posted 2026-08-11,
   <https://apply.workable.com/laravel/j/5186450D6E/>). Laravel's first
   dedicated managed-Postgres hire. Confirms the destination side (managed
   PostgreSQL on a PaaS) is growing, i.e. a continuing stream of
   into-managed-Postgres migrations.

Managed-database vendors (PlanetScale, Neon, Supabase, Laravel Cloud) each
hand-roll import tooling; no neutral tool owns "move this database into that
managed platform, safely, with evidence." The market gap the plan series
assumes is real.

## What the market signal confirms (keep, do not re-litigate)

- **Downtime-tolerant first, CDC later.** The posting optimises for
  downtime-tolerant migrations. The offline / fenced / bounded-outage /
  empty-target boundary in the README is the right first product. No change.
- **Fail-closed capability model.** The plan report that inventories every
  object and surfaces unsupported semantics *is* the "decision logic, edge
  cases" the market wants productised. No change.
- **Evidence trail as differentiator.** Reviewed content-addressed plans,
  hash-linked journal, exact verification, drift detection, mTLS binding.
  This is the moat; incumbents (pg_dump/pgloader, AWS DMS) do not lead with
  verifiable evidence. No change.

## Gaps the plan series must absorb

Ordered by how badly they block "the tool" status, not by effort.

1. **Throughput makes the stated downtime model unrealistic at enterprise
   size.** Row-at-a-time parameterized `INSERT` (no COPY protocol, no
   parallelism) plus a full second scan of both databases for verification,
   inside a full source freeze. Outage scales as roughly three full data
   passes at one round-trip per row. This is an engineering gap, not an
   architectural one — a COPY fast path and parallel chunks fit the existing
   chunk/journal contract — but no plan document currently sequences it.
2. **Managed-source privilege reality.** The write fence needs event
   triggers, sequence ownership transfer, and `pg_terminate_backend` on all
   other sessions. RDS/Aurora/Cloud SQL restrict some or all of these, and
   that is where enterprise sources live. The fallback `consistent-snapshot`
   mode is unusable for real apps because any sequence (any `serial`/identity
   column) blocks it. No document defines a managed-source profile or a
   degraded-mode contract.
3. **MySQL is required by the market and absent from the sequencing.**
   "~80% MySQL / 80% Postgres" is the explicit market shape; the Laravel
   ecosystem skews MySQL. [12](../../features/enterprise/12-postgresql-first-adapter.md)
   says "MySQL follows" but no plan document scopes the MySQL adapter, its
   storage-engine consistency proof, or its fence equivalent.
4. **Common-object coverage: extensions and grants.** Extensions block
   execution and ACLs are silently not migrated. An enterprise Postgres
   without `pg_stat_statements`/`pgcrypto`/`uuid-ossp` is rare; a migration
   that drops all grants is not "complete" in the customer's eyes even when
   every row verifies. Neither has a plan entry beyond "unsupported."
5. **No operator surface.** Execute prints three lines. No progress, no
   `monitor` command (the journal snapshot reader already exists for this),
   no cutover/abort verbs, no human-readable plan report. The market's end
   state is "wizards, self-serve flows"; the plan series stops at CLI
   correctness.
6. **The wedge product is unnamed.** Plan-only assessment — read-only, safe,
   nearly shippable — is exactly the pre-engagement artifact the market hire
   would produce by hand (analogy: AWS SCT assessment, YugabyteDB Voyager
   `assess`). No document treats plan-only as a shippable product with its
   own acceptance gates; today it is only a stage of execute.
7. **Competitive landscape omits pgcopydb.** The closest real competitor for
   the PG→PG quadrant (COPY-based, parallel, logical-decoding `--follow`).

## Current branch state (for honesty in later reading)

- Committed: PostgreSQL plan adapter, snapshot reader, create-only DDL,
  keyset-paginated copy, append-only journal, durable write fence,
  resume/fault-injection matrices, sequences/generated columns/partitions/
  FK/index subsets, mTLS, cancellation. PG 15/16/17 matrix passes.
- Committed 2026-08-11 evening: programmable-object migration — views and
  immutable SQL scalar functions via the typed AST, catalog format v5
  (`spike: migrate PostgreSQL programmable objects safely`).
- Everything is feature-gated and labeled `EXPERIMENTAL SPIKE — NOT FOR
  PRODUCTION`. Nothing here changes that.

---

## Global Constraints

- This plan changes **documents only**. No implementation work is authorized
  by this file; each expansion below lands as plan/roadmap text with its own
  acceptance gates in the style of
  [08](../../features/enterprise/08-implementation-prerequisites.md).
- Do not weaken existing safety boundaries to close a gap. A degraded mode
  (e.g. fence-less managed source) must be an explicit, recorded, reviewable
  capability — never a silent fallback.
- Do not add zero-downtime or online-cutover claims. CDC remains deferred and
  gated as in [01](../../features/enterprise/01-migration-landscape.md).
- Keep marketing claims out of design documents, per the existing
  competitive-landscape rules.

### Task 1: Expand the enterprise plan series

**Files:**

- Modify: `docs/features/enterprise/08-implementation-prerequisites.md`
- Modify: `docs/features/enterprise/12-postgresql-first-adapter.md`
- Modify: `docs/features/enterprise/competitive-landscape.md`
- Create: `docs/features/enterprise/13-throughput-and-copy-path.md`
- Create: `docs/features/enterprise/14-managed-source-profiles.md`
- Create: `docs/features/enterprise/15-assessment-product.md`

- [x] **Step 1: Throughput plan (gap 1).** Document 13: COPY-protocol fast
      path and bounded parallel chunk workers inside the existing
      chunk/journal contract; verification cost model; measured
      rows/sec gates per PG version before the beta claim.
- [x] **Step 2: Managed-source profiles (gap 2).** Document 14: privilege
      matrix for RDS, Aurora, Cloud SQL vs. the fence's requirements; define
      either a managed-compatible fence subset or an explicit recorded
      degraded mode; fix the `consistent-snapshot`+sequence dead end or
      document it as a permanent boundary.
- [x] **Step 3: MySQL sequencing (gap 3).** Extend 08 with a scoped MySQL
      adapter phase: storage-engine consistency proof, snapshot contract,
      fence equivalent, and which existing contracts (plan, journal, verify)
      carry over unchanged.
- [x] **Step 4: Extensions and grants (gap 4).** Extend 12's boundary
      section: a vetted-extension allowlist contract and an explicit
      ACL-migration decision (migrate, report-only, or block) — silence is
      not an option.
- [x] **Step 5: Assessment product (gap 6).** Document 15: plan-only as a
      standalone read-only deliverable with its own acceptance gates, output
      report format, and no-execute safety statement.
- [x] **Step 6: Competitive landscape (gap 7).** Add pgcopydb (and its
      `--follow` mode) to `competitive-landscape.md`.

### Task 2: Represent the migration track in the product roadmap

**Files:**

- Modify: `docs/ROADMAP.md`

- [x] **Step 1:** `ROADMAP.md` currently lists only "v1.19.0: Migrate —
      Schema migration generation," which is a different feature than this
      track. Add the enterprise migration track as its own roadmap entry with
      the staged products from the README (plan-only/assessment → offline
      bulk copy beta → online/CDC as future), cross-linking
      `docs/features/enterprise/`.
- [x] **Step 2:** Mark the assessment product as the first shippable
      milestone of the track, ahead of the execute beta.

### Task 3: Operator surface plan (gap 5)

**Files:**

- Modify: `docs/features/enterprise/observability-and-operations.md`

- [x] **Step 1:** Specify the minimum operator surface for the beta:
      progress output during copy/verify, a read-only `monitor` subcommand
      over the journal snapshot reader, explicit `abort` semantics, and a
      human-readable plan/assessment report. Wizard/self-serve flows remain
      future work, but the report format should be designed so a later UI can
      consume it.

## Definition of done for this plan

All checkboxes above are landed as document changes and reviewed in the same
adversarial style as rounds 1–3 ([09](../../features/enterprise/09-round-1-review-findings.md)–[11](../../features/enterprise/11-round-3-review-findings.md)).
Only then may an implementation phase claim its scope is complete against
the real requirements, and only 08's acceptance gates decide "done" for
implementation itself.

**Review complete 2026-08-11:** round 4 ran with two independent adversarial
lenses and resolved 19 findings; ledger at
[16](../../features/enterprise/16-round-4-review-findings.md).
