# Planners

A planner owns two or more output columns, or coordinates a parent/child
family, choosing related values together instead of relying on independent
generators to happen to agree. The compiler tracks every claimed output —
two generators/planners can never own the same column.

Planners attach under a table's `planners:` list:

```yaml
tables:
  sessions:
    planners:
      - kind: temporal.interval
        columns:
          { start: started_at, end: ended_at, duration: duration_seconds }
        # ...
```

Three planners ship with fully worked reference examples below
(`temporal.interval`, `workflow.progress_counters`, `commerce.order_family`);
the remaining planners follow with concise recipes.

## `temporal.interval`

Coordinates a start timestamp, an end timestamp, a duration, and (optionally)
an open/running flag so they always agree with each other.

```yaml
planners:
  - kind: temporal.interval
    columns:
      start: started_at
      end: ended_at
      duration: duration_seconds
      open: is_running # optional
    start:
      kind: observed_range # observed_range | range | monotonic
      min: 2024-01-01T00:00:00Z
      max: 2026-01-01T00:00:00Z
    duration:
      kind: histogram # histogram | observed | normal | fixed | uniform
      unit: seconds
      min: 30
      max: 43200
    open_probability: 0.07
    end_inclusive: true
    timezone: preserve # preserve | utc | a named IANA zone
```

| Field                   | Default                  | Meaning                                                                                                                                              |
| ----------------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `columns.start`         | required                 | Start timestamp column.                                                                                                                              |
| `columns.end`           | required                 | End timestamp column (nullable if `open_probability > 0`).                                                                                           |
| `columns.duration`      | required                 | Duration column, in `duration.unit`.                                                                                                                 |
| `columns.open`          | optional                 | A boolean flag column marking an open (not-yet-ended) row.                                                                                           |
| `start.kind`            | `range`                  | `range`/`observed_range` (uniform between `min`/`max`) or `monotonic` (`step_seconds`, default 1).                                                   |
| `start.min`/`start.max` | required for range kinds | Parseable timestamps (RFC 3339, `%Y-%m-%d %H:%M:%S`/`T`, or a bare date).                                                                            |
| `duration.kind`         | `uniform`                | `fixed` (`value`), `normal` (`mean`, `stddev`, `min`, `max`), `histogram`/`observed` (`min`, `max` — see caveat below), or `uniform` (`min`, `max`). |
| `duration.unit`         | `seconds`                | Any of nanosecond/microsecond/millisecond/second/minute/hour/day (and common abbreviations).                                                         |
| `open_probability`      | `0.0`                    | Fraction of rows left open (null `end`). Requires a nullable `end` column if `> 0`.                                                                  |
| `end_inclusive`         | `false`                  | If `true`, requires a minimum duration of at least one unit (a zero-length inclusive interval is otherwise impossible).                              |
| `timezone`              | `preserve`               | `preserve`/`utc` renders UTC with no offset; a named IANA zone renders an explicit DST-correct offset.                                               |

Closed rows always satisfy `end = start + duration` (subject to
`end_inclusive`); open rows have a `null` end and a coherent open flag.
Verification checks the equation on closed rows, null/non-null coherence on
the open flag, and the start range.

**Caveat:** `duration.kind: histogram`/`observed` currently draws from a
bounded min-skewed placeholder shape, not real profiled bucket data — treat
it as "roughly in range," not a faithful reproduction of an observed
duration distribution. `timezone: preserve` also always renders UTC today;
no original per-row zone is retained.

## `workflow.progress_counters`

Coordinates a total/processed/succeeded/failed/pending counter family plus
an optional status column and completion timestamp, so they stay internally
consistent (`succeeded + failed = processed`, `pending = total - processed`,
and so on).

```yaml
planners:
  - kind: workflow.progress_counters
    columns:
      total: total_rows
      processed: processed_rows
      succeeded: imported_rows
      failed: failed_rows
      pending: pending_rows
      status: status
      completed_at: completed_at
    progress:
      kind: mixture # mixture | complete | in_progress | not_started | observed
      complete_weight: 0.72
      active_weight: 0.23
      not_started_weight: 0.05
    partition: exact # exact | allow_unclassified
    completed_statuses: [completed, failed]
    active_statuses: [queued, running]
```

| Field                                              | Default                                                        | Meaning                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------------- | -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `columns.total`                                    | required                                                       | Total-count column.                                                                                                                                                                                                                                                                                       |
| `columns.processed`/`succeeded`/`failed`/`pending` | optional                                                       | Counter columns; only configured ones are written.                                                                                                                                                                                                                                                        |
| `columns.status`/`completed_at`                    | optional                                                       | Status label and completion timestamp.                                                                                                                                                                                                                                                                    |
| `total.kind`                                       | `uniform`                                                      | `fixed` (`value`) or `uniform` (`min`, `max`, both ≥0).                                                                                                                                                                                                                                                   |
| `progress.kind`                                    | `mixture`                                                      | `mixture` (weighted pick of complete/in_progress/not_started), or a fixed state (`complete`/`in_progress`/`not_started`). `observed` is rejected under `partition: exact` (no profile evidence is threaded to planners yet) and falls back to a default 0.7/0.25/0.05 mixture under `allow_unclassified`. |
| `partition`                                        | `exact`                                                        | `exact`: `succeeded + failed = processed` exactly. `allow_unclassified`: adds an `unclassified_ratio` (default 0.1) so `succeeded + failed + unclassified = processed`.                                                                                                                                   |
| `success_ratio`                                    | `0.9`                                                          | Share of `processed` that succeeds (vs. fails), before partition rounding.                                                                                                                                                                                                                                |
| `completed_statuses`/`active_statuses`             | required if `status` is configured and that state is reachable | Status vocabulary for the completed/active state groups.                                                                                                                                                                                                                                                  |

Partitioning uses exact largest-remainder integer apportionment — counters
never drift from their equations due to floating-point rounding. Completed
rows require `processed = total` and a non-null `completed_at`; active rows
stay incomplete with a `null` `completed_at`.

## `commerce.order_family`

The hardest planner: a cross-table family that coordinates an order's
`subtotal`/`tax`/`discount`/`shipping`/`total` with its line items'
`quantity`/`unit_price`/`tax`/`discount`/`line_total`, using exact minor-unit
integer money arithmetic so the child sums always equal the parent totals.

```yaml
planners:
  - kind: commerce.order_family
    children: order_items
    relationship: order_items_order
    columns:
      subtotal: subtotal
      discount: discount_total # optional
      tax: tax_total # optional
      shipping: shipping_total # optional
      total: grand_total
    child_columns:
      quantity: quantity
      unit_price: unit_price
      discount: discount_amount # optional
      tax: tax_amount # optional
      line_total: line_total
    currency_scale: 2
    rounding: largest_remainder # largest_remainder | last_line | bankers
    tax:
      kind: weighted_choice
      rates: [0.0, 0.08, 0.25]
      weights: [0.05, 0.15, 0.80]
```

| Field                                               | Default                    | Meaning                                                                                                                               |
| --------------------------------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `children`                                          | required                   | Name of the child (line-item) table.                                                                                                  |
| `relationship`                                      | required                   | Name of the FK relationship declared on the child table, referencing this parent.                                                     |
| `columns.subtotal`/`columns.total`                  | required                   | Parent money columns.                                                                                                                 |
| `columns.discount`/`columns.tax`/`columns.shipping` | optional                   | Only written if configured.                                                                                                           |
| `child_columns.quantity`/`unit_price`/`line_total`  | required                   | Child columns.                                                                                                                        |
| `child_columns.discount`/`child_columns.tax`        | optional                   | Only written if configured.                                                                                                           |
| `currency_scale`                                    | required                   | Decimal scale for every money column; must match each money column's own declared scale exactly (`GEN-ORDER-FAMILY-SCALE` otherwise). |
| `rounding`                                          | required                   | `largest_remainder` (residuals distributed so child sums equal the parent exactly), `last_line`, or `bankers`.                        |
| `quantity`                                          | `{ min: 1, max: 1 }`       | Per-line quantity range.                                                                                                              |
| `unit_price`                                        | `100`–`10,000` minor units | `{ min_minor, max_minor }` or `{ min, max }` (major units, converted at `currency_scale`).                                            |
| `tax`/`discount`                                    | zero rate                  | `{ kind: fixed_rate, rate }` or a weighted `{ rates: [...], weights: [...] }` choice.                                                 |
| `shipping`                                          | `0`                        | `{ amount_minor }` or `{ amount }`.                                                                                                   |

**The line count is not configured on the planner.** It comes entirely from
the child table's own `rows.distribution` (any `kind`: `fixed`, `uniform`,
`observed`, `poisson`, `histogram`) — the planner reads that shape to decide
how many lines each order gets; declaring a line count here would be a
second, conflicting source of truth, so it isn't accepted
(`GEN-ORDER-FAMILY-UNKNOWN-FIELD` on the old flat `line_items:`/`items:`
shape from an earlier draft).

**Caveat:** `commerce.order_family` is `Verification::Unsupported` at the
descriptor level — `--verify` cannot fully audit it. It does expose sum
checks for `discount`/`tax` columns (pure per-column sums), but not for
`subtotal`/`total`/`shipping`, since those involve products and offsets
rather than a plain sum.

## Phase 3B planners

Common structural planners with a narrower surface. Each targets one
table's own columns unless noted as cross-table.

### `temporal.timestamps`

`created_at`/`updated_at` plus any number of additional named timestamps,
kept in causal order.

```yaml
planners:
  - kind: temporal.timestamps
    columns:
      created_at: created_at
      updated_at: updated_at
      archived_at: archived_at # any number of extra flat entries
    created: { kind: range, min: 2020-01-01, max: 2026-01-01 }
    update_delay: { kind: uniform, unit: hours, min: 0, max: 72 }
    other_delay: { kind: uniform, unit: days, min: 0, max: 30 } # defaults to update_delay's block
```

Extra timestamp roles (like `archived_at` above) must be **flat sibling
keys directly under `columns:`**, not nested under a sub-key — the
compiler's ownership scan only reads one flat level. `updated_at` and every
other timestamp are guaranteed no earlier than `created_at`.

### `temporal.soft_delete`

```yaml
planners:
  - kind: temporal.soft_delete
    columns: { deleted_at: deleted_at, is_deleted: is_deleted } # is_deleted optional
    deletion_probability: 0.1
    deleted_range: { kind: range, min: 2024-01-01, max: 2026-01-01 }
```

`deletion_probability` (default `0`) requires a nullable `deleted_at` if
less than `1.0`. Null/non-null coherence between `is_deleted` and
`deleted_at` is only verified when an explicit `is_deleted` column is
configured; a `deleted_at`-only model still gets range verification.

### `temporal.lifecycle`

A state machine that walks through named states, stamping a timestamp at
each reached state and leaving later states `null`.

```yaml
planners:
  - kind: temporal.lifecycle
    columns:
      status: status
      draft: created_at
      active: activated_at
      archived: archived_at
    states: [draft, active, archived]
    weights: [0.1, 0.7, 0.2]
    start: { kind: range, min: 2024-01-01, max: 2026-01-01 }
    step: { kind: uniform, unit: days, min: 1, max: 14 }
```

Each row picks a terminal state by weighted draw; every state up to and
including that terminal gets a timestamp (in order, via `step`), later
states stay `null`. A non-nullable timestamp column for a state reachable as
non-terminal is a compile error (`GEN-LIFECYCLE-NULLABILITY`). Like
`temporal.timestamps`, state-to-column mappings must be flat sibling keys
under `columns:`.

### `hierarchy.tree`

A self-referential tree over a nullable self-FK (root rows have `parent:
null`).

```yaml
planners:
  - kind: hierarchy.tree
    columns: { parent: parent_id }
    root_ratio: 0.1
    max_depth: 6
    max_branching: 4
```

`root_ratio` (default `0.1`) is the fraction of rows with no parent.
`max_depth` (default `6`) and `max_branching` (unbounded if omitted) cap tree
shape. Rows are generated in index order; each is a root if it's the first
row, wins the `root_ratio` draw, or has no eligible earlier-row parent left
within the depth/branching bounds.

### `relation.polymorphic_pair`

A `(type, id)` pair that always points at a real row of the chosen target
table — the type and the referenced key are drawn atomically together.

```yaml
planners:
  - kind: relation.polymorphic_pair
    columns: { type: commentable_type, id: commentable_id }
    targets:
      - { table: posts, weight: 0.7 }
      - { table: photos, weight: 0.3, type: photo, id_column: id }
```

Each target defaults its `type` label to the table name and its `id_column`
to that table's primary key. Targets that resolve to zero rows are silently
dropped from the weighted pick. Every target needs a dense integer key
domain. **No runtime `--verify` predicate covers `(type, id)` atomicity**
today — it's guaranteed by construction and covered by unit tests only.

### `relation.junction_pair`

Assigns each row of a junction/pivot table a distinct `(left, right)` pair,
by construction unique (a bijection over the pair space), without a separate
`unique` modifier.

```yaml
planners:
  - kind: relation.junction_pair
    columns: { left: user_id, right: role_id }
    left_relationship: user_roles_user
    right_relationship: user_roles_role
```

Both named relationships must reference dense integer keys. Requesting more
rows than `left.count * right.count` is a compile error
(`GEN-JUNCTION-EXHAUSTED`).

### `relation.tenant_family`

Keeps a child row and its referenced parent row inside the same tenant
partition, for multi-tenant schemas.

```yaml
planners:
  - kind: relation.tenant_family
    columns: { tenant: tenant_id, parent: customer_id }
    relationship: orders_customer
    num_tenants: 8
    tenant_start: 1
```

The parent table's rows are partitioned into `num_tenants` contiguous,
balanced blocks; each child row draws a tenant, then a parent row from that
tenant's block only — so a child's `tenant_id` and its referenced parent's
tenant always agree by construction.

### `geo.coordinate_pair`

A latitude/longitude pair within configurable bounds.

```yaml
planners:
  - kind: geo.coordinate_pair
    columns: { latitude: lat, longitude: lng }
    precision: 6
    bounds: { min_lat: 35.0, max_lat: 71.0, min_lon: -25.0, max_lon: 45.0 }
```

`precision` (default `6`) is decimal places; `bounds` defaults to the full
global range. **No `--verify` predicate covers the bounds** — the internal
`InRange` predicate is hard-coded to timestamp columns, so it can't
represent a decimal range. Bounds correctness is guaranteed by construction
and covered by unit tests only.

### `file.metadata`

A coherent name/extension/MIME-type/size/hash tuple for a file-metadata row.

```yaml
planners:
  - kind: file.metadata
    columns:
      {
        name: file_name,
        extension: ext,
        mime_type: mime,
        size: size_bytes,
        hash: checksum,
      }
    extensions: [pdf, jpg, png, docx]
    size: { min: 1024, max: 5000000 }
    hash_kind: sha256 # md5 | sha1 | sha256 | sha512
```

`extensions` (optional) restricts the built-in ~30-entry extension/MIME
catalog; omit it to allow the whole catalog. **Only `size`'s non-negativity
has a `--verify` predicate** — the name/extension/MIME-type textual
coherence (e.g. the name's suffix matching the extension) has no equivalent
predicate variant and is covered by unit tests only, not by `--verify`.

## See also

- [Model reference](model-reference.md) — where `planners:` sits in a table.
- [Generators](generators.md) — single/few-column generators for everything a planner doesn't own.
- [Profiling and privacy](profiling-and-privacy.md#verification) — what `--verify` can and cannot check.
