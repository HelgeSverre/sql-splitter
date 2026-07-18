# Generators and modifiers

A `generator` produces a column's base value. `modifiers` form an ordered
pipeline applied to that value afterward (`null_rate`, `unique`, and so on).
Every kind below is a `kind:` string in a `generator:` or `modifiers[]`
entry — see [Column rules](model-reference.md#column-generatormodifier-rules-columns)
for the attachment shape.

Each generator/modifier declares which `family` values it `accepts` (see
[Schema representation](model-reference.md#column-rules-schemacolumns) for
the family list); attaching one to an incompatible column is a compile error
(`GEN-GENERATOR-TYPE`/`GEN-MODIFIER-TYPE`).

## Core generators

| Kind               | Fields (default)                                                  | Accepts              | Notes                                                                                         |
|--------------------|-------------------------------------------------------------------|----------------------|-----------------------------------------------------------------------------------------------|
| `constant`         | `value` (optional, default `NULL`)                                | any                  | Minimal YAML→value coercion.                                                                  |
| `null`             | none                                                              | any                  | Errors `GEN-NULL-ON-NON-NULLABLE` if the column isn't nullable.                               |
| `sequence`         | `start` (0), `step` (1)                                           | integer, big_integer | `GEN-SEQUENCE-ZERO-STEP` if `step: 0`. Usable as a dense FK/PK key domain.                    |
| `copy`             | `source` (required, sibling column name)                          | any                  | Copies another column's value; errors on missing source or a family mismatch.                 |
| `template`         | `parts` (required list of literal strings or `{ field: <name> }`) | text                 | Joins literal fragments and referenced sibling fields — no expression evaluation.             |
| `pattern`          | `mask` (required string)                                          | text                 | `#`=digit, `?`=upper, `@`=lower, `*`=alphanumeric, everything else literal.                   |
| `database_default` | none                                                              | any                  | Always emits the column's `DEFAULT`; the column is omitted from the rendered `INSERT`/`COPY`. |
| `json_value`       | `value` (optional, default `{}`)                                  | json                 | Errors `GEN-JSON-VALUE-INVALID` on an unparseable value.                                      |
| `integer`          | `min` (0), `max` (1000)                                           | integer, big_integer |                                                                                               |
| `decimal`          | `min` (0), `max` (1000), `scale` (2, 0–18)                        | decimal              |                                                                                               |
| `boolean`          | `probability` (0.5, in `[0,1]`)                                   | boolean              |                                                                                               |
| `string`           | `min_length` (8), `max_length` (= `min_length`)                   | text                 | Alphanumeric characters only.                                                                 |
| `bytes`            | `min_length` (16), `max_length` (= `min_length`)                  | bytes                |                                                                                               |
| `uuid`             | none                                                              | uuid, text           | RFC 4122 v4. Usable as a dense FK/PK key domain.                                              |
| `choice`           | `values` (required, non-empty)                                    | any                  | Uniform pick, family-coerced.                                                                 |
| `weighted_choice`  | `choices` (required, non-empty list of `{ value, weight }`)       | any                  | Weighted pick.                                                                                |

## Semantic generators

Fixed-shape, no-argument text generators drawn from a fake-data catalog
(locale is always English):

`person.first_name`, `person.last_name`, `person.full_name`,
`person.username`, `person.title`, `internet.email`, `internet.domain`,
`internet.url`, `internet.ipv4`, `internet.ipv6`, `internet.user_agent`,
`phone.number`, `phone.country_code`, `company.name`, `company.department`,
`company.job_title`, `address.line1`, `address.line2`, `address.city`,
`address.region`, `address.postcode`, `address.country`, `commerce.currency`,
`text.word`, `text.sentence`, `text.paragraph`, `text.slug`, `file.name`,
`file.extension`, `file.mime_type`, `network.mac`.

Configurable semantic generators:

| Kind                    | Fields (default)                                              | Accepts              | Notes                                                              |
|-------------------------|---------------------------------------------------------------|----------------------|--------------------------------------------------------------------|
| `address.latitude`      | none                                                          | decimal, text        | Uniform `[-90, 90]` at scale 6.                                    |
| `address.longitude`     | none                                                          | decimal, text        | Uniform `[-180, 180]` at scale 6.                                  |
| `commerce.product_name` | none                                                          | text                 | Two capitalized random words.                                      |
| `commerce.sku`          | none                                                          | text                 | `AAA-###-####` shape.                                              |
| `commerce.money`        | `min` (0), `max` (1000), `scale` (2, 0–18)                    | decimal, text        |                                                                    |
| `commerce.quantity`     | `min` (1), `max` (100)                                        | integer, big_integer |                                                                    |
| `file.size`             | `min` (1), `max` (10,000,000)                                 | integer, big_integer | Bytes.                                                             |
| `network.port`          | `min` (1), `max` (65535)                                      | integer, big_integer |                                                                    |
| `duration`              | `min` (0), `max` (86400)                                      | integer, big_integer | Seconds.                                                           |
| `identifier.token`      | `length` (32), `alphabet` (`alphanumeric`), `prefix` (`""`)   | text                 | `alphabet`: `alphanumeric \| hex \| numeric \| alpha \| url_safe`. |
| `identifier.nanoid`     | `length` (21), `alphabet` (`url_safe`), `prefix` (`""`)       | text                 |                                                                    |
| `identifier.ulid`       | none                                                          | text                 | 26-character Crockford base32.                                     |
| `identifier.hash`       | `length` (64)                                                 | text                 | Lowercase hex.                                                     |
| `date`                  | none                                                          | date_time, text      | Uniform over `1970-01-01`–`2035-12-31`.                            |
| `time`                  | none                                                          | date_time, text      | Uniform over `00:00:00`–`23:59:59`; bounds are not configurable.   |
| `datetime`              | none                                                          | date_time, text      | Same bounds as `date`.                                             |
| `before`                | `source` (required), `min_seconds` (1), `max_seconds` (86400) | date_time, text      | Reads a sibling column and subtracts a random offset in range.     |
| `after`                 | same as `before`                                              | date_time, text      | Adds the offset instead of subtracting.                            |

## Credential generators

High-confidence credential-shaped columns (password/token/API-key/secret
names) never infer a value derived from the source dump — see
[Profiling and privacy](profiling-and-privacy.md#credentials) for the
inference-side guarantee. These are the synthetic-only generators inference
picks, and are also available for explicit use:

| Kind                       | Fields (default)                                               | Accepts | Output shape                                                                                             |
|----------------------------|----------------------------------------------------------------|---------|----------------------------------------------------------------------------------------------------------|
| `credential.token`         | `length` (32), `alphabet` (`alphanumeric`), `prefix` (`""`)    | text    | Random string.                                                                                           |
| `credential.api_key`       | `length` (32), `alphabet` (`alphanumeric`), `prefix` (`"sk_"`) | text    | `sk_<32 chars>`.                                                                                         |
| `credential.secret`        | `length` (48), `alphabet` (`alphanumeric`), `prefix` (`""`)    | text    | Random string.                                                                                           |
| `credential.password_hash` | none                                                           | text    | `$synthetic$<64 hex chars>` — syntactically hash-shaped but never a real hash.                           |
| `credential.placeholder`   | none                                                           | text    | Fixed string `SYNTHETIC_PLACEHOLDER_NOT_A_REAL_CREDENTIAL` — deliberately not a parseable PEM/key shape. |

## Observed and statistical generators

These reproduce a _shape_ profiled from a source dump, not necessarily the
exact original values (except `observed_sample`, which does replay the
sampled literal values themselves — see the privacy notes linked above).

| Kind              | Fields                                                                             | Accepts                       | Notes                                                                            |
|-------------------|------------------------------------------------------------------------------------|-------------------------------|----------------------------------------------------------------------------------|
| `observed_sample` | `values` (required, non-empty; `{ value, weight }` or bare values)                 | any                           | Weighted replay of literal source values. Always flagged by `GEN-SOURCE-VALUES`. |
| `histogram`       | `bins` (required, sorted, non-overlapping `{ min, max, count }`), `scale` (0)      | integer, big_integer, decimal | Samples a bin by frequency, then uniformly within the bin.                       |
| `normal`          | `mean` (required), `std` (required, ≥0), `min`/`max` (optional clamp), `scale` (0) | integer, big_integer, decimal | Box–Muller Gaussian draw.                                                        |
| `lognormal`       | `mu` (required), `sigma` (required, ≥0), `min`/`max`, `scale` (0)                  | integer, big_integer, decimal | `exp(Normal(mu, sigma))`.                                                        |
| `monotonic`       | `start` (0), `step` (1, ≥0)                                                        | integer, big_integer          | `start + row_index * step` — row-indexed and reproducible, not shuffled.         |

## Relationship generators

| Kind                     | Fields                                                                                                                                     | Accepts                                 | Notes                                                                                                                        |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `relation.foreign_key`   | `relationship` (optional), `distribution` (optional: `uniform \| sequential \| weighted \| observed`), `null_rate` (optional, `0.0`–`1.0`) | integer, big_integer, text, uuid, other | A compiled marker, not an executable generator on its own — the engine assigns the value from the parent table's key domain. |
| `relation.composite_key` | same fields                                                                                                                                | same                                    | Multi-column reference, same mechanism.                                                                                      |

Only **dense integer key domains** are supported as FK targets today: a bare
integer primary key, a `sequence` generator, or `uuid`. Referencing any other
generator kind as a parent key raises `GEN-KEY-DOMAIN-UNSUPPORTED`.
`distribution: observed` currently uses a deterministic fixed root rather
than drawing fresh per-run entropy (undocumented upstream, tracked as a
known gap — don't rely on `observed` FK distribution for run-to-run
variation).

## Modifiers

Applied in list order, after the generator produces a base value. All are
`ColumnScope::OwnColumn` (no cross-column reads).

| Kind        | Fields (default)                                                                                            | Accepts                       | Behavior                                                                                                                                                                                                                                                                                                                                                            |
|-------------|-------------------------------------------------------------------------------------------------------------|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `null_rate` | `rate` (required, `0`–`1`)                                                                                  | any                           | Replaces the value with `NULL` at probability `rate`. Errors on a non-nullable column.                                                                                                                                                                                                                                                                              |
| `unique`    | `max_attempts` (10), `on_exhaustion` (`error \| warn \| widen`, default `error`), `max_tracked` (1,000,000) | any                           | Tracks seen values up to `max_tracked`; on collision, mutates the candidate (append a suffix for text, add an offset for numeric types, and so on). `widen` retries with a 10× attempt budget before falling back to `warn`'s behavior (accept the duplicate); families without a defined mutation (e.g. `boolean`) can't `widen` (`GEN-UNIQUE-WIDEN-UNSUPPORTED`). |
| `prefix`    | `value` (required), `max_length` (optional, truncates after prepending)                                     | text                          |                                                                                                                                                                                                                                                                                                                                                                     |
| `suffix`    | `value` (required), `max_length` (optional, truncates after appending)                                      | text                          |                                                                                                                                                                                                                                                                                                                                                                     |
| `truncate`  | `max_length` (required)                                                                                     | text                          | Keeps the first N Unicode scalars.                                                                                                                                                                                                                                                                                                                                  |
| `case`      | `mode` (required: `upper \| lower \| title`)                                                                | text                          |                                                                                                                                                                                                                                                                                                                                                                     |
| `clamp`     | `min` (required), `max` (required)                                                                          | integer, big_integer, decimal |                                                                                                                                                                                                                                                                                                                                                                     |
| `round`     | `scale` (required)                                                                                          | decimal                       | Half-up rounding to fewer places; a no-op if the target scale is ≥ the current one.                                                                                                                                                                                                                                                                                 |
| `format`    | `template` (required, containing a literal `{value}`)                                                       | text                          | Substitutes the current value into the template.                                                                                                                                                                                                                                                                                                                    |

There is no `mask` modifier in the `generate` catalog — value masking
(`****-****-****-XXXX`) is a `redact` concept, not part of this pipeline.

You rarely need to add `unique` by hand for a key: the compiler auto-attaches it
(with `on_exhaustion: error`) to any single-column primary key or single-column
`UNIQUE` column whose generator is not already inherently unique, so keys are
distinct by construction. `sequence`/`monotonic` and `uuid` are left alone, and
composite keys are not auto-enforced. See
[Key uniqueness by construction](model-reference.md#key-uniqueness-by-construction).

## `constant`/`weighted_choice`/`observed_sample` and source literals

`constant`, `weighted_choice`, and `observed_sample` are the generator kinds
that can persist a literal value drawn or derived from a source dump or
hand-authored into the model. Using any of them (or a `database_default`
deferring to a source-observed `DEFAULT`, or a verbatim `CHECK` constraint
literal) triggers the `GEN-SOURCE-VALUES` notice — see
[Profiling and privacy](profiling-and-privacy.md) for the full explanation
and how to build a literal-free model.

## See also

- [Model reference](model-reference.md) — the surrounding YAML shape (`columns:`, `modifiers:`, families).
- [Planners](planners.md) — multi-column generators that coordinate two or more outputs together.
- [Diagnostics](diagnostics.md) — every `GEN-*` code referenced above.
