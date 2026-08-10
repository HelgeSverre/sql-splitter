# Enum Conversion

**Status:** Implemented for the `convert` command

The converter preserves enum members between MySQL/MariaDB and PostgreSQL when
the input order contains enough type information. SQLite and MSSQL do not have
an equivalent enum type, so those targets use string types and report a lossy
conversion.

This file records implementation constraints. The maintained command and type
mapping documentation is under `website/src/content/docs/`.

## Supported conversions

| Source     | Target     | Result                                                       |
| ---------- | ---------- | ------------------------------------------------------------ |
| MySQL      | PostgreSQL | Inline `ENUM(...)` becomes a named PostgreSQL enum type      |
| PostgreSQL | MySQL      | A registered enum type becomes inline `ENUM(...)`            |
| MySQL      | SQLite     | Existing type mapping narrows `ENUM(...)` to `TEXT`          |
| MySQL      | MSSQL      | Existing type mapping narrows `ENUM(...)` to `NVARCHAR(255)` |
| PostgreSQL | SQLite     | A registered enum reference narrows to `TEXT`                |
| PostgreSQL | MSSQL      | A registered enum reference narrows to `NVARCHAR(255)`       |

The public `schema::ColumnType` API continues to classify enum columns as
`Text`. Adding a variant to this exhaustive public Rust enum would break source
compatibility in the 1.x series. First-class enum storage in the shared schema
model and enum-specific synthetic generators are deferred to a major release.

## MySQL to PostgreSQL

For this input:

```sql
CREATE TABLE orders (
  status ENUM('pending', 'shipped') NOT NULL
);
```

the converter emits a type definition before the table and replaces the column
type:

```sql
CREATE TYPE enum__orders__status AS ENUM ('pending', 'shipped');

CREATE TABLE orders (
  status enum__orders__status NOT NULL
);
```

`--enum-naming per-column` is the default. It allocates a distinct type for
each source table and column. `--enum-naming dedupe` reuses a generated type
when the complete ordered label list is identical.

Generated names are sanitized, limited to PostgreSQL's 63-byte identifier
limit, and assigned deterministic numeric suffixes after collisions.

MySQL and PostgreSQL do not use the same backslash escape rules. Labels are
decoded with MySQL rules before they are quoted for PostgreSQL. A MySQL enum
label containing NUL cannot be represented as PostgreSQL text. The converter
therefore skips the complete DDL statement with a warning, or returns an error
in strict mode, before it changes the enum registry.

## PostgreSQL to MySQL

For this input:

```sql
CREATE TYPE order_status AS ENUM ('pending', 'shipped');
CREATE TABLE orders (status order_status NOT NULL);
```

the `CREATE TYPE` statement populates the streaming registry and is omitted
from MySQL output. The table becomes:

```sql
CREATE TABLE orders (status ENUM('pending','shipped') NOT NULL);
```

The enum parser accepts quoted and schema-qualified identifiers. It accepts
ordinary strings, `E'...'` escape strings, `U&'...' [UESCAPE 'x']` Unicode
strings, and dollar-quoted strings. Unqualified references are converted only
when they identify one registered enum. Ambiguous names produce a warning and
are not guessed.

PostgreSQL enum arrays have no equivalent MySQL enum type. They become `JSON`
with a lossy-conversion warning. The same references become `TEXT` for SQLite
and `NVARCHAR(255)` for MSSQL.

## Streaming order

The converter processes one statement at a time and does not retain an entire
dump. PostgreSQL enum declarations must therefore appear before tables that use
them for semantic-preserving conversion.

An unknown PostgreSQL type reference is preserved because it might be a valid
non-enum user-defined type. If a later `CREATE TYPE ... AS ENUM` proves that an
earlier reference was an enum, the converter reports the out-of-order use.
Strict mode rejects the late definition.

`ALTER TYPE ... ADD VALUE` and supported enum renames update the registry for
later tables. If a table using the type was already emitted, the converter
cannot revise that output and reports a warning. Strict mode rejects this known
schema divergence.

## ALTER TABLE restrictions

The converter recognizes enum type references in `CREATE TABLE` and supported
single-action `ALTER TABLE` forms. It rewrites MySQL `MODIFY COLUMN` to
PostgreSQL `ALTER COLUMN ... TYPE` and the reverse PostgreSQL form to MySQL
`MODIFY COLUMN`.

Some source forms cannot be translated without losing other column or table
operations. The converter rejects or skips the complete statement before enum
registry mutation when it sees:

- `CHANGE COLUMN` with an enum, because it combines rename and type changes;
- MariaDB `MODIFY [COLUMN] IF EXISTS` with an enum;
- enum changes with column attributes that cannot be reconstructed;
- more than one top-level `ALTER TABLE` action.

PostgreSQL `USING` expressions are removed only for a recognized enum type
alteration. Comments, multiline whitespace, quoted qualified identifiers,
`ALTER TABLE IF EXISTS`, and PostgreSQL `ONLY` are handled by the DDL tokenizer
and the scoped grammar rewrite.

## Implementation

The implementation is deliberately limited to DDL type positions. It does not
replace matching words in comments, string literals, defaults, constraints, or
unrelated SQL.

- `src/convert/ddl.rs` tokenizes relevant DDL and returns positioned type
  references and decoded inline enum labels.
- `src/convert/enum_parser.rs` parses PostgreSQL enum definitions, alterations,
  renames, and type identities.
- `src/convert/enum_registry.rs` stores ordered labels, generated names, and
  emitted definitions.
- `src/convert/mod.rs` owns streaming state, target rewrites, warnings, and
  strict-mode behavior.

Derived PostgreSQL enum name and inline-type indexes are cached by registry
generation. The registry rebuilds these indexes only after a type definition or
rename changes the registered state.

## Verification

Focused behavior and regression tests are in:

- `src/convert/ddl.rs`
- `src/convert/enum_parser.rs`
- `src/convert/enum_registry.rs`
- `tests/convert_unit_test.rs`
- `tests/convert_integration_test.rs`

Run:

```bash
cargo nextest run --no-default-features \
  --test convert_unit_test \
  --test convert_integration_test
cargo test --no-default-features --doc
cargo clippy --no-default-features --lib -- -D warnings
cargo bench --no-default-features --bench convert_bench -- enum_conversion
```

## Future work

- Add first-class enum members to the shared schema model in a major release.
- Add enum-specific synthetic-data generators after the schema model can carry
  member lists.
- Consider bounded statement deferral for out-of-order enum definitions if it
  can preserve the converter's streaming memory guarantee.
