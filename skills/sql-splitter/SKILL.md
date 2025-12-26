---
name: sql-splitter
description: >
  High-performance CLI for working with SQL dump files: split/merge by table,
  analyze contents, validate integrity, convert between MySQL/PostgreSQL/SQLite,
  create FK-safe samples, shard multi-tenant dumps, generate ERD diagrams, and
  reorder for safe imports. Use when working with .sql dump files for migrations,
  dev seeding, CI validation, schema visualization, or data extraction.
license: MIT
compatibility: Requires sql-splitter binary installed (cargo install sql-splitter)
---

# sql-splitter Skill

This skill helps you use `sql-splitter` to manipulate SQL dump files safely and efficiently.

## When to Use This Skill

Use `sql-splitter` when:
- The user mentions **SQL dump files** (`.sql`, `.sql.gz`, `.sql.bz2`, `.sql.xz`, `.sql.zst`)
- The user wants to migrate, restore, or work with database dump files
- The user needs to validate, analyze, split, merge, convert, sample, or shard dumps
- Working with **MySQL, PostgreSQL, or SQLite** dump formats

## When NOT to Use This Skill

Do **not** use `sql-splitter` when:
- Running ad-hoc SQL queries against a live database (use `psql`/`mysql` directly)
- No dump file exists; only a running database is available
- The user needs interactive data editing rather than dump manipulation
- Working with dialects beyond MySQL/PostgreSQL/SQLite

---

## Command Reference

### split
Split a dump into per-table files.

```bash
sql-splitter split dump.sql --output tables/ --progress
sql-splitter split dump.sql --tables users,orders --output tables/
sql-splitter split dump.sql --schema-only --output schema/
sql-splitter split dump.sql --data-only --output data/
```

### merge
Merge per-table files back into a single dump.

```bash
sql-splitter merge tables/ --output restored.sql
sql-splitter merge tables/ --output restored.sql --transaction
sql-splitter merge tables/ --exclude logs,cache --output restored.sql
```

### analyze
Get statistics about a dump (read-only).

```bash
sql-splitter analyze dump.sql --progress
sql-splitter analyze "dumps/*.sql" --fail-fast
```

### convert
Convert between MySQL, PostgreSQL, and SQLite.

```bash
sql-splitter convert mysql.sql --to postgres --output pg.sql
sql-splitter convert pg_dump.sql --to mysql --output mysql.sql
sql-splitter convert dump.sql --from postgres --to sqlite --output sqlite.sql
sql-splitter convert mysql.sql --to postgres --output - | psql "$PG_CONN"
```

### validate
Check dump integrity (syntax, encoding, PK/FK).

```bash
sql-splitter validate dump.sql --strict --progress
sql-splitter validate "dumps/*.sql" --json --fail-fast
sql-splitter validate dump.sql --no-fk-checks --progress
```

### sample
Create reduced datasets with FK preservation.

```bash
sql-splitter sample dump.sql --output sampled.sql --percent 10 --preserve-relations
sql-splitter sample dump.sql --output sampled.sql --rows 1000 --preserve-relations
sql-splitter sample dump.sql --output sampled.sql --percent 10 --tables users,orders
sql-splitter sample dump.sql --output sampled.sql --percent 10 --seed 42
```

### shard
Extract tenant-specific data.

```bash
sql-splitter shard dump.sql --tenant-value 123 --tenant-column tenant_id --output tenant.sql
sql-splitter shard dump.sql --tenant-values "1,2,3" --tenant-column account_id --output shards/
```

### diff
Compare two SQL dumps for schema and data changes.

```bash
sql-splitter diff old.sql new.sql --progress
sql-splitter diff old.sql new.sql --schema-only
sql-splitter diff old.sql new.sql --data-only
sql-splitter diff old.sql new.sql --format json --output diff.json
sql-splitter diff old.sql new.sql --format sql --output migration.sql
sql-splitter diff old.sql new.sql --tables users,orders --progress
sql-splitter diff old.sql new.sql --verbose                           # Show sample PKs
sql-splitter diff old.sql new.sql --ignore-columns "*.updated_at"     # Ignore columns
sql-splitter diff old.sql new.sql --primary-key logs:ts+msg           # Override PK
sql-splitter diff old.sql new.sql --allow-no-pk                       # Tables without PK
```

### redact
Anonymize PII in SQL dumps by replacing sensitive data with fake, hashed, or null values.

```bash
# Using YAML config file
sql-splitter redact dump.sql --output safe.sql --config redact.yaml

# Using CLI flags
sql-splitter redact dump.sql --output safe.sql --null "*.ssn" --hash "*.email" --fake "*.name"

# Mask credit cards (keep last 4 digits)
sql-splitter redact dump.sql --output safe.sql --mask "****-****-****-XXXX=*.credit_card"

# Generate config by analyzing input file
sql-splitter redact dump.sql --generate-config --output redact.yaml

# Reproducible with seed
sql-splitter redact dump.sql --output safe.sql --config redact.yaml --seed 42

# Validate config only
sql-splitter redact dump.sql --config redact.yaml --validate

# With specific locale for fake data
sql-splitter redact dump.sql --output safe.sql --fake "*.name" --locale de_de
```

**Strategies:**
- `--null "pattern"`: Replace with NULL
- `--hash "pattern"`: SHA256 hash (deterministic, preserves FK integrity)
- `--fake "pattern"`: Generate realistic fake data
- `--mask "pattern=column"`: Partial masking
- `--constant "column=value"`: Fixed value replacement

**Fake generators:** email, name, first_name, last_name, phone, address, city, zip, company, ip, uuid, date, credit_card, ssn, lorem, and more.

### graph
Generate Entity-Relationship Diagrams (ERD) from SQL dumps.

```bash
# Interactive HTML ERD with dark/light mode and panzoom
sql-splitter graph dump.sql --output schema.html

# Graphviz DOT format with ERD-style tables
sql-splitter graph dump.sql --output schema.dot

# Mermaid erDiagram syntax (paste into GitHub/GitLab)
sql-splitter graph dump.sql --output schema.mmd --format mermaid

# JSON with full schema details
sql-splitter graph dump.sql --json

# Filter tables
sql-splitter graph dump.sql --tables "user*,order*" --exclude "log*"

# Show only circular dependencies
sql-splitter graph dump.sql --cycles-only

# Focus on specific table and its dependencies
sql-splitter graph dump.sql --table orders --transitive

# Show tables that depend on users
sql-splitter graph dump.sql --table users --reverse
```

### order
Reorder SQL dump in topological FK order for safe imports.

```bash
# Rewrite in safe import order
sql-splitter order dump.sql --output ordered.sql

# Check for cycles without rewriting
sql-splitter order dump.sql --check

# Reverse order (for DROP operations)
sql-splitter order dump.sql --reverse --output drop_order.sql
```

---

## Step-by-Step Patterns

### Pattern 1: Validate Before Use

Before using any dump from an external source:

1. **Validate integrity**
   ```bash
   sql-splitter validate path/to/dump.sql.gz --strict --progress
   ```

2. **If validation fails**, check:
   - Incorrect dialect? Try `--dialect=postgres` or `--dialect=mysql`
   - Encoding issues? Report specific errors to user
   - Truncated file? Check file size and completeness

3. **Analyze structure**
   ```bash
   sql-splitter analyze path/to/dump.sql.gz --progress
   ```

### Pattern 2: Database Migration

For migrating MySQL to PostgreSQL (or vice versa):

1. **Validate source**
   ```bash
   sql-splitter validate source.sql.gz --strict --progress
   ```

2. **Convert dialect**
   ```bash
   sql-splitter convert source.sql.gz --to postgres --output target.sql --strict
   ```

3. **Validate converted output**
   ```bash
   sql-splitter validate target.sql --dialect=postgres --strict
   ```

4. **Or stream directly**
   ```bash
   sql-splitter convert source.sql.gz --to postgres --output - | psql "$PG_CONN"
   ```

### Pattern 3: Create Dev Dataset

For creating smaller realistic data for development:

1. **Analyze to understand sizes**
   ```bash
   sql-splitter analyze prod.sql.zst --progress
   ```

2. **Sample with FK preservation**
   ```bash
   sql-splitter sample prod.sql.zst \
     --output dev_seed.sql \
     --percent 10 \
     --preserve-relations \
     --progress
   ```

3. **Restore to dev database**
   ```bash
   psql "$DEV_DB" < dev_seed.sql
   ```

### Pattern 4: CI Validation Gate

For validating dumps in CI pipelines:

```bash
sql-splitter validate "dumps/*.sql.gz" --json --fail-fast --strict
```

Parse with jq:
```bash
sql-splitter validate "dumps/*.sql.gz" --json --fail-fast \
  | jq '.results[] | select(.passed == false)'
```

### Pattern 5: Per-Table Editing

When the user needs to edit specific tables:

1. **Split**
   ```bash
   sql-splitter split dump.sql --output tables/ --progress
   ```

2. **Edit** the per-table files (`tables/users.sql`, etc.)

3. **Merge back**
   ```bash
   sql-splitter merge tables/ --output updated.sql --transaction
   ```

### Pattern 6: Tenant Extraction

For multi-tenant databases:

1. **Identify tenant column** (often `tenant_id`, `account_id`, `company_id`)

2. **Extract tenant data**
   ```bash
   sql-splitter shard dump.sql \
     --tenant-value 12345 \
     --tenant-column tenant_id \
     --output tenant_12345.sql \
     --progress
   ```

### Pattern 7: Comparing Dumps for Changes

For detecting schema or data changes between two versions:

1. **Full comparison (schema + data)**
   ```bash
   sql-splitter diff old_dump.sql new_dump.sql --progress
   ```

2. **Schema-only comparison** (fast, no data parsing)
   ```bash
   sql-splitter diff old.sql new.sql --schema-only
   ```

3. **Generate migration script**
   ```bash
   sql-splitter diff old.sql new.sql --format sql --output migration.sql
   ```

4. **JSON output for automation**
   ```bash
   sql-splitter diff old.sql new.sql --format json | jq '.summary'
   ```

### Pattern 8: Data Anonymization

For creating safe development/testing datasets:

1. **Generate redaction config by analyzing dump**
   ```bash
   sql-splitter redact dump.sql --generate-config --output redact.yaml
   ```

2. **Review and customize** the generated config

3. **Apply redaction**
   ```bash
   sql-splitter redact dump.sql --output safe.sql --config redact.yaml --progress
   ```

4. **Or use inline patterns for quick redaction**
   ```bash
   sql-splitter redact dump.sql --output safe.sql \
     --null "*.ssn,*.tax_id" \
     --hash "*.email" \
     --fake "*.name,*.phone"
   ```

5. **Validate the redacted output**
   ```bash
   sql-splitter validate safe.sql --strict
   ```

### Pattern 9: Schema Visualization

For understanding complex database schemas:

1. **Generate interactive ERD**
   ```bash
   sql-splitter graph dump.sql --output schema.html
   # Opens in browser with dark/light mode, zoom/pan
   ```

2. **For documentation (Mermaid)**
   ```bash
   sql-splitter graph dump.sql --output docs/schema.mmd --format mermaid
   # Paste into GitHub/GitLab/Notion
   ```

3. **Focus on specific area**
   ```bash
   # What does orders depend on?
   sql-splitter graph dump.sql --table orders --transitive --output orders.html
   
   # What depends on users?
   sql-splitter graph dump.sql --table users --reverse --output users_deps.html
   ```

4. **Find circular dependencies**
   ```bash
   sql-splitter graph dump.sql --cycles-only
   ```

### Pattern 10: Safe Import Order

For ensuring FK constraints don't fail during restore:

1. **Check for cycles**
   ```bash
   sql-splitter order dump.sql --check
   ```

2. **Reorder if needed**
   ```bash
   sql-splitter order dump.sql --output ordered.sql
   ```

3. **For DROP operations (reverse order)**
   ```bash
   sql-splitter order dump.sql --reverse --output drop_order.sql
   ```

---

## Common Flag Combinations

| Goal | Flags |
|------|-------|
| CI validation | `--strict --fail-fast --json` |
| Safe exploration | `--dry-run --progress` |
| Reproducible sampling | `--seed 42 --preserve-relations` |
| Fast progress feedback | `--progress` |
| Compressed output | Pipe to `gzip -c` or `zstd -c` |

---

## Error Handling

### Dialect Detection Issues
If auto-detection fails, specify explicitly:
```bash
sql-splitter validate dump.sql --dialect=postgres
sql-splitter convert dump.sql --from mysql --to postgres --output out.sql
```

### Validation Failures
- Parse `--json` output for specific errors
- Check for encoding issues, missing FKs, duplicate PKs
- Use `--no-fk-checks` to skip expensive integrity checks

### Large Files
- sql-splitter uses constant ~50MB memory
- Downstream tools may be bottlenecks
- Test with `sample` before full operations

---

## Implementation Checklist

When using this skill:

1. **Detect applicability**: Check for `.sql` files or dump-related tasks
2. **Clarify intent**: Validation? Conversion? Sampling? Splitting?
3. **Choose pattern**: Map goal to one of the patterns above
4. **Propose plan**: Explain steps before executing
5. **Use safe flags**: `--dry-run` first, then `--progress` for feedback
6. **Summarize results**: Report success/failure with key stats
