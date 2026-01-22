# Feature Ideas

This document outlines potential future features for sql-splitter. These are ideas for exploration and discussion.

---

## 1. Merge Command

**Status**: [Detailed design available](MERGE_FEATURE.md)

Combine split SQL table files back into a single dump file.

```bash
sql-splitter merge tables/ -o restored.sql
sql-splitter merge tables/ -o partial.sql --tables users,posts
sql-splitter merge tables/ -o restored.sql --auto-order  # FK dependency ordering
```

**Use cases:**

- Reconstruct full dump after editing individual tables
- Create partial dumps from subset of tables
- Reorder tables for dependency-aware imports

---

## 2. Query Command — Extract Specific Data

Filter and extract data using SQL-like syntax without loading into a database.

```bash
# Extract rows matching a condition
sql-splitter query dump.sql --table users --where "created_at > '2024-01-01'" -o recent.sql

# Extract specific columns
sql-splitter query dump.sql --table users --columns "id,email,name" -o minimal.sql

# Complex filtering
sql-splitter query dump.sql --table orders --where "total > 1000 AND status = 'completed'"
```

**Key features:**

- Stream processing (no full file load)
- Support basic WHERE conditions (=, >, <, LIKE, IN, AND, OR)
- Column projection (select subset of columns)
- Output as SQL INSERT statements or CSV

**Use cases:**

- GDPR data extraction ("give me all data for user X")
- Create development subsets from production dumps
- Extract specific records for debugging
- Data migration filtering

**Implementation complexity:** Medium-High (requires parsing INSERT VALUES)

---

## 3. Sample Command — Create Reduced Test Datasets

**Status**: [Detailed design available](SAMPLE_FEATURE.md)

Generate smaller representative datasets for development and testing.

```bash
# Sample 10% of each table
sql-splitter sample dump.sql -o dev.sql --percent 10

# Fixed row count per table
sql-splitter sample dump.sql -o dev.sql --rows 1000

# Preserve foreign key relationships
sql-splitter sample dump.sql -o dev.sql --rows 500 --preserve-relations

# Seed for reproducible samples
sql-splitter sample dump.sql -o dev.sql --percent 5 --seed 42
```

**Key features:**

- Percentage-based or fixed-count sampling
- `--preserve-relations`: Follow FK references to maintain integrity
- Deterministic sampling with `--seed`
- Stratified sampling options

**Use cases:**

- Create lightweight dev/test databases
- Generate CI test fixtures
- Reduce dump size for local development
- Create demo datasets

**Implementation complexity:** Medium (High with --preserve-relations)

---

## 4. Convert Command — Dialect Translation

Translate SQL dumps between MySQL, PostgreSQL, and SQLite formats.

```bash
# MySQL to PostgreSQL
sql-splitter convert mysql_dump.sql -o postgres.sql --from mysql --to postgres

# PostgreSQL to MySQL
sql-splitter convert pg_dump.sql -o mysql.sql --from postgres --to mysql

# SQLite to MySQL
sql-splitter convert sqlite.sql -o mysql.sql --from sqlite --to mysql

# Auto-detect source dialect
sql-splitter convert dump.sql -o converted.sql --to postgres
```

**Key features:**

- Identifier quoting conversion (backticks ↔ double quotes)
- Data type mapping (AUTO_INCREMENT ↔ SERIAL, etc.)
- Syntax transformation (LIMIT/OFFSET, boolean literals, etc.)
- INSERT syntax normalization
- COPY ↔ INSERT conversion (PostgreSQL)

**See:** [Detailed feasibility analysis](CONVERT_FEASIBILITY.md)

**Use cases:**

- Database migration between platforms
- Import third-party dumps into different database
- Standardize dump format across team

**Implementation complexity:** High (many edge cases per dialect pair)

---

## 5. Diff Command — Compare SQL Dumps

Show differences between two SQL dump files.

```bash
# Compare two dumps
sql-splitter diff old.sql new.sql

# Compare specific table
sql-splitter diff old.sql new.sql --table users

# Generate migration SQL
sql-splitter diff old.sql new.sql --output migration.sql

# Schema-only comparison
sql-splitter diff old.sql new.sql --schema-only
```

**Key features:**

- Schema diff: added/removed/modified tables, columns, indexes
- Data diff: added/removed/modified rows (by primary key)
- Generate ALTER TABLE statements for schema changes
- Generate INSERT/UPDATE/DELETE for data sync
- Summary statistics

**Output formats:**

- Human-readable diff
- SQL migration script
- JSON for programmatic use

**Use cases:**

- Review changes before applying updates
- Generate migration scripts
- Audit data changes between backups
- Validate dump consistency

**Implementation complexity:** High (requires full parsing and comparison)

---

## 6. Redact Command — Anonymize Sensitive Data

Replace personally identifiable information (PII) with fake or hashed data.

```bash
# Redact using config file
sql-splitter redact dump.sql -o safe.sql --config redact.yaml

# Redact specific columns
sql-splitter redact dump.sql -o safe.sql --columns "*.email,*.password,users.ssn"

# Redact with specific strategies
sql-splitter redact dump.sql -o safe.sql \
  --hash "users.email" \
  --fake "users.name,users.phone" \
  --null "users.ssn"
```

**Config file example (redact.yaml):**

```yaml
rules:
  - pattern: "*.email"
    strategy: fake_email
  - pattern: "*.password"
    strategy: constant
    value: "$2b$10$REDACTED"
  - pattern: "users.ssn"
    strategy: null
  - pattern: "*.phone"
    strategy: fake_phone
  - pattern: "*.name"
    strategy: fake_name
    locale: en_US
```

**Strategies:**

- `null` — Replace with NULL
- `constant` — Replace with fixed value
- `hash` — SHA256 hash (deterministic, preserves uniqueness)
- `fake_*` — Generate realistic fake data (email, name, phone, address)
- `mask` — Partial masking (e.g., `j***@example.com`)
- `shuffle` — Randomly swap values within column

**Use cases:**

- Create shareable test/demo datasets
- GDPR compliance for development environments
- Security audits and penetration testing
- Sharing data with third parties

**Implementation complexity:** Medium-High (parsing INSERT values, fake data generation)

---

## Priority Matrix

| Feature | Value  | Complexity  | Recommendation            |
| ------- | ------ | ----------- | ------------------------- |
| Merge   | High   | Low         | **Do first**              |
| Sample  | High   | Medium      | Do second                 |
| Redact  | High   | Medium-High | High value for enterprise |
| Query   | Medium | Medium-High | Nice to have              |
| Convert | Medium | High        | Complex but unique        |
| Diff    | Medium | High        | Consider later            |

---

## Contributing

Have an idea? Open an issue or discussion on GitHub!
