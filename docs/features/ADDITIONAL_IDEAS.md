# Additional Feature Ideas

**Date**: 2025-12-20  
**Updated**: 2025-12-21

Future feature ideas beyond the v2.x roadmap (Query, Redact, Diff, Validate, MSSQL).

---

## 1. Validate/Lint (`validate`)

Check dump files for structural integrity and common issues. _Planned for v2.2.0._

```bash
sql-splitter validate dump.sql
sql-splitter validate dump.sql --check-fk
sql-splitter validate dump.sql --format json
```

**Checks:** Syntax errors, DDL/DML consistency, duplicate PKs, FK integrity, encoding issues.

**Effort:** M-L (2-4 days)

---

## 2. Auto-Detect PII (`detect-pii`)

Scan schema and sample rows to suggest a redaction configuration. _Planned for v2.2.0._

```bash
sql-splitter detect-pii dump.sql -o redact-config.yaml
sql-splitter detect-pii dump.sql --preview
```

**Detection:** Column names (email, phone, ssn), data patterns (regex), statistical uniqueness.

**Effort:** S-M (1-2 days)

---

## 3. Canonicalize + Checksum

Normalize SQL formatting and compute checksums for CI verification.

```bash
sql-splitter canonicalize dump.sql -o normalized.sql --checksum
sql-splitter verify dump.sql --checksums checksums.json
```

**Use cases:** CI verification, artifact deduplication, integrity checks.

**Effort:** M (2-3 days)

---

## 4. Key Range Partitioning

Extend shard with date/time range partitioning.

```bash
sql-splitter shard dump.sql -o ranges/ \
  --key created_at \
  --ranges "2023-01-01,2024-01-01,2025-01-01"
```

**Use cases:** Archive old data, time-based retention, partial restores.

**Effort:** M (2-3 days, builds on shard)

---

## 5. Streaming Output Compression

Output directly to compressed formats.

```bash
sql-splitter split dump.sql -o tables/ --compress gzip
sql-splitter sample dump.sql -o sample.sql.zst --compress zstd
```

**Formats:** gzip, zstd, bzip2, xz

**Effort:** S (1 day)

---

## Priority Matrix

| Feature                | Value  | Complexity | Target |
| ---------------------- | ------ | ---------- | ------ |
| Validate               | High   | Medium     | v2.2.0 |
| Detect-PII             | High   | Low-Medium | v2.2.0 |
| Canonicalize           | Medium | Medium     | Future |
| Key Range Partitioning | Medium | Low        | Future |
| Streaming Compression  | Medium | Low        | Future |

---

## Shared Infrastructure (Available)

| Module                       | Built In | Used By                                           |
| ---------------------------- | -------- | ------------------------------------------------- |
| Schema Graph (`src/schema/`) | v1.5-1.6 | sample, shard, validate (planned), diff (planned) |
| Row Parsing (`src/parser/`)  | v1.5-1.7 | sample, shard, convert, redact (planned)          |
| PK Tracking                  | v1.5-1.6 | sample, shard, validate (planned)                 |
