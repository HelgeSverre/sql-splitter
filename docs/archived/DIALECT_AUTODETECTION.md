# Dialect Auto-Detection

## Overview

SQL Splitter can automatically detect the SQL dialect from file content when the `--dialect` flag is not specified.

## Usage

```bash
# Auto-detect dialect (reads first 8KB of file)
sql-splitter split dump.sql --output=tables

# Explicit dialect (skip auto-detection)
sql-splitter split dump.sql --output=tables --dialect=postgres
```

## Detection Strategy

The detector reads the first 8KB of the file and uses a weighted scoring system to identify the dialect:

### PostgreSQL Indicators

| Marker                     | Confidence | Score |
| -------------------------- | ---------- | ----- |
| `PostgreSQL database dump` | High       | +10   |
| `pg_dump`                  | High       | +10   |
| `COPY ... FROM stdin`      | Medium     | +5    |
| `search_path`              | Medium     | +5    |
| `$$` (dollar-quoting)      | Low        | +2    |
| `CREATE EXTENSION`         | Low        | +2    |

### MySQL/MariaDB Indicators

| Marker                                          | Confidence | Score |
| ----------------------------------------------- | ---------- | ----- |
| `MySQL dump`                                    | High       | +10   |
| `MariaDB dump`                                  | High       | +10   |
| `/*!40...` or `/*!50...` (conditional comments) | Medium     | +5    |
| `LOCK TABLES`                                   | Medium     | +5    |
| Backtick character (`` ` ``)                    | Low        | +2    |

### SQLite Indicators

| Marker              | Confidence | Score |
| ------------------- | ---------- | ----- |
| `SQLite`            | High       | +10   |
| `PRAGMA`            | Medium     | +5    |
| `BEGIN TRANSACTION` | Medium     | +5    |

## Scoring

The dialect with the highest score wins. If no markers are found (score = 0), MySQL is used as the default since it's the most common format.

Confidence levels reported to the user:

- **High confidence**: Score ≥ 10
- **Medium confidence**: Score ≥ 5
- **Low confidence**: Score < 5

## Implementation

The detection is implemented in `src/parser/mod.rs`:

```rust
pub fn detect_dialect(header: &[u8]) -> DialectDetectionResult;
pub fn detect_dialect_from_file(path: &Path) -> io::Result<DialectDetectionResult>;
```

## Examples

### PostgreSQL pg_dump output

```sql
--
-- PostgreSQL database dump
--
-- Dumped by pg_dump version 15.2

SET search_path = public;

COPY users (id, name) FROM stdin;
1   Alice
\.
```

Detected as: **PostgreSQL** (high confidence)

### MySQL mysqldump output

```sql
-- MySQL dump 10.13  Distrib 8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;

LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'Alice');
UNLOCK TABLES;
```

Detected as: **MySQL** (high confidence)

### SQLite .dump output

```sql
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES(1,'Alice');
COMMIT;
```

Detected as: **SQLite** (high confidence)
