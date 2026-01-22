# Convert Command Gap Analysis

**Status**: Post-v1.7.0 Analysis (Updated with comprehensive fixes)  
**Date**: 2025-12-21  
**Tested Against**: Real-world dumps from verify-realworld.sh

## Summary

The convert command has been significantly improved with comprehensive PostgreSQL support, including COPY → INSERT conversion, type cast stripping, schema prefix handling, and more.

## Issues Fixed

### 1. Block Comments at Statement Start ✅

**Issue**: Block comments (`/* */`) at the start of statements prevented statement type detection.

**Fix**: Added block comment handling to `strip_leading_comments_and_whitespace()`.

### 2. SMALLINT Display Width ✅

**Issue**: `SMALLINT(6)` was not converted to `SMALLINT` for PostgreSQL.

**Fix**: Added `RE_SMALLINT` replacement to `mysql_to_postgres()`.

### 3. PostgreSQL-Only Feature Filtering ✅

**Issue**: CREATE DOMAIN, CREATE TYPE, CREATE FUNCTION, CREATE SEQUENCE, triggers, etc. were passing through unchanged.

**Fix**: Added `is_postgres_only_feature()` check. These statements now produce warnings and are skipped.

**Affected Statements**: CREATE DOMAIN, CREATE TYPE, CREATE FUNCTION, CREATE PROCEDURE, CREATE AGGREGATE, CREATE OPERATOR, CREATE SEQUENCE, CREATE EXTENSION, CREATE SCHEMA, CREATE TRIGGER, ALTER variants, COMMENT ON.

### 4. Additional SET Command Filtering ✅

**Issue**: PostgreSQL-specific SET commands like `SET statement_timeout`, `SET default_table_access_method` were passing through.

**Fix**: Added to `is_postgres_session_command()` filter.

### 5. TIMESTAMP WITH TIME ZONE ✅

**Issue**: `timestamp with time zone` was not being converted to DATETIME.

**Fix**: Added `RE_TIMESTAMP_WITH_TZ` regex and conversion to MySQL and SQLite paths.

### 6. OWNER TO Filtering ✅

**Issue**: `ALTER TABLE ... OWNER TO` statements were passing through.

**Fix**: Added `OWNER TO` pattern to session command filter.

### 7. COPY → INSERT Conversion ✅

**Issue**: PostgreSQL COPY statements were not being converted to INSERT statements.

**Fix**: Implemented full COPY → INSERT conversion:

- Parses COPY header to extract table and column information
- Parses tab-separated data block
- Handles NULL markers (`\N`)
- Handles escape sequences (`\t`, `\n`, `\\`, octal)
- Generates batched INSERT statements (100 rows per INSERT)
- Properly escapes quotes for target dialect

### 8. Type Cast Stripping (::type) ✅

**Issue**: PostgreSQL type casts like `::text`, `::regclass`, `::character varying` were passing through.

**Fix**: Added `strip_postgres_casts()` to remove all `::type` patterns.

### 9. nextval() Conversion ✅

**Issue**: `DEFAULT nextval('sequence_name'::regclass)` was passing through.

**Fix**: Added `convert_nextval()` to strip `DEFAULT nextval(...)` entirely (AUTO_INCREMENT handles the functionality).

### 10. DEFAULT now() Conversion ✅

**Issue**: `DEFAULT now()` was not being converted to `DEFAULT CURRENT_TIMESTAMP`.

**Fix**: Added `convert_default_now()` for proper conversion.

### 11. Schema Prefix Stripping ✅

**Issue**: Schema-qualified names like `public.table_name` were passing through.

**Fix**: Added `strip_schema_prefix()` to remove common PostgreSQL schema prefixes (`public.`, `pg_catalog.`, `pg_temp.`).

---

## Remaining Gaps

### Low Priority (Rare in Practice)

| Gap                 | Severity | Notes                         |
| ------------------- | -------- | ----------------------------- |
| Array types         | Low      | Warning issued, no conversion |
| EXCLUDE constraints | Low      | Very rare in dumps            |
| Partial indexes     | Low      | `WHERE` clause in indexes     |
| Expression indexes  | Low      | Rare, would need parsing      |
| INTERVAL types      | Low      | PostgreSQL → MySQL            |

---

## Real-World Test Results (After Fixes)

### MySQL → PostgreSQL (classicmodels.sql)

| Metric                          | Before | After |
| ------------------------------- | ------ | ----- |
| Statements converted            | 1      | 26+   |
| Block comments handled          | ❌     | ✅    |
| SMALLINT display width stripped | ❌     | ✅    |

### PostgreSQL → MySQL (pagila-schema.sql)

| Metric                   | Before | After |
| ------------------------ | ------ | ----- |
| CREATE FUNCTION skipped  | ❌     | ✅    |
| CREATE DOMAIN skipped    | ❌     | ✅    |
| CREATE TYPE skipped      | ❌     | ✅    |
| CREATE SEQUENCE skipped  | ❌     | ✅    |
| OWNER TO filtered        | ❌     | ✅    |
| ::type casts stripped    | ❌     | ✅    |
| nextval() removed        | ❌     | ✅    |
| DEFAULT now() converted  | ❌     | ✅    |
| Schema prefix stripped   | ❌     | ✅    |
| TIMESTAMP WITH TIME ZONE | ❌     | ✅    |

### PostgreSQL → MySQL (pagila-data.sql)

| Metric             | Before | After                |
| ------------------ | ------ | -------------------- |
| COPY → INSERT      | ❌     | ✅                   |
| NULL handling (\N) | ❌     | ✅                   |
| Escape sequences   | ❌     | ✅                   |
| Batched inserts    | N/A    | ✅ (100 rows/INSERT) |

---

## Test Coverage

- [x] Real-world MySQL dumps: Classic Models, Sakila, WordPress
- [x] Real-world PostgreSQL dumps: Pagila, Northwind, Airlines
- [x] COPY data blocks with various data types
- [x] NULL values in COPY blocks
- [x] Escape sequences in COPY blocks
- [x] Schema-qualified table names
- [x] Type casts (::regclass, ::text, etc.)
- [x] DEFAULT nextval() sequences
- [x] DEFAULT now() timestamps
- [x] All 6 dialect conversion pairs

---

## Related Files

- [src/convert/mod.rs](file:///Users/helge/code/sql-splitter/src/convert/mod.rs) - Main converter
- [src/convert/copy_to_insert.rs](file:///Users/helge/code/sql-splitter/src/convert/copy_to_insert.rs) - COPY → INSERT conversion
- [src/convert/types.rs](file:///Users/helge/code/sql-splitter/src/convert/types.rs) - Type mapping
- [src/parser/mod.rs](file:///Users/helge/code/sql-splitter/src/parser/mod.rs) - Statement parsing
- [scripts/verify-realworld.sh](file:///Users/helge/code/sql-splitter/scripts/verify-realworld.sh) - Real-world verification script
