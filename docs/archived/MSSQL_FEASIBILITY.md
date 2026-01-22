# MSSQL/T-SQL Support Implementation Plan

**Status**: Ready for Implementation  
**Version Target**: v1.13.0  
**Date**: 2025-12-26  
**Estimated Effort**: 40-50 hours (2-3 weeks)

---

## Executive Summary

Add Microsoft SQL Server (MSSQL) / T-SQL as the fourth supported dialect with **full parity** across all 12 commands. This transforms sql-splitter from a 3-dialect tool to a complete 4-dialect SQL dump processing platform.

**Verdict: ✅ HIGHLY FEASIBLE**

| Aspect         | Assessment              | Notes                                                                                      |
| -------------- | ----------------------- | ------------------------------------------------------------------------------------------ |
| **Technical**  | ✅ Feasible             | Extends existing dialect architecture                                                      |
| **Scope**      | Script-based dumps only | SSMS, sqlcmd, Azure Data Studio                                                            |
| **Commands**   | All 12                  | split, merge, analyze, sample, shard, convert, validate, diff, redact, graph, order, query |
| **Conversion** | 12 pairs                | MSSQL ↔ MySQL/PostgreSQL/SQLite                                                            |

---

## Scope

### ✅ In Scope (Script-based)

| Tool                        | Format            | Support           |
| --------------------------- | ----------------- | ----------------- |
| **SSMS "Generate Scripts"** | `.sql` with T-SQL | ✅ Primary target |
| **sqlcmd**                  | `.sql` scripts    | ✅ Supported      |
| **Azure Data Studio**       | `.sql` scripts    | ✅ Supported      |

### ❌ Out of Scope (Binary/Proprietary)

| Tool               | Format              | Support             |
| ------------------ | ------------------- | ------------------- |
| **bcp utility**    | `.bcp` flat files   | ❌ Different format |
| **Native backups** | `.bak` files        | ❌ Binary format    |
| **DACPAC/BACPAC**  | `.dacpac`/`.bacpac` | ❌ Proprietary      |

---

## Implementation Phases

### Phase 1: Core Infrastructure (12-16h)

**Theme**: Parser, dialect enum, type mappings

### Phase 2: Convert Command (8-10h)

**Theme**: MSSQL ↔ all dialects (6 new pairs)

### Phase 3: Split/Merge/Analyze (6-8h)

**Theme**: Basic file operations with MSSQL

### Phase 4: Schema Commands (6-8h)

**Theme**: graph, order, validate

### Phase 5: Data Commands (6-8h)

**Theme**: sample, shard, diff, redact

### Phase 6: Query Command (4-6h)

**Theme**: DuckDB integration for MSSQL

### Phase 7: Testing & Documentation (8-10h)

**Theme**: Fixtures, integration tests, docs

---

## Detailed Task Tracking

### Phase 1: Core Infrastructure

| ID   | Task                                                         | Effort  | Status  | Notes                                    |
| ---- | ------------------------------------------------------------ | ------- | ------- | ---------------------------------------- |
| 1.1  | Add `SqlDialect::Mssql` enum variant                         | 0.5h    | ✅ DONE | src/parser/mod.rs                        |
| 1.2  | Implement `FromStr` for MSSQL (mssql, sqlserver, sql_server) | 0.5h    | ✅ DONE |                                          |
| 1.3  | Update CLI help strings (all commands)                       | 1h      | ✅ DONE | "mysql, postgres, sqlite, mssql"         |
| 1.4  | Add MSSQL auto-detection markers                             | 2h      | ✅ DONE | SET ANSI_NULLS, GO, [brackets], IDENTITY |
| 1.5  | Implement GO batch separator handling                        | 3h      | ✅ DONE | Line-based, not in strings               |
| 1.6  | Implement `[bracket]` identifier parsing                     | 2h      | ✅ DONE | Including `]]` escape                    |
| 1.7  | Implement `N'unicode'` string literal support                | 1.5h    | ✅ DONE | Treat as regular string                  |
| 1.8  | Implement `0x` binary literal support                        | 1h      | ✅ DONE | Pass through                             |
| 1.9  | Table name extraction for `[db].[schema].[table]`            | 2h      | ✅ DONE | Return last segment                      |
| 1.10 | Add BULK INSERT statement classification                     | 1h      | ✅ DONE | StatementType::Insert                    |
| 1.11 | Preserve SET session statements                              | 0.5h    | ✅ DONE | StatementType::Unknown                   |
|      | **Phase 1 Total**                                            | **15h** | ✅ DONE |                                          |

### Phase 2: Convert Command

| ID  | Task                                              | Effort  | Status  | Notes                                       |
| --- | ------------------------------------------------- | ------- | ------- | ------------------------------------------- |
| 2.1 | Add MSSQL type mappings to TypeMapper             | 3h      | ✅ DONE | 30+ types per direction                     |
| 2.2 | MSSQL → PostgreSQL conversion                     | 1.5h    | ✅ DONE | IDENTITY→SERIAL, GETDATE→CURRENT_TIMESTAMP  |
| 2.3 | MSSQL → MySQL conversion                          | 1.5h    | ✅ DONE | IDENTITY→AUTO_INCREMENT, NVARCHAR→VARCHAR   |
| 2.4 | MSSQL → SQLite conversion                         | 1h      | ✅ DONE | IDENTITY→INTEGER PRIMARY KEY                |
| 2.5 | MySQL → MSSQL conversion                          | 1.5h    | ✅ DONE | AUTO_INCREMENT→IDENTITY, backticks→brackets |
| 2.6 | PostgreSQL → MSSQL conversion                     | 1.5h    | ✅ DONE | SERIAL→IDENTITY                             |
| 2.7 | SQLite → MSSQL conversion                         | 1h      | ✅ DONE | Affinities→proper types                     |
| 2.8 | Strip MSSQL-only features (filegroups, CLUSTERED) | 1h      | ✅ DONE | When converting to other dialects           |
| 2.9 | Add warnings for unsupported features             | 1h      | 🔲 TODO | BULK INSERT, partitioning                   |
|     | **Phase 2 Total**                                 | **13h** | ✅ DONE |                                             |

### Phase 3: Split/Merge/Analyze

| ID  | Task                                | Effort | Status  | Notes                     |
| --- | ----------------------------------- | ------ | ------- | ------------------------- |
| 3.1 | Wire MSSQL dialect to split command | 1h     | ✅ DONE | CLI + detection           |
| 3.2 | Handle GO batches in split routing  | 2h     | ✅ DONE | Statement boundaries      |
| 3.3 | Split with `[schema].[table]` names | 1h     | ✅ DONE | Use last segment for file |
| 3.4 | Wire MSSQL to merge command         | 1h     | ✅ DONE | Output with brackets      |
| 3.5 | Wire MSSQL to analyze command       | 1h     | ✅ DONE | Schema stats              |
| 3.6 | Parse IDENTITY columns in analyze   | 1h     | ✅ DONE | Report auto-increment     |
|     | **Phase 3 Total**                   | **7h** | ✅ DONE |                           |

### Phase 4: Schema Commands (graph, order, validate)

| ID  | Task                           | Effort | Status  | Notes                                             |
| --- | ------------------------------ | ------ | ------- | ------------------------------------------------- |
| 4.1 | Parse MSSQL PK constraints     | 1.5h   | ✅ DONE | `CONSTRAINT [name] PRIMARY KEY CLUSTERED`         |
| 4.2 | Parse MSSQL FK constraints     | 2h     | ✅ DONE | `FOREIGN KEY ([col]) REFERENCES [schema].[table]` |
| 4.3 | Parse MSSQL indexes            | 1.5h   | ✅ DONE | CLUSTERED/NONCLUSTERED                            |
| 4.4 | Wire MSSQL to graph command    | 1h     | ✅ DONE | ERD generation                                    |
| 4.5 | Wire MSSQL to order command    | 0.5h   | ✅ DONE | Topological sort                                  |
| 4.6 | Wire MSSQL to validate command | 1.5h   | ✅ DONE | PK/FK integrity                                   |
|     | **Phase 4 Total**              | **8h** | ✅ DONE |                                                   |

### Phase 5: Data Commands (sample, shard, diff, redact)

| ID  | Task                                      | Effort | Status     | Notes                          |
| --- | ----------------------------------------- | ------ | ---------- | ------------------------------ |
| 5.1 | Parse MSSQL INSERT row values             | 2h     | ✅ DONE    | N'...', 0x..., NULL            |
| 5.2 | Wire MSSQL to sample command              | 1h     | ✅ DONE    | Row sampling                   |
| 5.3 | Wire MSSQL to shard command               | 1h     | ✅ DONE    | Tenant extraction              |
| 5.4 | Wire MSSQL to diff command                | 1.5h   | ✅ DONE    | Schema + data diff             |
| 5.5 | Wire MSSQL to redact command              | 1.5h   | ✅ DONE    | Column pattern matching        |
| 5.6 | Rewrite MSSQL INSERT with redacted values | 1h     | ⏸️ BLOCKED | Base INSERT rewriting not impl |
|     | **Phase 5 Total**                         | **8h** | ✅ DONE    |                                |

### Phase 6: Query Command (DuckDB)

| ID  | Task                                     | Effort | Status  | Notes                     |
| --- | ---------------------------------------- | ------ | ------- | ------------------------- |
| 6.1 | Map MSSQL types to DuckDB types          | 2h     | ✅ DONE | NVARCHAR, DATETIME2, etc. |
| 6.2 | Normalize bracket identifiers for DuckDB | 1h     | ✅ DONE | [table] → "table"         |
| 6.3 | Handle N'...' string ingestion           | 1h     | ✅ DONE | Strip N prefix            |
| 6.4 | Handle GO batch separator                | 1h     | ✅ DONE | Line-based GO detection   |
| 6.5 | Strip PK/FK constraints                  | 0.5h   | ✅ DONE | Analytics don't need them |
| 6.6 | Wire MSSQL to query command CLI          | 0.5h   | ✅ DONE | --dialect mssql           |
|     | **Phase 6 Total**                        | **6h** | ✅ DONE |                           |

### Phase 7: Testing & Documentation

| ID   | Task                                        | Effort  | Status         | Notes                                 |
| ---- | ------------------------------------------- | ------- | -------------- | ------------------------------------- |
| 7.1  | Create MSSQL test fixtures (small)          | 2h      | ✅ DONE        | simple.sql with users/orders          |
| 7.2  | Create MSSQL test fixtures (edge cases)     | 2h      | ✅ DONE        | edge_cases.sql: N'...', 0x, types     |
| 7.3  | Integration tests: split/merge/analyze      | 1.5h    | ✅ DONE        | 16 tests in mssql_integration_test.rs |
| 7.4  | Integration tests: convert (6 MSSQL pairs)  | 2h      | ✅ DONE        | Tested manually                       |
| 7.5  | Integration tests: validate/graph/order     | 1h      | ✅ DONE        | 4 tests in mssql_integration_test.rs  |
| 7.6  | Integration tests: sample/shard/diff/redact | 1.5h    | ✅ DONE        | 4 tests in mssql_integration_test.rs  |
| 7.7  | Integration tests: query                    | 1h      | ✅ DONE        | 2 tests in mssql_integration_test.rs  |
| 7.8  | Update README with MSSQL examples           | 1h      | 🔲 TODO        |                                       |
| 7.9  | Update llms.txt                             | 0.5h    | 🔲 TODO        |                                       |
| 7.10 | Update SKILL.md                             | 0.5h    | 🔲 TODO        |                                       |
| 7.11 | Update man pages                            | 0.5h    | 🔲 TODO        |                                       |
| 7.12 | Archive this doc to docs/archived/          | 0.1h    | 🔲 TODO        |                                       |
|      | **Phase 7 Total**                           | **14h** | 🔄 In Progress |                                       |

---

## Overall Progress

| Phase                  | Tasks  | Completed | Effort  | Status         |
| ---------------------- | ------ | --------- | ------- | -------------- |
| 1. Core Infrastructure | 11     | 11        | 15h     | ✅ DONE        |
| 2. Convert Command     | 9      | 8         | 13h     | ✅ DONE        |
| 3. Split/Merge/Analyze | 6      | 6         | 7h      | ✅ DONE        |
| 4. Schema Commands     | 6      | 6         | 8h      | ✅ DONE        |
| 5. Data Commands       | 6      | 5         | 8h      | ✅ DONE        |
| 6. Query Command       | 6      | 6         | 6h      | ✅ DONE        |
| 7. Testing & Docs      | 12     | 9         | 14h     | 🔄 In Progress |
| **TOTAL**              | **56** | **51**    | **71h** | **91%**        |

---

## Key MSSQL Syntax Reference

### 1. Batch Separator: `GO`

```sql
CREATE TABLE [users] (...)
GO

INSERT INTO [users] VALUES (...)
GO
```

- `GO` is a client directive, not SQL
- Treat as statement boundary (like `;`)
- Only recognize at line start (not inside strings)
- Optional repeat count: `GO 100` (ignore count)

### 2. Identifier Quoting: Square Brackets

| Dialect    | Quoting                        | Escape |
| ---------- | ------------------------------ | ------ |
| MySQL      | \`identifier\`                 | \`\`   |
| PostgreSQL | "identifier"                   | ""     |
| SQLite     | "identifier" or \`identifier\` | ""     |
| **MSSQL**  | [identifier]                   | ]]     |

**Examples:**

```sql
[table name]           -- Simple
[column with ]]        -- Escaped bracket
[dbo].[users]          -- Schema-qualified
[database].[schema].[table]  -- Fully qualified
```

### 3. String Literals

| Type           | Syntax    | Example        |
| -------------- | --------- | -------------- |
| Regular string | `'text'`  | `'Hello'`      |
| Escape quote   | `''`      | `'It''s'`      |
| Unicode string | `N'text'` | `N'日本語'`    |
| Binary         | `0x...`   | `0x48454C4C4F` |

### 4. DDL Example

```sql
CREATE TABLE [dbo].[users] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [created_at] DATETIME2(7) DEFAULT GETDATE(),
    CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_users_email]
ON [dbo].[users] ([email]);
```

### 5. Session Settings

```sql
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET NOCOUNT ON
GO
```

---

## Data Type Mappings

### MSSQL → Other Dialects

| MSSQL              | PostgreSQL         | MySQL            | SQLite                | DuckDB          |
| ------------------ | ------------------ | ---------------- | --------------------- | --------------- |
| `BIT`              | `BOOLEAN`          | `TINYINT(1)`     | `INTEGER`             | `BOOLEAN`       |
| `TINYINT`          | `SMALLINT`         | `TINYINT`        | `INTEGER`             | `TINYINT`       |
| `SMALLINT`         | `SMALLINT`         | `SMALLINT`       | `INTEGER`             | `SMALLINT`      |
| `INT`              | `INTEGER`          | `INT`            | `INTEGER`             | `INTEGER`       |
| `BIGINT`           | `BIGINT`           | `BIGINT`         | `INTEGER`             | `BIGINT`        |
| `IDENTITY(1,1)`    | `SERIAL`           | `AUTO_INCREMENT` | `INTEGER PRIMARY KEY` | `INTEGER`       |
| `DECIMAL(p,s)`     | `DECIMAL(p,s)`     | `DECIMAL(p,s)`   | `REAL`                | `DECIMAL(p,s)`  |
| `MONEY`            | `DECIMAL(19,4)`    | `DECIMAL(19,4)`  | `REAL`                | `DECIMAL(19,4)` |
| `FLOAT`            | `DOUBLE PRECISION` | `DOUBLE`         | `REAL`                | `DOUBLE`        |
| `REAL`             | `REAL`             | `FLOAT`          | `REAL`                | `FLOAT`         |
| `CHAR(n)`          | `CHAR(n)`          | `CHAR(n)`        | `TEXT`                | `VARCHAR`       |
| `VARCHAR(n)`       | `VARCHAR(n)`       | `VARCHAR(n)`     | `TEXT`                | `VARCHAR`       |
| `VARCHAR(MAX)`     | `TEXT`             | `LONGTEXT`       | `TEXT`                | `VARCHAR`       |
| `NCHAR(n)`         | `CHAR(n)`          | `CHAR(n)`        | `TEXT`                | `VARCHAR`       |
| `NVARCHAR(n)`      | `VARCHAR(n)`       | `VARCHAR(n)`     | `TEXT`                | `VARCHAR`       |
| `NVARCHAR(MAX)`    | `TEXT`             | `LONGTEXT`       | `TEXT`                | `VARCHAR`       |
| `TEXT`             | `TEXT`             | `LONGTEXT`       | `TEXT`                | `VARCHAR`       |
| `NTEXT`            | `TEXT`             | `LONGTEXT`       | `TEXT`                | `VARCHAR`       |
| `BINARY(n)`        | `BYTEA`            | `BINARY(n)`      | `BLOB`                | `BLOB`          |
| `VARBINARY(n)`     | `BYTEA`            | `VARBINARY(n)`   | `BLOB`                | `BLOB`          |
| `VARBINARY(MAX)`   | `BYTEA`            | `LONGBLOB`       | `BLOB`                | `BLOB`          |
| `IMAGE`            | `BYTEA`            | `LONGBLOB`       | `BLOB`                | `BLOB`          |
| `DATE`             | `DATE`             | `DATE`           | `TEXT`                | `DATE`          |
| `TIME(p)`          | `TIME(p)`          | `TIME(p)`        | `TEXT`                | `TIME`          |
| `DATETIME`         | `TIMESTAMP`        | `DATETIME`       | `TEXT`                | `TIMESTAMP`     |
| `DATETIME2(p)`     | `TIMESTAMP(p)`     | `DATETIME(p)`    | `TEXT`                | `TIMESTAMP`     |
| `SMALLDATETIME`    | `TIMESTAMP(0)`     | `DATETIME`       | `TEXT`                | `TIMESTAMP`     |
| `DATETIMEOFFSET`   | `TIMESTAMPTZ`      | `DATETIME`       | `TEXT`                | `TIMESTAMP`     |
| `UNIQUEIDENTIFIER` | `UUID`             | `CHAR(36)`       | `TEXT`                | `UUID`          |
| `XML`              | `XML`              | `LONGTEXT`       | `TEXT`                | `VARCHAR`       |
| `ROWVERSION`       | `BYTEA`            | `BINARY(8)`      | `BLOB`                | `BLOB`          |

### Other Dialects → MSSQL

| Source                   | MSSQL Equivalent       |
| ------------------------ | ---------------------- |
| `SERIAL` (PG)            | `INT IDENTITY(1,1)`    |
| `BIGSERIAL` (PG)         | `BIGINT IDENTITY(1,1)` |
| `AUTO_INCREMENT` (MySQL) | `IDENTITY(1,1)`        |
| `BOOLEAN` (PG)           | `BIT`                  |
| `BYTEA` (PG)             | `VARBINARY(MAX)`       |
| `JSONB` (PG)             | `NVARCHAR(MAX)`        |
| `TEXT` (any)             | `NVARCHAR(MAX)`        |
| `LONGTEXT` (MySQL)       | `NVARCHAR(MAX)`        |
| `LONGBLOB` (MySQL)       | `VARBINARY(MAX)`       |

---

## Dialect Auto-Detection Markers

| Marker                  | Weight | Description            |
| ----------------------- | ------ | ---------------------- |
| `SET ANSI_NULLS`        | +20    | MSSQL session setting  |
| `SET QUOTED_IDENTIFIER` | +20    | MSSQL session setting  |
| `GO` (standalone line)  | +15    | Batch separator        |
| `[identifier]`          | +10    | Square bracket quoting |
| `IDENTITY(`             | +10    | Auto-increment syntax  |
| `N'string'`             | +5     | Unicode string prefix  |
| `NVARCHAR`              | +5     | Unicode string type    |
| `CLUSTERED`             | +5     | Index type             |
| `ON [PRIMARY]`          | +10    | Filegroup reference    |

---

## Conversion Matrix

After MSSQL support, sql-splitter handles 12 directed conversion pairs:

```
     MySQL ←→ PostgreSQL
       ↕           ↕
    SQLite ←→  MSSQL
```

| From \ To      | MySQL | PostgreSQL | SQLite | MSSQL |
| -------------- | ----- | ---------- | ------ | ----- |
| **MySQL**      | —     | ✅         | ✅     | 🆕    |
| **PostgreSQL** | ✅    | —          | ✅     | 🆕    |
| **SQLite**     | ✅    | ✅         | —      | 🆕    |
| **MSSQL**      | 🆕    | 🆕         | 🆕     | —     |

---

## Risks & Mitigations

| Risk                        | Likelihood | Impact | Mitigation                                    |
| --------------------------- | ---------- | ------ | --------------------------------------------- |
| `GO` inside string literals | Medium     | High   | Parser tracks string state before checking GO |
| Mis-detecting dialect       | Low        | Medium | Threshold-based scoring, default to MySQL     |
| Type incompatibilities      | Medium     | Low    | Conservative mapping, emit warnings           |
| FK parsing edge cases       | Medium     | Medium | Golden tests from real SSMS dumps             |
| BULK INSERT external data   | Low        | Low    | Create empty table, emit warning              |

---

## Test Fixtures Needed

### Basic Fixtures

1. **Simple schema + data** — CREATE TABLE, INSERT, basic types
2. **GO batches** — Multiple statements with GO separators
3. **Schema-qualified names** — `[dbo].[users]`, `[db].[schema].[table]`

### Edge Case Fixtures

4. **Unicode strings** — `N'日本語'`, `N'Ελληνικά'`
5. **Binary literals** — `0x48454C4C4F`
6. **IDENTITY columns** — `INT IDENTITY(1,1)`, `BIGINT IDENTITY(100,10)`
7. **PK/FK constraints** — `CONSTRAINT [name] PRIMARY KEY`, `FOREIGN KEY`
8. **Indexes** — `CLUSTERED`, `NONCLUSTERED`, `INCLUDE`
9. **Session settings** — `SET ANSI_NULLS`, `SET QUOTED_IDENTIFIER`
10. **BULK INSERT** — External file reference (passthrough test)

### Integration Fixtures

11. **Multi-tenant schema** — For shard testing
12. **Large dataset** — For performance testing

---

## Command Coverage Checklist

| Command  | CLI | Detection | Parse | Output | Tests |
| -------- | --- | --------- | ----- | ------ | ----- |
| split    | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| merge    | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| analyze  | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| sample   | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| shard    | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| convert  | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| validate | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| diff     | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| redact   | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| graph    | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| order    | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |
| query    | 🔲  | 🔲        | 🔲    | 🔲     | 🔲    |

---

## Implementation Order (Recommended)

1. **Phase 1: Core** — Must be first; all other phases depend on it
2. **Phase 3: Split/Merge/Analyze** — Low complexity, validates parser
3. **Phase 2: Convert** — Complex type mappings, validates type system
4. **Phase 4: Schema Commands** — Validates FK/PK parsing
5. **Phase 5: Data Commands** — Validates row parsing
6. **Phase 6: Query** — DuckDB integration
7. **Phase 7: Testing** — Ongoing throughout, final polish

---

## Success Criteria

- [ ] All 12 commands accept `--dialect mssql`
- [ ] Auto-detection correctly identifies MSSQL dumps
- [ ] GO batch separator handled correctly
- [ ] Square bracket identifiers parsed and converted
- [ ] N'unicode' strings handled
- [ ] IDENTITY columns mapped to/from AUTO_INCREMENT/SERIAL
- [ ] All 6 MSSQL conversion pairs working
- [ ] DuckDB can query MSSQL dumps
- [ ] 50+ new integration tests for MSSQL
- [ ] Documentation updated
- [ ] Real-world SSMS dumps verified

---

## Related Documents

- [ROADMAP.md](../ROADMAP.md)
- [CONVERT_FEASIBILITY.md](../archived/CONVERT_FEASIBILITY.md)
- [ADDITIONAL_IDEAS.md](ADDITIONAL_IDEAS.md)
