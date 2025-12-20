# MSSQL/T-SQL Support Feasibility

**Status**: Analysis  
**Date**: 2025-12-20  
**Source**: Oracle analysis

## Overview

Analysis of adding Microsoft SQL Server (MSSQL) / T-SQL as a fourth supported dialect alongside MySQL, PostgreSQL, and SQLite.

**Verdict: Feasible for script-based dumps with moderate effort (~3-5 days)**

---

## How MSSQL Dumps Are Generated

### Supported (Script-based)

| Tool | Format | sql-splitter Support |
|------|--------|---------------------|
| **SSMS "Generate Scripts"** | `.sql` with T-SQL | ✅ Primary target |
| **sqlcmd** | `.sql` scripts | ✅ Supported |
| **Azure Data Studio** | `.sql` scripts | ✅ Supported |

### Not Supported (Binary/Proprietary)

| Tool | Format | Support |
|------|--------|---------|
| **bcp utility** | `.bcp` flat files | ❌ Different format |
| **Native backups** | `.bak` files | ❌ Binary format |
| **DACPAC/BACPAC** | `.dacpac`/`.bacpac` | ❌ Proprietary |

---

## Key Syntax Differences

### 1. Batch Separator: `GO`

```sql
CREATE TABLE [users] (...)
GO

INSERT INTO [users] VALUES (...)
GO
```

**Handling:**
- `GO` is a client directive, not SQL
- Treat as statement boundary (like `;`)
- Must only recognize at line start (not inside strings)
- Optional repeat count: `GO 100` (ignore count)

### 2. Identifier Quoting: Square Brackets

| Dialect | Quoting | Escape |
|---------|---------|--------|
| MySQL | \`identifier\` | \`\` |
| PostgreSQL | "identifier" | "" |
| SQLite | "identifier" or \`identifier\` | "" |
| **MSSQL** | [identifier] | ]] |

**Examples:**
```sql
[table name]           -- Simple
[column with ]]        -- Escaped bracket
[dbo].[users]          -- Schema-qualified
[database].[schema].[table]  -- Fully qualified
```

### 3. String Literals

| Type | Syntax | Example |
|------|--------|---------|
| Regular string | `'text'` | `'Hello'` |
| Escape quote | `''` | `'It''s'` |
| Unicode string | `N'text'` | `N'日本語'` |
| Binary | `0x...` | `0x48454C4C4F` |

### 4. INSERT Syntax

```sql
-- Standard (supported)
INSERT INTO [schema].[table] ([col1], [col2]) VALUES 
(1, 'value1'),
(2, 'value2');

-- Default values
INSERT INTO [table] DEFAULT VALUES;

-- Identity insert
SET IDENTITY_INSERT [table] ON;
INSERT INTO [table] ([id], [name]) VALUES (1, 'test');
SET IDENTITY_INSERT [table] OFF;
```

### 5. BULK INSERT (External Data)

```sql
BULK INSERT [schema].[table] 
FROM 'C:\data\file.bcp'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);
```

**Handling:** Treat as opaque statement associated with table. Data is external, cannot split by row.

### 6. DDL Specifics

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

**Key patterns:**
- `IDENTITY(seed, increment)` for auto-increment
- `CLUSTERED`/`NONCLUSTERED` index types
- `ON [PRIMARY]` filegroup specification
- `CONSTRAINT [name]` inline naming

### 7. T-SQL Session Settings

```sql
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET NOCOUNT ON
GO
```

**Handling:** Preserve these statements; they affect SQL Server behavior.

---

## Data Type Mappings

### MSSQL → Other Dialects

| MSSQL | PostgreSQL | MySQL | SQLite |
|-------|------------|-------|--------|
| `BIT` | `BOOLEAN` | `TINYINT(1)` | `INTEGER` |
| `TINYINT` | `SMALLINT` | `TINYINT` | `INTEGER` |
| `SMALLINT` | `SMALLINT` | `SMALLINT` | `INTEGER` |
| `INT` | `INTEGER` | `INT` | `INTEGER` |
| `BIGINT` | `BIGINT` | `BIGINT` | `INTEGER` |
| `IDENTITY(1,1)` | `SERIAL` | `AUTO_INCREMENT` | `INTEGER PRIMARY KEY` |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | `DECIMAL(p,s)` | `REAL` |
| `MONEY` | `DECIMAL(19,4)` | `DECIMAL(19,4)` | `REAL` |
| `FLOAT` | `DOUBLE PRECISION` | `DOUBLE` | `REAL` |
| `REAL` | `REAL` | `FLOAT` | `REAL` |
| `CHAR(n)` | `CHAR(n)` | `CHAR(n)` | `TEXT` |
| `VARCHAR(n)` | `VARCHAR(n)` | `VARCHAR(n)` | `TEXT` |
| `VARCHAR(MAX)` | `TEXT` | `LONGTEXT` | `TEXT` |
| `NCHAR(n)` | `CHAR(n)` | `CHAR(n)` | `TEXT` |
| `NVARCHAR(n)` | `VARCHAR(n)` | `VARCHAR(n)` | `TEXT` |
| `NVARCHAR(MAX)` | `TEXT` | `LONGTEXT` | `TEXT` |
| `TEXT` | `TEXT` | `LONGTEXT` | `TEXT` |
| `NTEXT` | `TEXT` | `LONGTEXT` | `TEXT` |
| `BINARY(n)` | `BYTEA` | `BINARY(n)` | `BLOB` |
| `VARBINARY(n)` | `BYTEA` | `VARBINARY(n)` | `BLOB` |
| `VARBINARY(MAX)` | `BYTEA` | `LONGBLOB` | `BLOB` |
| `IMAGE` | `BYTEA` | `LONGBLOB` | `BLOB` |
| `DATE` | `DATE` | `DATE` | `TEXT` |
| `TIME(p)` | `TIME(p)` | `TIME(p)` | `TEXT` |
| `DATETIME` | `TIMESTAMP` | `DATETIME` | `TEXT` |
| `DATETIME2(p)` | `TIMESTAMP(p)` | `DATETIME(p)` | `TEXT` |
| `SMALLDATETIME` | `TIMESTAMP(0)` | `DATETIME` | `TEXT` |
| `DATETIMEOFFSET` | `TIMESTAMPTZ` | `DATETIME` | `TEXT` |
| `UNIQUEIDENTIFIER` | `UUID` | `CHAR(36)` | `TEXT` |
| `XML` | `XML` | `LONGTEXT` | `TEXT` |
| `ROWVERSION` | `BYTEA` | `BINARY(8)` | `BLOB` |

### Other Dialects → MSSQL

| Source | MSSQL Equivalent |
|--------|-----------------|
| `SERIAL` (PG) | `INT IDENTITY(1,1)` |
| `BIGSERIAL` (PG) | `BIGINT IDENTITY(1,1)` |
| `AUTO_INCREMENT` (MySQL) | `IDENTITY(1,1)` |
| `BOOLEAN` (PG) | `BIT` |
| `BYTEA` (PG) | `VARBINARY(MAX)` |
| `JSONB` (PG) | `NVARCHAR(MAX)` |
| `TEXT` (any) | `NVARCHAR(MAX)` |

---

## Implementation Plan

### Phase 1: Lexer/Parser Updates

```rust
pub struct MssqlDialect;

impl Dialect for MssqlDialect {
    fn name(&self) -> &'static str { "mssql" }
    
    fn quote_identifier(&self, ident: &str) -> String {
        format!("[{}]", ident.replace(']', "]]"))
    }
    
    fn unquote_identifier(&self, quoted: &str) -> String {
        // Strip [ and ], unescape ]]
        let s = quoted.trim_start_matches('[').trim_end_matches(']');
        s.replace("]]", "]")
    }
    
    fn is_identifier_quote(&self, c: char) -> bool {
        c == '['
    }
    
    fn identifier_quote_chars(&self) -> (char, char) {
        ('[', ']')
    }
    
    fn string_escape_style(&self) -> StringEscapeStyle {
        StringEscapeStyle::DoubleQuote // '' for escaping
    }
    
    fn supports_unicode_prefix(&self) -> bool {
        true // N'...' strings
    }
}
```

### Phase 2: Statement Boundary Detection

```rust
fn is_go_statement(line: &str) -> bool {
    let trimmed = line.trim();
    // GO optionally followed by number
    trimmed.eq_ignore_ascii_case("go") || 
    trimmed.to_lowercase().starts_with("go ") && 
        trimmed[3..].trim().chars().all(|c| c.is_ascii_digit())
}

fn read_statement_mssql(&mut self) -> Option<Statement> {
    // Read until ; or GO on its own line
    // Handle GO as batch terminator
}
```

### Phase 3: Table Name Extraction

```rust
// Handle schema-qualified names
// [database].[schema].[table] -> table
// [dbo].[users] -> users
// [users] -> users

fn extract_table_name_mssql(stmt: &str) -> Option<String> {
    // Parse INSERT INTO [schema].[table]
    // Parse CREATE TABLE [schema].[table]
    // etc.
}
```

### Phase 4: Dialect Detection

Add MSSQL markers to auto-detection:

| Marker | Weight | Description |
|--------|--------|-------------|
| `SET ANSI_NULLS` | +20 | MSSQL session setting |
| `SET QUOTED_IDENTIFIER` | +20 | MSSQL session setting |
| `GO` (standalone line) | +15 | Batch separator |
| `[identifier]` | +10 | Square bracket quoting |
| `IDENTITY(` | +10 | Auto-increment syntax |
| `N'string'` | +5 | Unicode string prefix |
| `NVARCHAR` | +5 | Unicode string type |
| `CLUSTERED` | +5 | Index type |
| `ON [PRIMARY]` | +10 | Filegroup reference |

---

## Effort Estimate

| Component | Effort |
|-----------|--------|
| Lexer: bracket identifiers, N'...' strings, 0x binary | 4 hours |
| GO statement handling | 2 hours |
| Dialect trait implementation | 3 hours |
| Table name extraction (schema-qualified) | 3 hours |
| Data type mapping tables | 4 hours |
| Auto-detection markers | 2 hours |
| Integration with existing commands | 4 hours |
| Testing with real SSMS dumps | 6 hours |
| Documentation | 2 hours |
| **Total** | **~30 hours (3-5 days)** |

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Users expect bcp/bak support | Clear docs: "MSSQL support covers .sql scripts only" |
| sqlcmd meta-commands (`:r`, `:setvar`) | Preserve as-is; don't interpret |
| `GO` inside string literals | Robust lexer that tracks string state |
| Complex MERGE statements | Pass through; don't parse row-level |
| Filegroup/partition syntax | Preserve; strip for other dialects |

---

## Testing Strategy

### Test Fixtures Needed

1. **Simple SSMS export** — Schema + data, small
2. **Schema with IDENTITY and indexes** — DDL variations
3. **Large data export** — Multi-row INSERTs
4. **Unicode data** — N'...' strings
5. **BULK INSERT statements** — Passthrough test
6. **Mixed SET/GO statements** — Session handling

### Test Cases

- [ ] Split MSSQL dump into table files
- [ ] Merge MSSQL table files back
- [ ] Convert MSSQL → PostgreSQL
- [ ] Convert MSSQL → MySQL
- [ ] Convert PostgreSQL → MSSQL
- [ ] Convert MySQL → MSSQL
- [ ] Auto-detect MSSQL dialect
- [ ] Handle GO batches correctly
- [ ] Preserve IDENTITY columns
- [ ] Handle schema-qualified names

---

## Recommendation

**Implement MSSQL support for script-based dumps.**

The effort is moderate (~30 hours) and adds significant value for enterprise users who commonly work with SQL Server alongside MySQL/PostgreSQL.

**Scope for MVP:**
- SSMS-generated scripts
- sqlcmd-compatible scripts
- All existing commands (split, merge, analyze, etc.)
- Bidirectional convert with other dialects

**Out of scope (future):**
- bcp file parsing
- Native backup (.bak) support
- DACPAC/BACPAC support
- Advanced T-SQL analysis (stored procedures, dynamic SQL)

---

## Related

- [Convert Feature](CONVERT_FEASIBILITY.md)
- [Additional Ideas](ADDITIONAL_IDEAS.md)
