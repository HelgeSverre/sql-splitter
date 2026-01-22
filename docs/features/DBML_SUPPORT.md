# DBML Import/Export Support

**Status**: Planning
**Date**: 2025-01-22
**Priority**: Medium
**Effort**: ~35 hours

---

## Overview

Add bidirectional DBML (Database Markup Language) support to sql-splitter, enabling:

- **Export**: Convert SQL dumps to DBML schema definitions
- **Import**: Convert DBML files to SQL DDL statements

DBML is a human-readable DSL for database schema definitions, popular for documentation, ERD generation, and schema versioning with tools like dbdiagram.io.

---

## Problem Statement

**Current pain points:**

1. **Schema Documentation**: Users need to manually recreate schemas in DBML for documentation tools
2. **ERD Tools**: dbdiagram.io and similar tools accept DBML but not raw SQL dumps
3. **Schema Versioning**: DBML's clean syntax is preferred for git diffs over verbose SQL DDL
4. **Cross-Platform**: Teams using DBML as source-of-truth need SQL generation for their specific dialect

**Current workarounds:**

| Task        | Current Approach                      | Problems                             |
| ----------- | ------------------------------------- | ------------------------------------ |
| SQL → DBML  | Manual rewrite or dbdiagram.io import | Loss of fidelity, time-consuming     |
| DBML → SQL  | dbdiagram.io export or @dbml/cli      | Requires Node.js, no offline support |
| Schema docs | Use `graph` command with Mermaid      | Different syntax, not DBML ecosystem |

**Desired behavior:**

```bash
# Export SQL schema to DBML
sql-splitter graph dump.sql --format dbml -o schema.dbml

# Import DBML to SQL DDL
sql-splitter convert schema.dbml --to mysql -o schema.sql
```

---

## Command Interface

### Export (SQL → DBML)

Extends the existing `graph` command with a new output format:

```bash
# Export to DBML (schema only, no data)
sql-splitter graph dump.sql --format dbml -o schema.dbml

# Export with table filtering
sql-splitter graph dump.sql --format dbml --tables "users,orders,*_audit" -o schema.dbml

# Export specific table and its dependencies
sql-splitter graph dump.sql --format dbml --table orders --transitive -o orders.dbml

# Export to stdout
sql-splitter graph dump.sql --format dbml

# Include table/column notes as comments
sql-splitter graph dump.sql --format dbml --include-notes -o schema.dbml
```

### Import (DBML → SQL)

Extends the `convert` command to accept DBML as input:

```bash
# Convert DBML to MySQL
sql-splitter convert schema.dbml --to mysql -o schema.sql

# Convert DBML to PostgreSQL
sql-splitter convert schema.dbml --to postgres -o schema.sql

# Convert DBML to SQLite
sql-splitter convert schema.dbml --to sqlite -o schema.sql

# Convert DBML to MSSQL
sql-splitter convert schema.dbml --to mssql -o schema.sql

# Dry-run to preview conversion
sql-splitter convert schema.dbml --to mysql --dry-run
```

---

## CLI Options

### Export Options (graph command)

| Flag                | Description                                        | Default             |
| ------------------- | -------------------------------------------------- | ------------------- |
| `--format dbml`     | Output format                                      | (required for DBML) |
| `--include-notes`   | Include table/column comments as DBML notes        | `false`             |
| `--include-indexes` | Include index definitions                          | `true`              |
| `--group-by-schema` | Group tables by schema prefix                      | `true`              |
| `--table-groups`    | Generate TableGroup definitions for related tables | `false`             |

### Import Options (convert command)

| Flag              | Description                                        | Default    |
| ----------------- | -------------------------------------------------- | ---------- |
| `--from dbml`     | Source format (auto-detected from .dbml extension) | auto       |
| `--to <dialect>`  | Target SQL dialect                                 | (required) |
| `--schema-prefix` | Add schema prefix to all tables                    | none       |
| `--enum-style`    | How to handle enums: `native`, `check`, `varchar`  | `native`   |

---

## DBML Feature Mapping

### Supported DBML Elements

| DBML Element        | Export Support | Import Support | Notes                        |
| ------------------- | -------------- | -------------- | ---------------------------- |
| `Table`             | ✅ Full        | ✅ Full        | Schema prefix support        |
| `Column`            | ✅ Full        | ✅ Full        | Types, constraints           |
| `Primary Key`       | ✅ Full        | ✅ Full        | Single and composite         |
| `Foreign Key (Ref)` | ✅ Full        | ✅ Full        | All cardinalities            |
| `Indexes`           | ✅ Full        | ✅ Full        | Unique, composite, types     |
| `Enum`              | ✅ Full        | ✅ Full        | Per-dialect handling         |
| `Note`              | ⚠️ Partial     | ⚠️ Partial     | As SQL comments              |
| `TableGroup`        | ⚠️ Export only | ❌ Ignored     | Organizational only          |
| `TablePartial`      | ❌ N/A         | ⚠️ Expanded    | Templates expanded on import |
| `Project`           | ❌ N/A         | ❌ Ignored     | Metadata only                |

### Type Mapping

DBML uses generic types that map to dialect-specific types:

| DBML Type      | MySQL          | PostgreSQL     | SQLite    | MSSQL              |
| -------------- | -------------- | -------------- | --------- | ------------------ |
| `int`          | `INT`          | `INTEGER`      | `INTEGER` | `INT`              |
| `bigint`       | `BIGINT`       | `BIGINT`       | `INTEGER` | `BIGINT`           |
| `varchar`      | `VARCHAR(n)`   | `VARCHAR(n)`   | `TEXT`    | `NVARCHAR(n)`      |
| `text`         | `TEXT`         | `TEXT`         | `TEXT`    | `NVARCHAR(MAX)`    |
| `boolean`      | `TINYINT(1)`   | `BOOLEAN`      | `INTEGER` | `BIT`              |
| `timestamp`    | `TIMESTAMP`    | `TIMESTAMP`    | `TEXT`    | `DATETIME2`        |
| `date`         | `DATE`         | `DATE`         | `TEXT`    | `DATE`             |
| `decimal(p,s)` | `DECIMAL(p,s)` | `DECIMAL(p,s)` | `REAL`    | `DECIMAL(p,s)`     |
| `blob`         | `BLOB`         | `BYTEA`        | `BLOB`    | `VARBINARY(MAX)`   |
| `uuid`         | `CHAR(36)`     | `UUID`         | `TEXT`    | `UNIQUEIDENTIFIER` |
| `json`         | `JSON`         | `JSONB`        | `TEXT`    | `NVARCHAR(MAX)`    |

### Relationship Mapping

DBML relationship syntax to FK constraints:

| DBML Syntax           | Cardinality  | FK Direction            |
| --------------------- | ------------ | ----------------------- |
| `Ref: A.id < B.a_id`  | One-to-Many  | B references A          |
| `Ref: A.id > B.a_id`  | Many-to-One  | A references B          |
| `Ref: A.id - B.a_id`  | One-to-One   | B references A (unique) |
| `Ref: A.id <> B.a_id` | Many-to-Many | Junction table needed   |

---

## Implementation Architecture

### Directory Structure

```
src/
├── dbml/
│   ├── mod.rs           # Module exports
│   ├── parser.rs        # DBML text parser
│   ├── model.rs         # DBML AST types
│   ├── export.rs        # Schema → DBML conversion
│   └── import.rs        # DBML → Schema conversion
├── graph/
│   └── format/
│       └── dbml.rs      # DBML output formatter (new)
└── convert/
    └── dbml.rs          # DBML input handling (new)
```

### Key Data Structures

```rust
// src/dbml/model.rs - DBML AST

pub struct DbmlDocument {
    pub project: Option<Project>,
    pub enums: Vec<DbmlEnum>,
    pub tables: Vec<DbmlTable>,
    pub refs: Vec<DbmlRef>,
    pub table_groups: Vec<TableGroup>,
}

pub struct DbmlTable {
    pub schema: Option<String>,
    pub name: String,
    pub alias: Option<String>,
    pub columns: Vec<DbmlColumn>,
    pub indexes: Vec<DbmlIndex>,
    pub note: Option<String>,
}

pub struct DbmlColumn {
    pub name: String,
    pub col_type: String,
    pub settings: ColumnSettings,
    pub note: Option<String>,
}

pub struct ColumnSettings {
    pub primary_key: bool,
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<DefaultValue>,
    pub increment: bool,
    pub ref_: Option<InlineRef>,
}

#[derive(Clone)]
pub enum DefaultValue {
    Literal(String),       // 'value' or 123
    Expression(String),    // `now()`
    Boolean(bool),
    Null,
}

pub struct DbmlRef {
    pub name: Option<String>,
    pub from: RefEndpoint,
    pub to: RefEndpoint,
    pub cardinality: Cardinality,
    pub settings: RefSettings,
}

pub struct RefEndpoint {
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Clone, Copy)]
pub enum Cardinality {
    OneToOne,    // -
    OneToMany,   // <
    ManyToOne,   // >
    ManyToMany,  // <>
}

pub struct RefSettings {
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

pub struct DbmlIndex {
    pub columns: Vec<IndexColumn>,
    pub settings: IndexSettings,
}

pub struct IndexSettings {
    pub name: Option<String>,
    pub unique: bool,
    pub pk: bool,
    pub index_type: Option<String>, // btree, hash, gin, etc.
    pub note: Option<String>,
}

pub struct DbmlEnum {
    pub schema: Option<String>,
    pub name: String,
    pub values: Vec<EnumValue>,
}

pub struct EnumValue {
    pub name: String,
    pub note: Option<String>,
}
```

### Parser Implementation

```rust
// src/dbml/parser.rs - Recursive descent parser

pub struct DbmlParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> DbmlParser<'a> {
    pub fn parse(input: &str) -> Result<DbmlDocument> {
        let mut parser = DbmlParser { input, pos: 0 };
        parser.parse_document()
    }

    fn parse_document(&mut self) -> Result<DbmlDocument> {
        let mut doc = DbmlDocument::default();

        while !self.is_eof() {
            self.skip_whitespace_and_comments();

            match self.peek_keyword()? {
                "Project" => doc.project = Some(self.parse_project()?),
                "Enum" => doc.enums.push(self.parse_enum()?),
                "Table" => doc.tables.push(self.parse_table()?),
                "TablePartial" => {
                    let partial = self.parse_table_partial()?;
                    doc.partials.insert(partial.name.clone(), partial);
                }
                "TableGroup" => doc.table_groups.push(self.parse_table_group()?),
                "Ref" => doc.refs.push(self.parse_ref()?),
                _ => return Err(anyhow!("Unexpected token at position {}", self.pos)),
            }
        }

        Ok(doc)
    }

    fn parse_table(&mut self) -> Result<DbmlTable> {
        self.expect_keyword("Table")?;
        let (schema, name) = self.parse_table_name()?;
        let alias = self.parse_optional_alias()?;
        self.expect_char('{')?;

        let mut columns = Vec::new();
        let mut indexes = Vec::new();
        let mut note = None;

        while !self.check_char('}') {
            self.skip_whitespace_and_comments();

            if self.check_keyword("indexes") {
                indexes = self.parse_indexes_block()?;
            } else if self.check_keyword("Note") {
                note = Some(self.parse_note()?);
            } else if !self.check_char('}') {
                columns.push(self.parse_column()?);
            }
        }

        self.expect_char('}')?;

        Ok(DbmlTable { schema, name, alias, columns, indexes, note })
    }

    // ... additional parsing methods
}
```

### Export Implementation

```rust
// src/graph/format/dbml.rs

use crate::schema::{Schema, TableSchema, Column, ForeignKey};
use crate::graph::view::GraphView;

pub struct DbmlExporter<'a> {
    view: &'a GraphView,
    include_notes: bool,
    include_indexes: bool,
    group_by_schema: bool,
}

impl<'a> DbmlExporter<'a> {
    pub fn export(&self) -> String {
        let mut output = String::new();

        // Export enums first
        for enum_def in &self.view.enums {
            self.write_enum(&mut output, enum_def);
        }

        // Group tables by schema if requested
        let tables_by_schema = self.group_tables();

        for (schema, tables) in tables_by_schema {
            if self.group_by_schema && !schema.is_empty() {
                writeln!(output, "// Schema: {}", schema);
            }

            for table in tables {
                self.write_table(&mut output, table);
            }
        }

        // Export standalone refs (FK relationships)
        for rel in &self.view.relationships {
            self.write_ref(&mut output, rel);
        }

        output
    }

    fn write_table(&self, out: &mut String, table: &TableSchema) {
        // Table header
        if let Some(schema) = &table.schema_name {
            writeln!(out, "Table {}.{} {{", schema, self.quote_name(&table.name));
        } else {
            writeln!(out, "Table {} {{", self.quote_name(&table.name));
        }

        // Columns
        for col in &table.columns {
            self.write_column(out, col, table);
        }

        // Indexes block
        if self.include_indexes && !table.indexes.is_empty() {
            writeln!(out, "\n  indexes {{");
            for idx in &table.indexes {
                self.write_index(out, idx);
            }
            writeln!(out, "  }}");
        }

        // Table note
        if self.include_notes {
            if let Some(note) = &table.comment {
                writeln!(out, "\n  Note: '''{}'''", note);
            }
        }

        writeln!(out, "}}\n");
    }

    fn write_column(&self, out: &mut String, col: &Column, table: &TableSchema) {
        write!(out, "  {} {}", self.quote_name(&col.name), col.data_type);

        let mut settings = Vec::new();

        if col.is_primary_key {
            settings.push("pk".to_string());
        }
        if col.is_auto_increment {
            settings.push("increment".to_string());
        }
        if !col.is_nullable && !col.is_primary_key {
            settings.push("not null".to_string());
        }
        if col.is_unique && !col.is_primary_key {
            settings.push("unique".to_string());
        }
        if let Some(default) = &col.default_value {
            settings.push(format!("default: {}", self.format_default(default)));
        }

        // Inline FK reference
        if let Some(fk) = table.foreign_keys.iter().find(|fk| fk.columns.contains(&col.name)) {
            if fk.columns.len() == 1 {
                settings.push(format!("ref: > {}.{}", fk.ref_table, fk.ref_columns[0]));
            }
        }

        if self.include_notes {
            if let Some(note) = &col.comment {
                settings.push(format!("note: '{}'", note.replace('\'', "\\'")));
            }
        }

        if !settings.is_empty() {
            write!(out, " [{}]", settings.join(", "));
        }

        writeln!(out);
    }

    fn write_ref(&self, out: &mut String, rel: &Relationship) {
        // Only write standalone refs for composite FKs or when not using inline refs
        let symbol = match rel.cardinality {
            Cardinality::OneToOne => "-",
            Cardinality::OneToMany => "<",
            Cardinality::ManyToOne => ">",
            Cardinality::ManyToMany => "<>",
        };

        writeln!(out, "Ref: {}.{} {} {}.{}",
            rel.from_table, rel.from_column,
            symbol,
            rel.to_table, rel.to_column
        );
    }

    fn quote_name(&self, name: &str) -> String {
        if name.contains(' ') || name.contains('-') || is_reserved_word(name) {
            format!("\"{}\"", name)
        } else {
            name.to_string()
        }
    }
}
```

### Import Implementation

```rust
// src/convert/dbml.rs

use crate::dbml::{DbmlDocument, DbmlTable, Cardinality};
use crate::schema::SqlDialect;

pub struct DbmlImporter {
    target_dialect: SqlDialect,
    schema_prefix: Option<String>,
    enum_style: EnumStyle,
}

#[derive(Clone, Copy)]
pub enum EnumStyle {
    Native,     // Use dialect's native ENUM if available
    Check,      // Use CHECK constraints
    Varchar,    // Use VARCHAR with no constraints
}

impl DbmlImporter {
    pub fn import(&self, doc: &DbmlDocument) -> Result<String> {
        let mut output = String::new();

        // Header comment
        writeln!(output, "-- Generated by sql-splitter from DBML");
        writeln!(output, "-- Target dialect: {:?}", self.target_dialect);
        writeln!(output);

        // Enums (for dialects that support them)
        for enum_def in &doc.enums {
            if let Some(sql) = self.generate_enum(enum_def)? {
                writeln!(output, "{}", sql);
            }
        }

        // Tables in dependency order
        let ordered_tables = self.topological_sort(&doc.tables, &doc.refs)?;

        for table in ordered_tables {
            writeln!(output, "{}", self.generate_create_table(table)?);
        }

        // Standalone FK constraints (for MySQL mode or deferred)
        for ref_ in &doc.refs {
            if self.should_generate_alter_fk(ref_) {
                writeln!(output, "{}", self.generate_alter_fk(ref_)?);
            }
        }

        Ok(output)
    }

    fn generate_create_table(&self, table: &DbmlTable) -> Result<String> {
        let mut sql = String::new();

        let table_name = self.format_table_name(table);
        writeln!(sql, "CREATE TABLE {} (", table_name);

        let mut parts = Vec::new();

        // Columns
        for col in &table.columns {
            parts.push(self.generate_column(col)?);
        }

        // Primary key constraint
        let pk_cols: Vec<_> = table.columns.iter()
            .filter(|c| c.settings.primary_key)
            .map(|c| self.quote_identifier(&c.name))
            .collect();

        if !pk_cols.is_empty() && pk_cols.len() > 1 {
            // Composite PK as table constraint
            parts.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
        }

        // Inline FKs
        for col in &table.columns {
            if let Some(ref_) = &col.settings.ref_ {
                parts.push(self.generate_inline_fk(col, ref_)?);
            }
        }

        // Unique indexes as constraints
        for idx in &table.indexes {
            if idx.settings.unique {
                parts.push(self.generate_unique_constraint(idx)?);
            }
        }

        write!(sql, "{}", parts.join(",\n"));
        writeln!(sql, "\n){};", self.table_options());

        // Non-unique indexes as separate statements
        for idx in &table.indexes {
            if !idx.settings.unique && !idx.settings.pk {
                writeln!(sql, "{}", self.generate_create_index(&table.name, idx)?);
            }
        }

        Ok(sql)
    }

    fn generate_column(&self, col: &DbmlColumn) -> Result<String> {
        let mut parts = Vec::new();

        parts.push(self.quote_identifier(&col.name));
        parts.push(self.map_type(&col.col_type)?);

        if col.settings.not_null || col.settings.primary_key {
            parts.push("NOT NULL".to_string());
        }

        if col.settings.primary_key && !self.has_composite_pk() {
            parts.push(self.primary_key_syntax());
        }

        if col.settings.increment {
            parts.push(self.auto_increment_syntax());
        }

        if col.settings.unique && !col.settings.primary_key {
            parts.push("UNIQUE".to_string());
        }

        if let Some(default) = &col.settings.default {
            parts.push(format!("DEFAULT {}", self.format_default(default)?));
        }

        Ok(format!("  {}", parts.join(" ")))
    }

    fn map_type(&self, dbml_type: &str) -> Result<String> {
        // Parse generic DBML type and map to dialect-specific
        let (base_type, params) = parse_type_params(dbml_type)?;

        let mapped = match (self.target_dialect, base_type.to_lowercase().as_str()) {
            // Integer types
            (_, "int" | "integer") => "INTEGER",
            (_, "bigint") => "BIGINT",
            (_, "smallint") => "SMALLINT",
            (SqlDialect::MySQL, "tinyint") => "TINYINT",
            (SqlDialect::PostgreSQL, "tinyint") => "SMALLINT",

            // String types
            (SqlDialect::SQLite, "varchar" | "char") => "TEXT",
            (SqlDialect::MSSQL, "varchar") => "NVARCHAR",
            (_, "varchar") => "VARCHAR",
            (_, "text") => "TEXT",

            // Boolean
            (SqlDialect::MySQL, "boolean" | "bool") => "TINYINT(1)",
            (SqlDialect::PostgreSQL, "boolean" | "bool") => "BOOLEAN",
            (SqlDialect::SQLite, "boolean" | "bool") => "INTEGER",
            (SqlDialect::MSSQL, "boolean" | "bool") => "BIT",

            // ... additional mappings

            _ => dbml_type, // Pass through unknown types
        };

        Ok(if let Some(p) = params {
            format!("{}({})", mapped, p)
        } else {
            mapped.to_string()
        })
    }

    fn auto_increment_syntax(&self) -> String {
        match self.target_dialect {
            SqlDialect::MySQL => "AUTO_INCREMENT".to_string(),
            SqlDialect::PostgreSQL => "GENERATED ALWAYS AS IDENTITY".to_string(),
            SqlDialect::SQLite => "AUTOINCREMENT".to_string(), // Only with INTEGER PRIMARY KEY
            SqlDialect::MSSQL => "IDENTITY(1,1)".to_string(),
        }
    }

    fn quote_identifier(&self, name: &str) -> String {
        match self.target_dialect {
            SqlDialect::MySQL => format!("`{}`", name),
            SqlDialect::PostgreSQL | SqlDialect::SQLite => format!("\"{}\"", name),
            SqlDialect::MSSQL => format!("[{}]", name),
        }
    }
}
```

---

## Edge Cases

### 1. Many-to-Many Relationships

**Problem**: DBML supports `<>` cardinality but SQL requires junction tables.

**Solution**:

- **Export**: Detect junction tables (2 FKs, possibly composite PK) and emit `<>` ref
- **Import**: Generate junction table with `--expand-many-to-many` flag (optional)

```dbml
// DBML input
Ref: users.id <> roles.id

// Generated SQL (with --expand-many-to-many)
CREATE TABLE users_roles (
  user_id INT NOT NULL REFERENCES users(id),
  role_id INT NOT NULL REFERENCES roles(id),
  PRIMARY KEY (user_id, role_id)
);
```

### 2. Enum Handling

**Problem**: Different dialects handle enums differently.

**Solution**: Per-dialect enum generation with `--enum-style` flag.

```dbml
Enum status_type {
  pending
  active
  archived
}
```

| Dialect    | `--enum-style native`                          | `--enum-style check`      |
| ---------- | ---------------------------------------------- | ------------------------- |
| PostgreSQL | `CREATE TYPE status_type AS ENUM (...)`        | `CHECK (status IN (...))` |
| MySQL      | `ENUM('pending', 'active', 'archived')` inline | `CHECK (status IN (...))` |
| SQLite     | N/A (use check)                                | `CHECK (status IN (...))` |
| MSSQL      | N/A (use check)                                | `CHECK (status IN (...))` |

### 3. Reserved Words

**Problem**: Table/column names may conflict with SQL reserved words.

**Solution**: Always quote identifiers that match known reserved words or contain special characters.

### 4. Schema Prefixes

**Problem**: DBML supports `schema.table` notation but not all operations need it.

**Solution**:

- Export preserves schema prefixes from source dump
- Import uses `--schema-prefix` to add prefix, or strips it with `--no-schema`

### 5. Composite Foreign Keys

**Problem**: DBML inline refs only support single-column FKs.

**Solution**: Use standalone `Ref` definitions for composite FKs.

```dbml
// Composite FK
Ref: order_items.(order_id, product_id) > orders.(id, product_id)
```

### 6. Expression Defaults

**Problem**: Default expressions like `now()` vary by dialect.

**Solution**: Map common expressions on import:

| DBML     | MySQL               | PostgreSQL          | SQLite              | MSSQL       |
| -------- | ------------------- | ------------------- | ------------------- | ----------- |
| `now()`  | `CURRENT_TIMESTAMP` | `now()`             | `CURRENT_TIMESTAMP` | `GETDATE()` |
| `uuid()` | `UUID()`            | `gen_random_uuid()` | N/A                 | `NEWID()`   |

---

## Performance Considerations

| File Size | Tables | Parse Time | Export Time | Import Time |
| --------- | ------ | ---------- | ----------- | ----------- |
| 100 KB    | ~50    | <10 ms     | <5 ms       | <10 ms      |
| 1 MB      | ~200   | <50 ms     | <20 ms      | <50 ms      |
| 10 MB     | ~500   | <200 ms    | <100 ms     | <200 ms     |

DBML files are schema-only (no data), so they're inherently small. Performance is not a concern.

**Memory usage**: O(tables + columns + relationships) - negligible compared to data operations.

---

## Testing Strategy

### Unit Tests

- **Parser tests**: Valid DBML documents, syntax errors, edge cases
- **Type mapping tests**: All supported types for each dialect
- **Identifier quoting tests**: Reserved words, special characters
- **Ref parsing tests**: All cardinality types, inline and standalone

### Integration Tests

- **Roundtrip tests**: SQL → DBML → SQL produces equivalent schema
- **Cross-dialect tests**: DBML → MySQL, PostgreSQL, SQLite, MSSQL
- **Real-world schemas**: Parse dbdiagram.io samples

### Golden File Tests

```
tests/fixtures/dbml/
├── input/
│   ├── basic.dbml
│   ├── enums.dbml
│   ├── composite_pk.dbml
│   ├── many_to_many.dbml
│   └── complex_schema.dbml
└── expected/
    ├── mysql/
    ├── postgres/
    ├── sqlite/
    └── mssql/
```

### Edge Case Tests

1. Empty tables (no columns)
2. Tables with only PKs
3. Self-referential FKs
4. Circular FK references
5. Unicode table/column names
6. Very long identifiers
7. All-reserved-word names

---

## Example Workflows

### 1. Documentation from Production Schema

```bash
# Export production MySQL dump to DBML for documentation
pg_dump mydb --schema-only | sql-splitter graph - --format dbml -o schema.dbml

# Upload to dbdiagram.io or commit to git
git add schema.dbml && git commit -m "Update schema documentation"
```

### 2. Schema-as-Code Workflow

```bash
# Team edits schema.dbml directly
vim schema.dbml

# Generate SQL for each environment
sql-splitter convert schema.dbml --to postgres -o migrations/schema.sql
sql-splitter convert schema.dbml --to sqlite -o test/schema.sql

# Diff to verify changes
sql-splitter diff old_schema.sql migrations/schema.sql
```

### 3. Cross-Database Migration Planning

```bash
# Export existing Oracle schema to DBML
sql-splitter graph oracle_dump.sql --format dbml -o schema.dbml

# Edit DBML to adjust for PostgreSQL (manual step)
vim schema.dbml

# Generate PostgreSQL DDL
sql-splitter convert schema.dbml --to postgres -o pg_schema.sql
```

---

## Effort Estimate

| Component                          | Effort        |
| ---------------------------------- | ------------- |
| DBML parser (recursive descent)    | 10 hours      |
| DBML model types                   | 2 hours       |
| Export formatter (graph command)   | 6 hours       |
| Import converter (convert command) | 8 hours       |
| Type mapping tables                | 3 hours       |
| CLI integration                    | 2 hours       |
| Unit tests                         | 3 hours       |
| Integration tests                  | 2 hours       |
| Documentation                      | 2 hours       |
| **Total**                          | **~35 hours** |

---

## Future Enhancements

1. **TableGroup generation**: Automatically group related tables by prefix or FK clustering
2. **Note extraction**: Parse SQL comments as DBML notes
3. **Color/styling hints**: Export table colors for dbdiagram.io (via extended syntax)
4. **Partial templates**: Support DBML TablePartial for common column patterns
5. **Project metadata**: Preserve Project block with database name, note
6. **Bidirectional sync**: Detect drift between DBML and SQL schemas

---

## Limitations

- **No data export**: DBML is schema-only; use `sample` for data
- **No stored procedures**: DBML doesn't support procedures/functions
- **No triggers**: DBML doesn't represent triggers
- **No views**: DBML doesn't support view definitions
- **Expression limits**: Complex CHECK constraints may not roundtrip perfectly
- **Dialect-specific features**: Some features (e.g., PostgreSQL arrays) have no DBML equivalent

---

## Related Documents

- [Graph Feature](../archived/GRAPH_FEATURE.md) - Existing ERD generation
- [Convert Feature](../archived/CONVERT_FEASIBILITY.md) - Dialect conversion architecture
- [Enum Conversion](ENUM_CONVERSION.md) - Related enum handling
- [ROADMAP.md](../ROADMAP.md) - Version planning

---

## References

- [DBML Documentation](https://dbml.dbdiagram.io/docs/) - Official DBML spec
- [DBML Database Support](https://dbml.dbdiagram.io/database-support) - Supported databases
- [@dbml/core](https://github.com/holistics/dbml) - Reference implementation
- [dbdiagram.io](https://dbdiagram.io/) - Visual DBML editor
