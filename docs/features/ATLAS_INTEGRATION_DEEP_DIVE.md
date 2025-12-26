# Atlas Schema-as-Code Integration: Deep Dive

**Date**: 2025-12-24
**Feature**: Atlas Integration (v1.18.0)
**Effort**: 20 hours
**Priority**: Tier 1 (High Impact, Medium Effort)

## Table of Contents

1. [What is Atlas?](#what-is-atlas)
2. [Why Integrate with sql-splitter?](#why-integrate-with-sql-splitter)
3. [Integration Architecture](#integration-architecture)
4. [Implementation Details](#implementation-details)
5. [CLI Interface Design](#cli-interface-design)
6. [SQL to HCL Conversion](#sql-to-hcl-conversion)
7. [Migration Generation](#migration-generation)
8. [Use Cases](#use-cases)
9. [Challenges and Solutions](#challenges-and-solutions)
10. [Effort Breakdown](#effort-breakdown)

---

## What is Atlas?

**Atlas** is an open-source schema-as-code tool that manages database schemas using a declarative language (HCL) and automatically generates migrations.

### Key Concepts

```hcl
// schema.hcl - Atlas schema definition
table "users" {
  schema = schema.mydb

  column "id" {
    type = int
    auto_increment = true
  }

  column "email" {
    type = varchar(255)
    null = false
  }

  column "created_at" {
    type = timestamp
    default = sql("CURRENT_TIMESTAMP")
  }

  primary_key {
    columns = [column.id]
  }

  index "idx_email" {
    columns = [column.email]
    unique = true
  }
}

table "orders" {
  schema = schema.mydb

  column "id" {
    type = int
  }

  column "user_id" {
    type = int
  }

  foreign_key "fk_user" {
    columns = [column.user_id]
    ref_columns = [table.users.column.id]
    on_delete = CASCADE
  }
}
```

### Core Features

1. **Declarative Schema**: Define schema in HCL (HashiCorp Configuration Language)
2. **Migration Generation**: Automatically generate SQL migrations from schema changes
3. **Schema Inspection**: Import existing database schemas
4. **Validation**: Check migrations for safety, breaking changes
5. **Multi-Dialect**: MySQL, PostgreSQL, SQLite, SQL Server
6. **CI/CD Integration**: GitHub Actions, GitLab CI

### Why Atlas?

- **Infrastructure-as-Code for databases**: Same principles as Terraform
- **Type-safe**: Schema is validated, auto-completed in IDEs
- **Declarative**: Define desired state, Atlas generates migrations
- **Safety**: Detects destructive changes, suggests alternatives
- **Version control**: Schema lives in git alongside code

### Atlas Workflow

```bash
# 1. Define schema
cat > schema.hcl <<EOF
table "users" {
  column "id" { type = int }
  column "email" { type = varchar(255) }
}
EOF

# 2. Generate migration from current DB to desired schema
atlas migrate diff \
  --from "mysql://localhost/prod" \
  --to "file://schema.hcl" \
  --format "{{ sql . }}"

# Output: 20240101120000_add_users.sql
# CREATE TABLE users (id INT, email VARCHAR(255));

# 3. Apply migration
atlas migrate apply --url "mysql://localhost/prod"

# 4. Schema changes tracked in migration history
atlas migrate status
```

---

## Why Integrate with sql-splitter?

### The Problem

**Schema management pain points**:

1. **Reverse engineering**: Production DB → HCL schema for Atlas
   - Requires manual conversion or running Atlas against live DB
   - Can't generate from offline SQL dumps

2. **Testing migrations**: Need realistic test data
   - Atlas generates migrations but doesn't provide test data
   - Manual test data creation is tedious

3. **Migration validation**: Ensure migrations work on actual data
   - Atlas checks schema safety but not data compatibility
   - Need to test against production-like dumps

4. **Dump to IaC**: Legacy dumps → modern schema-as-code
   - Old projects have SQL dumps but no schema definitions
   - Migration to Atlas requires manual conversion

### The Opportunity

**sql-splitter + Atlas = Complete Schema Management**

```bash
# 1. Convert SQL dump → Atlas HCL
sql-splitter atlas-export dump.sql -o schema.hcl

# 2. Use Atlas for schema evolution
atlas migrate diff --from file://schema.hcl --to file://schema_v2.hcl

# 3. Test migration against dump
sql-splitter atlas-test-migration dump.sql migration.sql

# 4. Generate test data from schema
sql-splitter atlas-generate-data schema.hcl -o test_data.sql
```

### Value Propositions

1. **Offline schema export**: Generate Atlas HCL from dumps without DB access
2. **Migration testing**: Validate Atlas migrations against real dump data
3. **Test data generation**: Create realistic data matching Atlas schema
4. **Hybrid workflow**: Bridge between dumps (legacy) and IaC (modern)
5. **Schema documentation**: HCL is more readable than raw SQL

---

## Integration Architecture

### Three Integration Points

```
sql-splitter <--> Atlas
     │              │
     ├──────────────┤
     │              │
     1. Export      2. Test      3. Generate
     Dump → HCL     Migration    Data ← HCL
```

### 1. Export: SQL Dump → Atlas HCL

```bash
sql-splitter atlas-export dump.sql -o schema.hcl

# Converts CREATE TABLE statements to Atlas HCL format
```

**Implementation**: Parser AST → HCL generator

### 2. Test: Validate Atlas Migration

```bash
atlas migrate diff old_schema.hcl new_schema.hcl -o migration.sql
sql-splitter atlas-test dump.sql migration.sql

# Tests:
# - Migration applies cleanly
# - No data loss
# - Constraints still valid
# - Performance impact
```

**Implementation**: Apply migration to temp DB with dump data

### 3. Generate: Atlas HCL → Test Data

```bash
sql-splitter atlas-generate schema.hcl -o data.sql --rows 1000

# Generates INSERT statements matching schema constraints
```

**Implementation**: Reuse redact fake generators with schema constraints

---

## Implementation Details

### Core Data Structures

```rust
// src/integrations/atlas/types.rs

use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasSchema {
    pub tables: Vec<AtlasTable>,
    pub schemas: Vec<AtlasSchemaDecl>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasTable {
    pub name: String,
    pub schema: String,
    pub columns: Vec<AtlasColumn>,
    pub primary_key: Option<AtlasPrimaryKey>,
    pub foreign_keys: Vec<AtlasForeignKey>,
    pub indexes: Vec<AtlasIndex>,
    pub checks: Vec<AtlasCheck>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasColumn {
    pub name: String,
    pub r#type: AtlasType,
    pub null: bool,
    pub default: Option<AtlasDefault>,
    pub auto_increment: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AtlasType {
    Simple(String),           // "int", "varchar(255)"
    Qualified {               // More complex types
        r#type: String,
        size: Option<i32>,
        precision: Option<i32>,
        scale: Option<i32>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AtlasDefault {
    Literal(String),          // "active"
    Sql(String),              // sql("CURRENT_TIMESTAMP")
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasPrimaryKey {
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub r#type: Option<String>, // BTREE, HASH, etc.
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtlasCheck {
    pub name: String,
    pub expr: String,
}
```

### SQL to HCL Converter

```rust
// src/integrations/atlas/converter.rs

use crate::parser::{Statement, CreateTableStatement, ColumnDef};

pub struct AtlasConverter {
    dialect: Dialect,
}

impl AtlasConverter {
    pub fn convert_dump(&self, dump: &ParsedDump) -> Result<AtlasSchema> {
        let mut schema = AtlasSchema {
            tables: Vec::new(),
            schemas: vec![self.default_schema()],
        };

        for stmt in &dump.statements {
            if let Statement::CreateTable(create_table) = stmt {
                schema.tables.push(self.convert_table(create_table)?);
            }
        }

        Ok(schema)
    }

    fn convert_table(&self, create_table: &CreateTableStatement) -> Result<AtlasTable> {
        let mut table = AtlasTable {
            name: create_table.name.clone(),
            schema: "schema.mydb".to_string(), // Default schema
            columns: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            checks: Vec::new(),
        };

        // Convert columns
        for column in &create_table.columns {
            table.columns.push(self.convert_column(column)?);
        }

        // Extract constraints
        for constraint in &create_table.constraints {
            match constraint {
                TableConstraint::PrimaryKey(cols) => {
                    table.primary_key = Some(AtlasPrimaryKey {
                        columns: cols.clone(),
                    });
                }

                TableConstraint::ForeignKey { name, columns, ref_table, ref_columns, on_delete, on_update } => {
                    table.foreign_keys.push(AtlasForeignKey {
                        name: name.clone().unwrap_or_else(|| format!("fk_{}", columns.join("_"))),
                        columns: columns.clone(),
                        ref_table: ref_table.clone(),
                        ref_columns: ref_columns.clone(),
                        on_delete: on_delete.clone(),
                        on_update: on_update.clone(),
                    });
                }

                TableConstraint::Unique { name, columns } => {
                    table.indexes.push(AtlasIndex {
                        name: name.clone().unwrap_or_else(|| format!("idx_{}", columns.join("_"))),
                        columns: columns.clone(),
                        unique: true,
                        r#type: None,
                    });
                }

                TableConstraint::Index { name, columns, index_type } => {
                    table.indexes.push(AtlasIndex {
                        name: name.clone(),
                        columns: columns.clone(),
                        unique: false,
                        r#type: index_type.clone(),
                    });
                }

                TableConstraint::Check { name, expr } => {
                    table.checks.push(AtlasCheck {
                        name: name.clone().unwrap_or_else(|| format!("chk_{}", table.name)),
                        expr: expr.to_string(),
                    });
                }
            }
        }

        Ok(table)
    }

    fn convert_column(&self, column: &ColumnDef) -> Result<AtlasColumn> {
        Ok(AtlasColumn {
            name: column.name.clone(),
            r#type: self.convert_type(&column.data_type)?,
            null: !column.constraints.contains(&ColumnConstraint::NotNull),
            default: column.default.as_ref().map(|d| self.convert_default(d)),
            auto_increment: column.constraints.contains(&ColumnConstraint::AutoIncrement),
            comment: column.comment.clone(),
        })
    }

    fn convert_type(&self, data_type: &DataType) -> Result<AtlasType> {
        match data_type {
            DataType::Int => Ok(AtlasType::Simple("int".to_string())),
            DataType::BigInt => Ok(AtlasType::Simple("bigint".to_string())),
            DataType::VarChar(size) => Ok(AtlasType::Simple(format!("varchar({})", size))),
            DataType::Text => Ok(AtlasType::Simple("text".to_string())),
            DataType::Decimal { precision, scale } => {
                Ok(AtlasType::Simple(format!("decimal({},{})", precision, scale)))
            }
            DataType::DateTime => Ok(AtlasType::Simple("datetime".to_string())),
            DataType::Timestamp => Ok(AtlasType::Simple("timestamp".to_string())),
            DataType::Boolean => Ok(AtlasType::Simple("boolean".to_string())),
            DataType::Json => Ok(AtlasType::Simple("json".to_string())),
            DataType::Enum(values) => {
                // Atlas represents enums as CHECK constraints
                Ok(AtlasType::Simple(format!("varchar(255)")))
            }
            _ => bail!("Unsupported type: {:?}", data_type),
        }
    }

    fn convert_default(&self, default: &DefaultValue) -> AtlasDefault {
        match default {
            DefaultValue::Literal(lit) => AtlasDefault::Literal(lit.clone()),
            DefaultValue::CurrentTimestamp => AtlasDefault::Sql("CURRENT_TIMESTAMP".to_string()),
            DefaultValue::Null => AtlasDefault::Literal("NULL".to_string()),
            DefaultValue::Expression(expr) => AtlasDefault::Sql(expr.clone()),
        }
    }
}
```

### HCL Generator

```rust
// src/integrations/atlas/hcl_writer.rs

pub struct HclWriter {
    indent_level: usize,
}

impl HclWriter {
    pub fn write_schema(&mut self, schema: &AtlasSchema) -> Result<String> {
        let mut output = String::new();

        // Schema declaration
        for schema_decl in &schema.schemas {
            output.push_str(&self.write_schema_decl(schema_decl));
            output.push('\n');
        }

        // Tables
        for table in &schema.tables {
            output.push_str(&self.write_table(table)?);
            output.push_str("\n\n");
        }

        Ok(output)
    }

    fn write_table(&mut self, table: &AtlasTable) -> Result<String> {
        let mut output = String::new();

        output.push_str(&format!("table \"{}\" {{\n", table.name));
        self.indent_level += 1;

        // Schema reference
        self.write_line(&mut output, &format!("schema = schema.{}", table.schema.replace("schema.", "")));

        // Columns
        for column in &table.columns {
            output.push('\n');
            output.push_str(&self.write_column(column)?);
        }

        // Primary key
        if let Some(pk) = &table.primary_key {
            output.push('\n');
            output.push_str(&self.write_primary_key(pk)?);
        }

        // Foreign keys
        for fk in &table.foreign_keys {
            output.push('\n');
            output.push_str(&self.write_foreign_key(fk)?);
        }

        // Indexes
        for index in &table.indexes {
            output.push('\n');
            output.push_str(&self.write_index(index)?);
        }

        // Check constraints
        for check in &table.checks {
            output.push('\n');
            output.push_str(&self.write_check(check)?);
        }

        self.indent_level -= 1;
        output.push_str("}\n");

        Ok(output)
    }

    fn write_column(&mut self, column: &AtlasColumn) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.indent());
        output.push_str(&format!("column \"{}\" {{\n", column.name));
        self.indent_level += 1;

        // Type
        let type_str = match &column.r#type {
            AtlasType::Simple(s) => s.clone(),
            AtlasType::Qualified { r#type, size, .. } => {
                if let Some(sz) = size {
                    format!("{}({})", r#type, sz)
                } else {
                    r#type.clone()
                }
            }
        };
        self.write_line(&mut output, &format!("type = {}", type_str));

        // Null
        if !column.null {
            self.write_line(&mut output, "null = false");
        }

        // Default
        if let Some(default) = &column.default {
            let default_str = match default {
                AtlasDefault::Literal(lit) => format!("\"{}\"", lit),
                AtlasDefault::Sql(sql) => format!("sql(\"{}\")", sql),
            };
            self.write_line(&mut output, &format!("default = {}", default_str));
        }

        // Auto increment
        if column.auto_increment {
            self.write_line(&mut output, "auto_increment = true");
        }

        // Comment
        if let Some(comment) = &column.comment {
            self.write_line(&mut output, &format!("comment = \"{}\"", comment));
        }

        self.indent_level -= 1;
        output.push_str(&self.indent());
        output.push_str("}\n");

        Ok(output)
    }

    fn write_primary_key(&mut self, pk: &AtlasPrimaryKey) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.indent());
        output.push_str("primary_key {\n");
        self.indent_level += 1;

        let columns = pk.columns
            .iter()
            .map(|c| format!("column.{}", c))
            .collect::<Vec<_>>()
            .join(", ");

        self.write_line(&mut output, &format!("columns = [{}]", columns));

        self.indent_level -= 1;
        output.push_str(&self.indent());
        output.push_str("}\n");

        Ok(output)
    }

    fn write_foreign_key(&mut self, fk: &AtlasForeignKey) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.indent());
        output.push_str(&format!("foreign_key \"{}\" {{\n", fk.name));
        self.indent_level += 1;

        // Columns
        let columns = fk.columns
            .iter()
            .map(|c| format!("column.{}", c))
            .collect::<Vec<_>>()
            .join(", ");
        self.write_line(&mut output, &format!("columns = [{}]", columns));

        // Reference columns
        let ref_columns = fk.ref_columns
            .iter()
            .map(|c| format!("table.{}.column.{}", fk.ref_table, c))
            .collect::<Vec<_>>()
            .join(", ");
        self.write_line(&mut output, &format!("ref_columns = [{}]", ref_columns));

        // On delete
        if let Some(on_delete) = &fk.on_delete {
            self.write_line(&mut output, &format!("on_delete = {}", self.format_action(on_delete)));
        }

        // On update
        if let Some(on_update) = &fk.on_update {
            self.write_line(&mut output, &format!("on_update = {}", self.format_action(on_update)));
        }

        self.indent_level -= 1;
        output.push_str(&self.indent());
        output.push_str("}\n");

        Ok(output)
    }

    fn write_index(&mut self, index: &AtlasIndex) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.indent());
        output.push_str(&format!("index \"{}\" {{\n", index.name));
        self.indent_level += 1;

        // Columns
        let columns = index.columns
            .iter()
            .map(|c| format!("column.{}", c))
            .collect::<Vec<_>>()
            .join(", ");
        self.write_line(&mut output, &format!("columns = [{}]", columns));

        // Unique
        if index.unique {
            self.write_line(&mut output, "unique = true");
        }

        // Type
        if let Some(r#type) = &index.r#type {
            self.write_line(&mut output, &format!("type = {}", r#type));
        }

        self.indent_level -= 1;
        output.push_str(&self.indent());
        output.push_str("}\n");

        Ok(output)
    }

    fn write_check(&mut self, check: &AtlasCheck) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.indent());
        output.push_str(&format!("check \"{}\" {{\n", check.name));
        self.indent_level += 1;

        self.write_line(&mut output, &format!("expr = \"{}\"", check.expr));

        self.indent_level -= 1;
        output.push_str(&self.indent());
        output.push_str("}\n");

        Ok(output)
    }

    fn indent(&self) -> String {
        "  ".repeat(self.indent_level)
    }

    fn write_line(&self, output: &mut String, line: &str) {
        output.push_str(&self.indent());
        output.push_str(line);
        output.push('\n');
    }

    fn format_action(&self, action: &ReferentialAction) -> String {
        match action {
            ReferentialAction::Cascade => "CASCADE".to_string(),
            ReferentialAction::SetNull => "SET_NULL".to_string(),
            ReferentialAction::SetDefault => "SET_DEFAULT".to_string(),
            ReferentialAction::Restrict => "RESTRICT".to_string(),
            ReferentialAction::NoAction => "NO_ACTION".to_string(),
        }
    }
}
```

### Migration Testing

```rust
// src/integrations/atlas/migration_tester.rs

use std::process::Command;

pub struct MigrationTester {
    temp_db: TempDatabase,
}

impl MigrationTester {
    pub fn test_migration(
        &mut self,
        dump_path: &Path,
        migration_path: &Path,
    ) -> Result<MigrationTestResult> {
        // 1. Create temp database
        self.temp_db.create()?;

        // 2. Import dump
        println!("Importing dump...");
        self.temp_db.import_dump(dump_path)?;

        // 3. Snapshot pre-migration state
        let pre_stats = self.temp_db.collect_stats()?;

        // 4. Apply migration
        println!("Applying migration...");
        let migration_result = self.temp_db.execute_migration(migration_path)?;

        // 5. Validate post-migration state
        let post_stats = self.temp_db.collect_stats()?;

        // 6. Check for data loss
        let data_loss = self.detect_data_loss(&pre_stats, &post_stats)?;

        // 7. Check for constraint violations
        let violations = self.temp_db.check_constraints()?;

        // 8. Cleanup
        self.temp_db.drop()?;

        Ok(MigrationTestResult {
            success: data_loss.is_none() && violations.is_empty(),
            data_loss,
            constraint_violations: violations,
            pre_stats,
            post_stats,
            duration: migration_result.duration,
        })
    }

    fn detect_data_loss(
        &self,
        pre: &DatabaseStats,
        post: &DatabaseStats,
    ) -> Result<Option<DataLoss>> {
        let mut losses = Vec::new();

        // Check row counts
        for (table, pre_count) in &pre.row_counts {
            if let Some(post_count) = post.row_counts.get(table) {
                if post_count < pre_count {
                    losses.push(DataLossItem::RowsDeleted {
                        table: table.clone(),
                        before: *pre_count,
                        after: *post_count,
                    });
                }
            } else {
                losses.push(DataLossItem::TableDropped {
                    table: table.clone(),
                    rows_lost: *pre_count,
                });
            }
        }

        // Check column counts
        for (table, pre_cols) in &pre.column_counts {
            if let Some(post_cols) = post.column_counts.get(table) {
                if post_cols < pre_cols {
                    losses.push(DataLossItem::ColumnsDropped {
                        table: table.clone(),
                        before: *pre_cols,
                        after: *post_cols,
                    });
                }
            }
        }

        if losses.is_empty() {
            Ok(None)
        } else {
            Ok(Some(DataLoss { items: losses }))
        }
    }
}

#[derive(Debug)]
pub struct MigrationTestResult {
    pub success: bool,
    pub data_loss: Option<DataLoss>,
    pub constraint_violations: Vec<ConstraintViolation>,
    pub pre_stats: DatabaseStats,
    pub post_stats: DatabaseStats,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct DataLoss {
    pub items: Vec<DataLossItem>,
}

#[derive(Debug)]
pub enum DataLossItem {
    RowsDeleted { table: String, before: usize, after: usize },
    TableDropped { table: String, rows_lost: usize },
    ColumnsDropped { table: String, before: usize, after: usize },
}

#[derive(Debug)]
pub struct ConstraintViolation {
    pub table: String,
    pub constraint: String,
    pub violation_count: usize,
    pub examples: Vec<String>,
}
```

---

## CLI Interface Design

### Command 1: Export to Atlas HCL

```bash
# Basic export
sql-splitter atlas-export dump.sql -o schema.hcl

# Multi-file export (one file per table)
sql-splitter atlas-export dump.sql -o schemas/ --split

# Output:
# schemas/
#   ├── users.hcl
#   ├── orders.hcl
#   └── products.hcl

# Include specific schema name
sql-splitter atlas-export dump.sql -o schema.hcl --schema production

# Filter tables
sql-splitter atlas-export dump.sql -o schema.hcl --tables users,orders
```

### Command 2: Test Migration

```bash
# Test Atlas migration against dump
atlas migrate diff old.hcl new.hcl > migration.sql
sql-splitter atlas-test dump.sql migration.sql

# Output:
# Testing migration against dump...
# ✓ Migration applied successfully (2.3s)
# ✓ No data loss detected
# ✗ Constraint violations found:
#   - orders.fk_user: 5 orphaned rows
#   - products.chk_price: 12 rows violate CHECK (price > 0)
#
# Pre-migration:  1,234,567 rows across 5 tables
# Post-migration: 1,234,567 rows across 5 tables
#
# Recommendation: Fix constraint violations before deploying

# Save report
sql-splitter atlas-test dump.sql migration.sql --report test_report.json

# Test with auto-fix
sql-splitter atlas-test dump.sql migration.sql --fix-violations -o fixed_dump.sql
```

### Command 3: Generate Test Data from Schema

```bash
# Generate INSERT statements from Atlas schema
sql-splitter atlas-generate schema.hcl -o data.sql --rows 1000

# Respects all constraints:
# - Foreign keys (referential integrity)
# - Check constraints (value ranges)
# - Unique constraints (no duplicates)
# - Column types and sizes

# Customize generation
sql-splitter atlas-generate schema.hcl \
  --rows 10000 \
  --locale en_US \
  --seed 42 \
  --strategy realistic

# Output format
sql-splitter atlas-generate schema.hcl --format parquet -o data/
```

---

## SQL to HCL Conversion

### Example Conversion

**Input: MySQL dump**

```sql
CREATE TABLE users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  age INT CHECK (age >= 18 AND age <= 120),
  status ENUM('active', 'inactive', 'suspended') DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  COMMENT 'User accounts'
);

CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  total DECIMAL(10,2) NOT NULL CHECK (total > 0),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  INDEX idx_user_created (user_id, created_at)
);
```

**Output: Atlas HCL**

```hcl
schema "mydb" {
  charset = "utf8mb4"
  collate = "utf8mb4_unicode_ci"
}

table "users" {
  schema = schema.mydb
  comment = "User accounts"

  column "id" {
    type = int
    auto_increment = true
  }

  column "email" {
    type = varchar(255)
    null = false
  }

  column "age" {
    type = int
  }

  column "status" {
    type = varchar(255)
    default = "active"
  }

  column "created_at" {
    type = timestamp
    default = sql("CURRENT_TIMESTAMP")
  }

  primary_key {
    columns = [column.id]
  }

  index "email" {
    columns = [column.email]
    unique = true
  }

  check "age_range" {
    expr = "age >= 18 AND age <= 120"
  }

  check "status_enum" {
    expr = "status IN ('active', 'inactive', 'suspended')"
  }
}

table "orders" {
  schema = schema.mydb

  column "id" {
    type = int
    auto_increment = true
  }

  column "user_id" {
    type = int
    null = false
  }

  column "total" {
    type = decimal(10,2)
    null = false
  }

  column "created_at" {
    type = timestamp
    default = sql("CURRENT_TIMESTAMP")
  }

  primary_key {
    columns = [column.id]
  }

  foreign_key "orders_ibfk_1" {
    columns = [column.user_id]
    ref_columns = [table.users.column.id]
    on_delete = CASCADE
  }

  index "idx_user_created" {
    columns = [column.user_id, column.created_at]
  }

  check "total_positive" {
    expr = "total > 0"
  }
}
```

---

## Migration Generation

While Atlas generates migrations, sql-splitter can **test and validate** them.

### Workflow Integration

```bash
# 1. Export current production dump to HCL
pg_dump prod > prod_dump.sql
sql-splitter atlas-export prod_dump.sql -o schemas/prod.hcl

# 2. Developer updates schema
vim schemas/prod.hcl
# (add new column: users.phone_number)

# 3. Atlas generates migration
atlas migrate diff \
  --from file://schemas/prod.hcl \
  --to file://schemas/dev.hcl \
  > migrations/20250124_add_phone.sql

# 4. Test migration against real dump
sql-splitter atlas-test prod_dump.sql migrations/20250124_add_phone.sql

# 5. If test passes, deploy
atlas migrate apply --url postgresql://prod-db/mydb
```

---

## Use Cases

### 1. Legacy Migration to Infrastructure-as-Code

**Problem**: 10-year-old project with SQL dumps, no schema versioning.

**Solution**:
```bash
# Convert dumps to Atlas HCL
sql-splitter atlas-export legacy_dump.sql -o schemas/v1.hcl

# Now manage schema in git
git add schemas/v1.hcl
git commit -m "feat: import legacy schema to Atlas"

# Future changes tracked via Atlas
```

### 2. Testing Risky Migrations

**Problem**: Dropping column that might have data.

**Solution**:
```bash
# Atlas generates migration
atlas migrate diff old.hcl new.hcl > drop_column.sql

# Test against production dump
sql-splitter atlas-test prod_dump.sql drop_column.sql

# Output:
# ✗ Data loss detected:
#   - users.middle_name: 45,678 non-null values will be lost
#
# Recommendation: Add data migration script before dropping column
```

### 3. Multi-Environment Schema Consistency

**Problem**: Dev, staging, prod have schema drift.

**Solution**:
```bash
# Export all environments
sql-splitter atlas-export dev_dump.sql -o schemas/dev.hcl
sql-splitter atlas-export staging_dump.sql -o schemas/staging.hcl
sql-splitter atlas-export prod_dump.sql -o schemas/prod.hcl

# Diff schemas
diff schemas/dev.hcl schemas/prod.hcl

# Output shows drift:
# - dev has users.test_flag column (not in prod)
# - prod has orders.archived_at (not in dev)
```

### 4. Realistic Test Data Generation

**Problem**: Need test data matching exact production schema constraints.

**Solution**:
```bash
# Export prod schema to HCL
sql-splitter atlas-export prod_dump.sql -o prod_schema.hcl

# Generate conforming test data
sql-splitter atlas-generate prod_schema.hcl \
  --rows 100000 \
  --strategy realistic \
  -o test_data.sql

# Guaranteed to satisfy all FK/PK/CHECK/UNIQUE constraints
```

### 5. Schema Documentation

**Problem**: New developers don't understand database schema.

**Solution**:
```bash
# HCL is more readable than SQL
sql-splitter atlas-export dump.sql -o docs/schema.hcl

# Commit to repo
git add docs/schema.hcl
git commit -m "docs: add schema definition"

# Developers read HCL instead of SQL dumps
```

---

## Challenges and Solutions

### Challenge 1: Dialect Differences

**Problem**: MySQL `AUTO_INCREMENT` vs PostgreSQL `SERIAL` vs Atlas representation.

**Solution**: Normalize to Atlas types.

```rust
fn convert_auto_increment(&self, column: &ColumnDef) -> AtlasColumn {
    match self.dialect {
        Dialect::MySQL => {
            // AUTO_INCREMENT → auto_increment = true
            AtlasColumn {
                auto_increment: column.constraints.contains(&ColumnConstraint::AutoIncrement),
                ..
            }
        }
        Dialect::PostgreSQL => {
            // SERIAL → type = serial, auto_increment not needed
            if matches!(column.data_type, DataType::Serial) {
                AtlasColumn {
                    r#type: AtlasType::Simple("serial".to_string()),
                    auto_increment: false,
                    ..
                }
            } else {
                // Regular column
            }
        }
        _ => ...,
    }
}
```

### Challenge 2: Enum Handling

**Problem**: MySQL has native ENUM, PostgreSQL uses custom types, Atlas uses CHECK constraints.

**MySQL**:
```sql
status ENUM('active', 'inactive')
```

**Atlas HCL**:
```hcl
column "status" {
  type = varchar(255)
}

check "status_values" {
  expr = "status IN ('active', 'inactive')"
}
```

**Solution**: Convert ENUMs to VARCHAR + CHECK constraint.

```rust
fn convert_enum(&self, column: &ColumnDef, values: &[String]) -> Result<(AtlasColumn, AtlasCheck)> {
    let column = AtlasColumn {
        name: column.name.clone(),
        r#type: AtlasType::Simple("varchar(255)".to_string()),
        ..
    };

    let values_str = values.iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<_>>()
        .join(", ");

    let check = AtlasCheck {
        name: format!("{}_enum", column.name),
        expr: format!("{} IN ({})", column.name, values_str),
    };

    Ok((column, check))
}
```

### Challenge 3: Complex Default Values

**Problem**: Atlas distinguishes literal defaults from SQL expression defaults.

**MySQL**:
```sql
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
price DECIMAL DEFAULT 9.99
status VARCHAR DEFAULT 'active'
```

**Atlas HCL**:
```hcl
column "created_at" {
  default = sql("CURRENT_TIMESTAMP")  # SQL expression
}

column "price" {
  default = 9.99  # Literal
}

column "status" {
  default = "active"  # Literal
}
```

**Solution**: Detect SQL functions.

```rust
fn is_sql_function(default_value: &str) -> bool {
    let sql_functions = [
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "NOW()",
        "UUID()",
        "NEWID()",
    ];

    sql_functions.iter().any(|f| default_value.contains(f))
}

fn convert_default(&self, default: &DefaultValue) -> AtlasDefault {
    match default {
        DefaultValue::Literal(lit) => {
            if self.is_sql_function(lit) {
                AtlasDefault::Sql(lit.clone())
            } else {
                AtlasDefault::Literal(lit.clone())
            }
        }
        DefaultValue::CurrentTimestamp => {
            AtlasDefault::Sql("CURRENT_TIMESTAMP".to_string())
        }
        _ => ...,
    }
}
```

### Challenge 4: Foreign Key Cycles

**Problem**: Circular FK references break Atlas schema validation.

**users → orders → shipments → addresses → users**

**Solution**: Atlas handles this naturally with references, no special handling needed.

```hcl
# Atlas allows forward references
table "users" {
  # ...
}

table "orders" {
  foreign_key "fk_user" {
    columns = [column.user_id]
    ref_columns = [table.users.column.id]  # Backward ref OK
  }
}

table "addresses" {
  foreign_key "fk_user" {
    columns = [column.user_id]
    ref_columns = [table.users.column.id]  # Also backward ref
  }
}
```

---

## Effort Breakdown

### Phase 1: SQL → HCL Export (10 hours)

- **Parser AST → Atlas types** (3h)
  - Map SQL types to Atlas types
  - Handle dialect differences
  - Convert constraints to Atlas format

- **HCL writer** (4h)
  - Generate well-formatted HCL
  - Indentation, block structure
  - Comments preservation

- **CLI integration** (2h)
  - `atlas-export` command
  - Output formatting options
  - Multi-file split mode

- **Testing** (1h)
  - Test conversion for MySQL, PostgreSQL, SQLite
  - Validate generated HCL with Atlas CLI
  - Roundtrip testing (SQL → HCL → SQL)

### Phase 2: Migration Testing (6 hours)

- **Temp database management** (2h)
  - Create temp DB (MySQL/PostgreSQL)
  - Import dump
  - Execute migration
  - Cleanup

- **Data loss detection** (2h)
  - Row count comparison
  - Column count tracking
  - Table drop detection

- **Constraint validation** (1h)
  - Check FK integrity post-migration
  - Check constraint violations
  - Report formatting

- **CLI integration** (1h)
  - `atlas-test` command
  - Result reporting (text, JSON)

### Phase 3: Test Data Generation (4 hours)

- **Schema → data generator** (2h)
  - Parse Atlas HCL
  - Extract constraints
  - Feed to existing fake data generators

- **Constraint satisfaction** (1h)
  - FK dependency ordering
  - Unique value generation
  - CHECK constraint compliance

- **CLI integration** (1h)
  - `atlas-generate` command
  - Row count, locale, seed options

**Total: 20 hours**

---

## Next Steps

1. **v1.18.0 Implementation**:
   - Implement SQL → HCL export
   - Add migration testing capability
   - Documentation and examples

2. **v1.19.0 Enhancements**:
   - Test data generation from HCL
   - Schema diff visualization
   - Atlas Cloud integration (schema registry)

3. **Future**:
   - Bidirectional sync (HCL → SQL dump generation)
   - Migration linter (detect anti-patterns)
   - Performance impact prediction

---

**Recommendation**: Implement Atlas integration for v1.18.0. It complements the migrate feature (v1.15.0), provides infrastructure-as-code workflows, and positions sql-splitter as essential for modern schema management. The 20h effort is manageable and delivers high value for teams adopting IaC practices.
