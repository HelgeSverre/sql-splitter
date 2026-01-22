# dbt (data build tool) Integration: Deep Dive

**Date**: 2025-12-24
**Feature**: dbt Integration (v1.19.0)
**Effort**: 28 hours
**Priority**: Tier 1 (High Impact, High Effort)

## Table of Contents

1. [What is dbt?](#what-is-dbt)
2. [Why Integrate with sql-splitter?](#why-integrate-with-sql-splitter)
3. [Integration Architecture](#integration-architecture)
4. [Implementation Details](#implementation-details)
5. [CLI Interface Design](#cli-interface-design)
6. [Dump to dbt Project Conversion](#dump-to-dbt-project-conversion)
7. [Test Generation from Schema](#test-generation-from-schema)
8. [Source Configuration Generation](#source-configuration-generation)
9. [Use Cases](#use-cases)
10. [Challenges and Solutions](#challenges-and-solutions)
11. [Effort Breakdown](#effort-breakdown)

---

## What is dbt?

**dbt (data build tool)** is the industry-standard tool for transforming data in modern data warehouses using SQL and software engineering best practices.

### Core Concepts

```yaml
# dbt_project.yml
name: my_project
version: 1.0.0
profile: postgres

models:
  my_project:
    materialized: table
```

```sql
-- models/staging/stg_users.sql
{{ config(materialized='view') }}

SELECT
    id AS user_id,
    email,
    created_at,
    -- Standardize boolean
    CASE status WHEN 'active' THEN TRUE ELSE FALSE END AS is_active
FROM {{ source('raw', 'users') }}
```

```sql
-- models/marts/fct_orders.sql
{{ config(materialized='table') }}

SELECT
    o.id AS order_id,
    o.user_id,
    u.email AS user_email,
    o.total,
    o.created_at AS order_date
FROM {{ ref('stg_orders') }} AS o
LEFT JOIN {{ ref('stg_users') }} AS u
    ON o.user_id = u.user_id
```

```yaml
# models/schema.yml
models:
  - name: stg_users
    description: Staging layer for user data
    columns:
      - name: user_id
        description: Primary key
        tests:
          - unique
          - not_null

      - name: email
        tests:
          - unique
          - not_null
```

### dbt Workflow

```bash
# 1. Define sources (raw tables)
# sources.yml

# 2. Write models (transformations)
# models/*.sql

# 3. Add tests
# schema.yml

# 4. Run transformations
dbt run  # Execute all models

# 5. Test data quality
dbt test  # Run all tests

# 6. Generate documentation
dbt docs generate
dbt docs serve
```

### Key Features

1. **SQL-based transformations**: Write SELECT, dbt handles CREATE TABLE/VIEW
2. **DAG execution**: Dependency graph (ref/source) determines execution order
3. **Testing**: Built-in tests (unique, not_null, relationships, accepted_values)
4. **Documentation**: Auto-generated lineage diagrams, column-level docs
5. **Incremental models**: Only process new/changed data
6. **Snapshots**: Track historical changes (Type 2 SCD)
7. **Macros**: Reusable Jinja functions

### Why dbt?

- **Industry standard**: Used by thousands of data teams (Airbnb, GitLab, Spotify)
- **Software engineering for data**: Version control, testing, CI/CD
- **Warehouse-native**: Snowflake, BigQuery, Redshift, Databricks, PostgreSQL
- **Modular**: Reusable models, packages (dbt Hub)
- **Discoverable**: Auto-docs make data discoverable across org

---

## Why Integrate with sql-splitter?

### The Problem

**dbt adoption challenges**:

1. **Cold start**: New dbt projects start from scratch
   - Must manually create source configs for all raw tables
   - Tedious YAMLfile creation (100+ tables = hours of work)
   - No initial tests (users must discover what to test)

2. **Legacy database modernization**: Migrating to dbt from stored procedures
   - Existing databases have valuable schema knowledge (constraints, relationships)
   - This knowledge is lost when bootstrapping dbt
   - Must rediscover and redefine all relationships

3. **Test coverage**: Knowing what to test
   - Beginners don't know where to start
   - Advanced users miss edge cases
   - No way to auto-generate baseline tests from schema

4. **Documentation**: Describing what data means
   - Writing descriptions for 100+ tables/columns is overwhelming
   - Schema comments in dumps are ignored
   - Knowledge lives in tribal memory, not docs

### The Opportunity

**sql-splitter + dbt = Instant dbt Bootstrapping**

```bash
# 1. Generate complete dbt project from dump
sql-splitter dbt-init dump.sql -o my_dbt_project/

# Creates:
# - dbt_project.yml
# - models/sources.yml (all tables configured)
# - models/schema.yml (tests auto-generated from constraints)
# - models/staging/*.sql (one model per table)
# - README.md

# 2. Run dbt immediately
cd my_dbt_project/
dbt run   # ✓ Works out of the box!
dbt test  # ✓ 50+ tests auto-generated
dbt docs generate  # ✓ Full documentation

# 3. Iterate and refine
# Developers focus on business logic, not YAML boilerplate
```

### Value Propositions

1. **Zero-to-dbt in minutes**: From dump → production-ready dbt project
2. **Auto-test generation**: Constraints → dbt tests (unique, not_null, relationships)
3. **Schema knowledge transfer**: SQL comments → dbt descriptions
4. **Staging layer scaffolding**: One staging model per source table
5. **Best practices**: Generated project follows dbt conventions
6. **Migration accelerator**: Legacy DB → modern dbt workflow

---

## Integration Architecture

### Three-Layer Output

```
my_dbt_project/
├── dbt_project.yml              # Project config
├── README.md                     # Getting started guide
│
├── models/
│   ├── sources.yml               # Source definitions
│   │
│   ├── staging/                  # Staging layer
│   │   ├── _staging__sources.yml
│   │   ├── stg_users.sql
│   │   ├── stg_orders.sql
│   │   └── stg_products.sql
│   │
│   └── marts/                    # Analytics layer (empty, user fills)
│       └── .gitkeep
│
└── tests/
    └── assert_positive_total.sql  # Custom tests (from CHECK constraints)
```

### Generation Strategy

1. **Source configuration** (`sources.yml`):
   - All tables from dump
   - Freshness checks
   - Descriptions from comments

2. **Staging models** (`staging/*.sql`):
   - One SELECT per source table
   - Column aliasing (snake_case standardization)
   - Basic type casting

3. **Schema tests** (`schema.yml`):
   - `unique` tests from PRIMARY KEY / UNIQUE
   - `not_null` tests from NOT NULL constraints
   - `relationships` tests from FOREIGN KEY
   - `accepted_values` tests from ENUM / CHECK

4. **Custom tests** (`tests/*.sql`):
   - Complex CHECK constraints → SQL tests

---

## Implementation Details

### Core Data Structures

```rust
// src/integrations/dbt/types.rs

use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtProject {
    pub name: String,
    pub version: String,
    pub profile: String,
    pub model_paths: Vec<String>,
    pub test_paths: Vec<String>,
    pub models: DbtModels,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtModels {
    #[serde(flatten)]
    pub project_name: HashMap<String, DbtModelConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtModelConfig {
    pub materialized: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtSource {
    pub name: String,
    pub database: Option<String>,
    pub schema: String,
    pub description: Option<String>,
    pub tables: Vec<DbtSourceTable>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtSourceTable {
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<DbtColumn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<DbtFreshness>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtColumn {
    pub name: String,
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tests: Option<Vec<DbtTest>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DbtTest {
    Simple(String),  // "unique", "not_null"
    Complex {        // relationships, accepted_values
        #[serde(flatten)]
        test: HashMap<String, serde_json::Value>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtFreshness {
    pub warn_after: DbtDuration,
    pub error_after: DbtDuration,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtDuration {
    pub count: i32,
    pub period: String,  // "hour", "day"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbtModel {
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<DbtColumn>,
}
```

### Project Generator

```rust
// src/integrations/dbt/generator.rs

use crate::parser::{ParsedDump, CreateTableStatement};

pub struct DbtProjectGenerator {
    project_name: String,
    database_name: String,
    dialect: Dialect,
}

impl DbtProjectGenerator {
    pub fn generate(&self, dump: &ParsedDump, output_dir: &Path) -> Result<()> {
        // 1. Create directory structure
        self.create_directory_structure(output_dir)?;

        // 2. Generate dbt_project.yml
        let project = self.generate_project_config()?;
        self.write_project_yml(output_dir, &project)?;

        // 3. Generate sources.yml
        let sources = self.generate_sources(dump)?;
        self.write_sources_yml(output_dir, &sources)?;

        // 4. Generate staging models
        for table in &dump.tables {
            let model_sql = self.generate_staging_model(table)?;
            self.write_model_file(output_dir, &table.name, &model_sql)?;
        }

        // 5. Generate schema.yml with tests
        let schema = self.generate_schema_yml(dump)?;
        self.write_schema_yml(output_dir, &schema)?;

        // 6. Generate custom tests
        let custom_tests = self.generate_custom_tests(dump)?;
        for (test_name, test_sql) in custom_tests {
            self.write_test_file(output_dir, &test_name, &test_sql)?;
        }

        // 7. Generate README.md
        let readme = self.generate_readme(dump)?;
        fs::write(output_dir.join("README.md"), readme)?;

        println!("✓ Generated dbt project at: {}", output_dir.display());
        println!("  - {} source tables", dump.tables.len());
        println!("  - {} staging models", dump.tables.len());
        println!("  - {} tests", self.count_generated_tests(dump));

        Ok(())
    }

    fn generate_sources(&self, dump: &ParsedDump) -> Result<Vec<DbtSource>> {
        let mut sources = Vec::new();

        // Group tables by schema
        let tables_by_schema = dump.group_tables_by_schema();

        for (schema_name, tables) in tables_by_schema {
            let source_tables: Vec<DbtSourceTable> = tables
                .iter()
                .map(|table| self.convert_table_to_source(table))
                .collect::<Result<_>>()?;

            sources.push(DbtSource {
                name: schema_name.clone(),
                database: Some(self.database_name.clone()),
                schema: schema_name,
                description: Some(format!("Raw tables from {} database", self.database_name)),
                tables: source_tables,
            });
        }

        Ok(sources)
    }

    fn convert_table_to_source(&self, table: &CreateTableStatement) -> Result<DbtSourceTable> {
        let columns: Vec<DbtColumn> = table
            .columns
            .iter()
            .map(|col| DbtColumn {
                name: col.name.clone(),
                description: col.comment.clone(),
                tests: None,  // Tests go in schema.yml, not sources.yml
            })
            .collect();

        // Add freshness check for tables with timestamps
        let freshness = table.columns
            .iter()
            .find(|col| {
                matches!(col.name.as_str(), "created_at" | "updated_at" | "timestamp")
            })
            .map(|_| DbtFreshness {
                warn_after: DbtDuration { count: 24, period: "hour".to_string() },
                error_after: DbtDuration { count: 48, period: "hour".to_string() },
            });

        Ok(DbtSourceTable {
            name: table.name.clone(),
            description: table.comment.clone(),
            columns,
            freshness,
        })
    }

    fn generate_staging_model(&self, table: &CreateTableStatement) -> Result<String> {
        let mut sql = String::new();

        // Config block
        sql.push_str("{{ config(\n");
        sql.push_str("    materialized='view'\n");
        sql.push_str(") }}\n\n");

        // SELECT statement
        sql.push_str("SELECT\n");

        for (idx, column) in table.columns.iter().enumerate() {
            let comma = if idx < table.columns.len() - 1 { "," } else { "" };

            // Alias to snake_case if needed
            let alias = self.to_snake_case(&column.name);

            if alias != column.name {
                sql.push_str(&format!("    {} AS {}{}\n", column.name, alias, comma));
            } else {
                sql.push_str(&format!("    {}{}\n", column.name, comma));
            }
        }

        sql.push_str(&format!("FROM {{{{ source('{}', '{}') }}}}\n",
            self.infer_schema_name(table),
            table.name
        ));

        Ok(sql)
    }

    fn generate_schema_yml(&self, dump: &ParsedDump) -> Result<String> {
        let mut models = Vec::new();

        for table in &dump.tables {
            let mut columns = Vec::new();

            for column in &table.columns {
                let tests = self.generate_column_tests(table, column)?;

                columns.push(DbtColumn {
                    name: column.name.clone(),
                    description: column.comment.clone(),
                    tests: if tests.is_empty() { None } else { Some(tests) },
                });
            }

            models.push(DbtModel {
                name: format!("stg_{}", table.name),
                description: table.comment.clone()
                    .or_else(|| Some(format!("Staging model for {}", table.name))),
                columns,
            });
        }

        // Serialize to YAML
        let yaml = serde_yaml::to_string(&json!({
            "version": 2,
            "models": models,
        }))?;

        Ok(yaml)
    }

    fn generate_column_tests(
        &self,
        table: &CreateTableStatement,
        column: &ColumnDef,
    ) -> Result<Vec<DbtTest>> {
        let mut tests = Vec::new();

        // NOT NULL constraint
        if column.constraints.contains(&ColumnConstraint::NotNull) {
            tests.push(DbtTest::Simple("not_null".to_string()));
        }

        // PRIMARY KEY → unique + not_null
        if table.is_primary_key(&column.name) {
            tests.push(DbtTest::Simple("unique".to_string()));
            if !tests.iter().any(|t| matches!(t, DbtTest::Simple(s) if s == "not_null")) {
                tests.push(DbtTest::Simple("not_null".to_string()));
            }
        }

        // UNIQUE constraint
        if table.has_unique_constraint(&column.name) {
            tests.push(DbtTest::Simple("unique".to_string()));
        }

        // FOREIGN KEY → relationships test
        if let Some(fk) = table.get_foreign_key(&column.name) {
            tests.push(DbtTest::Complex {
                test: serde_json::from_value(json!({
                    "relationships": {
                        "to": format!("ref('stg_{}')", fk.ref_table),
                        "field": fk.ref_column,
                    }
                }))?,
            });
        }

        // ENUM → accepted_values test
        if let DataType::Enum(values) = &column.data_type {
            tests.push(DbtTest::Complex {
                test: serde_json::from_value(json!({
                    "accepted_values": {
                        "values": values,
                    }
                }))?,
            });
        }

        // CHECK constraints with IN (...) → accepted_values
        for constraint in &table.constraints {
            if let TableConstraint::Check { expr, .. } = constraint {
                if let Some(values) = self.extract_in_values(expr) {
                    tests.push(DbtTest::Complex {
                        test: serde_json::from_value(json!({
                            "accepted_values": {
                                "values": values,
                            }
                        }))?,
                    });
                }
            }
        }

        Ok(tests)
    }

    fn generate_custom_tests(&self, dump: &ParsedDump) -> Result<Vec<(String, String)>> {
        let mut tests = Vec::new();

        for table in &dump.tables {
            for constraint in &table.constraints {
                if let TableConstraint::Check { name, expr } = constraint {
                    // Only generate custom test if not handled by built-in tests
                    if !self.is_simple_check(expr) {
                        let test_name = name.clone()
                            .unwrap_or_else(|| format!("check_{}", table.name));

                        let test_sql = format!(
                            "-- Test: {}\nSELECT *\nFROM {{{{ ref('stg_{}') }}}}\nWHERE NOT ({})\n",
                            test_name,
                            table.name,
                            expr
                        );

                        tests.push((test_name, test_sql));
                    }
                }
            }
        }

        Ok(tests)
    }

    fn is_simple_check(&self, expr: &str) -> bool {
        // Simple checks handled by accepted_values test
        expr.contains(" IN (") || expr.contains(" in (")
    }
}
```

### Sources.yml Generator

```rust
// src/integrations/dbt/sources_writer.rs

impl DbtProjectGenerator {
    fn write_sources_yml(&self, output_dir: &Path, sources: &[DbtSource]) -> Result<()> {
        let yaml = serde_yaml::to_string(&json!({
            "version": 2,
            "sources": sources,
        }))?;

        let path = output_dir.join("models").join("sources.yml");
        fs::write(path, yaml)?;

        Ok(())
    }
}
```

**Example output**:

```yaml
version: 2

sources:
  - name: raw
    database: mydb
    schema: public
    description: Raw tables from mydb database

    tables:
      - name: users
        description: User accounts
        columns:
          - name: id
            description: Primary key
          - name: email
            description: User email address
          - name: created_at
            description: Account creation timestamp

        freshness:
          warn_after:
            count: 24
            period: hour
          error_after:
            count: 48
            period: hour

      - name: orders
        description: Customer orders
        columns:
          - name: id
          - name: user_id
          - name: total
          - name: created_at
```

---

## CLI Interface Design

### Command: dbt-init

```bash
# Basic usage
sql-splitter dbt-init dump.sql -o my_dbt_project/

# Customize project name
sql-splitter dbt-init dump.sql \
  --output my_dbt_project/ \
  --name analytics \
  --profile snowflake

# Filter tables (only include specific tables)
sql-splitter dbt-init dump.sql -o dbt/ --tables users,orders,products

# Include marts layer scaffolding
sql-splitter dbt-init dump.sql -o dbt/ --with-marts

# Output:
# ✓ Created dbt project structure
# ✓ Generated 15 source tables
# ✓ Generated 15 staging models
# ✓ Generated 45 tests
# ✓ Created README.md with next steps
#
# Next steps:
#   cd my_dbt_project/
#   dbt deps  # Install dependencies
#   dbt run   # Build models
#   dbt test  # Run tests
```

### Options

```
--output, -o          Output directory for dbt project
--name                Project name (default: inferred from dump filename)
--profile             dbt profile name (default: "postgres")
--schema              Source schema name (default: "raw")
--database            Source database name (default: inferred)
--tables              Filter to specific tables (comma-separated)
--with-marts          Generate example marts layer models
--staging-prefix      Prefix for staging models (default: "stg_")
--docs                Generate extended documentation
```

---

## Dump to dbt Project Conversion

### Input: SQL Dump

```sql
-- MySQL dump
CREATE TABLE users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  status ENUM('active', 'inactive') DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  COMMENT 'User accounts'
);

CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  total DECIMAL(10,2) NOT NULL CHECK (total > 0),
  status VARCHAR(20) CHECK (status IN ('pending', 'completed', 'cancelled')),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
```

### Output: dbt Project

**dbt_project.yml**:

```yaml
name: my_analytics
version: 1.0.0
config-version: 2
profile: postgres

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

models:
  my_analytics:
    staging:
      materialized: view
    marts:
      materialized: table
```

**models/sources.yml**:

```yaml
version: 2

sources:
  - name: raw
    database: mydb
    schema: public
    tables:
      - name: users
        description: User accounts
        columns:
          - name: id
            description: Primary key
          - name: email
          - name: status
          - name: created_at

        freshness:
          warn_after: { count: 24, period: hour }
          error_after: { count: 48, period: hour }

      - name: orders
        columns:
          - name: id
          - name: user_id
          - name: total
          - name: status
          - name: created_at

        freshness:
          warn_after: { count: 24, period: hour }
          error_after: { count: 48, period: hour }
```

**models/staging/stg_users.sql**:

```sql
{{ config(materialized='view') }}

SELECT
    id AS user_id,
    email,
    status,
    created_at
FROM {{ source('raw', 'users') }}
```

**models/staging/stg_orders.sql**:

```sql
{{ config(materialized='view') }}

SELECT
    id AS order_id,
    user_id,
    total,
    status,
    created_at
FROM {{ source('raw', 'orders') }}
```

**models/staging/\_staging\_\_models.yml**:

```yaml
version: 2

models:
  - name: stg_users
    description: Staging model for users
    columns:
      - name: user_id
        description: Primary key
        tests:
          - unique
          - not_null

      - name: email
        tests:
          - unique
          - not_null

      - name: status
        tests:
          - accepted_values:
              values: ["active", "inactive"]

  - name: stg_orders
    description: Staging model for orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null

      - name: user_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_users')
              field: user_id

      - name: total
        tests:
          - not_null

      - name: status
        tests:
          - accepted_values:
              values: ["pending", "completed", "cancelled"]
```

**tests/assert_positive_total.sql**:

```sql
-- Test: total must be positive
SELECT *
FROM {{ ref('stg_orders') }}
WHERE NOT (total > 0)
```

---

## Test Generation from Schema

### Constraint → Test Mapping

| SQL Constraint         | dbt Test             | Example                                         |
| ---------------------- | -------------------- | ----------------------------------------------- |
| `PRIMARY KEY`          | `unique`, `not_null` | `tests: [unique, not_null]`                     |
| `NOT NULL`             | `not_null`           | `tests: [not_null]`                             |
| `UNIQUE`               | `unique`             | `tests: [unique]`                               |
| `FOREIGN KEY`          | `relationships`      | `relationships: {to: ref('parent'), field: id}` |
| `ENUM('a','b')`        | `accepted_values`    | `accepted_values: {values: ['a', 'b']}`         |
| `CHECK (col IN (...))` | `accepted_values`    | `accepted_values: {values: [...]}`              |
| `CHECK (col > 0)`      | Custom test          | SQL file in `tests/`                            |

### Advanced Test Generation

```rust
impl DbtProjectGenerator {
    fn generate_advanced_tests(&self, table: &CreateTableStatement) -> Vec<DbtTest> {
        let mut tests = Vec::new();

        // Detect common patterns

        // 1. Email columns → regex validation
        for column in &table.columns {
            if column.name.contains("email") {
                tests.push(self.create_custom_test(
                    "email_format",
                    &column.name,
                    "email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'"
                ));
            }
        }

        // 2. Date range validation
        if table.has_column("created_at") && table.has_column("updated_at") {
            tests.push(self.create_custom_test(
                "updated_after_created",
                "updated_at",
                "updated_at >= created_at"
            ));
        }

        // 3. Percentage columns (0-100)
        for column in &table.columns {
            if column.name.contains("percent") || column.name.contains("rate") {
                tests.push(self.create_custom_test(
                    "valid_percentage",
                    &column.name,
                    &format!("{} >= 0 AND {} <= 100", column.name, column.name)
                ));
            }
        }

        tests
    }
}
```

---

## Source Configuration Generation

### Freshness Detection

```rust
impl DbtProjectGenerator {
    fn infer_freshness(&self, table: &CreateTableStatement) -> Option<DbtFreshness> {
        // Look for timestamp columns
        let has_timestamp = table.columns.iter().any(|col| {
            matches!(col.name.as_str(), "created_at" | "updated_at" | "timestamp" | "event_time")
        });

        if has_timestamp {
            // Default freshness checks
            Some(DbtFreshness {
                warn_after: DbtDuration {
                    count: 24,
                    period: "hour".to_string(),
                },
                error_after: DbtDuration {
                    count: 48,
                    period: "hour".to_string(),
                },
            })
        } else {
            None
        }
    }
}
```

### Multi-Schema Support

```rust
impl ParsedDump {
    fn group_tables_by_schema(&self) -> HashMap<String, Vec<&CreateTableStatement>> {
        let mut schemas = HashMap::new();

        for table in &self.tables {
            let schema = table.schema.clone()
                .unwrap_or_else(|| "public".to_string());

            schemas.entry(schema)
                .or_insert_with(Vec::new)
                .push(table);
        }

        schemas
    }
}
```

**Output** (multi-schema):

```yaml
version: 2

sources:
  - name: production
    database: mydb
    schema: production
    tables:
      - name: users
      - name: orders

  - name: analytics
    database: mydb
    schema: analytics
    tables:
      - name: user_metrics
      - name: order_stats
```

---

## Use Cases

### 1. New dbt Project Bootstrap

**Problem**: Starting dbt project from scratch is tedious.

**Solution**:

```bash
# Existing production database
pg_dump production > prod_dump.sql

# Generate dbt project
sql-splitter dbt-init prod_dump.sql -o dbt_project/

# Immediate value
cd dbt_project/
dbt run   # All staging models built
dbt test  # 100+ tests pass
dbt docs generate  # Full documentation

# Team can now focus on marts layer (business logic)
```

### 2. Legacy Database Modernization

**Problem**: 10-year-old database with stored procedures, no tests, no docs.

**Solution**:

```bash
# Export schema
mysqldump --no-data legacy_db > schema.sql

# Generate modern dbt project
sql-splitter dbt-init schema.sql -o modern_analytics/

# Now have:
# - All tables documented
# - Baseline tests (PK, FK, NOT NULL)
# - Staging layer as foundation
# - Can incrementally replace stored procs with dbt models
```

### 3. Data Quality Baseline

**Problem**: Need to establish data quality metrics for existing system.

**Solution**:

```bash
# Generate dbt project with tests
sql-splitter dbt-init dump.sql -o dbt/

# Run tests to get baseline
cd dbt/
dbt test --store-failures

# dbt creates audit tables with failures
# → Visibility into current data quality issues
# → Track improvement over time
```

### 4. Multi-Environment Consistency

**Problem**: Dev, staging, prod have different schemas.

**Solution**:

```bash
# Generate dbt project from each environment
sql-splitter dbt-init dev_dump.sql -o dbt_dev/
sql-splitter dbt-init prod_dump.sql -o dbt_prod/

# Diff projects to find inconsistencies
diff -r dbt_dev/models/sources.yml dbt_prod/models/sources.yml

# Use single dbt project for all environments
# Sources switch via profiles.yml
```

### 5. Schema Change Impact Analysis

**Problem**: Want to drop a column, unsure what downstream models use it.

**Solution**:

```bash
# Generate dbt project
sql-splitter dbt-init current_dump.sql -o dbt/

# dbt's DAG shows dependencies
cd dbt/
dbt run
dbt docs generate

# Search for column usage in docs
# → See all models/tests referencing column
# → Assess blast radius before change
```

---

## Challenges and Solutions

### Challenge 1: dbt Naming Conventions

**Problem**: SQL dumps use various naming (camelCase, PascalCase, mixed).

**Solution**: Normalize to snake_case.

```rust
impl DbtProjectGenerator {
    fn to_snake_case(&self, name: &str) -> String {
        // Convert to snake_case
        let mut result = String::new();
        for (i, ch) in name.chars().enumerate() {
            if ch.is_uppercase() && i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        }
        result
    }
}
```

**Example**:

```sql
-- Source table
CREATE TABLE UserAccounts (UserId INT, EmailAddress VARCHAR);

-- Generated staging model
SELECT
    UserId AS user_id,
    EmailAddress AS email_address
FROM {{ source('raw', 'UserAccounts') }}
```

### Challenge 2: Complex CHECK Constraints

**Problem**: Some CHECK constraints are too complex for `accepted_values`.

```sql
CHECK (start_date < end_date AND DATEDIFF(end_date, start_date) <= 365)
```

**Solution**: Generate custom test SQL.

```sql
-- tests/assert_valid_date_range.sql
SELECT *
FROM {{ ref('stg_subscriptions') }}
WHERE NOT (
    start_date < end_date
    AND DATEDIFF(end_date, start_date) <= 365
)
```

### Challenge 3: Circular Foreign Keys

**Problem**: Circular dependencies break dbt DAG.

```
users.manager_id → users.id
employees.department_id → departments.id
departments.manager_id → employees.id
```

**Solution**: Detect and warn.

```rust
impl DbtProjectGenerator {
    fn detect_circular_dependencies(&self, dump: &ParsedDump) -> Vec<String> {
        let graph = self.build_dependency_graph(dump);
        let cycles = graph.find_cycles();

        if !cycles.is_empty() {
            eprintln!("⚠ Warning: Circular foreign key dependencies detected:");
            for cycle in &cycles {
                eprintln!("  - {}", cycle.join(" → "));
            }
            eprintln!("\nThis may cause issues in dbt. Consider:");
            eprintln!("  1. Using ephemeral models");
            eprintln!("  2. Removing some relationships tests");
        }

        cycles
    }
}
```

### Challenge 4: Materialization Strategy

**Problem**: Should models be views or tables?

**Solution**: Smart defaults + user override.

```yaml
# Default: staging = view, marts = table
models:
  my_project:
    staging:
      materialized: view # Fast, no storage
    marts:
      materialized: table # Slow queries, need persistence
```

```bash
# Let users override
sql-splitter dbt-init dump.sql \
  --staging-materialized incremental \
  --marts-materialized view
```

### Challenge 5: Large Schemas

**Problem**: 500+ tables → 500+ staging models is overwhelming.

**Solution**: Table filtering + grouping.

```bash
# Only include core tables
sql-splitter dbt-init dump.sql \
  --tables "users,orders,products,payments" \
  -o core_dbt/

# Or exclude logging/audit tables
sql-splitter dbt-init dump.sql \
  --exclude-pattern "log_,audit_,temp_" \
  -o dbt/
```

---

## Effort Breakdown

### Phase 1: Project Structure Generation (8 hours)

- **Directory scaffolding** (1h)
  - Create dbt project structure
  - dbt_project.yml template
  - README.md generator

- **Sources.yml generation** (3h)
  - Parse all CREATE TABLE statements
  - Extract table/column metadata
  - Infer freshness checks
  - Multi-schema support

- **Staging models generation** (2h)
  - One model per table
  - Column aliasing to snake_case
  - Config blocks

- **Testing** (2h)
  - Test with MySQL, PostgreSQL dumps
  - Validate generated project with dbt CLI
  - Verify dbt run/test/docs work

### Phase 2: Test Generation (10 hours)

- **Built-in test mapping** (4h)
  - PK/UNIQUE → unique test
  - NOT NULL → not_null test
  - FK → relationships test
  - ENUM/CHECK IN → accepted_values test

- **Custom test generation** (3h)
  - Complex CHECK constraints → SQL tests
  - Pattern detection (email, percentage)
  - Multi-column constraints

- **Schema.yml generation** (2h)
  - Serialize models + columns + tests to YAML
  - Descriptions from comments
  - Format nicely

- **Testing** (1h)
  - Verify tests run with dbt test
  - Edge cases (no constraints, circular FKs)

### Phase 3: Advanced Features (6 hours)

- **Marts layer scaffolding** (2h)
  - Example fact/dimension models
  - Common transformations (deduplication, joins)
  - Optional --with-marts flag

- **Multi-dialect support** (2h)
  - Handle MySQL vs PostgreSQL differences
  - Dialect-specific type conversions
  - Auto-detect source dialect

- **Documentation enhancement** (1h)
  - Extract table/column comments
  - Generate data dictionary
  - Add to dbt docs

- **CLI polish** (1h)
  - Progress indicators
  - Summary statistics
  - Error handling

### Phase 4: Integration & Documentation (4 hours)

- **Integration testing** (2h)
  - End-to-end: dump → dbt project → dbt run/test/docs
  - Real-world dumps (MySQL, PostgreSQL)
  - Performance optimization

- **User documentation** (1h)
  - Usage examples
  - Best practices guide
  - Troubleshooting

- **Example projects** (1h)
  - Sample dumps
  - Generated dbt projects
  - README templates

**Total: 28 hours**

---

## Next Steps

1. **v1.19.0 Implementation**:
   - Implement dbt project generator
   - Support MySQL, PostgreSQL sources
   - Auto-generate tests from constraints
   - Documentation generation

2. **v1.20.0 Enhancements**:
   - Incremental model generation (detect append-only tables)
   - Snapshot generation (detect SCD tables)
   - Macro generation (common transformations)
   - dbt packages integration

3. **Future**:
   - AI-powered marts layer suggestions
   - Integration with dbt Cloud API
   - Real-time schema evolution tracking
   - Automated documentation from business glossary

---

**Recommendation**: Implement dbt integration for v1.19.0. This is a **game-changer** feature that positions sql-splitter as essential for dbt adoption. The 28h effort is justified by massive value: teams can go from "dump file" to "production dbt project" in minutes instead of days. This bridges the gap between legacy databases and modern analytics engineering.

## Strategic Impact

**Market positioning**:

- **Before**: sql-splitter is a niche dump utility
- **After**: sql-splitter is essential for dbt bootstrapping

**User testimonial** (projected):

> "We had 200+ tables and dreaded the YAML grind. sql-splitter dbt-init saved us 2 weeks of work. We went from dump to production dbt project in under an hour." — Data Engineer at Fortune 500

**Viral potential**: dbt community (50k+ Slack members) loves automation. A well-executed dbt integration could drive significant adoption.
