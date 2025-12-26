# Great Expectations Integration: Deep Dive

**Date**: 2025-12-24
**Feature**: Great Expectations Integration (v1.17.0)
**Effort**: 16 hours
**Priority**: Tier 1 (High Impact, Medium Effort)

## Table of Contents

1. [What is Great Expectations?](#what-is-great-expectations)
2. [Why Integrate with sql-splitter?](#why-integrate-with-sql-splitter)
3. [Integration Architecture](#integration-architecture)
4. [Implementation Details](#implementation-details)
5. [CLI Interface Design](#cli-interface-design)
6. [Expectation Generation Strategies](#expectation-generation-strategies)
7. [Type Mappings](#type-mappings)
8. [Use Cases](#use-cases)
9. [Performance Considerations](#performance-considerations)
10. [Challenges and Solutions](#challenges-and-solutions)
11. [Effort Breakdown](#effort-breakdown)

---

## What is Great Expectations?

**Great Expectations (GX)** is an open-source Python library for validating, documenting, and profiling data to maintain data quality.

### Key Concepts

```python
# Great Expectations workflow
import great_expectations as gx

# 1. Define expectations
expectation_suite = gx.ExpectationSuite("users_suite")
expectation_suite.add_expectation({
    "expectation_type": "expect_column_values_to_be_unique",
    "kwargs": {"column": "user_id"}
})

# 2. Validate data
validator = gx.Validator(df, expectation_suite)
results = validator.validate()

# 3. Generate documentation
gx.render.view_validation_result(results)
```

### Core Features

1. **Expectations**: Assertions about data (uniqueness, ranges, patterns, distributions)
2. **Validation**: Run expectations against datasets, get detailed results
3. **Data Docs**: Auto-generated HTML documentation of data quality
4. **Profiling**: Automatically infer expectations from sample data
5. **Integration**: Works with Pandas, Spark, SQL databases, data warehouses

### Why Great Expectations?

- **Industry standard**: Used by Airbnb, Superconductive, many data teams
- **Comprehensive**: 300+ built-in expectations
- **Extensible**: Custom expectations in Python
- **CI/CD friendly**: Fails pipelines on quality issues
- **Documentation**: Beautiful HTML reports

---

## Why Integrate with sql-splitter?

### The Problem

Current `sql-splitter validate` is limited:

```bash
# What we can do now
sql-splitter validate dump.sql
# ✓ Check FK integrity
# ✓ Check PK uniqueness
# ✓ Find orphaned rows

# What we CAN'T do
# ✗ Check data ranges (age between 0-120)
# ✗ Validate formats (email patterns, phone numbers)
# ✗ Check distributions (no nulls, specific value sets)
# ✗ Generate documentation
# ✗ Track quality over time
```

### The Opportunity

**Auto-generate Great Expectations suites from SQL dumps**, so users can:

1. **Bootstrap data quality testing** - No manual expectation writing
2. **Document data contracts** - Schema + quality rules in one place
3. **Continuous validation** - Run GX in CI/CD after dump restore
4. **Quality regression detection** - Track quality changes across dump versions
5. **Production monitoring** - Apply same rules to live databases

### Example Workflow

```bash
# 1. Generate GX suite from dump
sql-splitter gx-generate dump.sql -o gx/expectations/

# Creates:
# - users.json (expectation suite for users table)
# - orders.json (expectation suite for orders table)
# - great_expectations.yml (GX config)

# 2. Validate new dump against expectations
sql-splitter gx-validate new_dump.sql --suite gx/expectations/

# 3. View data docs
sql-splitter gx-docs gx/expectations/ --open

# 4. Export to production
# Now use same expectations on live Postgres/MySQL with native GX
```

---

## Integration Architecture

### Option A: Python Wrapper (Recommended)

Call Great Expectations CLI from Rust, avoiding Python bindings.

```rust
// src/integrations/great_expectations.rs

use std::process::Command;
use serde_json::Value;

pub struct GreatExpectationsWrapper {
    gx_dir: PathBuf,
}

impl GreatExpectationsWrapper {
    pub fn init(output_dir: &Path) -> Result<Self> {
        // Create GX directory structure
        fs::create_dir_all(output_dir.join("expectations"))?;
        fs::create_dir_all(output_dir.join("uncommitted"))?;

        // Generate great_expectations.yml
        let config = include_str!("templates/great_expectations.yml");
        fs::write(output_dir.join("great_expectations.yml"), config)?;

        Ok(Self { gx_dir: output_dir.to_path_buf() })
    }

    pub fn generate_suite(&self, table_stats: &TableStats) -> Result<ExpectationSuite> {
        let mut suite = ExpectationSuite::new(&table_stats.name);

        // Generate expectations from schema
        for column in &table_stats.columns {
            suite.add_expectations(self.infer_column_expectations(column)?);
        }

        // Add constraints as expectations
        for pk in &table_stats.primary_keys {
            suite.add_expectation(Expectation::column_values_unique(pk));
        }

        for fk in &table_stats.foreign_keys {
            suite.add_expectation(Expectation::foreign_key_valid(fk));
        }

        Ok(suite)
    }

    pub fn save_suite(&self, suite: &ExpectationSuite) -> Result<()> {
        let path = self.gx_dir
            .join("expectations")
            .join(format!("{}.json", suite.name));

        let json = serde_json::to_string_pretty(&suite.to_gx_json())?;
        fs::write(path, json)?;

        Ok(())
    }

    pub fn run_validation(&self, suite_name: &str, db_url: &str) -> Result<ValidationResult> {
        // Call GX CLI
        let output = Command::new("great_expectations")
            .args(&[
                "checkpoint", "run",
                suite_name,
                "--datasource", db_url,
            ])
            .current_dir(&self.gx_dir)
            .output()?;

        if !output.status.success() {
            bail!("GX validation failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Parse validation results
        let results: Value = serde_json::from_slice(&output.stdout)?;
        Ok(ValidationResult::from_gx_json(results))
    }
}
```

**Pros**:
- ✅ No Python bindings needed
- ✅ Uses standard GX CLI (familiar to users)
- ✅ Easy to debug (can run GX commands manually)
- ✅ Leverages full GX ecosystem

**Cons**:
- ❌ Requires GX installed (`pip install great-expectations`)
- ❌ Slower (process spawning overhead)
- ⚠️ Version compatibility concerns

### Option B: Native Rust Implementation

Implement subset of GX functionality in Rust (no Python dependency).

```rust
pub struct NativeValidator {
    expectations: Vec<Expectation>,
}

impl NativeValidator {
    pub fn validate(&self, dump: &ParsedDump) -> Result<ValidationResult> {
        let mut results = ValidationResult::new();

        for expectation in &self.expectations {
            let result = match expectation {
                Expectation::ColumnValuesUnique { column } => {
                    self.check_uniqueness(dump, column)?
                }
                Expectation::ColumnValuesBetween { column, min, max } => {
                    self.check_range(dump, column, min, max)?
                }
                Expectation::ColumnValuesMatchRegex { column, regex } => {
                    self.check_pattern(dump, column, regex)?
                }
                // ... implement ~20 most common expectations
            };

            results.add(result);
        }

        Ok(results)
    }
}
```

**Pros**:
- ✅ No external dependencies
- ✅ Fast (native Rust)
- ✅ Works offline, in containers

**Cons**:
- ❌ Need to reimplement GX expectations
- ❌ Subset only (can't support all 300+ expectations)
- ❌ No ecosystem integration
- ❌ Maintenance burden (keep up with GX changes)

### Decision: Hybrid Approach

**Phase 1 (v1.17.0)**: Python wrapper for suite generation
- Generate expectation JSON files (no GX required)
- Users run validation with native GX
- Simple integration, leverages GX ecosystem

**Phase 2 (v1.18.0)**: Add native validator for common expectations
- Validate dumps without Python
- Useful for CI/CD, quick checks
- Supports ~20 most common expectations

---

## Implementation Details

### Core Data Structures

```rust
// src/integrations/gx/types.rs

#[derive(Debug, Serialize, Deserialize)]
pub struct ExpectationSuite {
    pub expectation_suite_name: String,
    pub expectations: Vec<Expectation>,
    pub data_asset_type: String,
    pub meta: Meta,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "expectation_type")]
pub enum Expectation {
    #[serde(rename = "expect_column_to_exist")]
    ColumnToExist {
        kwargs: ColumnToExistKwargs,
    },

    #[serde(rename = "expect_column_values_to_be_unique")]
    ColumnValuesToBeUnique {
        kwargs: ColumnValuesToBeUniqueKwargs,
    },

    #[serde(rename = "expect_column_values_to_not_be_null")]
    ColumnValuesToNotBeNull {
        kwargs: ColumnValuesToNotBeNullKwargs,
    },

    #[serde(rename = "expect_column_values_to_be_between")]
    ColumnValuesToBeBetween {
        kwargs: ColumnValuesToBeBetweenKwargs,
    },

    #[serde(rename = "expect_column_values_to_match_regex")]
    ColumnValuesToMatchRegex {
        kwargs: ColumnValuesToMatchRegexKwargs,
    },

    #[serde(rename = "expect_column_values_to_be_in_set")]
    ColumnValuesToBeInSet {
        kwargs: ColumnValuesToBeInSetKwargs,
    },

    // Add more as needed (~20 common expectations)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnValuesToBeUniqueKwargs {
    pub column: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_by: Option<String>,
}
```

### Expectation Generation from Schema

```rust
// src/integrations/gx/generator.rs

use crate::parser::{Statement, CreateTableStatement, ColumnDef, ColumnConstraint};

pub struct ExpectationGenerator {
    strict_mode: bool,
    sample_size: usize,
}

impl ExpectationGenerator {
    pub fn generate_from_dump(&self, dump: &ParsedDump) -> Result<Vec<ExpectationSuite>> {
        let mut suites = Vec::new();

        for table in &dump.tables {
            let mut suite = ExpectationSuite::new(&table.name);

            // 1. Schema-based expectations
            for column in &table.columns {
                suite.add_expectations(self.generate_for_column(column)?);
            }

            // 2. Constraint-based expectations
            for constraint in &table.constraints {
                suite.add_expectation(self.generate_for_constraint(constraint)?);
            }

            // 3. Data-based expectations (if --profile flag)
            if self.strict_mode {
                suite.add_expectations(self.profile_data(&table)?);
            }

            suites.push(suite);
        }

        Ok(suites)
    }

    fn generate_for_column(&self, column: &ColumnDef) -> Result<Vec<Expectation>> {
        let mut expectations = Vec::new();

        // Column exists
        expectations.push(Expectation::column_to_exist(&column.name));

        // NOT NULL constraint
        if column.constraints.contains(&ColumnConstraint::NotNull) {
            expectations.push(Expectation::column_values_to_not_be_null(&column.name));
        }

        // Type-based expectations
        match column.data_type {
            DataType::Int | DataType::BigInt => {
                if let Some((min, max)) = self.infer_int_range(column) {
                    expectations.push(
                        Expectation::column_values_to_be_between(&column.name, min, max)
                    );
                }
            }

            DataType::VarChar(len) => {
                expectations.push(
                    Expectation::column_value_lengths_to_be_between(
                        &column.name,
                        0,
                        len as i64
                    )
                );
            }

            DataType::Date | DataType::DateTime => {
                // Reasonable date range
                expectations.push(
                    Expectation::column_values_to_be_between(
                        &column.name,
                        "1900-01-01",
                        "2100-12-31"
                    )
                );
            }

            DataType::Enum(values) => {
                expectations.push(
                    Expectation::column_values_to_be_in_set(&column.name, values)
                );
            }

            _ => {}
        }

        // Pattern-based expectations for common column names
        if column.name.contains("email") {
            expectations.push(
                Expectation::column_values_to_match_regex(
                    &column.name,
                    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                )
            );
        }

        if column.name.contains("url") || column.name.contains("website") {
            expectations.push(
                Expectation::column_values_to_match_regex(
                    &column.name,
                    r"^https?://.+"
                )
            );
        }

        if column.name.contains("phone") {
            expectations.push(
                Expectation::column_values_to_match_regex(
                    &column.name,
                    r"^\+?[1-9]\d{1,14}$" // E.164 format
                )
            );
        }

        Ok(expectations)
    }

    fn generate_for_constraint(&self, constraint: &TableConstraint) -> Result<Expectation> {
        match constraint {
            TableConstraint::PrimaryKey(columns) => {
                if columns.len() == 1 {
                    Ok(Expectation::column_values_to_be_unique(&columns[0]))
                } else {
                    Ok(Expectation::compound_columns_to_be_unique(columns))
                }
            }

            TableConstraint::Unique(columns) => {
                if columns.len() == 1 {
                    Ok(Expectation::column_values_to_be_unique(&columns[0]))
                } else {
                    Ok(Expectation::compound_columns_to_be_unique(columns))
                }
            }

            TableConstraint::ForeignKey { columns, ref_table, ref_columns } => {
                // GX doesn't have built-in FK expectation, use custom
                Ok(Expectation::custom_foreign_key_valid(
                    columns,
                    ref_table,
                    ref_columns
                ))
            }

            TableConstraint::Check(expression) => {
                // Try to parse check constraint into GX expectation
                self.parse_check_constraint_to_expectation(expression)
            }
        }
    }

    fn profile_data(&self, table: &TableStats) -> Result<Vec<Expectation>> {
        let mut expectations = Vec::new();

        // Sample data to infer expectations
        let sample = self.sample_table_data(table, self.sample_size)?;

        for column in &table.columns {
            let values = sample.get_column(&column.name)?;

            // Check if column has nulls
            let null_count = values.iter().filter(|v| v.is_null()).count();
            let null_percent = null_count as f64 / values.len() as f64;

            if null_percent < 0.01 {
                // < 1% nulls → expect no nulls
                expectations.push(
                    Expectation::column_values_to_not_be_null(&column.name)
                );
            }

            // Detect categorical columns (low cardinality)
            let unique_values: HashSet<_> = values.iter().collect();
            let cardinality = unique_values.len();

            if cardinality < 20 && cardinality < values.len() / 10 {
                // Low cardinality → expect values in set
                expectations.push(
                    Expectation::column_values_to_be_in_set(
                        &column.name,
                        unique_values.into_iter().cloned().collect()
                    )
                );
            }

            // Numeric range inference
            if let Some((min, max)) = self.infer_numeric_range(values) {
                let buffer = (max - min) * 0.1; // 10% buffer
                expectations.push(
                    Expectation::column_values_to_be_between(
                        &column.name,
                        min - buffer,
                        max + buffer
                    )
                );
            }
        }

        Ok(expectations)
    }
}
```

### Validation Against Dump

```rust
// src/integrations/gx/validator.rs

pub struct DumpValidator {
    dump: ParsedDump,
}

impl DumpValidator {
    pub fn validate(&self, suite: &ExpectationSuite) -> Result<ValidationResult> {
        let mut result = ValidationResult::new(&suite.expectation_suite_name);

        let table = self.dump.get_table(&suite.expectation_suite_name)?;

        for expectation in &suite.expectations {
            let check_result = self.validate_expectation(table, expectation)?;
            result.add_result(check_result);
        }

        Ok(result)
    }

    fn validate_expectation(
        &self,
        table: &TableData,
        expectation: &Expectation
    ) -> Result<ExpectationResult> {
        match expectation {
            Expectation::ColumnValuesToBeUnique { kwargs } => {
                self.check_uniqueness(table, &kwargs.column)
            }

            Expectation::ColumnValuesToNotBeNull { kwargs } => {
                self.check_not_null(table, &kwargs.column)
            }

            Expectation::ColumnValuesToBeBetween { kwargs } => {
                self.check_range(
                    table,
                    &kwargs.column,
                    kwargs.min_value,
                    kwargs.max_value
                )
            }

            Expectation::ColumnValuesToMatchRegex { kwargs } => {
                self.check_regex(table, &kwargs.column, &kwargs.regex)
            }

            // ... implement other validators
        }
    }

    fn check_uniqueness(&self, table: &TableData, column: &str) -> Result<ExpectationResult> {
        let values = table.get_column(column)?;
        let unique_count = values.iter().collect::<HashSet<_>>().len();
        let total_count = values.len();

        let success = unique_count == total_count;
        let unexpected_count = if success { 0 } else { total_count - unique_count };

        Ok(ExpectationResult {
            success,
            expectation_type: "expect_column_values_to_be_unique".to_string(),
            result: json!({
                "element_count": total_count,
                "unexpected_count": unexpected_count,
                "unexpected_percent": unexpected_count as f64 / total_count as f64 * 100.0,
            }),
        })
    }

    fn check_regex(&self, table: &TableData, column: &str, pattern: &str) -> Result<ExpectationResult> {
        let regex = Regex::new(pattern)?;
        let values = table.get_column(column)?;

        let mut unexpected = Vec::new();
        for (idx, value) in values.iter().enumerate() {
            if let Some(s) = value.as_str() {
                if !regex.is_match(s) {
                    unexpected.push((idx, s.to_string()));

                    // Limit samples
                    if unexpected.len() >= 20 {
                        break;
                    }
                }
            }
        }

        let success = unexpected.is_empty();

        Ok(ExpectationResult {
            success,
            expectation_type: "expect_column_values_to_match_regex".to_string(),
            result: json!({
                "element_count": values.len(),
                "unexpected_count": unexpected.len(),
                "unexpected_list": unexpected.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
            }),
        })
    }
}
```

---

## CLI Interface Design

### Command 1: Generate Expectation Suites

```bash
# Generate GX suites from dump
sql-splitter gx-generate dump.sql -o gx/

# Options:
#   --output, -o     Output directory for GX project
#   --profile        Profile data to infer additional expectations (slower)
#   --sample-size    Number of rows to sample for profiling (default: 10000)
#   --strict         Generate strict expectations (no nulls, exact ranges)
#   --tables         Only generate for specific tables
```

**Output structure**:
```
gx/
├── great_expectations.yml
├── expectations/
│   ├── users.json
│   ├── orders.json
│   └── products.json
└── uncommitted/
    └── validations/
```

### Command 2: Validate Dump

```bash
# Validate dump against expectations
sql-splitter gx-validate dump.sql --suite gx/expectations/

# Options:
#   --suite          Path to GX expectations directory
#   --table          Validate specific table only
#   --fail-fast      Stop on first validation failure
#   --output         Save validation results to JSON
#   --format         Output format: text, json, html
```

**Output**:
```
Validating dump.sql against 3 expectation suites...

✓ users (23/23 expectations passed)
✗ orders (18/20 expectations passed)
  ✗ expect_column_values_to_be_between (order_total): 2 values out of range
  ✗ expect_column_values_to_match_regex (email): 5 invalid emails
✓ products (15/15 expectations passed)

Summary: 56/58 expectations passed (96.6%)
```

### Command 3: Generate Data Docs

```bash
# Generate HTML documentation
sql-splitter gx-docs gx/ --open

# Opens browser with:
# - Expectation suite overview
# - Validation results
# - Data quality dashboard
```

### Integration with Existing Commands

```bash
# Enhance validate command with GX
sql-splitter validate dump.sql --gx-suite gx/expectations/

# Combines:
# - Built-in FK/PK validation
# - Great Expectations quality checks
# - Unified output
```

---

## Expectation Generation Strategies

### Strategy 1: Schema-Only (Fast)

Generate expectations from CREATE TABLE statements only.

**Pros**:
- ✅ Instant (no data parsing)
- ✅ Works with schema-only dumps
- ✅ Covers structural constraints

**Cons**:
- ❌ Misses data quality issues
- ❌ Can't infer ranges, patterns, cardinality

**Use case**: Quick setup, large dumps

### Strategy 2: Data Profiling (Comprehensive)

Sample INSERT data to infer statistical expectations.

**Pros**:
- ✅ Discovers real data patterns
- ✅ Detects quality issues (nulls, outliers)
- ✅ Generates realistic ranges

**Cons**:
- ❌ Slower (must parse data)
- ❌ Sample might not represent full dataset
- ⚠️ Risk of overfitting to sample

**Use case**: High-quality suites, smaller dumps

### Strategy 3: Hybrid (Recommended)

Schema-based + targeted profiling for key tables.

```bash
# Profile only critical tables
sql-splitter gx-generate dump.sql \
  --profile \
  --tables users,orders,payments \
  --sample-size 50000
```

**Pros**:
- ✅ Balances speed and quality
- ✅ Focus profiling where it matters
- ✅ Scalable to large dumps

---

## Type Mappings

### SQL to GX Expectation Mappings

| SQL Type | GX Expectation | Notes |
|----------|----------------|-------|
| `INT` | `expect_column_values_to_be_of_type("int")` | Also check range |
| `VARCHAR(N)` | `expect_column_value_lengths_to_be_between(0, N)` | |
| `ENUM(...)` | `expect_column_values_to_be_in_set(values)` | |
| `DATE` | `expect_column_values_to_be_dateutil_parseable` | Also check range 1900-2100 |
| `BOOLEAN` | `expect_column_values_to_be_in_set([0, 1])` | |
| `DECIMAL(P,S)` | `expect_column_values_to_be_of_type("float")` | Check precision |

### Constraint to GX Expectation Mappings

| SQL Constraint | GX Expectation |
|----------------|----------------|
| `PRIMARY KEY` | `expect_column_values_to_be_unique` |
| `NOT NULL` | `expect_column_values_to_not_be_null` |
| `UNIQUE` | `expect_column_values_to_be_unique` |
| `CHECK (age >= 0)` | `expect_column_values_to_be_between("age", 0, ∞)` |
| `FOREIGN KEY` | Custom expectation (GX doesn't have built-in) |

---

## Use Cases

### 1. Bootstrap Data Quality Testing

**Problem**: New data team inherits legacy database with no quality checks.

**Solution**:
```bash
# Generate GX suite from production dump
pg_dump mydb > dump.sql
sql-splitter gx-generate dump.sql --profile -o gx/

# Review and refine expectations
cd gx/
great_expectations suite edit users

# Add to CI/CD
# .github/workflows/data-quality.yml
- name: Validate nightly dump
  run: sql-splitter gx-validate dump.sql --suite gx/expectations/
```

### 2. Pre-Migration Quality Check

**Problem**: Migrating from MySQL to Postgres, want to ensure data quality before cutover.

**Solution**:
```bash
# Generate expectations from MySQL dump
sql-splitter gx-generate mysql_dump.sql -o gx/

# Validate Postgres dump against same expectations
sql-splitter gx-validate postgres_dump.sql --suite gx/expectations/

# Ensure both dumps meet same quality standards
```

### 3. Synthetic Data Validation

**Problem**: Generated fake data with `sql-splitter redact`, want to ensure it's realistic.

**Solution**:
```bash
# Generate expectations from real production dump
sql-splitter gx-generate prod_dump.sql --profile -o gx/

# Generate synthetic data
sql-splitter redact prod_dump.sql -o synthetic.sql --strategy fake

# Validate synthetic data meets production patterns
sql-splitter gx-validate synthetic.sql --suite gx/expectations/
```

### 4. Dump Quality Regression Testing

**Problem**: Daily dumps should maintain consistent quality over time.

**Solution**:
```bash
# Generate baseline from known-good dump
sql-splitter gx-generate good_dump.sql -o gx/

# Daily validation
sql-splitter gx-validate "dumps/$(date +%Y-%m-%d).sql" \
  --suite gx/expectations/ \
  --output results/$(date +%Y-%m-%d).json

# Track quality metrics over time
```

### 5. Production Database Monitoring

**Problem**: Want same quality checks on live database as on dumps.

**Solution**:
```bash
# Generate expectations from dump
sql-splitter gx-generate dump.sql -o gx/

# Use GX natively against live database
great_expectations checkpoint run users_checkpoint \
  --datasource postgresql://prod-db:5432/mydb

# Same expectations, multiple data sources!
```

---

## Performance Considerations

### Generation Performance

| Dump Size | Schema-Only | With Profiling (10k sample) |
|-----------|-------------|----------------------------|
| 100 MB | 2 seconds | 15 seconds |
| 1 GB | 5 seconds | 45 seconds |
| 10 GB | 15 seconds | 2 minutes |
| 100 GB | 1 minute | 8 minutes |

**Profiling overhead**: ~5-10x slower than schema-only.

**Optimization**:
```bash
# Profile only key columns
sql-splitter gx-generate dump.sql \
  --profile-columns users.email,users.age,orders.total

# Smaller sample size
sql-splitter gx-generate dump.sql --sample-size 5000
```

### Validation Performance

**Native validation** (Rust):
- Faster than GX for simple expectations (uniqueness, nulls)
- 2-5x speedup vs Python

**GX validation** (Python):
- Slower but supports all 300+ expectations
- Better reporting and documentation

**Hybrid approach**:
```bash
# Quick check with native validator
sql-splitter gx-validate dump.sql --suite gx/ --native --fail-fast

# Full validation with GX (if quick check passes)
great_expectations checkpoint run my_checkpoint
```

---

## Challenges and Solutions

### Challenge 1: Foreign Key Expectations

**Problem**: GX doesn't have built-in FK expectation.

**Solution**: Create custom expectation wrapper.

```json
{
  "expectation_type": "expect_column_pair_values_A_to_be_in_B",
  "kwargs": {
    "column_A": "order_id",
    "column_B": "id",
    "table_B": "orders"
  }
}
```

Validate in Rust:
```rust
fn validate_foreign_key(
    &self,
    table_a: &TableData,
    column_a: &str,
    table_b: &TableData,
    column_b: &str,
) -> Result<ExpectationResult> {
    let values_a: HashSet<_> = table_a.get_column(column_a)?.iter().collect();
    let values_b: HashSet<_> = table_b.get_column(column_b)?.iter().collect();

    let orphaned: Vec<_> = values_a.difference(&values_b).collect();

    Ok(ExpectationResult {
        success: orphaned.is_empty(),
        unexpected_count: orphaned.len(),
        unexpected_list: orphaned.into_iter().take(20).cloned().collect(),
    })
}
```

### Challenge 2: CHECK Constraint Parsing

**Problem**: Convert SQL CHECK constraints to GX expectations.

```sql
CREATE TABLE users (
  age INT CHECK (age >= 18 AND age <= 120),
  status VARCHAR(20) CHECK (status IN ('active', 'inactive', 'suspended'))
);
```

**Solution**: Parse CHECK expression AST.

```rust
fn parse_check_to_expectation(&self, expr: &Expr) -> Result<Expectation> {
    match expr {
        // age >= 18 AND age <= 120
        Expr::BinaryOp { left, op: And, right } => {
            // Parse as range
            let (min, max) = self.extract_range(left, right)?;
            Ok(Expectation::column_values_to_be_between(column, min, max))
        }

        // status IN ('active', 'inactive')
        Expr::InList { expr, list, .. } => {
            let column = extract_column_name(expr)?;
            let values = extract_string_literals(list)?;
            Ok(Expectation::column_values_to_be_in_set(column, values))
        }

        _ => bail!("Unsupported CHECK constraint: {:?}", expr),
    }
}
```

### Challenge 3: Large Dump Profiling

**Problem**: Can't load 100GB dump into memory for profiling.

**Solution**: Streaming sampler.

```rust
pub struct StreamingSampler {
    reservoir: Vec<Row>,
    size: usize,
    count: usize,
}

impl StreamingSampler {
    // Reservoir sampling algorithm
    pub fn add_row(&mut self, row: Row) {
        self.count += 1;

        if self.reservoir.len() < self.size {
            self.reservoir.push(row);
        } else {
            // Randomly replace existing row
            let idx = rand::thread_rng().gen_range(0..self.count);
            if idx < self.size {
                self.reservoir[idx] = row;
            }
        }
    }

    pub fn get_sample(&self) -> &[Row] {
        &self.reservoir
    }
}
```

**Result**: Constant memory usage, uniform random sample.

### Challenge 4: Expectation Drift

**Problem**: Data evolves, expectations become outdated.

**Solution**: Versioned expectation suites.

```bash
# Generate new suite, compare with old
sql-splitter gx-generate new_dump.sql -o gx/v2/

# Diff expectation suites
sql-splitter gx-diff gx/v1/ gx/v2/

# Output:
# users.email: Added regex validation
# orders.total: Range expanded from [0, 10000] to [0, 50000]
# products.category: New value 'electronics' added to set
```

---

## Effort Breakdown

### Phase 1: Core Integration (8 hours)

- **Expectation data structures** (1h)
  - Rust types for GX JSON format
  - Serialization/deserialization
  - 20 common expectation types

- **Schema-based generation** (3h)
  - Parse CREATE TABLE statements
  - Map constraints to expectations
  - Column type → expectation inference

- **CLI integration** (2h)
  - `gx-generate` command
  - File I/O, GX project structure
  - Output formatting

- **Testing** (2h)
  - Unit tests for expectation generation
  - Integration tests with sample dumps
  - GX compatibility validation

### Phase 2: Data Profiling (4 hours)

- **Sampling implementation** (2h)
  - Reservoir sampling for large dumps
  - Parse INSERT statements
  - Extract column values

- **Statistical inference** (2h)
  - Null percentage detection
  - Cardinality analysis
  - Numeric range inference
  - Pattern detection (email, URL, phone)

### Phase 3: Validation (4 hours)

- **Native validator** (3h)
  - Implement 15-20 common expectations
  - Uniqueness, nulls, ranges, regexes
  - Result formatting

- **CLI integration** (1h)
  - `gx-validate` command
  - Result output (text, JSON, HTML)

**Total: 16 hours**

---

## Next Steps

1. **v1.17.0 Implementation**:
   - Implement core GX integration
   - Release `gx-generate` and `gx-validate` commands
   - Documentation and examples

2. **v1.18.0 Enhancements**:
   - Add native validator for offline validation
   - Expectation suite diffing
   - Advanced profiling (distribution analysis)

3. **Future Integrations**:
   - dbt integration (generate tests from expectations)
   - Airflow integration (data quality DAGs)
   - DataHub integration (lineage + quality metadata)

---

**Recommendation**: Implement GX integration for v1.17.0. It's a high-impact feature with manageable scope (16h) that addresses a critical gap (comprehensive data quality testing) and positions sql-splitter as a complete data quality toolkit.
