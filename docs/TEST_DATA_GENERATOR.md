# Test Data Generator Design

**Status**: Draft  
**Date**: 2025-12-20

## Overview

A Rust-based deterministic test data generator that creates realistic multi-tenant SQL dumps for integration testing. The generator defines a single logical schema and can emit MySQL, PostgreSQL, and SQLite dumps at various scales.

## Goals

1. **Realistic complexity** — Mirror production multi-tenant SaaS patterns
2. **Full edge case coverage** — FK chains, self-refs, junctions, cycles
3. **Dialect agnostic** — One schema, three SQL outputs
4. **Reproducible** — Seed-based deterministic generation
5. **Scalable** — Small (unit tests) to Large (benchmarks)
6. **No real data exposure** — Synthetic names, no production schema

## Synthetic Schema

Based on patterns observed in real multi-tenant Laravel/MySQL applications, but with generic domain naming.

### Entity Relationship Diagram

```
                                    ┌─────────────────────────────────────────────┐
                                    │              MULTI-TENANT CORE              │
                                    └─────────────────────────────────────────────┘
                                                         │
                                                         ▼
                              ┌─────────────────────────────────────────────────────┐
                              │                      tenants                         │
                              │  id, name, slug, created_at, updated_at              │
                              └───────────────────────────┬─────────────────────────┘
                                                          │
                   ┌──────────────────┬───────────────────┼──────────────────┬─────────────────┐
                   │                  │                   │                  │                 │
                   ▼                  ▼                   ▼                  ▼                 ▼
            ┌───────────┐      ┌───────────┐       ┌───────────┐      ┌───────────┐     ┌───────────┐
            │   users   │      │ customers │       │ products  │      │  folders  │     │  roles    │
            │  tenant_id│      │ tenant_id │       │ tenant_id │      │ tenant_id │     │ tenant_id │
            │  email    │      │ name      │       │ sku       │      │ parent_id │     │ name      │
            │  name     │      │ email     │       │ price     │      │ (self-FK) │     │           │
            └─────┬─────┘      └─────┬─────┘       └─────┬─────┘      └───────────┘     └─────┬─────┘
                  │                  │                   │                                    │
                  │                  │                   │                                    │
         ┌────────┴────────┐        │                   │                                    │
         │                 │        │                   │                                    │
         ▼                 ▼        ▼                   ▼                                    ▼
   ┌───────────┐     ┌───────────────────┐       ┌───────────┐                        ┌───────────┐
   │ projects  │     │      orders       │       │categories │                        │user_roles │
   │ tenant_id │     │ tenant_id         │       │ tenant_id │                        │ (junction)│
   │ owner_id  │     │ customer_id (FK)  │       │ parent_id │                        │ user_id   │
   │           │     │ status            │       │ (self-FK) │                        │ role_id   │
   └─────┬─────┘     └─────────┬─────────┘       └───────────┘                        └───────────┘
         │                     │
         ▼                     ▼
   ┌───────────┐       ┌───────────────┐
   │   tasks   │       │  order_items  │
   │ tenant_id │       │ order_id (FK) │
   │project_id │       │ product_id(FK)│
   │assignee_id│       │ quantity      │
   └─────┬─────┘       └───────────────┘
         │
         ▼
   ┌────────────────────┐
   │     comments       │
   │ tenant_id          │
   │ parent_id (self-FK)│
   │ commentable_type   │  ◄── Polymorphic
   │ commentable_id     │
   └────────────────────┘


  ┌──────────────────────────────────────────────────────────────────────┐
  │                        GLOBAL TABLES (no tenant_id)                  │
  ├──────────────────────────────────────────────────────────────────────┤
  │  permissions        │  currencies        │  countries               │
  │  migrations         │  failed_jobs       │  job_batches             │
  └──────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────┐
  │                     JUNCTION TABLES (many-to-many)                   │
  ├──────────────────────────────────────────────────────────────────────┤
  │  role_permissions (role_id ─► roles, permission_id ─► permissions)  │
  │  user_roles (user_id ─► users, role_id ─► roles)                    │
  └──────────────────────────────────────────────────────────────────────┘
```

### Table Definitions

#### Tenant Core

```rust
Table::new("tenants")
    .column("id", Serial, primary_key: true)
    .column("name", VarChar(100), not_null: true)
    .column("slug", VarChar(50), not_null: true, unique: true)
    .timestamps()
```

#### Users (Tenant-Owned, Soft Deletes)

```rust
Table::new("users")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("email", VarChar(255), not_null: true)
    .column("name", VarChar(100))
    .column("role", VarChar(50))  // For testing WHERE filters
    .column("active", Boolean, default: true)
    .timestamps()
    .soft_deletes()  // deleted_at nullable timestamp
```

#### Projects & Tasks (FK Chain)

```rust
Table::new("projects")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("owner_id", Integer)
        .foreign_key("users", "id", on_delete: SetNull)
    .column("name", VarChar(200), not_null: true)
    .column("status", VarChar(20), default: "active")
    .timestamps()

Table::new("tasks")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("project_id", Integer, not_null: true)
        .foreign_key("projects", "id", on_delete: Cascade)
    .column("assignee_id", Integer)
        .foreign_key("users", "id", on_delete: SetNull)
    .column("title", VarChar(200), not_null: true)
    .column("priority", Integer, default: 0)
    .column("completed", Boolean, default: false)
    .timestamps()
```

#### Customers & Orders (Deep FK Chain for Sharding)

```rust
Table::new("customers")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("name", VarChar(200), not_null: true)
    .column("email", VarChar(255))
    .column("phone", VarChar(50))
    .timestamps()

Table::new("orders")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("customer_id", Integer, not_null: true)
        .foreign_key("customers", "id", on_delete: Cascade)
    .column("order_number", VarChar(50), not_null: true)
    .column("status", VarChar(20), default: "pending")
    .column("total", Decimal(10, 2))
    .timestamps()

Table::new("order_items")  // NO tenant_id - FK chain only
    .column("id", Serial, primary_key: true)
    .column("order_id", Integer, not_null: true)
        .foreign_key("orders", "id", on_delete: Cascade)
    .column("product_id", Integer, not_null: true)
        .foreign_key("products", "id", on_delete: Restrict)
    .column("quantity", Integer, not_null: true, default: 1)
    .column("unit_price", Decimal(10, 2), not_null: true)
```

#### Products & Categories (Hierarchical)

```rust
Table::new("categories")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("parent_id", Integer)
        .foreign_key("categories", "id", on_delete: Cascade)  // Self-FK
    .column("name", VarChar(100), not_null: true)
    .column("level", Integer, default: 0)
    .timestamps()

Table::new("products")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("category_id", Integer)
        .foreign_key("categories", "id", on_delete: SetNull)
    .column("sku", VarChar(50), not_null: true)
    .column("name", VarChar(200), not_null: true)
    .column("price", Decimal(10, 2), not_null: true)
    .column("active", Boolean, default: true)
    .timestamps()
```

#### Folders (Self-Referential Tree)

```rust
Table::new("folders")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("parent_id", Integer)
        .foreign_key("folders", "id", on_delete: Cascade)  // Self-FK
    .column("name", VarChar(100), not_null: true)
    .column("path", VarChar(500))  // Materialized path for testing
    .timestamps()
```

#### Comments (Self-Ref + Polymorphic)

```rust
Table::new("comments")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer, not_null: true)
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("parent_id", Integer)
        .foreign_key("comments", "id", on_delete: Cascade)  // Self-FK
    .column("user_id", Integer)
        .foreign_key("users", "id", on_delete: SetNull)
    .column("commentable_type", VarChar(50), not_null: true)  // "project", "task", etc.
    .column("commentable_id", Integer, not_null: true)
    .column("body", Text, not_null: true)
    .timestamps()
```

#### Roles & Permissions (Junction Tables)

```rust
Table::new("permissions")  // Global, no tenant_id
    .column("id", Serial, primary_key: true)
    .column("name", VarChar(100), not_null: true, unique: true)
    .column("description", VarChar(255))

Table::new("roles")
    .column("id", Serial, primary_key: true)
    .column("tenant_id", Integer)  // Nullable for global roles
        .foreign_key("tenants", "id", on_delete: Cascade)
    .column("name", VarChar(50), not_null: true)
    .column("is_system", Boolean, default: false)
    .timestamps()

Table::new("role_permissions")  // Junction
    .column("role_id", Integer, not_null: true)
        .foreign_key("roles", "id", on_delete: Cascade)
    .column("permission_id", Integer, not_null: true)
        .foreign_key("permissions", "id", on_delete: Cascade)
    .primary_key(["role_id", "permission_id"])

Table::new("user_roles")  // Junction
    .column("user_id", Integer, not_null: true)
        .foreign_key("users", "id", on_delete: Cascade)
    .column("role_id", Integer, not_null: true)
        .foreign_key("roles", "id", on_delete: Cascade)
    .primary_key(["user_id", "role_id"])
```

#### System/Framework Tables

```rust
Table::new("migrations")  // Global
    .column("id", Serial, primary_key: true)
    .column("migration", VarChar(255), not_null: true)
    .column("batch", Integer, not_null: true)

Table::new("failed_jobs")  // Global
    .column("id", Serial, primary_key: true)
    .column("uuid", VarChar(36), unique: true)
    .column("connection", Text, not_null: true)
    .column("queue", Text, not_null: true)
    .column("payload", Text, not_null: true)
    .column("exception", Text, not_null: true)
    .column("failed_at", Timestamp, default: CurrentTimestamp)

Table::new("job_batches")  // Global
    .column("id", VarChar(36), primary_key: true)
    .column("name", VarChar(255), not_null: true)
    .column("total_jobs", Integer, not_null: true)
    .column("pending_jobs", Integer, not_null: true)
    .column("failed_jobs", Integer, not_null: true)
    .column("created_at", Integer, not_null: true)

Table::new("currencies")  // Lookup
    .column("id", Serial, primary_key: true)
    .column("code", VarChar(3), not_null: true, unique: true)
    .column("name", VarChar(50), not_null: true)
    .column("symbol", VarChar(5))

Table::new("countries")  // Lookup
    .column("id", Serial, primary_key: true)
    .column("code", VarChar(2), not_null: true, unique: true)
    .column("name", VarChar(100), not_null: true)
```

### Total Table Count: 18

| Category | Tables | Has tenant_id |
|----------|--------|---------------|
| Tenant-owned | 10 | Yes |
| Child (FK only) | 1 | No (order_items) |
| Junction | 2 | No |
| Global/Lookup | 5 | No |
| **Total** | **18** | 10 (56%) |

---

## Data Generation

### Scale Profiles

```rust
pub enum Scale {
    /// Fast unit tests (~500 rows)
    Small,
    /// Integration tests (~10K rows)
    Medium,
    /// Benchmark tests (~200K rows)
    Large,
    /// Custom scale multiplier
    Custom { tenants: u32, multiplier: f32 },
}

impl Scale {
    pub fn config(&self) -> ScaleConfig {
        match self {
            Scale::Small => ScaleConfig {
                tenants: 3,
                users_per_tenant: 5,
                projects_per_tenant: 3,
                tasks_per_project: 5,
                customers_per_tenant: 5,
                orders_per_customer: 2,
                items_per_order: 3,
                products_per_tenant: 10,
                folders_per_tenant: 10,
                folder_depth: 3,
                comments_per_tenant: 20,
            },
            Scale::Medium => ScaleConfig {
                tenants: 10,
                users_per_tenant: 50,
                projects_per_tenant: 20,
                tasks_per_project: 25,
                customers_per_tenant: 50,
                orders_per_customer: 10,
                items_per_order: 5,
                products_per_tenant: 100,
                folders_per_tenant: 50,
                folder_depth: 5,
                comments_per_tenant: 200,
            },
            Scale::Large => ScaleConfig {
                tenants: 50,
                users_per_tenant: 200,
                projects_per_tenant: 100,
                tasks_per_project: 50,
                customers_per_tenant: 200,
                orders_per_customer: 25,
                items_per_order: 8,
                products_per_tenant: 500,
                folders_per_tenant: 200,
                folder_depth: 7,
                comments_per_tenant: 1000,
            },
            Scale::Custom { tenants, multiplier } => {
                // Apply multiplier to Medium config
                Scale::Medium.config().scale(*tenants, *multiplier)
            }
        }
    }
}
```

### Deterministic RNG

```rust
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

pub struct Generator {
    rng: ChaCha8Rng,
    config: ScaleConfig,
}

impl Generator {
    pub fn new(seed: u64, scale: Scale) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            config: scale.config(),
        }
    }
    
    pub fn generate(&mut self, dialect: Dialect) -> String {
        let mut output = String::new();
        
        // Header
        output.push_str(&self.render_header(dialect));
        
        // Schema (CREATE TABLEs)
        output.push_str(&self.render_schema(dialect));
        
        // Data (INSERTs or COPY)
        output.push_str(&self.render_data(dialect));
        
        // Footer
        output.push_str(&self.render_footer(dialect));
        
        output
    }
}
```

### Fake Data Helpers

Minimal fake data without heavy dependencies:

```rust
const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter",
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
];

const COMPANY_WORDS: &[&str] = &[
    "Tech", "Global", "Digital", "Smart", "Cloud", "Data", "Cyber",
    "Quantum", "Alpha", "Beta", "Omega", "Prime", "Core", "Max",
];

const PRODUCT_ADJECTIVES: &[&str] = &[
    "Premium", "Standard", "Basic", "Pro", "Ultra", "Lite", "Plus",
];

const PRODUCT_NOUNS: &[&str] = &[
    "Widget", "Gadget", "Device", "Tool", "Component", "Module", "Unit",
];

impl Generator {
    fn fake_name(&mut self) -> String {
        let first = self.choose(FIRST_NAMES);
        let last = self.choose(LAST_NAMES);
        format!("{} {}", first, last)
    }
    
    fn fake_email(&mut self, name: &str) -> String {
        let normalized = name.to_lowercase().replace(' ', ".");
        let domain = self.choose(&["example.com", "test.org", "demo.net"]);
        format!("{}@{}", normalized, domain)
    }
    
    fn fake_company(&mut self) -> String {
        let word1 = self.choose(COMPANY_WORDS);
        let word2 = self.choose(COMPANY_WORDS);
        format!("{}{} Inc", word1, word2)
    }
    
    fn fake_product(&mut self) -> String {
        let adj = self.choose(PRODUCT_ADJECTIVES);
        let noun = self.choose(PRODUCT_NOUNS);
        format!("{} {}", adj, noun)
    }
    
    fn fake_timestamp(&mut self, base: DateTime) -> DateTime {
        let days_offset = self.rng.gen_range(-365..0);
        base + Duration::days(days_offset)
    }
    
    fn choose<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.rng.gen_range(0..items.len())]
    }
}
```

---

## Dialect Rendering

### Type Mapping

```rust
pub enum SqlType {
    Serial,
    Integer,
    BigInt,
    VarChar(u16),
    Text,
    Boolean,
    Decimal(u8, u8),
    Timestamp,
    Date,
}

impl SqlType {
    pub fn render(&self, dialect: Dialect) -> &'static str {
        match (self, dialect) {
            // Serial/Auto-increment
            (SqlType::Serial, Dialect::MySql) => "INT AUTO_INCREMENT",
            (SqlType::Serial, Dialect::Postgres) => "SERIAL",
            (SqlType::Serial, Dialect::Sqlite) => "INTEGER",
            
            // Integer
            (SqlType::Integer, _) => "INTEGER",
            (SqlType::BigInt, _) => "BIGINT",
            
            // Strings
            (SqlType::VarChar(n), Dialect::Sqlite) => "TEXT",
            (SqlType::VarChar(n), _) => return format!("VARCHAR({})", n).leak(),
            (SqlType::Text, _) => "TEXT",
            
            // Boolean
            (SqlType::Boolean, Dialect::MySql) => "TINYINT(1)",
            (SqlType::Boolean, Dialect::Postgres) => "BOOLEAN",
            (SqlType::Boolean, Dialect::Sqlite) => "INTEGER",
            
            // Decimal
            (SqlType::Decimal(p, s), Dialect::Sqlite) => "REAL",
            (SqlType::Decimal(p, s), _) => return format!("DECIMAL({},{})", p, s).leak(),
            
            // Timestamp
            (SqlType::Timestamp, Dialect::MySql) => "DATETIME",
            (SqlType::Timestamp, Dialect::Postgres) => "TIMESTAMP",
            (SqlType::Timestamp, Dialect::Sqlite) => "TEXT",
            
            (SqlType::Date, Dialect::Sqlite) => "TEXT",
            (SqlType::Date, _) => "DATE",
        }
    }
}
```

### Identifier Quoting

```rust
impl Dialect {
    pub fn quote_identifier(&self, name: &str) -> String {
        match self {
            Dialect::MySql => format!("`{}`", name),
            Dialect::Postgres => format!("\"{}\"", name),
            Dialect::Sqlite => format!("\"{}\"", name),
        }
    }
}
```

### Value Rendering

```rust
impl Value {
    pub fn render(&self, dialect: Dialect) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => match dialect {
                Dialect::MySql | Dialect::Sqlite => if *b { "1" } else { "0" }.to_string(),
                Dialect::Postgres => if *b { "TRUE" } else { "FALSE" }.to_string(),
            },
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format!("{:.2}", f),
            Value::String(s) => {
                let escaped = match dialect {
                    Dialect::MySql => s.replace('\'', "\\'").replace('\\', "\\\\"),
                    Dialect::Postgres | Dialect::Sqlite => s.replace('\'', "''"),
                };
                format!("'{}'", escaped)
            },
            Value::Timestamp(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S")),
        }
    }
}
```

### INSERT vs COPY

```rust
impl Generator {
    fn render_table_data(&mut self, table: &Table, dialect: Dialect) -> String {
        let rows = self.generate_rows(table);
        
        match dialect {
            Dialect::Postgres if rows.len() > 100 => {
                // Use COPY for large datasets
                self.render_copy(table, &rows)
            },
            _ => {
                // Use INSERT
                self.render_inserts(table, &rows, dialect)
            }
        }
    }
    
    fn render_copy(&self, table: &Table, rows: &[Row]) -> String {
        let mut out = format!(
            "COPY {} ({}) FROM stdin;\n",
            table.name,
            table.columns.iter().map(|c| c.name).collect::<Vec<_>>().join(", ")
        );
        
        for row in rows {
            let values: Vec<String> = row.values.iter()
                .map(|v| match v {
                    Value::Null => "\\N".to_string(),
                    Value::String(s) => s.replace('\t', "\\t").replace('\n', "\\n"),
                    _ => v.render(Dialect::Postgres),
                })
                .collect();
            out.push_str(&values.join("\t"));
            out.push('\n');
        }
        out.push_str("\\.\n\n");
        out
    }
    
    fn render_inserts(&self, table: &Table, rows: &[Row], dialect: Dialect) -> String {
        let mut out = String::new();
        let q = |s| dialect.quote_identifier(s);
        
        // Batch into groups of 100 rows
        for chunk in rows.chunks(100) {
            out.push_str(&format!(
                "INSERT INTO {} ({}) VALUES\n",
                q(table.name),
                table.columns.iter().map(|c| q(c.name)).collect::<Vec<_>>().join(", ")
            ));
            
            for (i, row) in chunk.iter().enumerate() {
                let values: Vec<String> = row.values.iter()
                    .map(|v| v.render(dialect))
                    .collect();
                out.push_str(&format!("({})", values.join(", ")));
                if i < chunk.len() - 1 {
                    out.push_str(",\n");
                } else {
                    out.push_str(";\n\n");
                }
            }
        }
        out
    }
}
```

---

## CLI Interface

```bash
# Generate medium MySQL dump
cargo run -p test_data_gen -- --dialect mysql --scale medium --seed 42 > fixtures/mysql/medium.sql

# Generate small PostgreSQL dump
cargo run -p test_data_gen -- --dialect postgres --scale small --seed 42 > fixtures/postgres/small.sql

# Generate large SQLite dump for benchmarking
cargo run -p test_data_gen -- --dialect sqlite --scale large --seed 42 > fixtures/sqlite/large.sql

# Custom scale
cargo run -p test_data_gen -- --dialect mysql --tenants 100 --multiplier 2.0 --seed 42
```

### CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `--dialect` | Output dialect: mysql, postgres, sqlite | mysql |
| `--scale` | Size preset: small, medium, large | medium |
| `--tenants` | Custom tenant count (overrides scale) | — |
| `--multiplier` | Scale multiplier for rows | 1.0 |
| `--seed` | Random seed for reproducibility | 12345 |
| `--schema-only` | Only output CREATE TABLE statements | false |
| `--data-only` | Only output INSERT/COPY statements | false |
| `--pretty` | Add extra whitespace and comments | true |

---

## Project Structure

```
sql-splitter/
├── crates/
│   └── test_data_gen/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs           # Public API
│           ├── schema.rs        # Table/Column definitions
│           ├── generator.rs     # Data generation
│           ├── renderer.rs      # Dialect-specific SQL output
│           ├── fake.rs          # Fake data helpers
│           └── bin/
│               └── main.rs      # CLI binary
├── tests/
│   ├── fixtures/
│   │   ├── static/              # Hand-crafted edge cases
│   │   │   ├── mysql/
│   │   │   │   ├── weird_comments.sql
│   │   │   │   └── escaped_strings.sql
│   │   │   ├── postgres/
│   │   │   │   ├── copy_with_nulls.sql
│   │   │   │   └── dollar_quoting.sql
│   │   │   └── sqlite/
│   │   │       └── minimal.sql
│   │   └── generated/           # .gitignore'd
│   │       ├── mysql/
│   │       │   ├── small.sql
│   │       │   ├── medium.sql
│   │       │   └── large.sql
│   │       ├── postgres/
│   │       └── sqlite/
│   └── integration/
│       ├── common/mod.rs        # Test utilities
│       ├── split_test.rs
│       ├── merge_test.rs
│       ├── sample_test.rs
│       └── shard_test.rs
```

### Cargo.toml

```toml
# crates/test_data_gen/Cargo.toml
[package]
name = "test_data_gen"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "gen-fixtures"
path = "src/bin/main.rs"

[dependencies]
rand = "0.8"
rand_chacha = "0.3"
chrono = "0.4"
clap = { version = "4", features = ["derive"] }
```

---

## Integration with Tests

### Test Helper Module

```rust
// tests/common/mod.rs
use std::path::PathBuf;
use test_data_gen::{Generator, Dialect, Scale};

pub fn fixture_path(dialect: &str, scale: &str) -> PathBuf {
    let generated = format!("tests/fixtures/generated/{}/{}.sql", dialect, scale);
    let path = PathBuf::from(&generated);
    
    if !path.exists() {
        // Generate on-demand if not cached
        let dialect = match dialect {
            "mysql" => Dialect::MySql,
            "postgres" => Dialect::Postgres,
            "sqlite" => Dialect::Sqlite,
            _ => panic!("Unknown dialect"),
        };
        let scale = match scale {
            "small" => Scale::Small,
            "medium" => Scale::Medium,
            "large" => Scale::Large,
            _ => panic!("Unknown scale"),
        };
        
        let mut gen = Generator::new(12345, scale);
        let sql = gen.generate(dialect);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, sql).unwrap();
    }
    
    path
}

pub fn static_fixture(dialect: &str, name: &str) -> PathBuf {
    PathBuf::from(format!("tests/fixtures/static/{}/{}.sql", dialect, name))
}
```

### Example Integration Test

```rust
// tests/integration/shard_test.rs
use crate::common::fixture_path;
use sql_splitter::shard::{Shard, ShardConfig};
use tempfile::TempDir;

#[test]
fn test_shard_extracts_single_tenant() {
    let input = fixture_path("mysql", "medium");
    let output_dir = TempDir::new().unwrap();
    
    let config = ShardConfig {
        tenant_column: "tenant_id".to_string(),
        tenant_value: "1".to_string(),
        include_global: IncludeGlobal::Lookups,
        ..Default::default()
    };
    
    let stats = Shard::new(input, output_dir.path().join("tenant_1.sql"))
        .with_config(config)
        .run()
        .unwrap();
    
    // Tenant 1 should have:
    // - 1 tenant
    // - 50 users (medium scale)
    // - Related orders, tasks, etc.
    assert_eq!(stats.tenants_extracted, 1);
    assert!(stats.total_rows > 500);  // Substantial data
    assert!(stats.total_rows < 2000); // But not all tenants
    
    // Verify FK integrity
    assert_eq!(stats.orphan_warnings, 0);
}

#[test]
fn test_shard_handles_deep_fk_chain() {
    let input = fixture_path("mysql", "small");
    let output_dir = TempDir::new().unwrap();
    
    let stats = Shard::new(input, output_dir.path().join("tenant.sql"))
        .with_tenant_value("1")
        .run()
        .unwrap();
    
    // order_items has no tenant_id, should be included via:
    // order_items → orders → tenant_id = 1
    assert!(stats.tables_with_data.contains("order_items"));
}
```

---

## Static Fixtures for Edge Cases

Hand-crafted SQL files for specific syntax edge cases:

### MySQL: Escaped Strings

```sql
-- tests/fixtures/static/mysql/escaped_strings.sql
CREATE TABLE `test_escapes` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `data` VARCHAR(255)
);

INSERT INTO `test_escapes` (`id`, `data`) VALUES
(1, 'Simple string'),
(2, 'String with ''quotes'''),
(3, 'String with \'backslash quotes\''),
(4, 'Line1\nLine2'),
(5, 'Tab\there'),
(6, 'Backslash\\here');
```

### PostgreSQL: COPY with NULLs

```sql
-- tests/fixtures/static/postgres/copy_with_nulls.sql
CREATE TABLE copy_test (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  value INTEGER
);

COPY copy_test (id, name, value) FROM stdin;
1	Alice	100
2	\N	200
3	Carol	\N
\.
```

### SQLite: Minimal Multi-Tenant

```sql
-- tests/fixtures/static/sqlite/minimal_tenant.sql
CREATE TABLE "tenants" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL
);

CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "tenant_id" INTEGER NOT NULL REFERENCES "tenants"("id"),
  "email" TEXT NOT NULL
);

INSERT INTO "tenants" ("id", "name") VALUES (1, 'Tenant A'), (2, 'Tenant B');
INSERT INTO "users" ("id", "tenant_id", "email") VALUES
(1, 1, 'user1@a.com'),
(2, 1, 'user2@a.com'),
(3, 2, 'user1@b.com');
```

---

## Estimated Effort

| Component | Effort |
|-----------|--------|
| Schema model (tables, columns, FKs) | 4h |
| Generator core (RNG, scale configs) | 3h |
| Fake data helpers | 2h |
| MySQL renderer | 3h |
| PostgreSQL renderer (+ COPY) | 4h |
| SQLite renderer | 2h |
| CLI binary | 2h |
| Static fixtures (edge cases) | 3h |
| Test harness integration | 3h |
| Documentation | 2h |
| **Total** | **~28h** |

---

## Related

- [Roadmap](ROADMAP.md) — Feature development plan
- [Sample Feature](features/SAMPLE_FEATURE.md) — Uses generated fixtures
- [Shard Feature](features/SHARD_FEATURE.md) — Uses generated fixtures
