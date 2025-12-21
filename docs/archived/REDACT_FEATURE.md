# Redact Command Design

**Status**: Planning (v1.10.0)  
**Date**: 2025-12-20  
**Last Updated**: 2025-12-21

> **Implementation Plan**: See [REDACT_IMPLEMENTATION_PLAN.md](REDACT_IMPLEMENTATION_PLAN.md) for the detailed 8-phase implementation plan (~74h total effort).

## Overview

The `redact` command anonymizes sensitive data (PII) in SQL dumps by replacing real values with fake, hashed, or masked alternatives. This enables safe sharing of production-like data for development, testing, and demos.

## Command Interface

```bash
# Redact using config file
sql-splitter redact dump.sql -o safe.sql --config redact.yaml

# Redact specific columns inline
sql-splitter redact dump.sql -o safe.sql --columns "*.email,*.password,users.ssn"

# Redact with specific strategies
sql-splitter redact dump.sql -o safe.sql \
  --hash "users.email" \
  --fake "users.name,users.phone" \
  --null "users.ssn"

# Preview redaction (show samples)
sql-splitter redact dump.sql --preview --table users

# Validate config without processing
sql-splitter redact dump.sql --config redact.yaml --validate

# Deterministic redaction (reproducible)
sql-splitter redact dump.sql -o safe.sql --config redact.yaml --seed 42
```

## CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output file path | stdout |
| `-c, --config` | YAML config file | none |
| `--columns` | Columns to redact (glob pattern) | none |
| `--hash` | Columns to hash (SHA256) | none |
| `--fake` | Columns to replace with fake data | none |
| `--null` | Columns to set to NULL | none |
| `--mask` | Columns to partially mask | none |
| `--constant` | Column=value pairs | none |
| `--preview` | Show sample redactions | false |
| `--validate` | Validate config only | false |
| `--seed` | Random seed for reproducibility | random |
| `-t, --table` | Only redact specific tables | all |
| `-d, --dialect` | SQL dialect | auto-detect |
| `-p, --progress` | Show progress bar | false |

## Configuration File

### Full Example (redact.yaml)

```yaml
# Global settings
seed: 12345  # For reproducible fake data
locale: en_US  # For locale-specific fakes

# Default strategy for unmatched sensitive columns
defaults:
  strategy: null  # null, hash, mask, skip
  
# Column rules (processed in order, first match wins)
rules:
  # Exact column match
  - column: users.ssn
    strategy: null
    
  # Glob pattern matching
  - column: "*.email"
    strategy: fake_email
    
  - column: "*.password"
    strategy: constant
    value: "$2b$10$REDACTED_PASSWORD_HASH"
    
  - column: "*.phone"
    strategy: fake_phone
    format: "+1 (###) ###-####"
    
  - column: "users.name"
    strategy: fake_name
    
  - column: "*.first_name"
    strategy: fake_first_name
    
  - column: "*.last_name"
    strategy: fake_last_name
    
  - column: "*.address"
    strategy: fake_address
    
  - column: "*.credit_card"
    strategy: mask
    pattern: "****-****-****-XXXX"  # Keep last 4
    
  - column: "*.ip_address"
    strategy: fake_ip
    
  - column: "*.birth_date"
    strategy: fake_date
    min: "1950-01-01"
    max: "2005-12-31"
    
  # Hash for referential integrity
  - column: "*.user_email"
    strategy: hash
    preserve_domain: true  # user@gmail.com → a1b2c3@gmail.com
    
  # Table-specific override
  - column: admins.email
    strategy: skip  # Don't redact admin emails

# Tables to skip entirely
skip_tables:
  - schema_migrations
  - ar_internal_metadata
  
# Tables to include (if set, only these are processed)
# include_tables:
#   - users
#   - orders
```

## Redaction Strategies

### 1. `null` — Replace with NULL

```yaml
- column: "*.ssn"
  strategy: null
```

**Before:** `'123-45-6789'`  
**After:** `NULL`

### 2. `constant` — Fixed Value

```yaml
- column: "*.password"
  strategy: constant
  value: "$2b$10$REDACTED"
```

**Before:** `'$2b$10$real_hash...'`  
**After:** `'$2b$10$REDACTED'`

### 3. `hash` — Deterministic Hash

```yaml
- column: "*.email"
  strategy: hash
  algorithm: sha256  # sha256, md5, xxhash
  preserve_format: true  # Keep @domain.com
```

**Before:** `'john.doe@company.com'`  
**After:** `'a1b2c3d4@company.com'`

**Properties:**
- Same input → same output (referential integrity)
- One-way (can't reverse to original)
- Optional format preservation

### 4. `fake_*` — Realistic Fake Data

```yaml
- column: "*.name"
  strategy: fake_name
  locale: en_US
```

**Available fake strategies:**

| Strategy | Example Output |
|----------|----------------|
| `fake_email` | `jessica.smith@example.com` |
| `fake_name` | `Robert Johnson` |
| `fake_first_name` | `Sarah` |
| `fake_last_name` | `Williams` |
| `fake_phone` | `+1 (555) 234-5678` |
| `fake_address` | `123 Oak Street, Springfield, IL 62701` |
| `fake_street` | `456 Maple Avenue` |
| `fake_city` | `Portland` |
| `fake_state` | `California` |
| `fake_zip` | `90210` |
| `fake_country` | `United States` |
| `fake_company` | `Acme Corporation` |
| `fake_job_title` | `Software Engineer` |
| `fake_username` | `cooluser42` |
| `fake_url` | `https://example.com/page` |
| `fake_ip` | `192.168.1.100` |
| `fake_ipv6` | `2001:db8::1` |
| `fake_uuid` | `550e8400-e29b-41d4-a716-446655440000` |
| `fake_date` | `1985-07-23` |
| `fake_datetime` | `2024-03-15 14:30:00` |
| `fake_credit_card` | `4532015112830366` |
| `fake_iban` | `DE89370400440532013000` |
| `fake_lorem` | `Lorem ipsum dolor sit amet...` |

### 5. `mask` — Partial Masking

```yaml
- column: "*.credit_card"
  strategy: mask
  pattern: "****-****-****-XXXX"  # X = keep original
  
- column: "*.email"
  strategy: mask
  pattern: "X***@XXXXX"  # Keep first char and domain
```

**Before:** `'4532-0151-1283-0366'`  
**After:** `'****-****-****-0366'`

**Pattern syntax:**
- `*` = replace with `*`
- `X` = keep original character
- `#` = replace with random digit
- Any other char = literal

### 6. `shuffle` — Swap Within Column

```yaml
- column: "users.salary"
  strategy: shuffle
```

Randomly redistributes values within the column. Preserves distribution but breaks row correlation.

### 7. `skip` — No Redaction

```yaml
- column: "admins.email"
  strategy: skip
```

Explicitly skip redaction for specific columns.

## Column Matching

### Glob Patterns

| Pattern | Matches |
|---------|---------|
| `users.email` | Only `email` in `users` table |
| `*.email` | `email` column in any table |
| `users.*` | All columns in `users` table |
| `*.*_email` | Columns ending in `_email` in any table |
| `*.password*` | Columns containing `password` |

### Match Order

Rules are evaluated in order; first match wins:

```yaml
rules:
  - column: admins.email    # Specific: skip
    strategy: skip
  - column: "*.email"       # General: hash
    strategy: hash
```

Admin emails skipped, all other emails hashed.

## Implementation Architecture

### Core Components

```
src/
├── cmd/
│   └── redact.rs           # CLI handler
├── redactor/
│   ├── mod.rs              # Public API
│   ├── config.rs           # Config parsing
│   ├── matcher.rs          # Column pattern matching
│   ├── strategy/
│   │   ├── mod.rs          # Strategy trait
│   │   ├── null.rs
│   │   ├── constant.rs
│   │   ├── hash.rs
│   │   ├── fake.rs
│   │   ├── mask.rs
│   │   └── shuffle.rs
│   ├── parser.rs           # INSERT value extraction
│   └── writer.rs           # Output generation
```

### Key Types

```rust
pub struct RedactConfig {
    pub input: PathBuf,
    pub output: Option<PathBuf>,
    pub dialect: SqlDialect,
    pub rules: Vec<RedactRule>,
    pub skip_tables: Vec<String>,
    pub seed: Option<u64>,
    pub locale: String,
    pub progress: bool,
}

pub struct RedactRule {
    pub pattern: ColumnPattern,
    pub strategy: RedactStrategy,
}

pub enum ColumnPattern {
    Exact { table: String, column: String },
    TableGlob { table: String, column_pattern: String },
    ColumnGlob { column_pattern: String },
    FullGlob { pattern: String },
}

pub enum RedactStrategy {
    Null,
    Constant { value: String },
    Hash { algorithm: HashAlgorithm, preserve_format: bool },
    Fake { generator: FakeGenerator },
    Mask { pattern: String },
    Shuffle,
    Skip,
}

pub enum FakeGenerator {
    Email,
    Name,
    FirstName,
    LastName,
    Phone { format: Option<String> },
    Address,
    // ... etc
}

pub struct RedactStats {
    pub tables_processed: usize,
    pub rows_processed: u64,
    pub columns_redacted: HashMap<String, u64>,
    pub bytes_input: u64,
    pub bytes_output: u64,
}
```

### Streaming Architecture

```
Input File → Statement Parser → INSERT Parser → Column Matcher
                                                      ↓
                                               Strategy Applier
                                                      ↓
                                               INSERT Rebuilder
                                                      ↓
                                               Output Writer
```

**Key requirements:**
- Stream processing (no full file in memory)
- Parse INSERT, modify values, reserialize
- Maintain SQL validity

## Fake Data Generation

### Library Choice

Use `fake` crate with `rand` for Rust:

```rust
use fake::{Fake, Faker};
use fake::faker::name::en::*;
use fake::faker::internet::en::*;

let name: String = Name().fake();
let email: String = SafeEmail().fake();
```

### Locale Support

```yaml
locale: de_DE
```

Generates German names, addresses, etc.

### Seeded Generation

```yaml
seed: 42
```

Same seed + same input position = same fake value.
Enables reproducible test data.

## Edge Cases

### 1. Foreign Key References

If `users.email` is referenced by `orders.customer_email`:

**Solution:** Use `hash` strategy for both:
```yaml
- column: "*.email"
  strategy: hash
```

Same email → same hash, preserves referential integrity.

### 2. Unique Constraints

Fake data might generate duplicates:

**Solution:** 
- Hash strategies are deterministic (no duplicates if input unique)
- Fake strategies can append sequence number if needed
- `--ensure-unique` flag for critical columns

### 3. NULL Values

Original NULL should remain NULL (don't generate fake):
```sql
-- Original: INSERT INTO users (email) VALUES (NULL)
-- Redacted: INSERT INTO users (email) VALUES (NULL)
```

### 4. Data Type Mismatches

Fake phone in INT column:

**Solution:** Type-aware generation
- INT columns get numeric fakes
- Validate strategy matches column type

### 5. Multi-Value INSERTs

```sql
INSERT INTO users (email, name) VALUES
('a@example.com', 'Alice'),
('b@example.com', 'Bob');
```

Must redact each row independently.

### 6. Quoted Values & Escaping

Must handle:
- Single quotes in strings: `'O''Brien'`
- Backslash escapes (MySQL): `'line1\nline2'`
- Binary data: `X'48454C4C4F'`

## Security Considerations

### 1. One-Way Hashing

Never use reversible encoding for sensitive data.
SHA256 is default; consider Argon2 for passwords.

### 2. Seed Security

If seed is known, fake data is predictable.
Don't commit seed to version control for production use.

### 3. Audit Trail

Log which columns were redacted:
```
Redacted: users.email (1,234 values, strategy: hash)
Redacted: users.ssn (1,234 values, strategy: null)
```

### 4. Validation

`--validate` mode checks:
- All sensitive column patterns have rules
- No conflicting rules
- Strategies compatible with column types

## Performance Targets

| File Size | Target Time |
|-----------|-------------|
| 100 MB | < 5 seconds |
| 1 GB | < 30 seconds |
| 10 GB | < 5 minutes |

Bottleneck is typically fake data generation, not I/O.

### Optimizations

1. **Lazy initialization**: Only init fake generators for used strategies
2. **Batch hashing**: Hash multiple values together
3. **Pre-compiled patterns**: Compile glob patterns once
4. **Parallel processing**: Process multiple tables concurrently (future)

## Testing Strategy

### Unit Tests
- Each redaction strategy
- Pattern matching logic
- Config parsing
- Value extraction and replacement

### Integration Tests
- Full file redaction
- All dialects
- Roundtrip: redact → import → verify no PII

### Property Tests
- Output is valid SQL
- Redacted values match strategy expectations
- NULL handling correct

### Security Tests
- No original PII in output
- Hash collisions don't occur
- Deterministic with seed

## Example Workflows

### 1. Development Database

```bash
# Create safe dev copy from production
sql-splitter redact prod_backup.sql \
  -o dev_data.sql \
  --config redact.yaml \
  --seed 42
```

### 2. Quick Anonymization

```bash
# Inline column specification
sql-splitter redact dump.sql -o safe.sql \
  --null "*.ssn,*.tax_id" \
  --hash "*.email" \
  --fake "*.name,*.phone"
```

### 3. GDPR Compliance

```yaml
# gdpr-redact.yaml
rules:
  - column: "*.email"
    strategy: hash
  - column: "*.name"
    strategy: fake_name
  - column: "*.phone"
    strategy: null
  - column: "*.address"
    strategy: null
  - column: "*.ip_address"
    strategy: fake_ip
  - column: "*.birth_date"
    strategy: null
```

```bash
sql-splitter redact export.sql -o gdpr_safe.sql --config gdpr-redact.yaml
```

### 4. Demo Dataset

```bash
# Combine with sample for small demo
sql-splitter sample prod.sql --percent 5 | \
  sql-splitter redact --config demo-redact.yaml -o demo.sql
```

## Estimated Effort

| Component | Effort |
|-----------|--------|
| CLI and config parsing | 3 hours |
| Pattern matcher | 3 hours |
| INSERT parser/rebuilder | 6 hours |
| Strategy: null, constant, skip | 2 hours |
| Strategy: hash | 3 hours |
| Strategy: mask | 3 hours |
| Strategy: fake (all generators) | 8 hours |
| Strategy: shuffle | 3 hours |
| Locale support | 2 hours |
| Seeded RNG | 2 hours |
| Preview mode | 2 hours |
| Testing | 8 hours |
| **Total** | **~45 hours** |

## Future Enhancements

1. **Auto-detection**: Scan for columns likely containing PII
2. **Encryption**: Reversible encryption for authorized access
3. **Format preservation**: Maintain email domain distribution
4. **Statistical preservation**: Keep value distributions
5. **Conditional redaction**: `where: "role != 'admin'"`
6. **Plugin strategies**: Custom redaction functions

## Related

- [Sample Feature](SAMPLE_FEATURE.md) — Often used together
- [Query Feature](QUERY_FEATURE.md) — Extract before redacting
- [Split Command](../../src/cmd/split.rs)
