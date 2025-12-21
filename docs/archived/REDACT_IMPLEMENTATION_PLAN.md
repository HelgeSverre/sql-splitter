# Redact Command Implementation Plan v2

**Version**: v1.10.0  
**Last Updated**: 2025-12-21  
**Status**: Planning

This document provides a concrete implementation plan for the `redact` command.

---

## Executive Summary

The redact command anonymizes sensitive data (PII) in SQL dumps by replacing real values with fake, hashed, masked, or shuffled alternatives. This enables safe sharing of production-like data for development, testing, and demos.

**v1.10.0 Full Scope:**
- Streaming redaction of MySQL/PostgreSQL INSERT statements
- PostgreSQL COPY block redaction (in-place, maintaining COPY format)
- All strategies: null, constant, hash, mask, shuffle, fake_*, skip
- Full fake data catalog (25+ generators)
- Multi-locale support with fallback
- YAML config with glob patterns + `--generate-config` for auto-generation
- Deterministic mode with `--seed` for reproducibility
- Consistent `--progress` bar behavior

---

## Architecture Overview

### Data Flow

```
Input File → Statement Parser → Statement Router
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
              INSERT Router     COPY Router       Passthrough
                    │                 │                 │
                    ▼                 ▼                 │
              Row Parser        COPY Parser             │
                    │                 │                 │
                    ▼                 ▼                 │
              Column Matcher ◄────────┘                 │
                    │                                   │
                    ▼                                   │
              Strategy Applier                          │
                    │                                   │
                    ▼                                   │
              Statement Rebuilder                       │
                    │                                   │
                    └───────────────────────────────────┘
                                      │
                                      ▼
                                Output Writer
```

### Key Design Decisions

1. **Two-pass architecture** (like sample/shard):
   - Pass 1: Build schema lookup from CREATE TABLE statements
   - Pass 2: Stream through file, redacting INSERT and COPY statements

2. **COPY in-place redaction**: Maintain PostgreSQL COPY format (not converting to INSERT)

3. **Reuse existing modules**:
   - `Parser` for statement streaming
   - `InsertParser` (mysql_insert.rs) for INSERT row parsing
   - `parse_postgres_copy_rows` for COPY parsing
   - `SchemaBuilder` for column/type information
   - `glob` crate for pattern matching

4. **Full locale support**: All `fake` crate locales, fallback to `en` with warning

5. **Config generation**: `--generate-config` analyzes input and produces annotated YAML

---

## Dependencies

### New Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `fake` | 4 | Fake data generation (names, emails, phones, addresses, etc.) |
| `sha2` | 0.10 | SHA256 hashing for hash strategy |
| `hex` | 0.4 | Hex encoding for hash output |

### Existing Dependencies (Reused)

- `glob` (0.3) — Column pattern matching
- `serde` + `serde_yaml` — YAML config parsing
- `rand` (0.8) — RNG with seeding
- `ahash` — Fast HashMap for column lookups
- `indicatif` — Progress bar (consistent with other commands)


---

## File Structure

```
src/
├── cmd/
│   └── redact.rs              # CLI handler, argument parsing
├── redactor/
│   ├── mod.rs                 # Public API, Redactor struct, run()
│   ├── config.rs              # RedactConfig, YAML parsing, generation
│   ├── matcher.rs             # ColumnMatcher, glob pattern compilation
│   ├── strategy/
│   │   ├── mod.rs             # Strategy trait, StrategyKind enum
│   │   ├── null.rs            # NullStrategy
│   │   ├── constant.rs        # ConstantStrategy
│   │   ├── hash.rs            # HashStrategy (SHA256, format preservation)
│   │   ├── mask.rs            # MaskStrategy (pattern-based)
│   │   ├── shuffle.rs         # ShuffleStrategy (column redistribution)
│   │   ├── fake.rs            # FakeStrategy (all generators, locale)
│   │   └── skip.rs            # SkipStrategy (passthrough)
│   ├── insert_rewriter.rs     # MySQL/PostgreSQL INSERT rewriting
│   ├── copy_rewriter.rs       # PostgreSQL COPY block rewriting
│   ├── config_generator.rs    # --generate-config implementation
│   └── output.rs              # Statistics, JSON output
```

---

## CLI Interface

```rust
#[derive(Parser)]
#[command(visible_alias = "rd")]
#[command(after_help = "Examples:
  sql-splitter redact dump.sql -o safe.sql --config redact.yaml
  sql-splitter redact dump.sql -o safe.sql --null \"*.ssn\" --hash \"*.email\"
  sql-splitter redact dump.sql --generate-config -o redact.yaml
  sql-splitter redact dump.sql -o safe.sql --config redact.yaml --seed 42")]
pub struct RedactArgs {
    /// Input SQL file (supports .gz, .bz2, .xz, .zst)
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// Output file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// SQL dialect: mysql, postgres, sqlite (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// YAML config file for redaction rules
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    config: Option<PathBuf>,

    /// Generate annotated YAML config by analyzing input file
    #[arg(long, help_heading = MODE)]
    generate_config: bool,

    /// Columns to set to NULL (glob patterns, comma-separated)
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    null: Vec<String>,

    /// Columns to hash with SHA256 (glob patterns)
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    hash: Vec<String>,

    /// Columns to replace with fake data (glob patterns)
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    fake: Vec<String>,

    /// Columns to mask (format: pattern=column, e.g., "****-XXXX=*.credit_card")
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    mask: Vec<String>,

    /// Column=value pairs for constant replacement
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    constant: Vec<String>,

    /// Random seed for reproducible redaction
    #[arg(long, help_heading = MODE)]
    seed: Option<u64>,

    /// Locale for fake data generation (default: en)
    #[arg(long, default_value = "en", help_heading = MODE)]
    locale: String,

    /// Only redact specific tables (comma-separated)
    #[arg(short, long, value_delimiter = ',', help_heading = FILTERING)]
    tables: Vec<String>,

    /// Exclude specific tables (comma-separated)
    #[arg(short, long, value_delimiter = ',', help_heading = FILTERING)]
    exclude: Vec<String>,

    /// Fail on warnings (e.g., unsupported locale)
    #[arg(long, help_heading = BEHAVIOR)]
    strict: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Preview without writing files
    #[arg(long, help_heading = BEHAVIOR)]
    dry_run: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,

    /// Validate config only, don't process
    #[arg(long, help_heading = BEHAVIOR)]
    validate: bool,
}
```


---

## Phase 1: Core Infrastructure (10h)

### 1.1 CLI and Config Types (4h)

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 1.1.1 Add Redact command to CLI | 0.5h | `src/cmd/mod.rs`, `src/cmd/redact.rs` |
| 1.1.2 Define RedactConfig struct | 0.5h | All config fields |
| 1.1.3 Define YAML config types | 1h | `RedactYamlConfig`, `Rule`, `Defaults` |
| 1.1.4 Implement config loading | 0.5h | File parsing, validation |
| 1.1.5 CLI-to-config merging | 1h | Merge inline flags with YAML |
| 1.1.6 Config validation | 0.5h | Check patterns, strategies, params |

**YAML Config Structure:**

```yaml
# sql-splitter redact configuration
# Generated by: sql-splitter redact dump.sql --generate-config

# Random seed for reproducible redaction (optional)
# Same seed + same input = same output
seed: 12345

# Locale for fake data generation
# Supported: en, de_de, fr_fr, zh_cn, zh_tw, ja_jp, pt_br, ar_sa, etc.
# Falls back to 'en' with warning if locale not available
locale: en

# Default strategy for columns not matching any rule
# Options: null, skip (default: skip)
defaults:
  strategy: skip

# Redaction rules (processed in order, first match wins)
rules:
  # --- Email columns ---
  - column: "*.email"
    strategy: hash
    preserve_domain: true  # user@gmail.com → a1b2c3@gmail.com

  # --- Password columns ---
  - column: "*.password"
    strategy: constant
    value: "$2b$10$REDACTED_HASH"

  # --- SSN/Tax ID columns ---
  - column: "*.ssn"
    strategy: null

  - column: "*.tax_id"
    strategy: null

  # --- Name columns ---
  - column: "*.name"
    strategy: fake_name

  - column: "*.first_name"
    strategy: fake_first_name

  - column: "*.last_name"
    strategy: fake_last_name

  # --- Phone columns ---
  - column: "*.phone"
    strategy: fake_phone

  # --- Address columns ---
  - column: "*.address"
    strategy: fake_address

  - column: "*.street"
    strategy: fake_street

  - column: "*.city"
    strategy: fake_city

  - column: "*.zip"
    strategy: fake_zip

  # --- Credit card with masking ---
  - column: "*.credit_card"
    strategy: mask
    pattern: "****-****-****-XXXX"  # Keep last 4 digits

  # --- IP addresses ---
  - column: "*.ip_address"
    strategy: fake_ip

  # --- Dates ---
  - column: "*.birth_date"
    strategy: fake_date
    min: "1950-01-01"
    max: "2005-12-31"

  # --- Skip admin emails (specific override before general rule) ---
  - column: "admins.email"
    strategy: skip

# Tables to skip entirely (no redaction applied)
skip_tables:
  - schema_migrations
  - ar_internal_metadata

# Tables to include (if set, only these tables are processed)
# include_tables:
#   - users
#   - orders
```

### 1.2 Column Matcher (2h)

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 1.2.1 Define ColumnPattern type | 0.5h | Parsed glob representation |
| 1.2.2 Implement pattern compilation | 0.5h | Convert "*.email" to matcher |
| 1.2.3 Build column→strategy map | 0.5h | Pre-compute for each table/column |
| 1.2.4 Tests | 0.5h | Pattern matching edge cases |

### 1.3 Strategy Trait and Types (4h)

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 1.3.1 Define Strategy trait | 0.5h | `apply(&mut self, value: &RedactValue) -> RedactValue` |
| 1.3.2 Define StrategyKind enum | 1h | All strategy variants with params |
| 1.3.3 Define RedactValue type | 0.5h | NULL, String, Number, Hex, Other |
| 1.3.4 Implement strategy factory | 1.5h | Create strategy from config + locale |
| 1.3.5 Tests | 0.5h | Strategy creation |

**Key Types:**

```rust
#[derive(Debug, Clone)]
pub enum ValueKind {
    Null,
    String(String),
    Number(String),
    Hex(Vec<u8>),
    Other(String),
}

#[derive(Debug, Clone)]
pub struct RedactValue {
    pub kind: ValueKind,
    pub raw: Vec<u8>,
}

pub trait Strategy: Send {
    fn apply(&mut self, value: &RedactValue) -> RedactValue;
}

#[derive(Debug, Clone)]
pub enum StrategyKind {
    Null,
    Constant { value: String },
    Hash { algorithm: String, preserve_domain: bool },
    Mask { pattern: String },
    Shuffle,
    Fake(FakeKind),
    Skip,
}

#[derive(Debug, Clone)]
pub enum FakeKind {
    Email, Name, FirstName, LastName, Phone,
    Address, Street, City, State, Zip, Country,
    Company, JobTitle, Username, Url,
    Ip, Ipv6, Uuid, Date, DateTime,
    CreditCard, Iban, Lorem,
}
```


---

## Phase 2: Strategy Implementations (14h)

### 2.1 Simple Strategies (2h)

**Null Strategy (0.5h):**
```rust
pub struct NullStrategy;

impl Strategy for NullStrategy {
    fn apply(&mut self, _value: &RedactValue) -> RedactValue {
        RedactValue { kind: ValueKind::Null, raw: b"NULL".to_vec() }
    }
}
```

**Constant Strategy (0.5h):**
```rust
pub struct ConstantStrategy { value: String }

impl Strategy for ConstantStrategy {
    fn apply(&mut self, value: &RedactValue) -> RedactValue {
        if value.is_null() { return value.clone(); }
        let escaped = escape_sql_string(&self.value);
        RedactValue {
            kind: ValueKind::String(self.value.clone()),
            raw: format!("'{}'", escaped).into_bytes(),
        }
    }
}
```

**Skip Strategy (0.5h):**
```rust
pub struct SkipStrategy;

impl Strategy for SkipStrategy {
    fn apply(&mut self, value: &RedactValue) -> RedactValue {
        value.clone()
    }
}
```

### 2.2 Hash Strategy (2h)

**Features:**
- SHA256 hashing (default)
- Optional domain preservation for emails
- Deterministic output (same input → same hash)

```rust
use sha2::{Sha256, Digest};

pub struct HashStrategy {
    preserve_domain: bool,
}

impl Strategy for HashStrategy {
    fn apply(&mut self, value: &RedactValue) -> RedactValue {
        if value.is_null() { return value.clone(); }
        
        let Some(s) = value.as_string() else { return value.clone(); };
        
        let hashed = if self.preserve_domain && s.contains('@') {
            let parts: Vec<&str> = s.splitn(2, '@').collect();
            if parts.len() == 2 {
                format!("{}@{}", self.hash_str(parts[0]), parts[1])
            } else {
                self.hash_str(s)
            }
        } else {
            self.hash_str(s)
        };
        
        RedactValue::from_string(&hashed)
    }
}

impl HashStrategy {
    fn hash_str(&self, input: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let result = hasher.finalize();
        hex::encode(&result[..8])  // 16 hex chars
    }
}
```

### 2.3 Mask Strategy (3h)

**Pattern Syntax:**
- `*` = replace with `*`
- `X` = keep original character
- `#` = replace with random digit
- Any other char = literal

**Examples:**
- `****-****-****-XXXX` on `4532-0151-1283-0366` → `****-****-****-0366`
- `X***@XXXXX` on `john@example.com` → `j***@examp`

```rust
pub struct MaskStrategy {
    pattern: String,
    rng: StdRng,
}

impl Strategy for MaskStrategy {
    fn apply(&mut self, value: &RedactValue) -> RedactValue {
        if value.is_null() { return value.clone(); }
        
        let Some(s) = value.as_string() else { return value.clone(); };
        
        let chars: Vec<char> = s.chars().collect();
        let pattern_chars: Vec<char> = self.pattern.chars().collect();
        
        let mut result = String::new();
        let mut input_idx = 0;
        
        for p in &pattern_chars {
            match p {
                '*' => result.push('*'),
                'X' => {
                    if input_idx < chars.len() {
                        result.push(chars[input_idx]);
                        input_idx += 1;
                    }
                }
                '#' => {
                    result.push(char::from_digit(self.rng.gen_range(0..10), 10).unwrap());
                    input_idx += 1;
                }
                c => {
                    result.push(*c);
                    input_idx += 1;
                }
            }
        }
        
        RedactValue::from_string(&result)
    }
}
```

### 2.4 Shuffle Strategy (3h)

**Behavior:**
- Collects all values for column during first pass
- Redistributes randomly during output
- Preserves NULL positions
- Full memory implementation (revisit with chunking if profiling shows issues)

```rust
pub struct ShuffleStrategy {
    values: Vec<RedactValue>,
    shuffled: Vec<RedactValue>,
    index: usize,
    initialized: bool,
}

impl ShuffleStrategy {
    pub fn new() -> Self {
        Self { values: vec![], shuffled: vec![], index: 0, initialized: false }
    }
    
    /// Called during collection phase
    pub fn collect(&mut self, value: &RedactValue) {
        if !value.is_null() {
            self.values.push(value.clone());
        }
    }
    
    /// Called to finalize and shuffle
    pub fn finalize(&mut self, rng: &mut StdRng) {
        self.shuffled = self.values.clone();
        self.shuffled.shuffle(rng);
        self.initialized = true;
    }
}

impl Strategy for ShuffleStrategy {
    fn apply(&mut self, value: &RedactValue) -> RedactValue {
        if value.is_null() { return value.clone(); }
        
        if self.index < self.shuffled.len() {
            let result = self.shuffled[self.index].clone();
            self.index += 1;
            result
        } else {
            value.clone()  // Fallback if mismatch
        }
    }
}
```

**Note:** Shuffle requires a two-phase approach:
1. Collection phase: gather all non-NULL values
2. Application phase: distribute shuffled values

This adds complexity to the rewriter but maintains correctness.

### 2.5 Fake Strategy with Full Locale Support (4h)

**Supported Generators (25+):**

| Generator | Output Example | Locale-aware |
|-----------|----------------|--------------|
| `fake_email` | `jessica.smith@example.com` | Yes |
| `fake_name` | `Robert Johnson` | Yes |
| `fake_first_name` | `Sarah` | Yes |
| `fake_last_name` | `Williams` | Yes |
| `fake_phone` | `+1 (555) 234-5678` | Yes |
| `fake_address` | `123 Oak St, Springfield, IL 62701` | Yes |
| `fake_street` | `456 Maple Avenue` | Yes |
| `fake_city` | `Portland` | Yes |
| `fake_state` | `California` | Yes |
| `fake_zip` | `90210` | Yes |
| `fake_country` | `United States` | Yes |
| `fake_company` | `Acme Corporation` | Yes |
| `fake_job_title` | `Software Engineer` | No |
| `fake_username` | `cooluser42` | No |
| `fake_url` | `https://example.com/page` | No |
| `fake_ip` | `192.168.1.100` | No |
| `fake_ipv6` | `2001:db8::1` | No |
| `fake_uuid` | `550e8400-e29b-...` | No |
| `fake_date` | `1985-07-23` | No |
| `fake_datetime` | `2024-03-15 14:30:00` | No |
| `fake_credit_card` | `4532015112830366` | No |
| `fake_iban` | `DE89370400440532013000` | Yes |
| `fake_lorem` | `Lorem ipsum dolor...` | No |

**Locale Implementation:**

```rust
use fake::locales::*;

pub struct FakeStrategy {
    kind: FakeKind,
    locale: SupportedLocale,
    rng: StdRng,
}

#[derive(Debug, Clone)]
pub enum SupportedLocale {
    En, DeDe, FrFr, ZhCn, ZhTw, JaJp, PtBr, ArSa,
}

impl FakeStrategy {
    pub fn new(kind: FakeKind, locale_str: &str, rng: &mut StdRng, strict: bool) 
        -> anyhow::Result<Self> 
    {
        let locale = match locale_str.to_lowercase().as_str() {
            "en" | "en_us" => SupportedLocale::En,
            "de" | "de_de" => SupportedLocale::DeDe,
            "fr" | "fr_fr" => SupportedLocale::FrFr,
            "zh_cn" => SupportedLocale::ZhCn,
            "zh_tw" => SupportedLocale::ZhTw,
            "ja" | "ja_jp" => SupportedLocale::JaJp,
            "pt" | "pt_br" => SupportedLocale::PtBr,
            "ar" | "ar_sa" => SupportedLocale::ArSa,
            other => {
                if strict {
                    anyhow::bail!("Unsupported locale: {}. Use --locale with a supported value.", other);
                }
                eprintln!("Warning: Locale '{}' not supported, falling back to 'en'", other);
                SupportedLocale::En
            }
        };
        
        let seed: u64 = rng.gen();
        Ok(Self {
            kind,
            locale,
            rng: StdRng::seed_from_u64(seed),
        })
    }
    
    fn generate(&mut self) -> String {
        // Dispatch based on kind and locale
        match (&self.kind, &self.locale) {
            (FakeKind::Email, SupportedLocale::En) => {
                use fake::faker::internet::en::SafeEmail;
                SafeEmail().fake_with_rng(&mut self.rng)
            }
            (FakeKind::Name, SupportedLocale::En) => {
                use fake::faker::name::en::Name;
                Name().fake_with_rng(&mut self.rng)
            }
            (FakeKind::Name, SupportedLocale::DeDe) => {
                use fake::faker::name::de_de::Name;
                Name().fake_with_rng(&mut self.rng)
            }
            // ... etc for all combinations
            _ => self.generate_fallback_en(),
        }
    }
}
```


---

## Phase 3: Statement Rewriting (16h)

### 3.1 Schema Lookup (2h)

Reuse existing `SchemaBuilder` and `SchemaGraph` from sample/shard.

```rust
pub struct SchemaLookup {
    tables: AHashMap<String, TableColumns>,
}

pub struct TableColumns {
    pub columns: Vec<String>,
    pub column_types: Vec<ColumnType>,
    pub column_indices: AHashMap<String, usize>,
}

impl SchemaLookup {
    pub fn from_schema_graph(graph: &SchemaGraph) -> Self {
        // Convert SchemaGraph tables to lookup-optimized structure
    }
    
    pub fn get(&self, table: &str) -> Option<&TableColumns> {
        self.tables.get(&table.to_lowercase())
    }
}
```

### 3.2 INSERT Rewriting (5h)

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 3.2.1 Value extraction from INSERT | 2h | Parse multi-row INSERTs |
| 3.2.2 Value serialization | 1h | RedactValue → SQL bytes |
| 3.2.3 Statement reassembly | 1h | Header + VALUES + rows |
| 3.2.4 Tests | 1h | Roundtrip tests |

```rust
pub fn rewrite_insert(
    stmt: &[u8],
    table_name: &str,
    schema: &TableColumns,
    strategies: &mut [Box<dyn Strategy>],
    dialect: SqlDialect,
) -> anyhow::Result<Vec<u8>> {
    // 1. Find VALUES keyword position
    let values_pos = find_values_keyword(stmt)?;
    let header = &stmt[..values_pos + 6]; // Include "VALUES"
    
    // 2. Parse rows
    let rows = parse_insert_values(&stmt[values_pos..], schema, dialect)?;
    
    // 3. Apply strategies to each row
    let redacted_rows: Vec<Vec<RedactValue>> = rows.iter()
        .map(|row| {
            row.iter().zip(strategies.iter_mut())
                .map(|(val, strategy)| strategy.apply(val))
                .collect()
        })
        .collect();
    
    // 4. Rebuild statement
    let mut output = Vec::with_capacity(stmt.len());
    output.extend_from_slice(header);
    output.push(b'\n');
    
    for (i, row) in redacted_rows.iter().enumerate() {
        if i > 0 { output.extend_from_slice(b",\n"); }
        output.push(b'(');
        for (j, val) in row.iter().enumerate() {
            if j > 0 { output.extend_from_slice(b", "); }
            output.extend_from_slice(&val.raw);
        }
        output.push(b')');
    }
    output.extend_from_slice(b";\n");
    
    Ok(output)
}
```

### 3.3 PostgreSQL COPY Rewriting (7h)

**Key Challenge:** Maintain COPY format while redacting values in-place.

**COPY Format:**
```sql
COPY users (id, email, name) FROM stdin;
1	alice@example.com	Alice Smith
2	bob@example.com	Bob Jones
\.
```

**Approach:**
1. Parse COPY header for table name and column list
2. Parse each data line as tab-separated values
3. Apply strategies to each value
4. Rebuild lines maintaining tab separation
5. Preserve `\.` terminator

```rust
pub fn rewrite_copy_block(
    header: &[u8],      // "COPY users (id, email, name) FROM stdin;"
    data_lines: &[&[u8]], // Tab-separated value lines
    table_name: &str,
    schema: &TableColumns,
    strategies: &mut [Box<dyn Strategy>],
) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    
    // 1. Write header unchanged
    output.extend_from_slice(header);
    output.push(b'\n');
    
    // 2. Parse column order from COPY header
    let copy_columns = parse_copy_columns(header)?;
    let column_map = map_copy_to_schema(&copy_columns, schema);
    
    // 3. Process each data line
    for line in data_lines {
        let values = parse_copy_line(line)?;  // Tab-split
        let mut redacted_values: Vec<Vec<u8>> = Vec::new();
        
        for (i, val) in values.iter().enumerate() {
            let strategy_idx = column_map.get(i);
            let redact_val = parse_copy_value(val)?;  // Handle \N, escapes
            
            let new_val = if let Some(idx) = strategy_idx {
                strategies[*idx].apply(&redact_val)
            } else {
                redact_val
            };
            
            redacted_values.push(serialize_copy_value(&new_val)?);
        }
        
        // Join with tabs
        for (i, val) in redacted_values.iter().enumerate() {
            if i > 0 { output.push(b'\t'); }
            output.extend_from_slice(val);
        }
        output.push(b'\n');
    }
    
    // 4. Write terminator
    output.extend_from_slice(b"\\.\n");
    
    Ok(output)
}
```

**COPY Value Handling:**
- `\N` → NULL
- `\t` → tab (escaped)
- `\n` → newline (escaped)
- `\\` → backslash

### 3.4 Redactor Core (2h)

```rust
pub struct Redactor {
    dialect: SqlDialect,
    schema: SchemaLookup,
    matcher: ColumnMatcher,
    rng: StdRng,
    locale: String,
    strict: bool,
    include_tables: Option<HashSet<String>>,
    skip_tables: HashSet<String>,
    stats: RedactStats,
    shuffle_collectors: AHashMap<(String, String), ShuffleStrategy>,
}

impl Redactor {
    pub fn redact_statement(&mut self, stmt: &[u8]) -> anyhow::Result<Vec<u8>> {
        let (stmt_type, table_name) = Parser::parse_statement_with_dialect(stmt, self.dialect);
        
        match stmt_type {
            StatementType::Insert => self.redact_insert(stmt, table_name.as_deref()),
            StatementType::Copy => self.redact_copy(stmt, table_name.as_deref()),
            _ => Ok(stmt.to_vec()),
        }
    }
    
    fn redact_insert(&mut self, stmt: &[u8], table: Option<&str>) -> anyhow::Result<Vec<u8>> {
        let table = table.unwrap_or("");
        if self.should_skip_table(table) {
            return Ok(stmt.to_vec());
        }
        
        let Some(schema) = self.schema.get(table) else {
            self.stats.warnings.push(format!("No schema for table '{}', skipping", table));
            return Ok(stmt.to_vec());
        };
        
        let mut strategies = self.create_strategies_for_table(table, &schema.columns)?;
        rewrite_insert(stmt, table, schema, &mut strategies, self.dialect)
    }
    
    fn redact_copy(&mut self, stmt: &[u8], table: Option<&str>) -> anyhow::Result<Vec<u8>> {
        // Similar to redact_insert but uses COPY rewriter
    }
}
```


---

## Phase 4: Config Generation (6h)

### 4.1 Input Analysis (3h)

**Goal:** Analyze input file to detect tables, columns, and suggest redaction strategies.

**PII Detection Patterns:**

| Pattern | Suggested Strategy | Confidence |
|---------|-------------------|------------|
| `*email*` | hash (preserve_domain) | High |
| `*password*`, `*passwd*` | constant | High |
| `*ssn*`, `*social_security*` | null | High |
| `*tax_id*`, `*tin*` | null | High |
| `*phone*`, `*mobile*`, `*cell*` | fake_phone | High |
| `*name*` (not `*username*`) | fake_name | Medium |
| `*first_name*`, `*fname*` | fake_first_name | High |
| `*last_name*`, `*lname*`, `*surname*` | fake_last_name | High |
| `*address*`, `*street*` | fake_address | Medium |
| `*city*` | fake_city | Medium |
| `*zip*`, `*postal*` | fake_zip | Medium |
| `*credit_card*`, `*cc_*` | mask (****-****-****-XXXX) | High |
| `*ip_address*`, `*ip_addr*` | fake_ip | Medium |
| `*birth*`, `*dob*` | fake_date | Medium |
| `*company*`, `*organization*` | fake_company | Low |

```rust
pub struct ColumnAnalysis {
    pub table: String,
    pub column: String,
    pub column_type: String,
    pub suggested_strategy: Option<StrategyKind>,
    pub confidence: Confidence,
    pub sample_values: Vec<String>,  // For display
}

pub enum Confidence {
    High,    // Strong pattern match
    Medium,  // Partial match
    Low,     // Weak signal
    None,    // No suggestion
}

pub fn analyze_for_config(
    input: &Path,
    dialect: SqlDialect,
) -> anyhow::Result<Vec<ColumnAnalysis>> {
    // 1. Build schema
    let schema = build_schema_from_file(input, dialect)?;
    
    // 2. Sample a few values per column (for context)
    let samples = sample_column_values(input, &schema, dialect, 3)?;
    
    // 3. Apply pattern matching
    let mut analyses = Vec::new();
    for (table_name, table_schema) in &schema.tables {
        for col in &table_schema.columns {
            let analysis = analyze_column(table_name, col, &samples);
            analyses.push(analysis);
        }
    }
    
    Ok(analyses)
}
```

### 4.2 YAML Generation (3h)

**Goal:** Generate well-annotated YAML config file.

```rust
pub fn generate_config_yaml(
    analyses: &[ColumnAnalysis],
    output: &Path,
) -> anyhow::Result<()> {
    let mut yaml = String::new();
    
    // Header
    yaml.push_str("# sql-splitter redact configuration\n");
    yaml.push_str("# Generated by: sql-splitter redact <input> --generate-config\n");
    yaml.push_str("#\n");
    yaml.push_str("# Review and modify this file before running redaction.\n");
    yaml.push_str("# See: https://github.com/helgesverre/sql-splitter#redact-config\n");
    yaml.push_str("\n");
    
    // Seed
    yaml.push_str("# Random seed for reproducible redaction (optional)\n");
    yaml.push_str("# seed: 12345\n\n");
    
    // Locale
    yaml.push_str("# Locale for fake data generation\n");
    yaml.push_str("# Supported: en, de_de, fr_fr, zh_cn, zh_tw, ja_jp, pt_br, ar_sa\n");
    yaml.push_str("locale: en\n\n");
    
    // Defaults
    yaml.push_str("# Default strategy for columns not matching any rule\n");
    yaml.push_str("defaults:\n");
    yaml.push_str("  strategy: skip\n\n");
    
    // Rules - grouped by table
    yaml.push_str("# Redaction rules (processed in order, first match wins)\n");
    yaml.push_str("rules:\n");
    
    let mut by_table: BTreeMap<&str, Vec<&ColumnAnalysis>> = BTreeMap::new();
    for analysis in analyses {
        by_table.entry(&analysis.table).or_default().push(analysis);
    }
    
    for (table, columns) in by_table {
        yaml.push_str(&format!("\n  # --- Table: {} ---\n", table));
        
        for col in columns {
            if let Some(strategy) = &col.suggested_strategy {
                let confidence_note = match col.confidence {
                    Confidence::High => "",
                    Confidence::Medium => "  # Medium confidence",
                    Confidence::Low => "  # Low confidence - review",
                    Confidence::None => continue,
                };
                
                yaml.push_str(&format!("  - column: \"{}.{}\"\n", table, col.column));
                yaml.push_str(&format!("    strategy: {}\n", strategy.to_yaml_str()));
                
                // Add strategy-specific params
                match strategy {
                    StrategyKind::Hash { preserve_domain, .. } if *preserve_domain => {
                        yaml.push_str("    preserve_domain: true\n");
                    }
                    StrategyKind::Mask { pattern } => {
                        yaml.push_str(&format!("    pattern: \"{}\"\n", pattern));
                    }
                    StrategyKind::Constant { value } => {
                        yaml.push_str(&format!("    value: \"{}\"\n", value));
                    }
                    _ => {}
                }
                
                if !confidence_note.is_empty() {
                    yaml.push_str(&format!("    {}\n", confidence_note));
                }
            } else {
                // Columns without suggestion - comment out
                yaml.push_str(&format!("  # - column: \"{}.{}\"  # No PII detected\n", 
                    table, col.column));
                yaml.push_str(&format!("  #   strategy: skip\n"));
            }
        }
    }
    
    // Skip tables
    yaml.push_str("\n# Tables to skip entirely (no redaction applied)\n");
    yaml.push_str("skip_tables:\n");
    yaml.push_str("  # - schema_migrations\n");
    yaml.push_str("  # - ar_internal_metadata\n");
    
    std::fs::write(output, yaml)?;
    Ok(())
}
```


---

## Phase 5: Integration, Progress & Output (8h)

### 5.1 Main Run Function (3h)

```rust
pub fn run(config: RedactConfig) -> anyhow::Result<RedactStats> {
    // 1. Handle --generate-config mode
    if config.generate_config {
        let dialect = detect_dialect(&config.input, config.dialect.as_deref())?;
        let analyses = analyze_for_config(&config.input, dialect)?;
        let output = config.output.as_ref()
            .ok_or_else(|| anyhow!("--output required with --generate-config"))?;
        generate_config_yaml(&analyses, output)?;
        return Ok(RedactStats::config_generated(output));
    }
    
    // 2. Detect dialect
    let dialect = detect_dialect(&config.input, config.dialect.as_deref())?;
    
    // 3. Build schema (pass 1)
    let file_size = std::fs::metadata(&config.input)?.len();
    let progress = if config.progress && !config.json {
        Some(create_progress_bar(file_size, "Building schema"))
    } else {
        None
    };
    
    let temp_dir = tempfile::tempdir()?;
    let schema = build_schema_with_progress(&config.input, temp_dir.path(), dialect, &progress)?;
    
    if let Some(pb) = &progress {
        pb.finish_with_message("Schema built");
    }
    
    // 4. Compile rules
    let matcher = ColumnMatcher::from_config(&config)?;
    
    // 5. Validate rules against schema
    if config.validate {
        return validate_config(&config, &schema, &matcher);
    }
    
    // 6. Handle shuffle strategy (requires collection pass)
    let has_shuffle = matcher.has_shuffle_strategies();
    let shuffle_data = if has_shuffle {
        collect_shuffle_values(&config.input, &schema, &matcher, dialect)?
    } else {
        AHashMap::new()
    };
    
    // 7. Create RNG
    let mut rng = match config.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    };
    
    // 8. Create redactor
    let mut redactor = Redactor::new(
        dialect,
        schema,
        matcher,
        &mut rng,
        config.locale.clone(),
        config.strict,
        config.include_tables.clone(),
        config.skip_tables.clone(),
        shuffle_data,
    )?;
    
    // 9. Progress bar for main pass
    let progress = if config.progress && !config.json {
        Some(create_progress_bar(file_size, "Redacting"))
    } else {
        None
    };
    
    // 10. Stream and redact (pass 2 or 3)
    let input = open_file_with_compression(&config.input)?;
    let mut output = create_output_writer(&config.output)?;
    let mut parser = Parser::with_dialect(input, dialect);
    
    let mut bytes_processed = 0u64;
    while let Some(stmt) = parser.next_statement()? {
        if let Some(pb) = &progress {
            bytes_processed += stmt.len() as u64;
            pb.set_position(bytes_processed);
        }
        
        if config.dry_run {
            redactor.stats.record_statement(&stmt)?;
        } else {
            let redacted = redactor.redact_statement(&stmt)?;
            output.write_all(&redacted)?;
        }
    }
    
    output.flush()?;
    
    if let Some(pb) = &progress {
        pb.finish_with_message("Done");
    }
    
    Ok(redactor.stats)
}
```

### 5.2 Progress Bar Consistency (2h)

**Goal:** Match progress bar behavior with other commands (split, sample, convert, etc.)

```rust
use indicatif::{ProgressBar, ProgressStyle};

pub fn create_progress_bar(total_bytes: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total_bytes);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}")
            .unwrap()
            .progress_chars("█▓▒░  ")
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}
```

**Progress phases:**
1. "Building schema" - during schema extraction
2. "Collecting shuffle data" - if shuffle strategy used
3. "Redacting" - main processing pass
4. "Done" - completion

### 5.3 Statistics and JSON Output (3h)

```rust
#[derive(Debug, Default, Serialize)]
pub struct RedactStats {
    pub input_file: String,
    pub output_file: Option<String>,
    pub dialect: String,
    pub mode: String,  // "redact", "generate-config", "validate", "dry-run"
    
    pub statements_processed: u64,
    pub inserts_redacted: u64,
    pub copy_blocks_redacted: u64,
    pub rows_redacted: u64,
    pub bytes_input: u64,
    pub bytes_output: u64,
    pub elapsed_secs: f64,
    
    pub tables_processed: Vec<String>,
    pub tables_skipped: Vec<String>,
    
    pub columns_redacted: HashMap<String, ColumnStats>,
    
    pub strategies_used: HashMap<String, u64>,
    pub locale: String,
    pub seed: Option<u64>,
    
    pub warnings: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct ColumnStats {
    pub values_redacted: u64,
    pub nulls_preserved: u64,
    pub strategy: String,
}

impl RedactStats {
    pub fn print_text(&self) {
        println!("Redaction complete:");
        println!("  Input: {}", self.input_file);
        if let Some(out) = &self.output_file {
            println!("  Output: {}", out);
        }
        println!("  Dialect: {}", self.dialect);
        println!();
        println!("Statistics:");
        println!("  Statements processed: {}", self.statements_processed);
        println!("  INSERTs redacted: {}", self.inserts_redacted);
        println!("  COPY blocks redacted: {}", self.copy_blocks_redacted);
        println!("  Total rows redacted: {}", self.rows_redacted);
        println!();
        println!("Strategies used:");
        for (strategy, count) in &self.strategies_used {
            println!("  {}: {} values", strategy, count);
        }
        if !self.warnings.is_empty() {
            println!();
            println!("Warnings:");
            for w in &self.warnings {
                println!("  - {}", w);
            }
        }
    }
}
```


---

## Phase 6: Testing (10h)

### 6.1 Unit Tests (4h)

```rust
// tests/redact_unit_test.rs

// --- Strategy Tests ---
#[test] fn test_null_strategy_returns_null() { }
#[test] fn test_null_strategy_on_null_value() { }
#[test] fn test_constant_strategy_replaces_value() { }
#[test] fn test_constant_strategy_preserves_null() { }
#[test] fn test_constant_strategy_escapes_quotes() { }
#[test] fn test_skip_strategy_passthrough() { }
#[test] fn test_hash_strategy_deterministic() { }
#[test] fn test_hash_strategy_different_inputs() { }
#[test] fn test_hash_strategy_preserve_domain() { }
#[test] fn test_hash_strategy_no_preserve_domain() { }
#[test] fn test_mask_strategy_star_replaces() { }
#[test] fn test_mask_strategy_x_keeps() { }
#[test] fn test_mask_strategy_hash_random_digit() { }
#[test] fn test_mask_strategy_literal_chars() { }
#[test] fn test_mask_strategy_credit_card_pattern() { }
#[test] fn test_shuffle_strategy_collect_phase() { }
#[test] fn test_shuffle_strategy_apply_phase() { }
#[test] fn test_shuffle_strategy_preserves_nulls() { }
#[test] fn test_fake_email_format() { }
#[test] fn test_fake_name_not_empty() { }
#[test] fn test_fake_phone_format() { }
#[test] fn test_fake_deterministic_with_seed() { }
#[test] fn test_fake_locale_en() { }
#[test] fn test_fake_locale_de() { }
#[test] fn test_fake_locale_fallback_with_warning() { }
#[test] fn test_fake_locale_strict_fails() { }

// --- Matcher Tests ---
#[test] fn test_pattern_exact_match() { }
#[test] fn test_pattern_star_column() { }
#[test] fn test_pattern_star_table() { }
#[test] fn test_pattern_suffix_glob() { }
#[test] fn test_pattern_first_match_wins() { }
#[test] fn test_pattern_default_strategy() { }

// --- Config Tests ---
#[test] fn test_yaml_config_parsing_full() { }
#[test] fn test_yaml_config_minimal() { }
#[test] fn test_cli_flags_to_rules() { }
#[test] fn test_cli_overrides_yaml() { }
#[test] fn test_config_validation_missing_value() { }
#[test] fn test_config_validation_invalid_pattern() { }

// --- Value Parsing Tests ---
#[test] fn test_parse_insert_string_value() { }
#[test] fn test_parse_insert_null_value() { }
#[test] fn test_parse_insert_number_value() { }
#[test] fn test_parse_insert_hex_value() { }
#[test] fn test_parse_insert_escaped_string() { }
#[test] fn test_parse_copy_tab_separated() { }
#[test] fn test_parse_copy_null_marker() { }
#[test] fn test_parse_copy_escaped_tab() { }
#[test] fn test_parse_copy_escaped_newline() { }

// --- Rebuilding Tests ---
#[test] fn test_rebuild_insert_single_row() { }
#[test] fn test_rebuild_insert_multi_row() { }
#[test] fn test_rebuild_copy_block() { }
#[test] fn test_rebuild_copy_with_nulls() { }

// --- Config Generation Tests ---
#[test] fn test_analyze_detects_email_column() { }
#[test] fn test_analyze_detects_password_column() { }
#[test] fn test_analyze_detects_ssn_column() { }
#[test] fn test_generate_yaml_structure() { }
#[test] fn test_generate_yaml_annotations() { }
```

### 6.2 Integration Tests (6h)

```rust
// tests/redact_integration_test.rs

// --- Basic Redaction ---
#[test] fn test_redact_null_strategy_mysql() { }
#[test] fn test_redact_null_strategy_postgres() { }
#[test] fn test_redact_null_strategy_sqlite() { }
#[test] fn test_redact_constant_strategy() { }
#[test] fn test_redact_hash_strategy() { }
#[test] fn test_redact_hash_preserve_domain() { }
#[test] fn test_redact_mask_strategy() { }
#[test] fn test_redact_shuffle_strategy() { }
#[test] fn test_redact_fake_email() { }
#[test] fn test_redact_fake_name() { }
#[test] fn test_redact_fake_phone() { }
#[test] fn test_redact_fake_address() { }
#[test] fn test_redact_skip_strategy() { }

// --- PostgreSQL COPY ---
#[test] fn test_redact_postgres_copy_basic() { }
#[test] fn test_redact_postgres_copy_null_handling() { }
#[test] fn test_redact_postgres_copy_escaped_values() { }
#[test] fn test_redact_postgres_copy_maintains_format() { }
#[test] fn test_redact_postgres_copy_multi_table() { }

// --- Pattern Matching ---
#[test] fn test_redact_glob_star_column() { }
#[test] fn test_redact_glob_star_table() { }
#[test] fn test_redact_specific_table_column() { }
#[test] fn test_redact_first_match_wins() { }

// --- Table Filtering ---
#[test] fn test_redact_include_tables() { }
#[test] fn test_redact_exclude_tables() { }
#[test] fn test_redact_skip_tables_from_yaml() { }

// --- Determinism ---
#[test] fn test_redact_seed_reproducible() { }
#[test] fn test_redact_different_seeds_different_output() { }
#[test] fn test_redact_no_seed_random() { }

// --- Locale ---
#[test] fn test_redact_locale_en() { }
#[test] fn test_redact_locale_de() { }
#[test] fn test_redact_locale_fallback() { }
#[test] fn test_redact_locale_strict_fails() { }

// --- Edge Cases ---
#[test] fn test_redact_null_values_preserved() { }
#[test] fn test_redact_multi_row_insert() { }
#[test] fn test_redact_escaped_strings() { }
#[test] fn test_redact_no_schema_warns() { }
#[test] fn test_redact_empty_file() { }
#[test] fn test_redact_compressed_input() { }

// --- Config Generation ---
#[test] fn test_generate_config_creates_file() { }
#[test] fn test_generate_config_detects_pii() { }
#[test] fn test_generate_config_yaml_valid() { }
#[test] fn test_generate_config_roundtrip() { }

// --- YAML Config ---
#[test] fn test_redact_yaml_config_basic() { }
#[test] fn test_redact_yaml_defaults() { }
#[test] fn test_redact_yaml_rules_order() { }
#[test] fn test_redact_yaml_all_strategies() { }

// --- CLI ---
#[test] fn test_redact_cli_null_flag() { }
#[test] fn test_redact_cli_hash_flag() { }
#[test] fn test_redact_cli_fake_flag() { }
#[test] fn test_redact_cli_mask_flag() { }
#[test] fn test_redact_cli_constant_flag() { }
#[test] fn test_redact_cli_multiple_flags() { }
#[test] fn test_redact_cli_combined_with_yaml() { }

// --- Output Formats ---
#[test] fn test_redact_json_output() { }
#[test] fn test_redact_dry_run() { }
#[test] fn test_redact_validate_mode() { }

// --- SQL Validity ---
#[test] fn test_redact_output_valid_sql_mysql() { }
#[test] fn test_redact_output_valid_sql_postgres() { }
#[test] fn test_redact_output_importable() { }
```


---

## Phase 7: Documentation (6h)

### 7.1 README Updates (2h)

Add comprehensive Redact section to README.md:

```markdown
### Redact Options

| Flag                  | Description                                        | Default     |
|-----------------------|----------------------------------------------------|-------------|
| `-o, --output`        | Output file (default: stdout)                      | stdout      |
| `-d, --dialect`       | SQL dialect: `mysql`, `postgres`, `sqlite`         | auto-detect |
| `-c, --config`        | YAML config file for redaction rules               | —           |
| `--generate-config`   | Analyze input and generate annotated YAML config   | —           |
| `--null`              | Columns to set to NULL (glob patterns)             | —           |
| `--hash`              | Columns to hash with SHA256 (glob patterns)        | —           |
| `--fake`              | Columns for fake data (glob patterns)              | —           |
| `--mask`              | Columns to mask (format: `pattern=column`)         | —           |
| `--constant`          | Column=value pairs for constant replacement        | —           |
| `--seed`              | Random seed for reproducibility                    | random      |
| `--locale`            | Locale for fake data (en, de_de, fr_fr, etc.)      | en          |
| `-t, --tables`        | Only redact specific tables                        | all         |
| `-e, --exclude`       | Exclude specific tables                            | —           |
| `--strict`            | Fail on warnings (unsupported locale, etc.)        | —           |
| `-p, --progress`      | Show progress bar                                  | —           |
| `--dry-run`           | Preview without writing                            | —           |
| `--json`              | Output results as JSON                             | —           |
| `--validate`          | Validate config only                               | —           |

**Redaction Strategies:**

| Strategy | Description | Example |
|----------|-------------|---------|
| `null` | Replace with NULL | `'john@example.com'` → `NULL` |
| `constant` | Replace with fixed value | `'secret'` → `'REDACTED'` |
| `hash` | SHA256 hash (deterministic) | `'john@example.com'` → `'a1b2c3d4@example.com'` |
| `mask` | Pattern-based masking | `'4532-0151-1283-0366'` → `'****-****-****-0366'` |
| `shuffle` | Redistribute within column | Random swap preserving distribution |
| `fake_*` | Generate fake data | `'John Doe'` → `'Alice Smith'` |
| `skip` | Keep original value | No change |

**Mask Pattern Syntax:**

- `*` = replace with asterisk
- `X` = keep original character
- `#` = replace with random digit (0-9)
- Any other character = literal

**Fake Data Generators:**

`fake_email`, `fake_name`, `fake_first_name`, `fake_last_name`, `fake_phone`,
`fake_address`, `fake_street`, `fake_city`, `fake_state`, `fake_zip`, `fake_country`,
`fake_company`, `fake_job_title`, `fake_username`, `fake_url`, `fake_ip`, `fake_ipv6`,
`fake_uuid`, `fake_date`, `fake_datetime`, `fake_credit_card`, `fake_iban`, `fake_lorem`

**Supported Locales:**

`en` (default), `de_de`, `fr_fr`, `zh_cn`, `zh_tw`, `ja_jp`, `pt_br`, `ar_sa`

Falls back to `en` with warning if locale not available. Use `--strict` to fail instead.
```

### 7.2 Man Page Content (2h)

Ensure man page includes:
- Full option descriptions
- Strategy documentation
- YAML config format
- Examples
- Exit codes

### 7.3 llms.txt Updates (2h)

Add comprehensive redact workflow and reference:

```markdown
## Workflow 7: Data Anonymization for Development

**Goal:** Create safe, anonymized copies of production data for dev/testing.

\`\`\`bash
# 1. Generate config by analyzing input
sql-splitter redact prod.sql.gz --generate-config -o redact.yaml

# 2. Review and edit generated config
# (Config includes detected PII with suggested strategies)

# 3. Run redaction with config
sql-splitter redact prod.sql.gz -o safe.sql --config redact.yaml --progress

# 4. Reproducible redaction with seed
sql-splitter redact prod.sql.gz -o safe.sql --config redact.yaml --seed 42

# 5. Quick inline redaction
sql-splitter redact dump.sql -o safe.sql \
  --null "*.ssn,*.tax_id" \
  --hash "*.email" \
  --fake "*.name,*.phone" \
  --mask "****-****-****-XXXX=*.credit_card"
\`\`\`

**Redaction strategies:**

| Strategy | Use Case | Preserves Referential Integrity |
|----------|----------|--------------------------------|
| `hash` | Emails, usernames | Yes (same input → same output) |
| `fake_*` | Names, addresses | No (random each time unless seeded) |
| `null` | SSN, passwords | N/A (removes data) |
| `mask` | Credit cards, phones | Partial (keeps last N chars) |
| `shuffle` | Salaries, scores | Yes (redistributes within column) |
| `constant` | Passwords | N/A (fixed replacement) |
```


---

## Phase 8: Profiling and Optimization (4h)

### 8.1 Memory Profiling (2h)

**Goal:** Ensure memory usage is bounded and acceptable.

**Test Cases:**

| Test | Input Size | Expected Peak RSS | Notes |
|------|------------|-------------------|-------|
| Small file (no shuffle) | 10 MB | < 100 MB | Baseline |
| Medium file (no shuffle) | 100 MB | < 150 MB | Streaming works |
| Large file (no shuffle) | 1 GB | < 200 MB | Constant memory |
| Medium file (with shuffle) | 100 MB | TBD | Shuffle collects values |
| Large file (with shuffle) | 1 GB | TBD | May need chunking |

**Profiling Commands:**

```bash
# Profile without shuffle
./scripts/profile-memory.sh --command "redact" --size medium

# Profile with shuffle strategy
./scripts/profile-memory.sh --command "redact" --size medium \
  --extra-args "--config shuffle-test.yaml"
```

**Shuffle Strategy Optimization:**

If profiling shows excessive memory for shuffle:
1. Implement chunked shuffle (50k row sliding window)
2. Add `--max-shuffle-rows` flag with default
3. Document limitation

### 8.2 Throughput Benchmarking (2h)

**Goal:** Achieve reasonable throughput (target: 100+ MB/s for simple strategies).

**Benchmark Matrix:**

| Scenario | Target MB/s | Notes |
|----------|-------------|-------|
| Skip only (baseline) | 400+ | Near-passthrough |
| Null strategy | 300+ | Simple replacement |
| Hash strategy | 200+ | SHA256 overhead |
| Fake strategy | 100+ | Fake generation overhead |
| Mask strategy | 250+ | Pattern processing |
| Mixed strategies | 150+ | Real-world scenario |

**Benchmark Commands:**

```bash
# Add to benches/redact_bench.rs
cargo bench --bench redact_bench

# Manual timing
time sql-splitter redact large.sql -o /dev/null --null "*.email"
```

**Optimization Targets:**
- Pre-compile patterns once
- Reuse strategy instances per column
- Batch fake generation where possible
- Efficient COPY line parsing

---

## Effort Summary

| Phase | Description | Effort |
|-------|-------------|--------|
| Phase 1 | Core Infrastructure (CLI, config, matcher, types) | 10h |
| Phase 2 | Strategy Implementations (all 7 strategies) | 14h |
| Phase 3 | Statement Rewriting (INSERT + COPY) | 16h |
| Phase 4 | Config Generation (--generate-config) | 6h |
| Phase 5 | Integration, Progress & Output | 8h |
| Phase 6 | Testing (unit + integration) | 10h |
| Phase 7 | Documentation (README, man, llms.txt) | 6h |
| Phase 8 | Profiling and Optimization | 4h |
| **Total** | | **~74h** |

---

## Implementation Order

Recommended order for incremental development:

### Week 1: Core Foundation
1. **Phase 1.1-1.3**: CLI types, config, matcher, strategy types (10h)
2. **Phase 2.1**: Simple strategies (null, constant, skip) (2h)
3. **Phase 3.4**: Redactor core skeleton (2h)

### Week 2: Strategy Implementation
4. **Phase 2.2**: Hash strategy (2h)
5. **Phase 2.3**: Mask strategy (3h)
6. **Phase 2.5**: Fake strategy with locales (4h)
7. **Phase 3.2**: INSERT rewriting (5h)

### Week 3: COPY & Shuffle
8. **Phase 3.3**: PostgreSQL COPY rewriting (7h)
9. **Phase 2.4**: Shuffle strategy (3h)
10. **Phase 5.1-5.3**: Integration, progress, stats (8h)

### Week 4: Config Gen, Testing, Docs
11. **Phase 4**: Config generation (6h)
12. **Phase 6**: Testing (10h) — ongoing throughout
13. **Phase 7**: Documentation (6h)
14. **Phase 8**: Profiling (4h)

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| COPY parsing edge cases | Reuse existing `parse_postgres_copy_rows`, add tests |
| Shuffle memory for huge tables | Profile early, implement chunking if needed |
| Fake crate locale gaps | Fallback to `en` with warning |
| Complex mask patterns | Comprehensive pattern tests |
| String escaping bugs | Roundtrip validation tests |
| Performance regression | Benchmark suite, compare to other commands |

---

## Success Criteria

1. ✅ All 7 strategies work correctly (null, constant, hash, mask, shuffle, fake_*, skip)
2. ✅ PostgreSQL COPY blocks redacted in-place
3. ✅ `--generate-config` produces usable annotated YAML
4. ✅ YAML config with all options documented
5. ✅ All 8 locales work with fallback
6. ✅ Hash strategy is deterministic (same input → same output)
7. ✅ Fake strategy with `--seed` is reproducible
8. ✅ NULL values preserved across all strategies
9. ✅ Output is valid SQL (importable)
10. ✅ Progress bar consistent with other commands
11. ✅ Memory bounded for non-shuffle operations
12. ✅ Throughput ≥ 100 MB/s for typical workloads
13. ✅ All tests pass (80+ tests across all dialects)
14. ✅ README, man page, llms.txt documented

---

## YAML Configuration Reference

### Complete Option Reference

```yaml
# ============================================================
# sql-splitter redact configuration
# ============================================================
# 
# This file defines redaction rules for anonymizing SQL dumps.
# 
# Usage:
#   sql-splitter redact dump.sql -o safe.sql --config redact.yaml
# 
# Generate from input:
#   sql-splitter redact dump.sql --generate-config -o redact.yaml
# ============================================================

# ------------------------------------------------------------
# Global Settings
# ------------------------------------------------------------

# Random seed for reproducible redaction (optional)
# If set, same seed + same input = same output
# Useful for: testing, debugging, consistent dev environments
seed: 12345

# Locale for fake data generation
# Affects: names, addresses, cities, phone formats
# Supported values:
#   en      - English (default)
#   de_de   - German
#   fr_fr   - French
#   zh_cn   - Simplified Chinese
#   zh_tw   - Traditional Chinese
#   ja_jp   - Japanese
#   pt_br   - Brazilian Portuguese
#   ar_sa   - Arabic
# 
# If locale not available for a generator, falls back to 'en' with warning.
# Use --strict to fail instead of falling back.
locale: en

# Default strategy for columns not matching any rule
# Options:
#   skip    - Keep original value (safe default)
#   null    - Set to NULL
defaults:
  strategy: skip

# ------------------------------------------------------------
# Redaction Rules
# ------------------------------------------------------------
# 
# Rules are processed in order - first match wins.
# Use specific rules before general patterns.
# 
# Column patterns:
#   "users.email"      - Exact match: email column in users table
#   "*.email"          - Any table, email column
#   "users.*"          - All columns in users table
#   "*.*_email"        - Columns ending in _email in any table
#   "*.password*"      - Columns containing 'password'
# 
# Available strategies:
#   null         - Replace with NULL
#   constant     - Replace with fixed value (requires: value)
#   hash         - SHA256 hash (optional: preserve_domain for emails)
#   mask         - Pattern-based masking (requires: pattern)
#   shuffle      - Redistribute values within column
#   fake_*       - Generate fake data (see generator list below)
#   skip         - Keep original value
# 
# Fake generators:
#   fake_email, fake_name, fake_first_name, fake_last_name,
#   fake_phone, fake_address, fake_street, fake_city, fake_state,
#   fake_zip, fake_country, fake_company, fake_job_title,
#   fake_username, fake_url, fake_ip, fake_ipv6, fake_uuid,
#   fake_date, fake_datetime, fake_credit_card, fake_iban, fake_lorem

rules:
  # === High-sensitivity columns (null or constant) ===
  
  - column: "*.ssn"
    strategy: null
    # Social Security Numbers - remove entirely
    
  - column: "*.tax_id"
    strategy: null
    # Tax IDs - remove entirely
    
  - column: "*.password"
    strategy: constant
    value: "$2b$10$REDACTED_PASSWORD_HASH"
    # Passwords - replace with valid bcrypt hash
    
  - column: "*.password_hash"
    strategy: constant
    value: "$2b$10$REDACTED_PASSWORD_HASH"
    
  # === Email columns (hash to preserve FK relationships) ===
  
  - column: "*.email"
    strategy: hash
    preserve_domain: true
    # john@gmail.com → a1b2c3d4@gmail.com
    # Deterministic: same email always gets same hash
    
  # === Name columns (fake for realism) ===
  
  - column: "*.first_name"
    strategy: fake_first_name
    
  - column: "*.last_name"
    strategy: fake_last_name
    
  - column: "*.name"
    strategy: fake_name
    # Full name generator
    
  # === Contact information ===
  
  - column: "*.phone"
    strategy: fake_phone
    
  - column: "*.mobile"
    strategy: fake_phone
    
  - column: "*.address"
    strategy: fake_address
    
  - column: "*.street"
    strategy: fake_street
    
  - column: "*.city"
    strategy: fake_city
    
  - column: "*.zip"
    strategy: fake_zip
    
  - column: "*.postal_code"
    strategy: fake_zip
    
  # === Financial data (mask to preserve format) ===
  
  - column: "*.credit_card"
    strategy: mask
    pattern: "****-****-****-XXXX"
    # 4532-0151-1283-0366 → ****-****-****-0366
    # Pattern: * = mask, X = keep, # = random digit
    
  - column: "*.iban"
    strategy: fake_iban
    
  # === IP addresses ===
  
  - column: "*.ip_address"
    strategy: fake_ip
    
  - column: "*.ip"
    strategy: fake_ip
    
  # === Dates ===
  
  - column: "*.birth_date"
    strategy: fake_date
    min: "1950-01-01"
    max: "2005-12-31"
    
  - column: "*.dob"
    strategy: fake_date
    min: "1950-01-01"
    max: "2005-12-31"
    
  # === Salary/numeric shuffling ===
  
  - column: "employees.salary"
    strategy: shuffle
    # Redistributes values within column
    # Preserves statistical distribution
    # Breaks row-level correlation
    
  # === Specific overrides (before general patterns) ===
  
  - column: "admins.email"
    strategy: skip
    # Keep admin emails intact

# ------------------------------------------------------------
# Table Filtering
# ------------------------------------------------------------

# Tables to skip entirely (no redaction applied)
# Useful for: migrations, metadata, logs
skip_tables:
  - schema_migrations
  - ar_internal_metadata
  - django_migrations
  - __diesel_schema_migrations

# Tables to include (if set, ONLY these tables are processed)
# Useful for: targeted redaction of specific tables
# include_tables:
#   - users
#   - customers
#   - orders
```

---

## Related Documents

- [REDACT_FEATURE.md](REDACT_FEATURE.md) — Feature design
- [ROADMAP.md](../ROADMAP.md) — Project roadmap
- [src/parser/mysql_insert.rs](../../src/parser/mysql_insert.rs) — INSERT parsing
- [src/parser/postgres_copy.rs](../../src/parser/postgres_copy.rs) — COPY parsing
- [src/schema/mod.rs](../../src/schema/mod.rs) — Schema parsing

