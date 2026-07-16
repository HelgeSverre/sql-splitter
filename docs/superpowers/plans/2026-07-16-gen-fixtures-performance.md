# `gen-fixtures` Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the highest-cost temporary allocations from streaming fixture generation while preserving current SQL bytes and seeded determinism.

**Architecture:** Add a borrowed `Display` value that escapes SQL directly into its destination, then reuse scratch strings in simple generation. Benchmark and re-profile this output-compatible stage before deciding whether the approved row-batch or random-sampling stages are justified.

**Tech Stack:** Rust 2021, `std::fmt`, `std::io`, `rand` 0.10, `rand_chacha` 0.10, Cargo, Hyperfine, GNU time, Samply.

## Global Constraints

- Two runs with the same arguments and seed remain byte-for-byte identical.
- Stage 1 retains all four SHA-256 hashes in `docs/superpowers/specs/2026-07-16-gen-fixtures-performance-design.md`.
- Do not change CLI options, SQL syntax, dependencies, the non-streaming generator, or the 256 KB writer buffer.
- At least one representative workload improves by 10%, with no regression above 5% elsewhere.
- Peak RSS does not increase by more than 10% or 1 MB, whichever allowance is larger.

---

### Task 1: Write SQL literals without temporary strings

**Files:**

- Modify: `crates/test_data_gen/src/streaming.rs:8-18`
- Modify: `crates/test_data_gen/src/streaming.rs:384-402`
- Modify: `crates/test_data_gen/src/streaming.rs:2100-2118`
- Test: `crates/test_data_gen/src/streaming.rs:2153-2243`

**Interfaces:**

- Produces: private `SqlString<'a>::new(dialect: Dialect, value: &'a str) -> Self`
- Produces: `impl Display for SqlString<'_>` with existing dialect-specific escaping
- Changes: both private `format_string<'a>(&self, value: &'a str) -> SqlString<'a>` methods

- [ ] **Step 1: Add the failing formatter test**

```rust
#[test]
fn sql_string_formats_each_dialect() {
    let input = "slash\\quote'line\ncarriage\rtab\t";

    assert_eq!(
        SqlString::new(Dialect::MySql, input).to_string(),
        r#"'slash\\quote\'line\ncarriage\rtab\t'"#
    );
    assert_eq!(
        SqlString::new(Dialect::Postgres, input).to_string(),
        "'slash\\quote''line\ncarriage\rtab\t'"
    );
    assert_eq!(
        SqlString::new(Dialect::Sqlite, input).to_string(),
        "'slash\\quote''line\ncarriage\rtab\t'"
    );
    assert_eq!(
        SqlString::new(Dialect::Mssql, input).to_string(),
        "N'slash\\quote''line\ncarriage\rtab\t'"
    );
}
```

- [ ] **Step 2: Verify RED**

Run `cargo test -p test_data_gen streaming::tests::sql_string_formats_each_dialect`.

Expected: compilation fails because `SqlString` does not exist.

- [ ] **Step 3: Implement the borrowed formatter**

Import `std::fmt`, then add below `CHARS`:

```rust
#[derive(Debug, Clone, Copy)]
struct SqlString<'a> {
    dialect: Dialect,
    value: &'a str,
}

impl<'a> SqlString<'a> {
    fn new(dialect: Dialect, value: &'a str) -> Self {
        Self { dialect, value }
    }
}

impl fmt::Display for SqlString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.dialect == Dialect::Mssql { "N'" } else { "'" })?;
        let mut unescaped_start = 0;

        for (index, character) in self.value.char_indices() {
            let replacement = match (self.dialect, character) {
                (Dialect::MySql, '\\') => Some("\\\\"),
                (Dialect::MySql, '\'') => Some("\\'"),
                (Dialect::MySql, '\n') => Some("\\n"),
                (Dialect::MySql, '\r') => Some("\\r"),
                (Dialect::MySql, '\t') => Some("\\t"),
                (Dialect::Postgres | Dialect::Sqlite | Dialect::Mssql, '\'') => Some("''"),
                _ => None,
            };

            if let Some(replacement) = replacement {
                f.write_str(&self.value[unescaped_start..index])?;
                f.write_str(replacement)?;
                unescaped_start = index + character.len_utf8();
            }
        }

        f.write_str(&self.value[unescaped_start..])?;
        f.write_str("'")
    }
}
```

Replace both `escape_string` and `format_string` pairs with:

```rust
fn format_string<'a>(&self, value: &'a str) -> SqlString<'a> {
    SqlString::new(self.config.dialect, value)
}
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p test_data_gen streaming::tests::sql_string_formats_each_dialect
cargo test -p test_data_gen
cargo fmt --all -- --check
cargo clippy -p test_data_gen --all-targets -- -D warnings
```

Expected: every command passes.

- [ ] **Step 5: Commit Task 1**

```bash
git add crates/test_data_gen/src/streaming.rs
git commit -m "perf(test-data-gen): stream escaped SQL strings"
```

---

### Task 2: Reuse simple-mode random-string buffers

**Files:**

- Modify: `crates/test_data_gen/src/streaming.rs:253-409`
- Test: `crates/test_data_gen/src/streaming.rs:2153-2243`

**Interfaces:**

- Produces: private `fill_random_string(&mut self, buffer: &mut String, len: usize)`
- Removes: private `random_string(&mut self, len: usize) -> String`
- Removes: private simple-generator `escape_copy(&self, value: &str) -> String`

- [ ] **Step 1: Add the failing seeded-sequence test**

```rust
#[test]
fn fill_random_string_reuses_capacity_and_preserves_rng_sequence() {
    let config = StreamingConfig {
        seed: 42,
        ..Default::default()
    };
    let mut generator = StreamingGenerator::new(config);
    let mut buffer = String::with_capacity(50);
    let initial_capacity = buffer.capacity();

    generator.fill_random_string(&mut buffer, 20);
    assert_eq!(buffer, "oQj7WAvNUsOjttkYNWZp");
    assert_eq!(generator.rng.random_range(1..=1_000_000), 536_469);
    generator.fill_random_string(&mut buffer, 50);
    assert_eq!(
        buffer,
        "Fx4K8WL4Ttl KgkmBqe9jMurSOexcyql0vVIQRQGQ2gsm1ncWc"
    );
    assert_eq!(buffer.capacity(), initial_capacity);
}
```

- [ ] **Step 2: Verify RED**

Run `cargo test -p test_data_gen streaming::tests::fill_random_string_reuses_capacity_and_preserves_rng_sequence`.

Expected: compilation fails because `fill_random_string` does not exist.

- [ ] **Step 3: Implement buffer filling**

Replace `random_string` with:

```rust
fn fill_random_string(&mut self, buffer: &mut String, len: usize) {
    buffer.clear();
    for _ in 0..len {
        let index = self.rng.random_range(0..CHARS.len());
        buffer.push(CHARS[index] as char);
    }
}
```

In each of `write_insert_batch` and `write_copy_batch`, allocate these before the row loop:

```rust
let mut name = String::with_capacity(20);
let mut description = String::with_capacity(50);
```

Replace each per-row allocation with this call order, which preserves the RNG stream:

```rust
self.fill_random_string(&mut name, 20);
let value = self.rng.random_range(1..=1_000_000);
self.fill_random_string(&mut description, 50);
```

Use `self.format_string(&name)` and `self.format_string(&description)` in INSERT output. Use `name` and `description` directly in COPY output because `CHARS` contains no COPY escape characters. Remove `escape_copy` after removing its callers.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p test_data_gen streaming::tests::fill_random_string_reuses_capacity_and_preserves_rng_sequence
cargo test -p test_data_gen
cargo fmt --all -- --check
cargo clippy -p test_data_gen --all-targets -- -D warnings
```

Expected: every command passes.

- [ ] **Step 5: Commit Task 2**

```bash
git add crates/test_data_gen/src/streaming.rs
git commit -m "perf(test-data-gen): reuse random string buffers"
```

---

### Task 3: Prove compatibility and measure the result

**Files:**

- Verify: `crates/test_data_gen/src/streaming.rs`
- Reference: `docs/superpowers/specs/2026-07-16-gen-fixtures-performance-design.md`

**Interfaces:**

- Consumes: the output-compatible implementation from Tasks 1 and 2
- Produces: hash, benchmark, memory, and profile evidence for the Stage 2 decision

- [ ] **Step 1: Run final static and test verification**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test -p test_data_gen
cargo test
```

Expected: all commands pass without warnings or failures.

- [ ] **Step 2: Build optimized binaries and verify hashes**

```bash
cargo build --release -p test_data_gen --bin gen-fixtures
cargo build --profile profiling -p test_data_gen --bin gen-fixtures
mkdir -p /tmp/sql-splitter-gen-after
./target/release/gen-fixtures --dialect mysql --rows 125000 --tables 10 --seed 12345 --output /tmp/sql-splitter-gen-after/simple-mysql.sql
./target/release/gen-fixtures --dialect postgres --rows 125000 --tables 10 --seed 12345 --output /tmp/sql-splitter-gen-after/simple-postgres.sql
./target/release/gen-fixtures --dialect mysql --scale xlarge --seed 12345 --output /tmp/sql-splitter-gen-after/schema-xlarge-mysql.sql
./target/release/gen-fixtures --dialect postgres --scale xlarge --seed 12345 --output /tmp/sql-splitter-gen-after/schema-xlarge-postgres.sql
shasum -a 256 \
  /tmp/sql-splitter-gen-after/simple-mysql.sql \
  /tmp/sql-splitter-gen-after/simple-postgres.sql \
  /tmp/sql-splitter-gen-after/schema-xlarge-mysql.sql \
  /tmp/sql-splitter-gen-after/schema-xlarge-postgres.sql
```

Expected hashes:

```text
4bdaa3e6f0b7d23bd7eb13277a37f31dc5712bbf4f91980eb5b44dd48ed8a032  simple-mysql.sql
ff5aaf7c3c639a288bec707f660e7d51af7f581865a017ee8856b439f8d15046  simple-postgres.sql
5f1dd76233d627169238843faf916b2a79647754a7ff69acbf00448f88772d0e  schema-xlarge-mysql.sql
99b0e8904205ce208f4cdb62162900b5ab36ee3123cb0580cb7df5238513dadb  schema-xlarge-postgres.sql
```

- [ ] **Step 3: Run repeated benchmarks**

```bash
hyperfine --warmup 2 --runs 10 --style basic \
  --export-json /tmp/sql-splitter-gen-after/benchmark.json \
  './target/release/gen-fixtures --dialect mysql --rows 125000 --tables 10 --seed 12345 --output /dev/null' \
  './target/release/gen-fixtures --dialect postgres --rows 125000 --tables 10 --seed 12345 --output /dev/null' \
  './target/release/gen-fixtures --dialect mysql --scale xlarge --seed 12345 --output /dev/null' \
  './target/release/gen-fixtures --dialect postgres --scale xlarge --seed 12345 --output /dev/null'
```

Compare means with 1.039 s, 0.722 s, 1.574 s, and 1.276 s.

Expected: at least one improves by 10%; none regresses by more than 5%.

- [ ] **Step 4: Check memory and re-profile**

```bash
/opt/homebrew/bin/gtime -v ./target/release/gen-fixtures --dialect mysql --rows 125000 --tables 10 --seed 12345 --output /dev/null
/opt/homebrew/bin/gtime -v ./target/release/gen-fixtures --dialect mysql --scale xlarge --seed 12345 --output /dev/null
samply record --save-only --main-thread-only --unstable-presymbolicate \
  --rate 1000 \
  --profile-name gen-fixtures-after-stage-1 \
  --output /tmp/sql-splitter-gen-after/schema-xlarge-mysql-profile.json.gz \
  ./target/profiling/gen-fixtures --dialect mysql --scale xlarge --seed 12345 --output /dev/null
```

Expected maxima are 4,176 KB for simple MySQL and 34,690 KB for multi-tenant MySQL.

If row allocation, `format!`, or `Vec<String>::join` remains the largest actionable cost, write the Stage 2 `RowBatch` plan from the approved design. Otherwise stop because the profile does not justify that rewrite.
