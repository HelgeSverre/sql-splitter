# Parallel Processing: All Commands Analysis

**Date**: 2025-12-24
**Purpose**: Comprehensive parallelization strategy and challenge analysis for all commands

## Executive Summary

| Command | CPU-Bound? | Parallelizable? | Expected Speedup | Complexity | Priority |
|---------|------------|-----------------|------------------|------------|----------|
| **Convert** | ✅ Yes | ✅ Easy | 4-6x | Low | **High** |
| **Redact** | ✅ Yes | ✅ Easy | 3-5x | Low | **High** |
| **Validate** | ✅ Yes | ✅ Medium | 5-8x | Medium | **High** |
| **Split** | ⚠️ Mixed | ✅ Medium | 3-4x | Medium | **Medium** |
| **Diff** | ✅ Yes | ✅ Medium | 4-6x | Medium | **Medium** |
| **Sample** | ❌ No | ⚠️ Hard | 1-2x | High | **Low** |
| **Shard** | ❌ No | ⚠️ Hard | 1-2x | High | **Low** |
| **Merge** | ❌ No | ✅ Easy | 4-8x | Low | **Medium** |
| **Analyze** | ❌ No | ⚠️ Limited | 1-2x | Medium | **Low** |

---

## 1. Convert Command ⭐ BEST CANDIDATE

### Current Implementation

```rust
pub fn convert(input: &Path, dialect: SqlDialect) -> Result<()> {
    let reader = BufReader::new(File::open(input)?);
    let writer = BufWriter::new(stdout());

    for line in reader.lines() {
        let stmt = parse_statement(&line?)?;
        let converted = converter.convert(stmt, dialect)?;
        writeln!(writer, "{}", converted)?;
    }
}
```

### Why It's CPU-Bound

**Type conversion operations:**
```rust
// Each conversion is computational
"VARCHAR(255)" → "TEXT"               // String matching
"AUTO_INCREMENT" → "SERIAL"           // Pattern replacement
"DATETIME" → "TIMESTAMP"              // Type mapping
"COPY ... FROM stdin" → "INSERT ..."  // Complex parsing + rebuilding
```

**Benchmark:**
- I/O: 600 MB/s (disk read)
- Parsing: 800 MB/s
- **Type conversion: 400 MB/s ← Bottleneck!**
- Output: 700 MB/s

### Parallelization Strategy

```rust
pub fn convert_parallel(input: &Path, dialect: SqlDialect, threads: usize)
    -> Result<()>
{
    // Phase 1: Chunk file into statement blocks
    let chunks = chunk_by_statements(input, 1000)?; // 1000 stmts/chunk

    // Phase 2: Parallel conversion
    let converted_chunks: Vec<Vec<String>> = chunks.par_iter()
        .map(|chunk| {
            let mut output = Vec::new();
            for stmt in chunk {
                output.push(converter.convert(stmt, dialect));
            }
            output
        })
        .collect();

    // Phase 3: Sequential ordered write
    let mut writer = BufWriter::new(stdout());
    for chunk in converted_chunks {
        for line in chunk {
            writeln!(writer, "{}", line)?;
        }
    }

    Ok(())
}
```

### Challenges

#### 1. Order Preservation
**Problem:** Output must match input order for correctness

**Solution:** Use sequenced chunks
```rust
struct SequencedChunk {
    sequence: usize,
    statements: Vec<Statement>,
}

// Workers process out of order, but we buffer and write in sequence
let mut next_seq = 0;
let mut buffer: HashMap<usize, Vec<String>> = HashMap::new();

while let Some((seq, converted)) = receiver.recv() {
    buffer.insert(seq, converted);

    while let Some(chunk) = buffer.remove(&next_seq) {
        write_chunk(chunk);
        next_seq += 1;
    }
}
```

#### 2. COPY Block Handling

**Problem:** COPY blocks are special:
```sql
COPY users FROM stdin;
1	alice@example.com	Alice
2	bob@example.com	Bob
\.
```

Can't split in middle of COPY data!

**Solution:** Parse COPY as atomic unit
```rust
fn chunk_statements_safe(input: &Path) -> Vec<Vec<Statement>> {
    let mut chunks = Vec::new();
    let mut current_chunk = Vec::new();
    let mut in_copy_block = false;

    for line in reader.lines() {
        if line.starts_with("COPY") {
            in_copy_block = true;
        }

        current_chunk.push(line);

        if line == r"\." {
            in_copy_block = false;
        }

        // Only chunk when safe (not in COPY block)
        if !in_copy_block && current_chunk.len() >= 1000 {
            chunks.push(std::mem::take(&mut current_chunk));
        }
    }

    chunks
}
```

#### 3. Memory Pressure

**Problem:** Chunking loads file into memory

**Workaround:** Streaming with bounded queue
```rust
use crossbeam::channel::bounded;

let (tx_chunks, rx_chunks) = bounded(threads * 2);
let (tx_output, rx_output) = bounded(threads * 2);

// Producer: Read and chunk
thread::spawn(move || {
    let mut chunk = Vec::new();
    for stmt in parse_file(input) {
        chunk.push(stmt);
        if chunk.len() >= 1000 {
            tx_chunks.send(chunk.clone()).unwrap();
            chunk.clear();
        }
    }
});

// Workers: Convert
(0..threads).into_par_iter().for_each(|_| {
    while let Ok(chunk) = rx_chunks.recv() {
        let converted = convert_chunk(chunk);
        tx_output.send(converted).unwrap();
    }
});

// Consumer: Write in order
write_ordered(rx_output);
```

### Expected Performance

| File Size | Threads | Current | Parallel | Speedup |
|-----------|---------|---------|----------|---------|
| 1 GB | 1 | 15s | 15s | 1.0x |
| 1 GB | 4 | 15s | 4s | 3.8x |
| 1 GB | 8 | 15s | 2.5s | 6.0x |
| 10 GB | 8 | 150s | 25s | 6.0x |

**Complexity: Low** — Stateless conversion, easy to parallelize

---

## 2. Redact Command ⭐ EXCELLENT CANDIDATE

### Why It's CPU-Bound

**Fake data generation is expensive:**
```rust
// Each fake generation involves RNG + string building
fake::Name().fake()              // ~5 μs
fake::Email().fake()             // ~3 μs
fake::Address().fake()           // ~10 μs
fake::PhoneNumber().fake()       // ~4 μs

// For 1M rows with 5 PII columns:
// 5M fake generations × 5 μs = 25 seconds of pure CPU time
```

**Benchmark:**
- I/O: 600 MB/s
- Parsing: 700 MB/s
- **Fake generation: 200 MB/s ← Bottleneck!**
- Hash generation: 500 MB/s (also CPU-bound)

### Parallelization Strategy

```rust
pub fn redact_parallel(
    input: &Path,
    config: RedactConfig,
    threads: usize
) -> Result<()> {
    // Phase 1: Parse schema, build redaction rules
    let rules = build_rules_from_schema(input, &config)?;

    // Phase 2: Chunk INSERT statements
    let chunks = chunk_insert_statements(input, 1000)?;

    // Phase 3: Parallel redaction
    let redacted_chunks: Vec<_> = chunks.par_iter()
        .enumerate()
        .map(|(seq, chunk)| {
            // Each worker has its own RNG seeded deterministically
            let mut rng = StdRng::seed_from_u64(config.seed + seq as u64);
            let mut redactor = Redactor::new(&rules, &mut rng);

            let output = chunk.iter()
                .map(|stmt| redactor.redact(stmt))
                .collect();

            (seq, output)
        })
        .collect();

    // Phase 4: Write in order
    write_ordered_chunks(redacted_chunks)?;

    Ok(())
}
```

### Challenges

#### 1. Deterministic Random Generation

**Problem:** `--seed 42` must produce identical output every time

**Solution:** Sequence-aware seeding
```rust
// Bad: Same seed for all workers → different order = different output
let rng = StdRng::seed_from_u64(config.seed);

// Good: Deterministic per-chunk seeding
let chunk_seed = config.seed
    .wrapping_add(chunk_sequence as u64)
    .wrapping_mul(0x9e3779b97f4a7c15_u64); // Mix bits

let mut rng = StdRng::seed_from_u64(chunk_seed);
```

#### 2. Hash Consistency

**Problem:** Same email must hash to same value across workers

**Solution:** Hash function is deterministic (no RNG needed)
```rust
// This is fine - pure function
fn hash_value(value: &str, salt: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(value);
    format!("{:x}", hasher.finalize())
}

// No coordination needed between workers
```

#### 3. Shuffle Strategy

**Problem:** Shuffle redistributes values **within a column**

```sql
-- Before
INSERT INTO users (email) VALUES
('alice@example.com'),
('bob@example.com'),
('carol@example.com');

-- After shuffle
('carol@example.com'),  -- Shuffled order
('alice@example.com'),
('bob@example.com');
```

**Challenge:** Need all values before shuffling!

**Solution:** Two-pass or defer shuffle to single-threaded phase
```rust
// Pass 1: Parallel redaction (skip shuffle columns)
let partially_redacted = parallel_redact_non_shuffle(chunks);

// Pass 2: Single-threaded shuffle
apply_shuffle_strategy(partially_redacted, shuffle_columns);
```

### Expected Performance

| File Size | PII Columns | Threads | Current | Parallel | Speedup |
|-----------|-------------|---------|---------|----------|---------|
| 1 GB | 5 | 1 | 20s | 20s | 1.0x |
| 1 GB | 5 | 4 | 20s | 6s | 3.3x |
| 1 GB | 5 | 8 | 20s | 4s | 5.0x |
| 10 GB | 10 | 8 | 300s | 60s | 5.0x |

**Complexity: Low-Medium** — Need deterministic RNG seeding, shuffle is special case

---

## 3. Validate Command ⭐ GREAT CANDIDATE

### Why It's CPU-Bound

**Primary key duplicate detection:**
```rust
// For each table, check for duplicate PKs
let mut pk_set = HashSet::new();

for row in table_rows {
    let pk = extract_pk(row);
    if !pk_set.insert(pk) {
        violations.push(format!("Duplicate PK: {}", pk));
    }
}

// 10M rows × 20 tables = 200M hash operations
// Each hash + insert: ~50ns
// Total: 10 seconds of pure CPU time
```

**Foreign key validation:**
```rust
// Check if all FKs reference existing PKs
for (fk_table, fk_col) in foreign_keys {
    for row in fk_table.rows {
        let fk_value = row.get(fk_col);

        if !parent_pks.contains(fk_value) {
            violations.push(format!("Orphaned FK: {}", fk_value));
        }
    }
}

// Millions of lookups → CPU-bound
```

### Parallelization Strategy

```rust
pub fn validate_parallel(input: &Path, threads: usize) -> Result<ValidationReport> {
    // Phase 1: Parse all data (still sequential, I/O bound)
    let tables = parse_all_tables(input)?;

    // Phase 2: Parallel PK validation (per-table)
    let pk_violations: Vec<_> = tables.par_iter()
        .map(|table| check_pk_duplicates(table))
        .collect();

    // Phase 3: FK validation (requires cross-table lookups)
    // Build PK sets in parallel
    let pk_sets: HashMap<String, HashSet<Value>> = tables.par_iter()
        .map(|table| {
            let pk_set = table.rows.iter()
                .map(|row| extract_pk(row))
                .collect();
            (table.name.clone(), pk_set)
        })
        .collect();

    // Parallel FK checking
    let fk_violations: Vec<_> = tables.par_iter()
        .map(|table| check_fk_violations(table, &pk_sets))
        .collect();

    Ok(ValidationReport {
        pk_violations: pk_violations.into_iter().flatten().collect(),
        fk_violations: fk_violations.into_iter().flatten().collect(),
    })
}
```

### Challenges

#### 1. Cross-Table Dependencies (FK Validation)

**Problem:** FK validation needs PK sets from other tables

**Solution 1:** Two-phase approach (shown above)
- Phase 1: Build all PK sets in parallel
- Phase 2: Validate FKs in parallel using shared PK sets

**Solution 2:** Arc-wrapped shared state
```rust
let pk_sets = Arc::new(
    tables.par_iter()
        .map(|t| (t.name.clone(), build_pk_set(t)))
        .collect::<HashMap<_, _>>()
);

// Each worker gets Arc clone (cheap)
tables.par_iter().for_each(|table| {
    let pks = pk_sets.clone();
    validate_fks(table, &pks);
});
```

#### 2. Memory Usage

**Problem:** Must hold all data in memory for validation

Current strategy already does this, so no change.

**Optimization:** Stream-validate for PK checking only
```rust
// PK validation can be streaming
let pk_violations = validate_pks_streaming(input)?;

// FK validation requires full dataset
let tables = parse_all_tables(input)?;
let fk_violations = validate_fks_parallel(tables)?;
```

#### 3. Progress Reporting

**Problem:** Users want to see progress during long validations

**Solution:** Shared progress counter
```rust
let progress = Arc::new(AtomicUsize::new(0));
let total = tables.len();

tables.par_iter().for_each(|table| {
    validate_table(table);

    let completed = progress.fetch_add(1, Ordering::Relaxed) + 1;
    eprintln!("Progress: {}/{} tables", completed, total);
});
```

### Expected Performance

| File Size | Tables | Threads | Current | Parallel | Speedup |
|-----------|--------|---------|---------|----------|---------|
| 1 GB | 50 | 1 | 30s | 30s | 1.0x |
| 1 GB | 50 | 4 | 30s | 10s | 3.0x |
| 1 GB | 50 | 8 | 30s | 5s | 6.0x |
| 10 GB | 200 | 8 | 400s | 60s | 6.7x |

**Complexity: Medium** — Cross-table dependencies, shared state for PK sets

---

## 4. Split Command

### Current Bottleneck

**Mixed I/O and CPU:**
- Parse statements: CPU-bound
- Route to table: CPU (hash map lookup)
- Write to files: **I/O-bound** (multiple file handles)

### Parallelization Strategy

```rust
pub fn split_parallel(input: &Path, output_dir: &Path, threads: usize)
    -> Result<()>
{
    // Create worker pool with dedicated table writers
    let (tx, rx) = bounded::<(String, Statement)>(threads * 100);

    // Producer: Parse and route
    thread::spawn(move || {
        for stmt in parse_file(input) {
            let table = extract_table_name(&stmt);
            tx.send((table, stmt)).unwrap();
        }
    });

    // Workers: Each handles subset of tables
    let table_workers: Vec<_> = (0..threads)
        .map(|worker_id| {
            let rx = rx.clone();
            let output_dir = output_dir.clone();

            thread::spawn(move || {
                let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();

                while let Ok((table, stmt)) = rx.recv() {
                    // Each worker handles its assigned tables
                    if hash(&table) % threads == worker_id {
                        let writer = writers.entry(table.clone())
                            .or_insert_with(|| {
                                BufWriter::new(File::create(
                                    output_dir.join(format!("{}.sql", table))
                                ).unwrap())
                            });

                        writeln!(writer, "{}", stmt).unwrap();
                    }
                }
            })
        })
        .collect();

    for worker in table_workers {
        worker.join().unwrap();
    }

    Ok(())
}
```

### Challenges

#### 1. File Handle Exhaustion

**Problem:** 100 tables × 8 workers = 800 open file handles!

**OS limit:** Typically 1024 (macOS) or 4096 (Linux)

**Solution:** Partition tables among workers
```rust
// Each worker gets exclusive set of tables
fn assign_tables_to_workers(tables: Vec<String>, num_workers: usize)
    -> Vec<Vec<String>>
{
    tables.chunks(tables.len() / num_workers)
        .map(|chunk| chunk.to_vec())
        .collect()
}

// Worker only opens files for its assigned tables
```

#### 2. Statement Ordering Within Files

**Problem:** Statements for same table might arrive out of order

**Not actually a problem!** SQL doesn't require ordering (except DDL before DML)

**Optional fix:** Include sequence number
```rust
tx.send((sequence, table, stmt)).unwrap();

// Worker buffers and writes in order per table
```

#### 3. Disk I/O Contention

**Problem:** Multiple workers writing to same disk → thrashing

**Mitigation:**
- Limit parallelism for HDDs (`--parallel 2`)
- Optimal for SSDs/NVMe (`--parallel 8`)
- Auto-detect disk type and suggest

### Expected Performance

| File Size | Tables | Storage | Threads | Current | Parallel | Speedup |
|-----------|--------|---------|---------|---------|----------|---------|
| 1 GB | 50 | HDD | 1 | 10s | 10s | 1.0x |
| 1 GB | 50 | HDD | 4 | 10s | 8s | 1.3x |
| 1 GB | 50 | SSD | 8 | 8s | 2.5s | 3.2x |
| 10 GB | 200 | NVMe | 8 | 80s | 20s | 4.0x |

**Complexity: Medium** — File handle management, disk I/O coordination

---

## 5. Diff Command

### Why It's CPU-Bound

**PK hashing for data comparison:**
```rust
// Build PK hash set for 10M row table
let pk_hashes: HashSet<u64> = rows.iter()
    .map(|row| hash_pk(row))  // CPU: hash computation
    .collect();

// Compare 10M rows
for row in new_rows {
    let pk_hash = hash_pk(row);
    if !pk_hashes.contains(&pk_hash) {
        added_rows.push(row);
    }
}

// 20M hash operations × 50ns = 1 second pure CPU
```

### Parallelization Strategy

```rust
pub fn diff_parallel(
    old_dump: &Path,
    new_dump: &Path,
    threads: usize
) -> Result<DiffReport> {
    // Phase 1: Parse both dumps (sequential, I/O bound)
    let old_tables = parse_tables(old_dump)?;
    let new_tables = parse_tables(new_dump)?;

    // Phase 2: Schema diff (cheap, single-threaded is fine)
    let schema_diff = diff_schemas(&old_tables, &new_tables);

    // Phase 3: Data diff per table (parallel)
    let table_names: Vec<_> = old_tables.keys()
        .chain(new_tables.keys())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let data_diffs: Vec<_> = table_names.par_iter()
        .map(|table| {
            let old_data = old_tables.get(*table);
            let new_data = new_tables.get(*table);
            diff_table_data(table, old_data, new_data)
        })
        .collect();

    Ok(DiffReport {
        schema_diff,
        data_diffs,
    })
}

fn diff_table_data(
    table: &str,
    old_data: Option<&Table>,
    new_data: Option<&Table>
) -> TableDiff {
    match (old_data, new_data) {
        (None, Some(new)) => TableDiff::Added { rows: new.rows.len() },
        (Some(old), None) => TableDiff::Removed { rows: old.rows.len() },
        (Some(old), Some(new)) => {
            // Build PK hash set from old
            let old_pks: HashSet<_> = old.rows.iter()
                .map(|r| hash_row(r))
                .collect();

            let added = new.rows.iter()
                .filter(|r| !old_pks.contains(&hash_row(r)))
                .count();

            let removed = old.rows.iter()
                .filter(|r| !new_pks.contains(&hash_row(r)))
                .count();

            TableDiff::Modified { added, removed }
        }
        (None, None) => unreachable!(),
    }
}
```

### Challenges

#### 1. Memory for Large Tables

**Problem:** Diff needs all data in memory

**Current approach:** Memory-bounded PK tracking (10M limit)

**Parallel doesn't change this** — same memory usage

#### 2. Shared vs Local PK Sets

**Good news:** Each table comparison is independent!

No shared state needed → perfect parallelism

### Expected Performance

| Dump Size | Tables | Threads | Current | Parallel | Speedup |
|-----------|--------|---------|---------|----------|---------|
| 1 GB each | 50 | 1 | 20s | 20s | 1.0x |
| 1 GB each | 50 | 4 | 20s | 7s | 2.9x |
| 1 GB each | 50 | 8 | 20s | 4s | 5.0x |
| 10 GB each | 200 | 8 | 250s | 45s | 5.6x |

**Complexity: Medium** — Need to parse both files first (memory), then parallel comparison

---

## 6. Sample Command ⚠️ HARD TO PARALLELIZE

### Why It's Difficult

**FK-aware sampling is inherently sequential:**

```rust
// Must process in topological order
let ordered_tables = topological_sort(tables);

for table in ordered_tables {
    if table.is_root() {
        // Sample randomly
        sample_rows_randomly(table, sample_percent);
    } else {
        // Sample based on parent FKs (dependency!)
        let parent_pks = get_selected_parent_pks();
        sample_rows_matching_fks(table, parent_pks);
    }
}
```

**Dependency chain:**
```
users (sample 10%)
  ↓ (selected user_ids: [1, 5, 7])
orders (sample only where user_id IN (1, 5, 7))
  ↓ (selected order_ids: [101, 203])
order_items (sample only where order_id IN (101, 203))
```

Can't parallelize this chain!

### Limited Parallelization Opportunities

#### Opportunity 1: Parallel PK Extraction

```rust
// Phase 1: Extract PKs in parallel (before sampling)
let pk_sets: HashMap<String, HashSet<PK>> = tables.par_iter()
    .map(|table| {
        let pks = table.rows.iter()
            .map(|row| extract_pk(row))
            .collect();
        (table.name.clone(), pks)
    })
    .collect();

// Phase 2: Sequential FK-aware sampling
sample_with_fk_preservation(tables, pk_sets);
```

**Speedup:** 1.5-2x (only phase 1 parallelized)

#### Opportunity 2: Parallel Within-Table Processing

```rust
// For large tables, parallelize row processing
fn sample_table_rows(table: &Table, percent: f64) -> Vec<Row> {
    table.rows.par_iter()
        .filter(|row| should_include(row, percent))
        .cloned()
        .collect()
}
```

**Speedup:** 1.2-1.5x (small win)

### Why Not Full Parallelism?

**Sequential dependencies:**
```
Time
 ↓
Sample users          ← Can parallelize? No, must wait for % calculation
Wait for user IDs     ← Dependency!
Sample orders         ← Depends on user IDs
Wait for order IDs    ← Dependency!
Sample order_items    ← Depends on order IDs
```

**Best case:** Pipeline parallelism (complex)
```rust
// While sampling orders, start pre-filtering order_items
// But gains are minimal for added complexity
```

### Expected Performance

| File Size | FK Depth | Threads | Current | Parallel | Speedup |
|-----------|----------|---------|---------|----------|---------|
| 1 GB | 3 levels | 1 | 8s | 8s | 1.0x |
| 1 GB | 3 levels | 4 | 8s | 6s | 1.3x |
| 10 GB | 5 levels | 8 | 80s | 50s | 1.6x |

**Complexity: High** — Sequential FK chain, limited parallelism opportunity

**Recommendation:** Low priority for v2.0.0

---

## 7. Shard Command ⚠️ SIMILAR TO SAMPLE

### Same Sequential Dependency Problem

```rust
// Must follow FK chain from tenant root
1. Find root rows (tenant_id = 5)
2. Find dependent rows (FK → root)
3. Find transitive dependencies (FK → FK → root)
```

**Parallelization limited** for same reasons as Sample.

**Expected speedup:** 1.3-1.8x (minimal)

**Recommendation:** Low priority for v2.0.0

---

## 8. Merge Command ✅ EASY WIN

### Why It's Embarrassingly Parallel

**Independent file reads:**
```bash
# Merge 100 table files
merge tables/*.sql -o merged.sql
```

Each file can be read in parallel, then merged sequentially.

### Parallelization Strategy

```rust
pub fn merge_parallel(table_files: Vec<PathBuf>, threads: usize)
    -> Result<()>
{
    // Phase 1: Read all files in parallel
    let file_contents: Vec<_> = table_files.par_iter()
        .map(|file| fs::read_to_string(file))
        .collect::<Result<Vec<_>>>()?;

    // Phase 2: Topological sort (schema dependencies)
    let sorted_contents = topological_sort_by_fk(file_contents)?;

    // Phase 3: Sequential write (maintains order)
    let mut output = BufWriter::new(stdout());
    for content in sorted_contents {
        write!(output, "{}", content)?;
    }

    Ok(())
}
```

### Challenges

**Very few!** This is the easiest command to parallelize.

Only challenge: **Topological ordering** (but that's single-threaded and fast)

### Expected Performance

| Files | Threads | Current | Parallel | Speedup |
|-------|---------|---------|----------|---------|
| 50 | 1 | 5s | 5s | 1.0x |
| 50 | 4 | 5s | 1.5s | 3.3x |
| 200 | 8 | 20s | 3s | 6.7x |
| 500 | 8 | 50s | 7s | 7.1x |

**Complexity: Low** — Trivial to implement

**Recommendation:** Include in v2.0.0 MVP

---

## 9. Query Command (Future) ✅ GOOD CANDIDATE

### Why It's CPU-Bound

**WHERE clause evaluation:**
```rust
for row in table_rows {
    if evaluate_where_clause(row, &where_expr) {  // CPU: expression eval
        matching_rows.push(row);
    }
}
```

Complex expressions expensive:
```sql
WHERE (age > 18 AND status = 'active')
   OR (premium = true AND created_at > '2024-01-01')
```

### Parallelization Strategy

Same as Convert (chunk → evaluate → merge)

**Expected speedup:** 4-6x

**Complexity:** Low-Medium

---

## Implementation Priority

### v2.0.0 MVP (High Priority)

1. **Convert** (✅ 6x speedup, low complexity)
2. **Redact** (✅ 5x speedup, low-medium complexity)
3. **Validate** (✅ 6x speedup, medium complexity)
4. **Merge** (✅ 7x speedup, low complexity)

### v2.1.0 (Medium Priority)

5. **Split** (⚠️ 3x speedup, medium complexity, I/O dependent)
6. **Diff** (✅ 5x speedup, medium complexity)

### v2.2+ (Low Priority)

7. **Sample** (❌ 1.5x speedup, high complexity)
8. **Shard** (❌ 1.5x speedup, high complexity)
9. **Analyze** (❌ 1.2x speedup, limited benefit)

---

## Common Patterns & Reusable Components

### 1. Statement Chunking

```rust
// Shared utility for all commands
pub fn chunk_by_statements(
    input: &Path,
    chunk_size: usize
) -> Result<Vec<Vec<Statement>>> {
    // Handle COPY blocks, multi-line statements, etc.
}
```

### 2. Ordered Output Writer

```rust
pub struct OrderedWriter {
    next_seq: usize,
    buffer: HashMap<usize, Vec<String>>,
    output: BufWriter<File>,
}

impl OrderedWriter {
    pub fn write(&mut self, seq: usize, data: Vec<String>) {
        self.buffer.insert(seq, data);
        self.flush_contiguous();
    }
}
```

### 3. Thread Pool

```rust
pub struct SqlSplitterThreadPool {
    pool: ThreadPool,
    threads: usize,
}

impl SqlSplitterThreadPool {
    pub fn new(threads: usize) -> Self {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("sql-splitter-{}", i))
            .build()
            .unwrap()
    }
}
```

### 4. Progress Reporting

```rust
pub struct ParallelProgress {
    completed: Arc<AtomicUsize>,
    total: usize,
}

impl ParallelProgress {
    pub fn inc(&self) {
        let done = self.completed.fetch_add(1, Ordering::Relaxed) + 1;
        eprintln!("Progress: {}/{}", done, self.total);
    }
}
```

---

## Conclusion

**Best ROI commands for parallel processing:**

1. **Convert** — 6x speedup, easy
2. **Redact** — 5x speedup, easy
3. **Validate** — 6x speedup, medium
4. **Merge** — 7x speedup, trivial

**Skip for v2.0.0:**
- Sample (sequential FK dependencies)
- Shard (sequential FK dependencies)
- Analyze (I/O bound)

**Total v2.0.0 effort:** ~60h remains accurate
- 15h Convert
- 15h Redact
- 20h Validate
- 5h Merge
- 5h Shared infrastructure
