# Parallel Processing: Analyze Command Deep Dive

**Date**: 2025-12-24
**Context**: Implementation challenges for `sql-splitter analyze --parallel`

## Current Analyze Implementation

The `analyze` command currently:
1. Reads SQL dump sequentially
2. Parses each statement
3. Collects statistics per table:
   - Table count
   - Row count (INSERT statements)
   - Column count (CREATE TABLE)
   - Index count
   - Foreign key count
4. Outputs summary statistics

**Current performance:** ~600 MB/s (I/O bound, not CPU bound)

## Challenge 1: Sequential File Reading

### The Problem

```rust
// Current implementation (simplified)
pub fn analyze(file: &Path) -> Stats {
    let mut stats = Stats::new();
    let reader = BufReader::new(File::open(file)?);

    for line in reader.lines() {
        let stmt = parser.parse_statement(&line)?;
        stats.process(stmt);
    }

    stats
}
```

**Issue:** You can't parallelize reading from a single file handle.

### Solution Approaches

#### Option A: Chunked Reading (Complex)

```rust
pub fn analyze_parallel(file: &Path, threads: usize) -> Stats {
    let file_size = fs::metadata(file)?.len();
    let chunk_size = file_size / threads as u64;

    // Problem: Can't split in middle of SQL statement!
    // Need to find statement boundaries

    let chunks = find_statement_boundaries(file, chunk_size)?;

    let handles: Vec<_> = chunks.into_par_iter()
        .map(|(start, end)| {
            thread::spawn(move || analyze_chunk(file, start, end))
        })
        .collect();

    // Aggregate results
    let mut total_stats = Stats::new();
    for handle in handles {
        total_stats.merge(handle.join()?);
    }

    total_stats
}

fn find_statement_boundaries(file: &Path, chunk_size: u64)
    -> Result<Vec<(u64, u64)>>
{
    // This is HARD:
    // - Need to seek to chunk boundaries
    // - Scan forward to find statement terminator (;)
    // - But ; might be inside a string literal!
    // - Need to track whether we're in a string/comment

    // Essentially need a lightweight parser just for boundaries
    todo!("Complex implementation required")
}
```

**Challenges:**
- ❌ Must find safe statement boundaries (can't split mid-statement)
- ❌ Statement terminators (`;`) can appear in strings: `INSERT INTO logs (message) VALUES ('Error: failed;')`
- ❌ Multi-line statements complicate boundary detection
- ❌ Overhead of seeking and boundary detection may outweigh benefits

#### Option B: Pre-chunking Phase (Better)

```rust
pub fn analyze_parallel(file: &Path, threads: usize) -> Stats {
    // Phase 1: Single-threaded chunking
    let chunks = chunk_file_by_statements(file, 1000)?; // 1000 statements per chunk

    // Phase 2: Parallel analysis
    let stats: Vec<Stats> = chunks.par_iter()
        .map(|chunk| analyze_statements(chunk))
        .collect();

    // Phase 3: Merge results
    stats.into_iter()
        .fold(Stats::new(), |acc, s| acc.merge(s))
}

fn chunk_file_by_statements(file: &Path, chunk_size: usize)
    -> Result<Vec<Vec<Statement>>>
{
    let mut chunks = Vec::new();
    let mut current_chunk = Vec::new();
    let reader = BufReader::new(File::open(file)?);

    for line in reader.lines() {
        if let Some(stmt) = try_parse_statement(&line) {
            current_chunk.push(stmt);

            if current_chunk.len() >= chunk_size {
                chunks.push(std::mem::take(&mut current_chunk));
            }
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    Ok(chunks)
}
```

**Trade-off:**
- ✅ Safe: No mid-statement splitting
- ✅ Simple: Reuse existing parser
- ❌ Phase 1 is still single-threaded (reads entire file first)
- ❌ Memory usage: Must hold all statements in memory
- ⚠️ Only parallelizes the analysis phase, not I/O

## Challenge 2: I/O vs CPU Bound

### Benchmarking Reveals

```bash
# Test on 1GB dump
time sql-splitter analyze dump.sql

# Results:
# - CPU usage: ~25% (single core)
# - I/O wait: ~70%
# - Memory: ~50 MB

# Conclusion: I/O bound, not CPU bound!
```

**Analysis:**
- File reading: 600 MB/s (limited by disk)
- Parsing: 1200 MB/s (CPU could go faster)
- Statistics aggregation: ~3000 MB/s (negligible)

**Implication:** Parallelizing won't help much because we're waiting on disk, not CPU.

### When Parallel Would Help

Only beneficial if:
1. **NVMe SSDs:** 3+ GB/s sequential read → CPU becomes bottleneck
2. **Compressed input:** Decompression is CPU-intensive
3. **Network sources:** Multiple concurrent downloads

## Challenge 3: Shared State & Synchronization

### Naive Parallel Approach (Race Conditions!)

```rust
// WRONG: Data race!
pub fn analyze_parallel_broken(file: &Path) -> Stats {
    let stats = Arc::new(Mutex::new(Stats::new()));

    let chunks = chunk_file_by_statements(file, 1000)?;

    chunks.par_iter().for_each(|chunk| {
        for stmt in chunk {
            // Lock contention on every statement!
            let mut s = stats.lock().unwrap();
            s.process(stmt);
            // Lock released
        }
    });

    Arc::try_unwrap(stats).unwrap().into_inner().unwrap()
}
```

**Problems:**
- ❌ Lock contention: Workers fight over the mutex
- ❌ Serialization: Only one worker can update stats at a time
- ❌ Performance: Slower than single-threaded due to lock overhead!

### Correct Approach: Local Aggregation

```rust
pub fn analyze_parallel_correct(file: &Path, threads: usize) -> Stats {
    let chunks = chunk_file_by_statements(file, 1000)?;

    // Each worker maintains local stats
    let local_stats: Vec<Stats> = chunks.par_iter()
        .map(|chunk| {
            let mut local = Stats::new();
            for stmt in chunk {
                local.process(stmt); // No locks!
            }
            local
        })
        .collect();

    // Single final merge (fast)
    local_stats.into_iter()
        .fold(Stats::new(), |acc, s| acc.merge(s))
}
```

**Benefits:**
- ✅ No lock contention
- ✅ Cache-friendly (each worker has its own data)
- ✅ Final merge is O(n) where n = number of workers

## Challenge 4: Memory Usage

### Problem: In-Memory Chunks

```rust
fn chunk_file_by_statements(file: &Path, chunk_size: usize)
    -> Result<Vec<Vec<Statement>>>
{
    // Problem: Entire file loaded into memory!
    // 10 GB dump → 10+ GB RAM usage
}
```

### Solution: Streaming with Bounded Queue

```rust
use crossbeam::channel::{bounded, Sender, Receiver};

pub fn analyze_parallel_streaming(file: &Path, threads: usize) -> Stats {
    let (sender, receiver) = bounded::<Vec<Statement>>(threads * 2);

    // Producer thread: Read file and chunk
    let producer = thread::spawn(move || {
        let reader = BufReader::new(File::open(file).unwrap());
        let mut chunk = Vec::new();

        for line in reader.lines() {
            if let Some(stmt) = try_parse_statement(&line.unwrap()) {
                chunk.push(stmt);

                if chunk.len() >= 1000 {
                    sender.send(std::mem::take(&mut chunk)).unwrap();
                }
            }
        }

        if !chunk.is_empty() {
            sender.send(chunk).unwrap();
        }
    });

    // Consumer workers: Process chunks in parallel
    let stats: Vec<Stats> = (0..threads)
        .into_par_iter()
        .map(|_| {
            let mut local = Stats::new();

            while let Ok(chunk) = receiver.recv() {
                for stmt in chunk {
                    local.process(stmt);
                }
            }

            local
        })
        .collect();

    producer.join().unwrap();

    stats.into_iter()
        .fold(Stats::new(), |acc, s| acc.merge(s))
}
```

**Benefits:**
- ✅ Constant memory: Only `threads * 2` chunks in memory
- ✅ Streaming: Producer feeds workers continuously
- ✅ Backpressure: Bounded channel blocks producer if workers are slow

**Still has issues:**
- ❌ Producer is still single-threaded (I/O bottleneck)
- ⚠️ Complex: Multiple threads, channels, coordination

## Challenge 5: Compressed Files

### Why This Matters

```bash
# Compressed dumps are common
sql-splitter analyze dump.sql.gz  # gzip
sql-splitter analyze dump.sql.zst # zstd
```

**Current:** Decompression is single-threaded bottleneck

### Parallel Decompression

```rust
use zstd::stream::read::Decoder;

// Problem: Standard decompression is sequential
let file = File::open("dump.sql.zst")?;
let decoder = Decoder::new(file)?; // Single-threaded!
```

**Challenge:** Most compression formats are inherently sequential.

**Solution:** Use formats with parallel decompression:
- **zstd** with seekable frames (requires special encoding)
- **pigz** (parallel gzip)
- **Split archives** (dump split into multiple .gz files)

```bash
# Pre-split for parallel processing
split -b 1G dump.sql dump.part.
gzip dump.part.* # Creates dump.part.aa.gz, dump.part.ab.gz, ...

# Process in parallel
sql-splitter analyze "dump.part.*.gz" --parallel auto
```

## Practical Implementation Strategy

### Phase 1: MVP (Simple, Limited Benefit)

```rust
impl AnalyzeCommand {
    pub fn run(&self) -> Result<Stats> {
        if self.parallel > 1 && self.input.extension() == Some("gz") {
            // Parallel decompression path
            self.analyze_parallel_compressed()
        } else if self.parallel > 1 {
            // Chunked parallel path
            self.analyze_parallel_uncompressed()
        } else {
            // Single-threaded path (current)
            self.analyze_sequential()
        }
    }

    fn analyze_parallel_uncompressed(&self) -> Result<Stats> {
        // Option B: Pre-chunking phase
        let chunks = chunk_file_by_statements(&self.input, 1000)?;

        let local_stats: Vec<Stats> = chunks.par_iter()
            .map(|chunk| {
                let mut local = Stats::new();
                for stmt in chunk {
                    local.process(stmt);
                }
                local
            })
            .collect();

        Ok(local_stats.into_iter()
            .fold(Stats::new(), |acc, s| acc.merge(s)))
    }
}
```

**Expected speedup:**
- Uncompressed: 1.2-1.5x (limited by I/O)
- Compressed: 2-4x (decompression CPU-bound)

### Phase 2: Streaming (Better Memory)

Add streaming version with bounded queue (code above).

**Expected benefits:**
- Same performance as Phase 1
- Constant memory usage
- Works with 100 GB+ files

### Phase 3: Multi-File (Best Case)

```bash
# Analyze multiple files in parallel
sql-splitter analyze dumps/*.sql --parallel 8

# Each file processed by dedicated worker
# True parallelism: 8x speedup possible
```

## Benchmark Results (Projected)

| Scenario | 1 GB File | 10 GB File | Notes |
|----------|-----------|------------|-------|
| **Sequential (HDD)** | 10s | 100s | 100 MB/s disk |
| **Parallel 8x (HDD)** | 10s | 100s | No improvement (I/O bound) |
| **Sequential (NVMe)** | 1.5s | 15s | 700 MB/s |
| **Parallel 8x (NVMe)** | 0.8s | 8s | 2x speedup |
| **Sequential .gz (HDD)** | 30s | 300s | Decompression CPU-bound |
| **Parallel 8x .gz (HDD)** | 8s | 80s | 4x speedup! |

## Recommendation

### For Analyze Command

**Don't implement parallel for v1.11.0.** Reasons:
1. I/O bound on most systems (limited benefit)
2. Implementation complexity high
3. Better to focus on commands where parallelism helps more

**Instead, prioritize parallel for:**
1. **Convert** (CPU-bound type conversions)
2. **Redact** (CPU-bound fake data generation)
3. **Validate** (CPU-bound PK checking)

### When to Add Parallel Analyze

**v2.0.0+** when implementing parallel infrastructure:
- Reuse shared streaming worker pool
- Add compressed file optimization
- Provide modest speedup for compressed inputs

## Alternative: Multi-File Parallelism

**Much simpler, much more effective:**

```rust
// Analyze multiple dumps in parallel
pub fn analyze_multi_file(patterns: &[String], threads: usize) -> Vec<Stats> {
    let files = glob_files(patterns)?;

    files.par_iter()
        .map(|file| analyze_sequential(file))
        .collect()
}
```

```bash
# Real-world usage
sql-splitter analyze "backups/2024-*.sql" --parallel 8

# Each file on separate core - true parallelism!
```

**Benefits:**
- ✅ Trivial implementation
- ✅ Perfect parallelism (no coordination needed)
- ✅ Works with glob patterns already supported

## Code Example: Final Implementation

```rust
// src/cmd/analyze.rs

pub fn run(
    input: PathBuf,
    parallel: usize,
    // ... other flags
) -> Result<AnalyzeStats> {
    // Auto-detect if glob pattern
    if input.to_string_lossy().contains('*') {
        return analyze_glob_parallel(&input, parallel);
    }

    // Single file
    if parallel > 1 {
        eprintln!("Warning: --parallel has limited benefit for single files (I/O bound)");
        eprintln!("Hint: Use glob patterns for better parallelism: analyze 'dumps/*.sql' --parallel 8");
    }

    // Current single-threaded implementation
    analyze_sequential(&input)
}

fn analyze_glob_parallel(pattern: &Path, threads: usize) -> Result<AnalyzeStats> {
    let files = glob::glob(pattern.to_str().unwrap())?
        .collect::<Result<Vec<_>, _>>()?;

    if files.is_empty() {
        bail!("No files match pattern: {}", pattern.display());
    }

    println!("Analyzing {} files with {} workers...", files.len(), threads);

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?
        .install(|| {
            files.par_iter()
                .map(|file| analyze_sequential(file))
                .collect()
        })
}
```

## Lessons Learned

1. **Profile first:** Don't assume CPU-bound
2. **I/O is often the bottleneck:** Parallelizing CPU won't help
3. **Multi-file > single-file:** Easier and more effective
4. **Complexity has cost:** Streaming + channels + workers = bugs
5. **Start simple:** Sequential first, parallel when proven beneficial

---

**Recommendation for PARALLEL_PROCESSING.md:** Add this analysis and recommend skipping analyze for v2.0.0 MVP, focusing on convert/redact/validate where parallelism has clear CPU-bound benefits.
