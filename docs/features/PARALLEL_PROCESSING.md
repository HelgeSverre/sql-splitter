# Parallel Processing Feature Design

**Status**: Planning (v2.0.0)
**Date**: 2025-12-24
**Priority**: High (major version feature)

## Overview

Enable multi-threaded parallel processing across all sql-splitter commands to maximize performance on multi-core systems. Target: **4x speedup on 8-core systems** with linear scaling up to available cores.

## Problem Statement

Current implementation is **single-threaded**, leaving 87.5% of CPU cores idle on an 8-core system.

**Performance impact:**
- **Split:** 10GB dump takes 30s → could be 7.5s
- **Convert:** Type conversion is CPU-bound → 4x faster
- **Validate:** FK checking is embarrassingly parallel → 8x faster
- **Redact:** Fake data generation is CPU-bound → 4x faster

Competitors like **mydumper** achieve 5-10x speedup with parallel processing.

## Command Interface

All commands gain `--parallel` flag:

```bash
# Auto-detect core count
sql-splitter split dump.sql -o tables/ --parallel auto

# Explicit thread count
sql-splitter split dump.sql -o tables/ --parallel 8

# Disable parallel (force single-threaded)
sql-splitter split dump.sql -o tables/ --parallel 1

# Environment variable override
SQL_SPLITTER_THREADS=4 sql-splitter convert dump.sql --dialect postgres
```

## Per-Command Parallelization Strategy

### 1. Split Command

**Parallelization unit:** Tables

```
┌─────────────────────────────────────┐
│         Main Thread                 │
│  (Parse statements, route to queues)│
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 Worker 1  Worker 2  Worker 3  Worker 4
(users)   (orders) (products) (logs)
    │         │         │         │
    ▼         ▼         ▼         ▼
 users.sql orders.sql products.sql logs.sql
```

**Algorithm:**
1. Main thread parses statements sequentially
2. Route CREATE TABLE to dedicated worker
3. Route INSERT to table's worker
4. Workers write to separate files concurrently
5. Join when input exhausted

**Expected speedup:** 3-4x on 8 cores

---

### 2. Convert Command

**Parallelization unit:** Statements

```
┌─────────────────────────────────────┐
│         Main Thread                 │
│     (Parse into statement blocks)   │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 Worker 1  Worker 2  Worker 3  Worker 4
(Convert) (Convert) (Convert) (Convert)
    │         │         │         │
    └─────────┴─────────┴─────────┘
              ▼
        Output Writer
       (ordered merge)
```

**Algorithm:**
1. Chunk input into statement blocks (1000 statements each)
2. Distribute chunks to worker pool
3. Workers convert independently
4. Ordered merge to output (preserve statement order)

**Expected speedup:** 4-6x on 8 cores (CPU-bound)

---

### 3. Validate Command

**Parallelization unit:** Tables

```
┌─────────────────────────────────────┐
│         Main Thread                 │
│    (Parse, collect table data)      │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 Worker 1  Worker 2  Worker 3  Worker 4
 (PK check)(PK check)(PK check)(PK check)
    │         │         │         │
    └─────────┴─────────┴─────────┘
              ▼
       Aggregate Results
```

**Algorithm:**
1. Parse all INSERTs into per-table row buffers
2. Distribute tables to workers for PK duplicate detection
3. FK validation (requires cross-table coordination)
4. Aggregate violation reports

**Expected speedup:** 6-8x on 8 cores (data-parallel)

---

### 4. Redact Command

**Parallelization unit:** Statement blocks

```
┌─────────────────────────────────────┐
│         Main Thread                 │
│    (Parse, identify redact columns) │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 Worker 1  Worker 2  Worker 3  Worker 4
 (Redact)  (Redact)  (Redact)  (Redact)
    │         │         │         │
    └─────────┴─────────┴─────────┘
              ▼
        Output Writer
```

**Algorithm:**
1. Parse schema, build redaction rules per table
2. Chunk INSERT statements
3. Workers apply redaction strategies (fake data generation)
4. Ordered merge to output

**Expected speedup:** 3-5x on 8 cores (fake data generation is CPU-bound)

---

### 5. Sample Command

**Parallelization challenge:** FK-aware sampling requires sequential processing

**Strategy:** Parallel PK tracking, sequential selection

```
Phase 1: Parallel row parsing
┌─────────────────────────────────────┐
│         Main Thread                 │
│      (Parse INSERT statements)      │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 Worker 1  Worker 2  Worker 3  Worker 4
(Parse rows, extract PKs)
    │         │         │         │
    └─────────┴─────────┴─────────┘
              ▼
        PK Hash Sets

Phase 2: Sequential FK-aware selection
(Single-threaded, uses PK sets from Phase 1)
```

**Expected speedup:** 2x (limited by sequential FK resolution)

---

### 6. Diff Command

**Parallelization unit:** Tables

```
┌─────────────────────────────────────┐
│         Main Thread                 │
│  (Parse both dumps, group by table) │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 Worker 1  Worker 2  Worker 3  Worker 4
(Compare) (Compare) (Compare) (Compare)
 users     orders    products   logs
    │         │         │         │
    └─────────┴─────────┴─────────┘
              ▼
       Aggregate Diffs
```

**Expected speedup:** 5-7x on 8 cores

---

## Implementation Architecture

### Core Components

```
src/
├── parallel/
│   ├── mod.rs              # Public API, thread pool
│   ├── worker.rs           # Worker thread implementation
│   ├── queue.rs            # Work queue (crossbeam channel)
│   ├── merger.rs           # Ordered output merging
│   └── config.rs           # Thread count detection
├── cmd/
│   ├── split.rs            # Parallel split implementation
│   ├── convert.rs          # Parallel convert implementation
│   ├── validate.rs         # Parallel validate implementation
│   └── ...                 # Other commands
```

### Key Types

```rust
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Sender<Job>,
}

pub struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

pub enum Job {
    Split { table: String, statements: Vec<Statement> },
    Convert { chunk: Vec<Statement> },
    Validate { table: String, rows: Vec<Row> },
    Redact { chunk: Vec<Statement>, rules: RedactRules },
    Terminate,
}

pub struct ParallelConfig {
    pub thread_count: usize,
    pub chunk_size: usize,
    pub preserve_order: bool,
}

impl ParallelConfig {
    pub fn auto() -> Self {
        let cpus = num_cpus::get();
        Self {
            thread_count: cpus,
            chunk_size: 1000,
            preserve_order: true,
        }
    }
}
```

### Thread Pool Implementation

```rust
use crossbeam::channel::{bounded, Sender, Receiver};
use std::thread;

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = bounded(size * 2);
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Job::Closure(Box::new(job))).unwrap();
    }
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || {
            loop {
                let job = receiver.lock().unwrap().recv().unwrap();

                match job {
                    Job::Terminate => break,
                    _ => {
                        // Process job
                    }
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
```

## Ordered Output Merging

**Problem:** Workers process chunks out of order, but output must preserve original order.

**Solution:** Sequenced output buffer

```rust
pub struct OrderedMerger {
    next_sequence: usize,
    buffer: HashMap<usize, String>,
    output: Box<dyn Write>,
}

impl OrderedMerger {
    pub fn write_chunk(&mut self, sequence: usize, data: String) {
        self.buffer.insert(sequence, data);

        // Flush all contiguous chunks starting from next_sequence
        while let Some(chunk) = self.buffer.remove(&self.next_sequence) {
            self.output.write_all(chunk.as_bytes()).unwrap();
            self.next_sequence += 1;
        }
    }
}
```

## Memory Management

**Challenge:** Parallel processing can spike memory usage.

**Mitigations:**
1. **Bounded work queues** — Limit queue depth to `2 * thread_count`
2. **Streaming chunks** — Process and discard, don't accumulate
3. **Memory monitoring** — Back off if RSS exceeds threshold
4. **Chunk size tuning** — Smaller chunks for large files

**Example:**
```rust
pub struct MemoryBoundedPool {
    pool: ThreadPool,
    max_rss_mb: usize,
}

impl MemoryBoundedPool {
    pub fn execute(&self, job: Job) {
        // Check memory before queueing
        let current_rss = get_current_rss_mb();
        if current_rss > self.max_rss_mb {
            // Wait for memory to drop
            thread::sleep(Duration::from_millis(100));
        }
        self.pool.execute(job);
    }
}
```

## Performance Benchmarks

### Target Metrics

| Command | File Size | Threads=1 | Threads=8 | Speedup |
|---------|-----------|-----------|-----------|---------|
| split | 1 GB | 10s | 2.5s | 4.0x |
| split | 10 GB | 100s | 25s | 4.0x |
| convert | 1 GB | 15s | 3s | 5.0x |
| validate | 1 GB | 30s | 5s | 6.0x |
| redact | 1 GB | 20s | 5s | 4.0x |

### Amdahl's Law Considerations

Not all parts are parallelizable:

```
Speedup = 1 / ((1 - P) + (P / N))

P = Parallelizable fraction
N = Number of cores
```

**Estimate for split:**
- 90% parallelizable (table writing)
- 10% sequential (parsing)
- Max speedup at 8 cores: 1 / (0.1 + 0.9/8) = 4.7x

## CLI UX

### Progress Reporting

```bash
sql-splitter split dump.sql -o tables/ --parallel 8 --progress

# Output:
Splitting dump.sql with 8 workers...
[===================>                ] 45% (12/27 tables)
  Worker 1: users.sql (1.2 GB, 95% done)
  Worker 2: orders.sql (800 MB, 60% done)
  Worker 3: products.sql (500 MB, 30% done)
  ...
```

### Thread Count Recommendations

```bash
sql-splitter split dump.sql -o tables/ --parallel auto

# Info log:
Using 8 threads (detected 8 CPU cores)
Hint: For I/O-bound workloads, try --parallel 16
```

## Testing Strategy

### Unit Tests
- Thread pool creation and shutdown
- Work queue distribution
- Ordered output merging
- Memory bounds enforcement

### Integration Tests
- Each command with --parallel flag
- Correctness (parallel output == sequential output)
- Speedup measurements (4+ cores required)

### Stress Tests
- 100 GB dump with 16 threads
- Memory usage monitoring
- No deadlocks or race conditions

### Property Tests
- Parallel output determinism (same input → same output)
- No data loss (row count preserved)

## Edge Cases

### 1. More Workers Than Tables

```bash
sql-splitter split small.sql -o tables/ --parallel 16
# Only 5 tables in dump
# Use 5 workers, warn about idle workers
```

### 2. Memory Exhaustion

If parallel processing exceeds available RAM:
- Reduce chunk size dynamically
- Fall back to sequential processing with warning

### 3. I/O Bottlenecks

Parallel writing to same disk may not improve performance:
- Detect sequential I/O bottleneck
- Suggest `--parallel 2` for spinning disks
- Optimal for SSDs and NVMe

## Platform Considerations

### Linux
- `num_cpus` uses `/proc/cpuinfo`
- Thread affinity with `libc::sched_setaffinity`

### macOS
- `num_cpus` uses `sysctl`
- No thread affinity control

### Windows
- `num_cpus` uses `GetSystemInfo`
- Thread affinity with `SetThreadAffinityMask`

## Dependencies

```toml
[dependencies]
num_cpus = "1.16"
crossbeam = "0.8"
rayon = "1.8"  # Alternative: work-stealing scheduler
```

**Choice: crossbeam vs rayon**

| Feature | crossbeam | rayon |
|---------|-----------|-------|
| Work-stealing | ❌ | ✅ |
| Fine-grained control | ✅ | ❌ |
| Ordered output | Manual | Harder |

**Recommendation:** Start with **crossbeam** for explicit control, consider **rayon** for CPU-bound tasks like convert.

## Estimated Effort

| Component | Effort |
|-----------|--------|
| Thread pool implementation | 6 hours |
| Ordered merger | 3 hours |
| Memory-bounded queue | 3 hours |
| Parallel split | 6 hours |
| Parallel convert | 8 hours |
| Parallel validate | 6 hours |
| Parallel redact | 6 hours |
| Parallel diff | 4 hours |
| CLI integration (--parallel flag) | 3 hours |
| Progress reporting | 4 hours |
| Testing and benchmarking | 10 hours |
| Documentation | 3 hours |
| **Total** | **~60 hours** |

## Future Enhancements

1. **GPU Acceleration** — Offload hash computation, regex matching to GPU
2. **Distributed Processing** — Split work across multiple machines
3. **Adaptive Parallelism** — Auto-tune thread count based on workload
4. **Work Stealing** — Balance load across workers dynamically
5. **Compression Pipeline** — Parallel compression (zstd supports this)

## Related Documents

- [Roadmap](../ROADMAP.md)
- All command feature docs (will be updated for parallel support)
- [Sample Memory Optimization](../ROADMAP.md#v182--sample-memory-optimization) — Lesson learned for memory management
