# SQL Splitter Performance Analysis & Optimization Roadmap

**Date:** 2025-10-01
**Environment:**
- CPU: Apple M2 Max (ARM64)
- Memory: 32 GB
- Storage: Apple Fabric SSD (NVMe)
- Go Version: 1.24.6
- OS: Darwin 24.6.0

---

## Executive Summary

**Current Performance:** 219 MB/s (benchmark), 207 MB/s (real-world)
**Target Performance:** 500+ MB/s
**Gap:** 2.3x improvement needed

**Key Findings:**
1. ✅ **Regex pre-compilation is effective** - No `regexp.Compile` in hot path
2. ✅ **Pool optimizations working** - 32KB sync.Pool reduces allocations
3. ❌ **Byte-by-byte reading is the primary bottleneck** - 82.6% of CPU time in `ReadStatement`
4. ❌ **I/O syscalls dominate** - 60.9% of time in `syscall.syscall` (ReadByte calls)
5. ⚠️ **Parser is CPU-bound, not I/O-bound** - Throughput far below SSD capability (~3-4 GB/s)

**Verdict:** The target of 500 MB/s is **achievable** with 1-2 days of focused optimization work. The primary bottleneck is algorithmic (byte-by-byte processing), not hardware-limited.

---

## Benchmark Results

### After Recent Optimizations

#### BenchmarkParser_ReadStatement (5 runs, variance analysis)
```
Mean:     219.21 MB/s
Median:   219.37 MB/s
Std Dev:  1.68 MB/s (0.77% variance - excellent consistency)
Range:    217.22 - 221.46 MB/s
Memory:   618 KB/op, 1233 allocs/op
```

#### BenchmarkParser_ParseStatement (5 runs)
```
Mean:     317.3 ns/op
Memory:   77 B/op, 3 allocs/op
Variance: ±0.7% (highly stable)
```

#### Buffer Size Impact
```
4KB:    267.75 MB/s (BEST) - 514 KB/op, 10233 allocs/op
64KB:   264.35 MB/s         - 638 KB/op, 10233 allocs/op
256KB:  259.42 MB/s         - 1032 KB/op, 10233 allocs/op
1MB:    241.26 MB/s (WORST) - 2612 KB/op, 10235 allocs/op
```

**Insight:** Smaller buffers win due to better CPU cache locality. Current 256KB default is suboptimal.

#### Real-World Split Performance (52 MB file)
```
Throughput:       207.35 MB/s
Statements:       1112
Processing Time:  252 ms
```

**Note:** Real-world is 5.4% slower than benchmark due to:
- Writer overhead (13% of CPU time)
- Table name parsing (regex execution: 28.8% of CPU)
- Lock contention in WriterPool (minimal)

---

## CPU Profile Analysis

### ReadStatement Function Breakdown
```
Total CPU: 1.33s (17.27% of profile)
├── ReadByte syscalls:      570ms (42.9%) ← PRIMARY BOTTLENECK
├── buf append:             190ms (14.3%)
├── String boundary checks: 200ms (15.0%)
├── Semicolon check:        100ms (7.5%)
├── Buffer allocation:       90ms (6.8%)
└── Pool operations:         20ms (1.5%)
```

**Critical Finding:** `p.reader.ReadByte()` accounts for 570ms of 1.33s (42.9%) in the function. This is called once per byte, creating excessive syscall overhead despite bufio buffering.

### Real-World Split Operation Breakdown
```
Total CPU: 230ms
├── ReadStatement:          190ms (82.6%) ← DOMINANT BOTTLENECK
│   ├── syscall.syscall:    140ms (60.9%)
│   └── Parsing logic:       50ms (21.7%)
├── WriteStatement:          30ms (13.0%)
├── ParseStatement (regex):  10ms (4.3%)
└── Runtime overhead:        <1ms
```

**Conclusion:** The parser consumes 6.3x more CPU than the writer. This is a **CPU-bound** workload, not I/O-bound.

### Memory Profile Analysis
```
Total Allocations: 12.7 GB during benchmark

Top Allocators:
1. NewParser (bufio.NewReaderSize): 5.38 GB (42.4%) - Per-parser allocation (expected)
2. BenchmarkParser calls:           1.32 GB (10.4%) - Test overhead
3. ReadStatement (Pool.Get):        90 MB  (0.7%)  - Actual hot path (GOOD!)
4. Regex compilation (init):        510 MB (4.0%)  - One-time cost (expected)
```

**Insight:** Memory allocations are well-controlled. The sync.Pool is effective - only 90 MB allocated in hot path vs. 1.32 GB total statement processing.

---

## Bottleneck Distribution

| Component      | CPU %  | Classification | Optimization Potential |
|----------------|--------|----------------|------------------------|
| ReadStatement  | 82.6%  | CPU-bound      | **HIGH** (batched I/O) |
| I/O syscalls   | 60.9%  | Syscall-bound  | **HIGH** (reduce calls)|
| WriteStatement | 13.0%  | I/O-bound      | LOW (already buffered) |
| ParseStatement | 4.3%   | Regex-bound    | MEDIUM (manual parsing)|
| Runtime/GC     | <1%    | Well-optimized | None                   |

---

## Prioritized Optimization Roadmap

### PRIORITY 1: Batched Statement Reading (Impact: 2-3x speedup)

**Problem:**
Current implementation calls `ReadByte()` once per byte, resulting in function call overhead even with bufio buffering. For a 60-byte statement, this is 60 function calls.

**Solution:**
Read chunks of data and scan for semicolons with quote-aware state machine.

**Expected Gain:** 150-250 MB/s → **350-500 MB/s**
**Effort:** 4-6 hours
**Risk:** MEDIUM (string/escape handling must be verified carefully)
**Complexity:** Moderate - requires careful state machine for quotes/escapes

**Code Example:**
```go
// Optimized ReadStatement using buffered scanning
func (p *Parser) ReadStatement() ([]byte, error) {
	// Get buffer from pool
	bufPtr := p.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	insideString := false
	insideSingleQuote := false
	insideDoubleQuote := false
	escaped := false

	for {
		// Read chunk from bufio (typically 4KB at a time)
		chunk, err := p.reader.Peek(4096)
		if err != nil && err != io.EOF {
			p.bufferPool.Put(bufPtr)
			return nil, err
		}

		if len(chunk) == 0 {
			if len(buf) > 0 {
				result := make([]byte, len(buf))
				copy(result, buf)
				p.bufferPool.Put(bufPtr)
				return result, nil
			}
			p.bufferPool.Put(bufPtr)
			return nil, io.EOF
		}

		// Scan chunk for statement terminator
		consumed := 0
		for i := 0; i < len(chunk); i++ {
			b := chunk[i]
			consumed++

			// Track string boundaries
			if !escaped {
				if b == '\'' && !insideDoubleQuote {
					insideSingleQuote = !insideSingleQuote
					insideString = insideSingleQuote || insideDoubleQuote
				} else if b == '"' && !insideSingleQuote {
					insideDoubleQuote = !insideDoubleQuote
					insideString = insideSingleQuote || insideDoubleQuote
				}
			}

			// Track escaping
			escaped = (b == '\\' && !escaped)

			// Found statement terminator?
			if b == ';' && !insideString {
				// Append this final chunk
				buf = append(buf, chunk[:i+1]...)
				p.reader.Discard(consumed)

				result := make([]byte, len(buf))
				copy(result, buf)
				p.bufferPool.Put(bufPtr)
				return result, nil
			}
		}

		// No terminator found, consume entire chunk and continue
		buf = append(buf, chunk...)
		p.reader.Discard(consumed)
	}
}
```

**Validation Plan:**
1. Run existing test suite to verify correctness
2. Add edge case tests: escaped quotes, multiline strings, empty statements
3. Benchmark comparison: old vs. new implementation
4. Profile to confirm syscall reduction

**Expected Profile After Optimization:**
- `syscall.syscall`: 60.9% → ~15% (4x reduction)
- `ReadStatement` CPU: 82.6% → ~35%
- Throughput: 207 MB/s → 400-500 MB/s

---

### PRIORITY 2: Optimize ParseStatement Regex Usage (Impact: 1.2-1.5x speedup)

**Problem:**
Regex matching consumes 28.8% of ParseStatement CPU (1.11s of 3.86s). The regex engine uses backtracking for table name extraction, even after fast-path prefix checks.

**Solution:**
Replace regex with manual byte scanning after prefix match confirmation.

**Expected Gain:** 25-40 MB/s additional (on top of Priority 1)
**Effort:** 3-4 hours
**Risk:** LOW (input validation already happens upstream)
**Complexity:** Low - straightforward byte scanning

**Code Example:**
```go
// Optimized ParseStatement without regex for common cases
func (p *Parser) ParseStatement(stmt []byte) (StatementType, string) {
	stmt = bytes.TrimSpace(stmt)

	if len(stmt) < 6 {
		return Unknown, ""
	}

	// Fast uppercase conversion (first 20 bytes only)
	upperPrefix := bytes.ToUpper(stmt[:min(20, len(stmt))])

	// CREATE TABLE - manual table name extraction
	if bytes.HasPrefix(upperPrefix, []byte("CREATE TABLE")) {
		tableName := extractTableName(stmt, 12) // start after "CREATE TABLE"
		if tableName != "" {
			return CreateTable, tableName
		}
	}

	// INSERT INTO - manual table name extraction
	if bytes.HasPrefix(upperPrefix, []byte("INSERT INTO")) {
		tableName := extractTableName(stmt, 11) // start after "INSERT INTO"
		if tableName != "" {
			return Insert, tableName
		}
	}

	// Fallback to regex for complex cases (ALTER, CREATE INDEX, etc.)
	// These are less common so regex overhead is acceptable
	if bytes.HasPrefix(upperPrefix, []byte("CREATE INDEX")) {
		if matches := p.createIndexRe.FindSubmatch(stmt); matches != nil && len(matches) > 1 {
			return CreateIndex, string(matches[1])
		}
	}

	// ... other cases

	return Unknown, ""
}

// extractTableName manually extracts table name after a keyword
// Handles: tablename, `tablename`, "tablename"
func extractTableName(stmt []byte, startPos int) string {
	// Skip whitespace after keyword
	i := startPos
	for i < len(stmt) && (stmt[i] == ' ' || stmt[i] == '\t' || stmt[i] == '\n') {
		i++
	}

	if i >= len(stmt) {
		return ""
	}

	// Handle backticks or quotes
	var endChar byte
	if stmt[i] == '`' || stmt[i] == '"' {
		endChar = stmt[i]
		i++ // skip opening quote
	} else {
		endChar = 0
	}

	start := i

	// Scan until delimiter
	for i < len(stmt) {
		b := stmt[i]

		if endChar != 0 {
			// Quoted: look for matching quote
			if b == endChar {
				return string(stmt[start:i])
			}
		} else {
			// Unquoted: stop at space, paren, semicolon
			if b == ' ' || b == '\t' || b == '\n' || b == '(' || b == ';' {
				return string(stmt[start:i])
			}
		}
		i++
	}

	// Reached end without finding delimiter
	if endChar == 0 && i > start {
		return string(stmt[start:i])
	}

	return ""
}
```

**Validation Plan:**
1. Test against existing test suite (all table name formats)
2. Add benchmark comparison: regex vs. manual extraction
3. Verify backtick, quote, and unquoted table names
4. Edge cases: table names with special characters

**Expected Impact:**
- `ParseStatement` CPU: 325 ns/op → 200-250 ns/op (~30% improvement)
- Regex time: 1.11s → 0.3s (70% reduction for common cases)

---

### PRIORITY 3: Reduce Buffer Size for Better Cache Locality (Impact: 1.1-1.2x speedup)

**Problem:**
Current default buffer size is 256KB (MediumBufferSize for files >10MB). Benchmarks show 4KB buffers perform 3.3% better (267.75 vs 259.42 MB/s) due to L1/L2 cache hits.

**Solution:**
Adjust `DetermineBufferSize` to prefer smaller buffers unless file is extremely large.

**Expected Gain:** 10-20 MB/s (applied after Priority 1)
**Effort:** 30 minutes
**Risk:** VERY LOW (simple constant change)
**Complexity:** Trivial

**Code Example:**
```go
// Optimized buffer sizing for CPU cache locality
func DetermineBufferSize(fileSize int64) int {
	switch {
	case fileSize > 50*1024*1024*1024: // > 50GB - use larger buffer for fewer syscalls
		return HugeBufferSize // 1MB
	case fileSize > 10*1024*1024*1024: // > 10GB
		return LargeBufferSize // 512KB
	case fileSize > 1*1024*1024*1024: // > 1GB
		return MediumBufferSize // 256KB
	case fileSize > 100*1024*1024: // > 100MB
		return SmallBufferSize // 64KB
	default:
		return 4 * 1024 // 4KB - optimal for cache
	}
}
```

**Note:** After implementing Priority 1 (batched reading), re-benchmark to find optimal chunk size for `Peek()` calls. Initial recommendation: 4KB peek buffer.

---

### PRIORITY 4: Eliminate Redundant Statement Copies (Impact: 1.1x speedup)

**Problem:**
Each statement is copied 2-3 times:
1. `ReadStatement`: Pool buffer → result (line 122)
2. `WriteStatement`: stmt → stmtCopy (line 49)

**Solution:**
Use a zero-copy architecture where `ReadStatement` returns a buffer slice with ownership transfer.

**Expected Gain:** 15-25 MB/s
**Effort:** 3-4 hours
**Risk:** MEDIUM (requires careful ownership tracking)
**Complexity:** Moderate - requires API change

**Code Example:**
```go
// StatementBuffer is a pooled buffer with ownership semantics
type StatementBuffer struct {
	data []byte
	pool *sync.Pool
}

// Release returns the buffer to the pool
func (sb *StatementBuffer) Release() {
	if sb.pool != nil {
		data := sb.data[:0]
		sb.pool.Put(&data)
		sb.pool = nil
	}
}

// Bytes returns the underlying byte slice (valid until Release)
func (sb *StatementBuffer) Bytes() []byte {
	return sb.data
}

// Modified ReadStatement returns ownership-tracked buffer
func (p *Parser) ReadStatementBuffer() (*StatementBuffer, error) {
	bufPtr := p.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	// ... scanning logic ...

	return &StatementBuffer{
		data: buf,
		pool: p.bufferPool,
	}, nil
}

// Modified WriteStatement takes ownership of buffer
func (w *TableWriter) WriteStatementBuffer(stmtBuf *StatementBuffer) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write directly from buffer (no copy)
	if _, err := w.writer.Write(stmtBuf.Bytes()); err != nil {
		return err
	}
	if _, err := w.writer.WriteString("\n"); err != nil {
		return err
	}

	// Flush if needed
	if w.shouldFlush() {
		return w.writer.Flush()
	}

	return nil
}

// Updated Split function
func (s *Splitter) Split() error {
	// ... setup ...

	for {
		stmtBuf, err := p.ReadStatementBuffer()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		defer stmtBuf.Release() // Ensure cleanup

		stmtType, tableName := p.ParseStatement(stmtBuf.Bytes())

		if stmtType == parser.Unknown || tableName == "" {
			continue
		}

		w, err := s.writerPool.GetWriter(tableName, filename)
		if err != nil {
			return err
		}

		if err := w.WriteStatementBuffer(stmtBuf); err != nil {
			return err
		}

		s.updateStats(len(stmtBuf.Bytes()))
	}

	return s.writerPool.CloseAll()
}
```

**Validation Plan:**
1. Verify no buffer is used after Release()
2. Add `-race` detector tests
3. Benchmark memory allocations (should drop significantly)
4. Stress test with concurrent access patterns

**Expected Impact:**
- Memory allocations: 1233 allocs/op → ~800 allocs/op
- CPU overhead from copying: 10-15% reduction

---

### PRIORITY 5: SIMD Semicolon Search (Impact: 1.3-1.5x speedup for large statements)

**Problem:**
Scanning for semicolons is done byte-by-byte, even in the optimized version. For large INSERT statements (multi-MB), this is inefficient.

**Solution:**
Use SIMD (via assembly or `golang.org/x/sys/cpu`) to scan 16-32 bytes at a time for semicolons, then fallback to byte-scan near matches.

**Expected Gain:** 50-100 MB/s for workloads with large statements
**Effort:** 8-12 hours
**Risk:** HIGH (platform-specific, complex, requires ARM64 + x86_64 implementations)
**Complexity:** High - requires assembly knowledge

**Recommendation:** Defer until Priorities 1-3 are completed. SIMD provides diminishing returns for typical SQL statements (60-200 bytes) but shines for multi-MB statements.

**Pseudocode:**
```go
// Fast semicolon search using SIMD (conceptual)
func findSemicolonSIMD(data []byte, insideString bool) int {
	if insideString {
		return -1 // SIMD cannot handle string state
	}

	// Use SIMD to scan for ';' in 32-byte chunks
	// Implementation would use assembly: VPCMPEQB + VPMOVMSKB (x86_64)
	// or equivalent ARM64 NEON instructions

	// Pseudocode (actual implementation is assembly):
	// for i := 0; i < len(data)-32; i += 32 {
	//     mask := simd_compare_eq(data[i:i+32], ';')
	//     if mask != 0 {
	//         return i + trailing_zeros(mask)
	//     }
	// }

	// Fallback to byte-scan for remainder
	return -1
}
```

---

## Additional Low-Priority Optimizations

### 6. Parallel Table Writing (Impact: 1.1x for many-table workloads)
- **Current:** Sequential writes with mutex-protected WriterPool
- **Optimization:** Per-table write channels with goroutine workers
- **Effort:** 4-6 hours
- **Risk:** MEDIUM (requires careful synchronization)
- **Benefit:** Only helps if >100 unique tables

### 7. Memory-Mapped I/O (Impact: Unknown, likely negative)
- **Approach:** Use `mmap` instead of bufio.Reader
- **Risk:** HIGH - Go GC doesn't work well with mmap
- **Recommendation:** Skip - bufio is already efficient

### 8. Custom String→[]byte Conversion (Impact: Minimal)
- **Approach:** Use `unsafe.StringToBytes` to avoid allocation
- **Risk:** LOW (standard Go idiom)
- **Benefit:** Saves ~10 ns/op in ParseStatement
- **Recommendation:** Only if chasing last 5% of performance

---

## Timeline to 500 MB/s Target

### Day 1: Core Parser Optimization (4-8 hours)
- ✅ **Hour 1-2:** Implement batched ReadStatement (Priority 1)
- ✅ **Hour 3:** Add comprehensive test coverage
- ✅ **Hour 4:** Benchmark & validate (expect 350-450 MB/s)
- ✅ **Hour 5-6:** Optimize ParseStatement manual extraction (Priority 2)
- ✅ **Hour 7:** Buffer size tuning (Priority 3)
- ✅ **Hour 8:** Final benchmarks & profiling

**Expected End-of-Day 1:** 450-500 MB/s

### Day 2: Polish & Edge Cases (2-4 hours)
- ⚠️ **Hour 1-2:** Implement zero-copy StatementBuffer (Priority 4) if not at target
- ⚠️ **Hour 3:** Integration testing with 18GB test.sql
- ⚠️ **Hour 4:** Documentation & performance report

**Expected Final:** 500-550 MB/s

---

## Risk Assessment

| Optimization | Correctness Risk | Performance Risk | Mitigation                           |
|--------------|------------------|------------------|--------------------------------------|
| Priority 1   | MEDIUM           | LOW              | Extensive quote/escape test cases    |
| Priority 2   | LOW              | VERY LOW         | Regex fallback for edge cases        |
| Priority 3   | VERY LOW         | VERY LOW         | Simple constant change               |
| Priority 4   | MEDIUM           | LOW              | Race detector + ownership tracking   |
| Priority 5   | HIGH             | MEDIUM           | Defer until other optimizations done |

**Overall Risk:** LOW-MEDIUM. Priorities 1-3 are well-understood optimizations with clear validation strategies.

---

## Validation Checklist

Before deploying optimizations:

- [ ] All existing tests pass
- [ ] Benchmark improvement confirmed (5+ runs, <2% variance)
- [ ] Memory profile shows no regressions
- [ ] `-race` detector passes
- [ ] Real-world test.sql (18GB) processes correctly
- [ ] CPU profile confirms bottleneck reduction
- [ ] Edge cases tested: escaped quotes, multiline strings, very large statements
- [ ] Performance documented in git commit message

---

## Hardware Baseline Reference

**SSD Capability:**
- Sequential Read: ~3-4 GB/s (NVMe Apple Fabric)
- Current utilization: 207 MB/s ÷ 3000 MB/s = **6.9%**

**CPU Capability:**
- Single-core throughput: ~50 GB/s memory bandwidth (L1/L2 cache)
- Current utilization: Minimal - parser is **algorithmically bound**, not hardware-bound

**Conclusion:** There is massive headroom. The 500 MB/s target uses only 12.5% of SSD bandwidth and <1% of CPU capability. **The bottleneck is purely algorithmic inefficiency.**

---

## Profiling Commands for Reproduction

```bash
# Build release binary
CGO_ENABLED=0 go build -o sql-splitter-release -ldflags="-s -w" .

# Run benchmarks (5 iterations)
go test -bench=BenchmarkParser_ReadStatement -benchmem -count=5 ./internal/parser

# Generate CPU profile
go test -bench=BenchmarkParser_ReadStatement -cpuprofile=cpu.prof -benchtime=3s ./internal/parser

# Analyze profile
go tool pprof -top -cum cpu.prof
go tool pprof -list=ReadStatement cpu.prof

# Real-world profiling
go build -o profile_split profile_split.go
./profile_split test_medium.sql profile_output
go tool pprof -top -cum cpu_split.prof
```

---

## Conclusion

The SQL splitter is currently achieving **219 MB/s** in benchmarks and **207 MB/s** in real-world usage. To reach the **500 MB/s target**, the critical path is:

1. **Replace byte-by-byte reading with chunked scanning** (Priority 1) - 2-2.5x gain
2. **Replace regex with manual parsing for common cases** (Priority 2) - 1.2-1.5x gain
3. **Tune buffer sizes for cache locality** (Priority 3) - 1.1x gain

These three optimizations are **low-risk, high-impact, and achievable in 1-2 days**. The remaining optimizations (Priorities 4-5) offer diminishing returns and should only be pursued if the first three fall short of the 500 MB/s target.

**Recommendation:** Implement Priorities 1-3 immediately. Profile after each step. If 500 MB/s is reached after Priority 2, stop and ship. If not, proceed to Priority 4. Save Priority 5 (SIMD) for future optimization if extreme performance (>1 GB/s) is needed.
