# Competitive Analysis

**Date**: 2025-12-20  
**Purpose**: Reference for understanding the SQL dump processing ecosystem

## Executive Summary

sql-splitter occupies a **unique position** in the market by combining multiple capabilities that currently require separate tools. No existing tool offers the planned combination of: split + merge + sample with FK preservation + tenant sharding + redaction + query + diff + convert.

---

## Key Competitors by Feature

### Split/Merge

| Tool | Language | Stars | Split | Merge | Streaming | Notes |
|------|----------|-------|-------|-------|-----------|-------|
| **sql-splitter** | Rust | — | ✅ | 🟡 | ✅ | High-performance, multi-dialect |
| pgloader | Common Lisp | 6.2k | ❌ | ❌ | ✅ | Loader only, not splitter |
| mysqldumpsplit | Shell | — | ✅ | ❌ | ❌ | Basic scripts |

**Gap**: No robust split/merge tools exist. sql-splitter is unique here.

---

### Sample with FK Preservation

| Tool | Language | Stars | FK-Aware | Streaming | CLI-First | Notes |
|------|----------|-------|----------|-----------|-----------|-------|
| **sql-splitter** | Rust | — | 🟡 | ✅ | ✅ | Planned |
| **Jailer** | Java | 3.1k | ✅ | ❌ | ❌ | GUI-heavy, JDBC-based |

**[Jailer](https://github.com/Wisser/Jailer)** is the closest competitor:
- ✅ Excellent FK-preserving subsetting
- ✅ Topological sort output
- ✅ Multiple export formats (SQL, JSON, XML)
- ❌ Requires database connection (JDBC)
- ❌ GUI-focused, not CLI-first
- ❌ No streaming for large dumps
- ❌ No anonymization

**Gap**: No streaming, CLI-first, FK-aware sampler exists. sql-splitter can be first.

---

### Tenant/Shard Extraction

| Tool | Notes |
|------|-------|
| **sql-splitter** | Planned: FK chain resolution for tenant extraction |
| Jailer | Limited: can filter by starting entity |
| DuckDB | Via manual SQL queries only |

**Gap**: No tool specifically handles multi-tenant extraction with automatic FK chain following.

---

### Redaction/Anonymization

| Tool | Language | Stars | MySQL | PostgreSQL | SQLite | Streaming |
|------|----------|-------|-------|------------|--------|-----------|
| **sql-splitter** | Rust | — | 🟡 | 🟡 | 🟡 | ✅ |
| **nxs-data-anonymizer** | Go | 271 | ✅ | ✅ | ❌ | ✅ |
| pg-anonymizer | TypeScript | 236 | ❌ | ✅ | ❌ | ✅ |
| go-anonymize-mysqldump | Go | 60 | ✅ | ❌ | ❌ | ✅ |

**[nxs-data-anonymizer](https://github.com/nixys/nxs-data-anonymizer)** is notable:
- ✅ Go templates + Sprig functions for flexible rules
- ✅ Cross-column value linking
- ✅ Streaming/pipe-compatible
- ❌ No SQLite
- ❌ No FK handling

**Gap**: No SQLite anonymization tool. No combined sample+anonymize workflow.

---

### Query/Filter Dumps

| Tool | Language | Stars | Notes |
|------|----------|-------|-------|
| **sql-splitter** | Rust | — | Planned: WHERE-style filtering |
| **DuckDB** | C++ | 34.8k | Query via SQL, excellent but general-purpose |
| **sqlglot** | Python | 8.7k | Parse/transpile, not filter |

**[DuckDB](https://github.com/duckdb/duckdb)** could solve querying:
- ✅ Query SQL/CSV/JSON/Parquet directly
- ✅ Extremely powerful
- ❌ Overkill for simple dump filtering
- ❌ No FK-aware subsetting

---

### Dialect Conversion

| Tool | Language | Stars | Dialects | COPY↔INSERT |
|------|----------|-------|----------|-------------|
| **sql-splitter** | Rust | — | 4 (planned) | 🟡 |
| **sqlglot** | Python | 8.7k | 31 dialects | ❌ |
| pgloader | Common Lisp | 6.2k | → PG only | ✅ |
| mysql2postgres | Ruby | 716 | MySQL→PG | Partial |

**[sqlglot](https://github.com/tobymao/sqlglot)** is excellent for query transpilation:
- ✅ 31 dialect support
- ✅ Pure Python, fast
- ✅ AST manipulation
- ❌ Not designed for full dump conversion
- ❌ Doesn't handle COPY blocks

**Gap**: No tool handles full dump conversion with COPY↔INSERT and streaming.

---

### MSSQL Support

| Tool | MSSQL |
|------|-------|
| **sql-splitter** | 🟡 Planned |
| Jailer | ✅ (via JDBC) |
| pgloader | ❌ |
| sqlglot | ✅ (parsing only) |
| nxs-data-anonymizer | ❌ |

**Gap**: Major gap in ecosystem. No MSSQL dump processing tools.

---

## Comparison Matrix

| Feature | sql-splitter | pgloader | Jailer | nxs-anonymizer | sqlglot | DuckDB |
|---------|-------------|----------|--------|----------------|---------|--------|
| Split per-table | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Merge files | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Sample + FK | 🟡 | ❌ | ✅ | ❌ | ❌ | ❌ |
| Tenant sharding | 🟡 | ❌ | Limited | ❌ | ❌ | Via SQL |
| Redaction | 🟡 | ❌ | ❌ | ✅ | ❌ | ❌ |
| Query/Filter | 🟡 | ❌ | Limited | ❌ | ✅ | ✅ |
| Diff | 🟡 | ❌ | Limited | ❌ | ❌ | Via SQL |
| Convert dialects | 🟡 | → PG | Limited | ❌ | ✅ | ✅ |
| MySQL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| SQLite | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| MSSQL | 🟡 | ❌ | ✅ | ❌ | ✅ | ❌ |
| Streaming | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ |
| CLI-first | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| High-perf | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |

---

## sql-splitter's Unique Value Proposition

1. **Unified tool** — No context switching between 5 different tools
2. **Streaming architecture** — Handle 10GB+ dumps without memory issues
3. **CLI-first** — DevOps/automation friendly
4. **Multi-dialect** — MySQL, PostgreSQL, SQLite, MSSQL in one tool
5. **FK-aware operations** — Sample and shard preserve referential integrity
6. **Rust performance** — Faster than Python/Java alternatives
7. **Composable** — Split → Sample → Redact → Merge pipeline

---

## Potential Integrations

Consider these as complementary tools or inspiration:

| Tool | Use Case |
|------|----------|
| **sqlglot** | Reference for dialect conversion grammar |
| **DuckDB** | Alternative for complex ad-hoc queries |
| **Jailer** | Reference for FK subsetting algorithms |
| **nxs-data-anonymizer** | Reference for Go template-based redaction |
| **pgloader** | Reference for high-performance data loading |

---

## Recommendations

1. **Prioritize unique features** — Split/merge, FK-aware sample/shard are differentiators
2. **Don't over-invest in Convert** — sqlglot exists; focus on COPY↔INSERT which it lacks
3. **Market the combination** — "One tool for split + sample + anonymize + convert"
4. **Target DevOps** — CLI + streaming + pipes is the right approach
5. **Consider DuckDB integration** — For complex query needs, suggest DuckDB as complement

---

## Related

- [Roadmap](ROADMAP.md)
- [Jailer GitHub](https://github.com/Wisser/Jailer)
- [nxs-data-anonymizer GitHub](https://github.com/nixys/nxs-data-anonymizer)
- [sqlglot GitHub](https://github.com/tobymao/sqlglot)
- [DuckDB GitHub](https://github.com/duckdb/duckdb)
