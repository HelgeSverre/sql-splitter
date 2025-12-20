# Competitive Analysis

**Date**: 2025-12-21  
**Purpose**: Reference for understanding the SQL dump processing ecosystem

## Executive Summary

sql-splitter occupies a **unique position** in the market by combining multiple capabilities that currently require separate tools. As of v1.7.0, we offer: **split + merge + sample with FK preservation + tenant sharding + dialect conversion**. Planned features include: redaction, query, and diff.

No existing tool offers this combination in a single, streaming, CLI-first, multi-dialect tool.

---

## Current sql-splitter Feature Status (v1.7.0)

| Feature | Status | Version Added |
|---------|--------|---------------|
| Split per-table | ✅ Implemented | v1.0.0 |
| Analyze dumps | ✅ Implemented | v1.0.0 |
| Multi-dialect (MySQL, PostgreSQL, SQLite) | ✅ Implemented | v1.1.0 |
| Auto-detect dialect | ✅ Implemented | v1.2.0 |
| Compressed files (gzip, bz2, xz, zstd) | ✅ Implemented | v1.3.0 |
| Schema-only / Data-only filtering | ✅ Implemented | v1.3.0 |
| Shell completions | ✅ Implemented | v1.3.0 |
| Merge files | ✅ Implemented | v1.4.0 |
| FK-aware sampling | ✅ Implemented | v1.5.0 |
| Tenant sharding | ✅ Implemented | v1.6.0 |
| Dialect conversion | ✅ Implemented | v1.7.0 |
| Redaction/anonymization | 🟡 Planned | — |
| Query/Filter (WHERE-style) | 🟡 Planned | — |
| Diff dumps | 🟡 Planned | — |
| MSSQL support | 🟡 Planned | — |

---

## Key Competitors by Feature

### Split/Merge

| Tool | Language | Stars | Split | Merge | Streaming | Multi-dialect | Notes |
|------|----------|-------|-------|-------|-----------|---------------|-------|
| **sql-splitter** | Rust | — | ✅ | ✅ | ✅ | ✅ | High-performance, 3 dialects |
| **mydumper** | C | 3k | ✅ | ✅ | ✅ | ❌ | MySQL only, parallel dump/restore |
| **mysqldumpsplitter** | Shell | 500+ | ✅ | ❌ | ❌ | ❌ | Basic regex extraction |
| **pgloader** | Common Lisp | 5k+ | ❌ | ❌ | ✅ | ❌ | Loader only, not splitter |
| **Dumpling** | Go | 282 | ✅ | ❌ | ✅ | ❌ | Archived, MySQL/TiDB only |
| **SQLSplit** | C++ | 4 | ✅ | ✅ | ❌ | ❌ | Simple regex-based |

**[mydumper](https://github.com/mydumper/mydumper)** is notable:
- ✅ Multi-threaded parallel operations
- ✅ Consistent snapshots
- ✅ Active development (3k stars)
- ✅ Basic masquerading (anonymization)
- ❌ MySQL/MariaDB only
- ❌ Requires database connection for dump

**Gap**: No other tool combines split/merge with streaming + multi-dialect support. sql-splitter is unique.

---

### Sample with FK Preservation

| Tool | Language | Stars | FK-Aware | Streaming | CLI-First | Notes |
|------|----------|-------|----------|-----------|-----------|-------|
| **sql-splitter** | Rust | — | ✅ | ✅ | ✅ | v1.5.0 |
| **Jailer** | Java | 3.1k | ✅ | ❌ | ❌ | GUI-heavy, JDBC-based |
| **Condenser** | Python | 327 | ✅ | ❌ | ✅ | Config-driven, FK cycle breaking |
| **subsetter** | Python | ~10 | ✅ | ❌ | ✅ | Simple, pip installable |
| **DBSubsetter** | Scala | ~50 | ✅ | ❌ | ✅ | Less maintained |

**[Jailer](https://github.com/Wisser/Jailer)** is the most comprehensive:
- ✅ Excellent FK-preserving subsetting
- ✅ Topological sort output
- ✅ 12+ database support (via JDBC)
- ✅ Multiple export formats (SQL, JSON, XML, DbUnit)
- ❌ Requires database connection (JDBC)
- ❌ GUI-focused, not CLI-first
- ❌ No streaming for large dumps
- ❌ No anonymization

**[Condenser](https://github.com/TonicAI/condenser)** (by Tonic.ai):
- ✅ Simple YAML config
- ✅ FK cycle detection and breaking
- ✅ Passthrough tables support
- ✅ Implicit FK support
- ❌ PostgreSQL/MySQL only
- ❌ Limited to ~10GB databases
- ❌ Requires database connection

**Gap**: sql-splitter is the only streaming, CLI-first, FK-aware sampler that works on dump files directly.

---

### Tenant/Shard Extraction

| Tool | Notes |
|------|-------|
| **sql-splitter** | ✅ v1.6.0: FK chain resolution, auto tenant column detection |
| Jailer | Limited: can filter by starting entity |
| Condenser | Limited: via starting point constraints |
| DuckDB | Via manual SQL queries only |

**Gap**: sql-splitter is unique in offering dedicated multi-tenant extraction with automatic FK chain following directly on dump files.

---

### Redaction/Anonymization

| Tool | Language | Stars | MySQL | PostgreSQL | SQLite | Streaming | Notes |
|------|----------|-------|-------|------------|--------|-----------|-------|
| **sql-splitter** | Rust | — | 🟡 | 🟡 | 🟡 | ✅ | Planned |
| **nxs-data-anonymizer** | Go | 271 | ✅ | ✅ | ❌ | ✅ | Go templates + Sprig |
| **pynonymizer** | Python | 109 | ✅ | ✅ | ❌ | ❌ | Faker integration, GDPR focus |
| **myanon** | C | ~30 | ✅ | ❌ | ❌ | ✅ | stdin/stdout streaming |
| **pganonymize** | Python | — | ❌ | ✅ | ❌ | ❌ | YAML config |
| **pg-anonymizer** | TypeScript | 236 | ❌ | ✅ | ❌ | ✅ | |
| **go-anonymize-mysqldump** | Go | 60 | ✅ | ❌ | ❌ | ✅ | |
| **dumpctl** | Go | ~5 | ✅ | ❌ | ❌ | ✅ | Early stage |

**[pynonymizer](https://github.com/rwnx/pynonymizer)** is notable:
- ✅ Faker integration for realistic data
- ✅ GDPR compliance focus
- ✅ Compressed I/O
- ✅ MSSQL support
- ❌ Requires temp database (not pure streaming)
- ❌ No SQLite

**[myanon](https://github.com/ppomes/myanon)** is notable:
- ✅ True stdin/stdout streaming
- ✅ HMAC-SHA256 for consistent hashing
- ✅ Python/Faker rules
- ❌ MySQL-only

**Gap**: No SQLite anonymization tool exists. No combined sample+anonymize workflow.

---

### Dialect Conversion

| Tool | Language | Stars | Dialects | COPY↔INSERT | Streaming |
|------|----------|-------|----------|-------------|-----------|
| **sql-splitter** | Rust | — | 3 (✅) | ✅ | ✅ |
| **sqlglot** | Python | 7k+ | 31 | ❌ | ❌ |
| **pgloader** | Common Lisp | 5k+ | → PG only | ✅ | ✅ |
| **mysql2postgres** | Ruby | 300 | MySQL→PG | Partial | ❌ |
| **node-sql-parser** | JavaScript | 800 | 12 | ❌ | ❌ |
| **jOOQ Translator** | Web | — | 25+ | ❌ | ❌ |

**[sqlglot](https://github.com/tobymao/sqlglot)** is excellent for query transpilation:
- ✅ 31 dialect support
- ✅ AST manipulation and optimization
- ✅ Active development (7k+ stars)
- ❌ Not designed for full dump conversion
- ❌ Doesn't handle COPY blocks or session commands

**sql-splitter's convert advantages**:
- ✅ PostgreSQL COPY → INSERT with NULL/escape handling
- ✅ Session command stripping (SET, PRAGMA, etc.)
- ✅ 30+ data type mappings (AUTO_INCREMENT ↔ SERIAL, etc.)
- ✅ Streaming architecture
- ✅ Compressed input support

**Gap**: sql-splitter handles full dump conversion with COPY↔INSERT that no other tool does.

---

### Query/Filter Dumps

| Tool | Language | Stars | Notes |
|------|----------|-------|-------|
| **sql-splitter** | Rust | — | 🟡 Planned: WHERE-style filtering |
| **DuckDB** | C++ | 34.8k | Query SQL/CSV/JSON/Parquet directly |
| **sqlglot** | Python | 7k+ | Parse/transpile, not filter |

**[DuckDB](https://github.com/duckdb/duckdb)** could solve querying:
- ✅ Query SQL/CSV/JSON/Parquet directly
- ✅ Extremely powerful analytical engine
- ❌ Overkill for simple dump filtering
- ❌ No FK-aware subsetting
- ❌ Loads data into memory

---

### MSSQL Support

| Tool | MSSQL |
|------|-------|
| **sql-splitter** | 🟡 Planned |
| Jailer | ✅ (via JDBC) |
| pynonymizer | ✅ |
| sqlglot | ✅ (parsing only) |
| pgloader | ❌ |
| nxs-data-anonymizer | ❌ |

**Gap**: Major gap in ecosystem for MSSQL dump processing CLI tools.

---

## Comparison Matrix

| Feature | sql-splitter | mydumper | pgloader | Jailer | Condenser | nxs-anonymizer | sqlglot | DuckDB |
|---------|-------------|----------|----------|--------|-----------|----------------|---------|--------|
| Split per-table | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Merge files | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Sample + FK | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Tenant sharding | ✅ | ❌ | ❌ | Limited | Limited | ❌ | ❌ | Via SQL |
| Redaction | 🟡 | Basic | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Query/Filter | 🟡 | ❌ | ❌ | Limited | ❌ | ❌ | ✅ | ✅ |
| Diff | 🟡 | ❌ | ❌ | Limited | ❌ | ❌ | ❌ | Via SQL |
| Convert dialects | ✅ | ❌ | → PG | Limited | ❌ | ❌ | ✅ | ✅ |
| MySQL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| SQLite | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| MSSQL | 🟡 | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ |
| Streaming | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ |
| CLI-first | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| Works on dumps | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ |
| Compression | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |

---

## sql-splitter's Unique Value Proposition

1. **Unified tool** — Split + merge + sample + shard + convert in one binary
2. **Works on dump files** — No database connection required (unlike Jailer, Condenser, mydumper)
3. **Streaming architecture** — Handle 10GB+ dumps without memory issues
4. **CLI-first** — DevOps/automation friendly, pipe-compatible
5. **Multi-dialect** — MySQL, PostgreSQL, SQLite in one tool
6. **FK-aware operations** — Sample and shard preserve referential integrity
7. **Rust performance** — 600+ MB/s, faster than Python/Java alternatives
8. **Compression support** — gzip, bz2, xz, zstd auto-detected
9. **Composable** — Split → Sample → Convert → Merge pipeline

---

## Potential Integrations

Consider these as complementary tools or inspiration:

| Tool | Use Case |
|------|----------|
| **sqlglot** | Reference for dialect conversion grammar |
| **DuckDB** | Alternative for complex ad-hoc queries |
| **Jailer** | Reference for FK subsetting algorithms |
| **Condenser** | Reference for cycle detection in FK graphs |
| **nxs-data-anonymizer** | Reference for Go template-based redaction |
| **pynonymizer** | Reference for Faker-based anonymization |
| **pgloader** | Reference for high-performance data loading |
| **mydumper** | Reference for parallel dump operations |

---

## Recommendations

1. **Prioritize redaction** — Next major differentiator; combine with sample for powerful dev data workflow
2. **Don't over-invest in query** — DuckDB exists for complex needs; focus on simple WHERE filtering
3. **Market the combination** — "One tool for split + sample + anonymize + convert"
4. **Target DevOps** — CLI + streaming + pipes is the right approach
5. **Consider MSSQL** — Major gap in ecosystem for dump processing
6. **Highlight "works on dumps"** — Key differentiator vs Jailer/Condenser which require DB connections

---

## Related

- [Roadmap](ROADMAP.md)
- [Changelog](../CHANGELOG.md)

### Competitor Links

**Split/Merge:**
- [mydumper](https://github.com/mydumper/mydumper)
- [mysqldumpsplitter](https://github.com/kedarvj/mysqldumpsplitter)
- [Dumpling](https://github.com/pingcap/dumpling) (archived)

**FK-Aware Sampling:**
- [Jailer](https://github.com/Wisser/Jailer)
- [Condenser](https://github.com/TonicAI/condenser)
- [subsetter](https://github.com/msg555/subsetter)
- [DBSubsetter](https://github.com/bluerogue251/DBSubsetter)

**Anonymization:**
- [nxs-data-anonymizer](https://github.com/nixys/nxs-data-anonymizer)
- [pynonymizer](https://github.com/rwnx/pynonymizer)
- [myanon](https://github.com/ppomes/myanon)
- [pganonymize](https://pypi.org/project/pganonymize/)

**Dialect Conversion:**
- [sqlglot](https://github.com/tobymao/sqlglot)
- [pgloader](https://github.com/dimitri/pgloader)
- [mysql2postgres](https://github.com/mysql2postgres/mysql2postgres)
- [node-sql-parser](https://www.npmjs.com/package/node-sql-parser)

**General:**
- [DuckDB](https://github.com/duckdb/duckdb)
