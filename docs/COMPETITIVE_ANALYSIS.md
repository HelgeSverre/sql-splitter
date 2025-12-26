# Competitive Analysis

**Last Updated**: 2025-12-26
**Purpose**: Comprehensive competitive landscape and feature opportunity analysis

## Executive Summary

sql-splitter occupies a **unique position** in the SQL dump processing ecosystem by combining multiple capabilities that currently require separate tools. As of v1.9.0, we offer: **split + merge + analyze + validate + sample (FK-preserving) + shard + convert + diff + redact**.

No existing tool offers this combination in a single, streaming, CLI-first, multi-dialect binary.

**Key differentiators:**
- Works on dump files directly (no database connection required)
- Streaming architecture handles 10GB+ dumps
- Multi-dialect support (MySQL, PostgreSQL, SQLite)
- 600+ MB/s throughput

---

## Current sql-splitter Feature Status

| Feature | Status | Version |
|---------|--------|---------|
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
| Validate (integrity checks) | ✅ Implemented | v1.8.0 |
| Diff dumps | ✅ Implemented | v1.9.0 |
| Redaction/anonymization | ✅ Implemented | v1.9.0 |
| Query/Filter (WHERE-style) | 🟡 Planned | — |
| MSSQL support | 🟡 Planned | — |

---

## Core Competitors by Feature

### Split/Merge

| Tool | Language | Stars | Split | Merge | Streaming | Multi-dialect | Notes |
|------|----------|-------|-------|-------|-----------|---------------|-------|
| **sql-splitter** | Rust | — | ✅ | ✅ | ✅ | ✅ | High-performance, 3 dialects |
| **mydumper** | C | 3k | ✅ | ✅ | ✅ | ❌ | MySQL only, parallel dump/restore |
| **mysqldumpsplitter** | Shell | 500+ | ✅ | ❌ | ❌ | ❌ | Basic regex extraction |
| **pgloader** | Common Lisp | 5k+ | ❌ | ❌ | ✅ | ❌ | Loader only, not splitter |
| **Dumpling** | Go | 282 | ✅ | ❌ | ✅ | ❌ | Archived, MySQL/TiDB only |

**[mydumper](https://github.com/mydumper/mydumper)** is notable:
- ✅ Multi-threaded parallel operations
- ✅ Consistent snapshots
- ✅ Basic masquerading (anonymization)
- ❌ MySQL/MariaDB only
- ❌ Requires database connection for dump

**Gap**: No other tool combines split/merge with streaming + multi-dialect support.

---

### FK-Aware Sampling

| Tool | Language | Stars | FK-Aware | Streaming | CLI-First | Notes |
|------|----------|-------|----------|-----------|-----------|-------|
| **sql-splitter** | Rust | — | ✅ | ✅ | ✅ | v1.5.0 |
| **Jailer** | Java | 3.1k | ✅ | ❌ | ❌ | GUI-heavy, JDBC-based |
| **Condenser** | Python | 327 | ✅ | ❌ | ✅ | Config-driven, FK cycle breaking |
| **subsetter** | Python | ~10 | ✅ | ❌ | ✅ | Simple, pip installable |

**[Jailer](https://github.com/Wisser/Jailer)** is comprehensive:
- ✅ Excellent FK-preserving subsetting
- ✅ 12+ database support (via JDBC)
- ✅ Multiple export formats
- ❌ Requires database connection
- ❌ GUI-focused, not CLI-first

**[Condenser](https://github.com/TonicAI/condenser)** (by Tonic.ai):
- ✅ Simple YAML config
- ✅ FK cycle detection and breaking
- ❌ PostgreSQL/MySQL only
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
| **sql-splitter** | Rust | — | ✅ | ✅ | ✅ | ✅ | v1.9.0 |
| **nxs-data-anonymizer** | Go | 271 | ✅ | ✅ | ❌ | ✅ | Go templates + Sprig |
| **pynonymizer** | Python | 109 | ✅ | ✅ | ❌ | ❌ | Faker integration, GDPR focus |
| **myanon** | C | ~30 | ✅ | ❌ | ❌ | ✅ | stdin/stdout streaming |

**[pynonymizer](https://github.com/rwnx/pynonymizer)**:
- ✅ Faker integration for realistic data
- ✅ GDPR compliance focus
- ❌ Requires temp database (not pure streaming)
- ❌ No SQLite

**Gap**: sql-splitter is the only multi-dialect, streaming anonymizer with SQLite support.

---

### Dialect Conversion

| Tool | Language | Stars | Dialects | COPY↔INSERT | Streaming |
|------|----------|-------|----------|-------------|-----------|
| **sql-splitter** | Rust | — | 3 (✅) | ✅ | ✅ |
| **sqlglot** | Python | 7k+ | 31 | ❌ | ❌ |
| **pgloader** | Common Lisp | 5k+ | → PG only | ✅ | ✅ |
| **mysql2postgres** | Ruby | 300 | MySQL→PG | Partial | ❌ |

**[sqlglot](https://github.com/tobymao/sqlglot)** is excellent for query transpilation:
- ✅ 31 dialect support
- ✅ AST manipulation
- ❌ Not designed for full dump conversion
- ❌ Doesn't handle COPY blocks or session commands

**sql-splitter's convert advantages**:
- ✅ PostgreSQL COPY → INSERT with NULL/escape handling
- ✅ Session command stripping
- ✅ 30+ data type mappings
- ✅ Compressed input support

**Gap**: sql-splitter handles full dump conversion with COPY↔INSERT that no other tool does.

---

### Query/Filter Dumps

| Tool | Language | Stars | Notes |
|------|----------|-------|-------|
| **sql-splitter** | Rust | — | 🟡 Planned: WHERE-style filtering |
| **DuckDB** | C++ | 34.8k | Query SQL/CSV/JSON/Parquet directly |
| **sqlglot** | Python | 7k+ | Parse/transpile, not filter |

**[DuckDB](https://github.com/duckdb/duckdb)** could solve querying but is overkill for simple dump filtering.

---

### MSSQL Support

| Tool | MSSQL |
|------|-------|
| **sql-splitter** | 🟡 Planned |
| Jailer | ✅ (via JDBC) |
| pynonymizer | ✅ |
| sqlglot | ✅ (parsing only) |
| pgloader | ❌ |

**Gap**: Major gap in ecosystem for MSSQL dump processing CLI tools.

---

## Extended Competitor Landscape

### Schema Management & Versioning

| Tool | Category | Key Features | sql-splitter Opportunity |
|------|----------|--------------|--------------------------|
| **Liquibase** | Schema versioning | Changeset tracking, rollback, diff | Migration tracking |
| **Flyway** | Schema migration | Version control, repeatable migrations | Schema versioning |
| **Atlas** | Schema-as-code | Declarative schema, drift detection | Drift detection |
| **sqitch** | DB change mgmt | Plan-based migrations, VCS integration | Change tracking |
| **Skeema** | MySQL schema mgmt | Schema sync, workspace isolation | Workspace management |

### Data Quality & Profiling

| Tool | Category | Key Features | sql-splitter Opportunity |
|------|----------|--------------|--------------------------|
| **Great Expectations** | Data quality | Expectations as tests, profiling | Data quality checks |
| **dbt** | Data transformation | SQL-based tests, documentation | Test generation |
| **Apache Griffin** | Data quality | Accuracy, profiling, timeliness | Statistical profiling |
| **datafold** | Data diff | Column-level diff, value distribution | Distribution analysis |
| **soda-sql** | Data testing | SQL-based quality checks | Quality metrics |

### Database Optimization

| Tool | Category | Key Features | sql-splitter Opportunity |
|------|----------|--------------|--------------------------|
| **pt-query-digest** | Query analysis | Slow query analysis, recommendations | Query optimization |
| **pgBadger** | PostgreSQL analysis | Query stats, performance insights | Performance analysis |
| **MySQLTuner** | MySQL tuning | Configuration recommendations | Config optimization |
| **pganalyze** | PostgreSQL monitoring | Index recommendations, vacuum analysis | Index optimization |

### Test Data & Fixtures

| Tool | Category | Key Features | sql-splitter Opportunity |
|------|----------|--------------|--------------------------|
| **Faker** | Fake data | Locale-aware generators | (in redact) |
| **Mockaroo** | Test data | Schema-based generation, APIs | Schema-driven generation |
| **Snaplet** | Copy production | Subset + anonymize + seed | Production cloning |
| **tonic.ai** | Test data platform | Smart subsetting, masking | AI-powered subsetting |

### ETL & Data Pipeline

| Tool | Category | Key Features | sql-splitter Opportunity |
|------|----------|--------------|--------------------------|
| **dlt** | Data pipeline | Python-based ETL, schema evolution | Pipeline generation |
| **Airbyte** | Data integration | Connectors, CDC, normalization | CDC support |
| **Meltano** | ELT platform | Singer taps, dbt integration | Change data capture |

### Documentation & Discovery

| Tool | Category | Key Features | sql-splitter Opportunity |
|------|----------|--------------|--------------------------|
| **SchemaSpy** | DB documentation | HTML reports, diagrams | Interactive docs |
| **tbls** | DB documentation | Markdown docs, ER diagrams | Documentation generation |
| **Azimutt** | Schema explorer | Interactive exploration, AI chat | Interactive exploration |
| **DataHub** | Data catalog | Metadata, lineage, discovery | Metadata catalog |

---

## Comparison Matrix

| Feature | sql-splitter | mydumper | pgloader | Jailer | Condenser | nxs-anon | sqlglot | DuckDB |
|---------|-------------|----------|----------|--------|-----------|----------|---------|--------|
| Split per-table | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Merge files | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Sample + FK | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Tenant sharding | ✅ | ❌ | ❌ | Limited | Limited | ❌ | ❌ | Via SQL |
| Redaction | ✅ | Basic | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Query/Filter | 🟡 | ❌ | ❌ | Limited | ❌ | ❌ | ✅ | ✅ |
| Diff | ✅ | ❌ | ❌ | Limited | ❌ | ❌ | ❌ | Via SQL |
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

## Unique Value Proposition

1. **Unified tool** — Split + merge + sample + shard + convert + diff + redact in one binary
2. **Works on dump files** — No database connection required (unlike Jailer, Condenser, mydumper)
3. **Streaming architecture** — Handle 10GB+ dumps without memory issues
4. **CLI-first** — DevOps/automation friendly, pipe-compatible
5. **Multi-dialect** — MySQL, PostgreSQL, SQLite in one tool
6. **FK-aware operations** — Sample and shard preserve referential integrity
7. **Rust performance** — 600+ MB/s, faster than Python/Java alternatives
8. **Compression support** — gzip, bz2, xz, zstd auto-detected
9. **Composable** — Split → Sample → Redact → Convert → Merge pipeline

---

## Feature Opportunities

### Tier 1: High Impact, Unique Value

#### 1. Schema Drift Detection
Compare production dump against expected schema:
```bash
sql-splitter drift prod.sql schema.sql
# Detects: columns added/removed, type changes, missing indexes
```
**Gap**: Atlas does this but requires running database. sql-splitter works on dumps.
**Effort**: ~16h (extends diff command)

#### 2. Smart Index Recommendations
Analyze schema and suggest optimal indexes:
```bash
sql-splitter recommend dump.sql --slow-queries slow.log
# Suggests: missing indexes based on FKs, high-cardinality columns, query patterns
```
**Gap**: pganalyze/pt-query-digest require running DB
**Effort**: ~24h

#### 3. Data Quality Profiling
Profile data quality from dumps:
```bash
sql-splitter profile dump.sql
# Reports: NULL rates, duplicates, format validation, statistical outliers
```
**Gap**: Great Expectations requires Python setup
**Effort**: ~32h

#### 4. Change Data Capture (CDC)
Generate CDC events from dump diffs:
```bash
sql-splitter cdc old.sql new.sql --format json
# Outputs: INSERT/UPDATE/DELETE events for streaming
```
**Gap**: Airbyte/Meltano need live DB connection
**Effort**: ~28h

### Tier 2: High Value, Lower Effort

#### 5. Schema Size Optimization
Recommend efficient column types:
```bash
sql-splitter optimize dump.sql
# Suggests: BIGINT→INT, VARCHAR(255)→VARCHAR(50), etc.
```
**Effort**: ~12h

#### 6. Security Audit
Detect security issues in schema/data:
```bash
sql-splitter audit dump.sql --security
# Detects: plain text passwords, weak hashing, exposed PII
```
**Effort**: ~20h

#### 7. Compliance Check (GDPR, HIPAA)
Verify compliance:
```bash
sql-splitter compliance dump.sql --standard gdpr
# Checks: deletion cascades, data retention, consent tracking
```
**Effort**: ~24h

#### 8. Cost Estimation
Estimate cloud database costs:
```bash
sql-splitter cost dump.sql --cloud aws
# Estimates: RDS instance size, storage, backup costs
```
**Effort**: ~8h

### Tier 3: Innovative / Experimental

#### 9. AI-Powered Schema Suggestions
LLM-based schema optimization:
```bash
sql-splitter suggest dump.sql --ai
# Suggests: denormalization, partitioning, normalization fixes
```
**Effort**: ~40h

#### 10. Natural Language Query
Query dumps with natural language:
```bash
sql-splitter ask dump.sql "show me users who signed up in December"
```
**Effort**: ~24h

#### 11. Schema Testing Framework
Automated schema quality tests:
```bash
sql-splitter test dump.sql --config schema-tests.yaml
# Tests: all tables have PKs, no VARCHAR(255), FKs indexed
```
**Effort**: ~16h

---

## Strategic Recommendations

### Product Positioning
1. **"Complete Dump Toolkit"** — Split, convert, anonymize, analyze, optimize, secure, test
2. **Tagline**: "The Swiss Army knife for SQL dumps"

### Target Markets
1. **Enterprise** — Compliance (GDPR, HIPAA), security auditing, cost optimization
2. **Developer Experience** — Index recommendations, schema testing, quality profiling
3. **DevOps** — CLI-first, streaming, pipes, automation

### Priorities
1. **Complete v2.0** — Current roadmap features
2. **Quick wins** — Schema drift (16h), size optimization (12h), cost estimation (8h)
3. **Differentiation** — Data quality profiling, compliance checks
4. **Future** — AI integration for schema suggestions, natural language queries

---

## Competitor Links

### Split/Merge
- [mydumper](https://github.com/mydumper/mydumper)
- [mysqldumpsplitter](https://github.com/kedarvj/mysqldumpsplitter)
- [Dumpling](https://github.com/pingcap/dumpling) (archived)

### FK-Aware Sampling
- [Jailer](https://github.com/Wisser/Jailer)
- [Condenser](https://github.com/TonicAI/condenser)
- [subsetter](https://github.com/msg555/subsetter)

### Anonymization
- [nxs-data-anonymizer](https://github.com/nixys/nxs-data-anonymizer)
- [pynonymizer](https://github.com/rwnx/pynonymizer)
- [myanon](https://github.com/ppomes/myanon)

### Dialect Conversion
- [sqlglot](https://github.com/tobymao/sqlglot)
- [pgloader](https://github.com/dimitri/pgloader)
- [node-sql-parser](https://www.npmjs.com/package/node-sql-parser)

### Schema Management
- [Liquibase](https://github.com/liquibase/liquibase)
- [Flyway](https://github.com/flyway/flyway)
- [Atlas](https://github.com/ariga/atlas)
- [Skeema](https://github.com/skeema/skeema)

### Data Quality
- [Great Expectations](https://github.com/great-expectations/great_expectations)
- [dbt](https://github.com/dbt-labs/dbt-core)
- [soda-sql](https://github.com/sodadata/soda-sql)

### General
- [DuckDB](https://github.com/duckdb/duckdb)

---

## Related

- [Roadmap](ROADMAP.md)
- [Changelog](../CHANGELOG.md)
