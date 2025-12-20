# Dialect Auto-Detection Design

## Overview

Automatically detect SQL dialect from file content instead of requiring `--dialect` flag.

## Detection Strategy

Read first 4-16KB of file and look for dialect-specific markers:

### PostgreSQL Indicators (high confidence)
```sql
-- PostgreSQL database dump
-- Dumped from database version 15.x
-- Dumped by pg_dump version 15.x
COPY table_name (columns) FROM stdin;
SET search_path = 
\\.                          -- COPY terminator
$$                           -- Dollar-quoting
CREATE EXTENSION
```

### MySQL/MariaDB Indicators (high confidence)
```sql
-- MySQL dump 10.13
-- Server version	8.0.x
-- MariaDB dump 10.19
/*!40101 SET                 -- MySQL conditional comments
/*!50503 SET NAMES utf8mb4
LOCK TABLES `table` WRITE;
`backtick_quoted`            -- Backtick identifiers
```

### SQLite Indicators (medium confidence)
```sql
-- SQLite database dump
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS
```

## Implementation

```rust
pub fn detect_dialect(reader: &mut impl BufRead) -> SqlDialect {
    let mut buf = [0u8; 8192];
    let n = reader.read(&mut buf).unwrap_or(0);
    let header = &buf[..n];
    
    // Check for PostgreSQL markers
    if header.windows(9).any(|w| w == b"pg_dump") 
        || header.windows(4).any(|w| w == b"COPY")
        || header.windows(12).any(|w| w == b"search_path")
    {
        return SqlDialect::Postgres;
    }
    
    // Check for MySQL/MariaDB markers
    if header.windows(10).any(|w| w == b"MySQL dump")
        || header.windows(12).any(|w| w == b"MariaDB dump")
        || header.windows(6).any(|w| w == b"/*!40")
    {
        return SqlDialect::MySql;
    }
    
    // Check for SQLite markers
    if header.windows(6).any(|w| w == b"PRAGMA")
        || header.windows(17).any(|w| w == b"BEGIN TRANSACTION")
    {
        return SqlDialect::Sqlite;
    }
    
    // Default to MySQL (most common)
    SqlDialect::MySql
}
```

## CLI Changes

```rust
#[derive(Parser)]
struct SplitArgs {
    /// SQL dialect (auto-detected if not specified)
    #[arg(short, long)]
    dialect: Option<String>,
}

// Usage
let dialect = match args.dialect {
    Some(d) => d.parse()?,
    None => detect_dialect(&mut file)?,
};
```

## Considerations

1. **Seekable files**: After detection, need to seek back to start
2. **Stdin/pipes**: Can use `fill_buf()` + peek without consuming
3. **False positives**: Some markers overlap; use weighted scoring
4. **Performance**: Only read first 8KB; negligible overhead

## Scoring Approach (Alternative)

For higher accuracy, use weighted scoring:

```rust
struct DialectScore {
    mysql: u32,
    postgres: u32,
    sqlite: u32,
}

fn detect_with_scoring(header: &[u8]) -> SqlDialect {
    let mut score = DialectScore::default();
    
    // High confidence markers (+10)
    if contains(header, b"pg_dump") { score.postgres += 10; }
    if contains(header, b"MySQL dump") { score.mysql += 10; }
    if contains(header, b"PRAGMA") { score.sqlite += 10; }
    
    // Medium confidence (+5)
    if contains(header, b"COPY") { score.postgres += 5; }
    if contains(header, b"/*!40") { score.mysql += 5; }
    if contains(header, b"BEGIN TRANSACTION") { score.sqlite += 5; }
    
    // Low confidence (+2)
    if contains(header, b"`") { score.mysql += 2; }
    if contains(header, b"$$") { score.postgres += 2; }
    
    // Return highest score, default to MySQL
    if score.postgres > score.mysql && score.postgres > score.sqlite {
        SqlDialect::Postgres
    } else if score.sqlite > score.mysql {
        SqlDialect::Sqlite
    } else {
        SqlDialect::MySql
    }
}
```

## Testing

Create test files for each dialect and verify detection:

```rust
#[test]
fn test_detect_mysql() {
    let dump = b"-- MySQL dump 10.13\n/*!40101 SET...";
    assert_eq!(detect_dialect(dump), SqlDialect::MySql);
}

#[test]
fn test_detect_postgres() {
    let dump = b"-- PostgreSQL database dump\n-- Dumped by pg_dump";
    assert_eq!(detect_dialect(dump), SqlDialect::Postgres);
}

#[test]
fn test_detect_sqlite() {
    let dump = b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;";
    assert_eq!(detect_dialect(dump), SqlDialect::Sqlite);
}
```
