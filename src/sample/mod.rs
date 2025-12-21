//! Sample command for creating reduced datasets from SQL dumps.
//!
//! The sample command creates reduced datasets while optionally preserving
//! foreign key integrity through dependency-aware FK chain resolution.
//!
//! Supports MySQL, PostgreSQL, and SQLite dialects.

mod config;
mod reservoir;

pub use config::{DefaultClassifier, GlobalTableMode, SampleYamlConfig, TableClassification};
pub use reservoir::Reservoir;

use crate::parser::mysql_insert::{parse_mysql_insert_rows, ParsedRow, PkSet};
use crate::parser::postgres_copy::{parse_copy_columns, parse_postgres_copy_rows, ParsedCopyRow};
use crate::parser::{ContentFilter, Parser, SqlDialect, StatementType};
use crate::schema::{SchemaBuilder, SchemaGraph, TableId};
use crate::splitter::Splitter;
use ahash::AHashMap;
use indicatif::{ProgressBar, ProgressStyle};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Sampling mode
#[derive(Debug, Clone, Copy)]
pub enum SampleMode {
    /// Sample N% of rows from each table
    Percent(u32),
    /// Sample up to N rows from each table
    Rows(usize),
}

/// Configuration for the sample command
#[derive(Debug)]
pub struct SampleConfig {
    /// Input SQL file
    pub input: PathBuf,
    /// Output SQL file (None for stdout)
    pub output: Option<PathBuf>,
    /// SQL dialect
    pub dialect: SqlDialect,
    /// Sampling mode
    pub mode: SampleMode,
    /// Preserve foreign key relationships
    pub preserve_relations: bool,
    /// Only sample these tables (None = all)
    pub tables_filter: Option<Vec<String>>,
    /// Exclude these tables
    pub exclude: Vec<String>,
    /// Root tables for sampling (start from these)
    pub root_tables: Vec<String>,
    /// How to handle global/lookup tables
    pub include_global: GlobalTableMode,
    /// Random seed for reproducibility
    pub seed: u64,
    /// Dry run mode (show stats only)
    pub dry_run: bool,
    /// Show progress
    pub progress: bool,
    /// YAML config file path
    pub config_file: Option<PathBuf>,
    /// Maximum total rows to sample (explosion guard)
    pub max_total_rows: Option<usize>,
    /// Fail if any FK integrity issues detected
    pub strict_fk: bool,
    /// Include schema statements in output
    pub include_schema: bool,
}

impl Default for SampleConfig {
    fn default() -> Self {
        Self {
            input: PathBuf::new(),
            output: None,
            dialect: SqlDialect::MySql,
            mode: SampleMode::Percent(10),
            preserve_relations: false,
            tables_filter: None,
            exclude: Vec::new(),
            root_tables: Vec::new(),
            include_global: GlobalTableMode::Lookups,
            seed: rand::random(),
            dry_run: false,
            progress: false,
            config_file: None,
            max_total_rows: None,
            strict_fk: false,
            include_schema: true,
        }
    }
}

/// Statistics from sample operation
#[derive(Debug, Default)]
pub struct SampleStats {
    /// Number of tables sampled
    pub tables_sampled: usize,
    /// Number of tables skipped
    pub tables_skipped: usize,
    /// Total rows selected
    pub total_rows_selected: u64,
    /// Total rows seen
    pub total_rows_seen: u64,
    /// Per-table statistics
    pub table_stats: Vec<TableSampleStats>,
    /// Warning messages
    pub warnings: Vec<String>,
    /// FK orphan count (rows rejected due to missing parents)
    pub fk_orphans_rejected: u64,
}

/// Per-table sampling statistics
#[derive(Debug, Clone)]
pub struct TableSampleStats {
    pub name: String,
    pub rows_seen: u64,
    pub rows_selected: u64,
    pub classification: TableClassification,
}

/// Runtime state for a table during sampling
struct TableRuntime {
    /// Table name
    name: String,
    /// Selected rows with format metadata
    selected_rows: Vec<SelectedRow>,
    /// Primary key set for FK membership checks
    pk_set: PkSet,
    /// Rows seen count
    rows_seen: u64,
    /// Whether to skip this table
    skip: bool,
    /// Table classification
    classification: TableClassification,
    /// FK orphans rejected for this table
    fk_orphans: u64,
}

/// Combined row representation for both MySQL INSERT and PostgreSQL COPY
enum UnifiedRow {
    Insert(ParsedRow),
    Copy(ParsedCopyRow),
}

/// Row format indicator for output
#[derive(Debug, Clone, Copy, PartialEq)]
enum RowFormat {
    Insert,
    Copy,
}

/// Selected row with format metadata
struct SelectedRow {
    raw: Vec<u8>,
    format: RowFormat,
}

impl UnifiedRow {
    fn pk(&self) -> Option<&smallvec::SmallVec<[crate::parser::mysql_insert::PkValue; 2]>> {
        match self {
            UnifiedRow::Insert(r) => r.pk.as_ref(),
            UnifiedRow::Copy(r) => r.pk.as_ref(),
        }
    }

    fn fk_values(
        &self,
    ) -> &[(
        crate::parser::mysql_insert::FkRef,
        smallvec::SmallVec<[crate::parser::mysql_insert::PkValue; 2]>,
    )] {
        match self {
            UnifiedRow::Insert(r) => &r.fk_values,
            UnifiedRow::Copy(r) => &r.fk_values,
        }
    }

    fn into_selected(self) -> SelectedRow {
        match self {
            UnifiedRow::Insert(r) => SelectedRow {
                raw: r.raw,
                format: RowFormat::Insert,
            },
            UnifiedRow::Copy(r) => SelectedRow {
                raw: r.raw,
                format: RowFormat::Copy,
            },
        }
    }
}

/// Run the sample command
pub fn run(config: SampleConfig) -> anyhow::Result<SampleStats> {
    // Load YAML config if provided
    let yaml_config = if let Some(ref path) = config.config_file {
        Some(SampleYamlConfig::load(path)?)
    } else {
        None
    };

    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut stats = SampleStats::default();

    // Progress bar setup
    let progress_bar = if config.progress {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        Some(pb)
    } else {
        None
    };

    // Phase 0: Split into temp per-table files
    let temp_dir = TempDir::new()?;
    let tables_dir = temp_dir.path().join("tables");

    if let Some(ref pb) = progress_bar {
        pb.set_message("Splitting dump into per-table files...");
    }

    let splitter = Splitter::new(config.input.clone(), tables_dir.clone())
        .with_dialect(config.dialect)
        .with_content_filter(ContentFilter::All);

    let split_stats = splitter.split()?;

    if let Some(ref pb) = progress_bar {
        pb.set_message(format!(
            "Split complete: {} tables, {} statements",
            split_stats.tables_found, split_stats.statements_processed
        ));
    }

    // Phase 1: Build schema graph
    if let Some(ref pb) = progress_bar {
        pb.set_message("Building schema graph...");
    }

    let graph = build_schema_graph(&tables_dir, &config)?;

    let (topo_order, cyclic_tables) = graph.processing_order();

    if !cyclic_tables.is_empty() {
        let names: Vec<_> = cyclic_tables
            .iter()
            .filter_map(|&id| graph.table_name(id))
            .collect();
        let msg = format!(
            "Warning: {} tables have FK cycles (intra-cycle FK enforcement disabled): {:?}",
            cyclic_tables.len(),
            names
        );
        if config.progress {
            eprintln!("{}", msg);
        }
        stats.warnings.push(msg);
    }

    // Build set of cyclic table IDs for quick lookup
    let cyclic_set: ahash::AHashSet<TableId> = cyclic_tables.iter().copied().collect();

    // Determine root tables
    let explicit_roots: ahash::AHashSet<String> = config
        .root_tables
        .iter()
        .map(|s| s.to_lowercase())
        .collect();

    // Initialize table runtimes with classification
    let mut runtimes: AHashMap<TableId, TableRuntime> = AHashMap::new();
    for table in graph.schema.iter() {
        let classification =
            determine_classification(&table.name, &graph, table.id, &yaml_config, &explicit_roots);
        let skip = should_skip_table(&table.name, &config, &yaml_config, classification);

        runtimes.insert(
            table.id,
            TableRuntime {
                name: table.name.clone(),
                selected_rows: Vec::new(),
                pk_set: PkSet::default(),
                rows_seen: 0,
                skip,
                classification,
                fk_orphans: 0,
            },
        );
    }

    // Phase 2: Process tables in dependency order
    if let Some(ref pb) = progress_bar {
        pb.set_message(format!(
            "Sampling {} tables in dependency order...",
            topo_order.len()
        ));
    }

    // Process acyclic tables first, then cyclic tables
    let all_tables: Vec<TableId> = topo_order.into_iter().chain(cyclic_tables).collect();

    let mut total_selected: u64 = 0;

    for table_id in &all_tables {
        let table_schema = match graph.schema.table(*table_id) {
            Some(s) => s,
            None => continue,
        };

        // Check if we should skip this table
        let (should_skip, table_name, classification) = {
            let runtime = match runtimes.get(table_id) {
                Some(r) => r,
                None => continue,
            };
            (runtime.skip, runtime.name.clone(), runtime.classification)
        };

        if should_skip {
            stats.tables_skipped += 1;
            continue;
        }

        // Handle lookup/global tables specially
        let sample_mode = match classification {
            TableClassification::Lookup => {
                match config.include_global {
                    GlobalTableMode::None => {
                        stats.tables_skipped += 1;
                        continue;
                    }
                    GlobalTableMode::Lookups | GlobalTableMode::All => {
                        // Include all rows
                        SampleMode::Percent(100)
                    }
                }
            }
            TableClassification::System => {
                stats.tables_skipped += 1;
                continue;
            }
            _ => get_table_sample_mode(&table_name, &config, &yaml_config),
        };

        let table_file = tables_dir.join(format!("{}.sql", table_name));
        if !table_file.exists() {
            continue;
        }

        // Parse statements from this table file
        let file = File::open(&table_file)?;
        let mut parser = Parser::with_dialect(file, 64 * 1024, config.dialect);

        // Collect rows based on sampling mode
        let mut candidates: Vec<UnifiedRow> = Vec::new();
        let mut rows_seen = 0u64;
        let mut fk_orphans = 0u64;

        // For PostgreSQL COPY, track the current column order
        let mut copy_columns: Vec<String> = Vec::new();

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, _) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, config.dialect);

            match stmt_type {
                StatementType::Insert => {
                    let rows = parse_mysql_insert_rows(&stmt, table_schema)?;

                    for row in rows {
                        rows_seen += 1;
                        let unified = UnifiedRow::Insert(row);

                        if config.preserve_relations {
                            let (passes, orphan) = check_unified_fk_membership(
                                &unified,
                                table_schema,
                                &runtimes,
                                &cyclic_set,
                                table_id,
                            );
                            if !passes {
                                fk_orphans += 1;
                                if orphan && config.strict_fk {
                                    anyhow::bail!(
                                        "FK integrity violation in table '{}': row references missing parent",
                                        table_name
                                    );
                                }
                                continue;
                            }
                        }

                        candidates.push(unified);
                    }
                }
                StatementType::Copy => {
                    // Extract column order from COPY header
                    let header = String::from_utf8_lossy(&stmt);
                    copy_columns = parse_copy_columns(&header);
                }
                StatementType::Unknown if config.dialect == SqlDialect::Postgres => {
                    // This might be COPY data
                    if stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n") {
                        let rows =
                            parse_postgres_copy_rows(&stmt, table_schema, copy_columns.clone())?;

                        for row in rows {
                            rows_seen += 1;
                            let unified = UnifiedRow::Copy(row);

                            if config.preserve_relations {
                                let (passes, orphan) = check_unified_fk_membership(
                                    &unified,
                                    table_schema,
                                    &runtimes,
                                    &cyclic_set,
                                    table_id,
                                );
                                if !passes {
                                    fk_orphans += 1;
                                    if orphan && config.strict_fk {
                                        anyhow::bail!(
                                            "FK integrity violation in table '{}': row references missing parent",
                                            table_name
                                        );
                                    }
                                    continue;
                                }
                            }

                            candidates.push(unified);
                        }
                    }
                }
                _ => {}
            }
        }

        // Check max_total_rows guard
        if let Some(max) = config.max_total_rows {
            if total_selected + candidates.len() as u64 > max as u64 {
                let msg = format!(
                    "Warning: Reached max_total_rows limit ({}) at table '{}'",
                    max, table_name
                );
                stats.warnings.push(msg);
                break;
            }
        }

        // Apply sampling to candidates
        let selected = sample_rows(&candidates, sample_mode, &mut rng);

        // Update total count
        total_selected += selected.len() as u64;

        // Store selected rows and update PK set
        let runtime = runtimes.get_mut(table_id).unwrap();
        runtime.rows_seen = rows_seen;
        runtime.fk_orphans = fk_orphans;

        for row in selected {
            if let Some(pk) = row.pk() {
                runtime.pk_set.insert(pk.clone());
            }
            runtime.selected_rows.push(row.into_selected());
        }

        stats.fk_orphans_rejected += fk_orphans;

        stats.table_stats.push(TableSampleStats {
            name: runtime.name.clone(),
            rows_seen: runtime.rows_seen,
            rows_selected: runtime.selected_rows.len() as u64,
            classification: runtime.classification,
        });
    }

    // Calculate totals
    for table_stats in &stats.table_stats {
        stats.total_rows_seen += table_stats.rows_seen;
        stats.total_rows_selected += table_stats.rows_selected;
    }
    stats.tables_sampled = stats.table_stats.len();

    if let Some(ref pb) = progress_bar {
        pb.finish_with_message("Sampling complete");
    }

    // Phase 3: Output synthesis
    if config.dry_run {
        return Ok(stats);
    }

    if config.progress {
        eprintln!("Writing output...");
    }

    write_output(&config, &graph, &all_tables, &runtimes, &tables_dir, &stats)?;

    Ok(stats)
}

/// Build schema graph from split table files
fn build_schema_graph(tables_dir: &Path, config: &SampleConfig) -> anyhow::Result<SchemaGraph> {
    let mut builder = SchemaBuilder::new();

    for entry in fs::read_dir(tables_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map(|e| e == "sql").unwrap_or(false) {
            let file = File::open(&path)?;
            let mut parser = Parser::with_dialect(file, 64 * 1024, config.dialect);

            while let Some(stmt) = parser.read_statement()? {
                let stmt_str = String::from_utf8_lossy(&stmt);
                let (stmt_type, _) =
                    Parser::<&[u8]>::parse_statement_with_dialect(&stmt, config.dialect);

                match stmt_type {
                    StatementType::CreateTable => {
                        builder.parse_create_table(&stmt_str);
                    }
                    StatementType::AlterTable => {
                        builder.parse_alter_table(&stmt_str);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(SchemaGraph::from_schema(builder.build()))
}

/// Determine table classification
fn determine_classification(
    name: &str,
    graph: &SchemaGraph,
    table_id: TableId,
    yaml_config: &Option<SampleYamlConfig>,
    explicit_roots: &ahash::AHashSet<String>,
) -> TableClassification {
    // Check explicit roots first
    if explicit_roots.contains(&name.to_lowercase()) {
        return TableClassification::Root;
    }

    // Check YAML config
    if let Some(ref config) = yaml_config {
        let class = config.get_classification(name);
        if class != TableClassification::Normal {
            return class;
        }
    }

    // Check if it's a graph root (no parents)
    if graph.parents[table_id.0 as usize].is_empty() {
        return TableClassification::Root;
    }

    // Use default classifier
    DefaultClassifier::classify(name)
}

/// Check if a table should be skipped
fn should_skip_table(
    name: &str,
    config: &SampleConfig,
    yaml_config: &Option<SampleYamlConfig>,
    classification: TableClassification,
) -> bool {
    let name_lower = name.to_lowercase();

    // Check exclude list
    if config
        .exclude
        .iter()
        .any(|e| e.to_lowercase() == name_lower)
    {
        return true;
    }

    // Check YAML skip
    if let Some(ref yc) = yaml_config {
        if yc.should_skip(name) {
            return true;
        }
    }

    // Check include filter
    if let Some(ref filter) = config.tables_filter {
        if !filter.iter().any(|f| f.to_lowercase() == name_lower) {
            return true;
        }
    }

    // Skip system tables by default
    if classification == TableClassification::System {
        return true;
    }

    false
}

/// Get sample mode for a specific table
fn get_table_sample_mode(
    name: &str,
    config: &SampleConfig,
    yaml_config: &Option<SampleYamlConfig>,
) -> SampleMode {
    // Check YAML config first
    if let Some(ref yc) = yaml_config {
        if let Some(rows) = yc.get_rows(name) {
            return SampleMode::Rows(rows);
        }
        if let Some(percent) = yc.get_percent(name) {
            return SampleMode::Percent(percent);
        }
    }

    // Fall back to global config
    config.mode
}

/// Check FK membership for a unified row (works with both INSERT and COPY rows)
fn check_unified_fk_membership(
    row: &UnifiedRow,
    table_schema: &crate::schema::TableSchema,
    runtimes: &AHashMap<TableId, TableRuntime>,
    cyclic_set: &ahash::AHashSet<TableId>,
    current_table_id: &TableId,
) -> (bool, bool) {
    let mut passes = true;
    let mut is_orphan = false;

    for (fk_ref, fk_tuple) in row.fk_values() {
        if let Some(fk) = table_schema.foreign_keys.get(fk_ref.fk_index as usize) {
            if let Some(parent_id) = fk.referenced_table_id {
                // Skip FK check for cyclic tables
                if cyclic_set.contains(&parent_id) && cyclic_set.contains(current_table_id) {
                    continue;
                }

                // Check if parent row exists in parent's pk_set
                if let Some(parent_runtime) = runtimes.get(&parent_id) {
                    if !parent_runtime.pk_set.contains(fk_tuple) {
                        passes = false;
                        is_orphan = true;
                        break;
                    }
                }
            }
        }
    }

    (passes, is_orphan)
}

/// Sample rows according to sampling mode
fn sample_rows(candidates: &[UnifiedRow], mode: SampleMode, rng: &mut StdRng) -> Vec<UnifiedRow> {
    match mode {
        SampleMode::Percent(p) => {
            // Bernoulli sampling
            let prob = p as f64 / 100.0;
            candidates
                .iter()
                .filter(|_| rng.gen_bool(prob.min(1.0)))
                .map(|r| match r {
                    UnifiedRow::Insert(row) => UnifiedRow::Insert(row.clone()),
                    UnifiedRow::Copy(row) => UnifiedRow::Copy(row.clone()),
                })
                .collect()
        }
        SampleMode::Rows(n) => {
            // Reservoir sampling
            let mut reservoir = Reservoir::new(n, StdRng::from_rng(rng).unwrap());
            for row in candidates {
                let cloned = match row {
                    UnifiedRow::Insert(r) => UnifiedRow::Insert(r.clone()),
                    UnifiedRow::Copy(r) => UnifiedRow::Copy(r.clone()),
                };
                reservoir.consider(cloned);
            }
            reservoir.into_items()
        }
    }
}

/// Write sampled output
fn write_output(
    config: &SampleConfig,
    _graph: &SchemaGraph,
    table_order: &[TableId],
    runtimes: &AHashMap<TableId, TableRuntime>,
    tables_dir: &Path,
    stats: &SampleStats,
) -> anyhow::Result<()> {
    let mut writer: Box<dyn Write> = match &config.output {
        Some(path) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            Box::new(BufWriter::with_capacity(256 * 1024, File::create(path)?))
        }
        None => Box::new(BufWriter::new(std::io::stdout())),
    };

    // Write header comment
    write_header(&mut writer, config, stats)?;

    // Write dialect-specific header
    write_dialect_header(&mut writer, config.dialect)?;

    // Write schema for each table (if enabled)
    if config.include_schema {
        for &table_id in table_order {
            let runtime = match runtimes.get(&table_id) {
                Some(r) if !r.skip && !r.selected_rows.is_empty() => r,
                _ => continue,
            };

            let table_file = tables_dir.join(format!("{}.sql", runtime.name));
            if !table_file.exists() {
                continue;
            }

            // Copy schema statements from table file
            let file = File::open(&table_file)?;
            let mut parser = Parser::with_dialect(file, 64 * 1024, config.dialect);

            while let Some(stmt) = parser.read_statement()? {
                let (stmt_type, _) =
                    Parser::<&[u8]>::parse_statement_with_dialect(&stmt, config.dialect);

                if stmt_type.is_schema() {
                    writer.write_all(&stmt)?;
                    writer.write_all(b"\n")?;
                }
            }
        }
    }

    // Write data for each table
    for &table_id in table_order {
        let runtime = match runtimes.get(&table_id) {
            Some(r) if !r.skip && !r.selected_rows.is_empty() => r,
            _ => continue,
        };

        let table_name = &runtime.name;
        let row_count = runtime.selected_rows.len();

        writeln!(writer, "\n-- Data: {} ({} rows)", table_name, row_count)?;

        // Write INSERTs in chunks (compact multi-row format)
        const CHUNK_SIZE: usize = 1000;

        // Get the table name quoting based on dialect
        let quoted_name = match config.dialect {
            SqlDialect::MySql => format!("`{}`", table_name),
            SqlDialect::Postgres | SqlDialect::Sqlite => format!("\"{}\"", table_name),
        };

        for chunk in runtime.selected_rows.chunks(CHUNK_SIZE) {
            writeln!(writer, "INSERT INTO {} VALUES", quoted_name)?;

            for (i, row) in chunk.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b",\n")?;
                }

                // Convert row to INSERT VALUES format based on original format
                let values = match row.format {
                    RowFormat::Insert => {
                        // Already in INSERT format, but may need dialect conversion
                        match config.dialect {
                            SqlDialect::Postgres => convert_row_to_postgres(&row.raw),
                            _ => row.raw.clone(),
                        }
                    }
                    RowFormat::Copy => {
                        // Convert COPY format to INSERT VALUES
                        convert_copy_to_insert_values(&row.raw, config.dialect)
                    }
                };
                writer.write_all(&values)?;
            }

            writer.write_all(b";\n")?;
        }
    }

    // Write dialect-specific footer
    write_dialect_footer(&mut writer, config.dialect)?;

    writer.flush()?;

    Ok(())
}

/// Write header comment
fn write_header<W: Write>(
    writer: &mut W,
    config: &SampleConfig,
    stats: &SampleStats,
) -> std::io::Result<()> {
    writeln!(writer, "-- Sampled from: {}", config.input.display())?;
    writeln!(
        writer,
        "-- Date: {}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    )?;
    writeln!(
        writer,
        "-- Mode: {:?}{}",
        config.mode,
        if config.preserve_relations {
            ", preserve-relations"
        } else {
            ""
        }
    )?;
    writeln!(writer, "-- Seed: {}", config.seed)?;
    writeln!(writer, "-- Dialect: {}", config.dialect)?;
    writeln!(writer, "--")?;
    writeln!(writer, "-- Statistics:")?;
    writeln!(writer, "--   Tables sampled: {}", stats.tables_sampled)?;
    writeln!(writer, "--   Tables skipped: {}", stats.tables_skipped)?;

    let percent = if stats.total_rows_seen > 0 {
        (stats.total_rows_selected as f64 / stats.total_rows_seen as f64) * 100.0
    } else {
        0.0
    };
    writeln!(
        writer,
        "--   Total rows: {} (from {} original, {:.1}%)",
        stats.total_rows_selected, stats.total_rows_seen, percent
    )?;

    if stats.fk_orphans_rejected > 0 {
        writeln!(
            writer,
            "--   FK orphans rejected: {}",
            stats.fk_orphans_rejected
        )?;
    }

    if !stats.warnings.is_empty() {
        writeln!(writer, "--   Warnings: {}", stats.warnings.len())?;
    }

    writeln!(writer)?;

    Ok(())
}

/// Write dialect-specific header
fn write_dialect_header<W: Write>(writer: &mut W, dialect: SqlDialect) -> std::io::Result<()> {
    match dialect {
        SqlDialect::MySql => {
            writeln!(writer, "SET NAMES utf8mb4;")?;
            writeln!(writer, "SET FOREIGN_KEY_CHECKS = 0;")?;
        }
        SqlDialect::Postgres => {
            writeln!(writer, "SET client_encoding = 'UTF8';")?;
            writeln!(writer, "SET session_replication_role = replica;")?;
        }
        SqlDialect::Sqlite => {
            writeln!(writer, "PRAGMA foreign_keys = OFF;")?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

/// Write dialect-specific footer
fn write_dialect_footer<W: Write>(writer: &mut W, dialect: SqlDialect) -> std::io::Result<()> {
    writeln!(writer)?;
    match dialect {
        SqlDialect::MySql => {
            writeln!(writer, "SET FOREIGN_KEY_CHECKS = 1;")?;
        }
        SqlDialect::Postgres => {
            writeln!(writer, "SET session_replication_role = DEFAULT;")?;
        }
        SqlDialect::Sqlite => {
            writeln!(writer, "PRAGMA foreign_keys = ON;")?;
        }
    }
    Ok(())
}

/// Convert a MySQL-style row to PostgreSQL syntax
fn convert_row_to_postgres(row: &[u8]) -> Vec<u8> {
    // Simple conversion: just replace escaped quotes
    // A full implementation would handle more edge cases
    let mut result = Vec::with_capacity(row.len());
    let mut i = 0;

    while i < row.len() {
        if row[i] == b'\\' && i + 1 < row.len() && row[i + 1] == b'\'' {
            // MySQL: \' -> PostgreSQL: ''
            result.push(b'\'');
            result.push(b'\'');
            i += 2;
        } else {
            result.push(row[i]);
            i += 1;
        }
    }

    result
}

/// Convert PostgreSQL COPY format (tab-separated) to INSERT VALUES format
fn convert_copy_to_insert_values(row: &[u8], dialect: SqlDialect) -> Vec<u8> {
    let mut result = Vec::with_capacity(row.len() + 20);
    result.push(b'(');

    let fields: Vec<&[u8]> = row.split(|&b| b == b'\t').collect();

    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            result.extend_from_slice(b", ");
        }

        // Check for NULL marker
        if *field == b"\\N" {
            result.extend_from_slice(b"NULL");
        } else if field.is_empty() {
            // Empty string
            match dialect {
                SqlDialect::MySql => result.extend_from_slice(b"''"),
                SqlDialect::Postgres | SqlDialect::Sqlite => result.extend_from_slice(b"''"),
            }
        } else if is_numeric(field) {
            // Numeric value - no quotes needed
            result.extend_from_slice(field);
        } else {
            // String value - needs quoting
            result.push(b'\'');
            for &b in *field {
                match b {
                    b'\'' => {
                        // Escape single quote
                        match dialect {
                            SqlDialect::MySql => result.extend_from_slice(b"\\'"),
                            SqlDialect::Postgres | SqlDialect::Sqlite => {
                                result.extend_from_slice(b"''")
                            }
                        }
                    }
                    b'\\' if dialect == SqlDialect::MySql => {
                        // Escape backslash in MySQL
                        result.extend_from_slice(b"\\\\");
                    }
                    _ => result.push(b),
                }
            }
            result.push(b'\'');
        }
    }

    result.push(b')');
    result
}

/// Check if a byte slice represents a numeric value
fn is_numeric(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }

    let mut has_digit = false;
    let mut has_dot = false;
    let mut start = 0;

    // Handle leading sign
    if s[0] == b'-' || s[0] == b'+' {
        start = 1;
    }

    for &b in &s[start..] {
        match b {
            b'0'..=b'9' => has_digit = true,
            b'.' if !has_dot => has_dot = true,
            b'e' | b'E' => {
                // Scientific notation - just check rest is digits
                continue;
            }
            _ => return false,
        }
    }

    has_digit
}
