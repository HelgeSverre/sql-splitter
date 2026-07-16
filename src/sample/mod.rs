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

use crate::parser::mysql_insert::{hash_pk_tuple, PkHashSet, RowExtraction};
use crate::parser::{Parser, SqlDialect};
use crate::schema::{SchemaGraph, TableId};
use crate::transform_common::{
    build_schema_graph, for_each_data_row, quote_ident, split_to_temp_tables, write_dialect_footer,
    write_dialect_header, write_insert_chunk, RowFlow, RowFormat, RowSpillReader, RowSpillWriter,
    UnifiedRow,
};
use ahash::AHashMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

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
#[derive(Debug, Default, serde::Serialize)]
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
#[derive(Debug, Clone, serde::Serialize)]
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
    /// Primary key hashes for FK membership checks (compact: 8 bytes per key)
    pk_set: PkHashSet,
    /// Rows seen count
    rows_seen: u64,
    /// Rows selected count
    rows_selected: u64,
    /// Whether to skip this table
    skip: bool,
    /// Table classification
    classification: TableClassification,
    /// FK orphans rejected for this table
    fk_orphans: u64,
    /// Path to temp file containing selected row bytes (None if no rows selected yet)
    selected_temp_path: Option<PathBuf>,
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

    // Phase 0: Split into temp per-table files
    let split_phase = split_to_temp_tables(&config.input, config.dialect, config.progress)?;
    let temp_dir = split_phase.temp_dir;
    let tables_dir = split_phase.tables_dir;

    // Phase 1: Build schema graph
    if config.progress {
        eprintln!("Building schema graph...");
    }

    let graph = build_schema_graph(&tables_dir, config.dialect)?;

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
                pk_set: PkHashSet::default(),
                rows_seen: 0,
                rows_selected: 0,
                skip,
                classification,
                fk_orphans: 0,
                selected_temp_path: None,
            },
        );
    }

    // Create directory for selected row temp files
    let selected_dir = temp_dir.path().join("selected");
    fs::create_dir_all(&selected_dir)?;

    // Phase 2: Process tables in dependency order
    if config.progress {
        eprintln!(
            "Sampling {} tables in dependency order...",
            topo_order.len()
        );
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

        // Process table with streaming sampling - rows go directly to temp file
        let result = sample_table_streaming(
            &table_file,
            table_schema,
            *table_id,
            &table_name,
            sample_mode,
            &config,
            &runtimes,
            &cyclic_set,
            &selected_dir,
            &mut rng,
        )?;

        // Check max_total_rows guard
        if let Some(max) = config.max_total_rows {
            if total_selected + result.rows_selected > max as u64 {
                let msg = format!(
                    "Warning: Reached max_total_rows limit ({}) at table '{}'",
                    max, table_name
                );
                stats.warnings.push(msg);
                break;
            }
        }

        // Update total count
        total_selected += result.rows_selected;

        // Update runtime state and add PK hashes for FK checks by children
        // Safe: runtime existence was checked at loop start (line 323-326)
        let runtime = runtimes
            .get_mut(table_id)
            .expect("runtime must exist - checked at loop start");
        runtime.rows_seen = result.rows_seen;
        runtime.rows_selected = result.rows_selected;
        runtime.fk_orphans = result.fk_orphans;

        // Add PK hashes for FK membership checks by child tables
        for pk_hash in result.pk_hashes {
            runtime.pk_set.insert(pk_hash);
        }

        // Set the temp file path if we selected any rows
        if result.rows_selected > 0 {
            let temp_path = selected_dir.join(format!("{}.rows", table_name));
            if temp_path.exists() {
                runtime.selected_temp_path = Some(temp_path);
            }
        }

        stats.fk_orphans_rejected += result.fk_orphans;

        stats.table_stats.push(TableSampleStats {
            name: runtime.name.clone(),
            rows_seen: result.rows_seen,
            rows_selected: result.rows_selected,
            classification: runtime.classification,
        });
    }

    // Calculate totals
    for table_stats in &stats.table_stats {
        stats.total_rows_seen += table_stats.rows_seen;
        stats.total_rows_selected += table_stats.rows_selected;
    }
    stats.tables_sampled = stats.table_stats.len();

    if config.progress {
        eprintln!("Sampling complete");
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

/// Result from streaming sampling
struct StreamingSampleResult {
    rows_seen: u64,
    rows_selected: u64,
    fk_orphans: u64,
    /// PK hashes of selected rows (for FK checks by children)
    pk_hashes: Vec<u64>,
}

/// Apply the preserve-relations FK check to a row.
///
/// Returns `Ok(true)` if the row passes (or checking is disabled), `Ok(false)`
/// if the row is an orphan that should be skipped, and an error when strict FK
/// mode is enabled and an orphan is found.
fn passes_fk_or_bail(
    row: &UnifiedRow,
    table_schema: &crate::schema::TableSchema,
    table_id: TableId,
    table_name: &str,
    config: &SampleConfig,
    runtimes: &AHashMap<TableId, TableRuntime>,
    cyclic_set: &ahash::AHashSet<TableId>,
) -> anyhow::Result<bool> {
    if !config.preserve_relations {
        return Ok(true);
    }

    let (passes, orphan) =
        check_unified_fk_membership(row, table_schema, runtimes, cyclic_set, &table_id);
    if !passes {
        if orphan && config.strict_fk {
            anyhow::bail!(
                "FK integrity violation in table '{}': row references missing parent",
                table_name
            );
        }
        return Ok(false);
    }
    Ok(true)
}

/// Stream-sample a table: parse rows, apply FK checks, sample inline, write to temp file.
/// Returns StreamingSampleResult with stats and PK hashes.
/// Uses Bernoulli sampling for --percent mode (single pass).
/// For --rows mode, we use reservoir sampling on row indices with a second pass.
#[allow(clippy::too_many_arguments)]
fn sample_table_streaming(
    table_file: &Path,
    table_schema: &crate::schema::TableSchema,
    table_id: TableId,
    table_name: &str,
    sample_mode: SampleMode,
    config: &SampleConfig,
    runtimes: &AHashMap<TableId, TableRuntime>,
    cyclic_set: &ahash::AHashSet<TableId>,
    selected_dir: &Path,
    rng: &mut StdRng,
) -> anyhow::Result<StreamingSampleResult> {
    let mut rows_seen = 0u64;
    let mut rows_selected = 0u64;
    let mut fk_orphans = 0u64;

    // Temp file for selected rows
    let temp_path = selected_dir.join(format!("{}.rows", table_name));

    // Track PKs of selected rows (for children's FK checks)
    let mut selected_pk_hashes: Vec<u64> = Vec::new();

    match sample_mode {
        SampleMode::Percent(p) => {
            // Bernoulli sampling: decide immediately for each row
            let prob = p as f64 / 100.0;
            let mut spill: Option<RowSpillWriter> = None;

            for_each_data_row(
                table_file,
                table_schema,
                config.dialect,
                RowExtraction::PkFk,
                |row| {
                    rows_seen += 1;

                    // FK check
                    if !passes_fk_or_bail(
                        &row,
                        table_schema,
                        table_id,
                        table_name,
                        config,
                        runtimes,
                        cyclic_set,
                    )? {
                        fk_orphans += 1;
                        return Ok(RowFlow::Continue);
                    }

                    // Bernoulli sample
                    if rng.random::<f64>() < prob {
                        if spill.is_none() {
                            spill = Some(RowSpillWriter::create(&temp_path)?);
                        }
                        spill.as_mut().unwrap().write_row(row.format(), row.raw())?;

                        // Track PK hash
                        if let Some(pk) = row.pk() {
                            selected_pk_hashes.push(hash_pk_tuple(pk));
                        }
                        rows_selected += 1;
                    }

                    Ok(RowFlow::Continue)
                },
            )?;

            if let Some(spill) = spill {
                spill.finish()?;
            }
        }
        SampleMode::Rows(n) => {
            // Reservoir sampling: collect eligible row indices in first pass,
            // then write selected rows in second pass
            let mut reservoir: Reservoir<(u64, Option<u64>)> =
                Reservoir::new(n, StdRng::from_rng(&mut *rng));

            // First pass: build reservoir of (row_index, pk_hash)
            for_each_data_row(
                table_file,
                table_schema,
                config.dialect,
                RowExtraction::PkFk,
                |row| {
                    let current_idx = rows_seen;
                    rows_seen += 1;

                    if !passes_fk_or_bail(
                        &row,
                        table_schema,
                        table_id,
                        table_name,
                        config,
                        runtimes,
                        cyclic_set,
                    )? {
                        fk_orphans += 1;
                        return Ok(RowFlow::Continue);
                    }

                    let pk_hash = row.pk().map(hash_pk_tuple);
                    reservoir.consider((current_idx, pk_hash));
                    Ok(RowFlow::Continue)
                },
            )?;

            // Extract selected indices and PKs from reservoir
            let selected_items = reservoir.into_items();
            if selected_items.is_empty() {
                return Ok(StreamingSampleResult {
                    rows_seen,
                    rows_selected: 0,
                    fk_orphans,
                    pk_hashes: Vec::new(),
                });
            }

            // Collect PK hashes and sort indices for second pass
            let mut selected_indices: Vec<u64> = Vec::with_capacity(selected_items.len());
            for (idx, pk_hash) in selected_items {
                if let Some(h) = pk_hash {
                    selected_pk_hashes.push(h);
                }
                selected_indices.push(idx);
            }
            selected_indices.sort_unstable();

            // Second pass: write selected rows to temp file
            let mut spill = RowSpillWriter::create(&temp_path)?;
            let mut current_row_idx = 0u64;
            let mut select_iter = selected_indices.iter().peekable();

            for_each_data_row(
                table_file,
                table_schema,
                config.dialect,
                RowExtraction::PkFk,
                |row| {
                    let Some(&&next_idx) = select_iter.peek() else {
                        return Ok(RowFlow::Stop); // All selected rows written
                    };
                    if current_row_idx == next_idx {
                        spill.write_row(row.format(), row.raw())?;
                        rows_selected += 1;
                        select_iter.next();
                    }
                    current_row_idx += 1;
                    Ok(RowFlow::Continue)
                },
            )?;

            spill.finish()?;
        }
    }

    Ok(StreamingSampleResult {
        rows_seen,
        rows_selected,
        fk_orphans,
        pk_hashes: selected_pk_hashes,
    })
}

/// Check FK membership for a unified row (works with both INSERT and COPY rows)
/// Uses hash-based lookup for memory efficiency.
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

                // Check if parent row exists in parent's pk_set using hash lookup
                if let Some(parent_runtime) = runtimes.get(&parent_id) {
                    let fk_hash = hash_pk_tuple(fk_tuple);
                    if !parent_runtime.pk_set.contains(&fk_hash) {
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
                Some(r) if !r.skip && r.rows_selected > 0 => r,
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

    // Write data for each table (reading from temp files instead of memory)
    for &table_id in table_order {
        let runtime = match runtimes.get(&table_id) {
            Some(r) if !r.skip && r.rows_selected > 0 && r.selected_temp_path.is_some() => r,
            _ => continue,
        };

        let table_name = &runtime.name;
        let row_count = runtime.rows_selected;

        writeln!(writer, "\n-- Data: {} ({} rows)", table_name, row_count)?;

        // Get the table name quoting based on dialect
        let quoted_name = quote_ident(config.dialect, table_name);

        // Read rows from temp file and write INSERTs in chunks
        let temp_path = runtime.selected_temp_path.as_ref().unwrap();
        let mut spill_reader = RowSpillReader::open(temp_path)?;

        const CHUNK_SIZE: usize = 1000;
        let mut chunk_buffer: Vec<(RowFormat, Vec<u8>)> = Vec::with_capacity(CHUNK_SIZE);

        while let Some((format, row_bytes)) = spill_reader.next_row()? {
            chunk_buffer.push((format, row_bytes));

            if chunk_buffer.len() >= CHUNK_SIZE {
                write_insert_chunk(&mut writer, &quoted_name, &chunk_buffer, config.dialect)?;
                chunk_buffer.clear();
            }
        }

        // Write remaining rows
        if !chunk_buffer.is_empty() {
            write_insert_chunk(&mut writer, &quoted_name, &chunk_buffer, config.dialect)?;
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
