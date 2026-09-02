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
use crate::parser::SqlDialect;
use crate::schema::{SchemaGraph, TableId};
use crate::transform_common::{
    build_schema_graph, for_each_data_row, split_to_temp_tables, write_header_totals,
    write_transform_output, OutputTable, RowFlow, RowSpillReader, RowSpillWriter, UnifiedRow,
};
use ahash::AHashMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::fs;
use std::io::Write;
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

    // Split input into per-table files
    let split_phase = split_to_temp_tables(&config.input, config.dialect, config.progress)?;
    let temp_dir = split_phase.temp_dir;
    let tables_dir = split_phase.tables_dir;

    // Build the schema graph
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

    // Process tables in dependency order
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

        let remaining_capacity = config.max_total_rows.map(|max| {
            usize::try_from((max as u64).saturating_sub(total_selected)).unwrap_or(usize::MAX)
        });
        if remaining_capacity == Some(0) {
            stats.warnings.push(format!(
                "Warning: Reached max_total_rows limit ({}) at table '{}'",
                config.max_total_rows.unwrap_or_default(),
                table_name
            ));
            break;
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
            remaining_capacity,
        )?;

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

        if let Some(max) = config.max_total_rows {
            if total_selected >= max as u64 {
                stats.warnings.push(format!(
                    "Warning: Reached max_total_rows limit ({}) at table '{}'",
                    max, table_name
                ));
                break;
            }
        }
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

    // Synthesize the output
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
    if explicit_roots
        .iter()
        .any(|pattern| table_name_matches(pattern, name))
    {
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
    // Check exclude list
    if config
        .exclude
        .iter()
        .any(|pattern| table_name_matches(pattern, name))
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
        if !filter
            .iter()
            .any(|pattern| table_name_matches(pattern, name))
        {
            return true;
        }
    }

    // Skip system tables by default
    if classification == TableClassification::System {
        return true;
    }

    false
}

/// Match a configured table name against a parsed name.
///
/// Qualified dump names are preserved internally. Existing configuration that
/// uses an unqualified table name still matches the final name component;
/// qualified configuration remains an exact match and can disambiguate
/// schemas.
fn table_name_matches(pattern: &str, table_name: &str) -> bool {
    pattern.eq_ignore_ascii_case(table_name)
        || (!pattern.contains('.')
            && final_identifier_component(table_name)
                .is_some_and(|bare_name| pattern.eq_ignore_ascii_case(bare_name)))
}

/// Return the final identifier component, without treating dots inside quoted
/// components as qualification separators.
fn final_identifier_component(name: &str) -> Option<&str> {
    let bytes = name.as_bytes();
    let mut quote = None;
    let mut final_start = 0;
    let mut index = 0;

    while index < bytes.len() {
        match quote {
            Some(close) if bytes[index] == close => {
                if bytes.get(index + 1) == Some(&close) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            Some(_) => {}
            None => match bytes[index] {
                b'`' | b'"' => quote = Some(bytes[index]),
                b'[' => quote = Some(b']'),
                b'.' => final_start = index + 1,
                _ => {}
            },
        }
        index += 1;
    }

    (final_start > 0).then(|| &name[final_start..])
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
    remaining_capacity: Option<usize>,
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
                    if rng.random::<f64>() < prob
                        && remaining_capacity.is_none_or(|max| rows_selected < max as u64)
                    {
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
            let capacity = remaining_capacity.map_or(n, |max| n.min(max));
            let mut reservoir: Reservoir<(u64, Option<u64>)> =
                Reservoir::new(capacity, StdRng::from_rng(&mut *rng));

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

    if config.preserve_relations && has_self_referential_fk(table_schema, table_id) {
        extend_self_reference_closure(
            table_file,
            table_schema,
            table_id,
            config,
            selected_dir,
            &temp_path,
            &mut selected_pk_hashes,
            &mut rows_selected,
            remaining_capacity,
        )?;
    }

    Ok(StreamingSampleResult {
        rows_seen,
        rows_selected,
        fk_orphans,
        pk_hashes: selected_pk_hashes,
    })
}

/// Return true when a table has an FK that references the table itself.
fn has_self_referential_fk(table_schema: &crate::schema::TableSchema, table_id: TableId) -> bool {
    table_schema
        .foreign_keys
        .iter()
        .any(|fk| fk.referenced_table_id == Some(table_id))
}

/// Add selected ancestors for self-referencing FKs.
///
/// Self references cannot use the normal parent-table PK set because the
/// table is still being sampled. The initial sample is retained, then each
/// pass adds one level of missing selected ancestors to its spill file. This
/// keeps only selected PK hashes in memory and works when parents appear
/// after children in the dump.
#[allow(clippy::too_many_arguments)]
fn extend_self_reference_closure(
    table_file: &Path,
    table_schema: &crate::schema::TableSchema,
    table_id: TableId,
    config: &SampleConfig,
    selected_dir: &Path,
    temp_path: &Path,
    selected_pk_hashes: &mut Vec<u64>,
    rows_selected: &mut u64,
    remaining_capacity: Option<usize>,
) -> anyhow::Result<()> {
    if selected_pk_hashes.is_empty() {
        return Ok(());
    }

    let mut included: PkHashSet = selected_pk_hashes.iter().copied().collect();
    let mut required_parents = PkHashSet::default();

    // Find direct parents for the initially selected rows.
    for_each_data_row(
        table_file,
        table_schema,
        config.dialect,
        RowExtraction::PkFk,
        |row| {
            if row
                .pk()
                .is_some_and(|pk| included.contains(&hash_pk_tuple(pk)))
            {
                add_self_referenced_parent_hashes(
                    &row,
                    table_schema,
                    table_id,
                    &mut required_parents,
                );
            }
            Ok(RowFlow::Continue)
        },
    )?;

    let closure_path = temp_path.to_path_buf();
    let mut pass = 0usize;

    loop {
        let pending: PkHashSet = required_parents
            .iter()
            .filter(|hash| !included.contains(hash))
            .copied()
            .collect();
        if pending.is_empty() {
            break;
        }

        let next_path = selected_dir.join(format!("{}.closure-{}.rows", table_schema.name, pass));
        let mut writer = RowSpillWriter::create(&next_path)?;
        copy_spill_rows(&closure_path, &mut writer)?;
        let mut added = 0usize;

        for_each_data_row(
            table_file,
            table_schema,
            config.dialect,
            RowExtraction::PkFk,
            |row| {
                let Some(pk) = row.pk() else {
                    return Ok(RowFlow::Continue);
                };
                let pk_hash = hash_pk_tuple(pk);
                if !pending.contains(&pk_hash) {
                    return Ok(RowFlow::Continue);
                }
                if remaining_capacity.is_some_and(|max| *rows_selected >= max as u64) {
                    return Ok(RowFlow::SkipStatement);
                }

                writer.write_row(row.format(), row.raw())?;
                included.insert(pk_hash);
                selected_pk_hashes.push(pk_hash);
                *rows_selected += 1;
                added += 1;
                add_self_referenced_parent_hashes(
                    &row,
                    table_schema,
                    table_id,
                    &mut required_parents,
                );
                Ok(RowFlow::Continue)
            },
        )?;

        writer.finish()?;
        // `rename` replaces an existing destination on Unix but not Windows.
        // This is an internal spill file, so remove the previous generation
        // first and keep the closure update portable.
        fs::remove_file(&closure_path)?;
        fs::rename(&next_path, &closure_path)?;

        if added == 0 {
            if config.strict_fk {
                anyhow::bail!(
                    "FK integrity violation in table '{}': selected row references missing parent",
                    table_schema.name
                );
            }
            break;
        }
        pass += 1;
    }

    Ok(())
}

/// Copy a spill file into an open spill writer without materializing all rows.
fn copy_spill_rows(path: &Path, writer: &mut RowSpillWriter) -> anyhow::Result<()> {
    let mut reader = RowSpillReader::open(path)?;
    while let Some((format, raw)) = reader.next_row()? {
        writer.write_row(format, &raw)?;
    }
    Ok(())
}

/// Add the parent PKs referenced by this table's self-referencing FKs.
fn add_self_referenced_parent_hashes(
    row: &UnifiedRow,
    table_schema: &crate::schema::TableSchema,
    table_id: TableId,
    parent_hashes: &mut PkHashSet,
) {
    for (fk_ref, fk_tuple) in row.fk_values() {
        if table_schema
            .foreign_keys
            .get(fk_ref.fk_index as usize)
            .is_some_and(|fk| fk.referenced_table_id == Some(table_id))
        {
            parent_hashes.insert(hash_pk_tuple(fk_tuple));
        }
    }
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
                // A self-referencing table is handled by
                // `extend_self_reference_closure` after initial sampling.
                if parent_id == *current_table_id {
                    continue;
                }
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

/// Write the output file
fn write_output(
    config: &SampleConfig,
    _graph: &SchemaGraph,
    table_order: &[TableId],
    runtimes: &AHashMap<TableId, TableRuntime>,
    tables_dir: &Path,
    stats: &SampleStats,
) -> anyhow::Result<()> {
    let tables: Vec<OutputTable<'_>> = table_order
        .iter()
        .filter_map(|id| runtimes.get(id))
        .filter(|r| !r.skip && r.rows_selected > 0)
        .map(|r| OutputTable {
            name: &r.name,
            rows_selected: r.rows_selected,
            spill_path: r.selected_temp_path.as_deref(),
        })
        .collect();

    write_transform_output(
        config.output.as_deref(),
        config.dialect,
        config.include_schema,
        &tables,
        tables_dir,
        |w| write_header(w, config, stats),
    )
}

/// Write header comment
fn write_header<W: Write + ?Sized>(
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

    write_header_totals(
        writer,
        stats.total_rows_selected,
        stats.total_rows_seen,
        "rejected",
        stats.fk_orphans_rejected,
        &stats.warnings,
    )
}
