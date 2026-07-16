//! Shard command for extracting tenant-specific data from SQL dumps.
//!
//! The shard command extracts data belonging to a specific tenant by:
//! - Identifying tables with the tenant column (tenant roots)
//! - Following FK chains to include dependent data
//! - Including junction/pivot tables where any FK matches tenant data
//! - Optionally including global/lookup tables
//!
//! Supports MySQL, PostgreSQL, and SQLite dialects.

mod config;

pub use config::{
    DefaultShardClassifier, GlobalTableMode, ShardTableClassification, ShardYamlConfig,
};

use crate::parser::mysql_insert::{PkSet, PkValue, RowExtraction};
use crate::parser::{Parser, SqlDialect};
use crate::schema::{SchemaGraph, TableId, TableSchema};
use crate::transform_common::{
    build_schema_graph, for_each_data_row, quote_ident, split_to_temp_tables,
    write_dialect_footer, write_dialect_header, write_insert_chunk, RowFlow, RowFormat,
    RowSpillReader, RowSpillWriter, UnifiedRow,
};
use ahash::{AHashMap, AHashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Configuration for the shard command
#[derive(Debug)]
pub struct ShardConfig {
    /// Input SQL file
    pub input: PathBuf,
    /// Output SQL file (None for stdout)
    pub output: Option<PathBuf>,
    /// SQL dialect
    pub dialect: SqlDialect,
    /// Tenant column name (auto-detected if None)
    pub tenant_column: Option<String>,
    /// Tenant value to extract
    pub tenant_value: String,
    /// Explicit root tables (tables that have the tenant column)
    pub root_tables: Vec<String>,
    /// How to handle global/lookup tables
    pub include_global: GlobalTableMode,
    /// Dry run mode (show stats only)
    pub dry_run: bool,
    /// Show progress
    pub progress: bool,
    /// YAML config file path
    pub config_file: Option<PathBuf>,
    /// Maximum selected rows (memory guard)
    pub max_selected_rows: Option<usize>,
    /// Fail if any FK integrity issues detected
    pub strict_fk: bool,
    /// Include schema statements in output
    pub include_schema: bool,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            input: PathBuf::new(),
            output: None,
            dialect: SqlDialect::MySql,
            tenant_column: None,
            tenant_value: String::new(),
            root_tables: Vec::new(),
            include_global: GlobalTableMode::Lookups,
            dry_run: false,
            progress: false,
            config_file: None,
            max_selected_rows: Some(10_000_000),
            strict_fk: false,
            include_schema: true,
        }
    }
}

/// Statistics from shard operation
#[derive(Debug, Default, serde::Serialize)]
pub struct ShardStats {
    /// Number of tables processed
    pub tables_processed: usize,
    /// Number of tables skipped
    pub tables_skipped: usize,
    /// Number of tables with data included
    pub tables_with_data: usize,
    /// Total rows selected
    pub total_rows_selected: u64,
    /// Total rows seen
    pub total_rows_seen: u64,
    /// Per-table statistics
    pub table_stats: Vec<TableShardStats>,
    /// Warning messages
    pub warnings: Vec<String>,
    /// FK orphan count (rows with missing parents)
    pub fk_orphans_skipped: u64,
    /// Detected tenant column
    pub detected_tenant_column: Option<String>,
}

/// Per-table sharding statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct TableShardStats {
    pub name: String,
    pub rows_seen: u64,
    pub rows_selected: u64,
    pub classification: ShardTableClassification,
}

/// Runtime state for a table during sharding
struct TableRuntime {
    /// Table name
    name: String,
    /// Primary key set for FK membership checks
    pk_set: PkSet,
    /// Rows seen count
    rows_seen: u64,
    /// Rows selected count
    rows_selected: u64,
    /// Whether to skip this table
    skip: bool,
    /// Table classification
    classification: ShardTableClassification,
    /// FK orphans encountered
    fk_orphans: u64,
    /// Column index for tenant column (if this is a tenant root)
    tenant_column_index: Option<usize>,
    /// Path to temp file containing selected row bytes (None if no rows selected)
    selected_temp_path: Option<PathBuf>,
}

/// Run the shard command
pub fn run(config: ShardConfig) -> anyhow::Result<ShardStats> {
    let yaml_config = if let Some(ref path) = config.config_file {
        Some(ShardYamlConfig::load(path)?)
    } else {
        None
    };

    let mut stats = ShardStats::default();

    // Phase 0: Split into temp per-table files
    let split_phase = split_to_temp_tables(&config.input, config.dialect, config.progress)?;
    let temp_dir = split_phase.temp_dir;
    let tables_dir = split_phase.tables_dir;

    // Phase 1: Build schema graph
    if config.progress {
        eprintln!("Building schema graph...");
    }

    let graph = build_schema_graph(&tables_dir, config.dialect)?;

    // Detect or use configured tenant column
    let tenant_column = detect_tenant_column(&config, &yaml_config, &graph)?;
    stats.detected_tenant_column = Some(tenant_column.clone());

    if config.progress {
        eprintln!("Using tenant column: {}", tenant_column);
    }

    // Parse tenant value
    let tenant_pk_value = parse_tenant_value(&config.tenant_value);

    // Phase 2: Classify tables and build runtimes
    let (topo_order, cyclic_tables) = graph.processing_order();

    if !cyclic_tables.is_empty() {
        let names: Vec<_> = cyclic_tables
            .iter()
            .filter_map(|&id| graph.table_name(id))
            .collect();
        stats.warnings.push(format!(
            "{} tables have FK cycles (relaxed FK enforcement): {:?}",
            cyclic_tables.len(),
            names
        ));
    }

    let cyclic_set: AHashSet<TableId> = cyclic_tables.iter().copied().collect();

    // Determine tenant root tables
    let tenant_root_ids = find_tenant_root_tables(&graph, &tenant_column, &config, &yaml_config);

    // Build reachability from tenant roots
    let reachable_from_roots = compute_reachable_tables(&graph, &tenant_root_ids);

    // Initialize table runtimes with classification
    let mut runtimes: AHashMap<TableId, TableRuntime> = AHashMap::new();
    for table in graph.schema.iter() {
        let classification = classify_table(
            &table.name,
            table.id,
            &graph,
            &tenant_root_ids,
            &reachable_from_roots,
            &yaml_config,
        );

        let tenant_column_index = if classification == ShardTableClassification::TenantRoot {
            find_tenant_column_index(table, &tenant_column)
        } else {
            None
        };

        let skip = should_skip_table(&table.name, classification, &config, &yaml_config);

        runtimes.insert(
            table.id,
            TableRuntime {
                name: table.name.clone(),
                pk_set: PkSet::default(),
                rows_seen: 0,
                rows_selected: 0,
                skip,
                classification,
                fk_orphans: 0,
                tenant_column_index,
                selected_temp_path: None,
            },
        );
    }

    // Create directory for selected row temp files
    let selected_dir = temp_dir.path().join("selected");
    fs::create_dir_all(&selected_dir)?;

    // Phase 3: Process tables in dependency order
    if config.progress {
        eprintln!(
            "Processing {} tables for tenant {}...",
            topo_order.len() + cyclic_tables.len(),
            config.tenant_value
        );
    }

    let all_tables: Vec<TableId> = topo_order.into_iter().chain(cyclic_tables).collect();
    let mut total_selected: u64 = 0;

    for &table_id in &all_tables {
        let table_schema = match graph.schema.table(table_id) {
            Some(s) => s,
            None => continue,
        };

        let (should_skip, table_name, classification, tenant_col_idx) = {
            let runtime = match runtimes.get(&table_id) {
                Some(r) => r,
                None => continue,
            };
            (
                runtime.skip,
                runtime.name.clone(),
                runtime.classification,
                runtime.tenant_column_index,
            )
        };

        if should_skip {
            stats.tables_skipped += 1;
            continue;
        }

        // Handle lookup/system tables
        let include_all = match classification {
            ShardTableClassification::Lookup => match config.include_global {
                GlobalTableMode::None => {
                    stats.tables_skipped += 1;
                    continue;
                }
                GlobalTableMode::Lookups | GlobalTableMode::All => true,
            },
            ShardTableClassification::System => {
                stats.tables_skipped += 1;
                continue;
            }
            ShardTableClassification::Unknown => match config.include_global {
                GlobalTableMode::All => true,
                _ => {
                    stats.tables_skipped += 1;
                    continue;
                }
            },
            _ => false,
        };

        let table_file = tables_dir.join(format!("{}.sql", table_name));
        if !table_file.exists() {
            continue;
        }

        let temp_path = selected_dir.join(format!("{}.rows", table_name));
        let mut spill: Option<RowSpillWriter> = None;

        let mut rows_seen = 0u64;
        let mut fk_orphans = 0u64;
        let mut rows_selected = 0u64;

        for_each_data_row(
            &table_file,
            table_schema,
            config.dialect,
            RowExtraction::Full,
            |row| {
                rows_seen += 1;

                let should_include = include_all
                    || should_include_row(
                        &row,
                        table_schema,
                        classification,
                        tenant_col_idx,
                        &tenant_pk_value,
                        &runtimes,
                        &cyclic_set,
                        &table_id,
                    );

                if !should_include {
                    if classification == ShardTableClassification::TenantDependent {
                        fk_orphans += 1;
                    }
                    return Ok(RowFlow::Continue);
                }

                // Check max_selected_rows guard
                if let Some(max) = config.max_selected_rows {
                    if total_selected >= max as u64 {
                        if row.format() == RowFormat::Insert {
                            stats.warnings.push(format!(
                                "Reached max_selected_rows limit ({}) at table '{}'",
                                max, table_name
                            ));
                        }
                        return Ok(RowFlow::SkipStatement);
                    }
                }

                total_selected += 1;

                let runtime = runtimes.get_mut(&table_id).unwrap();
                if let Some(pk) = row.pk() {
                    runtime.pk_set.insert(pk.clone());
                }

                // Spill the selected row to a temp file (bounded memory)
                if spill.is_none() {
                    spill = Some(RowSpillWriter::create(&temp_path)?);
                }
                spill.as_mut().unwrap().write_row(row.format(), row.raw())?;
                rows_selected += 1;

                Ok(RowFlow::Continue)
            },
        )?;

        if let Some(spill) = spill {
            spill.finish()?;
        }

        let runtime = runtimes.get_mut(&table_id).unwrap();
        runtime.rows_seen = rows_seen;
        runtime.fk_orphans = fk_orphans;
        runtime.rows_selected = rows_selected;
        if rows_selected > 0 && temp_path.exists() {
            runtime.selected_temp_path = Some(temp_path);
        }
        stats.fk_orphans_skipped += fk_orphans;

        if rows_selected > 0 {
            stats.tables_with_data += 1;
        }

        stats.table_stats.push(TableShardStats {
            name: runtime.name.clone(),
            rows_seen: runtime.rows_seen,
            rows_selected: runtime.rows_selected,
            classification: runtime.classification,
        });
    }

    // Calculate totals
    for table_stat in &stats.table_stats {
        stats.total_rows_seen += table_stat.rows_seen;
        stats.total_rows_selected += table_stat.rows_selected;
    }
    stats.tables_processed = stats.table_stats.len();

    if config.progress {
        eprintln!("Processing complete");
    }

    // Phase 4: Output synthesis
    if config.dry_run {
        return Ok(stats);
    }

    write_output(&config, &graph, &all_tables, &runtimes, &tables_dir, &stats)?;

    Ok(stats)
}

/// Detect tenant column from config or by scanning schema
fn detect_tenant_column(
    config: &ShardConfig,
    yaml_config: &Option<ShardYamlConfig>,
    graph: &SchemaGraph,
) -> anyhow::Result<String> {
    // Check CLI option first
    if let Some(ref col) = config.tenant_column {
        return Ok(col.clone());
    }

    // Check YAML config
    if let Some(ref yaml) = yaml_config {
        if let Some(ref col) = yaml.tenant.column {
            return Ok(col.clone());
        }
    }

    // Auto-detect from schema
    for candidate in DefaultShardClassifier::TENANT_COLUMNS {
        let mut found_in_tables = 0;
        for table in graph.schema.iter() {
            if table.get_column(candidate).is_some() {
                found_in_tables += 1;
            }
        }
        if found_in_tables >= 2 {
            return Ok(candidate.to_string());
        }
    }

    anyhow::bail!(
        "Could not auto-detect tenant column. Please specify with --tenant-column. \
         Looked for: {:?}",
        DefaultShardClassifier::TENANT_COLUMNS
    )
}

/// Parse tenant value string into PkValue
fn parse_tenant_value(value: &str) -> PkValue {
    if let Ok(i) = value.parse::<i64>() {
        PkValue::Int(i)
    } else if let Ok(i) = value.parse::<i128>() {
        PkValue::BigInt(i)
    } else {
        PkValue::Text(value.into())
    }
}

/// Find tables that have the tenant column
fn find_tenant_root_tables(
    graph: &SchemaGraph,
    tenant_column: &str,
    config: &ShardConfig,
    yaml_config: &Option<ShardYamlConfig>,
) -> AHashSet<TableId> {
    let mut roots = AHashSet::new();

    // Explicit roots from config
    let explicit_roots: AHashSet<String> = config
        .root_tables
        .iter()
        .chain(
            yaml_config
                .as_ref()
                .map(|y| &y.tenant.root_tables)
                .unwrap_or(&Vec::new()),
        )
        .map(|s| s.to_lowercase())
        .collect();

    for table in graph.schema.iter() {
        let lower_name = table.name.to_lowercase();

        if explicit_roots.contains(&lower_name) || table.get_column(tenant_column).is_some() {
            roots.insert(table.id);
        }
    }

    roots
}

/// Compute tables reachable from tenant roots via FK relationships
fn compute_reachable_tables(
    graph: &SchemaGraph,
    tenant_roots: &AHashSet<TableId>,
) -> AHashSet<TableId> {
    let mut reachable = tenant_roots.clone();
    let mut queue: Vec<TableId> = tenant_roots.iter().copied().collect();

    while let Some(table_id) = queue.pop() {
        for &child_id in &graph.children[table_id.0 as usize] {
            if !reachable.contains(&child_id) {
                reachable.insert(child_id);
                queue.push(child_id);
            }
        }
    }

    reachable
}

/// Classify a table for sharding
fn classify_table(
    table_name: &str,
    table_id: TableId,
    graph: &SchemaGraph,
    tenant_roots: &AHashSet<TableId>,
    reachable: &AHashSet<TableId>,
    yaml_config: &Option<ShardYamlConfig>,
) -> ShardTableClassification {
    // Check YAML override first
    if let Some(ref yaml) = yaml_config {
        if let Some(class) = yaml.get_classification(table_name) {
            return class;
        }
    }

    // Check if it's a tenant root
    if tenant_roots.contains(&table_id) {
        return ShardTableClassification::TenantRoot;
    }

    // Check if reachable from tenant roots (dependent)
    if reachable.contains(&table_id) {
        // Check if it might be a junction table
        if is_junction_table(table_name, table_id, graph) {
            return ShardTableClassification::Junction;
        }
        return ShardTableClassification::TenantDependent;
    }

    // Check system patterns
    if DefaultShardClassifier::is_system_table(table_name) {
        return ShardTableClassification::System;
    }

    // Check lookup patterns
    if DefaultShardClassifier::is_lookup_table(table_name) {
        return ShardTableClassification::Lookup;
    }

    ShardTableClassification::Unknown
}

/// Check if a table is a junction table
fn is_junction_table(table_name: &str, table_id: TableId, graph: &SchemaGraph) -> bool {
    // Name-based heuristic
    if DefaultShardClassifier::is_junction_table_by_name(table_name) {
        return true;
    }

    // Structure-based: table with multiple FKs and few/no other columns
    if let Some(table) = graph.schema.table(table_id) {
        let fk_count = table.foreign_keys.len();
        let fk_col_count: usize = table.foreign_keys.iter().map(|fk| fk.columns.len()).sum();
        let total_cols = table.columns.len();

        // Junction tables typically have mostly FK columns
        if fk_count >= 2 && fk_col_count >= total_cols.saturating_sub(2) {
            return true;
        }
    }

    false
}

/// Find the index of the tenant column in a table
fn find_tenant_column_index(table: &TableSchema, tenant_column: &str) -> Option<usize> {
    table
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(tenant_column))
}

/// Determine if a table should be skipped
fn should_skip_table(
    table_name: &str,
    classification: ShardTableClassification,
    config: &ShardConfig,
    yaml_config: &Option<ShardYamlConfig>,
) -> bool {
    // Check YAML skip override
    if let Some(ref yaml) = yaml_config {
        if yaml.should_skip(table_name) {
            return true;
        }
    }

    // System tables always skipped
    if classification == ShardTableClassification::System {
        return true;
    }

    // Lookup tables depend on config
    if classification == ShardTableClassification::Lookup {
        return config.include_global == GlobalTableMode::None;
    }

    false
}

/// Check if a row should be included in the shard
#[allow(clippy::too_many_arguments)]
fn should_include_row(
    row: &UnifiedRow,
    table_schema: &TableSchema,
    classification: ShardTableClassification,
    tenant_column_index: Option<usize>,
    tenant_value: &PkValue,
    runtimes: &AHashMap<TableId, TableRuntime>,
    cyclic_set: &AHashSet<TableId>,
    current_table_id: &TableId,
) -> bool {
    match classification {
        ShardTableClassification::TenantRoot => {
            // Check tenant column value using column_map for correct mapping
            if let Some(idx) = tenant_column_index {
                if let Some(val) = row.get_column_value(idx) {
                    return val == tenant_value;
                }
            }
            false
        }
        ShardTableClassification::TenantDependent => {
            // Check if any FK points to a selected row
            for (fk_ref, fk_tuple) in row.fk_values() {
                if let Some(fk) = table_schema.foreign_keys.get(fk_ref.fk_index as usize) {
                    if let Some(parent_id) = fk.referenced_table_id {
                        // Skip FK check for cyclic tables
                        if cyclic_set.contains(&parent_id) && cyclic_set.contains(current_table_id)
                        {
                            continue;
                        }

                        if let Some(parent_runtime) = runtimes.get(&parent_id) {
                            if parent_runtime.pk_set.contains(fk_tuple) {
                                return true;
                            }
                        }
                    }
                }
            }
            false
        }
        ShardTableClassification::Junction => {
            // Include if ANY FK points to a selected row
            for (fk_ref, fk_tuple) in row.fk_values() {
                if let Some(fk) = table_schema.foreign_keys.get(fk_ref.fk_index as usize) {
                    if let Some(parent_id) = fk.referenced_table_id {
                        if let Some(parent_runtime) = runtimes.get(&parent_id) {
                            if parent_runtime.pk_set.contains(fk_tuple) {
                                return true;
                            }
                        }
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Write the sharded output
fn write_output(
    config: &ShardConfig,
    _graph: &SchemaGraph,
    table_order: &[TableId],
    runtimes: &AHashMap<TableId, TableRuntime>,
    tables_dir: &Path,
    stats: &ShardStats,
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
    config: &ShardConfig,
    stats: &ShardStats,
) -> std::io::Result<()> {
    writeln!(writer, "-- Sharded from: {}", config.input.display())?;
    writeln!(
        writer,
        "-- Date: {}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    )?;
    if let Some(ref col) = stats.detected_tenant_column {
        writeln!(writer, "-- Tenant column: {}", col)?;
    }
    writeln!(writer, "-- Tenant value: {}", config.tenant_value)?;
    writeln!(writer, "-- Dialect: {}", config.dialect)?;
    writeln!(writer, "--")?;
    writeln!(writer, "-- Statistics:")?;
    writeln!(writer, "--   Tables processed: {}", stats.tables_processed)?;
    writeln!(writer, "--   Tables with data: {}", stats.tables_with_data)?;
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

    if stats.fk_orphans_skipped > 0 {
        writeln!(
            writer,
            "--   FK orphans skipped: {}",
            stats.fk_orphans_skipped
        )?;
    }

    if !stats.warnings.is_empty() {
        writeln!(writer, "--   Warnings: {}", stats.warnings.len())?;
    }

    writeln!(writer)?;

    Ok(())
}

