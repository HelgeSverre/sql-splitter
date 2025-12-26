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

use crate::parser::mysql_insert::{parse_mysql_insert_rows, ParsedRow, PkSet, PkTuple, PkValue};
use crate::parser::postgres_copy::{parse_copy_columns, parse_postgres_copy_rows, ParsedCopyRow};
use crate::parser::{ContentFilter, Parser, SqlDialect, StatementType};
use crate::schema::{SchemaBuilder, SchemaGraph, TableId, TableSchema};
use crate::splitter::Splitter;
use ahash::{AHashMap, AHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

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
    /// Selected rows (raw INSERT format)
    selected_rows: Vec<SelectedRow>,
    /// Primary key set for FK membership checks
    pk_set: PkSet,
    /// Rows seen count
    rows_seen: u64,
    /// Whether to skip this table
    skip: bool,
    /// Table classification
    classification: ShardTableClassification,
    /// FK orphans encountered
    fk_orphans: u64,
    /// Column index for tenant column (if this is a tenant root)
    tenant_column_index: Option<usize>,
}

/// Row format indicator
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

/// Combined row representation for both MySQL INSERT and PostgreSQL COPY
enum UnifiedRow {
    Insert(ParsedRow),
    Copy(ParsedCopyRow),
}

impl UnifiedRow {
    fn pk(&self) -> Option<&PkTuple> {
        match self {
            UnifiedRow::Insert(r) => r.pk.as_ref(),
            UnifiedRow::Copy(r) => r.pk.as_ref(),
        }
    }

    fn fk_values(&self) -> &[(crate::parser::mysql_insert::FkRef, PkTuple)] {
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

/// Run the shard command
pub fn run(config: ShardConfig) -> anyhow::Result<ShardStats> {
    let yaml_config = if let Some(ref path) = config.config_file {
        Some(ShardYamlConfig::load(path)?)
    } else {
        None
    };

    let mut stats = ShardStats::default();

    // Get file size for progress tracking
    let file_size = std::fs::metadata(&config.input)?.len();

    // Progress bar setup - byte-based for the split phase
    let progress_bar = if config.progress {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("Splitting dump...");
        Some(pb)
    } else {
        None
    };

    // Phase 0: Split into temp per-table files
    let temp_dir = TempDir::new()?;
    let tables_dir = temp_dir.path().join("tables");

    let mut splitter = Splitter::new(config.input.clone(), tables_dir.clone())
        .with_dialect(config.dialect)
        .with_content_filter(ContentFilter::All);

    if let Some(ref pb) = progress_bar {
        let pb_clone = pb.clone();
        splitter = splitter.with_progress(move |bytes| {
            pb_clone.set_position(bytes);
        });
    }

    let split_stats = splitter.split()?;

    // Finish byte-based progress, switch to milestone messages
    if let Some(ref pb) = progress_bar {
        pb.finish_and_clear();
    }

    if config.progress {
        eprintln!(
            "Split complete: {} tables, {} statements",
            split_stats.tables_found, split_stats.statements_processed
        );
    }

    // Phase 1: Build schema graph
    if config.progress {
        eprintln!("Building schema graph...");
    }

    let graph = build_schema_graph(&tables_dir, &config)?;

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
                selected_rows: Vec::new(),
                pk_set: PkSet::default(),
                rows_seen: 0,
                skip,
                classification,
                fk_orphans: 0,
                tenant_column_index,
            },
        );
    }

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

        let file = File::open(&table_file)?;
        let mut parser = Parser::with_dialect(file, 64 * 1024, config.dialect);

        let mut rows_seen = 0u64;
        let mut fk_orphans = 0u64;
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

                        let should_include = if include_all {
                            true
                        } else {
                            should_include_row(
                                &unified,
                                table_schema,
                                classification,
                                tenant_col_idx,
                                &tenant_pk_value,
                                &runtimes,
                                &cyclic_set,
                                &table_id,
                            )
                        };

                        if !should_include {
                            if classification == ShardTableClassification::TenantDependent {
                                fk_orphans += 1;
                            }
                            continue;
                        }

                        // Check max_selected_rows guard
                        if let Some(max) = config.max_selected_rows {
                            if total_selected >= max as u64 {
                                stats.warnings.push(format!(
                                    "Reached max_selected_rows limit ({}) at table '{}'",
                                    max, table_name
                                ));
                                break;
                            }
                        }

                        total_selected += 1;

                        let runtime = runtimes.get_mut(&table_id).unwrap();
                        if let Some(pk) = unified.pk() {
                            runtime.pk_set.insert(pk.clone());
                        }
                        runtime.selected_rows.push(unified.into_selected());
                    }
                }
                StatementType::Copy => {
                    let header = String::from_utf8_lossy(&stmt);
                    copy_columns = parse_copy_columns(&header);
                }
                StatementType::Unknown if config.dialect == SqlDialect::Postgres => {
                    if stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n") {
                        let rows =
                            parse_postgres_copy_rows(&stmt, table_schema, copy_columns.clone())?;

                        for row in rows {
                            rows_seen += 1;
                            let unified = UnifiedRow::Copy(row);

                            let should_include = if include_all {
                                true
                            } else {
                                should_include_row(
                                    &unified,
                                    table_schema,
                                    classification,
                                    tenant_col_idx,
                                    &tenant_pk_value,
                                    &runtimes,
                                    &cyclic_set,
                                    &table_id,
                                )
                            };

                            if !should_include {
                                if classification == ShardTableClassification::TenantDependent {
                                    fk_orphans += 1;
                                }
                                continue;
                            }

                            if let Some(max) = config.max_selected_rows {
                                if total_selected >= max as u64 {
                                    break;
                                }
                            }

                            total_selected += 1;

                            let runtime = runtimes.get_mut(&table_id).unwrap();
                            if let Some(pk) = unified.pk() {
                                runtime.pk_set.insert(pk.clone());
                            }
                            runtime.selected_rows.push(unified.into_selected());
                        }
                    }
                }
                _ => {}
            }
        }

        let runtime = runtimes.get_mut(&table_id).unwrap();
        runtime.rows_seen = rows_seen;
        runtime.fk_orphans = fk_orphans;
        stats.fk_orphans_skipped += fk_orphans;

        if !runtime.selected_rows.is_empty() {
            stats.tables_with_data += 1;
        }

        stats.table_stats.push(TableShardStats {
            name: runtime.name.clone(),
            rows_seen: runtime.rows_seen,
            rows_selected: runtime.selected_rows.len() as u64,
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

/// Build schema graph from split table files
fn build_schema_graph(tables_dir: &Path, config: &ShardConfig) -> anyhow::Result<SchemaGraph> {
    let mut builder = SchemaBuilder::new();

    for entry in fs::read_dir(tables_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|e| e == "sql") {
            let file = File::open(&path)?;
            let mut parser = Parser::with_dialect(file, 64 * 1024, config.dialect);

            while let Some(stmt) = parser.read_statement()? {
                let (stmt_type, _) =
                    Parser::<&[u8]>::parse_statement_with_dialect(&stmt, config.dialect);

                match stmt_type {
                    StatementType::CreateTable => {
                        let stmt_str = String::from_utf8_lossy(&stmt);
                        builder.parse_create_table(&stmt_str);
                    }
                    StatementType::AlterTable => {
                        let stmt_str = String::from_utf8_lossy(&stmt);
                        builder.parse_alter_table(&stmt_str);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(SchemaGraph::from_schema(builder.build()))
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
            // Check tenant column value
            if let Some(idx) = tenant_column_index {
                match row {
                    UnifiedRow::Insert(r) => {
                        if let Some(val) = extract_column_value(&r.raw, idx) {
                            return &val == tenant_value;
                        }
                    }
                    UnifiedRow::Copy(r) => {
                        if let Some(val) = extract_copy_column_value(&r.raw, idx) {
                            return &val == tenant_value;
                        }
                    }
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

/// Extract a column value from INSERT row bytes by index
fn extract_column_value(raw: &[u8], column_index: usize) -> Option<PkValue> {
    let mut values = Vec::new();
    let mut current_start = 0;
    let mut in_string = false;
    let mut escape_next = false;
    let mut paren_depth = 0;

    // Skip leading (
    let start = raw.iter().position(|&b| b == b'(')?;
    let raw = &raw[start + 1..];

    for (i, &b) in raw.iter().enumerate() {
        if escape_next {
            escape_next = false;
            continue;
        }

        if b == b'\\' && in_string {
            escape_next = true;
            continue;
        }

        if b == b'\'' && !escape_next {
            in_string = !in_string;
            continue;
        }

        if in_string {
            continue;
        }

        match b {
            b'(' => paren_depth += 1,
            b')' => {
                if paren_depth == 0 {
                    values.push(&raw[current_start..i]);
                    break;
                }
                paren_depth -= 1;
            }
            b',' if paren_depth == 0 => {
                values.push(&raw[current_start..i]);
                current_start = i + 1;
            }
            _ => {}
        }
    }

    if column_index >= values.len() {
        return None;
    }

    parse_value_bytes(values[column_index])
}

/// Extract a column value from COPY row bytes by index
fn extract_copy_column_value(raw: &[u8], column_index: usize) -> Option<PkValue> {
    let fields: Vec<&[u8]> = raw.split(|&b| b == b'\t').collect();
    if column_index >= fields.len() {
        return None;
    }

    let field = fields[column_index];
    if field == b"\\N" {
        return Some(PkValue::Null);
    }

    parse_value_bytes(field)
}

/// Parse a byte slice into a PkValue
fn parse_value_bytes(bytes: &[u8]) -> Option<PkValue> {
    let trimmed = bytes
        .iter()
        .skip_while(|&&b| b == b' ')
        .take_while(|&&b| b != b' ')
        .copied()
        .collect::<Vec<_>>();

    if trimmed.is_empty() {
        return None;
    }

    // Check for NULL
    if trimmed.eq_ignore_ascii_case(b"null") {
        return Some(PkValue::Null);
    }

    // Remove quotes for strings
    let unquoted = if trimmed.first() == Some(&b'\'') && trimmed.last() == Some(&b'\'') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        &trimmed[..]
    };

    // Try parsing as number
    if let Ok(s) = std::str::from_utf8(unquoted) {
        if let Ok(i) = s.parse::<i64>() {
            return Some(PkValue::Int(i));
        }
        if let Ok(i) = s.parse::<i128>() {
            return Some(PkValue::BigInt(i));
        }
        return Some(PkValue::Text(s.into()));
    }

    None
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
                Some(r) if !r.skip && !r.selected_rows.is_empty() => r,
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

    // Write data for each table
    for &table_id in table_order {
        let runtime = match runtimes.get(&table_id) {
            Some(r) if !r.skip && !r.selected_rows.is_empty() => r,
            _ => continue,
        };

        let table_name = &runtime.name;
        let row_count = runtime.selected_rows.len();

        writeln!(writer, "\n-- Data: {} ({} rows)", table_name, row_count)?;

        const CHUNK_SIZE: usize = 1000;

        let quoted_name = match config.dialect {
            SqlDialect::MySql => format!("`{}`", table_name),
            SqlDialect::Postgres | SqlDialect::Sqlite => format!("\"{}\"", table_name),
            SqlDialect::Mssql => format!("[{}]", table_name),
        };

        for chunk in runtime.selected_rows.chunks(CHUNK_SIZE) {
            writeln!(writer, "INSERT INTO {} VALUES", quoted_name)?;

            for (i, row) in chunk.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b",\n")?;
                }

                let values = match row.format {
                    RowFormat::Insert => match config.dialect {
                        SqlDialect::Postgres => convert_row_to_postgres(&row.raw),
                        _ => row.raw.clone(),
                    },
                    RowFormat::Copy => convert_copy_to_insert_values(&row.raw, config.dialect),
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
        SqlDialect::Mssql => {
            writeln!(writer, "SET ANSI_NULLS ON;")?;
            writeln!(writer, "SET QUOTED_IDENTIFIER ON;")?;
            writeln!(writer, "SET NOCOUNT ON;")?;
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
        SqlDialect::Mssql => {
            // No footer needed
        }
    }
    Ok(())
}

/// Convert a MySQL-style row to PostgreSQL syntax
fn convert_row_to_postgres(row: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(row.len());
    let mut i = 0;

    while i < row.len() {
        if row[i] == b'\\' && i + 1 < row.len() && row[i + 1] == b'\'' {
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

/// Convert PostgreSQL COPY format to INSERT VALUES format
fn convert_copy_to_insert_values(row: &[u8], dialect: SqlDialect) -> Vec<u8> {
    let mut result = Vec::with_capacity(row.len() + 20);
    result.push(b'(');

    let fields: Vec<&[u8]> = row.split(|&b| b == b'\t').collect();

    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            result.extend_from_slice(b", ");
        }

        if *field == b"\\N" {
            result.extend_from_slice(b"NULL");
        } else if field.is_empty() {
            result.extend_from_slice(b"''");
        } else if is_numeric(field) {
            result.extend_from_slice(field);
        } else {
            result.push(b'\'');
            for &b in *field {
                match b {
                    b'\'' => match dialect {
                        SqlDialect::MySql => result.extend_from_slice(b"\\'"),
                        SqlDialect::Postgres | SqlDialect::Sqlite | SqlDialect::Mssql => {
                            result.extend_from_slice(b"''")
                        }
                    },
                    b'\\' if dialect == SqlDialect::MySql => {
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

    if s[0] == b'-' || s[0] == b'+' {
        start = 1;
    }

    for &b in &s[start..] {
        match b {
            b'0'..=b'9' => has_digit = true,
            b'.' if !has_dot => has_dot = true,
            b'e' | b'E' => continue,
            _ => return false,
        }
    }

    has_digit
}
