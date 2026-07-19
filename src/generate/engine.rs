//! The [`GenerationEngine`]: streams rows from a compiled [`GenerationPlan`].
//!
//! The engine walks the plan's tables in dependency order (parents before
//! children) and, for each table, produces every row by executing its column
//! owners, then hands the row to a [`RowSink`]. It owns one reusable row buffer
//! per active table so the hot loop never reallocates the row vector.
//!
//! # Foreign keys without spooling
//!
//! A foreign-key column cannot be produced from a per-row seed alone: choosing a
//! parent row needs the parent's *key domain* and row count, cross-table state
//! only the engine has. The engine therefore drives every FK column from its
//! [`CompiledRelationship`], regardless of whether the compiler recorded the
//! column as [`ColumnOwner::Relationship`] (an inferred FK) or
//! [`ColumnOwner::Generator`] with kind `relation.foreign_key` (an explicit
//! rule). Both converge here: the compiled generator marker is never executed;
//! its `distribution` / `null_rate` arguments were folded into the relationship
//! at compile time.
//!
//! # Reproducible key domains
//!
//! A parent key referenced by a child is materialized as a [`KeyDomain`]. A bare
//! integer primary key (the database sequence a child references) compiles to a
//! [`KeyDomain::DenseInteger`] over `1..=count`: the parent renders that id and
//! the child regenerates parent row `n`'s key in closed form, without storing
//! every key. Stateful or non-random-access parent keys are not materializable
//! yet and report `GEN-KEY-DOMAIN-UNSUPPORTED` (protected key spooling is a
//! later task).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use rand::{Rng, RngExt};
use rand_chacha::ChaCha8Rng;

use super::output::{FamilyBudget, FamilyBuffer, SpillKind, SpooledRow, TempConfig};
use super::plan::{
    ColumnOwner, CompiledRelationship, GenerationPlan, PlannedTable, RelationshipDistribution,
    ResolvedTableSeed,
};
use super::registry::{KeyRecipe, RowContext, RowView};
use super::seed::{SeedRoot, StreamId};
use super::value::{GenerateError, GeneratedValue};

/// A single generated row, handed to a [`RowSink`].
///
/// `values` aligns positionally with the table's `columns`. The engine reuses
/// one allocation across rows, moving the vector in and out of each
/// `GeneratedRow`, so a sink that needs to retain values must clone them.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedRow {
    /// Index of the owning table within the plan's `tables`.
    pub table_index: usize,
    /// Zero-based index of this row within its table.
    pub row_index: u64,
    /// Column values, positionally aligned with the table's columns.
    pub values: Vec<GeneratedValue>,
}

/// A summary of a completed engine run: rows written across every table.
///
/// This is the low-level engine-only report; the public
/// [`crate::generate::Generate`] API wraps it in a richer
/// [`crate::generate::GenerateReport`] that also carries diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EngineReport {
    /// Total rows written across every table.
    pub rows_written: u64,
}

/// A destination for generated rows: notified at each table boundary and given
/// every row in order.
pub trait RowSink {
    /// Called once before any of `table`'s rows are written.
    fn begin_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError>;

    /// Called once per generated row of `table`, in row order.
    fn write_row(&mut self, table: &PlannedTable, row: &GeneratedRow) -> Result<(), GenerateError>;

    /// Called once after all of `table`'s rows are written.
    fn end_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError>;
}

/// A random-access key generator: reproduces the key of parent row `row_index`
/// in closed form, without materializing the intervening rows. Used as a
/// component of a composite [`KeyDomain`].
pub trait RandomAccessKeyGenerator: Send {
    /// The key value at parent row `row_index`.
    fn key_at(&self, row_index: u64) -> GeneratedValue;
}

/// A dense integer key: `start + step * row_index`. The closed form a bare
/// integer primary key (a database sequence) reduces to.
#[derive(Debug, Clone, Copy)]
pub struct DenseIntegerKey {
    /// The key of parent row 0.
    pub start: i128,
    /// The per-row increment.
    pub step: i128,
}

impl RandomAccessKeyGenerator for DenseIntegerKey {
    fn key_at(&self, row_index: u64) -> GeneratedValue {
        GeneratedValue::Integer(self.start + self.step * row_index as i128)
    }
}

/// A per-row-reseeded random-access UUID key (RFC 4122 v4). Row `n`'s key
/// derives from the parent table's seed and a `primary_key.row.<n>` stream, so
/// any parent row's UUID is reproducible by index without storing every key.
pub struct SeededUuidKey {
    /// The parent table's resolved seed root.
    pub seed: SeedRoot,
    /// The parent table name (stream identity).
    pub table: String,
    /// The parent key column name (stream identity).
    pub column: String,
}

impl RandomAccessKeyGenerator for SeededUuidKey {
    fn key_at(&self, row_index: u64) -> GeneratedValue {
        seeded_uuid(self.seed, &self.table, &self.column, row_index)
    }
}

/// Derive the RFC 4122 v4 UUID for parent row `row_index` from a fresh,
/// row-indexed stream. Shared by [`SeededUuidKey`] and the parent's own key
/// rendering so a parent PK and the child's reference agree exactly.
fn seeded_uuid(seed: SeedRoot, table: &str, column: &str, row_index: u64) -> GeneratedValue {
    let mut rng = seed.stream(StreamId::operator(
        table,
        column,
        format!("primary_key.row.{row_index}"),
    ));
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    let hex = hex::encode(bytes);
    GeneratedValue::Text(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// A referenced parent key column: the generator's context-free [`KeyRecipe`]
/// shape plus the parent table/column/seed context the engine supplies, and the
/// row count. Renders the parent's own key and reconstructs a random-access
/// generator for child selection — both from the one shared derivation, so a
/// parent PK and the child's reference agree by construction.
struct ParentKey {
    shape: KeyRecipe,
    seed: SeedRoot,
    table: String,
    column: String,
    count: u64,
}

impl ParentKey {
    /// The key value at parent `row_index` (used to render the parent's own PK).
    fn key_at(&self, row_index: u64) -> GeneratedValue {
        match self.shape {
            KeyRecipe::Dense { start, step } => {
                GeneratedValue::Integer(start + step * row_index as i128)
            }
            KeyRecipe::Uuid => seeded_uuid(self.seed, &self.table, &self.column, row_index),
        }
    }

    /// A fresh random-access generator realizing this key (for child selection).
    fn generator(&self) -> Box<dyn RandomAccessKeyGenerator> {
        match self.shape {
            KeyRecipe::Dense { start, step } => Box::new(DenseIntegerKey { start, step }),
            KeyRecipe::Uuid => Box::new(SeededUuidKey {
                seed: self.seed,
                table: self.table.clone(),
                column: self.column.clone(),
            }),
        }
    }
}

/// A reproducible domain of parent keys a child references, addressable by
/// parent row index without storing every key.
pub enum KeyDomain {
    /// A contiguous integer sequence `start, start+step, …`, `count` rows long.
    DenseInteger {
        /// The key of parent row 0.
        start: i128,
        /// The per-row increment.
        step: i128,
        /// The number of parent rows.
        count: u64,
    },
    /// A single random-access key column (e.g. a seeded UUID/semantic key).
    Deterministic {
        /// The number of parent rows.
        count: u64,
        /// Reproduces any parent row's key on demand.
        generator: Box<dyn RandomAccessKeyGenerator>,
    },
    /// A composite key: one parent row index yields every component. Selection
    /// chooses the index once; each component is derived from that same index.
    Composite {
        /// The number of parent rows.
        count: u64,
        /// One generator per key component, in parent-column order.
        components: Vec<Box<dyn RandomAccessKeyGenerator>>,
    },
    /// A key domain whose value may be `NULL` (a nullable referenced key).
    Nullable(Box<KeyDomain>),
}

impl fmt::Debug for KeyDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyDomain::DenseInteger { start, step, count } => f
                .debug_struct("DenseInteger")
                .field("start", start)
                .field("step", step)
                .field("count", count)
                .finish(),
            KeyDomain::Deterministic { count, .. } => f
                .debug_struct("Deterministic")
                .field("count", count)
                .finish_non_exhaustive(),
            KeyDomain::Composite { count, components } => f
                .debug_struct("Composite")
                .field("count", count)
                .field("components", &components.len())
                .finish(),
            KeyDomain::Nullable(inner) => f.debug_tuple("Nullable").field(inner).finish(),
        }
    }
}

impl KeyDomain {
    /// The number of parent rows this domain addresses.
    pub fn count(&self) -> u64 {
        match self {
            KeyDomain::DenseInteger { count, .. }
            | KeyDomain::Deterministic { count, .. }
            | KeyDomain::Composite { count, .. } => *count,
            KeyDomain::Nullable(inner) => inner.count(),
        }
    }

    /// The single-column key at parent `row_index` (used when a parent key is
    /// materialized into the parent's own row).
    fn key_at(&self, row_index: u64) -> GeneratedValue {
        match self {
            KeyDomain::DenseInteger { start, step, .. } => {
                GeneratedValue::Integer(start + step * row_index as i128)
            }
            KeyDomain::Deterministic { generator, .. } => generator.key_at(row_index),
            KeyDomain::Composite { components, .. } => components
                .first()
                .map_or(GeneratedValue::Null, |c| c.key_at(row_index)),
            KeyDomain::Nullable(inner) => inner.key_at(row_index),
        }
    }

    /// The full key row (one value per key column) at parent `row_index`.
    fn key_row(&self, row_index: u64) -> Vec<GeneratedValue> {
        match self {
            KeyDomain::Composite { components, .. } => {
                components.iter().map(|c| c.key_at(row_index)).collect()
            }
            KeyDomain::Nullable(inner) => inner.key_row(row_index),
            _ => vec![self.key_at(row_index)],
        }
    }
}

/// Executes a compiled [`GenerationPlan`], streaming rows to a [`RowSink`].
pub struct GenerationEngine {
    plan: GenerationPlan,
}

impl GenerationEngine {
    /// Build an engine over a compiled plan.
    pub fn new(plan: GenerationPlan) -> Self {
        Self { plan }
    }

    /// Generate every table's rows in dependency order, writing each to `sink`.
    pub fn run(mut self, sink: &mut dyn RowSink) -> Result<EngineReport, GenerateError> {
        let key_domains = build_key_domains(&self.plan)?;
        let family_budget = FamilyBudget {
            max_bytes: self.plan.family_budget_bytes,
        };
        // A family child table is generated from its parent's spooled family
        // rows (drained at this position) rather than the ordinary row loop.
        let table_index_by_name: BTreeMap<String, usize> = self
            .plan
            .tables
            .iter()
            .enumerate()
            .map(|(index, table)| (table.name.clone(), index))
            .collect();
        let family_children = collect_family_children(&self.plan.tables);

        // Take ownership of the tables so per-column compiled generators can be
        // advanced mutably while the rest of the plan stays borrowable.
        let tables = std::mem::take(&mut self.plan.tables);
        let mut rows_written = 0u64;
        // Buffered family child rows, keyed by child table name, accumulated as
        // each parent is generated and drained when the child is reached.
        let mut family_buffers: BTreeMap<String, FamilyBuffer> = BTreeMap::new();

        for (table_index, mut table) in tables.into_iter().enumerate() {
            // A family child: render the spooled rows produced by its parent.
            if let Some(link) = family_children.get(&table.name) {
                rows_written += render_family_child(
                    table_index,
                    &mut table,
                    link,
                    &mut family_buffers,
                    &key_domains,
                    sink,
                )?;
                continue;
            }

            sink.begin_table(&table)?;
            let exec = TableExec::build(&table, &key_domains);
            let names: Vec<String> = table
                .columns
                .iter()
                .map(|column| column.schema.name.clone())
                .collect();
            let ncols = table.columns.len();
            let mut buffer = vec![GeneratedValue::Null; ncols];
            let mut selectors = exec.selectors;
            let mut planners = exec.planners;

            for row_index in 0..table.rows {
                // Non-reading owners first: defaults, materialized parent keys,
                // then foreign-key selections.
                for &i in &exec.defaults {
                    buffer[i] = GeneratedValue::Default;
                }
                for (i, key) in &exec.dense {
                    buffer[*i] = key_domains[key].key_at(row_index);
                }
                for selector in selectors.iter_mut() {
                    selector.assign(row_index, &mut buffer);
                }
                // Table planners produce their owned columns together, before
                // the per-column generator pass (a planner-owned column takes
                // its value from the planner, not a generator).
                for exec in planners.iter_mut() {
                    let planner = &mut table.planners[exec.index];
                    planner.generate_row(row_index, &mut exec.scratch)?;
                    for (slot, value) in exec.members.iter().zip(exec.scratch.iter_mut()) {
                        buffer[*slot] = std::mem::replace(value, GeneratedValue::Null);
                    }
                    // A family planner also produced this order's child lines:
                    // spool them (bounded — spills past the family budget) so
                    // they can be drained when the child table is reached.
                    if let Some(child_name) = planner.family_child_table().map(str::to_string) {
                        let child_rows = planner.take_family_children();
                        if !child_rows.is_empty() {
                            let child_index =
                                table_index_by_name.get(&child_name).copied().unwrap_or(0);
                            let buffer = family_buffers.entry(child_name).or_insert_with(|| {
                                FamilyBuffer::new(
                                    family_budget,
                                    child_index as u32,
                                    TempConfig::default(),
                                    SpillKind::Child,
                                )
                            });
                            for values in child_rows {
                                buffer
                                    .push(SpooledRow {
                                        table_id: child_index as u32,
                                        row_index,
                                        values,
                                    })
                                    .map_err(family_spool_error)?;
                            }
                        }
                    }
                }
                // Column generators last: they may read siblings produced above.
                // A materialized referenced key (in `dense`) is rendered from its
                // domain, so it is skipped here even though it is generator-owned.
                for i in 0..ncols {
                    if exec.dense_indices.contains(&i) {
                        continue;
                    }
                    let column = &mut table.columns[i];
                    let ColumnOwner::Generator { kind, compiled } = &mut column.owner else {
                        continue;
                    };
                    if is_fk_kind(kind) {
                        continue;
                    }
                    let (left, right) = buffer.split_at_mut(i);
                    let view = PartialRow {
                        names: &names[..i],
                        values: left,
                    };
                    let context = RowContext::new(row_index, &view);
                    compiled.generate(&context, &mut right[0])?;
                    // Owner value produced; run this column's modifier pipeline
                    // in declared order before the row is written.
                    for modifier in column.modifiers.iter_mut() {
                        modifier.apply(&context, &mut right[0])?;
                    }
                }

                let generated = GeneratedRow {
                    table_index,
                    row_index,
                    values: std::mem::take(&mut buffer),
                };
                sink.write_row(&table, &generated)?;
                buffer = generated.values;
                rows_written += 1;
            }

            sink.end_table(&table)?;
        }

        Ok(EngineReport { rows_written })
    }
}

/// A family child table's link to the parent family: the child columns the
/// planner produces (in spooled-value order) and the child relationship carrying
/// the foreign key back to the parent.
struct FamilyChildLink {
    child_writes: Vec<String>,
    relationship: String,
}

/// Discover every family child table declared by a planner on any parent table.
fn collect_family_children(tables: &[PlannedTable]) -> BTreeMap<String, FamilyChildLink> {
    let mut links = BTreeMap::new();
    for table in tables {
        for planner in &table.planners {
            if let (Some(child), Some(relationship)) =
                (planner.family_child_table(), planner.family_relationship())
            {
                links.insert(
                    child.to_string(),
                    FamilyChildLink {
                        child_writes: planner.child_writes().to_vec(),
                        relationship: relationship.to_string(),
                    },
                );
            }
        }
    }
    links
}

/// Render a family child table from the rows its parent spooled: each drained
/// row carries its parent row index (so the foreign key binds to the exact
/// parent that produced it) and the planner-owned child column values; the
/// remaining columns (the child primary key, other generators) run their own
/// owners. Returns the number of child rows written.
fn render_family_child(
    table_index: usize,
    table: &mut PlannedTable,
    link: &FamilyChildLink,
    family_buffers: &mut BTreeMap<String, FamilyBuffer>,
    key_domains: &BTreeMap<(String, String), ParentKey>,
    sink: &mut dyn RowSink,
) -> Result<u64, GenerateError> {
    sink.begin_table(table)?;
    let exec = TableExec::build(table, key_domains);
    let names: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.schema.name.clone())
        .collect();
    let ncols = table.columns.len();

    let slot_of = |name: &str| names.iter().position(|candidate| candidate == name);
    let child_slots: Vec<Option<usize>> =
        link.child_writes.iter().map(|name| slot_of(name)).collect();

    // The relationship (on this child) whose FK is set from the owning parent.
    let fk: Vec<(usize, (String, String))> = table
        .relationships
        .iter()
        .find(|relationship| relationship.name.as_deref() == Some(link.relationship.as_str()))
        .map(|relationship| {
            relationship
                .columns
                .iter()
                .zip(&relationship.parent_columns)
                .filter_map(|(child_col, parent_col)| {
                    slot_of(child_col).map(|slot| {
                        (
                            slot,
                            (relationship.parent_table.clone(), parent_col.clone()),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let drained = match family_buffers.remove(&table.name) {
        Some(mut buffer) => buffer.drain_rows().map_err(family_spool_error)?,
        None => Vec::new(),
    };

    let mut rows_written = 0u64;
    let mut buffer = vec![GeneratedValue::Null; ncols];
    for (child_index, spooled) in drained.into_iter().enumerate() {
        let child_index = child_index as u64;
        let parent_index = spooled.row_index;
        for value in buffer.iter_mut() {
            *value = GeneratedValue::Null;
        }
        for &i in &exec.defaults {
            buffer[i] = GeneratedValue::Default;
        }
        for (i, key) in &exec.dense {
            buffer[*i] = key_domains[key].key_at(child_index);
        }
        // Planner-owned child columns from the spooled values.
        for (slot, value) in child_slots.iter().zip(spooled.values) {
            if let Some(slot) = slot {
                buffer[*slot] = value;
            }
        }
        // Foreign key(s) bound to the exact parent that produced this line.
        for (slot, key) in &fk {
            buffer[*slot] = key_domains
                .get(key)
                .map_or(GeneratedValue::Null, |parent| parent.key_at(parent_index));
        }
        // Remaining generator-owned columns (e.g. the child primary key).
        for i in 0..ncols {
            if exec.dense_indices.contains(&i) {
                continue;
            }
            let column = &mut table.columns[i];
            let ColumnOwner::Generator { kind, compiled } = &mut column.owner else {
                continue;
            };
            if is_fk_kind(kind) {
                continue;
            }
            let (left, right) = buffer.split_at_mut(i);
            let view = PartialRow {
                names: &names[..i],
                values: left,
            };
            let context = RowContext::new(child_index, &view);
            compiled.generate(&context, &mut right[0])?;
            for modifier in column.modifiers.iter_mut() {
                modifier.apply(&context, &mut right[0])?;
            }
        }

        let generated = GeneratedRow {
            table_index,
            row_index: child_index,
            values: std::mem::take(&mut buffer),
        };
        sink.write_row(table, &generated)?;
        buffer = generated.values;
        rows_written += 1;
    }

    sink.end_table(table)?;
    Ok(rows_written)
}

/// Wrap a family spool I/O failure as a generation error.
fn family_spool_error(error: std::io::Error) -> GenerateError {
    GenerateError::diagnostic(
        &crate::diagnostic::codes::FAMILY_SPOOL,
        "runtime.family_spool",
        format!("buffering family child rows failed: {error}"),
    )
}

/// Whether a generator kind is a foreign-key marker driven by the engine rather
/// than executed as a column generator.
fn is_fk_kind(kind: &str) -> bool {
    kind == "relation.foreign_key" || kind == "relation.composite_key"
}

/// Whether a family is an integer family (materializable as a dense key).
fn is_integer_family(family: &crate::synthetic::schema::SqlTypeFamily) -> bool {
    use crate::synthetic::schema::SqlTypeFamily;
    matches!(family, SqlTypeFamily::Integer | SqlTypeFamily::BigInteger)
}

/// The [`SeedRoot`] a resolved table seed derives runtime streams from; a
/// `Random` table falls back to the zero root (per-run entropy for
/// relationship assignment is a follow-up).
fn seed_root_of(seed: &ResolvedTableSeed) -> SeedRoot {
    match seed {
        ResolvedTableSeed::Inherited(root) | ResolvedTableSeed::Fixed(root) => *root,
        ResolvedTableSeed::Random => SeedRoot::new(0),
    }
}

/// Build a [`ParentKey`] for every parent key column referenced by a
/// relationship anywhere in the plan. A referenced key must be reproducible by
/// random access (regenerated from a per-row seed, never spooled): a bare
/// integer primary key is a dense sequence, and a `uuid`-generator key reseeds
/// per row. A genuinely stateful or otherwise non-random-access key reports
/// `GEN-KEY-DOMAIN-UNSUPPORTED` (protected key spooling is a later task).
fn build_key_domains(
    plan: &GenerationPlan,
) -> Result<BTreeMap<(String, String), ParentKey>, GenerateError> {
    let mut domains: BTreeMap<(String, String), ParentKey> = BTreeMap::new();
    for table in &plan.tables {
        for relationship in &table.relationships {
            for parent_column in &relationship.parent_columns {
                let key = (relationship.parent_table.clone(), parent_column.clone());
                if domains.contains_key(&key) {
                    continue;
                }
                let Some(parent) = plan.table(&relationship.parent_table) else {
                    continue;
                };
                let Some(column) = parent
                    .columns
                    .iter()
                    .find(|column| &column.schema.name == parent_column)
                else {
                    continue;
                };
                let shape = match &column.owner {
                    // A bare integer primary key is a database sequence:
                    // children reference 1..=count and the parent renders it.
                    ColumnOwner::GeneratedByDatabase
                        if is_integer_family(&column.schema.family) =>
                    {
                        KeyRecipe::Dense { start: 1, step: 1 }
                    }
                    // Any generator that describes itself as a random-access key
                    // (`sequence` -> Dense, `uuid` -> Uuid) is materializable.
                    ColumnOwner::Generator { compiled, .. } => match compiled.key_recipe() {
                        Some(shape) => shape,
                        None => {
                            return Err(unsupported_key(&relationship.parent_table, parent_column))
                        }
                    },
                    _ => return Err(unsupported_key(&relationship.parent_table, parent_column)),
                };
                domains.insert(
                    key,
                    ParentKey {
                        shape,
                        seed: seed_root_of(&parent.seed),
                        table: parent.name.clone(),
                        column: parent_column.clone(),
                        count: parent.rows,
                    },
                );
            }
        }
    }
    Ok(domains)
}

/// The `GEN-KEY-DOMAIN-UNSUPPORTED` error for a parent key that cannot be
/// reproduced by random access (a stateful or otherwise non-describable key).
fn unsupported_key(parent_table: &str, parent_column: &str) -> GenerateError {
    GenerateError::diagnostic(
        &crate::diagnostic::codes::KEY_DOMAIN_UNSUPPORTED,
        format!("tables.{parent_table}.columns.{parent_column}"),
        format!(
            "parent key `{parent_table}.{parent_column}` cannot be materialized for random \
             access; supported: bare integer primary keys, `sequence`, and `uuid` keys \
             (stateful keys await protected key spooling)"
        ),
    )
}

/// The per-column execution plan for one table: which columns are constant
/// database placeholders, which are materialized parent keys, and which are
/// foreign-key selections.
struct TableExec {
    /// Columns emitted as `DEFAULT` (database-filled or unmodeled placeholders).
    defaults: Vec<usize>,
    /// Materialized referenced parent-key columns, keyed into the domain map.
    dense: Vec<(usize, (String, String))>,
    /// The set of `dense` column indices, for quick exclusion from the generator
    /// pass (a referenced random-access key is rendered from its domain, not its
    /// column generator).
    dense_indices: BTreeSet<usize>,
    /// Foreign-key selectors, one per active relationship.
    selectors: Vec<FkSelector>,
    /// Table planners that produce owned columns, one per active planner.
    planners: Vec<PlannerExec>,
}

impl TableExec {
    fn build(table: &PlannedTable, key_domains: &BTreeMap<(String, String), ParentKey>) -> Self {
        let mut dense: Vec<(usize, (String, String))> = Vec::new();
        let mut dense_indices: BTreeSet<usize> = BTreeSet::new();
        for (i, column) in table.columns.iter().enumerate() {
            let key = (table.name.clone(), column.schema.name.clone());
            if key_domains.contains_key(&key) {
                dense.push((i, key));
                dense_indices.insert(i);
            }
        }

        let seed_root = seed_root_of(&table.seed);
        let mut selectors: Vec<FkSelector> = Vec::new();
        let mut member_indices: BTreeSet<usize> = BTreeSet::new();
        for relationship in &table.relationships {
            if let Some(selector) = FkSelector::build(table, relationship, key_domains, seed_root) {
                member_indices.extend(selector.members.iter().copied());
                selectors.push(selector);
            }
        }

        // Map each planner's owned columns (by name) to row-buffer slots so the
        // engine can scatter the planner's per-row output into place. Slots a
        // planner owns are excluded from the DEFAULT pass below.
        let mut planners: Vec<PlannerExec> = Vec::new();
        let mut planner_indices: BTreeSet<usize> = BTreeSet::new();
        for (index, planner) in table.planners.iter().enumerate() {
            let members: Option<Vec<usize>> = planner
                .writes()
                .iter()
                .map(|name| {
                    table
                        .columns
                        .iter()
                        .position(|column| &column.schema.name == name)
                })
                .collect();
            let Some(members) = members else {
                continue;
            };
            planner_indices.extend(members.iter().copied());
            let scratch = vec![GeneratedValue::Null; members.len()];
            planners.push(PlannerExec {
                index,
                members,
                scratch,
            });
        }

        let mut defaults: Vec<usize> = Vec::new();
        for i in 0..table.columns.len() {
            if dense_indices.contains(&i)
                || member_indices.contains(&i)
                || planner_indices.contains(&i)
            {
                continue;
            }
            match &table.columns[i].owner {
                // A live column generator is executed in the generator pass.
                ColumnOwner::Generator { kind, .. } if !is_fk_kind(kind) => {}
                // Everything else (database-filled, defaults, a planner that
                // never mapped, or an FK that never resolved a selector) renders
                // as DEFAULT.
                _ => defaults.push(i),
            }
        }

        TableExec {
            defaults,
            dense,
            dense_indices,
            selectors,
            planners,
        }
    }
}

/// A per-planner execution binding: which compiled planner to invoke and the
/// row-buffer slots its produced columns scatter into.
struct PlannerExec {
    /// Index into [`PlannedTable::planners`].
    index: usize,
    /// Row-buffer slots for the planner's written columns, in `writes()` order.
    members: Vec<usize>,
    /// Reusable scratch aligned with `members`: the planner writes here each
    /// row, then the engine moves each value into its buffer slot.
    scratch: Vec<GeneratedValue>,
}

/// A per-relationship foreign-key selector: picks a parent row per child row and
/// writes the resulting key(s) into the child's foreign-key columns.
struct FkSelector {
    /// Child column indices carrying the key, in parent-column order.
    members: Vec<usize>,
    /// The parent key domain.
    domain: KeyDomain,
    /// The number of parent rows.
    parent_count: u64,
    /// How children are distributed across parents.
    distribution: RelationshipDistribution,
    /// Fraction of children whose key is `NULL` (nullable columns only).
    null_rate: f64,
    /// Whether every member column is nullable.
    nullable: bool,
    /// The relationship's per-row RNG, advanced sequentially.
    rng: ChaCha8Rng,
    /// Cumulative bucket weights for a weighted/observed histogram; empty
    /// otherwise.
    histogram: Vec<f64>,
}

impl FkSelector {
    fn build(
        table: &PlannedTable,
        relationship: &CompiledRelationship,
        key_domains: &BTreeMap<(String, String), ParentKey>,
        seed_root: SeedRoot,
    ) -> Option<Self> {
        let members: Vec<usize> = relationship
            .columns
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .position(|column| &column.schema.name == name)
            })
            .collect::<Option<_>>()?;

        // Each parent key column contributes a cloneable recipe; a single index
        // is chosen per child row and every component is derived from it.
        let parents: Vec<&ParentKey> = relationship
            .parent_columns
            .iter()
            .map(|parent_column| {
                key_domains.get(&(relationship.parent_table.clone(), parent_column.clone()))
            })
            .collect::<Option<_>>()?;
        let parent_count = parents.first()?.count;

        let domain = if parents.len() == 1 {
            match parents[0].shape {
                KeyRecipe::Dense { start, step } => KeyDomain::DenseInteger {
                    start,
                    step,
                    count: parent_count,
                },
                KeyRecipe::Uuid => KeyDomain::Deterministic {
                    count: parent_count,
                    generator: parents[0].generator(),
                },
            }
        } else {
            KeyDomain::Composite {
                count: parent_count,
                components: parents.iter().map(|p| p.generator()).collect(),
            }
        };

        let nullable = relationship.columns.iter().all(|name| {
            table
                .columns
                .iter()
                .find(|column| &column.schema.name == name)
                .is_some_and(|column| column.schema.nullable)
        });
        let null_rate = f64::from(relationship.null_permille) / 1000.0;
        let rng = seed_root.stream(StreamId::operator(
            table.name.as_str(),
            relationship.columns.join(","),
            "relation.foreign_key",
        ));
        let histogram = match relationship.distribution {
            RelationshipDistribution::Weighted | RelationshipDistribution::Observed => {
                build_histogram(parent_count)
            }
            RelationshipDistribution::Uniform | RelationshipDistribution::Sequential => Vec::new(),
        };

        Some(FkSelector {
            members,
            domain,
            parent_count,
            distribution: relationship.distribution,
            null_rate,
            nullable,
            rng,
            histogram,
        })
    }

    /// Assign this relationship's key(s) for the child at `row_index`.
    fn assign(&mut self, row_index: u64, buffer: &mut [GeneratedValue]) {
        // Null is a separate first decision, before any parent is chosen.
        if self.nullable && self.null_rate > 0.0 && self.rng.random::<f64>() < self.null_rate {
            self.fill(buffer, |_| GeneratedValue::Null);
            return;
        }
        if self.parent_count == 0 {
            self.fill(buffer, |_| GeneratedValue::Null);
            return;
        }
        let parent = self.pick(row_index);
        let keys = self.domain.key_row(parent);
        for (&member, key) in self.members.iter().zip(keys) {
            buffer[member] = key;
        }
    }

    fn fill(&self, buffer: &mut [GeneratedValue], value: impl Fn(usize) -> GeneratedValue) {
        for (position, &member) in self.members.iter().enumerate() {
            buffer[member] = value(position);
        }
    }

    /// Choose the parent row index for the child at `row_index`.
    fn pick(&mut self, row_index: u64) -> u64 {
        match self.distribution {
            RelationshipDistribution::Uniform => self.rng.random_range(0..self.parent_count),
            RelationshipDistribution::Sequential => row_index % self.parent_count,
            RelationshipDistribution::Weighted | RelationshipDistribution::Observed => {
                self.sample_histogram()
            }
        }
    }

    /// Draw a parent row index from the bounded histogram.
    fn sample_histogram(&mut self) -> u64 {
        let buckets = self.histogram.len();
        if buckets == 0 {
            return self.rng.random_range(0..self.parent_count);
        }
        let total = self.histogram[buckets - 1];
        let target = self.rng.random::<f64>() * total;
        let bucket = self
            .histogram
            .partition_point(|&cumulative| cumulative <= target)
            .min(buckets - 1) as u64;

        // Map the bucket back to a parent row index. With one bucket per parent
        // (the common small-fan-in case) this is the identity.
        let base = self.parent_count / buckets as u64;
        let remainder = self.parent_count % buckets as u64;
        let start = bucket * base + bucket.min(remainder);
        let size = base + u64::from(bucket < remainder);
        if size <= 1 {
            start
        } else {
            start + self.rng.random_range(0..size)
        }
    }
}

/// Build a bounded, decaying histogram over `parent_count` parents: earlier
/// parents carry proportionally more weight, capped at 256 buckets so memory
/// stays bounded regardless of parent count.
fn build_histogram(parent_count: u64) -> Vec<f64> {
    let buckets = parent_count.min(256) as usize;
    let mut cumulative = Vec::with_capacity(buckets);
    let mut acc = 0.0;
    for bucket in 0..buckets {
        acc += 1.0 / (bucket as f64 + 1.0);
        cumulative.push(acc);
    }
    cumulative
}

/// A [`RowView`] over the columns produced so far in the current row.
struct PartialRow<'a> {
    names: &'a [String],
    values: &'a [GeneratedValue],
}

impl RowView for PartialRow<'_> {
    fn get(&self, column: &str) -> Option<&GeneratedValue> {
        self.names
            .iter()
            .position(|name| name == column)
            .map(|i| &self.values[i])
    }
}
