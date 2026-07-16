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

use super::plan::{
    ColumnOwner, CompiledRelationship, GenerationPlan, PlannedTable, RelationshipDistribution,
    ResolvedTableSeed,
};
use super::registry::{RowContext, RowView};
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

/// A summary of a completed generation run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GenerateReport {
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

/// A cloneable recipe for a referenced parent key column: enough to both render
/// the parent's own key and reconstruct a random-access generator for child
/// selection, without cloning a trait object.
#[derive(Clone)]
enum KeyRecipe {
    /// A dense integer sequence `start + step * n` (a bare integer PK sequence).
    Dense { start: i128, step: i128 },
    /// A per-row-reseeded random UUID (a `uuid`-generator PK).
    Uuid {
        seed: SeedRoot,
        table: String,
        column: String,
    },
}

impl KeyRecipe {
    /// A fresh random-access generator realizing this recipe.
    fn generator(&self) -> Box<dyn RandomAccessKeyGenerator> {
        match self {
            KeyRecipe::Dense { start, step } => Box::new(DenseIntegerKey {
                start: *start,
                step: *step,
            }),
            KeyRecipe::Uuid {
                seed,
                table,
                column,
            } => Box::new(SeededUuidKey {
                seed: *seed,
                table: table.clone(),
                column: column.clone(),
            }),
        }
    }

    /// The key value at parent `row_index` (used to render the parent's own PK).
    fn key_at(&self, row_index: u64) -> GeneratedValue {
        match self {
            KeyRecipe::Dense { start, step } => {
                GeneratedValue::Integer(start + step * row_index as i128)
            }
            KeyRecipe::Uuid {
                seed,
                table,
                column,
            } => seeded_uuid(*seed, table, column, row_index),
        }
    }
}

/// A referenced parent key column: how to reproduce its key, and its row count.
struct ParentKey {
    recipe: KeyRecipe,
    count: u64,
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
    pub fn run(mut self, sink: &mut dyn RowSink) -> Result<GenerateReport, GenerateError> {
        let key_domains = build_key_domains(&self.plan)?;
        // Take ownership of the tables so per-column compiled generators can be
        // advanced mutably while the rest of the plan stays borrowable.
        let tables = std::mem::take(&mut self.plan.tables);
        let mut rows_written = 0u64;

        for (table_index, mut table) in tables.into_iter().enumerate() {
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

            for row_index in 0..table.rows {
                // Non-reading owners first: defaults, materialized parent keys,
                // then foreign-key selections.
                for &i in &exec.defaults {
                    buffer[i] = GeneratedValue::Default;
                }
                for (i, key) in &exec.dense {
                    buffer[*i] = key_domains[key].recipe.key_at(row_index);
                }
                for selector in selectors.iter_mut() {
                    selector.assign(row_index, &mut buffer);
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

        Ok(GenerateReport { rows_written })
    }
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
                let recipe = match &column.owner {
                    // A bare integer primary key is a database sequence:
                    // children reference 1..=count and the parent renders it.
                    ColumnOwner::GeneratedByDatabase
                        if is_integer_family(&column.schema.family) =>
                    {
                        KeyRecipe::Dense { start: 1, step: 1 }
                    }
                    // A `uuid` primary key is random-access: each parent row's
                    // key reseeds from `primary_key.row.<n>`.
                    ColumnOwner::Generator { kind, .. } if kind == "uuid" => KeyRecipe::Uuid {
                        seed: seed_root_of(&parent.seed),
                        table: parent.name.clone(),
                        column: parent_column.clone(),
                    },
                    _ => {
                        return Err(GenerateError::InvalidInput(format!(
                            "GEN-KEY-DOMAIN-UNSUPPORTED: parent key `{}.{}` cannot be materialized for random access; supported: bare integer primary keys and `uuid` keys (stateful keys await protected key spooling)",
                            relationship.parent_table, parent_column
                        )));
                    }
                };
                domains.insert(
                    key,
                    ParentKey {
                        recipe,
                        count: parent.rows,
                    },
                );
            }
        }
    }
    Ok(domains)
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

        let mut defaults: Vec<usize> = Vec::new();
        for i in 0..table.columns.len() {
            if dense_indices.contains(&i) || member_indices.contains(&i) {
                continue;
            }
            match &table.columns[i].owner {
                // A live column generator is executed in the generator pass.
                ColumnOwner::Generator { kind, .. } if !is_fk_kind(kind) => {}
                // Everything else (database-filled, defaults, planners, or an FK
                // that never resolved a selector) renders as DEFAULT.
                _ => defaults.push(i),
            }
        }

        TableExec {
            defaults,
            dense,
            dense_indices,
            selectors,
        }
    }
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
            match &parents[0].recipe {
                KeyRecipe::Dense { start, step } => KeyDomain::DenseInteger {
                    start: *start,
                    step: *step,
                    count: parent_count,
                },
                recipe => KeyDomain::Deterministic {
                    count: parent_count,
                    generator: recipe.generator(),
                },
            }
        } else {
            KeyDomain::Composite {
                count: parent_count,
                components: parents.iter().map(|p| p.recipe.generator()).collect(),
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
