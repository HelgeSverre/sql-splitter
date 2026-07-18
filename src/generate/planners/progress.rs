//! The `workflow.progress_counters` planner: coordinates a batch/job's counter,
//! status, and completion columns so every row tells one coherent lifecycle
//! story — `succeeded + failed = processed`, `pending = total - processed`, and
//! a completed row is fully processed with a non-null completion timestamp.
//!
//! # Total first, state second, exact partition third
//!
//! Each row is built in three deterministic steps drawn from independent seed
//! streams:
//!
//! 1. **TOTAL** — the size of the batch (a fixed or uniform integer draw).
//! 2. **STATE** — the lifecycle state (`complete`, `active`, `not_started`),
//!    either fixed by the configured kind or sampled from a normalized mixture.
//! 3. **PARTITION** — the exact integer split of `total` into the counter
//!    columns. The split is pure integer arithmetic: `processed` is chosen for
//!    the state, `pending = total - processed`, and `processed` is apportioned
//!    into `succeeded`/`failed` (and, under `allow_unclassified`, an unclassified
//!    remainder) with the largest-remainder method so the parts sum to
//!    `processed` exactly — no float rounding drift can make the counters
//!    disagree.
//!
//! # Owned columns and streams
//!
//! The planner owns every column named under its `columns:` mapping; the model
//! compiler turns those into ownership claims, so a colliding generator raises
//! `GEN-COLUMN-OWNER-CONFLICT`. Each row draws its total, state, progress
//! fraction, status pick, and completion offset from five *separate* stable
//! streams, so a seeded run reproduces exactly regardless of which rows land in
//! which state.

use chrono::{DateTime, Utc};
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use serde_yaml_ng::Value;

use crate::diagnostic::DiagnosticBag;
use crate::generate::registry::{
    Buffering, ColumnScope, CompileContext, CompiledPlanner, Determinism, PlannerDescriptor,
    PlannerFactory, PlannerPredicate, PredicateGuard, Verification,
};
use crate::generate::seed::StreamId;
use crate::generate::value::{GenerateError, GeneratedValue};
use crate::synthetic::model::PlannerConfig;
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

/// The default completion base instant (`2024-01-01T00:00:00Z`) in epoch
/// seconds, and the default window width completion timestamps spread across.
const COMPLETION_BASE_SECS: i64 = 1_704_067_200;
const COMPLETION_SPAN_SECS: i64 = 30 * 86_400;

/// Static description of the `workflow.progress_counters` planner.
pub static WORKFLOW_PROGRESS_COUNTERS_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "workflow.progress_counters",
    aliases: &[],
    summary: "Coordinates total/processed/succeeded/failed/pending counters, status, and completion so each row's lifecycle counters agree exactly.",
    arguments: &[],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `workflow.progress_counters` planner.
pub struct ProgressCountersFactory;

impl PlannerFactory for ProgressCountersFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &WORKFLOW_PROGRESS_COUNTERS_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_progress(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

// --- Lifecycle model --------------------------------------------------------

/// A row's lifecycle state, which fixes how `total` partitions into counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleState {
    /// Fully processed: `processed = total`, `pending = 0`, completion non-null.
    Complete,
    /// Partially processed: `0 <= processed < total`, completion null.
    Active,
    /// Untouched: `processed = 0`, `pending = total`, completion null.
    NotStarted,
}

/// How each row's lifecycle state is chosen.
enum StateMix {
    /// Every row is the same state (`complete`/`in_progress`/`not_started`).
    Fixed(LifecycleState),
    /// A normalized mixture; the three weights sum to a positive, finite total.
    Weighted {
        complete: f64,
        active: f64,
        not_started: f64,
    },
}

impl StateMix {
    /// Select a state from a uniform draw `r` in `[0, 1)`.
    fn select(&self, r: f64) -> LifecycleState {
        match self {
            StateMix::Fixed(state) => *state,
            StateMix::Weighted {
                complete,
                active,
                not_started,
            } => {
                let total = complete + active + not_started;
                let point = r * total;
                if point < *complete {
                    LifecycleState::Complete
                } else if point < complete + active {
                    LifecycleState::Active
                } else {
                    LifecycleState::NotStarted
                }
            }
        }
    }

    /// Whether each state can occur, as `(complete, active, not_started)`.
    fn reachable(&self) -> (bool, bool, bool) {
        match self {
            StateMix::Fixed(LifecycleState::Complete) => (true, false, false),
            StateMix::Fixed(LifecycleState::Active) => (false, true, false),
            StateMix::Fixed(LifecycleState::NotStarted) => (false, false, true),
            StateMix::Weighted {
                complete,
                active,
                not_started,
            } => (*complete > 0.0, *active > 0.0, *not_started > 0.0),
        }
    }
}

/// How each row's `total` is chosen. Always yields a non-negative integer.
enum TotalDraw {
    Fixed(i128),
    Uniform { min: i128, max: i128 },
}

impl TotalDraw {
    fn draw(&self, rng: &mut ChaCha8Rng) -> i128 {
        match self {
            TotalDraw::Fixed(value) => *value,
            TotalDraw::Uniform { min, max } => {
                if max <= min {
                    *min
                } else {
                    rng.random_range(*min..=*max)
                }
            }
        }
    }

    /// The largest `total` this draw can ever produce (for the overflow check).
    fn maximum(&self) -> i128 {
        match self {
            TotalDraw::Fixed(value) => *value,
            TotalDraw::Uniform { max, .. } => *max,
        }
    }
}

/// How `processed` splits into the classified counters.
#[derive(Clone, Copy)]
enum Partition {
    /// `succeeded + failed = processed` exactly.
    Exact,
    /// `succeeded + failed + unclassified = processed`; a share of every batch is
    /// left unclassified, so `succeeded + failed <= processed`.
    AllowUnclassified { unclassified_ratio: f64 },
}

// --- Column roles -----------------------------------------------------------

/// A role a configured column plays, in `generate_row` write order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Total,
    Processed,
    Succeeded,
    Failed,
    Pending,
    Status,
    CompletedAt,
}

/// The write-order of the roles the planner recognizes.
const ROLE_ORDER: [(&str, Role); 7] = [
    ("total", Role::Total),
    ("processed", Role::Processed),
    ("succeeded", Role::Succeeded),
    ("failed", Role::Failed),
    ("pending", Role::Pending),
    ("status", Role::Status),
    ("completed_at", Role::CompletedAt),
];

/// A resolved output column: its role, schema name, and type family.
struct Slot {
    role: Role,
    name: String,
    family: SqlTypeFamily,
}

// --- The compiled planner ---------------------------------------------------

/// The compiled `workflow.progress_counters` planner.
struct ProgressCountersPlanner {
    /// Owned columns in `generate_row` write order, parallel to `slots`.
    writes: Vec<String>,
    slots: Vec<Slot>,
    total_draw: TotalDraw,
    state_mix: StateMix,
    partition: Partition,
    /// Share of `processed` that succeeds (the rest fails); in `[0, 1]`.
    success_ratio: f64,
    completed_statuses: Vec<String>,
    active_statuses: Vec<String>,
    total_rng: ChaCha8Rng,
    state_rng: ChaCha8Rng,
    progress_rng: ChaCha8Rng,
    status_rng: ChaCha8Rng,
    completion_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

/// The counters and derived cells a single row resolves to.
struct RowCounts {
    total: i128,
    processed: i128,
    succeeded: i128,
    failed: i128,
    pending: i128,
    status: String,
    completed_at: Option<String>,
}

impl CompiledPlanner for ProgressCountersPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        _row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        let counts = self.draw_row();
        for (slot, cell) in self.slots.iter().zip(output.iter_mut()) {
            *cell = match slot.role {
                Role::Total => render_counter(counts.total, &slot.family),
                Role::Processed => render_counter(counts.processed, &slot.family),
                Role::Succeeded => render_counter(counts.succeeded, &slot.family),
                Role::Failed => render_counter(counts.failed, &slot.family),
                Role::Pending => render_counter(counts.pending, &slot.family),
                Role::Status => render_status(&counts.status, &slot.family),
                Role::CompletedAt => match &counts.completed_at {
                    Some(text) => render_datetime(text, &slot.family),
                    None => GeneratedValue::Null,
                },
            };
        }
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

impl ProgressCountersPlanner {
    /// Resolve one row's counters. All five streams are drawn unconditionally
    /// and in a fixed order so a seeded run reproduces regardless of the state
    /// each row lands in.
    fn draw_row(&mut self) -> RowCounts {
        let total = self.total_draw.draw(&mut self.total_rng).max(0);
        let state_pick = self.state_rng.random::<f64>();
        let progress_fraction = self.progress_rng.random::<f64>();
        let status_pick = self.status_rng.random::<f64>();
        let completion_offset = self.completion_rng.random::<f64>();

        let state = self.state_mix.select(state_pick);
        let processed = processed_for_state(state, total, progress_fraction);
        let pending = total - processed;
        let (succeeded, failed) = self.split_processed(processed);

        let status = match state {
            LifecycleState::Complete => pick(&self.completed_statuses, status_pick),
            LifecycleState::Active | LifecycleState::NotStarted => {
                pick(&self.active_statuses, status_pick)
            }
        };
        let completed_at = match state {
            LifecycleState::Complete => Some(completion_instant(completion_offset)),
            _ => None,
        };

        RowCounts {
            total,
            processed,
            succeeded,
            failed,
            pending,
            status,
            completed_at,
        }
    }

    /// Split `processed` into `(succeeded, failed)` by the largest-remainder
    /// method, so the parts sum to `processed` exactly under `Exact`, or leave a
    /// deterministic unclassified remainder under `AllowUnclassified`.
    fn split_processed(&self, processed: i128) -> (i128, i128) {
        let succeed = self.success_ratio;
        let fail = 1.0 - self.success_ratio;
        let parts = match self.partition {
            Partition::Exact => largest_remainder(processed, &[succeed, fail]),
            Partition::AllowUnclassified { unclassified_ratio } => {
                let classified = 1.0 - unclassified_ratio;
                largest_remainder(
                    processed,
                    &[succeed * classified, fail * classified, unclassified_ratio],
                )
            }
        };
        (parts[0], parts[1])
    }
}

/// The `processed` count for a state and (for active rows) a progress fraction.
/// Active rows are strictly incomplete for any positive `total`, so `pending`
/// stays positive; a degenerate `total <= 0` collapses to zero.
fn processed_for_state(state: LifecycleState, total: i128, progress_fraction: f64) -> i128 {
    match state {
        LifecycleState::Complete => total,
        LifecycleState::NotStarted => 0,
        LifecycleState::Active => {
            if total <= 0 {
                0
            } else {
                let raw = (progress_fraction * total as f64).floor() as i128;
                raw.clamp(0, total - 1)
            }
        }
    }
}

/// Apportion `amount` across `weights` by the largest-remainder method. The
/// returned parts are non-negative and sum to `amount` exactly (for
/// `amount >= 0` and a positive weight total); the integer sum is exact by
/// construction regardless of any float imprecision in the ratios.
fn largest_remainder(amount: i128, weights: &[f64]) -> Vec<i128> {
    let count = weights.len();
    let weight_total: f64 = weights.iter().sum();
    if amount <= 0 || weight_total <= 0.0 || !weight_total.is_finite() {
        return vec![0; count];
    }

    let mut parts = Vec::with_capacity(count);
    let mut remainders = Vec::with_capacity(count);
    let mut allocated: i128 = 0;
    for &weight in weights {
        let exact = amount as f64 * (weight.max(0.0) / weight_total);
        let floor = exact.floor();
        let whole = floor as i128;
        parts.push(whole);
        remainders.push(exact - floor);
        allocated += whole;
    }

    // Distribute the leftover units to the largest fractional remainders,
    // breaking ties by index so the split is stable.
    let mut leftover = amount - allocated;
    let mut order: Vec<usize> = (0..count).collect();
    order.sort_by(|&a, &b| {
        remainders[b]
            .partial_cmp(&remainders[a])
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(&b))
    });
    let mut cursor = 0;
    while leftover > 0 && !order.is_empty() {
        parts[order[cursor % count]] += 1;
        leftover -= 1;
        cursor += 1;
    }
    parts
}

/// Pick a status from `options` using a uniform draw `r` in `[0, 1)`. `options`
/// is non-empty whenever the state that reaches it is reachable (enforced at
/// compile time); an empty list yields an empty string as a last resort.
fn pick(options: &[String], r: f64) -> String {
    if options.is_empty() {
        return String::new();
    }
    let index = ((r * options.len() as f64).floor() as usize).min(options.len() - 1);
    options[index].clone()
}

/// A completion instant spread deterministically across the default window.
fn completion_instant(offset: f64) -> String {
    let extra = (offset * COMPLETION_SPAN_SECS as f64).floor() as i64;
    let secs = COMPLETION_BASE_SECS.saturating_add(extra);
    DateTime::<Utc>::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

// --- Rendering --------------------------------------------------------------

/// Render a counter in the representation `family` expects.
fn render_counter(value: i128, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(value),
        SqlTypeFamily::Decimal => GeneratedValue::Decimal {
            minor: value,
            scale: 0,
        },
        _ => GeneratedValue::Text(value.to_string()),
    }
}

/// Render a status label in the representation `family` expects.
fn render_status(status: &str, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Text | SqlTypeFamily::Uuid | SqlTypeFamily::Other => {
            GeneratedValue::Text(status.to_string())
        }
        _ => GeneratedValue::Text(status.to_string()),
    }
}

/// Render a completion timestamp in the representation `family` expects.
fn render_datetime(text: &str, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::DateTime => GeneratedValue::DateTime(text.to_string()),
        _ => GeneratedValue::Text(text.to_string()),
    }
}

// --- Compilation ------------------------------------------------------------

/// Validate `config` against the table schema and build the compiled planner,
/// gathering every independent error before returning.
fn compile_progress(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<ProgressCountersPlanner, DiagnosticBag> {
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();

    let columns = config.args.get("columns");
    let slots = resolve_slots(columns, table, path, &mut bag);
    let total_draw = compile_total(config.args.get("total"), path, &mut bag);
    let partition = compile_partition(config, path, &mut bag);
    let state_mix = compile_state(config.args.get("progress"), partition, path, &mut bag);
    let success_ratio = config
        .args
        .get("success_ratio")
        .and_then(as_f64)
        .unwrap_or(0.9)
        .clamp(0.0, 1.0);
    let completed_statuses = string_list(config.args.get("completed_statuses"));
    let active_statuses = string_list(config.args.get("active_statuses"));

    // The remaining checks depend on the resolved slots and state mixture.
    let has_role = |role: Role| slots.iter().any(|slot| slot.role == role);
    let (complete_reachable, active_reachable, not_started_reachable) = state_mix
        .as_ref()
        .map_or((false, false, false), StateMix::reachable);

    // Overflow: `total` must fit every counter column's capacity.
    if let Some(draw) = &total_draw {
        let maximum = draw.maximum();
        for slot in &slots {
            if is_counter(slot.role) {
                let capacity = family_capacity(&slot.family);
                if maximum > capacity {
                    bag.error(
                        "GEN-PROGRESS-OVERFLOW",
                        format!("{path}.total"),
                        format!(
                            "workflow.progress_counters `total` can reach {maximum}, exceeding the capacity {capacity} of counter column `{}`",
                            slot.name
                        ),
                    );
                }
            }
        }
    }

    // Status vocabulary: a configured status column needs a non-empty
    // vocabulary for every reachable state group.
    if has_role(Role::Status) {
        if complete_reachable && completed_statuses.is_empty() {
            bag.error(
                "GEN-PROGRESS-STATUS-VOCABULARY",
                format!("{path}.completed_statuses"),
                "workflow.progress_counters produces completed rows but `completed_statuses` is empty; supply at least one completed status".to_string(),
            );
        }
        if (active_reachable || not_started_reachable) && active_statuses.is_empty() {
            bag.error(
                "GEN-PROGRESS-STATUS-VOCABULARY",
                format!("{path}.active_statuses"),
                "workflow.progress_counters produces active or not-started rows but `active_statuses` is empty; supply at least one active status".to_string(),
            );
        }
    }

    // Completion: a non-nullable completion column cannot hold the null an
    // active or not-started row requires.
    if let Some(slot) = slots.iter().find(|slot| slot.role == Role::CompletedAt) {
        if (active_reachable || not_started_reachable) && !column_nullable(table, &slot.name) {
            bag.error(
                "GEN-PROGRESS-COMPLETION",
                format!("{path}.columns.completed_at"),
                format!(
                    "workflow.progress_counters `completed_at` column `{}` is not nullable, but active and not-started rows leave completion null",
                    slot.name
                ),
            );
        }
    }

    if bag.has_errors() {
        return Err(bag);
    }

    let total_draw = total_draw.expect("total resolved without errors");
    let state_mix = state_mix.expect("state resolved without errors");

    let writes = slots.iter().map(|slot| slot.name.clone()).collect();
    let (total_rng, state_rng, progress_rng, status_rng, completion_rng) = build_streams(context);
    let predicates = build_predicates(&slots, partition, &completed_statuses, &active_statuses);

    Ok(ProgressCountersPlanner {
        writes,
        slots,
        total_draw,
        state_mix,
        partition,
        success_ratio,
        completed_statuses,
        active_statuses,
        total_rng,
        state_rng,
        progress_rng,
        status_rng,
        completion_rng,
        predicates,
    })
}

/// Resolve the `columns:` mapping into ordered [`Slot`]s. The `total` role is
/// required; every other role is optional, but any named column must exist.
fn resolve_slots(
    columns: Option<&Value>,
    table: &PortableTable,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Vec<Slot> {
    let mut slots = Vec::new();
    for (role_key, role) in ROLE_ORDER {
        let Some(name) = role_name(columns, role_key) else {
            if role == Role::Total {
                bag.error(
                    "GEN-PROGRESS-COLUMN-MISSING",
                    format!("{path}.columns.total"),
                    "workflow.progress_counters requires a `total` column under `columns`"
                        .to_string(),
                );
            }
            continue;
        };
        match find_column(table, name) {
            Some(column) => slots.push(Slot {
                role,
                name: column.name.clone(),
                family: column.family.clone(),
            }),
            None => {
                bag.error(
                    "GEN-PROGRESS-COLUMN-MISSING",
                    format!("{path}.columns.{role_key}"),
                    format!(
                        "workflow.progress_counters `{role_key}` column `{name}` does not exist on table `{}`",
                        table.name
                    ),
                );
            }
        }
    }
    slots
}

/// Compile the `total:` block into a [`TotalDraw`]. Reports a negative or
/// inverted range.
fn compile_total(total: Option<&Value>, path: &str, bag: &mut DiagnosticBag) -> Option<TotalDraw> {
    let kind = total.and_then(|t| t.get("kind")).and_then(Value::as_str);
    let field = |key: &str| total.and_then(|t| t.get(key)).and_then(as_i128);

    let draw = match kind {
        Some("fixed") => {
            let value = field("value").unwrap_or(0);
            if value < 0 {
                bag.error(
                    "GEN-PROGRESS-TOTAL",
                    format!("{path}.total.value"),
                    format!("workflow.progress_counters `total.value` `{value}` is negative"),
                );
                return None;
            }
            TotalDraw::Fixed(value)
        }
        // `uniform`, or an omitted kind.
        _ => {
            let min = field("min").unwrap_or(0);
            let max = field("max").unwrap_or(min);
            if min < 0 {
                bag.error(
                    "GEN-PROGRESS-TOTAL",
                    format!("{path}.total.min"),
                    format!("workflow.progress_counters `total.min` `{min}` is negative"),
                );
                return None;
            }
            if max < min {
                bag.error(
                    "GEN-PROGRESS-TOTAL",
                    format!("{path}.total"),
                    "workflow.progress_counters `total.max` is below `total.min`".to_string(),
                );
                return None;
            }
            TotalDraw::Uniform { min, max }
        }
    };
    Some(draw)
}

/// Compile the `partition:` selector.
fn compile_partition(config: &PlannerConfig, path: &str, bag: &mut DiagnosticBag) -> Partition {
    match config.args.get("partition").and_then(Value::as_str) {
        Some("allow_unclassified") => {
            let unclassified_ratio = config
                .args
                .get("unclassified_ratio")
                .and_then(as_f64)
                .unwrap_or(0.1)
                .clamp(0.0, 1.0);
            Partition::AllowUnclassified { unclassified_ratio }
        }
        Some("exact") | None => Partition::Exact,
        Some(other) => {
            bag.error(
                "GEN-PROGRESS-PARTITION",
                format!("{path}.partition"),
                format!(
                    "workflow.progress_counters `partition` `{other}` is not one of `exact` or `allow_unclassified`"
                ),
            );
            Partition::Exact
        }
    }
}

/// Compile the `progress:` block into a [`StateMix`]. Rejects a zero-total or
/// non-finite mixture, and `observed` progress that cannot form an exact
/// partition because its evidence is absent.
fn compile_state(
    progress: Option<&Value>,
    partition: Partition,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<StateMix> {
    let kind = progress
        .and_then(|p| p.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("mixture");

    match kind {
        "complete" => Some(StateMix::Fixed(LifecycleState::Complete)),
        "in_progress" => Some(StateMix::Fixed(LifecycleState::Active)),
        "not_started" => Some(StateMix::Fixed(LifecycleState::NotStarted)),
        "observed" => {
            // Observed progress needs distribution evidence from a profile.
            // None is threaded here yet, so an exact partition over absent
            // observed evidence cannot be formed: reject it clearly.
            // A relaxed partition falls back to a documented default mixture.
            if let Partition::Exact = partition {
                bag.error(
                    "GEN-PROGRESS-OBSERVED",
                    format!("{path}.progress"),
                    "workflow.progress_counters `progress.kind: observed` needs observed evidence to form an exact partition, but none is available; supply explicit mixture weights or use `partition: allow_unclassified`".to_string(),
                );
                None
            } else {
                Some(default_mixture())
            }
        }
        // `mixture`, or an unknown kind treated as a mixture.
        _ => compile_mixture(progress, path, bag),
    }
}

/// A default lifecycle mixture used where no explicit weights are supplied.
fn default_mixture() -> StateMix {
    StateMix::Weighted {
        complete: 0.7,
        active: 0.25,
        not_started: 0.05,
    }
}

/// Compile explicit mixture weights, defaulting to an even split when none are
/// given and rejecting a zero-total or non-finite mixture.
fn compile_mixture(
    progress: Option<&Value>,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<StateMix> {
    let weight = |key: &str| progress.and_then(|p| p.get(key)).and_then(as_f64);
    let complete = weight("complete_weight");
    let active = weight("active_weight");
    let not_started = weight("not_started_weight");

    // With no weights at all, fall back to an even split; otherwise a missing
    // weight is zero.
    let (complete, active, not_started) =
        if complete.is_none() && active.is_none() && not_started.is_none() {
            (1.0, 1.0, 1.0)
        } else {
            (
                complete.unwrap_or(0.0),
                active.unwrap_or(0.0),
                not_started.unwrap_or(0.0),
            )
        };

    let weights = [complete, active, not_started];
    if weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
        bag.error(
            "GEN-PROGRESS-WEIGHTS",
            format!("{path}.progress"),
            "workflow.progress_counters mixture weights must be finite and non-negative"
                .to_string(),
        );
        return None;
    }
    let total: f64 = weights.iter().sum();
    if total <= 0.0 {
        bag.error(
            "GEN-PROGRESS-WEIGHTS",
            format!("{path}.progress"),
            "workflow.progress_counters mixture weights sum to zero; at least one weight must be positive".to_string(),
        );
        return None;
    }

    Some(StateMix::Weighted {
        complete,
        active,
        not_started,
    })
}

/// Build the five independent per-row streams, keyed on the table name behind
/// distinct operator prefixes so they stay stable and mutually independent.
fn build_streams(
    context: &CompileContext<'_>,
) -> (ChaCha8Rng, ChaCha8Rng, ChaCha8Rng, ChaCha8Rng, ChaCha8Rng) {
    let table = context.table().name.as_str();
    let stream = |suffix: &str| {
        context.rng(StreamId::operator(
            table,
            "workflow.progress_counters",
            format!("workflow.progress_counters.{suffix}"),
        ))
    };
    (
        stream("total"),
        stream("state"),
        stream("progress"),
        stream("status"),
        stream("completion"),
    )
}

/// Build the verification predicates the planner guarantees over its columns.
fn build_predicates(
    slots: &[Slot],
    partition: Partition,
    completed_statuses: &[String],
    active_statuses: &[String],
) -> Vec<PlannerPredicate> {
    let name_of = |role: Role| {
        slots
            .iter()
            .find(|slot| slot.role == role)
            .map(|slot| slot.name.clone())
    };
    let mut predicates = Vec::new();

    let total = name_of(Role::Total);
    let processed = name_of(Role::Processed);
    let succeeded = name_of(Role::Succeeded);
    let failed = name_of(Role::Failed);
    let pending = name_of(Role::Pending);
    let status = name_of(Role::Status);
    let completed_at = name_of(Role::CompletedAt);

    // succeeded + failed == processed (only exact under `partition: exact`).
    if let (Partition::Exact, Some(processed), Some(succeeded), Some(failed)) =
        (partition, &processed, &succeeded, &failed)
    {
        predicates.push(PlannerPredicate::CounterSum {
            addends: vec![succeeded.clone(), failed.clone()],
            sum: processed.clone(),
            guard: None,
        });
    }

    // processed + pending == total.
    if let (Some(total), Some(processed), Some(pending)) = (&total, &processed, &pending) {
        predicates.push(PlannerPredicate::CounterSum {
            addends: vec![processed.clone(), pending.clone()],
            sum: total.clone(),
            guard: None,
        });
    }

    // All counters are non-negative.
    let counters: Vec<String> = slots
        .iter()
        .filter(|slot| is_counter(slot.role))
        .map(|slot| slot.name.clone())
        .collect();
    if !counters.is_empty() {
        predicates.push(PlannerPredicate::NonNegative { columns: counters });
    }

    // State constraints tied to the status column and completion timestamp:
    // a completed row is fully processed with a non-null completion; an active
    // row leaves completion null.
    if let Some(status) = &status {
        if let (Some(total), Some(processed)) = (&total, &processed) {
            for value in completed_statuses {
                predicates.push(PlannerPredicate::CounterSum {
                    addends: vec![processed.clone()],
                    sum: total.clone(),
                    guard: Some(PredicateGuard::Equals {
                        column: status.clone(),
                        value: value.clone(),
                    }),
                });
            }
        }
        if let Some(completed_at) = &completed_at {
            for value in completed_statuses {
                predicates.push(PlannerPredicate::NotNullWhen {
                    column: completed_at.clone(),
                    guard: PredicateGuard::Equals {
                        column: status.clone(),
                        value: value.clone(),
                    },
                });
            }
            for value in active_statuses {
                predicates.push(PlannerPredicate::NullWhen {
                    column: completed_at.clone(),
                    guard: PredicateGuard::Equals {
                        column: status.clone(),
                        value: value.clone(),
                    },
                });
            }
        }
    }

    predicates
}

// --- Small helpers ----------------------------------------------------------

/// Whether a role is an integer counter (as opposed to status/timestamp).
fn is_counter(role: Role) -> bool {
    matches!(
        role,
        Role::Total | Role::Processed | Role::Succeeded | Role::Failed | Role::Pending
    )
}

/// The largest non-negative integer a column of `family` can hold.
fn family_capacity(family: &SqlTypeFamily) -> i128 {
    match family {
        SqlTypeFamily::Integer => i32::MAX as i128,
        SqlTypeFamily::BigInteger => i64::MAX as i128,
        _ => i128::MAX,
    }
}

/// The column name a `columns:` mapping assigns to `role`, if any.
fn role_name<'a>(columns: Option<&'a Value>, role: &str) -> Option<&'a str> {
    columns?.get(role).and_then(Value::as_str)
}

fn find_column<'a>(table: &'a PortableTable, name: &str) -> Option<&'a PortableColumn> {
    table.columns.iter().find(|column| column.name == name)
}

fn column_nullable(table: &PortableTable, name: &str) -> bool {
    find_column(table, name).is_some_and(|column| column.nullable)
}

/// Parse a YAML value into a list of strings (from a sequence of scalars).
fn string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Sequence(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

fn as_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(i128::from))
            .or_else(|| number.as_f64().map(|float| float as i128)),
        Value::String(text) => text.trim().parse::<i128>().ok(),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}
