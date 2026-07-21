//! Common same-table temporal planners plus the relationship/hierarchy
//! planners that build on the same execution pattern.
//!
//! Temporal planners include `temporal.timestamps` (a created/updated pair, plus
//! optional trailing timestamps), `temporal.soft_delete` (a coherent
//! `deleted_at`/`is_deleted` pair), and `temporal.lifecycle` (a status column
//! that only ever reaches legal states, each carrying a correctly-ordered
//! timestamp).
//!
//! Relationship and hierarchy planners include `hierarchy.tree` (a self-referential
//! parent_id tree with configurable-ratio roots and bounded depth/branching)
//! and the cross-table FK-side planners `relation.junction_pair` (unique
//! `(left, right)` edges via a deterministic pair-index permutation),
//! `relation.polymorphic_pair` (an atomic `(type, id)` pair choosing a target
//! table then a valid key in it), and `relation.tenant_family` (a same-tenant
//! foreign key drawn from the child's own tenant partition of the parent). The
//! cross-table planners cannot see the tables they reference, so the compiler
//! injects the referenced parent counts and dense key recipes as facts under
//! [`RELATION_FACTS_KEY`], driven off the descriptor's `cross_table` capability.
//!
//! # Shared machinery, separate planners
//!
//! Each planner is registered under its own `kind` with its own factory and
//! compiled type — no option-heavy mega-planner. What they share is *how* a
//! timestamp is chosen: [`InstantDraw`] picks a base instant (a bounded
//! random draw or a monotonically increasing one), and [`OffsetDraw`] picks a
//! non-negative whole-unit offset added to a base instant. Every timestamp is
//! reduced to a single internal UTC instant measured in nanoseconds since the
//! Unix epoch (an `i128`), so every "B happens no earlier than A" invariant is
//! pure checked integer arithmetic, matching the `temporal.interval` approach.
//!
//! # Owned columns and streams
//!
//! Each planner owns exactly the columns named under its `columns:` mapping;
//! the model compiler turns those into ownership claims, so a colliding
//! generator raises `GEN-COLUMN-OWNER-CONFLICT`. Every row draws its
//! constituent choices from independent, stably-keyed seed streams in a fixed
//! order — regardless of which branch a row lands in — so a seeded run
//! reproduces exactly.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use serde_yaml_ng::Value;

use crate::diagnostic::DiagnosticBag;
use crate::generate::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledPlanner, Determinism,
    PlannerDescriptor, PlannerFactory, PlannerPredicate, PredicateGuard, Verification,
};
use crate::generate::seed::StreamId;
use crate::generate::value::{GenerateError, GeneratedValue};
use crate::synthetic::model::PlannerConfig;
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

/// Nanoseconds per second, the base unit conversion for every offset.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

// =============================================================================
// Shared draws
// =============================================================================

/// How a row's base instant is chosen — shared by every planner's "starting
/// point" column (`temporal.timestamps`'s `created`, `temporal.soft_delete`'s
/// `deleted_range`, `temporal.lifecycle`'s `start`).
#[derive(Clone, Copy)]
enum InstantDraw {
    /// A uniformly random instant in the inclusive `[min_ns, max_ns]` range.
    Range { min_ns: i128, max_ns: i128 },
    /// A strictly increasing instant: row `n` starts at `min_ns + n * step_ns`.
    Monotonic { min_ns: i128, step_ns: i128 },
}

impl InstantDraw {
    fn draw(&self, rng: &mut ChaCha8Rng, row_index: u64) -> i128 {
        match *self {
            InstantDraw::Range { min_ns, max_ns } => rng.random_range(min_ns..=max_ns),
            InstantDraw::Monotonic { min_ns, step_ns } => {
                min_ns.saturating_add(step_ns.saturating_mul(row_index as i128))
            }
        }
    }
}

/// How a non-negative whole-unit offset added to a base instant is chosen —
/// shared by every "delay after X" column.
#[derive(Clone, Copy)]
enum OffsetDraw {
    Fixed(i128),
    Uniform { min: i128, max: i128 },
}

impl OffsetDraw {
    fn draw(&self, rng: &mut ChaCha8Rng) -> i128 {
        match *self {
            OffsetDraw::Fixed(value) => value,
            OffsetDraw::Uniform { min, max } => {
                if max <= min {
                    min
                } else {
                    rng.random_range(min..=max)
                }
            }
        }
    }
}

/// Compile an `{ kind, min, max }` / `{ kind: monotonic, min, step_seconds }`
/// block into an [`InstantDraw`]. Shared by `created`, `deleted_range`, and
/// `start`.
fn compile_instant_block(
    block: Option<&Value>,
    planner: &str,
    code: &'static str,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<InstantDraw> {
    let kind = block.and_then(|b| b.get("kind")).and_then(Value::as_str);
    let min_ns = block
        .and_then(|b| b.get("min"))
        .and_then(as_instant_ns)
        .or_else(|| {
            bag.error(
                code,
                format!("{path}.min"),
                format!("{planner} requires a parseable `min` timestamp"),
            );
            None
        })?;

    match kind {
        Some("monotonic") => {
            let step_seconds = block
                .and_then(|b| b.get("step_seconds"))
                .and_then(as_i128)
                .unwrap_or(1)
                .max(1);
            Some(InstantDraw::Monotonic {
                min_ns,
                step_ns: step_seconds.saturating_mul(NANOS_PER_SECOND),
            })
        }
        // `range`, or an omitted kind: a bounded random draw.
        _ => {
            let max_ns = block
                .and_then(|b| b.get("max"))
                .and_then(as_instant_ns)
                .or_else(|| {
                    bag.error(
                        code,
                        format!("{path}.max"),
                        format!("{planner} range requires a parseable `max` timestamp"),
                    );
                    None
                })?;
            if max_ns < min_ns {
                bag.error(
                    code,
                    path.to_string(),
                    format!("{planner} `max` is before `min`"),
                );
                return None;
            }
            Some(InstantDraw::Range { min_ns, max_ns })
        }
    }
}

/// Compile a `{ kind, unit, value | min, max }` block into an [`OffsetDraw`]
/// plus its unit size in nanoseconds. Shared by every delay/step block.
fn compile_offset_block(
    block: Option<&Value>,
    planner: &str,
    code: &'static str,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<(OffsetDraw, i128)> {
    let unit = block
        .and_then(|b| b.get("unit"))
        .and_then(Value::as_str)
        .unwrap_or("seconds");
    let Some(unit_nanos) = unit_nanos(unit) else {
        bag.error(
            code,
            format!("{path}.unit"),
            format!("{planner} `unit` `{unit}` is not a recognized time unit"),
        );
        return None;
    };

    let kind = block.and_then(|b| b.get("kind")).and_then(Value::as_str);
    let draw = match kind {
        Some("fixed") => {
            let value = block
                .and_then(|b| b.get("value"))
                .and_then(as_i128)
                .unwrap_or(0);
            check_nonneg_bounded(value, unit_nanos, planner, code, path, bag);
            OffsetDraw::Fixed(value)
        }
        // `uniform`, or an omitted kind.
        _ => {
            let min = block
                .and_then(|b| b.get("min"))
                .and_then(as_i128)
                .unwrap_or(0);
            let max = block
                .and_then(|b| b.get("max"))
                .and_then(as_i128)
                .unwrap_or(min);
            check_nonneg_bounded(min, unit_nanos, planner, code, path, bag);
            check_nonneg_bounded(max, unit_nanos, planner, code, path, bag);
            if max < min {
                bag.error(
                    code,
                    path.to_string(),
                    format!("{planner} `max` is below `min`"),
                );
            }
            OffsetDraw::Uniform { min, max }
        }
    };
    Some((draw, unit_nanos))
}

/// Report a negative offset bound or one whose nanosecond span overflows.
fn check_nonneg_bounded(
    units: i128,
    unit_nanos: i128,
    planner: &str,
    code: &'static str,
    path: &str,
    bag: &mut DiagnosticBag,
) {
    if units < 0 {
        bag.error(
            code,
            path.to_string(),
            format!("{planner} offset `{units}` is negative; offsets must be >= 0"),
        );
    } else if units.checked_mul(unit_nanos).is_none() {
        bag.error(
            code,
            path.to_string(),
            format!(
                "{planner} offset `{units}` overflows the representable nanosecond range at this unit"
            ),
        );
    }
}

/// Add a non-negative whole-unit offset to a base instant, in nanoseconds.
fn add_offset(base_ns: i128, offset_units: i128, unit_nanos: i128) -> Result<i128, GenerateError> {
    let span_ns = offset_units
        .checked_mul(unit_nanos)
        .ok_or_else(|| offset_overflow("offset multiplication"))?;
    base_ns
        .checked_add(span_ns)
        .ok_or_else(|| offset_overflow("base + offset addition"))
}

fn offset_overflow(step: &str) -> GenerateError {
    GenerateError::Overflow(format!(
        "temporal planner: {step} overflows the representable instant range"
    ))
}

// =============================================================================
// Rendering
// =============================================================================

/// Render an epoch-nanosecond instant to UTC wall-clock text, in the
/// representation `family` expects.
fn render_instant(
    instant_ns: i128,
    family: &SqlTypeFamily,
) -> Result<GeneratedValue, GenerateError> {
    let text = format_instant(instant_ns)?;
    Ok(match family {
        SqlTypeFamily::DateTime => GeneratedValue::DateTime(text),
        _ => GeneratedValue::Text(text),
    })
}

/// Format an epoch-nanosecond instant as UTC wall-clock text. An instant
/// outside chrono's representable timestamp range is an error rather than a
/// silent fallback to the 1970 epoch (which would break ordering invariants).
fn format_instant(instant_ns: i128) -> Result<String, GenerateError> {
    let secs = instant_ns.div_euclid(NANOS_PER_SECOND);
    let nanos = instant_ns.rem_euclid(NANOS_PER_SECOND) as u32;
    let secs = i64::try_from(secs).map_err(|_| instant_overflow())?;
    let utc = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(instant_overflow)?;
    Ok(utc.format("%Y-%m-%d %H:%M:%S").to_string())
}

fn instant_overflow() -> GenerateError {
    GenerateError::Overflow(
        "temporal planner: instant is outside the representable timestamp range".to_string(),
    )
}

/// Render a boolean flag in the representation `family` expects.
fn render_flag(flag: bool, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Boolean => GeneratedValue::Boolean(flag),
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(flag as i128),
        _ => GeneratedValue::Text(flag.to_string()),
    }
}

/// Render a status label in the representation `family` expects.
fn render_status(status: &str, _family: &SqlTypeFamily) -> GeneratedValue {
    GeneratedValue::Text(status.to_string())
}

// =============================================================================
// Value parsing helpers
// =============================================================================

fn unit_nanos(unit: &str) -> Option<i128> {
    let nanos = match unit {
        "nanosecond" | "nanoseconds" | "ns" => 1,
        "microsecond" | "microseconds" | "us" => 1_000,
        "millisecond" | "milliseconds" | "ms" => 1_000_000,
        "second" | "seconds" | "sec" | "s" => NANOS_PER_SECOND,
        "minute" | "minutes" | "min" => 60 * NANOS_PER_SECOND,
        "hour" | "hours" | "hr" | "h" => 3_600 * NANOS_PER_SECOND,
        "day" | "days" | "d" => 86_400 * NANOS_PER_SECOND,
        _ => return None,
    };
    Some(nanos)
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

/// Parse a timestamp value into epoch nanoseconds. Accepts RFC 3339
/// (`2024-01-01T00:00:00Z`), space- or `T`-separated naive timestamps, and bare
/// dates (interpreted as UTC midnight).
fn as_instant_ns(value: &Value) -> Option<i128> {
    let text = value.as_str()?.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(
            i128::from(dt.timestamp()) * NANOS_PER_SECOND + i128::from(dt.timestamp_subsec_nanos()),
        );
    }
    for format in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Some(i128::from(naive.and_utc().timestamp()) * NANOS_PER_SECOND);
        }
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|naive| i128::from(naive.and_utc().timestamp()) * NANOS_PER_SECOND)
}

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

/// Parse a YAML sequence of numbers into a list of `f64` weights.
fn number_list(value: Option<&Value>) -> Option<Vec<f64>> {
    match value {
        Some(Value::Sequence(items)) => items.iter().map(as_f64).collect(),
        _ => None,
    }
}

// =============================================================================
// temporal.timestamps
// =============================================================================

/// Static description of the `temporal.timestamps` planner.
pub static TEMPORAL_TIMESTAMPS_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "temporal.timestamps",
    aliases: &[],
    summary: "Coordinates created_at/updated_at (plus optional trailing timestamps) so update never precedes creation.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps created_at, updated_at, and optional trailing timestamp roles to columns.",
        },
        ArgumentSpec {
            name: "created",
            required: true,
            summary: "Configures the created timestamp range or monotonic sequence.",
        },
        ArgumentSpec {
            name: "update_delay",
            required: false,
            summary: "Configures the non-negative delay from creation to update.",
        },
        ArgumentSpec {
            name: "other_delay",
            required: false,
            summary: "Configures the delay from creation for additional timestamp roles.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `temporal.timestamps` planner.
pub struct TemporalTimestampsFactory;

impl PlannerFactory for TemporalTimestampsFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &TEMPORAL_TIMESTAMPS_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_timestamps(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

/// A resolved trailing "other" timestamp column, drawn from its own stream as
/// `created + offset`.
struct OtherColumn {
    family: SqlTypeFamily,
    rng: ChaCha8Rng,
}

struct TemporalTimestampsPlanner {
    writes: Vec<String>,
    created_family: SqlTypeFamily,
    updated_family: SqlTypeFamily,
    created_draw: InstantDraw,
    update_delay: OffsetDraw,
    update_unit_nanos: i128,
    other_delay: OffsetDraw,
    other_unit_nanos: i128,
    others: Vec<OtherColumn>,
    created_rng: ChaCha8Rng,
    update_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for TemporalTimestampsPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        let created_ns = self.created_draw.draw(&mut self.created_rng, row_index);
        let update_offset = self.update_delay.draw(&mut self.update_rng);
        let updated_ns = add_offset(created_ns, update_offset, self.update_unit_nanos)?;

        output[0] = render_instant(created_ns, &self.created_family)?;
        output[1] = render_instant(updated_ns, &self.updated_family)?;

        for (slot, other) in output[2..].iter_mut().zip(self.others.iter_mut()) {
            let offset = self.other_delay.draw(&mut other.rng);
            let other_ns = add_offset(created_ns, offset, self.other_unit_nanos)?;
            *slot = render_instant(other_ns, &other.family)?;
        }
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

fn compile_timestamps(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<TemporalTimestampsPlanner, DiagnosticBag> {
    const CODE: &str = crate::diagnostic::codes::TIMESTAMPS_COLUMN_MISSING.code;
    const RANGE_CODE: &str = crate::diagnostic::codes::TIMESTAMPS_RANGE.code;
    const DELAY_CODE: &str = crate::diagnostic::codes::TIMESTAMPS_DELAY.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");

    let created_col = resolve_required(columns, "created_at", table, path, CODE, &mut bag);
    let updated_col = resolve_required(columns, "updated_at", table, path, CODE, &mut bag);
    // Every `columns` entry beyond `created_at`/`updated_at` is a trailing
    // "other" timestamp, keyed by an arbitrary role name — kept as a flat
    // mapping (not a nested `others:` list) so the pre-compile column-ownership
    // scan (which only reads one level of `columns` mapping values) sees every
    // owned column. `other_cols` (and so `writes()`'s trailing columns) end up
    // in `serde_yaml_ng::Mapping` iteration order, i.e. the order the `others`
    // keys appear in the model's YAML text. That's deterministic for a given
    // model file (seeded runs still reproduce exactly), but reordering those
    // keys in the YAML reorders the produced columns too.
    let mut other_cols: Vec<&PortableColumn> = Vec::new();
    if let Some(Value::Mapping(map)) = columns {
        for (key, value) in map {
            let Some(role) = key.as_str() else { continue };
            if role == "created_at" || role == "updated_at" {
                continue;
            }
            let Some(name) = value.as_str() else { continue };
            match find_column(table, name) {
                Some(column) => other_cols.push(column),
                None => {
                    bag.error(
                        CODE,
                        format!("{path}.columns.{role}"),
                        format!(
                            "temporal.timestamps `{role}` column `{name}` does not exist on table `{}`",
                            table.name
                        ),
                    );
                }
            }
        }
    }

    let created_draw = compile_instant_block(
        config.args.get("created"),
        "temporal.timestamps `created`",
        RANGE_CODE,
        &format!("{path}.created"),
        &mut bag,
    );
    let (update_delay, update_unit_nanos) = compile_offset_block(
        config.args.get("update_delay"),
        "temporal.timestamps `update_delay`",
        DELAY_CODE,
        &format!("{path}.update_delay"),
        &mut bag,
    )
    .unzip();
    let other_delay_block = config
        .args
        .get("other_delay")
        .or(config.args.get("update_delay"));
    let (other_delay, other_unit_nanos) = compile_offset_block(
        other_delay_block,
        "temporal.timestamps `other_delay`",
        DELAY_CODE,
        &format!("{path}.other_delay"),
        &mut bag,
    )
    .unzip();

    if bag.has_errors() {
        return Err(bag);
    }

    let created = created_col.expect("created resolved without errors");
    let updated = updated_col.expect("updated resolved without errors");
    let created_draw = created_draw.expect("created draw resolved without errors");
    let update_delay = update_delay.expect("update delay resolved without errors");
    let update_unit_nanos = update_unit_nanos.expect("update unit resolved without errors");
    let other_delay = other_delay.expect("other delay resolved without errors");
    let other_unit_nanos = other_unit_nanos.expect("other unit resolved without errors");

    let mut writes = vec![created.name.clone(), updated.name.clone()];
    let mut others = Vec::with_capacity(other_cols.len());
    let mut other_predicates = Vec::with_capacity(other_cols.len());
    for column in other_cols {
        writes.push(column.name.clone());
        others.push(OtherColumn {
            family: column.family.clone(),
            rng: context.rng(StreamId::operator(
                table.name.as_str(),
                column.name.clone(),
                "temporal.timestamps.other",
            )),
        });
        other_predicates.push(PlannerPredicate::Ordering {
            earlier: created.name.clone(),
            later: column.name.clone(),
            guard: None,
        });
    }

    let created_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        created.name.clone(),
        "temporal.timestamps.created",
    ));
    let update_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        updated.name.clone(),
        "temporal.timestamps.update",
    ));

    let mut predicates = vec![PlannerPredicate::Ordering {
        earlier: created.name.clone(),
        later: updated.name.clone(),
        guard: None,
    }];
    predicates.extend(other_predicates);
    if let InstantDraw::Range { min_ns, max_ns } = created_draw {
        predicates.push(PlannerPredicate::InRange {
            column: created.name.clone(),
            min_nanos: min_ns,
            max_nanos: max_ns,
        });
    }

    Ok(TemporalTimestampsPlanner {
        writes,
        created_family: created.family.clone(),
        updated_family: updated.family.clone(),
        created_draw,
        update_delay,
        update_unit_nanos,
        other_delay,
        other_unit_nanos,
        others,
        created_rng,
        update_rng,
        predicates,
    })
}

/// Resolve a required column role to its schema column, reporting a missing
/// role or a role naming an absent column.
fn resolve_required<'a>(
    columns: Option<&Value>,
    role: &str,
    table: &'a PortableTable,
    path: &str,
    code: &'static str,
    bag: &mut DiagnosticBag,
) -> Option<&'a PortableColumn> {
    let Some(name) = role_name(columns, role) else {
        bag.error(
            code,
            format!("{path}.columns.{role}"),
            format!("requires a `{role}` column under `columns`"),
        );
        return None;
    };
    let column = find_column(table, name);
    if column.is_none() {
        bag.error(
            code,
            format!("{path}.columns.{role}"),
            format!(
                "`{role}` column `{name}` does not exist on table `{}`",
                table.name
            ),
        );
    }
    column
}

// =============================================================================
// temporal.soft_delete
// =============================================================================

/// Static description of the `temporal.soft_delete` planner.
///
/// Note: the null/non-null coherence between `deleted_at` and `is_deleted` is
/// only surfaced as a verification predicate when an `is_deleted` flag column
/// is configured — see [`build_soft_delete_predicates`]. A `deleted_at`-only
/// model still gets `InRange` coverage on `deleted_at`, but not the
/// null-iff-not-deleted / non-null-iff-deleted split, since there is no guard
/// column to state it against.
pub static TEMPORAL_SOFT_DELETE_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "temporal.soft_delete",
    aliases: &[],
    summary: "Coordinates a deleted_at timestamp and optional is_deleted flag so a deletion probability produces a coherent null/non-null pair.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps the required deleted_at role and optional is_deleted role to columns.",
        },
        ArgumentSpec {
            name: "deletion_probability",
            required: false,
            summary: "Probability that a generated row is marked deleted.",
        },
        ArgumentSpec {
            name: "deleted_range",
            required: true,
            summary: "Configures the range or monotonic sequence for deletion timestamps.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `temporal.soft_delete` planner.
pub struct TemporalSoftDeleteFactory;

impl PlannerFactory for TemporalSoftDeleteFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &TEMPORAL_SOFT_DELETE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_soft_delete(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

struct FlagColumn {
    name: String,
    family: SqlTypeFamily,
}

struct TemporalSoftDeletePlanner {
    writes: Vec<String>,
    deleted_at_family: SqlTypeFamily,
    flag: Option<FlagColumn>,
    deletion_probability: f64,
    deleted_draw: InstantDraw,
    decision_rng: ChaCha8Rng,
    instant_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for TemporalSoftDeletePlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Draw both streams unconditionally, in a fixed order, so a seeded run
        // reproduces regardless of which rows land deleted.
        let decision = self.decision_rng.random::<f64>();
        let deleted_ns = self.deleted_draw.draw(&mut self.instant_rng, row_index);
        let is_deleted = self.deletion_probability > 0.0 && decision < self.deletion_probability;

        output[0] = if is_deleted {
            render_instant(deleted_ns, &self.deleted_at_family)?
        } else {
            GeneratedValue::Null
        };
        if self.flag.is_some() {
            output[1] = render_flag(is_deleted, &output_flag_family(&self.flag));
        }
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

fn output_flag_family(flag: &Option<FlagColumn>) -> SqlTypeFamily {
    flag.as_ref()
        .map(|f| f.family.clone())
        .unwrap_or(SqlTypeFamily::Boolean)
}

fn compile_soft_delete(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<TemporalSoftDeletePlanner, DiagnosticBag> {
    const CODE: &str = crate::diagnostic::codes::SOFT_DELETE_COLUMN_MISSING.code;
    const RANGE_CODE: &str = crate::diagnostic::codes::SOFT_DELETE_RANGE.code;
    const NULLABILITY_CODE: &str = crate::diagnostic::codes::SOFT_DELETE_NULLABILITY.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");

    let deleted_col = resolve_required(columns, "deleted_at", table, path, CODE, &mut bag);
    let flag_col = role_name(columns, "is_deleted").and_then(|name| {
        find_column(table, name).or_else(|| {
            bag.error(
                CODE,
                format!("{path}.columns.is_deleted"),
                format!(
                    "temporal.soft_delete `is_deleted` column `{name}` does not exist on table `{}`",
                    table.name
                ),
            );
            None
        })
    });

    let deletion_probability = config
        .args
        .get("deletion_probability")
        .and_then(as_f64)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);

    // A non-nullable deleted_at cannot represent a row that isn't deleted,
    // which happens whenever some probability mass falls on "not deleted".
    if deletion_probability < 1.0 {
        if let Some(deleted) = deleted_col {
            if !deleted.nullable {
                bag.error(
                    NULLABILITY_CODE,
                    format!("{path}.deletion_probability"),
                    format!(
                        "temporal.soft_delete has deletion_probability {deletion_probability} but its `deleted_at` column `{}` is not nullable; a non-deleted row needs a null deleted_at",
                        deleted.name
                    ),
                );
            }
        }
    }

    let deleted_draw = compile_instant_block(
        config.args.get("deleted_range"),
        "temporal.soft_delete `deleted_range`",
        RANGE_CODE,
        &format!("{path}.deleted_range"),
        &mut bag,
    );

    if bag.has_errors() {
        return Err(bag);
    }

    let deleted = deleted_col.expect("deleted resolved without errors");
    let deleted_draw = deleted_draw.expect("deleted draw resolved without errors");

    let mut writes = vec![deleted.name.clone()];
    let flag = flag_col.map(|column| {
        writes.push(column.name.clone());
        FlagColumn {
            name: column.name.clone(),
            family: column.family.clone(),
        }
    });

    let decision_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        deleted.name.clone(),
        "temporal.soft_delete.decision",
    ));
    let instant_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        deleted.name.clone(),
        "temporal.soft_delete.instant",
    ));

    let predicates = build_soft_delete_predicates(
        &deleted.name,
        flag.as_ref(),
        deletion_probability,
        deleted_draw,
    );

    Ok(TemporalSoftDeletePlanner {
        writes,
        deleted_at_family: deleted.family.clone(),
        flag,
        deletion_probability,
        deleted_draw,
        decision_rng,
        instant_rng,
        predicates,
    })
}

fn build_soft_delete_predicates(
    deleted_at: &str,
    flag: Option<&FlagColumn>,
    _deletion_probability: f64,
    deleted_draw: InstantDraw,
) -> Vec<PlannerPredicate> {
    let mut predicates = Vec::new();

    // Every `PlannerPredicate::NullWhen`/`NotNullWhen` requires a guard, so the
    // null/non-null coherence invariant is only statable when an explicit flag
    // column names which rows are which (mirrors `temporal.interval`'s open
    // flag handling: without a flag, "null when open" would be a tautology
    // over the column itself).
    if let Some(flag) = flag {
        predicates.push(PlannerPredicate::NotNullWhen {
            column: deleted_at.to_string(),
            guard: PredicateGuard::Flag {
                column: flag.name.clone(),
                value: true,
            },
        });
        predicates.push(PlannerPredicate::NullWhen {
            column: deleted_at.to_string(),
            guard: PredicateGuard::Flag {
                column: flag.name.clone(),
                value: false,
            },
        });
    }

    if let InstantDraw::Range { min_ns, max_ns } = deleted_draw {
        predicates.push(PlannerPredicate::InRange {
            column: deleted_at.to_string(),
            min_nanos: min_ns,
            max_nanos: max_ns,
        });
    }

    predicates
}

// =============================================================================
// temporal.lifecycle
// =============================================================================

/// Static description of the `temporal.lifecycle` planner.
pub static TEMPORAL_LIFECYCLE_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "temporal.lifecycle",
    aliases: &[],
    summary: "Coordinates a status column that only reaches legal states, each carrying a correctly-ordered timestamp.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps the status role and each lifecycle state to its timestamp column.",
        },
        ArgumentSpec {
            name: "states",
            required: true,
            summary: "Ordered lifecycle state vocabulary.",
        },
        ArgumentSpec {
            name: "weights",
            required: false,
            summary: "Terminal-state weights in the same order as states.",
        },
        ArgumentSpec {
            name: "start",
            required: true,
            summary: "Configures the first lifecycle timestamp range or sequence.",
        },
        ArgumentSpec {
            name: "step",
            required: false,
            summary: "Configures the non-negative delay between lifecycle states.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `temporal.lifecycle` planner.
pub struct TemporalLifecycleFactory;

impl PlannerFactory for TemporalLifecycleFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &TEMPORAL_LIFECYCLE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_lifecycle(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

/// A state timestamp column mapped to its position in the legal chain.
struct StateSlot {
    /// Index into the planner's `states` chain.
    state_index: usize,
    name: String,
    family: SqlTypeFamily,
}

struct TemporalLifecyclePlanner {
    writes: Vec<String>,
    status_family: SqlTypeFamily,
    states: Vec<String>,
    /// Normalized terminal-state weights, parallel to `states`.
    weights: Vec<f64>,
    /// Timestamp slots, in the write order after `status`.
    slots: Vec<StateSlot>,
    start_draw: InstantDraw,
    step_delay: OffsetDraw,
    step_unit_nanos: i128,
    terminal_rng: ChaCha8Rng,
    start_rng: ChaCha8Rng,
    step_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for TemporalLifecyclePlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Every stream is drawn unconditionally and in a fixed order — the
        // terminal pick, the base instant, then one step offset per possible
        // transition — so a seeded run reproduces regardless of which state a
        // row terminates at.
        let terminal_pick = self.terminal_rng.random::<f64>();
        let base_ns = self.start_draw.draw(&mut self.start_rng, row_index);
        let mut instants = Vec::with_capacity(self.states.len());
        instants.push(base_ns);
        for _ in 1..self.states.len() {
            let offset = self.step_delay.draw(&mut self.step_rng);
            let previous = *instants
                .last()
                .expect("instants seeded with the base instant");
            instants.push(add_offset(previous, offset, self.step_unit_nanos)?);
        }

        let terminal_index = pick_weighted_index(&self.weights, terminal_pick);

        output[0] = render_status(&self.states[terminal_index], &self.status_family);
        for (slot, cell) in self.slots.iter().zip(output[1..].iter_mut()) {
            *cell = if slot.state_index <= terminal_index {
                render_instant(instants[slot.state_index], &slot.family)?
            } else {
                GeneratedValue::Null
            };
        }
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

/// Select an index from `weights` (assumed non-negative, summing to > 0)
/// using a uniform draw `r` in `[0, 1)`.
fn pick_weighted_index(weights: &[f64], r: f64) -> usize {
    let total: f64 = weights.iter().sum();
    if total <= 0.0 || weights.is_empty() {
        return 0;
    }
    let point = r * total;
    let mut cumulative = 0.0;
    for (index, weight) in weights.iter().enumerate() {
        cumulative += weight;
        if point < cumulative {
            return index;
        }
    }
    weights.len() - 1
}

fn compile_lifecycle(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<TemporalLifecyclePlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::LIFECYCLE_COLUMN_MISSING.code;
    const STATES_CODE: &str = crate::diagnostic::codes::LIFECYCLE_STATES.code;
    const VOCAB_CODE: &str = crate::diagnostic::codes::LIFECYCLE_STATUS_VOCABULARY.code;
    const WEIGHTS_CODE: &str = crate::diagnostic::codes::LIFECYCLE_WEIGHTS.code;
    const RANGE_CODE: &str = crate::diagnostic::codes::LIFECYCLE_RANGE.code;
    const STEP_CODE: &str = crate::diagnostic::codes::LIFECYCLE_STEP.code;
    const NULLABILITY_CODE: &str = crate::diagnostic::codes::LIFECYCLE_NULLABILITY.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");

    let status_col = resolve_required(columns, "status", table, path, COLUMN_CODE, &mut bag);

    let states = string_list(config.args.get("states"));
    if states.is_empty() {
        bag.error(
            STATES_CODE,
            format!("{path}.states"),
            "temporal.lifecycle requires a non-empty `states` chain".to_string(),
        );
    }
    let mut unique_states: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for state in &states {
        if !unique_states.insert(state.as_str()) {
            bag.error(
                STATES_CODE,
                format!("{path}.states"),
                format!("temporal.lifecycle `states` lists `{state}` more than once"),
            );
        }
    }

    // Every `columns` entry beyond `status` maps a state name to its timestamp
    // column — kept as a flat mapping (not a nested `timestamps:` map) so the
    // pre-compile column-ownership scan (which only reads one level of
    // `columns` mapping values) sees every owned column.
    let mut timestamp_slots: Vec<StateSlot> = Vec::new();
    if let Some(Value::Mapping(map)) = columns {
        for (key, value) in map {
            let Some(state_name) = key.as_str() else {
                continue;
            };
            if state_name == "status" {
                continue;
            }
            let Some(column_name) = value.as_str() else {
                continue;
            };
            let Some(state_index) = states.iter().position(|s| s == state_name) else {
                bag.error(
                    VOCAB_CODE,
                    format!("{path}.columns.{state_name}"),
                    format!(
                        "temporal.lifecycle `columns` names state `{state_name}`, which is not in `states`"
                    ),
                );
                continue;
            };
            match find_column(table, column_name) {
                Some(column) => timestamp_slots.push(StateSlot {
                    state_index,
                    name: column.name.clone(),
                    family: column.family.clone(),
                }),
                None => {
                    bag.error(
                        COLUMN_CODE,
                        format!("{path}.columns.{state_name}"),
                        format!(
                            "temporal.lifecycle `{state_name}` column `{column_name}` does not exist on table `{}`",
                            table.name
                        ),
                    );
                }
            }
        }
    }
    timestamp_slots.sort_by_key(|slot| slot.state_index);

    let weights = match number_list(config.args.get("weights")) {
        Some(weights) if weights.len() == states.len() => {
            if weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
                bag.error(
                    WEIGHTS_CODE,
                    format!("{path}.weights"),
                    "temporal.lifecycle `weights` must be finite and non-negative".to_string(),
                );
                None
            } else if weights.iter().sum::<f64>() <= 0.0 {
                bag.error(
                    WEIGHTS_CODE,
                    format!("{path}.weights"),
                    "temporal.lifecycle `weights` sum to zero; at least one weight must be positive"
                        .to_string(),
                );
                None
            } else {
                Some(weights)
            }
        }
        Some(_) => {
            bag.error(
                WEIGHTS_CODE,
                format!("{path}.weights"),
                "temporal.lifecycle `weights` must have exactly one entry per `states` element"
                    .to_string(),
            );
            None
        }
        None if !states.is_empty() => Some(vec![1.0; states.len()]),
        None => None,
    };

    // Impossible nullability: a state's timestamp column is non-nullable, but
    // some positive probability mass terminates before reaching that state,
    // leaving the column null.
    if let Some(weights) = &weights {
        let total: f64 = weights.iter().sum();
        let mut prefix = 0.0;
        for (index, state) in states.iter().enumerate() {
            if index > 0 && prefix > 0.0 && total > 0.0 {
                if let Some(slot) = timestamp_slots.iter().find(|s| s.state_index == index) {
                    if !column_nullable(table, &slot.name) {
                        bag.error(
                            NULLABILITY_CODE,
                            format!("{path}.columns.timestamps.{state}"),
                            format!(
                                "temporal.lifecycle `{state}` timestamp column `{}` is not nullable, but a row that terminates before `{state}` leaves it null",
                                slot.name
                            ),
                        );
                    }
                }
            }
            prefix += weights[index];
        }
    }

    let start_draw = compile_instant_block(
        config.args.get("start"),
        "temporal.lifecycle `start`",
        RANGE_CODE,
        &format!("{path}.start"),
        &mut bag,
    );
    let (step_delay, step_unit_nanos) = compile_offset_block(
        config.args.get("step"),
        "temporal.lifecycle `step`",
        STEP_CODE,
        &format!("{path}.step"),
        &mut bag,
    )
    .unzip();

    if bag.has_errors() {
        return Err(bag);
    }

    let status = status_col.expect("status resolved without errors");
    let weights = weights.expect("weights resolved without errors");
    let start_draw = start_draw.expect("start draw resolved without errors");
    let step_delay = step_delay.expect("step delay resolved without errors");
    let step_unit_nanos = step_unit_nanos.expect("step unit resolved without errors");

    let mut writes = vec![status.name.clone()];
    writes.extend(timestamp_slots.iter().map(|slot| slot.name.clone()));

    let terminal_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        status.name.clone(),
        "temporal.lifecycle.terminal",
    ));
    let start_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        status.name.clone(),
        "temporal.lifecycle.start",
    ));
    let step_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        status.name.clone(),
        "temporal.lifecycle.step",
    ));

    let predicates =
        build_lifecycle_predicates(&status.name, &states, &timestamp_slots, start_draw);

    Ok(TemporalLifecyclePlanner {
        writes,
        status_family: status.family.clone(),
        states,
        weights,
        slots: timestamp_slots,
        start_draw,
        step_delay,
        step_unit_nanos,
        terminal_rng,
        start_rng,
        step_rng,
        predicates,
    })
}

fn build_lifecycle_predicates(
    status: &str,
    states: &[String],
    slots: &[StateSlot],
    start_draw: InstantDraw,
) -> Vec<PlannerPredicate> {
    let mut predicates = Vec::new();

    for slot in slots {
        for (index, state) in states.iter().enumerate() {
            let guard = PredicateGuard::Equals {
                column: status.to_string(),
                value: state.clone(),
            };
            if index >= slot.state_index {
                predicates.push(PlannerPredicate::NotNullWhen {
                    column: slot.name.clone(),
                    guard,
                });
            } else {
                predicates.push(PlannerPredicate::NullWhen {
                    column: slot.name.clone(),
                    guard,
                });
            }
        }
    }

    for pair in slots.windows(2) {
        predicates.push(PlannerPredicate::Ordering {
            earlier: pair[0].name.clone(),
            later: pair[1].name.clone(),
            guard: None,
        });
    }

    if let (InstantDraw::Range { min_ns, max_ns }, Some(first)) =
        (start_draw, slots.iter().find(|slot| slot.state_index == 0))
    {
        predicates.push(PlannerPredicate::InRange {
            column: first.name.clone(),
            min_nanos: min_ns,
            max_nanos: max_ns,
        });
    }

    predicates
}

// =============================================================================
// hierarchy.tree
// =============================================================================

/// Static description of the `hierarchy.tree` planner.
///
/// A same-table planner despite touching a self-referential foreign key: it
/// owns exactly the `parent` column and references only EARLIER rows of its own
/// table, so it is `cross_table: false` and needs no cross-table fact injection.
pub static HIERARCHY_TREE_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "hierarchy.tree",
    aliases: &[],
    summary: "Builds a self-referential parent_id tree: configurable-ratio roots (null parent) and bounded-depth, bounded-branching descendants that reference an earlier row.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps the parent role to the nullable self-reference column.",
        },
        ArgumentSpec {
            name: "relationship",
            required: false,
            summary: "Self-relationship used to derive the referenced dense key recipe.",
        },
        ArgumentSpec {
            name: "key",
            required: false,
            summary: "Explicit dense key start and step, overriding relationship-derived values.",
        },
        ArgumentSpec {
            name: "root_ratio",
            required: false,
            summary: "Fraction of rows generated as roots with a null parent.",
        },
        ArgumentSpec {
            name: "max_depth",
            required: false,
            summary: "The deepest node's depth; roots are depth 0, so depths span 0..=max_depth.",
        },
        ArgumentSpec {
            name: "max_branching",
            required: false,
            summary: "Maximum number of direct children per parent; omitted means unbounded.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `hierarchy.tree` planner.
pub struct HierarchyTreeFactory;

impl PlannerFactory for HierarchyTreeFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &HIERARCHY_TREE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_tree(config, context).map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

/// The compiled `hierarchy.tree` planner. Rows are generated in index order, so
/// the planner accumulates the depth and remaining branching capacity of every
/// row it has already produced and only ever attaches a new non-root to an
/// earlier, still-eligible row.
struct HierarchyTreePlanner {
    writes: Vec<String>,
    parent_family: SqlTypeFamily,
    root_ratio: f64,
    max_depth: u32,
    /// Maximum children per node; `None` is unbounded.
    max_branching: Option<u32>,
    /// The self primary key's dense recipe: row `n`'s key is `key_start + n *
    /// key_step`. A non-root at row `n` references an earlier row `j`'s key.
    key_start: i128,
    key_step: i128,
    decision_rng: ChaCha8Rng,
    select_rng: ChaCha8Rng,
    /// The frontier of rows that can still parent a new child (depth < max_depth
    /// and, when bounded, remaining branching > 0). A node's depth and remaining
    /// branching are only ever read while it is eligible, so this is the *only*
    /// per-node state kept — memory stays bounded by the live frontier, not the
    /// total row count (`Buffering::Streaming`).
    eligible: Vec<EligibleNode>,
    predicates: Vec<PlannerPredicate>,
}

/// A row still on the tree frontier: its row index (for the child's parent key),
/// its depth, and how many more children it may still take.
struct EligibleNode {
    index: usize,
    depth: u32,
    remaining: u32,
}

impl HierarchyTreePlanner {
    /// The dense key of the row at `row_index`.
    fn key_of(&self, row_index: usize) -> i128 {
        self.key_start + self.key_step * row_index as i128
    }

    /// Render a parent key value in the representation the parent column expects.
    fn render_key(&self, key: i128) -> GeneratedValue {
        match self.parent_family {
            SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(key),
            _ => GeneratedValue::Text(key.to_string()),
        }
    }
}

impl CompiledPlanner for HierarchyTreePlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Draw both streams unconditionally and in a fixed order, so a seeded run
        // reproduces regardless of whether a row lands a root.
        let decision = self.decision_rng.random::<f64>();
        let selection = self.select_rng.random::<f64>();

        let n = row_index as usize;
        // The first row has no earlier row to attach to and is always a root;
        // otherwise a row is a root by the configured ratio, or forcibly when no
        // eligible parent remains within the depth/branching bounds.
        let is_root = n == 0 || decision < self.root_ratio || self.eligible.is_empty();

        let (parent_value, depth) = if is_root {
            (GeneratedValue::Null, 0)
        } else {
            let slot = (selection * self.eligible.len() as f64) as usize;
            let slot = slot.min(self.eligible.len() - 1);
            let parent_index = self.eligible[slot].index;
            let depth = self.eligible[slot].depth + 1;
            if self.max_branching.is_some() {
                self.eligible[slot].remaining -= 1;
                if self.eligible[slot].remaining == 0 {
                    self.eligible.swap_remove(slot);
                }
            }
            (self.render_key(self.key_of(parent_index)), depth)
        };

        // A node can parent future children only while it stays under the depth
        // bound (a child would sit at `depth + 1`).
        if depth < self.max_depth {
            self.eligible.push(EligibleNode {
                index: n,
                depth,
                remaining: self.max_branching.unwrap_or(u32::MAX).max(1),
            });
        }

        output[0] = parent_value;
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

fn compile_tree(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<HierarchyTreePlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::TREE_COLUMN_MISSING.code;
    const DEPTH_CODE: &str = crate::diagnostic::codes::TREE_DEPTH.code;
    const ROOT_CODE: &str = crate::diagnostic::codes::TREE_ROOT_RATIO.code;
    const BRANCH_CODE: &str = crate::diagnostic::codes::TREE_BRANCHING.code;
    const CYCLE_CODE: &str = crate::diagnostic::codes::TREE_REQUIRED_CYCLE.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");

    let parent_col = resolve_required(columns, "parent", table, path, COLUMN_CODE, &mut bag);

    // A non-nullable self-FK cannot hold the null a root needs, so every row
    // would have to reference another row: a required non-null self cycle with no
    // constructible seed. (A nullable parent lets roots be null, and every
    // non-root references an already-generated earlier row, so the FK is
    // satisfiable without deferral.)
    if let Some(parent) = parent_col {
        if !parent.nullable {
            bag.error(
                CYCLE_CODE,
                format!("{path}.columns.parent"),
                format!(
                    "hierarchy.tree parent column `{}` is not nullable, so tree roots (which need a null parent) cannot be represented; make it nullable to allow a constructible root seed",
                    parent.name
                ),
            );
        }
    }

    let root_ratio = config
        .args
        .get("root_ratio")
        .and_then(as_f64)
        .unwrap_or(0.1);
    if !(0.0..=1.0).contains(&root_ratio) {
        bag.error(
            ROOT_CODE,
            format!("{path}.root_ratio"),
            format!("hierarchy.tree `root_ratio` {root_ratio} must be within [0.0, 1.0]"),
        );
    }

    let max_depth = config.args.get("max_depth").and_then(as_i128).unwrap_or(6);
    if max_depth < 1 {
        bag.error(
            DEPTH_CODE,
            format!("{path}.max_depth"),
            format!("hierarchy.tree `max_depth` {max_depth} must be at least 1"),
        );
    }

    let max_branching = match config.args.get("max_branching").and_then(as_i128) {
        None => None,
        Some(branching) if branching >= 1 => Some(branching as u32),
        Some(branching) => {
            bag.error(
                BRANCH_CODE,
                format!("{path}.max_branching"),
                format!(
                    "hierarchy.tree `max_branching` {branching} must be at least 1 (omit it for unbounded branching)"
                ),
            );
            None
        }
    };

    // Derive the emitted parent_id key recipe from the ACTUAL primary key the
    // self-FK relationship references (a non-default `sequence` start/step must
    // be honored, or the produced parent_id values would not match any real
    // id). The compiler injects the referenced key's dense recipe under
    // `RELATION_FACTS_KEY`; an explicit `key:` block still wins over it, and a
    // plain default PK falls back to `1, 2, …`.
    let facts = config.args.get(RELATION_FACTS_KEY);
    let (derived_start, derived_step) = config
        .args
        .get("relationship")
        .and_then(Value::as_str)
        .and_then(|name| {
            facts
                .and_then(|f| f.get("relationships"))
                .and_then(|r| r.get(name))
        })
        .filter(|fact| fact.get("dense").and_then(Value::as_bool) == Some(true))
        .map(|fact| {
            (
                fact.get("start").and_then(as_i128).unwrap_or(1),
                fact.get("step").and_then(as_i128).unwrap_or(1),
            )
        })
        .unwrap_or((1, 1));

    let key = config.args.get("key");
    let key_start = key
        .and_then(|k| k.get("start"))
        .and_then(as_i128)
        .unwrap_or(derived_start);
    let key_step = key
        .and_then(|k| k.get("step"))
        .and_then(as_i128)
        .unwrap_or(derived_step);

    if bag.has_errors() {
        return Err(bag);
    }

    let parent = parent_col.expect("parent resolved without errors");
    let writes = vec![parent.name.clone()];
    let mut predicates = Vec::new();
    // Every produced key is `key_start + step * index` with non-negative index;
    // with a non-negative start and step the parent column is never negative
    // (roots are null, which the verifier does not treat as negative).
    if key_start >= 0 && key_step >= 0 {
        predicates.push(PlannerPredicate::NonNegative {
            columns: vec![parent.name.clone()],
        });
    }

    let decision_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        parent.name.clone(),
        "hierarchy.tree.decision",
    ));
    let select_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        parent.name.clone(),
        "hierarchy.tree.select",
    ));

    Ok(HierarchyTreePlanner {
        writes,
        parent_family: parent.family.clone(),
        root_ratio,
        max_depth: max_depth as u32,
        max_branching,
        key_start,
        key_step,
        decision_rng,
        select_rng,
        eligible: Vec::new(),
        predicates,
    })
}

// =============================================================================
// Cross-table FK-side planners: shared machinery
// =============================================================================

/// The private config key under which the compiler injects the resolved
/// parent-key facts an FK-side cross-table planner (`relation.junction_pair`,
/// `relation.tenant_family`, `relation.polymorphic_pair`) needs to produce valid
/// keys for tables it references but cannot see on its own. Documented as an
/// internal contract between [`super::super::compiler`] and these planners.
pub const RELATION_FACTS_KEY: &str = "__relation_facts";

/// A referenced table's resolved key domain: a dense integer sequence
/// `start, start + step, …`, `count` rows long. Row `i` (0-based) renders the
/// key `start + i * step`.
#[derive(Clone, Copy)]
struct DenseKey {
    start: i128,
    step: i128,
    count: u64,
}

impl DenseKey {
    /// The key of the referenced row at 0-based index `index`.
    fn key_at(&self, index: u64) -> i128 {
        self.start + self.step * index as i128
    }
}

/// Render an integer key value in the representation the FK column expects.
fn render_key(key: i128, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(key),
        _ => GeneratedValue::Text(key.to_string()),
    }
}

/// Resolve a relationship's injected fact into a [`DenseKey`], or report why it
/// cannot serve as a valid integer key domain (unknown relationship, or a
/// non-dense parent key such as a UUID).
fn dense_key_from_relationship(
    facts: Option<&Value>,
    rel_name: &str,
    planner: &str,
    role: &str,
    codes: [&'static str; 2],
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<DenseKey> {
    let [missing_code, key_code] = codes;
    let fact = facts
        .and_then(|f| f.get("relationships"))
        .and_then(|r| r.get(rel_name));
    let Some(fact) = fact else {
        bag.error(
            missing_code,
            format!("{path}.{role}"),
            format!(
                "{planner} `{role}` names relationship `{rel_name}`, which is not declared on this table"
            ),
        );
        return None;
    };
    dense_key_from_fact(fact, rel_name, planner, key_code, path, bag)
}

/// Resolve an already-located fact mapping into a [`DenseKey`].
fn dense_key_from_fact(
    fact: &Value,
    label: &str,
    planner: &str,
    key_code: &'static str,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<DenseKey> {
    let dense = fact.get("dense").and_then(Value::as_bool).unwrap_or(false);
    if !dense {
        bag.error(
            key_code,
            path.to_string(),
            format!(
                "{planner} target `{label}` does not have a dense integer key; only bare integer primary keys and `sequence` keys are supported"
            ),
        );
        return None;
    }
    let start = fact.get("start").and_then(as_i128).unwrap_or(1);
    let step = fact.get("step").and_then(as_i128).unwrap_or(1);
    let count = fact.get("count").and_then(Value::as_u64).unwrap_or(0);
    Some(DenseKey { start, step, count })
}

/// The greatest common divisor of two non-negative integers.
fn gcd(mut a: i128, mut b: i128) -> i128 {
    while b != 0 {
        (a, b) = (b, a.rem_euclid(b));
    }
    a.abs()
}

// =============================================================================
// relation.junction_pair
// =============================================================================

/// Static description of the `relation.junction_pair` planner.
pub static RELATION_JUNCTION_PAIR_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "relation.junction_pair",
    aliases: &[],
    summary: "Fills a junction row's two foreign keys with a UNIQUE (left, right) pair, using a deterministic pair-index permutation so edges never repeat.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps the left and right roles to the junction table's foreign-key columns.",
        },
        ArgumentSpec {
            name: "left_relationship",
            required: true,
            summary: "Relationship that supplies the left dense key domain.",
        },
        ArgumentSpec {
            name: "right_relationship",
            required: true,
            summary: "Relationship that supplies the right dense key domain.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: true,
};

/// Factory for the `relation.junction_pair` planner.
pub struct RelationJunctionPairFactory;

impl PlannerFactory for RelationJunctionPairFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &RELATION_JUNCTION_PAIR_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_junction_pair(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

/// The compiled `relation.junction_pair` planner. It maps the junction row index
/// `n` to a distinct index in `[0, left.count * right.count)` via the bijection
/// `idx(n) = (offset + n * stride) mod total` (with `gcd(stride, total) == 1`),
/// then decodes that index into a `(left_row, right_row)` pair. Distinct `n`
/// therefore always yield distinct pairs — uniqueness is by construction, not by
/// rejection sampling.
struct RelationJunctionPairPlanner {
    writes: Vec<String>,
    left_family: SqlTypeFamily,
    right_family: SqlTypeFamily,
    left: DenseKey,
    right: DenseKey,
    total: i128,
    stride: i128,
    offset: i128,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for RelationJunctionPairPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        if self.total <= 0 {
            output[0] = GeneratedValue::Null;
            output[1] = GeneratedValue::Null;
            return Ok(());
        }
        let idx = (self.offset + (row_index as i128) * self.stride).rem_euclid(self.total);
        let right_count = self.right.count as i128;
        let left_row = (idx.div_euclid(right_count)) as u64;
        let right_row = (idx.rem_euclid(right_count)) as u64;
        output[0] = render_key(self.left.key_at(left_row), &self.left_family);
        output[1] = render_key(self.right.key_at(right_row), &self.right_family);
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

fn compile_junction_pair(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<RelationJunctionPairPlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::JUNCTION_COLUMN_MISSING.code;
    const REL_CODE: &str = crate::diagnostic::codes::JUNCTION_RELATIONSHIP.code;
    const KEY_CODE: &str = crate::diagnostic::codes::JUNCTION_KEY_UNSUPPORTED.code;
    const EXHAUSTED_CODE: &str = crate::diagnostic::codes::JUNCTION_EXHAUSTED.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");
    let facts = config.args.get(RELATION_FACTS_KEY);

    let left_col = resolve_required(columns, "left", table, path, COLUMN_CODE, &mut bag);
    let right_col = resolve_required(columns, "right", table, path, COLUMN_CODE, &mut bag);

    let left_rel = config
        .args
        .get("left_relationship")
        .and_then(Value::as_str)
        .map(str::to_string);
    let right_rel = config
        .args
        .get("right_relationship")
        .and_then(Value::as_str)
        .map(str::to_string);
    if left_rel.is_none() {
        bag.error(
            REL_CODE,
            format!("{path}.left_relationship"),
            "relation.junction_pair requires a `left_relationship` naming the left foreign key"
                .to_string(),
        );
    }
    if right_rel.is_none() {
        bag.error(
            REL_CODE,
            format!("{path}.right_relationship"),
            "relation.junction_pair requires a `right_relationship` naming the right foreign key"
                .to_string(),
        );
    }

    let left = left_rel.as_deref().and_then(|name| {
        dense_key_from_relationship(
            facts,
            name,
            "relation.junction_pair",
            "left_relationship",
            [REL_CODE, KEY_CODE],
            path,
            &mut bag,
        )
    });
    let right = right_rel.as_deref().and_then(|name| {
        dense_key_from_relationship(
            facts,
            name,
            "relation.junction_pair",
            "right_relationship",
            [REL_CODE, KEY_CODE],
            path,
            &mut bag,
        )
    });

    // The junction can hold at most `left.count * right.count` distinct edges; a
    // larger row count could not fill unique pairs.
    let self_count = facts
        .and_then(|f| f.get("self_count"))
        .and_then(Value::as_u64);
    if let (Some(left), Some(right), Some(rows)) = (left, right, self_count) {
        let total = i128::from(left.count) * i128::from(right.count);
        if i128::from(rows) > total {
            bag.error(
                EXHAUSTED_CODE,
                path.to_string(),
                format!(
                    "relation.junction_pair needs {rows} unique (left, right) pairs but only {total} exist ({} left x {} right)",
                    left.count, right.count
                ),
            );
        }
    }

    if bag.has_errors() {
        return Err(bag);
    }

    let left_col = left_col.expect("left resolved without errors");
    let right_col = right_col.expect("right resolved without errors");
    let left = left.expect("left key resolved without errors");
    let right = right.expect("right key resolved without errors");
    let total = i128::from(left.count) * i128::from(right.count);

    // A seeded stride coprime with `total` makes `n -> (offset + n*stride) mod
    // total` a bijection, so distinct rows land on distinct pairs. `stride == 1`
    // is always coprime, so the search below always terminates on a valid value.
    let mut rng = context.rng(StreamId::operator(
        table.name.as_str(),
        format!("{},{}", left_col.name, right_col.name),
        "relation.junction_pair",
    ));
    let (stride, offset) = if total > 1 {
        let offset = rng.random_range(0..total);
        let mut stride = 1 + rng.random_range(0..total - 1);
        let mut tries = 0;
        while gcd(stride, total) != 1 && tries < 64 {
            stride = stride % (total - 1) + 1;
            tries += 1;
        }
        if gcd(stride, total) != 1 {
            stride = 1;
        }
        (stride, offset)
    } else {
        (1, 0)
    };

    let writes = vec![left_col.name.clone(), right_col.name.clone()];
    let mut predicates = Vec::new();
    if left.start >= 0 && left.step >= 0 && right.start >= 0 && right.step >= 0 {
        predicates.push(PlannerPredicate::NonNegative {
            columns: writes.clone(),
        });
    }

    Ok(RelationJunctionPairPlanner {
        writes,
        left_family: left_col.family.clone(),
        right_family: right_col.family.clone(),
        left,
        right,
        total,
        stride,
        offset,
        predicates,
    })
}

// =============================================================================
// relation.polymorphic_pair
// =============================================================================

/// Static description of the `relation.polymorphic_pair` planner.
pub static RELATION_POLYMORPHIC_PAIR_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "relation.polymorphic_pair",
    aliases: &[],
    summary: "Fills a (type, id) polymorphic pair atomically: a weighted choice picks a target table, then a valid key from that same target's key domain.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps the polymorphic type and id roles to columns.",
        },
        ArgumentSpec {
            name: "targets",
            required: true,
            summary: "Weighted target tables with optional type labels and id-column overrides.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: true,
};

/// Factory for the `relation.polymorphic_pair` planner.
pub struct RelationPolymorphicPairFactory;

impl PlannerFactory for RelationPolymorphicPairFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &RELATION_POLYMORPHIC_PAIR_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_polymorphic_pair(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

/// One resolved polymorphic target: the discriminator string written into the
/// type column and the key domain the id is drawn from.
struct PolyTarget {
    type_label: String,
    key: DenseKey,
}

/// The compiled `relation.polymorphic_pair` planner. Each row draws a target
/// (weighted) and then a valid key from that same target's domain, in a fixed
/// stream order, so the type and id are always chosen together and never
/// independently.
struct RelationPolymorphicPairPlanner {
    writes: Vec<String>,
    id_family: SqlTypeFamily,
    targets: Vec<PolyTarget>,
    weights: Vec<f64>,
    rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for RelationPolymorphicPairPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        _row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Draw the target pick, then the id pick, in a fixed order, so the pair
        // is atomic and a seeded run reproduces exactly.
        let target_pick = self.rng.random::<f64>();
        let id_pick = self.rng.random::<f64>();
        let index = pick_weighted_index(&self.weights, target_pick);
        let target = &self.targets[index];
        let row = if target.key.count == 0 {
            0
        } else {
            (id_pick * target.key.count as f64) as u64
        }
        .min(target.key.count.saturating_sub(1));

        output[0] = GeneratedValue::Text(target.type_label.clone());
        output[1] = render_key(target.key.key_at(row), &self.id_family);
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

fn compile_polymorphic_pair(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<RelationPolymorphicPairPlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::POLYMORPHIC_COLUMN_MISSING.code;
    const TARGETS_CODE: &str = crate::diagnostic::codes::POLYMORPHIC_TARGETS.code;
    const TARGET_UNKNOWN_CODE: &str = crate::diagnostic::codes::POLYMORPHIC_TARGET_UNKNOWN.code;
    const KEY_CODE: &str = crate::diagnostic::codes::POLYMORPHIC_KEY_UNSUPPORTED.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");
    let facts = config.args.get(RELATION_FACTS_KEY);

    let type_col = resolve_required(columns, "type", table, path, COLUMN_CODE, &mut bag);
    let id_col = resolve_required(columns, "id", table, path, COLUMN_CODE, &mut bag);

    let target_items = match config.args.get("targets") {
        Some(Value::Sequence(items)) if !items.is_empty() => items.as_slice(),
        _ => {
            bag.error(
                TARGETS_CODE,
                format!("{path}.targets"),
                "relation.polymorphic_pair requires a non-empty `targets` list".to_string(),
            );
            &[]
        }
    };

    let mut targets = Vec::new();
    let mut weights = Vec::new();
    for (index, item) in target_items.iter().enumerate() {
        let target_path = format!("{path}.targets[{index}]");
        let Some(table_name) = item.get("table").and_then(Value::as_str) else {
            bag.error(
                TARGETS_CODE,
                target_path,
                "relation.polymorphic_pair target requires a `table`".to_string(),
            );
            continue;
        };
        let table_fact = facts
            .and_then(|f| f.get("tables"))
            .and_then(|t| t.get(table_name));
        let Some(table_fact) = table_fact else {
            bag.error(
                TARGET_UNKNOWN_CODE,
                target_path,
                format!(
                    "relation.polymorphic_pair target `{table_name}` is not a table in the model"
                ),
            );
            continue;
        };
        // The id column: an explicit `id_column`, else the target's primary key.
        let id_column = item
            .get("id_column")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                table_fact
                    .get("primary_key")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        let Some(id_column) = id_column else {
            bag.error(
                TARGET_UNKNOWN_CODE,
                target_path,
                format!(
                    "relation.polymorphic_pair target `{table_name}` has no primary key; specify an `id_column`"
                ),
            );
            continue;
        };
        let key_fact = table_fact.get("keys").and_then(|k| k.get(&id_column));
        let Some(key_fact) = key_fact else {
            bag.error(
                KEY_CODE,
                target_path,
                format!(
                    "relation.polymorphic_pair target `{table_name}.{id_column}` does not have a dense integer key; only bare integer primary keys and `sequence` keys are supported"
                ),
            );
            continue;
        };
        let Some(key) = dense_key_from_fact(
            key_fact,
            &format!("{table_name}.{id_column}"),
            "relation.polymorphic_pair",
            KEY_CODE,
            &target_path,
            &mut bag,
        ) else {
            continue;
        };
        if key.count == 0 {
            // A target with no rows can never supply a valid id; drop it rather
            // than emit an unsatisfiable (type, id) pair.
            continue;
        }
        let type_label = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(table_name)
            .to_string();
        let weight = item.get("weight").and_then(as_f64).unwrap_or(1.0).max(0.0);
        targets.push(PolyTarget { type_label, key });
        weights.push(weight);
    }

    if targets.is_empty() && !bag.has_errors() {
        bag.error(
            TARGETS_CODE,
            format!("{path}.targets"),
            "relation.polymorphic_pair has no target with any rows to reference".to_string(),
        );
    }
    if weights.iter().sum::<f64>() <= 0.0 && !targets.is_empty() {
        // Degenerate weights: fall back to a uniform choice so every row still
        // resolves a valid (type, id) pair.
        weights = vec![1.0; targets.len()];
    }

    if bag.has_errors() {
        return Err(bag);
    }

    let type_col = type_col.expect("type resolved without errors");
    let id_col = id_col.expect("id resolved without errors");
    let writes = vec![type_col.name.clone(), id_col.name.clone()];

    let mut predicates = Vec::new();
    if targets.iter().all(|t| t.key.start >= 0 && t.key.step >= 0) {
        predicates.push(PlannerPredicate::NonNegative {
            columns: vec![id_col.name.clone()],
        });
    }

    let rng = context.rng(StreamId::operator(
        table.name.as_str(),
        format!("{},{}", type_col.name, id_col.name),
        "relation.polymorphic_pair",
    ));

    Ok(RelationPolymorphicPairPlanner {
        writes,
        id_family: id_col.family.clone(),
        targets,
        weights,
        rng,
        predicates,
    })
}

// =============================================================================
// relation.tenant_family
// =============================================================================

/// Static description of the `relation.tenant_family` planner.
pub static RELATION_TENANT_FAMILY_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "relation.tenant_family",
    aliases: &[],
    summary: "Selects a same-tenant foreign key: the parent rows are partitioned into tenant blocks, and each child's FK is drawn from the block of the child's own tenant.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps tenant and parent roles to the child table's columns.",
        },
        ArgumentSpec {
            name: "relationship",
            required: true,
            summary: "Relationship that supplies the parent dense key domain.",
        },
        ArgumentSpec {
            name: "num_tenants",
            required: false,
            summary: "Number of contiguous tenant partitions.",
        },
        ArgumentSpec {
            name: "tenant_start",
            required: false,
            summary: "First generated tenant identifier.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: true,
};

/// Factory for the `relation.tenant_family` planner.
pub struct RelationTenantFamilyFactory;

impl PlannerFactory for RelationTenantFamilyFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &RELATION_TENANT_FAMILY_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_tenant_family(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

/// The compiled `relation.tenant_family` planner. The parent's `count` rows are
/// partitioned into `num_tenants` contiguous, balanced tenant blocks by row
/// index: tenant `t` owns parent rows `[t*count/T, (t+1)*count/T)`, so the
/// tenant of parent row `p` is `p*T/count`. Each child draws a tenant, sets its
/// own `tenant` column to that tenant, and draws its foreign key from that
/// tenant's parent block — so the child and the parent it references always
/// share a tenant, by construction.
struct RelationTenantFamilyPlanner {
    writes: Vec<String>,
    tenant_family: SqlTypeFamily,
    parent_family: SqlTypeFamily,
    parent: DenseKey,
    num_tenants: u64,
    tenant_start: i128,
    tenant_rng: ChaCha8Rng,
    select_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl RelationTenantFamilyPlanner {
    /// The half-open parent-row range `[start, end)` owned by tenant `t`.
    fn tenant_block(&self, t: u64) -> (u64, u64) {
        let count = u128::from(self.parent.count);
        let tenants = u128::from(self.num_tenants);
        let start = (u128::from(t) * count / tenants) as u64;
        let end = (u128::from(t + 1) * count / tenants) as u64;
        (start, end.max(start + 1).min(self.parent.count))
    }
}

impl CompiledPlanner for RelationTenantFamilyPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        _row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Draw the tenant, then the in-tenant parent, in a fixed order.
        let tenant_pick = self.tenant_rng.random::<f64>();
        let parent_pick = self.select_rng.random::<f64>();

        let tenant = ((tenant_pick * self.num_tenants as f64) as u64).min(self.num_tenants - 1);
        let (start, end) = self.tenant_block(tenant);
        let size = end - start;
        let offset = if size == 0 {
            0
        } else {
            ((parent_pick * size as f64) as u64).min(size - 1)
        };
        let parent_row = start + offset;

        output[0] = match self.tenant_family {
            SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => {
                GeneratedValue::Integer(self.tenant_start + tenant as i128)
            }
            _ => GeneratedValue::Text((self.tenant_start + tenant as i128).to_string()),
        };
        output[1] = render_key(self.parent.key_at(parent_row), &self.parent_family);
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

fn compile_tenant_family(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<RelationTenantFamilyPlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::TENANT_COLUMN_MISSING.code;
    const REL_CODE: &str = crate::diagnostic::codes::TENANT_RELATIONSHIP.code;
    const KEY_CODE: &str = crate::diagnostic::codes::TENANT_KEY_UNSUPPORTED.code;
    const PARTITION_CODE: &str = crate::diagnostic::codes::TENANT_PARTITION.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");
    let facts = config.args.get(RELATION_FACTS_KEY);

    let tenant_col = resolve_required(columns, "tenant", table, path, COLUMN_CODE, &mut bag);
    let parent_col = resolve_required(columns, "parent", table, path, COLUMN_CODE, &mut bag);

    let rel = config
        .args
        .get("relationship")
        .and_then(Value::as_str)
        .map(str::to_string);
    if rel.is_none() {
        bag.error(
            REL_CODE,
            format!("{path}.relationship"),
            "relation.tenant_family requires a `relationship` naming the parent foreign key"
                .to_string(),
        );
    }
    let parent = rel.as_deref().and_then(|name| {
        dense_key_from_relationship(
            facts,
            name,
            "relation.tenant_family",
            "relationship",
            [REL_CODE, KEY_CODE],
            path,
            &mut bag,
        )
    });

    let num_tenants = config
        .args
        .get("num_tenants")
        .and_then(as_i128)
        .unwrap_or(4);
    let tenant_start = config
        .args
        .get("tenant_start")
        .and_then(as_i128)
        .unwrap_or(0);

    if num_tenants < 1 {
        bag.error(
            PARTITION_CODE,
            format!("{path}.num_tenants"),
            format!("relation.tenant_family `num_tenants` {num_tenants} must be at least 1"),
        );
    }
    if let Some(parent) = parent {
        if num_tenants >= 1 && num_tenants as u64 > parent.count {
            bag.error(
                PARTITION_CODE,
                format!("{path}.num_tenants"),
                format!(
                    "relation.tenant_family `num_tenants` {num_tenants} exceeds the {} parent rows, so some tenants would have no parent to reference",
                    parent.count
                ),
            );
        }
    }

    if bag.has_errors() {
        return Err(bag);
    }

    let tenant_col = tenant_col.expect("tenant resolved without errors");
    let parent_col = parent_col.expect("parent resolved without errors");
    let parent = parent.expect("parent key resolved without errors");
    let writes = vec![tenant_col.name.clone(), parent_col.name.clone()];

    let mut predicates = Vec::new();
    if parent.start >= 0 && parent.step >= 0 && tenant_start >= 0 {
        predicates.push(PlannerPredicate::NonNegative {
            columns: writes.clone(),
        });
    }

    let tenant_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        tenant_col.name.clone(),
        "relation.tenant_family.tenant",
    ));
    let select_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        parent_col.name.clone(),
        "relation.tenant_family.parent",
    ));

    Ok(RelationTenantFamilyPlanner {
        writes,
        tenant_family: tenant_col.family.clone(),
        parent_family: parent_col.family.clone(),
        parent,
        num_tenants: num_tenants as u64,
        tenant_start,
        tenant_rng,
        select_rng,
        predicates,
    })
}

// =============================================================================
// geo.coordinate_pair
// =============================================================================

/// Global inclusive latitude range, in whole degrees.
const LAT_MIN_DEGREES: f64 = -90.0;
const LAT_MAX_DEGREES: f64 = 90.0;
/// Global inclusive longitude range, in whole degrees.
const LON_MIN_DEGREES: f64 = -180.0;
const LON_MAX_DEGREES: f64 = 180.0;

/// Static description of the `geo.coordinate_pair` planner.
///
/// Note: no [`PlannerPredicate`] variant expresses a decimal-valued bounded
/// range — [`PlannerPredicate::InRange`] is timestamp-only (it compares
/// epoch nanoseconds). Range and bounding-box correctness are
/// therefore guaranteed by construction (every draw is a `random_range` over
/// the compiled bound) and covered by this module's tests, not by a runtime
/// predicate.
pub static GEO_COORDINATE_PAIR_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "geo.coordinate_pair",
    aliases: &[],
    summary: "Draws a coherent (latitude, longitude) pair within [-90, 90] x [-180, 180] (or a configured bounding box), at a configurable decimal precision.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps latitude and longitude roles to decimal columns.",
        },
        ArgumentSpec {
            name: "precision",
            required: false,
            summary: "Number of decimal places, from 0 through 9.",
        },
        ArgumentSpec {
            name: "bounds",
            required: false,
            summary: "Optional minimum and maximum latitude and longitude bounding box.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
    cross_table: false,
};

/// Factory for the `geo.coordinate_pair` planner.
pub struct GeoCoordinatePairFactory;

impl PlannerFactory for GeoCoordinatePairFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &GEO_COORDINATE_PAIR_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_coordinate_pair(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

struct GeoCoordinatePairPlanner {
    writes: Vec<String>,
    lat_family: SqlTypeFamily,
    lon_family: SqlTypeFamily,
    lat_min_minor: i128,
    lat_max_minor: i128,
    lon_min_minor: i128,
    lon_max_minor: i128,
    scale: u32,
    lat_rng: ChaCha8Rng,
    lon_rng: ChaCha8Rng,
}

impl CompiledPlanner for GeoCoordinatePairPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        _row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Both columns are drawn from independent streams, in a fixed order,
        // every row — the pair is coherent (always present together) simply
        // because a single planner call produces both.
        let lat_minor = self
            .lat_rng
            .random_range(self.lat_min_minor..=self.lat_max_minor);
        let lon_minor = self
            .lon_rng
            .random_range(self.lon_min_minor..=self.lon_max_minor);
        output[0] = render_decimal(&self.lat_family, lat_minor, self.scale);
        output[1] = render_decimal(&self.lon_family, lon_minor, self.scale);
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        Vec::new()
    }
}

/// Render a fixed-point value in whichever representation `family` expects
/// (mirrors `semantic.rs`'s private `decimal_value`; kept local since that
/// helper is module-private and duplicating it here is cheaper than exposing
/// it across modules for one shared use).
fn render_decimal(family: &SqlTypeFamily, minor: i128, scale: u32) -> GeneratedValue {
    match family {
        SqlTypeFamily::Decimal => GeneratedValue::Decimal { minor, scale },
        _ => GeneratedValue::Text(format_fixed_point(minor, scale)),
    }
}

/// Render `minor` units at `scale` decimal places as a fixed-point string.
fn format_fixed_point(minor: i128, scale: u32) -> String {
    if scale == 0 {
        return minor.to_string();
    }
    let negative = minor < 0;
    let magnitude = minor.unsigned_abs();
    let factor = 10u128.pow(scale);
    let whole = magnitude / factor;
    let frac = magnitude % factor;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{whole}.{frac:0width$}", width = scale as usize)
}

fn compile_coordinate_pair(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<GeoCoordinatePairPlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::COORDINATE_COLUMN_MISSING.code;
    const BOUNDS_CODE: &str = crate::diagnostic::codes::COORDINATE_BOUNDS.code;
    const PRECISION_CODE: &str = crate::diagnostic::codes::COORDINATE_PRECISION.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");

    let lat_col = resolve_required(columns, "latitude", table, path, COLUMN_CODE, &mut bag);
    let lon_col = resolve_required(columns, "longitude", table, path, COLUMN_CODE, &mut bag);

    let precision = config.args.get("precision").and_then(as_i128).unwrap_or(6);
    if !(0..=9).contains(&precision) {
        bag.error(
            PRECISION_CODE,
            format!("{path}.precision"),
            format!("geo.coordinate_pair `precision` {precision} must be within [0, 9]"),
        );
    }

    let bounds = config.args.get("bounds");
    let min_lat = bounds
        .and_then(|b| b.get("min_lat"))
        .and_then(as_f64)
        .unwrap_or(LAT_MIN_DEGREES);
    let max_lat = bounds
        .and_then(|b| b.get("max_lat"))
        .and_then(as_f64)
        .unwrap_or(LAT_MAX_DEGREES);
    let min_lon = bounds
        .and_then(|b| b.get("min_lon"))
        .and_then(as_f64)
        .unwrap_or(LON_MIN_DEGREES);
    let max_lon = bounds
        .and_then(|b| b.get("max_lon"))
        .and_then(as_f64)
        .unwrap_or(LON_MAX_DEGREES);

    if !(LAT_MIN_DEGREES..=LAT_MAX_DEGREES).contains(&min_lat)
        || !(LAT_MIN_DEGREES..=LAT_MAX_DEGREES).contains(&max_lat)
        || min_lat > max_lat
    {
        bag.error(
            BOUNDS_CODE,
            format!("{path}.bounds"),
            format!(
                "geo.coordinate_pair latitude bounds [{min_lat}, {max_lat}] must lie within [-90, 90] with min <= max"
            ),
        );
    }
    if !(LON_MIN_DEGREES..=LON_MAX_DEGREES).contains(&min_lon)
        || !(LON_MIN_DEGREES..=LON_MAX_DEGREES).contains(&max_lon)
        || min_lon > max_lon
    {
        bag.error(
            BOUNDS_CODE,
            format!("{path}.bounds"),
            format!(
                "geo.coordinate_pair longitude bounds [{min_lon}, {max_lon}] must lie within [-180, 180] with min <= max"
            ),
        );
    }

    if bag.has_errors() {
        return Err(bag);
    }

    let lat = lat_col.expect("latitude resolved without errors");
    let lon = lon_col.expect("longitude resolved without errors");
    let scale = precision as u32;
    let factor = 10f64.powi(scale as i32);
    let lat_min_minor = (min_lat * factor).round() as i128;
    let lat_max_minor = (max_lat * factor).round() as i128;
    let lon_min_minor = (min_lon * factor).round() as i128;
    let lon_max_minor = (max_lon * factor).round() as i128;

    let writes = vec![lat.name.clone(), lon.name.clone()];
    let lat_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        lat.name.clone(),
        "geo.coordinate_pair.latitude",
    ));
    let lon_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        lon.name.clone(),
        "geo.coordinate_pair.longitude",
    ));

    Ok(GeoCoordinatePairPlanner {
        writes,
        lat_family: lat.family.clone(),
        lon_family: lon.family.clone(),
        lat_min_minor,
        lat_max_minor,
        lon_min_minor,
        lon_max_minor,
        scale,
        lat_rng,
        lon_rng,
    })
}

// =============================================================================
// file.metadata
// =============================================================================

/// One recognized `(extension, mime_type)` pairing `file.metadata` can draw.
/// Extensions are matched case-insensitively but stored (and rendered)
/// lowercase.
struct FileTypeEntry {
    extension: &'static str,
    mime_type: &'static str,
}

/// The built-in extension/MIME catalog. Intentionally a plain, well-known
/// mapping (not tied to any locale or external crate) so `mime_type` is
/// always consistent with `extension` by construction.
const FILE_TYPE_CATALOG: &[FileTypeEntry] = &[
    FileTypeEntry {
        extension: "txt",
        mime_type: "text/plain",
    },
    FileTypeEntry {
        extension: "csv",
        mime_type: "text/csv",
    },
    FileTypeEntry {
        extension: "json",
        mime_type: "application/json",
    },
    FileTypeEntry {
        extension: "xml",
        mime_type: "application/xml",
    },
    FileTypeEntry {
        extension: "html",
        mime_type: "text/html",
    },
    FileTypeEntry {
        extension: "css",
        mime_type: "text/css",
    },
    FileTypeEntry {
        extension: "js",
        mime_type: "text/javascript",
    },
    FileTypeEntry {
        extension: "md",
        mime_type: "text/markdown",
    },
    FileTypeEntry {
        extension: "jpg",
        mime_type: "image/jpeg",
    },
    FileTypeEntry {
        extension: "jpeg",
        mime_type: "image/jpeg",
    },
    FileTypeEntry {
        extension: "png",
        mime_type: "image/png",
    },
    FileTypeEntry {
        extension: "gif",
        mime_type: "image/gif",
    },
    FileTypeEntry {
        extension: "webp",
        mime_type: "image/webp",
    },
    FileTypeEntry {
        extension: "svg",
        mime_type: "image/svg+xml",
    },
    FileTypeEntry {
        extension: "bmp",
        mime_type: "image/bmp",
    },
    FileTypeEntry {
        extension: "pdf",
        mime_type: "application/pdf",
    },
    FileTypeEntry {
        extension: "zip",
        mime_type: "application/zip",
    },
    FileTypeEntry {
        extension: "gz",
        mime_type: "application/gzip",
    },
    FileTypeEntry {
        extension: "tar",
        mime_type: "application/x-tar",
    },
    FileTypeEntry {
        extension: "mp3",
        mime_type: "audio/mpeg",
    },
    FileTypeEntry {
        extension: "wav",
        mime_type: "audio/wav",
    },
    FileTypeEntry {
        extension: "mp4",
        mime_type: "video/mp4",
    },
    FileTypeEntry {
        extension: "mov",
        mime_type: "video/quicktime",
    },
    FileTypeEntry {
        extension: "avi",
        mime_type: "video/x-msvideo",
    },
    FileTypeEntry {
        extension: "doc",
        mime_type: "application/msword",
    },
    FileTypeEntry {
        extension: "docx",
        mime_type: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    },
    FileTypeEntry {
        extension: "xls",
        mime_type: "application/vnd.ms-excel",
    },
    FileTypeEntry {
        extension: "xlsx",
        mime_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    },
    FileTypeEntry {
        extension: "ppt",
        mime_type: "application/vnd.ms-powerpoint",
    },
    FileTypeEntry {
        extension: "pptx",
        mime_type: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    },
    FileTypeEntry {
        extension: "bin",
        mime_type: "application/octet-stream",
    },
];

fn find_file_type(extension: &str) -> Option<&'static FileTypeEntry> {
    FILE_TYPE_CATALOG
        .iter()
        .find(|entry| entry.extension.eq_ignore_ascii_case(extension))
}

/// Plain, unremarkable words a synthetic file name is built from — no
/// locale/crate dependency, just enough variety to look plausible.
const FILE_NAME_WORDS: &[&str] = &[
    "report",
    "invoice",
    "photo",
    "backup",
    "export",
    "summary",
    "dataset",
    "image",
    "archive",
    "document",
    "manifest",
    "snapshot",
    "profile",
    "transcript",
    "spreadsheet",
    "presentation",
    "recording",
    "scan",
    "thumbnail",
    "upload",
];

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// A supported `hash_kind`: fixes the hex digest length `file.metadata`
/// draws. The digest is a uniformly random hex string of the correct
/// length/charset for the kind — digest-shaped, but never a hash of any real
/// content, so it is unmistakably synthetic (the same posture as
/// `identifier.hash`).
#[derive(Clone, Copy)]
enum HashKind {
    Md5,
    Sha1,
    Sha256,
    Sha512,
}

impl HashKind {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "md5" => Some(Self::Md5),
            "sha1" => Some(Self::Sha1),
            "sha256" => Some(Self::Sha256),
            "sha512" => Some(Self::Sha512),
            _ => None,
        }
    }

    fn hex_len(self) -> usize {
        match self {
            Self::Md5 => 32,
            Self::Sha1 => 40,
            Self::Sha256 => 64,
            Self::Sha512 => 128,
        }
    }
}

/// Static description of the `file.metadata` planner.
///
/// Note: only `size`'s nonnegativity has an equivalent in the existing
/// [`PlannerPredicate`] vocabulary ([`PlannerPredicate::NonNegative`]). There
/// is no "column ends with column" or cross-column text-equality predicate,
/// so the name/extension/mime_type textual coherence invariants are
/// guaranteed by construction (every row draws one target extension and
/// derives the name suffix and MIME type from it) and covered by this
/// module's tests rather than a runtime predicate.
pub static FILE_METADATA_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "file.metadata",
    aliases: &[],
    summary: "Coordinates a coherent file name/extension/mime_type/size/hash: the extension matches the name's suffix, mime_type matches the extension, size is a plausible byte count, and hash is a clearly-synthetic digest-shaped string.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps name and optional extension, MIME type, size, and hash roles to columns.",
        },
        ArgumentSpec {
            name: "extensions",
            required: false,
            summary: "Allow-list of recognized file extensions.",
        },
        ArgumentSpec {
            name: "size",
            required: false,
            summary: "Minimum and maximum generated byte size.",
        },
        ArgumentSpec {
            name: "hash_kind",
            required: false,
            summary: "Synthetic digest shape: md5, sha1, sha256, or sha512.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `file.metadata` planner.
pub struct FileMetadataFactory;

impl PlannerFactory for FileMetadataFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &FILE_METADATA_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_file_metadata(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

struct FileMetadataPlanner {
    writes: Vec<String>,
    entries: Vec<&'static FileTypeEntry>,
    has_extension: bool,
    has_mime_type: bool,
    size_family: Option<SqlTypeFamily>,
    size_min: i128,
    size_max: i128,
    hash_len: Option<usize>,
    ext_rng: ChaCha8Rng,
    name_rng: ChaCha8Rng,
    size_rng: ChaCha8Rng,
    hash_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for FileMetadataPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Every stream is drawn unconditionally, in a fixed order, so a
        // seeded run reproduces exactly regardless of which optional columns
        // this instance was configured to own.
        let ext_pick = self.ext_rng.random::<f64>();
        let word1_pick = self.name_rng.random::<f64>();
        let word2_pick = self.name_rng.random::<f64>();
        let size_pick = self.size_rng.random::<f64>();

        let entry = &self.entries[pick_index(ext_pick, self.entries.len())];
        let word1 = FILE_NAME_WORDS[pick_index(word1_pick, FILE_NAME_WORDS.len())];
        let word2 = FILE_NAME_WORDS[pick_index(word2_pick, FILE_NAME_WORDS.len())];
        let name = format!("{word1}_{word2}_{row_index}.{}", entry.extension);

        let mut slot = 0usize;
        output[slot] = GeneratedValue::Text(name);
        slot += 1;
        if self.has_extension {
            output[slot] = GeneratedValue::Text(entry.extension.to_string());
            slot += 1;
        }
        if self.has_mime_type {
            output[slot] = GeneratedValue::Text(entry.mime_type.to_string());
            slot += 1;
        }
        if let Some(family) = &self.size_family {
            let span = self
                .size_max
                .saturating_sub(self.size_min)
                .saturating_add(1);
            let offset = ((size_pick * span as f64) as i128).clamp(0, span - 1);
            output[slot] = render_key(self.size_min.saturating_add(offset), family);
            slot += 1;
        }
        if let Some(hash_len) = self.hash_len {
            let digest: String = (0..hash_len)
                .map(|_| HEX_DIGITS[self.hash_rng.random_range(0..HEX_DIGITS.len())] as char)
                .collect();
            output[slot] = GeneratedValue::Text(digest);
            slot += 1;
        }
        debug_assert_eq!(slot, output.len());
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

/// Map a uniform `[0, 1)` draw to an index in `[0, len)`.
fn pick_index(pick: f64, len: usize) -> usize {
    ((pick * len as f64) as usize).min(len.saturating_sub(1))
}

fn compile_file_metadata(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<FileMetadataPlanner, DiagnosticBag> {
    const COLUMN_CODE: &str = crate::diagnostic::codes::FILE_COLUMN_MISSING.code;
    const SIZE_CODE: &str = crate::diagnostic::codes::FILE_SIZE_RANGE.code;
    const HASH_CODE: &str = crate::diagnostic::codes::FILE_HASH_KIND.code;
    const EXTENSIONS_CODE: &str = crate::diagnostic::codes::FILE_EXTENSIONS.code;
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();
    let columns = config.args.get("columns");

    let name_col = resolve_required(columns, "name", table, path, COLUMN_CODE, &mut bag);
    let extension_col = resolve_optional(columns, "extension", table, path, COLUMN_CODE, &mut bag);
    let mime_col = resolve_optional(columns, "mime_type", table, path, COLUMN_CODE, &mut bag);
    let size_col = resolve_optional(columns, "size", table, path, COLUMN_CODE, &mut bag);
    let hash_col = resolve_optional(columns, "hash", table, path, COLUMN_CODE, &mut bag);

    let requested_extensions = string_list(config.args.get("extensions"));
    let mut entries: Vec<&'static FileTypeEntry> = Vec::new();
    if requested_extensions.is_empty() {
        entries.extend(FILE_TYPE_CATALOG.iter());
    } else {
        for extension in &requested_extensions {
            match find_file_type(extension) {
                Some(entry) => entries.push(entry),
                None => {
                    bag.error(
                        EXTENSIONS_CODE,
                        format!("{path}.extensions"),
                        format!(
                            "file.metadata `extensions` names `{extension}`, which is not a recognized file extension"
                        ),
                    );
                }
            }
        }
    }
    if entries.is_empty() && !bag.has_errors() {
        bag.error(
            EXTENSIONS_CODE,
            format!("{path}.extensions"),
            "file.metadata `extensions` must resolve to at least one recognized extension"
                .to_string(),
        );
    }

    let size_block = config.args.get("size");
    let size_min = size_block
        .and_then(|s| s.get("min"))
        .and_then(as_i128)
        .unwrap_or(0);
    let size_max = size_block
        .and_then(|s| s.get("max"))
        .and_then(as_i128)
        .unwrap_or(10_000_000);
    // Validated unconditionally — a nonsensical `size:` block is a user error
    // worth reporting even when no `size` column is wired to consume it (the
    // disconnect itself is worth surfacing, not silently accepting).
    if size_min < 0 {
        bag.error(
            SIZE_CODE,
            format!("{path}.size.min"),
            format!("file.metadata `size.min` {size_min} must be >= 0"),
        );
    }
    if size_max < size_min {
        bag.error(
            SIZE_CODE,
            format!("{path}.size.max"),
            format!("file.metadata `size.max` {size_max} is below `size.min` {size_min}"),
        );
    }

    let hash_kind_name = config
        .args
        .get("hash_kind")
        .and_then(Value::as_str)
        .unwrap_or("sha256");
    let hash_kind = HashKind::parse(hash_kind_name);
    if hash_col.is_some() && hash_kind.is_none() {
        bag.error(
            HASH_CODE,
            format!("{path}.hash_kind"),
            format!(
                "file.metadata `hash_kind` `{hash_kind_name}` is not a recognized hash kind (expected one of md5, sha1, sha256, sha512)"
            ),
        );
    }

    if bag.has_errors() {
        return Err(bag);
    }

    let name = name_col.expect("name resolved without errors");
    let mut writes = vec![name.name.clone()];
    let has_extension = extension_col.is_some();
    if let Some(column) = extension_col {
        writes.push(column.name.clone());
    }
    let has_mime_type = mime_col.is_some();
    if let Some(column) = mime_col {
        writes.push(column.name.clone());
    }
    let size_family = size_col.map(|column| {
        writes.push(column.name.clone());
        column.family.clone()
    });
    let hash_len = hash_col.map(|column| {
        writes.push(column.name.clone());
        hash_kind.unwrap_or(HashKind::Sha256).hex_len()
    });

    let mut predicates = Vec::new();
    if let Some(size) = size_col {
        if size_min >= 0 {
            predicates.push(PlannerPredicate::NonNegative {
                columns: vec![size.name.clone()],
            });
        }
    }

    let ext_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        name.name.clone(),
        "file.metadata.extension",
    ));
    let name_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        name.name.clone(),
        "file.metadata.name",
    ));
    let size_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        name.name.clone(),
        "file.metadata.size",
    ));
    let hash_rng = context.rng(StreamId::operator(
        table.name.as_str(),
        name.name.clone(),
        "file.metadata.hash",
    ));

    Ok(FileMetadataPlanner {
        writes,
        entries,
        has_extension,
        has_mime_type,
        size_family,
        size_min,
        size_max,
        hash_len,
        ext_rng,
        name_rng,
        size_rng,
        hash_rng,
        predicates,
    })
}

/// Resolve an optional column role: `None` when the role is absent from
/// `columns`, `Some` when present and valid, or a compile error when present
/// but naming a column that does not exist on the table.
fn resolve_optional<'a>(
    columns: Option<&Value>,
    role: &str,
    table: &'a PortableTable,
    path: &str,
    code: &'static str,
    bag: &mut DiagnosticBag,
) -> Option<&'a PortableColumn> {
    let name = role_name(columns, role)?;
    let column = find_column(table, name);
    if column.is_none() {
        bag.error(
            code,
            format!("{path}.columns.{role}"),
            format!(
                "file.metadata `{role}` column `{name}` does not exist on table `{}`",
                table.name
            ),
        );
    }
    column
}
