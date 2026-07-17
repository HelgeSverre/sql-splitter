//! Three common same-table temporal planners: `temporal.timestamps` (a
//! created/updated pair, plus optional trailing timestamps), `temporal.
//! soft_delete` (a coherent `deleted_at`/`is_deleted` pair), and `temporal.
//! lifecycle` (a status column that only ever reaches legal states, each
//! carrying a correctly-ordered timestamp).
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
//! pure checked integer arithmetic, matching Task 23's `temporal.interval`
//! approach.
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
    Buffering, ColumnScope, CompileContext, CompiledPlanner, Determinism, PlannerDescriptor,
    PlannerFactory, PlannerPredicate, PredicateGuard, Verification,
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
    arguments: &[],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
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
    const CODE: &str = "GEN-TIMESTAMPS-COLUMN-MISSING";
    const RANGE_CODE: &str = "GEN-TIMESTAMPS-RANGE";
    const DELAY_CODE: &str = "GEN-TIMESTAMPS-DELAY";
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
    // owned column.
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
pub static TEMPORAL_SOFT_DELETE_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "temporal.soft_delete",
    aliases: &[],
    summary: "Coordinates a deleted_at timestamp and optional is_deleted flag so a deletion probability produces a coherent null/non-null pair.",
    arguments: &[],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
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
    const CODE: &str = "GEN-SOFT-DELETE-COLUMN-MISSING";
    const RANGE_CODE: &str = "GEN-SOFT-DELETE-RANGE";
    const NULLABILITY_CODE: &str = "GEN-SOFT-DELETE-NULLABILITY";
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
    arguments: &[],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
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
    const COLUMN_CODE: &str = "GEN-LIFECYCLE-COLUMN-MISSING";
    const STATES_CODE: &str = "GEN-LIFECYCLE-STATES";
    const VOCAB_CODE: &str = "GEN-LIFECYCLE-STATUS-VOCABULARY";
    const WEIGHTS_CODE: &str = "GEN-LIFECYCLE-WEIGHTS";
    const RANGE_CODE: &str = "GEN-LIFECYCLE-RANGE";
    const STEP_CODE: &str = "GEN-LIFECYCLE-STEP";
    const NULLABILITY_CODE: &str = "GEN-LIFECYCLE-NULLABILITY";
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
