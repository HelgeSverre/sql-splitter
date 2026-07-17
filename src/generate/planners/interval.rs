//! The `temporal.interval` planner: coordinates a start/end/duration/open
//! column group so closed rows satisfy `end = start + duration` exactly and
//! open rows carry a coherent null/flag state.
//!
//! # One instant, integer arithmetic
//!
//! Every timestamp is reduced to a single internal UTC instant measured in
//! nanoseconds since the Unix epoch (an `i128`), and every duration to an
//! integer count of unit-sized nanoseconds. The interval equation
//! `end_ns = start_ns + duration_units * unit_nanos` is therefore pure checked
//! integer arithmetic — floats are used only to *choose* a duration from a
//! distribution, never to compute the equation. Timezone is carried as
//! rendering metadata: the instant is the same regardless of the zone it is
//! rendered in, so the equation holds across DST boundaries by construction.
//!
//! # Owned columns and streams
//!
//! The planner owns the columns named under its `columns:` mapping
//! (`start`/`end`/`duration`, plus an optional `open` flag). Each row draws the
//! start instant, the open-state, and the duration from three *separate* stable
//! seed streams, then derives the dependent columns rather than redrawing — so
//! the open decision never perturbs the duration stream and a seeded run
//! repeats exactly.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use chrono_tz::Tz;
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

/// Nanoseconds per second, the base unit conversion for the interval equation.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// Static description of the `temporal.interval` planner.
pub static TEMPORAL_INTERVAL_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "temporal.interval",
    aliases: &[],
    summary: "Coordinates a start/end/duration/open interval group so closed rows satisfy end = start + duration.",
    arguments: &[],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
};

/// Factory for the `temporal.interval` planner.
pub struct TemporalIntervalFactory;

impl PlannerFactory for TemporalIntervalFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &TEMPORAL_INTERVAL_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_interval(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

// --- Resolved column roles --------------------------------------------------

/// A resolved timestamp column: its schema name and family (so the rendered
/// value takes the `DateTime` or `Text` shape the column expects).
struct TimestampColumn {
    name: String,
    family: SqlTypeFamily,
}

/// A resolved duration column: name, family, whether it is nullable (open rows
/// null it out when possible), and how many nanoseconds one unit represents.
struct DurationColumn {
    name: String,
    family: SqlTypeFamily,
    nullable: bool,
    unit_nanos: i128,
}

/// A resolved open-flag column: name, family, and the flag value an *open* row
/// carries (a closed row carries its negation).
struct OpenColumn {
    name: String,
    family: SqlTypeFamily,
    open_value: bool,
}

/// How each row's start instant is chosen.
enum StartKind {
    /// A uniformly random instant in the inclusive `[min_ns, max_ns]` range.
    /// Covers both `range` and `observed_range` (the latter's bounds come from
    /// an observed profile; the mechanism is identical).
    Range { min_ns: i128, max_ns: i128 },
    /// A strictly increasing instant: row `n` starts at `min_ns + n * step_ns`.
    Monotonic { min_ns: i128, step_ns: i128 },
}

/// How each row's duration (in whole units) is chosen. All variants yield a
/// non-negative integer count of units bounded by the configured range.
enum DurationDraw {
    Fixed(i128),
    Uniform {
        min: i128,
        max: i128,
    },
    Normal {
        mean: f64,
        stddev: f64,
        min: i128,
        max: i128,
    },
    /// A bounded draw skewed toward the minimum (short durations are common).
    /// Covers both `histogram` and `observed`.
    Skewed {
        min: i128,
        max: i128,
    },
}

/// How instants are rendered to wall-clock text.
enum RenderZone {
    /// Render UTC wall clock with no offset (`preserve` keeps the observed
    /// zone; with no observed original zone it renders as UTC).
    Utc,
    /// Render the wall clock of a named IANA zone, DST-aware, with an explicit
    /// offset so the absolute instant round-trips.
    Named(Tz),
}

// --- The compiled planner ---------------------------------------------------

/// The compiled `temporal.interval` planner.
struct TemporalIntervalPlanner {
    /// Owned columns in `generate_row` write order: `[start, end, duration]`
    /// plus `open` when configured.
    writes: Vec<String>,
    start: TimestampColumn,
    end: TimestampColumn,
    duration: DurationColumn,
    open: Option<OpenColumn>,
    start_kind: StartKind,
    duration_draw: DurationDraw,
    open_probability: f64,
    zone: RenderZone,
    start_rng: ChaCha8Rng,
    open_rng: ChaCha8Rng,
    duration_rng: ChaCha8Rng,
    predicates: Vec<PlannerPredicate>,
}

impl CompiledPlanner for TemporalIntervalPlanner {
    fn writes(&self) -> &[String] {
        &self.writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        // Draw all three streams unconditionally and in a fixed order so the
        // open decision never perturbs the duration stream: a seeded run
        // repeats regardless of which rows land open.
        let start_ns = self.draw_start(row_index);
        let is_open =
            self.open_probability > 0.0 && self.open_rng.random::<f64>() < self.open_probability;
        let duration_units = self.draw_duration();

        output[0] = render_instant(start_ns, &self.zone, &self.start.family);

        if is_open {
            output[1] = GeneratedValue::Null;
            output[2] = self.open_duration();
        } else {
            let duration_ns = duration_units
                .checked_mul(self.duration.unit_nanos)
                .and_then(|ns| start_ns.checked_add(ns))
                .ok_or_else(|| {
                    GenerateError::Overflow(
                        "temporal.interval: start + duration overflows the representable instant range".to_string(),
                    )
                })?;
            output[1] = render_instant(duration_ns, &self.zone, &self.end.family);
            output[2] = render_duration(duration_units, &self.duration.family);
        }

        if let Some(open) = &self.open {
            let flag = is_open == open.open_value;
            output[3] = render_flag(flag, &open.family);
        }
        Ok(())
    }

    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        self.predicates.clone()
    }
}

impl TemporalIntervalPlanner {
    /// The start instant for `row_index`, in epoch nanoseconds.
    fn draw_start(&mut self, row_index: u64) -> i128 {
        match self.start_kind {
            StartKind::Range { min_ns, max_ns } => self.start_rng.random_range(min_ns..=max_ns),
            StartKind::Monotonic { min_ns, step_ns } => {
                // Saturating keeps a very long run inside the representable
                // range instead of wrapping; the equation still holds on the
                // clamped instant.
                min_ns.saturating_add(step_ns.saturating_mul(row_index as i128))
            }
        }
    }

    /// The duration for a row, in whole units (always non-negative).
    fn draw_duration(&mut self) -> i128 {
        match self.duration_draw {
            DurationDraw::Fixed(value) => value,
            DurationDraw::Uniform { min, max } => self.draw_uniform(min, max),
            DurationDraw::Skewed { min, max } => {
                if max <= min {
                    return min;
                }
                let t = self.duration_rng.random::<f64>();
                // Square biases the draw toward `min` (short intervals common)
                // while staying within `[min, max]`.
                let span = (max - min) as f64;
                min + (span * t * t) as i128
            }
            DurationDraw::Normal {
                mean,
                stddev,
                min,
                max,
            } => {
                let z = self.standard_normal();
                let value = (mean + z * stddev).round() as i128;
                value.clamp(min, max)
            }
        }
    }

    /// A uniform integer draw in `[min, max]`, tolerant of an inverted range.
    fn draw_uniform(&mut self, min: i128, max: i128) -> i128 {
        if max <= min {
            min
        } else {
            self.duration_rng.random_range(min..=max)
        }
    }

    /// A standard-normal draw via Box–Muller from the duration stream.
    fn standard_normal(&mut self) -> f64 {
        // Guard the log against exactly zero.
        let u1 = (self.duration_rng.random::<f64>()).max(f64::MIN_POSITIVE);
        let u2 = self.duration_rng.random::<f64>();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    /// The duration value an open row carries: `NULL` when the column is
    /// nullable, otherwise a coherent zero (no completed duration yet).
    fn open_duration(&self) -> GeneratedValue {
        if self.duration.nullable {
            GeneratedValue::Null
        } else {
            render_duration(0, &self.duration.family)
        }
    }
}

// --- Rendering --------------------------------------------------------------

/// Render an epoch-nanosecond instant to a wall-clock literal in `zone`, in the
/// representation `family` expects.
fn render_instant(instant_ns: i128, zone: &RenderZone, family: &SqlTypeFamily) -> GeneratedValue {
    let text = format_instant(instant_ns, zone);
    match family {
        SqlTypeFamily::DateTime => GeneratedValue::DateTime(text),
        _ => GeneratedValue::Text(text),
    }
}

/// Format an epoch-nanosecond instant as wall-clock text in `zone`.
fn format_instant(instant_ns: i128, zone: &RenderZone) -> String {
    let secs = instant_ns.div_euclid(NANOS_PER_SECOND);
    let nanos = instant_ns.rem_euclid(NANOS_PER_SECOND) as u32;
    let utc = DateTime::<Utc>::from_timestamp(secs as i64, nanos).unwrap_or_default();
    match zone {
        RenderZone::Utc => utc.format("%Y-%m-%d %H:%M:%S").to_string(),
        // Include the offset for a named zone so the absolute instant round-
        // trips even across a DST transition (the offset disambiguates the
        // repeated wall-clock hour).
        RenderZone::Named(tz) => utc
            .with_timezone(tz)
            .format("%Y-%m-%d %H:%M:%S%:z")
            .to_string(),
    }
}

/// Render a duration (in whole units) in the representation `family` expects.
fn render_duration(units: i128, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(units),
        _ => GeneratedValue::Text(units.to_string()),
    }
}

/// Render an open-flag boolean in the representation `family` expects.
fn render_flag(flag: bool, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Boolean => GeneratedValue::Boolean(flag),
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(flag as i128),
        _ => GeneratedValue::Text(flag.to_string()),
    }
}

// --- Compilation ------------------------------------------------------------

/// Validate `config` against the table schema and build the compiled planner,
/// gathering every independent error before returning.
fn compile_interval(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<TemporalIntervalPlanner, DiagnosticBag> {
    let mut bag = DiagnosticBag::default();
    let table = context.table();
    let path = context.path();

    let columns = config.args.get("columns");
    let start_col = resolve_role(columns, "start", table, path, &mut bag);
    let end_col = resolve_role(columns, "end", table, path, &mut bag);
    let duration_col = resolve_role(columns, "duration", table, path, &mut bag);
    // `open` is optional; only resolve it (and validate existence) if named.
    let open_col = role_name(columns, "open").and_then(|name| {
        find_column(table, name).or_else(|| {
            bag.error(
                "GEN-INTERVAL-COLUMN-MISSING",
                format!("{path}.columns.open"),
                format!(
                    "temporal.interval `open` column `{name}` does not exist on table `{}`",
                    table.name
                ),
            );
            None
        })
    });

    let open_probability = config
        .args
        .get("open_probability")
        .and_then(as_f64)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);

    // A non-nullable end cannot represent an open (end-less) row.
    if open_probability > 0.0 {
        if let Some(end) = end_col {
            if !end.nullable {
                bag.error(
                    "GEN-INTERVAL-OPEN-END",
                    format!("{path}.open_probability"),
                    format!(
                        "temporal.interval has open_probability {open_probability} but its `end` column `{}` is not nullable; an open row needs a null end",
                        end.name
                    ),
                );
            }
        }
    }

    let end_inclusive = config
        .args
        .get("end_inclusive")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let start_kind = compile_start(config.args.get("start"), path, &mut bag);
    let (duration_draw, unit_nanos) = compile_duration(config.args.get("duration"), path, &mut bag);
    let zone = compile_zone(config.args.get("timezone"), path, &mut bag);

    // Bail before touching the (possibly unresolved) column roles.
    if bag.has_errors() {
        return Err(bag);
    }

    let start = start_col.expect("start resolved without errors");
    let end = end_col.expect("end resolved without errors");
    let duration = duration_col.expect("duration resolved without errors");
    let start_kind = start_kind.expect("start kind resolved without errors");
    let duration_draw = duration_draw.expect("duration resolved without errors");
    let unit_nanos = unit_nanos.expect("duration unit resolved without errors");
    let zone = zone.expect("zone resolved without errors");

    let start = TimestampColumn {
        name: start.name.clone(),
        family: start.family.clone(),
    };
    let end = TimestampColumn {
        name: end.name.clone(),
        family: end.family.clone(),
    };
    let duration = DurationColumn {
        name: duration.name.clone(),
        family: duration.family.clone(),
        nullable: duration.nullable,
        unit_nanos,
    };
    let open = open_col.map(|column| OpenColumn {
        name: column.name.clone(),
        family: column.family.clone(),
        open_value: config
            .args
            .get("open_value")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    });

    let mut writes = vec![start.name.clone(), end.name.clone(), duration.name.clone()];
    if let Some(open) = &open {
        writes.push(open.name.clone());
    }

    let (start_rng, open_rng, duration_rng) = build_streams(context, &start, &open, &duration);
    let predicates = build_predicates(
        &start,
        &end,
        &duration,
        open.as_ref(),
        &start_kind,
        open_probability,
        end_inclusive,
    );

    Ok(TemporalIntervalPlanner {
        writes,
        start,
        end,
        duration,
        open,
        start_kind,
        duration_draw,
        open_probability,
        zone,
        start_rng,
        open_rng,
        duration_rng,
        predicates,
    })
}

/// Resolve a required column role to its schema column, reporting a missing
/// role or a role naming an absent column.
fn resolve_role<'a>(
    columns: Option<&Value>,
    role: &str,
    table: &'a PortableTable,
    path: &str,
    bag: &mut DiagnosticBag,
) -> Option<&'a PortableColumn> {
    let Some(name) = role_name(columns, role) else {
        bag.error(
            "GEN-INTERVAL-COLUMN-MISSING",
            format!("{path}.columns.{role}"),
            format!("temporal.interval requires a `{role}` column under `columns`"),
        );
        return None;
    };
    let column = find_column(table, name);
    if column.is_none() {
        bag.error(
            "GEN-INTERVAL-COLUMN-MISSING",
            format!("{path}.columns.{role}"),
            format!(
                "temporal.interval `{role}` column `{name}` does not exist on table `{}`",
                table.name
            ),
        );
    }
    column
}

/// The column name a `columns:` mapping assigns to `role`, if any.
fn role_name<'a>(columns: Option<&'a Value>, role: &str) -> Option<&'a str> {
    columns?.get(role).and_then(Value::as_str)
}

fn find_column<'a>(table: &'a PortableTable, name: &str) -> Option<&'a PortableColumn> {
    table.columns.iter().find(|column| column.name == name)
}

/// Compile the `start:` block into a [`StartKind`]. Returns `None` (after
/// recording an error) when the config is malformed.
fn compile_start(start: Option<&Value>, path: &str, bag: &mut DiagnosticBag) -> Option<StartKind> {
    let kind = start.and_then(|s| s.get("kind")).and_then(Value::as_str);
    let min_ns = start
        .and_then(|s| s.get("min"))
        .and_then(as_instant_ns)
        .or_else(|| {
            bag.error(
                "GEN-INTERVAL-START",
                format!("{path}.start.min"),
                "temporal.interval `start` requires a parseable `min` timestamp".to_string(),
            );
            None
        })?;

    match kind {
        Some("monotonic") => {
            let step_seconds = start
                .and_then(|s| s.get("step_seconds"))
                .and_then(as_i128)
                .unwrap_or(1)
                .max(1);
            Some(StartKind::Monotonic {
                min_ns,
                step_ns: step_seconds.saturating_mul(NANOS_PER_SECOND),
            })
        }
        // `range`, `observed_range`, or an omitted kind: a bounded random draw.
        _ => {
            let max_ns = start
                .and_then(|s| s.get("max"))
                .and_then(as_instant_ns)
                .or_else(|| {
                    bag.error(
                        "GEN-INTERVAL-START",
                        format!("{path}.start.max"),
                        "temporal.interval `start` range requires a parseable `max` timestamp"
                            .to_string(),
                    );
                    None
                })?;
            if max_ns < min_ns {
                bag.error(
                    "GEN-INTERVAL-START",
                    format!("{path}.start"),
                    "temporal.interval `start.max` is before `start.min`".to_string(),
                );
                return None;
            }
            Some(StartKind::Range { min_ns, max_ns })
        }
    }
}

/// Compile the `duration:` block into a [`DurationDraw`] plus its unit size in
/// nanoseconds. Reports negative and overflowing durations and unknown units.
fn compile_duration(
    duration: Option<&Value>,
    path: &str,
    bag: &mut DiagnosticBag,
) -> (Option<DurationDraw>, Option<i128>) {
    let unit = duration
        .and_then(|d| d.get("unit"))
        .and_then(Value::as_str)
        .unwrap_or("seconds");
    let Some(unit_nanos) = unit_nanos(unit) else {
        bag.error(
            "GEN-INTERVAL-DURATION",
            format!("{path}.duration.unit"),
            format!("temporal.interval `duration.unit` `{unit}` is not a recognized time unit"),
        );
        return (None, None);
    };

    let field_i128 = |key: &str| duration.and_then(|d| d.get(key)).and_then(as_i128);
    let field_f64 = |key: &str| duration.and_then(|d| d.get(key)).and_then(as_f64);
    let kind = duration.and_then(|d| d.get("kind")).and_then(Value::as_str);

    let draw = match kind {
        Some("fixed") => {
            let value = field_i128("value").unwrap_or(0);
            check_nonneg_bounded(value, unit_nanos, path, bag);
            DurationDraw::Fixed(value)
        }
        Some("normal") => {
            let mean = field_f64("mean").unwrap_or(0.0);
            let stddev = field_f64("stddev").unwrap_or(0.0).abs();
            let min = field_i128("min").unwrap_or(0);
            let max = field_i128("max").unwrap_or(min);
            check_nonneg_bounded(min, unit_nanos, path, bag);
            check_nonneg_bounded(max, unit_nanos, path, bag);
            DurationDraw::Normal {
                mean,
                stddev,
                min,
                max,
            }
        }
        Some("histogram") | Some("observed") => {
            let min = field_i128("min").unwrap_or(0);
            let max = field_i128("max").unwrap_or(min);
            check_nonneg_bounded(min, unit_nanos, path, bag);
            check_nonneg_bounded(max, unit_nanos, path, bag);
            DurationDraw::Skewed { min, max }
        }
        // `uniform`, or an omitted kind.
        _ => {
            let min = field_i128("min").unwrap_or(0);
            let max = field_i128("max").unwrap_or(min);
            check_nonneg_bounded(min, unit_nanos, path, bag);
            check_nonneg_bounded(max, unit_nanos, path, bag);
            DurationDraw::Uniform { min, max }
        }
    };

    (Some(draw), Some(unit_nanos))
}

/// Report a negative duration bound or one whose nanosecond span overflows.
fn check_nonneg_bounded(units: i128, unit_nanos: i128, path: &str, bag: &mut DiagnosticBag) {
    if units < 0 {
        bag.error(
            "GEN-INTERVAL-DURATION",
            format!("{path}.duration"),
            format!("temporal.interval duration `{units}` is negative; durations must be >= 0"),
        );
    } else if units.checked_mul(unit_nanos).is_none() {
        bag.error(
            "GEN-INTERVAL-DURATION",
            format!("{path}.duration"),
            format!(
                "temporal.interval duration `{units}` overflows the representable nanosecond range at this unit"
            ),
        );
    }
}

/// Compile the `timezone:` value into a [`RenderZone`]. An unknown IANA name is
/// a `GEN-INTERVAL-TIMEZONE` error.
fn compile_zone(zone: Option<&Value>, path: &str, bag: &mut DiagnosticBag) -> Option<RenderZone> {
    match zone.and_then(Value::as_str).unwrap_or("preserve") {
        "preserve" | "utc" | "UTC" => Some(RenderZone::Utc),
        name => match name.parse::<Tz>() {
            Ok(tz) => Some(RenderZone::Named(tz)),
            Err(_) => {
                bag.error(
                    "GEN-INTERVAL-TIMEZONE",
                    format!("{path}.timezone"),
                    format!("temporal.interval `timezone` `{name}` is not a valid IANA zone name"),
                );
                None
            }
        },
    }
}

/// Build the three independent per-row streams (start, open, duration) keyed on
/// each role's column so they stay stable and mutually independent.
fn build_streams(
    context: &CompileContext<'_>,
    start: &TimestampColumn,
    open: &Option<OpenColumn>,
    duration: &DurationColumn,
) -> (ChaCha8Rng, ChaCha8Rng, ChaCha8Rng) {
    let table = context.table().name.as_str();
    let open_key = open.as_ref().map_or("open", |column| column.name.as_str());
    (
        context.rng(StreamId::operator(
            table,
            start.name.clone(),
            "temporal.interval.start",
        )),
        context.rng(StreamId::operator(
            table,
            open_key.to_string(),
            "temporal.interval.open",
        )),
        context.rng(StreamId::operator(
            table,
            duration.name.clone(),
            "temporal.interval.duration",
        )),
    )
}

/// Build the verification predicates the planner guarantees over its columns.
fn build_predicates(
    start: &TimestampColumn,
    end: &TimestampColumn,
    duration: &DurationColumn,
    open: Option<&OpenColumn>,
    start_kind: &StartKind,
    open_probability: f64,
    end_inclusive: bool,
) -> Vec<PlannerPredicate> {
    let mut predicates = Vec::new();

    // Which rows are closed (and so satisfy the equation): all rows when no row
    // is ever open, else those the open flag / null end marks closed.
    let closed_guard = if open_probability > 0.0 {
        match open {
            Some(open) => Some(PredicateGuard::Flag {
                column: open.name.clone(),
                value: !open.open_value,
            }),
            None => Some(PredicateGuard::Null {
                column: end.name.clone(),
                is_null: false,
            }),
        }
    } else {
        None
    };

    predicates.push(PlannerPredicate::Equation {
        start: start.name.clone(),
        end: end.name.clone(),
        duration: duration.name.clone(),
        duration_unit_nanos: duration.unit_nanos,
        end_inclusive,
        guard: closed_guard,
    });

    // Open rows have a null end — checkable only when an explicit flag marks
    // them (otherwise "end is null when open" is a tautology on the end column).
    if open_probability > 0.0 {
        if let Some(open) = open {
            predicates.push(PlannerPredicate::NullWhen {
                column: end.name.clone(),
                guard: PredicateGuard::Flag {
                    column: open.name.clone(),
                    value: open.open_value,
                },
            });
        }
    }

    if let StartKind::Range { min_ns, max_ns } = start_kind {
        predicates.push(PlannerPredicate::InRange {
            column: start.name.clone(),
            min_nanos: *min_ns,
            max_nanos: *max_ns,
        });
    }

    predicates
}

// --- Value parsing helpers --------------------------------------------------

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
