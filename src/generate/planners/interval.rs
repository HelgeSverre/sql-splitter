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

use chrono_tz::Tz;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use serde_yaml_ng::Value;

use crate::diagnostic::DiagnosticBag;
use crate::generate::generators::observed::standard_normal;
use crate::generate::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledPlanner, Determinism,
    PlannerDescriptor, PlannerPredicate, PredicateGuard, Verification,
};
use crate::generate::seed::StreamId;
use crate::generate::value::{GenerateError, GeneratedValue};
use crate::synthetic::model::PlannerConfig;
use crate::synthetic::schema::SqlTypeFamily;

use super::structural::{
    check_nonneg_bounded, compile_instant_block, render_flag, render_instant, InstantDraw,
    RenderZone,
};
use super::{
    as_f64, as_i128, planner_factory, render_integer, resolve_role, unit_nanos, NANOS_PER_SECOND,
};

/// Static description of the `temporal.interval` planner.
pub static TEMPORAL_INTERVAL_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "temporal.interval",
    aliases: &[],
    summary: "Coordinates a start/end/duration/open interval group so closed rows satisfy end = start + duration.",
    arguments: &[
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps the required start, end, and duration roles plus an optional open role to columns.",
        },
        ArgumentSpec {
            name: "start",
            required: true,
            summary: "Configures the range, observed range, or monotonic start timestamp draw.",
        },
        ArgumentSpec {
            name: "duration",
            required: false,
            summary: "Configures the fixed, uniform, normal, histogram, or observed duration draw.",
        },
        ArgumentSpec {
            name: "open_probability",
            required: false,
            summary: "Probability that a row is open and therefore has no end timestamp.",
        },
        ArgumentSpec {
            name: "open_value",
            required: false,
            summary: "Boolean value written to the optional open column for an open row.",
        },
        ArgumentSpec {
            name: "end_inclusive",
            required: false,
            summary: "Whether the end timestamp is inclusive of the final duration unit.",
        },
        ArgumentSpec {
            name: "timezone",
            required: false,
            summary: "Timestamp rendering zone: preserve, utc, or an IANA timezone name.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
    cross_table: false,
};

/// Factory for the `temporal.interval` planner.
pub struct TemporalIntervalFactory;

planner_factory!(
    TemporalIntervalFactory,
    TEMPORAL_INTERVAL_DESCRIPTOR,
    compile_interval
);

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
    start_kind: InstantDraw,
    duration_draw: DurationDraw,
    open_probability: f64,
    end_inclusive: bool,
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
        // Rendering and verification operate at whole-second precision, so align
        // the drawn start to a second boundary. Otherwise a sub-second start
        // absorbs the inclusive `end = start + span - 1ns` adjustment and the
        // rendered end lands a second later than the verifier — which recomputes
        // from the second-precision start it parses back — expects.
        let start_ns = self.draw_start(row_index).div_euclid(NANOS_PER_SECOND) * NANOS_PER_SECOND;
        let is_open =
            self.open_probability > 0.0 && self.open_rng.random::<f64>() < self.open_probability;
        let duration_units = self.draw_duration();

        output[0] = render_instant(start_ns, &self.zone, &self.start.family)?;

        if is_open {
            output[1] = GeneratedValue::Null;
            output[2] = self.open_duration();
        } else {
            let span_ns = duration_units
                .checked_mul(self.duration.unit_nanos)
                .ok_or_else(interval_overflow)?;
            // Inclusive `[start, end]` renders `end` as the last covered instant
            // — one nanosecond (the smallest internal unit) before the half-open
            // boundary. Exclusive `[start, end)` renders the boundary itself.
            // A positive duration is guaranteed at compile time for the
            // inclusive case, so the subtraction never goes below `start`.
            let delta_ns = if self.end_inclusive {
                span_ns - 1
            } else {
                span_ns
            };
            let end_ns = start_ns
                .checked_add(delta_ns)
                .ok_or_else(interval_overflow)?;
            output[1] = render_instant(end_ns, &self.zone, &self.end.family)?;
            output[2] = render_integer(duration_units, &self.duration.family);
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
        self.start_kind.draw(&mut self.start_rng, row_index)
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
                let z = standard_normal(&mut self.duration_rng);
                let value = (mean + z * stddev).round() as i128;
                // Tolerate an inverted range (min > max) like the uniform/skewed
                // arms: collapse to `min` rather than panicking in std `clamp`.
                value.clamp(min, min.max(max))
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

    /// The duration value an open row carries: `NULL` when the column is
    /// nullable, otherwise a coherent zero (no completed duration yet).
    fn open_duration(&self) -> GeneratedValue {
        if self.duration.nullable {
            GeneratedValue::Null
        } else {
            render_integer(0, &self.duration.family)
        }
    }
}

// --- Rendering --------------------------------------------------------------

/// The error for an interval arithmetic step that overflows the representable
/// instant range.
fn interval_overflow() -> GenerateError {
    GenerateError::Overflow(
        "temporal.interval: start + duration overflows the representable instant range".to_string(),
    )
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
    const PLANNER: &str = TEMPORAL_INTERVAL_DESCRIPTOR.kind;
    const COLUMN_CODE: &str = crate::diagnostic::codes::INTERVAL_COLUMN_MISSING.code;
    let mut resolve = |role: &str, required: bool| {
        resolve_role(
            columns,
            role,
            table,
            path,
            PLANNER,
            COLUMN_CODE,
            required,
            &mut bag,
        )
    };
    let start_col = resolve("start", true);
    let end_col = resolve("end", true);
    let duration_col = resolve("duration", true);
    // `open` is optional; only resolve it (and validate existence) if named.
    let open_col = resolve("open", false);

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
                    crate::diagnostic::codes::INTERVAL_OPEN_END.code,
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

    let start_kind = compile_instant_block(
        config.args.get("start"),
        "temporal.interval `start`",
        crate::diagnostic::codes::INTERVAL_START.code,
        &format!("{path}.start"),
        &mut bag,
    );
    let (duration_draw, unit_nanos) = compile_duration(config.args.get("duration"), path, &mut bag);
    let zone = compile_zone(config.args.get("timezone"), path, &mut bag);

    // An inclusive interval renders `end = start + duration - 1ns`, so a
    // zero-length interval (minimum duration of 0) is impossible: `end` would
    // fall before `start`. Require a strictly positive minimum duration.
    if end_inclusive {
        if let Some(draw) = &duration_draw {
            if min_duration_units(draw) < 1 {
                bag.error(
                    crate::diagnostic::codes::INTERVAL_DURATION.code,
                    format!("{path}.duration"),
                    "temporal.interval `end_inclusive: true` requires a minimum duration of at least 1 unit; a zero-length closed interval is impossible".to_string(),
                );
            }
        }
    }

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
        end_inclusive,
        zone,
        start_rng,
        open_rng,
        duration_rng,
        predicates,
    })
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
            crate::diagnostic::codes::INTERVAL_DURATION.code,
            format!("{path}.duration.unit"),
            format!("temporal.interval `duration.unit` `{unit}` is not a recognized time unit"),
        );
        return (None, None);
    };

    const DURATION_CODE: &str = crate::diagnostic::codes::INTERVAL_DURATION.code;
    let duration_path = format!("{path}.duration");
    let field_i128 = |key: &str| duration.and_then(|d| d.get(key)).and_then(as_i128);
    let field_f64 = |key: &str| duration.and_then(|d| d.get(key)).and_then(as_f64);
    let kind = duration.and_then(|d| d.get("kind")).and_then(Value::as_str);

    let draw = match kind {
        Some("fixed") => {
            let value = field_i128("value").unwrap_or(0);
            check_nonneg_bounded(
                value,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
            DurationDraw::Fixed(value)
        }
        Some("normal") => {
            let mean = field_f64("mean").unwrap_or(0.0);
            let stddev = field_f64("stddev").unwrap_or(0.0).abs();
            let min = field_i128("min").unwrap_or(0);
            let max = field_i128("max").unwrap_or(min);
            check_nonneg_bounded(
                min,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
            check_nonneg_bounded(
                max,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
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
            check_nonneg_bounded(
                min,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
            check_nonneg_bounded(
                max,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
            DurationDraw::Skewed { min, max }
        }
        // `uniform`, or an omitted kind.
        _ => {
            let min = field_i128("min").unwrap_or(0);
            let max = field_i128("max").unwrap_or(min);
            check_nonneg_bounded(
                min,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
            check_nonneg_bounded(
                max,
                unit_nanos,
                "temporal.interval",
                "duration",
                DURATION_CODE,
                &duration_path,
                bag,
            );
            DurationDraw::Uniform { min, max }
        }
    };

    (Some(draw), Some(unit_nanos))
}

/// The smallest duration (in whole units) a draw can ever yield. Used to reject
/// a zero-length inclusive interval at compile time.
fn min_duration_units(draw: &DurationDraw) -> i128 {
    match draw {
        DurationDraw::Fixed(value) => *value,
        DurationDraw::Uniform { min, .. }
        | DurationDraw::Skewed { min, .. }
        | DurationDraw::Normal { min, .. } => *min,
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
                    crate::diagnostic::codes::INTERVAL_TIMEZONE.code,
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
    start_kind: &InstantDraw,
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

    if let InstantDraw::Range { min_ns, max_ns } = start_kind {
        predicates.push(PlannerPredicate::InRange {
            column: start.name.clone(),
            min_nanos: *min_ns,
            max_nanos: *max_ns,
        });
    }

    predicates
}
