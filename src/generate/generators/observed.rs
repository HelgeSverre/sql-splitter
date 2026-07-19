//! Observed and statistical generators.
//!
//! These generators replay a source dump's *shape* rather than a hand-authored
//! rule: a bounded set of observed values (`observed_sample`), a numeric
//! histogram (`histogram`), a Gaussian (`normal`) or log-Gaussian (`lognormal`)
//! fit, and a monotonic sequence with observed gaps (`monotonic`).
//!
//! Every one compiles its parameters exactly once: weights are pre-summed into
//! cumulative thresholds, histogram bins are validated (sorted, non-overlapping,
//! finite) and pre-summed, and numeric ranges are captured as plain scalars. The
//! per-row hot path never re-parses config. Compilation rejects the parameter
//! shapes that could produce nonsense values: non-finite numbers, unsorted or
//! overlapping bins, an empty `observed_sample`, and impossible ranges
//! (`min > max`).
//!
//! `observed_sample` is the one generator that embeds *literal source values*.
//! Inference records that risk (a source-derived rule) so the plan/report can
//! surface it; the generator itself simply replays the bounded set.

use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::model::GeneratorConfig;
use crate::synthetic::schema::{PortableColumn, SqlTypeFamily};

use crate::generate::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, Determinism,
    ExtensionRegistry, GeneratorDescriptor, GeneratorFactory, RowContext, Verification,
};
use crate::generate::seed::StreamId;
use crate::generate::value::{GenerateError, GeneratedValue};

// --- Shared helpers ----------------------------------------------------------

fn column<'a>(context: &CompileContext<'a>) -> &'a PortableColumn {
    context
        .column()
        .expect("observed generators are column-scoped")
}

fn stream(context: &CompileContext<'_>, kind: &str) -> ChaCha8Rng {
    let table = context.table().name.clone();
    let col = column(context).name.clone();
    context.rng(StreamId::column(table, col, kind.to_string()))
}

fn parse_f64(value: &serde_yaml_ng::Value) -> Option<f64> {
    match value {
        serde_yaml_ng::Value::Number(n) => n.as_f64(),
        serde_yaml_ng::Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_i128(value: &serde_yaml_ng::Value) -> Option<i128> {
    match value {
        serde_yaml_ng::Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_f64().map(|f| f as i128)),
        serde_yaml_ng::Value::String(s) => s.trim().parse::<i128>().ok(),
        _ => None,
    }
}

fn parse_u32(value: &serde_yaml_ng::Value) -> Option<u32> {
    parse_i128(value).and_then(|n| u32::try_from(n).ok())
}

/// Render a scalar YAML value as the text an `observed_sample` replays for a
/// text-family column.
fn display_yaml(value: &serde_yaml_ng::Value) -> String {
    match value {
        serde_yaml_ng::Value::Null => String::new(),
        serde_yaml_ng::Value::Bool(b) => b.to_string(),
        serde_yaml_ng::Value::Number(n) => n.to_string(),
        serde_yaml_ng::Value::String(s) => s.clone(),
        other => serde_yaml_ng::to_string(other)
            .unwrap_or_default()
            .trim_end()
            .to_string(),
    }
}

/// Coerce a literal `observed_sample` value into a [`GeneratedValue`] for
/// `family`. Text-ish families keep the rendered string; numeric families parse.
fn coerce_sample(
    value: &serde_yaml_ng::Value,
    family: &SqlTypeFamily,
) -> Result<GeneratedValue, String> {
    if matches!(value, serde_yaml_ng::Value::Null) {
        return Ok(GeneratedValue::Null);
    }
    match family {
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => parse_i128(value)
            .map(GeneratedValue::Integer)
            .ok_or_else(|| format!("expected an integer, found `{}`", display_yaml(value))),
        SqlTypeFamily::Boolean => match value {
            serde_yaml_ng::Value::Bool(b) => Ok(GeneratedValue::Boolean(*b)),
            other => Err(format!(
                "expected a boolean, found `{}`",
                display_yaml(other)
            )),
        },
        SqlTypeFamily::DateTime => Ok(GeneratedValue::DateTime(display_yaml(value))),
        SqlTypeFamily::Json => Ok(GeneratedValue::Json(display_yaml(value))),
        _ => Ok(GeneratedValue::Text(display_yaml(value))),
    }
}

/// Draw an index into a cumulative-weight table via one uniform draw.
fn draw_weighted(cumulative: &[f64], rng: &mut ChaCha8Rng) -> usize {
    let total = *cumulative.last().expect("non-empty by construction");
    let draw = rng.random::<f64>() * total;
    cumulative
        .iter()
        .position(|&threshold| draw < threshold)
        .unwrap_or(cumulative.len() - 1)
}

/// Emit a numeric sample as the representation `family` expects (decimal at
/// `scale`, otherwise a rounded integer).
fn numeric_value(sample: f64, family: &SqlTypeFamily, scale: u32) -> GeneratedValue {
    match family {
        SqlTypeFamily::Decimal => {
            let minor = (sample * 10f64.powi(scale as i32)).round() as i128;
            GeneratedValue::Decimal { minor, scale }
        }
        _ => GeneratedValue::Integer(sample.round() as i128),
    }
}

const ALL_FAMILIES: &[SqlTypeFamily] = &[
    SqlTypeFamily::Integer,
    SqlTypeFamily::BigInteger,
    SqlTypeFamily::Decimal,
    SqlTypeFamily::Boolean,
    SqlTypeFamily::Text,
    SqlTypeFamily::Bytes,
    SqlTypeFamily::Uuid,
    SqlTypeFamily::DateTime,
    SqlTypeFamily::Json,
    SqlTypeFamily::Other,
];

const NUMERIC_FAMILIES: &[SqlTypeFamily] = &[
    SqlTypeFamily::Integer,
    SqlTypeFamily::BigInteger,
    SqlTypeFamily::Decimal,
];

const INTEGER_FAMILIES: &[SqlTypeFamily] = &[SqlTypeFamily::Integer, SqlTypeFamily::BigInteger];

// --- observed_sample ---------------------------------------------------------

/// The `observed_sample` generator: a weighted replay of a bounded set of
/// values captured from the source dump. Each entry is `{ value, weight }`, or a
/// bare value (weight 1). Embeds literal source values, so inference marks it
/// source-derived.
pub struct ObservedSampleFactory;

static OBSERVED_SAMPLE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "observed_sample",
    aliases: &[],
    summary: "Weighted replay of a bounded set of values observed in the source dump.",
    arguments: &[ArgumentSpec {
        name: "values",
        required: true,
        summary: "The non-empty list of `{ value, weight }` entries (or bare values).",
    }],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for ObservedSampleFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &OBSERVED_SAMPLE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(raw) = config.args.get("values").and_then(|v| v.as_sequence()) else {
            bag.error(
                crate::diagnostic::codes::OBSERVED_SAMPLE_MISSING_VALUES.code,
                context.path(),
                "`observed_sample` requires a non-empty `values` list",
            );
            return Err(bag);
        };
        if raw.is_empty() {
            bag.error(
                crate::diagnostic::codes::OBSERVED_SAMPLE_EMPTY.code,
                context.path(),
                "`observed_sample.values` must not be empty",
            );
            return Err(bag);
        }

        let family = column(context).family.clone();
        let mut values = Vec::with_capacity(raw.len());
        let mut cumulative = Vec::with_capacity(raw.len());
        let mut total = 0.0f64;
        for entry in raw {
            let (raw_value, weight) = match entry {
                serde_yaml_ng::Value::Mapping(map) if map.contains_key("value") => {
                    let weight = map.get("weight").and_then(parse_f64).unwrap_or(1.0);
                    (map.get("value").expect("checked above"), weight)
                }
                other => (other, 1.0),
            };
            if !weight.is_finite() || weight < 0.0 {
                bag.error(
                    crate::diagnostic::codes::OBSERVED_SAMPLE_INVALID_WEIGHT.code,
                    context.path(),
                    format!("`observed_sample` weight {weight} must be finite and non-negative"),
                );
                continue;
            }
            match coerce_sample(raw_value, &family) {
                Ok(value) => {
                    total += weight;
                    values.push(value);
                    cumulative.push(total);
                }
                Err(message) => {
                    bag.error(
                        crate::diagnostic::codes::OBSERVED_SAMPLE_INVALID_VALUE.code,
                        context.path(),
                        message,
                    );
                }
            }
        }
        if total <= 0.0 {
            bag.error(
                crate::diagnostic::codes::OBSERVED_SAMPLE_ALL_ZERO.code,
                context.path(),
                "`observed_sample.values` weights must not all be zero",
            );
        }
        bag.into_result(())?;

        let rng = stream(context, "observed_sample");
        Ok(Box::new(CompiledObservedSample {
            values,
            cumulative,
            rng,
        }))
    }
}

struct CompiledObservedSample {
    values: Vec<GeneratedValue>,
    cumulative: Vec<f64>,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledObservedSample {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let index = draw_weighted(&self.cumulative, &mut self.rng);
        output.clone_from(&self.values[index]);
        Ok(())
    }
}

// --- histogram ---------------------------------------------------------------

/// The `histogram` generator: samples a bin by observed frequency, then draws a
/// uniform value inside that bin. Bins are `{ min, max, count }` and must be
/// sorted, non-overlapping, and finite.
pub struct HistogramFactory;

static HISTOGRAM_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "histogram",
    aliases: &[],
    summary: "Samples a numeric value from an observed frequency histogram.",
    arguments: &[
        ArgumentSpec {
            name: "bins",
            required: true,
            summary: "Sorted, non-overlapping `{ min, max, count }` bins.",
        },
        ArgumentSpec {
            name: "scale",
            required: false,
            summary: "Decimal places for decimal columns; defaults to 0.",
        },
    ],
    accepts: NUMERIC_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct Bin {
    min: f64,
    max: f64,
}

impl GeneratorFactory for HistogramFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &HISTOGRAM_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(raw) = config.args.get("bins").and_then(|v| v.as_sequence()) else {
            bag.error(
                crate::diagnostic::codes::HISTOGRAM_MISSING_BINS.code,
                context.path(),
                "`histogram` requires a non-empty `bins` list",
            );
            return Err(bag);
        };
        if raw.is_empty() {
            bag.error(
                crate::diagnostic::codes::HISTOGRAM_EMPTY.code,
                context.path(),
                "`histogram.bins` must not be empty",
            );
            return Err(bag);
        }

        let mut bins = Vec::with_capacity(raw.len());
        let mut cumulative = Vec::with_capacity(raw.len());
        let mut total = 0.0f64;
        let mut previous_max: Option<f64> = None;
        for entry in raw {
            let min = entry.get("min").and_then(parse_f64);
            let max = entry.get("max").and_then(parse_f64);
            let count = entry.get("count").and_then(parse_f64).unwrap_or(1.0);
            let (Some(min), Some(max)) = (min, max) else {
                bag.error(
                    crate::diagnostic::codes::HISTOGRAM_INVALID_BIN.code,
                    context.path(),
                    "each `histogram.bins` entry needs numeric `min` and `max`",
                );
                continue;
            };
            if !min.is_finite() || !max.is_finite() || !count.is_finite() || count < 0.0 {
                bag.error(
                    crate::diagnostic::codes::HISTOGRAM_NON_FINITE.code,
                    context.path(),
                    "`histogram` bin bounds and counts must be finite, counts non-negative",
                );
                continue;
            }
            if min > max {
                bag.error(
                    crate::diagnostic::codes::HISTOGRAM_RANGE.code,
                    context.path(),
                    format!("`histogram` bin min ({min}) must not exceed max ({max})"),
                );
                continue;
            }
            if let Some(prev) = previous_max {
                if min < prev {
                    bag.error(
                        crate::diagnostic::codes::HISTOGRAM_UNSORTED.code,
                        context.path(),
                        "`histogram.bins` must be sorted and non-overlapping",
                    );
                }
            }
            previous_max = Some(max);
            total += count;
            bins.push(Bin { min, max });
            cumulative.push(total);
        }
        if total <= 0.0 {
            bag.error(
                crate::diagnostic::codes::HISTOGRAM_ALL_ZERO.code,
                context.path(),
                "`histogram.bins` counts must not all be zero",
            );
        }
        bag.into_result(())?;

        let scale = config.args.get("scale").and_then(parse_u32).unwrap_or(0);
        let family = column(context).family.clone();
        let rng = stream(context, "histogram");
        Ok(Box::new(CompiledHistogram {
            bins,
            cumulative,
            family,
            scale,
            rng,
        }))
    }
}

struct CompiledHistogram {
    bins: Vec<Bin>,
    cumulative: Vec<f64>,
    family: SqlTypeFamily,
    scale: u32,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledHistogram {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let index = draw_weighted(&self.cumulative, &mut self.rng);
        let bin = &self.bins[index];
        let sample = if bin.min == bin.max {
            bin.min
        } else {
            bin.min + self.rng.random::<f64>() * (bin.max - bin.min)
        };
        *output = numeric_value(sample, &self.family, self.scale);
        Ok(())
    }
}

// --- normal / lognormal ------------------------------------------------------

/// Standard-normal draw via Box–Muller, using two uniform draws.
fn standard_normal(rng: &mut ChaCha8Rng) -> f64 {
    let u1 = rng.random::<f64>().max(f64::MIN_POSITIVE);
    let u2 = rng.random::<f64>();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// The `normal` generator: a Gaussian fit `N(mean, std)`, optionally clamped to
/// `[min, max]`.
pub struct NormalFactory;

static NORMAL_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "normal",
    aliases: &[],
    summary: "A Gaussian-distributed numeric value fit to `mean`/`std`.",
    arguments: &[
        ArgumentSpec {
            name: "mean",
            required: true,
            summary: "Distribution mean.",
        },
        ArgumentSpec {
            name: "std",
            required: true,
            summary: "Standard deviation (finite, non-negative).",
        },
        ArgumentSpec {
            name: "min",
            required: false,
            summary: "Inclusive lower clamp.",
        },
        ArgumentSpec {
            name: "max",
            required: false,
            summary: "Inclusive upper clamp.",
        },
        ArgumentSpec {
            name: "scale",
            required: false,
            summary: "Decimal places for decimal columns; defaults to 0.",
        },
    ],
    accepts: NUMERIC_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

/// The `lognormal` generator: `exp(N(mu, sigma))`, optionally clamped.
pub struct LogNormalFactory;

static LOGNORMAL_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "lognormal",
    aliases: &[],
    summary: "A log-normally distributed numeric value fit to log-space `mu`/`sigma`.",
    arguments: &[
        ArgumentSpec {
            name: "mu",
            required: true,
            summary: "Mean of the underlying normal (log space).",
        },
        ArgumentSpec {
            name: "sigma",
            required: true,
            summary: "Std-dev of the underlying normal (finite, non-negative).",
        },
        ArgumentSpec {
            name: "min",
            required: false,
            summary: "Inclusive lower clamp.",
        },
        ArgumentSpec {
            name: "max",
            required: false,
            summary: "Inclusive upper clamp.",
        },
        ArgumentSpec {
            name: "scale",
            required: false,
            summary: "Decimal places for decimal columns; defaults to 0.",
        },
    ],
    accepts: NUMERIC_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

/// Whether a Gaussian generator emits `normal` or `lognormal` shaped values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GaussianShape {
    Normal,
    LogNormal,
}

fn compile_gaussian(
    shape: GaussianShape,
    kind: &'static str,
    first_arg: &str,
    second_arg: &str,
    config: &GeneratorConfig,
    context: &CompileContext<'_>,
) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
    let mut bag = DiagnosticBag::default();
    let center = config.args.get(first_arg).and_then(parse_f64);
    let spread = config.args.get(second_arg).and_then(parse_f64);
    let (Some(center), Some(spread)) = (center, spread) else {
        bag.error(
            crate::diagnostic::codes::GAUSSIAN_MISSING_PARAMS.code,
            context.path(),
            format!("`{kind}` requires numeric `{first_arg}` and `{second_arg}`"),
        );
        return Err(bag);
    };
    if !center.is_finite() || !spread.is_finite() || spread < 0.0 {
        bag.error(
            crate::diagnostic::codes::GAUSSIAN_NON_FINITE.code,
            context.path(),
            format!("`{kind}` parameters must be finite and `{second_arg}` non-negative"),
        );
    }
    let min = config.args.get("min").and_then(parse_f64);
    let max = config.args.get("max").and_then(parse_f64);
    if let (Some(min), Some(max)) = (min, max) {
        if min > max {
            bag.error(
                crate::diagnostic::codes::GAUSSIAN_RANGE.code,
                context.path(),
                format!("`{kind}.min` ({min}) must not exceed `{kind}.max` ({max})"),
            );
        }
    }
    bag.into_result(())?;

    let scale = config.args.get("scale").and_then(parse_u32).unwrap_or(0);
    let family = column(context).family.clone();
    let rng = stream(context, kind);
    Ok(Box::new(CompiledGaussian {
        shape,
        center,
        spread,
        min,
        max,
        family,
        scale,
        rng,
    }))
}

impl GeneratorFactory for NormalFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &NORMAL_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        compile_gaussian(
            GaussianShape::Normal,
            "normal",
            "mean",
            "std",
            config,
            context,
        )
    }
}

impl GeneratorFactory for LogNormalFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &LOGNORMAL_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        compile_gaussian(
            GaussianShape::LogNormal,
            "lognormal",
            "mu",
            "sigma",
            config,
            context,
        )
    }
}

struct CompiledGaussian {
    shape: GaussianShape,
    center: f64,
    spread: f64,
    min: Option<f64>,
    max: Option<f64>,
    family: SqlTypeFamily,
    scale: u32,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledGaussian {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let z = standard_normal(&mut self.rng);
        let mut sample = match self.shape {
            GaussianShape::Normal => self.center + self.spread * z,
            GaussianShape::LogNormal => (self.center + self.spread * z).exp(),
        };
        if let Some(min) = self.min {
            sample = sample.max(min);
        }
        if let Some(max) = self.max {
            sample = sample.min(max);
        }
        *output = numeric_value(sample, &self.family, self.scale);
        Ok(())
    }
}

// --- monotonic ---------------------------------------------------------------

/// The `monotonic` generator: a non-decreasing integer sequence, `start +
/// row_index * step`, replaying an observed average gap. Row-indexed and so
/// fully reproducible.
pub struct MonotonicFactory;

static MONOTONIC_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "monotonic",
    aliases: &[],
    summary: "A non-decreasing integer sequence `start + row_index * step`.",
    arguments: &[
        ArgumentSpec {
            name: "start",
            required: false,
            summary: "The value at row 0; defaults to 0.",
        },
        ArgumentSpec {
            name: "step",
            required: false,
            summary: "The per-row increment; defaults to 1.",
        },
    ],
    accepts: INTEGER_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for MonotonicFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &MONOTONIC_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let start = config.args.get("start").and_then(parse_i128).unwrap_or(0);
        let step = config.args.get("step").and_then(parse_i128).unwrap_or(1);
        if step < 0 {
            bag.error(
                crate::diagnostic::codes::MONOTONIC_STEP.code,
                context.path(),
                "`monotonic.step` must not be negative",
            );
        }
        bag.into_result(Box::new(CompiledMonotonic { start, step }) as Box<dyn CompiledGenerator>)
    }
}

struct CompiledMonotonic {
    start: i128,
    step: i128,
}

impl CompiledGenerator for CompiledMonotonic {
    fn generate(
        &mut self,
        context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let index = i128::from(context.row_index());
        let value = self
            .start
            .checked_add(index.checked_mul(self.step).ok_or_else(|| {
                GenerateError::Overflow("`monotonic` sequence overflowed".to_string())
            })?)
            .ok_or_else(|| {
                GenerateError::Overflow("`monotonic` sequence overflowed".to_string())
            })?;
        *output = GeneratedValue::Integer(value);
        Ok(())
    }
}

// --- Registration ------------------------------------------------------------

/// Register the observed and statistical catalog into `registry`.
pub(crate) fn register_all(registry: &mut ExtensionRegistry) {
    registry
        .register_generator(Box::new(ObservedSampleFactory))
        .expect("built-in observed generator kinds are collision-free");
    registry
        .register_generator(Box::new(HistogramFactory))
        .expect("built-in observed generator kinds are collision-free");
    registry
        .register_generator(Box::new(NormalFactory))
        .expect("built-in observed generator kinds are collision-free");
    registry
        .register_generator(Box::new(LogNormalFactory))
        .expect("built-in observed generator kinds are collision-free");
    registry
        .register_generator(Box::new(MonotonicFactory))
        .expect("built-in observed generator kinds are collision-free");
}
