//! Core generator and modifier catalog.
//!
//! Every generator here compiles its YAML config exactly once into
//! [`CoreGenerator`], a closed enum of built-in operators. `CompiledCore`'s
//! `generate` matches on that enum directly — there is no string dispatch on
//! the per-row hot path, only a `match` on a plain discriminant.
//!
//! `constant` is deliberately not reimplemented here: [`super::ConstantFactory`]
//! already satisfies this module's factory/compiled split, so duplicating it
//! into [`CoreGenerator`] would only add a second, redundant implementation of
//! the same behavior.

use std::collections::HashSet;

use rand::{Rng, RngExt};
use rand_chacha::ChaCha8Rng;

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::model::{GeneratorConfig, ModifierConfig};
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

use crate::generate::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, CompiledModifier,
    Determinism, ExtensionRegistry, GeneratorDescriptor, GeneratorFactory, KeyRecipe,
    ModifierDescriptor, ModifierFactory, RowContext, Verification,
};
use crate::generate::seed::StreamId;
use crate::generate::value::{GenerateError, GeneratedValue};

// --- Shared helpers ----------------------------------------------------------

/// The column an operator is compiled against. Every generator and modifier
/// in this module is column-scoped, so the compiler always builds its
/// `CompileContext` with [`CompileContext::for_column`]; a missing column would
/// be a caller bug, not a data problem, hence the `expect`.
fn column<'a>(context: &CompileContext<'a>) -> &'a PortableColumn {
    context
        .column()
        .expect("core generators and modifiers are column-scoped")
}

/// The deterministic RNG stream for a column-scoped operator, keyed by
/// table, column, and the operator's own kind so two different generators on
/// the same column never share a stream.
fn stream(context: &CompileContext<'_>, kind: &str) -> ChaCha8Rng {
    let table = context.table().name.clone();
    let col = column(context).name.clone();
    context.rng(StreamId::column(table, col, kind.to_string()))
}

/// Render any [`GeneratedValue`] as the text a `template`/`format` operator
/// substitutes into its output.
fn display_value(value: &GeneratedValue) -> String {
    match value {
        GeneratedValue::Null | GeneratedValue::Default => String::new(),
        GeneratedValue::Boolean(b) => b.to_string(),
        GeneratedValue::Integer(i) => i.to_string(),
        GeneratedValue::Decimal { minor, scale } => format_decimal(*minor, *scale),
        GeneratedValue::Text(s) => s.clone(),
        GeneratedValue::Bytes(bytes) => hex::encode(bytes),
        GeneratedValue::DateTime(s) | GeneratedValue::Json(s) => s.clone(),
    }
}

/// Convert arbitrary text into a URL slug: lowercase, every run of
/// non-ASCII-alphanumeric characters collapses to a single `-`, and leading and
/// trailing dashes are trimmed. When `max_length` is set the result is truncated
/// to that many bytes (all slug bytes are ASCII, so bytes and chars coincide) and
/// any dash left dangling at the cut is trimmed. Non-ASCII-alphanumeric
/// characters (accents, CJK, …) act as separators; transliteration is out of
/// scope for this generator.
fn slugify(input: &str, max_length: Option<usize>) -> String {
    let mut slug = String::with_capacity(input.len());
    let mut pending_dash = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if pending_dash && !slug.is_empty() {
                slug.push('-');
            }
            pending_dash = false;
            slug.push(ch.to_ascii_lowercase());
        } else {
            // Defer the separator so trailing separators never emit a dash.
            pending_dash = true;
        }
    }
    if let Some(limit) = max_length {
        slug.truncate(limit);
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    slug
}

/// Render `minor` units at `scale` decimal places as a fixed-point string,
/// e.g. `(1050, 2)` -> `"10.50"`.
fn format_decimal(minor: i128, scale: u32) -> String {
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

/// A stable, order-independent key for tracking which values the `unique`
/// modifier has already emitted. `GeneratedValue` has no `Hash`/`Eq` impl (it
/// deliberately avoids one so its `Decimal`/float-adjacent variants aren't
/// pulled into hashing concerns outside this narrow use), so this renders a
/// canonical key from the value itself instead.
fn value_key(value: &GeneratedValue) -> String {
    match value {
        GeneratedValue::Null => "null".to_string(),
        GeneratedValue::Default => "default".to_string(),
        GeneratedValue::Boolean(b) => format!("bool:{b}"),
        GeneratedValue::Integer(i) => format!("int:{i}"),
        GeneratedValue::Decimal { minor, scale } => format!("dec:{minor}:{scale}"),
        GeneratedValue::Text(s) => format!("text:{s}"),
        GeneratedValue::Bytes(b) => format!("bytes:{}", hex::encode(b)),
        GeneratedValue::DateTime(s) => format!("datetime:{s}"),
        GeneratedValue::Json(s) => format!("json:{s}"),
    }
}

/// Minimal YAML -> string rendering for scalar config values (used for
/// literal template fragments and `display`-style coercion).
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

fn parse_f64(value: &serde_yaml_ng::Value) -> Option<f64> {
    match value {
        serde_yaml_ng::Value::Number(n) => n.as_f64(),
        serde_yaml_ng::Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_usize(value: &serde_yaml_ng::Value) -> Option<usize> {
    parse_i128(value).and_then(|n| usize::try_from(n).ok())
}

/// Parse a decimal literal (`10`, `10.5`, `"-3.140"`) into `(minor units,
/// scale)`, e.g. `"10.50"` -> `(1050, 2)`.
pub(super) fn parse_decimal(value: &serde_yaml_ng::Value) -> Option<(i128, u32)> {
    match value {
        serde_yaml_ng::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Some((i128::from(i), 0));
            }
            n.as_f64().and_then(|f| decimal_from_str(&f.to_string()))
        }
        serde_yaml_ng::Value::String(s) => decimal_from_str(s),
        _ => None,
    }
}

fn decimal_from_str(raw: &str) -> Option<(i128, u32)> {
    let trimmed = raw.trim();
    let (negative, unsigned) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (unsigned, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let scale = u32::try_from(frac_part.len()).ok()?;
    let digits = format!("{int_part}{frac_part}");
    let magnitude: i128 = digits.parse().ok()?;
    Some((if negative { -magnitude } else { magnitude }, scale))
}

fn json_from_yaml(value: &serde_yaml_ng::Value) -> Result<String, String> {
    let json_value: serde_json::Value = serde_json::to_value(value).map_err(|e| e.to_string())?;
    serde_json::to_string(&json_value).map_err(|e| e.to_string())
}

/// Coerce a raw YAML scalar into a [`GeneratedValue`] matching `family`, for
/// generators that accept literal values in their config (`constant`-style
/// values, `choice`/`weighted_choice` options, `json_value`'s default).
fn coerce_value(
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
        SqlTypeFamily::Decimal => parse_decimal(value)
            .map(|(minor, scale)| GeneratedValue::Decimal { minor, scale })
            .ok_or_else(|| format!("expected a decimal, found `{}`", display_yaml(value))),
        SqlTypeFamily::Boolean => match value {
            serde_yaml_ng::Value::Bool(b) => Ok(GeneratedValue::Boolean(*b)),
            other => Err(format!(
                "expected a boolean, found `{}`",
                display_yaml(other)
            )),
        },
        SqlTypeFamily::Bytes => Ok(GeneratedValue::Bytes(display_yaml(value).into_bytes())),
        SqlTypeFamily::Json => json_from_yaml(value).map(GeneratedValue::Json),
        SqlTypeFamily::DateTime => Ok(GeneratedValue::DateTime(display_yaml(value))),
        SqlTypeFamily::Text | SqlTypeFamily::Uuid | SqlTypeFamily::Other => {
            Ok(GeneratedValue::Text(display_yaml(value)))
        }
    }
}

/// Every family the broad, type-agnostic generators (`null`,
/// `database_default`, `choice`, `weighted_choice`) declare as `accepts`.
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

// --- The compiled-enum built-in ----------------------------------------------

/// Every core built-in generator, compiled once from its config. `generate` is
/// a single `match` over this enum — the hot path never re-parses YAML or
/// dispatches on a kind string.
enum CoreGenerator {
    Null,
    Sequence(SequenceState),
    Copy {
        source: String,
    },
    Slug {
        source: String,
        max_length: Option<usize>,
    },
    Template(Vec<TemplateFragment>),
    Pattern {
        mask: String,
        rng: ChaCha8Rng,
    },
    DatabaseDefault,
    Json(String),
    Integer(UniformInteger),
    Decimal(UniformDecimal),
    Boolean {
        probability: f64,
        rng: ChaCha8Rng,
        /// When `true`, the target column is an integer-family column (e.g. a
        /// MySQL `TINYINT(1)` boolean-by-convention) so the value is emitted as
        /// `0`/`1` rather than a native boolean.
        as_integer: bool,
    },
    String(UniformString),
    Bytes(UniformBytes),
    Uuid(ChaCha8Rng),
    Choice(ChoiceState),
    WeightedChoice(WeightedChoiceState),
}

struct SequenceState {
    /// The row-0 value, retained so the generator can describe itself as a
    /// random-access `Dense` key domain (see `CompiledGenerator::key_recipe`).
    start: i128,
    next: Option<i128>,
    step: i128,
}

enum TemplateFragment {
    Literal(String),
    Field(String),
}

struct UniformInteger {
    min: i128,
    max: i128,
    rng: ChaCha8Rng,
}

struct UniformDecimal {
    min_minor: i128,
    max_minor: i128,
    scale: u32,
    rng: ChaCha8Rng,
}

struct UniformString {
    min_len: usize,
    max_len: usize,
    rng: ChaCha8Rng,
}

struct UniformBytes {
    min_len: usize,
    max_len: usize,
    rng: ChaCha8Rng,
}

struct ChoiceState {
    values: Vec<GeneratedValue>,
    rng: ChaCha8Rng,
}

struct WeightedChoiceState {
    values: Vec<GeneratedValue>,
    /// Cumulative weight thresholds, same length as `values`, strictly
    /// increasing, last entry equal to the total weight.
    cumulative: Vec<f64>,
    rng: ChaCha8Rng,
}

/// The alphabet `string`/`pattern` draw plain characters from.
const ALPHANUMERIC: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const UPPER_ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const LOWER_ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
const DIGITS: &[u8] = b"0123456789";

fn random_alphabet_char(rng: &mut ChaCha8Rng, alphabet: &[u8]) -> char {
    let index = rng.random_range(0..alphabet.len());
    alphabet[index] as char
}

/// The compiled form of every [`CoreGenerator`] variant. One type, one
/// `match`, shared by every core factory below.
struct CompiledCore(CoreGenerator);

impl CompiledGenerator for CompiledCore {
    fn generate(
        &mut self,
        context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        match &mut self.0 {
            CoreGenerator::Null => {
                *output = GeneratedValue::Null;
            }
            CoreGenerator::Sequence(state) => {
                let current = state.next.ok_or_else(|| {
                    GenerateError::Overflow(
                        "sequence exhausted its representable range".to_string(),
                    )
                })?;
                *output = GeneratedValue::Integer(current);
                state.next = current.checked_add(state.step);
            }
            CoreGenerator::Copy { source } => {
                *output = context
                    .column(source)
                    .cloned()
                    .unwrap_or(GeneratedValue::Null);
            }
            CoreGenerator::Slug { source, max_length } => {
                let text = context
                    .column(source)
                    .map(display_value)
                    .unwrap_or_default();
                *output = GeneratedValue::Text(slugify(&text, *max_length));
            }
            CoreGenerator::Template(fragments) => {
                let mut rendered = String::new();
                for fragment in fragments {
                    match fragment {
                        TemplateFragment::Literal(text) => rendered.push_str(text),
                        TemplateFragment::Field(name) => {
                            if let Some(value) = context.column(name) {
                                rendered.push_str(&display_value(value));
                            }
                        }
                    }
                }
                *output = GeneratedValue::Text(rendered);
            }
            CoreGenerator::Pattern { mask, rng } => {
                let mut rendered = String::with_capacity(mask.len());
                for ch in mask.chars() {
                    let generated = match ch {
                        '#' => random_alphabet_char(rng, DIGITS),
                        '?' => random_alphabet_char(rng, UPPER_ALPHA),
                        '@' => random_alphabet_char(rng, LOWER_ALPHA),
                        '*' => random_alphabet_char(rng, ALPHANUMERIC),
                        literal => literal,
                    };
                    rendered.push(generated);
                }
                *output = GeneratedValue::Text(rendered);
            }
            CoreGenerator::DatabaseDefault => {
                *output = GeneratedValue::Default;
            }
            CoreGenerator::Json(rendered) => {
                *output = GeneratedValue::Json(rendered.clone());
            }
            CoreGenerator::Integer(state) => {
                let value = state.rng.random_range(state.min..=state.max);
                *output = GeneratedValue::Integer(value);
            }
            CoreGenerator::Decimal(state) => {
                let minor = state.rng.random_range(state.min_minor..=state.max_minor);
                *output = GeneratedValue::Decimal {
                    minor,
                    scale: state.scale,
                };
            }
            CoreGenerator::Boolean {
                probability,
                rng,
                as_integer,
            } => {
                let flag = rng.random_bool(*probability);
                *output = if *as_integer {
                    GeneratedValue::Integer(i128::from(flag))
                } else {
                    GeneratedValue::Boolean(flag)
                };
            }
            CoreGenerator::String(state) => {
                let len = if state.min_len == state.max_len {
                    state.min_len
                } else {
                    state.rng.random_range(state.min_len..=state.max_len)
                };
                let text: String = (0..len)
                    .map(|_| random_alphabet_char(&mut state.rng, ALPHANUMERIC))
                    .collect();
                *output = GeneratedValue::Text(text);
            }
            CoreGenerator::Bytes(state) => {
                let len = if state.min_len == state.max_len {
                    state.min_len
                } else {
                    state.rng.random_range(state.min_len..=state.max_len)
                };
                let mut bytes = vec![0u8; len];
                state.rng.fill_bytes(&mut bytes);
                *output = GeneratedValue::Bytes(bytes);
            }
            CoreGenerator::Uuid(rng) => {
                let mut bytes = [0u8; 16];
                rng.fill_bytes(&mut bytes);
                bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
                bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
                let hex = hex::encode(bytes);
                let formatted = format!(
                    "{}-{}-{}-{}-{}",
                    &hex[0..8],
                    &hex[8..12],
                    &hex[12..16],
                    &hex[16..20],
                    &hex[20..32]
                );
                *output = GeneratedValue::Text(formatted);
            }
            CoreGenerator::Choice(state) => {
                let index = state.rng.random_range(0..state.values.len());
                *output = state.values[index].clone();
            }
            CoreGenerator::WeightedChoice(state) => {
                let total = *state.cumulative.last().expect("non-empty by construction");
                let draw = state.rng.random::<f64>() * total;
                let index = state
                    .cumulative
                    .iter()
                    .position(|&threshold| draw < threshold)
                    .unwrap_or(state.values.len() - 1);
                *output = state.values[index].clone();
            }
        }
        Ok(())
    }

    fn key_recipe(&self) -> Option<KeyRecipe> {
        match &self.0 {
            // A sequence is row-ordinal: row `n` renders `start + n * step`,
            // exactly a dense integer key domain.
            CoreGenerator::Sequence(state) => Some(KeyRecipe::Dense {
                start: state.start,
                step: state.step,
            }),
            // A UUID key is reproducible per parent row via a row-indexed seed.
            CoreGenerator::Uuid(_) => Some(KeyRecipe::Uuid),
            _ => None,
        }
    }
}

// --- Factories ----------------------------------------------------------------

/// The `null` generator: always emits SQL `NULL`. Legal only on nullable
/// columns, checked once at compile time.
pub struct NullFactory;

static NULL_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "null",
    aliases: &[],
    summary: "Always emits SQL NULL.",
    arguments: &[],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for NullFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &NULL_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let col = column(context);
        if !col.nullable {
            bag.error(
                crate::diagnostic::codes::NULL_ON_NON_NULLABLE.code,
                context.path(),
                format!(
                    "`null` cannot generate column `{}`, which is not nullable",
                    col.name
                ),
            );
        }
        bag.into_result(Box::new(CompiledCore(CoreGenerator::Null)) as Box<dyn CompiledGenerator>)
    }
}

/// The `sequence` generator: an integer counter starting at `start` and
/// advancing by `step` each row.
pub struct SequenceFactory;

static SEQUENCE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "sequence",
    aliases: &[],
    summary: "An integer counter starting at `start`, advancing by `step`.",
    arguments: &[
        ArgumentSpec {
            name: "start",
            required: false,
            summary: "The first value emitted; defaults to 0.",
        },
        ArgumentSpec {
            name: "step",
            required: false,
            summary: "The increment applied after each row; defaults to 1.",
        },
    ],
    accepts: &[SqlTypeFamily::Integer, SqlTypeFamily::BigInteger],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for SequenceFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &SEQUENCE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let start = config.args.get("start").and_then(parse_i128).unwrap_or(0);
        let step = config.args.get("step").and_then(parse_i128).unwrap_or(1);
        let mut bag = DiagnosticBag::default();
        if step == 0 {
            bag.error(
                crate::diagnostic::codes::SEQUENCE_ZERO_STEP.code,
                context.path(),
                "`sequence.step` must not be zero",
            );
        }
        bag.into_result(
            Box::new(CompiledCore(CoreGenerator::Sequence(SequenceState {
                start,
                next: Some(start),
                step,
            }))) as Box<dyn CompiledGenerator>,
        )
    }
}

/// The `copy` generator: reuses a sibling column's already-generated value.
pub struct CopyFactory;

static COPY_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "copy",
    aliases: &[],
    summary: "Reuses the value already generated for another column on the same row.",
    arguments: &[ArgumentSpec {
        name: "source",
        required: true,
        summary: "The sibling column to copy the value from.",
    }],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for CopyFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &COPY_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(source) = config
            .args
            .get("source")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            bag.error(
                crate::diagnostic::codes::COPY_MISSING_SOURCE.code,
                context.path(),
                "`copy` requires a `source` column name",
            );
            return Err(bag);
        };

        match find_column(context.table(), &source) {
            None => {
                bag.error(
                    crate::diagnostic::codes::COPY_UNKNOWN_FIELD.code,
                    context.path(),
                    format!(
                        "`copy.source` references unknown column `{source}` on table `{}`",
                        context.table().name
                    ),
                );
            }
            Some(found) if found.family != column(context).family => {
                bag.error(
                    crate::diagnostic::codes::COPY_TYPE_MISMATCH.code,
                    context.path(),
                    format!(
                        "`copy.source` column `{source}` has family {:?}, but `{}` has family {:?}",
                        found.family,
                        column(context).name,
                        column(context).family
                    ),
                );
            }
            Some(_) => {}
        }

        bag.into_result(
            Box::new(CompiledCore(CoreGenerator::Copy { source })) as Box<dyn CompiledGenerator>
        )
    }
}

fn find_column<'a>(table: &'a PortableTable, name: &str) -> Option<&'a PortableColumn> {
    table.columns.iter().find(|c| c.name == name)
}

/// The `template` generator: joins literal fragments with sibling-column
/// substitutions. It does not evaluate conditions or expressions — only
/// literal text and `{ field: <name> }` references.
pub struct TemplateFactory;

static TEMPLATE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "template",
    aliases: &[],
    summary: "Joins literal text fragments with sibling-column values.",
    arguments: &[ArgumentSpec {
        name: "parts",
        required: true,
        summary: "A list of literal strings and `{ field: <name> }` references.",
    }],
    accepts: &[SqlTypeFamily::Text],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for TemplateFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &TEMPLATE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(parts) = config.args.get("parts").and_then(|v| v.as_sequence()) else {
            bag.error(
                crate::diagnostic::codes::TEMPLATE_MISSING_PARTS.code,
                context.path(),
                "`template` requires a `parts` list",
            );
            return Err(bag);
        };

        let mut fragments = Vec::with_capacity(parts.len());
        for part in parts {
            match part {
                serde_yaml_ng::Value::Mapping(mapping) => {
                    let field = mapping.get("field").and_then(|v| v.as_str());
                    match field {
                        Some(name) => {
                            if find_column(context.table(), name).is_none() {
                                bag.error(
                                    crate::diagnostic::codes::TEMPLATE_UNKNOWN_FIELD.code,
                                    context.path(),
                                    format!(
                                        "`template` references unknown column `{name}` on table `{}`",
                                        context.table().name
                                    ),
                                );
                            }
                            fragments.push(TemplateFragment::Field(name.to_string()));
                        }
                        None => {
                            bag.error(
                                crate::diagnostic::codes::TEMPLATE_INVALID_PART.code,
                                context.path(),
                                "each `template.parts` mapping entry needs a `field` key",
                            );
                        }
                    }
                }
                other => fragments.push(TemplateFragment::Literal(display_yaml(other))),
            }
        }

        bag.into_result(Box::new(CompiledCore(CoreGenerator::Template(fragments)))
            as Box<dyn CompiledGenerator>)
    }
}

/// The `slug` generator: derives a URL slug from another column's value on the
/// same row (lowercase, non-alphanumeric runs collapse to `-`, ends trimmed).
/// Like `copy`/`template` it reads a sibling column, so its `source` is a
/// dependency the compiler orders and cycle-checks.
pub struct SlugFactory;

static SLUG_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "slug",
    aliases: &[],
    summary: "Derives a URL slug from another column's value on the same row.",
    arguments: &[
        ArgumentSpec {
            name: "source",
            required: true,
            summary: "The sibling column whose value is slugified.",
        },
        ArgumentSpec {
            name: "max_length",
            required: false,
            summary: "Optional maximum slug length; a dash left at the cut is trimmed.",
        },
    ],
    accepts: &[SqlTypeFamily::Text],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for SlugFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &SLUG_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(source) = config
            .args
            .get("source")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            bag.error(
                crate::diagnostic::codes::SLUG_MISSING_SOURCE.code,
                context.path(),
                "`slug` requires a `source` column name",
            );
            return Err(bag);
        };

        if find_column(context.table(), &source).is_none() {
            bag.error(
                crate::diagnostic::codes::SLUG_UNKNOWN_FIELD.code,
                context.path(),
                format!(
                    "`slug.source` references unknown column `{source}` on table `{}`",
                    context.table().name
                ),
            );
        }

        let max_length = match config.args.get("max_length") {
            None => None,
            Some(value) => match value.as_u64().filter(|&n| n > 0) {
                Some(n) => Some(n as usize),
                None => {
                    bag.error(
                        crate::diagnostic::codes::SLUG_MAX_LENGTH.code,
                        context.path(),
                        "`slug.max_length` must be a positive integer",
                    );
                    None
                }
            },
        };

        bag.into_result(
            Box::new(CompiledCore(CoreGenerator::Slug { source, max_length }))
                as Box<dyn CompiledGenerator>,
        )
    }
}

/// The `pattern` generator: fills a character mask, where `#` draws a random
/// digit, `?` a random uppercase letter, `@` a random lowercase letter, `*` a
/// random alphanumeric character, and every other character passes through
/// literally (e.g. `"###-??"` -> `"482-QK"`).
pub struct PatternFactory;

static PATTERN_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "pattern",
    aliases: &[],
    summary: "Fills a character mask (`#` digit, `?` upper, `@` lower, `*` alphanumeric).",
    arguments: &[ArgumentSpec {
        name: "mask",
        required: true,
        summary: "The mask to fill, e.g. `\"###-??\"`.",
    }],
    accepts: &[SqlTypeFamily::Text],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for PatternFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &PATTERN_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(mask) = config
            .args
            .get("mask")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            bag.error(
                crate::diagnostic::codes::PATTERN_MISSING_MASK.code,
                context.path(),
                "`pattern` requires a `mask` string",
            );
            return Err(bag);
        };
        let rng = stream(context, "pattern");
        bag.into_result(Box::new(CompiledCore(CoreGenerator::Pattern { mask, rng }))
            as Box<dyn CompiledGenerator>)
    }
}

/// The `database_default` generator: always defers to the column's `DEFAULT`
/// expression, leaving the literal to the writer.
pub struct DatabaseDefaultFactory;

static DATABASE_DEFAULT_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "database_default",
    aliases: &[],
    summary: "Defers to the column's DEFAULT expression.",
    arguments: &[],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for DatabaseDefaultFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &DATABASE_DEFAULT_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        Ok(Box::new(CompiledCore(CoreGenerator::DatabaseDefault)))
    }
}

/// The `json_value` generator: emits a fixed JSON document, either the
/// configured `value` (converted from YAML to JSON) or `{}` when omitted.
pub struct JsonValueFactory;

static JSON_VALUE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "json_value",
    aliases: &[],
    summary: "Emits a fixed JSON document.",
    arguments: &[ArgumentSpec {
        name: "value",
        required: false,
        summary: "The document to emit; defaults to `{}`.",
    }],
    accepts: &[SqlTypeFamily::Json],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for JsonValueFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &JSON_VALUE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let rendered = match config.args.get("value") {
            Some(value) => match json_from_yaml(value) {
                Ok(json) => json,
                Err(message) => {
                    bag.error(
                        crate::diagnostic::codes::JSON_VALUE_INVALID.code,
                        context.path(),
                        message,
                    );
                    "{}".to_string()
                }
            },
            None => "{}".to_string(),
        };
        bag.into_result(
            Box::new(CompiledCore(CoreGenerator::Json(rendered))) as Box<dyn CompiledGenerator>
        )
    }
}

/// The `integer` generator: a uniformly random integer in `[min, max]`.
pub struct IntegerFactory;

static INTEGER_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "integer",
    aliases: &[],
    summary: "A uniformly random integer in [min, max].",
    arguments: &[
        ArgumentSpec {
            name: "min",
            required: false,
            summary: "Inclusive lower bound; defaults to 0.",
        },
        ArgumentSpec {
            name: "max",
            required: false,
            summary: "Inclusive upper bound; defaults to 1000.",
        },
    ],
    accepts: &[SqlTypeFamily::Integer, SqlTypeFamily::BigInteger],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for IntegerFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &INTEGER_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let min = config.args.get("min").and_then(parse_i128).unwrap_or(0);
        let max = config.args.get("max").and_then(parse_i128).unwrap_or(1000);
        let mut bag = DiagnosticBag::default();
        if min > max {
            bag.error(
                crate::diagnostic::codes::INTEGER_RANGE.code,
                context.path(),
                format!("`integer.min` ({min}) must not exceed `integer.max` ({max})"),
            );
        }
        let rng = stream(context, "integer");
        bag.into_result(
            Box::new(CompiledCore(CoreGenerator::Integer(UniformInteger {
                min,
                max,
                rng,
            }))) as Box<dyn CompiledGenerator>,
        )
    }
}

/// The `decimal` generator: a uniformly random fixed-point number in `[min,
/// max]` at the configured `scale`.
pub struct DecimalFactory;

static DECIMAL_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "decimal",
    aliases: &[],
    summary: "A uniformly random decimal in [min, max] at a fixed scale.",
    arguments: &[
        ArgumentSpec {
            name: "min",
            required: false,
            summary: "Inclusive lower bound; defaults to 0.",
        },
        ArgumentSpec {
            name: "max",
            required: false,
            summary: "Inclusive upper bound; defaults to 1000.",
        },
        ArgumentSpec {
            name: "scale",
            required: false,
            summary: "Decimal places; defaults to 2.",
        },
    ],
    accepts: &[SqlTypeFamily::Decimal],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for DecimalFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &DECIMAL_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let scale = config.args.get("scale").and_then(parse_usize).unwrap_or(2);
        let scale = match u32::try_from(scale) {
            Ok(scale) if scale <= 18 => scale,
            _ => {
                bag.error(
                    crate::diagnostic::codes::DECIMAL_SCALE.code,
                    context.path(),
                    "`decimal.scale` must be between 0 and 18",
                );
                2
            }
        };
        let factor = 10i128.pow(scale);
        let min_minor = config
            .args
            .get("min")
            .and_then(parse_decimal)
            .map_or(0, |(minor, from_scale)| rescale(minor, from_scale, scale));
        let max_minor = config
            .args
            .get("max")
            .and_then(parse_decimal)
            .map_or(1000 * factor, |(minor, from_scale)| {
                rescale(minor, from_scale, scale)
            });
        if min_minor > max_minor {
            bag.error(
                crate::diagnostic::codes::DECIMAL_RANGE.code,
                context.path(),
                format!(
                    "`decimal.min` ({}) must not exceed `decimal.max` ({})",
                    format_decimal(min_minor, scale),
                    format_decimal(max_minor, scale)
                ),
            );
        }
        let rng = stream(context, "decimal");
        bag.into_result(
            Box::new(CompiledCore(CoreGenerator::Decimal(UniformDecimal {
                min_minor,
                max_minor,
                scale,
                rng,
            }))) as Box<dyn CompiledGenerator>,
        )
    }
}

/// Rescale `minor` units from `from_scale` decimal places to `to_scale`.
fn rescale(minor: i128, from_scale: u32, to_scale: u32) -> i128 {
    if from_scale == to_scale {
        return minor;
    }
    if from_scale < to_scale {
        minor * 10i128.pow(to_scale - from_scale)
    } else {
        minor / 10i128.pow(from_scale - to_scale)
    }
}

/// The `boolean` generator: `true` with the configured `probability`
/// (default `0.5`).
pub struct BooleanFactory;

static BOOLEAN_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "boolean",
    aliases: &[],
    summary: "Emits `true` with the configured probability.",
    arguments: &[ArgumentSpec {
        name: "probability",
        required: false,
        summary: "Probability of `true`, in [0, 1]; defaults to 0.5.",
    }],
    accepts: &[
        SqlTypeFamily::Boolean,
        SqlTypeFamily::Integer,
        SqlTypeFamily::BigInteger,
    ],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for BooleanFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &BOOLEAN_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let probability = config
            .args
            .get("probability")
            .and_then(parse_f64)
            .unwrap_or(0.5);
        let mut bag = DiagnosticBag::default();
        if !(0.0..=1.0).contains(&probability) {
            bag.error(
                crate::diagnostic::codes::BOOLEAN_PROBABILITY.code,
                context.path(),
                "`boolean.probability` must be between 0 and 1",
            );
        }
        let rng = stream(context, "boolean");
        // A boolean-by-convention integer column (e.g. MySQL `TINYINT(1)`, which
        // classifies as the Integer family) must receive `0`/`1` integer values,
        // not a native boolean, so the rendered SQL is valid for the column type.
        let as_integer = context.column().is_some_and(|column| {
            matches!(
                column.family,
                SqlTypeFamily::Integer | SqlTypeFamily::BigInteger
            )
        });
        bag.into_result(Box::new(CompiledCore(CoreGenerator::Boolean {
            probability,
            rng,
            as_integer,
        })) as Box<dyn CompiledGenerator>)
    }
}

/// The `string` generator: a random alphanumeric string with length in
/// `[min_length, max_length]`.
pub struct StringFactory;

static STRING_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "string",
    aliases: &[],
    summary: "A random alphanumeric string with length in [min_length, max_length].",
    arguments: &[
        ArgumentSpec {
            name: "min_length",
            required: false,
            summary: "Minimum length; defaults to 8.",
        },
        ArgumentSpec {
            name: "max_length",
            required: false,
            summary: "Maximum length; defaults to `min_length`.",
        },
    ],
    accepts: &[SqlTypeFamily::Text],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for StringFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &STRING_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let min_len = config
            .args
            .get("min_length")
            .and_then(parse_usize)
            .unwrap_or(8);
        let max_len = config
            .args
            .get("max_length")
            .and_then(parse_usize)
            .unwrap_or(min_len);
        let mut bag = DiagnosticBag::default();
        if min_len > max_len {
            bag.error(
                crate::diagnostic::codes::STRING_LENGTH_RANGE.code,
                context.path(),
                "`string.min_length` must not exceed `string.max_length`",
            );
        }
        let rng = stream(context, "string");
        bag.into_result(Box::new(CompiledCore(CoreGenerator::String(UniformString {
            min_len,
            max_len,
            rng,
        }))) as Box<dyn CompiledGenerator>)
    }
}

/// The `bytes` generator: a random byte string with length in `[min_length,
/// max_length]`.
pub struct BytesFactory;

static BYTES_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "bytes",
    aliases: &[],
    summary: "A random byte string with length in [min_length, max_length].",
    arguments: &[
        ArgumentSpec {
            name: "min_length",
            required: false,
            summary: "Minimum length; defaults to 16.",
        },
        ArgumentSpec {
            name: "max_length",
            required: false,
            summary: "Maximum length; defaults to `min_length`.",
        },
    ],
    accepts: &[SqlTypeFamily::Bytes],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for BytesFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &BYTES_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let min_len = config
            .args
            .get("min_length")
            .and_then(parse_usize)
            .unwrap_or(16);
        let max_len = config
            .args
            .get("max_length")
            .and_then(parse_usize)
            .unwrap_or(min_len);
        let mut bag = DiagnosticBag::default();
        if min_len > max_len {
            bag.error(
                crate::diagnostic::codes::BYTES_LENGTH_RANGE.code,
                context.path(),
                "`bytes.min_length` must not exceed `bytes.max_length`",
            );
        }
        let rng = stream(context, "bytes");
        bag.into_result(Box::new(CompiledCore(CoreGenerator::Bytes(UniformBytes {
            min_len,
            max_len,
            rng,
        }))) as Box<dyn CompiledGenerator>)
    }
}

/// The `uuid` generator: a random RFC 4122 version-4 UUID.
pub struct UuidFactory;

static UUID_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "uuid",
    aliases: &[],
    summary: "A random RFC 4122 version-4 UUID.",
    arguments: &[],
    accepts: &[SqlTypeFamily::Uuid, SqlTypeFamily::Text],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for UuidFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &UUID_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, "uuid");
        Ok(Box::new(CompiledCore(CoreGenerator::Uuid(rng))))
    }
}

/// The `choice` generator: a uniformly random pick from a fixed list of
/// values.
pub struct ChoiceFactory;

static CHOICE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "choice",
    aliases: &[],
    summary: "A uniformly random pick from a fixed list of values.",
    arguments: &[ArgumentSpec {
        name: "values",
        required: true,
        summary: "The non-empty list of values to pick from.",
    }],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for ChoiceFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &CHOICE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(raw_values) = config.args.get("values").and_then(|v| v.as_sequence()) else {
            bag.error(
                crate::diagnostic::codes::CHOICE_MISSING_VALUES.code,
                context.path(),
                "`choice` requires a `values` list",
            );
            return Err(bag);
        };
        if raw_values.is_empty() {
            bag.error(
                crate::diagnostic::codes::CHOICE_EMPTY.code,
                context.path(),
                "`choice.values` must not be empty",
            );
            return Err(bag);
        }

        let family = &column(context).family;
        let mut values = Vec::with_capacity(raw_values.len());
        for raw in raw_values {
            match coerce_value(raw, family) {
                Ok(value) => values.push(value),
                Err(message) => {
                    bag.error(
                        crate::diagnostic::codes::CHOICE_INVALID_VALUE.code,
                        context.path(),
                        message,
                    );
                }
            }
        }
        bag.into_result(())?;

        let rng = stream(context, "choice");
        Ok(Box::new(CompiledCore(CoreGenerator::Choice(ChoiceState {
            values,
            rng,
        }))))
    }
}

/// The `weighted_choice` generator: a random pick from a list of `{ value,
/// weight }` entries, biased by weight.
pub struct WeightedChoiceFactory;

static WEIGHTED_CHOICE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "weighted_choice",
    aliases: &[],
    summary: "A weighted random pick from a list of `{ value, weight }` entries.",
    arguments: &[ArgumentSpec {
        name: "choices",
        required: true,
        summary: "The non-empty list of `{ value, weight }` entries.",
    }],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for WeightedChoiceFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &WEIGHTED_CHOICE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(raw_choices) = config.args.get("choices").and_then(|v| v.as_sequence()) else {
            bag.error(
                crate::diagnostic::codes::WEIGHTED_CHOICE_MISSING_CHOICES.code,
                context.path(),
                "`weighted_choice` requires a `choices` list",
            );
            return Err(bag);
        };
        if raw_choices.is_empty() {
            bag.error(
                crate::diagnostic::codes::WEIGHTED_CHOICE_EMPTY.code,
                context.path(),
                "`weighted_choice.choices` must not be empty",
            );
            return Err(bag);
        }

        let family = &column(context).family;
        let mut values = Vec::with_capacity(raw_choices.len());
        let mut cumulative = Vec::with_capacity(raw_choices.len());
        let mut total = 0.0f64;
        for choice in raw_choices {
            let value_field = choice.get("value");
            let weight_field = choice.get("weight").and_then(parse_f64);
            let (Some(raw_value), Some(weight)) = (value_field, weight_field) else {
                bag.error(
                    crate::diagnostic::codes::WEIGHTED_CHOICE_INVALID_ENTRY.code,
                    context.path(),
                    "each `weighted_choice.choices` entry needs a `value` and a numeric `weight`",
                );
                continue;
            };
            if !weight.is_finite() || weight < 0.0 {
                bag.error(
                    crate::diagnostic::codes::WEIGHTED_CHOICE_INVALID_WEIGHT.code,
                    context.path(),
                    format!("`weighted_choice` weight {weight} must be finite and non-negative"),
                );
                continue;
            }
            match coerce_value(raw_value, family) {
                Ok(value) => {
                    total += weight;
                    values.push(value);
                    cumulative.push(total);
                }
                Err(message) => {
                    bag.error(
                        crate::diagnostic::codes::WEIGHTED_CHOICE_INVALID_VALUE.code,
                        context.path(),
                        message,
                    );
                }
            }
        }
        if total <= 0.0 {
            bag.error(
                crate::diagnostic::codes::WEIGHTED_CHOICE_ALL_ZERO.code,
                context.path(),
                "`weighted_choice.choices` weights must not all be zero",
            );
        }
        bag.into_result(())?;

        let rng = stream(context, "weighted_choice");
        Ok(Box::new(CompiledCore(CoreGenerator::WeightedChoice(
            WeightedChoiceState {
                values,
                cumulative,
                rng,
            },
        ))))
    }
}

// --- Modifiers ----------------------------------------------------------------

/// The `null_rate` modifier: replaces the value with `NULL` with the
/// configured probability. Legal only on nullable columns.
pub struct NullRateFactory;

static NULL_RATE_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "null_rate",
    aliases: &[],
    summary: "Replaces the value with NULL with the configured probability.",
    arguments: &[ArgumentSpec {
        name: "rate",
        required: true,
        summary: "Probability of NULL, in [0, 1].",
    }],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct NullRateState {
    rate: f64,
    rng: ChaCha8Rng,
}

impl CompiledModifier for NullRateState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        if self.rng.random_bool(self.rate) {
            *value = GeneratedValue::Null;
        }
        Ok(())
    }
}

impl ModifierFactory for NullRateFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &NULL_RATE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(rate) = config.args.get("rate").and_then(parse_f64) else {
            bag.error(
                crate::diagnostic::codes::NULL_RATE_MISSING_RATE.code,
                context.path(),
                "`null_rate` requires a numeric `rate`",
            );
            return Err(bag);
        };
        if !(0.0..=1.0).contains(&rate) {
            bag.error(
                crate::diagnostic::codes::NULL_RATE_RANGE.code,
                context.path(),
                "`null_rate.rate` must be between 0 and 1",
            );
        }
        if !column(context).nullable {
            bag.error(
                crate::diagnostic::codes::NULL_RATE_ON_NON_NULLABLE.code,
                context.path(),
                format!(
                    "`null_rate` cannot apply to column `{}`, which is not nullable",
                    column(context).name
                ),
            );
        }
        let rng = stream(context, "null_rate");
        bag.into_result(Box::new(NullRateState { rate, rng }) as Box<dyn CompiledModifier>)
    }
}

/// How the `unique` modifier resolves a candidate it cannot make unique
/// within its attempt budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnExhaustion {
    /// Fail the row.
    Error,
    /// Keep the duplicate value; uniqueness is violated for this row only.
    Warn,
    /// Try substantially harder before falling back to `Warn`'s behavior.
    Widen,
}

/// Families the `unique` modifier can mutate a colliding candidate into an
/// alternative (append/increment style). Families outside this set can only
/// ever be de-duplicated by chance, since there is no defined mutation for
/// them — `on_exhaustion: widen` is therefore only legal here.
fn family_supports_widening(family: &SqlTypeFamily) -> bool {
    matches!(
        family,
        SqlTypeFamily::Integer
            | SqlTypeFamily::BigInteger
            | SqlTypeFamily::Decimal
            | SqlTypeFamily::Text
            | SqlTypeFamily::Bytes
            | SqlTypeFamily::Uuid
    )
}

/// Perturb `value` into a new candidate for the `attempt`-th retry, or `None`
/// if this value's family has no defined mutation.
fn mutate_candidate(value: &GeneratedValue, attempt: i128) -> Option<GeneratedValue> {
    match value {
        GeneratedValue::Integer(i) => i.checked_add(attempt).map(GeneratedValue::Integer),
        GeneratedValue::Decimal { minor, scale } => {
            minor
                .checked_add(attempt)
                .map(|minor| GeneratedValue::Decimal {
                    minor,
                    scale: *scale,
                })
        }
        GeneratedValue::Text(s) => Some(GeneratedValue::Text(format!("{s}-{attempt}"))),
        GeneratedValue::Bytes(bytes) => {
            let mut mutated = bytes.clone();
            mutated.push((attempt % 256) as u8);
            Some(GeneratedValue::Bytes(mutated))
        }
        _ => None,
    }
}

/// Default cap on how many distinct values the `unique` modifier will
/// remember, when `max_tracked` is not configured. Global uniqueness
/// genuinely requires remembering every value claimed so far — there is no
/// way to check "have I seen this before" without a history — so instead of
/// letting that history grow without bound (which would violate the
/// generator's memory budget on a large table), the budget is made explicit
/// and finite. A column that needs more distinct values than this should use
/// an inherently-unique generator (`sequence`, `uuid`, `identifier.*`)
/// instead of leaning on `unique` to deduplicate a huge value space.
const DEFAULT_UNIQUE_MAX_TRACKED: usize = 1_000_000;

/// The `unique` modifier: ensures every emitted value is distinct from every
/// value already seen, up to an explicit tracking budget (`max_tracked`) —
/// the full history of claimed values is kept, up to that cap, not an
/// unbounded set. Within that budget, a collision retries up to
/// `max_attempts` times by mutating the candidate before resolving via
/// `on_exhaustion`; a candidate that would grow the tracked history past
/// `max_tracked` resolves via `on_exhaustion` too, since there is no room
/// left to remember it.
pub struct UniqueFactory;

static UNIQUE_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "unique",
    aliases: &[],
    summary: "Ensures every emitted value is distinct from every value already seen, \
              within an explicit tracking budget (max_tracked).",
    arguments: &[
        ArgumentSpec {
            name: "max_attempts",
            required: false,
            summary: "Retries allowed before resolving via on_exhaustion; defaults to 10.",
        },
        ArgumentSpec {
            name: "on_exhaustion",
            required: false,
            summary: "`error` | `warn` | `widen`; defaults to `error`.",
        },
        ArgumentSpec {
            name: "max_tracked",
            required: false,
            summary: "Maximum number of distinct values remembered; defaults to 1,000,000. \
                      Reaching this budget resolves via on_exhaustion, the same as a collision.",
        },
    ],
    accepts: ALL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Supported,
};

/// The outcome of trying to claim a candidate value for uniqueness tracking.
enum Claim {
    /// Not seen before, and there was room left in the tracking budget.
    Tracked,
    /// Already claimed by an earlier row.
    Collision,
    /// Not seen before, but `max_tracked` distinct values are already
    /// tracked — there is no room to remember a new one.
    BudgetExceeded,
}

struct UniqueState {
    seen: HashSet<String>,
    max_attempts: usize,
    max_tracked: usize,
    on_exhaustion: OnExhaustion,
}

impl UniqueState {
    /// Try to claim `key` against the tracking budget.
    fn claim(&mut self, key: String) -> Claim {
        if self.seen.contains(&key) {
            return Claim::Collision;
        }
        if self.seen.len() >= self.max_tracked {
            return Claim::BudgetExceeded;
        }
        self.seen.insert(key);
        Claim::Tracked
    }

    /// Resolve a candidate that could not be claimed (a collision that
    /// survived every mutation attempt, or a budget exhaustion), per
    /// `on_exhaustion`.
    fn resolve(
        &self,
        value: &mut GeneratedValue,
        fallback: GeneratedValue,
        reason: &str,
    ) -> Result<(), GenerateError> {
        match self.on_exhaustion {
            OnExhaustion::Error => Err(GenerateError::Exhausted(format!(
                "`unique` {reason}; consider an inherently-unique generator \
                 (`sequence`, `uuid`, `identifier.*`) for large unique columns"
            ))),
            OnExhaustion::Warn | OnExhaustion::Widen => {
                *value = fallback;
                Ok(())
            }
        }
    }
}

impl CompiledModifier for UniqueState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        match self.claim(value_key(value)) {
            Claim::Tracked => return Ok(()),
            Claim::BudgetExceeded => {
                let fallback = value.clone();
                return self.resolve(
                    value,
                    fallback,
                    &format!(
                        "could not track a new value: the max_tracked budget of {} distinct \
                         values has been reached",
                        self.max_tracked
                    ),
                );
            }
            Claim::Collision => {}
        }

        let attempts = match self.on_exhaustion {
            OnExhaustion::Widen => self.max_attempts.saturating_mul(10),
            OnExhaustion::Error | OnExhaustion::Warn => self.max_attempts,
        };

        let mut last_candidate = value.clone();
        for attempt in 1..=attempts {
            let Some(candidate) = mutate_candidate(value, attempt as i128) else {
                break;
            };
            match self.claim(value_key(&candidate)) {
                Claim::Tracked => {
                    *value = candidate;
                    return Ok(());
                }
                Claim::BudgetExceeded => {
                    return self.resolve(
                        value,
                        candidate,
                        &format!(
                            "could not track a new value: the max_tracked budget of {} \
                             distinct values has been reached",
                            self.max_tracked
                        ),
                    );
                }
                Claim::Collision => {
                    last_candidate = candidate;
                }
            }
        }

        self.resolve(
            value,
            last_candidate,
            &format!("could not produce a distinct value within {attempts} attempts"),
        )
    }
}

impl ModifierFactory for UniqueFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &UNIQUE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let max_attempts = config
            .args
            .get("max_attempts")
            .and_then(parse_usize)
            .unwrap_or(10);
        let max_tracked = config
            .args
            .get("max_tracked")
            .and_then(parse_usize)
            .unwrap_or(DEFAULT_UNIQUE_MAX_TRACKED);
        let on_exhaustion = match config.args.get("on_exhaustion").and_then(|v| v.as_str()) {
            None | Some("error") => OnExhaustion::Error,
            Some("warn") => OnExhaustion::Warn,
            Some("widen") => OnExhaustion::Widen,
            Some(other) => {
                bag.error(
                    crate::diagnostic::codes::UNIQUE_INVALID_ON_EXHAUSTION.code,
                    context.path(),
                    format!(
                        "`unique.on_exhaustion` must be error, warn, or widen, found `{other}`"
                    ),
                );
                OnExhaustion::Error
            }
        };
        if on_exhaustion == OnExhaustion::Widen
            && !family_supports_widening(&column(context).family)
        {
            bag.error(
                crate::diagnostic::codes::UNIQUE_WIDEN_UNSUPPORTED.code,
                context.path(),
                format!(
                    "`unique.on_exhaustion: widen` is not supported for column `{}`'s type family {:?}",
                    column(context).name,
                    column(context).family
                ),
            );
        }
        bag.into_result(Box::new(UniqueState {
            seen: HashSet::new(),
            max_attempts,
            max_tracked,
            on_exhaustion,
        }) as Box<dyn CompiledModifier>)
    }
}

/// Shared compile logic for the four string-transform modifiers below: all
/// of them only accept `Text` columns.
static TEXT_ONLY: &[SqlTypeFamily] = &[SqlTypeFamily::Text];

/// The `prefix` modifier: prepends configured text, truncating to
/// `max_length` (Unicode scalars) afterward if configured.
pub struct PrefixFactory;

static PREFIX_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "prefix",
    aliases: &[],
    summary: "Prepends configured text to the value.",
    arguments: &[
        ArgumentSpec {
            name: "value",
            required: true,
            summary: "The text to prepend.",
        },
        ArgumentSpec {
            name: "max_length",
            required: false,
            summary: "Truncate to this many Unicode scalars afterward, if set.",
        },
    ],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct AffixState {
    text: String,
    max_length: Option<usize>,
    prefix: bool,
}

impl CompiledModifier for AffixState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let existing = value.as_text()?;
        let mut combined = if self.prefix {
            format!("{}{existing}", self.text)
        } else {
            format!("{existing}{}", self.text)
        };
        if let Some(max_length) = self.max_length {
            combined = combined.chars().take(max_length).collect();
        }
        *value = GeneratedValue::Text(combined);
        Ok(())
    }
}

fn compile_affix(
    config: &ModifierConfig,
    context: &CompileContext<'_>,
    prefix: bool,
) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
    let mut bag = DiagnosticBag::default();
    let Some(text) = config
        .args
        .get("value")
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        bag.error(
            crate::diagnostic::codes::AFFIX_MISSING_VALUE.code,
            context.path(),
            "`prefix`/`suffix` requires a `value` string",
        );
        return Err(bag);
    };
    let max_length = config.args.get("max_length").and_then(parse_usize);
    bag.into_result(Box::new(AffixState {
        text,
        max_length,
        prefix,
    }) as Box<dyn CompiledModifier>)
}

impl ModifierFactory for PrefixFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &PREFIX_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        compile_affix(config, context, true)
    }
}

/// The `suffix` modifier: appends configured text, truncating to
/// `max_length` (Unicode scalars) afterward if configured.
pub struct SuffixFactory;

static SUFFIX_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "suffix",
    aliases: &[],
    summary: "Appends configured text to the value.",
    arguments: &[
        ArgumentSpec {
            name: "value",
            required: true,
            summary: "The text to append.",
        },
        ArgumentSpec {
            name: "max_length",
            required: false,
            summary: "Truncate to this many Unicode scalars afterward, if set.",
        },
    ],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl ModifierFactory for SuffixFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &SUFFIX_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        compile_affix(config, context, false)
    }
}

/// The `truncate` modifier: keeps only the first `max_length` Unicode
/// scalars.
pub struct TruncateFactory;

static TRUNCATE_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "truncate",
    aliases: &[],
    summary: "Keeps only the first max_length Unicode scalars.",
    arguments: &[ArgumentSpec {
        name: "max_length",
        required: true,
        summary: "The number of Unicode scalars to keep.",
    }],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct TruncateState {
    max_length: usize,
}

impl CompiledModifier for TruncateState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let existing = value.as_text()?;
        *value = GeneratedValue::Text(existing.chars().take(self.max_length).collect());
        Ok(())
    }
}

impl ModifierFactory for TruncateFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &TRUNCATE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(max_length) = config.args.get("max_length").and_then(parse_usize) else {
            bag.error(
                crate::diagnostic::codes::TRUNCATE_MISSING_MAX_LENGTH.code,
                context.path(),
                "`truncate` requires a `max_length`",
            );
            return Err(bag);
        };
        bag.into_result(Box::new(TruncateState { max_length }) as Box<dyn CompiledModifier>)
    }
}

/// The letter case a `case` modifier converts text to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseMode {
    Upper,
    Lower,
    Title,
}

/// The `case` modifier: converts text to upper, lower, or title case.
pub struct CaseFactory;

static CASE_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "case",
    aliases: &[],
    summary: "Converts text to upper, lower, or title case.",
    arguments: &[ArgumentSpec {
        name: "mode",
        required: true,
        summary: "`upper` | `lower` | `title`.",
    }],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct CaseState {
    mode: CaseMode,
}

impl CompiledModifier for CaseState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let existing = value.as_text()?;
        let transformed = match self.mode {
            CaseMode::Upper => existing.to_uppercase(),
            CaseMode::Lower => existing.to_lowercase(),
            CaseMode::Title => title_case(existing),
        };
        *value = GeneratedValue::Text(transformed);
        Ok(())
    }
}

fn title_case(text: &str) -> String {
    text.split_inclusive(char::is_whitespace)
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

impl ModifierFactory for CaseFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &CASE_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let mode = match config.args.get("mode").and_then(|v| v.as_str()) {
            Some("upper") => CaseMode::Upper,
            Some("lower") => CaseMode::Lower,
            Some("title") => CaseMode::Title,
            other => {
                bag.error(
                    crate::diagnostic::codes::CASE_INVALID_MODE.code,
                    context.path(),
                    format!(
                        "`case.mode` must be upper, lower, or title, found `{}`",
                        other.unwrap_or("<missing>")
                    ),
                );
                CaseMode::Lower
            }
        };
        bag.into_result(Box::new(CaseState { mode }) as Box<dyn CompiledModifier>)
    }
}

/// The `clamp` modifier: restricts an `Integer` or `Decimal` value to `[min,
/// max]`.
pub struct ClampFactory;

static CLAMP_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "clamp",
    aliases: &[],
    summary: "Restricts a numeric value to [min, max].",
    arguments: &[
        ArgumentSpec {
            name: "min",
            required: true,
            summary: "Inclusive lower bound.",
        },
        ArgumentSpec {
            name: "max",
            required: true,
            summary: "Inclusive upper bound.",
        },
    ],
    accepts: &[
        SqlTypeFamily::Integer,
        SqlTypeFamily::BigInteger,
        SqlTypeFamily::Decimal,
    ],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct ClampState {
    min: f64,
    max: f64,
}

impl CompiledModifier for ClampState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        match value {
            GeneratedValue::Integer(i) => {
                *i = (*i).clamp(self.min as i128, self.max as i128);
            }
            GeneratedValue::Decimal { minor, scale } => {
                let factor = 10f64.powi(*scale as i32);
                let lo = (self.min * factor).round() as i128;
                let hi = (self.max * factor).round() as i128;
                *minor = (*minor).clamp(lo, hi);
            }
            other => {
                return Err(GenerateError::TypeMismatch {
                    expected: "Integer or Decimal",
                    found: other.type_name(),
                });
            }
        }
        Ok(())
    }
}

impl ModifierFactory for ClampFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &CLAMP_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let min = config.args.get("min").and_then(parse_f64);
        let max = config.args.get("max").and_then(parse_f64);
        let (Some(min), Some(max)) = (min, max) else {
            bag.error(
                crate::diagnostic::codes::CLAMP_MISSING_BOUNDS.code,
                context.path(),
                "`clamp` requires numeric `min` and `max`",
            );
            return Err(bag);
        };
        if min > max {
            bag.error(
                crate::diagnostic::codes::CLAMP_RANGE.code,
                context.path(),
                format!("`clamp.min` ({min}) must not exceed `clamp.max` ({max})"),
            );
        }
        bag.into_result(Box::new(ClampState { min, max }) as Box<dyn CompiledModifier>)
    }
}

/// The `round` modifier: rounds a `Decimal` value to fewer decimal places
/// (half-up), leaving it unchanged if `scale` is not smaller than the
/// value's current scale.
pub struct RoundFactory;

static ROUND_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "round",
    aliases: &[],
    summary: "Rounds a decimal value to fewer decimal places.",
    arguments: &[ArgumentSpec {
        name: "scale",
        required: true,
        summary: "Target number of decimal places.",
    }],
    accepts: &[SqlTypeFamily::Decimal],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct RoundState {
    scale: u32,
}

impl CompiledModifier for RoundState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let (minor, current_scale) = value.as_decimal()?;
        if self.scale >= current_scale {
            return Ok(());
        }
        let diff = current_scale - self.scale;
        let factor = 10i128.pow(diff);
        let half = factor / 2;
        let rounded = if minor >= 0 {
            (minor + half) / factor
        } else {
            -((-minor + half) / factor)
        };
        *value = GeneratedValue::Decimal {
            minor: rounded,
            scale: self.scale,
        };
        Ok(())
    }
}

impl ModifierFactory for RoundFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &ROUND_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(scale) = config.args.get("scale").and_then(parse_usize) else {
            bag.error(
                crate::diagnostic::codes::ROUND_MISSING_SCALE.code,
                context.path(),
                "`round` requires a `scale`",
            );
            return Err(bag);
        };
        let Ok(scale) = u32::try_from(scale) else {
            bag.error(
                crate::diagnostic::codes::ROUND_SCALE_RANGE.code,
                context.path(),
                "`round.scale` is out of range",
            );
            return Err(bag);
        };
        bag.into_result(Box::new(RoundState { scale }) as Box<dyn CompiledModifier>)
    }
}

/// The `format` modifier: substitutes the current text value into a
/// `{value}` template.
pub struct FormatFactory;

static FORMAT_DESCRIPTOR: ModifierDescriptor = ModifierDescriptor {
    kind: "format",
    aliases: &[],
    summary: "Substitutes the current text value into a `{value}` template.",
    arguments: &[ArgumentSpec {
        name: "template",
        required: true,
        summary: "A string containing the literal `{value}` placeholder.",
    }],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct FormatState {
    template: String,
}

impl CompiledModifier for FormatState {
    fn apply(
        &mut self,
        _context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let existing = value.as_text()?;
        *value = GeneratedValue::Text(self.template.replace("{value}", existing));
        Ok(())
    }
}

impl ModifierFactory for FormatFactory {
    fn descriptor(&self) -> &'static ModifierDescriptor {
        &FORMAT_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(template) = config
            .args
            .get("template")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            bag.error(
                crate::diagnostic::codes::FORMAT_MISSING_TEMPLATE.code,
                context.path(),
                "`format` requires a `template` string",
            );
            return Err(bag);
        };
        bag.into_result(Box::new(FormatState { template }) as Box<dyn CompiledModifier>)
    }
}

/// Register every core generator and modifier factory into `registry`.
pub(crate) fn register_all(registry: &mut ExtensionRegistry) {
    registry
        .register_generator(Box::new(NullFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(SequenceFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(CopyFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(TemplateFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(SlugFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(PatternFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(DatabaseDefaultFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(JsonValueFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(IntegerFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(DecimalFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(BooleanFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(StringFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(BytesFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(UuidFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(ChoiceFactory))
        .expect("built-in generator kinds are collision-free");
    registry
        .register_generator(Box::new(WeightedChoiceFactory))
        .expect("built-in generator kinds are collision-free");

    registry
        .register_modifier(Box::new(NullRateFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(UniqueFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(PrefixFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(SuffixFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(TruncateFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(CaseFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(ClampFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(RoundFactory))
        .expect("built-in modifier kinds are collision-free");
    registry
        .register_modifier(Box::new(FormatFactory))
        .expect("built-in modifier kinds are collision-free");
}
