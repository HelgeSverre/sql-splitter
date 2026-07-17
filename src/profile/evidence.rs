//! Neutral profiling evidence: observations and confidence only.
//!
//! These types record *what was observed* in a dump — counts, bounded
//! summaries, and sketched estimates — with no opinion about how to regenerate
//! it. Nothing here names a generator kind (no "email", no "faker"); mapping
//! evidence to generation strategies is a later, separate concern
//! (Task 20's heuristics consume these structs without depending on the
//! accumulator internals that produced them).
//!
//! Every summary is bounded: a column that scans a billion rows produces the
//! same size evidence as one that scans a thousand. Raw values are never
//! retained beyond a small, budget-capped sample.

use crate::profile::ProfileDepth;
use crate::synthetic::schema::PortableSchema;
use serde::{Deserialize, Serialize};

/// Top-level profile of a whole dump.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DumpProfile {
    /// How deep the profiler was asked to look.
    pub depth: ProfileDepth,
    /// The dump's DDL schema, captured in the same single pass that gathered
    /// the value evidence. A portable, dialect-neutral snapshot so consumers
    /// (inference) never have to re-read the dump to recover its schema.
    pub schema: PortableSchema,
    /// Per-table evidence, in discovery order.
    pub tables: Vec<TableEvidence>,
    /// Stable diagnostic codes raised while profiling (e.g.
    /// `GEN-PROFILE-SCHEMA-LATE` when data preceded its table's DDL by more than
    /// the retained sample). Empty on a clean, schema-before-data dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Evidence gathered for a single table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableEvidence {
    /// Table name as it appeared in the dump.
    pub table: String,
    /// Observed row count, when the profiler counted rows.
    pub row_count: Option<u64>,
    /// Per-column evidence, in column order.
    pub columns: Vec<ColumnEvidence>,
    /// Observed relationships to other tables.
    pub relationships: Vec<RelationshipEvidence>,
    /// Confidence in this table's evidence, in `[0, 1]`.
    pub confidence: f64,
}

/// Evidence gathered for a single column. Only the sub-summaries relevant to
/// the observed value shapes are populated; the rest stay `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnEvidence {
    /// Column name (filled in by the profiler; empty when produced directly
    /// from a bare [`crate::profile::ColumnSketches`]).
    pub name: String,
    /// Total values observed, including nulls.
    pub total_count: u64,
    /// How many observed values were SQL `NULL`.
    pub null_count: u64,
    /// `null_count / total_count`, or `0.0` when nothing was observed.
    pub null_rate: f64,
    /// HyperLogLog estimate of the number of distinct non-null values.
    pub distinct_estimate: f64,
    /// A small, budget-bounded sample of retained value renderings.
    pub sample_values: Vec<String>,
    /// How many retained samples were byte-truncated to fit the ceiling.
    pub truncated_sample_count: u64,
    /// Present when boolean values were observed.
    pub boolean: Option<BooleanEvidence>,
    /// Present when numeric (integer/decimal) values were observed.
    pub numeric: Option<NumericEvidence>,
    /// Maximum decimal scale observed, when decimals were observed.
    pub decimal_scale: Option<u8>,
    /// Present when text values were observed.
    pub string_shape: Option<StringShapeEvidence>,
    /// Heavy hitters (approximate top-k), most frequent first.
    pub top_k: Vec<TopKEntry>,
    /// Present when timestamp values were observed.
    pub timestamp_range: Option<TimestampRangeEvidence>,
    /// Fraction of observed JSON values that parsed as valid JSON.
    pub json_valid_rate: Option<f64>,
    /// Confidence in this column's evidence, in `[0, 1]`.
    pub confidence: f64,
}

/// True/false tallies for a boolean-valued column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BooleanEvidence {
    pub true_count: u64,
    pub false_count: u64,
}

/// Bounded numeric summary: extent, mean, and interpolated quantiles from a
/// streaming histogram.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NumericEvidence {
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

/// Which character classes appeared anywhere in the observed strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CharClasses {
    pub lower: bool,
    pub upper: bool,
    pub digit: bool,
    pub whitespace: bool,
    pub punctuation: bool,
    pub non_ascii: bool,
}

/// Shape of a text column: lengths, empty rate, alphabet, and the longest
/// common prefix/suffix shared by every observed value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StringShapeEvidence {
    pub count: u64,
    pub empty_count: u64,
    pub empty_rate: f64,
    pub min_len: usize,
    pub max_len: usize,
    pub mean_len: f64,
    pub classes: CharClasses,
    pub common_prefix: String,
    pub common_suffix: String,
    /// Whether the prefix/suffix was capped at the affix byte ceiling.
    pub truncated_affix: bool,
}

/// One approximate heavy hitter: a value, its (over)estimated count, and the
/// maximum possible overestimate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopKEntry {
    pub value: String,
    pub count: u64,
    pub error: u64,
}

/// Lexical min/max of observed timestamp strings (ISO-8601 sorts lexically).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampRangeEvidence {
    pub min: String,
    pub max: String,
}

/// Evidence about an observed relationship between two columns.
///
/// No data-driven relationship inference is wired yet (Task 19); this type
/// exists so the neutral evidence surface is complete and stable for
/// downstream consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipEvidence {
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
    /// Fraction of child values found in the parent, in `[0, 1]`.
    pub coverage: f64,
    /// Confidence in this relationship, in `[0, 1]`.
    pub confidence: f64,
}
