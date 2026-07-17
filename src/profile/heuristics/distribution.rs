//! Observed-distribution heuristics: replay the *shape* the profiler measured.
//!
//! In rising order of specificity these recognize a constant column, a 0/1
//! integer boolean-by-convention (so MySQL `TINYINT(1)` and a real `BOOLEAN`
//! converge), a low-cardinality categorical (weighted replay of the observed
//! categories), a numeric histogram / Gaussian, and — as the last resort before
//! the type fallback — a bounded weighted sample of the observed values.
//!
//! Rules that embed literal source values (constant, categorical,
//! observed_sample) report those literals so the resolver can raise the
//! source-derived warning. Statistical fits (boolean rate, histogram, normal)
//! embed only aggregate parameters, never raw values.

use super::{
    generator_with, weighted_entries, yaml, Candidate, ColumnContext, Confidence, Precedence,
};
use crate::profile::evidence::{ColumnEvidence, NumericEvidence};
use crate::synthetic::schema::SqlTypeFamily;

/// The most distinct values a column may have before it stops being treated as
/// a categorical enum.
const MAX_CATEGORICAL_DISTINCT: u64 = 20;
/// The most values an `observed_sample` fallback replays.
const MAX_SAMPLE_ENTRIES: usize = 64;

/// Propose observed-distribution candidates for a column.
pub(super) fn candidates(ctx: &ColumnContext<'_>) -> Vec<Candidate> {
    let Some(evidence) = ctx.evidence() else {
        return Vec::new();
    };
    let non_null = evidence.total_count.saturating_sub(evidence.null_count);
    if non_null == 0 {
        return Vec::new();
    }

    let distinct = evidence.distinct_estimate.round().max(0.0) as u64;
    let column = ctx.column();
    let mut out = Vec::new();

    // Constant: effectively one distinct non-null value.
    if distinct <= 1 {
        if let Some(literal) = representative_value(evidence) {
            out.push(
                Candidate::new(
                    Precedence::ObservedDistribution,
                    Confidence::High,
                    "observed_constant",
                    generator_with("constant", [("value", yaml(&literal))]),
                )
                .with_source_literals(vec![literal]),
            );
            return out;
        }
    }

    // Boolean-by-convention: a two-valued 0/1 integer column.
    if matches!(
        column.family,
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger
    ) && is_zero_one(evidence)
    {
        let probability = zero_one_true_rate(evidence);
        out.push(Candidate::new(
            Precedence::ObservedDistribution,
            Confidence::High,
            "observed_boolean_0_1",
            generator_with("boolean", [("probability", yaml(probability))]),
        ));
    }

    // Low-cardinality categorical: a weighted replay of the observed values.
    if (1..=MAX_CATEGORICAL_DISTINCT).contains(&distinct)
        && !evidence.top_k.is_empty()
        && distinct * 2 <= non_null.max(2)
    {
        let entries: Vec<(String, u64)> = evidence
            .top_k
            .iter()
            .map(|e| (e.value.clone(), e.count.max(1)))
            .collect();
        let literals: Vec<String> = entries.iter().map(|(v, _)| v.clone()).collect();
        out.push(
            Candidate::new(
                Precedence::ObservedDistribution,
                Confidence::High,
                "observed_categorical",
                generator_with("weighted_choice", [("choices", weighted_entries(&entries))]),
            )
            .with_source_literals(literals),
        );
    }

    // Numeric spread: a histogram (primary) and a Gaussian (alternate). Both
    // embed only aggregate parameters.
    if let Some(numeric) = evidence.numeric {
        if distinct > 2 {
            let scale = evidence.decimal_scale.unwrap_or(0);
            out.push(Candidate::new(
                Precedence::ObservedDistribution,
                Confidence::Medium,
                "observed_histogram",
                histogram_generator(&numeric, scale),
            ));
            out.push(Candidate::new(
                Precedence::ObservedDistribution,
                Confidence::Low,
                "observed_normal",
                normal_generator(&numeric, scale),
            ));
        }
    }

    // Bounded weighted sample: the last resort for high-cardinality text with a
    // retained sample and no stronger match. Embeds source literals.
    if out.is_empty()
        && matches!(
            column.family,
            SqlTypeFamily::Text | SqlTypeFamily::Other | SqlTypeFamily::Uuid
        )
    {
        if let Some(entries) = sample_entries(evidence) {
            let literals: Vec<String> = entries.iter().map(|(v, _)| v.clone()).collect();
            out.push(
                Candidate::new(
                    Precedence::ObservedDistribution,
                    Confidence::Medium,
                    "observed_sample",
                    generator_with("observed_sample", [("values", weighted_entries(&entries))]),
                )
                .with_source_literals(literals),
            );
        }
    }

    out
}

/// A single representative non-null literal (the top heavy hitter or first
/// sample), for the constant rule.
fn representative_value(evidence: &ColumnEvidence) -> Option<String> {
    evidence
        .top_k
        .first()
        .map(|e| e.value.clone())
        .or_else(|| evidence.sample_values.first().cloned())
}

/// Whether every observed heavy hitter is `0` or `1` and exactly two values
/// appeared — the 0/1 integer boolean convention.
fn is_zero_one(evidence: &ColumnEvidence) -> bool {
    let distinct = evidence.distinct_estimate.round() as i64;
    if distinct != 2 || evidence.top_k.len() < 2 {
        return false;
    }
    evidence
        .top_k
        .iter()
        .all(|e| e.value == "0" || e.value == "1")
}

/// The observed fraction of `1`s among a 0/1 column's heavy hitters.
fn zero_one_true_rate(evidence: &ColumnEvidence) -> f64 {
    let ones: u64 = evidence
        .top_k
        .iter()
        .filter(|e| e.value == "1")
        .map(|e| e.count)
        .sum();
    let total: u64 = evidence.top_k.iter().map(|e| e.count).sum();
    if total == 0 {
        return 0.5;
    }
    round4(ones as f64 / total as f64)
}

/// A four-bin histogram from the observed quantiles, weighted 50/40/9/1.
fn histogram_generator(
    numeric: &NumericEvidence,
    scale: u8,
) -> crate::synthetic::model::GeneratorConfig {
    let bounds = [
        (numeric.min, numeric.p50, 50u64),
        (numeric.p50, numeric.p90, 40),
        (numeric.p90, numeric.p99, 9),
        (numeric.p99, numeric.max, 1),
    ];
    let mut bins = Vec::new();
    let mut cursor = numeric.min;
    for (lo, hi, count) in bounds {
        let lo = lo.max(cursor);
        let hi = hi.max(lo);
        cursor = hi;
        let mut map = serde_yaml_ng::Mapping::new();
        map.insert(yaml("min"), yaml(round4(lo)));
        map.insert(yaml("max"), yaml(round4(hi)));
        map.insert(yaml("count"), yaml(count));
        bins.push(serde_yaml_ng::Value::Mapping(map));
    }
    generator_with(
        "histogram",
        [
            ("bins", serde_yaml_ng::Value::Sequence(bins)),
            ("scale", yaml(scale)),
        ],
    )
}

/// A Gaussian fit: mean from the observed mean, std estimated from the
/// inter-quantile spread, clamped to the observed range.
fn normal_generator(
    numeric: &NumericEvidence,
    scale: u8,
) -> crate::synthetic::model::GeneratorConfig {
    let spread = ((numeric.p90 - numeric.p50) / 1.2816).abs();
    let std = if spread > 0.0 {
        spread
    } else {
        ((numeric.max - numeric.min) / 6.0).abs()
    };
    generator_with(
        "normal",
        [
            ("mean", yaml(round4(numeric.mean))),
            ("std", yaml(round4(std.max(0.0)))),
            ("min", yaml(round4(numeric.min))),
            ("max", yaml(round4(numeric.max))),
            ("scale", yaml(scale)),
        ],
    )
}

/// A bounded `[{value, weight}]` set from the top-k (preferred) or the retained
/// sample, capped at [`MAX_SAMPLE_ENTRIES`].
fn sample_entries(evidence: &ColumnEvidence) -> Option<Vec<(String, u64)>> {
    if !evidence.top_k.is_empty() {
        let entries: Vec<(String, u64)> = evidence
            .top_k
            .iter()
            .take(MAX_SAMPLE_ENTRIES)
            .map(|e| (e.value.clone(), e.count.max(1)))
            .collect();
        return Some(entries);
    }
    if evidence.sample_values.is_empty() {
        return None;
    }
    let entries: Vec<(String, u64)> = evidence
        .sample_values
        .iter()
        .take(MAX_SAMPLE_ENTRIES)
        .map(|v| (v.clone(), 1))
        .collect();
    Some(entries)
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}
