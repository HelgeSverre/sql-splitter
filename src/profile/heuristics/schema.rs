//! Schema-driven heuristics: rules a column's declared type and constraints
//! justify on their own, before any value evidence is considered.
//!
//! Two precedence classes live here. A declared identity/sequence or a bound
//! `DEFAULT` is a *schema constraint* — it outranks name and distribution
//! guesses. Everything else is a *type fallback*: a plain, low-confidence
//! generator for the column's SQL family, the floor every column is guaranteed.

use super::{generator, generator_with, yaml, Candidate, ColumnContext, Confidence, Precedence};
use crate::synthetic::schema::SqlTypeFamily;

/// Propose the schema-justified candidates for a column.
pub(super) fn candidates(ctx: &ColumnContext<'_>) -> Vec<Candidate> {
    let column = ctx.column();
    let mut out = Vec::new();

    // Identity / auto-increment / serial -> a dense sequence. This is the one
    // schema rule that is (near) certain and outranks the credential guard.
    if is_identity(ctx) {
        out.push(Candidate::new(
            Precedence::SchemaConstraint,
            Confidence::Certain,
            "schema_identity",
            generator_with("sequence", [("start", yaml(1))]),
        ));
    } else if column.default_sql.is_some()
        && !column.primary_key
        && !super::credential::is_guarded(ctx)
    {
        // A bound DEFAULT the source relied on: defer to it — but never for a
        // credential column, whose synthetic generator must win over any
        // source-derived rule (`database_default` renders the source's value).
        out.push(Candidate::new(
            Precedence::SchemaConstraint,
            Confidence::Medium,
            "schema_default",
            generator("database_default"),
        ));
    }

    // A declared boolean column: a boolean generator is a schema fact, not a
    // guess. (The 0/1-integer convention is handled by the distribution
    // heuristic so cross-dialect inference converges.)
    if column.family == SqlTypeFamily::Boolean {
        out.push(Candidate::new(
            Precedence::SchemaConstraint,
            Confidence::High,
            "schema_boolean",
            boolean_generator(ctx),
        ));
    }

    out.push(type_fallback(ctx));
    out
}

/// The guaranteed floor: a plain generator for the column's family.
pub(super) fn type_fallback(ctx: &ColumnContext<'_>) -> Candidate {
    let column = ctx.column();
    let generator = match column.family {
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => {
            let (min, max) = numeric_bounds(ctx).unwrap_or((0, 1000));
            generator_with("integer", [("min", yaml(min)), ("max", yaml(max))])
        }
        SqlTypeFamily::Decimal => {
            let scale = ctx.evidence().and_then(|e| e.decimal_scale).unwrap_or(2);
            let (min, max) = numeric_bounds(ctx).unwrap_or((0, 1000));
            generator_with(
                "decimal",
                [
                    ("min", yaml(min)),
                    ("max", yaml(max)),
                    ("scale", yaml(scale)),
                ],
            )
        }
        SqlTypeFamily::Boolean => boolean_generator(ctx),
        SqlTypeFamily::DateTime => generator("datetime"),
        SqlTypeFamily::Json => generator("json_value"),
        SqlTypeFamily::Uuid => generator("uuid"),
        SqlTypeFamily::Bytes => generator("bytes"),
        SqlTypeFamily::Text | SqlTypeFamily::Other => {
            let (min, max) = string_lengths(ctx);
            generator_with(
                "string",
                [("min_length", yaml(min)), ("max_length", yaml(max))],
            )
        }
    };
    Candidate::new(
        Precedence::TypeFallback,
        Confidence::Low,
        "type_fallback",
        generator,
    )
}

/// Whether the column is a declared identity / auto-increment / serial key.
fn is_identity(ctx: &ColumnContext<'_>) -> bool {
    let column = ctx.column();
    let lower = column.source_type.to_ascii_lowercase();
    let integer = matches!(
        column.family,
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger
    );
    column.identity
        || (integer && (lower.contains("serial") || lower.contains("auto_increment")))
        || (integer && column.primary_key && column.default_sql.is_none() && single_column_pk(ctx))
}

/// Whether the column is the sole primary key of its table (so treating it as a
/// dense sequence is safe).
fn single_column_pk(ctx: &ColumnContext<'_>) -> bool {
    let pk = &ctx.table().primary_key;
    pk.len() == 1 && pk[0] == ctx.column().name
}

/// A boolean generator whose `probability` replays the observed true rate.
fn boolean_generator(ctx: &ColumnContext<'_>) -> crate::synthetic::model::GeneratorConfig {
    if let Some(boolean) = ctx.evidence().and_then(|e| e.boolean) {
        let total = boolean.true_count + boolean.false_count;
        if total > 0 {
            let probability = boolean.true_count as f64 / total as f64;
            let probability = (probability * 10_000.0).round() / 10_000.0;
            return generator_with("boolean", [("probability", yaml(probability))]);
        }
    }
    generator("boolean")
}

/// Observed integer `[min, max]` bounds, when numeric evidence is present.
fn numeric_bounds(ctx: &ColumnContext<'_>) -> Option<(i64, i64)> {
    let numeric = ctx.evidence()?.numeric?;
    let min = numeric.min.floor();
    let max = numeric.max.ceil();
    if min.is_finite() && max.is_finite() {
        Some((min as i64, max as i64))
    } else {
        None
    }
}

/// Observed `[min, max]` string lengths, defaulting to a small span.
fn string_lengths(ctx: &ColumnContext<'_>) -> (usize, usize) {
    match ctx.evidence().and_then(|e| e.string_shape.as_ref()) {
        Some(shape) => (
            shape.min_len.max(1),
            shape.max_len.max(shape.min_len).max(1),
        ),
        None => (8, 16),
    }
}
