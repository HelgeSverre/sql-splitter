//! Built-in planner factories.
//!
//! Planners coordinate table-level structure — row counts, parent/child
//! fan-out, and other cross-column decisions — via the
//! [`PlannerFactory`](super::registry::PlannerFactory) /
//! [`CompiledPlanner`](super::registry::CompiledPlanner) traits.
//!
//! The catalog includes same-table temporal and workflow planners, correlated
//! order-family generation, and structural planners for common relational and
//! lifecycle patterns.

pub mod interval;
pub mod order_family;
pub mod progress;
pub mod structural;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde_yaml_ng::Value;

use crate::diagnostic::DiagnosticBag;
use crate::generate::value::GeneratedValue;
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

pub use interval::TemporalIntervalFactory;
pub use order_family::OrderFamilyFactory;
pub use progress::ProgressCountersFactory;
pub use structural::{
    FileMetadataFactory, GeoCoordinatePairFactory, HierarchyTreeFactory,
    RelationJunctionPairFactory, RelationPolymorphicPairFactory, RelationTenantFamilyFactory,
    TemporalLifecycleFactory, TemporalSoftDeleteFactory, TemporalTimestampsFactory,
};

/// Implement [`PlannerFactory`](super::registry::PlannerFactory) for a unit
/// struct: `descriptor()` returns `$descriptor` and `compile()` delegates to
/// `$compile(config, context)`, boxing the resulting planner.
macro_rules! planner_factory {
    ($factory:ty, $descriptor:expr, $compile:path) => {
        impl $crate::generate::registry::PlannerFactory for $factory {
            fn descriptor(&self) -> &'static $crate::generate::registry::PlannerDescriptor {
                &$descriptor
            }

            fn compile(
                &self,
                config: &$crate::synthetic::model::PlannerConfig,
                context: &$crate::generate::registry::CompileContext<'_>,
            ) -> Result<
                Box<dyn $crate::generate::registry::CompiledPlanner>,
                $crate::diagnostic::DiagnosticBag,
            > {
                $compile(config, context).map(|planner| {
                    Box::new(planner) as Box<dyn $crate::generate::registry::CompiledPlanner>
                })
            }
        }
    };
}
pub(super) use planner_factory;

// =============================================================================
// Shared YAML / schema helpers
// =============================================================================

/// Nanoseconds per second, the base unit conversion for every offset.
pub(super) const NANOS_PER_SECOND: i128 = 1_000_000_000;

pub(super) fn unit_nanos(unit: &str) -> Option<i128> {
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

pub(super) fn as_i128(value: &Value) -> Option<i128> {
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

pub(super) fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Parse a timestamp value into epoch nanoseconds. Accepts RFC 3339
/// (`2024-01-01T00:00:00Z`), space- or `T`-separated naive timestamps, and bare
/// dates (interpreted as UTC midnight).
pub(super) fn as_instant_ns(value: &Value) -> Option<i128> {
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

/// The column name a `columns:` mapping assigns to `role`, if any.
pub(super) fn role_name<'a>(columns: Option<&'a Value>, role: &str) -> Option<&'a str> {
    columns?.get(role).and_then(Value::as_str)
}

pub(super) fn find_column<'a>(table: &'a PortableTable, name: &str) -> Option<&'a PortableColumn> {
    table.columns.iter().find(|column| column.name == name)
}

pub(super) fn column_nullable(table: &PortableTable, name: &str) -> bool {
    find_column(table, name).is_some_and(|column| column.nullable)
}

/// Resolve a column role under `columns:` to its schema column. A `required`
/// role that is absent is a `code` error; any role naming a column that does
/// not exist on `table` is a `code` error. Returns `None` in both cases and
/// for an absent optional role.
#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_role<'a>(
    columns: Option<&Value>,
    role: &str,
    table: &'a PortableTable,
    path: &str,
    planner: &str,
    code: &'static str,
    required: bool,
    bag: &mut DiagnosticBag,
) -> Option<&'a PortableColumn> {
    let Some(name) = role_name(columns, role) else {
        if required {
            bag.error(
                code,
                format!("{path}.columns.{role}"),
                format!("{planner} requires a `{role}` column under `columns`"),
            );
        }
        return None;
    };
    let column = find_column(table, name);
    if column.is_none() {
        bag.error(
            code,
            format!("{path}.columns.{role}"),
            format!(
                "{planner} `{role}` column `{name}` does not exist on table `{}`",
                table.name
            ),
        );
    }
    column
}

/// Render a whole number (a key, counter, or duration) in the representation
/// `family` expects: an integer for integer families, text otherwise.
pub(super) fn render_integer(value: i128, family: &SqlTypeFamily) -> GeneratedValue {
    match family {
        SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(value),
        _ => GeneratedValue::Text(value.to_string()),
    }
}

/// Render a status label; every family receives it as text.
pub(super) fn render_status(status: &str) -> GeneratedValue {
    GeneratedValue::Text(status.to_string())
}

/// Parse a YAML value into a list of strings (from a sequence of scalars).
pub(super) fn string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Sequence(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// Parse a YAML sequence of numbers into a list of `f64` weights; `None` when
/// the value is not a sequence or any element is non-numeric.
pub(super) fn number_list(value: Option<&Value>) -> Option<Vec<f64>> {
    match value {
        Some(Value::Sequence(items)) => items.iter().map(as_f64).collect(),
        _ => None,
    }
}
