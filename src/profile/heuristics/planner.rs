//! Planner reconnaissance: nominate table planners when — and only when —
//! every column and relationship they require is present.
//!
//! This pass records nominations as informational diagnostics instead of writing `PlannerConfig`
//! entries into the model, so `--explain` and the report can show which planners
//! the table is a candidate for without changing generation behavior.

use crate::diagnostic::{codes, Diagnostic};
use crate::profile::evidence::TableEvidence;
use crate::synthetic::schema::{PortableTable, SqlTypeFamily};

/// Nominate any planners `table` qualifies for, as informational diagnostics.
pub(super) fn nominations(
    table: &PortableTable,
    _evidence: Option<&TableEvidence>,
) -> Vec<Diagnostic> {
    let mut out = Vec::new();

    // A created/updated timestamp pair can drive `temporal.timestamps`.
    if has_column(table, "created_at", SqlTypeFamily::DateTime)
        && has_column(table, "updated_at", SqlTypeFamily::DateTime)
    {
        out.push(Diagnostic::info(
            &codes::INFER_PLANNER_NOMINATE,
            format!("tables.{}", table.name),
            format!(
                "table `{}` is a candidate for `temporal.timestamps` because it has compatible \
                 `created_at` and `updated_at` columns",
                table.name
            ),
        ));
    }

    // A latitude/longitude pair can drive `geo.coordinate_pair`.
    if has_column(table, "latitude", SqlTypeFamily::Decimal)
        && has_column(table, "longitude", SqlTypeFamily::Decimal)
    {
        out.push(Diagnostic::info(
            &codes::INFER_PLANNER_NOMINATE,
            format!("tables.{}", table.name),
            format!(
                "table `{}` is a candidate for `geo.coordinate_pair` because it has compatible \
                 `latitude` and `longitude` columns",
                table.name
            ),
        ));
    }

    out
}

fn has_column(table: &PortableTable, name: &str, family: SqlTypeFamily) -> bool {
    table
        .columns
        .iter()
        .any(|c| c.name == name && c.family == family)
}
