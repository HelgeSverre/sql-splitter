//! Planner reconnaissance: nominate table planners when — and only when —
//! every column and relationship they require is present.
//!
//! This pass records nominations as warnings instead of writing `PlannerConfig`
//! entries into the model, so `--explain` and the report can show which planners
//! the table is a candidate for without changing generation behavior.

use crate::profile::evidence::TableEvidence;
use crate::synthetic::schema::{PortableTable, SqlTypeFamily};

/// Nominate any planners `table` qualifies for, as human-readable warnings.
pub(super) fn nominations(table: &PortableTable, _evidence: Option<&TableEvidence>) -> Vec<String> {
    let mut out = Vec::new();

    // `relation.children`: this table is the child side of a declared FK, so a
    // fan-out planner could allocate its rows across the parent.
    for fk in &table.relationships {
        out.push(format!(
            "GEN-INFER-PLANNER-NOMINATE: table `{}` is a candidate for the `relation.children` \
             planner via its foreign key to `{}`",
            table.name, fk.referenced_table
        ));
    }

    // A created/updated timestamp pair could drive a temporal-ordering planner.
    if has_column(table, "created_at", SqlTypeFamily::DateTime)
        && has_column(table, "updated_at", SqlTypeFamily::DateTime)
    {
        out.push(format!(
            "GEN-INFER-PLANNER-NOMINATE: table `{}` is a candidate for the temporal-ordering \
             planner (created_at <= updated_at)",
            table.name
        ));
    }

    // A latitude/longitude pair could drive a geo-coordinate planner.
    if has_column(table, "latitude", SqlTypeFamily::Decimal)
        && has_column(table, "longitude", SqlTypeFamily::Decimal)
    {
        out.push(format!(
            "GEN-INFER-PLANNER-NOMINATE: table `{}` is a candidate for the geo-coordinate \
             planner (latitude/longitude)",
            table.name
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
