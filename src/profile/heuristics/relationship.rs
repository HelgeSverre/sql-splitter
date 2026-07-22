//! Relationship heuristics: recognize the columns a declared foreign key owns.
//!
//! A column that participates in a declared foreign key is *not* given a
//! column generator at all — the compiler assigns it structurally from the
//! table's relationship (preserving referential integrity), and the emitted
//! model carries the relationship explicitly (see
//! [`super::declared_relationships`]). This heuristic just reports which columns
//! that covers, so the resolver can skip them and record the decision.
//!
//! Name/type-based FK *candidates* (a bare `user_id` with no declared FK) are a
//! documented follow-up; the minimal correct behavior here is declared FKs.

use crate::synthetic::schema::PortableTable;

/// Whether `column` participates in any declared foreign key on `table`.
pub(super) fn is_foreign_key_column(table: &PortableTable, column: &str) -> bool {
    table
        .relationships
        .iter()
        .any(|fk| fk.columns.iter().any(|c| c == column))
}
