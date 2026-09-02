//! ERD (Entity-Relationship Diagram) generation module.
//!
//! This module provides:
//! - Full schema visualization with tables, columns, and relationships
//! - Cycle detection using Tarjan's SCC algorithm
//! - Multiple output formats: DOT (Graphviz), Mermaid, JSON, HTML
//! - Topological ordering for safe import order

pub mod analysis;
pub mod format;
pub mod view;

#[allow(unused_imports)] // Public API re-exports used by external consumers and tests
pub use analysis::Cycle;
pub use analysis::{cyclic_tables, find_cycles};
pub use format::{to_dot, to_html, to_json, to_mermaid, Layout, OutputFormat};
pub use view::GraphView;
#[allow(unused_imports)] // Public API re-exports used by external consumers and tests
pub use view::{ColumnInfo, EdgeInfo, TableInfo};

#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::view::{Cardinality, ColumnInfo, EdgeInfo, GraphView, TableInfo};
    use ahash::AHashMap;

    fn col(
        name: &str,
        col_type: &str,
        pk: bool,
        nullable: bool,
        fk: Option<(&str, &str)>,
    ) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: col_type.to_string(),
            is_primary_key: pk,
            is_foreign_key: fk.is_some(),
            is_nullable: nullable,
            references_table: fk.map(|(t, _)| t.to_string()),
            references_column: fk.map(|(_, c)| c.to_string()),
        }
    }

    /// `users(id PK, email)` and `orders(id PK, user_id FK -> users.id)`.
    pub(crate) fn create_test_view() -> GraphView {
        let mut tables = AHashMap::new();
        tables.insert(
            "users".to_string(),
            TableInfo {
                name: "users".to_string(),
                columns: vec![
                    col("id", "INT", true, false, None),
                    col("email", "VARCHAR(255)", false, true, None),
                ],
            },
        );
        tables.insert(
            "orders".to_string(),
            TableInfo {
                name: "orders".to_string(),
                columns: vec![
                    col("id", "INT", true, false, None),
                    col("user_id", "INT", false, false, Some(("users", "id"))),
                ],
            },
        );
        let edges = vec![EdgeInfo {
            from_table: "orders".to_string(),
            from_column: "user_id".to_string(),
            to_table: "users".to_string(),
            to_column: "id".to_string(),
            cardinality: Cardinality::ManyToOne,
        }];
        GraphView { tables, edges }
    }
}
