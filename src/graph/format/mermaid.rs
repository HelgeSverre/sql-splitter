//! Mermaid erDiagram format output.

use crate::graph::view::GraphView;

/// Generate Mermaid erDiagram from a graph view
pub fn to_mermaid(view: &GraphView) -> String {
    let mut output = String::new();

    // Use erDiagram for proper ERD visualization
    output.push_str("erDiagram\n");

    // Generate entity definitions with attributes
    for table in view.sorted_tables() {
        let safe_name = escape_mermaid_id(&table.name);
        output.push_str(&format!("    {} {{\n", safe_name));

        for col in &table.columns {
            let key_marker = if col.is_primary_key {
                "PK"
            } else if col.is_foreign_key {
                "FK"
            } else {
                ""
            };

            let col_type = escape_mermaid_type(&col.col_type);
            let col_name = escape_mermaid_id(&col.name);

            if key_marker.is_empty() {
                output.push_str(&format!("        {} {}\n", col_type, col_name));
            } else {
                output.push_str(&format!(
                    "        {} {} {}\n",
                    col_type, col_name, key_marker
                ));
            }
        }

        output.push_str("    }\n");
    }

    if !view.edges.is_empty() {
        output.push('\n');
    }

    // Generate relationships
    for edge in &view.edges {
        let from = escape_mermaid_id(&edge.from_table);
        let to = escape_mermaid_id(&edge.to_table);
        let cardinality = edge.cardinality.as_mermaid();
        let label = edge.from_column.clone();

        output.push_str(&format!(
            "    {} {} {} : \"{}\"\n",
            from, cardinality, to, label
        ));
    }

    output
}

/// Escape a string for use as a Mermaid entity ID
fn escape_mermaid_id(s: &str) -> String {
    // Mermaid IDs should be alphanumeric with underscores
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Escape a type string for Mermaid (no spaces, special chars)
fn escape_mermaid_type(s: &str) -> String {
    // Remove parentheses content for cleaner display
    let base = if let Some(paren_pos) = s.find('(') {
        &s[..paren_pos]
    } else {
        s
    };
    base.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::view::{Cardinality, ColumnInfo, EdgeInfo, TableInfo};
    use ahash::AHashMap;

    fn create_test_view() -> GraphView {
        let mut tables = AHashMap::new();

        tables.insert(
            "users".to_string(),
            TableInfo {
                name: "users".to_string(),
                columns: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        col_type: "INT".to_string(),
                        is_primary_key: true,
                        is_foreign_key: false,
                        is_nullable: false,
                        references_table: None,
                        references_column: None,
                    },
                    ColumnInfo {
                        name: "email".to_string(),
                        col_type: "VARCHAR(255)".to_string(),
                        is_primary_key: false,
                        is_foreign_key: false,
                        is_nullable: true,
                        references_table: None,
                        references_column: None,
                    },
                ],
            },
        );

        tables.insert(
            "orders".to_string(),
            TableInfo {
                name: "orders".to_string(),
                columns: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        col_type: "INT".to_string(),
                        is_primary_key: true,
                        is_foreign_key: false,
                        is_nullable: false,
                        references_table: None,
                        references_column: None,
                    },
                    ColumnInfo {
                        name: "user_id".to_string(),
                        col_type: "INT".to_string(),
                        is_primary_key: false,
                        is_foreign_key: true,
                        is_nullable: false,
                        references_table: Some("users".to_string()),
                        references_column: Some("id".to_string()),
                    },
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

    #[test]
    fn test_mermaid_er_diagram() {
        let view = create_test_view();
        let output = to_mermaid(&view);

        assert!(output.contains("erDiagram"));
        assert!(output.contains("users {"));
        assert!(output.contains("orders {"));
    }

    #[test]
    fn test_mermaid_columns() {
        let view = create_test_view();
        let output = to_mermaid(&view);

        assert!(output.contains("INT id PK"));
        assert!(output.contains("INT user_id FK"));
        assert!(output.contains("VARCHAR email"));
    }

    #[test]
    fn test_mermaid_relationships() {
        let view = create_test_view();
        let output = to_mermaid(&view);

        assert!(output.contains("}o--||"));
        assert!(output.contains(": \"user_id\""));
    }
}
