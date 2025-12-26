//! Graphviz DOT format output for ERD diagrams.

use crate::graph::format::Layout;
use crate::graph::view::GraphView;

/// Generate DOT format output with ERD-style tables showing all columns
pub fn to_dot(view: &GraphView, layout: Layout) -> String {
    let mut output = String::new();

    // Header
    output.push_str("digraph ERD {\n");
    output.push_str("  graph [pad=\"0.5\", nodesep=\"1\", ranksep=\"1.5\"];\n");

    // Layout direction
    let rankdir = match layout {
        Layout::LR => "LR",
        Layout::TB => "TB",
    };
    output.push_str(&format!("  rankdir={};\n", rankdir));

    // Node styling for ERD tables
    output.push_str("  node [shape=none, margin=0];\n");
    output.push_str("  edge [arrowhead=crow, arrowtail=none, dir=both];\n\n");

    // Generate each table as an HTML-like label
    for table in view.sorted_tables() {
        let label = generate_table_label(table);
        output.push_str(&format!(
            "  {} [label=<{}>];\n",
            escape_dot_id(&table.name),
            label
        ));
    }

    if !view.edges.is_empty() {
        output.push('\n');
    }

    // Generate edges (FK relationships)
    for edge in &view.edges {
        let label = format!("{}→{}", edge.from_column, edge.to_column);
        output.push_str(&format!(
            "  {}:{} -> {}:{} [label=\"{}\"];\n",
            escape_dot_id(&edge.from_table),
            escape_dot_id(&edge.from_column),
            escape_dot_id(&edge.to_table),
            escape_dot_id(&edge.to_column),
            label
        ));
    }

    output.push_str("}\n");
    output
}

/// Generate HTML-like table label for DOT
fn generate_table_label(table: &crate::graph::view::TableInfo) -> String {
    let mut html = String::new();

    // Table structure with styling
    html.push_str("<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">");

    // Table header
    html.push_str(&format!(
        "<TR><TD BGCOLOR=\"#4a5568\" COLSPAN=\"3\"><FONT COLOR=\"white\"><B>{}</B></FONT></TD></TR>",
        escape_html(&table.name)
    ));

    // Column headers
    html.push_str("<TR>");
    html.push_str("<TD BGCOLOR=\"#e2e8f0\"><FONT POINT-SIZE=\"10\"><B>Column</B></FONT></TD>");
    html.push_str("<TD BGCOLOR=\"#e2e8f0\"><FONT POINT-SIZE=\"10\"><B>Type</B></FONT></TD>");
    html.push_str("<TD BGCOLOR=\"#e2e8f0\"><FONT POINT-SIZE=\"10\"><B>Key</B></FONT></TD>");
    html.push_str("</TR>");

    // Columns
    for col in &table.columns {
        let key_marker = if col.is_primary_key {
            "🔑 PK"
        } else if col.is_foreign_key {
            "🔗 FK"
        } else {
            ""
        };

        let null_marker = if col.is_nullable && !col.is_primary_key {
            " <FONT COLOR=\"#888888\">NULL</FONT>"
        } else {
            ""
        };

        html.push_str("<TR>");
        html.push_str(&format!(
            "<TD ALIGN=\"LEFT\" PORT=\"{}\">{}{}</TD>",
            escape_html(&col.name),
            escape_html(&col.name),
            null_marker
        ));
        html.push_str(&format!(
            "<TD ALIGN=\"LEFT\"><FONT COLOR=\"#666666\">{}</FONT></TD>",
            escape_html(&col.col_type)
        ));
        html.push_str(&format!("<TD ALIGN=\"CENTER\">{}</TD>", key_marker));
        html.push_str("</TR>");
    }

    html.push_str("</TABLE>");
    html
}

/// Escape a string for use in DOT HTML labels
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Escape a string for use as a DOT node ID
fn escape_dot_id(s: &str) -> String {
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') && !s.is_empty() {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }
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
                        is_nullable: false,
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
    fn test_dot_contains_table_structure() {
        let view = create_test_view();
        let output = to_dot(&view, Layout::LR);

        assert!(output.contains("digraph ERD"));
        assert!(output.contains("rankdir=LR"));
        assert!(output.contains("<B>users</B>"));
        assert!(output.contains("<B>orders</B>"));
        assert!(output.contains("🔑 PK"));
        assert!(output.contains("🔗 FK"));
    }

    #[test]
    fn test_dot_contains_columns() {
        let view = create_test_view();
        let output = to_dot(&view, Layout::LR);

        assert!(output.contains("email"));
        assert!(output.contains("VARCHAR(255)"));
        assert!(output.contains("user_id"));
    }

    #[test]
    fn test_dot_contains_edges() {
        let view = create_test_view();
        let output = to_dot(&view, Layout::LR);

        assert!(output.contains("orders:user_id -> users:id"));
    }
}
