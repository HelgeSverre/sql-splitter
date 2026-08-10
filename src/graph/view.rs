//! Graph view with filtering and focus capabilities for ERD generation.

use crate::schema::{ColumnType, SchemaGraph};
use ahash::{AHashMap, AHashSet};
use glob::Pattern;
use std::collections::VecDeque;

/// Information about a column in a table
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column type (as string for display)
    pub col_type: String,
    /// Whether this column is a primary key
    pub is_primary_key: bool,
    /// Whether this column is a foreign key
    pub is_foreign_key: bool,
    /// Whether this column is nullable
    pub is_nullable: bool,
    /// If FK, which table it references
    pub references_table: Option<String>,
    /// If FK, which column it references
    pub references_column: Option<String>,
}

/// Information about a table for ERD rendering
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// All columns in order
    pub columns: Vec<ColumnInfo>,
}

/// Information about an edge (FK relationship) in the graph
#[derive(Debug, Clone)]
pub struct EdgeInfo {
    /// Source table (child with FK)
    pub from_table: String,
    /// Source column (FK column)
    pub from_column: String,
    /// Target table (parent being referenced)
    pub to_table: String,
    /// Target column (referenced column, usually PK)
    pub to_column: String,
    /// Relationship cardinality (for ERD display)
    pub cardinality: Cardinality,
}

/// Relationship cardinality for ERD
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Cardinality {
    #[default]
    ManyToOne, // Most common: child has FK to parent
    OneToOne,
    OneToMany,
    ManyToMany,
}

impl Cardinality {
    /// Mermaid ERD notation
    pub fn as_mermaid(self) -> &'static str {
        match self {
            Cardinality::ManyToOne => "}o--||",
            Cardinality::OneToOne => "||--||",
            Cardinality::OneToMany => "||--o{",
            Cardinality::ManyToMany => "}o--o{",
        }
    }
}

/// A filtered view of a schema graph for ERD visualization
#[derive(Debug)]
pub struct GraphView {
    /// Tables included in this view with full column info
    pub tables: AHashMap<String, TableInfo>,
    /// Edges between tables (FK relationships)
    pub edges: Vec<EdgeInfo>,
}

impl GraphView {
    /// Create a full view from a schema graph (all tables and edges)
    pub fn from_schema_graph(graph: &SchemaGraph) -> Self {
        let mut tables = AHashMap::new();
        let mut edges = Vec::new();

        // Build FK lookup: which columns are FKs and what they reference
        let mut fk_lookup: AHashMap<(String, String), (String, String)> = AHashMap::new();

        for table_schema in graph.schema.iter() {
            for fk in &table_schema.foreign_keys {
                for (i, col_name) in fk.column_names.iter().enumerate() {
                    let ref_col = fk.referenced_columns.get(i).cloned().unwrap_or_default();
                    fk_lookup.insert(
                        (table_schema.name.clone(), col_name.clone()),
                        (fk.referenced_table.clone(), ref_col),
                    );
                }
            }
        }

        // Build table info with full column details
        for table_schema in graph.schema.iter() {
            let mut columns = Vec::new();

            for col in &table_schema.columns {
                let is_fk = fk_lookup.contains_key(&(table_schema.name.clone(), col.name.clone()));
                let (ref_table, ref_col) = fk_lookup
                    .get(&(table_schema.name.clone(), col.name.clone()))
                    .cloned()
                    .map(|(t, c)| (Some(t), Some(c)))
                    .unwrap_or((None, None));

                columns.push(ColumnInfo {
                    name: col.name.clone(),
                    col_type: format_column_type(&col.col_type),
                    is_primary_key: col.is_primary_key,
                    is_foreign_key: is_fk,
                    is_nullable: col.is_nullable,
                    references_table: ref_table,
                    references_column: ref_col,
                });
            }

            tables.insert(
                table_schema.name.clone(),
                TableInfo {
                    name: table_schema.name.clone(),
                    columns,
                },
            );
        }

        // Build edges from FK relationships
        for table_schema in graph.schema.iter() {
            for fk in &table_schema.foreign_keys {
                // Create one edge per FK column pair
                for (i, col_name) in fk.column_names.iter().enumerate() {
                    let ref_col = fk
                        .referenced_columns
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| "id".to_string());

                    edges.push(EdgeInfo {
                        from_table: table_schema.name.clone(),
                        from_column: col_name.clone(),
                        to_table: fk.referenced_table.clone(),
                        to_column: ref_col,
                        cardinality: Cardinality::ManyToOne,
                    });
                }
            }
        }

        Self { tables, edges }
    }

    /// Filter to include only tables matching the given patterns
    pub fn filter_tables(&mut self, patterns: &[Pattern]) {
        if patterns.is_empty() {
            return;
        }

        let matching: AHashSet<String> = self
            .tables
            .keys()
            .filter(|name| patterns.iter().any(|p| p.matches(name)))
            .cloned()
            .collect();

        self.apply_node_filter(&matching);
    }

    /// Exclude tables matching the given patterns
    pub fn exclude_tables(&mut self, patterns: &[Pattern]) {
        if patterns.is_empty() {
            return;
        }

        let remaining: AHashSet<String> = self
            .tables
            .keys()
            .filter(|name| !patterns.iter().any(|p| p.matches(name)))
            .cloned()
            .collect();

        self.apply_node_filter(&remaining);
    }

    /// Focus on a specific table and its relationships
    pub fn focus_table(
        &mut self,
        table: &str,
        transitive: bool,
        reverse: bool,
        max_depth: Option<usize>,
    ) {
        if !self.tables.contains_key(table) {
            self.tables.clear();
            self.edges.clear();
            return;
        }

        let mut result_nodes = AHashSet::new();
        result_nodes.insert(table.to_string());

        // Build adjacency maps for traversal
        let (outgoing, incoming) = self.build_adjacency_maps();

        if transitive {
            // Show tables this table depends on (parents, transitively)
            self.traverse(&outgoing, table, max_depth, &mut result_nodes);
        }

        if reverse {
            // Show tables that depend on this table (children, transitively)
            self.traverse(&incoming, table, max_depth, &mut result_nodes);
        }

        // If neither transitive nor reverse, show direct connections only
        if !transitive && !reverse {
            if let Some(parents) = outgoing.get(table) {
                for parent in parents {
                    result_nodes.insert(parent.clone());
                }
            }
            if let Some(children) = incoming.get(table) {
                for child in children {
                    result_nodes.insert(child.clone());
                }
            }
        }

        self.apply_node_filter(&result_nodes);
    }

    /// Keep only tables that are part of cycles
    pub fn filter_to_cyclic_tables(&mut self, cyclic_tables: &AHashSet<String>) {
        self.apply_node_filter(cyclic_tables);
    }

    /// Get the number of tables in the view
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Get the number of edges in the view
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Check if the view is empty
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get tables sorted alphabetically
    pub fn sorted_tables(&self) -> Vec<&TableInfo> {
        let mut tables: Vec<_> = self.tables.values().collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    }

    /// Get table info by name
    pub fn get_table(&self, name: &str) -> Option<&TableInfo> {
        self.tables.get(name)
    }

    // Private helper methods

    fn apply_node_filter(&mut self, keep: &AHashSet<String>) {
        self.tables.retain(|n, _| keep.contains(n));
        self.edges
            .retain(|e| keep.contains(&e.from_table) && keep.contains(&e.to_table));
    }

    fn build_adjacency_maps(
        &self,
    ) -> (AHashMap<String, Vec<String>>, AHashMap<String, Vec<String>>) {
        let mut outgoing: AHashMap<String, Vec<String>> = AHashMap::new();
        let mut incoming: AHashMap<String, Vec<String>> = AHashMap::new();

        for edge in &self.edges {
            outgoing
                .entry(edge.from_table.clone())
                .or_default()
                .push(edge.to_table.clone());
            incoming
                .entry(edge.to_table.clone())
                .or_default()
                .push(edge.from_table.clone());
        }

        (outgoing, incoming)
    }

    fn traverse(
        &self,
        adjacency: &AHashMap<String, Vec<String>>,
        start: &str,
        max_depth: Option<usize>,
        result: &mut AHashSet<String>,
    ) {
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        queue.push_back((start.to_string(), 0));

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max) = max_depth {
                if depth >= max {
                    continue;
                }
            }

            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors {
                    if result.insert(neighbor.clone()) {
                        queue.push_back((neighbor.clone(), depth + 1));
                    }
                }
            }
        }
    }
}

/// Format a ColumnType for display
fn format_column_type(col_type: &ColumnType) -> String {
    match col_type {
        ColumnType::Int => "INT".to_string(),
        ColumnType::BigInt => "BIGINT".to_string(),
        ColumnType::Text => "VARCHAR".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Decimal => "DECIMAL".to_string(),
        ColumnType::DateTime => "DATETIME".to_string(),
        ColumnType::Bool => "BOOL".to_string(),
        ColumnType::Other(s) => s.to_uppercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                        col_type: "VARCHAR".to_string(),
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
    fn test_table_info() {
        let view = create_test_view();
        assert_eq!(view.table_count(), 2);

        let users = view.get_table("users").unwrap();
        assert_eq!(users.columns.len(), 2);
        assert!(users.columns[0].is_primary_key);
    }

    #[test]
    fn test_edge_info() {
        let view = create_test_view();
        assert_eq!(view.edge_count(), 1);

        let edge = &view.edges[0];
        assert_eq!(edge.from_table, "orders");
        assert_eq!(edge.from_column, "user_id");
        assert_eq!(edge.to_table, "users");
        assert_eq!(edge.to_column, "id");
    }

    #[test]
    fn test_exclude_tables() {
        let mut view = create_test_view();
        let patterns = vec![Pattern::new("orders").unwrap()];
        view.exclude_tables(&patterns);

        assert!(!view.tables.contains_key("orders"));
        assert!(view.tables.contains_key("users"));
        assert_eq!(view.edge_count(), 0); // Edge removed since orders is gone
    }
}
