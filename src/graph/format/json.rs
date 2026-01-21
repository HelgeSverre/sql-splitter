//! JSON format output for ERD data.

use crate::graph::view::GraphView;
use schemars::JsonSchema;
use serde::Serialize;

/// JSON representation of the ERD
#[derive(Debug, Serialize, JsonSchema)]
pub struct ErdJson {
    pub tables: Vec<TableJson>,
    pub relationships: Vec<RelationshipJson>,
    pub stats: ErdStats,
}

/// JSON representation of a table with full column details
#[derive(Debug, Serialize, JsonSchema)]
pub struct TableJson {
    pub name: String,
    pub columns: Vec<ColumnJson>,
}

/// JSON representation of a column
#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnJson {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
    pub is_nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references_table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references_column: Option<String>,
}

/// JSON representation of a relationship
#[derive(Debug, Serialize, JsonSchema)]
pub struct RelationshipJson {
    pub from_table: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
    pub cardinality: String,
}

/// ERD statistics
#[derive(Debug, Serialize, JsonSchema)]
pub struct ErdStats {
    pub table_count: usize,
    pub column_count: usize,
    pub relationship_count: usize,
}

/// Generate JSON output from a graph view
pub fn to_json(view: &GraphView) -> String {
    let erd = build_erd_json(view);
    serde_json::to_string_pretty(&erd).unwrap_or_else(|_| "{}".to_string())
}

/// Build the JSON structure
pub fn build_erd_json(view: &GraphView) -> ErdJson {
    let mut total_columns = 0;

    // Build tables with columns
    let tables: Vec<TableJson> = view
        .sorted_tables()
        .iter()
        .map(|table| {
            let columns: Vec<ColumnJson> = table
                .columns
                .iter()
                .map(|col| ColumnJson {
                    name: col.name.clone(),
                    col_type: col.col_type.clone(),
                    is_primary_key: col.is_primary_key,
                    is_foreign_key: col.is_foreign_key,
                    is_nullable: col.is_nullable,
                    references_table: col.references_table.clone(),
                    references_column: col.references_column.clone(),
                })
                .collect();

            total_columns += columns.len();

            TableJson {
                name: table.name.clone(),
                columns,
            }
        })
        .collect();

    // Build relationships
    let relationships: Vec<RelationshipJson> = view
        .edges
        .iter()
        .map(|e| RelationshipJson {
            from_table: e.from_table.clone(),
            from_column: e.from_column.clone(),
            to_table: e.to_table.clone(),
            to_column: e.to_column.clone(),
            cardinality: format!("{:?}", e.cardinality),
        })
        .collect();

    ErdJson {
        tables,
        relationships,
        stats: ErdStats {
            table_count: view.table_count(),
            column_count: total_columns,
            relationship_count: view.edge_count(),
        },
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
                        col_type: "VARCHAR".to_string(),
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
                columns: vec![ColumnInfo {
                    name: "user_id".to_string(),
                    col_type: "INT".to_string(),
                    is_primary_key: false,
                    is_foreign_key: true,
                    is_nullable: false,
                    references_table: Some("users".to_string()),
                    references_column: Some("id".to_string()),
                }],
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
    fn test_json_structure() {
        let view = create_test_view();
        let erd = build_erd_json(&view);

        assert_eq!(erd.tables.len(), 2);
        assert_eq!(erd.relationships.len(), 1);
        assert_eq!(erd.stats.table_count, 2);
        assert_eq!(erd.stats.column_count, 3);
    }

    #[test]
    fn test_json_columns() {
        let view = create_test_view();
        let erd = build_erd_json(&view);

        let users = erd.tables.iter().find(|t| t.name == "users").unwrap();
        assert_eq!(users.columns.len(), 2);

        let id_col = users.columns.iter().find(|c| c.name == "id").unwrap();
        assert!(id_col.is_primary_key);
        assert!(!id_col.is_nullable);
    }

    #[test]
    fn test_json_fk_references() {
        let view = create_test_view();
        let erd = build_erd_json(&view);

        let orders = erd.tables.iter().find(|t| t.name == "orders").unwrap();
        let fk_col = orders.columns.iter().find(|c| c.name == "user_id").unwrap();

        assert!(fk_col.is_foreign_key);
        assert_eq!(fk_col.references_table, Some("users".to_string()));
        assert_eq!(fk_col.references_column, Some("id".to_string()));
    }

    #[test]
    fn test_json_output() {
        let view = create_test_view();
        let output = to_json(&view);

        assert!(output.contains("\"name\": \"orders\""));
        assert!(output.contains("\"is_primary_key\": true"));
        assert!(output.contains("\"references_table\": \"users\""));
    }
}
