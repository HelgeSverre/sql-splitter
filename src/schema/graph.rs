//! Schema dependency graph for FK-aware operations.
//!
//! Provides:
//! - Dependency graph construction from schema FK relationships
//! - Topological sorting for processing order
//! - Cycle detection for handling circular FK relationships

use super::{Schema, TableId};
use std::collections::VecDeque;

/// Schema dependency graph built from foreign key relationships.
///
/// The graph represents parent → child relationships where:
/// - A parent is a table referenced by another table's FK
/// - A child is a table that has an FK referencing another table
///
/// This ordering allows processing parents before children, ensuring
/// that when sampling/filtering children, parent data is already available.
#[derive(Debug)]
pub struct SchemaGraph {
    /// The underlying schema
    pub schema: Schema,
    /// For each table, list of parent tables (tables this table references via FK)
    pub parents: Vec<Vec<TableId>>,
    /// For each table, list of child tables (tables that reference this table via FK)
    pub children: Vec<Vec<TableId>>,
}

/// Result of topological sort
#[derive(Debug)]
pub struct TopoSortResult {
    /// Tables in topological order (parents before children)
    pub order: Vec<TableId>,
    /// Tables that are part of cycles (could not be ordered)
    pub cyclic_tables: Vec<TableId>,
}

impl SchemaGraph {
    /// Build a dependency graph from a schema
    pub fn from_schema(schema: Schema) -> Self {
        let n = schema.table_schemas.len();
        let mut parents: Vec<Vec<TableId>> = vec![Vec::new(); n];
        let mut children: Vec<Vec<TableId>> = vec![Vec::new(); n];

        for table in &schema.table_schemas {
            let child_id = table.id;

            for fk in &table.foreign_keys {
                if let Some(parent_id) = fk.referenced_table_id {
                    // Avoid self-references in the graph (handle separately)
                    if parent_id != child_id {
                        // Child depends on parent
                        if !parents[child_id.0 as usize].contains(&parent_id) {
                            parents[child_id.0 as usize].push(parent_id);
                        }
                        // Parent has child dependent
                        if !children[parent_id.0 as usize].contains(&child_id) {
                            children[parent_id.0 as usize].push(child_id);
                        }
                    }
                }
            }
        }

        Self {
            schema,
            parents,
            children,
        }
    }

    /// Get the number of tables in the graph
    pub fn len(&self) -> usize {
        self.schema.len()
    }

    /// Check if the graph is empty
    pub fn is_empty(&self) -> bool {
        self.schema.is_empty()
    }

    /// Get the table name for a table ID
    pub fn table_name(&self, id: TableId) -> Option<&str> {
        self.schema.table(id).map(|t| t.name.as_str())
    }

    /// Check if a table has a self-referential FK
    pub fn has_self_reference(&self, id: TableId) -> bool {
        self.schema
            .table(id)
            .map(|t| {
                t.foreign_keys
                    .iter()
                    .any(|fk| fk.referenced_table_id == Some(id))
            })
            .unwrap_or(false)
    }

    /// Get tables that have self-referential FKs
    pub fn self_referential_tables(&self) -> Vec<TableId> {
        (0..self.len())
            .map(|i| TableId(i as u32))
            .filter(|&id| self.has_self_reference(id))
            .collect()
    }

    /// Perform topological sort using Kahn's algorithm.
    ///
    /// Returns tables in dependency order (parents before children).
    /// Tables that are part of cycles are returned separately.
    pub fn topo_sort(&self) -> TopoSortResult {
        let n = self.len();
        if n == 0 {
            return TopoSortResult {
                order: Vec::new(),
                cyclic_tables: Vec::new(),
            };
        }

        // Calculate in-degrees (number of parents for each table)
        let mut in_degree: Vec<usize> = vec![0; n];
        for (i, parents) in self.parents.iter().enumerate() {
            in_degree[i] = parents.len();
        }

        // Start with tables that have no parents (roots)
        let mut queue: VecDeque<TableId> = VecDeque::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                queue.push_back(TableId(i as u32));
            }
        }

        let mut order = Vec::with_capacity(n);

        while let Some(table_id) = queue.pop_front() {
            order.push(table_id);

            // Reduce in-degree of all children
            for &child_id in &self.children[table_id.0 as usize] {
                in_degree[child_id.0 as usize] -= 1;
                if in_degree[child_id.0 as usize] == 0 {
                    queue.push_back(child_id);
                }
            }
        }

        // Tables with remaining in-degree > 0 are part of cycles
        let cyclic_tables: Vec<TableId> = in_degree
            .iter()
            .enumerate()
            .filter(|(_, &deg)| deg > 0)
            .map(|(i, _)| TableId(i as u32))
            .collect();

        TopoSortResult {
            order,
            cyclic_tables,
        }
    }

    /// Get processing order for sampling/sharding.
    ///
    /// Returns all tables in order: first the topologically sorted acyclic tables,
    /// then the cyclic tables (which need special handling).
    pub fn processing_order(&self) -> (Vec<TableId>, Vec<TableId>) {
        let result = self.topo_sort();
        (result.order, result.cyclic_tables)
    }

    /// Check if table A is an ancestor of table B (A is referenced by B directly or transitively)
    pub fn is_ancestor(&self, ancestor: TableId, descendant: TableId) -> bool {
        if ancestor == descendant {
            return false;
        }

        let mut visited = vec![false; self.len()];
        let mut queue = VecDeque::new();
        queue.push_back(descendant);

        while let Some(current) = queue.pop_front() {
            for &parent in &self.parents[current.0 as usize] {
                if parent == ancestor {
                    return true;
                }
                if !visited[parent.0 as usize] {
                    visited[parent.0 as usize] = true;
                    queue.push_back(parent);
                }
            }
        }

        false
    }

    /// Get all ancestor tables of a given table (tables it depends on, directly or transitively)
    pub fn ancestors(&self, id: TableId) -> Vec<TableId> {
        let mut ancestors = Vec::new();
        let mut visited = vec![false; self.len()];
        let mut queue = VecDeque::new();

        for &parent in &self.parents[id.0 as usize] {
            queue.push_back(parent);
            visited[parent.0 as usize] = true;
        }

        while let Some(current) = queue.pop_front() {
            ancestors.push(current);
            for &parent in &self.parents[current.0 as usize] {
                if !visited[parent.0 as usize] {
                    visited[parent.0 as usize] = true;
                    queue.push_back(parent);
                }
            }
        }

        ancestors
    }

    /// Get all descendant tables of a given table (tables that depend on it)
    pub fn descendants(&self, id: TableId) -> Vec<TableId> {
        let mut descendants = Vec::new();
        let mut visited = vec![false; self.len()];
        let mut queue = VecDeque::new();

        for &child in &self.children[id.0 as usize] {
            queue.push_back(child);
            visited[child.0 as usize] = true;
        }

        while let Some(current) = queue.pop_front() {
            descendants.push(current);
            for &child in &self.children[current.0 as usize] {
                if !visited[child.0 as usize] {
                    visited[child.0 as usize] = true;
                    queue.push_back(child);
                }
            }
        }

        descendants
    }

    /// Get root tables (tables with no parents/dependencies)
    pub fn root_tables(&self) -> Vec<TableId> {
        self.parents
            .iter()
            .enumerate()
            .filter(|(_, parents)| parents.is_empty())
            .map(|(i, _)| TableId(i as u32))
            .collect()
    }

    /// Get leaf tables (tables with no children/dependents)
    pub fn leaf_tables(&self) -> Vec<TableId> {
        self.children
            .iter()
            .enumerate()
            .filter(|(_, children)| children.is_empty())
            .map(|(i, _)| TableId(i as u32))
            .collect()
    }
}
