//! Graph analysis algorithms: cycle detection and topological sort.

use crate::graph::view::GraphView;
use ahash::{AHashMap, AHashSet};

/// A cycle in the graph (list of table names forming the cycle)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cycle {
    pub tables: Vec<String>,
}

impl Cycle {
    /// Check if this is a self-referencing cycle (single table)
    pub fn is_self_reference(&self) -> bool {
        self.tables.len() == 1
    }

    /// Format the cycle for display
    pub fn display(&self) -> String {
        if self.is_self_reference() {
            format!("{} -> {} (self-reference)", self.tables[0], self.tables[0])
        } else {
            let mut parts = self.tables.clone();
            parts.push(self.tables[0].clone()); // Complete the cycle
            parts.join(" -> ")
        }
    }
}

/// Find all cycles in the graph using Tarjan's SCC algorithm
pub fn find_cycles(view: &GraphView) -> Vec<Cycle> {
    let mut finder = TarjanSCC::new(view);
    finder.find_sccs();

    let mut cycles = Vec::new();

    for scc in &finder.sccs {
        if scc.len() == 1 {
            // Check if it's a self-referencing table
            let table = &scc[0];
            if view
                .edges
                .iter()
                .any(|e| &e.from_table == table && &e.to_table == table)
            {
                cycles.push(Cycle {
                    tables: scc.clone(),
                });
            }
        } else if scc.len() > 1 {
            // Multi-table cycle
            cycles.push(Cycle {
                tables: scc.clone(),
            });
        }
    }

    cycles
}

/// Get all tables that are part of any cycle
pub fn cyclic_tables(view: &GraphView) -> AHashSet<String> {
    let cycles = find_cycles(view);
    let mut tables = AHashSet::new();
    for cycle in cycles {
        for table in cycle.tables {
            tables.insert(table);
        }
    }
    tables
}

/// Tarjan's Strongly Connected Components algorithm
struct TarjanSCC<'a> {
    view: &'a GraphView,
    index_counter: usize,
    stack: Vec<String>,
    on_stack: AHashSet<String>,
    indices: AHashMap<String, usize>,
    lowlinks: AHashMap<String, usize>,
    sccs: Vec<Vec<String>>,
    adjacency: AHashMap<String, Vec<String>>,
}

impl<'a> TarjanSCC<'a> {
    fn new(view: &'a GraphView) -> Self {
        // Build adjacency list from tables
        let mut adjacency: AHashMap<String, Vec<String>> = AHashMap::new();
        for table_name in view.tables.keys() {
            adjacency.insert(table_name.clone(), Vec::new());
        }
        for edge in &view.edges {
            if view.tables.contains_key(&edge.from_table)
                && view.tables.contains_key(&edge.to_table)
            {
                adjacency
                    .entry(edge.from_table.clone())
                    .or_default()
                    .push(edge.to_table.clone());
            }
        }

        Self {
            view,
            index_counter: 0,
            stack: Vec::new(),
            on_stack: AHashSet::new(),
            indices: AHashMap::new(),
            lowlinks: AHashMap::new(),
            sccs: Vec::new(),
            adjacency,
        }
    }

    fn find_sccs(&mut self) {
        let nodes: Vec<_> = self.view.tables.keys().cloned().collect();
        for node in nodes {
            if !self.indices.contains_key(&node) {
                self.strongconnect(&node);
            }
        }
    }

    fn strongconnect(&mut self, v: &str) {
        // Set the depth index for v
        self.indices.insert(v.to_string(), self.index_counter);
        self.lowlinks.insert(v.to_string(), self.index_counter);
        self.index_counter += 1;
        self.stack.push(v.to_string());
        self.on_stack.insert(v.to_string());

        // Consider successors of v
        if let Some(neighbors) = self.adjacency.get(v).cloned() {
            for w in neighbors {
                if !self.indices.contains_key(&w) {
                    // Successor w has not yet been visited; recurse on it
                    self.strongconnect(&w);
                    let v_lowlink = self.lowlinks[v];
                    let w_lowlink = self.lowlinks[&w];
                    self.lowlinks
                        .insert(v.to_string(), v_lowlink.min(w_lowlink));
                } else if self.on_stack.contains(&w) {
                    // Successor w is in stack S and hence in the current SCC
                    let v_lowlink = self.lowlinks[v];
                    let w_index = self.indices[&w];
                    self.lowlinks.insert(v.to_string(), v_lowlink.min(w_index));
                }
            }
        }

        // If v is a root node, pop the stack and generate an SCC
        if self.lowlinks[v] == self.indices[v] {
            let mut scc = Vec::new();
            loop {
                let w = self.stack.pop().unwrap();
                self.on_stack.remove(&w);
                scc.push(w.clone());
                if w == v {
                    break;
                }
            }
            self.sccs.push(scc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::view::{Cardinality, TableInfo};

    fn create_simple_table(name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            columns: vec![],
        }
    }

    fn create_edge(from: &str, to: &str) -> crate::graph::EdgeInfo {
        crate::graph::EdgeInfo {
            from_table: from.to_string(),
            from_column: "fk".to_string(),
            to_table: to.to_string(),
            to_column: "id".to_string(),
            cardinality: Cardinality::ManyToOne,
        }
    }

    fn create_acyclic_view() -> GraphView {
        let mut tables = AHashMap::new();
        tables.insert("users".to_string(), create_simple_table("users"));
        tables.insert("orders".to_string(), create_simple_table("orders"));
        tables.insert("products".to_string(), create_simple_table("products"));

        let edges = vec![create_edge("orders", "users")];

        GraphView { tables, edges }
    }

    fn create_self_ref_view() -> GraphView {
        let mut tables = AHashMap::new();
        tables.insert("categories".to_string(), create_simple_table("categories"));

        let edges = vec![create_edge("categories", "categories")];

        GraphView { tables, edges }
    }

    fn create_multi_cycle_view() -> GraphView {
        let mut tables = AHashMap::new();
        tables.insert("a".to_string(), create_simple_table("a"));
        tables.insert("b".to_string(), create_simple_table("b"));
        tables.insert("c".to_string(), create_simple_table("c"));

        let edges = vec![
            create_edge("a", "b"),
            create_edge("b", "c"),
            create_edge("c", "a"),
        ];

        GraphView { tables, edges }
    }

    #[test]
    fn test_no_cycles() {
        let view = create_acyclic_view();
        let cycles = find_cycles(&view);
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_self_reference_cycle() {
        let view = create_self_ref_view();
        let cycles = find_cycles(&view);
        assert_eq!(cycles.len(), 1);
        assert!(cycles[0].is_self_reference());
        assert_eq!(cycles[0].tables, vec!["categories"]);
    }

    #[test]
    fn test_multi_table_cycle() {
        let view = create_multi_cycle_view();
        let cycles = find_cycles(&view);
        assert_eq!(cycles.len(), 1);
        assert!(!cycles[0].is_self_reference());
        assert_eq!(cycles[0].tables.len(), 3);
    }

    #[test]
    fn test_cyclic_tables() {
        let view = create_multi_cycle_view();
        let tables = cyclic_tables(&view);
        assert!(tables.contains("a"));
        assert!(tables.contains("b"));
        assert!(tables.contains("c"));
    }
}
