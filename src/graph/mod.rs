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

pub use analysis::{cyclic_tables, find_cycles, Cycle};
pub use format::{to_dot, to_html, to_json, to_mermaid, Layout, OutputFormat};
pub use view::GraphView;
// Re-export for tests and external use
#[allow(unused_imports)]
pub use view::{ColumnInfo, EdgeInfo, TableInfo};
