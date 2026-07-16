//! Portable schema representation for synthetic data generation.
//!
//! [`crate::schema::Schema`] is a *runtime* model shaped around DDL parsing:
//! it carries dialect quirks, resolved foreign key IDs, and mutable state
//! that only matters while a dump is being read. Generation stages need
//! something calmer — a dialect-agnostic, serializable snapshot of "what a
//! table looks like" that can be written to disk, diffed, or hand-edited
//! between a schema-inspection pass and a generation pass.
//!
//! [`schema::PortableSchema`] is that snapshot. Build one from a parsed
//! [`crate::schema::Schema`] with [`schema::PortableSchema::from_runtime`].

pub mod schema;

pub use schema::{PortableColumn, PortableSchema, PortableTable, SqlTypeFamily};
