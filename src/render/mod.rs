//! Allocation-lean SQL rendering primitives.
//!
//! These types replace the temporary-string-heavy path generators used to
//! take (formatting an escaped copy, then a quoted copy, then joining rows
//! with `Vec<String>`) with borrowed `Display` formatting and reusable
//! buffers. See `docs/superpowers/specs/2026-07-16-gen-fixtures-performance-design.md`
//! for the measurements that motivated this module.

mod ddl;
mod random;
mod row_batch;
mod sql;
mod sql_string;

pub use random::RandomBlock;
pub use row_batch::RowBatch;
pub use sql::{RenderOptions, SqlRenderer};
pub use sql_string::SqlString;
