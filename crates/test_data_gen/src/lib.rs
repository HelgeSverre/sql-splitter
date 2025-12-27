//! Test Data Generator for sql-splitter integration tests.
//!
//! Generates deterministic, FK-consistent multi-tenant SQL dumps
//! for testing split, merge, sample, and shard commands.
//!
//! # Example
//!
//! ```rust
//! use test_data_gen::{Generator, Scale, Renderer, RenderConfig};
//!
//! // Generate small dataset with seed for reproducibility
//! let mut gen = Generator::new(42, Scale::Small);
//! let data = gen.generate();
//!
//! // Render to MySQL SQL
//! let renderer = Renderer::new(RenderConfig::mysql());
//! let sql = renderer.render_to_string(&data).unwrap();
//!
//! println!("{}", sql);
//! ```

pub mod fake;
pub mod generator;
pub mod renderer;
pub mod schema;
pub mod streaming;

pub use generator::{GeneratedData, Generator, Scale, SqlValue, TableData};
pub use renderer::{Dialect, RenderConfig, Renderer};
pub use schema::{Column, FkAction, ForeignKey, Schema, SqlType, Table, TableRole};
pub use streaming::{MultiTenantConfig, MultiTenantGenerator, StreamingConfig, StreamingGenerator};
