//! Synthetic data generation engine.
//!
//! This module hosts the pieces generators are built from: stable, seedable
//! RNG streams ([`seed`]) so a run is fully reproducible from a single root
//! seed, and a dialect-agnostic value representation ([`value`]) that
//! generators produce instead of writing SQL literals directly.

pub mod seed;
pub mod value;

pub use seed::{derive_seed, SeedRoot, StreamId};
pub use value::{GenerateError, GeneratedValue};
