//! Built-in planner factories.
//!
//! Planners coordinate table-level structure — row counts, parent/child
//! fan-out, and other cross-column decisions — via the
//! [`PlannerFactory`](super::registry::PlannerFactory) /
//! [`CompiledPlanner`](super::registry::CompiledPlanner) traits.
//!
//! The Phase 3A catalog begins with [`interval::TemporalIntervalFactory`], the
//! same-table `temporal.interval` planner that establishes the planner
//! execution pattern the later family planners reuse.

pub mod interval;

pub use interval::TemporalIntervalFactory;
