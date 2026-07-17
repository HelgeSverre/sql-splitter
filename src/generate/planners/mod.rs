//! Built-in planner factories.
//!
//! Planners coordinate table-level structure — row counts, parent/child
//! fan-out, and other cross-column decisions — via the
//! [`PlannerFactory`](super::registry::PlannerFactory) /
//! [`CompiledPlanner`](super::registry::CompiledPlanner) traits.
//!
//! The Phase 3A catalog begins with [`interval::TemporalIntervalFactory`], the
//! same-table `temporal.interval` planner that establishes the planner
//! execution pattern the later family planners reuse, and continues with
//! [`progress::ProgressCountersFactory`], the `workflow.progress_counters`
//! planner that coordinates a job's lifecycle counters.

pub mod interval;
pub mod progress;

pub use interval::TemporalIntervalFactory;
pub use progress::ProgressCountersFactory;
