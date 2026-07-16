//! Built-in planner factories.
//!
//! Planners coordinate table-level structure — row counts, parent/child
//! fan-out, and other cross-column decisions — via the
//! [`PlannerFactory`](super::registry::PlannerFactory) /
//! [`CompiledPlanner`](super::registry::CompiledPlanner) traits.
//!
//! This module is intentionally empty for now: no planner is implemented in
//! this phase, so [`ExtensionRegistry::standard`](super::registry::ExtensionRegistry::standard)
//! installs none. The planner catalog is populated by Task 22 (spooling), and
//! the heuristic catalog by Task 20 — only after neutral evidence exists.
