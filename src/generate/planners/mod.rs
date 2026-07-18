//! Built-in planner factories.
//!
//! Planners coordinate table-level structure — row counts, parent/child
//! fan-out, and other cross-column decisions — via the
//! [`PlannerFactory`](super::registry::PlannerFactory) /
//! [`CompiledPlanner`](super::registry::CompiledPlanner) traits.
//!
//! The catalog includes same-table temporal and workflow planners, correlated
//! order-family generation, and structural planners for common relational and
//! lifecycle patterns.

pub mod interval;
pub mod order_family;
pub mod progress;
pub mod structural;

pub use interval::TemporalIntervalFactory;
pub use order_family::OrderFamilyFactory;
pub use progress::ProgressCountersFactory;
pub use structural::{
    FileMetadataFactory, GeoCoordinatePairFactory, HierarchyTreeFactory,
    RelationJunctionPairFactory, RelationPolymorphicPairFactory, RelationTenantFamilyFactory,
    TemporalLifecycleFactory, TemporalSoftDeleteFactory, TemporalTimestampsFactory,
};
