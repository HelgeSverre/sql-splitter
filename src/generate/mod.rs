//! Synthetic data generation engine.
//!
//! This module hosts the pieces generators are built from: stable, seedable
//! RNG streams ([`seed`]) so a run is fully reproducible from a single root
//! seed, and a dialect-agnostic value representation ([`value`]) that
//! generators produce instead of writing SQL literals directly.

pub mod compiler;
pub mod engine;
pub mod generators;
pub mod plan;
pub mod planners;
pub mod registry;
pub mod seed;
pub mod value;

pub use compiler::{CompileOptions, ModelCompiler, TableCountKind, TableCountOverride};
pub use engine::{
    DenseIntegerKey, GenerateReport, GeneratedRow, GenerationEngine, KeyDomain,
    RandomAccessKeyGenerator, RowSink,
};
pub use generators::ConstantFactory;
pub use plan::{
    ColumnOwner, CompiledOutput, CompiledRelationship, ExecutionPhase, GenerationPlan,
    PlanEstimates, PlannedColumn, PlannedTable, RelationshipDistribution, ResolvedTableSeed,
};
pub use registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, CompiledModifier,
    CompiledPlanner, Determinism, ExtensionRegistry, GeneratorDescriptor, GeneratorFactory,
    ModifierDescriptor, ModifierFactory, PlanContext, PlannerDescriptor, PlannerFactory,
    RowContext, RowView, Verification,
};
pub use seed::{derive_seed, SeedRoot, StreamId};
pub use value::{GenerateError, GeneratedValue};
