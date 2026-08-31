//! Deterministic bounded Cascades optimizer core.
//!
//! This module owns the long-term optimizer contracts. It intentionally does
//! not expose the legacy ordered-pass pipeline: adapters may translate bound
//! plans into these types, while all equivalence, property, cost, budget and
//! winner decisions live here.

pub mod budget;
pub mod cache;
pub mod calibration;
pub mod column;
pub mod cost {
    pub use crate::physical::cost::*;
}
pub mod enforcer;
pub mod engine;
pub mod enumerators;
pub mod estimate;
pub mod external_cost;
pub mod grant;
pub mod memo_builder;
pub mod ids {
    pub use crate::physical::identity::*;
}
pub mod memo;
pub mod progress;
pub mod properties {
    pub use crate::physical::requirements::*;
}
pub mod region;
pub mod rules;
pub mod scalar;
mod scalar_lowering;
pub mod statistics;
pub mod verifier;

pub use budget::{BudgetDecision, SearchBudget, SearchLedger};
pub use cache::{CompileStabilityMode, PlanDependencies, StabilityPolicy};
pub use calibration::{LocalOperatorWork, MachineCalibrationBundle, OpClassRegistry};
pub use column::{
    ColumnCatalog, ColumnDesc, ColumnOrigin, ColumnVisibility, GroupColumn, GroupSchema,
};
pub use cost::{CompactRange, SearchCost};
pub use engine::{CascadesEngine, GrantOptimization, GrantWinner, SearchMode};
pub use enumerators::{JoinRegion, JoinRegionEnumerator};
pub use estimate::{Estimate, UncertaintySet, UncertaintySetArena};
pub use external_cost::{ExternalCallWork, RoutineCostProfile};
pub use grant::{GrantInvarianceProof, GrantSensitivitySummary};
pub use ids::*;
pub use memo::{Memo, OptimizationGoal, Winner};
pub use memo_builder::{
    AlternativeOrigin, LogicalAlternative, MemoBuilder, OptimizationInput, OptimizationOutput,
    OptimizedVariant, ResultPresentation, GRAPH_REGION_ENUMERATOR_RULE,
    JOIN_REGION_ENUMERATOR_RULE, SEARCH_REGION_ENUMERATOR_RULE,
};
pub use properties::{ProvidedProperties, RequiredProperties};
pub use region::{
    FacetCriticality, JointCostProof, RegionArtifactKind, RegionCandidateContract,
    RegionDependencyEdge, RegionDependencyKind, RegionFacet, RegionFacetKind, RegionForest,
    RegionOwnedArtifact,
};
pub use rules::{
    CostComposition, ImplementationRegistry, PhysicalImplementation, TransformationRule,
};
pub use scalar::{ScalarArena, ScalarKind, ScalarNode, ScalarProperties};
pub use statistics::{EstimatorAlgebra, RelationEstimate, StatisticsSnapshot};
pub use verifier::{MemoVerifier, WinnerVerifier};
