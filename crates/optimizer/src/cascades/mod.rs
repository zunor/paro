// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic bounded Cascades optimizer core.
//!
//! Explicit alternative to staged planning. Memo equivalence, task lifecycles
//! and candidate archives live here; plan contracts, binding catalogs and cost
//! equations have strategy-independent owners. The default driver does not
//! enter this engine or silently fall back to it.

pub mod bounds;
pub mod budget;
pub mod control;
pub mod cost {
    pub use crate::physical::cost::*;
}
pub mod engine;
pub mod governor;
pub mod grant;
pub mod ids;
pub mod memo;
pub mod oracle;
pub mod planner;
pub mod properties;
pub mod quality;
pub mod region;
pub mod rules;
pub mod scalar;
pub(crate) mod scalar_lowering;
pub mod tasks;
pub(crate) use crate::binding::BindingCatalog;
pub mod verifier;

pub use crate::binding::column::{
    ColumnCatalog, ColumnDesc, ColumnOrigin, ColumnVisibility, GroupColumn, GroupSchema,
};
pub use crate::cost::calibration::{LocalOperatorWork, MachineCalibrationBundle, OpClassRegistry};
pub use budget::{BudgetDecision, SearchBudget, SearchLedger};
pub use cost::{CompactRange, SearchCost};
pub use engine::{
    CascadesEngine, CostContext, DiagnosticStopReason, GrantOptimization, GrantWinner,
    PricedIncumbent, SearchMode, SearchStop, SearchStopReason, SeedPlan,
};
pub use governor::{
    CalibrationScope, EconomicDecision, EconomicSignal, Governor, GovernorSnapshot, GovernorStop,
    PlanMilestone, PlanningPolicy,
};
pub use grant::{GrantSensitivitySummary, GrantSharingProof};
pub use ids::*;
pub use memo::{
    ContinuationContract, FrozenCandidate, Memo, OptimizationContext, OptimizationGoal,
    OptimizationPhase, SharedOwnership, Winner,
};
pub use planner::{
    AlternativeOrigin, LogicalAlternative, MemoBuilder, OptimizationInput, OptimizationOutput,
    OptimizedVariant, ResultPresentation, SearchSummary, GRAPH_REGION_ENUMERATOR_RULE,
    SEARCH_REGION_ENUMERATOR_RULE,
};
pub use properties::{ProvidedProperties, RequiredProperties};
pub use quality::{
    AggregateRegionWitness, BundleCapability, BundleFact, BundleId, BundleInput, BundleResult,
    BundleState, PReadyCertificate, QualityBundleRegistry, QualityBundleSpec,
};
pub use region::{
    FacetCriticality, JointCostProof, RegionArtifactKind, RegionCandidateContract,
    RegionDependencyEdge, RegionDependencyKind, RegionFacet, RegionFacetKind, RegionForest,
    RegionOwnedArtifact,
};
pub use rules::{
    CostComposition, ImplementationRegistry, PhysicalImplementation, TransformationRule,
};
pub use scalar::{ScalarArena, ScalarKind, ScalarNode, ScalarProperties};
pub use tasks::{
    BoundContext, BoundProof, BoundProofId, BoundProofKind, CompletionObligation,
    CompletionObligationId, Cursor, CursorId, EvaluationId, EvaluationKey, InputRevisionId,
    ReadSet, ReadSetId, ReservationId, StopReason, SubproblemKey, TaskId, TaskIntent, TaskIntentId,
    TaskKind, TaskKindProfile, TaskObjectId, TaskOutcome, TaskRegistry, TaskRegistryProfile,
    TaskRequest, TaskState, TaskWakeup,
};
pub use verifier::{MemoVerifier, WinnerVerifier};
