// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic bounded Cascades optimizer core.
//!
//! This module owns the long-term optimizer contracts. It intentionally does
//! not expose the legacy ordered-pass pipeline: adapters may translate bound
//! plans into these types, while all equivalence, property, cost, budget and
//! winner decisions live here.

pub mod budget;
pub mod calibration;
mod catalog_identity;
pub mod column;
pub mod control;
pub mod cost {
    pub use crate::physical::cost::*;
}
pub mod enforcer;
pub mod engine;
pub mod governor;
pub mod grant;
pub mod planner;
pub mod ids {
    pub use crate::physical::identity::*;
}
pub mod memo;
pub mod properties {
    pub use crate::physical::requirements::*;
}
pub mod region;
pub mod rules;
pub mod scalar;
mod scalar_lowering;
pub mod tasks;
pub(crate) use scalar_lowering::BindingCatalog;
pub mod verifier;

pub use budget::{BudgetDecision, SearchBudget, SearchLedger};
pub use calibration::{LocalOperatorWork, MachineCalibrationBundle, OpClassRegistry};
pub use column::{
    ColumnCatalog, ColumnDesc, ColumnOrigin, ColumnVisibility, GroupColumn, GroupSchema,
};
pub use cost::{CompactRange, SearchCost};
pub use engine::{CascadesEngine, GrantOptimization, GrantWinner, SearchMode};
pub use governor::{
    CalibrationScope, EconomicDecision, EconomicSignal, Governor, GovernorSnapshot, GovernorStop,
    PlanMilestone, PlanningPolicy,
};
pub use grant::{GrantSensitivitySummary, GrantSharingProof};
pub use ids::*;
pub use memo::{Memo, OptimizationGoal, Winner};
pub use planner::{
    AlternativeOrigin, LogicalAlternative, MemoBuilder, OptimizationInput, OptimizationOutput,
    OptimizedVariant, ResultPresentation, SearchSummary, GRAPH_REGION_ENUMERATOR_RULE,
    SEARCH_REGION_ENUMERATOR_RULE,
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
pub use tasks::{
    BoundContext, BoundProof, BoundProofId, BoundProofKind, CompletionObligation,
    CompletionObligationId, Cursor, CursorId, EvaluationId, EvaluationKey, InputRevisionId,
    ReadSet, ReadSetId, ReservationId, StopReason, SubproblemKey, TaskId, TaskIntent, TaskIntentId,
    TaskKind, TaskObjectId, TaskOutcome, TaskRegistry, TaskRegistryProfile, TaskRequest, TaskState,
    TaskWakeup,
};
pub use verifier::{MemoVerifier, WinnerVerifier};
