// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statement-time context model shared by planning, optimization, and execution.

mod attached_databases;
mod ddl;
mod effective_settings;
mod execution_resources;
mod memory_governance;
mod query_resources;
mod runtime_limits;
mod session_metadata;
mod statement_cancellation;
mod statement_context;
mod statement_environment;
mod statement_options;
mod statement_time;
mod statement_view;
mod txn_admission;
mod write_class;
mod write_guard;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use attached_databases::{
    AttachedDatabaseCommitFrontierSnapshot, AttachedDatabaseCommitPoisonSnapshot,
    AttachedDatabaseDirectory, AttachedDatabaseSnapshot,
    AttachedDatabaseTransactionMetricsSnapshot, AttachedDatabaseWalMetricsSnapshot,
    DatabaseSnapshotIdentity,
};
pub use ddl::{DdlApplyContext, IndexBuildHandle, PreparedIndexArtifact};
pub use effective_settings::EffectiveSettings;
pub use execution_resources::ExecutionResources;
pub use memory_governance::{
    QueryMemoryBudgetSpec, QueryMemoryCoordinator, QueryMemoryRegistration, QueryMemoryTarget,
};
pub use paro_transaction::{
    IndexedReadTracker, NormalizedPredicate, NormalizedPredicateRead, NormalizedPredicateTerm,
    PredicateAtom, PredicateBound, PredicateExpr, PredicateFallbackScope, PredicateNormalizer,
    PredicateValue, ReadDependency, ReadDependencyIndex, ReadDependencyIndexOptions, ReadRecorder,
    ReadSnapshot, ReadTrackerHandle, ReadTrackerSavepointMark, ReadTrackingPolicy,
    RecordingReadTracker, TransactionView,
};
pub use query_resources::{
    ConnectionInfoProvider, ConnectionInfoSnapshot, GraphIndexProvider, GraphRegistry,
    QueryResourceGovernance, QueryResources, SharedPlanCacheHandle,
};
pub use runtime_limits::RuntimeLimits;
pub use session_metadata::{
    CursorSummary, PreparedStatementSummary, SessionMetadataProvider, SessionMetadataRows,
    SettingRow,
};
pub use statement_cancellation::{
    NoopStatementTimeoutDriver, StatementCancelReason, StatementCancellation,
    StatementTimeoutDriver,
};
pub use statement_context::{CompileEnvironmentKey, StatementContext};
pub use statement_environment::{StatementAuthContext, StatementEnvironment};
pub use statement_options::{ExplainOutputType, StatementOptions, StatementSource};
pub use statement_time::StatementTimeContext;
pub use statement_view::StatementView;
pub use txn_admission::{
    catalog_object_resource, ddl_lock_requests, dml_table_lock_requests, schema_resource,
    table_resource, CatalogEffect, DdlExecutionProfile, MixedDmlPolicy, PendingDdlAdmission,
    RuntimeEffect, TxnAdmissionState,
};
pub use write_class::WriteClass;
pub use write_guard::{WriteDatabase, WriteGuard, WriteGuardMark};

#[cfg(any(test, feature = "test-support"))]
pub use test_support::TestStatementContextBuilder;
