// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL-style session/front-end routing, prepared state, and transaction-aware execution.

mod active_query;
mod completion;
mod completion_infer;
mod config;
mod copy_metrics;
mod copy_protocol;
mod ddl;
mod dispatch;
mod execute;
mod execution_control;
mod prepared;
mod registered_state;
pub mod result;
mod session;
mod state;
pub mod test_support;
mod transaction;
mod utility;

pub use active_query::{ActiveQueryContext, QueryProgress};
pub use completion::{DiscardCommand, StatementCompletion};
pub use completion_infer::{infer_statement_completion, initial_statement_completion};
pub use config::{ProfilerPrintFormat, ProfilingCoverage, SessionConfig};
pub use copy_metrics::{
    copy_stdin_metrics, CopyStdinMetrics, CopyStdinMetricsSnapshot, CopyStdinRejectReason,
};
pub use copy_protocol::{CopyInSpec, CopyProtocolSink, CopyProtocolSource, ProtocolResultSink};
pub use ddl::SessionDdlBridge;
pub use dispatch::{
    classify_statement, dispatch_statement, utility_command_from_statement, FrontendRoute,
    PreparedCommand, StatementClass, UtilityCommand,
};
pub use execution_control::{
    ActiveStatementControl, ConnectionShutdownReason, SessionExecutionControl,
};
pub use prepared::binary_codec::{
    decode_binary_param, encode_binary_value, is_binary_recv_supported, is_binary_send_supported,
};
pub use prepared::extended_query::{
    BindMessage, CloseTarget, DescribeTarget, ExecutePortalMessage, ExtendedQueryMessage,
    ExtendedQueryResponder, ParseMessage,
};
pub use prepared::plan_cache::PlanCacheMode;
pub use prepared::portal::{
    CursorHoldability, ExecutionCursorHandle, FormatCode, PortalExecutionState, ScrollMode,
};
pub use prepared::store::{
    PortalEntry, PortalKind, PortalStoreMark, PreparedState, PreparedStatementEntry,
    PreparedStatementSource,
};
pub use prepared::typed_parameters::{BoundParameter, TypedParameterEnv};
pub use registered_state::{RegisteredStateManager, SessionContextState};
pub use result::collecting_sink::{CollectedError, CollectedResult, CollectingSink};
pub use result::profiler::{MetricType, ProfileSegment, QueryMetrics, QueryProfiler};
pub use result::progress::{
    AtomicQueryProgress, NoOpProgressBarDisplay, ProgressBar, ProgressBarDisplay,
    TerminalProgressBarDisplay,
};
pub use result::query::QueryResult;
pub use result::sink::ResultSink;
pub use session::{Session, TransactionState};
pub use state::session_state::SessionState;
pub use test_support::TestSessionBuilder;
pub use transaction::block_kind::{BlockKind, SavepointFrame};
pub use transaction::commit::{CommitFailure, CommitOutcome};
pub use transaction::ddl_changes::{CatalogOpBatch, PreparedCatalogOp};
pub use transaction::local_settings::{SettingOverlayChange, TransactionLocalSettings};
pub use transaction::session_transaction::{FrozenTransaction, SessionTransaction};
