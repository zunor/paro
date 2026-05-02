// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Live session state, `StatementContext` integration, and front-end entry points.

use crate::active_query::{ActiveQueryContext, QueryProgress};
use crate::auth_policy::SessionAuthPolicy;
use crate::config::SessionConfig;
use crate::ddl::SessionDdlBridge;
use crate::execution_control::{ConnectionShutdownReason, SessionExecutionControl};
use crate::prepared::store::{parameter_types_to_pg_array, PortalStoreMark};
use crate::registered_state::{RegisteredStateManager, SessionContextState};
use crate::result::retained_store::SessionMemoryBudget;
use crate::state::session_metadata::SharedSessionMetadataState;
use crate::state::session_state::SessionState;
use crate::transaction::block_kind::BlockKind;
use crate::transaction::session_transaction::SessionTransaction;
use crate::utility::settings::{
    collect_setting_rows, initialize_setting_store, reconcile_effective_settings,
};
use paro_catalog::entry::PropertyGraphCatalogEntry;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::search_path::CatalogSearchPath;
use paro_common::allocator::{Allocator, BufferAllocator, MemoryTag};
use paro_common::effect::{GraphDmlTableDelta, PostCommitHookDescriptor};
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_common::version::{pg_compat_server_version, PG_COMPAT_SERVER_VERSION_NUM};
use paro_context::{
    AttachedDatabaseCommitFrontierSnapshot, AttachedDatabaseCommitPoisonSnapshot,
    AttachedDatabaseDirectory, AttachedDatabaseSnapshot,
    AttachedDatabaseTransactionMetricsSnapshot, AttachedDatabaseWalMetricsSnapshot,
    CompileEnvironmentKey, CursorSummary, DatabaseSnapshotIdentity, EffectiveSettings,
    ExecutionResources, PreparedStatementSummary, QueryResources, RuntimeLimits,
    SessionMetadataRows, StatementCancelReason, StatementCancellation, StatementContext,
    StatementEnvironment, StatementOptions, StatementSource, StatementView,
};
use paro_execution::operator::ddl::refresh_property_graph::{
    mark_property_graph_stale, refresh_property_graph_committed,
    schedule_property_graph_background_rebuild,
};
use paro_execution::query_executor::executor::Executor;
use paro_instance::{DatabaseHandle, Instance};
use paro_storage::metrics::storage_metrics;
use paro_transaction::{CommitAckPolicy, DatabaseId, IsolationLevel, ReadTrackingPolicy};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const STARTUP_SERVER_ENCODING: &str = "UTF8";
const STARTUP_CLIENT_ENCODING: &str = "UTF8";
const STARTUP_DATE_STYLE: &str = "ISO, MDY";
const STARTUP_TIME_ZONE: &str = "UTC";
const STARTUP_INTEGER_DATETIMES: &str = "on";
const STARTUP_STANDARD_CONFORMING_STRINGS: &str = "on";
const STARTUP_IS_SUPERUSER: &str = "on";
const MAX_COPY_STDIN_MEMORY_LIMIT: usize = 1024 * 1024 * 1024;

/// Session-scoped state for a single client connection.
pub struct Session {
    /// Unique session identifier
    pub id: u64,
    /// The global database instance root
    pub instance: Arc<Instance>,
    /// The current session configuration
    pub config: SessionConfig,
    /// Resolved effective settings after applying session and transaction-local overlays.
    pub(crate) effective_settings: HashMap<String, Value>,
    /// Session-level state (search_path, user, prepared statements, etc.)
    pub state: SessionState,
    /// Boot-time routine privilege policy derived from the startup environment.
    auth_policy: SessionAuthPolicy,
    /// Shared session-scoped metadata mirror for pg_settings / pg_prepared_statements / pg_cursors.
    pub session_metadata: Arc<SharedSessionMetadataState>,
    /// Data for the currently running transaction
    pub transaction: SessionTransaction,
    /// Highest durable-only async commit from this connection that later reads must observe.
    async_commit_floor: AtomicU64,
    /// Test-only commit acknowledgement override. Production SQL/pgwire commits are
    /// always required-published until an explicit durable-async protocol is designed.
    #[cfg(test)]
    commit_ack_policy: AtomicU64,
    /// Shared execution control separating connection shutdown from statement cancellation.
    execution_control: Arc<SessionExecutionControl>,
    /// The current attached database this session is pointing to
    pub current_database: Arc<DatabaseHandle>,
    /// Per-query execution state owned by the currently running front-end statement.
    active_query: Option<ActiveQueryContext>,
    /// Current query progress
    query_progress: QueryProgress,
    /// Registered state manager for extensible session state
    registered_state: RegisteredStateManager,
    /// Buffered COPY FROM STDIN payload limit used by the transitional bridge.
    copy_stdin_memory_limit: usize,
    /// Session-owned retained result budget for holdable portals/cursors.
    session_memory_budget: Arc<SessionMemoryBudget>,
}

impl Session {
    /// Create a new session attached to an instance.
    pub fn new(id: u64, instance: Arc<Instance>) -> Self {
        Self::with_user(id, instance, "paro")
    }

    pub fn transaction_id(&self) -> u64 {
        self.transaction_writer_id().into_raw()
    }

    pub fn transaction_writer_id(&self) -> paro_transaction::WriterId {
        self.transaction
            .transaction_id()
            .map(paro_transaction::WriterId::new)
            .unwrap_or_else(paro_transaction::WriterId::permanent)
    }

    pub fn transaction_read_ts(&self) -> paro_transaction::ReadTs {
        self.transaction
            .start_time()
            .map(paro_transaction::ReadTs::new)
            .unwrap_or_else(paro_transaction::ReadTs::no_active_transaction)
    }

    pub fn transaction_visible_commit_ts(&self) -> paro_transaction::CommitTs {
        paro_transaction::CommitTs::new(self.transaction_visible_version())
    }

    pub fn transaction_start_time(&self) -> u64 {
        self.transaction_read_ts().into_raw()
    }

    pub fn transaction_visible_version(&self) -> u64 {
        self.transaction.visible_version().unwrap_or_else(|| {
            self.current_database
                .transaction_manager()
                .published_commit_id()
        })
    }

    pub(crate) fn record_async_commit_floor(&self, commit_id: u64) {
        self.async_commit_floor
            .fetch_max(commit_id, Ordering::AcqRel);
    }

    pub(crate) fn commit_ack_policy(&self) -> CommitAckPolicy {
        #[cfg(test)]
        {
            match self.commit_ack_policy.load(Ordering::Acquire) {
                1 => CommitAckPolicy::DurableOnlyAsync,
                _ => CommitAckPolicy::RequiredPublished,
            }
        }
        #[cfg(not(test))]
        {
            CommitAckPolicy::RequiredPublished
        }
    }

    #[cfg(test)]
    pub(crate) fn set_commit_ack_policy_for_tests(&self, policy: CommitAckPolicy) {
        let encoded = match policy {
            CommitAckPolicy::RequiredPublished => 0,
            CommitAckPolicy::DurableOnlyAsync => 1,
        };
        self.commit_ack_policy.store(encoded, Ordering::Release);
    }

    pub(crate) fn wait_for_async_commit_floor_published(&self) -> Result<()> {
        let floor = self.async_commit_floor.load(Ordering::Acquire);
        if floor == 0 {
            return Ok(());
        }

        self.current_database
            .transaction_manager()
            .wait_for_published_commit_id_at_least(floor)?;
        self.async_commit_floor
            .compare_exchange(floor, 0, Ordering::AcqRel, Ordering::Acquire)
            .ok();
        Ok(())
    }

    pub fn active_transaction(&self) -> Option<Arc<paro_storage::transaction::txn::Transaction>> {
        self.transaction.active_transaction().ok()
    }

    pub fn freeze_statement_context(
        &self,
        options: StatementOptions,
        cancellation: StatementCancellation,
    ) -> Arc<StatementContext> {
        let settings = Arc::new(EffectiveSettings::new(self.effective_settings.clone()));
        let runtime_tuning = self.instance.runtime_tuning().snapshot();
        let scheduler_threads = self.instance.get_scheduler().number_of_threads().max(1) as usize;
        let max_threads = settings.threads().unwrap_or(scheduler_threads);
        let max_memory = settings
            .memory_limit()
            .unwrap_or(runtime_tuning.maximum_memory);
        let temp_directory = settings
            .temp_directory()
            .unwrap_or_else(|| runtime_tuning.temporary_directory.clone());
        let max_temp_directory_size = settings
            .max_temp_directory_size()
            .unwrap_or(runtime_tuning.max_temp_directory_size);
        let limits = RuntimeLimits {
            max_threads,
            max_memory,
            use_temporary_directory: !temp_directory.is_empty(),
            temporary_directory: temp_directory.clone(),
            max_temp_directory_size,
            force_external: settings.force_external(),
        };

        let mut databases = self.instance.database_registry().get_databases();
        databases.sort_by(|left, right| left.name().cmp(right.name()));
        let storage_metric_snapshot = storage_metrics().snapshot();
        let databases = databases
            .into_iter()
            .map(|database| {
                let wal_metrics = database.wal_lifecycle_metrics();
                let journal_apply_metrics = database.journal_apply_metrics();
                let commit_frontier = database.commit_frontier_snapshot();
                let commit_poison = database.commit_poison_snapshot();
                let manager_metrics = database.transaction_manager().registry_metrics_snapshot();
                let sequencer_metrics = database.transaction_manager().commit_sequencer_metrics();
                let backpressure_metrics = database
                    .transaction_manager()
                    .commit_backpressure_snapshot();
                let commit_ack_mode = match manager_metrics.txn_commit_ack_mode_last {
                    1 => "durable_only_async",
                    _ => "required_published",
                }
                .to_string();
                AttachedDatabaseSnapshot {
                    identity: DatabaseSnapshotIdentity {
                        id: database.id(),
                        name: database.name().to_string(),
                        path: database.path().to_string(),
                        db_type: database.db_type(),
                    },
                    catalog: database.catalog().clone(),
                    tablet_meta: database.tablet_meta_manager(),
                    wal_metrics: AttachedDatabaseWalMetricsSnapshot {
                        checkpoint_success_total: wal_metrics.checkpoint_success_total,
                        checkpoint_failure_total: wal_metrics.checkpoint_failure_total,
                        wal_health_check_total: wal_metrics.wal_health_check_total,
                        wal_keep_from: wal_metrics.wal_keep_from,
                        recovery_mode: wal_metrics.recovery_mode.as_str().to_string(),
                        main_wal_needs_truncation: wal_metrics.main_wal_needs_truncation,
                        checkpoint_wal_needs_truncation: false,
                        recovery_wal_needs_truncation: false,
                        journal_apply_queue_depth: journal_apply_metrics.queue_depth,
                        journal_apply_queue_depth_peak: journal_apply_metrics.queue_depth_peak,
                        journal_apply_active_workers: journal_apply_metrics.active_workers,
                        journal_apply_active_workers_peak: journal_apply_metrics
                            .active_workers_peak,
                        journal_apply_mailbox_count: journal_apply_metrics.mailbox_count,
                        journal_apply_applied_lag: journal_apply_metrics.applied_lag,
                        journal_apply_published_lag: journal_apply_metrics.published_lag,
                        journal_apply_durable_wait_count: journal_apply_metrics.durable_wait_count,
                        journal_apply_durable_wait_micros: journal_apply_metrics
                            .durable_wait_micros,
                        journal_apply_applied_wait_count: journal_apply_metrics.applied_wait_count,
                        journal_apply_applied_wait_micros: journal_apply_metrics
                            .applied_wait_micros,
                        journal_apply_published_wait_count: journal_apply_metrics
                            .published_wait_count,
                        journal_apply_published_wait_micros: journal_apply_metrics
                            .published_wait_micros,
                        journal_commit_bytes_total: 0,
                        journal_group_count: 0,
                        journal_group_size_last: 0,
                        journal_group_size_peak: 0,
                        journal_sync_latency_micros_total: 0,
                        journal_sync_latency_micros_peak: 0,
                        journal_replay_rowsets_total: 0,
                        journal_replay_delete_patches_total: 0,
                        journal_inline_delete_patch_count: 0,
                        journal_delete_patch_count: 0,
                    },
                    transaction_metrics: AttachedDatabaseTransactionMetricsSnapshot {
                        txn_begin_count: manager_metrics.txn_begin_count,
                        txn_begin_latency_us_total: manager_metrics.txn_begin_latency_us_total,
                        txn_begin_latency_us_peak: manager_metrics.txn_begin_latency_us_peak,
                        txn_commit_count: manager_metrics.txn_commit_count,
                        txn_commit_latency_us_total: manager_metrics.txn_commit_latency_us_total,
                        txn_commit_latency_us_peak: manager_metrics.txn_commit_latency_us_peak,
                        txn_commit_prepare_latency_us_total: manager_metrics
                            .txn_commit_prepare_latency_us_total,
                        txn_commit_prepare_latency_us_peak: manager_metrics
                            .txn_commit_prepare_latency_us_peak,
                        txn_commit_validate_latency_us_total: manager_metrics
                            .txn_commit_validate_latency_us_total,
                        txn_commit_validate_latency_us_peak: manager_metrics
                            .txn_commit_validate_latency_us_peak,
                        group_commit_fence_us_total: sequencer_metrics.fence_duration_us_total,
                        group_commit_fence_us_peak: sequencer_metrics.fence_duration_us_peak,
                        txn_commit_durable_latency_us_total: manager_metrics
                            .txn_commit_durable_latency_us_total,
                        txn_commit_durable_latency_us_peak: manager_metrics
                            .txn_commit_durable_latency_us_peak,
                        commit_required_publish_wait_us_total: manager_metrics
                            .txn_commit_required_publish_wait_us_total,
                        commit_required_publish_wait_us_peak: manager_metrics
                            .txn_commit_required_publish_wait_us_peak,
                        txn_commit_publish_latency_us_total: manager_metrics
                            .txn_commit_publish_latency_us_total,
                        txn_commit_publish_latency_us_peak: manager_metrics
                            .txn_commit_publish_latency_us_peak,
                        commit_ack_mode,
                        write_conflict_index_size: manager_metrics.write_conflict_index_size,
                        write_conflict_index_fine_entries: manager_metrics
                            .write_conflict_index_fine_entries,
                        write_conflict_index_fine_summary_entries: manager_metrics
                            .write_conflict_index_fine_summary_entries,
                        write_conflict_index_coarse_entries: manager_metrics
                            .write_conflict_index_coarse_entries,
                        lock_wait_count: manager_metrics.lock_wait_count,
                        lock_wait_duration_us: manager_metrics.lock_wait_duration_us,
                        lock_wound_wait_abort_count: manager_metrics.lock_wound_wait_abort_count,
                        lock_deadlock_abort_count: manager_metrics.lock_deadlock_abort_count,
                        durable_published_lag_commits: backpressure_metrics.durable_published_lag,
                        durable_published_lag_ms: backpressure_metrics.durable_published_lag_ms,
                        backpressure_throttle_count: backpressure_metrics.throttle_count,
                        ssi_validation_abort_count: manager_metrics.ssi_validation_abort_count,
                        ssi_abort_due_to_coarse_scan_marker: manager_metrics
                            .ssi_abort_due_to_coarse_scan_marker,
                        read_tracker_record_count: manager_metrics.read_tracker_record_count,
                        read_tracker_coarsened_count: manager_metrics.read_tracker_coarsened_count,
                        read_tracking_hint_count: manager_metrics.read_tracking_hint_count,
                        read_tracking_policy_escalation_count: manager_metrics
                            .read_tracking_policy_escalation_count,
                        read_tracking_point_critical_count: manager_metrics
                            .read_tracking_point_critical_count,
                        read_tracking_range_critical_count: manager_metrics
                            .read_tracking_range_critical_count,
                        read_tracking_analytical_scan_count: manager_metrics
                            .read_tracking_analytical_scan_count,
                        read_tracking_safe_snapshot_preferred_count: manager_metrics
                            .read_tracking_safe_snapshot_preferred_count,
                        derived_index_lag_ts: storage_metric_snapshot.derived_index_lag_ts,
                        derived_delta_merge_cost: storage_metric_snapshot.derived_delta_merge_cost,
                        commit_participant_count: backpressure_metrics.participant_count as u64,
                        inflight_batch_conflict_reject_count: sequencer_metrics
                            .reject_in_batch_write_conflict,
                        retention_watermark_lag_ms: manager_metrics.retention_watermark_lag_ms,
                        oldest_active_rw_lag_ms: manager_metrics.oldest_active_rw_lag_ms,
                        read_snapshot_lease_count: manager_metrics.read_snapshot_lease_count,
                        active_rw_txn_count: manager_metrics.active_rw_txn_count,
                    },
                    commit_frontier: AttachedDatabaseCommitFrontierSnapshot {
                        durable_commit_id: commit_frontier.durable_commit_id,
                        published_commit_id: commit_frontier.published_commit_id,
                        durable_commit_bytes: commit_frontier.durable_commit_bytes,
                        published_commit_bytes: commit_frontier.published_commit_bytes,
                        durable_to_published_bytes_lag: commit_frontier
                            .durable_to_published_bytes_lag,
                        stale_bytes_at_poison: commit_frontier.stale_bytes_at_poison,
                        publish_failure_watermark: commit_frontier.publish_failure_watermark,
                        publish_failure_cause: commit_frontier.publish_failure_cause,
                        wait_count: commit_frontier.wait_count,
                        wait_wake_count: commit_frontier.wait_wake_count,
                        notify_all_count: commit_frontier.notify_all_count,
                        notify_suppressed_count: commit_frontier.notify_suppressed_count,
                        publish_failure_count: commit_frontier.publish_failure_count,
                    },
                    commit_poison: AttachedDatabaseCommitPoisonSnapshot {
                        admission_state: commit_poison.admission_state,
                        admission_open: commit_poison.admission_open,
                        poisoned: commit_poison.poisoned,
                        poison_cause: commit_poison.poison_cause,
                        first_blocked_commit_ts: commit_poison.first_blocked_commit_ts,
                    },
                }
            })
            .collect();

        let ddl = if self.has_active_transaction() {
            Some(Arc::new(SessionDdlBridge::new(
                self.current_database.clone(),
                self.transaction.ddl_changes(),
                self.transaction.admission_state(),
                self.transaction.write_guard(),
                self.active_transaction()
                    .expect("active transaction must exist when building DDL bridge"),
                self.transaction_id(),
                self.transaction_start_time(),
            )) as Arc<dyn paro_context::DdlApplyContext>)
        } else {
            None
        };

        let current_user = self.current_user().to_string();
        let auth = self.auth_policy.auth_context_for_user(&current_user);

        Arc::new(StatementContext {
            env: StatementEnvironment {
                current_database: self.current_database.name().to_string(),
                current_schema: self.current_schema().to_string(),
                current_user: current_user.clone(),
                search_path: self.search_path().get().to_vec(),
                auth,
            },
            txn: StatementView {
                transaction: paro_context::TransactionView::new(
                    self.transaction_writer_id(),
                    self.transaction_read_ts(),
                    paro_context::ReadSnapshot::new(
                        paro_transaction::ReadTs::new(self.transaction_visible_version()),
                        self.current_database
                            .transaction_manager()
                            .retention_registry()
                            .lease_read_snapshot(paro_transaction::ReadTs::new(
                                self.transaction_visible_version(),
                            ))
                            .ok()
                            .map(Arc::new),
                    ),
                    self.transaction.isolation_level(),
                    paro_transaction::CommandId::new(self.current_command_id()),
                    self.transaction.read_tracker(),
                    self.transaction.participant_states(),
                ),
                active: self.active_transaction(),
                write_guard: Some(self.transaction.write_guard()),
                admission: Some(self.transaction.admission_state()),
                retention_registry: Some(
                    self.current_database
                        .transaction_manager()
                        .retention_registry()
                        .clone(),
                ),
            },
            ddl,
            settings: settings.clone(),
            options,
            databases: Arc::new(AttachedDatabaseDirectory::new(
                self.instance.database_registry().visible_generation(),
                Some(self.current_database.name().to_string()),
                databases,
            )),
            limits,
            cancellation,
            execution_tracker: self
                .execution_control
                .active_statement()
                .map(|statement| statement as Arc<dyn paro_context::StatementExecutionTracker>),
            services: Arc::new(QueryResources {
                infra: Arc::new(ExecutionResources {
                    scheduler: self.instance.get_scheduler().clone(),
                    buffer_pool: self.instance.get_buffer_pool().clone(),
                    buffer_manager: self.instance.get_buffer_manager().clone(),
                    query_memory_coordinator: Some(self.instance.get_memory_arbitrator().clone()),
                }),
                cast_functions: self.instance.cast_functions().clone(),
                graph_index: self.instance.graph_manager().clone(),
                python_runtime: Some(self.instance.python_runtime().clone()),
                governance: paro_context::QueryResourceGovernance::default(),
                plan_cache: None,
                connection_info: None,
            }),
            graph_registry: self.instance.graph_manager().clone(),
            session_metadata: self.session_metadata.clone(),
        })
    }

    pub fn freeze_query_context(&self) -> Arc<StatementContext> {
        self.freeze_statement_context(
            StatementOptions {
                source: StatementSource::SimpleQuery,
                ..StatementOptions::default()
            },
            self.compile_scope_cancellation(),
        )
    }

    pub fn freeze_internal_statement_context(&self) -> Arc<StatementContext> {
        self.freeze_statement_context(
            StatementOptions {
                source: StatementSource::Internal,
                ..StatementOptions::default()
            },
            self.compile_scope_cancellation(),
        )
    }

    pub fn compile_environment_key(&self) -> CompileEnvironmentKey {
        self.freeze_query_context().compile_environment_key()
    }

    /// Create a new session with a specific user name.
    pub fn with_user(id: u64, instance: Arc<Instance>, user_name: impl Into<String>) -> Self {
        Self::with_user_and_execution_control(
            id,
            instance,
            user_name,
            Arc::new(SessionExecutionControl::new()),
        )
    }

    pub(crate) fn with_user_and_execution_control(
        id: u64,
        instance: Arc<Instance>,
        user_name: impl Into<String>,
        execution_control: Arc<SessionExecutionControl>,
    ) -> Self {
        let current_database = instance
            .database_registry()
            .default_database()
            .expect("Default database must exist");
        let default_db_name = current_database.name().to_string();
        let default_copy_stdin_memory_limit =
            default_copy_stdin_memory_limit(instance.runtime_tuning().snapshot().maximum_memory);

        let user_name = user_name.into();
        let mut session = Self {
            id,
            instance: instance.clone(),
            config: SessionConfig::default(),
            effective_settings: HashMap::new(),
            state: SessionState::new(&default_db_name, &user_name),
            auth_policy: SessionAuthPolicy::from_env(),
            session_metadata: Arc::new(SharedSessionMetadataState::default()),
            transaction: SessionTransaction::new(),
            async_commit_floor: AtomicU64::new(0),
            #[cfg(test)]
            commit_ack_policy: AtomicU64::new(0),
            execution_control,
            current_database,
            active_query: None,
            query_progress: QueryProgress::default(),
            registered_state: RegisteredStateManager::new(),
            copy_stdin_memory_limit: default_copy_stdin_memory_limit,
            session_memory_budget: Arc::new(SessionMemoryBudget::new(
                instance.runtime_tuning().snapshot().maximum_memory,
                instance.get_memory_arbitrator().clone(),
            )),
        };

        tracing::info!(
            target: targets::SESSION,
            session_id = session.id,
            user = %user_name,
            database = %session.current_database.name(),
            "Session created"
        );

        initialize_setting_store(&mut session);
        session.refresh_session_metadata();
        session
    }

    /// Sets a session-level setting and refreshes derived state/metadata.
    pub fn set_session_setting(&mut self, name: &str, value: Value) -> Result<()> {
        self.config.set_setting(name, value);
        reconcile_effective_settings(self)?;
        self.refresh_session_metadata();
        Ok(())
    }

    /// Resets a session-level setting to its default value and refreshes derived state/metadata.
    pub fn reset_session_setting(&mut self, name: &str) -> Result<()> {
        self.config.reset_setting(name);
        reconcile_effective_settings(self)?;
        self.refresh_session_metadata();
        Ok(())
    }

    /// Returns the startup ParameterStatus values advertised to PostgreSQL clients.
    pub fn startup_parameters(&self) -> Vec<(&'static str, String)> {
        vec![
            ("server_version", pg_compat_server_version()),
            (
                "server_version_num",
                PG_COMPAT_SERVER_VERSION_NUM.to_string(),
            ),
            ("server_encoding", STARTUP_SERVER_ENCODING.to_string()),
            ("client_encoding", STARTUP_CLIENT_ENCODING.to_string()),
            ("DateStyle", STARTUP_DATE_STYLE.to_string()),
            ("TimeZone", STARTUP_TIME_ZONE.to_string()),
            ("integer_datetimes", STARTUP_INTEGER_DATETIMES.to_string()),
            (
                "standard_conforming_strings",
                STARTUP_STANDARD_CONFORMING_STRINGS.to_string(),
            ),
            ("is_superuser", STARTUP_IS_SUPERUSER.to_string()),
            ("application_name", self.state.application_name.clone()),
        ]
    }

    pub fn set_current_database(&mut self, database_name: &str) -> Result<()> {
        if self.transaction.has_active_transaction()
            && !self
                .current_database
                .name()
                .eq_ignore_ascii_case(database_name)
        {
            return Err(paro_error::invalid_transaction_state(
                "USE DATABASE cannot change the current database while a transaction is active; use qualified names for cross-database reads"
                    .to_string(),
            ));
        }
        let previous_database = self.current_database.name().to_string();
        let db = self
            .instance
            .database_registry()
            .get_database(database_name)
            .ok_or_else(|| {
                paro_error::catalog(format!("Database \"{}\" does not exist", database_name))
            })?;
        if !db.is_ready() {
            return Err(paro_error::cannot_connect_now().detail(format!(
                "database \"{}\" is not ready: {}",
                database_name,
                db.commit_health_detail()
            )));
        }
        self.current_database = db;
        // Update session state to reflect new database
        self.state.set_current_database(database_name);

        if previous_database == database_name {
            tracing::debug!(
                target: targets::SESSION,
                session_id = self.id,
                database = %database_name,
                "Session database unchanged"
            );
        } else {
            tracing::info!(
                target: targets::SESSION,
                session_id = self.id,
                previous_database = %previous_database,
                database = %database_name,
                "Session database changed"
            );
        }
        reconcile_effective_settings(self)?;
        self.refresh_session_metadata();
        Ok(())
    }

    pub fn reset_session_state(&mut self) {
        tracing::debug!(
            target: targets::SESSION,
            session_id = self.id,
            database = %self.current_database.name(),
            "Session state reset"
        );

        self.config = SessionConfig::default();
        self.state.reset(self.current_database.name());
        self.transaction = SessionTransaction::new();
        if let Some(active) = self.active_query.take() {
            self.execution_control.finish_statement(active.control());
        }
        self.query_progress = QueryProgress::default();
        reconcile_effective_settings(self)
            .expect("builtin session settings must reconcile on session reset");
        self.refresh_session_metadata();
    }

    pub fn execution_control(&self) -> &Arc<SessionExecutionControl> {
        &self.execution_control
    }

    pub fn copy_stdin_memory_limit(&self) -> usize {
        self.copy_stdin_memory_limit
    }

    pub fn set_copy_stdin_memory_limit(&mut self, limit: usize) {
        self.copy_stdin_memory_limit = limit;
    }

    pub fn session_memory_budget(&self) -> Arc<SessionMemoryBudget> {
        self.session_memory_budget.clone()
    }

    pub fn session_retained_bytes(&self) -> usize {
        self.session_memory_budget.retained_bytes()
    }

    pub fn refresh_session_metadata(&mut self) {
        let settings = collect_setting_rows(self);

        let mut prepared_statements = self
            .state
            .prepared
            .statements()
            .map(|entry| PreparedStatementSummary {
                name: entry.name.clone(),
                statement: entry.source_sql.clone(),
                parameter_types: parameter_types_to_pg_array(&entry.parameter_types),
                from_sql: matches!(
                    entry.source,
                    crate::prepared::store::PreparedStatementSource::Sql
                ),
                generic_plans: i64::from(entry.generic_plan.is_some()),
                custom_plans: i64::from(entry.custom_plan_executions),
            })
            .collect::<Vec<_>>();
        prepared_statements.sort_by(|a, b| a.name.cmp(&b.name));

        let mut cursors = self
            .state
            .prepared
            .portals()
            .map(|entry| {
                let retention = entry.snapshot_retention.as_ref();
                let owner = retention.and_then(|retention| retention.owner());
                CursorSummary {
                    name: entry.name.clone(),
                    statement: entry.source_sql.clone(),
                    is_holdable: matches!(
                        entry.holdability,
                        crate::prepared::portal::CursorHoldability::WithHold
                    ),
                    is_binary: entry.result_formats.iter().any(|format| {
                        matches!(format, crate::prepared::portal::FormatCode::Binary)
                    }),
                    is_scrollable: matches!(
                        entry.scroll_mode,
                        crate::prepared::portal::ScrollMode::Scroll
                    ),
                    snapshot_read_ts: retention.map(|retention| retention.read_ts().into_raw()),
                    snapshot_pin_duration_us: retention
                        .and_then(|retention| retention.pin_duration_us()),
                    snapshot_owner_session_id: owner
                        .as_ref()
                        .and_then(|owner| owner.owner_session_id),
                    snapshot_portal_id: owner
                        .as_ref()
                        .and_then(|owner| owner.portal_id.as_ref().map(ToString::to_string)),
                    snapshot_retention_policy: retention
                        .map(|retention| retention.policy().as_str().to_string())
                        .unwrap_or_else(|| "none".to_string()),
                }
            })
            .collect::<Vec<_>>();
        cursors.sort_by(|a, b| a.name.cmp(&b.name));

        self.session_metadata.replace(SessionMetadataRows {
            settings,
            prepared_statements,
            cursors,
        });
    }

    pub(crate) fn clear_protocol_unnamed_objects(&mut self) {
        if self.state.clear_protocol_unnamed_objects() {
            self.refresh_session_metadata();
        }
    }

    pub(crate) fn current_portal_mark(&self) -> PortalStoreMark {
        self.state.prepared.current_portal_mark()
    }

    pub(crate) fn on_transaction_commit_prepared(&mut self) {
        self.state.prepared.on_transaction_commit();
    }

    pub(crate) fn notify_transaction_commit(&mut self) {
        self.registered_state.notify_transaction_commit();
    }

    pub(crate) fn on_transaction_rollback_prepared(&mut self) {
        self.state.prepared.on_transaction_rollback();
    }

    pub(crate) fn notify_transaction_rollback(&mut self, cause: Option<&ParoError>) {
        self.registered_state.notify_transaction_rollback(cause);
    }

    pub(crate) fn on_savepoint_rollback_prepared(&mut self, mark: PortalStoreMark) {
        self.state.prepared.on_savepoint_rollback(mark);
    }

    #[inline]
    pub fn effective_settings(&self) -> &HashMap<String, Value> {
        &self.effective_settings
    }

    #[inline]
    pub fn effective_setting(&self, name: &str) -> Option<&Value> {
        self.effective_settings.get(&name.to_lowercase())
    }

    // ============================================================
    // Active Query Context
    // ============================================================

    /// Begins a new query context.
    ///
    /// This should be called at the start of query execution to track
    /// the currently executing query.
    ///
    /// - `ClientContext::BeginQueryInternal()`
    ///
    /// Creates ActiveQueryContext but NOT the Executor.
    /// The Executor is created later when actually executing (in execute_statement).
    pub fn begin_statement_scope(&mut self, query: &str) {
        let buffer_pool = self.instance.get_buffer_pool();
        storage_metrics().set_memory_usage_snapshot(&buffer_pool.get_memory_usage_info());

        let control = self
            .execution_control
            .begin_statement(self.current_statement_timeout());
        let ctx = ActiveQueryContext::new(query, control);
        self.active_query = Some(ctx);
        self.query_progress.initialize();
        self.registered_state.notify_query_begin();
        tracing::trace!(
            target: targets::QUERY,
            session_id = self.id,
            query,
            "statement scope started"
        );
    }

    /// Ends the current query context.
    ///
    /// This should be called when query execution completes (success or failure).
    ///
    /// - `ClientContext::EndQueryInternal()`
    pub fn finish_statement_scope(&mut self, success: bool) {
        self.registered_state.notify_query_end(None);

        if let Some(ctx) = self.active_query.take() {
            let elapsed = ctx.elapsed();
            self.execution_control.finish_statement(ctx.control());
            tracing::trace!(
                target: targets::QUERY,
                session_id = self.id,
                success,
                elapsed_ms = elapsed.as_millis(),
                "statement scope finished"
            );
        }
        self.query_progress = QueryProgress::default();
    }

    /// Ends the current query context with an error.
    ///
    /// This variant allows passing the actual error to registered states.
    pub fn finish_statement_scope_with_error(&mut self, error: &ParoError) {
        self.registered_state.notify_query_end(Some(error));

        if let Some(ctx) = self.active_query.take() {
            let elapsed = ctx.elapsed();
            self.execution_control.finish_statement(ctx.control());
            tracing::trace!(
                target: targets::QUERY,
                session_id = self.id,
                success = false,
                elapsed_ms = elapsed.as_millis(),
                "statement scope finished with error"
            );
        }
        self.query_progress = QueryProgress::default();
    }

    /// Returns the current query string, if any.
    #[inline]
    pub fn get_current_query(&self) -> Option<&str> {
        self.active_query.as_ref().map(|ctx| ctx.query())
    }

    /// Returns whether there is an active query.
    #[inline]
    pub fn has_active_query(&self) -> bool {
        self.active_query.is_some()
    }

    /// Returns a reference to the current query progress.
    #[inline]
    pub fn get_query_progress(&self) -> &QueryProgress {
        &self.query_progress
    }

    /// Returns a mutable reference to the current query progress.
    #[inline]
    pub fn get_query_progress_mut(&mut self) -> &mut QueryProgress {
        &mut self.query_progress
    }

    /// Updates the query progress.
    pub fn update_query_progress(&mut self, rows_processed: u64, total_rows: u64) {
        self.query_progress.update(rows_processed, total_rows);
    }

    /// Cancels the current query if one is active.
    ///
    /// Requests cancellation of the current active statement.
    pub fn cancel_active_statement(&self) {
        if self
            .execution_control
            .cancel_active_statement(StatementCancelReason::UserRequest)
        {
            tracing::info!(
                target: targets::QUERY,
                session_id = self.id,
                "cancelling active statement"
            );
        }
    }

    pub fn request_connection_shutdown(&self, reason: ConnectionShutdownReason) {
        self.execution_control.request_connection_shutdown(reason);
    }

    pub fn current_statement_cancellation(&self) -> Option<StatementCancellation> {
        self.execution_control
            .active_statement()
            .map(|statement| statement.cancellation())
    }

    pub fn current_statement_execution_attempt(&self) -> Option<StatementCancellation> {
        self.current_statement_cancellation()
            .map(|cancellation| cancellation.child_execution_attempt())
    }

    pub fn compile_scope_cancellation(&self) -> StatementCancellation {
        self.execution_control
            .compile_scope_cancellation(self.current_statement_timeout())
    }

    pub fn check_active_statement_cancellation(&self) -> Result<()> {
        match self.current_statement_cancellation() {
            Some(cancellation) => cancellation.check(),
            None => Ok(()),
        }
    }

    /// Returns a reference to the active query context, if any.
    #[inline]
    pub fn active_query(&self) -> Option<&ActiveQueryContext> {
        self.active_query.as_ref()
    }

    /// Returns a mutable reference to the active query context, if any.
    #[inline]
    pub fn active_query_mut(&mut self) -> Option<&mut ActiveQueryContext> {
        self.active_query.as_mut()
    }

    /// Returns the elapsed time since the current query started.
    ///
    /// Returns `None` if no query is active.
    pub fn query_elapsed(&self) -> Option<std::time::Duration> {
        self.active_query.as_ref().map(|ctx| ctx.elapsed())
    }

    /// Gets the Executor for the currently active query.
    ///
    /// # Panics
    ///
    /// Panics if there is no active query or if the executor has not been initialized.
    ///
    /// The Executor is owned by `ActiveQueryContext` and created when execution starts
    /// (not in `begin_statement_scope()`). This ensures proper cleanup when queries end.
    pub fn get_executor(&self) -> &Executor {
        self.active_query
            .as_ref()
            .expect("No active query")
            .executor()
    }

    /// Gets a mutable reference to the Executor for the currently active query.
    ///
    /// # Panics
    ///
    /// Panics if there is no active query or if the executor has not been initialized.
    pub fn get_executor_mut(&mut self) -> &mut Executor {
        self.active_query
            .as_mut()
            .expect("No active query")
            .executor_mut()
    }

    /// Sets the executor for the current active query.
    ///
    /// This should be called when execution starts, after `begin_statement_scope()`.
    ///
    /// # Panics
    ///
    /// Panics if there is no active query.
    pub fn set_executor(&mut self, executor: Executor) {
        self.active_query
            .as_mut()
            .expect("No active query")
            .set_executor(executor);
    }

    // ============================================================
    // Registered State Management
    // ============================================================

    /// Gets or creates a registered state by type.
    ///
    /// If the state doesn't exist, it will be created using `Default::default()`.
    ///
    /// # Type Parameters
    /// * `T` - The state type, must implement `SessionContextState + Default`
    ///
    /// # Arguments
    /// * `key` - The key to identify the state
    ///
    /// # Returns
    /// The state wrapped in `Arc<Mutex<dyn SessionContextState>>`
    ///
    /// # Example
    /// ```ignore
    /// let cache = session.get_or_create_state::<MyCache>("my_cache");
    /// ```
    pub fn get_or_create_state<T>(&self, key: &str) -> Arc<Mutex<dyn SessionContextState>>
    where
        T: SessionContextState + Default + 'static,
    {
        self.registered_state.get_or_create::<T>(key)
    }

    /// Gets a registered state by key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The state if found, `None` otherwise
    pub fn get_state(&self, key: &str) -> Option<Arc<Mutex<dyn SessionContextState>>> {
        self.registered_state.get(key)
    }

    /// Registers a new state with the given key.
    ///
    /// # Arguments
    /// * `key` - The key to identify the state
    /// * `state` - The state to register
    pub fn register_state<T>(&self, key: &str, state: T)
    where
        T: SessionContextState + 'static,
    {
        self.registered_state.insert(key, state);
    }

    /// Removes a registered state by key.
    ///
    /// # Arguments
    /// * `key` - The key to remove
    ///
    /// # Returns
    /// `true` if the state was removed, `false` if not found
    pub fn remove_state(&self, key: &str) -> bool {
        self.registered_state.remove(key)
    }

    /// Checks if a state exists and is of the expected type.
    ///
    /// # Type Parameters
    /// * `T` - The expected state type
    ///
    /// # Arguments
    /// * `key` - The key to check
    pub fn has_state<T: 'static>(&self, key: &str) -> bool {
        self.registered_state.contains::<T>(key)
    }

    /// Returns the number of registered states.
    pub fn state_count(&self) -> usize {
        self.registered_state.len()
    }

    // ============================================================
    // Session State Accessors
    // ============================================================

    /// Returns the current schema name (first schema in search_path).
    ///
    /// This is equivalent to PostgreSQL's `current_schema()` function.
    #[inline]
    pub fn current_schema(&self) -> &str {
        self.state.current_schema()
    }

    /// Returns the current user name.
    ///
    /// This is equivalent to PostgreSQL's `current_user` / `session_user`.
    #[inline]
    pub fn current_user(&self) -> &str {
        self.state.current_user()
    }

    /// Returns a reference to the catalog search path.
    #[inline]
    pub fn search_path(&self) -> &CatalogSearchPath {
        self.state.search_path()
    }

    /// Returns a mutable reference to the catalog search path.
    #[inline]
    pub fn search_path_mut(&mut self) -> &mut CatalogSearchPath {
        self.state.search_path_mut()
    }

    pub fn set_schema(&mut self, schema: impl Into<String>) -> Result<()> {
        self.set_session_setting("search_path", Value::Varchar(schema.into()))
    }

    /// Returns whether the query has been interrupted.
    #[inline]
    pub fn connection_shutdown_requested(&self) -> bool {
        self.execution_control.connection_shutdown_requested()
    }

    fn current_statement_timeout(&self) -> Option<std::time::Duration> {
        EffectiveSettings::new(self.effective_settings.clone()).statement_timeout()
    }

    // ============================================================
    // Memory Management
    // ============================================================

    /// Get the buffer manager for this session.
    #[inline]
    pub fn buffer_manager(&self) -> &Arc<dyn paro_storage::buffer::BufferManager> {
        self.instance.get_buffer_manager()
    }

    /// Get the shared buffer allocator for this session.
    #[inline]
    pub fn buffer_allocator(&self) -> Arc<dyn Allocator> {
        self.buffer_manager().get_buffer_allocator()
    }

    /// Get the buffer pool for this session.
    ///
    /// The buffer pool is shared across the instance and tracks all memory usage.
    #[inline]
    pub fn buffer_pool(&self) -> &Arc<paro_storage::buffer::BufferPool> {
        self.instance.get_buffer_pool()
    }

    /// Get an allocator for the given memory tag.
    ///
    /// This uses BufferAllocator which integrates with the BufferPool,
    /// allowing proper memory tracking and management.
    pub fn allocator(&self, tag: MemoryTag) -> Arc<dyn Allocator> {
        // If tag is the default Allocator, use the shared one from BufferManager
        if tag == MemoryTag::Allocator {
            self.buffer_allocator()
        } else {
            // Otherwise create a new tagged allocator wrapping the shared BufferManager
            Arc::new(BufferAllocator::new(
                self.buffer_manager().clone() as Arc<dyn paro_common::allocator::BufferManager>,
                tag,
            ))
        }
    }

    // ============================================================
    // Transaction Management
    // ============================================================

    /// Begin an explicit transaction (for BEGIN/START TRANSACTION command).
    ///
    /// This disables auto-commit mode and starts a new transaction.
    /// All subsequent statements will be part of this transaction until
    /// an explicit COMMIT or ROLLBACK.
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin_explicit_transaction(&mut self) -> Result<()> {
        self.begin_explicit_transaction_with_characteristics(None, None)
    }

    pub fn begin_explicit_transaction_with_characteristics(
        &mut self,
        isolation_level: Option<IsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<()> {
        if self.transaction.has_active_transaction() {
            return Err(paro_error::transaction_active());
        }

        self.transaction.begin_explicit_block_with_characteristics(
            self.current_database.transaction_manager(),
            DatabaseId::new(self.current_database.id()),
            self.current_database.name(),
            isolation_level,
            read_only,
        )?;

        self.registered_state.notify_transaction_begin();

        Ok(())
    }

    pub fn set_transaction_characteristics(
        &mut self,
        isolation_level: Option<IsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<()> {
        self.transaction
            .set_transaction_characteristics(isolation_level, read_only)?;
        self.refresh_session_metadata();
        Ok(())
    }

    pub fn set_default_transaction_read_only(&mut self, read_only: bool) {
        self.transaction.set_default_read_only(read_only);
        self.refresh_session_metadata();
    }

    /// Commit the current transaction (for COMMIT command).
    ///
    /// If there is no active transaction, this is a no-op (PostgreSQL compatible).
    ///
    /// After commit, auto-commit mode is re-enabled.
    pub fn commit_transaction(&mut self) -> Result<()> {
        if !self.transaction.has_active_transaction() {
            // PostgreSQL: COMMIT without active transaction is a warning, not an error
            tracing::debug!(target: targets::TRANSACTION, "COMMIT without active transaction - no-op");
            return Ok(());
        }

        self.commit_via_pipeline()
    }

    /// Rollback the current transaction (for ROLLBACK command).
    ///
    /// If there is no active transaction, this is a no-op (PostgreSQL compatible).
    ///
    /// After rollback, auto-commit mode is re-enabled.
    pub fn rollback_transaction(&mut self) -> Result<()> {
        if !self.transaction.has_active_transaction() {
            // PostgreSQL: ROLLBACK without active transaction is a warning, not an error
            tracing::debug!(target: targets::TRANSACTION, "ROLLBACK without active transaction - no-op");
            return Ok(());
        }

        self.rollback_via_pipeline(None)
    }

    // ============================================================
    // Implicit Transaction Block
    // ============================================================

    /// Begin an implicit transaction block for multi-statement execution.
    ///
    /// This is called when the second statement arrives in a multi-statement
    /// request. The implicit block provides atomicity for the entire batch.
    ///
    /// Reference: PostgreSQL `BeginImplicitTransactionBlock()` in
    /// `src/backend/tcop/postgres.c`
    pub fn begin_implicit_transaction_block(&mut self) -> Result<()> {
        let started_new = self
            .transaction
            .begin_implicit_transaction_block_for_database(
                self.current_database.transaction_manager(),
                DatabaseId::new(self.current_database.id()),
                self.current_database.name(),
            )?;

        if started_new {
            self.registered_state.notify_transaction_begin();
        }

        Ok(())
    }

    /// End an implicit transaction block by committing.
    ///
    /// This is called when all statements in a multi-statement request
    /// have been successfully executed.
    ///
    /// Reference: PostgreSQL `EndImplicitTransactionBlock()` in
    /// `src/backend/tcop/postgres.c`
    pub fn end_implicit_transaction_block(&mut self) -> Result<()> {
        if !self.transaction.is_in_implicit_block() {
            return Ok(());
        }
        if !self.transaction.has_active_transaction() {
            self.transaction.clear_transaction();
            return Ok(());
        }

        self.commit_via_pipeline()
    }

    /// Rollback an implicit transaction block.
    ///
    /// This is called when an error occurs during multi-statement execution
    /// within an implicit transaction block.
    pub fn rollback_implicit_transaction(&mut self) -> Result<()> {
        if !self.transaction.is_in_implicit_block() {
            return Ok(());
        }
        if !self.transaction.has_active_transaction() {
            self.transaction.clear_transaction();
            return Ok(());
        }

        self.rollback_via_pipeline(None)
    }

    /// Returns whether we are in an implicit transaction block.
    #[inline]
    pub fn is_in_implicit_block(&self) -> bool {
        self.transaction.is_in_implicit_block()
    }

    /// Returns whether we are in an explicit transaction block.
    #[inline]
    pub fn is_in_explicit_block(&self) -> bool {
        self.transaction.is_in_explicit_block()
    }

    /// Returns whether the transaction is in a failed state.
    ///
    /// When true, all statements except ROLLBACK will return an error.
    #[inline]
    pub fn is_transaction_failed(&self) -> bool {
        self.transaction.is_failed()
    }

    /// Marks the current transaction as failed.
    ///
    /// This is called when an error occurs during statement execution
    /// within an explicit transaction block.
    pub fn set_transaction_failed(&mut self) {
        self.transaction.set_failed();
    }

    /// Returns the transaction block kind.
    #[inline]
    pub fn transaction_block_kind(&self) -> BlockKind {
        self.transaction.block_kind()
    }

    // ============================================================
    // Command Counter
    // ============================================================

    /// Returns the current command ID.
    ///
    /// The command ID is used for visibility control within a transaction.
    #[inline]
    pub fn current_command_id(&self) -> u32 {
        self.transaction.current_command_id()
    }

    /// Increments the command counter.
    ///
    /// This should be called after each non-transaction-control statement
    /// executes successfully. Transaction control statements (BEGIN, COMMIT,
    /// ROLLBACK) do NOT increment the counter.
    ///
    /// Reference: PostgreSQL `CommandCounterIncrement()`
    pub fn command_counter_increment(&mut self) {
        self.transaction.command_counter_increment();
    }

    /// Returns whether there is an active transaction.
    #[inline]
    pub fn has_active_transaction(&self) -> bool {
        self.transaction.has_active_transaction()
    }

    /// Returns whether auto-commit is enabled.
    #[inline]
    pub fn is_auto_commit(&self) -> bool {
        self.transaction.is_auto_commit()
    }

    /// Returns the transaction state for protocol responses (e.g., ReadyForQuery).
    ///
    /// Returns:
    /// - `Idle` ('I'): No transaction is active
    /// - `InTransaction` ('T'): Inside a transaction block
    /// - `Failed` ('E'): In a failed transaction
    pub fn transaction_state(&self) -> TransactionState {
        if self.transaction.is_failed() {
            TransactionState::Failed
        } else if self.transaction.has_active_transaction() {
            TransactionState::InTransaction
        } else {
            TransactionState::Idle
        }
    }

    /// Get a CatalogSnapshot for the current database in this session
    pub fn catalog_txn_view(&self) -> paro_catalog::mvcc::CatalogSnapshot {
        if self.has_active_transaction() {
            paro_catalog::mvcc::CatalogSnapshot::writer(
                self.transaction_id(),
                self.transaction_start_time(),
            )
        } else {
            paro_catalog::mvcc::CatalogSnapshot::read_only(
                self.current_database
                    .transaction_manager()
                    .published_commit_id(),
            )
        }
    }

    // ============================================================
    // Internal Transaction Methods (used by execute.rs)
    // ============================================================

    /// Internal: Begin a transaction (for auto-commit handling).
    ///
    /// This is used by `execute.rs` for automatic transaction management.
    pub(crate) fn begin_transaction_internal(&mut self) -> Result<()> {
        self.wait_for_async_commit_floor_published()?;
        self.transaction.begin_transaction_for_database(
            self.current_database.transaction_manager(),
            DatabaseId::new(self.current_database.id()),
            self.current_database.name(),
            ReadTrackingPolicy::SafeSnapshotPreferred,
        )?;
        self.registered_state.notify_transaction_begin();
        Ok(())
    }

    fn graph_update_hits_columns(
        updated_columns: &std::collections::BTreeSet<u32>,
        graph_columns: &[u32],
    ) -> bool {
        graph_columns
            .iter()
            .any(|column_id| updated_columns.contains(column_id))
    }

    pub(crate) fn apply_post_commit_hooks(
        &self,
        hooks: &[PostCommitHookDescriptor],
        commit_id: u64,
    ) -> Result<()> {
        let mut first_error = None;
        for hook in hooks {
            match hook {
                PostCommitHookDescriptor::GraphDmlMaintenance { deltas } => {
                    if let Err(error) =
                        self.apply_property_graph_dml_hook_descriptors(deltas, commit_id)
                    {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn apply_property_graph_dml_hook_descriptors(
        &self,
        dml_deltas: &[GraphDmlTableDelta],
        commit_id: u64,
    ) -> Result<()> {
        if dml_deltas.is_empty() {
            return Ok(());
        }
        let catalog = self.current_database.catalog().clone();
        let visible_txn = CatalogSnapshot::read_only(commit_id.saturating_add(1));
        let visible_graphs = catalog.scan_property_graphs(&visible_txn);

        let mut graphs_to_stale: HashMap<String, Arc<PropertyGraphCatalogEntry>> = HashMap::new();
        let mut graphs_to_refresh: HashMap<String, Arc<PropertyGraphCatalogEntry>> = HashMap::new();
        let mut first_error = None;

        for delta in dml_deltas {
            let table_oid = delta.table_oid;
            let updated_columns = delta.updated_columns.iter().copied().collect();
            for graph_entry in &visible_graphs {
                let graph_name = graph_entry.info.graph_name.clone();
                let vertex_structural = graph_entry
                    .info
                    .vertex_tables
                    .iter()
                    .find(|vertex| vertex.table_oid == table_oid)
                    .map(|vertex| {
                        delta.inserted > 0
                            || delta.deleted > 0
                            || Self::graph_update_hits_columns(
                                &updated_columns,
                                &vertex.key_column_ids,
                            )
                    })
                    .unwrap_or(false);
                if vertex_structural {
                    graphs_to_refresh.remove(&graph_name);
                    graphs_to_stale.insert(graph_name, Arc::clone(graph_entry));
                    continue;
                }

                let edge_structural = graph_entry
                    .info
                    .edge_tables
                    .iter()
                    .find(|edge| edge.table_oid == table_oid)
                    .map(|edge| {
                        delta.inserted > 0
                            || delta.deleted > 0
                            || Self::graph_update_hits_columns(
                                &updated_columns,
                                &edge.source_key_column_ids,
                            )
                            || Self::graph_update_hits_columns(
                                &updated_columns,
                                &edge.destination_key_column_ids,
                            )
                    })
                    .unwrap_or(false);
                if edge_structural && !graphs_to_stale.contains_key(&graph_name) {
                    graphs_to_refresh.insert(graph_name, Arc::clone(graph_entry));
                }
            }
        }

        for graph_entry in graphs_to_stale.values() {
            let graph_index = self.instance.graph_manager().clone();
            let graph_registry = self.instance.graph_manager().clone();
            if let Err(err) = mark_property_graph_stale(
                catalog.as_ref(),
                graph_index.as_ref(),
                graph_registry.as_ref(),
                graph_entry,
            ) {
                if first_error.is_none() {
                    first_error = Some(err.clone());
                }
                tracing::warn!(
                    target: targets::TRANSACTION,
                    graph = %graph_entry.info.graph_name,
                    error = %err,
                    "Failed to mark property graph stale after structural vertex DML"
                );
            } else {
                schedule_property_graph_background_rebuild(
                    catalog.clone(),
                    graph_index,
                    graph_registry,
                    graph_entry.clone(),
                    commit_id.saturating_add(1),
                );
            }
        }

        for (graph_name, graph_entry) in graphs_to_refresh {
            if graphs_to_stale.contains_key(&graph_name) {
                continue;
            }
            let graph_index = self.instance.graph_manager().clone();
            let graph_registry = self.instance.graph_manager().clone();
            if let Err(err) = refresh_property_graph_committed(
                catalog.clone(),
                graph_index.clone(),
                graph_registry.clone(),
                graph_entry.clone(),
                commit_id.saturating_add(1),
            ) {
                if first_error.is_none() {
                    first_error = Some(err.clone());
                }
                tracing::warn!(
                    target: targets::TRANSACTION,
                    graph = %graph_name,
                    error = %err,
                    "Property graph edge refresh failed after commit; falling back to STALE"
                );
                if let Err(stale_err) = mark_property_graph_stale(
                    catalog.as_ref(),
                    graph_index.as_ref(),
                    graph_registry.as_ref(),
                    &graph_entry,
                ) {
                    if first_error.is_none() {
                        first_error = Some(stale_err.clone());
                    }
                    tracing::warn!(
                        target: targets::TRANSACTION,
                        graph = %graph_name,
                        error = %stale_err,
                        "Failed to mark property graph stale after refresh fallback"
                    );
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn default_copy_stdin_memory_limit(cluster_max_memory: usize) -> usize {
    (cluster_max_memory / 4).min(MAX_COPY_STDIN_MEMORY_LIMIT)
}

/// Transaction state for protocol responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No transaction is active ('I' in PostgreSQL protocol)
    Idle,
    /// Inside a transaction block ('T' in PostgreSQL protocol)
    InTransaction,
    /// In a failed transaction ('E' in PostgreSQL protocol)
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn startup_parameter_map(session: &Session) -> HashMap<&'static str, String> {
        session.startup_parameters().into_iter().collect()
    }

    #[test]
    fn with_user_preserves_startup_identity() {
        let instance = Instance::new_in_memory();
        let session = Session::with_user(7, instance, "alice");

        assert_eq!(session.current_user(), "alice");
        assert_eq!(session.state.current_user(), "alice");
    }

    #[test]
    fn startup_parameters_report_required_defaults() {
        let instance = Instance::new_in_memory();
        let session = Session::new(1, instance);
        let params = startup_parameter_map(&session);

        assert_eq!(
            params.get("server_version").unwrap(),
            &pg_compat_server_version()
        );
        assert_eq!(
            params.get("server_version_num").unwrap(),
            PG_COMPAT_SERVER_VERSION_NUM
        );
        assert_eq!(
            params.get("standard_conforming_strings").unwrap(),
            STARTUP_STANDARD_CONFORMING_STRINGS
        );
        assert_eq!(params.get("is_superuser").unwrap(), STARTUP_IS_SUPERUSER);
        assert_eq!(params.get("application_name").unwrap(), "");
    }

    #[test]
    fn set_session_setting_reconciles_application_name() {
        let instance = Instance::new_in_memory();
        let mut session = Session::with_user(1, instance, "alice");

        session
            .set_session_setting("application_name", Value::Varchar("psql".to_string()))
            .unwrap();

        assert_eq!(session.state.application_name, "psql");
        assert_eq!(
            session.effective_setting("application_name"),
            Some(&Value::Varchar("psql".to_string()))
        );
        assert_eq!(
            startup_parameter_map(&session)
                .get("application_name")
                .unwrap(),
            "psql"
        );

        session.reset_session_setting("application_name").unwrap();
        assert_eq!(session.state.application_name, "");
        assert_eq!(
            startup_parameter_map(&session)
                .get("application_name")
                .unwrap(),
            ""
        );
    }

    #[test]
    fn transaction_id_uses_storage_allocated_writer_id_without_extra_offset() {
        let instance = Instance::new_in_memory();
        let mut session = Session::new(1, instance);

        session.begin_explicit_transaction().unwrap();

        assert_eq!(
            session.transaction_id(),
            paro_transaction::TRANSACTION_ID_START
        );
        assert_eq!(
            session.catalog_txn_view().writer_id(),
            Some(paro_transaction::TRANSACTION_ID_START)
        );

        let statement = session.freeze_statement_context(
            StatementOptions::default(),
            StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None),
        );
        assert_eq!(
            statement.transaction_id(),
            paro_transaction::TRANSACTION_ID_START
        );
        assert_eq!(
            statement.catalog_txn_view().writer_id(),
            Some(paro_transaction::TRANSACTION_ID_START)
        );

        session.rollback_transaction().unwrap();
    }

    #[test]
    fn use_database_cannot_move_active_transaction_to_another_database() {
        let instance = Instance::new_in_memory();
        instance.create_database("analytics").unwrap();
        let mut session = Session::new(1, instance);

        session.begin_explicit_transaction().unwrap();
        let err = session.set_current_database("analytics").unwrap_err();

        assert!(err.to_string().contains("while a transaction is active"));
        assert_ne!(session.current_database.name(), "analytics");
        session.rollback_transaction().unwrap();
    }
}
