// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::lifecycle::bootstrap::InstanceBootstrap;
use crate::lifecycle::recovery::InstanceRecovery;
use crate::{
    ConnectionId, DatabaseStartupStatus, Instance, InstanceBuilder, InstanceConfig,
    InstanceDdlOwner, InstanceLifecycleState, InstanceQuiesceProof, InstanceShutdownMode,
    ManagedConnection,
};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn new_unbootstrapped_in_memory_instance(config: InstanceConfig) -> Arc<Instance> {
    InstanceBuilder::in_memory(config)
        .build_unbootstrapped()
        .expect("unbootstrapped in-memory instance should be constructible")
}

#[test]
fn test_instance_with_custom_config() {
    let mut config = InstanceConfig::in_memory();
    config.options.maximum_threads = Some(4);
    config.options.maximum_memory = 1024 * 1024 * 1024; // 1GB

    let instance = Instance::new_in_memory_with_config(config).unwrap();
    assert_eq!(instance.number_of_threads(), 4);
    assert!(instance.is_in_memory());
}

#[test]
fn test_instance_database_operations() {
    let instance = Instance::new_in_memory();

    let result = instance.create_database("test_db");
    assert!(
        result.is_ok(),
        "Failed to create database: {:?}",
        result.err()
    );

    let db = result.unwrap();
    assert_eq!(db.name(), "test_db");
    assert!(db.is_ready());
    assert!(db.has_storage_manager());

    let retrieved = instance.database_registry().get_database("test_db");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name(), "test_db");

    let drop_result = instance.drop_database("test_db");
    assert!(
        drop_result.is_ok(),
        "Failed to drop database: {:?}",
        drop_result.err()
    );

    let retrieved_after_drop = instance.database_registry().get_database("test_db");
    assert!(retrieved_after_drop.is_none());
}

#[test]
fn test_instance_multiple_databases() {
    let instance = Instance::new_in_memory();

    assert!(instance.create_database("db1").is_ok());
    assert!(instance.create_database("db2").is_ok());
    assert!(instance.create_database("db3").is_ok());

    assert!(instance.database_registry().get_database("db1").is_some());
    assert!(instance.database_registry().get_database("db2").is_some());
    assert!(instance.database_registry().get_database("db3").is_some());

    let count = instance.database_registry().approx_database_count();
    assert!(count >= 3, "Expected at least 3 databases, got {}", count);
}

#[test]
fn test_default_database_is_ready_after_bootstrap() {
    let instance = Instance::new_in_memory();
    let default_db = instance
        .database_registry()
        .get_database("postgres")
        .expect("default database should exist");

    assert!(default_db.is_ready());
    assert!(default_db.has_storage_manager());
}

#[test]
fn test_in_memory_bootstrap_uses_memory_catalog_store_and_startup_report() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_default_database("memdb"),
    )
    .unwrap();

    assert!(
        instance.metadata.layout().is_none(),
        "in-memory instance should not materialize a persistent instance layout"
    );

    let catalog = instance.metadata.load_catalog().unwrap();
    assert_eq!(catalog.databases.len(), 1);
    assert_eq!(catalog.databases[0].name, "memdb");
    assert_eq!(catalog.default_database_id, Some(1));

    let run_state = instance
        .metadata
        .run_state_store()
        .load()
        .expect("in-memory run state should load")
        .expect("bootstrap should persist a run state");
    assert_eq!(run_state.state, InstanceLifecycleState::Running);

    let startup_report = instance.startup_report();
    assert_eq!(startup_report.databases.len(), 1);
    assert_eq!(startup_report.databases[0].name, "memdb");
    assert_eq!(
        startup_report.databases[0].status,
        DatabaseStartupStatus::Recovered
    );
}

#[test]
fn test_system_database_bootstraps_before_user_databases_are_published() {
    let instance = new_unbootstrapped_in_memory_instance(
        InstanceConfig::in_memory().with_default_database("memdb"),
    );

    assert!(
        instance.database_service.registry().system.read().is_none(),
        "manual constructor helper should start before bootstrap"
    );
    assert!(
        instance.database_registry().get_database("memdb").is_none(),
        "managed databases must not be published before bootstrap/recovery"
    );

    InstanceBootstrap::run(&instance).expect("bootstrap should initialize system state");
    assert!(
        instance.database_service.registry().system.read().is_some(),
        "system database must be initialized during bootstrap"
    );
    assert!(
        instance.database_registry().get_database("memdb").is_none(),
        "bootstrap prepares the durable catalog but must not publish user databases yet"
    );

    let catalog = instance.metadata.load_catalog().unwrap();
    assert_eq!(catalog.default_database_id, Some(1));
    assert_eq!(catalog.databases.len(), 1);
    assert_eq!(catalog.databases[0].name, "memdb");

    let startup_report = InstanceRecovery::run(&instance, None)
        .expect("recovery should publish the default managed database");
    *instance.lifecycle.startup_report.write().unwrap() = startup_report;

    assert!(
        instance.database_registry().get_database("memdb").is_some(),
        "managed databases should only become visible after recovery runs"
    );
}

#[test]
fn test_in_memory_database_ddl_updates_memory_catalog_store() {
    let instance = Instance::new_in_memory_with_config(
        InstanceConfig::in_memory().with_default_database("memdb"),
    )
    .unwrap();

    instance
        .create_database("analytics")
        .expect("create database should succeed");
    instance
        .rename_database("analytics", "warehouse")
        .expect("rename database should succeed");

    let mut catalog = instance.metadata.load_catalog().unwrap();
    assert!(catalog.find_database_by_name("analytics").is_none());
    let renamed = catalog
        .find_database_by_name("warehouse")
        .expect("renamed database should be reflected in memory catalog");
    assert_eq!(renamed.name, "warehouse");

    instance
        .drop_database("warehouse")
        .expect("drop database should succeed");
    catalog = instance.metadata.load_catalog().unwrap();

    assert!(catalog.find_database_by_name("warehouse").is_none());
    assert!(
        instance
            .database_registry()
            .get_database("warehouse")
            .is_none(),
        "runtime registry should stay in sync with the memory-backed catalog store"
    );
}

#[test]
fn test_persistent_instance_catalog_and_run_state_share_metadata_store() {
    let dir = tempdir().expect("tempdir");
    let instance = Instance::new(
        InstanceConfig::new().with_instance_root(dir.path().to_string_lossy().to_string()),
    )
    .expect("persistent instance should open");

    let catalog_store = instance
        .metadata
        .catalog_store()
        .durable_store()
        .expect("persistent catalog should use a durable metadata store");
    let run_state_store = instance
        .metadata
        .run_state_store()
        .durable_store()
        .expect("persistent run_state should use a durable metadata store");

    assert!(
        Arc::ptr_eq(catalog_store, run_state_store),
        "persistent instance meta consumers must share the same MetadataStore Arc"
    );
}

struct TestManagedConnection {
    id: ConnectionId,
    active: std::sync::atomic::AtomicBool,
}

impl TestManagedConnection {
    fn new(id: ConnectionId) -> Self {
        Self {
            id,
            active: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl ManagedConnection for TestManagedConnection {
    fn connection_id(&self) -> ConnectionId {
        self.id
    }

    fn is_active(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn description(&self) -> String {
        format!("TestManagedConnection({})", self.id)
    }
}

#[test]
fn test_verify_quiesced_for_clean_shutdown_sets_gate_and_blocks_new_work() {
    let instance = Instance::new_in_memory();

    let _proof = instance
        .verify_quiesced_for_clean_shutdown()
        .expect("instance without tracked work should quiesce");

    assert!(
        instance.create_database("blocked").is_err(),
        "new DDL should be rejected after shutdown gate closes"
    );
    assert!(
        instance.checkpoint().is_err(),
        "instance control-plane ops should fail fast after shutdown gate closes"
    );
}

#[test]
fn test_verify_quiesced_for_clean_shutdown_rejects_active_tracked_connections() {
    let instance = Instance::new_in_memory();
    let connection_id = instance.get_connection_manager().assign_connection_id();
    let connection: Arc<dyn ManagedConnection> =
        Arc::new(TestManagedConnection::new(connection_id));
    instance
        .get_connection_manager()
        .add_connection(Arc::clone(&connection));

    let err = instance
        .verify_quiesced_for_clean_shutdown()
        .expect_err("tracked connections should block clean-shutdown proof");
    assert!(
        err.data()
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("tracked connection"),
        "proof rejection should explain tracked connection drain requirements"
    );
}

#[test]
fn test_shutdown_clean_rejects_mismatched_proof_without_mutating_run_state() {
    let instance = Instance::new_in_memory();
    let initial_run_state = instance
        .metadata
        .run_state_store()
        .load()
        .expect("run state should load")
        .expect("bootstrap should persist Running state");

    let err = instance
        .shutdown_clean(
            InstanceShutdownMode::TryCheckpoint,
            InstanceQuiesceProof {
                boot_id: instance.lifecycle.boot_id.saturating_add(1),
                _private: (),
            },
        )
        .expect_err("mismatched proof must reject clean shutdown");

    assert!(
        err.data()
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("does not match the current boot"),
        "proof mismatch should be surfaced in the shutdown error detail"
    );
    assert!(
        !instance.is_invalidated(),
        "proof validation failure must not invalidate the instance"
    );

    let run_state = instance
        .metadata
        .run_state_store()
        .load()
        .expect("run state should still load")
        .expect("run state should still exist");
    assert_eq!(run_state.state, InstanceLifecycleState::Running);
    assert_eq!(run_state.boot_id, initial_run_state.boot_id);
}

#[test]
fn test_shutdown_waits_for_active_ddl_before_finishing() {
    let instance = Instance::new_in_memory();
    let ddl_guard = instance
        .lock_ddl(InstanceDdlOwner::CreateDatabase)
        .expect("test should acquire DDL guard");

    let cloned = Arc::clone(&instance);
    let (result_tx, result_rx) = mpsc::channel();
    let shutdown_thread = thread::spawn(move || {
        let result = cloned.shutdown_dirty(InstanceShutdownMode::TryCheckpoint);
        result_tx.send(result.map(|_| ())).unwrap();
    });

    thread::sleep(Duration::from_millis(50));
    assert!(
        result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "shutdown should wait while another DDL owner is still holding the lock"
    );

    drop(ddl_guard);

    assert!(
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown result should be delivered once DDL lock is released")
            .is_ok(),
        "shutdown should complete after the in-flight DDL owner exits"
    );
    shutdown_thread.join().unwrap();
}

#[test]
fn test_lock_ddl_rechecks_lifecycle_gate_after_waiting_for_lock() {
    let instance = Instance::new_in_memory();
    let ddl_guard = instance
        .lock_ddl(InstanceDdlOwner::CreateDatabase)
        .expect("test should acquire the first DDL guard");

    let cloned = Arc::clone(&instance);
    let (result_tx, result_rx) = mpsc::channel();
    let blocked_thread = thread::spawn(move || {
        let result = cloned
            .lock_ddl(InstanceDdlOwner::RenameDatabase)
            .map(|_| ());
        result_tx.send(result).unwrap();
    });

    thread::sleep(Duration::from_millis(50));
    let _proof = instance
        .verify_quiesced_for_clean_shutdown()
        .expect("closing the lifecycle gate should succeed");
    drop(ddl_guard);

    let err = result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocked DDL should resolve after the first owner exits")
        .expect_err("second DDL owner must be rejected after shutdown gate closes");
    assert!(
        err.data()
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("shutting down"),
        "post-lock gate recheck should reject work that was queued before shutdown"
    );
    blocked_thread.join().unwrap();
}

#[test]
fn test_instance_wal_lifecycle_metrics_aggregation() {
    let instance = Instance::new_in_memory();
    let default_db = instance
        .database_registry()
        .get_database("postgres")
        .expect("default database should exist");

    default_db.check_wal_health().unwrap();
    default_db.force_checkpoint().unwrap();
    default_db.set_wal_keep_from(0);

    let metrics = instance.wal_lifecycle_metrics();
    assert!(metrics.database_count >= 1);
    assert!(metrics.wal_health_check_total >= 1);
    assert!(metrics.checkpoint_success_total >= 1);
    assert!(metrics.wal_keep_from_pinned_dbs >= 1);
    assert!(metrics.wal_keep_from_keep_all_dbs >= 1);
}

#[test]
fn test_instance_runtime_memory_settings_sync() {
    let instance = Instance::new_in_memory();
    instance.set_memory_limit(2 * 1024 * 1024).unwrap();

    assert_eq!(
        instance.get_buffer_manager().get_max_memory(),
        2 * 1024 * 1024
    );
    assert_eq!(
        instance.runtime_tuning().snapshot().maximum_memory,
        2 * 1024 * 1024
    );
}

#[test]
fn test_instance_runtime_temp_settings_sync() {
    let instance = Instance::new_in_memory();
    let temp_dir = std::env::temp_dir().join("paro_instance_runtime_temp");

    instance
        .set_temporary_directory(temp_dir.to_string_lossy().to_string())
        .unwrap();
    instance
        .set_max_temp_directory_size(Some(128 * 1024))
        .unwrap();

    let runtime_tuning = instance.runtime_tuning().snapshot();
    assert_eq!(
        runtime_tuning.temporary_directory,
        temp_dir.to_string_lossy().to_string()
    );
    assert_eq!(runtime_tuning.max_temp_directory_size, Some(128 * 1024));
    assert!(instance.get_buffer_pool().has_temporary_directory());
    assert_eq!(
        instance.get_buffer_pool().get_swap_limit(),
        Some(128 * 1024)
    );
}
