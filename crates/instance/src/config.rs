// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Configuration for an instance and its built-in registries.

use crate::database::handle::AccessMode;
use crate::database::hooks::RecoveryHook;
use crate::database::registry::DatabaseFilePathManager;
use crate::lifecycle::startup_report::StartupPolicy;
use paro_function::scalar::cast::CastFunctionSet;
use paro_scheduler::scheduler::ThreadAffinityMode;
use paro_storage::buffer::{BufferManager, BufferPool};
use paro_storage::compaction::compaction_manager::CompactionAdmissionPolicy;
use paro_storage::index::hnsw::HnswIntegritySchedulerConfig;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Collation binding for string comparisons.
#[derive(Debug)]
pub struct CollationBinding {
    /// Registered collation callbacks
    collations: Mutex<Vec<String>>,
}

impl CollationBinding {
    /// Create a new CollationBinding with default collations.
    pub fn new() -> Self {
        Self {
            collations: Mutex::new(Vec::new()),
        }
    }

    /// Register a collation.
    pub fn register_collation(&self, name: String) {
        let mut collations = self.collations.lock().unwrap();
        if !collations.contains(&name) {
            collations.push(name);
        }
    }

    /// Check if a collation is registered.
    pub fn has_collation(&self, name: &str) -> bool {
        let collations = self.collations.lock().unwrap();
        collations.iter().any(|c| c.eq_ignore_ascii_case(name))
    }

    /// Get all registered collations.
    pub fn get_collations(&self) -> Vec<String> {
        let collations = self.collations.lock().unwrap();
        collations.clone()
    }
}

impl Default for CollationBinding {
    fn default() -> Self {
        Self::new()
    }
}

/// Set of registered index types.
#[derive(Debug)]
pub struct IndexTypeSet {
    /// Registered index types (name -> type info)
    index_types: Mutex<HashMap<String, String>>,
}

impl IndexTypeSet {
    /// Create a new IndexTypeSet with default index types.
    pub fn new() -> Self {
        let mut index_types = HashMap::new();
        index_types.insert("ART".to_string(), "Adaptive Radix Tree".to_string());
        index_types.insert("BTREE".to_string(), "B-Tree Index".to_string());

        Self {
            index_types: Mutex::new(index_types),
        }
    }

    /// Register an index type.
    pub fn register_index_type(&self, name: String, description: String) {
        let mut index_types = self.index_types.lock().unwrap();
        index_types.insert(name.to_uppercase(), description);
    }

    /// Find an index type by name.
    pub fn find_by_name(&self, name: &str) -> Option<String> {
        let index_types = self.index_types.lock().unwrap();
        index_types.get(&name.to_uppercase()).cloned()
    }

    /// Get all registered index types.
    pub fn get_index_types(&self) -> Vec<String> {
        let index_types = self.index_types.lock().unwrap();
        index_types.keys().cloned().collect()
    }
}

impl Default for IndexTypeSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Runtime checkpoint scheduling and retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointConfigOptions {
    /// Trigger bytes threshold for automatic checkpoint scheduling.
    pub trigger_bytes: u64,
    /// Wall-clock interval between automatic checkpoint attempts.
    pub trigger_interval: Duration,
    /// Exact-prefix drain timeout for one checkpoint attempt.
    pub drain_timeout: Duration,
    /// Maximum number of concurrent bundle serialization workers.
    pub max_concurrent_writers: usize,
    /// Artifact GC per-pass batch size.
    pub artifact_gc_batch_size: usize,
    /// Artifact GC total delete budget per sweep.
    pub artifact_gc_delete_budget: usize,
    /// Committed checkpoint bundle delete budget per sweep.
    pub checkpoint_gc_delete_budget: usize,
    /// Segment prune delete budget per sweep.
    pub segment_prune_delete_budget: usize,
}

impl Default for CheckpointConfigOptions {
    fn default() -> Self {
        Self {
            trigger_bytes: 1 << 24, // 16 MiB
            trigger_interval: Duration::from_secs(300),
            drain_timeout: Duration::from_secs(30),
            max_concurrent_writers: 4,
            artifact_gc_batch_size: 64,
            artifact_gc_delete_budget: 256,
            checkpoint_gc_delete_budget: 8,
            segment_prune_delete_budget: 32,
        }
    }
}

/// Instance-level compaction resource and foreground-admission policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionConfigOptions {
    pub max_concurrency: usize,
    pub admission: CompactionAdmissionPolicy,
}

impl Default for CompactionConfigOptions {
    fn default() -> Self {
        Self {
            // Reserve foreground capacity until maintenance becomes a
            // cooperatively sliced scheduler workload.
            max_concurrency: 1,
            admission: CompactionAdmissionPolicy::default(),
        }
    }
}

/// Immutable boot-time configuration shared by the whole instance.
#[derive(Debug)]
pub struct BootConfig {
    pub instance_root: String,
    pub default_database: String,
    pub startup_policy: StartupPolicy,
    pub delete_patch_inline_row_ref_threshold: usize,
    pub cast_functions: Arc<CastFunctionSet>,
    pub collation_bindings: Arc<CollationBinding>,
    pub index_types: Arc<IndexTypeSet>,
    pub buffer_pool: Arc<BufferPool>,
    pub buffer_manager_override: Option<Arc<dyn BufferManager>>,
    pub path_manager: Option<Arc<DatabaseFilePathManager>>,
    pub initial_maximum_memory: usize,
    pub initial_maximum_threads: Option<usize>,
    pub pin_threads: ThreadAffinityMode,
    pub checkpoint: CheckpointConfigOptions,
    pub compaction: CompactionConfigOptions,
    pub hnsw_integrity: HnswIntegritySchedulerConfig,
    pub initial_temporary_directory: String,
    pub initial_use_temporary_directory: bool,
    pub initial_max_temp_directory_size: Option<usize>,
}

impl BootConfig {
    pub fn is_in_memory(&self) -> bool {
        self.instance_root == ":memory:"
    }

    pub fn default_database_name(&self) -> &str {
        if self.default_database.is_empty() {
            "postgres"
        } else {
            &self.default_database
        }
    }

    pub fn effective_max_threads(&self) -> usize {
        self.initial_maximum_threads
            .unwrap_or_else(InstanceConfigOptions::get_system_max_threads)
    }

    pub(crate) fn from_config(config: &InstanceConfig) -> Self {
        Self {
            instance_root: config.options.instance_root.clone(),
            default_database: config.options.default_database.clone(),
            startup_policy: config.options.startup_policy,
            delete_patch_inline_row_ref_threshold: config
                .options
                .delete_patch_inline_row_ref_threshold,
            cast_functions: Arc::clone(&config.cast_functions),
            collation_bindings: Arc::clone(&config.collation_bindings),
            index_types: Arc::clone(&config.index_types),
            buffer_pool: Arc::clone(&config.buffer_pool),
            buffer_manager_override: config.buffer_manager.clone(),
            path_manager: config.path_manager.clone(),
            initial_maximum_memory: config.options.maximum_memory,
            initial_maximum_threads: config.options.maximum_threads,
            pin_threads: config.options.pin_threads,
            checkpoint: config.options.checkpoint,
            compaction: config.options.compaction,
            hnsw_integrity: config.options.hnsw_integrity,
            initial_temporary_directory: config.options.temporary_directory.clone(),
            initial_use_temporary_directory: config.options.use_temporary_directory,
            initial_max_temp_directory_size: config.options.max_temp_directory_size,
        }
    }
}

/// Configuration options for the instance runtime.
#[derive(Debug, Clone)]
pub struct InstanceConfigOptions {
    /// Root directory that owns `instance/` and `databases/`.
    ///
    /// This is instance-level state rather than a single database path.
    /// `:memory:` keeps the existing in-memory mode semantics.
    pub instance_root: String,
    /// Access mode of the database (ReadOnly or ReadWrite).
    pub access_mode: AccessMode,
    /// Checkpoint runtime coordination policy.
    pub checkpoint: CheckpointConfigOptions,
    /// Background compaction scheduling and debt relief policy.
    pub compaction: CompactionConfigOptions,
    /// Optional whole-artifact HNSW authentication policy.
    ///
    /// Lazy range checks remain mandatory and are not controlled by this
    /// setting. This policy only governs background residency work.
    pub hnsw_integrity: HnswIntegritySchedulerConfig,
    /// Maximum memory used by the database system (in bytes).
    pub maximum_memory: usize,
    /// Maximum threads used by the database system.
    pub maximum_threads: Option<usize>,
    /// Whether worker threads should be pinned to CPU cores.
    ///
    pub pin_threads: ThreadAffinityMode,
    /// Whether to use a temporary directory for intermediates.
    pub use_temporary_directory: bool,
    /// Directory to store temporary structures.
    pub temporary_directory: String,
    /// Maximum spill size for temporary directory (`None` means unlimited).
    pub max_temp_directory_size: Option<usize>,
    /// Inline row-ref cutoff for delete patch encoding.
    pub delete_patch_inline_row_ref_threshold: usize,
    /// Whether to enable external access (file system, network).
    pub enable_external_access: bool,
    /// Default database name.
    pub default_database: String,
    /// Startup behavior when a durable database cannot be recovered.
    pub startup_policy: StartupPolicy,
}

impl InstanceConfigOptions {
    /// Get the system's maximum thread count.
    ///
    /// Returns the number of available CPU cores, with a minimum of 1.
    pub fn get_system_max_threads() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1)
    }

    /// Get the effective maximum threads.
    ///
    /// Returns the configured value if set, otherwise returns system CPU count.
    pub fn effective_max_threads(&self) -> usize {
        self.maximum_threads
            .unwrap_or_else(Self::get_system_max_threads)
    }
}

impl Default for InstanceConfigOptions {
    fn default() -> Self {
        Self {
            instance_root: String::new(),
            access_mode: AccessMode::ReadWrite,
            checkpoint: CheckpointConfigOptions::default(),
            compaction: CompactionConfigOptions::default(),
            hnsw_integrity: HnswIntegritySchedulerConfig::default(),
            maximum_memory: 1024 * 1024 * 1024, // 1GB
            // None means "use system default" which is resolved in effective_max_threads()
            maximum_threads: None,
            pin_threads: ThreadAffinityMode::Auto,
            use_temporary_directory: true,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            delete_patch_inline_row_ref_threshold: 256,
            enable_external_access: true,
            default_database: "postgres".to_string(),
            startup_policy: StartupPolicy::Strict,
        }
    }
}

impl InstanceConfigOptions {
    /// Create options for in-memory mode.
    ///
    /// Note: For in-memory mode used in tests, we default to 1 thread for determinism.
    /// Production in-memory databases should use the default (system CPU count).
    pub fn in_memory() -> Self {
        Self {
            instance_root: ":memory:".to_string(),
            maximum_memory: 1024 * 1024, // 1MB for tests
            // For test in-memory mode, use 1 thread for determinism
            // Production code should use None (system default)
            maximum_threads: Some(1),
            pin_threads: ThreadAffinityMode::Auto,
            use_temporary_directory: false,
            enable_external_access: false,
            ..Default::default()
        }
    }

    /// Check if this is an in-memory database.
    pub fn is_in_memory(&self) -> bool {
        self.instance_root == ":memory:"
    }
}

/// Configuration for a Paro Instance.
///
/// Contains CastFunctionSet, CollationBinding, IndexTypeSet.
pub struct InstanceConfig {
    /// Configuration options.
    pub options: InstanceConfigOptions,
    /// Cast function set for type conversions.
    ///
    pub cast_functions: Arc<CastFunctionSet>,
    /// Collation binding for string comparisons.
    ///
    pub collation_bindings: Arc<CollationBinding>,
    /// Set of registered index types.
    ///
    pub index_types: Arc<IndexTypeSet>,
    /// Global shared buffer pool.
    pub buffer_pool: Arc<BufferPool>,
    /// Optional custom buffer manager.
    pub buffer_manager: Option<Arc<dyn BufferManager>>,
    /// Reference to the database file path manager.
    pub path_manager: Option<Arc<DatabaseFilePathManager>>,
    /// Optional override for the startup recovery hook pipeline.
    recovery_hooks: Option<Vec<Arc<dyn RecoveryHook>>>,
}

impl std::fmt::Debug for InstanceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceConfig")
            .field("options", &self.options)
            .field("cast_functions", &self.cast_functions)
            .field("collation_bindings", &self.collation_bindings)
            .field("index_types", &self.index_types)
            .field("buffer_pool", &self.buffer_pool)
            .field("has_buffer_manager", &self.buffer_manager.is_some())
            .field("has_path_manager", &self.path_manager.is_some())
            .field(
                "recovery_hook_count",
                &self
                    .recovery_hooks
                    .as_ref()
                    .map(|hooks| hooks.len())
                    .unwrap_or(0),
            )
            .finish()
    }
}

impl Default for InstanceConfig {
    fn default() -> Self {
        let mut cast_functions = CastFunctionSet::new();
        crate::builtin::casts::BuiltinCasts::register_all(&mut cast_functions);
        let options = InstanceConfigOptions::default();
        let buffer_pool = Arc::new(BufferPool::new(options.maximum_memory));
        buffer_pool.set_weak_self(Arc::downgrade(&buffer_pool));

        Self {
            options,
            cast_functions: Arc::new(cast_functions),
            collation_bindings: Arc::new(CollationBinding::new()),
            index_types: Arc::new(IndexTypeSet::new()),
            buffer_pool,
            buffer_manager: None,
            path_manager: None,
            recovery_hooks: None,
        }
    }
}

impl InstanceConfig {
    /// Create a new InstanceConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config for in-memory mode.
    pub fn in_memory() -> Self {
        let mut config = Self::default();
        config.options = InstanceConfigOptions::in_memory();
        config
    }

    /// Set the maximum memory for the buffer pool.
    pub fn with_max_memory(mut self, max_memory: usize) -> Self {
        self.options.maximum_memory = max_memory;
        let buffer_pool = Arc::new(BufferPool::new(max_memory));
        buffer_pool.set_weak_self(Arc::downgrade(&buffer_pool));
        self.buffer_pool = buffer_pool;
        self
    }

    /// Set the number of threads for the task scheduler.
    pub fn with_num_threads(mut self, num_threads: usize) -> Self {
        self.options.maximum_threads = Some(num_threads);
        self
    }

    /// Set the instance-owned HNSW background authentication policy.
    pub fn with_hnsw_integrity_scheduler(mut self, policy: HnswIntegritySchedulerConfig) -> Self {
        self.options.hnsw_integrity = policy;
        self
    }

    /// Set worker thread affinity mode.
    pub fn with_thread_affinity_mode(mut self, mode: ThreadAffinityMode) -> Self {
        self.options.pin_threads = mode;
        self
    }

    /// Set the default database name.
    pub fn with_default_database(mut self, name: impl Into<String>) -> Self {
        self.options.default_database = name.into();
        self
    }

    /// Set the persistent instance root.
    pub fn with_instance_root(mut self, instance_root: impl Into<String>) -> Self {
        self.options.instance_root = instance_root.into();
        self
    }

    /// Set the startup policy.
    pub fn with_startup_policy(mut self, policy: StartupPolicy) -> Self {
        self.options.startup_policy = policy;
        self
    }

    /// Set the access mode.
    pub fn with_access_mode(mut self, mode: AccessMode) -> Self {
        self.options.access_mode = mode;
        self
    }

    /// Set a custom cast function set.
    pub fn with_cast_functions(mut self, cast_functions: Arc<CastFunctionSet>) -> Self {
        self.cast_functions = cast_functions;
        self
    }

    /// Set the path manager.
    pub fn with_path_manager(mut self, path_manager: Arc<DatabaseFilePathManager>) -> Self {
        self.path_manager = Some(path_manager);
        self
    }

    /// Override the startup recovery hook pipeline.
    pub fn with_recovery_hooks(mut self, recovery_hooks: Vec<Arc<dyn RecoveryHook>>) -> Self {
        self.recovery_hooks = Some(recovery_hooks);
        self
    }

    /// Get a reference to the cast functions.
    pub fn get_cast_functions(&self) -> &Arc<CastFunctionSet> {
        &self.cast_functions
    }

    /// Set the buffer pool.
    pub fn with_buffer_pool(mut self, buffer_pool: Arc<BufferPool>) -> Self {
        self.buffer_pool = buffer_pool;
        self
    }

    /// Set a custom buffer manager.
    pub fn with_buffer_manager(mut self, buffer_manager: Arc<dyn BufferManager>) -> Self {
        self.buffer_manager = Some(buffer_manager);
        self
    }

    /// Set the temporary directory.
    pub fn with_temporary_directory(mut self, path: impl Into<String>) -> Self {
        self.options.temporary_directory = path.into();
        self.options.use_temporary_directory = true;
        self
    }

    pub fn with_delete_patch_inline_row_ref_threshold(mut self, threshold: usize) -> Self {
        self.options.delete_patch_inline_row_ref_threshold = threshold;
        self
    }

    /// Get a reference to the collation bindings.

    /// Get a reference to the collation bindings.
    ///
    pub fn get_collation_bindings(&self) -> &Arc<CollationBinding> {
        &self.collation_bindings
    }

    /// Get a reference to the index types.
    ///
    pub fn get_index_types(&self) -> &Arc<IndexTypeSet> {
        &self.index_types
    }

    /// Check if this is an in-memory database.
    pub fn is_in_memory(&self) -> bool {
        self.options.is_in_memory()
    }

    pub(crate) fn take_recovery_hooks(&mut self) -> Option<Vec<Arc<dyn RecoveryHook>>> {
        self.recovery_hooks.take()
    }
}

impl From<&paro_common::config::ClusterConfig> for InstanceConfig {
    fn from(config: &paro_common::config::ClusterConfig) -> Self {
        let options = InstanceConfigOptions {
            instance_root: String::new(),
            access_mode: config.access_mode.into(),
            checkpoint: CheckpointConfigOptions::default(),
            compaction: CompactionConfigOptions::default(),
            hnsw_integrity: HnswIntegritySchedulerConfig::default(),
            maximum_memory: config.max_memory,
            maximum_threads: config.num_threads,
            pin_threads: match config.pin_threads {
                paro_common::config::ThreadPinMode::Off => ThreadAffinityMode::Off,
                paro_common::config::ThreadPinMode::On => ThreadAffinityMode::On,
                paro_common::config::ThreadPinMode::Auto => ThreadAffinityMode::Auto,
            },
            use_temporary_directory: true,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            delete_patch_inline_row_ref_threshold: config.delete_patch_inline_row_ref_threshold,
            enable_external_access: config.enable_external_access,
            default_database: config.default_database.clone(),
            startup_policy: StartupPolicy::Strict,
        };

        let mut cast_functions = CastFunctionSet::new();
        crate::builtin::casts::BuiltinCasts::register_all(&mut cast_functions);
        let buffer_pool = Arc::new(BufferPool::new(options.maximum_memory));
        buffer_pool.set_weak_self(Arc::downgrade(&buffer_pool));

        Self {
            options,
            cast_functions: Arc::new(cast_functions),
            collation_bindings: Arc::new(CollationBinding::new()),
            index_types: Arc::new(IndexTypeSet::new()),
            buffer_pool,
            buffer_manager: None,
            path_manager: None,
            recovery_hooks: None,
        }
    }
}

impl From<&paro_common::config::CheckpointConfig> for CheckpointConfigOptions {
    fn from(config: &paro_common::config::CheckpointConfig) -> Self {
        Self {
            trigger_bytes: config.trigger_bytes as u64,
            trigger_interval: config.trigger_interval,
            drain_timeout: config.drain_timeout,
            max_concurrent_writers: config.max_concurrent_writers,
            artifact_gc_batch_size: config.artifact_gc_batch_size,
            artifact_gc_delete_budget: config.artifact_gc_delete_budget,
            checkpoint_gc_delete_budget: config.checkpoint_gc_delete_budget,
            segment_prune_delete_budget: config.segment_prune_delete_budget,
        }
    }
}

impl From<&paro_common::config::ParoConfig> for InstanceConfig {
    fn from(config: &paro_common::config::ParoConfig) -> Self {
        let mut instance = Self::from(&config.cluster);
        instance.options.checkpoint = CheckpointConfigOptions::from(&config.storage.checkpoint);
        instance
    }
}

impl From<paro_common::config::AccessMode> for AccessMode {
    fn from(mode: paro_common::config::AccessMode) -> Self {
        match mode {
            paro_common::config::AccessMode::ReadWrite => AccessMode::ReadWrite,
            paro_common::config::AccessMode::ReadOnly => AccessMode::ReadOnly,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collation_binding() {
        let binding = CollationBinding::new();

        // Register collations
        binding.register_collation("en_US".to_string());
        binding.register_collation("zh_CN".to_string());

        // Check collations
        assert!(binding.has_collation("en_US"));
        assert!(binding.has_collation("zh_CN"));
        assert!(!binding.has_collation("fr_FR"));

        // Case insensitive check
        assert!(binding.has_collation("EN_US"));

        // Get all collations
        let collations = binding.get_collations();
        assert_eq!(collations.len(), 2);
    }

    #[test]
    fn test_index_type_set() {
        let index_types = IndexTypeSet::new();

        // Default index types
        assert!(index_types.find_by_name("ART").is_some());
        assert!(index_types.find_by_name("BTREE").is_some());

        // Case insensitive
        assert!(index_types.find_by_name("art").is_some());
        assert!(index_types.find_by_name("btree").is_some());

        // Register custom index type
        index_types.register_index_type("HASH".to_string(), "Hash Index".to_string());
        assert!(index_types.find_by_name("HASH").is_some());

        // Get all index types
        let types = index_types.get_index_types();
        assert!(types.len() >= 3);
    }

    #[test]
    fn test_default_config() {
        let config = InstanceConfig::default();
        assert!(!config.is_in_memory());
        assert_eq!(config.options.maximum_memory, 1024 * 1024 * 1024);
        assert_eq!(config.options.default_database, "postgres");
        assert_eq!(config.options.pin_threads, ThreadAffinityMode::Auto);
        assert_eq!(config.options.checkpoint.trigger_bytes, 1 << 24);

        // Check new fields
        assert!(config.get_collation_bindings().get_collations().is_empty());
        assert!(config.get_index_types().find_by_name("ART").is_some());
    }

    #[test]
    fn test_paro_config_maps_storage_checkpoint_policy() {
        let mut config = paro_common::config::ParoConfig::default();
        config.storage.checkpoint.trigger_bytes = 32 * 1024 * 1024;
        config.storage.checkpoint.trigger_interval = Duration::from_secs(17);
        config.storage.checkpoint.drain_timeout = Duration::from_secs(9);
        config.storage.checkpoint.max_concurrent_writers = 6;
        config.storage.checkpoint.artifact_gc_batch_size = 11;
        config.storage.checkpoint.artifact_gc_delete_budget = 22;
        config.storage.checkpoint.checkpoint_gc_delete_budget = 3;
        config.storage.checkpoint.segment_prune_delete_budget = 7;

        let instance = InstanceConfig::from(&config);
        assert_eq!(instance.options.checkpoint.trigger_bytes, 32 * 1024 * 1024);
        assert_eq!(
            instance.options.checkpoint.trigger_interval,
            Duration::from_secs(17)
        );
        assert_eq!(
            instance.options.checkpoint.drain_timeout,
            Duration::from_secs(9)
        );
        assert_eq!(instance.options.checkpoint.max_concurrent_writers, 6);
        assert_eq!(instance.options.checkpoint.artifact_gc_batch_size, 11);
        assert_eq!(instance.options.checkpoint.artifact_gc_delete_budget, 22);
        assert_eq!(instance.options.checkpoint.checkpoint_gc_delete_budget, 3);
        assert_eq!(instance.options.checkpoint.segment_prune_delete_budget, 7);
    }

    #[test]
    fn test_in_memory_config() {
        let config = InstanceConfig::in_memory();
        assert!(config.is_in_memory());
        assert_eq!(config.options.maximum_memory, 1024 * 1024);
        assert_eq!(config.options.maximum_threads, Some(1));
        assert_eq!(config.options.pin_threads, ThreadAffinityMode::Auto);
    }

    #[test]
    fn test_config_builder() {
        let config = InstanceConfig::new()
            .with_max_memory(2 * 1024 * 1024 * 1024)
            .with_num_threads(4)
            .with_default_database("mydb")
            .with_instance_root("/tmp/my-instance");

        assert_eq!(config.options.maximum_memory, 2 * 1024 * 1024 * 1024);
        assert_eq!(config.options.maximum_threads, Some(4));
        assert_eq!(config.options.pin_threads, ThreadAffinityMode::Auto);
        assert_eq!(config.options.default_database, "mydb");
        assert_eq!(config.options.instance_root, "/tmp/my-instance");
    }
}
