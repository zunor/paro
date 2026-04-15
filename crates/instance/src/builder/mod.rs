use self::runtime_resources::RuntimeResources;
use crate::database::default_recovery_hooks;
use crate::file_system::DatabaseFileSystem;
use crate::lifecycle::startup_report::InstanceStartupDisposition;
use crate::lifecycle::InstanceLifecycle;
use crate::metadata::instance_catalog_store::InstanceCatalogStore;
use crate::metadata::instance_layout::InstanceLayout;
use crate::metadata::instance_owner::InstanceOwnerGuard;
use crate::metadata::instance_run_state::generate_boot_id;
use crate::metadata::instance_run_state::InstanceRunStateStore;
use crate::metadata::InstanceMetadata;
use crate::{BootConfig, Instance, InstanceConfig, ManagedDatabaseService};
use paro_common::logging::targets;
use paro_storage::meta::{FileMetadataStore, MetadataStore};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod runtime_resources;

enum InstanceMode {
    InMemory,
    Persistent(PathBuf),
}

pub struct InstanceBuilder {
    config: InstanceConfig,
    mode: InstanceMode,
}

impl InstanceBuilder {
    pub fn new(config: InstanceConfig) -> Self {
        if config.is_in_memory() {
            Self::in_memory(config)
        } else {
            let path = PathBuf::from(config.options.instance_root.clone());
            Self::persistent(path, config)
        }
    }

    pub fn in_memory(mut config: InstanceConfig) -> Self {
        config.options.instance_root = ":memory:".to_string();
        Self {
            config,
            mode: InstanceMode::InMemory,
        }
    }

    pub fn persistent(path: impl AsRef<Path>, config: InstanceConfig) -> Self {
        Self {
            config,
            mode: InstanceMode::Persistent(path.as_ref().to_path_buf()),
        }
    }

    pub fn build(self) -> paro_common::error::Result<Arc<Instance>> {
        let instance = self.build_unbootstrapped()?;
        instance.bootstrap()?;
        Ok(instance)
    }

    pub(crate) fn build_unbootstrapped(self) -> paro_common::error::Result<Arc<Instance>> {
        match self.mode {
            InstanceMode::InMemory => Self::build_in_memory(self.config),
            InstanceMode::Persistent(path) => Self::build_persistent(path, self.config),
        }
    }

    fn build_in_memory(mut config: InstanceConfig) -> paro_common::error::Result<Arc<Instance>> {
        Self::prepare_buffer_pool(&config)?;
        let boot_config = Arc::new(BootConfig::from_config(&config));
        let runtime =
            RuntimeResources::build(&boot_config, Arc::new(DatabaseFileSystem::in_memory()))
                .into_runtime(&config, &boot_config);

        Ok(Self::finish_build(
            &mut config,
            boot_config,
            InstanceMetadata::new_in_memory(),
            runtime,
        ))
    }

    fn build_persistent(
        path: PathBuf,
        mut config: InstanceConfig,
    ) -> paro_common::error::Result<Arc<Instance>> {
        let layout = InstanceLayout::new(path);
        config.options.instance_root = layout.root().to_string_lossy().to_string();
        Self::ensure_data_dir_exists(&layout)?;
        let owner_guard = InstanceOwnerGuard::acquire(&layout)?;
        Self::prepare_buffer_pool(&config)?;

        let boot_config = Arc::new(BootConfig::from_config(&config));
        let runtime = RuntimeResources::build(&boot_config, Arc::new(DatabaseFileSystem::local()))
            .into_runtime(&config, &boot_config);
        let meta_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(layout.meta_dir()).map_err(|e| {
                paro_common::error::internal(format!("Failed to open instance metadata store: {e}"))
            })?);

        Ok(Self::finish_build(
            &mut config,
            boot_config,
            InstanceMetadata::new_persistent(
                layout,
                Arc::new(InstanceCatalogStore::with_store(Arc::clone(&meta_store))),
                Arc::new(InstanceRunStateStore::with_store(meta_store)),
                owner_guard,
            ),
            runtime,
        ))
    }

    fn finish_build(
        config: &mut InstanceConfig,
        boot_config: Arc<BootConfig>,
        metadata: InstanceMetadata,
        runtime: crate::runtime::InstanceRuntime,
    ) -> Arc<Instance> {
        let recovery_hooks = config
            .take_recovery_hooks()
            .unwrap_or_else(default_recovery_hooks);
        let boot_id = generate_boot_id();

        Arc::new(Instance {
            boot_config: Arc::clone(&boot_config),
            metadata,
            runtime,
            lifecycle: InstanceLifecycle::new(
                boot_config.startup_policy,
                InstanceStartupDisposition::FullRecovery,
                boot_id,
            ),
            database_service: ManagedDatabaseService::new_with_boot_config(
                boot_config.as_ref(),
                recovery_hooks,
            ),
        })
    }

    fn prepare_buffer_pool(config: &InstanceConfig) -> paro_common::error::Result<()> {
        if !config.options.temporary_directory.is_empty() {
            config
                .buffer_pool
                .set_temporary_directory(config.options.temporary_directory.clone())?;
        }
        if config.options.max_temp_directory_size.is_some() {
            config
                .buffer_pool
                .set_swap_limit(config.options.max_temp_directory_size)?;
        }
        Ok(())
    }

    fn ensure_data_dir_exists(layout: &InstanceLayout) -> paro_common::error::Result<()> {
        let data_dir = layout.root();
        if data_dir.exists() {
            return Ok(());
        }

        std::fs::create_dir_all(data_dir).map_err(|e| {
            paro_common::error::internal(format!("Failed to create data directory: {e}"))
        })?;
        tracing::info!(
            target: targets::INSTANCE,
            path = %data_dir.display(),
            "Created data directory"
        );
        Ok(())
    }
}

impl Instance {
    pub fn new(config: InstanceConfig) -> paro_common::error::Result<Arc<Self>> {
        InstanceBuilder::new(config).build()
    }

    pub fn new_in_memory() -> Arc<Self> {
        Self::new_in_memory_with_config(InstanceConfig::in_memory())
            .expect("Failed to create in-memory instance")
    }

    pub fn new_in_memory_with_config(
        config: InstanceConfig,
    ) -> paro_common::error::Result<Arc<Self>> {
        InstanceBuilder::in_memory(config).build()
    }

    pub fn new_persistent(
        path: impl AsRef<Path>,
        config: InstanceConfig,
    ) -> paro_common::error::Result<Arc<Self>> {
        InstanceBuilder::persistent(path, config).build()
    }
}
