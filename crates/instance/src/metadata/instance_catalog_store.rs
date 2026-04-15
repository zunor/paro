use crate::metadata::instance_catalog::InstanceCatalog;
use parking_lot::Mutex;
use paro_storage::meta::MetadataStore;
use std::sync::Arc;

pub const INSTANCE_CATALOG_KEY: &str = "catalog";

enum InstanceCatalogBackend {
    Durable(Arc<dyn MetadataStore>),
    Memory(Mutex<Option<InstanceCatalog>>),
}

pub struct InstanceCatalogStore {
    backend: InstanceCatalogBackend,
}

impl std::fmt::Debug for InstanceCatalogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceCatalogStore")
            .field(
                "backend",
                &match &self.backend {
                    InstanceCatalogBackend::Durable(_) => "durable",
                    InstanceCatalogBackend::Memory(_) => "memory",
                },
            )
            .finish()
    }
}

impl InstanceCatalogStore {
    pub fn new_in_memory() -> Self {
        Self {
            backend: InstanceCatalogBackend::Memory(Mutex::new(None)),
        }
    }

    pub fn with_store(store: Arc<dyn MetadataStore>) -> Self {
        Self {
            backend: InstanceCatalogBackend::Durable(store),
        }
    }

    #[cfg(test)]
    pub(crate) fn durable_store(&self) -> Option<&Arc<dyn MetadataStore>> {
        match &self.backend {
            InstanceCatalogBackend::Durable(store) => Some(store),
            InstanceCatalogBackend::Memory(_) => None,
        }
    }

    pub fn exists(&self) -> anyhow::Result<bool> {
        match &self.backend {
            InstanceCatalogBackend::Durable(store) => store
                .exists(INSTANCE_CATALOG_KEY)
                .map_err(|e| anyhow::anyhow!(e)),
            InstanceCatalogBackend::Memory(catalog) => Ok(catalog.lock().is_some()),
        }
    }

    pub fn load(&self) -> anyhow::Result<Option<InstanceCatalog>> {
        match &self.backend {
            InstanceCatalogBackend::Durable(store) => {
                let Some(raw) = store
                    .get(INSTANCE_CATALOG_KEY)
                    .map_err(|e| anyhow::anyhow!(e))?
                else {
                    return Ok(None);
                };
                let catalog: InstanceCatalog = serde_json::from_slice(&raw)?;
                catalog.validate()?;
                Ok(Some(catalog))
            }
            InstanceCatalogBackend::Memory(catalog) => Ok(catalog.lock().clone()),
        }
    }

    pub fn save(&self, catalog: &mut InstanceCatalog) -> anyhow::Result<()> {
        catalog.touch();
        catalog.validate()?;

        match &self.backend {
            InstanceCatalogBackend::Durable(store) => {
                let payload = serde_json::to_vec_pretty(catalog)?;
                store
                    .durable_put(INSTANCE_CATALOG_KEY, &payload)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            InstanceCatalogBackend::Memory(slot) => {
                *slot.lock() = Some(catalog.clone());
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::instance_catalog::{
        DatabaseRecord, DatabaseRecordState, INSTANCE_CATALOG_FORMAT_VERSION,
    };
    use crate::metadata::instance_layout::InstanceLayout;
    use paro_storage::meta::FileMetadataStore;
    use tempfile::tempdir;

    #[test]
    fn persistent_store_writes_catalog_json() {
        let dir = tempdir().unwrap();
        let layout = InstanceLayout::new(dir.path());
        let meta_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(layout.meta_dir()).unwrap());
        let store = InstanceCatalogStore::with_store(meta_store);
        let mut catalog = InstanceCatalog {
            format_version: INSTANCE_CATALOG_FORMAT_VERSION,
            next_database_id: 2,
            default_database_id: Some(1),
            databases: vec![DatabaseRecord::new(
                1,
                "postgres".to_string(),
                DatabaseRecordState::Ready,
                layout.managed_database_dir(1).to_string_lossy().to_string(),
            )],
            last_updated_ms: 0,
        };

        store.save(&mut catalog).unwrap();

        assert!(layout.catalog_path().exists(), "catalog.json should exist");

        let loaded = store.load().unwrap().expect("catalog should load");
        assert_eq!(loaded.default_database_id, Some(1));
        assert_eq!(loaded.databases.len(), 1);
        assert_eq!(loaded.databases[0].database_id, 1);
    }
}
