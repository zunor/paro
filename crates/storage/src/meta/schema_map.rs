use super::metadata_store::MetadataStore;
use crate::tablet::TabletSchemaRef;
use parking_lot::RwLock;
use paro_common::error::{self as paro_error, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Schema identity key `(schema_id, schema_version)`.
pub type SchemaKey = (u64, u32);

/// Global deduplicated schema cache shared by all tablets.
#[derive(Debug, Default)]
pub struct GlobalSchemaMap {
    schemas: RwLock<HashMap<SchemaKey, TabletSchemaRef>>,
}

impl GlobalSchemaMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build the metadata key used by `MetadataStore`.
    pub fn schema_store_key(schema_id: u64, schema_version: u32) -> String {
        format!("schema/{schema_id}/{schema_version}")
    }

    pub fn len(&self) -> usize {
        self.schemas.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.schemas.read().is_empty()
    }

    pub fn contains(&self, schema_id: u64, schema_version: u32) -> bool {
        self.schemas
            .read()
            .contains_key(&(schema_id, schema_version))
    }

    pub fn get(&self, schema_id: u64, schema_version: u32) -> Option<TabletSchemaRef> {
        self.schemas
            .read()
            .get(&(schema_id, schema_version))
            .cloned()
    }

    /// Atomically returns an existing schema or inserts the provided one.
    pub fn get_or_insert(
        &self,
        schema_id: u64,
        schema_version: u32,
        schema: TabletSchemaRef,
    ) -> Result<TabletSchemaRef> {
        if schema.schema_id() != schema_id || schema.schema_version() != schema_version {
            return Err(paro_error::invalid_input(format!(
                "Schema identity mismatch: key=({}, {}), schema=({}, {})",
                schema_id,
                schema_version,
                schema.schema_id(),
                schema.schema_version()
            )));
        }

        let mut guard = self.schemas.write();
        let entry = guard.entry((schema_id, schema_version)).or_insert(schema);
        Ok(Arc::clone(entry))
    }

    pub fn remove(&self, schema_id: u64, schema_version: u32) -> Option<TabletSchemaRef> {
        self.schemas.write().remove(&(schema_id, schema_version))
    }

    pub fn clear(&self) {
        self.schemas.write().clear();
    }

    /// Persist schema bytes to `schema/{schema_id}/{schema_version}` and cache it.
    pub fn save_to_store(
        &self,
        store: &dyn MetadataStore,
        schema: TabletSchemaRef,
    ) -> Result<TabletSchemaRef> {
        let schema_id = schema.schema_id();
        let schema_version = schema.schema_version();
        let canonical = self.get_or_insert(schema_id, schema_version, schema)?;
        let key = Self::schema_store_key(schema_id, schema_version);
        let bytes = canonical.serialize()?;
        store.put(&key, &bytes)?;
        Ok(canonical)
    }

    /// Load a schema from `MetadataStore`, then deduplicate it into the map.
    pub fn load_from_store(
        &self,
        store: &dyn MetadataStore,
        schema_id: u64,
        schema_version: u32,
    ) -> Result<Option<TabletSchemaRef>> {
        let key = Self::schema_store_key(schema_id, schema_version);
        let Some(bytes) = store.get(&key)? else {
            return Ok(None);
        };

        let schema = Arc::new(crate::tablet::TabletSchema::deserialize(&bytes)?);
        let canonical = self.get_or_insert(schema_id, schema_version, schema)?;
        Ok(Some(canonical))
    }

    /// Remove a schema from both cache and persistent store.
    pub fn remove_from_store(
        &self,
        store: &dyn MetadataStore,
        schema_id: u64,
        schema_version: u32,
    ) -> Result<()> {
        self.remove(schema_id, schema_version);
        let key = Self::schema_store_key(schema_id, schema_version);
        store.delete(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::FileMetadataStore;
    use crate::tablet::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use std::thread;
    use tempfile::tempdir;

    fn test_schema(schema_id: u64, schema_version: u32, value_col: &str) -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, value_col, LogicalType::Varchar),
        ];
        Arc::new(
            TabletSchema::with_version(schema_id, schema_version, columns, KeysType::PrimaryKeys)
                .unwrap(),
        )
    }

    #[test]
    fn schema_map_dedup_test() {
        let map = GlobalSchemaMap::new();
        let first = test_schema(7, 3, "v1");
        let second = test_schema(7, 3, "v2");

        let dedup_first = map.get_or_insert(7, 3, Arc::clone(&first)).unwrap();
        let dedup_second = map.get_or_insert(7, 3, Arc::clone(&second)).unwrap();

        assert!(Arc::ptr_eq(&dedup_first, &dedup_second));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn schema_map_concurrent_get_or_insert_test() {
        let map = Arc::new(GlobalSchemaMap::new());
        let mut handles = Vec::new();

        for worker in 0..8 {
            let map = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                let schema = test_schema(42, 9, &format!("value_{worker}"));
                map.get_or_insert(42, 9, schema).unwrap()
            }));
        }

        let mut inserted = Vec::new();
        for handle in handles {
            inserted.push(handle.join().unwrap());
        }

        let canonical = inserted.first().unwrap().clone();
        for schema in &inserted {
            assert!(Arc::ptr_eq(&canonical, schema));
        }
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn schema_map_remove_keeps_external_refs_test() {
        let map = GlobalSchemaMap::new();
        let schema = test_schema(11, 2, "payload");
        let canonical = map.get_or_insert(11, 2, Arc::clone(&schema)).unwrap();
        assert!(map.contains(11, 2));

        let external_ref = Arc::clone(&canonical);
        let removed = map.remove(11, 2).unwrap();
        assert!(Arc::ptr_eq(&removed, &canonical));
        assert!(!map.contains(11, 2));

        drop(removed);
        assert_eq!(external_ref.schema_id(), 11);
        assert_eq!(external_ref.schema_version(), 2);
    }

    #[test]
    fn schema_map_store_roundtrip_test() {
        let tmp = tempdir().unwrap();
        let store = FileMetadataStore::new(tmp.path()).unwrap();
        let map = GlobalSchemaMap::new();
        let schema = test_schema(99, 4, "payload");

        map.save_to_store(&store, Arc::clone(&schema)).unwrap();
        assert!(tmp.path().join("schema/99/4.bin").exists());

        let loaded = map.load_from_store(&store, 99, 4).unwrap().unwrap();
        assert_eq!(loaded.schema_id(), 99);
        assert_eq!(loaded.schema_version(), 4);
        assert!(Arc::ptr_eq(&loaded, &map.get(99, 4).unwrap()));

        map.remove_from_store(&store, 99, 4).unwrap();
        assert!(store.get("schema/99/4").unwrap().is_none());
        assert!(!map.contains(99, 4));
    }
}
