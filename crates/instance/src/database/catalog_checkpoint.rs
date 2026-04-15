use crate::storage_manager::StorageManager;
use crc32fast::hash as crc32;
use paro_catalog::collection::InstallMode;
use paro_catalog::database_catalog::ParoCatalog;
#[cfg(test)]
use paro_catalog::entry::CatalogType;
use paro_catalog::entry::{CatalogEntryEnum, SchemaEntry};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_storage::meta::{MetadataOp, MetadataStore, TabletMetaManager};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

pub const CATALOG_CHECKPOINT_KEY: &str = "catalog/checkpoint";
pub const CATALOG_CHECKPOINT_ID_KEY: &str = "catalog/checkpoint_id";

const CATALOG_CHECKPOINT_MAGIC: &[u8; 4] = b"PCAT";
const CATALOG_CHECKPOINT_VERSION: u16 = 4;

pub struct CatalogCheckpoint;

impl CatalogCheckpoint {
    pub fn decode_marker(raw: &[u8]) -> anyhow::Result<u64> {
        if raw.len() != 8 {
            return Err(anyhow::anyhow!(
                "Invalid catalog checkpoint marker length: expected 8, got {}",
                raw.len()
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(raw);
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn read_marker(store: &dyn MetadataStore) -> anyhow::Result<Option<u64>> {
        store
            .get(CATALOG_CHECKPOINT_ID_KEY)
            .map_err(|e| anyhow::anyhow!(e))
            .and_then(|raw| raw.map(|v| Self::decode_marker(&v)).transpose())
    }

    pub fn next_marker(store: &dyn MetadataStore) -> anyhow::Result<u64> {
        let current = Self::read_marker(store)?.unwrap_or(0);
        current.checked_add(1).ok_or_else(|| {
            anyhow::anyhow!("Catalog checkpoint marker overflow at value {}", current)
        })
    }

    pub fn write_metadata_batch(
        store: &dyn MetadataStore,
        catalog_bytes: Vec<u8>,
    ) -> anyhow::Result<u64> {
        let checkpoint_marker = Self::next_marker(store)?;
        let marker_bytes = checkpoint_marker.to_le_bytes().to_vec();
        let metadata_ops = vec![
            MetadataOp::Put {
                key: CATALOG_CHECKPOINT_KEY.to_string(),
                value: catalog_bytes,
            },
            MetadataOp::Put {
                key: CATALOG_CHECKPOINT_ID_KEY.to_string(),
                value: marker_bytes,
            },
        ];
        store
            .write_batch(&metadata_ops)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(checkpoint_marker)
    }

    pub fn serialize(catalog: &ParoCatalog) -> anyhow::Result<Vec<u8>> {
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema_entries = catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time);

        let mut schema_payloads = Vec::new();
        for entry in schema_entries {
            let CatalogEntryEnum::Schema(schema) = entry.as_ref() else {
                continue;
            };
            let metadata = schema
                .serialize_metadata()
                .map_err(|e| anyhow::anyhow!(e))?;
            let contents = schema
                .serialize_contents()
                .map_err(|e| anyhow::anyhow!(e))?;
            schema_payloads.push((schema.base.name.clone(), metadata, contents));
        }

        let mut body = Vec::new();
        Self::write_field(&mut body, "catalog_name", catalog.name().as_bytes())?;
        Self::write_field(
            &mut body,
            "default_schema",
            paro_catalog::catalog::DEFAULT_SCHEMA.as_bytes(),
        )?;
        Self::write_field(
            &mut body,
            "object_id_allocator_watermark",
            &catalog.current_object_id().to_le_bytes(),
        )?;

        let schema_count = u64::try_from(schema_payloads.len()).map_err(|_| {
            anyhow::anyhow!(
                "Catalog checkpoint schema count overflow: {}",
                schema_payloads.len()
            )
        })?;
        body.write_all(&schema_count.to_le_bytes())?;

        for (schema_name, metadata, contents) in schema_payloads {
            let metadata_field = format!("schema_metadata({})", schema_name);
            let contents_field = format!("schema_contents({})", schema_name);
            Self::write_field(&mut body, &metadata_field, &metadata)?;
            Self::write_field(&mut body, &contents_field, &contents)?;
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOG_CHECKPOINT_MAGIC);
        bytes.write_all(&CATALOG_CHECKPOINT_VERSION.to_le_bytes())?;
        bytes.write_all(&(body.len() as u32).to_le_bytes())?;
        bytes.extend_from_slice(&body);
        bytes.write_all(&crc32(&body).to_le_bytes())?;
        Ok(bytes)
    }

    pub fn deserialize(
        bytes: &[u8],
        catalog: &ParoCatalog,
        tablet_meta: Option<Arc<TabletMetaManager>>,
    ) -> anyhow::Result<()> {
        let mut cursor = Cursor::new(bytes);

        let mut magic = [0u8; 4];
        cursor.read_exact(&mut magic)?;
        if &magic != CATALOG_CHECKPOINT_MAGIC {
            return Err(anyhow::anyhow!(
                "Invalid catalog checkpoint magic: expected {:?}, got {:?}",
                CATALOG_CHECKPOINT_MAGIC,
                magic
            ));
        }

        let mut version_buf = [0u8; 2];
        cursor.read_exact(&mut version_buf)?;
        let version = u16::from_le_bytes(version_buf);
        if version != CATALOG_CHECKPOINT_VERSION {
            return Err(anyhow::anyhow!(
                "Unsupported catalog checkpoint version {}",
                version
            ));
        }

        let mut body_len_buf = [0u8; 4];
        cursor.read_exact(&mut body_len_buf)?;
        let body_len = u32::from_le_bytes(body_len_buf) as usize;

        let mut body = vec![0u8; body_len];
        cursor.read_exact(&mut body)?;

        let mut checksum_buf = [0u8; 4];
        cursor.read_exact(&mut checksum_buf)?;
        let expected_checksum = u32::from_le_bytes(checksum_buf);
        let actual_checksum = crc32(&body);
        if actual_checksum != expected_checksum {
            return Err(anyhow::anyhow!(
                "Catalog checkpoint checksum mismatch: expected {}, got {}",
                expected_checksum,
                actual_checksum
            ));
        }

        if cursor.position() != bytes.len() as u64 {
            return Err(anyhow::anyhow!(
                "Catalog checkpoint has trailing bytes: {}",
                bytes.len() as u64 - cursor.position()
            ));
        }

        Self::deserialize_body(&body, catalog, tablet_meta)
    }

    pub fn load_from_store(
        catalog: &ParoCatalog,
        store: &dyn MetadataStore,
        tablet_meta: Option<Arc<TabletMetaManager>>,
    ) -> anyhow::Result<()> {
        let checkpoint_data = store
            .get(CATALOG_CHECKPOINT_KEY)
            .map_err(|e| anyhow::anyhow!(e))?;

        if let Some(bytes) = checkpoint_data {
            tracing::info!(
                target: paro_common::logging::targets::CHECKPOINT,
                db = %catalog.name(),
                bytes = bytes.len(),
                "Loading catalog from checkpoint"
            );
            Self::deserialize(&bytes, catalog, tablet_meta)?;
        } else {
            tracing::debug!(
                target: paro_common::logging::targets::CHECKPOINT,
                db = %catalog.name(),
                "No catalog checkpoint found in MetadataStore"
            );
        }

        Ok(())
    }

    pub fn marker_from_storage(storage: &dyn StorageManager) -> anyhow::Result<Option<u64>> {
        storage
            .get_metadata_store()
            .map(Self::read_marker)
            .transpose()
            .map(|value| value.flatten())
    }

    fn write_field(buf: &mut Vec<u8>, field: &str, payload: &[u8]) -> anyhow::Result<()> {
        let len = u32::try_from(payload.len()).map_err(|_| {
            anyhow::anyhow!(
                "Catalog checkpoint field '{}' is too large: {} bytes",
                field,
                payload.len()
            )
        })?;
        buf.write_all(&len.to_le_bytes())?;
        buf.write_all(payload)?;
        Ok(())
    }

    fn read_field(buf: &mut Cursor<&[u8]>) -> anyhow::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        buf.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        buf.read_exact(&mut payload)?;
        Ok(payload)
    }

    fn deserialize_body(
        body: &[u8],
        catalog: &ParoCatalog,
        tablet_meta: Option<Arc<TabletMetaManager>>,
    ) -> anyhow::Result<()> {
        let mut cursor = Cursor::new(body);

        let _catalog_name = String::from_utf8(Self::read_field(&mut cursor)?)?;
        let _default_schema = String::from_utf8(Self::read_field(&mut cursor)?)?;
        let allocator_watermark = {
            let payload = Self::read_field(&mut cursor)?;
            if payload.len() != 8 {
                return Err(anyhow::anyhow!(
                    "Catalog checkpoint field 'object_id_allocator_watermark' is too large: {} bytes",
                    payload.len()
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&payload);
            u64::from_le_bytes(bytes)
        };

        let mut count_buf = [0u8; 8];
        cursor.read_exact(&mut count_buf)?;
        let schema_count = u64::from_le_bytes(count_buf);

        for _ in 0..schema_count {
            let metadata = Self::read_field(&mut cursor)?;
            let contents = Self::read_field(&mut cursor)?;
            let mut schema_entry_recovered =
                SchemaEntry::deserialize_metadata(&metadata, catalog.gc_epoch_handle())
                    .map_err(|e| anyhow::anyhow!(e))?;
            schema_entry_recovered
                .install_contents(&contents, tablet_meta.clone())
                .map_err(|e| anyhow::anyhow!(e))?;
            let schema_entry_enum =
                Arc::new(CatalogEntryEnum::Schema(Arc::new(schema_entry_recovered)));
            catalog
                .get_schema_collection()
                .install_committed(schema_entry_enum, InstallMode::ReplaceExisting)
                .map_err(|err| anyhow::anyhow!(err))?;
        }

        catalog.bump_object_id_allocator(allocator_watermark);
        catalog
            .rebuild_dependency_graph()
            .map_err(|err| anyhow::anyhow!(err))?;

        if cursor.position() != body.len() as u64 {
            return Err(anyhow::anyhow!(
                "Catalog checkpoint body has trailing bytes: {}",
                body.len() as u64 - cursor.position()
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::entry::{
        CatalogEntryEnum, CreateIndexInfo, CreateSequenceInfo, CreateViewInfo, IndexBuildState,
        IndexCatalogEntry, LogicalIndex, SequenceCatalogEntry, ViewCatalogEntry,
    };
    use paro_common::types::LogicalType;
    use paro_parser::parse_one;

    fn parse_query(sql: &str) -> Box<paro_parser::ast::Query> {
        match parse_one(sql).expect("query should parse").stmt {
            paro_parser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        }
    }

    #[test]
    fn checkpoint_roundtrip_preserves_view_and_sequence_oid() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);
        catalog.set_object_id_allocator(1_000_000_000_000);

        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&read_txn, "public").unwrap();

        let view_entry = Arc::new(CatalogEntryEnum::View(Arc::new(ViewCatalogEntry::new(
            CreateViewInfo::new(
                "public".to_string(),
                "checkpoint_view".to_string(),
                parse_query("SELECT 1 AS id"),
            )
            .with_column_names(vec!["id".to_string()])
            .with_column_types(vec![LogicalType::Integer]),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::View)
            .expect("view collection")
            .install_committed(view_entry, InstallMode::RejectExisting)
            .expect("view install should succeed");

        let sequence_entry = Arc::new(CatalogEntryEnum::Sequence(Arc::new(
            SequenceCatalogEntry::new(
                CreateSequenceInfo::new("public".to_string(), "checkpoint_seq".to_string()),
                0,
                catalog.name().to_string(),
            )
            .expect("sequence info should be valid"),
        )));
        schema
            .collection(CatalogType::Sequence)
            .expect("sequence collection")
            .install_committed(sequence_entry, InstallMode::RejectExisting)
            .expect("sequence install should succeed");

        let index_entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            CreateIndexInfo::new(
                "public".to_string(),
                "checkpoint_table".to_string(),
                "checkpoint_idx".to_string(),
                vec![LogicalIndex::new(0)],
                vec![LogicalType::Integer],
            )
            .with_catalog(catalog.name().to_string())
            .with_build_state(IndexBuildState::Ready),
            42,
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(index_entry, InstallMode::RejectExisting)
            .expect("index install should succeed");

        let original_view_oid = schema
            .get_view(
                read_txn.transaction_id,
                read_txn.start_time,
                "checkpoint_view",
            )
            .expect("view should exist before checkpoint")
            .object_id();
        let original_sequence_oid = schema
            .get_sequence(
                read_txn.transaction_id,
                read_txn.start_time,
                "checkpoint_seq",
            )
            .expect("sequence should exist before checkpoint")
            .object_id();
        let original_index_oid = schema
            .get_index(
                read_txn.transaction_id,
                read_txn.start_time,
                "checkpoint_idx",
            )
            .expect("index should exist before checkpoint")
            .object_id();
        let original_allocator_watermark = catalog.current_object_id();

        let checkpoint_bytes = CatalogCheckpoint::serialize(&catalog).unwrap();

        let restored = ParoCatalog::new("test_db".to_string());
        restored.initialize(false);
        CatalogCheckpoint::deserialize(&checkpoint_bytes, &restored, None).unwrap();

        let restored_schema = restored.get_schema(&read_txn, "public").unwrap();
        let restored_view_oid = restored_schema
            .get_view(
                read_txn.transaction_id,
                read_txn.start_time,
                "checkpoint_view",
            )
            .expect("view should survive checkpoint roundtrip")
            .object_id();
        let restored_sequence_oid = restored_schema
            .get_sequence(
                read_txn.transaction_id,
                read_txn.start_time,
                "checkpoint_seq",
            )
            .expect("sequence should survive checkpoint roundtrip")
            .object_id();
        let restored_index_oid = restored_schema
            .get_index(
                read_txn.transaction_id,
                read_txn.start_time,
                "checkpoint_idx",
            )
            .expect("index should survive checkpoint roundtrip")
            .object_id();

        assert_eq!(restored_view_oid, original_view_oid);
        assert_eq!(restored_sequence_oid, original_sequence_oid);
        assert_eq!(restored_index_oid, original_index_oid);
        let restored_allocator_watermark = restored.current_object_id();
        assert!(restored_allocator_watermark >= original_allocator_watermark);
        let allocated_after_restore = restored.next_object_id();
        assert!(allocated_after_restore >= restored_allocator_watermark);
        assert!(restored.current_object_id() > allocated_after_restore);
    }
}
