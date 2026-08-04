// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::checkpoint::view::CheckpointView;
use crc32fast::hash as crc32;
use paro_catalog::collection::InstallMode;
use paro_catalog::database_catalog::ParoCatalog;
#[cfg(test)]
use paro_catalog::entry::CatalogType;
use paro_catalog::entry::{CatalogEntryEnum, SchemaEntry};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_storage::meta::TabletMetaManager;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

const CATALOG_SNAPSHOT_MAGIC: &[u8; 4] = b"PCAT";
const CATALOG_SNAPSHOT_VERSION: u16 = 4;

/// Catalog snapshot writer / loader used by checkpoint base-image flows.
pub struct CatalogWriter;

impl CatalogWriter {
    pub fn serialize(catalog: &ParoCatalog) -> anyhow::Result<Vec<u8>> {
        Self::serialize_at(catalog, u64::MAX)
    }

    pub fn serialize_view(catalog: &ParoCatalog, view: &CheckpointView) -> anyhow::Result<Vec<u8>> {
        Self::serialize_at(catalog, view.catalog_snapshot_ts)
    }

    fn serialize_at(catalog: &ParoCatalog, snapshot_ts: u64) -> anyhow::Result<Vec<u8>> {
        let txn = CatalogSnapshot::read_only(snapshot_ts);
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
                .serialize_contents_at(snapshot_ts)
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
                "Catalog snapshot schema count overflow: {}",
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
        bytes.extend_from_slice(CATALOG_SNAPSHOT_MAGIC);
        bytes.write_all(&CATALOG_SNAPSHOT_VERSION.to_le_bytes())?;
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
        if &magic != CATALOG_SNAPSHOT_MAGIC {
            return Err(anyhow::anyhow!(
                "Invalid catalog snapshot magic: expected {:?}, got {:?}",
                CATALOG_SNAPSHOT_MAGIC,
                magic
            ));
        }

        let mut version_buf = [0u8; 2];
        cursor.read_exact(&mut version_buf)?;
        let version = u16::from_le_bytes(version_buf);
        if version != CATALOG_SNAPSHOT_VERSION {
            return Err(anyhow::anyhow!(
                "Unsupported catalog snapshot version {}",
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
                "Catalog snapshot checksum mismatch: expected {}, got {}",
                expected_checksum,
                actual_checksum
            ));
        }

        if cursor.position() != bytes.len() as u64 {
            return Err(anyhow::anyhow!(
                "Catalog snapshot has trailing bytes: {}",
                bytes.len() as u64 - cursor.position()
            ));
        }

        Self::deserialize_body(&body, catalog, tablet_meta)
    }

    fn write_field(buf: &mut Vec<u8>, field: &str, payload: &[u8]) -> anyhow::Result<()> {
        let len = u32::try_from(payload.len()).map_err(|_| {
            anyhow::anyhow!(
                "Catalog snapshot field '{}' is too large: {} bytes",
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
                    "Catalog snapshot field 'object_id_allocator_watermark' is too large: {} bytes",
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
            let mut schema_entry_recovered = SchemaEntry::deserialize_metadata(
                &metadata,
                Arc::clone(catalog.object_id_allocator()),
                catalog.gc_epoch_handle(),
            )
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
                "Catalog snapshot body has trailing bytes: {}",
                body.len() as u64 - cursor.position()
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::view::{CheckpointCut, CheckpointView};
    use paro_catalog::catalog::Catalog;
    use paro_catalog::entry::{
        CatalogEntryEnum, CreateIndexInfo, CreateSequenceInfo, CreateViewInfo, IndexBuildState,
        IndexCatalogEntry, LogicalIndex, SequenceCatalogEntry, ViewCatalogEntry,
    };
    use paro_common::checkpoint::{CheckpointFrontier, RecoverySummary};
    use paro_common::types::LogicalType;
    use paro_parser::parse_one;
    use paro_storage::meta::{FileMetadataStore, MetadataStore};
    use tempfile::tempdir;

    fn parse_query(sql: &str) -> Box<paro_parser::ast::Query> {
        match parse_one(sql).expect("query should parse").stmt {
            paro_parser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        }
    }

    fn create_test_meta_manager() -> Arc<TabletMetaManager> {
        let temp_dir = tempdir().unwrap();
        let store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(temp_dir.path().join("meta")).unwrap());
        Arc::new(TabletMetaManager::with_store_and_data_root(
            store,
            temp_dir.keep(),
        ))
    }

    #[test]
    fn catalog_snapshot_roundtrip_preserves_view_and_sequence_oid() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);
        catalog.bump_object_id_allocator(1_000_000_000_000);

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
            catalog.object_id_allocator().allocate(),
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
                catalog.object_id_allocator().allocate(),
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
            catalog.object_id_allocator().allocate(),
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

        let checkpoint_bytes = CatalogWriter::serialize(&catalog).unwrap();

        let restored = ParoCatalog::new("test_db".to_string());
        restored.initialize(false);
        CatalogWriter::deserialize(
            &checkpoint_bytes,
            &restored,
            Some(create_test_meta_manager()),
        )
        .unwrap();

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

    #[test]
    fn serialize_view_respects_catalog_snapshot_ts() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);

        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&read_txn, "public").unwrap();
        let views = schema
            .collection(CatalogType::View)
            .expect("view collection");

        let before_cut = Arc::new(CatalogEntryEnum::View(Arc::new(ViewCatalogEntry::new(
            CreateViewInfo::new(
                "public".to_string(),
                "before_cut".to_string(),
                parse_query("SELECT 1 AS id"),
            )
            .with_column_names(vec!["id".to_string()])
            .with_column_types(vec![LogicalType::Integer]),
            100,
            catalog.name().to_string(),
            catalog.object_id_allocator().allocate(),
        ))));
        views
            .install_replayed(10, before_cut, InstallMode::RejectExisting)
            .expect("pre-cut view install should succeed");

        let after_cut = Arc::new(CatalogEntryEnum::View(Arc::new(ViewCatalogEntry::new(
            CreateViewInfo::new(
                "public".to_string(),
                "after_cut".to_string(),
                parse_query("SELECT 2 AS id"),
            )
            .with_column_names(vec!["id".to_string()])
            .with_column_types(vec![LogicalType::Integer]),
            101,
            catalog.name().to_string(),
            catalog.object_id_allocator().allocate(),
        ))));
        views
            .install_replayed(11, after_cut, InstallMode::RejectExisting)
            .expect("post-cut view install should succeed");

        let view = CheckpointView::new(
            CheckpointCut {
                target_lsn: 4,
                issued_at_micros: 1,
            },
            CheckpointFrontier {
                checkpoint_lsn: 4,
                checkpoint_commit_id: 10,
                checkpoint_maintenance_id: 0,
            },
            RecoverySummary {
                max_lsn: 4,
                max_commit_id: 10,
                max_maintenance_id: 0,
                max_catalog_commit_id: 10,
                max_seen_object_id: 101,
            },
            11,
        )
        .expect("checkpoint view should be aligned");

        let checkpoint_bytes = CatalogWriter::serialize_view(&catalog, &view).unwrap();
        let restored = ParoCatalog::new("test_db".to_string());
        restored.initialize(false);
        CatalogWriter::deserialize(
            &checkpoint_bytes,
            &restored,
            Some(create_test_meta_manager()),
        )
        .unwrap();

        let restored_schema = restored.get_schema(&read_txn, "public").unwrap();
        assert!(restored_schema
            .get_view(read_txn.transaction_id, read_txn.start_time, "before_cut")
            .is_some());
        assert!(restored_schema
            .get_view(read_txn.transaction_id, read_txn.start_time, "after_cut")
            .is_none());
    }
}
