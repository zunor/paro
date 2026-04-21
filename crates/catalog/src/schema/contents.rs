// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::collection::{
    CatalogCollection, CatalogGcStats, CollectionFamily, CollectionLockKey, InstallMode,
};
use crate::entry::{
    CatalogEntryEnum, CatalogObjectId, CatalogType, IndexCatalogEntry, PropertyGraphCatalogEntry,
    RoutineCatalogEntry, SequenceCatalogEntry, TableCatalogEntry, ViewCatalogEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_storage::meta::TabletMetaManager;
use std::io::{Cursor, Read, Write};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct SchemaContents {
    pub(crate) tables: Arc<CatalogCollection>,
    pub(crate) views: Arc<CatalogCollection>,
    pub(crate) indexes: Arc<CatalogCollection>,
    pub(crate) property_graphs: Arc<CatalogCollection>,
    pub(crate) functions: Arc<CatalogCollection>,
    pub(crate) routines: Arc<CatalogCollection>,
    pub(crate) table_functions: Arc<CatalogCollection>,
    pub(crate) copy_functions: Arc<CatalogCollection>,
    pub(crate) sequences: Arc<CatalogCollection>,
    pub(crate) types: Arc<CatalogCollection>,
    pub(crate) collations: Arc<CatalogCollection>,
}

impl SchemaContents {
    pub fn new(
        catalog: &str,
        schema_name: &str,
        schema_object_id: CatalogObjectId,
        gc_epoch: Arc<AtomicU64>,
    ) -> Self {
        let schema_prefix = format!("{}.{}", catalog, schema_name);
        Self {
            tables: CatalogCollection::new(
                format!("{}.tables", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Tables),
                Arc::clone(&gc_epoch),
            ),
            views: CatalogCollection::new(
                format!("{}.views", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Views),
                Arc::clone(&gc_epoch),
            ),
            indexes: CatalogCollection::new(
                format!("{}.indexes", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Indexes),
                Arc::clone(&gc_epoch),
            ),
            property_graphs: CatalogCollection::new(
                format!("{}.property_graphs", schema_prefix),
                CollectionLockKey::schema_family(
                    schema_object_id,
                    CollectionFamily::PropertyGraphs,
                ),
                Arc::clone(&gc_epoch),
            ),
            functions: CatalogCollection::new(
                format!("{}.functions", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Functions),
                Arc::clone(&gc_epoch),
            ),
            routines: CatalogCollection::new(
                format!("{}.routines", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Routines),
                Arc::clone(&gc_epoch),
            ),
            table_functions: CatalogCollection::new(
                format!("{}.table_functions", schema_prefix),
                CollectionLockKey::schema_family(
                    schema_object_id,
                    CollectionFamily::TableFunctions,
                ),
                Arc::clone(&gc_epoch),
            ),
            copy_functions: CatalogCollection::new(
                format!("{}.copy_functions", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::CopyFunctions),
                Arc::clone(&gc_epoch),
            ),
            sequences: CatalogCollection::new(
                format!("{}.sequences", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Sequences),
                Arc::clone(&gc_epoch),
            ),
            types: CatalogCollection::new(
                format!("{}.types", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Types),
                Arc::clone(&gc_epoch),
            ),
            collations: CatalogCollection::new(
                format!("{}.collations", schema_prefix),
                CollectionLockKey::schema_family(schema_object_id, CollectionFamily::Collations),
                Arc::clone(&gc_epoch),
            ),
        }
    }

    pub fn collection(&self, entry_type: CatalogType) -> Option<&Arc<CatalogCollection>> {
        match entry_type {
            CatalogType::Table => Some(&self.tables),
            CatalogType::View => Some(&self.views),
            CatalogType::Index => Some(&self.indexes),
            CatalogType::PropertyGraph => Some(&self.property_graphs),
            CatalogType::Sequence => Some(&self.sequences),
            CatalogType::Routine => Some(&self.routines),
            CatalogType::ScalarFunction | CatalogType::AggregateFunction => Some(&self.functions),
            CatalogType::TableFunction => Some(&self.table_functions),
            CatalogType::CopyFunction => Some(&self.copy_functions),
            CatalogType::Type => Some(&self.types),
            CatalogType::Collation => Some(&self.collations),
            _ => None,
        }
    }

    fn write_entry_block<F>(
        &self,
        buffer: &mut Vec<u8>,
        collection: &Arc<CatalogCollection>,
        snapshot_ts: u64,
        serialize_entry: F,
    ) -> Result<()>
    where
        F: Fn(&CatalogEntryEnum) -> Result<Option<Vec<u8>>>,
    {
        let mut payloads = Vec::new();
        for entry in collection.scan(0, snapshot_ts) {
            if let Some(bytes) = serialize_entry(entry.as_ref())? {
                payloads.push(bytes);
            }
        }

        buffer.write_all(&(payloads.len() as u64).to_le_bytes())?;
        for payload in payloads {
            buffer.write_all(&(payload.len() as u32).to_le_bytes())?;
            buffer.write_all(&payload)?;
        }
        Ok(())
    }

    fn read_entry_block<F>(&self, cursor: &mut Cursor<&[u8]>, deserialize_entry: F) -> Result<()>
    where
        F: Fn(Vec<u8>) -> Result<Arc<CatalogEntryEnum>>,
    {
        let mut count_buf = [0u8; 8];
        cursor.read_exact(&mut count_buf)?;
        let count = u64::from_le_bytes(count_buf) as usize;

        for _ in 0..count {
            let mut len_buf = [0u8; 4];
            cursor.read_exact(&mut len_buf)?;
            let entry_len = u32::from_le_bytes(len_buf) as usize;
            let mut entry_bytes = vec![0u8; entry_len];
            cursor.read_exact(&mut entry_bytes)?;
            let entry = deserialize_entry(entry_bytes)?;
            if let Some(collection) = self.collection(entry.entry_type()) {
                collection.install_committed(entry, InstallMode::RejectExisting)?;
            } else {
                return Err(paro_error::internal(format!(
                    "No schema collection for entry type {:?}",
                    entry.entry_type()
                )));
            }
        }
        Ok(())
    }

    pub fn serialize_payload_at(&self, snapshot_ts: u64) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        self.write_entry_block(
            &mut buffer,
            &self.tables,
            snapshot_ts,
            |entry| match entry {
                CatalogEntryEnum::Table(table) => table.serialize().map(Some),
                _ => Ok(None),
            },
        )?;
        self.write_entry_block(
            &mut buffer,
            &self.indexes,
            snapshot_ts,
            |entry| match entry {
                CatalogEntryEnum::Index(index) => index.serialize_to_bytes().map(Some),
                _ => Ok(None),
            },
        )?;
        self.write_entry_block(
            &mut buffer,
            &self.property_graphs,
            snapshot_ts,
            |entry| match entry {
                CatalogEntryEnum::PropertyGraph(graph) => graph.serialize_to_bytes().map(Some),
                _ => Ok(None),
            },
        )?;
        self.write_entry_block(&mut buffer, &self.views, snapshot_ts, |entry| match entry {
            CatalogEntryEnum::View(view) => view.serialize_to_bytes().map(Some),
            _ => Ok(None),
        })?;
        self.write_entry_block(
            &mut buffer,
            &self.sequences,
            snapshot_ts,
            |entry| match entry {
                CatalogEntryEnum::Sequence(sequence) => sequence.serialize_to_bytes().map(Some),
                _ => Ok(None),
            },
        )?;
        self.write_entry_block(
            &mut buffer,
            &self.routines,
            snapshot_ts,
            |entry| match entry {
                CatalogEntryEnum::Routine(routine) => routine.serialize_to_bytes().map(Some),
                _ => Ok(None),
            },
        )?;

        Ok(buffer)
    }

    pub fn install_serialized_payload(
        &self,
        payload: &[u8],
        catalog: &str,
        meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Result<()> {
        let mut cursor = Cursor::new(payload);

        self.read_entry_block(&mut cursor, |entry_bytes| {
            let table = TableCatalogEntry::deserialize(
                &entry_bytes,
                catalog.to_string(),
                meta_manager.clone(),
            )?;
            Ok(Arc::new(CatalogEntryEnum::Table(Arc::new(table))))
        })?;
        self.read_entry_block(&mut cursor, |entry_bytes| {
            let index = IndexCatalogEntry::deserialize(&entry_bytes, catalog.to_string())?;
            Ok(Arc::new(CatalogEntryEnum::Index(Arc::new(index))))
        })?;
        self.read_entry_block(&mut cursor, |entry_bytes| {
            let graph = PropertyGraphCatalogEntry::deserialize(&entry_bytes, catalog.to_string())?;
            Ok(Arc::new(CatalogEntryEnum::PropertyGraph(Arc::new(graph))))
        })?;
        self.read_entry_block(&mut cursor, |entry_bytes| {
            let view = ViewCatalogEntry::deserialize(&entry_bytes, catalog.to_string())?;
            Ok(Arc::new(CatalogEntryEnum::View(Arc::new(view))))
        })?;
        self.read_entry_block(&mut cursor, |entry_bytes| {
            let sequence = SequenceCatalogEntry::deserialize(&entry_bytes, catalog.to_string())?;
            Ok(Arc::new(CatalogEntryEnum::Sequence(Arc::new(sequence))))
        })?;
        self.read_entry_block(&mut cursor, |entry_bytes| {
            let routine = RoutineCatalogEntry::deserialize(&entry_bytes, catalog.to_string())?;
            Ok(Arc::new(CatalogEntryEnum::Routine(Arc::new(routine))))
        })?;

        if cursor.position() != payload.len() as u64 {
            return Err(paro_error::internal(format!(
                "Schema contents payload has trailing bytes: {}",
                payload.len() as u64 - cursor.position()
            )));
        }

        Ok(())
    }

    pub(crate) fn gc(&self, watermark: u64) -> CatalogGcStats {
        let mut stats = CatalogGcStats::default();
        for collection in [
            &self.tables,
            &self.views,
            &self.indexes,
            &self.property_graphs,
            &self.functions,
            &self.routines,
            &self.table_functions,
            &self.copy_functions,
            &self.sequences,
            &self.types,
            &self.collations,
        ] {
            stats.merge(&collection.gc(watermark));
        }
        stats
    }
}
