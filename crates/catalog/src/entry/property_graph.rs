// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Property graph catalog entry.

use super::catalog_entry::{
    AlterInfo, CatalogEntry, CatalogObjectId, CatalogObjectRef, CatalogType, CreateInfo,
    DependencyList, InCatalogEntry, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Write};
use std::sync::{Arc, LazyLock, Weak};

/// Vertex table definition for property graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VertexTableInfo {
    pub table_name: String,
    pub table_oid: u64,
    pub key_column_ids: Vec<u32>,
    pub label: String,
    pub property_column_ids: Vec<u32>,
}

/// Edge table definition for property graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTableInfo {
    pub table_name: String,
    pub table_oid: u64,
    pub key_column_ids: Vec<u32>,
    pub source_key_column_ids: Vec<u32>,
    pub source_vertex_table: String,
    pub source_ref_column_ids: Vec<u32>,
    pub destination_key_column_ids: Vec<u32>,
    pub destination_vertex_table: String,
    pub destination_ref_column_ids: Vec<u32>,
    pub label: String,
    pub property_column_ids: Vec<u32>,
}

/// Create info for property graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePropertyGraphInfo {
    pub catalog: String,
    pub schema: String,
    pub graph_name: String,
    pub if_not_exists: bool,
    pub vertex_tables: Vec<VertexTableInfo>,
    pub edge_tables: Vec<EdgeTableInfo>,
}

impl CreatePropertyGraphInfo {
    pub fn new(catalog: String, schema: String, graph_name: String) -> Self {
        Self {
            catalog,
            schema,
            graph_name,
            if_not_exists: false,
            vertex_tables: Vec::new(),
            edge_tables: Vec::new(),
        }
    }
}

/// Property graph catalog entry.
#[derive(Debug)]
pub struct PropertyGraphCatalogEntry {
    pub base: SchemaEntryMeta,
    pub info: CreatePropertyGraphInfo,
}

impl PropertyGraphCatalogEntry {
    pub fn build_dependencies(info: &CreatePropertyGraphInfo) -> DependencyList {
        let mut deps = DependencyList::new();
        for vertex in &info.vertex_tables {
            deps.add_regular(CatalogObjectRef::in_schema(
                CatalogObjectId::from_raw(vertex.table_oid),
                CatalogType::Table,
                info.catalog.clone(),
                None,
                info.schema.clone(),
                vertex.table_name.clone(),
            ));
        }
        for edge in &info.edge_tables {
            deps.add_regular(CatalogObjectRef::in_schema(
                CatalogObjectId::from_raw(edge.table_oid),
                CatalogType::Table,
                info.catalog.clone(),
                None,
                info.schema.clone(),
                edge.table_name.clone(),
            ));
        }
        deps
    }

    pub fn new(
        info: CreatePropertyGraphInfo,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Self {
        Self::with_object_id(info, timestamp, catalog, object_id)
    }

    pub fn with_object_id(
        info: CreatePropertyGraphInfo,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Self {
        let dependencies = Self::build_dependencies(&info);
        let base = SchemaEntryMeta::with_dependencies(
            CatalogType::PropertyGraph,
            catalog,
            info.schema.clone(),
            info.graph_name.clone(),
            object_id,
            timestamp,
            dependencies,
        );

        Self { base, info }
    }

    pub fn to_sql(&self) -> String {
        format!(
            "CREATE PROPERTY GRAPH {}.{};",
            self.base.schema_name, self.base.base.name
        )
    }

    /// Serialize to bytes for WAL/catalog persistence.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        buffer.write_all(&self.base.base.object_id.raw().to_le_bytes())?;
        buffer.write_all(&self.base.base.timestamp().to_le_bytes())?;

        write_string(&mut buffer, &self.base.base.name)?;
        write_string(&mut buffer, &self.base.schema_name)?;
        write_string(&mut buffer, &self.info.catalog)?;
        write_string(&mut buffer, &self.info.schema)?;
        write_string(&mut buffer, &self.info.graph_name)?;
        buffer.write_all(&[u8::from(self.info.if_not_exists)])?;

        buffer.write_all(&(self.info.vertex_tables.len() as u32).to_le_bytes())?;
        for vertex in &self.info.vertex_tables {
            write_string(&mut buffer, &vertex.table_name)?;
            buffer.write_all(&vertex.table_oid.to_le_bytes())?;
            write_u32_vec(&mut buffer, &vertex.key_column_ids)?;
            write_string(&mut buffer, &vertex.label)?;
            write_u32_vec(&mut buffer, &vertex.property_column_ids)?;
        }

        buffer.write_all(&(self.info.edge_tables.len() as u32).to_le_bytes())?;
        for edge in &self.info.edge_tables {
            write_string(&mut buffer, &edge.table_name)?;
            buffer.write_all(&edge.table_oid.to_le_bytes())?;
            write_u32_vec(&mut buffer, &edge.key_column_ids)?;
            write_u32_vec(&mut buffer, &edge.source_key_column_ids)?;
            write_string(&mut buffer, &edge.source_vertex_table)?;
            write_u32_vec(&mut buffer, &edge.source_ref_column_ids)?;
            write_u32_vec(&mut buffer, &edge.destination_key_column_ids)?;
            write_string(&mut buffer, &edge.destination_vertex_table)?;
            write_u32_vec(&mut buffer, &edge.destination_ref_column_ids)?;
            write_string(&mut buffer, &edge.label)?;
            write_u32_vec(&mut buffer, &edge.property_column_ids)?;
        }

        Ok(buffer)
    }

    /// Deserialize from bytes.
    pub fn deserialize(bytes: &[u8], catalog: String) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let oid = read_u64(&mut cursor)?;
        let timestamp = read_u64(&mut cursor)?;
        let graph_name = read_string(&mut cursor)?;
        let schema_name = read_string(&mut cursor)?;
        let info_catalog = read_string(&mut cursor)?;
        let info_schema = read_string(&mut cursor)?;
        let info_graph_name = read_string(&mut cursor)?;
        let if_not_exists = read_u8(&mut cursor)? == 1;

        let vertex_count = read_u32(&mut cursor)? as usize;
        let mut vertex_tables = Vec::with_capacity(vertex_count);
        for _ in 0..vertex_count {
            let table_name = read_string(&mut cursor)?;
            let table_oid = read_u64(&mut cursor)?;
            let key_column_ids = read_u32_vec(&mut cursor)?;
            let label = read_string(&mut cursor)?;
            let property_column_ids = read_u32_vec(&mut cursor)?;
            vertex_tables.push(VertexTableInfo {
                table_name,
                table_oid,
                key_column_ids,
                label,
                property_column_ids,
            });
        }

        let edge_count = read_u32(&mut cursor)? as usize;
        let mut edge_tables = Vec::with_capacity(edge_count);
        for _ in 0..edge_count {
            let table_name = read_string(&mut cursor)?;
            let table_oid = read_u64(&mut cursor)?;
            let key_column_ids = read_u32_vec(&mut cursor)?;
            let source_key_column_ids = read_u32_vec(&mut cursor)?;
            let source_vertex_table = read_string(&mut cursor)?;
            let source_ref_column_ids = read_u32_vec(&mut cursor)?;
            let destination_key_column_ids = read_u32_vec(&mut cursor)?;
            let destination_vertex_table = read_string(&mut cursor)?;
            let destination_ref_column_ids = read_u32_vec(&mut cursor)?;
            let label = read_string(&mut cursor)?;
            let property_column_ids = read_u32_vec(&mut cursor)?;
            edge_tables.push(EdgeTableInfo {
                table_name,
                table_oid,
                key_column_ids,
                source_key_column_ids,
                source_vertex_table,
                source_ref_column_ids,
                destination_key_column_ids,
                destination_vertex_table,
                destination_ref_column_ids,
                label,
                property_column_ids,
            });
        }

        let info = CreatePropertyGraphInfo {
            catalog: info_catalog,
            schema: info_schema,
            graph_name: info_graph_name,
            if_not_exists,
            vertex_tables,
            edge_tables,
        };

        let base = SchemaEntryMeta::with_dependencies(
            CatalogType::PropertyGraph,
            catalog,
            schema_name,
            graph_name,
            CatalogObjectId::from_raw(oid),
            timestamp,
            Self::build_dependencies(&info),
        );

        Ok(Self { base, info })
    }
}

impl CatalogEntry for PropertyGraphCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::PropertyGraph
    }

    fn catalog_name(&self) -> &str {
        &self.base.base.catalog
    }

    fn timestamp(&self) -> u64 {
        self.base.base.timestamp()
    }

    fn set_timestamp(&self, ts: u64) {
        self.base.base.set_timestamp(ts);
    }

    fn is_deleted(&self) -> bool {
        self.base.base.is_deleted()
    }

    fn set_deleted(&self, deleted: bool) {
        self.base.base.set_deleted(deleted);
    }

    fn child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.child()
    }

    fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>) {
        self.base.base.set_child(child);
    }

    fn parent(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.parent()
    }

    fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>) {
        self.base.base.set_parent(parent);
    }

    fn is_temporary(&self) -> bool {
        self.base.base.temporary
    }

    fn is_internal(&self) -> bool {
        self.base.base.internal
    }

    fn comment(&self) -> Option<&str> {
        None
    }

    fn set_comment(&self, comment: Option<String>) {
        self.base.base.set_comment(comment);
    }

    fn tags(&self) -> &HashMap<String, String> {
        static EMPTY: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn set_tags(&self, tags: HashMap<String, String>) {
        self.base.base.set_tags(tags);
    }

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER PROPERTY GRAPH"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("PROPERTY GRAPH copy"))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let mut info = CreateInfo::new(
            CatalogType::PropertyGraph,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        info.sql = Some(self.to_sql());
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

impl StandardEntry for PropertyGraphCatalogEntry {
    fn schema_name(&self) -> &str {
        &self.base.schema_name
    }

    fn dependencies(&self) -> &DependencyList {
        static EMPTY: LazyLock<DependencyList> = LazyLock::new(DependencyList::new);
        &EMPTY
    }

    fn set_dependencies(&self, dependencies: DependencyList) {
        self.base.set_dependencies(dependencies);
    }
}

impl InCatalogEntry for PropertyGraphCatalogEntry {}

pub fn graph_schema_fingerprint(info: &CreatePropertyGraphInfo) -> String {
    let mut hasher = DefaultHasher::new();
    info.catalog.hash(&mut hasher);
    info.schema.hash(&mut hasher);
    info.graph_name.hash(&mut hasher);

    for vertex in &info.vertex_tables {
        vertex.table_name.hash(&mut hasher);
        vertex.table_oid.hash(&mut hasher);
        vertex.label.hash(&mut hasher);
        vertex.key_column_ids.hash(&mut hasher);
        vertex.property_column_ids.hash(&mut hasher);
    }

    for edge in &info.edge_tables {
        edge.table_name.hash(&mut hasher);
        edge.table_oid.hash(&mut hasher);
        edge.label.hash(&mut hasher);
        edge.key_column_ids.hash(&mut hasher);
        edge.source_key_column_ids.hash(&mut hasher);
        edge.source_vertex_table.hash(&mut hasher);
        edge.source_ref_column_ids.hash(&mut hasher);
        edge.destination_key_column_ids.hash(&mut hasher);
        edge.destination_vertex_table.hash(&mut hasher);
        edge.destination_ref_column_ids.hash(&mut hasher);
        edge.property_column_ids.hash(&mut hasher);
    }

    format!("fp:{:016x}", hasher.finish())
}

fn write_string(buffer: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    buffer.write_all(&(bytes.len() as u32).to_le_bytes())?;
    buffer.write_all(bytes)?;
    Ok(())
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))
}

fn write_u32_vec(buffer: &mut Vec<u8>, values: &[u32]) -> Result<()> {
    buffer.write_all(&(values.len() as u32).to_le_bytes())?;
    for value in values {
        buffer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

fn read_u32_vec(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u32>> {
    let len = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_u32(cursor)?);
    }
    Ok(values)
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_info() -> CreatePropertyGraphInfo {
        CreatePropertyGraphInfo {
            catalog: "main".to_string(),
            schema: "public".to_string(),
            graph_name: "social_network".to_string(),
            if_not_exists: true,
            vertex_tables: vec![
                VertexTableInfo {
                    table_name: "person".to_string(),
                    table_oid: 1001,
                    key_column_ids: vec![0],
                    label: "Person".to_string(),
                    property_column_ids: vec![1, 2],
                },
                VertexTableInfo {
                    table_name: "company".to_string(),
                    table_oid: 1002,
                    key_column_ids: vec![0],
                    label: "Company".to_string(),
                    property_column_ids: vec![1],
                },
            ],
            edge_tables: vec![EdgeTableInfo {
                table_name: "works_at".to_string(),
                table_oid: 2001,
                key_column_ids: vec![0],
                source_key_column_ids: vec![1],
                source_vertex_table: "person".to_string(),
                source_ref_column_ids: vec![0],
                destination_key_column_ids: vec![2],
                destination_vertex_table: "company".to_string(),
                destination_ref_column_ids: vec![0],
                label: "WorksAt".to_string(),
                property_column_ids: vec![3],
            }],
        }
    }

    #[test]
    fn property_graph_entry_roundtrip() {
        let entry = PropertyGraphCatalogEntry::new(
            sample_info(),
            42,
            "main".to_string(),
            CatalogObjectId::from_raw(10_001),
        );
        let bytes = entry.serialize_to_bytes().unwrap();
        let restored = PropertyGraphCatalogEntry::deserialize(&bytes, "main".to_string()).unwrap();

        assert_eq!(restored.base.base.name, "social_network");
        assert_eq!(restored.object_id(), entry.object_id());
        assert_eq!(restored.base.schema_name, "public");
        assert_eq!(restored.base.base.timestamp(), 42);
        assert_eq!(restored.info, entry.info);
    }

    #[test]
    fn property_graph_entry_type_and_sql() {
        let entry = PropertyGraphCatalogEntry::new(
            sample_info(),
            7,
            "main".to_string(),
            CatalogObjectId::from_raw(10_002),
        );
        assert_eq!(entry.entry_type(), CatalogType::PropertyGraph);
        assert!(entry.to_sql().contains("CREATE PROPERTY GRAPH"));
    }
}
