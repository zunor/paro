use crate::ddl::object_key::DdlObjectKey;
use crate::types::LogicalType;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DdlWalColumnInfo {
    pub name: String,
    pub logical_type: LogicalType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DdlWalConstraint {
    pub constraint_type: String,
    pub columns: Vec<u32>,
    pub expression: Option<String>,
    pub referenced_table: Option<String>,
    pub referenced_columns: Option<Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateSchemaPayload {
    #[serde(default)]
    pub object_id: u64,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropSchemaPayload {
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateTablePayload {
    #[serde(default)]
    pub object_id: u64,
    pub columns: Vec<DdlWalColumnInfo>,
    pub constraints: Vec<DdlWalConstraint>,
    pub if_not_exists: bool,
    pub storage: Option<DdlStorageDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropTablePayload {
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DdlStorageDescriptor {
    pub format_version: u16,
    pub tablet_id: u64,
    pub table_id: u64,
    pub partition_id: u64,
    pub schema_id: u64,
    pub schema_version: u32,
    pub schema_hash: u32,
    pub data_dir: String,
    pub keys_type: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateViewPayload {
    #[serde(default)]
    pub object_id: u64,
    pub sql: String,
    pub column_aliases: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<DdlDependencyRef>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropViewPayload {
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateIndexPayload {
    #[serde(default)]
    pub object_id: u64,
    pub table_name: String,
    pub column_ids: Vec<u32>,
    pub column_types: Vec<LogicalType>,
    pub index_type: String,
    pub is_unique: bool,
    pub if_not_exists: bool,
    pub fulltext_config: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropIndexPayload {
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreatePropertyGraphPayload {
    #[serde(default)]
    pub object_id: u64,
    pub schema: String,
    pub graph_name: String,
    pub if_not_exists: bool,
    pub vertex_tables: Vec<PropertyGraphVertexPayload>,
    pub edge_tables: Vec<PropertyGraphEdgePayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PropertyGraphVertexPayload {
    pub table_name: String,
    pub table_oid: u64,
    pub key_column_ids: Vec<u32>,
    pub label: String,
    pub property_column_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PropertyGraphEdgePayload {
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropPropertyGraphPayload {
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateSequencePayload {
    #[serde(default)]
    pub object_id: u64,
    pub if_not_exists: bool,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start_value: i64,
    pub cycle: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropSequencePayload {
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AlterEntryPayload {
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DdlDependencyObjectRef {
    pub object_id: u64,
    pub kind: String,
    pub catalog_name: String,
    pub schema_id: Option<u64>,
    pub schema_name: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DdlDependencyRef {
    pub object: DdlDependencyObjectRef,
    pub dependency_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DdlChange {
    CreateSchema(CreateSchemaPayload),
    DropSchema(DropSchemaPayload),
    CreateTable(CreateTablePayload),
    DropTable(DropTablePayload),
    CreateView(CreateViewPayload),
    DropView(DropViewPayload),
    CreateIndex(CreateIndexPayload),
    DropIndex(DropIndexPayload),
    CreatePropertyGraph(CreatePropertyGraphPayload),
    DropPropertyGraph(DropPropertyGraphPayload),
    CreateSequence(CreateSequencePayload),
    DropSequence(DropSequencePayload),
    AlterEntry(AlterEntryPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DdlChangeRecord {
    pub key: DdlObjectKey,
    pub change: DdlChange,
}
