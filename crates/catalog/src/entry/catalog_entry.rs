//! Catalog Entry Base Types
//!
//! This module defines the core catalog entry traits and shared metadata types.

use paro_common::error::{self as paro_error, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};

// ============================================================================
// CatalogObjectId - stable persisted identity (same value as WAL / checkpoint)
// ============================================================================

/// Stable object identity for catalog objects; bitwise identical to the persisted `u64` in storage.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct CatalogObjectId(pub u64);

impl CatalogObjectId {
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Stable display metadata for a catalog object dependency.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CatalogObjectRef {
    pub id: CatalogObjectId,
    pub kind: CatalogType,
    pub catalog_name: String,
    pub schema_id: Option<CatalogObjectId>,
    pub schema_name: Option<String>,
    pub name: String,
}

impl CatalogObjectRef {
    pub fn new(
        id: CatalogObjectId,
        kind: CatalogType,
        catalog_name: String,
        schema_id: Option<CatalogObjectId>,
        schema_name: Option<String>,
        name: String,
    ) -> Self {
        Self {
            id,
            kind,
            catalog_name,
            schema_id,
            schema_name,
            name,
        }
    }

    pub fn schema(id: CatalogObjectId, catalog_name: String, name: String) -> Self {
        Self::new(id, CatalogType::Schema, catalog_name, None, None, name)
    }

    pub fn in_schema(
        id: CatalogObjectId,
        kind: CatalogType,
        catalog_name: String,
        schema_id: Option<CatalogObjectId>,
        schema_name: String,
        name: String,
    ) -> Self {
        Self::new(id, kind, catalog_name, schema_id, Some(schema_name), name)
    }

    pub fn display_name(&self) -> String {
        match &self.schema_name {
            Some(schema_name) => format!("{schema_name}.{}", self.name),
            None => self.name.clone(),
        }
    }

    pub fn serialize(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_all(&self.id.raw().to_le_bytes())?;
        writer.write_all(&[self.kind.to_byte()])?;
        writer.write_all(&self.schema_id.map(|id| id.raw()).unwrap_or(0).to_le_bytes())?;
        Self::write_string(writer, &self.catalog_name)?;
        Self::write_optional_string(writer, self.schema_name.as_deref())?;
        Self::write_string(writer, &self.name)?;
        Ok(())
    }

    pub fn deserialize(reader: &mut dyn Read) -> Result<Self> {
        let mut u64_buf = [0u8; 8];
        reader.read_exact(&mut u64_buf)?;
        let id = CatalogObjectId::from_raw(u64::from_le_bytes(u64_buf));

        let mut kind_buf = [0u8; 1];
        reader.read_exact(&mut kind_buf)?;
        let kind = CatalogType::from_byte(kind_buf[0])?;

        reader.read_exact(&mut u64_buf)?;
        let schema_id_raw = u64::from_le_bytes(u64_buf);
        let schema_id = (schema_id_raw != 0).then(|| CatalogObjectId::from_raw(schema_id_raw));

        let catalog_name = Self::read_string(reader, "dependency catalog name")?;
        let schema_name = Self::read_optional_string(reader, "dependency schema name")?;
        let name = Self::read_string(reader, "dependency object name")?;

        Ok(Self {
            id,
            kind,
            catalog_name,
            schema_id,
            schema_name,
            name,
        })
    }

    fn write_string(writer: &mut dyn Write, value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let len = u32::try_from(bytes.len()).map_err(|_| {
            paro_error::serialization_error(format!(
                "dependency field exceeds u32 length: {} bytes",
                bytes.len()
            ))
        })?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(bytes)?;
        Ok(())
    }

    fn write_optional_string(writer: &mut dyn Write, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => {
                writer.write_all(&[1])?;
                Self::write_string(writer, value)?;
            }
            None => writer.write_all(&[0])?,
        }
        Ok(())
    }

    fn read_string(reader: &mut dyn Read, label: &str) -> Result<String> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        reader.read_exact(&mut bytes)?;
        String::from_utf8(bytes).map_err(|error| {
            paro_error::serialization_error(format!("invalid UTF-8 in {label}: {error}"))
        })
    }

    fn read_optional_string(reader: &mut dyn Read, label: &str) -> Result<Option<String>> {
        let mut flag = [0u8; 1];
        reader.read_exact(&mut flag)?;
        if flag[0] == 0 {
            return Ok(None);
        }
        Self::read_string(reader, label).map(Some)
    }
}

// ============================================================================
// CatalogType - Entry type enumeration
// ============================================================================

/// The type of catalog entry.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CatalogType {
    Invalid,
    Table,
    Schema,
    View,
    Index,
    PropertyGraph,
    Sequence,
    ScalarFunction,
    AggregateFunction,
    TableFunction,
    Type,
    Collation,
    Database,
    Macro,
    Pragma,
    CopyFunction,
    Secret,
    DependencyEntry,
}

impl CatalogType {
    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            CatalogType::Invalid => "INVALID",
            CatalogType::Table => "TABLE",
            CatalogType::Schema => "SCHEMA",
            CatalogType::View => "VIEW",
            CatalogType::Index => "INDEX",
            CatalogType::PropertyGraph => "PROPERTY_GRAPH",
            CatalogType::Sequence => "SEQUENCE",
            CatalogType::ScalarFunction => "SCALAR_FUNCTION",
            CatalogType::AggregateFunction => "AGGREGATE_FUNCTION",
            CatalogType::TableFunction => "TABLE_FUNCTION",
            CatalogType::Type => "TYPE",
            CatalogType::Collation => "COLLATION",
            CatalogType::Database => "DATABASE",
            CatalogType::Macro => "MACRO",
            CatalogType::Pragma => "PRAGMA",
            CatalogType::CopyFunction => "COPY_FUNCTION",
            CatalogType::Secret => "SECRET",
            CatalogType::DependencyEntry => "DEPENDENCY_ENTRY",
        }
    }

    pub fn to_byte(self) -> u8 {
        match self {
            CatalogType::Invalid => 0,
            CatalogType::Table => 1,
            CatalogType::Schema => 2,
            CatalogType::View => 3,
            CatalogType::Index => 4,
            CatalogType::PropertyGraph => 5,
            CatalogType::Sequence => 6,
            CatalogType::ScalarFunction => 7,
            CatalogType::AggregateFunction => 8,
            CatalogType::TableFunction => 9,
            CatalogType::Type => 10,
            CatalogType::Collation => 11,
            CatalogType::Database => 12,
            CatalogType::Macro => 13,
            CatalogType::Pragma => 14,
            CatalogType::CopyFunction => 15,
            CatalogType::Secret => 16,
            CatalogType::DependencyEntry => 17,
        }
    }

    pub fn from_byte(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CatalogType::Invalid),
            1 => Ok(CatalogType::Table),
            2 => Ok(CatalogType::Schema),
            3 => Ok(CatalogType::View),
            4 => Ok(CatalogType::Index),
            5 => Ok(CatalogType::PropertyGraph),
            6 => Ok(CatalogType::Sequence),
            7 => Ok(CatalogType::ScalarFunction),
            8 => Ok(CatalogType::AggregateFunction),
            9 => Ok(CatalogType::TableFunction),
            10 => Ok(CatalogType::Type),
            11 => Ok(CatalogType::Collation),
            12 => Ok(CatalogType::Database),
            13 => Ok(CatalogType::Macro),
            14 => Ok(CatalogType::Pragma),
            15 => Ok(CatalogType::CopyFunction),
            16 => Ok(CatalogType::Secret),
            17 => Ok(CatalogType::DependencyEntry),
            other => Err(paro_error::serialization_error(format!(
                "invalid catalog type byte: {}",
                other
            ))),
        }
    }
}

impl std::fmt::Display for CatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================
// Dependency Types
// ============================================================================

/// Dependency type for catalog entries.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DependencyType {
    /// Regular dependency - blocks drop
    Regular,
    /// Automatic dependency - dropped automatically
    Automatic,
    /// Ownership dependency - owns the dependent
    Owns,
    /// Owned by dependency - owned by another entry
    OwnedBy,
}

impl DependencyType {
    pub fn to_byte(self) -> u8 {
        match self {
            DependencyType::Regular => 0,
            DependencyType::Automatic => 1,
            DependencyType::Owns => 2,
            DependencyType::OwnedBy => 3,
        }
    }

    pub fn from_byte(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Regular),
            1 => Ok(Self::Automatic),
            2 => Ok(Self::Owns),
            3 => Ok(Self::OwnedBy),
            other => Err(paro_error::serialization_error(format!(
                "invalid dependency type byte: {}",
                other
            ))),
        }
    }
}

/// Information about a catalog entry (for dependency tracking).
///
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogEntryInfo {
    /// Entry type
    pub entry_type: CatalogType,
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Entry name
    pub name: String,
}

impl CatalogEntryInfo {
    pub fn new(entry_type: CatalogType, catalog: String, schema: String, name: String) -> Self {
        Self {
            entry_type,
            catalog,
            schema,
            name,
        }
    }

    /// Create for schema-level entry
    pub fn for_schema(catalog: String, name: String) -> Self {
        Self::new(CatalogType::Schema, catalog, String::new(), name)
    }

    /// Create for table entry
    pub fn for_table(catalog: String, schema: String, name: String) -> Self {
        Self::new(CatalogType::Table, catalog, schema, name)
    }
}

/// A single dependency entry.
///
#[derive(Debug, Clone)]
pub struct Dependency {
    /// The catalog entry info this depends on
    pub entry: CatalogObjectRef,
    /// The type of dependency
    pub dependency_type: DependencyType,
}

impl Dependency {
    pub fn new(entry: CatalogObjectRef, dependency_type: DependencyType) -> Self {
        Self {
            entry,
            dependency_type,
        }
    }

    pub fn regular(entry: CatalogObjectRef) -> Self {
        Self::new(entry, DependencyType::Regular)
    }

    pub fn automatic(entry: CatalogObjectRef) -> Self {
        Self::new(entry, DependencyType::Automatic)
    }

    pub fn owns(entry: CatalogObjectRef) -> Self {
        Self::new(entry, DependencyType::Owns)
    }
}

/// List of dependencies for a catalog entry.
///
#[derive(Debug, Clone, Default)]
pub struct DependencyList {
    dependencies: Vec<Dependency>,
}

impl DependencyList {
    pub fn new() -> Self {
        Self {
            dependencies: Vec::new(),
        }
    }

    /// Add a dependency
    pub fn add_dependency(&mut self, entry: CatalogObjectRef, dep_type: DependencyType) {
        self.dependencies.push(Dependency::new(entry, dep_type));
    }

    /// Add a regular (blocking) dependency
    pub fn add_regular(&mut self, entry: CatalogObjectRef) {
        self.add_dependency(entry, DependencyType::Regular);
    }

    /// Check if contains a dependency
    pub fn contains(&self, entry: &CatalogObjectRef) -> bool {
        self.dependencies.iter().any(|d| d.entry.id == entry.id)
    }

    /// Get all dependencies
    pub fn dependencies(&self) -> &[Dependency] {
        &self.dependencies
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.dependencies.is_empty()
    }

    /// Get count
    pub fn len(&self) -> usize {
        self.dependencies.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Dependency> {
        self.dependencies.iter()
    }

    pub fn serialize(&self, writer: &mut dyn Write) -> Result<()> {
        let count = u32::try_from(self.dependencies.len()).map_err(|_| {
            paro_error::serialization_error(format!(
                "too many dependencies to serialize: {}",
                self.dependencies.len()
            ))
        })?;
        writer.write_all(&count.to_le_bytes())?;
        for dependency in &self.dependencies {
            dependency.entry.serialize(writer)?;
            writer.write_all(&[dependency.dependency_type.to_byte()])?;
        }
        Ok(())
    }

    pub fn deserialize(reader: &mut dyn Read) -> Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let count = u32::from_le_bytes(len_buf) as usize;
        let mut dependencies = Vec::with_capacity(count);
        for _ in 0..count {
            let entry = CatalogObjectRef::deserialize(reader)?;
            let mut dep_type_buf = [0u8; 1];
            reader.read_exact(&mut dep_type_buf)?;
            let dependency_type = DependencyType::from_byte(dep_type_buf[0])?;
            dependencies.push(Dependency::new(entry, dependency_type));
        }
        Ok(Self { dependencies })
    }

    /// Merge another dependency list
    pub fn merge(&mut self, other: &DependencyList) {
        for dep in &other.dependencies {
            if !self.dependencies.iter().any(|existing| {
                existing.entry.id == dep.entry.id && existing.dependency_type == dep.dependency_type
            }) {
                self.dependencies.push(dep.clone());
            }
        }
    }
}

// ============================================================================
// AlterInfo - Alter operation information
// ============================================================================

/// Type of ALTER operation.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterType {
    /// Invalid alter type
    Invalid,
    /// Alter table
    AlterTable,
    /// Alter view
    AlterView,
    /// Set comment
    SetComment,
    /// Set tags
    SetTags,
}

/// Information for ALTER operations.
///
#[derive(Debug, Clone)]
pub struct AlterInfo {
    /// Alter type
    pub alter_type: AlterType,
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Entry name
    pub name: String,
    /// If not found behavior
    pub if_not_found: OnEntryNotFound,
    /// New name (for RENAME)
    pub new_name: Option<String>,
    /// New comment (for SET COMMENT)
    pub new_comment: Option<String>,
    /// New tags (for SET TAGS)
    pub new_tags: Option<HashMap<String, String>>,
    /// Allow altering internal entries
    pub allow_internal: bool,
}

impl AlterInfo {
    pub fn new(alter_type: AlterType, catalog: String, schema: String, name: String) -> Self {
        Self {
            alter_type,
            catalog,
            schema,
            name,
            if_not_found: OnEntryNotFound::ThrowException,
            new_name: None,
            new_comment: None,
            new_tags: None,
            allow_internal: false,
        }
    }

    pub fn rename(catalog: String, schema: String, name: String, new_name: String) -> Self {
        let mut info = Self::new(AlterType::AlterTable, catalog, schema, name);
        info.new_name = Some(new_name);
        info
    }

    pub fn set_comment(catalog: String, schema: String, name: String, comment: String) -> Self {
        let mut info = Self::new(AlterType::SetComment, catalog, schema, name);
        info.new_comment = Some(comment);
        info
    }
}

/// Behavior when entry is not found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnEntryNotFound {
    #[default]
    ThrowException,
    ReturnNull,
}

// ============================================================================
// CreateInfo - Base for all create info types
// ============================================================================

/// Base information for CREATE operations.
///
#[derive(Debug, Clone)]
pub struct CreateInfo {
    /// Entry type
    pub entry_type: CatalogType,
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Entry name
    pub name: String,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// Whether this is temporary
    pub temporary: bool,
    /// Whether this is internal
    pub internal: bool,
    /// SQL string (for ToSQL)
    pub sql: Option<String>,
    /// Comment
    pub comment: Option<String>,
    /// Tags
    pub tags: HashMap<String, String>,
    /// Dependencies
    pub dependencies: DependencyList,
}

impl CreateInfo {
    pub fn new(entry_type: CatalogType, catalog: String, schema: String, name: String) -> Self {
        Self {
            entry_type,
            catalog,
            schema,
            name,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            temporary: false,
            internal: false,
            sql: None,
            comment: None,
            tags: HashMap::new(),
            dependencies: DependencyList::new(),
        }
    }
}

/// Behavior when creating an entry that already exists.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnCreateConflict {
    /// Throw an error
    #[default]
    ErrorOnConflict,
    /// Replace the existing entry
    ReplaceOnConflict,
    /// Ignore and return existing entry
    IgnoreOnConflict,
    /// Alter the existing entry (for functions)
    AlterOnConflict,
}

// ============================================================================
// CatalogEntry Trait - Core trait for all catalog entries
// ============================================================================

/// Core trait for all catalog entries.
///
///
/// This trait defines the interface that all catalog entries must implement,
/// including MVCC fields (timestamp, deleted, child, parent), metadata
/// (comment, tags), and behavior methods (alter, rollback, serialize, to_sql).
pub trait CatalogEntry: Send + Sync + Debug {
    // ========================================================================
    // Basic identification
    // ========================================================================

    /// Stable object identity (persisted oid).
    fn object_id(&self) -> CatalogObjectId;

    /// Get the entry's name.
    fn name(&self) -> &str;

    /// Get the entry type.
    fn entry_type(&self) -> CatalogType;

    /// Get the catalog (database) name this entry belongs to.
    fn catalog_name(&self) -> &str;

    // ========================================================================
    // MVCC fields
    // ========================================================================

    /// Get the timestamp when this entry was created/modified.
    fn timestamp(&self) -> u64;

    /// Set the timestamp.
    fn set_timestamp(&self, ts: u64);

    /// Check if this entry has been deleted.
    fn is_deleted(&self) -> bool;

    /// Mark this entry as deleted.
    fn set_deleted(&self, deleted: bool);

    // ========================================================================
    // Version chain (MVCC)
    // ========================================================================

    /// Get the child entry (newer version in the chain).
    fn child(&self) -> Option<Arc<dyn CatalogEntry>>;

    /// Set the child entry.
    fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>);

    /// Check if this entry has a child.
    fn has_child(&self) -> bool {
        self.child().is_some()
    }

    /// Get the parent entry (older version in the chain).
    fn parent(&self) -> Option<Arc<dyn CatalogEntry>>;

    /// Set the parent entry (weak reference to avoid cycles).
    fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>);

    /// Check if this entry has a parent.
    fn has_parent(&self) -> bool {
        self.parent().is_some()
    }

    // ========================================================================
    // Flags
    // ========================================================================

    /// Check if this is a temporary entry (not added to WAL).
    fn is_temporary(&self) -> bool;

    /// Check if this is an internal entry (cannot be deleted, not dumped).
    fn is_internal(&self) -> bool;

    // ========================================================================
    // Metadata (comment and tags)
    // ========================================================================

    /// Get the comment on this entry.
    fn comment(&self) -> Option<&str>;

    /// Set the comment on this entry.
    fn set_comment(&self, comment: Option<String>);

    /// Get the tags on this entry.
    fn tags(&self) -> &HashMap<String, String>;

    /// Set the tags on this entry.
    fn set_tags(&self, tags: HashMap<String, String>);

    // ========================================================================
    // Behavior methods
    // ========================================================================

    /// Create an altered copy of this entry.
    ///
    fn alter(&self, info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>>;

    /// Undo an alter operation.
    ///
    fn undo_alter(&self, info: &AlterInfo) -> Result<()>;

    /// Rollback changes to this entry.
    ///
    fn rollback(&self, prev_entry: &dyn CatalogEntry) -> Result<()>;

    /// Called when this entry is dropped.
    ///
    fn on_drop(&self) -> Result<()> {
        Ok(())
    }

    /// Create a copy of this entry.
    ///
    fn copy(&self) -> Result<Arc<dyn CatalogEntry>>;

    /// Get the CreateInfo for this entry.
    ///
    fn get_info(&self) -> Result<CreateInfo>;

    /// Set this entry as the root (newest entry in the chain).
    ///
    fn set_as_root(&self);

    /// Convert to SQL string for SHOW CREATE.
    ///
    fn to_sql(&self) -> String;

    /// Serialize this entry.
    ///
    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()>;

    /// Verify this entry's integrity.
    ///
    fn verify(&self) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// InCatalogEntry Trait - Entry that belongs to a catalog
// ============================================================================

/// Trait for entries that belong to a catalog (database).
///
pub trait InCatalogEntry: CatalogEntry {
    /// Get the parent catalog (database) name.
    fn parent_catalog(&self) -> &str {
        self.catalog_name()
    }
}

// ============================================================================
// StandardEntry Trait - Entry that belongs to a schema
// ============================================================================

/// Trait for standard entries that belong to a schema.
///
pub trait StandardEntry: InCatalogEntry {
    /// Get the parent schema name.
    fn schema_name(&self) -> &str;

    /// Get the dependencies of this entry.
    fn dependencies(&self) -> &DependencyList;

    /// Set the dependencies of this entry.
    fn set_dependencies(&self, dependencies: DependencyList);
}

// ============================================================================
// CatalogEntryMeta - Base struct for all catalog entries
// ============================================================================

/// Base struct containing common fields for all catalog entries.
///
///
/// This struct holds the MVCC fields (timestamp, deleted, child, parent)
/// and metadata (comment, tags).
#[derive(Debug)]
pub struct CatalogEntryMeta {
    /// Entry type
    pub entry_type: CatalogType,
    /// Entry name
    pub name: String,
    /// Catalog (database) name
    pub catalog: String,
    /// Stable object identity (persisted oid).
    pub object_id: CatalogObjectId,
    /// Internal flag (cannot be deleted, not dumped)
    pub internal: bool,
    /// Temporary flag (not added to WAL)
    pub temporary: bool,

    // MVCC fields
    /// Timestamp when created/modified
    timestamp: AtomicU64,
    /// Deleted flag
    deleted: AtomicBool,
    /// Child entry (newer version)
    child: RwLock<Option<Arc<dyn CatalogEntry>>>,
    /// Parent entry (older version, weak ref)
    parent: RwLock<Option<Weak<dyn CatalogEntry>>>,

    // Metadata
    /// Comment on this entry
    comment: RwLock<Option<String>>,
    /// Tags on this entry
    tags: RwLock<HashMap<String, String>>,
}

impl CatalogEntryMeta {
    /// Create a new catalog entry base.
    pub fn new(
        entry_type: CatalogType,
        catalog: String,
        name: String,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        Self {
            entry_type,
            name,
            catalog,
            object_id,
            internal: false,
            temporary: false,
            timestamp: AtomicU64::new(timestamp),
            deleted: AtomicBool::new(false),
            child: RwLock::new(None),
            parent: RwLock::new(None),
            comment: RwLock::new(None),
            tags: RwLock::new(HashMap::new()),
        }
    }

    /// Create with internal flag set.
    pub fn new_internal(
        entry_type: CatalogType,
        catalog: String,
        name: String,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        let mut base = Self::new(entry_type, catalog, name, object_id, timestamp);
        base.internal = true;
        base
    }

    // ========================================================================
    // MVCC field accessors
    // ========================================================================

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::SeqCst)
    }

    /// Set timestamp
    pub fn set_timestamp(&self, ts: u64) {
        self.timestamp.store(ts, Ordering::SeqCst);
    }

    /// Check if deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::SeqCst)
    }

    /// Mark as deleted
    pub fn set_deleted(&self, deleted: bool) {
        self.deleted.store(deleted, Ordering::SeqCst);
    }

    // ========================================================================
    // Version chain
    // ========================================================================

    /// Get child entry
    pub fn child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.child.read().ok()?.clone()
    }

    /// Set child entry
    pub fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>) {
        if let Ok(mut guard) = self.child.write() {
            *guard = child;
        }
    }

    /// Check if has child
    pub fn has_child(&self) -> bool {
        self.child.read().ok().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Take child entry (removes it)
    pub fn take_child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.child.write().ok()?.take()
    }

    /// Get parent entry
    pub fn parent(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.parent.read().ok()?.as_ref().and_then(|w| w.upgrade())
    }

    /// Set parent entry
    pub fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>) {
        if let Ok(mut guard) = self.parent.write() {
            *guard = parent;
        }
    }

    /// Check if has parent
    pub fn has_parent(&self) -> bool {
        self.parent().is_some()
    }

    // ========================================================================
    // Metadata
    // ========================================================================

    /// Get comment
    pub fn comment(&self) -> Option<String> {
        self.comment.read().ok()?.clone()
    }

    /// Set comment
    pub fn set_comment(&self, comment: Option<String>) {
        if let Ok(mut guard) = self.comment.write() {
            *guard = comment;
        }
    }

    /// Get tags (cloned)
    pub fn tags(&self) -> HashMap<String, String> {
        self.tags.read().ok().map(|g| g.clone()).unwrap_or_default()
    }

    /// Set tags
    pub fn set_tags(&self, tags: HashMap<String, String>) {
        if let Ok(mut guard) = self.tags.write() {
            *guard = tags;
        }
    }

    // ========================================================================
    // Serialization helpers
    // ========================================================================

    /// Serialize base fields
    pub fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        writer.write_all(&self.object_id.raw().to_le_bytes())?;
        // Timestamp
        writer.write_all(&self.timestamp().to_le_bytes())?;
        // Entry type
        writer.write_all(&(self.entry_type as u8).to_le_bytes())?;
        // Flags
        let flags: u8 = (self.internal as u8) | ((self.temporary as u8) << 1);
        writer.write_all(&[flags])?;
        // Name
        let name_bytes = self.name.as_bytes();
        writer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(name_bytes)?;
        // Catalog
        let catalog_bytes = self.catalog.as_bytes();
        writer.write_all(&(catalog_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(catalog_bytes)?;
        // Comment
        match self.comment() {
            Some(c) => {
                writer.write_all(&[1u8])?;
                let c_bytes = c.as_bytes();
                writer.write_all(&(c_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(c_bytes)?;
            }
            None => {
                writer.write_all(&[0u8])?;
            }
        }
        // Tags
        let tags = self.tags();
        writer.write_all(&(tags.len() as u32).to_le_bytes())?;
        for (k, v) in &tags {
            let k_bytes = k.as_bytes();
            writer.write_all(&(k_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(k_bytes)?;
            let v_bytes = v.as_bytes();
            writer.write_all(&(v_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(v_bytes)?;
        }

        Ok(())
    }
}

// ============================================================================
// SchemaEntryMeta - Base struct for schema-level entries
// ============================================================================

/// Base struct for entries that belong to a schema.
///
#[derive(Debug)]
pub struct SchemaEntryMeta {
    /// Base catalog entry
    pub base: CatalogEntryMeta,
    /// Schema name
    pub schema_name: String,
    /// Dependencies
    dependencies: RwLock<DependencyList>,
}

impl SchemaEntryMeta {
    /// Create a new standard entry base.
    pub fn new(
        entry_type: CatalogType,
        catalog: String,
        schema_name: String,
        name: String,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        Self {
            base: CatalogEntryMeta::new(entry_type, catalog, name, object_id, timestamp),
            schema_name,
            dependencies: RwLock::new(DependencyList::new()),
        }
    }

    /// Create with dependencies.
    pub fn with_dependencies(
        entry_type: CatalogType,
        catalog: String,
        schema_name: String,
        name: String,
        object_id: CatalogObjectId,
        timestamp: u64,
        dependencies: DependencyList,
    ) -> Self {
        Self {
            base: CatalogEntryMeta::new(entry_type, catalog, name, object_id, timestamp),
            schema_name,
            dependencies: RwLock::new(dependencies),
        }
    }

    /// Get dependencies
    pub fn dependencies(&self) -> DependencyList {
        self.dependencies
            .read()
            .ok()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    /// Set dependencies
    pub fn set_dependencies(&self, dependencies: DependencyList) {
        if let Ok(mut guard) = self.dependencies.write() {
            *guard = dependencies;
        }
    }
}

// ============================================================================
// OID Generation
// ============================================================================

/// Allocate the next object id from the process-wide allocator (new objects only).
#[inline]
pub fn allocate_object_id() -> CatalogObjectId {
    CatalogObjectId(crate::database_catalog::next_object_id())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_type_display() {
        assert_eq!(CatalogType::Table.as_str(), "TABLE");
        assert_eq!(CatalogType::Schema.as_str(), "SCHEMA");
        assert_eq!(CatalogType::Index.as_str(), "INDEX");
        assert_eq!(format!("{}", CatalogType::View), "VIEW");
    }

    #[test]
    fn test_catalog_entry_base() {
        let base = CatalogEntryMeta::new(
            CatalogType::Table,
            "main".to_string(),
            "test_table".to_string(),
            CatalogObjectId::from_raw(1),
            100,
        );
        assert_eq!(base.entry_type, CatalogType::Table);
        assert_eq!(base.name, "test_table");
        assert_eq!(base.catalog, "main");
        assert_eq!(base.object_id.raw(), 1);
        assert_eq!(base.timestamp(), 100);
        assert!(!base.is_deleted());
        assert!(!base.internal);
        assert!(!base.temporary);
    }

    #[test]
    fn test_mvcc_fields() {
        let base = CatalogEntryMeta::new(
            CatalogType::Table,
            "main".to_string(),
            "test".to_string(),
            CatalogObjectId::from_raw(1),
            100,
        );

        // Test timestamp
        assert_eq!(base.timestamp(), 100);
        base.set_timestamp(200);
        assert_eq!(base.timestamp(), 200);

        // Test deleted
        assert!(!base.is_deleted());
        base.set_deleted(true);
        assert!(base.is_deleted());

        // Test child/parent
        assert!(!base.has_child());
        assert!(!base.has_parent());
    }

    #[test]
    fn test_metadata() {
        let base = CatalogEntryMeta::new(
            CatalogType::Table,
            "main".to_string(),
            "test".to_string(),
            CatalogObjectId::from_raw(1),
            100,
        );

        // Test comment
        assert!(base.comment().is_none());
        base.set_comment(Some("Test comment".to_string()));
        assert_eq!(base.comment(), Some("Test comment".to_string()));

        // Test tags
        assert!(base.tags().is_empty());
        let mut tags = HashMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        base.set_tags(tags);
        assert_eq!(base.tags().get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_dependency_list() {
        let mut deps = DependencyList::new();
        assert!(deps.is_empty());

        let info = CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(42),
            CatalogType::Table,
            "main".to_string(),
            None,
            "public".to_string(),
            "t1".to_string(),
        );
        deps.add_regular(info.clone());

        assert!(!deps.is_empty());
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&info));
    }

    #[test]
    fn test_allocate_object_id() {
        let a = allocate_object_id();
        let b = allocate_object_id();
        assert!(b.raw() > a.raw());
    }

    #[test]
    fn test_standard_entry_base() {
        let base = SchemaEntryMeta::new(
            CatalogType::Table,
            "main".to_string(),
            "public".to_string(),
            "users".to_string(),
            CatalogObjectId::from_raw(1),
            100,
        );

        assert_eq!(base.base.name, "users");
        assert_eq!(base.schema_name, "public");
        assert!(base.dependencies().is_empty());

        // Add dependencies
        let mut deps = DependencyList::new();
        deps.add_regular(CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(99),
            CatalogType::Table,
            "main".to_string(),
            None,
            "public".to_string(),
            "other".to_string(),
        ));
        base.set_dependencies(deps);

        assert!(!base.dependencies().is_empty());
    }
}
