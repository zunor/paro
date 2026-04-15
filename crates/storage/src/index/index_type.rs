//! # Index Type
//!
//! Registration structure for index types with build and create callbacks.
//!
//! ## Design Notes
//!
//! The `IndexType` structure allows registering new index types with the system.
//! Each index type provides callbacks for:
//!
//! - Building indexes (bind, sort, init, sink, combine, finalize)
//! - Creating index instances
//! - Planning index creation
//!
//! This extensible design allows adding new index types (e.g., HNSW, B+Tree)
//! without modifying the core index infrastructure.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use crate::buffer::BufferManager;

use super::{BoundIndex, IndexConstraintType, IndexStorageInfo};
use crate::index::ColumnId;

// =============================================================================
// Index Type Info
// =============================================================================

/// Extra information for an index type.
///
/// Index types can store additional metadata here, such as distance metrics
/// for vector indexes or other type-specific configuration.
pub trait IndexTypeInfo: Send + Sync + Any {
    /// Returns self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;
}

// =============================================================================
// Build State Types
// =============================================================================

/// Bind data for index building.
///
/// Contains information gathered during the bind phase of index creation.
pub trait IndexBuildBindData: Send + Sync + Any {
    /// Returns self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns self as mutable Any for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Global state for index building.
///
/// Shared across all threads during parallel index construction.
pub trait IndexBuildGlobalState: Send + Sync + Any {
    /// Returns self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns self as mutable Any for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Local state for index building.
///
/// Per-thread state during parallel index construction.
pub trait IndexBuildLocalState: Send + Any {
    /// Returns self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns self as mutable Any for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// =============================================================================
// Input Structures
// =============================================================================

/// Input for creating an index instance.
pub struct CreateIndexInput<'a> {
    /// Constraint type for the index
    pub constraint_type: IndexConstraintType,
    /// Name of the index
    pub name: &'a str,
    /// Physical column IDs to index
    pub column_ids: &'a [ColumnId],
    /// Logical types of the indexed columns
    pub logical_types: &'a [LogicalType],
    /// Storage info for loading from disk (if any)
    pub storage_info: Option<&'a IndexStorageInfo>,
    /// Index-specific options
    pub options: &'a HashMap<String, Value>,
}

/// Input for the bind phase of index building.
pub struct IndexBuildBindInput<'a> {
    /// Name of the index being created
    pub index_name: &'a str,
    /// Physical column IDs to index
    pub column_ids: &'a [ColumnId],
    /// Logical types of the indexed columns
    pub logical_types: &'a [LogicalType],
    /// Index-specific options
    pub options: &'a HashMap<String, Value>,
}

/// Input for determining if sorting is needed.
pub struct IndexBuildSortInput<'a> {
    /// Bind data from the bind phase
    pub bind_data: Option<&'a dyn IndexBuildBindData>,
}

/// Input for initializing global build state.
pub struct IndexBuildInitGlobalStateInput<'a> {
    /// Bind data from the bind phase
    pub bind_data: Option<&'a dyn IndexBuildBindData>,
    /// Physical column IDs to index
    pub column_ids: &'a [ColumnId],
    /// Logical types of the indexed columns
    pub logical_types: &'a [LogicalType],
    /// Buffer manager for memory allocation
    pub buffer_manager: Arc<dyn BufferManager>,
}

/// Input for initializing local build state.
pub struct IndexBuildInitLocalStateInput<'a> {
    /// Bind data from the bind phase
    pub bind_data: Option<&'a dyn IndexBuildBindData>,
    /// Global state
    pub global_state: &'a dyn IndexBuildGlobalState,
}

/// Input for the sink phase of index building.
pub struct IndexBuildSinkInput<'a> {
    /// Bind data from the bind phase
    pub bind_data: Option<&'a dyn IndexBuildBindData>,
    /// Global state (mutable)
    pub global_state: &'a mut dyn IndexBuildGlobalState,
    /// Local state (mutable)
    pub local_state: &'a mut dyn IndexBuildLocalState,
}

/// Input for combining local states into global state.
pub struct IndexBuildCombineInput<'a> {
    /// Bind data from the bind phase
    pub bind_data: Option<&'a dyn IndexBuildBindData>,
    /// Global state (mutable)
    pub global_state: &'a mut dyn IndexBuildGlobalState,
    /// Local state to combine
    pub local_state: &'a mut dyn IndexBuildLocalState,
}

/// Input for finalizing index construction.
pub struct IndexBuildFinalizeInput<'a> {
    /// Global state containing the built index data
    pub global_state: &'a mut dyn IndexBuildGlobalState,
    /// Index name
    pub name: &'a str,
    /// Constraint type
    pub constraint_type: IndexConstraintType,
    /// Physical column IDs
    pub column_ids: &'a [ColumnId],
    /// Logical types
    pub logical_types: &'a [LogicalType],
}

/// Input for planning index creation.
pub struct PlanIndexInput<'a> {
    /// Index type info
    pub index_info: Option<&'a dyn IndexTypeInfo>,
    /// Index name
    pub index_name: &'a str,
    /// Constraint type
    pub constraint_type: IndexConstraintType,
}

// =============================================================================
// Callback Type Aliases
// =============================================================================

/// Callback for the bind phase of index building.
pub type IndexBuildBindFn = fn(input: &IndexBuildBindInput) -> Result<Box<dyn IndexBuildBindData>>;

/// Callback for determining if sorting is needed.
pub type IndexBuildSortFn = fn(input: &IndexBuildSortInput) -> bool;

/// Callback for initializing global build state.
pub type IndexBuildGlobalInitFn =
    fn(input: &IndexBuildInitGlobalStateInput) -> Result<Box<dyn IndexBuildGlobalState>>;

/// Callback for initializing local build state.
pub type IndexBuildLocalInitFn =
    fn(input: &IndexBuildInitLocalStateInput) -> Result<Box<dyn IndexBuildLocalState>>;

/// Callback for the sink phase (processing data chunks).
pub type IndexBuildSinkFn =
    fn(input: &mut IndexBuildSinkInput, key_chunk: &Chunk, row_ids: &[u64]) -> Result<()>;

/// Callback for combining local states.
pub type IndexBuildCombineFn = fn(input: &mut IndexBuildCombineInput) -> Result<()>;

/// Callback for finalizing index construction.
pub type IndexBuildFinalizeFn = fn(input: IndexBuildFinalizeInput) -> Result<Arc<dyn BoundIndex>>;

/// Callback for creating an index instance.
pub type IndexCreateFn = fn(input: &CreateIndexInput) -> Result<Arc<dyn BoundIndex>>;

/// Callback for planning index creation.
pub type IndexCreatePlanFn = fn(input: &PlanIndexInput) -> Result<()>;

// =============================================================================
// Index Type
// =============================================================================

/// An index type registration.
///
/// This structure contains all the callbacks needed to build and create
/// indexes of a specific type (e.g., ART, HNSW, B+Tree).
#[derive(Default)]
pub struct IndexType {
    /// Name of the index type (e.g., "ART", "HNSW")
    pub name: String,

    // Build callbacks
    /// Bind phase callback
    pub build_bind: Option<IndexBuildBindFn>,
    /// Sort determination callback
    pub build_sort: Option<IndexBuildSortFn>,
    /// Global state initialization callback
    pub build_global_init: Option<IndexBuildGlobalInitFn>,
    /// Local state initialization callback
    pub build_local_init: Option<IndexBuildLocalInitFn>,
    /// Sink phase callback
    pub build_sink: Option<IndexBuildSinkFn>,
    /// Combine callback
    pub build_combine: Option<IndexBuildCombineFn>,
    /// Finalize callback
    pub build_finalize: Option<IndexBuildFinalizeFn>,

    // Create callbacks
    /// Plan creation callback
    pub create_plan: Option<IndexCreatePlanFn>,
    /// Instance creation callback
    pub create_instance: Option<IndexCreateFn>,

    /// Extra information for the index type
    pub index_info: Option<Arc<dyn IndexTypeInfo>>,
}

impl IndexType {
    /// Creates a new IndexType with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Sets the build_bind callback.
    pub fn with_build_bind(mut self, f: IndexBuildBindFn) -> Self {
        self.build_bind = Some(f);
        self
    }

    /// Sets the build_sort callback.
    pub fn with_build_sort(mut self, f: IndexBuildSortFn) -> Self {
        self.build_sort = Some(f);
        self
    }

    /// Sets the build_global_init callback.
    pub fn with_build_global_init(mut self, f: IndexBuildGlobalInitFn) -> Self {
        self.build_global_init = Some(f);
        self
    }

    /// Sets the build_local_init callback.
    pub fn with_build_local_init(mut self, f: IndexBuildLocalInitFn) -> Self {
        self.build_local_init = Some(f);
        self
    }

    /// Sets the build_sink callback.
    pub fn with_build_sink(mut self, f: IndexBuildSinkFn) -> Self {
        self.build_sink = Some(f);
        self
    }

    /// Sets the build_combine callback.
    pub fn with_build_combine(mut self, f: IndexBuildCombineFn) -> Self {
        self.build_combine = Some(f);
        self
    }

    /// Sets the build_finalize callback.
    pub fn with_build_finalize(mut self, f: IndexBuildFinalizeFn) -> Self {
        self.build_finalize = Some(f);
        self
    }

    /// Sets the create_plan callback.
    pub fn with_create_plan(mut self, f: IndexCreatePlanFn) -> Self {
        self.create_plan = Some(f);
        self
    }

    /// Sets the create_instance callback.
    pub fn with_create_instance(mut self, f: IndexCreateFn) -> Self {
        self.create_instance = Some(f);
        self
    }

    /// Sets the index_info.
    pub fn with_index_info(mut self, info: Arc<dyn IndexTypeInfo>) -> Self {
        self.index_info = Some(info);
        self
    }

    /// Returns true if this index type supports building.
    pub fn supports_build(&self) -> bool {
        self.build_finalize.is_some()
    }

    /// Returns true if this index type supports direct creation.
    pub fn supports_create(&self) -> bool {
        self.create_instance.is_some()
    }
}

impl std::fmt::Debug for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexType")
            .field("name", &self.name)
            .field("has_build_bind", &self.build_bind.is_some())
            .field("has_build_sort", &self.build_sort.is_some())
            .field("has_build_global_init", &self.build_global_init.is_some())
            .field("has_build_local_init", &self.build_local_init.is_some())
            .field("has_build_sink", &self.build_sink.is_some())
            .field("has_build_combine", &self.build_combine.is_some())
            .field("has_build_finalize", &self.build_finalize.is_some())
            .field("has_create_plan", &self.create_plan.is_some())
            .field("has_create_instance", &self.create_instance.is_some())
            .field("has_index_info", &self.index_info.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_type_new() {
        let index_type = IndexType::new("TEST");
        assert_eq!(index_type.name, "TEST");
        assert!(!index_type.supports_build());
        assert!(!index_type.supports_create());
    }

    #[test]
    fn test_index_type_builder() {
        fn mock_finalize(_: IndexBuildFinalizeInput) -> Result<Arc<dyn BoundIndex>> {
            unimplemented!()
        }

        fn mock_create(_: &CreateIndexInput) -> Result<Arc<dyn BoundIndex>> {
            unimplemented!()
        }

        let index_type = IndexType::new("TEST")
            .with_build_finalize(mock_finalize)
            .with_create_instance(mock_create);

        assert!(index_type.supports_build());
        assert!(index_type.supports_create());
    }
}
