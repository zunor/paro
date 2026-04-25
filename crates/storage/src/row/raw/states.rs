// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! State objects used by the raw row append/scan substrate.

use crate::buffer::{BlockId, BufferHandle};
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::error::Result;
use paro_common::types::{ArrayType, LogicalType};
use paro_common::vector::{
    DataRef, DecodedVectorRef, SelectionRef, SelectionVector, ValidityMask, Vector, VECTOR_SIZE,
};
use std::sync::{Arc, Mutex};

/// Block pinning behavior for raw row operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RawRowPinProperties {
    /// Invalid/uninitialized state
    #[default]
    Invalid,
    /// Keep all blocks pinned during operation (for both read/write)
    KeepEverythingPinned,
    /// Unpin blocks after they are processed (for both read/write)
    UnpinAfterDone,
    /// Destroy blocks after they are processed (read only)
    DestroyAfterDone,
    /// Assume blocks are already pinned (read only)
    AlreadyPinned,
}

/// Map from block id to buffer handle.
///
/// Uses a simple Vec instead of HashMap for small collections.
/// This avoids heap allocations per entry.
#[derive(Debug, Default)]
pub struct BufferHandleMap {
    /// `(block_id, handle)` pairs
    pub handles: Vec<(BlockId, BufferHandle)>,
}

impl BufferHandleMap {
    /// Create a new empty map.
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// Insert a handle for the given block id.
    ///
    /// # Panics
    /// Panics if a handle for this index already exists.
    pub fn insert(&mut self, index: BlockId, handle: BufferHandle) {
        debug_assert!(
            self.find(index).is_none(),
            "Handle for block {} already exists",
            index
        );
        self.handles.push((index, handle));
    }

    /// Find a handle by block id.
    pub fn find(&self, index: BlockId) -> Option<&BufferHandle> {
        self.handles
            .iter()
            .find(|(idx, _)| *idx == index)
            .map(|(_, h)| h)
    }

    /// Find a mutable handle by block id.
    pub fn find_mut(&mut self, index: BlockId) -> Option<&mut BufferHandle> {
        self.handles
            .iter_mut()
            .find(|(idx, _)| *idx == index)
            .map(|(_, h)| h)
    }

    /// Remove a handle by block id.
    pub fn remove(&mut self, index: BlockId) -> Option<BufferHandle> {
        if let Some(pos) = self.handles.iter().position(|(idx, _)| *idx == index) {
            Some(self.handles.remove(pos).1)
        } else {
            None
        }
    }

    /// Clear all handles.
    pub fn clear(&mut self) {
        self.handles.clear();
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Get the number of handles.
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Iterate over all handles.
    pub fn iter(&self) -> impl Iterator<Item = (BlockId, &BufferHandle)> {
        self.handles.iter().map(|(idx, h)| (*idx, h))
    }
}

/// Pin state for raw row operations.
///
/// Tracks which row and heap blocks are currently pinned.
#[derive(Debug, Default)]
pub struct RawRowPinState {
    /// Handles to pinned row blocks
    pub row_handles: BufferHandleMap,
    /// Handles to pinned heap blocks
    pub heap_handles: BufferHandleMap,
    /// Pin properties for this operation
    pub properties: RawRowPinProperties,
}

impl RawRowPinState {
    /// Create a new pin state with the given properties.
    pub fn new(properties: RawRowPinProperties) -> Self {
        Self {
            row_handles: BufferHandleMap::new(),
            heap_handles: BufferHandleMap::new(),
            properties,
        }
    }

    /// Reset the pin state, clearing all handles.
    pub fn reset(&mut self) {
        self.row_handles.clear();
        self.heap_handles.clear();
        // properties are not reset unless explicitly set
    }
}

/// Combined data for list/array types.
///
/// Used in within-collection operations to combine list entries from multiple rows.
///
#[derive(Debug)]
pub struct CombinedListData {
    /// Selection data for combined entries
    pub selection_data: Option<SelectionVector>,
    /// Combined list entries
    pub combined_list_entries: Vec<ListEntry>,
    /// Combined validity mask
    pub combined_validity: ValidityMask,
}

/// A list entry (offset, length) pair.
/// Used for LIST/ARRAY types.
#[derive(Debug, Clone, Copy, Default)]
pub struct ListEntry {
    pub offset: usize,
    pub length: usize,
}

impl CombinedListData {
    /// Create a new CombinedListData with default values.
    pub fn new() -> Self {
        Self {
            selection_data: None,
            combined_list_entries: vec![ListEntry::default(); VECTOR_SIZE],
            combined_validity: ValidityMask::new(VECTOR_SIZE),
        }
    }
}

impl Default for CombinedListData {
    fn default() -> Self {
        Self::new()
    }
}

/// Borrowed vector view for raw row operations.
///
/// Raw row append/scatter only needs a short-lived access layer over the source
/// chunk. Keeping this borrowed avoids materializing incremental, constant, or
/// range selections on every append.
#[derive(Debug)]
pub struct RawRowVectorView<'a> {
    /// Decoded vector access for data reads.
    pub decoded: DecodedVectorRef<'a>,
    /// Child views for nested types (STRUCT, LIST, ARRAY).
    pub children: Vec<RawRowVectorView<'a>>,
    /// Combined list data for within-collection operations.
    pub combined_list_data: Option<Box<CombinedListData>>,
    /// Optional list entries for ARRAY vectors, exposed through decoded.data.
    pub array_list_entries: Option<Vec<ListEntry>>,
}

impl<'a> RawRowVectorView<'a> {
    /// Create a borrowed raw-row view from a vector.
    pub fn try_from_vector(vector: &'a Vector, count: usize) -> Result<Self> {
        let mut decoded = vector.try_decode_ref(count)?;
        let mut children = Vec::new();
        let mut array_list_entries = None;

        match vector.logical_type() {
            LogicalType::Struct(_fields) => {
                if let Some(struct_children) = vector.children() {
                    children.reserve(struct_children.len());
                    for child in struct_children.iter() {
                        children.push(Self::try_from_vector(child, count)?);
                    }
                }
            }
            LogicalType::List(_) => {
                let base = Self::collection_base_vector(vector);
                if let Some(child) = base.child() {
                    children.push(Self::try_from_vector(child, child.len())?);
                }
            }
            LogicalType::Array(_, array_size) => {
                let array_size = *array_size;
                let child = paro_common::vector::ArrayVector::get_entry(vector);
                let child_count = child.len();
                let list_entry_count = Self::array_list_entry_count(
                    array_size,
                    child_count,
                    decoded.validity().capacity(),
                );

                let mut entries = Vec::with_capacity(list_entry_count);
                for i in 0..list_entry_count {
                    entries.push(ListEntry {
                        offset: i * array_size,
                        length: array_size,
                    });
                }

                decoded.set_data(DataRef::Ptr(entries.as_ptr() as *const u8));
                array_list_entries = Some(entries);
                children.push(Self::try_from_vector(child, child_count)?);
            }
            _ => {}
        }

        Ok(Self {
            decoded,
            children,
            combined_list_data: None,
            array_list_entries,
        })
    }

    fn collection_base_vector(vector: &'a Vector) -> &'a Vector {
        if vector.vector_type() == paro_common::vector::VectorType::Dictionary {
            let child = vector.child().expect("dictionary collection child");
            return Self::collection_base_vector(child);
        }
        vector
    }

    /// Determine how many fake list entries are needed for ARRAY.
    ///
    /// Uses template specialization to define different sort key layouts.
    /// to avoid out-of-bounds when selection vectors reference higher physical indices.
    fn array_list_entry_count(
        array_size: usize,
        child_size: usize,
        validity_capacity: usize,
    ) -> usize {
        if array_size == 0 {
            return validity_capacity;
        }
        let entries_for_child = child_size.saturating_add(array_size) / array_size;
        entries_for_child.max(validity_capacity)
    }

    /// Get the selection vector from decoded format.
    #[inline]
    pub fn sel(&self) -> &SelectionRef<'a> {
        self.decoded.sel()
    }

    /// Get the validity mask from decoded format.
    #[inline]
    pub fn validity(&self) -> &paro_common::vector::ValidityRef<'a> {
        self.decoded.validity()
    }

    /// Get the data reference from decoded format.
    #[inline]
    pub fn data(&self) -> DataRef {
        self.decoded.data()
    }

    /// Check if value at logical index is valid.
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.decoded.is_valid(idx)
    }

    /// Get typed data pointer.
    #[inline]
    pub fn get_data<T>(&self) -> *const T {
        self.decoded.get_data::<T>()
    }

    /// Get value at logical index.
    ///
    /// # Safety
    /// Caller must ensure T matches the actual data type.
    #[inline]
    pub unsafe fn get_value<T: Copy>(&self, idx: usize) -> T {
        self.decoded.get_value::<T>(idx)
    }
}

/// Borrowed decoded view for a raw-row source chunk.
#[derive(Debug)]
pub struct RawRowChunkView<'a> {
    /// Vector views for each column.
    pub vector_data: Vec<Option<RawRowVectorView<'a>>>,
}

impl<'a> RawRowChunkView<'a> {
    /// Build borrowed vector views for every column in a chunk.
    pub fn try_decode(chunk: &'a paro_common::chunk::Chunk) -> Result<Self> {
        let count = chunk.size();
        let mut vector_data = Vec::with_capacity(chunk.column_count());
        for col_idx in 0..chunk.column_count() {
            let view = chunk
                .column(col_idx)
                .map(|vector| RawRowVectorView::try_from_vector(vector, count))
                .transpose()?;
            vector_data.push(view);
        }
        Ok(Self { vector_data })
    }

    /// Get vector view for a specific column.
    #[inline]
    pub fn get_vector_format(&self, col_idx: usize) -> Option<&RawRowVectorView<'a>> {
        self.vector_data.get(col_idx).and_then(|view| view.as_ref())
    }

    /// Get mutable vector view for a specific column.
    #[inline]
    pub fn get_vector_format_mut(&mut self, col_idx: usize) -> Option<&mut RawRowVectorView<'a>> {
        self.vector_data
            .get_mut(col_idx)
            .and_then(|view| view.as_mut())
    }
}

/// Per-chunk state for raw row operations.
///
/// Contains vectors for row/heap locations and temporary storage.
#[derive(Debug)]
pub struct RawRowChunkState {
    /// Column indices to operate on
    pub column_ids: Vec<usize>,
    /// Row location pointers (POINTER type)
    pub row_locations: Vector,
    /// Heap location pointers (POINTER type)
    pub heap_locations: Vector,
    /// Heap sizes per row (UBIGINT type)
    pub heap_sizes: Vector,
    /// utility selection vector for operations
    pub utility_sel: SelectionVector,
    /// Chunk parts being processed (indices into segment's chunk_parts)
    pub chunk_part_indices: Vec<(usize, usize)>,
    /// Cached cast vectors for columns containing ARRAY types.
    /// Caches list-cast vectors for ARRAY gather/scatter paths.
    pub array_cast_vectors: Vec<Option<Vector>>,
}

impl Default for RawRowChunkState {
    fn default() -> Self {
        Self::new()
    }
}

impl RawRowChunkState {
    /// Create a new chunk state.
    pub fn new() -> Self {
        let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        Self {
            column_ids: Vec::new(),
            row_locations: Vector::try_new(LogicalType::UBigInt, VECTOR_SIZE, allocator.clone())
                .expect("row location vector allocation failed"),
            heap_locations: Vector::try_new(LogicalType::UBigInt, VECTOR_SIZE, allocator.clone())
                .expect("heap location vector allocation failed"),
            heap_sizes: Vector::try_new(LogicalType::UBigInt, VECTOR_SIZE, allocator.clone())
                .expect("heap size vector allocation failed"),
            utility_sel: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator)
                .expect("utility selection allocation failed"),
            chunk_part_indices: Vec::new(),
            array_cast_vectors: Vec::new(),
        }
    }

    /// Create a chunk state for specific columns.
    pub fn with_columns(column_ids: Vec<usize>) -> Self {
        let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        Self {
            column_ids,
            row_locations: Vector::try_new(LogicalType::UBigInt, VECTOR_SIZE, allocator.clone())
                .expect("row location vector allocation failed"),
            heap_locations: Vector::try_new(LogicalType::UBigInt, VECTOR_SIZE, allocator.clone())
                .expect("heap location vector allocation failed"),
            heap_sizes: Vector::try_new(LogicalType::UBigInt, VECTOR_SIZE, allocator.clone())
                .expect("heap size vector allocation failed"),
            utility_sel: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator)
                .expect("utility selection allocation failed"),
            chunk_part_indices: Vec::new(),
            array_cast_vectors: Vec::new(),
        }
    }

    /// Reset the chunk state for reuse.
    pub fn reset(&mut self) {
        self.chunk_part_indices.clear();
        for cached in self.array_cast_vectors.iter_mut() {
            if let Some(vector) = cached.as_mut() {
                vector.set_len(0);
            }
        }
        // Vectors are reused, no need to reset
    }

    /// Set the column IDs to operate on.
    pub fn set_column_ids(&mut self, column_ids: Vec<usize>) {
        self.column_ids = column_ids;
    }

    fn contains_array_type(logical_type: &LogicalType) -> bool {
        match logical_type {
            LogicalType::Array(_, _) => true,
            LogicalType::List(child) => Self::contains_array_type(child),
            LogicalType::Struct(fields) => fields
                .iter()
                .any(|(_, field_type)| Self::contains_array_type(field_type)),
            _ => false,
        }
    }

    /// Initialize cached ARRAY->LIST cast vectors for the selected columns.
    ///
    /// This initializes chunk-state where ARRAY columns cache
    /// list-cast vectors used by gather/scatter paths.
    pub fn initialize_array_cast_vectors(
        &mut self,
        column_types: &[LogicalType],
        column_ids: &[usize],
    ) {
        if self.array_cast_vectors.len() < column_types.len() {
            self.array_cast_vectors
                .resize_with(column_types.len(), || None);
        }

        for (col_idx, logical_type) in column_types.iter().enumerate() {
            if !column_ids.contains(&col_idx) {
                continue;
            }
            if Self::contains_array_type(logical_type) {
                let list_type = ArrayType::convert_to_list(logical_type);
                self.array_cast_vectors[col_idx] = Some(
                    Vector::try_new(list_type, VECTOR_SIZE, self.utility_sel.allocator().clone())
                        .expect("array cast vector allocation failed"),
                );
            } else {
                self.array_cast_vectors[col_idx] = None;
            }
        }
    }

    /// Get a mutable cached cast vector for a column.
    pub fn get_array_cast_vector_mut(&mut self, col_idx: usize) -> Option<&mut Vector> {
        self.array_cast_vectors
            .get_mut(col_idx)
            .and_then(|entry| entry.as_mut())
    }

    /// Create a borrowed decoded view for the current source chunk.
    pub fn try_decode<'a>(
        &mut self,
        chunk: &'a paro_common::chunk::Chunk,
    ) -> Result<RawRowChunkView<'a>> {
        RawRowChunkView::try_decode(chunk)
    }
}

/// Combined state for append operations.
#[derive(Debug, Default)]
pub struct RawRowAppendState {
    /// Pin state for the append operation
    pub pin_state: RawRowPinState,
    /// Chunk state for the append operation
    pub chunk_state: RawRowChunkState,
}

impl RawRowAppendState {
    /// Create a new append state.
    pub fn new() -> Self {
        Self {
            pin_state: RawRowPinState::default(),
            chunk_state: RawRowChunkState::new(),
        }
    }

    /// Create an append state with specific properties.
    pub fn with_properties(properties: RawRowPinProperties) -> Self {
        Self {
            pin_state: RawRowPinState::new(properties),
            chunk_state: RawRowChunkState::new(),
        }
    }
}

/// Combined state for scan operations.
#[derive(Debug)]
pub struct RawRowScanState {
    /// Pin state for the scan operation
    pub pin_state: RawRowPinState,
    /// Chunk state for the scan operation
    pub chunk_state: ChunkStateWithParts, // Updated to reference the right struct if needed
    /// Current segment index being scanned
    pub segment_index: Option<usize>,
    /// Current chunk index within the segment
    pub chunk_index: Option<usize>,
}

/// To match original file if I missed something
pub type ChunkStateWithParts = RawRowChunkState;

impl Default for RawRowScanState {
    fn default() -> Self {
        Self::new()
    }
}

impl RawRowScanState {
    /// Create a new scan state.
    pub fn new() -> Self {
        Self {
            pin_state: RawRowPinState::default(),
            chunk_state: RawRowChunkState::new(),
            segment_index: None,
            chunk_index: None,
        }
    }

    /// Create a scan state with specific properties.
    pub fn with_properties(properties: RawRowPinProperties) -> Self {
        Self {
            pin_state: RawRowPinState::new(properties),
            chunk_state: RawRowChunkState::new(),
            segment_index: None,
            chunk_index: None,
        }
    }

    /// Reset the scan state for a new scan.
    pub fn reset(&mut self) {
        self.pin_state.reset();
        self.chunk_state.reset();
        self.segment_index = None;
        self.chunk_index = None;
    }

    /// Check if the scan is complete.
    pub fn is_complete(&self) -> bool {
        // Will be set by RawRowCollection during scan
        false
    }
}

/// Parallel scan state with synchronization.
#[derive(Debug, Default)]
pub struct RawRowParallelScanState {
    /// The underlying scan state
    pub scan_state: RawRowScanState,
    /// Lock guarding assignment of scan work to local scan states.
    pub lock: Mutex<()>,
}

impl RawRowParallelScanState {
    /// Create a new parallel scan state.
    pub fn new() -> Self {
        Self {
            scan_state: RawRowScanState::new(),
            lock: Mutex::new(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_pin_properties_default() {
        let props = RawRowPinProperties::default();
        assert_eq!(props, RawRowPinProperties::Invalid);
    }

    #[test]
    fn test_buffer_handle_map() {
        // Test with real handles would require a buffer pool,
        // but we can test the structure logic here with invalid handles if needed.
        let mut map = BufferHandleMap::new();
        assert!(map.is_empty());

        map.insert(0, BufferHandle::invalid());
        map.insert(5, BufferHandle::invalid());

        assert_eq!(map.len(), 2);
        assert!(map.find(0).is_some());
        assert!(map.find(5).is_some());
        assert!(map.find(3).is_none());

        let removed = map.remove(0);
        assert!(removed.is_some());
        assert_eq!(map.len(), 1);
        assert!(map.find(0).is_none());

        map.clear();
        assert!(map.is_empty());
    }

    #[test]
    fn test_pin_state() {
        let mut state = RawRowPinState::new(RawRowPinProperties::KeepEverythingPinned);
        assert_eq!(state.properties, RawRowPinProperties::KeepEverythingPinned);
        assert!(state.row_handles.is_empty());
        assert!(state.heap_handles.is_empty());

        state.row_handles.insert(0, BufferHandle::invalid());
        assert_eq!(state.row_handles.len(), 1);

        state.reset();
        // properties are NOT reset
        assert_eq!(state.properties, RawRowPinProperties::KeepEverythingPinned);
        assert!(state.row_handles.is_empty());
    }

    #[test]
    fn test_chunk_state() {
        let state = RawRowChunkState::new();
        assert!(state.column_ids.is_empty());
        assert!(state.chunk_part_indices.is_empty());

        let state_with_cols = RawRowChunkState::with_columns(vec![0, 2, 3]);
        assert_eq!(state_with_cols.column_ids, vec![0, 2, 3]);
    }

    #[test]
    fn test_chunk_state_initialize_array_cast_vectors() {
        let mut state = RawRowChunkState::new();
        let types = vec![
            LogicalType::Integer,
            LogicalType::Array(Box::new(LogicalType::Integer), 2),
            LogicalType::Struct(vec![(
                "embedding".to_string(),
                LogicalType::Array(Box::new(LogicalType::Float), 3),
            )]),
        ];

        state.initialize_array_cast_vectors(&types, &[0, 1, 2]);
        assert!(state.array_cast_vectors[0].is_none());
        assert!(state.array_cast_vectors[1].is_some());
        assert!(state.array_cast_vectors[2].is_some());

        let cast_type_1 = state.array_cast_vectors[1]
            .as_ref()
            .unwrap()
            .logical_type()
            .clone();
        assert_eq!(
            cast_type_1,
            LogicalType::List(Box::new(LogicalType::Integer))
        );

        let cast_type_2 = state.array_cast_vectors[2]
            .as_ref()
            .unwrap()
            .logical_type()
            .clone();
        assert_eq!(cast_type_2, ArrayType::convert_to_list(&types[2]));
    }

    #[test]
    fn test_append_state() {
        let state = RawRowAppendState::new();
        assert_eq!(state.pin_state.properties, RawRowPinProperties::Invalid);

        let state = RawRowAppendState::with_properties(RawRowPinProperties::UnpinAfterDone);
        assert_eq!(
            state.pin_state.properties,
            RawRowPinProperties::UnpinAfterDone
        );
    }

    #[test]
    fn test_scan_state() {
        let mut state = RawRowScanState::new();
        assert!(state.segment_index.is_none());
        assert!(state.chunk_index.is_none());

        state.segment_index = Some(0);
        state.chunk_index = Some(5);

        state.reset();
        assert!(state.segment_index.is_none());
        assert!(state.chunk_index.is_none());
    }

    #[test]
    fn test_parallel_scan_state() {
        let state = RawRowParallelScanState::new();
        assert!(state.scan_state.segment_index.is_none());
    }

    #[test]
    fn test_raw_row_vector_view_from_flat_vector() {
        let vec = test_i32_vector(&[10, 20, 30, 40]);
        let format = RawRowVectorView::try_from_vector(&vec, 4).unwrap();

        assert!(format.is_valid(0));
        assert!(format.is_valid(1));
        assert!(format.is_valid(2));
        assert!(format.is_valid(3));

        assert_eq!(format.sel().get(0), 0);
        assert_eq!(format.sel().get(1), 1);
        assert_eq!(format.sel().get(2), 2);
        assert_eq!(format.sel().get(3), 3);
    }

    #[test]
    fn test_raw_row_vector_view_from_constant_vector() {
        use paro_common::types::LogicalType;

        let vec = test_constant_vector(LogicalType::Integer, 42i32, 4);
        let format = RawRowVectorView::try_from_vector(&vec, 4).unwrap();

        assert_eq!(format.sel().get(0), 0);
        assert_eq!(format.sel().get(1), 0);
        assert_eq!(format.sel().get(2), 0);
        assert_eq!(format.sel().get(3), 0);

        assert!(format.is_valid(0));
        assert!(format.is_valid(1));
        assert!(format.is_valid(2));
        assert!(format.is_valid(3));
    }

    #[test]
    fn test_raw_row_vector_view_array_sets_unified_data_to_list_entries() {
        use paro_common::types::LogicalType;
        use std::sync::Arc;

        let child = Arc::new(test_i32_vector(&[1, 2, 3, 4, 5, 6]));
        let array = paro_common::test_utils::test_array_vector(LogicalType::Integer, child, 2, 3);
        let format = RawRowVectorView::try_from_vector(&array, 2).unwrap();

        let entries = format
            .array_list_entries
            .as_ref()
            .expect("array entries should be populated");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[1].offset, 3);
        assert_eq!(entries[2].offset, 6);
        assert_eq!(entries[0].length, 3);

        // ARRAY should expose fake list_entry data through decoded.data.
        let expected_ptr = entries.as_ptr() as *const u8;
        assert!(matches!(
            format.decoded.data(),
            DataRef::Ptr(ptr) if ptr == expected_ptr
        ));

        // Child format should recurse over the full flattened child.
        assert_eq!(format.children.len(), 1);
        assert_eq!(format.children[0].sel().len(), 6);
    }

    #[test]
    fn test_raw_row_vector_view_dictionary_array_entries_cover_selection() {
        use paro_common::types::LogicalType;
        use std::sync::Arc;

        let child = Arc::new(test_i32_vector(&[10, 11, 20, 21, 30, 31]));
        let array = Arc::new(paro_common::test_utils::test_array_vector(
            LogicalType::Integer,
            child,
            3,
            2,
        ));
        let dict_array = paro_common::test_utils::test_dictionary(array, vec![2]);

        let format = RawRowVectorView::try_from_vector(&dict_array, 1).unwrap();
        let entries = format
            .array_list_entries
            .as_ref()
            .expect("array entries should be populated");

        let selected_idx = format.sel().get(0);
        assert!(selected_idx < entries.len());
        assert_eq!(entries[selected_idx].offset, selected_idx * 2);
        assert_eq!(entries[selected_idx].length, 2);

        let expected_ptr = entries.as_ptr() as *const u8;
        assert!(matches!(
            format.decoded.data(),
            DataRef::Ptr(ptr) if ptr == expected_ptr
        ));
    }

    #[test]
    fn test_chunk_state_decode_returns_borrowed_view() {
        use std::sync::Arc;

        let mut state = RawRowChunkState::new();

        // Create a chunk with two columns
        let vec1 = test_i32_vector(&[1, 2, 3]);
        let vec2 = test_i64_vector(&[100, 200, 300]);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(vec1), Arc::new(vec2)]);

        let view = state.try_decode(&chunk).unwrap();

        assert_eq!(view.vector_data.len(), 2);
        assert!(view.get_vector_format(0).is_some());
        assert!(view.get_vector_format(1).is_some());

        let fmt0 = view.get_vector_format(0).unwrap();
        assert!(fmt0.is_valid(0));
        assert!(fmt0.is_valid(1));
        assert!(fmt0.is_valid(2));
    }

    #[test]
    fn test_chunk_state_try_decode_keeps_symbolic_selections() {
        use std::sync::Arc;

        let flat = test_i64_vector(&[10, 20]);
        let constant = test_constant_vector(LogicalType::BigInt, 99_i64, 2);
        let range = test_i64_vector(&[1, 2, 3, 4])
            .slice_ref(1, 2)
            .expect("range slice");
        let chunk =
            test_chunk_from_arc_vectors(vec![Arc::new(flat), Arc::new(constant), Arc::new(range)]);

        let mut state = RawRowChunkState::new();
        let view = state.try_decode(&chunk).unwrap();

        assert!(matches!(
            view.get_vector_format(0).unwrap().sel(),
            SelectionRef::Incremental { count: 2 }
        ));
        assert!(matches!(
            view.get_vector_format(1).unwrap().sel(),
            SelectionRef::Constant { count: 2 }
        ));
        assert!(matches!(
            view.get_vector_format(2).unwrap().sel(),
            SelectionRef::Range {
                offset: 1,
                count: 2
            }
        ));
    }

    #[test]
    fn test_list_entry() {
        let entry = ListEntry {
            offset: 10,
            length: 5,
        };
        assert_eq!(entry.offset, 10);
        assert_eq!(entry.length, 5);

        let default_entry = ListEntry::default();
        assert_eq!(default_entry.offset, 0);
        assert_eq!(default_entry.length, 0);
    }

    #[test]
    fn test_combined_list_data() {
        let data = CombinedListData::new();
        assert!(data.selection_data.is_none());
        assert_eq!(data.combined_list_entries.len(), VECTOR_SIZE);
    }
}
