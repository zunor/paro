// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! State objects used by the raw row append/scan substrate.

use crate::buffer::{BlockId, BufferHandle};
use paro_common::types::{ArrayType, LogicalType};
use paro_common::vector::{DecodedVector, SelectionVector, ValidityMask, Vector, VECTOR_SIZE};
use std::sync::Mutex;

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
    /// Combined decoded format for accessing combined list data
    pub combined_data: DecodedVector,
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
            combined_data: DecodedVector::empty(),
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

/// Vector format for raw row operations.
///
/// This structure provides decoded access to vector data, handling different
/// vector types (Flat, Constant, Dictionary) transparently through DecodedVector.
///
///
/// # Key Design
/// - `decoded`: Provides sel + data + validity for uniform access
/// - `original_sel`: Original selection vector (if any) for slice operations
/// - `children`: Nested formats for STRUCT/LIST/ARRAY types
/// - `combined_list_data`: Used for within-collection operations
#[derive(Debug)]
pub struct RawRowVectorFormat {
    /// Original selection vector pointer (for slice operations)
    pub original_sel: Option<SelectionVector>,
    /// Owned original selection vector
    pub original_owned_sel: Option<SelectionVector>,
    /// Decoded vector access for data reads
    pub decoded: DecodedVector,
    /// Child formats for nested types (STRUCT, LIST, ARRAY)
    pub children: Vec<RawRowVectorFormat>,
    /// Combined list data for within-collection operations
    pub combined_list_data: Option<Box<CombinedListData>>,
    /// Optional: list entries for ArrayVector (faked as list)
    pub array_list_entries: Option<Vec<ListEntry>>,
}

impl RawRowVectorFormat {
    /// Create a new RawRowVectorFormat from a vector.
    ///
    /// This converts the vector to decoded format and saves the original selection
    /// vector for slice operations.
    ///
    /// # Arguments
    /// * `vector` - The source vector
    /// * `count` - Number of elements to access
    ///
    pub fn from_vector(vector: &Vector, count: usize) -> Self {
        let mut format = Self::empty();
        Self::decode_internal(&mut format, vector, count);
        format
    }

    /// Internal recursive entry point for decoded format conversion.
    fn decode_internal(format: &mut Self, vector: &Vector, count: usize) {
        format.decoded = vector.decode(count);

        // Save original_sel from decoded format for slice operations.
        let original_sel = format.decoded.sel().clone();
        format.original_sel = Some(original_sel.clone());
        format.original_owned_sel = Some(original_sel);

        format.children.clear();
        format.combined_list_data = None;
        format.array_list_entries = None;

        match vector.logical_type() {
            LogicalType::Struct(_fields) => {
                if let Some(children) = vector.children() {
                    for child in children.iter() {
                        let mut child_format = Self::empty();
                        Self::decode_internal(&mut child_format, child, count);
                        format.children.push(child_format);
                    }
                }
            }
            LogicalType::List(_) => {
                if let Some(child) = vector.child() {
                    // For LIST, recurse with the child cardinality.
                    let child_count = child.len();
                    let mut child_format = Self::empty();
                    Self::decode_internal(&mut child_format, child, child_count);
                    format.children.push(child_format);
                }
            }
            LogicalType::Array(_, array_size) => {
                let array_size = *array_size;
                let child = paro_common::vector::ArrayVector::get_entry(vector);
                let child_count = child.len();
                let list_entry_count = Self::array_list_entry_count(
                    array_size,
                    child_count,
                    format.decoded.validity().capacity(),
                );

                // For ARRAY we fake list entries so collection logic can reuse LIST paths.
                let mut entries = Vec::with_capacity(list_entry_count);
                for i in 0..list_entry_count {
                    entries.push(ListEntry {
                        offset: i * array_size,
                        length: array_size,
                    });
                }
                format.array_list_entries = Some(entries);

                if let Some(entries) = format.array_list_entries.as_ref() {
                    format.decoded.set_data(entries.as_ptr() as *const u8);
                }

                let mut child_format = Self::empty();
                Self::decode_internal(&mut child_format, child, child_count);
                format.children.push(child_format);
            }
            _ => {}
        }
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

    /// Create an empty RawRowVectorFormat.
    pub fn empty() -> Self {
        Self {
            original_sel: None,
            original_owned_sel: None,
            decoded: DecodedVector::empty(),
            children: Vec::new(),
            combined_list_data: None,
            array_list_entries: None,
        }
    }

    /// Get the selection vector from decoded format.
    #[inline]
    pub fn sel(&self) -> &SelectionVector {
        self.decoded.sel()
    }

    /// Get the validity mask from decoded format.
    #[inline]
    pub fn validity(&self) -> &ValidityMask {
        self.decoded.validity()
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

impl Default for RawRowVectorFormat {
    fn default() -> Self {
        Self::empty()
    }
}

/// Per-chunk state for raw row operations.
///
/// Contains vectors for row/heap locations and temporary storage.
#[derive(Debug)]
pub struct RawRowChunkState {
    /// Vector data formats for each column.
    /// This provides decoded access to vector data regardless of vector type.
    ///
    pub vector_data: Vec<RawRowVectorFormat>,
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
        Self {
            vector_data: Vec::new(),
            column_ids: Vec::new(),
            row_locations: Vector::new(LogicalType::UBigInt), // Pointer as u64
            heap_locations: Vector::new(LogicalType::UBigInt),
            heap_sizes: Vector::new(LogicalType::UBigInt),
            utility_sel: SelectionVector::with_capacity(VECTOR_SIZE),
            chunk_part_indices: Vec::new(),
            array_cast_vectors: Vec::new(),
        }
    }

    /// Create a chunk state for specific columns.
    pub fn with_columns(column_ids: Vec<usize>) -> Self {
        Self {
            vector_data: Vec::new(),
            column_ids,
            row_locations: Vector::new(LogicalType::UBigInt),
            heap_locations: Vector::new(LogicalType::UBigInt),
            heap_sizes: Vector::new(LogicalType::UBigInt),
            utility_sel: SelectionVector::with_capacity(VECTOR_SIZE),
            chunk_part_indices: Vec::new(),
            array_cast_vectors: Vec::new(),
        }
    }

    /// Reset the chunk state for reuse.
    pub fn reset(&mut self) {
        self.vector_data.clear();
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
                self.array_cast_vectors[col_idx] =
                    Some(Vector::with_capacity(list_type, VECTOR_SIZE));
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

    /// Initialize vector_data for the given column count.
    ///
    /// This allocates empty RawRowVectorFormat entries for each column.
    /// Call `decode` to populate them with actual vector data.
    ///
    pub fn initialize_vector_data(&mut self, column_count: usize) {
        self.vector_data.clear();
        self.vector_data.reserve(column_count);
        for _ in 0..column_count {
            self.vector_data.push(RawRowVectorFormat::empty());
        }
    }

    /// Convert Chunk vectors to decoded format.
    ///
    /// This populates vector_data with DecodedVector for each column,
    /// enabling uniform access to vector data regardless of the underlying
    /// vector type (Flat, Constant, Dictionary, etc.).
    ///
    /// # Arguments
    /// * `chunk` - The Chunk to convert
    ///
    ///
    /// # Example
    /// ```ignore
    /// let chunk = Chunk::from_vectors(vec![vec1, vec2]);
    /// state.decode(&chunk);
    /// // Now state.vector_data[0] and state.vector_data[1] contain decoded formats
    /// ```
    pub fn decode(&mut self, chunk: &paro_common::chunk::Chunk) {
        let count = chunk.size();
        let column_count = chunk.column_count();

        // Ensure vector_data has space for all columns
        if self.vector_data.len() < column_count {
            self.vector_data
                .resize_with(column_count, RawRowVectorFormat::empty);
        }

        // Convert each column to decoded format
        for col_idx in 0..column_count {
            if let Some(vector) = chunk.column(col_idx) {
                self.vector_data[col_idx] = RawRowVectorFormat::from_vector(vector, count);
            }
        }
    }

    /// Get vector_data for a specific column.
    #[inline]
    pub fn get_vector_format(&self, col_idx: usize) -> Option<&RawRowVectorFormat> {
        self.vector_data.get(col_idx)
    }

    /// Get mutable vector_data for a specific column.
    #[inline]
    pub fn get_vector_format_mut(&mut self, col_idx: usize) -> Option<&mut RawRowVectorFormat> {
        self.vector_data.get_mut(col_idx)
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
    fn test_raw_row_vector_format_empty() {
        let format = RawRowVectorFormat::empty();
        assert!(format.children.is_empty());
        assert!(format.combined_list_data.is_none());
        assert!(format.array_list_entries.is_none());
    }

    #[test]
    fn test_raw_row_vector_format_from_flat_vector() {
        let vec = Vector::from_i32(&[10, 20, 30, 40]);
        let format = RawRowVectorFormat::from_vector(&vec, 4);

        // Check that decoded format is set up correctly
        assert!(format.is_valid(0));
        assert!(format.is_valid(1));
        assert!(format.is_valid(2));
        assert!(format.is_valid(3));

        // Check selection maps correctly (incremental for flat)
        assert_eq!(format.sel().get(0), 0);
        assert_eq!(format.sel().get(1), 1);
        assert_eq!(format.sel().get(2), 2);
        assert_eq!(format.sel().get(3), 3);
    }

    #[test]
    fn test_raw_row_vector_format_from_constant_vector() {
        use paro_common::types::LogicalType;

        let vec = Vector::constant(LogicalType::Integer, 42i32, 4);
        let format = RawRowVectorFormat::from_vector(&vec, 4);

        // For constant vectors, all selections point to 0
        assert_eq!(format.sel().get(0), 0);
        assert_eq!(format.sel().get(1), 0);
        assert_eq!(format.sel().get(2), 0);
        assert_eq!(format.sel().get(3), 0);

        // All values should be valid
        assert!(format.is_valid(0));
        assert!(format.is_valid(1));
        assert!(format.is_valid(2));
        assert!(format.is_valid(3));
    }

    #[test]
    fn test_raw_row_vector_format_array_sets_unified_data_to_list_entries() {
        use paro_common::types::LogicalType;
        use std::sync::Arc;

        let child = Arc::new(Vector::from_i32(&[1, 2, 3, 4, 5, 6]));
        let array = Vector::from_array(LogicalType::Integer, child, 2, 3);
        let format = RawRowVectorFormat::from_vector(&array, 2);

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
        assert_eq!(format.decoded.data(), expected_ptr);

        // Child format should recurse over the full flattened child.
        assert_eq!(format.children.len(), 1);
        assert_eq!(format.children[0].sel().len(), 6);
    }

    #[test]
    fn test_raw_row_vector_format_dictionary_array_entries_cover_selection() {
        use paro_common::types::LogicalType;
        use std::sync::Arc;

        let child = Arc::new(Vector::from_i32(&[10, 11, 20, 21, 30, 31]));
        let array = Arc::new(Vector::from_array(LogicalType::Integer, child, 3, 2));
        let dict_array = Vector::dictionary(array, vec![2]);

        let format = RawRowVectorFormat::from_vector(&dict_array, 1);
        let entries = format
            .array_list_entries
            .as_ref()
            .expect("array entries should be populated");

        let selected_idx = format.sel().get(0);
        assert!(selected_idx < entries.len());
        assert_eq!(entries[selected_idx].offset, selected_idx * 2);
        assert_eq!(entries[selected_idx].length, 2);

        let expected_ptr = entries.as_ptr() as *const u8;
        assert_eq!(format.decoded.data(), expected_ptr);
    }

    #[test]
    fn test_chunk_state_initialize_vector_data() {
        let mut state = RawRowChunkState::new();

        // Initially empty
        assert!(state.vector_data.is_empty());

        // Initialize for 3 columns
        state.initialize_vector_data(3);
        assert_eq!(state.vector_data.len(), 3);

        // Reset should clear
        state.reset();
        assert!(state.vector_data.is_empty());
    }

    #[test]
    fn test_chunk_state_decode() {
        use paro_common::chunk::Chunk;
        use std::sync::Arc;

        let mut state = RawRowChunkState::new();

        // Create a chunk with two columns
        let vec1 = Vector::from_i32(&[1, 2, 3]);
        let vec2 = Vector::from_i64(&[100, 200, 300]);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(vec1), Arc::new(vec2)]);

        // Convert to decoded format
        state.decode(&chunk);

        // Check that vector_data has 2 entries
        assert_eq!(state.vector_data.len(), 2);

        // Both formats should have valid entries
        assert!(state.get_vector_format(0).is_some());
        assert!(state.get_vector_format(1).is_some());

        // Check values through the format (using physical indices after selection mapping)
        let fmt0 = state.get_vector_format(0).unwrap();
        assert!(fmt0.is_valid(0));
        assert!(fmt0.is_valid(1));
        assert!(fmt0.is_valid(2));
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
