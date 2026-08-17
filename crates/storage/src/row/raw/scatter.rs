// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scatter columnar chunks into the raw row backend.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, StringView};
use paro_common::vector::{
    DataRef, SelectionRef, SelectionVector, ValidityRef, Vector, VECTOR_SIZE,
};

use super::{
    ListEntry, RawRowChunkState, RawRowChunkView, RawRowCollection, RawRowLayout, RawRowPinState,
    RawRowSegment, RawRowVectorView,
};

// =============================================================================
// Core Functions
// =============================================================================

/// Build row storage for appending data.
///
/// This allocates space in the segment for the given number of rows
/// and returns pointers to the allocated locations.
///
/// # Arguments
/// * `pin_state` - Pin state for memory management
/// * `segment` - The segment to allocate in
/// * `layout` - The raw row layout
/// * `heap_sizes` - Heap sizes for each row (for variable-length data)
/// * `count` - Number of rows to allocate
///
/// # Returns
/// Tuple of (row_locations, heap_locations)
pub fn build_rows(
    pin_state: &mut RawRowPinState,
    segment: &mut RawRowSegment,
    layout: &RawRowLayout,
    heap_sizes: &[usize],
    count: usize,
) -> Result<(Vec<*mut u8>, Vec<*mut u8>)> {
    let mut row_locations = Vec::with_capacity(count);
    let mut heap_locations = Vec::with_capacity(count);

    if count == 0 {
        return Ok((row_locations, heap_locations));
    }

    let mut offset = 0;
    while offset < count {
        let remaining = count - offset;

        // Allocate space.
        let allocation = {
            let allocator = segment.allocator_mut();
            let accounting_class = allocator.accounting_class();
            let heap_sizes_opt = if layout.all_constant() {
                None
            } else {
                Some(&heap_sizes[offset..offset + remaining])
            };

            allocator.allocate_rows(remaining, heap_sizes_opt).map_err(|err| {
                paro_error::internal(format!(
                    "failed to allocate row slots: class={accounting_class:?}, offset={offset}, remaining={remaining}, count={count}, error={err}"
                ))
            })?
        };

        // Get pointers.
        let (current_row_locations, current_heap_locations) = {
            let allocator = segment.allocator_mut();
            let mut current_row_locs = Vec::with_capacity(allocation.count);
            let row_width = layout.get_row_width();
            for i in 0..allocation.count {
                let row_offset = allocation.row_block_offset + i * row_width;
                if let Ok(ptr) = allocator.get_row_pointer_pinned(
                    pin_state,
                    allocation.row_block_index,
                    row_offset,
                ) {
                    current_row_locs.push(ptr);
                }
            }

            let mut current_heap_locs = Vec::with_capacity(allocation.count);
            if let Some(heap_info) = &allocation.heap_info {
                let mut heap_offset = heap_info.heap_block_offset;
                for heap_size in heap_sizes[offset..offset + allocation.count].iter() {
                    if *heap_size > 0 {
                        if let Ok(ptr) = allocator.get_heap_pointer_pinned(
                            pin_state,
                            heap_info.heap_block_index,
                            heap_offset,
                        ) {
                            current_heap_locs.push(ptr);
                        } else {
                            current_heap_locs.push(std::ptr::null_mut());
                        }
                        heap_offset += *heap_size;
                    } else {
                        current_heap_locs.push(std::ptr::null_mut());
                    }
                }
            } else {
                current_heap_locs.resize(current_row_locs.len(), std::ptr::null_mut());
            }
            (current_row_locs, current_heap_locs)
        };

        row_locations.extend(current_row_locations);
        heap_locations.extend(current_heap_locations);

        // Split the allocation across logical chunks so each chunk stays within VECTOR_SIZE.
        let mut allocation_row_offset = 0usize;
        let mut allocation_heap_offset = allocation
            .heap_info
            .as_ref()
            .map(|info| info.heap_block_offset)
            .unwrap_or(0);
        while allocation_row_offset < allocation.count {
            let chunk_idx = segment.get_or_create_chunk_index();
            let chunk_row_count = segment
                .chunks
                .get(chunk_idx)
                .map(|chunk| VECTOR_SIZE.saturating_sub(chunk.count))
                .unwrap_or(VECTOR_SIZE);
            if chunk_row_count == 0 {
                continue;
            }

            let part_row_count = (allocation.count - allocation_row_offset).min(chunk_row_count);
            let part_heap_size = if layout.all_constant() {
                0
            } else {
                heap_sizes[offset + allocation_row_offset
                    ..offset + allocation_row_offset + part_row_count]
                    .iter()
                    .sum()
            };
            let row_block_offset =
                allocation.row_block_offset + allocation_row_offset * layout.get_row_width();
            let part = if let Some(heap_info) = &allocation.heap_info {
                super::RawRowChunkPart::new(
                    allocation.row_block_index as u32,
                    row_block_offset as u32,
                    if part_heap_size > 0 {
                        heap_info.heap_block_index as u32
                    } else {
                        u32::MAX
                    },
                    if part_heap_size > 0 {
                        allocation_heap_offset as u32
                    } else {
                        0
                    },
                    part_heap_size,
                    part_row_count as u32,
                )
            } else {
                super::RawRowChunkPart::new_without_heap(
                    allocation.row_block_index as u32,
                    row_block_offset as u32,
                    part_row_count as u32,
                )
            };

            if let Some(heap_info) = &allocation.heap_info {
                if part_heap_size > 0 {
                    let allocator = segment.allocator();
                    if let Ok(ptr) = allocator.get_heap_pointer_pinned(
                        pin_state,
                        heap_info.heap_block_index,
                        allocation_heap_offset,
                    ) {
                        *part.heap_base_address.lock().unwrap() = Some(ptr as usize);
                    }
                }
            }

            segment.add_part_to_chunk(chunk_idx, part);
            allocation_row_offset += part_row_count;
            allocation_heap_offset += part_heap_size;
        }

        offset += allocation.count;
    }

    Ok((row_locations, heap_locations))
}

/// Initialize validity masks to all-valid state.
fn initialize_validity_masks(row_locations: &[*mut u8], flag_width: usize, count: usize) {
    for row_location in row_locations.iter().take(count) {
        // SAFETY: row_locations[i] is valid and flag_width bytes are available
        unsafe {
            std::ptr::write_bytes(*row_location, 0xFF, flag_width);
        }
    }
}

// =============================================================================
// Compute Heap Sizes
// =============================================================================

/// Compute heap sizes for variable-length data in a Chunk.
///
/// This function correctly handles all vector types (Flat, Constant, Dictionary)
/// by using borrowed raw-row vector views for proper index mapping.
///
/// # Arguments
/// * `layout` - The raw row layout
/// * `chunk_view` - Borrowed decoded chunk view
/// * `append_sel` - Optional selection vector for appending
/// * `count` - Number of rows to process
///
/// # Returns
/// Vector of heap sizes for each row.
///
pub fn compute_heap_sizes(
    layout: &RawRowLayout,
    chunk_view: &mut RawRowChunkView<'_>,
    append_sel: Option<&SelectionVector>,
    count: usize,
) -> Vec<usize> {
    let mut heap_sizes = vec![0usize; count];

    if layout.all_constant() {
        return heap_sizes;
    }

    for col_idx in layout.get_variable_columns() {
        if let Some(format) = chunk_view.get_vector_format_mut(*col_idx) {
            compute_vector_heap_sizes(
                layout.get_types().get(*col_idx),
                format,
                &mut heap_sizes,
                append_sel,
                count,
            );
        }
    }

    heap_sizes
}

/// Compute heap sizes for a single vector.
fn compute_vector_heap_sizes(
    logical_type: Option<&LogicalType>,
    format: &mut RawRowVectorView<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let Some(logical_type) = logical_type else {
        return;
    };

    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            compute_string_heap_sizes(format, heap_sizes, append_sel, count);
        }
        LogicalType::Struct(fields) => {
            for (idx, (_, field_type)) in fields.iter().enumerate() {
                if let Some(child_format) = format.children.get_mut(idx) {
                    compute_vector_heap_sizes(
                        Some(field_type),
                        child_format,
                        heap_sizes,
                        append_sel,
                        count,
                    );
                }
            }
        }
        LogicalType::List(_) | LogicalType::Array(_, _) => {
            compute_collection_heap_sizes(logical_type, format, heap_sizes, append_sel, count);
        }
        _ => {}
    }
}

/// Compute heap sizes for string data.
///
/// This correctly handles CONSTANT and DICTIONARY vectors by using the
/// selection vector from DecodedVectorOwned to map indices.
///
/// Compute heap sizes for string data.
///
/// This correctly handles CONSTANT and DICTIONARY vectors by using the
/// selection vector from DecodedVectorOwned to map indices.
///
fn compute_string_heap_sizes(
    format: &mut RawRowVectorView<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_validity = format.validity();
    let has_append_sel = append_sel.is_some();
    let all_valid = source_validity.all_valid();

    match (has_append_sel, all_valid) {
        (true, true) => {
            compute_string_heap_sizes_internal::<true, true>(format, heap_sizes, append_sel, count)
        }
        (true, false) => {
            compute_string_heap_sizes_internal::<true, false>(format, heap_sizes, append_sel, count)
        }
        (false, true) => {
            compute_string_heap_sizes_internal::<false, true>(format, heap_sizes, append_sel, count)
        }
        (false, false) => compute_string_heap_sizes_internal::<false, false>(
            format, heap_sizes, append_sel, count,
        ),
    }
}

#[inline(always)]
fn compute_string_heap_sizes_internal<const HAS_APPEND_SEL: bool, const ALL_VALID: bool>(
    format: &mut RawRowVectorView<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_sel = format.sel();
    let source_validity = format.validity();

    for (i, heap_size) in heap_sizes.iter_mut().enumerate().take(count) {
        // Map logical index to append index
        let append_idx = if HAS_APPEND_SEL {
            unsafe { append_sel.unwrap_unchecked().get(i) }
        } else {
            i
        };

        // Map append index to physical source index via DecodedVectorOwned's selection
        let source_idx = source_sel.get(append_idx);

        let is_valid = if ALL_VALID {
            true
        } else {
            source_validity.is_valid(source_idx)
        };

        if !is_valid {
            continue;
        }

        // Get string using physical index
        let data_ptr = format.get_data::<paro_common::types::StringView>();
        if !data_ptr.is_null() {
            unsafe {
                let string_t = &*data_ptr.add(source_idx);
                let len = string_t.len();
                // Only non-inlined strings need heap space
                if !string_t.is_inlined() {
                    *heap_size += len;
                }
            }
        }
    }
}

struct CollectionListData<'a> {
    sel: SelectionRef<'a>,
    entries: Vec<ListEntry>,
    validity: ValidityRef<'a>,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RawListEntry {
    offset: u32,
    length: u32,
}

#[inline]
fn append_index(append_sel: Option<&SelectionVector>, idx: usize) -> usize {
    append_sel.map(|sel| sel.get(idx)).unwrap_or(idx)
}

#[inline]
fn is_valid_at(validity: &ValidityRef<'_>, idx: usize) -> bool {
    validity.all_valid() || (idx < validity.capacity() && validity.is_valid(idx))
}

fn max_selected_source_index(
    sel: &SelectionRef<'_>,
    append_sel: Option<&SelectionVector>,
    count: usize,
) -> usize {
    let mut max_idx = 0usize;
    for i in 0..count {
        let append_idx = append_index(append_sel, i);
        if append_idx >= sel.len() {
            continue;
        }
        max_idx = max_idx.max(sel.get(append_idx));
    }
    max_idx
}

fn get_collection_entries(
    format: &RawRowVectorView<'_>,
    append_sel: Option<&SelectionVector>,
    count: usize,
) -> Option<Vec<ListEntry>> {
    if let Some(entries) = format.array_list_entries.as_ref() {
        let required_len =
            max_selected_source_index(format.sel(), append_sel, count).saturating_add(1);
        let safe_len = required_len.min(entries.len());
        return Some(entries[..safe_len].to_vec());
    }

    let data_ptr = format.get_data::<RawListEntry>();
    if data_ptr.is_null() {
        return None;
    }

    let len = format.validity().capacity();
    if len == 0 {
        return Some(Vec::new());
    }

    let required_len = max_selected_source_index(format.sel(), append_sel, count).saturating_add(1);
    let safe_len = required_len.min(len);

    let mut entries = Vec::with_capacity(safe_len);
    for i in 0..safe_len {
        let raw = unsafe { std::ptr::read_unaligned(data_ptr.add(i)) };
        entries.push(ListEntry {
            offset: raw.offset as usize,
            length: raw.length as usize,
        });
    }
    Some(entries)
}

fn compute_fixed_within_collection_heap_sizes(
    logical_type: &LogicalType,
    list_data: &CollectionListData<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let value_size = RawRowLayout::get_type_size(logical_type);

    for (i, heap_size) in heap_sizes.iter_mut().enumerate().take(count) {
        let append_idx = append_index(append_sel, i);
        if append_idx >= list_data.sel.len() {
            continue;
        }
        let list_idx = list_data.sel.get(append_idx);
        if !is_valid_at(&list_data.validity, list_idx) {
            continue;
        }
        let Some(list_entry) = list_data.entries.get(list_idx) else {
            continue;
        };
        if list_entry.length == 0 {
            continue;
        }
        *heap_size += RawRowLayout::validity_mask_size(list_entry.length);
        *heap_size += list_entry.length * value_size;
    }
}

fn compute_string_within_collection_heap_sizes(
    format: &mut RawRowVectorView<'_>,
    list_data: &CollectionListData<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_sel = format.sel();
    let source_validity = format.validity();
    let source_data = format.get_data::<paro_common::types::StringView>();

    for (i, heap_size) in heap_sizes.iter_mut().enumerate().take(count) {
        let append_idx = append_index(append_sel, i);
        if append_idx >= list_data.sel.len() {
            continue;
        }
        let list_idx = list_data.sel.get(append_idx);
        if !is_valid_at(&list_data.validity, list_idx) {
            continue;
        }

        let Some(list_entry) = list_data.entries.get(list_idx) else {
            continue;
        };
        if list_entry.length == 0 {
            continue;
        }

        *heap_size += RawRowLayout::validity_mask_size(list_entry.length);
        *heap_size += list_entry.length * paro_common::types::StringView::SIZE;

        if source_data.is_null() {
            continue;
        }

        for child_i in 0..list_entry.length {
            let child_idx = list_entry.offset + child_i;
            if child_idx >= source_sel.len() {
                continue;
            }
            let source_idx = source_sel.get(child_idx);
            if !is_valid_at(source_validity, source_idx) {
                continue;
            }

            unsafe {
                let string_t = &*source_data.add(source_idx);
                let str_len = string_t.len();
                if str_len > StringView::INLINE_CAPACITY {
                    *heap_size += str_len;
                }
            }
        }
    }
}

fn compute_struct_within_collection_heap_sizes(
    fields: &[(String, LogicalType)],
    format: &mut RawRowVectorView<'_>,
    list_data: &CollectionListData<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    for (i, heap_size) in heap_sizes.iter_mut().enumerate().take(count) {
        let append_idx = append_index(append_sel, i);
        if append_idx >= list_data.sel.len() {
            continue;
        }
        let list_idx = list_data.sel.get(append_idx);
        if !is_valid_at(&list_data.validity, list_idx) {
            continue;
        }

        let Some(list_entry) = list_data.entries.get(list_idx) else {
            continue;
        };
        if list_entry.length == 0 {
            continue;
        }

        *heap_size += RawRowLayout::validity_mask_size(list_entry.length);
    }

    for (idx, (_, field_type)) in fields.iter().enumerate() {
        if let Some(child_format) = format.children.get_mut(idx) {
            compute_within_collection_heap_sizes(
                field_type,
                child_format,
                list_data,
                heap_sizes,
                append_sel,
                count,
            );
        }
    }
}

fn compute_collection_within_collection_heap_sizes(
    logical_type: &LogicalType,
    format: &mut RawRowVectorView<'_>,
    list_data: &CollectionListData<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let Some(child_type) = logical_type.element_type() else {
        return;
    };
    let Some(child_format) = format.children.first() else {
        return;
    };
    let Some(source_entries) = get_collection_entries(format, None, format.sel().len()) else {
        return;
    };
    let source_sel = format.sel();
    let source_validity = format.validity();

    for (i, heap_size) in heap_sizes.iter_mut().enumerate().take(count) {
        let append_idx = append_index(append_sel, i);
        if append_idx >= list_data.sel.len() {
            continue;
        }
        let list_idx = list_data.sel.get(append_idx);
        if !is_valid_at(&list_data.validity, list_idx) {
            continue;
        }
        let Some(list_entry) = list_data.entries.get(list_idx) else {
            continue;
        };
        if list_entry.length == 0 {
            continue;
        }

        // Child collection metadata in parent:
        // - validity mask for child collection entries
        // - per-entry pointer/slot area
        *heap_size += RawRowLayout::validity_mask_size(list_entry.length);
        *heap_size += list_entry.length * std::mem::size_of::<u64>();

        for child_i in 0..list_entry.length {
            let child_list_idx_idx = list_entry.offset + child_i;
            if child_list_idx_idx >= source_sel.len() {
                continue;
            }
            let child_list_idx = source_sel.get(child_list_idx_idx);
            if !is_valid_at(source_validity, child_list_idx) {
                continue;
            }
            let child_entry = source_entries
                .get(child_list_idx)
                .copied()
                .unwrap_or_default();
            *heap_size +=
                compute_single_collection_heap_size(child_type, child_format, child_entry);
        }
    }
}

fn compute_single_collection_heap_size(
    logical_type: &LogicalType,
    format: &RawRowVectorView<'_>,
    list_entry: ListEntry,
) -> usize {
    // Per collection entry header.
    let mut heap_size = std::mem::size_of::<u64>();
    if list_entry.length == 0 {
        return heap_size;
    }

    let source_sel = format.sel();
    let source_validity = format.validity();
    heap_size += RawRowLayout::validity_mask_size(list_entry.length);

    if RawRowLayout::type_is_constant_size(logical_type) {
        heap_size += list_entry.length * RawRowLayout::get_type_size(logical_type);
        return heap_size;
    }

    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            heap_size += list_entry.length * paro_common::types::StringView::SIZE;
            let source_data = format.get_data::<paro_common::types::StringView>();
            if source_data.is_null() {
                return heap_size;
            }

            for child_i in 0..list_entry.length {
                let child_idx = list_entry.offset + child_i;
                if child_idx >= source_sel.len() {
                    continue;
                }
                let source_idx = source_sel.get(child_idx);
                if !is_valid_at(source_validity, source_idx) {
                    continue;
                }
                unsafe {
                    let string_t = &*source_data.add(source_idx);
                    let str_len = string_t.len();
                    if str_len > StringView::INLINE_CAPACITY {
                        heap_size += str_len;
                    }
                }
            }
        }
        LogicalType::List(_) | LogicalType::Array(_, _) => {
            heap_size += list_entry.length * std::mem::size_of::<u64>();

            let Some(child_type) = logical_type.element_type() else {
                return heap_size;
            };
            let Some(child_format) = format.children.first() else {
                return heap_size;
            };
            let Some(source_entries) = get_collection_entries(format, None, source_sel.len())
            else {
                return heap_size;
            };

            for child_i in 0..list_entry.length {
                let child_idx = list_entry.offset + child_i;
                if child_idx >= source_sel.len() {
                    continue;
                }
                let source_idx = source_sel.get(child_idx);
                if !is_valid_at(source_validity, source_idx) {
                    continue;
                }
                let child_entry = source_entries.get(source_idx).copied().unwrap_or_default();
                heap_size +=
                    compute_single_collection_heap_size(child_type, child_format, child_entry);
            }
        }
        _ => {}
    }

    heap_size
}

fn compute_within_collection_heap_sizes(
    logical_type: &LogicalType,
    format: &mut RawRowVectorView<'_>,
    list_data: &CollectionListData<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    if RawRowLayout::type_is_constant_size(logical_type) {
        compute_fixed_within_collection_heap_sizes(
            logical_type,
            list_data,
            heap_sizes,
            append_sel,
            count,
        );
        return;
    }

    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            compute_string_within_collection_heap_sizes(
                format, list_data, heap_sizes, append_sel, count,
            );
        }
        LogicalType::Struct(fields) => {
            compute_struct_within_collection_heap_sizes(
                fields, format, list_data, heap_sizes, append_sel, count,
            );
        }
        LogicalType::List(_) | LogicalType::Array(_, _) => {
            compute_collection_within_collection_heap_sizes(
                logical_type,
                format,
                list_data,
                heap_sizes,
                append_sel,
                count,
            );
        }
        _ => {}
    }
}

/// Compute heap sizes for collection types (LIST, ARRAY).
fn compute_collection_heap_sizes(
    logical_type: &LogicalType,
    format: &mut RawRowVectorView<'_>,
    heap_sizes: &mut [usize],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let Some(entries) = get_collection_entries(format, append_sel, count) else {
        return;
    };
    let source_sel = format.sel().clone();
    let source_validity = format.validity().clone();
    let entries = entries.to_vec();

    for (i, heap_size) in heap_sizes.iter_mut().enumerate().take(count) {
        let append_idx = append_index(append_sel, i);
        if append_idx >= source_sel.len() {
            continue;
        }
        let source_idx = source_sel.get(append_idx);
        if is_valid_at(&source_validity, source_idx) {
            *heap_size += std::mem::size_of::<u64>();
        }
    }

    let Some(child_type) = logical_type.element_type() else {
        return;
    };
    let Some(child_format) = format.children.get_mut(0) else {
        return;
    };

    let list_data = CollectionListData {
        sel: source_sel,
        entries,
        validity: source_validity,
    };

    compute_within_collection_heap_sizes(
        child_type,
        child_format,
        &list_data,
        heap_sizes,
        append_sel,
        count,
    );
}

// =============================================================================
// Scatter Functions
// =============================================================================

/// Scatter a Chunk to row-based storage.
///
/// This is the main entry point for writing columnar data to rows.
/// It correctly handles all vector types (Flat, Constant, Dictionary)
/// by using borrowed raw-row vector views.
///
/// # Arguments
/// * `layout` - The raw row layout
/// * `chunk` - The data chunk (needed for column type access)
/// * `chunk_view` - Borrowed decoded chunk view
/// * `row_locations` - Pointers to row storage
/// * `heap_locations` - Pointers to heap storage
/// * `append_sel` - Optional selection vector
/// * `count` - Number of rows to scatter
///
pub fn scatter_chunk(
    layout: &RawRowLayout,
    chunk: &Chunk,
    chunk_view: &mut RawRowChunkView<'_>,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    // Initialize validity mask for each row (all valid initially)
    if !layout.all_valid() {
        initialize_validity_masks(row_locations, layout.get_flag_width(), count);
    }

    #[cfg(debug_assertions)]
    let original_heap_locations = if !layout.all_constant() {
        Some(heap_locations.to_vec())
    } else {
        None
    };

    // Set heap sizes if we have variable-length data
    let heap_sizes_vec = if !layout.all_constant() {
        Some(compute_heap_sizes(layout, chunk_view, append_sel, count))
    } else {
        None
    };
    if let Some(heap_sizes) = heap_sizes_vec.as_ref() {
        if let Some(heap_offset) = layout.get_heap_size_offset() {
            for i in 0..count {
                unsafe {
                    let ptr = row_locations[i].add(heap_offset);
                    std::ptr::write(ptr as *mut u64, heap_sizes[i] as u64);
                }
            }
        }
    }

    // Scatter each column using decoded vector access.
    for col_idx in 0..layout.column_count() {
        if let Some(vector) = chunk.column(col_idx) {
            if let Some(format) = chunk_view.get_vector_format(col_idx) {
                let offset = layout.get_offsets()[col_idx];
                scatter_vector(
                    layout,
                    vector.as_ref(),
                    format,
                    col_idx,
                    offset,
                    row_locations,
                    heap_locations,
                    append_sel,
                    count,
                );
            }
        }
    }

    #[cfg(debug_assertions)]
    if let (Some(heap_sizes), Some(original_heap_locations)) =
        (heap_sizes_vec.as_ref(), original_heap_locations.as_ref())
    {
        for i in 0..count {
            if heap_sizes[i] == 0 || original_heap_locations[i].is_null() {
                continue;
            }
            let expected = unsafe { original_heap_locations[i].add(heap_sizes[i]) };
            debug_assert_eq!(
                heap_locations[i],
                expected,
                "Heap write mismatch at row {i}: expected advance {}, got {}",
                heap_sizes[i],
                (heap_locations[i] as usize).saturating_sub(original_heap_locations[i] as usize)
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn scatter_vector(
    layout: &RawRowLayout,
    vector: &Vector,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let logical_type = vector.logical_type();

    match logical_type {
        // Fixed-size numeric types
        LogicalType::Boolean => scatter_fixed::<u8>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::TinyInt => scatter_fixed::<i8>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::UTinyInt => scatter_fixed::<u8>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::SmallInt => scatter_fixed::<i16>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::USmallInt => scatter_fixed::<u16>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Integer => scatter_fixed::<i32>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::UInteger => scatter_fixed::<u32>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::BigInt => scatter_fixed::<i64>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::UBigInt => scatter_fixed::<u64>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Float => scatter_fixed::<f32>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Double => scatter_fixed::<f64>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Date => scatter_fixed::<i32>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Time | LogicalType::TimestampTz => scatter_fixed::<i64>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Timestamp => scatter_fixed::<i64>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::HugeInt => scatter_fixed::<i128>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::UHugeInt => scatter_fixed::<u128>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Uuid => scatter_fixed::<u128>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Interval => scatter_fixed::<i128>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                scatter_fixed::<i64>(
                    layout,
                    format,
                    col_idx,
                    offset,
                    row_locations,
                    append_sel,
                    count,
                );
            } else {
                scatter_fixed::<i128>(
                    layout,
                    format,
                    col_idx,
                    offset,
                    row_locations,
                    append_sel,
                    count,
                );
            }
        }

        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            scatter_string(
                layout,
                format,
                col_idx,
                offset,
                row_locations,
                heap_locations,
                append_sel,
                count,
            );
        }

        LogicalType::Array(_, _) => {
            array_row_scatter(
                layout,
                format,
                col_idx,
                offset,
                row_locations,
                heap_locations,
                append_sel,
                count,
                logical_type,
            );
        }
        LogicalType::Struct(fields) => {
            scatter_struct(
                layout,
                vector,
                format,
                col_idx,
                offset,
                row_locations,
                heap_locations,
                append_sel,
                count,
                fields,
            );
        }
        LogicalType::List(_) => {
            list_row_scatter(
                layout,
                format,
                col_idx,
                offset,
                row_locations,
                heap_locations,
                append_sel,
                count,
                logical_type,
            );
        }
        _ => {
            let type_size = RawRowLayout::get_type_size(logical_type);
            for row_location in row_locations.iter().take(count) {
                unsafe {
                    std::ptr::write_bytes(row_location.add(offset), 0, type_size);
                }
            }
        }
    }
}

/// Scatter Struct values to row storage.
#[allow(clippy::too_many_arguments)]
fn scatter_struct(
    layout: &RawRowLayout,
    vector: &Vector,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
    fields: &[(String, LogicalType)],
) {
    let source_validity = format.validity();
    let has_append_sel = append_sel.is_some();
    let all_valid = source_validity.all_valid();

    // Clear top-level validity bit for NULL struct entries.
    if !all_valid && !layout.all_valid() {
        let source_sel = format.sel();
        let entry_idx = col_idx / 8;
        let bit_idx = col_idx % 8;

        for (i, row_ptr) in row_locations.iter().enumerate().take(count) {
            let append_idx = if has_append_sel {
                unsafe { append_sel.unwrap_unchecked().get(i) }
            } else {
                i
            };
            let source_idx = source_sel.get(append_idx);
            if !source_validity.is_valid(source_idx) {
                unsafe {
                    let validity_ptr = row_ptr.add(entry_idx);
                    let current = std::ptr::read(validity_ptr);
                    std::ptr::write(validity_ptr, current & !(1 << bit_idx));
                }
            }
        }
    }

    // Build row pointers for the nested struct layout.
    let mut struct_row_locations = Vec::with_capacity(count);
    for row_ptr in row_locations.iter().take(count) {
        unsafe {
            struct_row_locations.push(row_ptr.add(offset));
        }
    }

    let struct_layout = RawRowLayout::struct_layout(fields);
    if struct_layout.get_flag_width() > 0 {
        initialize_validity_masks(&struct_row_locations, struct_layout.get_flag_width(), count);
    }

    let Some(children) = vector.children() else {
        return;
    };

    for (field_idx, child_vec) in children.iter().enumerate() {
        let Some(child_format) = format.children.get(field_idx) else {
            continue;
        };
        let child_offset = struct_layout.get_offsets()[field_idx];
        scatter_vector(
            &struct_layout,
            child_vec.as_ref(),
            child_format,
            field_idx,
            child_offset,
            &struct_row_locations,
            heap_locations,
            append_sel,
            count,
        );
    }
}

/// Scatter fixed-size values to row storage.
///
/// This correctly handles CONSTANT and DICTIONARY vectors by using the
/// selection vector from DecodedVectorOwned to map indices.
///
///
/// This is the dispatch function that selects the optimal templated version
/// based on runtime conditions.
fn scatter_fixed<T: Copy + Default>(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_validity = format.validity();
    let has_append_sel = append_sel.is_some();
    let all_valid = source_validity.all_valid();

    // Dispatch to the optimal templated version based on runtime conditions
    // This selects the right template instantiation
    match (has_append_sel, all_valid) {
        (true, true) => scatter_fixed_internal::<T, true, true>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        (true, false) => scatter_fixed_internal::<T, true, false>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        (false, true) => scatter_fixed_internal::<T, false, true>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
        (false, false) => scatter_fixed_internal::<T, false, false>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            append_sel,
            count,
        ),
    }
}

/// Internal templated scatter function using const generics.
///
/// This function is instantiated at compile time with different combinations
/// of HAS_APPEND_SEL and ALL_VALID, avoiding runtime conditionals in the hot loop.
///
/// See `RawRowTemplatedScatterInternal<T, HAS_APPEND_SEL, HAS_SOURCE_SEL, ALL_VALID>`
///
/// Note: We don't need HAS_SOURCE_SEL because in Rust we always use the selection
/// vector from DecodedVectorOwned (it's incremental for Flat vectors, so the
/// overhead is minimal).
#[inline(always)]
fn scatter_fixed_internal<T: Copy + Default, const HAS_APPEND_SEL: bool, const ALL_VALID: bool>(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_sel = format.sel();
    let source_validity = format.validity();
    let data_ref = format.data();

    let entry_idx = col_idx / 8;
    let bit_idx = col_idx % 8;

    for (i, row_ptr) in row_locations.iter().enumerate().take(count) {
        // Map logical index to append index
        // With const generic, this branch is evaluated at compile time
        let append_idx = if HAS_APPEND_SEL {
            // SAFETY: append_sel is guaranteed to be Some when HAS_APPEND_SEL is true
            unsafe { append_sel.unwrap_unchecked().get(i) }
        } else {
            i
        };

        // Map append index to physical source index via DecodedVectorOwned's selection
        let source_idx = source_sel.get(append_idx);

        // With const generic, this branch is evaluated at compile time
        let is_valid = if ALL_VALID {
            true
        } else {
            source_validity.is_valid(source_idx)
        };

        if is_valid {
            unsafe {
                let dst = row_ptr.add(offset) as *mut T;
                match data_ref {
                    DataRef::Ptr(ptr) if !ptr.is_null() => {
                        let src = (ptr as *const T).add(source_idx);
                        let value = std::ptr::read(src);
                        std::ptr::write_unaligned(dst, value);
                    }
                    DataRef::SequenceI64 { start, increment }
                        if std::mem::size_of::<T>() == std::mem::size_of::<i64>() =>
                    {
                        let value = start + source_idx as i64 * increment;
                        std::ptr::write_unaligned(dst.cast::<i64>(), value);
                    }
                    _ => {
                        std::ptr::write_unaligned(dst, T::default());
                        if !layout.all_valid() {
                            let validity_ptr = row_ptr.add(entry_idx);
                            let current = std::ptr::read(validity_ptr);
                            std::ptr::write(validity_ptr, current & !(1 << bit_idx));
                        }
                    }
                }
            }
        } else {
            unsafe {
                let dst = row_ptr.add(offset) as *mut T;
                std::ptr::write_unaligned(dst, T::default());

                if !layout.all_valid() {
                    let validity_ptr = row_ptr.add(entry_idx);
                    let current = std::ptr::read(validity_ptr);
                    std::ptr::write(validity_ptr, current & !(1 << bit_idx));
                }
            }
        }
    }
}

/// Scatter string values to row storage.
///
/// String layout in row (16 bytes total):
/// - 4 bytes: length
/// - 12 bytes: inline data (for short strings) OR 4 bytes prefix + 8 bytes heap pointer
///
/// This correctly handles CONSTANT and DICTIONARY vectors.
/// Scatter string values to row storage.
///
/// String layout in row (16 bytes total):
/// - 4 bytes: length
/// - 12 bytes: inline data (for short strings) OR 4 bytes prefix + 8 bytes heap pointer
///
/// This correctly handles CONSTANT and DICTIONARY vectors.
///
#[allow(clippy::too_many_arguments)]
fn scatter_string(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_validity = format.validity();
    let has_append_sel = append_sel.is_some();
    let all_valid = source_validity.all_valid();

    // Dispatch to the optimal templated version based on runtime conditions
    match (has_append_sel, all_valid) {
        (true, true) => scatter_string_internal::<true, true>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            heap_locations,
            append_sel,
            count,
        ),
        (true, false) => scatter_string_internal::<true, false>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            heap_locations,
            append_sel,
            count,
        ),
        (false, true) => scatter_string_internal::<false, true>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            heap_locations,
            append_sel,
            count,
        ),
        (false, false) => scatter_string_internal::<false, false>(
            layout,
            format,
            col_idx,
            offset,
            row_locations,
            heap_locations,
            append_sel,
            count,
        ),
    }
}

/// Internal templated scatter function for strings using const generics.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn scatter_string_internal<const HAS_APPEND_SEL: bool, const ALL_VALID: bool>(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
) {
    let source_sel = format.sel();
    let source_validity = format.validity();
    let data_ptr = format.get_data::<paro_common::types::StringView>();

    let entry_idx = col_idx / 8;
    let bit_idx = col_idx % 8;

    for (i, row_ptr) in row_locations.iter().enumerate().take(count) {
        // Map logical index to append index
        let append_idx = if HAS_APPEND_SEL {
            unsafe { append_sel.unwrap_unchecked().get(i) }
        } else {
            i
        };

        // Map append index to physical source index via DecodedVectorOwned's selection
        let source_idx = source_sel.get(append_idx);

        let is_valid = if ALL_VALID {
            true
        } else {
            source_validity.is_valid(source_idx)
        };

        if is_valid && !data_ptr.is_null() {
            unsafe {
                let string_t = &*data_ptr.add(source_idx);
                let bytes = string_t.as_bytes();
                let len = bytes.len();

                let dst = row_ptr.add(offset);
                let row_value = if string_t.is_inlined() {
                    *string_t
                } else {
                    let heap_ptr = heap_locations[i];
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), heap_ptr, len);
                    heap_locations[i] = heap_ptr.add(len);
                    // SAFETY: the copied bytes are initialized in row-owned
                    // storage that outlives the target cell.
                    StringView::from_raw_parts(heap_ptr, len as u32)
                };
                // SAFETY: `dst` is a writable varlen cell in this row.
                row_value.write_cell(dst);
            }
        } else {
            // Write null string
            unsafe {
                let dst = row_ptr.add(offset);
                StringView::empty().write_cell(dst);

                if !layout.all_valid() {
                    let validity_ptr = row_ptr.add(entry_idx);
                    let current = std::ptr::read(validity_ptr);
                    std::ptr::write(validity_ptr, current & !(1 << bit_idx));
                }
            }
        }
    }
}

#[inline]
unsafe fn clear_collection_validity_bit(mask_ptr: *mut u8, idx: usize) {
    let byte_idx = idx / 8;
    let bit_shift = idx % 8;
    let byte_ptr = mask_ptr.add(byte_idx);
    let current = std::ptr::read(byte_ptr);
    std::ptr::write(byte_ptr, current & !(1 << bit_shift));
}

unsafe fn scatter_collection_payload(
    logical_type: &LogicalType,
    format: &RawRowVectorView<'_>,
    list_entry: ListEntry,
    validity_mask_ptr: *mut u8,
    payload_ptr: *mut u8,
) -> *mut u8 {
    if list_entry.length == 0 {
        return payload_ptr;
    }

    let source_sel = format.sel();
    let source_validity = format.validity();

    if RawRowLayout::type_is_constant_size(logical_type) {
        let element_size = RawRowLayout::get_type_size(logical_type);
        let source_data = format.get_data::<u8>();
        for elem_i in 0..list_entry.length {
            let source_pos = list_entry.offset + elem_i;
            let dst_ptr = payload_ptr.add(elem_i * element_size);

            let Some(source_idx) =
                (source_pos < source_sel.len()).then(|| source_sel.get(source_pos))
            else {
                clear_collection_validity_bit(validity_mask_ptr, elem_i);
                std::ptr::write_bytes(dst_ptr, 0, element_size);
                continue;
            };

            if !is_valid_at(source_validity, source_idx) || source_data.is_null() {
                clear_collection_validity_bit(validity_mask_ptr, elem_i);
                std::ptr::write_bytes(dst_ptr, 0, element_size);
                continue;
            }

            let src_ptr = source_data.add(source_idx * element_size);
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, element_size);
        }
        return payload_ptr.add(list_entry.length * element_size);
    }

    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            let inline_size = paro_common::types::StringView::SIZE;
            let source_data = format.get_data::<paro_common::types::StringView>();
            let mut heap_cursor = payload_ptr.add(list_entry.length * inline_size);

            for elem_i in 0..list_entry.length {
                let source_pos = list_entry.offset + elem_i;
                let dst_ptr =
                    payload_ptr.add(elem_i * inline_size) as *mut paro_common::types::StringView;

                let Some(source_idx) =
                    (source_pos < source_sel.len()).then(|| source_sel.get(source_pos))
                else {
                    clear_collection_validity_bit(validity_mask_ptr, elem_i);
                    std::ptr::write(dst_ptr, paro_common::types::StringView::empty());
                    continue;
                };

                if !is_valid_at(source_validity, source_idx) || source_data.is_null() {
                    clear_collection_validity_bit(validity_mask_ptr, elem_i);
                    std::ptr::write(dst_ptr, paro_common::types::StringView::empty());
                    continue;
                }

                let source_string = *source_data.add(source_idx);
                if source_string.is_inlined() || source_string.is_empty() {
                    std::ptr::write(dst_ptr, source_string);
                } else {
                    let bytes = source_string.as_bytes();
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), heap_cursor, bytes.len());
                    let mut relocated = source_string;
                    relocated.set_ptr(heap_cursor as *const u8);
                    std::ptr::write(dst_ptr, relocated);
                    heap_cursor = heap_cursor.add(bytes.len());
                }
            }

            heap_cursor
        }
        LogicalType::Struct(fields) => {
            // Update validity mask for struct entries.
            for elem_i in 0..list_entry.length {
                let source_pos = list_entry.offset + elem_i;

                let Some(source_idx) =
                    (source_pos < source_sel.len()).then(|| source_sel.get(source_pos))
                else {
                    clear_collection_validity_bit(validity_mask_ptr, elem_i);
                    continue;
                };

                if !is_valid_at(source_validity, source_idx) {
                    clear_collection_validity_bit(validity_mask_ptr, elem_i);
                }
            }

            let mut current_payload = payload_ptr;
            if list_entry.length == 0 {
                return current_payload;
            }

            let field_mask_size = RawRowLayout::validity_mask_size(list_entry.length);

            for (idx, (_, field_type)) in fields.iter().enumerate() {
                let Some(child_format) = format.children.get(idx) else {
                    continue;
                };

                // Initialize field validity mask and scatter field payload.
                std::ptr::write_bytes(current_payload, 0xFF, field_mask_size);
                let field_mask_ptr = current_payload;
                let field_payload_ptr = current_payload.add(field_mask_size);

                current_payload = scatter_collection_payload(
                    field_type,
                    child_format,
                    list_entry,
                    field_mask_ptr,
                    field_payload_ptr,
                );
            }

            current_payload
        }
        LogicalType::List(_) | LogicalType::Array(_, _) => {
            let pointer_size = std::mem::size_of::<usize>();
            let pointer_area = payload_ptr;
            let mut nested_heap_cursor = payload_ptr.add(list_entry.length * pointer_size);
            let nested_entries = get_collection_entries(format, None, source_sel.len());
            let next_logical_type = logical_type
                .element_type()
                .expect("Collection must have child logical type");
            let next_format = format.children.first();

            for elem_i in 0..list_entry.length {
                let source_pos = list_entry.offset + elem_i;
                let pointer_ptr = pointer_area.add(elem_i * pointer_size) as *mut *mut u8;

                let Some(source_idx) =
                    (source_pos < source_sel.len()).then(|| source_sel.get(source_pos))
                else {
                    clear_collection_validity_bit(validity_mask_ptr, elem_i);
                    std::ptr::write(pointer_ptr, std::ptr::null_mut());
                    continue;
                };

                if !is_valid_at(source_validity, source_idx) {
                    clear_collection_validity_bit(validity_mask_ptr, elem_i);
                    std::ptr::write(pointer_ptr, std::ptr::null_mut());
                    continue;
                }

                let nested_entry = nested_entries
                    .as_ref()
                    .and_then(|entries| entries.get(source_idx))
                    .copied()
                    .unwrap_or_default();

                let nested_heap_ptr = nested_heap_cursor;
                std::ptr::write(pointer_ptr, nested_heap_ptr);
                std::ptr::write(nested_heap_ptr as *mut u64, nested_entry.length as u64);
                nested_heap_cursor = nested_heap_cursor.add(8);

                if nested_entry.length == 0 {
                    continue;
                }

                let nested_mask_size = RawRowLayout::validity_mask_size(nested_entry.length);
                std::ptr::write_bytes(nested_heap_cursor, 0xFF, nested_mask_size);
                let nested_mask_ptr = nested_heap_cursor;
                let nested_payload_ptr = nested_heap_cursor.add(nested_mask_size);

                nested_heap_cursor = if let Some(next_format) = next_format {
                    scatter_collection_payload(
                        next_logical_type,
                        next_format,
                        nested_entry,
                        nested_mask_ptr,
                        nested_payload_ptr,
                    )
                } else {
                    nested_payload_ptr
                };
            }

            nested_heap_cursor
        }
        _ => payload_ptr,
    }
}

/// Scatter collection values (LIST, ARRAY) to row storage.
#[allow(clippy::too_many_arguments)]
fn scatter_collection_internal(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
    logical_type: &LogicalType,
) {
    let source_validity = format.validity();
    let has_append_sel = append_sel.is_some();
    let all_valid = source_validity.all_valid();

    let entry_idx = col_idx / 8;
    let bit_idx = col_idx % 8;

    let child_type = logical_type
        .element_type()
        .expect("Collection must have element type");
    let collection_entries = get_collection_entries(format, append_sel, count);

    for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
        let append_idx = if has_append_sel {
            unsafe { append_sel.unwrap_unchecked().get(i) }
        } else {
            i
        };
        let source_idx = format.sel().get(append_idx);

        let is_valid = all_valid || source_validity.is_valid(source_idx);

        if is_valid {
            unsafe {
                let heap_ptr = heap_locations[i];
                let dst = row_ptr.add(offset);

                // Write heap pointer to row
                std::ptr::write(dst as *mut *mut u8, heap_ptr);

                let list_entry = collection_entries
                    .as_ref()
                    .and_then(|entries| entries.get(source_idx))
                    .copied()
                    .or_else(|| {
                        logical_type.array_dimension().map(|array_size| ListEntry {
                            offset: source_idx.saturating_mul(array_size),
                            length: array_size,
                        })
                    })
                    .unwrap_or_default();
                let length = list_entry.length;

                // Write header to heap (length)
                std::ptr::write(heap_ptr as *mut u64, length as u64);

                let mut current_heap_ptr = heap_ptr.add(8);

                if length > 0 {
                    let mask_size = RawRowLayout::validity_mask_size(length);
                    // Initially all elements valid
                    std::ptr::write_bytes(current_heap_ptr, 0xFF, mask_size);
                    let element_validity_ptr = current_heap_ptr;
                    let element_payload_ptr = current_heap_ptr.add(mask_size);

                    current_heap_ptr = if let Some(child_format) = format.children.first() {
                        scatter_collection_payload(
                            child_type,
                            child_format,
                            list_entry,
                            element_validity_ptr,
                            element_payload_ptr,
                        )
                    } else {
                        element_payload_ptr
                    };
                }
                // Update heap_locations for next row
                heap_locations[i] = current_heap_ptr;
            }
        } else {
            // Write null collection
            unsafe {
                let dst = row_ptr.add(offset);
                std::ptr::write(dst as *mut *mut u8, std::ptr::null_mut());

                if !layout.all_valid() {
                    let validity_ptr = row_ptr.add(entry_idx);
                    let current = std::ptr::read(validity_ptr);
                    std::ptr::write(validity_ptr, current & !(1 << bit_idx));
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn array_row_scatter(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
    logical_type: &LogicalType,
) {
    scatter_collection_internal(
        layout,
        format,
        col_idx,
        offset,
        row_locations,
        heap_locations,
        append_sel,
        count,
        logical_type,
    );
}

#[allow(clippy::too_many_arguments)]
fn list_row_scatter(
    layout: &RawRowLayout,
    format: &RawRowVectorView<'_>,
    col_idx: usize,
    offset: usize,
    row_locations: &[*mut u8],
    heap_locations: &mut [*mut u8],
    append_sel: Option<&SelectionVector>,
    count: usize,
    logical_type: &LogicalType,
) {
    scatter_collection_internal(
        layout,
        format,
        col_idx,
        offset,
        row_locations,
        heap_locations,
        append_sel,
        count,
        logical_type,
    );
}

// =============================================================================
// Append Functions
// =============================================================================

/// Append a Chunk to a RawRowCollection.
///
/// This is the main high-level function for adding data to a collection.
/// It handles:
/// 1. Decoding vectors into a stable access form
/// 2. Computing heap sizes for variable-length data
/// 3. Allocating row and heap space
/// 4. Scattering the data to row format
///
/// # Arguments
/// * `collection` - The collection to append to
/// * `pin_state` - Pin state for the operation
/// * `segment` - The segment to append to
/// * `chunk` - The data chunk to append
/// * `chunk_state` - Chunk state (will call decode internally)
///
/// # Returns
/// Number of rows appended
pub fn append_chunk(
    collection: &mut RawRowCollection,
    pin_state: &mut RawRowPinState,
    segment: &mut RawRowSegment,
    chunk: &Chunk,
    chunk_state: &mut RawRowChunkState,
) -> Result<usize> {
    let count = chunk.size();
    if count == 0 {
        return Ok(0);
    }

    // Initialize borrowed vector views from chunk.
    let mut chunk_view = chunk_state.try_decode(chunk)?;

    let layout = collection.layout();

    let heap_sizes = compute_heap_sizes(layout, &mut chunk_view, None, count);

    let (row_locations, mut heap_locations) =
        build_rows(pin_state, segment, layout, &heap_sizes, count)?;

    scatter_chunk(
        layout,
        chunk,
        &mut chunk_view,
        &row_locations,
        &mut heap_locations,
        None,
        count,
    );

    // Update collection count
    let total_heap_size: usize = heap_sizes.iter().sum();
    collection.add_count(count, total_heap_size);

    Ok(count)
}

/// Append a Chunk with selection vector.
///
/// # Arguments
/// * `collection` - The collection to append to
/// * `pin_state` - Pin state for the operation
/// * `segment` - The segment to append to
/// * `chunk` - The data chunk to append
/// * `chunk_state` - Chunk state (will call decode internally)
/// * `sel` - Selection vector specifying which rows to append
/// * `count` - Number of rows to append (from selection)
///
/// # Returns
/// Number of rows appended
pub fn append_chunk_with_sel(
    collection: &mut RawRowCollection,
    pin_state: &mut RawRowPinState,
    segment: &mut RawRowSegment,
    chunk: &Chunk,
    chunk_state: &mut RawRowChunkState,
    sel: &SelectionVector,
    count: usize,
) -> Result<usize> {
    if count == 0 {
        return Ok(0);
    }

    // Initialize borrowed vector views from chunk.
    let mut chunk_view = chunk_state.try_decode(chunk)?;

    let layout = collection.layout();

    let heap_sizes = compute_heap_sizes(layout, &mut chunk_view, Some(sel), count);

    let (row_locations, mut heap_locations) =
        build_rows(pin_state, segment, layout, &heap_sizes, count)?;

    scatter_chunk(
        layout,
        chunk,
        &mut chunk_view,
        &row_locations,
        &mut heap_locations,
        Some(sel),
        count,
    );

    // Update collection count
    let total_heap_size: usize = heap_sizes.iter().sum();
    collection.add_count(count, total_heap_size);

    Ok(count)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::raw::RawRowValidityType;
    use crate::test_utils::*;
    use std::sync::Arc;

    fn create_test_layout(types: Vec<LogicalType>) -> RawRowLayout {
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        layout
    }

    #[test]
    fn test_scatter_fixed_flat_vector() {
        let layout = create_test_layout(vec![LogicalType::Integer]);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(test_i32_vector(&[10, 20, 30, 40]))]);

        let mut chunk_state = RawRowChunkState::new();
        let chunk_view = chunk_state.try_decode(&chunk).unwrap();

        let row_width = layout.get_row_width();
        let mut storage: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; row_width]).collect();
        let row_locations: Vec<*mut u8> = storage.iter_mut().map(|b| b.as_mut_ptr()).collect();

        initialize_validity_masks(&row_locations, layout.get_flag_width(), 4);

        if let Some(format) = chunk_view.get_vector_format(0) {
            scatter_fixed::<i32>(
                &layout,
                format,
                0,
                layout.get_offsets()[0],
                &row_locations,
                None,
                4,
            );
        }

        // Verify all values
        unsafe {
            let offset = layout.get_offsets()[0];
            for i in 0..4 {
                let value = std::ptr::read_unaligned(storage[i].as_ptr().add(offset) as *const i32);
                assert_eq!(value, (i as i32 + 1) * 10);
            }
        }
    }

    #[test]
    fn test_scatter_fixed_constant_vector() {
        // This is the key test - CONSTANT vectors caused the original panic
        let layout = create_test_layout(vec![LogicalType::Integer]);

        // Create a CONSTANT vector with value 42, count 4
        let constant_vec = test_constant_vector(LogicalType::Integer, 42i32, 4);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(constant_vec)]);

        let mut chunk_state = RawRowChunkState::new();
        let chunk_view = chunk_state.try_decode(&chunk).unwrap();

        let row_width = layout.get_row_width();
        let mut storage: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; row_width]).collect();
        let row_locations: Vec<*mut u8> = storage.iter_mut().map(|b| b.as_mut_ptr()).collect();

        initialize_validity_masks(&row_locations, layout.get_flag_width(), 4);

        if let Some(format) = chunk_view.get_vector_format(0) {
            // This should NOT panic, unlike the old code
            scatter_fixed::<i32>(
                &layout,
                format,
                0,
                layout.get_offsets()[0],
                &row_locations,
                None,
                4,
            );
        }

        // Verify all values are 42
        unsafe {
            let offset = layout.get_offsets()[0];
            for i in 0..4 {
                let value = std::ptr::read_unaligned(storage[i].as_ptr().add(offset) as *const i32);
                assert_eq!(value, 42, "Row {} should have value 42", i);
            }
        }
    }

    #[test]
    fn test_scatter_fixed_dictionary_vector() {
        let layout = create_test_layout(vec![LogicalType::Integer]);

        // Create a DICTIONARY vector: indices [2, 0, 1, 2] into child [100, 200, 300]
        let child = test_i32_vector(&[100, 200, 300]);
        let dict_vec = paro_common::test_utils::test_dictionary(Arc::new(child), vec![2, 0, 1, 2]);

        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(dict_vec)]);

        let mut chunk_state = RawRowChunkState::new();
        let chunk_view = chunk_state.try_decode(&chunk).unwrap();

        let row_width = layout.get_row_width();
        let mut storage: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; row_width]).collect();
        let row_locations: Vec<*mut u8> = storage.iter_mut().map(|b| b.as_mut_ptr()).collect();

        initialize_validity_masks(&row_locations, layout.get_flag_width(), 4);

        if let Some(format) = chunk_view.get_vector_format(0) {
            scatter_fixed::<i32>(
                &layout,
                format,
                0,
                layout.get_offsets()[0],
                &row_locations,
                None,
                4,
            );
        }

        // Verify values match dictionary lookup
        unsafe {
            let offset = layout.get_offsets()[0];
            // indices [2, 0, 1, 2] -> values [300, 100, 200, 300]
            assert_eq!(
                std::ptr::read_unaligned(storage[0].as_ptr().add(offset) as *const i32),
                300
            );
            assert_eq!(
                std::ptr::read_unaligned(storage[1].as_ptr().add(offset) as *const i32),
                100
            );
            assert_eq!(
                std::ptr::read_unaligned(storage[2].as_ptr().add(offset) as *const i32),
                200
            );
            assert_eq!(
                std::ptr::read_unaligned(storage[3].as_ptr().add(offset) as *const i32),
                300
            );
        }
    }

    #[test]
    fn test_compute_heap_sizes_constant_string() {
        // Test that CONSTANT string vectors don't cause panic
        use paro_common::runtime_value::Value;

        let layout = create_test_layout(vec![LogicalType::Varchar]);

        // Create a CONSTANT varchar vector with a long string (> 12 bytes)
        let constant_value = Value::Varchar("this is a very long constant string".to_string());
        let constant_vec = test_constant_from_value(
            LogicalType::Varchar,
            &constant_value, // 35 bytes
            4,
        );

        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(constant_vec)]);

        let mut chunk_state = RawRowChunkState::new();
        let mut chunk_view = chunk_state.try_decode(&chunk).unwrap();

        // This should NOT panic
        let heap_sizes = compute_heap_sizes(&layout, &mut chunk_view, None, 4);

        assert_eq!(heap_sizes.len(), 4);
        // All 4 rows should have the same heap size (constant value)
        for size in &heap_sizes {
            assert_eq!(*size, 35, "All rows should need 35 bytes of heap");
        }
    }

    #[test]
    fn test_scatter_chunk_mixed_types() {
        let layout = create_test_layout(vec![LogicalType::Integer, LogicalType::BigInt]);

        // Create a mixed chunk: FLAT integer + CONSTANT bigint
        let flat_vec = test_i32_vector(&[1, 2, 3, 4]);
        let constant_vec = test_constant_vector(LogicalType::BigInt, 1000i64, 4);

        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(flat_vec), Arc::new(constant_vec)]);

        let mut chunk_state = RawRowChunkState::new();
        let mut chunk_view = chunk_state.try_decode(&chunk).unwrap();

        let row_width = layout.get_row_width();
        let mut storage: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; row_width]).collect();
        let row_locations: Vec<*mut u8> = storage.iter_mut().map(|b| b.as_mut_ptr()).collect();
        let mut heap_locations = vec![std::ptr::null_mut(); 4];

        // Use the scatter function
        scatter_chunk(
            &layout,
            &chunk,
            &mut chunk_view,
            &row_locations,
            &mut heap_locations,
            None,
            4,
        );

        // Verify values
        unsafe {
            let offset0 = layout.get_offsets()[0];
            let offset1 = layout.get_offsets()[1];

            for i in 0..4 {
                let int_value =
                    std::ptr::read_unaligned(storage[i].as_ptr().add(offset0) as *const i32);
                let bigint_value =
                    std::ptr::read_unaligned(storage[i].as_ptr().add(offset1) as *const i64);

                assert_eq!(int_value, (i as i32) + 1);
                assert_eq!(bigint_value, 1000);
            }
        }
    }

    #[test]
    fn test_scatter_with_selection() {
        let layout = create_test_layout(vec![LogicalType::Integer]);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(test_i32_vector(&[10, 20, 30, 40]))]);

        let mut chunk_state = RawRowChunkState::new();
        let chunk_view = chunk_state.try_decode(&chunk).unwrap();

        // Selection: pick indices [1, 3]
        let sel = test_selection(vec![1, 3]);

        let row_width = layout.get_row_width();
        let mut storage: Vec<Vec<u8>> = (0..2).map(|_| vec![0u8; row_width]).collect();
        let row_locations: Vec<*mut u8> = storage.iter_mut().map(|b| b.as_mut_ptr()).collect();

        initialize_validity_masks(&row_locations, layout.get_flag_width(), 2);

        if let Some(format) = chunk_view.get_vector_format(0) {
            scatter_fixed::<i32>(
                &layout,
                format,
                0,
                layout.get_offsets()[0],
                &row_locations,
                Some(&sel),
                2,
            );
        }

        // Verify: should have values 20 and 40
        unsafe {
            let offset = layout.get_offsets()[0];
            assert_eq!(
                std::ptr::read_unaligned(storage[0].as_ptr().add(offset) as *const i32),
                20
            );
            assert_eq!(
                std::ptr::read_unaligned(storage[1].as_ptr().add(offset) as *const i32),
                40
            );
        }
    }

    #[test]
    fn test_initialize_validity_masks() {
        let row_width = 16;
        let flag_width = 2;

        let mut storage: Vec<Vec<u8>> = (0..3).map(|_| vec![0u8; row_width]).collect();
        let row_locations: Vec<*mut u8> = storage.iter_mut().map(|b| b.as_mut_ptr()).collect();

        initialize_validity_masks(&row_locations, flag_width, 3);

        // Verify first flag_width bytes are all 0xFF
        for row in &storage {
            for byte in &row[..flag_width] {
                assert_eq!(*byte, 0xFF);
            }
        }
    }

    #[test]
    fn test_compute_heap_sizes_nested_array_varchar() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Varchar), 2);
        let layout = create_test_layout(vec![array_type.clone()]);

        let values = [
            "tiny",
            "this string is definitely long",
            "",
            "another long value",
        ];
        let child = Arc::new(test_string_vector(&values));
        let array_vector =
            paro_common::test_utils::test_array_vector(LogicalType::Varchar, child, 2, 2);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(array_vector)]);

        let mut chunk_state = RawRowChunkState::new();
        let mut chunk_view = chunk_state.try_decode(&chunk).unwrap();
        let heap_sizes = compute_heap_sizes(&layout, &mut chunk_view, None, 2);

        let base = std::mem::size_of::<u64>()
            + RawRowLayout::validity_mask_size(2)
            + 2 * paro_common::types::StringView::SIZE;
        let expected_0 = base + values[1].len();
        let expected_1 = base + values[3].len();

        assert_eq!(heap_sizes, vec![expected_0, expected_1]);
    }

    #[test]
    fn test_compute_heap_sizes_nested_array_of_array_recurses_without_materialized_selection() {
        let inner_array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let outer_array_type = LogicalType::Array(Box::new(inner_array_type.clone()), 2);
        let layout = create_test_layout(vec![outer_array_type]);

        let inner_child = Arc::new(test_i32_vector(&[1, 2, 3, 4, 5, 6, 7, 8]));
        let inner_array_vector = Arc::new(paro_common::test_utils::test_array_vector(
            LogicalType::Integer,
            inner_child,
            4,
            2,
        ));
        let outer_array_vector =
            paro_common::test_utils::test_array_vector(inner_array_type, inner_array_vector, 2, 2);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(outer_array_vector)]);

        let mut chunk_state = RawRowChunkState::new();
        let mut chunk_view = chunk_state.try_decode(&chunk).unwrap();
        let heap_sizes = compute_heap_sizes(&layout, &mut chunk_view, None, 2);

        // Outer header + outer child metadata + per-inner-array headers/masks/values.
        let expected = std::mem::size_of::<u64>()
            + RawRowLayout::validity_mask_size(2)
            + 2 * std::mem::size_of::<u64>()
            + 2 * (std::mem::size_of::<u64>()
                + RawRowLayout::validity_mask_size(2)
                + 2 * std::mem::size_of::<i32>());
        assert_eq!(heap_sizes, vec![expected, expected]);

        let format = chunk_view
            .get_vector_format(0)
            .expect("outer format should exist");
        let inner_format = format.children.first().expect("inner array format");
        let value_format = inner_format
            .children
            .first()
            .expect("inner array child format");
        assert!(
            value_format.combined_list_data.is_none(),
            "heap-size recursion should not allocate combined selection scratch"
        );
    }
}
