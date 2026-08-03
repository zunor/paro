// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::table::{
    GlobalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindInput,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult, TableFunctionSet,
};

#[derive(Clone)]
struct ParoMemoryBindData;

impl TableFunctionBindData for ParoMemoryBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
struct MemoryRow {
    tag: String,
    memory_usage_bytes: i64,
    temporary_storage_bytes: i64,
}

struct ParoMemoryGlobalState {
    rows: Vec<MemoryRow>,
    offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoMemoryGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.rows.is_empty() {
            return 100.0;
        }
        let offset = self.offset.load(Ordering::Relaxed);
        (offset as f64 / self.rows.len() as f64) * 100.0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn paro_memory_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("tag".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("memory_usage_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("temporary_storage_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoMemoryBindData)))
}

fn paro_memory_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let buffer_pool = input.buffer_manager()?.get_buffer_pool();
    let snapshot = buffer_pool.get_memory_usage_info();
    let temporary_storage = buffer_pool.get_temporary_storage_by_tag();
    let rows = MemoryTag::all()
        .iter()
        .map(|tag| MemoryRow {
            tag: tag.name().to_string(),
            memory_usage_bytes: snapshot.get(*tag).max(0),
            temporary_storage_bytes: i64::try_from(temporary_storage[tag.as_index()])
                .unwrap_or(i64::MAX),
        })
        .collect();

    Ok(Some(Box::new(ParoMemoryGlobalState {
        rows,
        offset: AtomicUsize::new(0),
    })))
}

fn paro_memory_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let Some(gstate) = input
        .global_state
        .and_then(|state| state.as_any().downcast_ref::<ParoMemoryGlobalState>())
    else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.rows.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.rows.len() - offset);
    let rows = &gstate.rows[offset..offset + batch_size];
    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let tags: Vec<&str> = rows.iter().map(|row| row.tag.as_str()).collect();
    let memory_usage: Vec<i64> = rows.iter().map(|row| row.memory_usage_bytes).collect();
    let temporary_storage: Vec<i64> = rows.iter().map(|row| row.temporary_storage_bytes).collect();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_strings(&tags, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_i64(&memory_usage, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_i64(&temporary_storage, output_allocator.clone())?;
    }
    output.set_cardinality(batch_size);

    if gstate.offset.load(Ordering::Relaxed) >= gstate.rows.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_memory_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map_or(-1.0, |state| state.get_progress())
}

pub fn create_paro_memory_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_memory", vec![]);
    func.bind = Some(paro_memory_bind);
    func.init_global = Some(paro_memory_init_global);
    func.function = Some(paro_memory_function);
    func.table_scan_progress = Some(paro_memory_progress);

    let mut set = TableFunctionSet::new("paro_memory");
    set.add_function(func);
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TestTableFunctionRuntimeContext;
    use paro_storage::buffer::{BufferManager, StandardBufferManager};
    use std::sync::Arc;

    #[test]
    fn test_create_paro_memory_function_set() {
        let set = create_paro_memory_function_set();
        assert_eq!(set.name, "paro_memory");
        assert_eq!(set.functions.len(), 1);
    }

    #[test]
    fn initialization_reads_memory_from_the_runtime_buffer_manager() {
        let buffer_manager: Arc<dyn BufferManager> =
            Arc::new(StandardBufferManager::with_defaults(16 * 1024 * 1024));
        let _allocation = buffer_manager
            .allocate_temp(MemoryTag::InMemoryTable, 4096)
            .expect("test allocation should succeed");
        let runtime =
            TestTableFunctionRuntimeContext::with_buffer_manager(Arc::clone(&buffer_manager));
        let input = TableFunctionInitInput::new(&runtime, None, &[]);

        let state = paro_memory_init_global(&input)
            .expect("runtime-backed initialization should succeed")
            .expect("paro_memory should create global state");
        let state = state
            .as_any()
            .downcast_ref::<ParoMemoryGlobalState>()
            .expect("unexpected global state type");
        let row = state
            .rows
            .iter()
            .find(|row| row.tag == MemoryTag::InMemoryTable.name())
            .expect("in-memory table tag should be reported");

        assert!(row.memory_usage_bytes >= 4096);
    }
}
