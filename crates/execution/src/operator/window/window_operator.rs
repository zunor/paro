// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Window Operator Implementation
//!
//!
//! ## Design Notes
//! - Window is a blocking operator (Sink + Source)
//! - Sink/finalize are delegated to SortStrategy
//! - Source executes WindowHashGroup staged pipeline
//! - Current scope keeps blocking window only (no streaming window)

use std::any::Any;
use std::cmp::Ordering;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::memory::{
    AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryGrant,
    MemoryOwner, MemoryReleaseHandle,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_function::window::WindowFunctionType;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{Expression, OrderByExpression, WindowExpression};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_window_expression;
use crate::explain::types::ExplainRuntimeStats;
use crate::memory_runtime::RetainedChunkVec;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkInput,
    OperatorSourceInput,
};
use crate::operator::{OrderPreservationType, PhysicalOperator};
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};
use crate::sorting::sort::Sort;

/// Row key for sorting and partitioning.
#[derive(Debug, Clone, Copy)]
struct RowKey {
    chunk_idx: usize,
    row_idx: usize,
}

#[derive(Debug)]
struct AccountedRowKeys {
    keys: AccountedVec<RowKey>,
}

impl AccountedRowKeys {
    fn new(memory: &MemoryAccountingContext) -> Self {
        Self {
            keys: AccountedVec::new_with_accounting(
                grant_for_context(memory),
                memory.tag(),
                memory.accounting_class(),
            ),
        }
    }

    fn from_slice(memory: &MemoryAccountingContext, keys: &[RowKey]) -> Result<Self> {
        let mut accounted = Self::new(memory);
        accounted.keys.try_extend_from_slice(keys)?;
        Ok(accounted)
    }

    fn push(&mut self, key: RowKey) -> Result<()> {
        self.keys.try_push(key)?;
        Ok(())
    }

    fn as_slice(&self) -> &[RowKey] {
        self.keys.as_slice()
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl Deref for AccountedRowKeys {
    type Target = [RowKey];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

/// Partition information.
#[derive(Debug, Clone)]
struct Partition {
    /// Start index in sorted rows
    start: usize,
    /// End index (exclusive) in sorted rows
    end: usize,
}

/// Window function result value.
#[derive(Debug, Clone)]
enum WindowValue {
    Null,
    BigInt(i64),
    Double(f64),
    Integer(i32),
    Varchar(String),
}

impl WindowValue {
    fn payload_bytes(&self) -> usize {
        match self {
            Self::Varchar(value) => value.capacity(),
            _ => 0,
        }
    }
}

#[derive(Debug)]
struct AccountedWindowValues {
    memory: MemoryAccountingContext,
    values: AccountedVec<WindowValue>,
    payload_handles: AccountedVec<MemoryReleaseHandle>,
}

impl AccountedWindowValues {
    fn new(memory: &MemoryAccountingContext) -> Self {
        let metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        Self {
            memory: memory.clone(),
            values: AccountedVec::new_with_accounting(
                grant_for_context(memory),
                memory.tag(),
                memory.accounting_class(),
            ),
            payload_handles: AccountedVec::new_with_accounting(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
        }
    }

    fn with_capacity(memory: &MemoryAccountingContext, capacity: usize) -> Result<Self> {
        let mut values = Self::new(memory);
        values.values.try_reserve(capacity)?;
        Ok(values)
    }

    fn push(&mut self, value: WindowValue) -> Result<()> {
        let payload_bytes = value.payload_bytes();
        if payload_bytes > 0 {
            self.payload_handles.try_reserve(1)?;
        }
        self.values.try_push(value)?;
        if payload_bytes > 0 {
            let handle = match self.memory.retain(payload_bytes) {
                Ok(handle) => handle,
                Err(err) => {
                    self.values.pop();
                    return Err(err.into());
                }
            };
            self.payload_handles.try_push(handle)?;
        }
        Ok(())
    }

    fn as_slice(&self) -> &[WindowValue] {
        self.values.as_slice()
    }
}

impl Deref for AccountedWindowValues {
    type Target = [WindowValue];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Drop for AccountedWindowValues {
    fn drop(&mut self) {
        for handle in self.payload_handles.iter() {
            handle.release();
        }
    }
}

#[derive(Debug)]
struct AccountedWindowResults {
    results: AccountedVec<AccountedWindowValues>,
}

impl AccountedWindowResults {
    fn new(memory: &MemoryAccountingContext) -> Self {
        let metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        Self {
            results: AccountedVec::new_with_accounting(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
        }
    }

    fn push(&mut self, values: AccountedWindowValues) -> Result<()> {
        self.results.try_push(values)?;
        Ok(())
    }

    fn iter(&self) -> std::slice::Iter<'_, AccountedWindowValues> {
        self.results.iter()
    }
}

impl Deref for AccountedWindowResults {
    type Target = [AccountedWindowValues];

    fn deref(&self) -> &Self::Target {
        self.results.as_slice()
    }
}

fn window_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        MemoryAccountingClass::Revocable,
    )
}

fn sort_backed_memory_context(
    gstate: &SortBackedGlobalSinkState,
    ctx: &ExecutionContext,
) -> MemoryAccountingContext {
    gstate
        .sort_state
        .as_any()
        .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>()
        .map(crate::sorting::sort::SortGlobalSinkState::memory_context)
        .unwrap_or_else(|| window_memory_context(ctx))
}

fn grant_for_context(memory: &MemoryAccountingContext) -> MemoryGrant {
    if let Some(owner) = memory.owner() {
        MemoryGrant::new(0, memory.domain(), owner).expect("zero-byte window grant should fit")
    } else {
        MemoryGrant::detached(usize::MAX / 4, memory.domain())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowGroupStage {
    Sort,
    Materialize,
    Mask,
    Sink,
    Finalize,
    GetData,
    Done,
}

#[derive(Debug, Clone, Copy)]
struct WindowSourceTask {
    group_idx: usize,
    stage: WindowGroupStage,
}

#[derive(Debug, Clone)]
struct SortHashGroup {
    hash_bin: usize,
    chunks: Arc<Mutex<RetainedChunkVec>>,
    keys: Arc<Mutex<AccountedRowKeys>>,
}

trait SortStrategy: std::fmt::Debug + Send + Sync {
    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>>;
    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>>;
    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkResultType>;
    fn combine(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkCombineResultType>;
    fn finalize(&self, global_state: &dyn GlobalSinkState) -> Result<SinkFinalizeType>;

    fn sort_column_data(&self, _hash_bin: usize) -> Result<()> {
        Ok(())
    }

    fn materialize_column_data(&self, _hash_bin: usize) -> Result<()> {
        Ok(())
    }

    fn get_hash_groups(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
    ) -> Result<Vec<SortHashGroup>>;

    fn is_externalized(&self, _global_state: &dyn GlobalSinkState) -> bool {
        false
    }
}

#[derive(Debug)]
struct SortBackedGlobalSinkState {
    sort_state: Box<dyn GlobalSinkState>,
    hash_groups: Mutex<Option<Vec<SortHashGroup>>>,
}

impl GlobalSinkState for SortBackedGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "WindowSortBackedGlobalSinkState"
    }
}

#[derive(Debug, Default)]
struct SortBackedLocalSinkState {
    sort_state: Option<Box<dyn LocalSinkState>>,
}

impl LocalSinkState for SortBackedLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct NaturalGlobalSinkState {
    chunks: Mutex<RetainedChunkVec>,
    hash_groups: Mutex<Option<Vec<SortHashGroup>>>,
}

impl NaturalGlobalSinkState {
    fn new(memory: MemoryAccountingContext) -> Self {
        Self {
            chunks: Mutex::new(RetainedChunkVec::new(memory)),
            hash_groups: Mutex::new(None),
        }
    }
}

impl GlobalSinkState for NaturalGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "WindowNaturalGlobalSinkState"
    }
}

#[derive(Debug)]
struct NaturalLocalSinkState {
    local_chunks: RetainedChunkVec,
}

impl NaturalLocalSinkState {
    fn new(memory: MemoryAccountingContext) -> Self {
        Self {
            local_chunks: RetainedChunkVec::new(memory),
        }
    }
}

impl LocalSinkState for NaturalLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn expr_column_index(expr: &Expression) -> Option<usize> {
    match expr {
        Expression::ColumnRef(col_ref) => Some(col_ref.binding.column_index),
        Expression::Reference(ref_expr) => Some(ref_expr.index),
        _ => None,
    }
}

fn compare_column_values(vec_a: &Vector, idx_a: usize, vec_b: &Vector, idx_b: usize) -> Ordering {
    match vec_a.logical_type() {
        LogicalType::Boolean => {
            let a = vec_a.get_bool(idx_a);
            let b = vec_b.get_bool(idx_b);
            a.cmp(&b)
        }
        LogicalType::TinyInt => {
            let a = vec_a.get_i8(idx_a);
            let b = vec_b.get_i8(idx_b);
            a.cmp(&b)
        }
        LogicalType::SmallInt => {
            let a = vec_a.get_i16(idx_a);
            let b = vec_b.get_i16(idx_b);
            a.cmp(&b)
        }
        LogicalType::Integer => {
            let a = vec_a.get_i32(idx_a);
            let b = vec_b.get_i32(idx_b);
            a.cmp(&b)
        }
        LogicalType::BigInt => {
            let a = vec_a.get_i64(idx_a);
            let b = vec_b.get_i64(idx_b);
            a.cmp(&b)
        }
        LogicalType::Float => {
            let a = vec_a.get_f32(idx_a);
            let b = vec_b.get_f32(idx_b);
            match (a, b) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            }
        }
        LogicalType::Double => {
            let a = vec_a.get_f64(idx_a);
            let b = vec_b.get_f64(idx_b);
            match (a, b) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            }
        }
        LogicalType::Varchar => {
            let a = vec_a.get_string(idx_a);
            let b = vec_b.get_string(idx_b);
            a.cmp(&b)
        }
        _ => Ordering::Equal,
    }
}

fn values_equal(chunks: &[Chunk], a: &RowKey, b: &RowKey, expr: &Expression) -> bool {
    let Some(col_idx) = expr_column_index(expr) else {
        return true;
    };

    let chunk_a = &chunks[a.chunk_idx];
    let chunk_b = &chunks[b.chunk_idx];

    let is_null_a = chunk_a.data[col_idx].is_null(a.row_idx);
    let is_null_b = chunk_b.data[col_idx].is_null(b.row_idx);

    if is_null_a && is_null_b {
        return true;
    }
    if is_null_a || is_null_b {
        return false;
    }

    compare_column_values(
        &chunk_a.data[col_idx],
        a.row_idx,
        &chunk_b.data[col_idx],
        b.row_idx,
    ) == Ordering::Equal
}

fn same_partition(chunks: &[Chunk], a: &RowKey, b: &RowKey, partitions: &[Expression]) -> bool {
    for partition_expr in partitions {
        if !values_equal(chunks, a, b, partition_expr) {
            return false;
        }
    }
    true
}

fn build_row_keys(chunks: &[Chunk], memory: &MemoryAccountingContext) -> Result<AccountedRowKeys> {
    let mut keys = AccountedRowKeys::new(memory);
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        for row_idx in 0..chunk.size() {
            keys.push(RowKey { chunk_idx, row_idx })?;
        }
    }
    Ok(keys)
}

fn collect_sorted_chunks(
    sort: &Sort,
    ctx: &ExecutionContext,
    sink_state: &dyn GlobalSinkState,
    memory: &MemoryAccountingContext,
) -> Result<RetainedChunkVec> {
    let gsource = sort.get_global_source_state(ctx, sink_state)?;
    let mut lsource = sort.get_local_source_state(ctx, gsource.as_ref())?;

    let mut chunks = RetainedChunkVec::new(memory.clone());
    loop {
        let mut out = Chunk::try_new(ctx.allocator(paro_common::allocator::MemoryTag::BaseTable))?;
        let result = sort.get_data(ctx, &mut out, gsource.as_ref(), lsource.as_mut())?;
        if out.size() > 0 {
            chunks.push(out)?;
        }

        if result == SourceResultType::Finished {
            break;
        }
    }

    Ok(chunks)
}

fn split_keys_by_partitions(
    chunks: &[Chunk],
    keys: &[RowKey],
    partitions: &[Expression],
) -> Result<Vec<Vec<RowKey>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    if partitions.is_empty() {
        return Ok(vec![keys.to_vec()]);
    }

    let mut groups = Vec::new();
    let mut start = 0;

    for i in 1..keys.len() {
        if !same_partition(chunks, &keys[i - 1], &keys[i], partitions) {
            groups.push(keys[start..i].to_vec());
            start = i;
        }
    }
    groups.push(keys[start..].to_vec());

    Ok(groups)
}

fn window_order_to_sort_order(order: &OrderByExpression) -> OrderByNode {
    OrderByNode {
        expression: order.expression.clone(),
        ascending: order.ascending,
        nulls_first: order.nulls_first,
    }
}

#[derive(Debug)]
struct FullSortStrategy {
    sort: Arc<Sort>,
}

impl FullSortStrategy {
    fn new(expr: &WindowExpression, input_types: Vec<LogicalType>) -> Result<Self> {
        let orders = expr.orders.iter().map(window_order_to_sort_order).collect();
        let sort = Arc::new(Sort::new(orders, input_types, vec![], false)?);
        Ok(Self { sort })
    }
}

impl SortStrategy for FullSortStrategy {
    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let sort_state = self.sort.get_global_sink_state(ctx)?;
        Ok(Box::new(SortBackedGlobalSinkState {
            sort_state,
            hash_groups: Mutex::new(None),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(SortBackedLocalSinkState::default()))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid FullSort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortBackedLocalSinkState>()
            .expect("Invalid FullSort local sink state");

        if lstate.sort_state.is_none() {
            lstate.sort_state = Some(self.sort.get_local_sink_state(ctx)?);
        }

        self.sort.sink(
            ctx,
            chunk,
            gstate.sort_state.as_ref(),
            lstate
                .sort_state
                .as_mut()
                .expect("sort local state missing")
                .as_mut(),
        )
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkCombineResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid FullSort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortBackedLocalSinkState>()
            .expect("Invalid FullSort local sink state");

        if lstate.sort_state.is_none() {
            return Ok(SinkCombineResultType::Finished);
        }

        self.sort.combine(
            ctx,
            gstate.sort_state.as_ref(),
            lstate
                .sort_state
                .as_mut()
                .expect("sort local state missing")
                .as_mut(),
        )
    }

    fn finalize(&self, global_state: &dyn GlobalSinkState) -> Result<SinkFinalizeType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid FullSort global sink state");
        self.sort.finalize(gstate.sort_state.as_ref())
    }

    fn get_hash_groups(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
    ) -> Result<Vec<SortHashGroup>> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid FullSort global sink state");

        if let Some(groups) = gstate.hash_groups.lock().unwrap().as_ref() {
            return Ok(groups.clone());
        }

        let memory = sort_backed_memory_context(gstate, ctx);
        let chunks = collect_sorted_chunks(&self.sort, ctx, gstate.sort_state.as_ref(), &memory)?;
        let keys = build_row_keys(chunks.as_slice(), &memory)?;
        if keys.is_empty() {
            *gstate.hash_groups.lock().unwrap() = Some(Vec::new());
            return Ok(Vec::new());
        }

        let groups = vec![SortHashGroup {
            hash_bin: 0,
            chunks: Arc::new(Mutex::new(chunks)),
            keys: Arc::new(Mutex::new(keys)),
        }];

        *gstate.hash_groups.lock().unwrap() = Some(groups.clone());
        Ok(groups)
    }

    fn is_externalized(&self, global_state: &dyn GlobalSinkState) -> bool {
        let Some(gstate) = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
        else {
            return false;
        };
        gstate
            .sort_state
            .as_any()
            .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>()
            .map(|state| state.is_external())
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct HashedSortStrategy {
    sort: Arc<Sort>,
    partitions: Vec<Expression>,
}

impl HashedSortStrategy {
    fn new(expr: &WindowExpression, input_types: Vec<LogicalType>) -> Result<Self> {
        let mut orders = Vec::new();
        for partition in &expr.partitions {
            orders.push(OrderByNode {
                expression: partition.clone(),
                ascending: true,
                nulls_first: false,
            });
        }
        for order in &expr.orders {
            orders.push(window_order_to_sort_order(order));
        }

        let sort = Arc::new(Sort::new(orders, input_types, vec![], false)?);
        Ok(Self {
            sort,
            partitions: expr.partitions.clone(),
        })
    }
}

impl SortStrategy for HashedSortStrategy {
    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let sort_state = self.sort.get_global_sink_state(ctx)?;
        Ok(Box::new(SortBackedGlobalSinkState {
            sort_state,
            hash_groups: Mutex::new(None),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(SortBackedLocalSinkState::default()))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid HashedSort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortBackedLocalSinkState>()
            .expect("Invalid HashedSort local sink state");

        if lstate.sort_state.is_none() {
            lstate.sort_state = Some(self.sort.get_local_sink_state(ctx)?);
        }

        self.sort.sink(
            ctx,
            chunk,
            gstate.sort_state.as_ref(),
            lstate
                .sort_state
                .as_mut()
                .expect("sort local state missing")
                .as_mut(),
        )
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkCombineResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid HashedSort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<SortBackedLocalSinkState>()
            .expect("Invalid HashedSort local sink state");

        if lstate.sort_state.is_none() {
            return Ok(SinkCombineResultType::Finished);
        }

        self.sort.combine(
            ctx,
            gstate.sort_state.as_ref(),
            lstate
                .sort_state
                .as_mut()
                .expect("sort local state missing")
                .as_mut(),
        )
    }

    fn finalize(&self, global_state: &dyn GlobalSinkState) -> Result<SinkFinalizeType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid HashedSort global sink state");
        self.sort.finalize(gstate.sort_state.as_ref())
    }

    fn get_hash_groups(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
    ) -> Result<Vec<SortHashGroup>> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .expect("Invalid HashedSort global sink state");

        if let Some(groups) = gstate.hash_groups.lock().unwrap().as_ref() {
            return Ok(groups.clone());
        }

        let memory = sort_backed_memory_context(gstate, ctx);
        let chunks = collect_sorted_chunks(&self.sort, ctx, gstate.sort_state.as_ref(), &memory)?;
        let keys = build_row_keys(chunks.as_slice(), &memory)?;
        if keys.is_empty() {
            *gstate.hash_groups.lock().unwrap() = Some(Vec::new());
            return Ok(Vec::new());
        }

        let partitioned_keys =
            split_keys_by_partitions(chunks.as_slice(), keys.as_slice(), &self.partitions)?;
        let chunk_ref = Arc::new(Mutex::new(chunks));

        let groups = partitioned_keys
            .into_iter()
            .enumerate()
            .map(|(idx, keys)| {
                Ok(SortHashGroup {
                    hash_bin: idx,
                    chunks: Arc::clone(&chunk_ref),
                    keys: Arc::new(Mutex::new(AccountedRowKeys::from_slice(&memory, &keys)?)),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        *gstate.hash_groups.lock().unwrap() = Some(groups.clone());
        Ok(groups)
    }

    fn is_externalized(&self, global_state: &dyn GlobalSinkState) -> bool {
        let Some(gstate) = global_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
        else {
            return false;
        };
        gstate
            .sort_state
            .as_any()
            .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>()
            .map(|state| state.is_external())
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct NaturalSortStrategy;

impl SortStrategy for NaturalSortStrategy {
    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(NaturalGlobalSinkState::new(
            window_memory_context(ctx),
        )))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(NaturalLocalSinkState::new(window_memory_context(
            ctx,
        ))))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        _global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkResultType> {
        if chunk.is_empty() {
            return Ok(SinkResultType::NeedMoreInput);
        }

        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<NaturalLocalSinkState>()
            .expect("Invalid NaturalSort local sink state");
        lstate.local_chunks.push(chunk.clone())?;
        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
        local_state: &mut dyn LocalSinkState,
    ) -> Result<SinkCombineResultType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<NaturalGlobalSinkState>()
            .expect("Invalid NaturalSort global sink state");
        let lstate = local_state
            .as_any_mut()
            .downcast_mut::<NaturalLocalSinkState>()
            .expect("Invalid NaturalSort local sink state");

        let mut chunks = gstate.chunks.lock().unwrap();
        chunks.append_from(&mut lstate.local_chunks)?;
        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, global_state: &dyn GlobalSinkState) -> Result<SinkFinalizeType> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<NaturalGlobalSinkState>()
            .expect("Invalid NaturalSort global sink state");
        if gstate.chunks.lock().unwrap().is_empty() {
            Ok(SinkFinalizeType::NoOutputPossible)
        } else {
            Ok(SinkFinalizeType::Ready)
        }
    }

    fn get_hash_groups(
        &self,
        ctx: &ExecutionContext,
        global_state: &dyn GlobalSinkState,
    ) -> Result<Vec<SortHashGroup>> {
        let gstate = global_state
            .as_any()
            .downcast_ref::<NaturalGlobalSinkState>()
            .expect("Invalid NaturalSort global sink state");

        if let Some(groups) = gstate.hash_groups.lock().unwrap().as_ref() {
            return Ok(groups.clone());
        }

        let memory = window_memory_context(ctx);
        let mut source_chunks = RetainedChunkVec::new(memory.clone());
        {
            let mut chunks = gstate.chunks.lock().unwrap();
            source_chunks.append_from(&mut chunks)?;
        }
        let keys = build_row_keys(source_chunks.as_slice(), &memory)?;
        if keys.is_empty() {
            *gstate.hash_groups.lock().unwrap() = Some(Vec::new());
            return Ok(Vec::new());
        }

        let groups = vec![SortHashGroup {
            hash_bin: 0,
            chunks: Arc::new(Mutex::new(source_chunks)),
            keys: Arc::new(Mutex::new(keys)),
        }];

        *gstate.hash_groups.lock().unwrap() = Some(groups.clone());
        Ok(groups)
    }
}

#[derive(Debug)]
struct WindowHashGroup {
    hash_bin: usize,
    stage: WindowGroupStage,
    chunks: Option<Arc<Mutex<RetainedChunkVec>>>,
    keys: Arc<Mutex<AccountedRowKeys>>,
    window_results: Option<AccountedWindowResults>,
    output_idx: usize,
}

impl WindowHashGroup {
    fn from_sort_group(group: SortHashGroup) -> Self {
        Self {
            hash_bin: group.hash_bin,
            stage: WindowGroupStage::Sort,
            chunks: Some(group.chunks),
            keys: group.keys,
            window_results: None,
            output_idx: 0,
        }
    }

    fn try_next_task(&self, group_idx: usize) -> Option<WindowSourceTask> {
        if self.stage == WindowGroupStage::Done {
            None
        } else {
            Some(WindowSourceTask {
                group_idx,
                stage: self.stage,
            })
        }
    }

    fn advance_stage(&mut self) {
        self.stage = match self.stage {
            WindowGroupStage::Sort => WindowGroupStage::Materialize,
            WindowGroupStage::Materialize => WindowGroupStage::Mask,
            WindowGroupStage::Mask => WindowGroupStage::Sink,
            WindowGroupStage::Sink => WindowGroupStage::Finalize,
            WindowGroupStage::Finalize => WindowGroupStage::GetData,
            WindowGroupStage::GetData | WindowGroupStage::Done => self.stage,
        };
    }

    fn mark_done(&mut self) {
        self.stage = WindowGroupStage::Done;
        self.output_idx = 0;
        self.window_results = None;
        self.chunks = None;
    }
}

#[derive(Debug)]
struct WindowSourceRuntime {
    groups: Vec<Option<WindowHashGroup>>,
    next_group_idx: usize,
}

impl WindowSourceRuntime {
    fn new(groups: Vec<WindowHashGroup>) -> Self {
        Self {
            groups: groups.into_iter().map(Some).collect(),
            next_group_idx: 0,
        }
    }

    fn try_next_task(&mut self) -> Option<WindowSourceTask> {
        while self.next_group_idx < self.groups.len() {
            match self.groups[self.next_group_idx].as_ref() {
                None => {
                    self.next_group_idx += 1;
                    continue;
                }
                Some(group) if group.stage == WindowGroupStage::Done => {
                    self.groups[self.next_group_idx] = None;
                    self.next_group_idx += 1;
                    continue;
                }
                Some(group) => return group.try_next_task(self.next_group_idx),
            }
        }
        None
    }

    fn finish_group_if_done(&mut self, idx: usize) {
        if let Some(group) = self.groups.get(idx).and_then(|g| g.as_ref()) {
            if group.stage == WindowGroupStage::Done {
                self.groups[idx] = None;
            }
        }

        while self.next_group_idx < self.groups.len() && self.groups[self.next_group_idx].is_none()
        {
            self.next_group_idx += 1;
        }
    }
}

/// Physical window operator.
///
/// Computes window functions over partitioned and ordered data.
/// This is a blocking operator that materializes all input before producing output.
#[derive(Debug)]
pub struct Window {
    /// Output types (input types + window function result types)
    types: Vec<LogicalType>,
    /// Window expressions to compute
    expressions: Vec<WindowExpression>,
    /// Child operator
    child: Arc<dyn PhysicalOperator>,
    /// Number of input columns (before window results)
    input_width: usize,
    /// Stored global sink state for the source phase.
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

#[derive(Debug)]
pub struct WindowGlobalSinkState {
    strategy: Arc<dyn SortStrategy>,
    strategy_sink_state: Box<dyn GlobalSinkState>,
}

impl GlobalSinkState for WindowGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "WindowGlobalSinkState"
    }
}

#[derive(Debug, Default)]
pub struct WindowLocalSinkState {
    strategy_local_state: Option<Box<dyn LocalSinkState>>,
}

impl LocalSinkState for WindowLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct WindowGlobalSourceState {
    strategy: Arc<dyn SortStrategy>,
    runtime: Mutex<WindowSourceRuntime>,
}

impl WindowGlobalSourceState {
    fn new(strategy: Arc<dyn SortStrategy>, groups: Vec<WindowHashGroup>) -> Self {
        Self {
            strategy,
            runtime: Mutex::new(WindowSourceRuntime::new(groups)),
        }
    }
}

impl GlobalSourceState for WindowGlobalSourceState {
    fn max_threads(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct WindowLocalSourceState;

impl LocalSourceState for WindowLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Window {
    /// Create a new window operator.
    pub fn new(expressions: Vec<WindowExpression>, child: Arc<dyn PhysicalOperator>) -> Self {
        let child_types = child.types().to_vec();
        let input_width = child_types.len();

        // Output types = input types + window result types
        let mut types = child_types;
        for expr in &expressions {
            types.push(expr.return_type());
        }

        Self {
            types,
            expressions,
            child,
            input_width,
            sink_state: Mutex::new(None),
        }
    }

    fn sort_expression_index(&self) -> usize {
        let mut idx = 0;
        let mut max_orders = 0;
        for (i, expr) in self.expressions.iter().enumerate() {
            if expr.orders.len() > max_orders {
                max_orders = expr.orders.len();
                idx = i;
            }
        }
        idx
    }

    fn create_sort_strategy(&self) -> Result<Arc<dyn SortStrategy>> {
        let sort_expr_idx = self.sort_expression_index();
        let sort_expr = &self.expressions[sort_expr_idx];
        let input_types = self.types[..self.input_width].to_vec();

        let strategy: Arc<dyn SortStrategy> = if !sort_expr.partitions.is_empty() {
            Arc::new(HashedSortStrategy::new(sort_expr, input_types)?)
        } else if !sort_expr.orders.is_empty() {
            Arc::new(FullSortStrategy::new(sort_expr, input_types)?)
        } else {
            Arc::new(NaturalSortStrategy)
        };

        Ok(strategy)
    }

    /// Compare two rows for partitioning.
    fn compare_partition(
        &self,
        chunks: &[Chunk],
        a: &RowKey,
        b: &RowKey,
        partitions: &[Expression],
    ) -> bool {
        same_partition(chunks, a, b, partitions)
    }

    /// Compare two rows for ordering.
    fn compare_order(
        &self,
        chunks: &[Chunk],
        a: &RowKey,
        b: &RowKey,
        orders: &[OrderByExpression],
    ) -> Ordering {
        for order in orders {
            let cmp = self.compare_order_value(chunks, a, b, order);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }

    /// Compare values for a single ORDER BY expression.
    fn compare_order_value(
        &self,
        chunks: &[Chunk],
        a: &RowKey,
        b: &RowKey,
        order: &OrderByExpression,
    ) -> Ordering {
        let Some(col_idx) = expr_column_index(&order.expression) else {
            return Ordering::Equal;
        };

        let chunk_a = &chunks[a.chunk_idx];
        let chunk_b = &chunks[b.chunk_idx];

        let is_null_a = chunk_a.data[col_idx].is_null(a.row_idx);
        let is_null_b = chunk_b.data[col_idx].is_null(b.row_idx);

        match (is_null_a, is_null_b) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if order.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, true) => {
                return if order.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, false) => {}
        }

        let cmp = compare_column_values(
            &chunk_a.data[col_idx],
            a.row_idx,
            &chunk_b.data[col_idx],
            b.row_idx,
        );

        if order.ascending {
            cmp
        } else {
            cmp.reverse()
        }
    }

    /// Find partition boundaries in sorted data.
    fn find_partitions(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        expr: &WindowExpression,
    ) -> Vec<Partition> {
        if sorted_keys.is_empty() {
            return Vec::new();
        }

        let mut partitions = Vec::new();
        let mut start = 0;

        for i in 1..sorted_keys.len() {
            if !self.compare_partition(
                chunks,
                &sorted_keys[i - 1],
                &sorted_keys[i],
                &expr.partitions,
            ) {
                partitions.push(Partition { start, end: i });
                start = i;
            }
        }

        // Last partition
        partitions.push(Partition {
            start,
            end: sorted_keys.len(),
        });

        partitions
    }

    /// Check if two rows are peers (same ORDER BY values).
    fn are_peers(
        &self,
        chunks: &[Chunk],
        a: &RowKey,
        b: &RowKey,
        orders: &[OrderByExpression],
    ) -> bool {
        self.compare_order(chunks, a, b, orders) == Ordering::Equal
    }

    /// Compute window function results for a partition.
    fn compute_window_function(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &WindowExpression,
        results: &mut AccountedWindowValues,
    ) -> Result<()> {
        let partition_size = partition.end - partition.start;

        match expr.function.function_type {
            WindowFunctionType::RowNumber => {
                for i in 0..partition_size {
                    results.push(WindowValue::BigInt((i + 1) as i64))?;
                }
            }
            WindowFunctionType::Rank => {
                let mut rank = 1i64;
                for i in 0..partition_size {
                    if i > 0 {
                        let prev_key = &sorted_keys[partition.start + i - 1];
                        let curr_key = &sorted_keys[partition.start + i];
                        if !self.are_peers(chunks, prev_key, curr_key, &expr.orders) {
                            rank = (i + 1) as i64;
                        }
                    }
                    results.push(WindowValue::BigInt(rank))?;
                }
            }
            WindowFunctionType::DenseRank => {
                let mut rank = 1i64;
                for i in 0..partition_size {
                    if i > 0 {
                        let prev_key = &sorted_keys[partition.start + i - 1];
                        let curr_key = &sorted_keys[partition.start + i];
                        if !self.are_peers(chunks, prev_key, curr_key, &expr.orders) {
                            rank += 1;
                        }
                    }
                    results.push(WindowValue::BigInt(rank))?;
                }
            }
            WindowFunctionType::PercentRank => {
                if partition_size <= 1 {
                    for _ in 0..partition_size {
                        results.push(WindowValue::Double(0.0))?;
                    }
                } else {
                    let mut rank = 1i64;
                    for i in 0..partition_size {
                        if i > 0 {
                            let prev_key = &sorted_keys[partition.start + i - 1];
                            let curr_key = &sorted_keys[partition.start + i];
                            if !self.are_peers(chunks, prev_key, curr_key, &expr.orders) {
                                rank = (i + 1) as i64;
                            }
                        }
                        let percent = (rank - 1) as f64 / (partition_size - 1) as f64;
                        results.push(WindowValue::Double(percent))?;
                    }
                }
            }
            WindowFunctionType::CumeDist => {
                let mut peer_end = 0usize;
                for i in 0..partition_size {
                    // Find end of peer group
                    if i >= peer_end {
                        peer_end = i + 1;
                        while peer_end < partition_size {
                            let curr_key = &sorted_keys[partition.start + i];
                            let next_key = &sorted_keys[partition.start + peer_end];
                            if !self.are_peers(chunks, curr_key, next_key, &expr.orders) {
                                break;
                            }
                            peer_end += 1;
                        }
                    }
                    let cume = peer_end as f64 / partition_size as f64;
                    results.push(WindowValue::Double(cume))?;
                }
            }
            WindowFunctionType::Ntile => {
                // Get n from first argument
                let n = self.get_ntile_buckets(chunks, sorted_keys, partition, expr);
                if n <= 0 {
                    for _ in 0..partition_size {
                        results.push(WindowValue::Null)?;
                    }
                } else {
                    let n = n as usize;
                    for i in 0..partition_size {
                        let bucket = (i * n / partition_size) + 1;
                        results.push(WindowValue::BigInt(bucket as i64))?;
                    }
                }
            }
            WindowFunctionType::Lead | WindowFunctionType::Lag => {
                self.compute_lead_lag(chunks, sorted_keys, partition, expr, results)?;
            }
            WindowFunctionType::FirstValue => {
                self.compute_first_value(chunks, sorted_keys, partition, expr, results)?;
            }
            WindowFunctionType::LastValue => {
                self.compute_last_value(chunks, sorted_keys, partition, expr, results)?;
            }
            WindowFunctionType::NthValue => {
                self.compute_nth_value(chunks, sorted_keys, partition, expr, results)?;
            }
            WindowFunctionType::Aggregate => {
                // Aggregate window functions not yet supported
                for _ in 0..partition_size {
                    results.push(WindowValue::Null)?;
                }
            }
        }

        Ok(())
    }

    /// Get NTILE bucket count from expression.
    fn get_ntile_buckets(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &WindowExpression,
    ) -> i64 {
        if expr.children.is_empty() {
            return 1;
        }

        // For MVP, assume constant value
        match &expr.children[0] {
            Expression::Constant(c) => match &c.value {
                Value::BigInt(v) => *v,
                Value::Integer(v) => *v as i64,
                _ => 1,
            },
            Expression::ColumnRef(col_ref) => {
                // Get value from first row of partition
                if partition.start < sorted_keys.len() {
                    let key = &sorted_keys[partition.start];
                    let chunk = &chunks[key.chunk_idx];
                    chunk.data[col_ref.binding.column_index]
                        .get_i64(key.row_idx)
                        .unwrap_or(1)
                } else {
                    1
                }
            }
            _ => 1,
        }
    }

    /// Compute LEAD/LAG function.
    fn compute_lead_lag(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &WindowExpression,
        results: &mut AccountedWindowValues,
    ) -> Result<()> {
        let partition_size = partition.end - partition.start;
        let is_lead = expr.function.function_type == WindowFunctionType::Lead;

        // Get offset (default 1)
        let offset = if expr.children.len() >= 2 {
            match &expr.children[1] {
                Expression::Constant(c) => match &c.value {
                    Value::BigInt(v) => *v,
                    Value::Integer(v) => *v as i64,
                    _ => 1,
                },
                _ => 1,
            }
        } else {
            1
        };

        // Get default value
        let default_value = if expr.children.len() >= 3 {
            self.get_window_value_from_expr(chunks, sorted_keys, partition, &expr.children[2])
        } else {
            WindowValue::Null
        };

        for i in 0..partition_size {
            let target_idx = if is_lead {
                i as i64 + offset
            } else {
                i as i64 - offset
            };

            if target_idx < 0 || target_idx >= partition_size as i64 {
                results.push(default_value.clone())?;
            } else {
                let key = &sorted_keys[partition.start + target_idx as usize];
                let value = self.get_value_from_row(chunks, key, expr);
                results.push(value)?;
            }
        }
        Ok(())
    }

    /// Compute FIRST_VALUE function.
    fn compute_first_value(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &WindowExpression,
        results: &mut AccountedWindowValues,
    ) -> Result<()> {
        let partition_size = partition.end - partition.start;
        if partition_size == 0 {
            return Ok(());
        }

        // Get value from first row
        let first_key = &sorted_keys[partition.start];
        let first_value = self.get_value_from_row(chunks, first_key, expr);

        for _ in 0..partition_size {
            results.push(first_value.clone())?;
        }
        Ok(())
    }

    /// Compute LAST_VALUE function.
    fn compute_last_value(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &WindowExpression,
        results: &mut AccountedWindowValues,
    ) -> Result<()> {
        let partition_size = partition.end - partition.start;
        if partition_size == 0 {
            return Ok(());
        }

        // For default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        // last_value is the current row's value
        // For MVP, we use the entire partition
        let last_key = &sorted_keys[partition.end - 1];
        let last_value = self.get_value_from_row(chunks, last_key, expr);

        for _ in 0..partition_size {
            results.push(last_value.clone())?;
        }
        Ok(())
    }

    /// Compute NTH_VALUE function.
    fn compute_nth_value(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &WindowExpression,
        results: &mut AccountedWindowValues,
    ) -> Result<()> {
        let partition_size = partition.end - partition.start;

        // Get n from second argument
        let n = if expr.children.len() >= 2 {
            match &expr.children[1] {
                Expression::Constant(c) => match &c.value {
                    Value::BigInt(v) => *v as usize,
                    Value::Integer(v) => *v as usize,
                    _ => 1,
                },
                _ => 1,
            }
        } else {
            1
        };

        let value = if n > 0 && n <= partition_size {
            let key = &sorted_keys[partition.start + n - 1];
            self.get_value_from_row(chunks, key, expr)
        } else {
            WindowValue::Null
        };

        for _ in 0..partition_size {
            results.push(value.clone())?;
        }
        Ok(())
    }

    /// Get value from a row for window function.
    fn get_value_from_row(
        &self,
        chunks: &[Chunk],
        key: &RowKey,
        expr: &WindowExpression,
    ) -> WindowValue {
        if expr.children.is_empty() {
            return WindowValue::Null;
        }

        self.get_window_value_from_expr(
            chunks,
            std::slice::from_ref(key),
            &Partition { start: 0, end: 1 },
            &expr.children[0],
        )
    }

    /// Get window value from expression.
    fn get_window_value_from_expr(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        partition: &Partition,
        expr: &Expression,
    ) -> WindowValue {
        match expr {
            Expression::Constant(c) => match &c.value {
                Value::Null(_) => WindowValue::Null,
                Value::BigInt(v) => WindowValue::BigInt(*v),
                Value::Integer(v) => WindowValue::Integer(*v),
                Value::Double(v) => WindowValue::Double(*v),
                Value::Varchar(v) => WindowValue::Varchar(v.clone()),
                _ => WindowValue::Null,
            },
            Expression::ColumnRef(col_ref) => {
                if partition.start < sorted_keys.len() {
                    let key = &sorted_keys[partition.start];
                    let chunk = &chunks[key.chunk_idx];
                    let vec = &chunk.data[col_ref.binding.column_index];

                    if vec.is_null(key.row_idx) {
                        return WindowValue::Null;
                    }

                    match vec.logical_type() {
                        LogicalType::BigInt => vec
                            .get_i64(key.row_idx)
                            .map(WindowValue::BigInt)
                            .unwrap_or(WindowValue::Null),
                        LogicalType::Integer => vec
                            .get_i32(key.row_idx)
                            .map(WindowValue::Integer)
                            .unwrap_or(WindowValue::Null),
                        LogicalType::Double => vec
                            .get_f64(key.row_idx)
                            .map(WindowValue::Double)
                            .unwrap_or(WindowValue::Null),
                        LogicalType::Varchar => vec
                            .get_string(key.row_idx)
                            .map(|s| WindowValue::Varchar(s.to_string()))
                            .unwrap_or(WindowValue::Null),
                        _ => WindowValue::Null,
                    }
                } else {
                    WindowValue::Null
                }
            }
            _ => WindowValue::Null,
        }
    }

    fn compute_group_window_results(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        memory: &MemoryAccountingContext,
    ) -> Result<AccountedWindowResults> {
        let mut all_results = AccountedWindowResults::new(memory);

        for expr in &self.expressions {
            let partitions = self.find_partitions(chunks, sorted_keys, expr);
            let mut expr_results = AccountedWindowValues::with_capacity(memory, sorted_keys.len())?;

            for partition in &partitions {
                self.compute_window_function(
                    chunks,
                    sorted_keys,
                    partition,
                    expr,
                    &mut expr_results,
                )?;
            }

            all_results.push(expr_results)?;
        }

        Ok(all_results)
    }

    /// Build output chunk with window results.
    fn build_output_chunk(
        &self,
        chunks: &[Chunk],
        sorted_keys: &[RowKey],
        window_results: &AccountedWindowResults,
        start_idx: usize,
        count: usize,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
    ) -> Result<Chunk> {
        let actual_count = count.min(sorted_keys.len().saturating_sub(start_idx));
        if actual_count == 0 {
            return Chunk::try_new(allocator);
        }

        let mut output_vectors = Vec::with_capacity(self.types.len());

        // Copy input columns
        for col_idx in 0..self.input_width {
            let col_type = &self.types[col_idx];
            let mut output_vec =
                Vector::try_new(col_type.clone(), actual_count, allocator.clone())?;

            for i in 0..actual_count {
                let key = &sorted_keys[start_idx + i];
                let src_chunk = &chunks[key.chunk_idx];
                let src_vec = &src_chunk.data[col_idx];

                if src_vec.is_null(key.row_idx) {
                    output_vec.set_null(i, true);
                } else {
                    output_vec.copy_at(i, src_vec, key.row_idx);
                }
            }

            output_vec.set_count(actual_count);
            output_vectors.push(Arc::new(output_vec));
        }

        // Add window result columns
        for (expr_idx, results) in window_results.iter().enumerate() {
            let col_type = &self.types[self.input_width + expr_idx];
            let mut output_vec =
                Vector::try_new(col_type.clone(), actual_count, allocator.clone())?;

            for i in 0..actual_count {
                let result = &results.as_slice()[start_idx + i];
                match result {
                    WindowValue::Null => output_vec.set_null(i, true),
                    WindowValue::BigInt(v) => output_vec.set_i64(i, *v),
                    WindowValue::Double(v) => output_vec.set_f64(i, *v),
                    WindowValue::Integer(v) => output_vec.set_i32(i, *v),
                    WindowValue::Varchar(v) => output_vec.set_string(i, v),
                }
            }

            output_vec.set_count(actual_count);
            output_vectors.push(Arc::new(output_vec));
        }

        let mut chunk = Chunk::from_arc_vectors(output_vectors, allocator);
        chunk.set_cardinality(actual_count);
        Ok(chunk)
    }
}

impl PhysicalOperator for Window {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Window
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        Window::runtime_memory_stats(self)
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        if self.expressions.is_empty() {
            return vec![];
        }

        let mut params = vec![format!(
            "Window Functions: {}",
            self.expressions
                .iter()
                .map(format_window_expression)
                .collect::<Vec<_>>()
                .join(", ")
        )];

        let mut externalized = false;
        if let Some(sink_state) = self.sink_state() {
            if let Some(window_sink_state) =
                sink_state.as_any().downcast_ref::<WindowGlobalSinkState>()
            {
                externalized = window_sink_state
                    .strategy
                    .is_externalized(window_sink_state.strategy_sink_state.as_ref());
            }
        }
        params.push(format!("External: {externalized}"));

        params
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn source_order(&self) -> OrderPreservationType {
        let sort_expr = self.sort_expression_index();
        let expr = &self.expressions[sort_expr];

        if !expr.partitions.is_empty() {
            OrderPreservationType::NoOrder
        } else if expr.orders.is_empty() {
            OrderPreservationType::InsertionOrder
        } else {
            OrderPreservationType::FixedOrder
        }
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut lock = self.sink_state.lock().unwrap();
        *lock = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().unwrap().clone()
    }

    // ========== Sink Interface ==========

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let strategy = self.create_sort_strategy()?;
        let strategy_sink_state = strategy.get_global_sink_state(ctx)?;

        Ok(Box::new(WindowGlobalSinkState {
            strategy,
            strategy_sink_state,
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(WindowLocalSinkState::default()))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<WindowGlobalSinkState>()
            .expect("Invalid global state type for Window");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<WindowLocalSinkState>()
            .expect("Invalid local state type for Window");

        if lstate.strategy_local_state.is_none() {
            lstate.strategy_local_state = Some(gstate.strategy.get_local_sink_state(ctx)?);
        }

        gstate.strategy.sink(
            ctx,
            chunk,
            gstate.strategy_sink_state.as_ref(),
            lstate
                .strategy_local_state
                .as_mut()
                .expect("window strategy local sink state missing")
                .as_mut(),
        )
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        input: &mut crate::operator::state::OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<WindowGlobalSinkState>()
            .expect("Invalid global state type for Window");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<WindowLocalSinkState>()
            .expect("Invalid local state type for Window");

        if lstate.strategy_local_state.is_none() {
            return Ok(SinkCombineResultType::Finished);
        }

        gstate.strategy.combine(
            ctx,
            gstate.strategy_sink_state.as_ref(),
            lstate
                .strategy_local_state
                .as_mut()
                .expect("window strategy local sink state missing")
                .as_mut(),
        )
    }

    fn finalize(
        &self,
        input: &crate::operator::state::OperatorSinkFinalizeInput,
    ) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<WindowGlobalSinkState>()
            .expect("Invalid global state type for Window");

        gstate
            .strategy
            .finalize(gstate.strategy_sink_state.as_ref())
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let maybe_stored = self.sink_state();

        let (strategy, groups) = if let Some(stored) = maybe_stored.as_ref() {
            let window_sink_state = stored
                .as_any()
                .downcast_ref::<WindowGlobalSinkState>()
                .ok_or_else(|| {
                    paro_common::error::internal(
                        "Invalid stored sink state type for Window".to_string(),
                    )
                })?;

            let strategy = Arc::clone(&window_sink_state.strategy);
            let groups =
                strategy.get_hash_groups(ctx, window_sink_state.strategy_sink_state.as_ref())?;
            (strategy, groups)
        } else {
            let window_sink_state = sink_state
                .ok_or_else(|| {
                    paro_common::error::internal(
                        "Window requires sink_state to create source state".to_string(),
                    )
                })?
                .as_any()
                .downcast_ref::<WindowGlobalSinkState>()
                .ok_or_else(|| {
                    paro_common::error::internal("Invalid sink state type for Window".to_string())
                })?;

            let strategy = Arc::clone(&window_sink_state.strategy);
            let groups =
                strategy.get_hash_groups(ctx, window_sink_state.strategy_sink_state.as_ref())?;
            (strategy, groups)
        };

        let window_groups = groups
            .into_iter()
            .map(WindowHashGroup::from_sort_group)
            .collect::<Vec<_>>();

        Ok(Box::new(WindowGlobalSourceState::new(
            strategy,
            window_groups,
        )))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(WindowLocalSourceState))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<WindowGlobalSourceState>()
            .ok_or_else(|| {
                paro_common::error::internal(
                    "Invalid global source state type for Window".to_string(),
                )
            })?;

        let mut runtime = gstate.runtime.lock().unwrap();

        loop {
            let Some(task) = runtime.try_next_task() else {
                chunk.set_cardinality(0);
                return Ok(SourceResultType::Finished);
            };

            let group = runtime.groups[task.group_idx]
                .as_mut()
                .expect("window hash group unexpectedly missing");

            match task.stage {
                WindowGroupStage::Sort => {
                    gstate.strategy.sort_column_data(group.hash_bin)?;
                    group.advance_stage();
                }
                WindowGroupStage::Materialize => {
                    gstate.strategy.materialize_column_data(group.hash_bin)?;
                    group.advance_stage();
                }
                WindowGroupStage::Mask => {
                    group.advance_stage();
                }
                WindowGroupStage::Sink => {
                    let memory = window_memory_context(ctx);
                    let results = {
                        let chunks = group
                            .chunks
                            .as_ref()
                            .expect("window group chunks should exist in sink stage")
                            .lock()
                            .unwrap();
                        let keys = group.keys.lock().unwrap();
                        self.compute_group_window_results(
                            chunks.as_slice(),
                            keys.as_slice(),
                            &memory,
                        )?
                    };
                    group.window_results = Some(results);
                    group.advance_stage();
                }
                WindowGroupStage::Finalize => {
                    // v1 finalize stage is lightweight; heavy finalize is inside window evaluators.
                    group.advance_stage();
                }
                WindowGroupStage::GetData => {
                    let keys_len = group.keys.lock().unwrap().len();
                    if group.output_idx >= keys_len {
                        group.mark_done();
                        runtime.finish_group_if_done(task.group_idx);
                        continue;
                    }

                    let output_count = (keys_len - group.output_idx).min(VECTOR_SIZE);
                    *chunk = {
                        let chunks = group
                            .chunks
                            .as_ref()
                            .expect("window group chunks should exist in getdata stage")
                            .lock()
                            .unwrap();
                        let keys = group.keys.lock().unwrap();
                        let window_results = group
                            .window_results
                            .as_ref()
                            .expect("window group results should be materialized before getdata");
                        self.build_output_chunk(
                            chunks.as_slice(),
                            keys.as_slice(),
                            window_results,
                            group.output_idx,
                            output_count,
                            chunk.allocator().clone(),
                        )?
                    };

                    group.output_idx += output_count;
                    if group.output_idx >= keys_len {
                        group.mark_done();
                        runtime.finish_group_if_done(task.group_idx);
                    }

                    return Ok(SourceResultType::HaveMoreOutput);
                }
                WindowGroupStage::Done => {
                    runtime.finish_group_if_done(task.group_idx);
                }
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Window {
    pub fn expressions(&self) -> &[WindowExpression] {
        &self.expressions
    }

    pub fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(window_sink_state) = sink_state.as_any().downcast_ref::<WindowGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };
        let spilled = window_sink_state
            .strategy
            .is_externalized(window_sink_state.strategy_sink_state.as_ref());
        let peak_memory_bytes = window_sink_state
            .strategy_sink_state
            .as_any()
            .downcast_ref::<SortBackedGlobalSinkState>()
            .and_then(|sort_backed| {
                sort_backed
                    .sort_state
                    .as_any()
                    .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>()
            })
            .map(|sort_sink_state| sort_sink_state.peak_reservation() as u64);
        ExplainRuntimeStats {
            spilled: Some(spilled),
            peak_memory_bytes,
            temp_storage_bytes: None,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::window::WindowFunction;
    use paro_planner::expression::{ReferenceExpression, WindowFrame};

    use crate::operator::scan::dummy_scan::PhysicalDummyScan;

    #[test]
    fn explain_params_include_external_flag() {
        let child = Arc::new(PhysicalDummyScan::with_types(vec![
            LogicalType::Integer,
            LogicalType::Integer,
        ])) as Arc<dyn PhysicalOperator>;

        let window_expr = WindowExpression {
            function: WindowFunction::row_number(),
            children: vec![],
            partitions: vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            orders: vec![OrderByExpression {
                expression: Expression::Reference(ReferenceExpression::new(
                    1,
                    LogicalType::Integer,
                )),
                ascending: false,
                nulls_first: false,
            }],
            frame: WindowFrame::default(),
            ignore_nulls: false,
            return_type: LogicalType::BigInt,
        };

        let window = Window::new(vec![window_expr], child);
        let params = window.explain_params();

        assert!(
            params.iter().any(|param| param == "External: false"),
            "expected external flag in explain params, got: {params:?}"
        );
    }
}
