// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime breaker handle registry.
//!
//! `crate::pipeline::handles::BreakerHandleCatalog` is lowering metadata.
//! This registry is created per execution attempt and owns the concrete
//! breaker handles referenced by role global state.

use std::marker::PhantomData;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};

use crate::physical::properties::PipelineProperties;
use crate::physical::row_type::RowType;
use crate::pipeline::graph::PipelineId;
use crate::pipeline::handles::{
    BreakerHandleCatalog, BreakerHandleEntry, BreakerHandleId, BreakerHandleKind,
};
use crate::runtime::context::OperatorCleanupContext;

use super::aggregate::AggregateHandle;
use super::cleanup::{CleanupReason, CleanupStatus, RuntimeCleanup};
use super::cte::CteHandle;
use super::delim::DelimHandle;
use super::external_table::ExternalTableHandle;
use super::join::JoinBuildHandle;
use super::materialized::MaterializedHandle;
use super::recursive::RecursiveTableHandle;
use super::set_operation::SetOperationHandle;
use super::sort::{SortHandle, TopNHandle};
use super::window::WindowHandle;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HandleRef<T> {
    id: BreakerHandleId,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for HandleRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for HandleRef<T> {}

impl<T> HandleRef<T> {
    pub fn new(id: BreakerHandleId) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn id(self) -> BreakerHandleId {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct BreakerHandleMetadata {
    pub id: BreakerHandleId,
    pub kind: BreakerHandleKind,
    pub row_type: RowType,
    pub producer: Option<PipelineId>,
    pub consumers: Box<[PipelineId]>,
    pub properties: PipelineProperties,
}

impl BreakerHandleMetadata {
    pub fn from_catalog_entry(entry: &BreakerHandleEntry) -> Self {
        Self {
            id: entry.id,
            kind: entry.kind,
            row_type: entry.row_type.clone(),
            producer: entry.producer,
            consumers: entry.consumers.to_vec().into_boxed_slice(),
            properties: entry.properties.clone(),
        }
    }
}

#[derive(Debug)]
pub enum RuntimeBreakerHandle {
    Materialized(Arc<MaterializedHandle>),
    Sort(Arc<SortHandle>),
    TopN(Arc<TopNHandle>),
    HashJoinBuild(Arc<JoinBuildHandle>),
    Aggregate(Arc<AggregateHandle>),
    Window(Arc<WindowHandle>),
    SetOperation(Arc<SetOperationHandle>),
    Cte(Arc<CteHandle>),
    Delim(Arc<DelimHandle>),
    RecursiveTable(Arc<RecursiveTableHandle>),
    ExternalTable(Arc<ExternalTableHandle>),
}

impl RuntimeBreakerHandle {
    fn from_catalog_entry(entry: &BreakerHandleEntry) -> Self {
        let metadata = BreakerHandleMetadata::from_catalog_entry(entry);
        match entry.kind {
            BreakerHandleKind::Materialized => {
                Self::Materialized(Arc::new(MaterializedHandle::new(metadata)))
            }
            BreakerHandleKind::Sort => Self::Sort(Arc::new(SortHandle::new(metadata))),
            BreakerHandleKind::TopN => Self::TopN(Arc::new(TopNHandle::new(metadata))),
            BreakerHandleKind::HashJoinBuild => {
                Self::HashJoinBuild(Arc::new(JoinBuildHandle::new(metadata)))
            }
            BreakerHandleKind::Aggregate => {
                Self::Aggregate(Arc::new(AggregateHandle::new(metadata)))
            }
            BreakerHandleKind::Window => Self::Window(Arc::new(WindowHandle::new(metadata))),
            BreakerHandleKind::SetOperation => {
                Self::SetOperation(Arc::new(SetOperationHandle::new(metadata)))
            }
            BreakerHandleKind::Cte => Self::Cte(Arc::new(CteHandle::new(metadata))),
            BreakerHandleKind::Delim => Self::Delim(Arc::new(DelimHandle::new(metadata))),
            BreakerHandleKind::RecursiveTable => {
                Self::RecursiveTable(Arc::new(RecursiveTableHandle::new(metadata)))
            }
            BreakerHandleKind::ExternalTable => {
                Self::ExternalTable(Arc::new(ExternalTableHandle::new(metadata)))
            }
        }
    }

    #[inline]
    pub fn id(&self) -> BreakerHandleId {
        self.metadata().id
    }

    #[inline]
    pub fn kind(&self) -> BreakerHandleKind {
        self.metadata().kind
    }

    pub fn metadata(&self) -> &BreakerHandleMetadata {
        match self {
            Self::Sort(handle) => handle.metadata(),
            Self::TopN(handle) => handle.metadata(),
            Self::Materialized(handle) => handle.metadata(),
            Self::Cte(handle) => handle.metadata(),
            Self::HashJoinBuild(handle) => handle.metadata(),
            Self::Aggregate(handle) => handle.metadata(),
            Self::Window(handle) => handle.metadata(),
            Self::SetOperation(handle) => handle.metadata(),
            Self::Delim(handle) => handle.metadata(),
            Self::RecursiveTable(handle) => handle.metadata(),
            Self::ExternalTable(handle) => handle.metadata(),
        }
    }

    pub fn cleanup_status(&self) -> CleanupStatus {
        match self {
            Self::Sort(handle) => handle.cleanup_status(),
            Self::TopN(handle) => handle.cleanup_status(),
            Self::Materialized(handle) => handle.cleanup_status(),
            Self::Cte(handle) => handle.cleanup_status(),
            Self::HashJoinBuild(handle) => handle.cleanup_status(),
            Self::Aggregate(handle) => handle.cleanup_status(),
            Self::Window(handle) => handle.cleanup_status(),
            Self::SetOperation(handle) => handle.cleanup_status(),
            Self::Delim(handle) => handle.cleanup_status(),
            Self::RecursiveTable(handle) => handle.cleanup_status(),
            Self::ExternalTable(handle) => handle.cleanup_status(),
        }
    }
}

impl RuntimeCleanup for RuntimeBreakerHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        match self {
            Self::Sort(handle) => handle.cleanup(ctx, reason),
            Self::TopN(handle) => handle.cleanup(ctx, reason),
            Self::Materialized(handle) => handle.cleanup(ctx, reason),
            Self::Cte(handle) => handle.cleanup(ctx, reason),
            Self::HashJoinBuild(handle) => handle.cleanup(ctx, reason),
            Self::Aggregate(handle) => handle.cleanup(ctx, reason),
            Self::Window(handle) => handle.cleanup(ctx, reason),
            Self::SetOperation(handle) => handle.cleanup(ctx, reason),
            Self::Delim(handle) => handle.cleanup(ctx, reason),
            Self::RecursiveTable(handle) => handle.cleanup(ctx, reason),
            Self::ExternalTable(handle) => handle.cleanup(ctx, reason),
        }
    }
}

pub trait TypedBreakerHandle: RuntimeCleanup + Send + Sync + std::fmt::Debug + 'static {
    const KIND: BreakerHandleKind;

    fn clone_from_slot(slot: &RuntimeBreakerHandle) -> Option<Arc<Self>>
    where
        Self: Sized;
}

macro_rules! typed_handle {
    ($ty:ty, $kind:expr, $variant:ident) => {
        impl TypedBreakerHandle for $ty {
            const KIND: BreakerHandleKind = $kind;

            fn clone_from_slot(slot: &RuntimeBreakerHandle) -> Option<Arc<Self>> {
                match slot {
                    RuntimeBreakerHandle::$variant(handle) => Some(handle.clone()),
                    _ => None,
                }
            }
        }
    };
}

typed_handle!(
    MaterializedHandle,
    BreakerHandleKind::Materialized,
    Materialized
);
typed_handle!(SortHandle, BreakerHandleKind::Sort, Sort);
typed_handle!(TopNHandle, BreakerHandleKind::TopN, TopN);
typed_handle!(
    JoinBuildHandle,
    BreakerHandleKind::HashJoinBuild,
    HashJoinBuild
);
typed_handle!(AggregateHandle, BreakerHandleKind::Aggregate, Aggregate);
typed_handle!(WindowHandle, BreakerHandleKind::Window, Window);
typed_handle!(
    SetOperationHandle,
    BreakerHandleKind::SetOperation,
    SetOperation
);
typed_handle!(CteHandle, BreakerHandleKind::Cte, Cte);
typed_handle!(DelimHandle, BreakerHandleKind::Delim, Delim);
typed_handle!(
    RecursiveTableHandle,
    BreakerHandleKind::RecursiveTable,
    RecursiveTable
);
typed_handle!(
    ExternalTableHandle,
    BreakerHandleKind::ExternalTable,
    ExternalTable
);

#[derive(Debug, Default)]
pub struct BreakerHandleRegistry {
    handles: Box<[RuntimeBreakerHandle]>,
}

impl BreakerHandleRegistry {
    pub fn from_catalog(catalog: &BreakerHandleCatalog) -> Result<Self> {
        catalog.validate()?;
        let handles = catalog
            .iter()
            .map(RuntimeBreakerHandle::from_catalog_entry)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self { handles })
    }

    #[inline]
    pub fn get<T>(&self, handle_ref: HandleRef<T>) -> Result<Arc<T>>
    where
        T: TypedBreakerHandle,
    {
        let Some(slot) = self.handles.get(handle_ref.id().index()) else {
            return Err(paro_error::internal("breaker handle id is out of bounds"));
        };
        T::clone_from_slot(slot).ok_or_else(|| {
            paro_error::internal(format!(
                "breaker handle type mismatch: id {} expected {:?}, got {:?}",
                handle_ref.id().index(),
                T::KIND,
                slot.kind()
            ))
        })
    }

    #[inline]
    pub fn get_by_id(&self, id: BreakerHandleId) -> Option<&RuntimeBreakerHandle> {
        self.handles.get(id.index())
    }

    #[inline]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &RuntimeBreakerHandle> {
        self.handles.iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    pub fn cleanup_all(
        &self,
        ctx: &mut OperatorCleanupContext,
        reason: CleanupReason,
    ) -> Result<()> {
        let mut first_error = None;
        for handle in self.handles.iter().rev() {
            if let Err(error) = handle.cleanup(ctx, reason) {
                ctx.query.errors.record_secondary(error.clone());
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn reset_materialized_handles_produced_by(&self, producers: &[PipelineId]) {
        for handle in self.handles.iter() {
            let RuntimeBreakerHandle::Materialized(materialized) = handle else {
                continue;
            };
            let Some(producer) = materialized.metadata().producer else {
                continue;
            };
            if producers.contains(&producer) {
                materialized.reset_for_reuse();
            }
        }
    }

    pub fn live_handle_count(&self) -> usize {
        self.handles
            .iter()
            .filter(|handle| handle.cleanup_status() == CleanupStatus::Live)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::test_utils::test_allocator;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementCancelReason};

    use crate::explain::profiler::OperatorProfiler;
    use crate::memory_runtime::QueryMemoryPool;
    use crate::operators::sort::topn_heap::{TopNBoundaryValue, TopNHeap};
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleId};
    use crate::runtime::breaker::{
        AggregateRuntimeState, CleanupStatus, HashAggregateRuntimeState, TopNRuntimeState,
    };
    use crate::runtime::context::{OperatorCleanupContext, QueryErrorId, QueryRuntimeContext};
    use crate::runtime::parameter::ParameterBindings;
    use crate::runtime::scratch::TaskMemoryGrants;
    use crate::runtime::QueryOutputPort;
    use crate::thread_context::ThreadContext;

    use super::*;

    fn row_type() -> RowType {
        RowType::new(vec!["a".to_string()], vec![LogicalType::Integer])
    }

    fn query_context() -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        )
    }

    fn with_cleanup_context<R>(
        query: &QueryRuntimeContext,
        f: impl FnOnce(&mut OperatorCleanupContext<'_>) -> R,
    ) -> R {
        let thread = ThreadContext::single_threaded();
        let memory = TaskMemoryGrants::detached(test_allocator());
        let mut profiler = OperatorProfiler::disabled();
        let mut ctx = OperatorCleanupContext {
            query,
            pipeline: None,
            operator: None,
            thread: &thread,
            memory: memory.call_scope(),
            cancel: &query.cancellation,
            profiler: &mut profiler,
        };
        f(&mut ctx)
    }

    fn all_kind_registry() -> (BreakerHandleRegistry, Vec<BreakerHandleId>) {
        let mut builder = BreakerHandleCatalogBuilder::default();
        let kinds = [
            BreakerHandleKind::Materialized,
            BreakerHandleKind::Sort,
            BreakerHandleKind::TopN,
            BreakerHandleKind::HashJoinBuild,
            BreakerHandleKind::Aggregate,
            BreakerHandleKind::Window,
            BreakerHandleKind::SetOperation,
            BreakerHandleKind::Cte,
            BreakerHandleKind::Delim,
            BreakerHandleKind::RecursiveTable,
            BreakerHandleKind::ExternalTable,
        ];
        let ids = kinds
            .iter()
            .map(|kind| builder.register(*kind, row_type(), PipelineProperties::default()))
            .collect::<Vec<_>>();
        let registry =
            BreakerHandleRegistry::from_catalog(&builder.finish()).expect("registry should build");
        (registry, ids)
    }

    fn assert_all_cleanup_status(registry: &BreakerHandleRegistry, expected: CleanupStatus) {
        for handle in registry.iter() {
            match handle {
                RuntimeBreakerHandle::Materialized(handle) => {
                    assert_eq!(handle.cleanup_status(), expected)
                }
                RuntimeBreakerHandle::Sort(handle) => assert_eq!(handle.cleanup_status(), expected),
                RuntimeBreakerHandle::TopN(handle) => assert_eq!(handle.cleanup_status(), expected),
                RuntimeBreakerHandle::HashJoinBuild(handle) => {
                    assert_eq!(handle.cleanup_status(), expected);
                    assert_eq!(handle.spill.cleanup_status(), expected);
                }
                RuntimeBreakerHandle::Aggregate(handle) => {
                    assert_eq!(handle.cleanup_status(), expected)
                }
                RuntimeBreakerHandle::Window(handle) => {
                    assert_eq!(handle.cleanup_status(), expected)
                }
                RuntimeBreakerHandle::SetOperation(handle) => {
                    assert_eq!(handle.cleanup_status(), expected)
                }
                RuntimeBreakerHandle::Cte(handle) => assert_eq!(handle.cleanup_status(), expected),
                RuntimeBreakerHandle::Delim(handle) => {
                    assert_eq!(handle.cleanup_status(), expected)
                }
                RuntimeBreakerHandle::RecursiveTable(handle) => {
                    assert_eq!(handle.cleanup_status(), expected)
                }
                RuntimeBreakerHandle::ExternalTable(_) => {}
            }
        }
    }

    fn seed_cleanup_payloads(registry: &BreakerHandleRegistry, ids: &[BreakerHandleId]) {
        let mut materialized_chunks = vec![Chunk::try_new(test_allocator()).expect("chunk")];
        registry
            .get::<MaterializedHandle>(HandleRef::new(ids[0]))
            .expect("materialized")
            .append_chunks(&mut materialized_chunks)
            .expect("append materialized");

        registry
            .get::<TopNHandle>(HandleRef::new(ids[2]))
            .expect("topn")
            .initialize(TopNRuntimeState {
                heap: TopNHeap::new(vec![LogicalType::Integer], &[], 1, 0),
                boundary: Arc::new(TopNBoundaryValue::new()),
            })
            .expect("initialize topn");

        registry
            .get::<AggregateHandle>(HandleRef::new(ids[4]))
            .expect("aggregate")
            .initialize(AggregateRuntimeState::Hash(HashAggregateRuntimeState {
                tables: Vec::new(),
                spilled_payloads: Vec::new(),
                spilled_states: Vec::new(),
                spilled_outputs: None,
                ordered_collectors: Vec::new(),
            }))
            .expect("initialize aggregate");

        let mut window_chunks = vec![Chunk::try_new(test_allocator()).expect("chunk")];
        registry
            .get::<WindowHandle>(HandleRef::new(ids[5]))
            .expect("window")
            .append_chunks(&mut window_chunks)
            .expect("append window");

        let mut cte_chunks = vec![Chunk::try_new(test_allocator()).expect("chunk")];
        registry
            .get::<CteHandle>(HandleRef::new(ids[7]))
            .expect("cte")
            .append_chunks(&mut cte_chunks)
            .expect("append cte");
    }

    #[test]
    fn registry_resolves_typed_handles_without_any_downcast() {
        let mut builder = BreakerHandleCatalogBuilder::default();
        let id = builder.register(
            BreakerHandleKind::Materialized,
            row_type(),
            PipelineProperties::default(),
        );
        let registry =
            BreakerHandleRegistry::from_catalog(&builder.finish()).expect("registry should build");

        let handle = registry
            .get::<MaterializedHandle>(HandleRef::new(id))
            .expect("materialized handle");

        assert_eq!(handle.metadata().id, id);
        assert_eq!(handle.metadata().kind, BreakerHandleKind::Materialized);
    }

    #[test]
    fn registry_rejects_wrong_typed_handle() {
        let mut builder = BreakerHandleCatalogBuilder::default();
        let id = builder.register(
            BreakerHandleKind::Materialized,
            row_type(),
            PipelineProperties::default(),
        );
        let registry =
            BreakerHandleRegistry::from_catalog(&builder.finish()).expect("registry should build");

        let err = registry
            .get::<JoinBuildHandle>(HandleRef::new(id))
            .expect_err("wrong handle type should fail");

        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn registry_allocates_runtime_handle_for_each_catalog_kind() {
        let mut builder = BreakerHandleCatalogBuilder::default();
        let kinds = [
            BreakerHandleKind::Materialized,
            BreakerHandleKind::Sort,
            BreakerHandleKind::TopN,
            BreakerHandleKind::HashJoinBuild,
            BreakerHandleKind::Aggregate,
            BreakerHandleKind::Window,
            BreakerHandleKind::SetOperation,
            BreakerHandleKind::Cte,
            BreakerHandleKind::Delim,
            BreakerHandleKind::RecursiveTable,
        ];
        let ids = kinds
            .iter()
            .map(|kind| builder.register(*kind, row_type(), PipelineProperties::default()))
            .collect::<Vec<_>>();
        let registry =
            BreakerHandleRegistry::from_catalog(&builder.finish()).expect("registry should build");

        assert_eq!(registry.len(), kinds.len());
        for (id, kind) in ids.into_iter().zip(kinds) {
            let slot = registry.get_by_id(id).expect("runtime handle slot");
            assert_eq!(slot.kind(), kind);
            assert_eq!(slot.id(), id);
        }
    }

    #[test]
    fn cleanup_all_cancelled_marks_every_breaker_and_releases_pending_state() {
        let (registry, ids) = all_kind_registry();
        seed_cleanup_payloads(&registry, &ids);

        let materialized = registry
            .get::<MaterializedHandle>(HandleRef::new(ids[0]))
            .expect("materialized");
        let window = registry
            .get::<WindowHandle>(HandleRef::new(ids[5]))
            .expect("window");
        let cte = registry
            .get::<CteHandle>(HandleRef::new(ids[7]))
            .expect("cte");
        assert_eq!(materialized.pending_chunk_count(), 1);
        assert_eq!(window.pending_chunk_count(), 1);
        assert_eq!(cte.pending_chunk_count(), 1);

        let query = query_context();
        with_cleanup_context(&query, |ctx| {
            registry
                .cleanup_all(
                    ctx,
                    CleanupReason::Cancelled(StatementCancelReason::UserRequest),
                )
                .expect("cleanup all");
        });

        assert_all_cleanup_status(&registry, CleanupStatus::Cancelled);
        assert_eq!(materialized.pending_chunk_count(), 0);
        assert_eq!(window.pending_chunk_count(), 0);
        assert_eq!(cte.pending_chunk_count(), 0);
        assert!(registry
            .get::<TopNHandle>(HandleRef::new(ids[2]))
            .expect("topn")
            .boundary()
            .is_err());
        assert!(registry
            .get::<AggregateHandle>(HandleRef::new(ids[4]))
            .expect("aggregate")
            .take_state()
            .expect("aggregate state")
            .is_none());
        assert_eq!(query.errors.secondary_count(), 0);

        with_cleanup_context(&query, |ctx| {
            registry
                .cleanup_all(ctx, CleanupReason::Failed(QueryErrorId::new(99)))
                .expect("idempotent cleanup");
        });
        assert_all_cleanup_status(&registry, CleanupStatus::Cancelled);
    }

    #[test]
    fn cleanup_all_failed_marks_every_breaker_from_live_state() {
        let (registry, ids) = all_kind_registry();
        seed_cleanup_payloads(&registry, &ids);

        let query = query_context();
        let root = query.record_operator_error(paro_common::error::internal("root failure"));
        with_cleanup_context(&query, |ctx| {
            registry
                .cleanup_all(ctx, CleanupReason::Failed(root))
                .expect("cleanup all failed");
        });

        assert_all_cleanup_status(&registry, CleanupStatus::Failed);
        assert_eq!(query.errors.root_error_id(), Some(root));
        assert_eq!(query.errors.secondary_count(), 0);
    }
}
