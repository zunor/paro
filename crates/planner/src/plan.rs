// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical plan wrapper and plan-node metadata.

pub mod arena;
pub use arena::LogicalPlan;

use std::mem::ManuallyDrop;
use std::ops::ControlFlow;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::binder::context::BindContext;
use crate::operator::{ColumnBinding, LogicalOperator, LogicalOutputLayout};

/// Stable node identifier within a planning session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanNodeId(pub u32);

impl PlanNodeId {
    /// Shared id for synthetic plan nodes that do not participate in identity tracking.
    ///
    /// Callers may create multiple nested synthetic nodes with the same id
    /// (for example `execution::physical_plan::search_lowering`), so synthetic
    /// ids are intentionally not unique.
    pub const SYNTHETIC: Self = Self(0);

    /// Whether this id is the non-unique placeholder used by synthetic
    /// plans. Identity-sensitive caches must reject it instead of silently
    /// aliasing two occurrences.
    pub const fn is_synthetic(self) -> bool {
        self.0 == Self::SYNTHETIC.0
    }
}

/// Cardinality interval persisted on a logical plan node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CardinalityEstimate {
    pub min: u64,
    pub expected: u64,
    pub max: u64,
}

impl CardinalityEstimate {
    pub fn exact(n: u64) -> Self {
        Self {
            min: n,
            expected: n,
            max: n,
        }
    }
}

/// Provenance controls which optimizer phase owns a cardinality annotation.
///
/// Tree-local statistics can always be recomputed after a rewrite. A join
/// graph estimate, however, accounts for equality classes and joint domains
/// across the whole associative region; reconstructing it from one physical
/// tree cut loses that information.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum CardinalityProvenance {
    #[default]
    Statistics,
    JoinGraph,
}

/// Strength of a node-local uniqueness proof.
///
/// Structural proofs are valid for planning and cardinality, but only a proof
/// that remains rooted in an enforced catalog key may select execution paths
/// where a duplicate is diagnosed as storage corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UniqueKeyProvenance {
    CatalogEnforced,
    Structural,
}

/// One column of a unique key in a node's current output layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UniqueKeyColumn {
    pub output_index: usize,
    pub binding: ColumnBinding,
}

/// Cached unique-key proof produced by statistics gathering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UniqueKey {
    pub columns: Box<[UniqueKeyColumn]>,
    pub provenance: UniqueKeyProvenance,
}

impl UniqueKey {
    pub fn new(
        columns: impl IntoIterator<Item = UniqueKeyColumn>,
        provenance: UniqueKeyProvenance,
    ) -> Self {
        Self {
            columns: columns.into_iter().collect(),
            provenance,
        }
    }
}

/// Statistics attached to a logical plan node.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeStats {
    pub estimated_cardinality: Option<CardinalityEstimate>,
    pub cardinality_provenance: CardinalityProvenance,
    /// Conservative row-count estimate used only when this result becomes an
    /// irreversible materialization, such as a hash-build input.
    ///
    /// This is deliberately independent of `estimated_cardinality`: an
    /// equality or range selectivity can rank join orders without proving
    /// that a filtered fact subtree is safe to materialize at the reduced
    /// point estimate. It is neither a semantic upper bound nor a correctness
    /// proof and must not clamp the group's cardinality envelope.
    pub materialization_risk_cardinality: Option<u64>,
    /// Node-local unique keys derived once in the statistics post-order pass.
    pub unique_keys: Vec<UniqueKey>,
}

impl NodeStats {
    /// Invalidate facts whose proof is tied to this node's current output
    /// layout or child semantics.
    ///
    /// Cardinality annotations describe the node's row domain and have their
    /// own provenance contract. Unique-key witnesses additionally contain
    /// positional output ordinals, so an operator or child replacement must
    /// never carry them across the structural mutation implicitly.
    pub fn invalidate_structural_facts(&mut self) {
        self.unique_keys.clear();
    }

    /// Replace the complete row-count contract as one coherent update.
    pub fn set_cardinality(
        &mut self,
        estimate: CardinalityEstimate,
        provenance: CardinalityProvenance,
        materialization_risk_cardinality: Option<u64>,
    ) {
        self.estimated_cardinality = Some(estimate);
        self.cardinality_provenance = provenance;
        self.materialization_risk_cardinality = materialization_risk_cardinality;
    }

    /// Carry join-graph row estimates across a row-preserving wrapper.
    ///
    /// Unique keys are deliberately excluded: their positional layout must be
    /// re-derived by the wrapper's statistics fold.
    pub fn inherit_cardinality_from(&mut self, source: &Self) {
        self.estimated_cardinality = source.estimated_cardinality;
        self.cardinality_provenance = source.cardinality_provenance;
        self.materialization_risk_cardinality = source.materialization_risk_cardinality;
    }
}

/// Mutable, occurrence-owned IR at binder and local semantic-rule boundaries.
/// The relational search representation is [`LogicalPlan`], whose children
/// are immutable arena indices. This type must not be stored in Memo payloads.
#[derive(Debug)]
pub struct OwnedLogicalPlan {
    pub id: PlanNodeId,
    pub stats: NodeStats,
    pub operator: LogicalOperator,
}

#[derive(Debug)]
pub struct PlannedStatement {
    pub types: Vec<LogicalType>,
    pub names: Vec<String>,
    pub plan: OwnedLogicalPlan,
}

/// Stateful consumer for the canonical iterative post-order plan traversal.
///
/// `child_completed` is invoked at the same boundary a recursive walker would
/// return from one child and before it descends into the next sibling. Passes
/// that publish producer state for later siblings can therefore share the
/// traversal engine without duplicating its detach/rebuild machinery.
pub trait LogicalPlanPostOrderFolder<State> {
    fn child_completed(
        &mut self,
        _parent_skeleton: &arena::LogicalPlanNode<()>,
        _completed_children: &[Box<OwnedLogicalPlan>],
        _completed_states: &[State],
        _remaining_children: &[Box<OwnedLogicalPlan>],
    ) -> Result<()> {
        Ok(())
    }

    fn fold(
        &mut self,
        plan: OwnedLogicalPlan,
        child_states: Vec<State>,
    ) -> Result<(OwnedLogicalPlan, State)>;
}

struct ClosurePostOrderFolder<F> {
    transform: F,
}

impl<State, F> LogicalPlanPostOrderFolder<State> for ClosurePostOrderFolder<F>
where
    F: FnMut(OwnedLogicalPlan, Vec<State>) -> Result<(OwnedLogicalPlan, State)>,
{
    fn fold(
        &mut self,
        plan: OwnedLogicalPlan,
        child_states: Vec<State>,
    ) -> Result<(OwnedLogicalPlan, State)> {
        (self.transform)(plan, child_states)
    }
}

impl PlannedStatement {
    pub fn types(&self) -> Vec<LogicalType> {
        self.types.clone()
    }

    pub fn names(&self) -> Vec<String> {
        self.names.clone()
    }
}

impl OwnedLogicalPlan {
    pub fn new(bind_ctx: &BindContext, operator: LogicalOperator) -> Self {
        Self {
            id: bind_ctx.next_plan_id(),
            stats: NodeStats::default(),
            operator,
        }
    }

    pub fn synthetic(operator: LogicalOperator) -> Self {
        Self {
            id: PlanNodeId::SYNTHETIC,
            stats: NodeStats::default(),
            operator,
        }
    }

    pub fn dummy_scan(bind_ctx: &BindContext) -> Self {
        Self::new(bind_ctx, LogicalOperator::DummyScan)
    }

    /// Column names produced by this plan node (delegates to the wrapped operator).
    pub fn output_names(&self) -> Vec<String> {
        self.operator.output_names()
    }

    /// Logical types of output columns.
    pub fn types(&self) -> Vec<LogicalType> {
        self.output_layout().into_types()
    }

    /// Column bindings produced by this plan node.
    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.output_layout().into_bindings()
    }

    /// Execution-facing types and bindings produced by this node, derived in
    /// one stack-safe traversal and guaranteed to remain positionally aligned.
    /// SQL-visible display names have a separate contract.
    pub fn output_layout(&self) -> LogicalOutputLayout {
        self.operator.output_layout()
    }

    /// Child plan nodes (one level).
    pub fn children(&self) -> Vec<&OwnedLogicalPlan> {
        self.operator.children()
    }

    pub fn is_empty_result(&self) -> bool {
        matches!(self.operator, LogicalOperator::EmptyResult(_))
    }

    pub fn map_operator(self, f: impl FnOnce(LogicalOperator) -> LogicalOperator) -> Self {
        self.try_map_operator(|operator| Ok(f(operator)))
            .expect("infallible operator mapping cannot fail")
    }

    pub fn try_map_operator(
        self,
        f: impl FnOnce(LogicalOperator) -> Result<LogicalOperator>,
    ) -> Result<Self> {
        let (id, mut stats, operator) = self.into_parts();
        let operator = f(operator)?;
        stats.invalidate_structural_facts();
        Ok(Self {
            id,
            stats,
            operator,
        })
    }

    pub fn map_children(self, mut f: impl FnMut(OwnedLogicalPlan) -> OwnedLogicalPlan) -> Self {
        self.try_map_children(|child| Ok(f(child)))
            .expect("infallible child mapping cannot fail")
    }

    pub fn try_map_children(
        self,
        mut f: impl FnMut(OwnedLogicalPlan) -> Result<OwnedLogicalPlan>,
    ) -> Result<Self> {
        let (id, mut stats, operator) = self.into_parts();
        let operator = operator.try_map_owned_children(&mut f)?;
        stats.invalidate_structural_facts();
        Ok(Self {
            id,
            stats,
            operator,
        })
    }

    /// Rebuild children known to be relationally and positionally identical.
    ///
    /// This escape hatch exists for the canonical traversal engine and
    /// binder-owned structural copies. Rewriters must use
    /// [`OwnedLogicalPlan::try_map_children`], whose contract invalidates cached
    /// layout-dependent facts.
    pub(crate) fn try_rebuild_children_preserving_stats(
        self,
        mut f: impl FnMut(OwnedLogicalPlan) -> Result<OwnedLogicalPlan>,
    ) -> Result<Self> {
        let (id, stats, operator) = self.into_parts();
        Ok(Self {
            id,
            stats,
            operator: operator.try_map_owned_children(&mut f)?,
        })
    }

    /// Iteratively transform a plan after all of its children have been
    /// transformed, while folding one caller-defined state per subtree.
    ///
    /// Logical plans can become substantially deeper than the SQL surface
    /// shape after decorrelation and CTE rewrites. Keeping the traversal here
    /// gives every pass a bounded native stack and a single child
    /// detach/rebuild contract instead of duplicating recursive walkers.
    pub fn try_fold_post_order<State>(
        self,
        transform: impl FnMut(OwnedLogicalPlan, Vec<State>) -> Result<(OwnedLogicalPlan, State)>,
    ) -> Result<(OwnedLogicalPlan, State)> {
        self.try_fold_post_order_with(&mut ClosurePostOrderFolder { transform })
    }

    /// Canonical iterative post-order fold with a sibling-completion hook.
    pub fn try_fold_post_order_with<State>(
        self,
        folder: &mut impl LogicalPlanPostOrderFolder<State>,
    ) -> Result<(OwnedLogicalPlan, State)> {
        struct Frame<State> {
            skeleton: arena::LogicalPlanNode<()>,
            remaining: std::vec::IntoIter<Box<OwnedLogicalPlan>>,
            // Keep the detached child allocations; unboxing/reboxing here
            // adds an allocation for every unchanged ownership edge.
            #[allow(clippy::vec_box)]
            children: Vec<Box<OwnedLogicalPlan>>,
            child_states: Vec<State>,
        }

        impl<State> Frame<State> {
            fn detach(plan: OwnedLogicalPlan) -> Result<Self> {
                let (skeleton, detached) = arena::LogicalPlanNode::detach(plan);
                let child_count = detached.len();
                Ok(Self {
                    skeleton,
                    remaining: detached.into_iter(),
                    children: Vec::with_capacity(child_count),
                    child_states: Vec::with_capacity(child_count),
                })
            }

            fn rebuild(self) -> Result<(OwnedLogicalPlan, Vec<State>)> {
                let plan = self.skeleton.assemble(self.children)?;
                Ok((plan, self.child_states))
            }
        }

        let mut frames = vec![Frame::detach(self)?];
        loop {
            if let Some(child) = frames.last_mut().and_then(|frame| frame.remaining.next()) {
                frames.push(Frame::detach(*child)?);
                continue;
            }
            let frame = frames
                .pop()
                .ok_or_else(|| paro_error::internal("post-order traversal stack is empty"))?;
            let (plan, child_states) = frame.rebuild()?;
            let (plan, state) = folder.fold(plan, child_states)?;
            let Some(parent) = frames.last_mut() else {
                return Ok((plan, state));
            };
            parent.children.push(Box::new(plan));
            parent.child_states.push(state);
            folder.child_completed(
                &parent.skeleton,
                &parent.children,
                &parent.child_states,
                parent.remaining.as_slice(),
            )?;
        }
    }

    /// Iterative post-order map without a caller-visible fold state.
    pub fn try_map_post_order(
        self,
        mut transform: impl FnMut(OwnedLogicalPlan) -> Result<OwnedLogicalPlan>,
    ) -> Result<OwnedLogicalPlan> {
        self.try_fold_post_order(|plan, _children: Vec<()>| Ok((transform(plan)?, ())))
            .map(|(plan, ())| plan)
    }

    /// Replace one identified node with an owned, single-use transformation.
    ///
    /// Plan ids are unique except for [`PlanNodeId::SYNTHETIC`], which is
    /// rejected. Layout-dependent facts are invalidated on the replaced node
    /// before it reaches the closure and on every ancestor whose child
    /// changed; unrelated subtrees retain their facts.
    pub fn try_replace_node(
        self,
        target: PlanNodeId,
        replace: impl FnOnce(OwnedLogicalPlan) -> Result<OwnedLogicalPlan>,
    ) -> Result<(OwnedLogicalPlan, bool)> {
        if target == PlanNodeId::SYNTHETIC {
            return Err(paro_error::internal(
                "synthetic plan ids cannot identify a unique replacement target",
            ));
        }
        let mut replace = Some(replace);
        self.try_fold_post_order(|mut plan, child_replacements: Vec<bool>| {
            let child_replaced = child_replacements.into_iter().any(|replaced| replaced);
            if plan.id == target {
                plan.stats.invalidate_structural_facts();
                let replace = replace.take().ok_or_else(|| {
                    paro_error::internal("plan contains a duplicate non-synthetic node id")
                })?;
                return Ok((replace(plan)?, true));
            }
            if child_replaced {
                plan.stats.invalidate_structural_facts();
            }
            Ok((plan, child_replaced))
        })
    }

    /// Visit every node with a bounded native stack.
    pub fn try_visit_pre_order(
        &self,
        mut visitor: impl FnMut(&OwnedLogicalPlan) -> Result<()>,
    ) -> Result<()> {
        let mut pending = vec![self];
        while let Some(plan) = pending.pop() {
            visitor(plan)?;
            pending.extend(plan.children().into_iter().rev());
        }
        Ok(())
    }

    pub fn visit_children_mut<'a, F>(&'a mut self, f: F) -> ControlFlow<()>
    where
        F: FnMut(&'a mut OwnedLogicalPlan) -> ControlFlow<()>,
    {
        self.operator.visit_children_mut(f)
    }

    /// `true` if this subtree is a graph scan/expand chain (see `LogicalOperator::is_graph_chain`).
    pub fn is_graph_chain(&self) -> bool {
        self.operator.is_graph_chain()
    }

    /// Dismantle an owned node without running its custom tree destructor.
    ///
    /// This is the only supported way to move fields out of `OwnedLogicalPlan`:
    /// the type owns a stack-safe [`Drop`] implementation, so ordinary field
    /// moves are intentionally rejected by Rust.
    pub fn into_parts(self) -> (PlanNodeId, NodeStats, LogicalOperator) {
        let plan = ManuallyDrop::new(self);
        // SAFETY: `plan` will not be dropped, and every non-Copy field is read
        // exactly once into the returned ownership tuple.
        unsafe {
            (
                plan.id,
                std::ptr::read(&plan.stats),
                std::ptr::read(&plan.operator),
            )
        }
    }

    pub fn into_operator(self) -> LogicalOperator {
        let (_, _, operator) = self.into_parts();
        operator
    }

    /// Temporarily detach an operator while retaining the node metadata. The
    /// caller must install a replacement before publishing the plan again.
    pub fn take_operator(&mut self) -> LogicalOperator {
        self.stats.invalidate_structural_facts();
        std::mem::replace(&mut self.operator, LogicalOperator::DummyScan)
    }
}

impl Drop for OwnedLogicalPlan {
    fn drop(&mut self) {
        fn detach_children(operator: LogicalOperator) -> Vec<OwnedLogicalPlan> {
            let mut detached = Vec::new();
            let skeleton = operator
                .try_map_owned_children(&mut |child| {
                    detached.push(child);
                    Ok(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan))
                })
                .expect("infallible plan-child detachment cannot fail");
            // The skeleton owns only shallow dummy children. Dropping it here
            // cannot recurse into the original plan tree.
            drop(skeleton);
            detached
        }

        let root = std::mem::replace(&mut self.operator, LogicalOperator::DummyScan);
        let mut pending = detach_children(root);
        while let Some(mut plan) = pending.pop() {
            let operator = std::mem::replace(&mut plan.operator, LogicalOperator::DummyScan);
            pending.extend(detach_children(operator));
            // `plan` now owns no original descendants; its Drop is shallow.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{EmptyResult, ExpressionGet, Join};

    fn structural_key() -> UniqueKey {
        UniqueKey::new(
            [UniqueKeyColumn {
                output_index: 0,
                binding: ColumnBinding::new(1, 0),
            }],
            UniqueKeyProvenance::Structural,
        )
    }

    #[test]
    fn logical_plan_ids_share_bind_context_counter() {
        let root_ctx = BindContext::new();
        let child_ctx = root_ctx.create_child();

        let plan_a = OwnedLogicalPlan::dummy_scan(&root_ctx);
        let plan_b = OwnedLogicalPlan::dummy_scan(&child_ctx);
        let plan_c = OwnedLogicalPlan::dummy_scan(&root_ctx);

        assert_eq!(plan_a.id, PlanNodeId(1));
        assert_eq!(plan_b.id, PlanNodeId(2));
        assert_eq!(plan_c.id, PlanNodeId(3));
    }

    #[test]
    fn synthetic_plan_uses_shared_synthetic_id_and_default_stats() {
        let synthetic = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);

        assert_eq!(synthetic.id, PlanNodeId::SYNTHETIC);
        assert_eq!(synthetic.stats, NodeStats::default());
        assert!(!synthetic.is_empty_result());
    }

    #[test]
    fn structural_mutation_invalidates_positional_unique_keys() {
        let mut child = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);
        child.stats.unique_keys.push(structural_key());
        let mut plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::EmptyResult(EmptyResult::new(child)));
        plan.stats.unique_keys.push(structural_key());

        let plan = plan.map_children(|child| child);

        assert!(plan.stats.unique_keys.is_empty());
        assert_eq!(plan.children()[0].stats.unique_keys, vec![structural_key()]);
    }

    #[test]
    fn canonical_traversal_preserves_unmodified_node_facts() {
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);
        plan.stats.unique_keys.push(structural_key());

        let (plan, ()) = plan
            .try_fold_post_order(|plan, _: Vec<()>| Ok((plan, ())))
            .expect("identity traversal");

        assert_eq!(plan.stats.unique_keys, vec![structural_key()]);
    }

    #[test]
    fn node_replacement_invalidates_only_the_changed_ancestor_path() {
        let bind_context = BindContext::new();
        let mut left = OwnedLogicalPlan::dummy_scan(&bind_context);
        let left_id = left.id;
        left.stats.unique_keys.push(structural_key());
        let mut right = OwnedLogicalPlan::dummy_scan(&bind_context);
        right.stats.unique_keys.push(structural_key());
        let mut root = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Join(Join::cross(left, right)),
        );
        root.stats.unique_keys.push(structural_key());

        let (root, replaced) = root
            .try_replace_node(left_id, Ok)
            .expect("replace identified node");

        assert!(replaced);
        assert!(root.stats.unique_keys.is_empty());
        assert!(root.children()[0].stats.unique_keys.is_empty());
        assert_eq!(root.children()[1].stats.unique_keys, vec![structural_key()]);
    }

    #[test]
    fn post_order_fold_handles_deep_plans_without_native_recursion() {
        const DEPTH: usize = 10_000;
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);
        for _ in 0..DEPTH {
            plan =
                OwnedLogicalPlan::synthetic(LogicalOperator::EmptyResult(EmptyResult::new(plan)));
        }

        let (plan, node_count) = plan
            .try_fold_post_order(|plan, children: Vec<usize>| {
                Ok((plan, 1 + children.into_iter().sum::<usize>()))
            })
            .expect("bounded post-order traversal");
        assert_eq!(node_count, DEPTH + 1);

        drop(plan);
    }

    #[test]
    fn output_metadata_handles_deep_binary_join_chains_with_a_bounded_stack() {
        const DEPTH: usize = 10_000;
        const TEST_STACK_BYTES: usize = 512 * 1024;

        std::thread::Builder::new()
            .name("deep-output-layout".to_string())
            .stack_size(TEST_STACK_BYTES)
            .spawn(|| {
                fn leaf(table_index: usize) -> OwnedLogicalPlan {
                    OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                        table_index,
                        vec![],
                        vec![format!("c{table_index}")],
                        vec![LogicalType::Integer],
                    )))
                }

                let mut plan = leaf(0);
                for table_index in 1..=DEPTH {
                    plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::cross(
                        plan,
                        leaf(table_index),
                    )));
                }

                let names = plan.output_names();
                assert_eq!(names.len(), DEPTH + 1);
                assert!(names
                    .iter()
                    .enumerate()
                    .all(|(index, name)| name == &format!("c{index}")));
                drop(names);

                let layout = plan.output_layout();
                assert_eq!(layout.len(), DEPTH + 1);
                assert!(layout
                    .types()
                    .iter()
                    .all(|logical_type| logical_type == &LogicalType::Integer));
                assert!(layout
                    .bindings()
                    .iter()
                    .enumerate()
                    .all(|(index, binding)| { *binding == ColumnBinding::new(index, 0) }));
                drop(layout);

                drop(plan);
            })
            .expect("spawn bounded-stack schema test")
            .join()
            .expect("bounded-stack schema test completed");
    }
}
