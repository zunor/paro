// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transactional staging of planner rewrites into Memo groups.

use super::*;

use smallvec::SmallVec;

#[cfg(test)]
thread_local! { static STAGING_PAYLOAD_CONSTRUCTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(super) struct StagedEquivalent {
    pub(super) key: LogicalExprKey,
    pub(super) payload: LogicalPayloadId,
    pub(super) operator_encoding: Box<[u8]>,
    pub(super) logical_properties: LogicalProperties,
    pub(super) cardinality: GroupCardinality,
}

/// Fact witnesses have deliberately different namespaces. A settlement
/// witness names an entry in the settlement-local fact transaction; native
/// witnesses distinguish a Memo boundary snapshot from a local shell edge.
/// Keeping the distinction in the shared contract prevents a private FactId
/// from being mistaken for a Memo fact or for a local derived fact.
#[derive(Debug, Clone)]
pub(super) enum ResidentInputFact {
    Memo(Arc<paro_planner::operator::bound_reference::BoundRelationFacts>),
    Local {
        node_id: paro_planner::plan::PlanNodeId,
        stats: NodeStats,
        columns: Box<[ColumnId]>,
        layout: paro_planner::operator::LogicalOutputLayout,
    },
}

#[derive(Debug, Clone)]
pub(super) enum ResidentInputFacts {
    Settlement(Box<[usize]>),
    Native(Box<[ResidentInputFact]>),
}

/// The one structural result passed from a planner producer to Memo staging.
/// Settlement and native preparation use the same identity namespace and the
/// same contract; only their fact-witness domain differs. Facts are evidence
/// for validation and dependency tracking, never a substitute for the current
/// Memo read at publication.
#[derive(Debug, Clone)]
pub(super) struct ResidentNodeContract {
    pub(super) operator_fingerprint: Fingerprint,
    pub(super) operator_encoding: Box<[u8]>,
    pub(super) scalar_roots: Box<[ScalarExprId]>,
    pub(super) output_columns: Box<[ColumnId]>,
    pub(super) output_layout: paro_planner::operator::LogicalOutputLayout,
    pub(super) input_facts: ResidentInputFacts,
}

impl ResidentNodeContract {
    fn input_fact_count(&self) -> usize {
        match &self.input_facts {
            ResidentInputFacts::Settlement(facts) => facts.len(),
            ResidentInputFacts::Native(facts) => facts.len(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ColumnInternOrigin {
    Derived,
    Internal,
}

/// Intern one layout into the single planner-session identity namespace.
/// Settlement and native preparation deliberately share this helper; the
/// origin policy only preserves the historical provenance for newly-created
/// columns and never changes an existing binding's identity.
pub(super) fn intern_columns_into(
    identity: &mut PlannerResidentIdentity<'_>,
    layout: &paro_planner::operator::LogicalOutputLayout,
    origin: ColumnInternOrigin,
    names: Option<&[String]>,
) -> Result<Box<[ColumnId]>> {
    layout
        .bindings()
        .iter()
        .zip(layout.types())
        .enumerate()
        .map(|(index, (binding, ty))| {
            if let Some(id) = identity
                .binding_ids
                .get(binding.table_index, binding.column_index, ty)
                .copied()
            {
                return Ok(id);
            }
            let origin = match origin {
                ColumnInternOrigin::Derived => ColumnOrigin::Derived {
                    key: typed_binding_fingerprint(
                        *binding,
                        logical_type_fingerprint(ty),
                    ),
                },
                ColumnInternOrigin::Internal => ColumnOrigin::Internal {
                    key: Fingerprint(identity.columns.len() as u128),
                },
            };
            let id = identity.columns.intern(
                ty.clone(),
                true,
                origin,
                ColumnVisibility::Visible,
                names.and_then(|names| names.get(index).cloned()),
            )?;
            identity.binding_ids.insert(
                binding.table_index,
                binding.column_index,
                ty,
                id,
            )?;
            Ok(id)
        })
        .collect()
}

pub(super) enum StagingInput {
    /// A settled occurrence already owned by the session arena.
    Arena(paro_planner::plan::arena::PlanIndex),
    /// A closed native shell whose leaves are immutable Memo group holes.
    /// The shell is already flattened in post-order, so staging maps its
    /// child links to Memo groups without detaching/assembling an owned tree
    /// or importing it into a second arena.
    Native {
        shell: NativeShell,
        /// A native preparation contract is produced in the same sidecar
        /// transaction as staging.  Empty means the caller intentionally
        /// needs the ordinary native path (for example a non-target rule).
        resident_nodes: HashMap<paro_planner::plan::PlanNodeId, ResidentNodeContract>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct NativeShell {
    pub(super) nodes: Box<[NativeNode]>,
    pub(super) root: usize,
}

#[derive(Debug, Clone)]
pub(super) struct NativeNode {
    pub(super) id: paro_planner::plan::PlanNodeId,
    pub(super) stats: NodeStats,
    pub(super) operator: LogicalOperator<NativeChild>,
    /// Proof lineage for a Memo expression copied into this shell. Fresh
    /// nodes emitted by a native rewrite leave this empty; copied nodes keep
    /// their selected transformation proofs so an outer rewrite cannot turn
    /// an adopted inner aggregate/domain choice into audit-only metadata.
    pub(super) source_proofs: Box<[EquivalenceProof]>,
}

#[derive(Debug, Clone)]
pub(super) enum NativeChild {
    Node(usize),
    /// A pattern binding that already names the Memo group it consumes.  This
    /// variant is used by native rule producers; unlike `Group`, it does not
    /// need a temporary BoundReferenceId → GroupId side map during staging.
    /// The reference still carries the immutable facts/layout contract used by
    /// the native cost and statistics paths.
    MemoGroup {
        group: GroupId,
        id: paro_planner::plan::PlanNodeId,
        stats: NodeStats,
        layout: paro_planner::operator::LogicalOutputLayout,
        names: Arc<[String]>,
        reference: paro_planner::operator::BoundReference,
    },
    Group {
        id: paro_planner::plan::PlanNodeId,
        stats: NodeStats,
        layout: paro_planner::operator::LogicalOutputLayout,
        names: Arc<[String]>,
        reference: paro_planner::operator::BoundReference,
    },
}

impl NativeChild {
    pub(super) fn memo_group(
        memo: &Memo,
        state: &PlannerTransformState,
        facts: &boundary::BoundarySnapshot,
        group: GroupId,
        layout: &PlannerBindingLayout,
        names: Arc<[String]>,
    ) -> Result<Self> {
        if layout.bindings().len() != layout.types().len() {
            return Err(paro_error::internal("native Memo operand has inconsistent binding/type arity"));
        }
        let group = memo.canonical_group(group);
        let transport = facts.transport(memo, state, group, layout)?;
        let reference = paro_planner::operator::BoundReference::new(
            paro_planner::operator::BoundReferenceId::group_hole(state.bind_context.next_plan_id().0),
            layout.bindings().to_vec(), layout.types().to_vec(),
        ).with_facts(transport)?;
        let stats = NodeStats {
            estimated_cardinality: facts.cardinality(memo, group),
            unique_keys: reference.facts.unique_keys.clone(),
            ..Default::default()
        };
        Ok(Self::MemoGroup {
            group, id: state.bind_context.next_plan_id(), stats,
            layout: layout.as_ref().clone(), names, reference,
        })
    }
}

impl NativeShell {
    /// Build a closed native shell directly from an exact pattern binding.
    ///
    /// The generic planner transformation path needs an `OwnedLogicalPlan` so
    /// legacy rules can inspect a subtree. Native rules already have the exact
    /// operator payload and the group-hole pattern, so rebuilding that tree is
    /// pure transport overhead. This constructor preserves the operator
    /// payload, attaches the Memo-owned boundary facts, and records only a
    /// post-order vector of native nodes.
    pub(super) fn from_pattern(
        memo: &Memo,
        state: &PlannerTransformState,
        binding: &PatternOperand,
        facts: &boundary::BoundarySnapshot,
    ) -> Result<Option<Self>> {
        Self::from_pattern_impl(memo, state, binding, facts, false)
            .map(|result| result.map(|(shell, _)| shell))
    }

    /// Build a native shell and retain the layouts calculated while the
    /// pattern is lowered.  `from_pattern` intentionally keeps its old
    /// allocation profile for callers that do not inspect layouts; native
    /// rules which need layouts should use this entry point instead of
    /// walking the immutable shell a second time.
    pub(super) fn from_pattern_with_layouts(
        memo: &Memo,
        state: &PlannerTransformState,
        binding: &PatternOperand,
        facts: &boundary::BoundarySnapshot,
    ) -> Result<Option<(Self, Vec<paro_planner::operator::LogicalOutputLayout>)>> {
        let Some((shell, Some(layouts))) =
            Self::from_pattern_impl(memo, state, binding, facts, true)?
        else {
            return Ok(None);
        };
        Ok(Some((shell, layouts)))
    }

    fn from_pattern_impl(
        memo: &Memo,
        state: &PlannerTransformState,
        binding: &PatternOperand,
        facts: &boundary::BoundarySnapshot,
        collect_layouts: bool,
    ) -> Result<Option<(Self, Option<Vec<paro_planner::operator::LogicalOutputLayout>>)>> {
        let mut group_names = HashMap::<usize, Arc<[String]>>::new();

        struct Built {
            child: NativeChild,
            layout: Arc<paro_planner::operator::LogicalOutputLayout>,
            names: Arc<[String]>,
        }

        fn group(
            memo: &Memo,
            state: &PlannerTransformState,
            facts: &boundary::BoundarySnapshot,
            group: GroupId,
            layout: &PlannerBindingLayout,
            group_names: &mut HashMap<usize, Arc<[String]>>,
        ) -> Result<Built> {
            let names = group_names
                .entry(layout.bindings().len())
                .or_insert_with(|| {
                    (0..layout.bindings().len())
                        .map(|index| format!("__bound_reference_{index}"))
                        .collect::<Vec<_>>()
                        .into()
                })
                .clone();
            Ok(Built {
                child: NativeChild::memo_group(memo, state, facts, group, layout, names.clone())?,
                layout: layout.clone(),
                names,
            })
        }

        fn expression(
            memo: &Memo,
            state: &PlannerTransformState,
            facts: &boundary::BoundarySnapshot,
            operand: &PatternOperand,
            nodes: &mut Vec<NativeNode>,
            layouts: &mut Option<Vec<paro_planner::operator::LogicalOutputLayout>>,
            expected_layout: Option<&PlannerBindingLayout>,
            group_names: &mut HashMap<usize, Arc<[String]>>,
        ) -> Result<Option<Built>> {
            match operand {
                PatternOperand::Group(group_id) => {
                    let Some(layout) = expected_layout else {
                        return Err(paro_error::internal(
                            "native pattern root cannot be an untyped Memo group",
                        ));
                    };
                    Ok(Some(group(
                        memo,
                        state,
                        facts,
                        *group_id,
                        layout,
                        group_names,
                    )?))
                }
                PatternOperand::Expression {
                    group: _group,
                    expression: expression_id,
                    children,
                } => {
                    let logical = memo.logical_expr(*expression_id).ok_or_else(|| {
                        paro_error::internal("native pattern references an unknown expression")
                    })?;
                    let payload = state
                        .payloads
                        .logical
                        .get(logical.payload.index())
                        .ok_or_else(|| {
                            paro_error::internal("native pattern references an unknown payload")
                        })?;
                    let metadata = state.metadata.get(&logical.payload).ok_or_else(|| {
                        paro_error::internal("native pattern payload has no metadata")
                    })?;
                    if children.len() != metadata.child_layouts.len() {
                        return Err(paro_error::internal(
                            "native pattern child arity disagrees with metadata",
                        ));
                    }
                    let mut built_children = SmallVec::<[Built; 2]>::with_capacity(children.len());
                    for (child, layout) in children.iter().zip(&metadata.child_layouts) {
                        let Some(built) = expression(
                            memo,
                            state,
                            facts,
                            child,
                            nodes,
                            layouts,
                            Some(layout),
                            group_names,
                        )?
                        else {
                            return Ok(None);
                        };
                        built_children.push(built);
                    }
                    let mut child_iter = built_children.iter().map(|child| child.child.clone());
                    let operator = payload
                        .semantic_template
                        .operator
                        .clone()
                        .try_map_child_links(&mut |_| {
                            child_iter.next().ok_or_else(|| {
                                paro_error::internal("native pattern lost an operator child")
                            })
                        })?;
                    if child_iter.next().is_some() {
                        return Err(paro_error::internal(
                            "native pattern retained excess operator children",
                        ));
                    }
                    let child_layouts = built_children
                        .iter()
                        .map(|child| child.layout.as_ref())
                        .collect::<SmallVec<[_; 2]>>();
                    let layout = Arc::new(operator.output_layout_from_child_refs(&child_layouts));
                    let child_names = built_children
                        .iter()
                        .map(|child| child.names.as_ref())
                        .collect::<SmallVec<[_; 2]>>();
                    let names: Arc<[String]> =
                        operator.output_names_from_child_refs(&child_names).into();
                    let mut stats = NodeStats::default();
                    stats.estimated_cardinality = facts.cardinality(
                        memo,
                        match operand {
                            PatternOperand::Expression { group, .. } => *group,
                            PatternOperand::Group(_) => unreachable!(),
                        },
                    );
                    let node_index = nodes.len();
                    nodes.push(NativeNode {
                        id: state.bind_context.next_plan_id(),
                        stats,
                        operator,
                        source_proofs: logical.proofs.iter().cloned().collect(),
                    });
                    if let Some(layouts) = layouts.as_mut() {
                        debug_assert_eq!(layouts.len(), node_index);
                        layouts.push(layout.as_ref().clone());
                    }
                    Ok(Some(Built {
                        child: NativeChild::Node(node_index),
                        layout,
                        names,
                    }))
                }
            }
        }

        let mut nodes = Vec::new();
        let mut layouts = collect_layouts.then(Vec::new);
        let Some(root) = expression(
            memo,
            state,
            facts,
            binding,
            &mut nodes,
            &mut layouts,
            None,
            &mut group_names,
        )?
        else {
            return Ok(None);
        };
        let NativeChild::Node(root) = root.child else {
            return Err(paro_error::internal(
                "native pattern root must be an expression",
            ));
        };
        Ok(Some((
            Self {
                nodes: nodes.into_boxed_slice(),
                root,
            },
            layouts,
        )))
    }

    pub(super) fn root_operator(&self) -> &LogicalOperator<NativeChild> {
        &self.nodes[self.root].operator
    }

    pub(super) fn layouts(&self) -> Result<Vec<paro_planner::operator::LogicalOutputLayout>> {
        let mut layouts =
            Vec::<paro_planner::operator::LogicalOutputLayout>::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let mut children = SmallVec::<[&NativeChild; 2]>::new();
            node.operator
                .visit_child_links(&mut |child| children.push(child));
            let mut child_layouts =
                SmallVec::<[paro_planner::operator::LogicalOutputLayout; 2]>::with_capacity(
                    children.len(),
                );
            for child in children {
                child_layouts.push(match child {
                    NativeChild::Node(index) => layouts.get(*index).cloned().ok_or_else(|| {
                        paro_error::internal("native shell layout references an incomplete node")
                    })?,
                    NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                        layout.clone()
                    }
                });
            }
            let refs = child_layouts.iter().collect::<SmallVec<[_; 2]>>();
            layouts.push(node.operator.output_layout_from_child_refs(&refs));
        }
        Ok(layouts)
    }

    pub(super) fn root_layout(&self) -> Result<paro_planner::operator::LogicalOutputLayout> {
        self.layouts()?
            .get(self.root)
            .cloned()
            .ok_or_else(|| paro_error::internal("native shell has no root layout"))
    }

    /// Lower an already-validated closed owned shell exactly once. The output
    /// is a compact post-order node list; real descendants become node
    /// indices and Memo holes keep their immutable BoundReference facts.
    pub(super) fn from_owned(
        root: OwnedLogicalPlan,
        selected_proofs: &HashMap<paro_planner::plan::PlanNodeId, Box<[EquivalenceProof]>>,
    ) -> Result<Self> {
        enum Frame {
            Enter(OwnedLogicalPlan),
            Exit {
                skeleton: paro_planner::plan::arena::LogicalPlanNode<()>,
                arity: usize,
            },
        }

        let mut pending = vec![Frame::Enter(root)];
        let mut completed = Vec::<NativeChild>::new();
        let mut nodes = Vec::<NativeNode>::new();
        while let Some(frame) = pending.pop() {
            match frame {
                Frame::Enter(plan) => {
                    if matches!(&plan.operator, LogicalOperator::BoundReference(_)) {
                        let layout = plan.output_layout();
                        let names = Arc::<[String]>::from(plan.output_names());
                        let (id, stats, operator) = plan.into_parts();
                        let LogicalOperator::BoundReference(reference) = operator else {
                            unreachable!("bound-reference match changed while consuming plan")
                        };
                        completed.push(NativeChild::Group {
                            id,
                            stats,
                            layout,
                            names,
                            reference,
                        });
                        continue;
                    }
                    let (skeleton, children) =
                        paro_planner::plan::arena::LogicalPlanNode::detach(plan);
                    let arity = children.len();
                    pending.push(Frame::Exit { skeleton, arity });
                    pending.extend(children.into_iter().rev().map(|child| Frame::Enter(*child)));
                }
                Frame::Exit { skeleton, arity } => {
                    let start = completed.len().checked_sub(arity).ok_or_else(|| {
                        paro_error::internal("native shell lost a child while flattening")
                    })?;
                    let children = completed.drain(start..).collect::<Vec<_>>();
                    let mut children = children.into_iter();
                    let operator = skeleton.operator.try_map_child_links(&mut |_| {
                        children.next().ok_or_else(|| {
                            paro_error::internal("native shell child arity mismatch")
                        })
                    })?;
                    if children.next().is_some() {
                        return Err(paro_error::internal(
                            "native shell retained excess child links",
                        ));
                    }
                    let id = nodes.len();
                    nodes.push(NativeNode {
                        id: skeleton.id,
                        stats: skeleton.stats,
                        operator,
                        source_proofs: selected_proofs
                            .get(&skeleton.id)
                            .cloned()
                            .unwrap_or_default(),
                    });
                    completed.push(NativeChild::Node(id));
                }
            }
        }
        let NativeChild::Node(root) = completed
            .pop()
            .ok_or_else(|| paro_error::internal("native shell has no non-bound-reference root"))?
        else {
            return Err(paro_error::internal(
                "native shell root must not be a Memo group hole",
            ));
        };
        if !completed.is_empty() {
            return Err(paro_error::internal(
                "native shell flattening left detached roots",
            ));
        }
        Ok(Self {
            nodes: nodes.into_boxed_slice(),
            root,
        })
    }
}

pub(super) struct StagingRequest {
    pub(super) input: StagingInput,
    pub(super) input_facts: boundary::BoundarySnapshot,
    pub(super) column_stats: SharedColumnStatistics,
    pub(super) column_stat_scopes: HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
    /// Structural lowering already produced by settlement or native
    /// preparation in the same planner-session catalogs. Each occurrence is
    /// consumed exactly once; an empty map intentionally selects the ordinary
    /// lowering path.
    pub(super) resident_nodes:
        HashMap<paro_planner::plan::PlanNodeId, ResidentNodeContract>,
    pub(super) target: StagingTarget,
    pub(super) regions: StagingRegionRequirements,
    /// Opaque Memo inputs retained by the transformed expression. Inputs
    /// legitimately discarded by a relational rewrite are removed before
    /// staging; every surviving transport node is consumed exactly once.
    pub(super) nested_group_holes: BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
    /// Selected proof lineage keyed by stable occurrence node id. New rewrite
    /// nodes do not inherit this map and receive only their own transformation
    /// proof when published.
    pub(super) selected_proofs: HashMap<paro_planner::plan::PlanNodeId, Box<[EquivalenceProof]>>,
}

pub(super) struct StagingTarget {
    pub(super) group: GroupId,
    pub(super) rule: RuleId,
    pub(super) budget_class: TransformationBudgetClass,
    pub(super) input_context: OptimizationContextId,
    pub(super) child_context: OptimizationContextId,
    pub(super) refined_cardinality_kind: Option<CardinalityRecipeKind>,
}

pub(super) struct StagingRegionRequirements {
    pub(super) preserved_facet: Option<Fingerprint>,
    pub(super) extended_required_facets: Box<[Fingerprint]>,
    pub(super) inherited_runtime_filter_facet: Option<Fingerprint>,
}

pub(super) fn stage_transformed_expression(
    request: StagingRequest,
    memo: &mut Memo,
    state: &mut PlannerTransformState,
) -> Result<Option<StagedEquivalent>> {
    let _b3 = crate::work_partition::enter_b3(crate::work_partition::Bucket::Staging);
    let StagingRequest {
        input,
        input_facts,
        column_stats,
        column_stat_scopes,
        resident_nodes,
        target:
            StagingTarget {
                group: target,
                rule,
                budget_class,
                input_context,
                child_context,
                refined_cardinality_kind,
            },
        regions:
            StagingRegionRequirements {
                preserved_facet: preserved_region_facet,
                extended_required_facets: extended_required_region_facets,
                inherited_runtime_filter_facet,
            },
        nested_group_holes,
        selected_proofs,
    } = request;

    #[derive(Clone)]
    struct NodeState {
        id: paro_planner::plan::PlanNodeId,
        group: GroupId,
        stats: NodeStats,
        /// Immutable child column identities are shared by every post-order
        /// consumer. Native staging clones `NodeState` while walking the
        /// flattened shell, so an `Arc` avoids copying this slice per edge.
        columns: Arc<[ColumnId]>,
        layout: Arc<paro_planner::operator::LogicalOutputLayout>,
        names: Arc<[String]>,
        region_scope: PlannerRegionScope,
        boundary_reference_id: Option<paro_planner::operator::BoundReferenceId>,
        boundary_facts: Option<Arc<paro_planner::operator::bound_reference::BoundRelationFacts>>,
    }

    struct StagingOptions<'a> {
        column_stats: &'a Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
        column_stat_scopes: &'a HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
        rule: RuleId,
        group_budget: BudgetDimension,
    }

    struct StagingSession<'a> {
        memo: &'a mut Memo,
        state: &'a mut PlannerTransformState,
        options: StagingOptions<'a>,
        pending_runtime_filter_facets: Vec<RegionFacet>,
        nested_group_holes: BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        facts: boundary::BoundarySnapshot,
        search_candidates: HashMap<paro_planner::plan::PlanNodeId, OwnedLogicalPlan>,
        selected_proofs: HashMap<paro_planner::plan::PlanNodeId, Box<[EquivalenceProof]>>,
        resident_nodes: HashMap<paro_planner::plan::PlanNodeId, ResidentNodeContract>,
    }

    enum NodeStagingInput {
        Settled {
            node: paro_planner::plan::arena::LogicalPlanNode<()>,
            layout: Arc<paro_planner::operator::LogicalOutputLayout>,
            resident: Option<ResidentNodeContract>,
        },
        Native {
            id: paro_planner::plan::PlanNodeId,
            stats: NodeStats,
            operator: LogicalOperator<GroupId>,
            source_proofs: Box<[EquivalenceProof]>,
            resident: Option<ResidentNodeContract>,
        },
    }

    struct NodeStagingRequest {
        input: NodeStagingInput,
        target: Option<GroupId>,
        required_region_facet: Option<Fingerprint>,
        inherited_runtime_filter_facet: Option<Fingerprint>,
        node_context: OptimizationContextId,
        target_child_context: Option<OptimizationContextId>,
        refined_cardinality_kind: Option<CardinalityRecipeKind>,
    }

    fn resolve_group_hole_reference(
        session: &mut StagingSession<'_>,
        id: paro_planner::plan::PlanNodeId,
        stats: NodeStats,
        layout: paro_planner::operator::LogicalOutputLayout,
        names: Arc<[String]>,
        reference: paro_planner::operator::BoundReference,
    ) -> Result<NodeState> {
        let group = session
            .nested_group_holes
            .remove(&reference.reference_id)
            .ok_or_else(|| {
                paro_error::internal("staging reached an unregistered Memo group hole")
            })?;
        resolve_group_reference(session, group, id, stats, layout, names, reference)
    }

    fn resolve_direct_group_reference(
        session: &mut StagingSession<'_>,
        group: GroupId,
        id: paro_planner::plan::PlanNodeId,
        stats: NodeStats,
        layout: paro_planner::operator::LogicalOutputLayout,
        names: Arc<[String]>,
        reference: paro_planner::operator::BoundReference,
    ) -> Result<NodeState> {
        resolve_group_reference(session, group, id, stats, layout, names, reference)
    }

    fn resolve_group_reference(
        session: &mut StagingSession<'_>,
        group: GroupId,
        id: paro_planner::plan::PlanNodeId,
        stats: NodeStats,
        layout: paro_planner::operator::LogicalOutputLayout,
        names: Arc<[String]>,
        reference: paro_planner::operator::BoundReference,
    ) -> Result<NodeState> {
        let bindings = layout.bindings();
        let types = layout.types();
        if bindings.len() != types.len() {
            return Err(paro_error::internal(
                "nested group hole has inconsistent binding/type arity",
            ));
        }
        let columns = bindings
            .into_iter()
            .zip(types)
            .map(|(&binding, logical_type)| {
                session
                    .state
                    .binding_ids
                    .get(binding.table_index, binding.column_index, &logical_type)
                    .copied()
                    .ok_or_else(|| {
                        paro_error::internal("nested group hole references an unknown column")
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let group = session.memo.canonical_group(group);
        let contract = session
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("nested group hole references an unknown group"))?;
        if columns.iter().copied().collect::<BTreeSet<_>>() != contract.schema.ids() {
            return Err(paro_error::internal(
                "nested group hole changes its referenced group schema",
            ));
        }
        Ok(NodeState {
            id,
            group,
            stats,
            columns: columns.into(),
            layout: Arc::new(layout),
            names,
            region_scope: PlannerRegionScope::group(group),
            boundary_reference_id: Some(reference.reference_id),
            boundary_facts: Some(reference.facts.clone()),
        })
    }

    fn stage_node(
        session: &mut StagingSession<'_>,
        request: NodeStagingRequest,
        child_states: Vec<NodeState>,
    ) -> Result<Option<(NodeState, Option<StagedEquivalent>)>> {
        let _b3 = crate::work_partition::enter_b3(crate::work_partition::Bucket::Encoding);
        let NodeStagingRequest {
            input,
            target,
            required_region_facet,
            inherited_runtime_filter_facet,
            node_context,
            target_child_context,
            refined_cardinality_kind,
        } = request;
        let (id, stats, semantic_operator, settled_layout, source_proofs, resident) = match input {
            NodeStagingInput::Settled {
                node,
                layout,
                resident,
            } => {
                crate::work_partition::settled_node(false);
                if let LogicalOperator::BoundReference(reference) = &node.operator {
                    if target.is_some()
                        || !session
                            .nested_group_holes
                            .contains_key(&reference.reference_id)
                    {
                        return Err(paro_error::internal(
                            "staging reached an unregistered or root Memo group hole",
                        ));
                    }
                    let names = Arc::from(node.operator.output_names_from_child_refs(&[]));
                    let LogicalOperator::BoundReference(reference) = node.operator else {
                        unreachable!()
                    };
                    let node = resolve_group_hole_reference(
                        session,
                        node.id,
                        node.stats,
                        Arc::unwrap_or_clone(layout),
                        names,
                        reference,
                    )?;
                    return Ok(Some((node, None)));
                }
                let source_proofs = session
                    .selected_proofs
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_default();
                (
                    node.id,
                    node.stats,
                    node.operator,
                    Some(layout),
                    source_proofs,
                    resident,
                )
            }
            NodeStagingInput::Native {
                id,
                stats,
                operator,
                source_proofs,
                resident,
            } => {
                let semantic_operator = operator
                    .clone()
                    .try_map_child_links(&mut |_| Ok::<_, std::convert::Infallible>(()))
                    .expect("mapping native group references to a semantic shell cannot fail");
                (
                    id,
                    stats.clone(),
                    semantic_operator,
                    None,
                    source_proofs,
                    resident,
                )
            }
        };
        crate::work_partition::staging_payload(false);
        let native_direct = settled_layout.is_none();
        if native_direct {
            if let LogicalOperator::Aggregate(aggregate) = &semantic_operator {
                if aggregate.groups.len() >= 9 {
                    tracing::debug!(
                        target: "paro::optimizer::native_staging",
                        groups = ?aggregate.groups,
                        "staged native aggregate grouping"
                    );
                }
            }
        }
        // The canonical extraction template deliberately has no output demand
        // or occurrence statistics. Derive the published schema/facts from the
        // settled occurrence, before erasing those annotations for storage.
        let memo = &mut *session.memo;
        let state = &mut *session.state;
        let options = &session.options;
        if id.is_synthetic() && !options.column_stat_scopes.is_empty() {
            return Err(paro_error::internal(
                "synthetic plan id cannot select a column-statistics scope",
            ));
        }
        let column_stats = options
            .column_stat_scopes
            .get(&id)
            .unwrap_or(options.column_stats);
        let pending_runtime_filter_facets = &mut session.pending_runtime_filter_facets;

        let child_layouts = child_states
            .iter()
            .map(|child| child.layout.as_ref())
            .collect::<Vec<_>>();
        if let Some(contract) = resident.as_ref() {
            if contract.input_fact_count() != child_states.len() {
                return Err(paro_error::internal(
                    "resident contract has an incompatible fact arity",
                ));
            }
            if let ResidentInputFacts::Native(expected_facts) = &contract.input_facts {
                for (expected, child) in expected_facts.iter().zip(&child_states) {
                    let Some(actual) = child.boundary_facts.as_ref() else {
                        return Err(paro_error::internal(
                            "native resident contract lost an input fact witness",
                        ));
                    };
                    match expected {
                        ResidentInputFact::Memo(expected) => {
                            // Pointer equality is the normal same-snapshot
                            // path; structural equality permits an
                            // equivalent transport rebuilt after an
                            // unchanged fact revision, but never accepts a
                            // changed column domain, lineage, uniqueness
                            // proof, or row bound.
                            if !Arc::ptr_eq(expected, actual)
                                && expected.as_ref() != actual.as_ref()
                            {
                                return Err(paro_error::internal(
                                    "native resident contract is stale for a Memo input fact",
                                ));
                            }
                        }
                        ResidentInputFact::Local {
                            node_id,
                            stats,
                            columns,
                            layout,
                        } => {
                            if child.id != *node_id
                                || child.stats != *stats
                                || child.columns.as_ref() != columns.as_ref()
                                || child.layout.as_ref() != layout
                            {
                                return Err(paro_error::internal(
                                    "native resident contract is stale for a local input fact",
                                ));
                            }
                        }
                    }
                }
            }
        }
        let output_layout = match (settled_layout, resident.as_ref()) {
            (Some(layout), Some(contract)) => {
                if layout.as_ref() != &contract.output_layout {
                    return Err(paro_error::internal(
                    "resident contract disagrees with arena output layout",
                    ));
                }
                layout
            }
            (Some(layout), None) => layout,
            (None, Some(contract)) => Arc::new(contract.output_layout.clone()),
            (None, None) => {
                Arc::new(semantic_operator.output_layout_from_child_refs(&child_layouts))
            }
        };
        let output_bindings = output_layout.bindings();
        let output_types = output_layout.types();
        let child_names = child_states
            .iter()
            .map(|child| child.names.as_ref())
            .collect::<Vec<_>>();
        let output_names =
            Arc::<[String]>::from(semantic_operator.output_names_from_child_refs(&child_names));
        if output_bindings.len() != output_types.len() {
            return Err(paro_error::internal(
                "transformed plan output binding/type arity mismatch",
            ));
        }
        let output_columns: Arc<[ColumnId]> = if let Some(contract) = resident.as_ref() {
            if contract.output_columns.len() != output_bindings.len()
                || contract.output_layout.bindings() != output_bindings
                || contract.output_layout.types() != output_types
            {
                return Err(paro_error::internal(
                    "settled resident contract changed output column mapping",
                ));
            }
            for ((binding, logical_type), column) in output_bindings
                .iter()
                .copied()
                .zip(output_types.iter())
                .zip(contract.output_columns.iter().copied())
            {
                if state
                    .binding_ids
                    .get(binding.table_index, binding.column_index, logical_type)
                    .copied()
                    != Some(column)
                {
                    return Err(paro_error::internal(
                        "resident contract uses a foreign column identity",
                    ));
                }
            }
            Arc::from(contract.output_columns.clone())
        } else {
            intern_columns_into(
                &mut PlannerResidentIdentity {
                    columns: &mut state.columns,
                    scalars: &mut state.scalars,
                    binding_ids: &mut state.binding_ids,
                },
                &output_layout,
                ColumnInternOrigin::Derived,
                Some(output_names.as_ref()),
            )?
            .into()
        };
        let unique_columns: BTreeSet<_> = output_columns.iter().copied().collect();
        let schema = GroupSchema::new(
            unique_columns
                .iter()
                .map(|id| {
                    state.columns.get(*id).cloned().ok_or_else(|| {
                        paro_error::internal("transformed plan lost a column descriptor")
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )?;
        let child_maximum_cardinalities = child_states
            .iter()
            .map(|child| {
                memo.group(child.group)
                    .and_then(|group| group.logical_properties.maximum_cardinality)
            })
            .collect::<Vec<_>>();
        let mut logical_properties =
            derive_logical_properties(&semantic_operator, &child_maximum_cardinalities);
        attach_group_column_domains(
            &mut logical_properties,
            &output_bindings,
            &output_columns,
            column_stats.as_ref(),
            &schema,
        )?;
        if let LogicalOperator::CTERef(reference) = &semantic_operator {
            logical_properties
                .cte_references
                .insert(cte_reference_domain(reference, &output_columns)?);
        }
        if let LogicalOperator::MaterializedCTE(cte) = &semantic_operator {
            if let Some(producer) = child_states.first() {
                memo.register_cte_producer(
                    cte.cte_index,
                    producer.group,
                    cte_producer_columns(cte, producer.group, memo, &state.binding_ids)?,
                )?;
            }
        }
        let output_rows_hard_upper = logical_properties.maximum_cardinality;
        let search_candidate = session.search_candidates.remove(&id);
        if search_candidate.is_some() {
            debug!(
                target: targets::OPTIMIZER,
                rule = options.rule.0,
                operator = ?semantic_operator.op_type(),
                "attached search provider to transformed logical expression"
            );
        }
        // Preserve binding semantics before Query IR interning replaces
        // operator expressions with scalar-arena references.
        // Scalar interning only borrows child column identities.  Cloning each
        // `Box<[ColumnId]>` here made every post-order staging node copy the
        // complete child layout before the native Memo key was built.
        let (scalar_roots, operator_fingerprint, operator_encoding) =
            if let Some(contract) = resident.as_ref() {
                // The arena occurrence is immutable between settlement and
                // staging.  Its contract was produced from that occurrence
                // and the same session catalogs, so copying these small ID
                // slices is sufficient; no scalar walk, interning, or
                // operator re-encoding is needed here.
                if contract.output_columns.as_ref() != output_columns.as_ref() {
                    return Err(paro_error::internal(
                        "settled resident contract disagrees with output identities",
                    ));
                }
                (
                    contract.scalar_roots.clone(),
                    contract.operator_fingerprint,
                    contract.operator_encoding.clone(),
                )
            } else {
                let child_columns = child_states
                    .iter()
                    .map(|child| child.columns.as_ref())
                    .collect::<Vec<_>>();
                let scalar_roots = intern_operator_scalars(
                    &semantic_operator,
                    &output_columns,
                    &child_columns,
                    &mut state.binding_ids,
                    &mut state.columns,
                    &mut state.scalars,
                )?;
                let (operator_fingerprint, operator_encoding) =
                    query_operator_identity(&semantic_operator, &scalar_roots, &state.scalars)?;
                (scalar_roots, operator_fingerprint, operator_encoding)
            };
        let key = LogicalExprKey {
            operator: operator_fingerprint,
            scalars: scalar_roots,
            children: child_states
                .iter()
                .map(|child| child.group)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        };
        let logical_identity = stable_cardinality_recipe(operator_fingerprint, &operator_encoding);
        let mut cardinality =
            derive_group_cardinality(&semantic_operator, &key.children, &stats, logical_identity);
        if let Some(target) = target {
            cardinality = if let Some(kind) = refined_cardinality_kind {
                cardinality.with_kind(kind)
            } else {
                let target = memo.canonical_group(target);
                memo.group(target)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "shape-only transformation targets an unknown cardinality group",
                        )
                    })?
                    .cardinality
                    .clone()
            };
        }

        if target.is_none() {
            // Group merges canonicalize child identities in the Memo without
            // rewriting this sidecar's historical keys. Compare children in
            // the current union-find domain so rediscovering the same shell
            // reuses its group instead of colliding in the allocation ledger.
            let equivalent_key =
                |candidate: &LogicalExprKey| {
                    candidate.operator == key.operator
                        && candidate.scalars == key.scalars
                        && candidate.children.len() == key.children.len()
                        && candidate.children.iter().zip(key.children.iter()).all(
                            |(left, right)| {
                                memo.canonical_group(*left) == memo.canonical_group(*right)
                            },
                        )
                };
            if let Some((group, _)) = state
                .expression_groups
                .range(
                    LogicalExprKey {
                        operator: key.operator,
                        scalars: Box::new([]),
                        children: Box::new([]),
                    }..,
                )
                .take_while(|(candidate, _)| candidate.operator == key.operator)
                .filter(|(candidate, _)| equivalent_key(candidate))
                .flat_map(|(_, candidates)| candidates.iter().copied())
                .find(|(group, logical)| {
                    let payload = memo.logical_expr(*logical).map(|logical| logical.payload);
                    let context_matches = payload
                        .and_then(|payload| state.metadata.get(&payload))
                        .is_some_and(|metadata| metadata.input_context == node_context);
                    let structure_matches = payload
                        .and_then(|payload| state.payloads.logical.get(payload.index()))
                        .is_some_and(|payload| {
                            payload.operator_encoding.as_ref() == operator_encoding.as_ref()
                        });
                    context_matches
                        && structure_matches
                        && memo.group(*group).is_some_and(|existing| {
                            existing.schema == schema
                                && existing
                                    .logical_properties
                                    .same_contract(&logical_properties)
                        })
                })
            {
                let group = memo.canonical_group(group);
                // Reusing identity must not discard facts derived in the new
                // semantic context. This is particularly important for a CTE
                // reference after predicate pushdown: its operator key is
                // unchanged, while the owner proves a much tighter row
                // domain. Equivalent facts intersect at the group boundary;
                // no payload-local snapshot is allowed to freeze the older
                // estimate.
                memo.update_group_facts(group, |existing, existing_cardinality| {
                    existing.merge_equivalent_facts(&logical_properties)?;
                    *existing_cardinality = std::mem::take(existing_cardinality)
                        .canonical_with(cardinality.clone());
                    Ok(())
                })?;
                return Ok(Some((
                    NodeState {
                        id,
                        group,
                        stats: stats.clone(),
                        columns: Arc::clone(&output_columns),
                        layout: Arc::clone(&output_layout),
                        names: Arc::clone(&output_names),
                        region_scope: PlannerRegionScope::new(
                            group,
                            child_states.iter().map(|child| child.region_scope.clone()),
                        ),
                        boundary_reference_id: None,
                        boundary_facts: None,
                    },
                    None,
                )));
            }
        }

        let group = if let Some(target) = target {
            let target = memo.canonical_group(target);
            let contract = memo.group(target).ok_or_else(|| {
                paro_error::internal("transformation targets an unknown equivalence group")
            })?;
            if contract.schema != schema
                || !contract
                    .logical_properties
                    .same_contract(&logical_properties)
            {
                return Err(paro_error::internal(format!(
                    "transformation rule {} changed its target group logical contract: target_schema={:?}, output_schema={schema:?}, target_properties={:?}, output_properties={logical_properties:?}",
                    options.rule.0,
                    contract.schema, contract.logical_properties,
                )));
            }
            target
        } else {
            let allocation_identity = {
                let mut allocation = StableFingerprintBuilder::default();
                allocation.write_bytes(b"paro.transformed-group.v2");
                allocation.write_fingerprint(logical_identity);
                allocation.write_u64(node_context.0 as u64);
                allocation.write_bytes(&operator_encoding);
                // The optional-group ledger identity must distinguish the
                // same operator shell over different child groups.  The
                // logical key keeps children separate, but omitting them
                // here turns a legitimate alternative into a duplicate
                // allocation error when two native CTE partitions have the
                // same local operator over different producer groups.
                allocation.write_u64(key.children.len() as u64);
                for child in &key.children {
                    allocation.write_u64(memo.canonical_group(*child).0 as u64);
                }
                allocation.write_u64(schema.columns().len() as u64);
                for column in schema.columns() {
                    allocation.write_u64(column.id.0 as u64);
                    allocation.write_u64(column.nullable as u64);
                }
                allocation.write_u64(logical_properties.unique_keys.len() as u64);
                for key in &logical_properties.unique_keys {
                    allocation.write_u64(key.len() as u64);
                    for column in key {
                        allocation.write_u64(column.0 as u64);
                    }
                }
                allocation.write_u64(logical_properties.outer_references.len() as u64);
                for column in &logical_properties.outer_references {
                    allocation.write_u64(column.0 as u64);
                }
                allocation.finish()
            };
            let Some(group) = memo.create_optional_group(
                options.group_budget,
                allocation_identity,
                schema,
                logical_properties.clone(),
                cardinality.clone(),
            )?
            else {
                return Ok(None);
            };
            group
        };
        let region_scope = PlannerRegionScope::new(
            group,
            child_states.iter().map(|child| child.region_scope.clone()),
        );

        if target.is_some() {
            if let Some(existing) =
                memo.logical_expr_for_structural_key(group, &key, &operator_encoding)
            {
                let existing_context = state
                    .metadata
                    .get(&existing.payload)
                    .map(|metadata| metadata.input_context)
                    .ok_or_else(|| {
                        paro_error::internal("existing target expression lost planner metadata")
                    })?;
                if existing_context != node_context {
                    // Structural identity is not occurrence identity. Until
                    // the Memo index carries context as a first-class key, an
                    // advisory rewrite that collides with the same relational
                    // key in another expression-path context must decline.
                    // Returning `None` lets the outer TransformContext roll
                    // back every recursively staged child and sidecar write;
                    // this expected miss is not an optimizer corruption.
                    return Ok(Some((
                        NodeState {
                            id,
                            group,
                            stats: stats.clone(),
                            columns: Arc::clone(&output_columns),
                            layout: Arc::clone(&output_layout),
                            names: Arc::clone(&output_names),
                            region_scope,
                            boundary_reference_id: None,
                            boundary_facts: None,
                        },
                        None,
                    )));
                }
                return Ok(Some((
                    NodeState {
                        id,
                        group,
                        stats: stats.clone(),
                        columns: Arc::clone(&output_columns),
                        layout: Arc::clone(&output_layout),
                        names: Arc::clone(&output_names),
                        region_scope,
                        boundary_reference_id: None,
                        boundary_facts: None,
                    },
                    Some(StagedEquivalent {
                        key,
                        payload: existing.payload,
                        operator_encoding,
                        logical_properties,
                        cardinality,
                    }),
                )));
            }
        }

        // Identity reuse still performs all fact merges and context checks.
        // Only new payloads consume the extraction template and cost vectors.
        // Read the same child snapshots, never post-merge Memo statistics.
        crate::work_partition::staging_payload(true);
        #[cfg(test)]
        STAGING_PAYLOAD_CONSTRUCTIONS.with(|count| count.set(count.get() + 1));
        // The old path instantiated each child transport, assembled the
        // parent, detached it, and assembled it again before identity lookup.
        // Keep the established owned capability/cost contract, but construct
        // its one-node view only for a genuinely new payload. Exact child
        // facts have already been reconciled at the publication boundary.
        let cost_plan = if native_direct {
            None
        } else {
            crate::work_partition::settled_node(true);
            let children = child_states.iter().map(|child| {
                let reference = paro_planner::operator::BoundReference::new(
                    child.boundary_reference_id.ok_or_else(|| paro_error::internal("settled child lost its occurrence"))?,
                    child.layout.bindings().to_vec(),
                    child.layout.types().to_vec(),
                ).with_facts(child.boundary_facts.clone().ok_or_else(|| paro_error::internal("settled child lost published facts"))?)?;
                Ok(Box::new(OwnedLogicalPlan {
                    id: child.id, stats: child.stats.clone(),
                    operator: LogicalOperator::BoundReference(reference),
                }))
            }).collect::<Result<Vec<_>>>()?;
            Some(paro_planner::plan::arena::LogicalPlanNode {
                id, stats: stats.clone(), operator: semantic_operator.clone(),
            }.assemble(children)?)
        };
        let semantic_template = semantic_plan::canonical_template(
            paro_planner::plan::arena::LogicalPlanNode {
                id, stats: NodeStats::default(), operator: semantic_operator.clone(),
            },
        );
        let native_cost_inputs = native_direct.then(|| {
            let child_row_widths = child_states
                .iter()
                .map(|child| planner_row_width_from_layout(&child.layout, state.scan_access_cost))
                .collect::<Vec<_>>();
            let child_expected_rows = child_states
                .iter()
                .map(|child| {
                    child
                        .stats
                        .estimated_cardinality
                        .map(|cardinality| cardinality.expected as f64)
                        .unwrap_or(1.0)
                })
                .collect::<Vec<_>>();
            let child_materialization_risk_rows = child_states
                .iter()
                .map(|child| {
                    child
                        .stats
                        .materialization_risk_cardinality
                        .or_else(|| child.stats.estimated_cardinality.map(|rows| rows.max))
                        .unwrap_or(1)
                })
                .collect::<Vec<_>>();
            let output_row_width =
                planner_row_width_from_layout(&output_layout, state.scan_access_cost);
            (
                child_row_widths,
                child_expected_rows,
                child_materialization_risk_rows,
                output_row_width,
            )
        });
        let Some(scalar_facts) = super::super::scalar_facts::NativeScalarFacts::derive(
            &semantic_template.operator,
            &key.scalars,
            &state.scalars,
            &state.binding_ids,
            &state.columns,
            || memo.control().checkpoint(),
        )?
        else {
            return Ok(None);
        };
        let (payload, baseline_payload) = state.payloads.push_logical(PlannerLogicalPayload {
            scalar_facts,
            semantic_template,
            operator_encoding: operator_encoding.clone(),
            column_stats: column_stats.clone(),
        });
        let search = search_candidate
            .map(|search_plan| {
                stage_search_implementation(
                    SearchStagingRequest {
                        plan: search_plan,
                        expected_output_bindings: &output_bindings,
                        expected_output_types: &output_types,
                        output_columns: &output_columns,
                        materialized_columns: &unique_columns,
                        binding_ids: &state.binding_ids,
                        operator_fingerprint,
                        output_rows_hard_upper,
                        column_stats: column_stats.as_ref(),
                        scan_access_cost: state.scan_access_cost,
                    },
                    &mut state.payloads,
                )
            })
            .transpose()?;
        let native_filter_inputs = if native_direct
            && matches!(
                semantic_operator,
                LogicalOperator::Join(Join::Comparison(_))
            ) {
            child_states
                .iter()
                .map(|child| {
                    child
                        .boundary_facts
                        .as_deref()
                        .map(|facts| RuntimeFilterInput::Boundary {
                            layout: child.layout.as_ref(),
                            facts,
                        })
                })
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let implementations = if native_direct {
            planner_native_implementation_set(
                &semantic_operator,
                state.rowset_scan_pushdown,
                &native_filter_inputs,
            )
        } else {
            planner_implementation_set(
                cost_plan
                    .as_ref()
                    .expect("owned staging input must retain its cost plan"),
                state.rowset_scan_pushdown,
            )
        };
        if let LogicalOperator::Join(Join::Comparison(join)) = &semantic_operator {
            debug!(
                target: targets::OPTIMIZER,
                rule = options.rule.0,
                baseline = ?implementations.baseline,
                join_type = ?join.join_type,
                runtime_filter_candidate = implementations.hash_join_runtime_filter,
                build_left_runtime_filter_candidate = implementations.hash_join_build_left_runtime_filter,
                probe_operator = ?semantic_operator.op_type(),
                conditions = ?join.conditions,
                "staged transformed physical join implementation set"
            );
        }
        let runtime_filter_candidate = implementations.hash_join_runtime_filter
            || implementations.hash_join_build_left_runtime_filter;
        let runtime_filter_scope =
            runtime_filter_candidate.then(|| std::iter::once(group).collect::<BTreeSet<_>>());
        let local_cost = if native_direct {
            let (
                child_row_widths,
                child_expected_rows,
                _child_materialization_risk_rows,
                output_row_width,
            ) = native_cost_inputs
                .as_ref()
                .expect("native staging input must retain native cost facts");
            planner_native_operator_cost(
                &semantic_operator,
                &stats,
                child_states.len(),
                output_rows_hard_upper,
                &child_maximum_cardinalities,
                child_expected_rows,
                child_row_widths,
                *output_row_width,
            )?
        } else {
            planner_operator_cost(
                cost_plan
                    .as_ref()
                    .expect("owned staging input must retain its cost plan"),
                child_states.len(),
                output_rows_hard_upper,
                &child_maximum_cardinalities,
                state.scan_access_cost,
            )?
        };
        let cost_facts = if native_direct {
            let (
                child_row_widths,
                _child_expected_rows,
                child_materialization_risk_rows,
                output_row_width,
            ) = native_cost_inputs
                .as_ref()
                .expect("native staging input must retain native cost facts");
            planner_native_cost_facts(
                &semantic_operator,
                child_materialization_risk_rows,
                child_row_widths,
                *output_row_width,
                state.scan_access_cost,
                &native_filter_inputs,
                column_stats.as_ref(),
                &state.binding_ids,
            )?
        } else {
            planner_cost_facts(
                cost_plan
                    .as_ref()
                    .expect("owned staging input must retain its cost plan"),
                column_stats.as_ref(),
                &state.binding_ids,
                state.scan_access_cost,
            )?
        };
        let metadata = PlannerOperatorMetadata {
            origin_rule: Some(options.rule),
            selected_proofs: source_proofs,
            operator_type: semantic_operator.op_type(),
            operator_fingerprint,
            provided: ProvidedProperties {
                ordering: derive_provided_ordering(
                    &semantic_operator,
                    &output_columns,
                    child_states.first().map(|child| child.columns.as_ref()),
                    &state.binding_ids,
                ),
                partitioning: ProvidedPartitioning::Singleton,
                materialization: ProvidedMaterialization {
                    values: unique_columns,
                    locators: BTreeMap::new(),
                },
                mutation_safety: ProvidedMutationSafety::NotApplicable,
                representation: ProvidedRepresentation::Flat,
                replayability: ProvidedReplayability::OnePass,
                result_guarantee: provided_result_guarantee(&semantic_operator),
            },
            local_cost,
            implementations,
            grant_dependency: planner_grant_dependency(&semantic_operator),
            spillable: planner_operator_spillable(&semantic_operator),
            cost_facts,
            output_columns: output_columns.to_vec().into_boxed_slice(),
            child_layouts: child_states
                .iter()
                .map(|child| Arc::clone(&child.layout))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            child_required: intern_child_requirements(
                memo,
                child_states.iter().map(|child| child.columns.as_ref()),
            )?,
            child_row_goals: child_row_goals(&semantic_operator, child_states.len()),
            search,
            input_context: node_context,
            child_context: target_child_context.unwrap_or(node_context),
            required_region_facet: target.and(required_region_facet),
            runtime_filter_region_facet: None,
            structural_retained_children: planner_structural_retained_children(&semantic_operator),
            baseline_payload,
        };
        if state.metadata.insert(payload, metadata).is_some() {
            return Err(paro_error::internal(
                "transformed planner payload metadata was assigned twice",
            ));
        }

        let staged = if target.is_some() {
            if runtime_filter_candidate {
                let mut facet = if let Some(fingerprint) = inherited_runtime_filter_facet {
                    memo.regions()
                        .nodes
                        .iter()
                        .flat_map(|region| region.facets.iter())
                        .find(|facet| facet.fingerprint == fingerprint)
                        .cloned()
                        .ok_or_else(|| {
                            paro_error::internal(
                                "transformation inherited an unknown runtime-filter facet",
                            )
                        })?
                } else {
                    let mut facet = planner_region_facet(
                        RegionFacetKind::RuntimeFilter,
                        FacetCriticality::Optional,
                        logical_identity,
                        operator_fingerprint,
                        group,
                        BTreeSet::new(),
                    );
                    facet.priority = 2_000 + RegionFacetKind::RuntimeFilter as u16;
                    facet
                };
                facet
                    .scope
                    .extend(runtime_filter_scope.clone().expect("candidate scope"));
                let fingerprint = facet.fingerprint;
                state
                    .metadata
                    .get_mut(&payload)
                    .ok_or_else(|| {
                        paro_error::internal("transformed runtime-filter payload disappeared")
                    })?
                    .runtime_filter_region_facet = Some(fingerprint);
                pending_runtime_filter_facets.push(facet);
            }
            Some(StagedEquivalent {
                key,
                payload,
                operator_encoding,
                logical_properties,
                cardinality,
            })
        } else {
            let logical = memo.insert_logical_with_operator_encoding_and_tag(
                group,
                key.clone(),
                payload,
                EquivalenceProof::TransformationDescendant { rule: options.rule },
                operator_encoding,
                operator_tag(semantic_operator.op_type()),
            )?;
            state.record_expression_group(key, group, logical);
            if runtime_filter_candidate {
                let mut facet = planner_region_facet(
                    RegionFacetKind::RuntimeFilter,
                    FacetCriticality::Optional,
                    logical_identity,
                    operator_fingerprint,
                    group,
                    runtime_filter_scope.expect("candidate scope"),
                );
                facet.priority = 2_000 + RegionFacetKind::RuntimeFilter as u16;
                let fingerprint = facet.fingerprint;
                // Publish the tentative ownership before normalization.  If
                // this optional facet makes the forest exceed its structural
                // bound, the common dropped-facet path can now disable both
                // the facet and its implementation atomically.  Leaving the
                // implementation enabled with `None` ownership would admit a
                // physical artifact that no region can prove.
                state
                    .metadata
                    .get_mut(&payload)
                    .ok_or_else(|| {
                        paro_error::internal("dynamic runtime-filter payload disappeared")
                    })?
                    .runtime_filter_region_facet = Some(fingerprint);
                pending_runtime_filter_facets.push(facet);
            }
            None
        };
        Ok(Some((
            NodeState {
                id,
                group,
                stats: stats.clone(),
                columns: output_columns,
                layout: output_layout,
                names: output_names,
                region_scope,
                boundary_reference_id: None,
                boundary_facts: None,
            },
            staged,
        )))
    }

    let provider_roots = match &input {
        StagingInput::Arena(plan) => {
            let plan_view = state.staging_arena.plan(*plan)?;
            crate::search::optimizer::SearchOptimizer::candidate_arena_roots(&plan_view)?
        }
        // A native shell has no real Get/scan leaf. Search providers require
        // an owned scan occurrence and therefore cannot be produced from this
        // direct GroupRef path; such rules remain on the settled path.
        StagingInput::Native { .. } => Vec::new(),
    };
    let search_context = if !provider_roots.is_empty() {
        let session_context = state
            .session
            .clone()
            .ok_or_else(|| paro_error::internal("search planning has no statement context"))?;
        let mut search_context =
            crate::context::OptimizationContext::new(session_context, state.bind_context.clone());
        search_context.column_stats = column_stats.clone();
        search_context.cost_model = state.cost_model.clone();
        search_context.verify_enabled = state.verify_enabled;
        Some(search_context)
    } else {
        None
    };

    // Search providers consume an explicit bounded Filter/TopN pattern. Read
    // that window before detaching its children; the rest of staging operates
    // on scalar shells and immutable group facts, never rebuilt descendants.
    let mut search_candidates = HashMap::new();
    if let Some(context) = &search_context {
        for index in provider_roots {
            if !memo.control().checkpoint()? {
                return Ok(None);
            }
            let node = state
                .staging_arena
                .export_checked(index, || context.session.cancellation.check())?;
            if let Some(candidate) = crate::search::optimizer::SearchOptimizer::new()
                .physical_candidate_for_root(&node, context)?
            {
                if node.id.is_synthetic() {
                    return Err(paro_error::internal(
                        "synthetic plan id cannot identify a search candidate",
                    ));
                }
                if search_candidates.insert(node.id, candidate).is_some() {
                    return Err(paro_error::internal(
                        "search provider has ambiguous occurrence identity",
                    ));
                }
            }
        }
    }
    let (root, staged, pending_runtime_filter_facets) = {
        let mut session = StagingSession {
            memo,
            state,
            options: StagingOptions {
                column_stats: &column_stats,
                column_stat_scopes: &column_stat_scopes,
                rule,
                group_budget: budget_class.group_dimension(),
            },
            pending_runtime_filter_facets: Vec::new(),
            nested_group_holes,
            facts: input_facts,
            search_candidates,
            selected_proofs,
            resident_nodes,
        };
        let (root, staged) = match input {
            StagingInput::Arena(root_index) => {
                use paro_planner::plan::arena::{LogicalPlanNode, PlanIndex};
                session.state.staging_arena.get(root_index)?;
                let mut completed = BTreeMap::<PlanIndex, NodeState>::new();
                let mut root_result = None;
                let Some(post_order) = session
                    .state
                    .staging_arena
                    .post_order_controlled(root_index, || session.memo.control().checkpoint())?
                else {
                    return Ok(None);
                };
                for index in post_order {
                    if !session.memo.control().checkpoint()? {
                        return Ok(None);
                    }
                    if let Some(statement) = &session.state.session {
                        statement.cancellation.check()?;
                    }
                    let node = session.state.staging_arena.get(index)?.clone();
                    let layout = session.state.staging_arena.shared_output_layout(index)?;
                    let resident = session.resident_nodes.remove(&node.id);
                    let is_root = index == root_index;
                    let mut child_states = Vec::new();
                    let operator = node.operator.try_map_child_links(&mut |child| {
                        let state = completed
                            .get(&child)
                            .ok_or_else(|| paro_error::internal("staging lost an arena input"))?;
                        child_states.push(state.clone());
                        Ok::<_, paro_error::ParoError>(())
                    })?;
                    let plan = LogicalPlanNode {
                        id: node.id,
                        stats: node.stats,
                        operator,
                    };
                    let Some((mut node, staged)) = stage_node(
                        &mut session,
                        NodeStagingRequest {
                            input: NodeStagingInput::Settled {
                                node: plan,
                                layout,
                                resident,
                            },
                            target: is_root.then_some(target),
                            required_region_facet: is_root
                                .then_some(preserved_region_facet)
                                .flatten(),
                            inherited_runtime_filter_facet: is_root
                                .then_some(inherited_runtime_filter_facet)
                                .flatten(),
                            node_context: if is_root {
                                input_context
                            } else {
                                child_context
                            },
                            target_child_context: is_root.then_some(child_context),
                            refined_cardinality_kind: is_root
                                .then_some(refined_cardinality_kind)
                                .flatten(),
                        },
                        child_states,
                    )?
                    else {
                        return Ok(None);
                    };
                    if !is_root {
                        let layout = node.layout.clone();
                        let facts = if let Some(facts) = node.boundary_facts.clone() {
                            facts
                        } else {
                            session
                                .facts
                                .settle_group(session.memo, session.state, node.group)?;
                            session.facts.transport(
                                session.memo,
                                session.state,
                                node.group,
                                &layout,
                            )?
                        };
                        let reference_id = node.boundary_reference_id.unwrap_or_else(|| {
                            paro_planner::operator::BoundReferenceId::node_occurrence(node.id.0)
                        });
                        node.boundary_facts = Some(facts);
                        node.boundary_reference_id = Some(reference_id);
                        completed.insert(index, node.clone());
                    }
                    if is_root {
                        root_result = Some((node, staged));
                    } else {
                        debug_assert!(staged.is_none());
                    }
                }
                root_result.ok_or_else(|| paro_error::internal("staging has no completed root"))?
            }
            StagingInput::Native {
                shell: native,
                resident_nodes,
            } => {
                session.resident_nodes = resident_nodes;
                if native.nodes.is_empty() || native.root >= native.nodes.len() {
                    return Err(paro_error::internal("native staging has no valid root"));
                }
                let node_count = native.nodes.len();
                let root_index = native.root;
                let mut completed = Vec::<Option<(GroupId, NodeState)>>::with_capacity(node_count);
                let mut root_result = None;
                for (index, native_node) in native.nodes.into_vec().into_iter().enumerate() {
                    if !session.memo.control().checkpoint()? {
                        return Ok(None);
                    }
                    if let Some(statement) = &session.state.session {
                        statement.cancellation.check()?;
                    }
                    let is_root = index == root_index;
                    let mut child_states = Vec::new();
                    let operator = native_node.operator.try_map_child_links(&mut |child| {
                        match child {
                            NativeChild::Node(child_index) => {
                                let (group, state) = completed
                                    .get(child_index)
                                    .and_then(Option::as_ref)
                                    .cloned()
                                    .ok_or_else(|| {
                                        paro_error::internal(
                                            "native staging referenced an incomplete child node",
                                        )
                                    })?;
                                child_states.push(state);
                                Ok::<_, paro_error::ParoError>(group)
                            }
                            NativeChild::MemoGroup {
                                group,
                                id,
                                stats,
                                layout,
                                names,
                                reference,
                            } => {
                                let group = session.memo.canonical_group(group);
                                let node = resolve_direct_group_reference(
                                    &mut session,
                                    group,
                                    id,
                                    stats,
                                    layout,
                                    names,
                                    reference,
                                )?;
                                if node.group != group {
                                    return Err(paro_error::internal(
                                        "native Memo group reference changed its group identity",
                                    ));
                                }
                                child_states.push(node);
                                Ok::<_, paro_error::ParoError>(group)
                            }
                            NativeChild::Group {
                                id,
                                stats,
                                layout,
                                names,
                                reference,
                            } => {
                                let node = resolve_group_hole_reference(
                                    &mut session,
                                    id,
                                    stats,
                                    layout,
                                    names,
                                    reference,
                                )?;
                                let group = node.group;
                                child_states.push(node);
                                Ok::<_, paro_error::ParoError>(group)
                            }
                        }
                    })?;
                    let resident = session.resident_nodes.remove(&native_node.id);
                    let Some((mut node, staged)) = stage_node(
                        &mut session,
                        NodeStagingRequest {
                            input: NodeStagingInput::Native {
                                id: native_node.id,
                                stats: native_node.stats,
                                operator,
                                source_proofs: native_node.source_proofs,
                                resident,
                            },
                            target: is_root.then_some(target),
                            required_region_facet: is_root
                                .then_some(preserved_region_facet)
                                .flatten(),
                            inherited_runtime_filter_facet: is_root
                                .then_some(inherited_runtime_filter_facet)
                                .flatten(),
                            node_context: if is_root {
                                input_context
                            } else {
                                child_context
                            },
                            target_child_context: is_root.then_some(child_context),
                            refined_cardinality_kind: is_root
                                .then_some(refined_cardinality_kind)
                                .flatten(),
                        },
                        child_states,
                    )?
                    else {
                        return Ok(None);
                    };
                    if is_root {
                        if root_result.replace((node, staged)).is_some() {
                            return Err(paro_error::internal(
                                "native staging encountered multiple roots",
                            ));
                        }
                    } else {
                        session
                            .facts
                            .settle_group(session.memo, session.state, node.group)?;
                        let layout = node.layout.clone();
                        let facts = session.facts.transport(
                            session.memo,
                            session.state,
                            node.group,
                            &layout,
                        )?;
                        node.boundary_facts = Some(facts);
                        node.boundary_reference_id = Some(
                            paro_planner::operator::BoundReferenceId::node_occurrence(node.id.0),
                        );
                        if completed.len() != index {
                            return Err(paro_error::internal(
                                "native staging node order is not post-order",
                            ));
                        }
                        completed.push(Some((node.group, node)));
                        debug_assert!(staged.is_none());
                    }
                }
                root_result.ok_or_else(|| paro_error::internal("native staging has no root"))?
            }
        };
        if !session.nested_group_holes.is_empty() {
            return Err(paro_error::internal(
                "transformation rewrite discarded an opaque Memo group hole",
            ));
        }
        if !session.resident_nodes.is_empty() {
            return Err(paro_error::internal(
                "settlement produced an unconsumed resident lowering contract",
            ));
        }
        (root, staged, session.pending_runtime_filter_facets)
    };
    let Some(staged) = staged else {
        return Ok(None);
    };
    let mut region_facets = Vec::with_capacity(
        extended_required_region_facets
            .len()
            .saturating_add(pending_runtime_filter_facets.len()),
    );
    for fingerprint in extended_required_region_facets {
        let mut facet = memo
            .regions()
            .nodes
            .iter()
            .flat_map(|region| region.facets.iter())
            .find(|facet| facet.fingerprint == fingerprint)
            .cloned()
            .ok_or_else(|| paro_error::internal("preserved planning facet disappeared"))?;
        let ceiling = match facet.criticality {
            FacetCriticality::Required => memo.budget().max_mandatory_region_groups as usize,
            FacetCriticality::Optional => usize::from(memo.budget().max_composite_region_groups),
        };
        let (scope, overflow) = root.region_scope.materialize_bounded(memo, ceiling);
        if overflow && facet.criticality == FacetCriticality::Required {
            return Err(paro_error::internal(
                "required planning-region closure exceeds query complexity ceiling",
            ));
        }
        facet.scope.extend(scope);
        region_facets.push(facet);
    }
    // Optional facets created inside a transformed mandatory region are
    // normalized only after that region owns its complete rewritten scope.
    // Publishing them during recursive staging would compare them with the
    // stale pre-transformation scope and permanently drop otherwise nested
    // runtime filters as an apparent oversized overlap.
    region_facets.extend(pending_runtime_filter_facets);
    if !region_facets.is_empty() {
        if let Some(session) = &state.session {
            session.cancellation.check()?;
        }
        let dropped = memo.upsert_region_facets(region_facets)?;
        disable_dropped_runtime_filter_facets(state, &dropped)?;
    }
    Ok(Some(staged))
}

fn disable_dropped_runtime_filter_facets(
    state: &mut PlannerTransformState,
    dropped: &[Fingerprint],
) -> Result<()> {
    let dropped = dropped.iter().copied().collect::<BTreeSet<_>>();
    let payloads = state
        .metadata
        .iter()
        .filter_map(|(payload, metadata)| {
            metadata
                .runtime_filter_region_facet
                .is_some_and(|facet| dropped.contains(&facet))
                .then_some(*payload)
        })
        .collect::<Vec<_>>();
    for payload in payloads {
        state.disable_runtime_filter(payload)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, TableCatalogEntry};
    use paro_common::types::LogicalType;
    use paro_context::TestStatementContextBuilder;
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_planner::operator::join::{Join, JoinCondition, JoinType};
    use paro_planner::operator::{ComparisonJoin, ExpressionGet, Get};
    use paro_planner::plan::CardinalityEstimate;
    use paro_storage::table::table_factory::TableFactory;

    use super::*;

    include!("staging/native_runtime_filter_tests.rs");

    fn test_base_get(
        table_index: usize,
        object_id: u64,
        name: &str,
        rows: u64,
    ) -> OwnedLogicalPlan {
        let storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("table storage"),
        );
        let table = Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            name.to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage,
            CatalogObjectId::from_raw(object_id),
            0,
        ));
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            table_index,
            vec!["id".to_string()],
            vec![LogicalType::Integer],
            table,
        ))));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        plan
    }

    fn equality_join(
        left: OwnedLogicalPlan,
        right: OwnedLogicalPlan,
        rows: u64,
    ) -> OwnedLogicalPlan {
        let condition = JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        );
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(JoinType::Inner, left, right, vec![condition]),
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        plan
    }

    #[test]
    fn alias_projections_allocate_distinct_schema_contracts() {
        use paro_planner::expression::ColumnRefExpression;
        use paro_planner::operator::{Projection, SetOperation};
        let source = || test_base_get(0, 30_099, "shared_source", 100);
        let union = |left, right| {
            OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::union(
                10,
                left,
                right,
                true,
                vec![LogicalType::Integer],
            )))
        };
        let mut input = MemoBuilder::build(
            union(source(), source()),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let project = |table| {
            OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                table,
                source(),
                vec![Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
                )],
            )))
        };
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let staged = stage_transformed_expression(
            StagingRequest {
                input_facts: boundary::BoundarySnapshot::default(),
                input: StagingInput::Arena(
                    state
                        .staging_arena
                        .import(union(project(2), project(3)))
                        .unwrap(),
                ),
                column_stats: Arc::new(HashMap::new()),
                column_stat_scopes: HashMap::new(),
                resident_nodes: HashMap::new(),
                target: StagingTarget {
                    group: input.root,
                    rule: RuleId(999),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(),
                selected_proofs: HashMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap()
        .expect("alias projections must not collide in group allocation admission");
        let children = &staged.key.children;
        assert_eq!(children.len(), 2);
        assert_ne!(children[0], children[1]);
        assert_ne!(
            input.memo.group(children[0]).unwrap().schema,
            input.memo.group(children[1]).unwrap().schema
        );
    }

    #[test]
    fn staging_preserves_the_settled_root_projection_before_canonicalization() {
        use paro_planner::operator::{Filter, ProjectionMap};
        let make_plan = || {
            let source = equality_join(
                test_base_get(0, 70_001, "left_source", 10),
                test_base_get(1, 70_002, "right_source", 10),
                10,
            );
            let mut filter = Filter::new(source, vec![]);
            filter.projection_map = ProjectionMap::new(vec![1]);
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter))
        };
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let plan = state.staging_arena.import(make_plan()).unwrap();
        let staged = stage_transformed_expression(
            StagingRequest {
                input: StagingInput::Arena(plan),
                input_facts: boundary::BoundarySnapshot::default(),
                column_stats: Arc::new(HashMap::new()),
                column_stat_scopes: HashMap::new(),
                resident_nodes: HashMap::new(),
                target: StagingTarget {
                    group: input.root,
                    rule: RuleId(999),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(),
                selected_proofs: HashMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap()
        .unwrap();
        let metadata = &state.metadata[&staged.payload];
        assert_eq!(metadata.output_columns.len(), 1);
        assert_eq!(
            input.memo.group(input.root).unwrap().schema.columns().len(),
            1
        );
        let LogicalOperator::Filter(template) = &state.payloads.logical[staged.payload.index()]
            .semantic_template
            .operator
        else {
            panic!("expected canonical filter template")
        };
        assert!(template.projection_map.is_all());
    }

    #[test]
    fn native_shell_stages_without_importing_a_second_arena() {
        use paro_planner::operator::{BoundReference, Filter};

        let make_plan = || Filter::new(test_base_get(0, 70_101, "native_source", 10), vec![]);
        let mut input = MemoBuilder::build(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(make_plan())),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let root = input.root;
        let root_expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let source = input
            .memo
            .logical_expr(root_expression)
            .unwrap()
            .key
            .children[0];
        let source_plan = test_base_get(0, 70_101, "native_source", 10);
        let bindings = source_plan.get_column_bindings();
        let types = source_plan.types();
        let reference_id = paro_planner::operator::BoundReferenceId::group_hole(70_101);
        let reference = BoundReference::new(reference_id, bindings, types);
        let native = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::BoundReference(reference)),
            vec![],
        )));
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let arena_len = state.staging_arena.len();
        let native = NativeShell::from_owned(native, &HashMap::new()).unwrap();
        let payload_before = input.memo.logical_expr(root_expression).unwrap().payload;
        let constructions_before = STAGING_PAYLOAD_CONSTRUCTIONS.with(std::cell::Cell::get);
        let staged = stage_transformed_expression(
            StagingRequest {
                input: StagingInput::Native {
                    shell: native,
                    resident_nodes: HashMap::new(),
                },
                input_facts: boundary::BoundarySnapshot::default(),
                column_stats: Arc::new(HashMap::new()),
                column_stat_scopes: HashMap::new(),
                resident_nodes: HashMap::new(),
                target: StagingTarget {
                    group: root,
                    rule: RuleId(999),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::from([(reference_id, source)]),
                selected_proofs: HashMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap();
        assert_eq!(staged.as_ref().unwrap().payload, payload_before);
        assert_eq!(STAGING_PAYLOAD_CONSTRUCTIONS.with(std::cell::Cell::get), constructions_before,
            "exact duplicate must preserve its facts and reuse payload before constructing an extraction clone");
        assert_eq!(state.staging_arena.len(), arena_len);
    }

    #[test]
    fn direct_memo_group_shell_does_not_consume_legacy_hole_registry() {
        use paro_planner::operator::{BoundReference, Filter, ProjectionMap};

        let source_plan = test_base_get(0, 70_102, "direct_native_source", 10);
        let layout = source_plan.output_layout();
        let names: Arc<[String]> = source_plan.output_names().into();
        let mut input = MemoBuilder::build(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(source_plan, vec![]))),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let root = input.root;
        let root_expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let source = input
            .memo
            .logical_expr(root_expression)
            .unwrap()
            .key
            .children[0];
        let reference_id = paro_planner::operator::BoundReferenceId::group_hole(70_102);
        let reference = BoundReference::new(
            reference_id,
            layout.bindings().to_vec(),
            layout.types().to_vec(),
        );
        let native = NativeShell {
            nodes: Box::new([NativeNode {
                id: paro_planner::plan::PlanNodeId(70_103),
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(Filter {
                    expressions: vec![],
                    child: NativeChild::MemoGroup {
                        group: source,
                        id: paro_planner::plan::PlanNodeId(70_104),
                        stats: NodeStats::default(),
                        layout,
                        names,
                        reference,
                    },
                    projection_map: ProjectionMap::all(),
                }),
                source_proofs: Box::new([]),
            }]),
            root: 0,
        };
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let payload_before = input.memo.logical_expr(root_expression).unwrap().payload;
        let constructions_before = STAGING_PAYLOAD_CONSTRUCTIONS.with(std::cell::Cell::get);
        let staged = stage_transformed_expression(
            StagingRequest {
                input: StagingInput::Native {
                    shell: native,
                    resident_nodes: HashMap::new(),
                },
                input_facts: boundary::BoundarySnapshot::default(),
                column_stats: Arc::new(HashMap::new()),
                column_stat_scopes: HashMap::new(),
                resident_nodes: HashMap::new(),
                target: StagingTarget {
                    group: root,
                    rule: RuleId(1_000),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(),
                selected_proofs: HashMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap();
        assert_eq!(staged.as_ref().unwrap().payload, payload_before);
        assert_eq!(STAGING_PAYLOAD_CONSTRUCTIONS.with(std::cell::Cell::get), constructions_before,
            "exact duplicate must preserve its facts and reuse payload before constructing an extraction clone");
    }

    #[test]
    fn settled_identity_reuse_retains_fresh_column_facts() {
        use paro_planner::operator::Filter;
        use paro_storage::statistics::BaseStatistics;
        let plan = || OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            test_base_get(0, 70_105, "settled_fact_source", 100), vec![],
        )));
        let mut input = MemoBuilder::build(plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let root = input.root;
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let baseline = input.memo.logical_expr(input.memo.group(root).unwrap().logical_exprs()[0]).unwrap().payload;
        let mut prior = None;
        for ndv in [8, 3, 3] {
            let root_index = state.staging_arena.import(plan()).unwrap();
            let resident = state.staging_arena.shared_output_layout(root_index).unwrap();
            let column = *state.binding_ids.get(0, 0, &LogicalType::Integer).unwrap();
            let stats = Arc::new(ColumnStatistics::with_estimated_distinct(
                BaseStatistics::create_unknown(LogicalType::Integer), Some(ndv),
            ));
            let before = STAGING_PAYLOAD_CONSTRUCTIONS.with(std::cell::Cell::get);
            let staged = stage_transformed_expression(StagingRequest {
                input: StagingInput::Arena(root_index),
                input_facts: boundary::BoundarySnapshot::default(),
                column_stats: Arc::new(HashMap::from([(ColumnBinding::new(0, 0), stats)])),
                column_stat_scopes: HashMap::new(),
                resident_nodes: HashMap::new(),
                target: StagingTarget {
                    group: root, rule: RuleId(999), budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0), child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None, extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(), selected_proofs: HashMap::new(),
            }, &mut input.memo, &mut state).unwrap().unwrap();
            assert_eq!(staged.payload, baseline);
            assert_eq!(STAGING_PAYLOAD_CONSTRUCTIONS.with(std::cell::Cell::get), before);
            assert_eq!(staged.logical_properties.column_domains[&column].ranking_point, ndv as u64);
            let value = staged.logical_properties.column_domains.clone();
            if let Some((previous_ndv, previous)) = prior {
                assert_eq!(value == previous, ndv == previous_ndv);
            }
            prior = Some((ndv, value));
            assert!(Arc::ptr_eq(&resident, &state.staging_arena.shared_output_layout(root_index).unwrap()));
        }
    }

    #[test]
    fn production_settlement_contract_is_consumed_without_second_identity_lowering() {
        let make_plan = || {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                test_base_get(0, 70_106, "resident_contract_source", 32),
                vec![],
            )))
        };
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let root = input.root;
        let mut state = input.planner_state.write().unwrap();
        let session = TestStatementContextBuilder::minimal().build();
        state.session = Some(session.clone());
        let environment = PlannerRuleEnvironment {
            control: input.memo.control().clone(),
            bind_context: state.bind_context.clone(),
            session,
            cost_model: state.cost_model.clone(),
            budget: input.memo.budget().clone(),
            verify_enabled: false,
        };
        let settled =
            super::super::settle_with_session_arena(&mut state, make_plan(), &environment)
                .unwrap()
                .unwrap();
        assert!(
            !settled.resident_nodes.is_empty(),
            "settlement must publish at least one resident node contract"
        );
        let columns_before_staging = state.columns.len();
        let scalars_before_staging = state.scalars.len();
        let resident_nodes = settled.resident_nodes;
        let staged = stage_transformed_expression(
            StagingRequest {
                input: StagingInput::Arena(settled.plan),
                input_facts: boundary::BoundarySnapshot::default(),
                column_stats: settled.statistics,
                column_stat_scopes: settled.scopes,
                resident_nodes,
                target: StagingTarget {
                    group: root,
                    rule: RuleId(991),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(),
                selected_proofs: HashMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap()
        .expect("resident contract should reach the production staging path");
        // A new Memo payload is allowed here: the test isolates the lowering
        // contract, not the separate logical-expression identity policy.
        // The resident IDs must nevertheless be consumed without extending
        // either session identity catalog.
        assert!(staged.key.operator != Fingerprint::default());
        assert_eq!(state.columns.len(), columns_before_staging);
        assert_eq!(state.scalars.len(), scalars_before_staging);
    }

    #[test]
    fn root_key_collision_in_another_context_declines_and_rolls_back() {
        let bind_context = BindContext::new();
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                Vec::new(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let staged_plan = duplicate_plan_preserving_indices(&plan, bind_context.shared().as_ref());
        let mut input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
        let root = input.root;
        let (schema, properties, cardinality) = {
            let group = input.memo.group(root).unwrap();
            (
                group.schema.clone(),
                group.logical_properties.clone(),
                group.cardinality.clone(),
            )
        };
        let groups_before = input.memo.group_count();
        let state = input.planner_state.clone();
        state.write().unwrap().session = Some(TestStatementContextBuilder::minimal().build());
        let metadata_before = state.read().unwrap().metadata.len();
        let mut transaction = TransformContext::new(&mut input.memo, root);

        let outcome = transaction
            .with_sidecar_transaction(
                state.clone(),
                PlannerTransformState::savepoint,
                PlannerTransformState::rollback_to,
                |memo, state| {
                    // Model work performed while recursively staging a plan;
                    // a context collision at its root must cause all of it to
                    // be discarded by the common advisory-miss path.
                    memo.create_group(schema, properties, cardinality);
                    stage_transformed_expression(
                        StagingRequest {
                            input: StagingInput::Arena(
                                state.staging_arena.import(staged_plan).unwrap(),
                            ),
                            input_facts: boundary::BoundarySnapshot::default(),
                            column_stats: Arc::new(HashMap::new()),
                            column_stat_scopes: HashMap::new(),
                            resident_nodes: HashMap::new(),
                            target: StagingTarget {
                                group: root,
                                rule: RuleId(999),
                                budget_class: TransformationBudgetClass::Local,
                                input_context: OptimizationContextId(1),
                                child_context: OptimizationContextId(1),
                                refined_cardinality_kind: None,
                            },
                            regions: StagingRegionRequirements {
                                preserved_facet: None,
                                extended_required_facets: Box::new([]),
                                inherited_runtime_filter_facet: None,
                            },
                            nested_group_holes: BTreeMap::new(),
                            selected_proofs: HashMap::new(),
                        },
                        memo,
                        state,
                    )
                },
            )
            .unwrap();

        assert!(outcome.is_none());
        assert_eq!(transaction.memo().group_count(), groups_before + 1);
        transaction.rollback().unwrap();
        assert_eq!(input.memo.group_count(), groups_before);
        assert_eq!(state.read().unwrap().metadata.len(), metadata_before);
    }

    #[test]
    fn transformed_join_uses_reattached_children_for_physical_facts() {
        let baseline = equality_join(
            equality_join(
                test_base_get(0, 30_001, "fact", 20_000),
                test_base_get(1, 30_002, "first_dimension", 20),
                20,
            ),
            test_base_get(2, 30_003, "second_dimension", 30),
            20,
        );
        let transformed = equality_join(
            equality_join(
                test_base_get(0, 30_001, "fact", 20_000),
                test_base_get(2, 30_003, "second_dimension", 30),
                30,
            ),
            test_base_get(1, 30_002, "first_dimension", 20),
            20,
        );
        let mut input =
            MemoBuilder::build(baseline, BindContext::new(), SearchBudget::default()).unwrap();
        let root = input.root;
        let state = input.planner_state.clone();
        state.write().unwrap().session = Some(TestStatementContextBuilder::minimal().build());
        let mut transaction = TransformContext::new(&mut input.memo, root);

        let staged = transaction
            .with_sidecar_transaction(
                state.clone(),
                PlannerTransformState::savepoint,
                PlannerTransformState::rollback_to,
                |memo, state| {
                    stage_transformed_expression(
                        StagingRequest {
                            input: StagingInput::Arena(
                                state.staging_arena.import(transformed).unwrap(),
                            ),
                            input_facts: boundary::BoundarySnapshot::default(),
                            column_stats: Arc::new(HashMap::new()),
                            column_stat_scopes: HashMap::new(),
                            resident_nodes: HashMap::new(),
                            target: StagingTarget {
                                group: root,
                                rule: JOIN_REGION_ENUMERATION_RULE,
                                budget_class: TransformationBudgetClass::Local,
                                input_context: OptimizationContextId(0),
                                child_context: OptimizationContextId(0),
                                refined_cardinality_kind: Some(
                                    CardinalityRecipeKind::ConstraintRefined,
                                ),
                            },
                            regions: StagingRegionRequirements {
                                preserved_facet: None,
                                extended_required_facets: Box::new([]),
                                inherited_runtime_filter_facet: None,
                            },
                            nested_group_holes: BTreeMap::new(),
                            selected_proofs: HashMap::new(),
                        },
                        memo,
                        state,
                    )
                },
            )
            .unwrap()
            .expect("reordered join should stage");

        let planner_state = state.read().unwrap();
        let metadata = planner_state
            .metadata
            .get(&staged.payload)
            .expect("staged root metadata");
        assert!(metadata.implementations.hash_join_runtime_filter);
        assert_eq!(
            metadata
                .cost_facts
                .runtime_filter_probe_sources
                .iter()
                .map(|source| source.source)
                .collect::<Vec<_>>(),
            vec![WorkSourceId(0)]
        );
        assert!(metadata
            .cost_facts
            .child_row_widths
            .iter()
            .all(|width| *width > 8));
    }
}
