// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Arena-backed immutable physical plan.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};

use super::children::{PlanChildren, PlanChildrenArena};
use super::dependencies::PlanDependencies;
use super::edges::{PhysicalEdgeArena, PhysicalEdgeKind};
use super::explain::types::{
    ExplainDoc, ExplainNode, ExplainProperty, ExplainValue, EXPLAIN_FORMAT_VERSION,
};
use super::identity::{Fingerprint, StableFingerprintBuilder};
use super::ids::PhysicalPlanNodeId;
use super::node::PhysicalPlanNode;
use super::portfolio::ExecutionResourceContract;
use super::properties::{PhysicalGrantContract, PlanPropertyMap};
use super::row_type::{ColumnIdentity, RowType};
use super::specs::{AggregateSpec, NestedLoopJoinSpec, PhysicalNodeKind, SearchSourceSpec};
use paro_catalog::entry::{StandardEntry, TableCatalogEntry};
use paro_common::types::LogicalType;
use paro_planner::expression::{
    AggregateExpression, AggregateType, Expression, OperatorType, WindowFrameBound, WindowFrameType,
};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition};
use paro_planner::operator::ExplainSpec;
use paro_planner::plan::CardinalityEstimate;
use paro_storage::index::{Predicate, PredicateTree};
use paro_storage::search::CapabilityToken;
use paro_storage::table::segment_reorderer::{
    OrderByStatistics, SegmentOrderOptions, SegmentOrderType,
};

#[derive(Debug, Clone, Default)]
pub struct PhysicalPlanNodeArena {
    nodes: Vec<PhysicalPlanNode>,
}

impl PhysicalPlanNodeArena {
    pub fn push(&mut self, mut node: PhysicalPlanNode) -> PhysicalPlanNodeId {
        let id = PhysicalPlanNodeId::new(self.nodes.len());
        node.id = id;
        self.nodes.push(node);
        id
    }

    pub fn get(&self, id: PhysicalPlanNodeId) -> Option<&PhysicalPlanNode> {
        self.nodes.get(id.index())
    }

    pub fn get_mut(&mut self, id: PhysicalPlanNodeId) -> Option<&mut PhysicalPlanNode> {
        self.nodes.get_mut(id.index())
    }

    pub fn iter(&self) -> impl Iterator<Item = &PhysicalPlanNode> {
        self.nodes.iter()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub root: PhysicalPlanNodeId,
    pub nodes: PhysicalPlanNodeArena,
    pub children: PlanChildrenArena,
    pub edges: PhysicalEdgeArena,
    pub properties: PlanPropertyMap,
    pub dependencies: PlanDependencies,
    /// Bound only after portfolio admission. It is not an optimizer input and
    /// therefore does not participate in the portfolio fingerprint.
    pub execution_resources: Option<ExecutionResourceContract>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalIdentityError {
    InvalidRoot,
    InvalidEdge,
    InvalidChild,
    Cycle,
    MissingAuxiliaryDependency {
        node: PhysicalPlanNodeId,
        dependency: u32,
    },
    /// No typed identity schema has been declared for this implementation.
    /// Identity generation must fail closed instead of assigning a shared
    /// placeholder to semantically different physical payloads.
    UnsupportedKind {
        kind: &'static str,
    },
    /// Two auxiliary producers have the same local key but no canonical
    /// ordering proof. Refuse to manufacture a cross-run identity from arena
    /// allocation order.
    AmbiguousAuxiliaryOrder,
}

impl fmt::Display for PhysicalIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoot => formatter.write_str("physical identity has an invalid root"),
            Self::InvalidEdge => formatter.write_str("physical identity has an invalid edge"),
            Self::InvalidChild => formatter.write_str("physical identity has an invalid child"),
            Self::Cycle => formatter.write_str("physical identity graph contains a cycle"),
            Self::MissingAuxiliaryDependency { node, dependency } => write!(
                formatter,
                "physical identity node {node:?} references missing auxiliary dependency {dependency}"
            ),
            Self::UnsupportedKind { kind } => write!(
                formatter,
                "physical identity has no typed canonical encoder for {kind}"
            ),
            Self::AmbiguousAuxiliaryOrder => formatter.write_str(
                "physical identity has ambiguous auxiliary producer ordering",
            ),
        }
    }
}

impl PhysicalPlan {
    pub fn new(
        root: PhysicalPlanNodeId,
        nodes: PhysicalPlanNodeArena,
        children: PlanChildrenArena,
        properties: PlanPropertyMap,
    ) -> Self {
        Self {
            root,
            nodes,
            children,
            edges: PhysicalEdgeArena::default(),
            properties,
            dependencies: PlanDependencies::default(),
            execution_resources: None,
        }
    }

    pub fn node(&self, id: PhysicalPlanNodeId) -> &PhysicalPlanNode {
        self.nodes
            .get(id)
            .expect("physical plan node id must refer to arena entry")
    }

    pub fn child_ids<'a>(&'a self, children: &'a PlanChildren) -> &'a [PhysicalPlanNodeId] {
        children.as_slice(&self.children)
    }

    /// Portfolio identity includes the selected implementation's operating
    /// points, not merely its Memo expression and enforcer shape. Two equal-cost
    /// sorts can have the same structural fingerprint while being proved for
    /// different memory classes. Merging those plans would keep only one of
    /// the proofs and advertise it for both classes.
    ///
    /// Admission is the intersection of every node's contract. Canonicalize
    /// that conjunction independently of arena ids, node order and duplicate
    /// constraints. Auxiliary producers are included: extraction has already
    /// compacted the complete executable arena before this method is called.
    pub(crate) fn portfolio_fingerprint(
        &self,
        structural: Fingerprint,
    ) -> paro_common::error::Result<Fingerprint> {
        let mut contracts = BTreeSet::new();
        for node in self.nodes.iter() {
            let properties = self.properties.get(node.id).ok_or_else(|| {
                paro_common::error::internal(
                    "portfolio identity requires every node grant contract",
                )
            })?;
            if properties.grant_contract != PhysicalGrantContract::Invariant {
                contracts.insert(properties.grant_contract);
            }
        }
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.physical.portfolio-admission.v1");
        fingerprint.write_fingerprint(structural);
        fingerprint.write_u64(contracts.len() as u64);
        for contract in contracts {
            match contract {
                PhysicalGrantContract::Invariant => {}
                PhysicalGrantContract::Parallelism { tasks } => {
                    fingerprint.write_u64(1);
                    fingerprint.write_u64(u64::from(tasks));
                }
                PhysicalGrantContract::Class(class) => {
                    fingerprint.write_u64(2);
                    fingerprint.write_u64(u64::from(class.0));
                }
            }
        }
        Ok(fingerprint.finish())
    }

    /// Remove nodes made unreachable by physical rewrites and reassign dense
    /// ids. A physical plan arena is part of the observable plan contract; it
    /// must not retain folded operators that can be mistaken for consumers.
    pub fn compact_reachable(&mut self) {
        let mut reachable = vec![false; self.nodes.len()];
        let mut stack = vec![self.root];
        loop {
            while let Some(id) = stack.pop() {
                if std::mem::replace(&mut reachable[id.index()], true) {
                    continue;
                }
                stack.extend_from_slice(self.child_ids(&self.node(id).children));
            }
            let mut discovered_auxiliary_producer = false;
            for edge in self.edges.iter() {
                if reachable[edge.consumer.index()] && !reachable[edge.producer.index()] {
                    stack.push(edge.producer);
                    discovered_auxiliary_producer = true;
                }
            }
            if !discovered_auxiliary_producer {
                break;
            }
        }

        if reachable.iter().all(|reachable| *reachable) {
            return;
        }

        let mut remap = vec![PhysicalPlanNodeId::INVALID; reachable.len()];
        let mut next_index = 0;
        for (old_index, is_reachable) in reachable.iter().copied().enumerate() {
            if is_reachable {
                remap[old_index] = PhysicalPlanNodeId::new(next_index);
                next_index += 1;
            }
        }

        let old_children = std::mem::take(&mut self.children);
        let old_nodes = std::mem::take(&mut self.nodes.nodes);
        let mut nodes = PhysicalPlanNodeArena::default();
        let mut children = PlanChildrenArena::default();
        for mut node in old_nodes
            .into_iter()
            .filter(|node| reachable[node.id.index()])
        {
            let remapped_children = node
                .children
                .as_slice(&old_children)
                .iter()
                .map(|child| remap[child.index()])
                .collect();
            node.children = children.pack(remapped_children);
            node.id = PhysicalPlanNodeId::INVALID;
            nodes.push(node);
        }

        self.root = remap[self.root.index()];
        self.properties.retain_remapped(&reachable, &remap);
        let edge_remap = self.edges.retain_remapped(&reachable, &remap);
        self.properties.remap_auxiliary_edges(&edge_remap);
        self.nodes = nodes;
        self.children = children;
    }

    /// Return a structural one-row guarantee, independent of optimizer
    /// cardinality estimates. Consumers may use this as a semantic proof.
    pub fn guarantees_exactly_one_row(&self, id: PhysicalPlanNodeId) -> bool {
        let node = self.node(id);
        match &node.kind {
            PhysicalNodeKind::Aggregate(spec) => {
                spec.grouping_key_count == 0
                    && spec.grouping_sets.len() <= 1
                    && spec.having_filter.is_empty()
            }
            PhysicalNodeKind::Project(_) | PhysicalNodeKind::Sort(_) => {
                let [child] = self.child_ids(&node.children) else {
                    return false;
                };
                self.guarantees_exactly_one_row(*child)
            }
            _ => false,
        }
    }

    pub fn format_tree(&self) -> String {
        let mut out = String::new();
        self.format_node(self.root, 0, &mut out);
        out
    }

    /// Stable identity of the executable physical structure.  This is an
    /// identity for receipt correlation, not a claim of SQL equivalence: the
    /// explain representation carries operator payloads while the tree
    /// carries child topology and output layout.  Keep the domain/version
    /// explicit so a consumer never treats a later encoding as compatible.
    pub fn structural_identity_fingerprint(&self) -> Result<Fingerprint, PhysicalIdentityError> {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(b"paro.physical-plan-structure.v5.typed-canonical");

        // The traversal is iterative on purpose.  Physical plans can contain
        // long unary spines and a fingerprint must not depend on recursion
        // depth or arena allocation order.
        let order = self.canonical_postorder()?;
        for (node, properties) in self.properties.iter() {
            for dependency in &properties.auxiliary_dependencies {
                if self
                    .edges
                    .get(super::edges::PhysicalEdgeId(*dependency))
                    .is_none()
                {
                    return Err(PhysicalIdentityError::MissingAuxiliaryDependency {
                        node,
                        dependency: *dependency,
                    });
                }
            }
        }
        let canonical_ids = order
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index as u64))
            .collect::<BTreeMap<_, _>>();
        builder.write_u64(order.len() as u64);
        builder.write_u64(*canonical_ids.get(&self.root).unwrap_or(&u64::MAX));

        for (canonical, id) in order.iter().enumerate() {
            let node = self.node(*id);
            builder.write_u64(canonical as u64);
            builder.write_bytes(node.kind.name().as_bytes());
            write_canonical_kind(self, *id, &mut builder)?;
            write_row_type(&mut builder, &node.output);
            let children = self.child_ids(&node.children);
            builder.write_u64(children.len() as u64);
            for child in children {
                builder.write_u64(*canonical_ids.get(child).unwrap_or(&u64::MAX));
            }
            if let Some(properties) = self.properties.get(*id) {
                builder.write_u64(1);
                write_hashed(
                    &mut builder,
                    b"required-properties",
                    &properties.required_from_parent,
                );
                write_hashed(&mut builder, b"provided-properties", &properties.provided);
                write_hashed(
                    &mut builder,
                    b"characteristics",
                    &properties.characteristics,
                );
                write_optional_fingerprint(&mut builder, properties.region_owner);
                write_hashed_slice(
                    &mut builder,
                    b"owned-artifacts",
                    &properties.owned_artifacts,
                );
                let mut dependencies = properties
                    .auxiliary_dependencies
                    .iter()
                    .filter_map(|edge| self.edges.get(super::edges::PhysicalEdgeId(*edge)))
                    .filter_map(|edge| {
                        Some((
                            *canonical_ids.get(&edge.producer)?,
                            *canonical_ids.get(&edge.consumer)?,
                            edge_kind_key(edge.kind),
                        ))
                    })
                    .collect::<Vec<_>>();
                dependencies.sort_unstable();
                builder.write_u64(dependencies.len() as u64);
                for (producer, consumer, (kind, fingerprint)) in dependencies {
                    builder.write_u64(producer);
                    builder.write_u64(consumer);
                    builder.write_u64(kind);
                    if let Some(fingerprint) = fingerprint {
                        builder.write_fingerprint(fingerprint);
                    }
                }
            } else {
                builder.write_u64(0);
            }
        }

        let mut edges = self
            .edges
            .iter()
            .filter_map(|edge| {
                Some((
                    *canonical_ids.get(&edge.producer)?,
                    *canonical_ids.get(&edge.consumer)?,
                    edge.kind,
                ))
            })
            .collect::<Vec<_>>();
        edges.sort_unstable_by_key(|(producer, consumer, kind)| {
            (*consumer, *producer, edge_kind_key(*kind))
        });
        builder.write_u64(edges.len() as u64);
        for (producer, consumer, kind) in edges {
            builder.write_u64(producer);
            builder.write_u64(consumer);
            let (tag, fingerprint) = edge_kind_key(kind);
            builder.write_u64(tag);
            if let Some(fingerprint) = fingerprint {
                builder.write_fingerprint(fingerprint);
            }
        }
        write_hashed(&mut builder, b"plan-dependencies", &self.dependencies);
        Ok(builder.finish())
    }

    fn canonical_postorder(&self) -> Result<Vec<PhysicalPlanNodeId>, PhysicalIdentityError> {
        if self.root == PhysicalPlanNodeId::INVALID || self.nodes.get(self.root).is_none() {
            return Err(PhysicalIdentityError::InvalidRoot);
        }
        if self.edges.iter().any(|edge| {
            edge.producer == PhysicalPlanNodeId::INVALID
                || edge.consumer == PhysicalPlanNodeId::INVALID
                || self.nodes.get(edge.producer).is_none()
                || self.nodes.get(edge.consumer).is_none()
        }) {
            return Err(PhysicalIdentityError::InvalidEdge);
        }
        let mut order = Vec::new();
        let mut visited = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        let mut stack = vec![(self.root, false)];
        while let Some((id, expanded)) = stack.pop() {
            if id == PhysicalPlanNodeId::INVALID || self.nodes.get(id).is_none() {
                return Err(PhysicalIdentityError::InvalidChild);
            }
            if expanded {
                visiting.remove(&id);
                order.push(id);
                continue;
            }
            if visiting.contains(&id) {
                return Err(PhysicalIdentityError::Cycle);
            }
            if !visited.insert(id) {
                continue;
            }
            visiting.insert(id);
            stack.push((id, true));
            let node = self.node(id);
            if self.child_ids(&node.children).iter().any(|child| {
                *child == PhysicalPlanNodeId::INVALID || self.nodes.get(*child).is_none()
            }) {
                return Err(PhysicalIdentityError::InvalidChild);
            }
            for child in self.child_ids(&node.children).iter().rev() {
                stack.push((*child, false));
            }
            let mut producers = self
                .edges
                .iter()
                .filter(|edge| edge.consumer == id)
                .map(|edge| {
                    Ok((
                        edge_kind_key(edge.kind),
                        self.local_identity_key(edge.producer)?,
                        edge.producer,
                    ))
                })
                .collect::<Result<Vec<_>, PhysicalIdentityError>>()?;
            // Do not use the arena id as a tie breaker. Equal typed payloads
            // are interchangeable; an arena id would make otherwise equal
            // plans differ across extraction runs.
            producers.sort_unstable_by_key(|(kind, local, _)| (*kind, *local));
            if producers
                .windows(2)
                .any(|pair| pair[0].0 == pair[1].0 && pair[0].1 == pair[1].1)
            {
                return Err(PhysicalIdentityError::AmbiguousAuxiliaryOrder);
            }
            for (_, _, producer) in producers.into_iter().rev() {
                stack.push((producer, false));
            }
        }
        if !visiting.is_empty() {
            return Err(PhysicalIdentityError::Cycle);
        }
        Ok(order)
    }

    fn local_identity_key(
        &self,
        id: PhysicalPlanNodeId,
    ) -> Result<Fingerprint, PhysicalIdentityError> {
        let node = self.node(id);
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(node.kind.name().as_bytes());
        write_canonical_kind(self, id, &mut builder)?;
        write_row_type(&mut builder, &node.output);
        Ok(builder.finish())
    }

    pub fn format_explain_text_with_spec(&self, spec: &ExplainSpec) -> String {
        let doc = self.to_explain_doc(*spec);
        let mut out = String::new();
        self.format_explain_node(&doc.root, spec, 0, false, &mut out);
        out
    }

    pub fn format_explain_json(&self, spec: ExplainSpec) -> String {
        self.to_explain_doc(spec).to_json().to_string()
    }

    fn to_explain_doc(&self, spec: ExplainSpec) -> ExplainDoc {
        ExplainDoc {
            format_version: EXPLAIN_FORMAT_VERSION,
            spec,
            root: self.explain_node(self.root),
            summary: Vec::new(),
        }
    }

    fn format_node(&self, id: PhysicalPlanNodeId, depth: usize, out: &mut String) {
        let node = self.node(id);
        for _ in 0..depth {
            out.push_str("  ");
        }
        let _ = writeln!(
            out,
            "#{:03} {} logical={:?} cols={}",
            id.index(),
            node.label.display_name,
            node.label.logical_plan_node,
            node.output.column_count()
        );
        for child in self.child_ids(&node.children) {
            self.format_node(*child, depth + 1, out);
        }
    }

    fn format_explain_node(
        &self,
        node: &ExplainNode,
        spec: &ExplainSpec,
        depth: usize,
        child_prefix: bool,
        out: &mut String,
    ) {
        let operator_indent = Self::explain_operator_indent(depth);
        for _ in 0..operator_indent {
            out.push(' ');
        }
        if child_prefix {
            out.push_str("->  ");
        }
        out.push_str(&node.header_text(spec));
        out.push('\n');
        self.format_explain_properties(node, spec, depth, out);
        for child in &node.children {
            self.format_explain_node(child, spec, depth + 1, true, out);
        }
    }

    fn format_explain_properties(
        &self,
        node: &ExplainNode,
        spec: &ExplainSpec,
        depth: usize,
        out: &mut String,
    ) {
        let property_indent = Self::explain_property_indent(depth);
        let mut write_property = |line: String| {
            for _ in 0..property_indent {
                out.push(' ');
            }
            let _ = writeln!(out, "{line}");
        };

        let mut output_schema = None;
        for property in &node.properties {
            if property.label == "Output Schema" {
                output_schema = Some(property.text_line());
                continue;
            }
            write_property(property.text_line());
        }
        if spec.detail.verbose {
            if let Some(cardinality) = node.estimated_cardinality {
                write_property(format!(
                    "Cardinality: [{}, {}, {}]",
                    cardinality.min, cardinality.expected, cardinality.max
                ));
            }
            if let Some(output_schema) = output_schema {
                write_property(output_schema);
            }
        }
    }

    /// MatrixOne-compatible text layout: the first child starts two columns
    /// below its parent, while each subsequent child starts at the parent's
    /// property continuation column. This keeps `->` on the operator line
    /// instead of making it look like a property prefix.
    fn explain_operator_indent(depth: usize) -> usize {
        match depth {
            0 => 0,
            depth => 2usize.saturating_add(depth.saturating_sub(1).saturating_mul(6)),
        }
    }

    fn explain_property_indent(depth: usize) -> usize {
        if depth == 0 {
            2
        } else {
            Self::explain_operator_indent(depth) + 6
        }
    }

    fn explain_node(&self, id: PhysicalPlanNodeId) -> ExplainNode {
        let node = self.node(id);
        ExplainNode {
            node_id: Some((id.index() + 1) as u64),
            logical_node_id: (!node.label.logical_plan_node.is_synthetic())
                .then_some(u64::from(node.label.logical_plan_node.0)),
            operator_name: explain_operator_name(&node.kind).to_string(),
            relation_name: explain_relation_name(&node.kind),
            relation_alias: explain_relation_alias(&node.kind),
            output_names: node.output.explain_names(matches!(
                &node.kind,
                PhysicalNodeKind::HashJoin(_)
                    | PhysicalNodeKind::NestedLoopJoin(_)
                    | PhysicalNodeKind::SortRangeJoin(_)
                    | PhysicalNodeKind::ClassicIeJoin(_)
                    | PhysicalNodeKind::CrossProduct(_)
                    | PhysicalNodeKind::DelimJoin(_)
            )),
            estimated_cardinality: explain_cardinality(&node.kind, node.cardinality),
            actual: None,
            properties: collect_explain_properties(self, id, node),
            children: self
                .child_ids(&node.children)
                .iter()
                .map(|child| self.explain_node(*child))
                .collect(),
        }
    }

    fn input_names(&self, id: PhysicalPlanNodeId, child_index: usize) -> Vec<String> {
        self.child_ids(&self.node(id).children)
            .get(child_index)
            .map(|child| self.expression_scope_names(*child))
            .unwrap_or_default()
    }

    /// Names used only while rendering expressions consumed by `id`'s
    /// parent. Internal definitions are expanded at most one producer hop;
    /// they never become the producer's schema or feed another definition.
    fn expression_scope_names(&self, id: PhysicalPlanNodeId) -> Vec<String> {
        let node = self.node(id);
        let mut names = node.output.explain_names(true);
        match &node.kind {
            PhysicalNodeKind::Project(spec) => {
                let stable_input_names = self
                    .child_ids(&node.children)
                    .first()
                    .map(|child| self.node(*child).output.explain_names(true))
                    .unwrap_or_default();
                let formatter = ExplainExpressionFormatter::new(&stable_input_names);
                for (index, expression) in spec.expressions.iter().enumerate() {
                    if node
                        .output
                        .identities
                        .get(index)
                        .is_some_and(super::row_type::ColumnIdentity::is_internal)
                    {
                        names[index] = formatter.format(expression);
                    }
                }
            }
            PhysicalNodeKind::Window(spec) => {
                let stable_input_names = self
                    .child_ids(&node.children)
                    .first()
                    .map(|child| self.node(*child).output.explain_names(true))
                    .unwrap_or_default();
                let formatter = ExplainExpressionFormatter::new(&stable_input_names);
                for (offset, expression) in spec.expressions.iter().enumerate() {
                    let index = spec.input_width + offset;
                    if let Some(name) = names.get_mut(index) {
                        *name = formatter.format(&Expression::Window(expression.clone().into()));
                    }
                }
            }
            PhysicalNodeKind::Aggregate(spec) => {
                let stable_input_names = self
                    .child_ids(&node.children)
                    .first()
                    .map(|child| self.node(*child).output.explain_names(true))
                    .unwrap_or_default();
                if let Some(scope_names) = aggregate_scope_names(spec, &stable_input_names) {
                    for (index, scope_name) in scope_names.into_iter().enumerate() {
                        if node
                            .output
                            .identities
                            .get(index)
                            .is_some_and(super::row_type::ColumnIdentity::is_internal)
                        {
                            names[index] = scope_name;
                        }
                    }
                }
            }
            PhysicalNodeKind::Filter(spec) => {
                if let Some(child) = self.child_ids(&node.children).first() {
                    let child_names = self.expression_scope_names(*child);
                    names = spec
                        .projection_map
                        .iter()
                        .filter_map(|index| child_names.get(*index).cloned())
                        .collect();
                }
            }
            PhysicalNodeKind::Sort(spec) => {
                if let Some(child) = self.child_ids(&node.children).first() {
                    let child_names = self.expression_scope_names(*child);
                    names = spec
                        .projection_map
                        .iter()
                        .filter_map(|index| child_names.get(*index).cloned())
                        .collect();
                }
            }
            PhysicalNodeKind::Limit(_)
            | PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::EmptyResult(_) => {
                if let Some(child) = self.child_ids(&node.children).first() {
                    let child_names = self.expression_scope_names(*child);
                    if child_names.len() == names.len() {
                        names = child_names;
                    }
                }
            }
            PhysicalNodeKind::HashJoin(spec) => {
                let children = self.child_ids(&node.children);
                if let [left, right] = children {
                    let left_names = self.expression_scope_names(*left);
                    let right_names = self.expression_scope_names(*right);
                    for (natural_index, source_index) in
                        spec.left_projection.iter().copied().enumerate()
                    {
                        let Some(name) = left_names.get(source_index) else {
                            continue;
                        };
                        if let Some(output_index) =
                            spec.output_permutation.destination_of(natural_index)
                        {
                            if let Some(output_name) = names.get_mut(output_index) {
                                *output_name = name.clone();
                            }
                        }
                    }
                    let build_natural_offset = spec.left_projection.len();
                    for (build_index, source_index) in spec
                        .build_input_projection
                        .iter()
                        .copied()
                        .take(spec.build_output_count)
                        .enumerate()
                    {
                        let Some(name) = right_names.get(source_index) else {
                            continue;
                        };
                        if let Some(output_index) = spec
                            .output_permutation
                            .destination_of(build_natural_offset + build_index)
                        {
                            if let Some(output_name) = names.get_mut(output_index) {
                                *output_name = name.clone();
                            }
                        }
                    }
                }
            }
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                names = self.join_expression_scope_names(
                    node,
                    &spec.left_projection,
                    &spec.right_projection,
                );
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                names = self.join_expression_scope_names(
                    node,
                    &spec.left_projection,
                    &spec.right_projection,
                );
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                names = self.join_expression_scope_names(
                    node,
                    &spec.left_projection,
                    &spec.right_projection,
                );
            }
            PhysicalNodeKind::CrossProduct(_) => {
                let children = self.child_ids(&node.children);
                if let [left, right] = children {
                    names = self
                        .expression_scope_names(*left)
                        .into_iter()
                        .chain(self.expression_scope_names(*right))
                        .collect();
                }
            }
            PhysicalNodeKind::DelimJoin(_) => {
                if let Some(consumer) = self.child_ids(&node.children).get(1) {
                    let consumer_names = self.expression_scope_names(*consumer);
                    if consumer_names.len() == names.len() {
                        names = consumer_names;
                    }
                }
            }
            _ => {}
        }
        names
    }

    fn join_expression_scope_names(
        &self,
        node: &PhysicalPlanNode,
        left_projection: &[usize],
        right_projection: &[usize],
    ) -> Vec<String> {
        let children = self.child_ids(&node.children);
        let [left, right] = children else {
            return node.output.explain_names(true);
        };
        let left_names = self.expression_scope_names(*left);
        let right_names = self.expression_scope_names(*right);
        left_projection
            .iter()
            .filter_map(|index| left_names.get(*index).cloned())
            .chain(
                right_projection
                    .iter()
                    .filter_map(|index| right_names.get(*index).cloned()),
            )
            .collect()
    }

    fn join_input_names(&self, id: PhysicalPlanNodeId, child_index: usize) -> Vec<String> {
        let Some(child_id) = self
            .child_ids(&self.node(id).children)
            .get(child_index)
            .copied()
        else {
            return Vec::new();
        };
        self.node(child_id).output.explain_names(true)
    }
}

fn write_row_type(builder: &mut StableFingerprintBuilder, row: &RowType) {
    builder.write_u64(row.names.len() as u64);
    for name in &row.names {
        builder.write_bytes(name.as_bytes());
    }
    builder.write_u64(row.types.len() as u64);
    for logical_type in &row.types {
        write_logical_type(builder, logical_type);
    }
    builder.write_u64(row.identities.len() as u64);
    for identity in &row.identities {
        write_column_identity(builder, identity);
    }
}

fn write_column_identity(builder: &mut StableFingerprintBuilder, identity: &ColumnIdentity) {
    match identity {
        ColumnIdentity::Visible { name, qualifier } => {
            builder.write_u64(0);
            builder.write_bytes(name.as_bytes());
            match qualifier {
                Some(path) => {
                    builder.write_u64(1);
                    builder.write_u64(path.len() as u64);
                    for component in path.iter() {
                        builder.write_bytes(component.as_bytes());
                    }
                }
                None => builder.write_u64(0),
            }
        }
        ColumnIdentity::Internal => builder.write_u64(1),
        ColumnIdentity::InternalNamed(name) => {
            builder.write_u64(2);
            builder.write_bytes(name.as_bytes());
        }
        ColumnIdentity::Locator { object_id } => {
            builder.write_u64(3);
            builder.write_u64(*object_id);
        }
    }
}

fn write_logical_type(builder: &mut StableFingerprintBuilder, logical_type: &LogicalType) {
    match logical_type {
        LogicalType::Boolean => builder.write_u64(0),
        LogicalType::TinyInt => builder.write_u64(1),
        LogicalType::SmallInt => builder.write_u64(2),
        LogicalType::Integer => builder.write_u64(3),
        LogicalType::BigInt => builder.write_u64(4),
        LogicalType::HugeInt => builder.write_u64(5),
        LogicalType::UTinyInt => builder.write_u64(6),
        LogicalType::USmallInt => builder.write_u64(7),
        LogicalType::UInteger => builder.write_u64(8),
        LogicalType::UBigInt => builder.write_u64(9),
        LogicalType::UHugeInt => builder.write_u64(10),
        LogicalType::Float => builder.write_u64(11),
        LogicalType::Double => builder.write_u64(12),
        LogicalType::Decimal { precision, scale } => {
            builder.write_u64(13);
            builder.write_u64(u64::from(*precision));
            builder.write_u64(u64::from(*scale));
        }
        LogicalType::Varchar => builder.write_u64(14),
        LogicalType::VarcharCollation(collation) => {
            builder.write_u64(15);
            builder.write_bytes(collation.as_bytes());
        }
        LogicalType::TsVector => builder.write_u64(16),
        LogicalType::TsQuery => builder.write_u64(17),
        LogicalType::Date => builder.write_u64(18),
        LogicalType::Timestamp => builder.write_u64(19),
        LogicalType::TimestampTz => builder.write_u64(20),
        LogicalType::Time => builder.write_u64(21),
        LogicalType::Interval => builder.write_u64(22),
        LogicalType::Blob => builder.write_u64(23),
        LogicalType::Uuid => builder.write_u64(24),
        LogicalType::Json => builder.write_u64(25),
        LogicalType::Jsonb => builder.write_u64(26),
        LogicalType::Null => builder.write_u64(27),
        LogicalType::IntegerLiteral(value) => {
            builder.write_u64(28);
            builder.write_i64(*value);
        }
        LogicalType::StringLiteral => builder.write_u64(29),
        LogicalType::Unknown => builder.write_u64(30),
        LogicalType::Array(element, length) => {
            builder.write_u64(31);
            builder.write_u64(*length as u64);
            write_logical_type(builder, element);
        }
        LogicalType::List(element) => {
            builder.write_u64(32);
            write_logical_type(builder, element);
        }
        LogicalType::Struct(fields) => {
            builder.write_u64(33);
            builder.write_u64(fields.len() as u64);
            for (name, field_type) in fields {
                builder.write_bytes(name.as_bytes());
                write_logical_type(builder, field_type);
            }
        }
    }
}

/// Hash adapter used only for fields whose Rust representation already has a
/// value-semantic `Hash` implementation.  The adapter deliberately encodes
/// primitive writes through `StableFingerprintBuilder`, so it never inherits
/// the platform-dependent byte order or hasher state of `DefaultHasher`.
struct CanonicalHasher<'a> {
    builder: &'a mut StableFingerprintBuilder,
}

impl Hasher for CanonicalHasher<'_> {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.builder.write_bytes(bytes);
    }

    fn write_u8(&mut self, value: u8) {
        self.builder.write_u64(u64::from(value));
    }

    fn write_u16(&mut self, value: u16) {
        self.builder.write_u64(u64::from(value));
    }

    fn write_u32(&mut self, value: u32) {
        self.builder.write_u64(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        self.builder.write_u64(value);
    }

    fn write_u128(&mut self, value: u128) {
        self.builder.write_bytes(&value.to_le_bytes());
    }

    fn write_usize(&mut self, value: usize) {
        self.builder.write_u64(value as u64);
    }

    fn write_i8(&mut self, value: i8) {
        self.builder.write_i64(i64::from(value));
    }

    fn write_i16(&mut self, value: i16) {
        self.builder.write_i64(i64::from(value));
    }

    fn write_i32(&mut self, value: i32) {
        self.builder.write_i64(i64::from(value));
    }

    fn write_i64(&mut self, value: i64) {
        self.builder.write_i64(value);
    }

    fn write_i128(&mut self, value: i128) {
        self.builder.write_bytes(&value.to_le_bytes());
    }

    fn write_isize(&mut self, value: isize) {
        self.builder.write_i64(value as i64);
    }
}

fn write_hashed<T: Hash>(builder: &mut StableFingerprintBuilder, tag: &[u8], value: &T) {
    builder.write_bytes(tag);
    value.hash(&mut CanonicalHasher { builder });
}

/// Hash a sequence with its cardinality in the transcript.  The standard
/// `Hash` implementation for slices is intentionally not used directly here:
/// it is allowed to omit the length, while the identity contract must
/// distinguish `[a, b]` from `[a, b, c]` even when the common prefix hashes to
/// the same byte stream.
fn write_hashed_slice<T: Hash>(builder: &mut StableFingerprintBuilder, tag: &[u8], values: &[T]) {
    builder.write_bytes(tag);
    builder.write_u64(values.len() as u64);
    for value in values {
        value.hash(&mut CanonicalHasher { builder });
    }
}

fn write_index_matrix(builder: &mut StableFingerprintBuilder, tag: &[u8], values: &[Box<[usize]>]) {
    builder.write_bytes(tag);
    builder.write_u64(values.len() as u64);
    for row in values {
        builder.write_u64(row.len() as u64);
        for value in row {
            builder.write_u64(*value as u64);
        }
    }
}

fn write_optional_fingerprint(builder: &mut StableFingerprintBuilder, value: Option<Fingerprint>) {
    match value {
        Some(value) => {
            builder.write_u64(1);
            builder.write_fingerprint(value);
        }
        None => builder.write_u64(0),
    }
}

/// Encode the physical payload through the public, bounded EXPLAIN value
/// model.  Unlike the old Debug fallback this is an explicit schema: no
/// pointer, arena id, allocator address, or formatter-specific struct dump is
/// part of the identity.  The operator-specific EXPLAIN property builders are
/// the single semantic projection shared by human and machine consumers.
fn write_canonical_kind(
    plan: &PhysicalPlan,
    id: PhysicalPlanNodeId,
    builder: &mut StableFingerprintBuilder,
) -> Result<(), PhysicalIdentityError> {
    let node = plan.node(id);
    builder.write_bytes(b"physical-kind-explain-schema.v1");
    builder.write_bytes(node.kind.name().as_bytes());
    let properties = collect_explain_properties(plan, id, node);
    builder.write_u64(properties.len() as u64);
    for property in properties {
        builder.write_bytes(property.label.as_bytes());
        write_explain_value(builder, &property.value);
    }
    write_semantic_kind_fields(builder, &node.kind)?;
    Ok(())
}

fn write_semantic_kind_fields(
    builder: &mut StableFingerprintBuilder,
    kind: &PhysicalNodeKind,
) -> Result<(), PhysicalIdentityError> {
    use crate::cascades::physical_expression_fingerprint as expression_fingerprint;

    fn write_expressions<'a>(
        builder: &mut StableFingerprintBuilder,
        expressions: impl IntoIterator<Item = &'a paro_planner::expression::Expression>,
    ) {
        let expressions = expressions.into_iter().collect::<Vec<_>>();
        builder.write_u64(expressions.len() as u64);
        for expression in expressions {
            builder.write_fingerprint(expression_fingerprint(expression));
        }
    }

    fn write_strings(
        builder: &mut StableFingerprintBuilder,
        values: impl IntoIterator<Item = impl AsRef<str>>,
    ) {
        let values = values.into_iter().collect::<Vec<_>>();
        builder.write_u64(values.len() as u64);
        for value in values {
            builder.write_bytes(value.as_ref().as_bytes());
        }
    }

    fn write_spill_policy(
        builder: &mut StableFingerprintBuilder,
        policy: super::specs::SpillExecutionPolicy,
    ) {
        use super::specs::SpillExecutionPolicy;
        builder.write_u64(match policy {
            SpillExecutionPolicy::InMemory => 0,
            SpillExecutionPolicy::Adaptive => 1,
            SpillExecutionPolicy::ForcedExternal => 2,
        });
    }

    // Ordinary and partition-window aggregates own the same execution
    // payload. Encode it once, without reconstructing an operator or relying
    // on its abbreviated EXPLAIN presentation. Estimated capacity and the
    // resource operating point are not part of structural identity.
    fn write_aggregate(builder: &mut StableFingerprintBuilder, spec: &super::specs::AggregateSpec) {
        builder.write_u64(spec.grouping_key_count as u64);
        write_hashed_slice(
            builder,
            b"state-output-projection",
            &spec.state_output_projection,
        );
        write_expressions(builder, spec.projection_exprs.iter());
        write_hashed_slice(builder, b"aggregate-payload-types", &spec.payload_types);
        write_expressions(builder, spec.groups.iter());
        write_expressions(builder, spec.aggregates.iter());
        write_hashed_slice(builder, b"group-key-encodings", &spec.group_key_encodings);
        write_index_matrix(builder, b"grouping-sets", &spec.grouping_sets);
        write_index_matrix(builder, b"grouping-functions", &spec.grouping_functions);
        write_index_matrix(builder, b"aggregate-inputs", &spec.aggregate_inputs);
        write_hashed_slice(builder, b"aggregate-filters", &spec.aggregate_filters);
        write_index_matrix(builder, b"aggregate-orders", &spec.aggregate_orders);
        write_expressions(builder, spec.having_filter.iter());
        write_spill_policy(builder, spec.spill_policy);
        builder.write_u64(spec.post_reduction.is_some() as u64);
        if let Some(post) = &spec.post_reduction {
            write_hashed_slice(builder, b"post-aggregate-types", &post.aggregate_types);
            write_expressions(builder, post.reducers.iter());
            write_hashed_slice(builder, b"post-reducer-types", &post.reducer_types);
            write_expressions(builder, post.scalar_expressions.iter());
            write_hashed_slice(builder, b"post-scalar-types", &post.scalar_types);
            write_expressions(builder, std::iter::once(&post.predicate));
            write_hashed(
                builder,
                b"post-input-rollup-sources",
                &post.input_rollup_sources,
            );
        }
        builder.write_u64(spec.perfect_hash.is_some() as u64);
        if let Some(perfect) = &spec.perfect_hash {
            write_hashed_slice(builder, b"perfect-group-minima", &perfect.group_minima);
            write_hashed_slice(
                builder,
                b"perfect-group-cardinalities",
                &perfect.group_cardinalities,
            );
        }
        write_strings(builder, spec.output_names.iter());
        write_hashed_slice(builder, b"aggregate-output-types", &spec.output_types);
    }

    match kind {
        PhysicalNodeKind::Filter(spec) => {
            builder.write_u64(1);
            write_expressions(builder, spec.expressions.iter());
            write_hashed_slice(builder, b"projection-map", &spec.projection_map);
        }
        PhysicalNodeKind::Project(spec) => {
            builder.write_u64(2);
            write_expressions(builder, spec.expressions.iter());
            write_strings(builder, spec.output_names.iter());
            builder.write_u64(spec.visible_count as u64);
        }
        PhysicalNodeKind::RowsetScan(spec) => {
            builder.write_u64(3);
            builder.write_u64(spec.table_index as u64);
            builder.write_u64(spec.emit_row_id as u64);
            write_hashed_slice(
                builder,
                b"column-projection",
                spec.column_projection.columns(),
            );
            write_hashed_slice(
                builder,
                b"value-projections",
                spec.column_projection.value_projections(),
            );
            write_expressions(builder, spec.residual_predicates.iter());
            write_expressions(builder, spec.runtime_filter_expressions.iter());
            builder.write_u64(spec.predicate.is_some() as u64);
            if let Some(predicate) = &spec.predicate {
                super::predicate_identity::encode_predicate(
                    builder,
                    predicate,
                    crate::cascades::encode_value,
                );
            }
            builder.write_u64(spec.table.base.base.object_id.raw());
        }
        PhysicalNodeKind::Values(spec) => {
            builder.write_u64(16);
            builder.write_u64(spec.table_index as u64);
            match &spec.relation_alias {
                Some(alias) => {
                    builder.write_u64(1);
                    builder.write_bytes(alias.as_bytes());
                }
                None => builder.write_u64(0),
            }
            builder.write_u64(spec.expressions.len() as u64);
            for row in &spec.expressions {
                builder.write_u64(row.len() as u64);
                for expression in row {
                    builder.write_fingerprint(expression_fingerprint(expression));
                }
            }
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"values-output-types", &spec.output_types);
        }
        PhysicalNodeKind::ExpressionScan(spec) => {
            builder.write_u64(17);
            builder.write_u64(spec.table_index as u64);
            builder.write_u64(spec.expressions.len() as u64);
            for row in &spec.expressions {
                builder.write_u64(row.len() as u64);
                for expression in row {
                    builder.write_fingerprint(expression_fingerprint(expression));
                }
            }
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"expression-scan-output-types", &spec.output_types);
        }
        PhysicalNodeKind::Limit(spec) => {
            builder.write_u64(4);
            if let Some(limit) = &spec.limit {
                builder.write_u64(1);
                builder.write_fingerprint(expression_fingerprint(limit));
            } else {
                builder.write_u64(0);
            }
            if let Some(offset) = &spec.offset {
                builder.write_u64(1);
                builder.write_fingerprint(expression_fingerprint(offset));
            } else {
                builder.write_u64(0);
            }
        }
        PhysicalNodeKind::Sort(spec) => {
            builder.write_u64(5);
            builder.write_u64(spec.orders.len() as u64);
            for order in &spec.orders {
                builder.write_fingerprint(expression_fingerprint(&order.expression));
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
            }
            write_hashed_slice(builder, b"sort-projection", &spec.projection_map);
        }
        PhysicalNodeKind::TopN(spec) => {
            builder.write_u64(6);
            builder.write_u64(spec.orders.len() as u64);
            for order in &spec.orders {
                builder.write_fingerprint(expression_fingerprint(&order.expression));
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
            }
            write_hashed_slice(builder, b"topn-projection", &spec.projection_map);
            builder.write_u64(spec.limit as u64);
            builder.write_u64(spec.offset as u64);
        }
        PhysicalNodeKind::HashJoin(spec) => {
            builder.write_u64(7);
            builder.write_bytes(spec.join_type.to_string().as_bytes());
            builder.write_u64(match spec.anti_join_mode {
                paro_planner::operator::join::AntiJoinMode::Regular => 0,
                paro_planner::operator::join::AntiJoinMode::NullAware => 1,
            });
            match spec.mark_semantics {
                paro_planner::operator::join::MarkJoinSemantics::NotMark => builder.write_u64(0),
                paro_planner::operator::join::MarkJoinSemantics::TwoValued => builder.write_u64(1),
                paro_planner::operator::join::MarkJoinSemantics::ThreeValuedFrom(index) => {
                    builder.write_u64(2);
                    builder.write_u64(index as u64);
                }
            }
            write_join_conditions(builder, &spec.key_conditions);
            write_join_conditions(builder, &spec.build_residual_conditions);
            write_hashed_slice(builder, b"hash-left-projection", &spec.left_projection);
            write_hashed_slice(
                builder,
                b"hash-build-projection",
                &spec.build_input_projection,
            );
            builder.write_u64(spec.build_output_count as u64);
            builder.write_u64(spec.build_keys_unique as u64);
            builder.write_u64(spec.probe_residual_count as u64);
            if let Some(runtime_filter) = &spec.runtime_filter {
                builder.write_u64(1);
                builder.write_fingerprint(runtime_filter.artifact);
                write_hashed_slice(
                    builder,
                    b"runtime-filter-conditions",
                    &runtime_filter.condition_indices,
                );
            } else {
                builder.write_u64(0);
            }
        }
        PhysicalNodeKind::NestedLoopJoin(spec) => {
            builder.write_u64(8);
            builder.write_bytes(spec.join_type.to_string().as_bytes());
            write_join_conditions(builder, &spec.conditions);
            write_expressions(builder, spec.arbitrary_condition.iter());
            write_hashed_slice(builder, b"nested-left-projection", &spec.left_projection);
            write_hashed_slice(builder, b"nested-right-projection", &spec.right_projection);
        }
        PhysicalNodeKind::Aggregate(spec) => {
            builder.write_u64(9);
            write_aggregate(builder, spec);
        }
        PhysicalNodeKind::CrossProduct(spec) => {
            builder.write_u64(18);
            write_hashed_slice(builder, b"cross-left-types", &spec.left_output_types);
            write_hashed_slice(builder, b"cross-right-types", &spec.right_output_types);
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"cross-output-types", &spec.output_types);
            write_spill_policy(builder, spec.spill_policy);
        }
        PhysicalNodeKind::PartitionAggregateWindow(spec) => {
            builder.write_u64(19);
            builder.write_u64(match spec.domain {
                super::specs::PartitionAggregateDomain::Global => 0,
                super::specs::PartitionAggregateDomain::Keyed => 1,
            });
            write_hashed_slice(builder, b"partition-input-types", &spec.input_types);
            write_hashed_slice(builder, b"partition-detail-columns", &spec.detail_columns);
            write_aggregate(builder, &spec.aggregate);
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"partition-output-types", &spec.output_types);
        }
        PhysicalNodeKind::Window(spec) => {
            builder.write_u64(10);
            builder.write_u64(spec.window_index as u64);
            builder.write_u64(spec.input_width as u64);
            builder.write_u64(spec.expressions.len() as u64);
            for expression in &spec.expressions {
                builder.write_fingerprint(expression_fingerprint(
                    &paro_planner::expression::Expression::Window(expression.clone().into()),
                ));
            }
        }
        PhysicalNodeKind::MaterializedCte(spec) => {
            builder.write_u64(11);
            builder.write_u64(spec.cte_index as u64);
            builder.write_u64(spec.ref_count as u64);
            write_strings(builder, spec.column_names.iter());
        }
        PhysicalNodeKind::RecursiveCte(spec) => {
            builder.write_u64(12);
            builder.write_u64(spec.cte_index as u64);
            builder.write_u64(spec.union_all as u64);
            write_strings(builder, spec.column_names.iter());
        }
        PhysicalNodeKind::CteScan(spec) => {
            builder.write_u64(13);
            builder.write_u64(spec.cte_index as u64);
            builder.write_u64(spec.table_index as u64);
        }
        PhysicalNodeKind::SetOperation(spec) => {
            builder.write_u64(14);
            builder.write_u64(spec.table_index as u64);
            builder.write_bytes(spec.op.to_string().as_bytes());
            builder.write_u64(spec.all as u64);
        }
        PhysicalNodeKind::DummyScan(_) | PhysicalNodeKind::EmptyResult(_) => {
            builder.write_u64(15);
        }
        PhysicalNodeKind::TableFunctionScan(spec) if spec.bind_data.is_none() => {
            // Ordinary table functions bind from these arguments at execution.
            // Statement-specific opaque bind data has no canonical contract and
            // must continue to fail closed rather than use Debug or an address.
            builder.write_u64(16);
            builder.write_bytes(spec.function.name.as_bytes());
            write_hashed_slice(builder, b"signature", &spec.function.arguments);
            write_hashed(builder, b"varargs", &spec.function.varargs);
            write_hashed_slice(
                builder,
                b"named-parameters",
                &spec.function.named_parameters,
            );
            builder.write_u64(spec.function.projection_pushdown as u64);
            builder.write_u64(spec.function.filter_pushdown as u64);
            builder.write_u64(spec.table_index as u64);
            write_expressions(builder, spec.arguments.iter());
            write_hashed(builder, b"projection", &spec.projection_ids);
            write_hashed_slice(builder, b"input-types", &spec.input_table_types);
            write_strings(builder, spec.input_table_names.iter());
            write_hashed_slice(builder, b"output-types", &spec.output_types);
            write_strings(builder, spec.output_names.iter());
            builder.write_u64(spec.with_ordinality as u64);
        }
        _ => {
            return Err(PhysicalIdentityError::UnsupportedKind { kind: kind.name() });
        }
    }
    Ok(())
}

fn write_join_conditions(builder: &mut StableFingerprintBuilder, conditions: &[JoinCondition]) {
    builder.write_u64(conditions.len() as u64);
    for condition in conditions {
        builder.write_fingerprint(crate::cascades::physical_expression_fingerprint(
            &condition.left,
        ));
        builder.write_fingerprint(crate::cascades::physical_expression_fingerprint(
            &condition.right,
        ));
        builder.write_u64(match condition.comparison {
            JoinComparisonType::Equal => 0,
            JoinComparisonType::NotEqual => 1,
            JoinComparisonType::LessThan => 2,
            JoinComparisonType::GreaterThan => 3,
            JoinComparisonType::LessThanOrEqual => 4,
            JoinComparisonType::GreaterThanOrEqual => 5,
            JoinComparisonType::NotDistinctFrom => 6,
            JoinComparisonType::DistinctFrom => 7,
        });
    }
}

fn write_explain_value(builder: &mut StableFingerprintBuilder, value: &ExplainValue) {
    match value {
        ExplainValue::String(value) => {
            builder.write_u64(0);
            builder.write_bytes(value.as_bytes());
        }
        ExplainValue::Integer(value) => {
            builder.write_u64(1);
            builder.write_i64(*value);
        }
        ExplainValue::Unsigned(value) => {
            builder.write_u64(2);
            builder.write_u64(*value);
        }
        ExplainValue::Float(value) => {
            builder.write_u64(3);
            builder.write_u64(value.to_bits());
        }
        ExplainValue::Bool(value) => {
            builder.write_u64(4);
            builder.write_u64(*value as u64);
        }
        ExplainValue::Bytes(value) => {
            builder.write_u64(5);
            builder.write_u64(*value);
        }
        ExplainValue::List(values) => {
            builder.write_u64(6);
            builder.write_u64(values.len() as u64);
            for value in values {
                write_explain_value(builder, value);
            }
        }
    }
}

fn edge_kind_key(kind: PhysicalEdgeKind) -> (u64, Option<Fingerprint>) {
    match kind {
        PhysicalEdgeKind::Data => (0, None),
        PhysicalEdgeKind::Control => (1, None),
        PhysicalEdgeKind::RuntimeFilter(fingerprint) => (2, Some(fingerprint)),
        PhysicalEdgeKind::SharedSpool(fingerprint) => (3, Some(fingerprint)),
        PhysicalEdgeKind::FixpointFeedback(fingerprint) => (4, Some(fingerprint)),
    }
}

fn collect_explain_properties(
    plan: &PhysicalPlan,
    id: PhysicalPlanNodeId,
    node: &PhysicalPlanNode,
) -> Vec<ExplainProperty> {
    let mut properties = Vec::new();
    let input_names = plan.input_names(id, 0);
    let output_names = node.output.explain_names(false);
    let input_formatter = ExplainExpressionFormatter::new(&input_names);

    match &node.kind {
        PhysicalNodeKind::Project(spec) => {
            if !spec.output_names.is_empty() {
                let scope_names = plan.expression_scope_names(id);
                let outputs = output_names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        if node
                            .output
                            .identities
                            .get(index)
                            .is_some_and(super::row_type::ColumnIdentity::is_internal)
                            && scope_names.get(index).is_some_and(|scope| scope != name)
                        {
                            scope_names[index].clone()
                        } else {
                            name.clone()
                        }
                    })
                    .collect::<Vec<_>>();
                push_list_property(&mut properties, "Output", &outputs);
            }
        }
        PhysicalNodeKind::RowsetScan(spec) => {
            let scan_formatter = ExplainExpressionFormatter::new(&output_names);
            let column_ids = spec
                .column_projection
                .columns()
                .iter()
                .map(|column| column.to_string())
                .collect::<Vec<_>>();
            if !column_ids.is_empty() {
                push_list_property(&mut properties, "Column IDs", &column_ids);
            }
            if !output_names.is_empty() {
                push_list_property(&mut properties, "Columns", &output_names);
            }
            if let Some(predicate) = &spec.predicate {
                push_string_property(
                    &mut properties,
                    "Pushed Predicate",
                    format_predicate_tree(predicate, spec.table.as_ref()),
                );
            }
            if spec.planned_materialization().is_late() {
                push_string_property(&mut properties, "Late Materialize", "auto".to_string());
            }
            if !spec.residual_predicates.is_empty() {
                push_string_property(
                    &mut properties,
                    "Residual Predicate",
                    spec.residual_predicates
                        .iter()
                        .map(|expression| scan_formatter.format(expression))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            if !spec.runtime_filter_expressions.is_empty() {
                push_string_property(
                    &mut properties,
                    "Runtime Filter",
                    spec.runtime_filter_expressions
                        .iter()
                        .map(|expression| scan_formatter.format(expression))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            if let Some(order) = &spec.scan_order {
                push_string_property(
                    &mut properties,
                    "Scan Order",
                    format_segment_order(order, spec.table.as_ref()),
                );
            }
        }
        PhysicalNodeKind::Filter(spec) => {
            let filter_formatter = ExplainExpressionFormatter::new(&input_names);
            if !spec.expressions.is_empty() {
                push_string_property(
                    &mut properties,
                    "Filter",
                    spec.expressions
                        .iter()
                        .map(|expression| filter_formatter.format(expression))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
        }
        PhysicalNodeKind::Sort(spec) => {
            if !spec.orders.is_empty() {
                push_string_property(
                    &mut properties,
                    "Sort Key",
                    input_formatter.format_order_by(&spec.orders),
                );
            }
        }
        PhysicalNodeKind::TopN(spec) => {
            if !spec.orders.is_empty() {
                push_string_property(
                    &mut properties,
                    "Sort Key",
                    input_formatter.format_order_by(&spec.orders),
                );
            }
            push_string_property(&mut properties, "Limit", spec.limit.to_string());
        }
        PhysicalNodeKind::VectorSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_vector_search_properties(&mut properties, spec);
        }
        PhysicalNodeKind::SparseVectorSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_sparse_search_properties(&mut properties, spec);
        }
        PhysicalNodeKind::FullTextSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_fulltext_search_properties(&mut properties, spec);
        }
        PhysicalNodeKind::AdaptiveSearch(spec) => {
            push_string_property(&mut properties, "Strategy", "adaptive".to_string());
            push_search_token_properties(
                &mut properties,
                search_source_token(spec.selected.as_ref()),
            );
            push_search_source_properties(&mut properties, spec.selected.as_ref());
        }
        PhysicalNodeKind::Limit(spec) => {
            if let Some(limit) = &spec.limit {
                push_string_property(&mut properties, "Limit", input_formatter.format(limit));
            }
            if let Some(offset) = &spec.offset {
                push_string_property(&mut properties, "Offset", input_formatter.format(offset));
            }
        }
        PhysicalNodeKind::HashJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            if let Some(runtime_filter) = &spec.runtime_filter {
                push_string_property(
                    &mut properties,
                    "Runtime Filter",
                    format!(
                        "wait_complete capability={:?} peak={}B builders={} artifact={:?}",
                        runtime_filter.resource.capability,
                        runtime_filter.resource.peak_memory_bytes,
                        runtime_filter.resource.max_local_builders,
                        runtime_filter.artifact
                    ),
                );
            }
            if spec.build_time_integer_index.is_some() {
                push_string_property(
                    &mut properties,
                    "Build Index",
                    "integer_build_time".to_string(),
                );
            }
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(
                &mut properties,
                &spec.key_conditions,
                &left_names,
                &right_names,
            );
            push_join_conditions(
                &mut properties,
                &spec.build_residual_conditions,
                &left_names,
                &right_names,
            );
        }
        PhysicalNodeKind::NestedLoopJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            if let Some(strategy) = nested_loop_strategy(spec) {
                push_string_property(&mut properties, "Strategy", strategy.to_string());
            }
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(&mut properties, &spec.conditions, &left_names, &right_names);
            if let Some(condition) = &spec.arbitrary_condition {
                let combined_names = left_names
                    .iter()
                    .chain(right_names.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                push_string_property(
                    &mut properties,
                    "Join Filter",
                    ExplainExpressionFormatter::new(&combined_names).format(condition),
                );
            }
        }
        PhysicalNodeKind::SortRangeJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_string_property(&mut properties, "Strategy", "sort_range".to_string());
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(&mut properties, &spec.conditions, &left_names, &right_names);
        }
        PhysicalNodeKind::ClassicIeJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_string_property(&mut properties, "Strategy", "classic_ie_join".to_string());
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(&mut properties, &spec.conditions, &left_names, &right_names);
        }
        PhysicalNodeKind::GraphScan(spec) => {
            push_string_property(&mut properties, "Graph", spec.graph_name.clone());
            push_string_property(&mut properties, "Vertex Label", spec.label.clone());
            push_string_property(
                &mut properties,
                "Table",
                spec.vertex_info.table_name.clone(),
            );
            if spec.filter.is_some() {
                push_string_property(&mut properties, "Filter", "<pushed down>".to_string());
            }
        }
        PhysicalNodeKind::GraphExpand(spec) => {
            push_string_property(&mut properties, "Graph", spec.graph_name.clone());
            push_string_property(&mut properties, "Edge Label", spec.edge_info.label.clone());
            push_string_property(
                &mut properties,
                "Direction",
                expand_direction_name(spec.direction).to_string(),
            );
            if spec.min_hops != 1 || spec.max_hops != 1 {
                push_string_property(
                    &mut properties,
                    "Hops",
                    format_hops(spec.min_hops, spec.max_hops),
                );
            }
        }
        PhysicalNodeKind::GraphShortestPath(spec) => {
            push_string_property(&mut properties, "Graph", spec.graph_name.clone());
            push_string_property(&mut properties, "Edge Label", spec.edge_info.label.clone());
            push_string_property(
                &mut properties,
                "Direction",
                expand_direction_name(spec.direction).to_string(),
            );
            push_string_property(
                &mut properties,
                "Hops",
                format_hops(spec.min_hops, spec.max_hops),
            );
        }
        PhysicalNodeKind::GraphProject(spec) => {
            if !spec.output_names.is_empty() {
                push_list_property(&mut properties, "Output", &output_names);
            }
            if !spec.filters.is_empty() {
                push_string_property(&mut properties, "Filter", "<pushed down>".to_string());
            }
        }
        PhysicalNodeKind::RowFetch(spec) => {
            let visible_output_count = spec
                .projection
                .as_ref()
                .map_or(output_names.len(), |projection| projection.visible_count);
            let visible_outputs = &output_names[..visible_output_count.min(output_names.len())];
            if !visible_outputs.is_empty() {
                push_list_property(&mut properties, "Output", visible_outputs);
            }
            push_string_property(&mut properties, "Sources", spec.mappings.len().to_string());
        }
        PhysicalNodeKind::Aggregate(spec) => {
            push_aggregate_properties(&mut properties, spec, &input_names);
        }
        PhysicalNodeKind::PartitionAggregateWindow(spec) => {
            push_aggregate_properties(&mut properties, &spec.aggregate, &input_names);
            push_string_property(
                &mut properties,
                "Retained Detail Columns",
                spec.detail_columns.len().to_string(),
            );
            if !output_names.is_empty() {
                push_list_property(&mut properties, "Output", &output_names);
            }
        }
        PhysicalNodeKind::MaterializedCte(spec) => {
            push_string_property(&mut properties, "CTE Name", spec.cte_name.clone());
            push_string_property(
                &mut properties,
                "Materialization",
                format_cte_materialization(spec.materialized).to_string(),
            );
            push_string_property(
                &mut properties,
                "Reference Count",
                spec.ref_count.to_string(),
            );
        }
        PhysicalNodeKind::RecursiveCte(spec) => {
            push_string_property(&mut properties, "CTE Name", spec.cte_name.clone());
            push_string_property(&mut properties, "Union All", spec.union_all.to_string());
        }
        PhysicalNodeKind::CteScan(spec) => {
            push_string_property(&mut properties, "CTE Index", spec.cte_index.to_string());
            push_string_property(&mut properties, "Table Index", spec.table_index.to_string());
        }
        PhysicalNodeKind::Insert(spec) => {
            push_string_property(&mut properties, "Table", spec.table.name().to_string());
            if !spec.column_index_map.is_empty() {
                let mapping = spec
                    .column_index_map
                    .iter()
                    .enumerate()
                    .filter_map(|(input_idx, column_idx)| {
                        spec.table
                            .columns
                            .get(*column_idx)
                            .map(|column| format!("input#{input_idx}->{}", column.name))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if !mapping.is_empty() {
                    push_string_property(&mut properties, "Column Mapping", mapping);
                }
            }
        }
        PhysicalNodeKind::CopyToFile(spec) => {
            push_string_property(&mut properties, "File", spec.file_path.clone());
            push_string_property(
                &mut properties,
                "PerThreadOutput",
                spec.per_thread_output.to_string(),
            );
        }
        _ => {}
    }

    push_string_property(&mut properties, "Output Schema", format_output_schema(node));

    properties
}

fn push_aggregate_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &AggregateSpec,
    input_names: &[String],
) {
    let formatter = ExplainExpressionFormatter::new(input_names);
    if spec.grouping_key_count > 0 {
        push_string_property(
            properties,
            "Group Key",
            spec.groups
                .iter()
                .map(|expression| format_payload_expr(expression, spec, &formatter))
                .collect::<Vec<_>>()
                .join(", "),
        );
        if spec.initial_lookup_hash_key_count < spec.grouping_key_count {
            push_string_property(
                properties,
                "Initial Lookup Hash Key",
                spec.groups[..spec.initial_lookup_hash_key_count]
                    .iter()
                    .map(|expression| format_payload_expr(expression, spec, &formatter))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
    }
    let aggregate_names = spec
        .aggregates
        .iter()
        .map(|expression| format_aggregate_expr(expression, spec, &formatter))
        .collect::<Vec<_>>();
    if !aggregate_names.is_empty() {
        push_string_property(properties, "Aggregates", aggregate_names.join(", "));
    }
    // Emit-time HAVING references aggregate results, not the output layout
    // (which also contains grouping keys and may be independently projected).
    let result_formatter = ExplainExpressionFormatter::new(&aggregate_names);
    if !spec.having_filter.is_empty() {
        push_string_property(
            properties,
            "Having",
            spec.having_filter
                .iter()
                .map(|expression| result_formatter.format(expression))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
    }
    if let Some(reduction) = &spec.post_reduction {
        push_string_property(
            properties,
            "Post Reduction",
            reduction
                .reducers
                .iter()
                .map(|expression| result_formatter.format(expression))
                .collect::<Vec<_>>()
                .join(", "),
        );
        push_string_property(
            properties,
            "Post Predicate",
            result_formatter.format(&reduction.predicate),
        );
    }
    if !spec.grouping_sets.is_empty() {
        push_string_property(
            properties,
            "Grouping Sets",
            spec.grouping_sets
                .iter()
                .map(|set| {
                    format!(
                        "({})",
                        set.iter()
                            .map(|index| {
                                spec.groups.get(*index).map_or_else(
                                    || format!("<group {index}>"),
                                    |expression| format_payload_expr(expression, spec, &formatter),
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
}

fn expand_direction_name(direction: paro_planner::operator::ExpandDirection) -> &'static str {
    match direction {
        paro_planner::operator::ExpandDirection::Forward => "forward",
        paro_planner::operator::ExpandDirection::Backward => "backward",
        paro_planner::operator::ExpandDirection::Both => "both",
    }
}

fn search_capability_state_name(
    state: &paro_storage::search::SearchCapabilityState,
) -> &'static str {
    match state {
        paro_storage::search::SearchCapabilityState::Queryable => "queryable",
        paro_storage::search::SearchCapabilityState::NotQueryable { reason } => match reason {
            paro_storage::search::SearchNotQueryableReason::CoverageIncomplete => {
                "not_queryable:coverage_incomplete"
            }
            paro_storage::search::SearchNotQueryableReason::TailOverBudget => {
                "not_queryable:tail_over_budget"
            }
            paro_storage::search::SearchNotQueryableReason::FreshnessRequired => {
                "not_queryable:freshness_required"
            }
            paro_storage::search::SearchNotQueryableReason::ProviderDisabled => {
                "not_queryable:provider_disabled"
            }
        },
    }
}

fn search_request_mode_name(mode: &paro_storage::search::SearchRequestMode) -> String {
    match mode {
        paro_storage::search::SearchRequestMode::Filter => "filter".to_string(),
        paro_storage::search::SearchRequestMode::TopK { limit } => format!("top_k:{limit}"),
    }
}

fn push_search_token_properties(properties: &mut Vec<ExplainProperty>, token: &CapabilityToken) {
    push_string_property(
        properties,
        "Search Definition",
        token.definition_id.to_string(),
    );
    push_string_property(
        properties,
        "Search Generation",
        token.generation_id.to_string(),
    );
    push_string_property(properties, "Search Root", token.root_version.to_string());
    push_string_property(
        properties,
        "Search Capability",
        search_capability_state_name(&token.capability_state).to_string(),
    );
}

fn push_search_filter_properties(
    properties: &mut Vec<ExplainProperty>,
    predicate: Option<&crate::physical::specs::SearchPredicateTemplate>,
    contract: crate::physical::specs::SearchFilterContract,
    materialization: Option<paro_storage::search::ExactFilterMaterialization>,
    table: &TableCatalogEntry,
) {
    let Some(predicate) = predicate else {
        return;
    };
    push_string_property(
        properties,
        "Pushed Predicate",
        format_search_predicate(predicate, table),
    );
    let contract = match (contract, materialization) {
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual,
            Some(paro_storage::search::ExactFilterMaterialization::ScalarIndex),
        ) => "exact segment row set from scalar index; ordinal admission and posting scan; no residual filter",
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual,
            Some(paro_storage::search::ExactFilterMaterialization::ColumnScan),
        ) => "exact segment row set materialized by column scan; no residual filter",
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual,
            Some(paro_storage::search::ExactFilterMaterialization::Mixed {
                indexed_rows,
                scanned_rows,
            }),
        ) => {
            push_string_property(
                properties,
                "Filter Row-Set Coverage",
                format!("indexed rows {indexed_rows}; column-scan rows {scanned_rows}"),
            );
            "exact segment row set from mixed scalar indexes and column scans; no residual filter"
        }
        (crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual, None) => {
            "exact segment row set; materialization unknown; no residual filter"
        }
        (crate::physical::specs::SearchFilterContract::None, _) => "unproven",
    };
    push_string_property(properties, "Filter Pushdown", contract.to_string());
}

fn push_dense_search_filter_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::VectorSearchSpec,
) {
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.filter_materialization,
        spec.table.as_ref(),
    );
    if spec.params.objective == paro_storage::index::hnsw::HnswSearchObjective::Exact {
        if spec.predicate.is_some() {
            push_string_property(
                properties,
                "Filtered Strategy",
                "query objective forces exact row-set distance scan; HNSW navigation, predicate topology, and adaptive refinement are disabled"
                    .to_string(),
            );
        }
        return;
    }
    if spec.predicate.is_none() {
        return;
    }

    let has_runtime_parameters = spec
        .predicate
        .as_ref()
        .is_some_and(|predicate| predicate.has_runtime_parameters());
    if !has_runtime_parameters {
        if let Some(rows) = spec.estimated_filter_rows {
            push_string_property(properties, "Filter Rows (estimated)", rows.to_string());
        }
    }

    let predicate_columns = paro_storage::index::collect_predicate_columns(
        spec.predicate
            .as_ref()
            .expect("predicate checked above")
            .tree(),
    );
    let accelerated_columns = spec
        .filter_topology
        .columns()
        .iter()
        .copied()
        .filter(|column| predicate_columns.contains(column))
        .map(|column| table_column_name(spec.table.as_ref(), column as usize))
        .collect::<Vec<_>>();
    // Runtime prepares exact segment row sets and lowers every immutable
    // artifact from its actual physical scan workload. EXPLAIN describes that
    // contract rather than pretending a plan-time selectivity estimate fixes
    // the executed path.
    let exact_scan = if accelerated_columns.is_empty() {
        "exact row-set distance scan"
    } else {
        "exact row-set distance scan using scalar-block covering vector ranges for compatible ordinal predicates"
    };
    let strategy = format!(
        "runtime cost choice per immutable artifact: {exact_scan} is compared with graph work using definition-pinned physical cost coefficients, effective ef, and exact covering/base row counts; graph execution uses predicate-agnostic HNSW navigation with deferred global-beam admission for broad predicates, exact eager admission and hierarchical scalar-block topology for selective predicates, observed two-hop repair, and exact fallback"
    );
    push_string_property(properties, "Filtered Strategy", strategy);
    if !accelerated_columns.is_empty() {
        push_string_property(
            properties,
            "Exact Filter Scan",
            format!(
                "covering scalar-block vector ranges available for compatible single-column ordinal predicates on {}; base vector pages are not gathered",
                accelerated_columns.join(", ")
            ),
        );
        push_string_property(
            properties,
            "Predicate Topology",
            format!(
                "available on {}: tagged hierarchical scalar-block HNSW plus bounded vector-aware cross-block routing; every admitted block has a durable entry point and exact row-set admission",
                accelerated_columns.join(", ")
            ),
        );
    }
}

fn push_vector_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::VectorSearchSpec,
) {
    let widths =
        spec.search_policy
            .effective_widths(spec.k, spec.params.ef, spec.params.rerank_window);
    push_string_property(properties, "Search Candidate", "dense vector".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Limit", spec.k.to_string());
    push_string_property(
        properties,
        "Search Objective",
        match spec.params.objective {
            paro_storage::index::hnsw::HnswSearchObjective::CostOptimized => {
                "cost_optimized: definition-pinned cost selects exact scoring or graph traversal"
                    .to_string()
            }
            paro_storage::index::hnsw::HnswSearchObjective::Exact => {
                "exact: score every admitted vector; graph traversal disabled".to_string()
            }
        },
    );
    push_string_property(
        properties,
        "Search Ef",
        format!(
            "{} (effective {} per graph shard)",
            spec.params
                .ef
                .map_or_else(|| "default".to_string(), |ef| ef.to_string()),
            widths.ef
        ),
    );
    push_string_property(
        properties,
        "Graph Shards",
        format!(
            "{} (estimated aggregate beam width {})",
            spec.graph_shard_count,
            widths.ef.saturating_mul(spec.graph_shard_count),
        ),
    );
    push_string_property(
        properties,
        "Exact Rerank Window",
        format!(
            "{} (effective {}, definition policy {})",
            spec.params
                .rerank_window
                .map_or_else(|| "default".to_string(), |window| window.to_string()),
            widths.rerank_window,
            spec.search_policy.rerank_policy,
        ),
    );
    push_string_property(
        properties,
        "Exact/Graph Cost Profile",
        format!(
            "random-access={} units, exact-f32={} units/dimension, sequential={} units/dimension, symmetric-i16={} units/dimension, graph={} unique scores/ef, routing={} (source={}, definition-pinned); observed generation average level-0 degree={:.2} is descriptive, not a cost cap",
            spec.search_policy.distance_cost.random_access_cost_units,
            spec.search_policy.distance_cost.exact_f32_dimension_cost_units,
            spec.search_policy.distance_cost.sequential_dimension_cost_units,
            spec.search_policy.distance_cost.symmetric_i16_dimension_cost_units,
            spec.search_policy.distance_cost.graph_scored_points_per_ef,
            spec.search_policy.vector_encoding,
            spec.search_policy.distance_cost.source,
            spec.avg_level0_degree,
        ),
    );
    push_dense_search_filter_properties(properties, spec);
}

fn push_sparse_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::SparseVectorSearchSpec,
) {
    push_string_property(properties, "Search Candidate", "sparse vector".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Limit", spec.k.to_string());
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.filter_materialization,
        spec.table.as_ref(),
    );
}

fn push_fulltext_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::FullTextSearchSpec,
) {
    push_string_property(properties, "Search Candidate", "full text".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Mode", search_request_mode_name(&spec.mode));
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.filter_materialization,
        spec.table.as_ref(),
    );
}

fn push_search_source_properties(properties: &mut Vec<ExplainProperty>, source: &SearchSourceSpec) {
    match source {
        SearchSourceSpec::Vector(spec) => push_vector_search_properties(properties, spec),
        SearchSourceSpec::Sparse(spec) => push_sparse_search_properties(properties, spec),
        SearchSourceSpec::FullText(spec) => push_fulltext_search_properties(properties, spec),
    }
}

fn search_source_token(source: &SearchSourceSpec) -> &CapabilityToken {
    match source {
        SearchSourceSpec::Vector(spec) => &spec.capability_token,
        SearchSourceSpec::Sparse(spec) => &spec.capability_token,
        SearchSourceSpec::FullText(spec) => &spec.capability_token,
    }
}

fn push_string_property(properties: &mut Vec<ExplainProperty>, label: &'static str, value: String) {
    properties.push(ExplainProperty::new(
        label,
        ExplainValue::String(bound_explain_text(value)),
    ));
}

fn push_list_property(
    properties: &mut Vec<ExplainProperty>,
    label: &'static str,
    values: &[String],
) {
    let mut bounded = values
        .iter()
        .take(EXPLAIN_EXPRESSION_MAX_NODES)
        .cloned()
        .map(bound_explain_text)
        .map(ExplainValue::String)
        .collect::<Vec<_>>();
    if values.len() > EXPLAIN_EXPRESSION_MAX_NODES {
        bounded.push(ExplainValue::String("…".to_string()));
    }
    properties.push(ExplainProperty::new(label, ExplainValue::List(bounded)));
}

fn push_join_conditions(
    properties: &mut Vec<ExplainProperty>,
    conditions: &[JoinCondition],
    left_names: &[String],
    right_names: &[String],
) {
    if conditions.is_empty() {
        return;
    }
    properties.push(ExplainProperty::new(
        "Join Condition",
        ExplainValue::List(
            conditions
                .iter()
                .map(|condition| {
                    ExplainValue::String(format_join_condition(condition, left_names, right_names))
                })
                .collect(),
        ),
    ));
}

fn explain_operator_name(kind: &PhysicalNodeKind) -> &'static str {
    match kind {
        PhysicalNodeKind::Project(_) => "PROJECTION",
        PhysicalNodeKind::Sort(_) => "ORDER_BY",
        PhysicalNodeKind::Aggregate(_) => "AGGREGATE",
        _ => kind.name(),
    }
}

fn explain_relation_name(kind: &PhysicalNodeKind) -> Option<String> {
    match kind {
        PhysicalNodeKind::RowsetScan(spec) => spec.relation_name.clone(),
        PhysicalNodeKind::VectorSearch(spec) => Some(qualified_table_name(spec.table.as_ref())),
        PhysicalNodeKind::SparseVectorSearch(spec) => {
            Some(qualified_table_name(spec.table.as_ref()))
        }
        PhysicalNodeKind::FullTextSearch(spec) => Some(qualified_table_name(spec.table.as_ref())),
        PhysicalNodeKind::AdaptiveSearch(spec) => Some(qualified_table_name(spec.table.as_ref())),
        _ => None,
    }
}

fn explain_relation_alias(kind: &PhysicalNodeKind) -> Option<String> {
    match kind {
        PhysicalNodeKind::RowsetScan(spec) => spec.relation_alias.clone(),
        PhysicalNodeKind::Values(spec) => spec.relation_alias.clone(),
        _ => None,
    }
}

fn explain_cardinality(
    kind: &PhysicalNodeKind,
    estimated: Option<CardinalityEstimate>,
) -> Option<CardinalityEstimate> {
    let fallback = fallback_cardinality(kind);
    match (estimated, fallback) {
        (Some(current), Some(fallback))
            if is_search_scan(kind) && current.expected == 0 && fallback.expected > 0 =>
        {
            Some(fallback)
        }
        (Some(current), _) => Some(current),
        (None, fallback) => fallback,
    }
}

fn is_search_scan(kind: &PhysicalNodeKind) -> bool {
    matches!(
        kind,
        PhysicalNodeKind::VectorSearch(_)
            | PhysicalNodeKind::SparseVectorSearch(_)
            | PhysicalNodeKind::FullTextSearch(_)
            | PhysicalNodeKind::AdaptiveSearch(_)
    )
}

fn fallback_cardinality(kind: &PhysicalNodeKind) -> Option<CardinalityEstimate> {
    let rows = match kind {
        PhysicalNodeKind::VectorSearch(spec) => spec
            .estimated_total_rows
            .filter(|rows| *rows > 0)
            .or_else(|| table_row_count(spec.table.as_ref())),
        PhysicalNodeKind::SparseVectorSearch(spec) => spec
            .table
            .storage
            .as_ref()
            .and_then(|table| table.sparse_index_statistics(spec.column_id as u32))
            .map(|stats| stats.num_indexed_vectors as u64)
            .filter(|rows| *rows > 0)
            .or_else(|| table_row_count(spec.table.as_ref())),
        PhysicalNodeKind::FullTextSearch(spec) => spec
            .table
            .storage
            .as_ref()
            .and_then(|table| table.fulltext_index_statistics(spec.column_id as u32))
            .map(|stats| stats.total_docs as u64)
            .filter(|rows| *rows > 0)
            .or_else(|| table_row_count(spec.table.as_ref())),
        PhysicalNodeKind::AdaptiveSearch(spec) => table_row_count(spec.table.as_ref()),
        _ => None,
    }?;
    Some(CardinalityEstimate::exact(rows))
}

fn table_row_count(table: &paro_catalog::entry::TableCatalogEntry) -> Option<u64> {
    table
        .storage
        .as_ref()
        .and_then(|storage| storage.tablet().statistics().ok())
        .map(|stats| stats.num_rows)
        .filter(|rows| *rows > 0)
        .or_else(|| {
            table
                .statistics()
                .and_then(|stats| (stats.row_count > 0).then_some(stats.row_count))
        })
}

fn qualified_table_name(table: &paro_catalog::entry::TableCatalogEntry) -> String {
    format!("{}.{}", table.schema_name(), table.name())
}

fn format_segment_order(order: &SegmentOrderOptions, table: &TableCatalogEntry) -> String {
    let stat = match order.order_by {
        OrderByStatistics::Min => "min",
        OrderByStatistics::Max => "max",
    };
    let direction = match order.order_type {
        SegmentOrderType::Asc => "ASC",
        SegmentOrderType::Desc => "DESC",
    };
    let limit = order
        .row_limit
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();
    let offset = if order.row_offset == 0 {
        String::new()
    } else {
        format!(" OFFSET {}", order.row_offset)
    };
    let column = table_column_name(table, order.column_idx);
    format!("{column} {direction} by {stat}{limit}{offset}")
}

fn format_hops(min_hops: u64, max_hops: u64) -> String {
    if max_hops == u64::MAX {
        format!("{{{},}}", min_hops)
    } else {
        format!("{{{min_hops},{max_hops}}}")
    }
}

fn format_output_schema(node: &PhysicalPlanNode) -> String {
    if node.output.column_count() == 0 {
        return "(none)".to_string();
    }
    let visible_width = match &node.kind {
        PhysicalNodeKind::RowFetch(spec) => spec
            .projection
            .as_ref()
            .map_or(node.output.column_count(), |projection| {
                projection.visible_count
            }),
        _ => node.output.column_count(),
    };
    node.output
        .identities
        .iter()
        .enumerate()
        .zip(node.output.types.iter())
        .take(visible_width)
        .map(|((ordinal, identity), ty)| {
            let name = match identity {
                super::row_type::ColumnIdentity::Visible {
                    name,
                    qualifier: Some(qualifier),
                } => qualifier
                    .iter()
                    .map(|part| format_schema_identifier(part))
                    .chain(std::iter::once(format_schema_identifier(name)))
                    .collect::<Vec<_>>()
                    .join("."),
                super::row_type::ColumnIdentity::Visible {
                    name,
                    qualifier: None,
                } => format_schema_identifier(name),
                super::row_type::ColumnIdentity::Internal => {
                    format_schema_identifier(&format!("__internal_{}", ordinal + 1))
                }
                super::row_type::ColumnIdentity::InternalNamed(name) => {
                    format_schema_identifier(name)
                }
                super::row_type::ColumnIdentity::Locator { .. } => {
                    format_schema_identifier("rowid")
                }
            };
            format!("{name} {ty}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_schema_identifier(name: &str) -> String {
    if !name.is_empty()
        && name.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        })
    {
        return name.to_string();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn format_join_condition(
    condition: &JoinCondition,
    left_names: &[String],
    right_names: &[String],
) -> String {
    let left = ExplainExpressionFormatter::new(left_names).format(&condition.left);
    let right = ExplainExpressionFormatter::new(right_names).format(&condition.right);
    format!(
        "{} {} {}",
        left,
        join_comparison_symbol(condition.comparison),
        right
    )
}

fn join_comparison_symbol(comparison: JoinComparisonType) -> &'static str {
    match comparison {
        JoinComparisonType::Equal => "=",
        JoinComparisonType::NotEqual => "<>",
        JoinComparisonType::LessThan => "<",
        JoinComparisonType::GreaterThan => ">",
        JoinComparisonType::LessThanOrEqual => "<=",
        JoinComparisonType::GreaterThanOrEqual => ">=",
        JoinComparisonType::NotDistinctFrom => "IS NOT DISTINCT FROM",
        JoinComparisonType::DistinctFrom => "IS DISTINCT FROM",
    }
}

fn nested_loop_strategy(spec: &NestedLoopJoinSpec) -> Option<&'static str> {
    if is_range_only_join(&spec.conditions) {
        Some("nl_fallback")
    } else {
        None
    }
}

fn is_range_only_join(conditions: &[JoinCondition]) -> bool {
    !conditions.is_empty()
        && conditions.iter().all(|condition| {
            matches!(
                condition.comparison,
                JoinComparisonType::LessThan
                    | JoinComparisonType::GreaterThan
                    | JoinComparisonType::LessThanOrEqual
                    | JoinComparisonType::GreaterThanOrEqual
            )
        })
}

fn format_cte_materialization(
    materialized: paro_planner::binder::ir::CTEMaterialize,
) -> &'static str {
    match materialized {
        paro_planner::binder::ir::CTEMaterialize::Default
        | paro_planner::binder::ir::CTEMaterialize::Materialized => "MATERIALIZED",
        paro_planner::binder::ir::CTEMaterialize::NotMaterialized => "NOT MATERIALIZED",
    }
}

fn aggregate_scope_names(spec: &AggregateSpec, input_names: &[String]) -> Option<Vec<String>> {
    let formatter = ExplainExpressionFormatter::new(input_names);
    let mut state_names = spec
        .groups
        .iter()
        .map(|expression| format_payload_expr(expression, spec, &formatter))
        .chain(
            spec.aggregates
                .iter()
                .map(|expression| format_aggregate_expr(expression, spec, &formatter)),
        )
        .collect::<Vec<_>>();
    state_names.extend(spec.grouping_functions.iter().map(|grouping| {
        let arguments = grouping
            .iter()
            .filter_map(|index| spec.groups.get(*index))
            .map(|expression| format_payload_expr(expression, spec, &formatter))
            .collect::<Vec<_>>()
            .join(", ");
        bound_explain_text(format!("grouping({arguments})"))
    }));

    if spec.state_output_projection.is_empty() {
        return Some(state_names);
    }
    spec.state_output_projection
        .iter()
        .map(|index| state_names.get(*index).cloned())
        .collect()
}

fn format_payload_expr(
    expression: &Expression,
    spec: &AggregateSpec,
    formatter: &ExplainExpressionFormatter<'_>,
) -> String {
    if let Expression::Reference(reference) = expression {
        if let Some(payload) = spec.projection_exprs.get(reference.index) {
            return formatter.format(payload);
        }
    }
    formatter.format(expression)
}

fn format_aggregate_expr(
    expression: &Expression,
    spec: &AggregateSpec,
    formatter: &ExplainExpressionFormatter<'_>,
) -> String {
    let Expression::Aggregate(aggregate) = expression else {
        return formatter.format(expression);
    };
    format_bound_aggregate(aggregate, &|child| {
        format_payload_expr(child, spec, formatter)
    })
}

fn format_bound_aggregate(
    aggregate: &AggregateExpression,
    format_child: &impl Fn(&Expression) -> String,
) -> String {
    let distinct = if aggregate.aggr_type == AggregateType::Distinct {
        "DISTINCT "
    } else {
        ""
    };
    let args = if aggregate.children.is_empty() {
        "*".to_string()
    } else {
        aggregate
            .children
            .iter()
            .map(format_child)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut rendered = format!("{}({distinct}{args})", aggregate.function.name);
    if let Some(filter) = aggregate.filter.as_ref() {
        rendered.push_str(" FILTER (WHERE ");
        rendered.push_str(&format_child(filter));
        rendered.push(')');
    }
    if !aggregate.order_bys.is_empty() {
        rendered.push_str(" WITHIN GROUP (ORDER BY ");
        rendered.push_str(
            &aggregate
                .order_bys
                .iter()
                .map(|order| {
                    format!(
                        "{} {} NULLS {}",
                        format_child(&order.expression),
                        if order.ascending { "ASC" } else { "DESC" },
                        if order.nulls_first { "FIRST" } else { "LAST" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
        rendered.push(')');
    }
    rendered
}

struct ExplainExpressionFormatter<'a> {
    columns: &'a [String],
}

#[cfg(test)]
mod expression_format_tests {
    use super::*;
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConstantExpression, ReferenceExpression,
    };

    #[test]
    fn comparison_presentation_does_not_depend_on_native_hash_orientation() {
        let names = ["sum(amount)".to_string()];
        let formatter = ExplainExpressionFormatter::new(&names);
        let column =
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into());
        for literal in [Value::Integer(100), Value::Null(LogicalType::Integer)] {
            let constant =
                Expression::Constant(ConstantExpression::new(literal, LogicalType::Integer).into());
            for op in [
                ComparisonType::Equal,
                ComparisonType::NotEqual,
                ComparisonType::LessThan,
                ComparisonType::LessThanOrEqual,
                ComparisonType::GreaterThan,
                ComparisonType::GreaterThanOrEqual,
                ComparisonType::DistinctFrom,
                ComparisonType::NotDistinctFrom,
            ] {
                assert_eq!(op.flipped().flipped(), op);
                let normal = Expression::Comparison(
                    ComparisonExpression::new(op, column.clone(), constant.clone()).into(),
                );
                let reversed = Expression::Comparison(
                    ComparisonExpression::new(op.flipped(), constant.clone(), column.clone())
                        .into(),
                );
                let before = reversed.allocation_identity();
                assert_eq!(formatter.format(&normal), formatter.format(&reversed));
                assert_eq!(reversed.allocation_identity(), before);
                assert!(formatter.format(&normal).starts_with("sum(amount) "));
            }
        }
    }

    #[test]
    fn explain_text_uses_matrixone_operator_and_property_columns() {
        assert_eq!(PhysicalPlan::explain_operator_indent(0), 0);
        assert_eq!(PhysicalPlan::explain_operator_indent(1), 2);
        assert_eq!(PhysicalPlan::explain_operator_indent(2), 8);
        assert_eq!(PhysicalPlan::explain_operator_indent(3), 14);

        assert_eq!(PhysicalPlan::explain_property_indent(0), 2);
        assert_eq!(PhysicalPlan::explain_property_indent(1), 8);
        assert_eq!(PhysicalPlan::explain_property_indent(2), 14);
    }
}

const EXPLAIN_EXPRESSION_MAX_NODES: usize = 1_024;
const EXPLAIN_EXPRESSION_MAX_DEPTH: usize = 64;
const EXPLAIN_EXPRESSION_MAX_BYTES: usize = 16 * 1_024;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ExplainPrecedence {
    Lowest,
    Or,
    And,
    Not,
    Comparison,
    Primary,
}

struct ExplainFormatBudget {
    remaining_nodes: Cell<usize>,
}

impl ExplainFormatBudget {
    fn new() -> Self {
        Self {
            remaining_nodes: Cell::new(EXPLAIN_EXPRESSION_MAX_NODES),
        }
    }

    fn enter(&self, depth: usize) -> bool {
        let remaining = self.remaining_nodes.get();
        if depth >= EXPLAIN_EXPRESSION_MAX_DEPTH || remaining == 0 {
            return false;
        }
        self.remaining_nodes.set(remaining - 1);
        true
    }
}

impl<'a> ExplainExpressionFormatter<'a> {
    fn new(columns: &'a [String]) -> Self {
        Self { columns }
    }

    fn column(&self, index: usize) -> String {
        self.columns
            .get(index)
            .cloned()
            .unwrap_or_else(|| format!("<column {index}>"))
    }

    fn format_order_by(&self, orders: &[paro_planner::binder::ir::OrderByNode]) -> String {
        bound_explain_text(
            orders
                .iter()
                .map(|order| {
                    format!(
                        "{} {} NULLS {}",
                        self.format(&order.expression),
                        if order.ascending { "ASC" } else { "DESC" },
                        if order.nulls_first { "FIRST" } else { "LAST" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        )
    }

    fn format(&self, expression: &Expression) -> String {
        let budget = ExplainFormatBudget::new();
        bound_explain_text(self.format_expression(
            expression,
            ExplainPrecedence::Lowest,
            0,
            &budget,
        ))
    }

    fn format_expression(
        &self,
        expression: &Expression,
        parent_precedence: ExplainPrecedence,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        if !budget.enter(depth) {
            return "…".to_string();
        }
        let next_depth = depth + 1;
        let (rendered, precedence) = match expression {
            Expression::Reference(reference) => {
                (self.column(reference.index), ExplainPrecedence::Primary)
            }
            Expression::ColumnRef(column) => (
                self.column(column.binding.column_index),
                ExplainPrecedence::Primary,
            ),
            Expression::Constant(constant) => {
                (constant.value.to_string(), ExplainPrecedence::Primary)
            }
            Expression::Parameter(parameter) => (
                format!("${}", parameter.slot.index.index() + 1),
                ExplainPrecedence::Primary,
            ),
            Expression::Comparison(comparison) => {
                // Native scalar identity may choose either orientation by
                // fingerprint. Presentation has its own stable convention:
                // keep a lone literal on the right. This changes no IR or
                // evaluation order and prevents hash order from changing a
                // named predicate boundary in EXPLAIN consumers.
                let (left, op, right) =
                    if matches!(comparison.left.as_ref(), Expression::Constant(_))
                        && !matches!(comparison.right.as_ref(), Expression::Constant(_))
                    {
                        (
                            &comparison.right,
                            comparison.comparison_type.flipped(),
                            &comparison.left,
                        )
                    } else {
                        (
                            &comparison.left,
                            comparison.comparison_type,
                            &comparison.right,
                        )
                    };
                (
                    format!(
                        "{} {} {}",
                        self.format_expression(
                            left,
                            ExplainPrecedence::Comparison,
                            next_depth,
                            budget,
                        ),
                        op,
                        self.format_expression(
                            right,
                            ExplainPrecedence::Comparison,
                            next_depth,
                            budget,
                        )
                    ),
                    ExplainPrecedence::Comparison,
                )
            }
            Expression::Conjunction(conjunction) => {
                let (separator, precedence) = match conjunction.conjunction_type {
                    paro_planner::expression::ConjunctionType::And => {
                        (" AND ", ExplainPrecedence::And)
                    }
                    paro_planner::expression::ConjunctionType::Or => {
                        (" OR ", ExplainPrecedence::Or)
                    }
                };
                (
                    conjunction
                        .children
                        .iter()
                        .map(|child| self.format_expression(child, precedence, next_depth, budget))
                        .collect::<Vec<_>>()
                        .join(separator),
                    precedence,
                )
            }
            Expression::Cast(cast) => {
                let cast_name = if cast.try_cast { "TRY_CAST" } else { "CAST" };
                (
                    format!(
                        "{cast_name}({} AS {})",
                        self.format_expression(
                            &cast.child,
                            ExplainPrecedence::Lowest,
                            next_depth,
                            budget,
                        ),
                        cast.target_type
                    ),
                    ExplainPrecedence::Primary,
                )
            }
            Expression::Function(function) => (
                format!(
                    "{}({})",
                    function.function.name.as_str(),
                    function
                        .children
                        .iter()
                        .map(|child| self.format_expression(
                            child,
                            ExplainPrecedence::Lowest,
                            next_depth,
                            budget,
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                ExplainPrecedence::Primary,
            ),
            Expression::Aggregate(aggregate) => (
                format_bound_aggregate(aggregate, &|child| {
                    self.format_expression(child, ExplainPrecedence::Lowest, next_depth, budget)
                }),
                ExplainPrecedence::Primary,
            ),
            Expression::Case(case_expression) => (
                format!(
                    "CASE WHEN {} THEN {} ELSE {} END",
                    self.format_expression(
                        &case_expression.check,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    ),
                    self.format_expression(
                        &case_expression.result_if_true,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    ),
                    self.format_expression(
                        &case_expression.result_if_false,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    )
                ),
                ExplainPrecedence::Primary,
            ),
            Expression::Operator(operator) => self.format_operator(operator, next_depth, budget),
            Expression::Subquery(subquery) => {
                let kind = match subquery.subquery_type {
                    paro_planner::expression::SubqueryType::Scalar => "SUBQUERY",
                    paro_planner::expression::SubqueryType::Exists => "EXISTS SUBQUERY",
                    paro_planner::expression::SubqueryType::NotExists => "NOT EXISTS SUBQUERY",
                    paro_planner::expression::SubqueryType::Any => "ANY SUBQUERY",
                    paro_planner::expression::SubqueryType::All => "ALL SUBQUERY",
                };
                if subquery.children.is_empty() {
                    (format!("<{kind}>"), ExplainPrecedence::Primary)
                } else {
                    (
                        format!(
                            "{} {} <{kind}>",
                            subquery
                                .children
                                .iter()
                                .map(|child| self.format_expression(
                                    child,
                                    ExplainPrecedence::Lowest,
                                    next_depth,
                                    budget,
                                ))
                                .collect::<Vec<_>>()
                                .join(", "),
                            subquery.comparison_type
                        ),
                        ExplainPrecedence::Comparison,
                    )
                }
            }
            Expression::Window(window) => (
                self.format_window(window, next_depth, budget),
                ExplainPrecedence::Primary,
            ),
        };
        let rendered = bound_explain_text(rendered);
        if precedence < parent_precedence {
            bound_explain_text(format!("({rendered})"))
        } else {
            rendered
        }
    }

    fn format_operator(
        &self,
        operator: &paro_planner::expression::OperatorExpression,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> (String, ExplainPrecedence) {
        let child = |index: usize, precedence: ExplainPrecedence| {
            operator
                .children
                .get(index)
                .map(|child| self.format_expression(child, precedence, depth, budget))
        };
        let children = || {
            operator
                .children
                .iter()
                .map(|child| {
                    self.format_expression(child, ExplainPrecedence::Lowest, depth, budget)
                })
                .collect::<Vec<_>>()
        };
        match operator.operator_type {
            OperatorType::In | OperatorType::NotIn => (
                child(0, ExplainPrecedence::Comparison).map_or_else(
                    || "<invalid IN>".to_string(),
                    |left| {
                        format!(
                            "{} {}IN ({})",
                            left,
                            if operator.operator_type == OperatorType::NotIn {
                                "NOT "
                            } else {
                                ""
                            },
                            operator.children[1..]
                                .iter()
                                .map(|child| self.format_expression(
                                    child,
                                    ExplainPrecedence::Lowest,
                                    depth,
                                    budget,
                                ))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                ),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::Not => (
                child(0, ExplainPrecedence::Not)
                    .map(|child| format!("NOT {child}"))
                    .unwrap_or_else(|| "<invalid NOT>".to_string()),
                ExplainPrecedence::Not,
            ),
            OperatorType::IsNull => (
                child(0, ExplainPrecedence::Comparison)
                    .map(|child| format!("{child} IS NULL"))
                    .unwrap_or_else(|| "<invalid IS NULL>".to_string()),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::IsNotNull => (
                child(0, ExplainPrecedence::Comparison)
                    .map(|child| format!("{child} IS NOT NULL"))
                    .unwrap_or_else(|| "<invalid IS NOT NULL>".to_string()),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::Coalesce => (
                format!("COALESCE({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::Like | OperatorType::ILike => (
                match (
                    child(0, ExplainPrecedence::Comparison),
                    child(1, ExplainPrecedence::Comparison),
                ) {
                    (Some(left), Some(right)) => format!(
                        "{} {} {}",
                        left,
                        if operator.operator_type == OperatorType::Like {
                            "LIKE"
                        } else {
                            "ILIKE"
                        },
                        right
                    ),
                    _ => "<invalid LIKE>".to_string(),
                },
                ExplainPrecedence::Comparison,
            ),
            OperatorType::ArrayConstructor => (
                format!("[{}]", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::StructConstructor => (
                format!("({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::ArrayExtract => (
                match (
                    child(0, ExplainPrecedence::Primary),
                    child(1, ExplainPrecedence::Lowest),
                ) {
                    (Some(array), Some(index)) => format!("{array}[{index}]"),
                    _ => "<invalid array extract>".to_string(),
                },
                ExplainPrecedence::Primary,
            ),
            OperatorType::ErrorIfMultipleRows => (
                format!("error_if_multiple_rows({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
        }
    }

    fn format_window(
        &self,
        window: &paro_planner::expression::WindowExpression,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        let mut rendered = format!(
            "{}({}) OVER (",
            window.function_name(),
            window
                .arguments()
                .iter()
                .map(|argument| self.format_expression(
                    argument,
                    ExplainPrecedence::Lowest,
                    depth,
                    budget,
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !window.partitions.is_empty() {
            rendered.push_str("PARTITION BY ");
            rendered.push_str(
                &window
                    .partitions
                    .iter()
                    .map(|partition| {
                        self.format_expression(partition, ExplainPrecedence::Lowest, depth, budget)
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !window.orders.is_empty() {
            if !window.partitions.is_empty() {
                rendered.push(' ');
            }
            rendered.push_str("ORDER BY ");
            rendered.push_str(
                &window
                    .orders
                    .iter()
                    .map(|order| {
                        format!(
                            "{} {} NULLS {}",
                            self.format_expression(
                                &order.expression,
                                ExplainPrecedence::Lowest,
                                depth,
                                budget,
                            ),
                            if order.ascending { "ASC" } else { "DESC" },
                            if order.nulls_first { "FIRST" } else { "LAST" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !window.partitions.is_empty() || !window.orders.is_empty() {
            rendered.push(' ');
        }
        rendered.push_str(match window.frame.frame_type {
            WindowFrameType::Rows => "ROWS BETWEEN ",
            WindowFrameType::Range => "RANGE BETWEEN ",
        });
        rendered.push_str(&self.format_window_bound(
            &window.frame.start_bound,
            window.frame.start_is_preceding,
            depth,
            budget,
        ));
        rendered.push_str(" AND ");
        rendered.push_str(&self.format_window_bound(
            &window.frame.end_bound,
            window.frame.end_is_preceding,
            depth,
            budget,
        ));
        rendered.push(')');
        bound_explain_text(rendered)
    }

    fn format_window_bound(
        &self,
        bound: &WindowFrameBound,
        preceding: bool,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        match bound {
            WindowFrameBound::Unbounded => format!(
                "UNBOUNDED {}",
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Offset(offset) => format!(
                "{} {}",
                self.format_expression(offset, ExplainPrecedence::Lowest, depth, budget),
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
        }
    }
}

fn bound_explain_text(mut text: String) -> String {
    if text.len() <= EXPLAIN_EXPRESSION_MAX_BYTES {
        return text;
    }
    let mut boundary = EXPLAIN_EXPRESSION_MAX_BYTES.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text.truncate(boundary);
    text.push('…');
    text
}

fn table_column_name(table: &TableCatalogEntry, column_id: usize) -> String {
    table
        .columns
        .get(column_id)
        .map(|column| column.name.clone())
        .unwrap_or_else(|| format!("<column {column_id}>"))
}

fn format_predicate_tree<V: std::fmt::Display>(
    predicate: &PredicateTree<V>,
    table: &TableCatalogEntry,
) -> String {
    let budget = ExplainFormatBudget::new();
    format_predicate_tree_with_precedence(predicate, table, ExplainPrecedence::Lowest, 0, &budget)
}

fn format_predicate_tree_with_precedence<V: std::fmt::Display>(
    predicate: &PredicateTree<V>,
    table: &TableCatalogEntry,
    parent_precedence: ExplainPrecedence,
    depth: usize,
    budget: &ExplainFormatBudget,
) -> String {
    if !budget.enter(depth) {
        return "…".to_string();
    }
    let next_depth = depth + 1;
    let (rendered, precedence) = match predicate {
        PredicateTree::Leaf(predicate) => (
            format_predicate(predicate, table),
            ExplainPrecedence::Primary,
        ),
        PredicateTree::And(children) => (
            children
                .iter()
                .map(|child| {
                    format_predicate_tree_with_precedence(
                        child,
                        table,
                        ExplainPrecedence::And,
                        next_depth,
                        budget,
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            ExplainPrecedence::And,
        ),
        PredicateTree::Or(children) => (
            children
                .iter()
                .map(|child| {
                    format_predicate_tree_with_precedence(
                        child,
                        table,
                        ExplainPrecedence::Or,
                        next_depth,
                        budget,
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR "),
            ExplainPrecedence::Or,
        ),
    };
    if precedence < parent_precedence {
        bound_explain_text(format!("({rendered})"))
    } else {
        bound_explain_text(rendered)
    }
}

fn format_predicate<V: std::fmt::Display>(
    predicate: &Predicate<V>,
    table: &TableCatalogEntry,
) -> String {
    let name = |column_id: u32| table_column_name(table, column_id as usize);
    match predicate {
        Predicate::Eq { column_id, value } => format!("{} = {value}", name(*column_id)),
        Predicate::NotEq { column_id, value } => format!("{} != {value}", name(*column_id)),
        Predicate::Lt { column_id, value } => format!("{} < {value}", name(*column_id)),
        Predicate::Le { column_id, value } => format!("{} <= {value}", name(*column_id)),
        Predicate::Gt { column_id, value } => format!("{} > {value}", name(*column_id)),
        Predicate::Ge { column_id, value } => format!("{} >= {value}", name(*column_id)),
        Predicate::In { column_id, values } => format!(
            "{} IN ({})",
            name(*column_id),
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Predicate::FixedIn { column_id, values } => {
            format!("{} IN ({} fixed values)", name(*column_id), values.len())
        }
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => format!("{} BETWEEN {lower} AND {upper}", name(*column_id)),
        Predicate::IsNull { column_id } => format!("{} IS NULL", name(*column_id)),
        Predicate::IsNotNull { column_id } => format!("{} IS NOT NULL", name(*column_id)),
        Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        } => format!(
            "{} {} PREFIX {prefix:?}",
            name(*column_id),
            if *negated { "NOT" } else { "HAS" }
        ),
        Predicate::StringPrefixIn {
            column_id,
            prefixes,
        } => format!(
            "{} HAS PREFIX IN ({})",
            name(*column_id),
            prefixes
                .iter()
                .map(|prefix| format!("{prefix:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Predicate::StringLike {
            column_id,
            pattern,
            negated,
        } => format!(
            "{} {} {pattern:?}",
            name(*column_id),
            if *negated { "NOT LIKE" } else { "LIKE" }
        ),
        Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        } => format!(
            "{} {comparison} {}",
            name(*left_column_id),
            name(*right_column_id)
        ),
    }
}

fn format_search_predicate(
    predicate: &crate::physical::specs::SearchPredicateTemplate,
    table: &TableCatalogEntry,
) -> String {
    format_predicate_tree(predicate.tree(), table)
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use crate::physical::cost::MemoryCompletion;
    use crate::physical::identity::{MutationBarrierId, SnapshotId};
    use crate::physical::specs::{DummyScanSpec, MutationInputSpoolSpec};
    use crate::physical::{InlinePlanChildren, ResourceGrantClassId};
    use crate::physical::{OperatorLabel, RowType};
    use paro_common::types::LogicalType;
    use paro_planner::plan::PlanNodeId;

    fn dummy_plan(prefix_unreachable: bool, label: &str, output_name: &str) -> PhysicalPlan {
        let mut nodes = PhysicalPlanNodeArena::default();
        if prefix_unreachable {
            nodes.push(PhysicalPlanNode {
                id: PhysicalPlanNodeId::INVALID,
                output: RowType::new(Vec::new(), Vec::new()),
                cardinality: None,
                kind: PhysicalNodeKind::DummyScan(DummyScanSpec),
                children: PlanChildren::Empty,
                label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "unreachable"),
            });
        }
        let root = nodes.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output: RowType::new(vec![output_name.to_string()], vec![LogicalType::Unknown]),
            cardinality: None,
            kind: PhysicalNodeKind::DummyScan(DummyScanSpec),
            children: PlanChildren::Empty,
            label: OperatorLabel::new(PlanNodeId::SYNTHETIC, label),
        });
        PhysicalPlan::new(
            root,
            nodes,
            PlanChildrenArena::default(),
            PlanPropertyMap::default(),
        )
    }

    #[test]
    fn structural_identity_ignores_arena_and_display_allocation() {
        let left = dummy_plan(false, "left presentation", "value");
        let right = dummy_plan(true, "right presentation", "value");
        assert_eq!(
            left.structural_identity_fingerprint().unwrap(),
            right.structural_identity_fingerprint().unwrap()
        );
    }

    #[test]
    fn structural_identity_includes_output_layout() {
        let left = dummy_plan(false, "same", "left");
        let right = dummy_plan(false, "same", "right");
        assert_ne!(
            left.structural_identity_fingerprint().unwrap(),
            right.structural_identity_fingerprint().unwrap()
        );
    }

    #[test]
    fn structural_identity_excludes_cost_and_resource_operating_point() {
        let mut left = dummy_plan(false, "same", "value");
        let mut right = dummy_plan(false, "same", "value");
        left.nodes
            .get_mut(left.root)
            .expect("dummy root")
            .cardinality = Some(CardinalityEstimate::exact(1));
        right
            .nodes
            .get_mut(right.root)
            .expect("dummy root")
            .cardinality = Some(CardinalityEstimate::exact(99));
        right.execution_resources = Some(ExecutionResourceContract {
            class: ResourceGrantClassId(2),
            minimum_memory_bytes: 1,
            working_set_memory_bytes: 2,
            memory_ceiling_bytes: 3,
            memory_completion: MemoryCompletion::Guaranteed,
            max_parallel_tasks: 4,
            external_worker_slots: 0,
        });
        assert_eq!(
            left.structural_identity_fingerprint().unwrap(),
            right.structural_identity_fingerprint().unwrap()
        );
    }

    #[test]
    fn structural_identity_covers_cross_product_and_nested_aggregate_payload() {
        use crate::physical::specs::{
            AggregateSpec, CrossProductSpec, PartitionAggregateDomain,
            PartitionAggregateWindowSpec, SpillExecutionPolicy,
        };

        fn fingerprint(kind: PhysicalNodeKind) -> Fingerprint {
            let mut builder = StableFingerprintBuilder::default();
            write_semantic_kind_fields(&mut builder, &kind).unwrap();
            builder.finish()
        }

        let mut cross = CrossProductSpec {
            left_output_types: Box::new([LogicalType::Integer]),
            right_output_types: Box::new([LogicalType::BigInt]),
            output_names: Box::new(["a".into(), "b".into()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
            spill_policy: SpillExecutionPolicy::Adaptive,
        };
        let original = fingerprint(PhysicalNodeKind::CrossProduct(cross.clone()));
        cross.spill_policy = SpillExecutionPolicy::ForcedExternal;
        assert_ne!(
            original,
            fingerprint(PhysicalNodeKind::CrossProduct(cross.clone()))
        );
        cross.spill_policy = SpillExecutionPolicy::Adaptive;
        cross.left_output_types = Box::new([LogicalType::BigInt]);
        assert_ne!(original, fingerprint(PhysicalNodeKind::CrossProduct(cross)));

        let aggregate = AggregateSpec {
            grouping_key_count: 0,
            initial_lookup_hash_key_count: 0,
            state_output_projection: Box::new([]),
            estimated_input_rows: Some(10),
            projection_exprs: Box::new([]),
            payload_types: Box::new([]),
            groups: Box::new([]),
            group_key_encodings: Box::new([]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([]),
            aggregate_filters: Box::new([]),
            aggregate_orders: Box::new([]),
            post_reduction: None,
            having_filter: Box::new([]),
            spill_policy: SpillExecutionPolicy::Adaptive,
            perfect_hash: None,
            output_names: Box::new([]),
            output_types: Box::new([]),
        };
        let mut window = PartitionAggregateWindowSpec {
            domain: PartitionAggregateDomain::Global,
            input_types: Box::new([LogicalType::Integer]),
            detail_columns: Box::new([0]),
            aggregate,
            output_names: Box::new(["a".into()]),
            output_types: Box::new([LogicalType::Integer]),
        };
        let original = fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(
            window.clone(),
        )));
        window.aggregate.estimated_input_rows = Some(100);
        assert_eq!(
            original,
            fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(
                window.clone()
            )))
        );
        window.aggregate.spill_policy = SpillExecutionPolicy::ForcedExternal;
        assert_ne!(
            original,
            fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(
                window.clone()
            )))
        );
        window.aggregate.spill_policy = SpillExecutionPolicy::Adaptive;
        window.detail_columns = Box::new([]);
        assert_ne!(
            original,
            fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(window)))
        );
    }

    #[test]
    fn structural_identity_fails_closed_for_cycles_and_invalid_edges() {
        let mut cyclic = dummy_plan(false, "same", "value");
        cyclic.nodes.get_mut(cyclic.root).unwrap().children =
            PlanChildren::Inline(InlinePlanChildren::new(&[cyclic.root]));
        assert_eq!(
            cyclic.structural_identity_fingerprint(),
            Err(PhysicalIdentityError::Cycle)
        );

        let mut invalid_edge = dummy_plan(false, "same", "value");
        invalid_edge.edges.push(
            invalid_edge.root,
            PhysicalPlanNodeId::INVALID,
            PhysicalEdgeKind::Data,
        );
        assert_eq!(
            invalid_edge.structural_identity_fingerprint(),
            Err(PhysicalIdentityError::InvalidEdge)
        );
    }

    #[test]
    fn table_function_identity_tracks_payload_and_rejects_opaque_binding() {
        use crate::physical::specs::TableFunctionScanSpec;
        use paro_function::table::{BoundTableFunctionData, TableFunction, TableFunctionBindData};
        use std::sync::Arc;

        #[derive(Clone)]
        struct Opaque;
        impl TableFunctionBindData for Opaque {
            fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
                Box::new(self.clone())
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        let mut plan = dummy_plan(false, "function", "value");
        let spec = TableFunctionScanSpec {
            function: Arc::new(TableFunction::new("catalog_rows", vec![])),
            bind_data: None,
            table_index: 0,
            arguments: Box::new([]),
            projection_ids: None,
            input_table_types: Box::new([]),
            input_table_names: Box::new([]),
            output_names: Box::new(["value".to_string()]),
            output_types: Box::new([LogicalType::BigInt]),
            with_ordinality: false,
        };
        plan.nodes.get_mut(plan.root).unwrap().kind =
            PhysicalNodeKind::TableFunctionScan(spec.clone());
        let original = plan.structural_identity_fingerprint().unwrap();
        let mut changed = spec;
        changed.with_ordinality = true;
        plan.nodes.get_mut(plan.root).unwrap().kind =
            PhysicalNodeKind::TableFunctionScan(changed.clone());
        assert_ne!(original, plan.structural_identity_fingerprint().unwrap());
        changed.bind_data = Some(BoundTableFunctionData::new(Box::new(Opaque)));
        plan.nodes.get_mut(plan.root).unwrap().kind = PhysicalNodeKind::TableFunctionScan(changed);
        assert_eq!(
            plan.structural_identity_fingerprint(),
            Err(PhysicalIdentityError::UnsupportedKind {
                kind: "TABLE_FUNCTION_SCAN"
            })
        );
    }

    #[test]
    fn structural_identity_fails_closed_for_unencoded_operator_payloads() {
        let mut nodes = PhysicalPlanNodeArena::default();
        let root = nodes.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output: RowType::new(Vec::new(), Vec::new()),
            cardinality: None,
            kind: PhysicalNodeKind::MutationInputSpool(MutationInputSpoolSpec {
                barrier: MutationBarrierId::new(0),
                targets: Default::default(),
                snapshot: SnapshotId::new(0),
            }),
            children: PlanChildren::Empty,
            label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "values"),
        });
        let plan = PhysicalPlan::new(
            root,
            nodes,
            PlanChildrenArena::default(),
            PlanPropertyMap::default(),
        );
        assert_eq!(
            plan.structural_identity_fingerprint(),
            Err(PhysicalIdentityError::UnsupportedKind {
                kind: "MUTATION_INPUT_SPOOL"
            })
        );
    }
}
