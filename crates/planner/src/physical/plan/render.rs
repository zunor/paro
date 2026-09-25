// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plain EXPLAIN document construction and tree layout.

use super::PhysicalPlan;
use crate::expression::Expression;
use crate::logical::operator::ExplainSpec;
use crate::physical::explain::types::{ExplainDoc, ExplainNode, EXPLAIN_FORMAT_VERSION};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::node::PhysicalPlanNode;
use crate::physical::specs::PhysicalNodeKind;
use expression::ExplainExpressionFormatter;
use properties::*;
use std::fmt::Write;
mod expression;
pub(super) mod properties;
impl PhysicalPlan {
    pub fn format_tree(&self) -> String {
        let mut out = String::new();
        self.format_node(self.root, 0, &mut out);
        out
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
                        .is_some_and(crate::physical::row_type::ColumnIdentity::is_internal)
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
                            .is_some_and(crate::physical::row_type::ColumnIdentity::is_internal)
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
