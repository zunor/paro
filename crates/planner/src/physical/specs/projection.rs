// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Projection composition preserves observable evaluation and output identity.
use super::ProjectSpec;
use crate::expression::{Expression, ExpressionIterator};

impl ProjectSpec {
    /// Compose this projection over its direct input projection.
    ///
    /// The result is equivalent to `self(inner(input))`. Composition is
    /// rejected when it could drop, duplicate, or reorder observable
    /// expression evaluation. Keeping this contract on the physical spec lets
    /// both tree rewrites and pipeline lowering use the same semantic guard.
    pub fn compose_over(&self, inner: &Self) -> Option<Self> {
        if self
            .expressions
            .iter()
            .chain(inner.expressions.iter())
            .any(|expression| expression.evaluation_properties().is_reorder_fence())
        {
            return None;
        }

        let mut references = vec![0usize; inner.expressions.len()];
        for expression in &self.expressions {
            if !count_physical_references(expression, &inner.expressions, &mut references) {
                return None;
            }
        }
        if inner
            .expressions
            .iter()
            .zip(&references)
            .any(|(expression, &count)| !expression.is_passive_value() && count != 1)
        {
            return None;
        }

        let mut expressions = self.expressions.to_vec();
        for expression in &mut expressions {
            substitute_physical_references(expression, &inner.expressions);
        }
        Some(Self {
            expressions: expressions.into_boxed_slice(),
            output_names: self.output_names.clone(),
            visible_count: self.visible_count,
        })
    }
}

fn count_physical_references(
    expression: &Expression,
    inner: &[Expression],
    references: &mut [usize],
) -> bool {
    if let Expression::Reference(reference) = expression {
        let Some(inner_expression) = inner.get(reference.index) else {
            return false;
        };
        if reference.return_type != inner_expression.return_type() {
            return false;
        }
        references[reference.index] += 1;
        return true;
    }

    let mut valid = true;
    ExpressionIterator::enumerate_children(expression, |child| {
        valid &= count_physical_references(child, inner, references);
    });
    valid
}

fn substitute_physical_references(expression: &mut Expression, inner: &[Expression]) {
    if let Expression::Reference(reference) = expression {
        *expression = inner[reference.index].clone();
        return;
    }
    ExpressionIterator::enumerate_children_mut(expression, |child| {
        substitute_physical_references(child, inner);
    });
}
