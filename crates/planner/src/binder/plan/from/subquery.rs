use crate::binder::ir::BoundFromSubquery;
use crate::binder::Binder;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::{LogicalOperator, Projection};
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_subquery_ref(
        &mut self,
        sub_ref: BoundFromSubquery,
    ) -> Result<LogicalOperator> {
        let child = self.plan_query(*sub_ref.subquery)?;

        let child_bindings = child.get_column_bindings();
        let needs_projection_wrapper = child_bindings
            .iter()
            .any(|binding| binding.table_index != sub_ref.subquery_index);

        if needs_projection_wrapper {
            let expressions: Vec<Expression> = child_bindings
                .iter()
                .enumerate()
                .map(|(i, binding)| {
                    Expression::ColumnRef(ColumnRefExpression::new(
                        *binding,
                        sub_ref
                            .column_types
                            .get(i)
                            .cloned()
                            .unwrap_or(paro_common::types::LogicalType::Unknown),
                    ))
                })
                .collect();

            let projection =
                Projection::new(sub_ref.subquery_index, self.wrap_plan(child), expressions)
                    .with_output_names(sub_ref.column_names.clone());
            Ok(LogicalOperator::Projection(projection))
        } else {
            Ok(child)
        }
    }
}
