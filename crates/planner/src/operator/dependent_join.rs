//! Temporary operator for correlated subqueries and `LATERAL` until decorrelation replaces it with plain joins.

use super::{ColumnBinding, JoinType};
use crate::binder::CorrelatedColumnInfo;
use crate::expression::{ComparisonType, Expression};
use crate::plan::LogicalPlan;
use paro_common::types::LogicalType;

#[derive(Debug, Clone)]
pub struct AnyAllPayload {
    pub comparison_type: ComparisonType,
    pub expression_children: Vec<Expression>,
    pub child_types: Vec<LogicalType>,
    pub child_targets: Vec<LogicalType>,
}

#[derive(Debug, Clone)]
pub enum MarkSubqueryKind {
    Exists,
    NotExists,
    Any(AnyAllPayload),
    All(AnyAllPayload),
}

#[derive(Debug, Clone)]
pub enum DependentJoinKind {
    Scalar,
    Mark {
        mark_index: usize,
        subquery: MarkSubqueryKind,
    },
    Lateral {
        join_type: JoinType,
        join_condition: Option<Expression>,
    },
}

/// DependentJoin represents a join with correlated columns.
///
/// This is a temporary construct used during planning for correlated subqueries.
/// It will be transformed into a regular join by `DependentJoinFlattener`.
#[derive(Debug)]
pub struct DependentJoin {
    /// Left child operator (outer query).
    pub left: Box<LogicalPlan>,
    /// Right child operator (subquery / lateral rhs).
    pub right: Box<LogicalPlan>,
    /// The list of columns that have correlations with the right side.
    pub correlated_columns: Vec<CorrelatedColumnInfo>,
    /// Encodes the legal dependent-join state for the specific subquery shape.
    pub kind: DependentJoinKind,
}

impl DependentJoin {
    pub fn scalar(
        left: LogicalPlan,
        right: LogicalPlan,
        correlated_columns: Vec<CorrelatedColumnInfo>,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            correlated_columns,
            kind: DependentJoinKind::Scalar,
        }
    }

    pub fn mark_exists(
        left: LogicalPlan,
        right: LogicalPlan,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        mark_index: usize,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            correlated_columns,
            kind: DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::Exists,
            },
        }
    }

    pub fn mark_not_exists(
        left: LogicalPlan,
        right: LogicalPlan,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        mark_index: usize,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            correlated_columns,
            kind: DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::NotExists,
            },
        }
    }

    pub fn mark_any(
        left: LogicalPlan,
        right: LogicalPlan,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        mark_index: usize,
        payload: AnyAllPayload,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            correlated_columns,
            kind: DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::Any(payload),
            },
        }
    }

    pub fn mark_all(
        left: LogicalPlan,
        right: LogicalPlan,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        mark_index: usize,
        payload: AnyAllPayload,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            correlated_columns,
            kind: DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::All(payload),
            },
        }
    }

    pub fn lateral(
        left: LogicalPlan,
        right: LogicalPlan,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        join_type: JoinType,
        join_condition: Option<Expression>,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            correlated_columns,
            kind: DependentJoinKind::Lateral {
                join_type,
                join_condition,
            },
        }
    }

    pub fn has_correlated_columns(&self) -> bool {
        !self.correlated_columns.is_empty()
    }

    pub fn correlated_column_count(&self) -> usize {
        self.correlated_columns.len()
    }

    pub fn mark_index(&self) -> Option<usize> {
        match &self.kind {
            DependentJoinKind::Mark { mark_index, .. } => Some(*mark_index),
            _ => None,
        }
    }

    pub fn join_condition(&self) -> Option<&Expression> {
        match &self.kind {
            DependentJoinKind::Lateral { join_condition, .. } => join_condition.as_ref(),
            _ => None,
        }
    }

    pub fn join_condition_mut(&mut self) -> Option<&mut Expression> {
        match &mut self.kind {
            DependentJoinKind::Lateral { join_condition, .. } => join_condition.as_mut(),
            _ => None,
        }
    }

    pub fn mark_subquery(&self) -> Option<&MarkSubqueryKind> {
        match &self.kind {
            DependentJoinKind::Mark { subquery, .. } => Some(subquery),
            _ => None,
        }
    }

    pub fn mark_subquery_mut(&mut self) -> Option<&mut MarkSubqueryKind> {
        match &mut self.kind {
            DependentJoinKind::Mark { subquery, .. } => Some(subquery),
            _ => None,
        }
    }

    pub fn any_all_payload(&self) -> Option<&AnyAllPayload> {
        match self.mark_subquery() {
            Some(MarkSubqueryKind::Any(payload) | MarkSubqueryKind::All(payload)) => Some(payload),
            _ => None,
        }
    }

    pub fn any_all_payload_mut(&mut self) -> Option<&mut AnyAllPayload> {
        match self.mark_subquery_mut() {
            Some(MarkSubqueryKind::Any(payload) | MarkSubqueryKind::All(payload)) => Some(payload),
            _ => None,
        }
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        let mut types = self.left.types();
        match self.kind {
            DependentJoinKind::Mark { .. } => types.push(LogicalType::Boolean),
            DependentJoinKind::Scalar | DependentJoinKind::Lateral { .. } => {
                types.extend(self.right.types())
            }
        }
        types
    }

    pub fn get_column_bindings(
        &self,
        left_bindings: &[ColumnBinding],
        right_bindings: &[ColumnBinding],
    ) -> Vec<ColumnBinding> {
        let mut bindings = left_bindings.to_vec();
        match self.kind {
            DependentJoinKind::Mark { mark_index, .. } => {
                bindings.push(ColumnBinding::new(mark_index, 0));
            }
            DependentJoinKind::Scalar | DependentJoinKind::Lateral { .. } => {
                bindings.extend(right_bindings.iter().copied())
            }
        }
        bindings
    }

    pub fn output_names(&self) -> Vec<String> {
        let mut names = self.left.output_names();
        match self.kind {
            DependentJoinKind::Mark { .. } => names.push("mark".to_string()),
            DependentJoinKind::Scalar | DependentJoinKind::Lateral { .. } => {
                names.extend(self.right.output_names())
            }
        }
        names
    }

    pub fn name(&self) -> &'static str {
        "DEPENDENT_JOIN"
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::context::BindContext;
    use crate::operator::LogicalOperator;

    use super::*;

    fn dummy_plan() -> LogicalPlan {
        let ctx = BindContext::new();
        LogicalPlan::dummy_scan(&ctx)
    }

    fn correlated() -> Vec<CorrelatedColumnInfo> {
        vec![CorrelatedColumnInfo {
            table_index: 0,
            column_index: 0,
            return_type: LogicalType::Integer,
            name: "a".to_string(),
            depth: 1,
        }]
    }

    #[test]
    fn scalar_constructor_sets_kind_and_correlation_metadata() {
        let join = DependentJoin::scalar(dummy_plan(), dummy_plan(), correlated());

        assert!(join.has_correlated_columns());
        assert_eq!(join.correlated_column_count(), 1);
        assert!(matches!(join.kind, DependentJoinKind::Scalar));
        assert!(join.mark_index().is_none());
    }

    #[test]
    fn mark_any_constructor_embeds_payload_in_kind() {
        let payload = AnyAllPayload {
            comparison_type: ComparisonType::LessThan,
            expression_children: vec![],
            child_types: vec![LogicalType::Integer],
            child_targets: vec![LogicalType::BigInt],
        };
        let join = DependentJoin::mark_any(dummy_plan(), dummy_plan(), vec![], 77, payload);

        match join.mark_subquery() {
            Some(MarkSubqueryKind::Any(payload)) => {
                assert_eq!(payload.comparison_type, ComparisonType::LessThan);
                assert_eq!(payload.child_targets, vec![LogicalType::BigInt]);
            }
            other => panic!("expected any payload, got {other:?}"),
        }
        assert_eq!(join.mark_index(), Some(77));
    }

    #[test]
    fn lateral_constructor_keeps_join_type_and_condition() {
        use crate::expression::ConstantExpression;
        use paro_common::runtime_value::Value;

        let condition = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });
        let join = DependentJoin::lateral(
            dummy_plan(),
            dummy_plan(),
            vec![],
            JoinType::Left,
            Some(condition),
        );

        match &join.kind {
            DependentJoinKind::Lateral {
                join_type,
                join_condition,
            } => {
                assert_eq!(*join_type, JoinType::Left);
                assert!(join_condition.is_some());
            }
            other => panic!("expected lateral kind, got {other:?}"),
        }
    }

    #[test]
    fn mark_dependent_join_outputs_left_columns_plus_marker_binding() {
        let ctx = BindContext::new();
        let mut join = DependentJoin::mark_exists(dummy_plan(), dummy_plan(), vec![], 88);
        join.left = Box::new(LogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(crate::operator::ExpressionGet::new(
                10,
                vec![],
                vec!["c0".to_string()],
                vec![LogicalType::Integer],
            )),
        ));
        join.right = Box::new(LogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(crate::operator::ExpressionGet::new(
                20,
                vec![],
                vec!["c0".to_string()],
                vec![LogicalType::Varchar],
            )),
        ));

        let bindings = join.get_column_bindings(
            &join.left.get_column_bindings(),
            &join.right.get_column_bindings(),
        );
        assert_eq!(
            bindings,
            vec![ColumnBinding::new(10, 0), ColumnBinding::new(88, 0)]
        );
        assert_eq!(
            join.output_names(),
            vec!["c0".to_string(), "mark".to_string()]
        );
    }

    #[test]
    fn test_dependent_join_name() {
        let join = DependentJoin::scalar(dummy_plan(), dummy_plan(), vec![]);
        assert_eq!(join.name(), "DEPENDENT_JOIN");
    }
}
