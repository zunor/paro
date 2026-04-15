//! Subquery planning (correlated, decorrelation, recursive, delayed copy).

mod correlated;
mod correlation_boundary;
mod decorrelate;
mod delayed_copy;
mod dispatcher;
mod flatten_plan;
mod has_correlated_expressions;
mod recursive_planner;
mod rewrite_correlated_expressions;

pub use correlation_boundary::{
    split_child_correlated_columns, CorrelationBoundaryMode, CorrelationProjectionMode,
    CorrelationSplit,
};
pub(crate) use decorrelate::flatten_dependent_join;
pub(crate) use delayed_copy::{copy_subquery_top_level, copy_subquery_top_level_plan};
pub use flatten_plan::{flatten_all_dependent_joins, has_dependent_join};
pub(crate) use has_correlated_expressions::{
    expression_has_correlated_columns_at_depth, operator_has_correlated_columns_at_depth,
};
pub(crate) use recursive_planner::RecursiveSubqueryPlanner;
pub(crate) use rewrite_correlated_expressions::{
    build_correlated_column_map, CorrelatedColumnMap, RewriteCorrelatedExpressions,
};
