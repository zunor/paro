// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{
    ColumnBinding, ExpressionGet, LogicalOperator, Projection, RowFetch, RowFetchSource,
};
use paro_planner::plan::LogicalPlan;

use super::*;

#[test]
fn lowers_late_row_fetch_with_resolved_carrier_rowid() {
    const CARRIER: usize = 40;
    const MATERIALIZED: usize = 41;
    const OUTPUT: usize = 42;

    let ctx = BindContext::new();
    let table = super::tests::test_get().table.expect("stored table");
    let carrier = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            CARRIER,
            Vec::new(),
            vec!["key".into(), "rowid".into()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        )),
    );
    let fetch = LogicalPlan::new(
        &ctx,
        LogicalOperator::RowFetch(RowFetch::new(
            CARRIER,
            vec![RowFetchSource {
                materialized_table_index: MATERIALIZED,
                rowid: Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(CARRIER, 1),
                    LogicalType::BigInt,
                )),
                table,
            }],
            carrier,
        )),
    );
    let projection = Projection::new(
        OUTPUT,
        fetch,
        vec![
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(CARRIER, 0),
                LogicalType::Integer,
            )),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(MATERIALIZED, 2),
                LogicalType::Varchar,
            )),
        ],
    )
    .with_output_names(vec!["key".into(), "payload".into()]);
    let mut logical = LogicalPlan::new(&ctx, LogicalOperator::Projection(projection));
    crate::column_binding_resolver::ColumnBindingResolver::resolve(&mut logical.operator)
        .expect("late row-fetch bindings resolve");

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let physical = generator
        .generate(&logical)
        .expect("late row-fetch should lower");

    let PhysicalNodeKind::RowFetchProject(spec) = &physical.node(physical.root).kind else {
        panic!("expected row-fetch project root");
    };
    assert_eq!(spec.carrier_table_index, CARRIER);
    assert_eq!(spec.rowid_mappings.len(), 1);
    assert_eq!(spec.rowid_mappings[0].table_index, MATERIALIZED);
    assert_eq!(spec.rowid_mappings[0].rowid_col_idx, 1);
    assert_eq!(
        spec.output_types.as_ref(),
        [LogicalType::Integer, LogicalType::Varchar]
    );
    assert!(PhysicalPlanGenerator::ensure_fully_typed(&physical).is_ok());
}
