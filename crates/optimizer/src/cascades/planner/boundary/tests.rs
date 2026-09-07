// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, ConstantExpression};
use paro_planner::operator::{Projection, SetOpType, SetOperation};

fn source(table: usize) -> OwnedLogicalPlan {
    let mut plan = super::super::tests::test_base_get(table, table as u64 + 1, "facts", 10);
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10));
    plan
}

fn project(plan: OwnedLogicalPlan, table: usize) -> OwnedLogicalPlan {
    let binding = plan.get_column_bindings()[0];
    OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        table,
        plan,
        vec![Expression::ColumnRef(ColumnRefExpression::new(
            binding,
            LogicalType::Integer,
        ))],
    )))
}

fn input(plan: OwnedLogicalPlan, budget: SearchBudget) -> OptimizationInput {
    MemoBuilder::build(plan, BindContext::new(), budget).unwrap()
}

fn grouped_branch_with_tag(
    source_table: usize,
    output_table: usize,
    tag: &str,
) -> OwnedLogicalPlan {
    let aggregate = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(
        paro_planner::operator::Aggregate::new(
            output_table + 10,
            output_table + 11,
            output_table + 12,
            source(source_table),
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(source_table, 0),
                LogicalType::Integer,
            ))],
            vec![],
            vec![],
            vec![],
        ),
    ));
    OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        output_table,
        aggregate,
        vec![
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(output_table + 10, 0),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Varchar(tag.to_string()),
                LogicalType::Varchar,
            )),
        ],
    )))
}

#[test]
fn native_boundary_retains_alias_lineage_and_records_inherited_statistics() {
    let mut input = input(project(source(0), 1), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        snapshot
            .cardinality(context.memo(), input.root)
            .unwrap()
            .expected,
        10
    );
    let sources = snapshot.groups[&input.root]
        .lineage
        .values()
        .next()
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].source, 0);
    let source_group = GroupId::new(sources[0].occurrence);
    let first_work = context
        .memo()
        .group(input.root)
        .unwrap()
        .ledger
        .consumed(BudgetDimension::RuleWorkPerGroup);
    let reused = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(Arc::ptr_eq(
        &snapshot.groups[&input.root],
        &reused.groups[&input.root]
    ));
    let second_work = context
        .memo()
        .group(input.root)
        .unwrap()
        .ledger
        .consumed(BudgetDimension::RuleWorkPerGroup)
        - first_work;
    assert!(
        second_work < first_work,
        "a current read log must not rebuild the evidence recipes: {first_work} -> {second_work}"
    );
    let reads = context.take_fact_reads();
    assert!(reads.iter().any(|read| read.group == source_group));
    let root_read = PatternRead::facts_from_group(context.memo(), input.root).unwrap();
    drop(context);
    input.memo.group_mut(source_group).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(9),
        CardinalityRecipeKind::Statistics,
        20,
        20,
        20,
    );
    assert!(root_read.is_current(&input.memo).unwrap());
    assert!(reads
        .iter()
        .any(|read| !read.is_current(&input.memo).unwrap()));
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let refreshed = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(!Arc::ptr_eq(
        &snapshot.groups[&input.root],
        &refreshed.groups[&input.root]
    ));
    assert_eq!(
        refreshed
            .cardinality(context.memo(), input.root)
            .unwrap()
            .expected,
        20
    );
}

#[test]
fn value_domain_change_invalidates_the_fact_value_not_only_the_read_cursor() {
    use paro_planner::operator::bound_reference::BoundColumnValues;
    use paro_storage::statistics::BaseStatistics;
    let mut input = input(source(0), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let column = *input
        .memo
        .group(input.root)
        .unwrap()
        .schema
        .ids()
        .first()
        .unwrap();
    let mut values = Vec::new();
    for constant in [2001, 2002, 2001] {
        input
            .memo
            .group_mut(input.root)
            .unwrap()
            .logical_properties
            .column_values
            .insert(
                column,
                BoundColumnValues::new(BaseStatistics::from_constant(&Value::Integer(constant)))
                    .unwrap(),
            );
        let mut ctx = TransformContext::new(&mut input.memo, input.root);
        let snapshot = BoundarySnapshot::read(
            &mut ctx,
            &state,
            &PatternOperand::Group(input.root),
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let mut fingerprint = StableFingerprintBuilder::default();
        snapshot.encode_group(input.root, &mut fingerprint).unwrap();
        values.push(fingerprint.finish());
    }
    assert_ne!(values[0], values[1]);
    assert_eq!(values[0], values[2]);
}

#[test]
fn finite_replay_proof_survives_a_recursive_identity_alternative() {
    let mut input = input(source(0), SearchBudget::default());
    let mut state = input.planner_state.write().unwrap();
    let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let original = input.memo.logical_expr(expression).unwrap().payload;
    let mut metadata = state.metadata[&original].clone();
    metadata.operator_type = paro_planner::operator::LogicalOperatorType::Filter;
    metadata.child_layouts = Box::new([PlannerBindingLayout {
        bindings: Box::new([ColumnBinding::new(0, 0)]),
        types: Box::new([LogicalType::Integer]),
    }]);
    let column_stats = state.payloads.logical[original.index()]
        .column_stats
        .clone();
    let (payload, _) = state.payloads.push_logical(PlannerLogicalPayload {
        semantic_template: paro_planner::plan::arena::LogicalPlanNode::from_shell(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
                paro_planner::operator::Filter::new(source(0), vec![]),
            )),
        ),
        operator_encoding: Box::new([]),
        column_stats,
    });
    state.metadata.insert(payload, metadata);
    input
        .memo
        .insert_logical(
            input.root,
            LogicalExprKey {
                operator: Fingerprint(9191),
                scalars: Box::new([]),
                children: Box::new([input.root]),
            },
            payload,
            EquivalenceProof::Normalization { rule: RuleId(9191) },
        )
        .unwrap();
    let mut ctx = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut ctx,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(
        snapshot.groups[&input.root].can_replay,
        "unknown evidence from an identity cycle cannot refute a finite proof"
    );
}

#[test]
fn aggregate_key_is_derived_from_native_shell_without_cached_plan_statistics() {
    let aggregate = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(
        paro_planner::operator::Aggregate::new(
            1,
            2,
            3,
            source(0),
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(0, 0),
                LogicalType::Integer,
            ))],
            vec![],
            vec![],
            vec![],
        ),
    ));
    assert!(aggregate.stats.unique_keys.is_empty());
    let mut input = input(project(aggregate, 4), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let layout = PlannerBindingLayout {
        bindings: Box::new([ColumnBinding::new(4, 0)]),
        types: Box::new([LogicalType::Integer]),
    };
    let transported = snapshot
        .transport(context.memo(), &state, input.root, &layout)
        .unwrap();
    assert_eq!(transported.unique_keys.len(), 1);
    assert_eq!(
        transported.unique_keys[0].columns[0].binding,
        layout.bindings[0]
    );
    assert_eq!(
        transported.unique_keys[0].provenance,
        UniqueKeyProvenance::Structural
    );
}

#[test]
fn group_boundary_never_unions_coverage_from_different_alternatives() {
    let mut input = MemoBuilder::build_alternatives(
        vec![
            LogicalAlternative {
                plan: project(source(0), 2),
                source: AlternativeOrigin::Baseline,
                column_stats: Arc::new(HashMap::new()),
            },
            LogicalAlternative {
                plan: project(source(1), 2),
                source: AlternativeOrigin::Specialized {
                    rule: TOP_N_INTRODUCTION_RULE,
                },
                column_stats: Arc::new(HashMap::new()),
            },
        ],
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap();
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(snapshot.groups[&input.root]
        .lineage
        .values()
        .all(Option::is_none));
}

#[test]
fn repeated_union_occurrences_cannot_claim_one_source_work_identity() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
        1,
        source(0),
        source(0),
        SetOpType::Union,
        true,
        vec![LogicalType::Integer],
    )));
    let mut input = input(plan, SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(snapshot.groups[&input.root]
        .lineage
        .values()
        .all(Option::is_none));
}

#[test]
fn disjoint_finite_grouping_domains_make_union_all_key_composable() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
        30,
        grouped_branch_with_tag(0, 20, "store"),
        grouped_branch_with_tag(1, 21, "web"),
        SetOpType::Union,
        true,
        vec![LogicalType::Integer, LogicalType::Varchar],
    )));
    let mut input = input(plan, SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let output = input.memo.group(input.root).unwrap().schema.columns();
    assert!(snapshot.groups[&input.root]
        .unique_keys
        .contains(&Box::from([output[0].id, output[1].id])));
    assert!(snapshot.groups[&input.root]
        .grouping_unique_keys
        .contains(&Box::from([output[0].id, output[1].id])));
}

#[test]
fn exhausted_fact_read_preserves_baseline_and_reports_incomplete_work() {
    let mut budget = SearchBudget::default();
    budget.max_rule_work_units_per_group = 2;
    let mut input = input(project(source(0), 1), budget);
    let before = input.memo.logical_expr_count();
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    assert!(BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup
    )
    .unwrap()
    .is_none());
    assert_eq!(context.memo().logical_expr_count(), before);
    assert!(context
        .memo()
        .group(input.root)
        .unwrap()
        .ledger
        .exhaustion_events()
        .any(|(dimension, _)| *dimension == BudgetDimension::RuleWorkPerGroup));
}

#[test]
fn cte_registry_is_observed_even_before_a_producer_exists_and_rolls_back() {
    let mut input = input(project(source(0), 1), SearchBudget::default());
    let column = input.memo.group(input.root).unwrap().schema.columns()[0].id;
    input
        .memo
        .group_mut(input.root)
        .unwrap()
        .logical_properties
        .cte_references
        .insert(CteReferenceDomain {
            cte_index: 7,
            columns: Box::new([column]),
        });
    let read = PatternRead::facts_from_group(&input.memo, input.root).unwrap();
    let savepoint = input.memo.transformation_savepoint();
    input
        .memo
        .register_cte_producer(7, input.root, Box::new([column]));
    assert!(!read.is_current(&input.memo).unwrap());
    assert_eq!(input.memo.take_changed_cte_readers(), vec![input.root]);
    input.memo.rollback_transformation(savepoint).unwrap();
    assert!(read.is_current(&input.memo).unwrap());
    assert!(input.memo.take_changed_cte_readers().is_empty());
}

#[test]
fn shared_dag_facts_do_not_expand_bag_occurrences() {
    let bind = BindContext::new();
    let mut plan = source(0);
    for table in 1..9 {
        let right = duplicate_plan_preserving_indices(&plan, bind.shared().as_ref());
        plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
            table,
            plan,
            right,
            SetOpType::Union,
            true,
            vec![LogicalType::Integer],
        )));
    }
    let mut budget = SearchBudget::default();
    budget.max_rule_work_units_per_group = 128;
    let mut input = input(plan, budget);
    // Import is occurrence-preserving. Explicitly establish the shared DAG
    // whose evidence reader is under test, independently of import policy.
    let groups = input
        .memo
        .groups()
        .map(|group| (group.id, group.schema.columns().to_vec()))
        .collect::<Vec<_>>();
    let mut canonical = Vec::new();
    for (group, schema) in groups {
        if let Some((previous, _)) = canonical.iter().find(|(_, existing)| existing == &schema) {
            input.memo.merge_groups(*previous, group).unwrap();
        } else {
            canonical.push((group, schema));
        }
    }
    input.root = input.memo.canonical_group(input.root);
    assert_eq!(input.memo.groups().count(), 9);
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert_eq!(snapshot.groups.len(), 9);
    assert_eq!(context.take_fact_reads().len(), 9);
}
