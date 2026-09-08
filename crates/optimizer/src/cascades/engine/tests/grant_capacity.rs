// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exercise sharing through the real engine, including goal normalization,
//! exact child selection, empty enforcement, and streaming task inheritance.

use super::*;
use crate::cascades::properties::{
    NullOrder, OrderingKey, OrderingScope, RequiredOrdering, SortDirection,
};

struct PipelineImplementation {
    classes: BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
    child_required: crate::cascades::ids::PropertySetId,
}

impl PhysicalImplementation for PipelineImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(981)
    }

    fn grant_dependency_for(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        _: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        if expr.key.children.is_empty() {
            GrantDependencyDescriptor::Parallelism
        } else {
            GrantDependencyDescriptor::Invariant
        }
    }

    fn matches(
        &self,
        _: &crate::cascades::memo::LogicalExpr,
        _: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> bool {
        true
    }

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx.memo.logical_expr(expr).unwrap();
        let tasks = match goal.grant {
            GrantGoalKey::Invariant(_) => 1,
            GrantGoalKey::Parallelism { tasks, .. } => tasks,
            GrantGoalKey::Class(class) => self.classes[&class].max_parallel_tasks,
        };
        let mut enforcer = EnforcerCostInput::unbounded(CompactRange::point(100_000.0)?, 8);
        enforcer.max_parallel_tasks = tasks;
        if let GrantGoalKey::Class(class) = goal.grant {
            enforcer.hard_memory_bytes = self.classes[&class].hard_memory_bytes;
            enforcer.spill_policy = self.classes[&class].spill_policy;
        }
        let is_source = logical.key.children.is_empty();
        Ok(Box::new([PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: logical.key.children.clone(),
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(expr.0),
            provided: provided(),
            child_goals: logical
                .key
                .children
                .iter()
                .map(|child| {
                    (
                        *child,
                        OptimizationGoal {
                            required: self.child_required,
                            ..goal
                        },
                    )
                })
                .collect(),
            local_cost: cost(if is_source { 20_000.0 } else { 10_000.0 }),
            source_filter_apply_cost: None,
            task_supply: if is_source {
                TaskSupplyContract::Source { tasks }
            } else {
                TaskSupplyContract::Streaming { input: 0 }
            },
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: enforcer,
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: true,
        }]))
    }
}

fn optimize_pipeline(
    classes: &[ResourceGrantClass],
    root_required: RequiredProperties,
) -> (CascadesEngine, GrantOptimization, GroupId, GroupId) {
    let mut budget = crate::cascades::budget::SearchBudget::default();
    budget.max_grant_classes = classes.len() as u8;
    let mut memo = Memo::new(budget);
    let input_required = memo.intern_required(required()).unwrap();
    let root_required = memo.intern_required(root_required).unwrap();
    let mut groups = Vec::new();
    for ordinal in 0..3 {
        let group = memo.create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(981 + ordinal),
                scalars: Box::new([]),
                children: groups.last().copied().into_iter().collect(),
            },
            LogicalPayloadId(ordinal as u32),
            EquivalenceProof::Initial,
        )
        .unwrap();
        groups.push(group);
    }
    let mut registry = ImplementationRegistry::default();
    registry
        .register_implementation(PipelineImplementation {
            classes: classes.iter().map(|class| (class.id, *class)).collect(),
            child_required: input_required,
        })
        .unwrap();
    let mut engine = CascadesEngine::new(memo, registry);
    let optimized = engine
        .optimize_for_grants(
            groups[2],
            OptimizationGoal {
                required: root_required,
                row_goal: RowGoal::All,
                objective: ObjectiveProfile::Latency,
                grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
                context: OptimizationContextId(0),
            },
            AdmissibleGrantSetId(9),
            classes.iter().copied(),
            SearchMode::Direct,
        )
        .unwrap();
    (engine, optimized, groups[0], groups[2])
}

fn class(id: u32, tasks: u16, memory: u64) -> ResourceGrantClass {
    ResourceGrantClass {
        id: ResourceGrantClassId(id),
        max_parallel_tasks: tasks,
        hard_memory_bytes: memory,
        spill_policy: SpillPolicy::Forbidden,
    }
}

#[test]
fn capacity_survives_sharing_empty_enforcement_and_streaming_parents() {
    let classes = [
        class(1, 1, 1 << 20),
        class(2, 4, 2 << 20),
        class(3, 4, 4 << 20),
    ];
    let (engine, optimized, source, root) = optimize_pipeline(&classes, required());
    assert_eq!(optimized.winners.len(), 3);
    let winners = &optimized.winners;
    assert_eq!(
        winners[1].winner.candidate, winners[2].winner.candidate,
        "memory-only changes may still share a capacity-specific winner"
    );
    assert_ne!(winners[0].winner.candidate, winners[1].winner.candidate);
    for (winner, class) in winners.iter().zip(classes) {
        assert_eq!(
            winner.goal.grant,
            GrantGoalKey::Parallelism {
                admissible: AdmissibleGrantSetId(9),
                tasks: class.max_parallel_tasks,
            }
        );
        assert_eq!(
            winner.winner.cost.output_pipeline_tasks,
            class.max_parallel_tasks
        );
        // Independent work oracle: one source and two projections, regardless
        // of capacity. Only the estimated completion time can improve.
        assert_eq!(winner.winner.cost.work_latency.expected, 40_000.0);
    }
    assert!(
        winners[1].winner.cost.critical_path.expected
            < winners[0].winner.cost.critical_path.expected
    );
    for group in [source, root] {
        let goals: Vec<_> = engine.memo().group(group).unwrap().winners().collect();
        assert_eq!(goals.len(), 2);
        for (goal, winner) in goals {
            let GrantGoalKey::Parallelism { tasks, .. } = goal.grant else {
                panic!("capacity lost")
            };
            assert_eq!(winner.cost.output_pipeline_tasks, tasks);
        }
    }
}

#[test]
fn required_sort_retains_memory_class_while_children_share_capacity() {
    let classes = [class(1, 4, 1_024), class(2, 4, 2 << 20)];
    let mut ordered = required();
    ordered.ordering = OrderingRequirement::Ordered(RequiredOrdering {
        keys: Box::new([OrderingKey {
            column: ColumnId(0),
            direction: SortDirection::Asc,
            nulls: NullOrder::Last,
            collation: None,
        }]),
        scope: OrderingScope::Global,
    });
    let (engine, optimized, source, _) = optimize_pipeline(&classes, ordered);
    assert!(matches!(
        optimized.sensitivity,
        GrantSensitivitySummary::RequiredEnforcement { .. }
    ));
    assert_eq!(
        optimized.winners.len(),
        1,
        "the no-spill 1 KiB class cannot sort 800 KiB"
    );
    assert_eq!(
        optimized.winners[0].goal.grant,
        GrantGoalKey::Class(classes[1].id)
    );
    let source_goals: Vec<_> = engine.memo().group(source).unwrap().winners().collect();
    assert_eq!(source_goals.len(), 1);
    assert!(matches!(
        source_goals[0].0.grant,
        GrantGoalKey::Parallelism { tasks: 4, .. }
    ));
}
