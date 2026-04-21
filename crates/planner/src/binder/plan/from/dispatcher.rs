// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundFromItem;
use crate::binder::Binder;
use crate::operator::LogicalOperator;
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_table_ref(&mut self, table_ref: BoundFromItem) -> Result<LogicalOperator> {
        match table_ref {
            BoundFromItem::BaseTable(base_ref) => self.plan_base_table_ref(base_ref),
            BoundFromItem::Join(join_ref) => self.plan_join_ref(join_ref),
            BoundFromItem::Subquery(sub_ref) => self.plan_subquery_ref(sub_ref),
            BoundFromItem::TableFunction(tf_ref) => self.plan_table_function_ref(tf_ref),
            BoundFromItem::ExternalRoutine(routine_ref) => {
                self.plan_external_routine_ref(routine_ref)
            }
            BoundFromItem::CTE(cte_ref) => self.plan_cte_ref(cte_ref),
            BoundFromItem::GraphTable(graph_ref) => self.plan_graph_table_ref(graph_ref),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::{test_binder, test_binder_with_search_path};
    use crate::operator::{ComparisonJoin, Join, LogicalExternalTable};
    use crate::verify::verify_physical_planner_invariants;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::entry::{CreateRoutineInfo, OnCreateConflict};
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_catalog::search_path::CatalogSearchEntry;
    use paro_common::types::LogicalType;
    use paro_parser::parse_one;
    use paro_routine::{
        CapabilityProfile, DeclaredEnvSpec, PermissionSpec, PythonEntrypointRef,
        PythonImplementationRef, PythonRuntimeSelector, RoutineArgument, RoutineExecutionContract,
        RoutineFamily, RoutineImplementationRef, RoutineNullPolicy, RoutineOwner, RoutineReturn,
        RoutineSecurityMode, RoutineSemantics, RoutineSideEffects, RoutineStability,
        RoutineTableColumn, RowSemantics, SourceBlobRef, TableRoutineContract,
    };
    use std::process::Output;

    fn contains_dependent_join(plan: &LogicalOperator) -> bool {
        let mut stack = vec![plan];
        while let Some(op) = stack.pop() {
            if matches!(op, LogicalOperator::DependentJoin(_)) {
                return true;
            }
            for child in op.children() {
                stack.push(&child.operator);
            }
        }
        false
    }

    fn find_first_comparison_join(plan: &LogicalOperator) -> Option<&ComparisonJoin> {
        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => Some(join),
            _ => plan
                .children()
                .iter()
                .find_map(|child| find_first_comparison_join(&child.operator)),
        }
    }

    fn find_external_table(plan: &LogicalOperator) -> Option<&LogicalExternalTable> {
        match plan {
            LogicalOperator::ExternalTable(table) => Some(table),
            _ => plan
                .children()
                .iter()
                .find_map(|child| find_external_table(&child.operator)),
        }
    }

    fn install_table_routine(binder: &Binder, name: &str) {
        let catalog = binder.catalog();
        catalog.initialize(false);
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let schema = catalog
            .get_schema(&txn, "public")
            .expect("public schema should exist");
        schema
            .create_routine(
                &txn,
                CreateRoutineInfo {
                    catalog: "paro".to_string(),
                    schema: "public".to_string(),
                    name: name.to_string(),
                    owner: RoutineOwner {
                        principal: "paro".to_string(),
                    },
                    arguments: vec![RoutineArgument {
                        name: Some("a".to_string()),
                        data_type: LogicalType::Integer,
                    }],
                    family: RoutineFamily::TableBatch,
                    return_type: RoutineReturn::Table(vec![RoutineTableColumn {
                        name: "value".to_string(),
                        data_type: LogicalType::Integer,
                    }]),
                    execution_contract: RoutineExecutionContract::Table(
                        TableRoutineContract { rows_hint: Some(4) },
                    ),
                    semantics: RoutineSemantics {
                        stability: RoutineStability::Stable,
                        null_policy: RoutineNullPolicy::CalledOnNullInput,
                        side_effects: RoutineSideEffects::None,
                        row_semantics: RowSemantics::RelationExpanding,
                        may_block: false,
                    },
                    implementation: RoutineImplementationRef::Python(PythonImplementationRef {
                        source_blob: SourceBlobRef {
                            id: format!("inline:public:{name}"),
                            inline_source: "return [value for value in a.materialize_py()]"
                                .to_string(),
                        },
                        entrypoint: PythonEntrypointRef::Batch {
                            handler: "batch".to_string(),
                        },
                        runtime: PythonRuntimeSelector::SystemDefault,
                    }),
                    environment: DeclaredEnvSpec::empty(PythonRuntimeSelector::SystemDefault),
                    permissions: PermissionSpec {
                        security_mode: RoutineSecurityMode::Invoker,
                        capability_profile: CapabilityProfile::process_default(),
                    },
                    on_conflict: OnCreateConflict::ErrorOnConflict,
                    sql: format!(
                        "CREATE FUNCTION public.{name}(a INTEGER) RETURNS TABLE (value INTEGER) LANGUAGE python AS $$return a$$"
                    ),
                },
            )
            .expect("install table routine");
    }

    fn test_binder_with_public_search_path() -> Binder {
        test_binder_with_search_path(vec![CatalogSearchEntry::schema_only("public")])
    }

    fn lateral_probe_sql(case: &str) -> &'static str {
        match case {
            "nested_outer_correlated_subquery_inside_lateral_rhs" => {
                "SELECT * \
                 FROM (VALUES (10), (20)) AS o(grp) \
                 CROSS JOIN LATERAL ( \
                   SELECT EXISTS( \
                     SELECT 1 \
                     WHERE EXISTS( \
                       SELECT 1 \
                       FROM (VALUES (10, 4), (20, 5)) AS d(grp, score) \
                       WHERE d.grp = o.grp \
                     ) \
                   ) AS has_match \
                 ) AS s"
            }
            other => panic!("unknown lateral probe case: {other}"),
        }
    }

    fn run_lateral_probe(case: &str) -> Output {
        let exe = std::env::current_exe().expect("current test binary");
        std::process::Command::new(exe)
            .arg("--exact")
            .arg("binder::plan::from::dispatcher::tests::lateral_probe_harness")
            .arg("--nocapture")
            .env("PARO_LATERAL_CASE", case)
            .env("RUST_MIN_STACK", "33554432")
            .output()
            .expect("run lateral probe subprocess")
    }

    fn assert_lateral_probe_succeeds(case: &str) {
        let output = run_lateral_probe(case);
        assert!(output.status.success(), "{output:?}");
    }

    #[test]
    fn planner_flattens_inner_lateral_join_into_delim_ready_comparison_join() {
        let mut binder = test_binder();
        let statement =
            parse_one("SELECT * FROM (SELECT 1 AS x) t JOIN LATERAL (SELECT t.x AS y) s ON true")
                .expect("parse")
                .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));

        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Inner);
        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
        assert_eq!(join.conditions.len(), 1);
    }

    #[test]
    fn planner_flattens_cross_lateral_join_into_inner_comparison_join() {
        let mut binder = test_binder();
        let statement =
            parse_one("SELECT * FROM (SELECT 1 AS x) t CROSS JOIN LATERAL (SELECT t.x AS y) s")
                .expect("parse")
                .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));

        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Inner);
        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
        assert_eq!(join.conditions.len(), 1);
    }

    #[test]
    fn planner_flattens_left_lateral_join_with_on_true() {
        let mut binder = test_binder();
        let statement = parse_one(
            "SELECT * FROM (SELECT 1 AS x) t LEFT JOIN LATERAL (SELECT t.x AS y) s ON true",
        )
        .expect("parse")
        .stmt;
        let bound = binder.bind(statement).expect("bind");

        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Left);
        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
        assert_eq!(join.conditions.len(), 1);
    }

    #[test]
    fn planner_flattens_subquery_inside_inner_join_on_condition() {
        let mut binder = test_binder();
        let statement = parse_one(
            "SELECT * \
             FROM (SELECT 1 AS x) t \
             JOIN (SELECT 1 AS y) s \
               ON s.y = t.x \
              AND EXISTS (SELECT 1 WHERE t.x = 1)",
        )
        .expect("parse")
        .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));
    }

    #[test]
    fn planner_flattens_nested_outer_correlated_subquery_inside_lateral_rhs() {
        assert_lateral_probe_succeeds("nested_outer_correlated_subquery_inside_lateral_rhs");
    }

    #[test]
    fn planner_lowers_external_table_routine_with_layout_sensitive_argument_projection() {
        let mut binder = test_binder_with_public_search_path();
        install_table_routine(&binder, "py_expand");
        let statement = parse_one("SELECT * FROM py_expand(1)").expect("parse").stmt;
        let bound = binder.bind(statement).expect("bind");

        let table = find_external_table(&bound.plan.operator).expect("external table");
        assert!(!table.parameterized);
        let child = table.child.as_ref().expect("external table child");
        let LogicalOperator::Projection(projection) = &child.operator else {
            panic!("external table child must be a projection");
        };
        assert_eq!(
            projection.output_names,
            vec!["__external_arg_1".to_string()]
        );
    }

    #[test]
    fn planner_flattens_lateral_external_table_routine_into_comparison_join() {
        let mut binder = test_binder_with_public_search_path();
        install_table_routine(&binder, "py_scale");
        let statement = parse_one(
            "SELECT t.x, s.value \
             FROM (VALUES (1), (2), (3)) AS t(x) \
             CROSS JOIN LATERAL py_scale(t.x) AS s \
             ORDER BY 1, 2",
        )
        .expect("parse")
        .stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));
        let join = find_first_comparison_join(&bound.plan.operator).expect("comparison join");
        assert_eq!(join.join_type, crate::operator::JoinType::Inner);

        let table = find_external_table(&bound.plan.operator).expect("external table");
        assert!(table.lateral);
        assert!(table.parameterized);
    }

    #[test]
    fn lateral_probe_harness() {
        let Ok(case) = std::env::var("PARO_LATERAL_CASE") else {
            return;
        };

        let mut binder = test_binder();
        let statement = parse_one(lateral_probe_sql(&case)).expect("parse").stmt;
        let bound = binder.bind(statement).expect("bind");

        assert!(!contains_dependent_join(&bound.plan.operator));
        verify_physical_planner_invariants(&bound.plan.operator)
            .expect("flattened plan invariants");

        tracing::info!(
            target: "paro_planner.lateral_probe",
            "verified lateral probe case={case}"
        );
    }
}
