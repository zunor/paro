// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable compiled statement images and per-execution inputs.

use std::sync::Arc;

use crate::pipeline::StatementProgram;
use crate::runtime::{ParameterBindingEpoch, ParameterBindings};
use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::typed_parameters::TypedParameterEnv;
use paro_common::types::LogicalType;
use paro_context::{CompileEnvironmentKey, StatementContext};
use paro_optimizer::physical::{
    Fingerprint, PhysicalNodeKind, SearchSourceSpec, StableFingerprintBuilder,
};
use paro_storage::search::OpenSearchCursorResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumnDesc {
    pub name: String,
    pub logical_type: LogicalType,
}

impl ResultColumnDesc {
    pub fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            name: name.into(),
            logical_type,
        }
    }
}

/// A shareable, immutable program image produced by the compiler.
///
/// Runtime parameter values deliberately do not live here. Cloning a compiled
/// statement is therefore an O(1) operation suitable for prepared-plan caches.
#[derive(Debug, Clone)]
pub struct CompiledStatement {
    image: Arc<CompiledStatementImage>,
    compile_work: Option<paro_context::CompileWork>,
}

#[derive(Debug)]
struct CompiledStatementImage {
    program: StatementProgram,
    result_schema: Box<[ResultColumnDesc]>,
    parameter_types: Box<[LogicalType]>,
    compile_environment: CompileEnvironmentKey,
}

impl CompiledStatement {
    pub fn new(
        program: StatementProgram,
        result_schema: Vec<ResultColumnDesc>,
        parameter_types: Vec<LogicalType>,
        compile_environment: CompileEnvironmentKey,
    ) -> Self {
        Self {
            compile_work: None,
            image: Arc::new(CompiledStatementImage {
                program,
                result_schema: result_schema.into_boxed_slice(),
                parameter_types: parameter_types.into_boxed_slice(),
                compile_environment,
            }),
        }
    }

    pub fn with_compile_work(mut self, work: paro_context::CompileWork) -> Self {
        self.compile_work = Some(work);
        self
    }

    pub fn compile_work(&self) -> Option<paro_context::CompileWork> {
        self.compile_work
    }

    #[inline]
    pub fn program(&self) -> &StatementProgram {
        &self.image.program
    }

    #[inline]
    pub fn result_schema(&self) -> &[ResultColumnDesc] {
        &self.image.result_schema
    }

    #[inline]
    pub fn parameter_types(&self) -> &[LogicalType] {
        &self.image.parameter_types
    }

    #[inline]
    pub fn compile_environment(&self) -> &CompileEnvironmentKey {
        &self.image.compile_environment
    }

    /// Validate capabilities whose generation can move without a catalog
    /// epoch change. Catalog bindings/settings are checked separately by the
    /// compile-environment key; this closes the search/graph dependency gap
    /// before a cached immutable image is reused.
    pub fn dynamic_dependencies_available(&self, ctx: &StatementContext) -> bool {
        statement_program_dependencies_available(&self.image.program, ctx)
    }

    #[inline]
    pub fn is_query(&self) -> bool {
        !self.image.result_schema.is_empty()
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.image.result_schema.len()
    }

    pub fn result_names(&self) -> Vec<String> {
        self.image
            .result_schema
            .iter()
            .map(|col| col.name.clone())
            .collect()
    }

    pub fn result_types(&self) -> Vec<LogicalType> {
        self.image
            .result_schema
            .iter()
            .map(|col| col.logical_type.clone())
            .collect()
    }

    /// Returns true when both handles point at the same compiled image.
    #[inline]
    pub fn shares_image_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.image, &other.image)
    }

    /// Process-local identity only, for exact cold/warm image correlation.
    /// Not a stable physical fingerprint or proof of plan quality.
    pub fn diagnostic_image_identity(&self) -> u64 {
        Arc::as_ptr(&self.image) as usize as u64
    }
}

fn statement_program_dependencies_available(
    program: &StatementProgram,
    ctx: &StatementContext,
) -> bool {
    match program {
        StatementProgram::Portfolio(portfolio) => portfolio
            .variants
            .iter()
            .any(|variant| physical_plan_dependencies_available(&variant.plan, ctx)),
        StatementProgram::Pipeline { plan, .. } => physical_plan_dependencies_available(plan, ctx),
        StatementProgram::ExplainAnalyze { target, .. } => {
            statement_program_dependencies_available(target, ctx)
        }
        StatementProgram::Utility(_) => true,
    }
}

pub(crate) fn physical_plan_dependencies_available(
    plan: &paro_optimizer::physical::PhysicalPlan,
    ctx: &StatementContext,
) -> bool {
    fn domain_fingerprint(domain: u64, value: u64) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_u64(domain);
        builder.write_u64(value);
        builder.finish()
    }

    fn table_search_planning_state_available(
        plan: &paro_optimizer::physical::PhysicalPlan,
        table: &paro_catalog::entry::TableCatalogEntry,
    ) -> bool {
        let key = domain_fingerprint(1, table.object_id().raw());
        let Some(expected) = plan.dependencies.search_planning_signatures.get(&key) else {
            return false;
        };
        table
            .storage
            .as_ref()
            .map_or(0, |storage| storage.search_planning_signature())
            == *expected
    }

    fn search_available(
        table: &paro_catalog::entry::TableCatalogEntry,
        token: &paro_storage::search::CapabilityToken,
    ) -> bool {
        table.storage.as_ref().is_some_and(|storage| {
            matches!(
                storage.open_search_generation_snapshot_with_token(token),
                Ok(OpenSearchCursorResult::Opened(_))
            )
        })
    }

    fn graph_key(id: &paro_common::identity::GraphId) -> Fingerprint {
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.graph-generation.v1");
        fingerprint.write_bytes(id.runtime_key().as_bytes());
        fingerprint.finish()
    }

    fn graph_available(
        plan: &paro_optimizer::physical::PhysicalPlan,
        ctx: &StatementContext,
        schema_name: &str,
        graph_name: &str,
    ) -> bool {
        let id =
            paro_common::identity::GraphId::new(ctx.current_database(), schema_name, graph_name);
        let Some(expected) = plan.dependencies.graph_generations.get(&graph_key(&id)) else {
            return false;
        };
        ctx.graph_snapshot(&id)
            .is_some_and(|snapshot| snapshot.generation_id() == *expected)
    }

    plan.nodes.iter().all(|node| match &node.kind {
        PhysicalNodeKind::RowsetScan(_) => true,
        PhysicalNodeKind::VectorSearch(spec) => {
            table_search_planning_state_available(plan, &spec.table)
                && search_available(&spec.table, &spec.capability_token)
        }
        PhysicalNodeKind::SparseVectorSearch(spec) => {
            table_search_planning_state_available(plan, &spec.table)
                && search_available(&spec.table, &spec.capability_token)
        }
        PhysicalNodeKind::FullTextSearch(spec) => {
            table_search_planning_state_available(plan, &spec.table)
                && search_available(&spec.table, &spec.capability_token)
        }
        PhysicalNodeKind::AdaptiveSearch(spec) => match spec.selected.as_ref() {
            SearchSourceSpec::Vector(source) => {
                table_search_planning_state_available(plan, &spec.table)
                    && search_available(&spec.table, &source.capability_token)
            }
            SearchSourceSpec::Sparse(source) => {
                table_search_planning_state_available(plan, &spec.table)
                    && search_available(&spec.table, &source.capability_token)
            }
            SearchSourceSpec::FullText(source) => {
                table_search_planning_state_available(plan, &spec.table)
                    && search_available(&spec.table, &source.capability_token)
            }
        },
        PhysicalNodeKind::GraphScan(spec) => {
            graph_available(plan, ctx, &spec.schema_name, &spec.graph_name)
        }
        PhysicalNodeKind::GraphExpand(spec) => {
            graph_available(plan, ctx, &spec.schema_name, &spec.graph_name)
        }
        PhysicalNodeKind::GraphShortestPath(spec) => {
            graph_available(plan, ctx, &spec.schema_name, &spec.graph_name)
        }
        _ => true,
    })
}

/// A single execution of a compiled statement with query-local bindings.
///
/// This is the only accepted executor input, keeping parameter values out of
/// prepared-plan caches and making the plan/value lifetime boundary explicit.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    statement: CompiledStatement,
    bindings: Arc<ParameterBindings>,
}

impl ExecutionRequest {
    pub fn new(statement: CompiledStatement, bindings: ParameterBindings) -> Result<Self> {
        Self::validate_bindings(&statement, &bindings)?;
        Ok(Self {
            statement,
            bindings: Arc::new(bindings),
        })
    }

    fn validate_bindings(
        statement: &CompiledStatement,
        bindings: &ParameterBindings,
    ) -> Result<()> {
        if statement.parameter_types().len() != bindings.len() {
            return Err(paro_error::protocol_violation(format!(
                "compiled statement expects {} parameters, but execution supplied {}",
                statement.parameter_types().len(),
                bindings.len()
            )));
        }
        for (index, expected) in statement.parameter_types().iter().enumerate() {
            let actual = bindings
                .logical_type(paro_common::typed_parameters::RuntimeParamId::new(index))
                .expect("parameter count checked");
            if actual != expected {
                return Err(paro_error::protocol_violation(format!(
                    "parameter {} has type {}, but compiled statement expects {}",
                    index + 1,
                    actual,
                    expected
                )));
            }
        }
        Ok(())
    }

    pub fn unparameterized(statement: CompiledStatement) -> Result<Self> {
        Self::new(statement, ParameterBindings::empty())
    }

    pub fn from_typed_env(
        statement: CompiledStatement,
        parameter_env: &TypedParameterEnv,
    ) -> Result<Self> {
        Self::new(
            statement,
            ParameterBindings::from_typed_env(parameter_env, ParameterBindingEpoch::new(1)),
        )
    }

    /// Replaces a stale compiled image while retaining this execution's bindings.
    ///
    /// Revalidation is intentional here: catalog changes can cause recompilation
    /// to produce a different parameter signature even when the SQL is unchanged.
    pub fn with_statement(self, statement: CompiledStatement) -> Result<Self> {
        Self::validate_bindings(&statement, &self.bindings)?;
        Ok(Self {
            statement,
            bindings: self.bindings,
        })
    }

    #[inline]
    pub fn statement(&self) -> &CompiledStatement {
        &self.statement
    }

    pub fn into_parts(self) -> (CompiledStatement, Arc<ParameterBindings>) {
        (self.statement, self.bindings)
    }
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_context::TestStatementContextBuilder;

    use super::*;
    use crate::physical::UtilitySpec;
    use crate::pipeline::{StatementProgram, UtilityProgram};
    use paro_planner::binder::ir::statement::BoundCreateSchemaInfo;

    fn statement(parameter_types: Vec<LogicalType>) -> CompiledStatement {
        CompiledStatement::new(
            StatementProgram::Utility(UtilityProgram {
                spec: UtilitySpec::CreateSchema(BoundCreateSchemaInfo {
                    database_name: "test".to_string(),
                    schema_name: "test".to_string(),
                    if_not_exists: false,
                }),
            }),
            Vec::new(),
            parameter_types,
            TestStatementContextBuilder::minimal()
                .build()
                .compile_environment_key(),
        )
    }

    #[test]
    fn compiled_statement_clones_share_the_program_image() {
        let compiled = statement(Vec::new());
        let cloned = compiled.clone();

        assert!(compiled.shares_image_with(&cloned));
    }

    #[test]
    fn execution_request_rejects_binding_type_changes() {
        let compiled = statement(vec![LogicalType::Integer]);
        let bindings = ParameterBindings::new(
            vec![Value::BigInt(7)],
            vec![LogicalType::BigInt],
            ParameterBindingEpoch::new(1),
        )
        .unwrap();

        let error = ExecutionRequest::new(compiled, bindings).unwrap_err();

        assert!(error.to_string().contains("parameter 1 has type"));
        assert!(error.to_string().contains("expects INTEGER"));
    }

    #[test]
    fn execution_request_reuses_bindings_when_replacing_a_stale_statement() {
        let bindings = ParameterBindings::new(
            vec![Value::Integer(7)],
            vec![LogicalType::Integer],
            ParameterBindingEpoch::new(1),
        )
        .unwrap();
        let request = ExecutionRequest::new(statement(vec![LogicalType::Integer]), bindings)
            .expect("matching bindings");
        let original_bindings = request.clone().into_parts().1;
        let replacement = statement(vec![LogicalType::Integer]);

        let replaced = request
            .with_statement(replacement.clone())
            .expect("matching replacement signature");
        let (replaced_statement, replaced_bindings) = replaced.into_parts();

        assert!(replaced_statement.shares_image_with(&replacement));
        assert!(Arc::ptr_eq(&original_bindings, &replaced_bindings));
    }

    #[test]
    fn graph_dependency_admission_uses_the_statement_pin_not_the_live_publication() {
        use paro_common::identity::GraphId;
        use paro_optimizer::physical::{
            GraphScanSpec, OperatorLabel, PhysicalPlan, PhysicalPlanNode, PhysicalPlanNodeArena,
            PhysicalPlanNodeId, PlanChildren, RowType,
        };
        use paro_storage::index::graph::{
            GraphBuildInput, GraphManifest, GraphProjectionIndex, GraphRuntimeHandle, GraphState,
            GraphStorageGeneration,
        };

        struct Provider(GraphRuntimeHandle);
        impl paro_context::GraphIndexProvider for Provider {
            fn snapshot(
                &self,
                _: &GraphId,
            ) -> Option<paro_storage::index::graph::GraphReadSnapshot> {
                Some(self.0.snapshot())
            }
        }
        let generation = |id| {
            GraphStorageGeneration::from_index(
                GraphProjectionIndex::build(&GraphBuildInput {
                    graph_name: "g".into(),
                    vertex_tables: vec![],
                    edge_tables: vec![],
                    build_backward_adjacency: true,
                })
                .unwrap(),
                GraphManifest::new("g".into(), GraphState::Ready, "schema".into()),
                id,
            )
        };
        let provider = Arc::new(Provider(GraphRuntimeHandle::new(generation(1))));
        let mut context = TestStatementContextBuilder::minimal()
            .build()
            .as_ref()
            .clone();
        Arc::make_mut(&mut context.services).graph_index = provider.clone();
        let id = GraphId::new(context.current_database(), "public", "g");
        let compiled_generation = context.graph_snapshot(&id).unwrap().generation_id();
        let mut key = StableFingerprintBuilder::default();
        key.write_bytes(b"paro.graph-generation.v1");
        key.write_bytes(id.runtime_key().as_bytes());
        let mut nodes = PhysicalPlanNodeArena::default();
        let root = nodes.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output: RowType::new(vec![], vec![]),
            cardinality: None,
            kind: PhysicalNodeKind::GraphScan(Box::new(GraphScanSpec {
                vertex_info: paro_catalog::entry::VertexTableInfo {
                    table_name: "vertices".into(),
                    table_oid: 1,
                    key_column_ids: vec![0],
                    label: "Node".into(),
                    property_column_ids: vec![],
                },
                filter: None,
                table_index: 1,
                label: "Node".into(),
                graph_name: "g".into(),
                schema_name: "public".into(),
                output_types: Box::new([]),
            })),
            children: PlanChildren::Empty,
            label: OperatorLabel::new(paro_planner::plan::PlanNodeId::SYNTHETIC, "graph"),
        });
        let mut plan = PhysicalPlan::new(root, nodes, Default::default(), Default::default());
        plan.dependencies
            .graph_generations
            .insert(key.finish(), compiled_generation);
        provider.0.publish(generation(2));
        assert!(physical_plan_dependencies_available(&plan, &context));
        let mut next_statement = context.clone();
        next_statement.graph_snapshots = Default::default();
        assert!(
            !physical_plan_dependencies_available(&plan, &next_statement),
            "a new statement must recompile a generation-dependent cached plan"
        );
    }
}
