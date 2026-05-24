// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::operators::external::runtime_bridge::{
    ExternalRoutineDescriptor, ExternalRuntimeBridge,
};

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_external_project(
        &mut self,
        project: &LogicalExternalProject,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(project.child.as_ref())?;
        let input_names = align_output_names(
            project.child.output_names(),
            project.child.types().len(),
            "external project input",
        )?;
        let routines = project
            .expressions
            .iter()
            .map(|expr| external_routine_descriptor(&expr.routine_meta))
            .collect::<Vec<_>>();
        let spec = ExternalProjectSpec {
            routines: routines.into_boxed_slice(),
            expressions: project.expressions.clone().into_boxed_slice(),
            cost: project.cost,
            bridge: Arc::new(ExternalRuntimeBridge::default_bridge()),
            input_names: input_names.into_boxed_slice(),
            input_types: project.child.types().into_boxed_slice(),
            output_names: align_output_names(
                project.output_names.clone(),
                project.returned_types.len(),
                "external project output",
            )?
            .into_boxed_slice(),
            output_types: project.returned_types.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::ExternalProject(spec), vec![child]))
    }

    pub(crate) fn lower_external_table(
        &mut self,
        table: &LogicalExternalTable,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = table
            .child
            .as_deref()
            .map(|child| self.generate_node(child))
            .transpose()?;
        let argument_count = match &table.call_expression {
            Expression::Function(function) => function.children.len(),
            _ => 0,
        };
        let passthrough_count = child
            .map(|child_id| {
                self.plan_node_output(child_id)
                    .types
                    .len()
                    .saturating_sub(argument_count)
            })
            .unwrap_or(0);
        let worker_output_count =
            table
                .returned_types
                .len()
                .checked_sub(passthrough_count)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "external table emitted output has fewer columns ({}) than pass-through columns ({passthrough_count})",
                        table.returned_types.len()
                    ))
                })?;
        let spec = ExternalTableSpec {
            routine: external_routine_descriptor(&table.call),
            worker_output_types: table.returned_types[..worker_output_count]
                .to_vec()
                .into_boxed_slice(),
            emitted_output_types: table.returned_types.clone().into_boxed_slice(),
            argument_count,
            lateral: table.lateral,
            parameterized: table.parameterized,
            estimated_cardinality: 1,
            cost: table.cost,
            bridge: Arc::new(ExternalRuntimeBridge::default_bridge()),
        };
        let children = child.into_iter().collect::<Vec<_>>();
        Ok((PhysicalNodeKind::ExternalTable(spec), children))
    }
}

fn external_routine_descriptor(
    meta: &paro_external::routine::bound::BoundRoutineCallMeta,
) -> ExternalRoutineDescriptor {
    let label = meta
        .spec
        .as_ref()
        .map(|spec| format!("{}.{}", spec.schema, spec.name))
        .unwrap_or_else(|| format!("{:?}", meta.identity));
    ExternalRoutineDescriptor {
        label,
        identity: meta.identity.clone(),
        semantics: meta.semantics.clone(),
        spec: meta.spec.clone(),
    }
}
