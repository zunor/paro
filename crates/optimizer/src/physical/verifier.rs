// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};

use crate::physical::{PhysicalEdgeKind, PhysicalPlan, PhysicalPlanNodeId};

pub struct PhysicalPlanVerifier;

impl PhysicalPlanVerifier {
    pub fn verify(plan: &PhysicalPlan) -> Result<()> {
        if plan.nodes.is_empty() || plan.nodes.get(plan.root).is_none() {
            return Err(paro_error::internal("physical plan has no valid root node"));
        }
        if plan.properties.len() != plan.nodes.len() {
            return Err(paro_error::internal(
                "physical plan property map is incomplete",
            ));
        }

        let mut dependencies = vec![Vec::<PhysicalPlanNodeId>::new(); plan.nodes.len()];
        let mut artifact_owners = BTreeMap::new();
        for node in plan.nodes.iter() {
            if node.id.index() >= plan.nodes.len() {
                return Err(paro_error::internal("physical node ID is out of range"));
            }
            let properties = plan.properties.get(node.id).ok_or_else(|| {
                paro_error::internal("physical node has no optimizer property contract")
            })?;
            properties.provided.validate()?;
            properties.cumulative_cost.validate()?;
            match (properties.region_owner, properties.origin) {
                (Some(owner), crate::physical::PlanOrigin::SpecializedRegion(origin))
                    if owner == origin => {}
                (None, crate::physical::PlanOrigin::SpecializedRegion(_)) | (Some(_), _) => {
                    return Err(paro_error::internal(
                        "physical node region owner disagrees with its plan origin",
                    ));
                }
                (None, _) => {}
            }
            if properties.region_owner.is_none() && !properties.owned_artifacts.is_empty() {
                return Err(paro_error::internal(
                    "physical node owns an auxiliary artifact outside a planning region",
                ));
            }
            for artifact in properties.owned_artifacts.iter().copied() {
                if artifact_owners
                    .insert(artifact.fingerprint, (node.id, artifact.kind))
                    .is_some()
                {
                    return Err(paro_error::internal(
                        "physical auxiliary artifact has more than one region owner",
                    ));
                }
            }
            if let crate::physical::PhysicalNodeKind::MutationInputSpool(spec) = &node.kind {
                let [child] = plan.child_ids(&node.children) else {
                    return Err(paro_error::internal(
                        "mutation input spool must have exactly one data child",
                    ));
                };
                let expected = crate::physical::requirements::ProvidedMutationSafety::MaterializedMutationInput {
                    targets: spec.targets.clone(),
                    snapshot: spec.snapshot,
                    barrier: spec.barrier,
                };
                if properties.provided.mutation_safety != expected {
                    return Err(paro_error::internal(
                        "mutation input spool property proof disagrees with its physical spec",
                    ));
                }
                if plan.nodes.get(*child).is_none() {
                    return Err(paro_error::internal(
                        "mutation input spool references an unknown child",
                    ));
                }
            }
            match &node.kind {
                crate::physical::PhysicalNodeKind::Insert(spec) => {
                    verify_write_sink(plan, node.id, &spec.table, &spec.write, None)?
                }
                crate::physical::PhysicalNodeKind::Update(spec) => verify_write_sink(
                    plan,
                    node.id,
                    &spec.table,
                    &spec.write,
                    Some(spec.row_id_index),
                )?,
                crate::physical::PhysicalNodeKind::Delete(spec) => verify_write_sink(
                    plan,
                    node.id,
                    &spec.table,
                    &spec.write,
                    Some(spec.row_id_index),
                )?,
                _ => {}
            }
            if let crate::physical::PhysicalNodeKind::Aggregate(spec) = &node.kind {
                let spillable =
                    spec.spill_policy != crate::physical::SpillExecutionPolicy::InMemory;
                if properties.characteristics.spillable != spillable {
                    return Err(paro_error::internal(
                        "aggregate spill policy disagrees with its physical characteristics",
                    ));
                }
                if spec.perfect_hash.is_some()
                    && spec.spill_policy != crate::physical::SpillExecutionPolicy::InMemory
                {
                    return Err(paro_error::internal(
                        "perfect-hash aggregate cannot advertise a spill path",
                    ));
                }
            }
            let declared_spill_policy = match &node.kind {
                crate::physical::PhysicalNodeKind::HashJoin(spec) => Some(spec.spill_policy),
                crate::physical::PhysicalNodeKind::CrossProduct(spec) => Some(spec.spill_policy),
                crate::physical::PhysicalNodeKind::Sort(spec) => Some(spec.spill_policy),
                _ => None,
            };
            if let Some(policy) = declared_spill_policy {
                let spillable = policy != crate::physical::SpillExecutionPolicy::InMemory;
                if properties.characteristics.spillable != spillable {
                    return Err(paro_error::internal(
                        "operator spill policy disagrees with its physical characteristics",
                    ));
                }
            }
            if matches!(
                &node.kind,
                crate::physical::PhysicalNodeKind::CrossProduct(spec)
                    if spec.spill_policy == crate::physical::SpillExecutionPolicy::Adaptive
            ) {
                return Err(paro_error::internal(
                    "cross product must select one immutable materialization representation",
                ));
            }
            if let crate::physical::PhysicalNodeKind::HashJoin(spec) = &node.kind {
                verify_hash_join_output_permutation(spec)?;
                if let Some(runtime_filter) = &spec.runtime_filter {
                    runtime_filter.mapped_key_types(&spec.key_conditions)?;
                    if let Some(resources) = plan.execution_resources {
                        if runtime_filter.resource.max_local_builders
                            != resources.max_parallel_tasks
                        {
                            return Err(paro_error::internal(
                                "runtime-filter builder count disagrees with admitted DOP",
                            ));
                        }
                    }
                    if !properties.owned_artifacts.iter().any(|artifact| {
                        artifact.kind == crate::physical::AuxiliaryArtifactKind::RuntimeFilter
                            && artifact.fingerprint == runtime_filter.artifact
                    }) {
                        return Err(paro_error::internal(
                            "runtime-filter hash join does not own its typed region artifact",
                        ));
                    }
                    let [probe, build] = plan.child_ids(&node.children) else {
                        return Err(paro_error::internal(
                            "runtime-filter hash join must have two data children",
                        ));
                    };
                    if !matches!(
                        runtime_filter.wait_policy,
                        crate::physical::RuntimeFilterWaitPolicy::WaitComplete
                    ) {
                        return Err(paro_error::internal(
                            "runtime-filter hash join has an unsupported wait policy",
                        ));
                    }
                    let matching_edges = plan
                        .edges
                        .iter()
                        .filter(|edge| {
                            edge.producer == *build
                                && edge.kind
                                    == PhysicalEdgeKind::RuntimeFilter(runtime_filter.artifact)
                        })
                        .collect::<Vec<_>>();
                    if matching_edges.is_empty() {
                        return Err(paro_error::internal(
                            "runtime-filter hash join owns no matching auxiliary edge",
                        ));
                    }
                    let expected_consumers = crate::physical::lineage::runtime_filter_consumers_in(
                        &plan.nodes,
                        &plan.children,
                        *probe,
                        spec,
                    );
                    if expected_consumers.is_empty()
                        || matching_edges.len() != expected_consumers.len()
                        || matching_edges
                            .iter()
                            .any(|edge| !expected_consumers.contains(&edge.consumer))
                    {
                        return Err(paro_error::internal(
                            "runtime-filter consumers do not match the probe's row-preserving rowset scans",
                        ));
                    }
                }
            }
            if let crate::physical::PhysicalNodeKind::RowFetch(spec) = &node.kind {
                verify_row_fetch(plan, node.id, spec)?;
            }
            if !properties
                .provided
                .satisfies(&properties.required_from_parent)
            {
                return Err(paro_error::internal(
                    "physical node does not satisfy its recorded parent requirement",
                ));
            }
            for child in plan.child_ids(&node.children) {
                if plan.nodes.get(*child).is_none() {
                    return Err(paro_error::internal(
                        "physical node references an unknown data child",
                    ));
                }
                dependencies[child.index()].push(node.id);
            }
        }

        let mut feedback_by_region = BTreeMap::new();
        let mut runtime_filter_edges = BTreeSet::new();
        let mut runtime_filter_artifacts = BTreeSet::new();
        for edge in plan.edges.iter() {
            if plan.nodes.get(edge.producer).is_none() || plan.nodes.get(edge.consumer).is_none() {
                return Err(paro_error::internal(
                    "physical auxiliary edge references an unknown node",
                ));
            }
            match edge.kind {
                PhysicalEdgeKind::FixpointFeedback(region) => {
                    if feedback_by_region.insert(region, edge.id).is_some() {
                        return Err(paro_error::internal(
                            "recursive region contains more than one feedback edge",
                        ));
                    }
                }
                PhysicalEdgeKind::RuntimeFilter(artifact) => {
                    if !runtime_filter_edges.insert((artifact, edge.consumer)) {
                        return Err(paro_error::internal(
                            "runtime-filter artifact has a duplicate consumer edge",
                        ));
                    }
                    runtime_filter_artifacts.insert(artifact);
                    let Some((owner, kind)) = artifact_owners.get(&artifact).copied() else {
                        return Err(paro_error::internal(
                            "runtime-filter edge has no typed region artifact owner",
                        ));
                    };
                    if kind != crate::physical::AuxiliaryArtifactKind::RuntimeFilter {
                        return Err(paro_error::internal(
                            "runtime-filter edge points at a differently typed artifact",
                        ));
                    }
                    let owners = plan
                        .nodes
                        .iter()
                        .filter(|node| {
                            let crate::physical::PhysicalNodeKind::HashJoin(spec) = &node.kind
                            else {
                                return false;
                            };
                            let Some(runtime_filter) = &spec.runtime_filter else {
                                return false;
                            };
                            let [probe, build] = plan.child_ids(&node.children) else {
                                return false;
                            };
                            runtime_filter.artifact == artifact
                                && *build == edge.producer
                                && crate::physical::lineage::runtime_filter_consumers_in(
                                    &plan.nodes,
                                    &plan.children,
                                    *probe,
                                    spec,
                                )
                                .contains(&edge.consumer)
                        })
                        .count();
                    if owners != 1
                        || !matches!(
                            &plan.node(owner).kind,
                            crate::physical::PhysicalNodeKind::HashJoin(spec)
                                if spec
                                    .runtime_filter
                                    .as_ref()
                                    .is_some_and(|filter| filter.artifact == artifact)
                        )
                    {
                        return Err(paro_error::internal(
                            "runtime-filter edge has no unique AuxiliaryPlanRegion owner",
                        ));
                    }
                    dependencies[edge.producer.index()].push(edge.consumer);
                }
                _ => dependencies[edge.producer.index()].push(edge.consumer),
            }
            let consumer = plan.properties.get(edge.consumer).unwrap();
            if !consumer.auxiliary_dependencies.contains(&edge.id.0) {
                return Err(paro_error::internal(
                    "auxiliary edge is not owned by its consumer property contract",
                ));
            }
        }
        for (artifact, (_, kind)) in &artifact_owners {
            if *kind == crate::physical::AuxiliaryArtifactKind::RuntimeFilter
                && !runtime_filter_artifacts.contains(artifact)
            {
                return Err(paro_error::internal(
                    "region-owned runtime-filter artifact has no physical edge",
                ));
            }
        }
        for (consumer_id, properties) in plan.properties.iter() {
            for dependency in properties.auxiliary_dependencies.iter().copied() {
                let edge = plan
                    .edges
                    .get(crate::physical::PhysicalEdgeId(dependency))
                    .ok_or_else(|| {
                        paro_error::internal("physical node owns an unknown auxiliary dependency")
                    })?;
                if edge.consumer != consumer_id {
                    return Err(paro_error::internal(
                        "physical auxiliary dependency is owned by the wrong consumer",
                    ));
                }
            }
        }
        if let Some(resources) = plan.execution_resources {
            let root = plan
                .properties
                .get(plan.root)
                .ok_or_else(|| paro_error::internal("admitted plan root has no properties"))?;
            if root.cumulative_cost.minimum_memory_bytes > resources.minimum_memory_bytes
                || resources.minimum_memory_bytes > resources.working_set_memory_bytes
                || root.cumulative_cost.preferred_memory_bytes()
                    > resources.working_set_memory_bytes
                || resources.working_set_memory_bytes > resources.memory_ceiling_bytes
                || root.cumulative_cost.peak_memory_upper > resources.memory_ceiling_bytes
                || root.cumulative_cost.external_worker_slots_upper
                    > resources.external_worker_slots
                || resources.max_parallel_tasks == 0
            {
                return Err(paro_error::internal(
                    "admitted plan exceeds its execution resource contract",
                ));
            }
            if plan.properties.iter().any(|(_, properties)| {
                !properties
                    .grant_contract
                    .accepts(resources.class, resources.max_parallel_tasks)
            }) {
                return Err(paro_error::internal(
                    "admitted plan reservation violates a node grant contract",
                ));
            }
        }
        verify_acyclic(&dependencies)
    }
}

fn verify_hash_join_output_permutation(spec: &crate::physical::HashJoinSpec) -> Result<()> {
    let output_count = spec.output_types.len();
    if spec.output_names.len() != output_count || spec.output_permutation.len() != output_count {
        return Err(paro_error::internal(
            "hash join output names, types, and permutation have inconsistent arity",
        ));
    }
    for natural_index in 0..output_count {
        let Some(output_index) = spec.output_permutation.destination_of(natural_index) else {
            return Err(paro_error::internal(
                "hash join output permutation is missing a natural ordinal",
            ));
        };
        if output_index >= output_count {
            return Err(paro_error::internal(
                "hash join output permutation contains an out-of-range destination ordinal",
            ));
        }
        if spec.output_permutation.natural_of(output_index) != Some(natural_index) {
            return Err(paro_error::internal(
                "hash join output permutation is not bijective",
            ));
        }
    }

    if !spec.output_permutation.is_identity()
        && !matches!(
            spec.join_type,
            paro_planner::operator::join::JoinType::Inner
                | paro_planner::operator::join::JoinType::Left
                | paro_planner::operator::join::JoinType::Right
                | paro_planner::operator::join::JoinType::Outer
        )
    {
        return Err(paro_error::internal(
            "non-identity hash join output permutation is unsupported for this join type",
        ));
    }
    let Some(build_output_types) = spec.build_payload_types.get(..spec.build_output_count) else {
        return Err(paro_error::internal(
            "hash join visible build output exceeds its payload layout",
        ));
    };
    let valid_mark_contract = crate::physical::hash_join_mark_contract_is_supported(
        spec.join_type,
        spec.mark_semantics,
        !spec.build_residual_conditions.is_empty(),
    );
    if !valid_mark_contract {
        return Err(paro_error::internal(
            "hash join has an incompatible MARK truth-value contract",
        ));
    }
    let mut natural_types = spec.left_output_types.to_vec();
    if spec.join_type == paro_planner::operator::join::JoinType::Mark {
        natural_types.push(paro_common::types::LogicalType::Boolean);
    } else {
        natural_types.extend(build_output_types.iter().cloned());
    }
    if natural_types.len() != output_count {
        return Err(paro_error::internal(
            "hash join natural and declared output arity disagree",
        ));
    }
    for (natural_index, natural_type) in natural_types.iter().enumerate() {
        let output_index = spec
            .output_permutation
            .destination_of(natural_index)
            .expect("permutation arity checked above");
        if spec.output_types.get(output_index) != Some(natural_type) {
            return Err(paro_error::internal(
                "hash join output permutation changes a column's logical type",
            ));
        }
    }
    Ok(())
}

fn verify_row_fetch(
    plan: &PhysicalPlan,
    node: PhysicalPlanNodeId,
    spec: &crate::physical::RowFetchSpec,
) -> Result<()> {
    let [child] = plan.child_ids(&plan.node(node).children) else {
        return Err(paro_error::internal(
            "row fetch must have exactly one carrier child",
        ));
    };
    let child_width = plan.node(*child).output.column_count();
    if spec.raw_output_names.len() != spec.raw_output_types.len() {
        return Err(paro_error::internal(
            "row-fetch raw output names and types are not aligned",
        ));
    }
    let fetched_width = spec.mappings.iter().try_fold(0usize, |width, mapping| {
        if mapping.rowid_col_idx >= child_width {
            return Err(paro_error::internal(
                "row-fetch rowid slot exceeds the carrier width",
            ));
        }
        width
            .checked_add(mapping.column_ids.len())
            .ok_or_else(|| paro_error::internal("row-fetch output width overflow"))
    })?;
    if spec.raw_output_types.len() != child_width + fetched_width {
        return Err(paro_error::internal(
            "row-fetch raw output does not match its carrier and fetched columns",
        ));
    }

    let output = &plan.node(node).output;
    let Some(projection) = &spec.projection else {
        if output.names.as_ref() != spec.raw_output_names.as_ref()
            || output.types.as_ref() != spec.raw_output_types.as_ref()
        {
            return Err(paro_error::internal(
                "standalone row-fetch output disagrees with its physical spec",
            ));
        }
        return Ok(());
    };
    let width = projection.expressions.len();
    if projection.output_names.len() != width || projection.output_types.len() != width {
        return Err(paro_error::internal(
            "row-fetch projection expressions, names, and types are not aligned",
        ));
    }
    if projection.visible_count > width {
        return Err(paro_error::internal(
            "row-fetch visible output prefix exceeds its projection width",
        ));
    }
    if projection
        .expressions
        .iter()
        .zip(projection.output_types.iter())
        .any(|(expression, output_type)| expression.return_type() != *output_type)
    {
        return Err(paro_error::internal(
            "row-fetch projection expression type disagrees with its output type",
        ));
    }
    if output.names.as_ref() != projection.output_names.as_ref()
        || output.types.as_ref() != projection.output_types.as_ref()
    {
        return Err(paro_error::internal(
            "fused row-fetch output disagrees with its projection spec",
        ));
    }
    Ok(())
}

fn verify_write_sink(
    plan: &PhysicalPlan,
    sink: PhysicalPlanNodeId,
    table: &paro_catalog::entry::TableCatalogEntry,
    write: &crate::physical::WriteContract,
    row_id_index: Option<usize>,
) -> Result<()> {
    if sink != plan.root {
        return Err(paro_error::internal(
            "mutation sink must be the root of its physical statement",
        ));
    }
    if write.target_object_id != table.object_id().raw() {
        return Err(paro_error::internal(
            "mutation sink table disagrees with its WriteContract target",
        ));
    }
    let [child] = plan.child_ids(&plan.node(sink).children) else {
        return Err(paro_error::internal(
            "mutation sink must have exactly one query input",
        ));
    };
    let child_properties = plan.properties.get(*child).ok_or_else(|| {
        paro_error::internal("mutation sink child has no optimizer property contract")
    })?;
    if let Some(row_id_index) = row_id_index {
        let child_node = plan.node(*child);
        if !matches!(
            child_node.output.identities.get(row_id_index),
            Some(crate::physical::ColumnIdentity::Locator { object_id })
                if *object_id == write.target_object_id
        ) {
            return Err(paro_error::internal(
                "mutation sink row-id slot is not the target relation locator",
            ));
        }
    }
    if !child_properties
        .provided
        .mutation_safety
        .satisfies(&write.mutation_safety)
    {
        return Err(paro_error::internal(
            "mutation query input does not satisfy its WriteContract safety requirement",
        ));
    }
    if write.returning != crate::physical::ReturningImageContract::CountOnly {
        return Err(paro_error::not_implemented(
            "mutation RETURNING image execution is not implemented; post-write locator fetch is forbidden",
        ));
    }
    Ok(())
}

fn verify_acyclic(dependencies: &[Vec<PhysicalPlanNodeId>]) -> Result<()> {
    fn visit(
        node: PhysicalPlanNodeId,
        dependencies: &[Vec<PhysicalPlanNodeId>],
        colors: &mut [u8],
    ) -> Result<()> {
        match colors[node.index()] {
            1 => {
                return Err(paro_error::internal(
                    "physical data/control dependency graph contains a cycle",
                ))
            }
            2 => return Ok(()),
            _ => {}
        }
        colors[node.index()] = 1;
        for successor in &dependencies[node.index()] {
            visit(*successor, dependencies, colors)?;
        }
        colors[node.index()] = 2;
        Ok(())
    }

    let mut colors = vec![0; dependencies.len()];
    for index in 0..dependencies.len() {
        visit(PhysicalPlanNodeId::new(index), dependencies, &mut colors)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use paro_planner::plan::PlanNodeId;

    use super::*;
    use crate::physical::cost::SearchCost;
    use crate::physical::identity::Fingerprint;
    use crate::physical::properties::{
        PhysicalCharacteristics, PhysicalGrantContract, PhysicalNodeProperties, PlanOrigin,
        PlanPropertyMap,
    };
    use crate::physical::requirements::{
        ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning,
        ProvidedProperties, ProvidedReplayability, ProvidedRepresentation, RequiredProperties,
        ResultGuarantee,
    };
    use crate::physical::{
        DummyScanSpec, OperatorLabel, PhysicalNodeKind, PhysicalPlanNode, PhysicalPlanNodeArena,
        PlanChildren, PlanChildrenArena, RowType,
    };

    fn properties(dependencies: &[u32]) -> PhysicalNodeProperties {
        PhysicalNodeProperties {
            required_from_parent: RequiredProperties::default(),
            provided: ProvidedProperties {
                ordering: ProvidedOrdering::Unordered,
                partitioning: ProvidedPartitioning::Singleton,
                materialization: ProvidedMaterialization {
                    values: BTreeSet::new(),
                    locators: BTreeMap::new(),
                },
                mutation_safety: ProvidedMutationSafety::NotApplicable,
                representation: ProvidedRepresentation::Flat,
                replayability: ProvidedReplayability::OnePass,
                result_guarantee: ResultGuarantee::Exact,
            },
            characteristics: PhysicalCharacteristics::default(),
            output_estimate: None,
            cumulative_cost: SearchCost::ZERO,
            grant_contract: PhysicalGrantContract::Invariant,
            auxiliary_dependencies: dependencies.into(),
            region_owner: None,
            owned_artifacts: Box::new([]),
            origin: PlanOrigin::Direct,
            winner_goal: Fingerprint(1),
        }
    }

    fn node(children: PlanChildren) -> PhysicalPlanNode {
        PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output: RowType::new(Vec::new(), Vec::new()),
            cardinality: None,
            kind: PhysicalNodeKind::DummyScan(DummyScanSpec),
            children,
            label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "TEST"),
        }
    }

    #[test]
    fn complete_single_node_plan_is_valid() {
        let mut nodes = PhysicalPlanNodeArena::default();
        let root = nodes.push(node(PlanChildren::Empty));
        let mut properties_by_node = PlanPropertyMap::default();
        properties_by_node.insert(root, properties(&[]));
        let plan = PhysicalPlan::new(
            root,
            nodes,
            PlanChildrenArena::default(),
            properties_by_node,
        );
        PhysicalPlanVerifier::verify(&plan).unwrap();
    }

    #[test]
    fn portfolio_shares_invariant_nodes_but_not_class_specific_enforcers() {
        use crate::physical::{
            PhysicalPlanPortfolio, ResourceGrantClass, ResourceGrantClassId, SpillPolicy,
        };

        let classes = [1, 2].map(|id| ResourceGrantClass {
            id: ResourceGrantClassId(id),
            hard_memory_bytes: u64::from(id) * 1024,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 4,
        });
        for class_specific_enforcer in [false, true] {
            let plans = classes
                .iter()
                .map(|class| {
                    let mut nodes = PhysicalPlanNodeArena::default();
                    let source = nodes.push(node(PlanChildren::Empty));
                    let mut children = PlanChildrenArena::default();
                    let root = nodes.push(node(children.pack(vec![source])));
                    let mut properties_by_node = PlanPropertyMap::default();
                    properties_by_node.insert(source, properties(&[]));
                    let mut root_properties = properties(&[]);
                    if class_specific_enforcer {
                        root_properties.grant_contract = PhysicalGrantContract::Class(class.id);
                    }
                    properties_by_node.insert(root, root_properties);
                    let plan = PhysicalPlan::new(root, nodes, children, properties_by_node);
                    // The selected provider, shape, enforcer steps and price can
                    // all be identical; the operating-point proof still matters.
                    let fingerprint = plan.portfolio_fingerprint(Fingerprint(17)).unwrap();
                    (class.id, plan, fingerprint, SearchCost::ZERO)
                })
                .collect::<Vec<_>>();
            let portfolio = PhysicalPlanPortfolio::build(
                crate::physical::ObjectiveProfile::Latency,
                classes,
                plans,
            )
            .unwrap();
            portfolio.verify().unwrap();
            assert_eq!(
                portfolio.variants.len(),
                if class_specific_enforcer { 2 } else { 1 }
            );
            if !class_specific_enforcer {
                assert_eq!(portfolio.variants[0].admissible_classes.len(), 2);
            }
        }
    }

    #[test]
    fn admission_checks_child_capacity_not_only_the_root_class() {
        let class = crate::physical::ResourceGrantClassId(7);
        let mut nodes = PhysicalPlanNodeArena::default();
        let source = nodes.push(node(PlanChildren::Empty));
        let mut children = PlanChildrenArena::default();
        let root = nodes.push(node(children.pack(vec![source])));
        let mut properties_by_node = PlanPropertyMap::default();
        let mut source_properties = properties(&[]);
        source_properties.grant_contract = PhysicalGrantContract::Parallelism { tasks: 4 };
        properties_by_node.insert(source, source_properties);
        let mut root_properties = properties(&[]);
        root_properties.grant_contract = PhysicalGrantContract::Class(class);
        properties_by_node.insert(root, root_properties);
        let mut plan = PhysicalPlan::new(root, nodes, children, properties_by_node);
        plan.execution_resources = Some(crate::physical::ExecutionResourceContract {
            class,
            minimum_memory_bytes: 0,
            working_set_memory_bytes: 0,
            memory_ceiling_bytes: 1024,
            memory_completion: crate::physical::MemoryCompletion::Guaranteed,
            max_parallel_tasks: 2,
            external_worker_slots: 0,
        });
        assert!(PhysicalPlanVerifier::verify(&plan)
            .unwrap_err()
            .to_string()
            .contains("node grant contract"));
        plan.execution_resources
            .as_mut()
            .unwrap()
            .max_parallel_tasks = 4;
        PhysicalPlanVerifier::verify(&plan).unwrap();
    }

    #[test]
    fn missing_property_contract_is_rejected() {
        let mut nodes = PhysicalPlanNodeArena::default();
        let root = nodes.push(node(PlanChildren::Empty));
        let plan = PhysicalPlan::new(
            root,
            nodes,
            PlanChildrenArena::default(),
            PlanPropertyMap::default(),
        );
        assert!(PhysicalPlanVerifier::verify(&plan).is_err());
    }

    #[test]
    fn unowned_auxiliary_edge_is_rejected() {
        let mut nodes = PhysicalPlanNodeArena::default();
        let producer = nodes.push(node(PlanChildren::Empty));
        let consumer = nodes.push(node(PlanChildren::Empty));
        let mut properties_by_node = PlanPropertyMap::default();
        properties_by_node.insert(producer, properties(&[]));
        properties_by_node.insert(consumer, properties(&[]));
        let mut plan = PhysicalPlan::new(
            consumer,
            nodes,
            PlanChildrenArena::default(),
            properties_by_node,
        );
        plan.edges.push(
            producer,
            consumer,
            PhysicalEdgeKind::RuntimeFilter(Fingerprint(2)),
        );
        assert!(PhysicalPlanVerifier::verify(&plan).is_err());
    }
}
