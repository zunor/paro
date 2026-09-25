// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed physical payload encoding; wire domains and order are unchanged.

use super::render::properties::collect_explain_properties;
use super::{PhysicalIdentityError, PhysicalPlan};
use crate::logical::operator::join::{JoinComparisonType, JoinCondition};
use crate::physical::edges::PhysicalEdgeKind;
use crate::physical::explain::types::ExplainValue;
use crate::physical::identity::{Fingerprint, StableFingerprintBuilder};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::row_type::{ColumnIdentity, RowType};
use crate::physical::specs::PhysicalNodeKind;
use paro_common::types::LogicalType;
use std::hash::{Hash, Hasher};
pub(super) fn write_row_type(builder: &mut StableFingerprintBuilder, row: &RowType) {
    builder.write_u64(row.names.len() as u64);
    for name in &row.names {
        builder.write_bytes(name.as_bytes());
    }
    builder.write_u64(row.types.len() as u64);
    for logical_type in &row.types {
        write_logical_type(builder, logical_type);
    }
    builder.write_u64(row.identities.len() as u64);
    for identity in &row.identities {
        write_column_identity(builder, identity);
    }
}

fn write_column_identity(builder: &mut StableFingerprintBuilder, identity: &ColumnIdentity) {
    match identity {
        ColumnIdentity::Visible { name, qualifier } => {
            builder.write_u64(0);
            builder.write_bytes(name.as_bytes());
            match qualifier {
                Some(path) => {
                    builder.write_u64(1);
                    builder.write_u64(path.len() as u64);
                    for component in path.iter() {
                        builder.write_bytes(component.as_bytes());
                    }
                }
                None => builder.write_u64(0),
            }
        }
        ColumnIdentity::Internal => builder.write_u64(1),
        ColumnIdentity::InternalNamed(name) => {
            builder.write_u64(2);
            builder.write_bytes(name.as_bytes());
        }
        ColumnIdentity::Locator { object_id } => {
            builder.write_u64(3);
            builder.write_u64(*object_id);
        }
    }
}

fn write_logical_type(builder: &mut StableFingerprintBuilder, logical_type: &LogicalType) {
    match logical_type {
        LogicalType::Boolean => builder.write_u64(0),
        LogicalType::TinyInt => builder.write_u64(1),
        LogicalType::SmallInt => builder.write_u64(2),
        LogicalType::Integer => builder.write_u64(3),
        LogicalType::BigInt => builder.write_u64(4),
        LogicalType::HugeInt => builder.write_u64(5),
        LogicalType::UTinyInt => builder.write_u64(6),
        LogicalType::USmallInt => builder.write_u64(7),
        LogicalType::UInteger => builder.write_u64(8),
        LogicalType::UBigInt => builder.write_u64(9),
        LogicalType::UHugeInt => builder.write_u64(10),
        LogicalType::Float => builder.write_u64(11),
        LogicalType::Double => builder.write_u64(12),
        LogicalType::Decimal { precision, scale } => {
            builder.write_u64(13);
            builder.write_u64(u64::from(*precision));
            builder.write_u64(u64::from(*scale));
        }
        LogicalType::Varchar => builder.write_u64(14),
        LogicalType::VarcharCollation(collation) => {
            builder.write_u64(15);
            builder.write_bytes(collation.as_bytes());
        }
        LogicalType::TsVector => builder.write_u64(16),
        LogicalType::TsQuery => builder.write_u64(17),
        LogicalType::Date => builder.write_u64(18),
        LogicalType::Timestamp => builder.write_u64(19),
        LogicalType::TimestampTz => builder.write_u64(20),
        LogicalType::Time => builder.write_u64(21),
        LogicalType::Interval => builder.write_u64(22),
        LogicalType::Blob => builder.write_u64(23),
        LogicalType::Uuid => builder.write_u64(24),
        LogicalType::Json => builder.write_u64(25),
        LogicalType::Jsonb => builder.write_u64(26),
        LogicalType::Null => builder.write_u64(27),
        LogicalType::IntegerLiteral(value) => {
            builder.write_u64(28);
            builder.write_i64(*value);
        }
        LogicalType::StringLiteral => builder.write_u64(29),
        LogicalType::Unknown => builder.write_u64(30),
        LogicalType::Array(element, length) => {
            builder.write_u64(31);
            builder.write_u64(*length as u64);
            write_logical_type(builder, element);
        }
        LogicalType::List(element) => {
            builder.write_u64(32);
            write_logical_type(builder, element);
        }
        LogicalType::Struct(fields) => {
            builder.write_u64(33);
            builder.write_u64(fields.len() as u64);
            for (name, field_type) in fields {
                builder.write_bytes(name.as_bytes());
                write_logical_type(builder, field_type);
            }
        }
    }
}

/// Hash adapter used only for fields whose Rust representation already has a
/// value-semantic `Hash` implementation.  The adapter deliberately encodes
/// primitive writes through `StableFingerprintBuilder`, so it never inherits
/// the platform-dependent byte order or hasher state of `DefaultHasher`.
struct CanonicalHasher<'a> {
    builder: &'a mut StableFingerprintBuilder,
}

impl Hasher for CanonicalHasher<'_> {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.builder.write_bytes(bytes);
    }

    fn write_u8(&mut self, value: u8) {
        self.builder.write_u64(u64::from(value));
    }

    fn write_u16(&mut self, value: u16) {
        self.builder.write_u64(u64::from(value));
    }

    fn write_u32(&mut self, value: u32) {
        self.builder.write_u64(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        self.builder.write_u64(value);
    }

    fn write_u128(&mut self, value: u128) {
        self.builder.write_bytes(&value.to_le_bytes());
    }

    fn write_usize(&mut self, value: usize) {
        self.builder.write_u64(value as u64);
    }

    fn write_i8(&mut self, value: i8) {
        self.builder.write_i64(i64::from(value));
    }

    fn write_i16(&mut self, value: i16) {
        self.builder.write_i64(i64::from(value));
    }

    fn write_i32(&mut self, value: i32) {
        self.builder.write_i64(i64::from(value));
    }

    fn write_i64(&mut self, value: i64) {
        self.builder.write_i64(value);
    }

    fn write_i128(&mut self, value: i128) {
        self.builder.write_bytes(&value.to_le_bytes());
    }

    fn write_isize(&mut self, value: isize) {
        self.builder.write_i64(value as i64);
    }
}

pub(super) fn write_hashed<T: Hash>(builder: &mut StableFingerprintBuilder, tag: &[u8], value: &T) {
    builder.write_bytes(tag);
    value.hash(&mut CanonicalHasher { builder });
}

/// Hash a sequence with its cardinality in the transcript.  The standard
/// `Hash` implementation for slices is intentionally not used directly here:
/// it is allowed to omit the length, while the identity contract must
/// distinguish `[a, b]` from `[a, b, c]` even when the common prefix hashes to
/// the same byte stream.
pub(super) fn write_hashed_slice<T: Hash>(
    builder: &mut StableFingerprintBuilder,
    tag: &[u8],
    values: &[T],
) {
    builder.write_bytes(tag);
    builder.write_u64(values.len() as u64);
    for value in values {
        value.hash(&mut CanonicalHasher { builder });
    }
}

fn write_index_matrix(builder: &mut StableFingerprintBuilder, tag: &[u8], values: &[Box<[usize]>]) {
    builder.write_bytes(tag);
    builder.write_u64(values.len() as u64);
    for row in values {
        builder.write_u64(row.len() as u64);
        for value in row {
            builder.write_u64(*value as u64);
        }
    }
}

pub(super) fn write_optional_fingerprint(
    builder: &mut StableFingerprintBuilder,
    value: Option<Fingerprint>,
) {
    match value {
        Some(value) => {
            builder.write_u64(1);
            builder.write_fingerprint(value);
        }
        None => builder.write_u64(0),
    }
}

/// Encode the physical payload through the public, bounded EXPLAIN value
/// model.  Unlike the old Debug fallback this is an explicit schema: no
/// pointer, arena id, allocator address, or formatter-specific struct dump is
/// part of the identity.  The operator-specific EXPLAIN property builders are
/// the single semantic projection shared by human and machine consumers.
pub(super) fn write_canonical_kind(
    plan: &PhysicalPlan,
    id: PhysicalPlanNodeId,
    builder: &mut StableFingerprintBuilder,
) -> Result<(), PhysicalIdentityError> {
    let node = plan.node(id);
    builder.write_bytes(b"physical-kind-explain-schema.v1");
    builder.write_bytes(node.kind.name().as_bytes());
    let properties = collect_explain_properties(plan, id, node);
    builder.write_u64(properties.len() as u64);
    for property in properties {
        builder.write_bytes(property.label.as_bytes());
        write_explain_value(builder, &property.value);
    }
    write_semantic_kind_fields(builder, &node.kind)?;
    Ok(())
}

mod graph;
mod search;
#[cfg(test)]
mod tests;

fn write_semantic_kind_fields(
    builder: &mut StableFingerprintBuilder,
    kind: &PhysicalNodeKind,
) -> Result<(), PhysicalIdentityError> {
    use crate::physical::scalar_identity::physical_expression_fingerprint as expression_fingerprint;

    fn write_expressions<'a>(
        builder: &mut StableFingerprintBuilder,
        expressions: impl IntoIterator<Item = &'a crate::expression::Expression>,
    ) {
        let expressions = expressions.into_iter().collect::<Vec<_>>();
        builder.write_u64(expressions.len() as u64);
        for expression in expressions {
            builder.write_fingerprint(expression_fingerprint(expression));
        }
    }

    fn write_strings(
        builder: &mut StableFingerprintBuilder,
        values: impl IntoIterator<Item = impl AsRef<str>>,
    ) {
        let values = values.into_iter().collect::<Vec<_>>();
        builder.write_u64(values.len() as u64);
        for value in values {
            builder.write_bytes(value.as_ref().as_bytes());
        }
    }

    fn write_spill_policy(
        builder: &mut StableFingerprintBuilder,
        policy: crate::physical::specs::SpillExecutionPolicy,
    ) {
        use crate::physical::specs::SpillExecutionPolicy;
        builder.write_u64(match policy {
            SpillExecutionPolicy::InMemory => 0,
            SpillExecutionPolicy::Adaptive => 1,
            SpillExecutionPolicy::ForcedExternal => 2,
        });
    }

    fn write_mark(
        builder: &mut StableFingerprintBuilder,
        semantics: crate::logical::operator::join::MarkJoinSemantics,
    ) {
        use crate::logical::operator::join::MarkJoinSemantics;
        match semantics {
            MarkJoinSemantics::NotMark => builder.write_u64(0),
            MarkJoinSemantics::TwoValued => builder.write_u64(1),
            MarkJoinSemantics::ThreeValuedFrom(index) => {
                builder.write_u64(2);
                builder.write_u64(index as u64);
            }
        }
    }

    fn write_mutation(
        builder: &mut StableFingerprintBuilder,
        write: &crate::physical::WriteContract,
    ) {
        builder.write_u64(write.target_object_id);
        builder.write_u64(write.target_relation.0 as u64);
        write_hashed(builder, b"modified-columns", &write.modified_columns);
        write_hashed(
            builder,
            b"modified-key-columns",
            &write.modified_key_columns,
        );
        builder.write_u64(match write.returning {
            crate::physical::ReturningImageContract::CountOnly => 0,
            crate::physical::ReturningImageContract::BeforeImage => 1,
            crate::physical::ReturningImageContract::AfterImage => 2,
        });
        match &write.mutation_safety {
            crate::physical::requirements::MutationSafetyRequirement::None => builder.write_u64(0),
            crate::physical::requirements::MutationSafetyRequirement::StableReadBeforeWrite {
                targets,
                snapshot,
            } => {
                builder.write_u64(1);
                write_hashed(builder, b"mutation-targets", targets);
                builder.write_u64(snapshot.0 as u64);
            }
        }
        // The transaction version is an execution dependency, not a different
        // operator topology. The typed snapshot slot above is structural.
    }

    fn write_external_routine(
        builder: &mut StableFingerprintBuilder,
        routine: &crate::physical::specs::ExternalRoutineDescriptor,
    ) {
        // RoutineSpec is the versioned, typed catalog contract: no pointers,
        // maps with nondeterministic order, or runtime worker handles. Keep
        // implementation/environment/permissions, not just the display label.
        builder.write_bytes(b"paro.external-routine.v1");
        builder.write_bytes(
            &serde_json::to_vec(&(&routine.identity, &routine.semantics, &routine.spec))
                .expect("typed external routine contract"),
        );
    }

    // Ordinary and partition-window aggregates own the same execution
    // payload. Encode it once, without reconstructing an operator or relying
    // on its abbreviated EXPLAIN presentation. Estimated capacity and the
    // resource operating point are not part of structural identity.
    fn write_aggregate(
        builder: &mut StableFingerprintBuilder,
        spec: &crate::physical::specs::AggregateSpec,
    ) {
        builder.write_u64(spec.grouping_key_count as u64);
        write_hashed_slice(
            builder,
            b"state-output-projection",
            &spec.state_output_projection,
        );
        write_expressions(builder, spec.projection_exprs.iter());
        write_hashed_slice(builder, b"aggregate-payload-types", &spec.payload_types);
        write_expressions(builder, spec.groups.iter());
        write_expressions(builder, spec.aggregates.iter());
        write_hashed_slice(builder, b"group-key-encodings", &spec.group_key_encodings);
        write_index_matrix(builder, b"grouping-sets", &spec.grouping_sets);
        write_index_matrix(builder, b"grouping-functions", &spec.grouping_functions);
        write_index_matrix(builder, b"aggregate-inputs", &spec.aggregate_inputs);
        write_hashed_slice(builder, b"aggregate-filters", &spec.aggregate_filters);
        write_index_matrix(builder, b"aggregate-orders", &spec.aggregate_orders);
        write_expressions(builder, spec.having_filter.iter());
        write_spill_policy(builder, spec.spill_policy);
        builder.write_u64(spec.post_reduction.is_some() as u64);
        if let Some(post) = &spec.post_reduction {
            write_hashed_slice(builder, b"post-aggregate-types", &post.aggregate_types);
            write_expressions(builder, post.reducers.iter());
            write_hashed_slice(builder, b"post-reducer-types", &post.reducer_types);
            write_expressions(builder, post.scalar_expressions.iter());
            write_hashed_slice(builder, b"post-scalar-types", &post.scalar_types);
            write_expressions(builder, std::iter::once(&post.predicate));
            write_hashed(
                builder,
                b"post-input-rollup-sources",
                &post.input_rollup_sources,
            );
        }
        builder.write_u64(spec.perfect_hash.is_some() as u64);
        if let Some(perfect) = &spec.perfect_hash {
            write_hashed_slice(builder, b"perfect-group-minima", &perfect.group_minima);
            write_hashed_slice(
                builder,
                b"perfect-group-cardinalities",
                &perfect.group_cardinalities,
            );
        }
        write_strings(builder, spec.output_names.iter());
        write_hashed_slice(builder, b"aggregate-output-types", &spec.output_types);
    }

    match kind {
        PhysicalNodeKind::Filter(spec) => {
            builder.write_u64(1);
            write_expressions(builder, spec.expressions.iter());
            write_hashed_slice(builder, b"projection-map", &spec.projection_map);
        }
        PhysicalNodeKind::Project(spec) => {
            builder.write_u64(2);
            write_expressions(builder, spec.expressions.iter());
            write_strings(builder, spec.output_names.iter());
            builder.write_u64(spec.visible_count as u64);
        }
        PhysicalNodeKind::RowsetScan(spec) => {
            builder.write_u64(3);
            builder.write_u64(spec.table_index as u64);
            builder.write_u64(spec.emit_row_id as u64);
            write_hashed_slice(
                builder,
                b"column-projection",
                spec.column_projection.columns(),
            );
            write_hashed_slice(
                builder,
                b"value-projections",
                spec.column_projection.value_projections(),
            );
            write_expressions(builder, spec.residual_predicates.iter());
            write_expressions(builder, spec.runtime_filter_expressions.iter());
            builder.write_u64(spec.predicate.is_some() as u64);
            if let Some(predicate) = &spec.predicate {
                crate::physical::predicate_identity::encode_predicate(
                    builder,
                    predicate,
                    crate::physical::scalar_identity::encode_value,
                );
            }
            builder.write_u64(spec.table.base.base.object_id.raw());
        }
        PhysicalNodeKind::Values(spec) => {
            builder.write_u64(16);
            builder.write_u64(spec.table_index as u64);
            match &spec.relation_alias {
                Some(alias) => {
                    builder.write_u64(1);
                    builder.write_bytes(alias.as_bytes());
                }
                None => builder.write_u64(0),
            }
            builder.write_u64(spec.expressions.len() as u64);
            for row in &spec.expressions {
                builder.write_u64(row.len() as u64);
                for expression in row {
                    builder.write_fingerprint(expression_fingerprint(expression));
                }
            }
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"values-output-types", &spec.output_types);
        }
        PhysicalNodeKind::ExpressionScan(spec) => {
            builder.write_u64(17);
            builder.write_u64(spec.table_index as u64);
            builder.write_u64(spec.expressions.len() as u64);
            for row in &spec.expressions {
                builder.write_u64(row.len() as u64);
                for expression in row {
                    builder.write_fingerprint(expression_fingerprint(expression));
                }
            }
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"expression-scan-output-types", &spec.output_types);
        }
        PhysicalNodeKind::Limit(spec) => {
            builder.write_u64(4);
            if let Some(limit) = &spec.limit {
                builder.write_u64(1);
                builder.write_fingerprint(expression_fingerprint(limit));
            } else {
                builder.write_u64(0);
            }
            if let Some(offset) = &spec.offset {
                builder.write_u64(1);
                builder.write_fingerprint(expression_fingerprint(offset));
            } else {
                builder.write_u64(0);
            }
        }
        PhysicalNodeKind::Sort(spec) => {
            builder.write_u64(5);
            builder.write_u64(spec.orders.len() as u64);
            for order in &spec.orders {
                builder.write_fingerprint(expression_fingerprint(&order.expression));
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
            }
            write_hashed_slice(builder, b"sort-projection", &spec.projection_map);
        }
        PhysicalNodeKind::TopN(spec) => {
            builder.write_u64(6);
            builder.write_u64(spec.orders.len() as u64);
            for order in &spec.orders {
                builder.write_fingerprint(expression_fingerprint(&order.expression));
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
            }
            write_hashed_slice(builder, b"topn-projection", &spec.projection_map);
            builder.write_u64(spec.limit as u64);
            builder.write_u64(spec.offset as u64);
        }
        PhysicalNodeKind::HashJoin(spec) => {
            builder.write_u64(7);
            builder.write_bytes(spec.join_type.to_string().as_bytes());
            builder.write_u64(match spec.anti_join_mode {
                crate::logical::operator::join::AntiJoinMode::Regular => 0,
                crate::logical::operator::join::AntiJoinMode::NullAware => 1,
            });
            match spec.mark_semantics {
                crate::logical::operator::join::MarkJoinSemantics::NotMark => builder.write_u64(0),
                crate::logical::operator::join::MarkJoinSemantics::TwoValued => {
                    builder.write_u64(1)
                }
                crate::logical::operator::join::MarkJoinSemantics::ThreeValuedFrom(index) => {
                    builder.write_u64(2);
                    builder.write_u64(index as u64);
                }
            }
            write_join_conditions(builder, &spec.key_conditions);
            write_join_conditions(builder, &spec.build_residual_conditions);
            write_hashed_slice(builder, b"hash-left-projection", &spec.left_projection);
            write_hashed_slice(
                builder,
                b"hash-build-projection",
                &spec.build_input_projection,
            );
            builder.write_u64(spec.build_output_count as u64);
            builder.write_u64(spec.build_keys_unique as u64);
            builder.write_u64(spec.probe_residual_count as u64);
            if let Some(runtime_filter) = &spec.runtime_filter {
                builder.write_u64(1);
                builder.write_fingerprint(runtime_filter.artifact);
                write_hashed_slice(
                    builder,
                    b"runtime-filter-conditions",
                    &runtime_filter.condition_indices,
                );
            } else {
                builder.write_u64(0);
            }
        }
        PhysicalNodeKind::NestedLoopJoin(spec) => {
            builder.write_u64(8);
            builder.write_bytes(spec.join_type.to_string().as_bytes());
            write_join_conditions(builder, &spec.conditions);
            write_expressions(builder, spec.arbitrary_condition.iter());
            write_hashed_slice(builder, b"nested-left-projection", &spec.left_projection);
            write_hashed_slice(builder, b"nested-right-projection", &spec.right_projection);
        }
        PhysicalNodeKind::SortRangeJoin(spec) => {
            builder.write_u64(27);
            builder.write_bytes(spec.join_type.to_string().as_bytes());
            write_mark(builder, spec.mark_semantics);
            write_join_conditions(builder, &spec.conditions);
            write_hashed_slice(builder, b"range-left", &spec.left_projection);
            write_hashed_slice(builder, b"range-right", &spec.right_projection);
            write_hashed_slice(builder, b"range-left-types", &spec.left_output_types);
            write_hashed_slice(builder, b"range-right-types", &spec.right_output_types);
        }
        PhysicalNodeKind::ClassicIeJoin(spec) => {
            builder.write_u64(28);
            builder.write_bytes(spec.join_type.to_string().as_bytes());
            write_mark(builder, spec.mark_semantics);
            write_join_conditions(builder, &spec.conditions);
            write_hashed_slice(builder, b"ie-left", &spec.left_projection);
            write_hashed_slice(builder, b"ie-right", &spec.right_projection);
            write_hashed_slice(builder, b"ie-left-types", &spec.left_output_types);
            write_hashed_slice(builder, b"ie-right-types", &spec.right_output_types);
        }
        PhysicalNodeKind::Aggregate(spec) => {
            builder.write_u64(9);
            write_aggregate(builder, spec);
        }
        PhysicalNodeKind::CrossProduct(spec) => {
            builder.write_u64(18);
            write_hashed_slice(builder, b"cross-left-types", &spec.left_output_types);
            write_hashed_slice(builder, b"cross-right-types", &spec.right_output_types);
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"cross-output-types", &spec.output_types);
            write_spill_policy(builder, spec.spill_policy);
        }
        PhysicalNodeKind::DelimJoin(spec) => {
            builder.write_u64(20);
            builder.write_u64(match spec.side {
                crate::physical::specs::DelimJoinSideSpec::Left => 0,
                crate::physical::specs::DelimJoinSideSpec::Right => 1,
            });
            write_expressions(builder, spec.duplicate_keys.iter());
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"delim-join-output-types", &spec.output_types);
        }
        PhysicalNodeKind::DelimScan(spec) => {
            builder.write_u64(21);
            match spec.target {
                crate::physical::specs::DelimScanTarget::CachedOuter => builder.write_u64(0),
                crate::physical::specs::DelimScanTarget::Values { table_index } => {
                    builder.write_u64(1);
                    builder.write_u64(table_index as u64);
                }
            }
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"delim-scan-output-types", &spec.output_types);
        }
        PhysicalNodeKind::PartitionAggregateWindow(spec) => {
            builder.write_u64(19);
            builder.write_u64(match spec.domain {
                crate::physical::specs::PartitionAggregateDomain::Global => 0,
                crate::physical::specs::PartitionAggregateDomain::Keyed => 1,
            });
            write_hashed_slice(builder, b"partition-input-types", &spec.input_types);
            write_hashed_slice(builder, b"partition-detail-columns", &spec.detail_columns);
            write_aggregate(builder, &spec.aggregate);
            write_strings(builder, spec.output_names.iter());
            write_hashed_slice(builder, b"partition-output-types", &spec.output_types);
        }
        PhysicalNodeKind::Window(spec) => {
            builder.write_u64(10);
            builder.write_u64(spec.window_index as u64);
            builder.write_u64(spec.input_width as u64);
            builder.write_u64(spec.expressions.len() as u64);
            for expression in &spec.expressions {
                builder.write_fingerprint(expression_fingerprint(
                    &crate::expression::Expression::Window(expression.clone().into()),
                ));
            }
        }
        PhysicalNodeKind::MaterializedCte(spec) => {
            builder.write_u64(11);
            builder.write_u64(spec.cte_index as u64);
            builder.write_u64(spec.ref_count as u64);
            write_strings(builder, spec.column_names.iter());
        }
        PhysicalNodeKind::RecursiveCte(spec) => {
            builder.write_u64(12);
            builder.write_u64(spec.cte_index as u64);
            builder.write_u64(spec.union_all as u64);
            write_strings(builder, spec.column_names.iter());
        }
        PhysicalNodeKind::CteScan(spec) => {
            builder.write_u64(13);
            builder.write_u64(spec.cte_index as u64);
            builder.write_u64(spec.table_index as u64);
        }
        PhysicalNodeKind::SetOperation(spec) => {
            builder.write_u64(14);
            builder.write_u64(spec.table_index as u64);
            builder.write_bytes(spec.op.to_string().as_bytes());
            builder.write_u64(spec.all as u64);
        }
        PhysicalNodeKind::DummyScan(_) | PhysicalNodeKind::EmptyResult(_) => {
            builder.write_u64(15);
        }
        PhysicalNodeKind::TableFunctionScan(spec) => {
            builder.write_u64(16);
            if let Some(binding) = &spec.bind_data {
                let payload = binding
                    .as_ref()
                    .canonical_plan_payload()
                    .ok_or(PhysicalIdentityError::UnsupportedKind { kind: kind.name() })?;
                builder.write_u64(1);
                builder.write_bytes(&payload);
            } else {
                builder.write_u64(0);
            }
            builder.write_bytes(spec.function.name.as_bytes());
            write_hashed_slice(builder, b"signature", &spec.function.arguments);
            write_hashed(builder, b"varargs", &spec.function.varargs);
            write_hashed_slice(
                builder,
                b"named-parameters",
                &spec.function.named_parameters,
            );
            builder.write_u64(spec.function.projection_pushdown as u64);
            builder.write_u64(spec.function.filter_pushdown as u64);
            builder.write_u64(spec.table_index as u64);
            write_expressions(builder, spec.arguments.iter());
            write_hashed(builder, b"projection", &spec.projection_ids);
            write_hashed_slice(builder, b"input-types", &spec.input_table_types);
            write_strings(builder, spec.input_table_names.iter());
            write_hashed_slice(builder, b"output-types", &spec.output_types);
            write_strings(builder, spec.output_names.iter());
            builder.write_u64(spec.with_ordinality as u64);
        }
        PhysicalNodeKind::Insert(spec) => {
            builder.write_u64(22);
            builder.write_u64(spec.table.base.base.object_id.raw());
            write_hashed_slice(builder, b"insert-columns", &spec.column_index_map);
            write_hashed_slice(builder, b"insert-types", &spec.expected_types);
            builder.write_u64(spec.copy_from_read_csv as u64);
            write_mutation(builder, &spec.write);
            builder.write_u64(spec.on_conflict.is_some() as u64);
            if let Some(conflict) = &spec.on_conflict {
                write_hashed_slice(builder, b"conflict-target", &conflict.target_columns);
                match &conflict.action {
                    crate::logical::operator::InsertOnConflictAction::DoNothing => {
                        builder.write_u64(0)
                    }
                    crate::logical::operator::InsertOnConflictAction::DoUpdate {
                        target_columns,
                        source_columns,
                    } => {
                        builder.write_u64(1);
                        write_hashed_slice(builder, b"conflict-update-target", target_columns);
                        write_hashed_slice(builder, b"conflict-update-source", source_columns);
                    }
                }
            }
        }
        PhysicalNodeKind::Update(spec) => {
            builder.write_u64(23);
            builder.write_u64(spec.table.base.base.object_id.raw());
            write_hashed_slice(builder, b"update-columns", &spec.columns);
            builder.write_u64(spec.row_id_index as u64);
            write_mutation(builder, &spec.write);
        }
        PhysicalNodeKind::Delete(spec) => {
            builder.write_u64(24);
            builder.write_u64(spec.table.base.base.object_id.raw());
            builder.write_u64(spec.row_id_index as u64);
            builder.write_u64(spec.is_full_table_delete as u64);
            write_mutation(builder, &spec.write);
        }
        PhysicalNodeKind::MutationInputSpool(spec) => {
            builder.write_u64(25);
            builder.write_u64(spec.barrier.0 as u64);
            write_hashed(builder, b"mutation-spool-targets", &spec.targets);
            builder.write_u64(spec.snapshot.0 as u64);
        }
        PhysicalNodeKind::CopyToFile(spec) => {
            builder.write_u64(26);
            builder.write_bytes(&spec.bind_data.canonical_plan_payload());
            builder.write_bytes(spec.file_path.as_bytes());
            builder.write_u64(spec.per_thread_output as u64);
            write_hashed_slice(builder, b"copy-output-types", &spec.output_types);
        }
        PhysicalNodeKind::ExternalProject(spec) => {
            builder.write_u64(29);
            builder.write_u64(spec.routines.len() as u64);
            for routine in &spec.routines {
                write_external_routine(builder, routine);
            }
            builder.write_u64(spec.expressions.len() as u64);
            for expression in &spec.expressions {
                builder.write_bytes(expression.output_name.as_bytes());
                builder.write_fingerprint(
                    crate::physical::scalar_identity::physical_expression_fingerprint(
                        &expression.expression,
                    ),
                );
                builder.write_bytes(
                    &serde_json::to_vec(&expression.routine_meta)
                        .expect("typed bound external call"),
                );
            }
            write_hashed_slice(builder, b"external-input-types", &spec.input_types);
            write_strings(builder, spec.input_names.iter());
        }
        PhysicalNodeKind::ExternalTable(spec) => {
            builder.write_u64(30);
            write_external_routine(builder, &spec.routine);
            write_hashed_slice(builder, b"external-worker-types", &spec.worker_output_types);
            write_hashed_slice(
                builder,
                b"external-emitted-types",
                &spec.emitted_output_types,
            );
            builder.write_u64(spec.argument_count as u64);
            builder.write_u64(spec.lateral as u64);
            builder.write_u64(spec.parameterized as u64);
        }
        _ => {
            if graph::write(builder, kind) || search::write(builder, kind) {
                return Ok(());
            }
            return Err(PhysicalIdentityError::UnsupportedKind { kind: kind.name() });
        }
    }
    Ok(())
}

fn write_join_conditions(builder: &mut StableFingerprintBuilder, conditions: &[JoinCondition]) {
    builder.write_u64(conditions.len() as u64);
    for condition in conditions {
        builder.write_fingerprint(
            crate::physical::scalar_identity::physical_expression_fingerprint(&condition.left),
        );
        builder.write_fingerprint(
            crate::physical::scalar_identity::physical_expression_fingerprint(&condition.right),
        );
        builder.write_u64(match condition.comparison {
            JoinComparisonType::Equal => 0,
            JoinComparisonType::NotEqual => 1,
            JoinComparisonType::LessThan => 2,
            JoinComparisonType::GreaterThan => 3,
            JoinComparisonType::LessThanOrEqual => 4,
            JoinComparisonType::GreaterThanOrEqual => 5,
            JoinComparisonType::NotDistinctFrom => 6,
            JoinComparisonType::DistinctFrom => 7,
        });
    }
}

fn write_explain_value(builder: &mut StableFingerprintBuilder, value: &ExplainValue) {
    match value {
        ExplainValue::String(value) => {
            builder.write_u64(0);
            builder.write_bytes(value.as_bytes());
        }
        ExplainValue::Integer(value) => {
            builder.write_u64(1);
            builder.write_i64(*value);
        }
        ExplainValue::Unsigned(value) => {
            builder.write_u64(2);
            builder.write_u64(*value);
        }
        ExplainValue::Float(value) => {
            builder.write_u64(3);
            builder.write_u64(value.to_bits());
        }
        ExplainValue::Bool(value) => {
            builder.write_u64(4);
            builder.write_u64(*value as u64);
        }
        ExplainValue::Bytes(value) => {
            builder.write_u64(5);
            builder.write_u64(*value);
        }
        ExplainValue::List(values) => {
            builder.write_u64(6);
            builder.write_u64(values.len() as u64);
            for value in values {
                write_explain_value(builder, value);
            }
        }
    }
}

pub(super) fn edge_kind_key(kind: PhysicalEdgeKind) -> (u64, Option<Fingerprint>) {
    match kind {
        PhysicalEdgeKind::Data => (0, None),
        PhysicalEdgeKind::Control => (1, None),
        PhysicalEdgeKind::RuntimeFilter(fingerprint) => (2, Some(fingerprint)),
        PhysicalEdgeKind::SharedSpool(fingerprint) => (3, Some(fingerprint)),
        PhysicalEdgeKind::FixpointFeedback(fingerprint) => (4, Some(fingerprint)),
    }
}
