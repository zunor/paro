// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fact-backed Memo boundaries. No logical tree or representative is built.

use super::*;
use crate::cascades::scalar::ScalarKind;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::operator::bound_reference::{
    BoundRelationFactValues, BoundRelationFacts, BoundSourceColumn,
};
use paro_planner::plan::{UniqueKey, UniqueKeyColumn, UniqueKeyNullSemantics, UniqueKeyProvenance};

#[cfg(test)]
mod tests;

fn layout_column_ids(
    state: &PlannerTransformState,
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Result<Vec<ColumnId>> {
    layout
        .bindings()
        .iter()
        .zip(layout.types())
        .map(|(binding, ty)| {
            state
                .binding_ids
                .get(binding.table_index, binding.column_index, ty)
                .copied()
                .ok_or_else(|| paro_error::internal("boundary layout has an unknown column"))
        })
        .collect()
}

fn encode_distinct_provenance(
    encoder: &mut StableFingerprintBuilder,
    provenance: paro_storage::statistics::DistinctProvenance,
) {
    use paro_storage::statistics::DistinctProvenance::*;
    match provenance {
        Unknown => encoder.write_u64(0),
        Derived => encoder.write_u64(1),
        ObservedFull => encoder.write_u64(2),
        ObservedPartial {
            observed_rows,
            total_rows,
        } => {
            encoder.write_u64(3);
            encoder.write_u64(observed_rows);
            encoder.write_u64(total_rows);
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct GroupFactValue {
    can_replay: bool,
    column_domains: BTreeMap<ColumnId, GroupColumnDomain>,
    column_values: BTreeMap<ColumnId, paro_planner::operator::bound_reference::BoundColumnValues>,
    relational: bool,
    unique_keys: BTreeSet<Box<[ColumnId]>>,
    /// Structural keys whose equality is valid in the SQL GROUP BY domain.
    /// Catalog UNIQUE keys are intentionally absent unless another proof has
    /// established their NULL safety.
    grouping_unique_keys: BTreeSet<Box<[ColumnId]>>,
    /// Exact, finite SQL grouping domains. Absence means unknown; an empty set
    /// is never published. These values make disjoint UNION ALL partitions a
    /// composable uniqueness proof rather than a rule-local observation.
    grouping_domains: BTreeMap<ColumnId, BTreeSet<SafeGroupingValue>>,
    cardinality: Option<CardinalityEnvelope>,
    maximum_cardinality: Option<u64>,
    lineage: BTreeMap<ColumnId, Option<Vec<BoundSourceColumn>>>,
    control: bool,
}

/// Published immutable evidence owns its lazy value identity. A revision is a
/// read cursor, not part of this identity; repeated binding checks reuse it
/// without serializing every column, source lineage, and grouping proof.
#[derive(Debug, Default)]
struct GroupFacts {
    value: GroupFactValue,
    fingerprint: std::sync::OnceLock<Fingerprint>,
    transports: std::sync::Mutex<HashMap<u64, Vec<BoundaryTransport>>>,
}

#[derive(Debug)]
struct BoundaryTransport {
    layout: PlannerBindingLayout,
    columns: Box<[ColumnId]>,
    facts: Arc<BoundRelationFacts>,
}

impl From<GroupFactValue> for GroupFacts {
    fn from(value: GroupFactValue) -> Self {
        Self {
            value,
            fingerprint: std::sync::OnceLock::new(),
            transports: std::sync::Mutex::default(),
        }
    }
}

impl std::ops::Deref for GroupFacts {
    type Target = GroupFactValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl GroupFacts {
    /// Publish only key witnesses. Callers deriving relational keys must not
    /// materialize distributions, NDV domains and source lineage as a side
    /// effect of reading this one facet.
    fn keys_in_layout(
        &self,
        layout: &paro_planner::operator::LogicalOutputLayout,
        columns: &[ColumnId],
    ) -> Vec<UniqueKey> {
        let ordinals = columns
            .iter()
            .enumerate()
            .map(|(index, column)| (*column, index))
            .collect::<BTreeMap<_, _>>();
        self.unique_keys
            .union(&self.grouping_unique_keys)
            .filter_map(|key| {
                let columns = key
                    .iter()
                    .map(|column| {
                        let output_index = *ordinals.get(column)?;
                        Some(UniqueKeyColumn {
                            output_index,
                            binding: layout.bindings()[output_index],
                        })
                    })
                    .collect::<Option<Vec<_>>>()?;
                let null_semantics = if self.grouping_unique_keys.contains(key) {
                    UniqueKeyNullSemantics::NullsEqual
                } else {
                    UniqueKeyNullSemantics::NullsDistinct
                };
                Some(UniqueKey::new(
                    columns,
                    UniqueKeyProvenance::Structural,
                    null_semantics,
                ))
            })
            .collect()
    }

    fn fingerprint(&self) -> Fingerprint {
        *self.fingerprint.get_or_init(|| {
            let mut encoder = StableFingerprintBuilder::default();
            encoder.write_bytes(b"paro.memo.boundary-value.v1");
            self.value.encode(&mut encoder);
            encoder.finish()
        })
    }
}

impl GroupFactValue {
    fn encode(&self, encoder: &mut StableFingerprintBuilder) {
        let facts = self;
        encoder.write_u64(facts.relational as u64);
        encoder.write_u64(facts.cardinality.is_some() as u64);
        if let Some(range) = facts.cardinality {
            for value in [
                range.lower,
                range.expected_lower,
                range.expected_upper,
                range.upper,
            ] {
                encoder.write_u64(value);
            }
        }
        encoder.write_u64(facts.control as u64);
        encoder.write_u64(facts.can_replay as u64);
        encoder.write_u64(facts.maximum_cardinality.is_some() as u64);
        encoder.write_u64(facts.maximum_cardinality.unwrap_or(0));
        encoder.write_u64(facts.column_domains.len() as u64);
        for (column, domain) in &facts.column_domains {
            encoder.write_u64(column.0 as u64);
            encoder.write_u64(domain.expected_lower);
            encoder.write_u64(domain.expected_upper);
            encoder.write_u64(domain.ranking_point);
            encode_distinct_provenance(encoder, domain.provenance);
            encoder.write_u64(domain.guaranteed_upper.is_some() as u64);
            encoder.write_u64(domain.guaranteed_upper.unwrap_or(0));
        }
        encoder.write_u64(facts.column_values.len() as u64);
        for (column, value) in &facts.column_values {
            encoder.write_u64(column.0 as u64);
            encoder.write_bytes(value.encoding());
        }
        encoder.write_u64(facts.unique_keys.len() as u64);
        for key in &facts.unique_keys {
            encoder.write_u64(key.len() as u64);
            for column in key {
                encoder.write_u64(column.0 as u64);
            }
        }
        encoder.write_u64(facts.grouping_unique_keys.len() as u64);
        for key in &facts.grouping_unique_keys {
            encoder.write_u64(key.len() as u64);
            for column in key {
                encoder.write_u64(column.0 as u64);
            }
        }
        encoder.write_u64(facts.grouping_domains.len() as u64);
        for (column, domain) in &facts.grouping_domains {
            encoder.write_u64(column.0 as u64);
            encoder.write_u64(domain.len() as u64);
            for value in domain {
                value.encode(encoder);
            }
        }
        encoder.write_u64(facts.lineage.len() as u64);
        for (column, sources) in &facts.lineage {
            encoder.write_u64(column.0 as u64);
            encoder.write_u64(sources.is_some() as u64);
            if let Some(sources) = sources {
                encoder.write_u64(sources.len() as u64);
                for source in sources {
                    for value in [source.source, source.occurrence, source.column] {
                        encoder.write_u64(value as u64);
                    }
                    encoder.write_u64(source.rows.is_some() as u64);
                    if let Some(rows) = source.rows {
                        for value in [rows.min, rows.expected, rows.max] {
                            encoder.write_u64(value);
                        }
                    }
                    encoder.write_u64(source.distinct.is_some() as u64);
                    encoder.write_u64(source.distinct.unwrap_or(0));
                    encoder.write_u64(source.unique as u64);
                }
            }
        }
    }
}

/// Values whose SQL grouping equality is total and matches structural
/// equality. NULL, floating point, collated strings and nested values are
/// deliberately excluded from this proof domain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SafeGroupingValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    HugeInt(i128),
    UTinyInt(u8),
    USmallInt(u16),
    UInteger(u32),
    UBigInt(u64),
    UHugeInt(u128),
    Decimal(i128, u8, u8),
    Varchar(String),
    Blob(Vec<u8>),
    Uuid(u128),
    Date(i32),
    Timestamp(i64),
    TimestampTz(i64),
    Time(i64),
    Interval(i32, i32, i64),
}

impl SafeGroupingValue {
    fn encode(&self, encoder: &mut StableFingerprintBuilder) {
        macro_rules! primitive {
            ($($variant:ident),* $(,)?) => {
                match self {
                    $(Self::$variant(value) => Value::$variant(*value),)*
                    Self::Varchar(value) => Value::Varchar(value.clone()),
                    Self::Blob(value) => Value::Blob(value.clone()),
                    Self::Decimal(value, precision, scale) => Value::Decimal(*value, *precision, *scale),
                    Self::Interval(months, days, micros) => Value::Interval(*months, *days, *micros),
                }
            };
        }
        let value = primitive!(
            Boolean,
            TinyInt,
            SmallInt,
            Integer,
            BigInt,
            HugeInt,
            UTinyInt,
            USmallInt,
            UInteger,
            UBigInt,
            UHugeInt,
            Uuid,
            Date,
            Timestamp,
            TimestampTz,
            Time
        );
        crate::cascades::scalar_lowering::encode_value(encoder, &value);
    }

    fn from_value(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Boolean(value) => Self::Boolean(*value),
            Value::TinyInt(value) => Self::TinyInt(*value),
            Value::SmallInt(value) => Self::SmallInt(*value),
            Value::Integer(value) => Self::Integer(*value),
            Value::BigInt(value) => Self::BigInt(*value),
            Value::HugeInt(value) => Self::HugeInt(*value),
            Value::UTinyInt(value) => Self::UTinyInt(*value),
            Value::USmallInt(value) => Self::USmallInt(*value),
            Value::UInteger(value) => Self::UInteger(*value),
            Value::UBigInt(value) => Self::UBigInt(*value),
            Value::UHugeInt(value) => Self::UHugeInt(*value),
            Value::Decimal(value, precision, scale) => Self::Decimal(*value, *precision, *scale),
            Value::Varchar(value) => Self::Varchar(value.clone()),
            Value::Blob(value) => Self::Blob(value.clone()),
            Value::Uuid(value) => Self::Uuid(*value),
            Value::Date(value) => Self::Date(*value),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::TimestampTz(value) => Self::TimestampTz(*value),
            Value::Time(value) => Self::Time(*value),
            Value::Interval(months, days, micros) => Self::Interval(*months, *days, *micros),
            Value::Null(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::List(_, _)
            | Value::Struct(_, _)
            | Value::Array(_, _, _) => return None,
        })
    }
}

#[derive(Clone, Default)]
pub(super) struct BoundarySnapshot {
    groups: BTreeMap<GroupId, Arc<GroupFacts>>,
}

#[derive(Debug, Default)]
pub(super) struct BoundaryFactCache {
    entries: BTreeMap<(GroupId, bool), CachedFacts>,
}

#[derive(Debug)]
struct CachedFacts {
    read: PatternRead,
    inputs: Vec<(GroupId, Option<Arc<GroupFacts>>)>,
    facts: Arc<GroupFacts>,
    reads: Option<Arc<FactReadLog>>,
}

/// Persistent evidence edges, not a copied transitive read set. A finite log
/// permits a cache hit without rebuilding recipes, layouts or column domains.
/// Cyclic/unfinished derivations deliberately do not publish such a log.
#[derive(Debug)]
struct FactReadLog {
    local: PatternRead,
    inputs: Box<[Arc<FactReadLog>]>,
}

type ReadValidation = HashMap<usize, (Arc<FactReadLog>, bool)>;

impl FactReadLog {
    fn validate(
        root: Arc<Self>,
        ctx: &mut TransformContext<'_>,
        state: &PlannerTransformState,
        dimension: BudgetDimension,
        checked: &mut ReadValidation,
    ) -> Result<Option<bool>> {
        let identity = Arc::as_ptr(&root) as usize;
        let mut pending = vec![(root, false)];
        while let Some((read, finish)) = pending.pop() {
            if let Some(session) = &state.session {
                session.cancellation.check()?;
            }
            let key = Arc::as_ptr(&read) as usize;
            if checked.contains_key(&key) {
                continue;
            }
            if !ctx.admit_fact_work(dimension, 1)? {
                return Ok(None);
            }
            if finish {
                let current = read
                    .inputs
                    .iter()
                    .all(|input| checked[&(Arc::as_ptr(input) as usize)].1);
                checked.insert(key, (read, current));
            } else if read.local.group.index() >= ctx.memo().group_count()
                || !read.local.is_current(ctx.memo())?
            {
                checked.insert(key, (read, false));
            } else {
                ctx.record_fact_read(read.local);
                if !ctx.admit_fact_work(dimension, read.inputs.len())? {
                    return Ok(None);
                }
                pending.push((read.clone(), true));
                pending.extend(
                    read.inputs
                        .iter()
                        .rev()
                        .cloned()
                        .map(|input| (input, false)),
                );
            }
        }
        Ok(Some(checked[&identity].1))
    }
}

impl CachedFacts {
    fn matches(&self, read: PatternRead, inputs: &[(GroupId, Option<Arc<GroupFacts>>)]) -> bool {
        self.read == read
            && self.inputs.len() == inputs.len()
            && self
                .inputs
                .iter()
                .zip(inputs)
                .all(|((left, a), (right, b))| {
                    left == right
                        && match (a, b) {
                            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                            (None, None) => true,
                            _ => false,
                        }
                })
    }
}

impl BoundarySnapshot {
    pub(super) fn contains_group(&self, memo: &Memo, group: GroupId) -> bool {
        self.groups.contains_key(&memo.canonical_group(group))
    }

    /// Derive one newly published shell from already observed input facts.
    /// Staging is not allowed to choose or reconstruct input alternatives.
    /// Missing evidence in another recipe remains conservatively unknown.
    pub(super) fn settle_group(
        &mut self,
        memo: &Memo,
        state: &PlannerTransformState,
        group: GroupId,
    ) -> Result<()> {
        let group = memo.canonical_group(group);
        let facts = self.derive(memo, state, group, true)?;
        self.groups.insert(group, Arc::new(facts.into()));
        Ok(())
    }

    /// Canonical value identity for every bound relational boundary. Preserve
    /// each fact's operand identity: an unordered bag aliases a selective
    /// left input with a selective right input when their facts swap.
    pub(super) fn binding_value_fingerprint(
        &self,
        memo: &Memo,
        binding: &PatternOperand,
    ) -> Result<Fingerprint> {
        let mut groups = BTreeSet::new();
        let mut pending = vec![binding];
        while let Some(operand) = pending.pop() {
            let group = match operand {
                PatternOperand::Group(group) => *group,
                PatternOperand::Expression {
                    group, children, ..
                } => {
                    pending.extend(children.iter());
                    *group
                }
            };
            groups.insert(memo.canonical_group(group));
        }
        let mut values = Vec::with_capacity(groups.len());
        for group in groups {
            let facts = self
                .groups
                .get(&group)
                .ok_or_else(|| paro_error::internal("binding identity read unobserved facts"))?;
            values.push((group, facts.fingerprint()));
        }
        let mut encoder = StableFingerprintBuilder::default();
        encoder.write_bytes(b"paro.memo.boundary-binding-value.v2");
        encoder.write_u64(values.len() as u64);
        for (group, value) in values {
            encoder.write_u64(group.0 as u64);
            encoder.write_fingerprint(value);
        }
        Ok(encoder.finish())
    }

    /// Encode resolved boundary evidence, not internal binary join shape or
    /// the revisions of unrelated alternatives. A changed inherited input
    /// invalidates graph reuse exactly when its consumed facts change.
    pub(super) fn encode_group(
        &self,
        group: GroupId,
        encoder: &mut StableFingerprintBuilder,
    ) -> Result<()> {
        let facts = self
            .groups
            .get(&group)
            .ok_or_else(|| paro_error::internal("graph identity read unobserved facts"))?;
        facts.value.encode(encoder);
        Ok(())
    }
    /// Traverse the evidence DAG once, admitting every group, shell, edge and
    /// output column before allocating its result. Cycles cannot establish
    /// source coverage; a missing child witness is conservatively unknown.
    pub(super) fn read(
        ctx: &mut TransformContext<'_>,
        state: &PlannerTransformState,
        binding: &PatternOperand,
        dimension: BudgetDimension,
    ) -> Result<Option<Self>> {
        let mut pending = Vec::new();
        let mut operands = vec![binding];
        while let Some(operand) = operands.pop() {
            if !ctx.admit_fact_work(dimension, 1)? {
                return Ok(None);
            }
            let (group, relational) = match operand {
                PatternOperand::Group(group) => (*group, true),
                PatternOperand::Expression {
                    group, children, ..
                } => {
                    operands.extend(children.iter());
                    (*group, false)
                }
            };
            pending.push((ctx.memo().canonical_group(group), relational, false));
        }
        let mut active = BTreeSet::new();
        let mut read_logs = BTreeMap::<GroupId, Arc<FactReadLog>>::new();
        let mut checked_reads = ReadValidation::new();
        let mut result = Self {
            groups: BTreeMap::new(),
        };
        while let Some((group, relational, finish)) = pending.pop() {
            if let Some(session) = &state.session {
                session.cancellation.check()?;
            }
            if result
                .groups
                .get(&group)
                .is_some_and(|facts| facts.relational || !relational)
            {
                continue;
            }
            if finish {
                let input_count = ctx
                    .memo()
                    .cardinality_dependencies(group)
                    .count()
                    .saturating_add(if relational {
                        ctx.memo()
                            .group(group)
                            .unwrap()
                            .logical_exprs()
                            .iter()
                            .map(|expression| {
                                ctx.memo()
                                    .logical_expr(*expression)
                                    .unwrap()
                                    .key
                                    .children
                                    .len()
                            })
                            .sum::<usize>()
                    } else {
                        0
                    });
                if !ctx.admit_fact_work(dimension, 1 + input_count)? {
                    return Ok(None);
                }
                let mut input_ids = BTreeSet::new();
                if relational {
                    for expression in ctx.memo().group(group).unwrap().logical_exprs() {
                        input_ids.extend(
                            ctx.memo()
                                .logical_expr(*expression)
                                .unwrap()
                                .key
                                .children
                                .iter()
                                .map(|group| ctx.memo().canonical_group(*group)),
                        );
                    }
                }
                input_ids.extend(
                    ctx.memo()
                        .cardinality_dependencies(group)
                        .map(|(group, _)| ctx.memo().canonical_group(group)),
                );
                let inputs = input_ids
                    .into_iter()
                    .map(|id| (id, result.groups.get(&id).cloned()))
                    .collect::<Vec<_>>();
                let read = if relational {
                    PatternRead::from_group(ctx.memo(), group)?
                } else {
                    PatternRead::facts_from_group(ctx.memo(), group)?
                };
                let reads = inputs
                    .iter()
                    .map(|(input, _)| read_logs.get(input).cloned())
                    .collect::<Option<Box<[_]>>>()
                    .map(|inputs| {
                        Arc::new(FactReadLog {
                            local: read,
                            inputs,
                        })
                    });
                let mut cache = state
                    .boundary_cache
                    .lock()
                    .expect("boundary fact cache poisoned");
                if let Some(cached) = cache
                    .entries
                    .get_mut(&(group, relational))
                    .filter(|cached| cached.matches(read, &inputs))
                {
                    cached.reads = reads.clone();
                    if let Some(reads) = reads {
                        read_logs.insert(group, reads);
                    }
                    result.groups.insert(group, cached.facts.clone());
                    active.remove(&(group, relational));
                    continue;
                }
                let proof_units = if relational {
                    ctx.memo()
                        .group(group)
                        .unwrap()
                        .logical_exprs()
                        .iter()
                        .map(|expression| {
                            let logical = ctx.memo().logical_expr(*expression).unwrap();
                            logical
                                .key
                                .children
                                .iter()
                                .filter_map(|child| {
                                    result.groups.get(&ctx.memo().canonical_group(*child))
                                })
                                .map(|facts| {
                                    facts
                                        .lineage
                                        .values()
                                        .flatten()
                                        .map(Vec::len)
                                        .sum::<usize>()
                                        .saturating_add(
                                            facts
                                                .unique_keys
                                                .iter()
                                                .map(|key| key.len())
                                                .sum::<usize>(),
                                        )
                                        .saturating_add(
                                            facts
                                                .grouping_unique_keys
                                                .iter()
                                                .map(|key| key.len())
                                                .sum::<usize>(),
                                        )
                                })
                                .sum::<usize>()
                                .saturating_add(
                                    state.metadata[&logical.payload]
                                        .child_layouts
                                        .iter()
                                        .map(|layout| layout.bindings().len())
                                        .sum::<usize>(),
                                )
                        })
                        .sum()
                } else {
                    0
                };
                if !ctx.admit_fact_work(dimension, proof_units)? {
                    return Ok(None);
                }
                let derived = result.derive(ctx.memo(), state, group, relational)?;
                let facts = cache
                    .entries
                    .get(&(group, relational))
                    .filter(|cached| cached.facts.value == derived)
                    .map(|cached| cached.facts.clone())
                    .unwrap_or_else(|| Arc::new(derived.into()));
                cache.entries.insert(
                    (group, relational),
                    CachedFacts {
                        read,
                        inputs,
                        facts: facts.clone(),
                        reads: reads.clone(),
                    },
                );
                if let Some(reads) = reads {
                    read_logs.insert(group, reads);
                }
                result.groups.insert(group, facts);
                active.remove(&(group, relational));
                continue;
            }
            if active.contains(&(group, relational)) {
                continue;
            }
            let cached = state
                .boundary_cache
                .lock()
                .expect("boundary fact cache poisoned")
                .entries
                .get(&(group, relational))
                .and_then(|entry| Some((entry.facts.clone(), entry.reads.clone()?)));
            if let Some((facts, reads)) = cached {
                match FactReadLog::validate(
                    reads.clone(),
                    ctx,
                    state,
                    dimension,
                    &mut checked_reads,
                )? {
                    None => return Ok(None),
                    Some(true) => {
                        result.groups.insert(group, facts);
                        read_logs.insert(group, reads);
                        continue;
                    }
                    Some(false) => {}
                }
            }
            let width = ctx
                .memo()
                .group(group)
                .ok_or_else(|| paro_error::internal("fact reader lost group"))?
                .schema
                .columns()
                .len();
            if !ctx.admit_fact_work(dimension, 1 + width)? {
                return Ok(None);
            }
            ctx.record_fact_read(if relational {
                PatternRead::from_group(ctx.memo(), group)?
            } else {
                PatternRead::facts_from_group(ctx.memo(), group)?
            });
            active.insert((group, relational));
            pending.push((group, relational, true));
            // Copy only admitted ids. Neither the matcher nor this reader may
            // hide a recursive cardinality walk in a one-unit observation.
            let expressions = if relational {
                ctx.memo().group(group).unwrap().logical_exprs().len()
            } else {
                0
            };
            for ordinal in 0..expressions {
                if !ctx.admit_fact_work(dimension, 1)? {
                    return Ok(None);
                }
                let expression = ctx.memo().group(group).unwrap().logical_exprs()[ordinal];
                let arity = ctx
                    .memo()
                    .logical_expr(expression)
                    .unwrap()
                    .key
                    .children
                    .len();
                if !ctx.admit_fact_work(dimension, arity)? {
                    return Ok(None);
                }
                pending.extend(
                    ctx.memo()
                        .logical_expr(expression)
                        .unwrap()
                        .key
                        .children
                        .iter()
                        .map(|child| (ctx.memo().canonical_group(*child), true, false)),
                );
            }
            let dependencies = ctx.memo().cardinality_dependencies(group).count();
            if !ctx.admit_fact_work(dimension, dependencies)? {
                return Ok(None);
            }
            pending.extend(
                ctx.memo()
                    .cardinality_dependencies(group)
                    .map(|(input, _)| (ctx.memo().canonical_group(input), false, false)),
            );
        }
        Ok(Some(result))
    }

    pub(super) fn cardinality(&self, memo: &Memo, group: GroupId) -> Option<CardinalityEstimate> {
        let range = self.groups.get(&memo.canonical_group(group))?.cardinality?;
        Some(CardinalityEstimate {
            min: range.lower,
            expected: range
                .expected_lower
                .saturating_add((range.expected_upper - range.expected_lower) / 2),
            max: range.upper,
        })
    }

    /// Prove NULLability for one exact Memo-boundary column.  Native rules do
    /// not have an owned settlement tree to borrow a statistics map from, so
    /// they use the same canonical group and interned-column identity as
    /// `transport`. Missing value evidence is unknown and fails closed.
    pub(super) fn binding_is_non_null(
        &self,
        memo: &Memo,
        state: &PlannerTransformState,
        group: GroupId,
        binding: ColumnBinding,
        ty: &LogicalType,
    ) -> bool {
        let Some(column) = state
            .binding_ids
            .get(binding.table_index, binding.column_index, ty)
            .copied()
        else {
            return false;
        };
        self.groups
            .get(&memo.canonical_group(group))
            .and_then(|facts| facts.column_values.get(&column))
            .is_some_and(|value| !value.statistics().can_have_null())
    }

    pub(super) fn transport(
        &self,
        memo: &Memo,
        state: &PlannerTransformState,
        group: GroupId,
        layout: &PlannerBindingLayout,
    ) -> Result<Arc<BoundRelationFacts>> {
        let group = memo.canonical_group(group);
        let facts = self
            .groups
            .get(&group)
            .ok_or_else(|| paro_error::internal("unobserved Memo boundary"))?;
        let columns = layout_column_ids(state, layout)?;
        // The immutable evidence value owns its typed views. Revision changes
        // with the same value can reuse them; changed evidence cannot. Hashing
        // indexes a collision bucket only, with the complete layout and column
        // mapping compared before reuse (never an address-only cache key).
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        columns.hash(&mut hasher);
        layout.bindings().hash(&mut hasher);
        layout.types().hash(&mut hasher);
        let key = hasher.finish();
        let mut transports = facts
            .transports
            .lock()
            .map_err(|_| paro_error::internal("boundary transport cache poisoned"))?;
        if let Some(cached) = transports.get(&key).into_iter().flatten().find(|entry| {
            entry.columns.as_ref() == columns && entry.layout.as_ref() == layout.as_ref()
        }) {
            return Ok(cached.facts.clone());
        }
        let unique_keys = facts.keys_in_layout(layout, &columns);
        let grouping_unique_keys = unique_keys
            .iter()
            .filter(|key| key.null_semantics == UniqueKeyNullSemantics::NullsEqual)
            .cloned()
            .collect();
        let transport = Arc::new(BoundRelationFacts::new(
            BoundRelationFactValues {
                can_replay: facts.can_replay,
                cardinality: self.cardinality(memo, group),
                maximum_cardinality: facts.maximum_cardinality,
                column_domains: columns
                    .iter()
                    .map(|column| {
                        let domain = facts.column_domains.get(column);
                        paro_planner::operator::bound_reference::BoundColumnDomain {
                            expected_distinct: domain.and_then(|domain| domain.expected()),
                            guaranteed_distinct_upper: domain
                                .and_then(|domain| domain.guaranteed_upper),
                            provenance: domain
                                .map(|domain| domain.provenance)
                                .unwrap_or(paro_storage::statistics::DistinctProvenance::Unknown),
                        }
                    })
                    .collect(),
                column_values: columns
                    .iter()
                    .map(|column| facts.column_values.get(column).cloned())
                    .collect(),
                unique_keys,
                grouping_unique_keys,
                source_lineage: columns
                    .iter()
                    .map(|column| facts.lineage.get(column).cloned().flatten())
                    .collect(),
                contains_control_region: facts.control,
            },
            layout.types().to_vec(),
        ));
        transports.entry(key).or_default().push(BoundaryTransport {
            layout: layout.clone(),
            columns: columns.into_boxed_slice(),
            facts: transport.clone(),
        });
        Ok(transport)
    }

    fn derive(
        &self,
        memo: &Memo,
        state: &PlannerTransformState,
        id: GroupId,
        relational: bool,
    ) -> Result<GroupFactValue> {
        let group = memo
            .group(id)
            .ok_or_else(|| paro_error::internal("fact derivation lost group"))?;
        let mut column_values = BTreeMap::new();
        let mut column_domains = BTreeMap::new();
        for column in group.schema.ids() {
            if let Some(value) = memo.column_value_domain(id, column)? {
                column_values.insert(column, value);
            }
            if let Some(domain) = memo.column_domain(id, column) {
                column_domains.insert(column, domain);
            }
        }
        let mut inherited = None;
        let mut producer = None;
        for (input, is_producer) in memo.cardinality_dependencies(id) {
            if let Some(range) = self
                .groups
                .get(&memo.canonical_group(input))
                .and_then(|facts| facts.cardinality)
            {
                let target = if is_producer {
                    &mut producer
                } else {
                    &mut inherited
                };
                *target = Some(
                    target.map_or(range, |previous: CardinalityEnvelope| previous.hull(range)),
                );
            }
        }
        let mut cardinality = producer.or(memo.local_cardinality_envelope(id));
        if let Some(range) = inherited {
            cardinality = Some(cardinality.map_or(range, |previous| previous.hull(range)));
        }
        cardinality =
            cardinality.map(|range| range.clamp(group.logical_properties.maximum_cardinality));
        if !relational {
            return Ok(GroupFactValue {
                cardinality,
                maximum_cardinality: group.logical_properties.maximum_cardinality,
                column_domains,
                column_values,
                control: true,
                ..GroupFactValue::default()
            });
        }
        let mut common: Option<BTreeMap<ColumnId, Option<Vec<BoundSourceColumn>>>> = None;
        let mut common_grouping_domains: Option<BTreeMap<ColumnId, BTreeSet<SafeGroupingValue>>> =
            None;
        let mut unique_keys = group.logical_properties.unique_keys.clone();
        let mut grouping_unique_keys = BTreeSet::new();
        let mut control = false;
        let mut can_replay = false;
        for expression in group.logical_exprs() {
            let logical = memo
                .logical_expr(*expression)
                .ok_or_else(|| paro_error::internal("fact recipe lost expression"))?;
            let metadata = state
                .metadata
                .get(&logical.payload)
                .ok_or_else(|| paro_error::internal("fact recipe has no metadata"))?;
            let operator = &state.payloads.logical[logical.payload.index()]
                .semantic_template
                .operator;
            // Replayability is a semantic proof of the equivalence class.
            // One finite, effect-free derivation proves it; a recursive
            // identity alternative must not erase that proof. As with keys,
            // unknown evidence from another equivalent recipe is not false.
            can_replay |= logical.key.scalars.iter().all(|root| {
                state
                    .scalars
                    .get(*root)
                    .is_some_and(|scalar| scalar.properties.can_repeat_evaluation())
            }) && logical.key.children.iter().all(|child| {
                self.groups
                    .get(&memo.canonical_group(*child))
                    .is_some_and(|facts| facts.can_replay)
            }) && !matches!(
                operator,
                LogicalOperator::ExternalTable(_)
                    | LogicalOperator::ExternalProject(_)
                    | LogicalOperator::TableFunctionGet(_)
                    | LogicalOperator::DependentJoin(_)
                    | LogicalOperator::RecursiveCTE(_)
                    | LogicalOperator::CTERef(_)
                    | LogicalOperator::DelimGet(_)
                    | LogicalOperator::MaterializedCTE(_)
            );
            let child_layouts = metadata
                .child_layouts
                .iter()
                .map(Arc::as_ref)
                .collect::<Vec<_>>();
            let child_keys = logical
                .key
                .children
                .iter()
                .zip(&metadata.child_layouts)
                .map(|(group, layout)| {
                    if self.groups.contains_key(&memo.canonical_group(*group)) {
                        let columns = layout_column_ids(state, layout)?;
                        Ok(self.groups[&memo.canonical_group(*group)]
                            .keys_in_layout(layout, &columns))
                    } else {
                        Ok(Vec::new())
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let layout = operator.output_layout_from_child_refs(&child_layouts);
            let local_keys = crate::statistics::unique_keys::derive_unique_keys_from_facts(
                operator,
                &layout,
                &child_layouts,
                &child_keys.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            );
            for key in local_keys {
                let null_safe = key.null_semantics == UniqueKeyNullSemantics::NullsEqual;
                let key = key
                    .columns
                    .iter()
                    .map(|column| {
                        let ty = layout.types().get(column.output_index)?;
                        let id = *state.binding_ids.get(
                            column.binding.table_index,
                            column.binding.column_index,
                            ty,
                        )?;
                        metadata.output_columns.contains(&id).then_some(id)
                    })
                    .collect::<Option<BTreeSet<_>>>();
                if let Some(key) = key {
                    if !key.is_empty() {
                        if null_safe {
                            grouping_unique_keys.insert(key.iter().copied().collect());
                        }
                        unique_keys.insert(key.into_iter().collect());
                    }
                }
            }
            let child = |index: usize| {
                logical
                    .key
                    .children
                    .get(index)
                    .and_then(|group| self.groups.get(&memo.canonical_group(*group)))
            };
            control |= matches!(
                operator,
                LogicalOperator::MaterializedCTE(_) | LogicalOperator::RecursiveCTE(_)
            ) || matches!(operator, LogicalOperator::Join(Join::Comparison(join)) if !join.duplicate_eliminated_columns.is_empty())
                || (0..logical.key.children.len())
                    .any(|index| child(index).is_none_or(|facts| facts.control));
            let column_at = |index: usize, ordinal: usize| -> Option<ColumnId> {
                let layout = metadata.child_layouts.get(index)?;
                let binding = layout.bindings().get(ordinal)?;
                state
                    .binding_ids
                    .get(
                        binding.table_index,
                        binding.column_index,
                        layout.types().get(ordinal)?,
                    )
                    .copied()
            };
            let scalar_column = |root: ScalarExprId| -> Option<ColumnId> {
                match state.scalars.get(root)?.kind {
                    ScalarKind::Column(column) => Some(column),
                    _ => None,
                }
            };
            let scalar_domain = |root: ScalarExprId, index: usize| {
                let scalar = state.scalars.get(root)?;
                if let ScalarKind::Constant { value } = &scalar.kind {
                    if matches!(
                        scalar.logical_type,
                        paro_common::types::LogicalType::VarcharCollation(_)
                    ) {
                        return None;
                    }
                    return SafeGroupingValue::from_value(value.value())
                        .map(|value| BTreeSet::from([value]));
                }
                let column = scalar_column(root)?;
                child(index)?.grouping_domains.get(&column).cloned()
            };
            let mut local_grouping_domains = BTreeMap::new();
            for (ordinal, output) in metadata.output_columns.iter().copied().enumerate() {
                let domain = match operator {
                    LogicalOperator::Projection(_) => logical
                        .key
                        .scalars
                        .get(ordinal)
                        .and_then(|root| scalar_domain(*root, 0)),
                    LogicalOperator::Aggregate(aggregate) if ordinal < aggregate.groups.len() => {
                        logical
                            .key
                            .scalars
                            .get(ordinal)
                            .and_then(|root| scalar_domain(*root, 0))
                    }
                    LogicalOperator::SetOperation(setop)
                        if setop.setop_type == paro_planner::operator::SetOpType::Union
                            && setop.setop_all =>
                    {
                        let left = column_at(0, ordinal)
                            .and_then(|column| child(0)?.grouping_domains.get(&column).cloned());
                        let right = column_at(1, ordinal)
                            .and_then(|column| child(1)?.grouping_domains.get(&column).cloned());
                        left.zip(right).map(|(mut left, right)| {
                            left.extend(right);
                            left
                        })
                    }
                    LogicalOperator::Filter(_)
                    | LogicalOperator::Order(_)
                    | LogicalOperator::Limit(_)
                    | LogicalOperator::TopN(_)
                    | LogicalOperator::Window(_)
                    | LogicalOperator::EmptyResult(_)
                    | LogicalOperator::RowFetch(_)
                    | LogicalOperator::ExternalProject(_) => {
                        child(0).and_then(|facts| facts.grouping_domains.get(&output).cloned())
                    }
                    LogicalOperator::MaterializedCTE(_) => {
                        child(1).and_then(|facts| facts.grouping_domains.get(&output).cloned())
                    }
                    _ => None,
                };
                if let Some(domain) = domain.filter(|domain| !domain.is_empty()) {
                    local_grouping_domains.insert(output, domain);
                }
            }
            if let LogicalOperator::SetOperation(setop) = operator {
                let union_children = (setop.setop_type == paro_planner::operator::SetOpType::Union
                    && setop.setop_all)
                    .then(|| child(0).zip(child(1)))
                    .flatten();
                if let Some((left, right)) = union_children {
                    let key_ordinals = |facts: &GroupFacts, index: usize| {
                        facts
                            .grouping_unique_keys
                            .iter()
                            .filter_map(|key| {
                                key.iter()
                                    .map(|column| {
                                        let layout = metadata.child_layouts.get(index)?;
                                        layout.bindings().iter().zip(layout.types()).position(
                                            |(binding, ty)| {
                                                state.binding_ids.get(
                                                    binding.table_index,
                                                    binding.column_index,
                                                    ty,
                                                ) == Some(column)
                                            },
                                        )
                                    })
                                    .collect::<Option<BTreeSet<_>>>()
                            })
                            .collect::<Vec<_>>()
                    };
                    let left_keys = key_ordinals(left, 0);
                    let right_keys = key_ordinals(right, 1);
                    for left_key in &left_keys {
                        for right_key in &right_keys {
                            for ordinal in 0..metadata.output_columns.len() {
                                let left_domain = column_at(0, ordinal)
                                    .and_then(|column| left.grouping_domains.get(&column));
                                let right_domain = column_at(1, ordinal)
                                    .and_then(|column| right.grouping_domains.get(&column));
                                let Some((left_domain, right_domain)) =
                                    left_domain.zip(right_domain)
                                else {
                                    continue;
                                };
                                if left_domain.is_disjoint(right_domain) {
                                    let mut key =
                                        left_key.union(right_key).copied().collect::<BTreeSet<_>>();
                                    key.insert(ordinal);
                                    let key = key
                                        .into_iter()
                                        .filter_map(|ordinal| {
                                            metadata.output_columns.get(ordinal).copied()
                                        })
                                        .collect::<BTreeSet<_>>()
                                        .into_iter()
                                        .collect::<Box<[_]>>();
                                    unique_keys.insert(key.clone());
                                    grouping_unique_keys.insert(key);
                                }
                            }
                        }
                    }
                }
            }
            let lineage = |index: usize, column: ColumnId| {
                child(index)
                    .and_then(|facts| facts.lineage.get(&column))
                    .cloned()
                    .flatten()
            };
            let mut local = BTreeMap::new();
            for (ordinal, column) in metadata.output_columns.iter().copied().enumerate() {
                let source = |get: &paro_planner::operator::Get, source_index: usize| {
                    (get.table.is_some() && get.stored_column(source_index).is_some()).then(|| {
                        vec![BoundSourceColumn {
                            source: get.table_index,
                            occurrence: id.index(),
                            column: ordinal,
                            rows: cardinality.map(|range| CardinalityEstimate {
                                min: range.lower,
                                expected: range.expected_lower.saturating_add(
                                    (range.expected_upper - range.expected_lower) / 2,
                                ),
                                max: range.upper,
                            }),
                            distinct: group
                                .logical_properties
                                .column_domains
                                .get(&column)
                                .and_then(|domain| domain.expected()),
                            unique: unique_keys.iter().any(|key| key.as_ref() == [column]),
                        }]
                    })
                };
                let sources = match operator {
                    LogicalOperator::Get(get) => source(get, ordinal),
                    LogicalOperator::SearchScan(search) => search
                        .projections
                        .get(ordinal)
                        .and_then(|expression| match expression {
                            Expression::Reference(reference) => Some(reference.index),
                            Expression::ColumnRef(column)
                                if column.depth == 0
                                    && column.binding.table_index == search.get.table_index =>
                            {
                                Some(column.binding.column_index)
                            }
                            _ => None,
                        })
                        .and_then(|index| source(&search.get, index)),
                    LogicalOperator::FullTextFilterScan(search) => search
                        .get
                        .returned_types
                        .iter()
                        .enumerate()
                        .find(|(index, ty)| {
                            state.binding_ids.get(search.get.table_index, *index, ty)
                                == Some(&column)
                        })
                        .and_then(|(index, _)| source(&search.get, index)),
                    LogicalOperator::Filter(_) => lineage(0, column),
                    LogicalOperator::Projection(_) => logical
                        .key
                        .scalars
                        .get(ordinal)
                        .and_then(|root| scalar_column(*root))
                        .and_then(|column| lineage(0, column)),
                    LogicalOperator::SetOperation(setop)
                        if setop.setop_type == paro_planner::operator::SetOpType::Union
                            && setop.setop_all =>
                    {
                        column_at(0, ordinal)
                            .and_then(|column| lineage(0, column))
                            .zip(column_at(1, ordinal).and_then(|column| lineage(1, column)))
                            .and_then(|(mut left, right)| {
                                // A source-work identity cannot represent two bag
                                // occurrences, even when their Memo group is shared.
                                if left
                                    .iter()
                                    .any(|a| right.iter().any(|b| a.source == b.source))
                                {
                                    return None;
                                }
                                left.extend(right);
                                Some(left)
                            })
                    }
                    LogicalOperator::Join(Join::Comparison(join))
                        if join.duplicate_eliminated_columns.is_empty() && !join.delim_flipped =>
                    {
                        if join.join_type.preserves_left_values() {
                            lineage(0, column)
                        } else {
                            None
                        }
                        .or_else(|| {
                            if join.join_type.preserves_right_values() {
                                lineage(1, column)
                            } else {
                                None
                            }
                        })
                    }
                    _ => None,
                };
                local.insert(column, sources);
            }
            if let Some(common) = &mut common {
                for (column, sources) in common.iter_mut() {
                    // A witness from just one alternative is insufficient.
                    // Different paths or incomplete coverage remain unknown.
                    if local.get(column) != Some(sources) {
                        *sources = None;
                    }
                }
            } else {
                common = Some(local);
            }
            if let Some(common) = &mut common_grouping_domains {
                // A finite domain proven by any equivalent expression is a
                // relation fact. Multiple proofs intersect: missing evidence
                // is unknown, not a contradiction that erases a stronger
                // proof supplied by another alternative.
                for (column, domain) in local_grouping_domains {
                    common
                        .entry(column)
                        .and_modify(|current| {
                            current.retain(|value| domain.contains(value));
                        })
                        .or_insert(domain);
                }
                common.retain(|_, domain| !domain.is_empty());
            } else {
                common_grouping_domains = Some(local_grouping_domains);
            }
        }
        Ok(GroupFactValue {
            column_domains,
            column_values,
            can_replay,
            relational,
            unique_keys,
            grouping_unique_keys,
            grouping_domains: common_grouping_domains.unwrap_or_default(),
            cardinality,
            maximum_cardinality: group.logical_properties.maximum_cardinality,
            lineage: common.unwrap_or_default(),
            control: control || group.logical_exprs().is_empty(),
        })
    }
}
